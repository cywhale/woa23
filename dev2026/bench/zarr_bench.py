"""White-box benchmark: reproduces what the API does to the Zarr store, stage by stage.

Runs on the machine that holds the data (VM24). It mirrors the pipeline in
`woa23_app.process_woa23_data` -- open_zarr -> sel -> to_dataframe -> polars ->
concat -> pivot -- but times each stage separately, so the cost lands on a
specific line rather than on "the query".

It also answers two questions the wall-clock alone cannot:

  read amplification
      How many elements must be decompressed to satisfy a selection, versus how
      many the caller asked for. With chunks of lat=90 x lon=360, a single-cell
      profile still pulls whole global slabs; this puts a number on that.

  where the scheduler goes
      The app sets a distributed Dask client as the default, so every compute
      round-trips through the scheduler. `--mode` A/B tests that against plain
      threaded dask, synchronous dask, and no dask at all.

Usage:
    uv run python -m bench.zarr_bench --store /path/to/data \
        --mode numpy --mode threads --mode distributed \
        --repeat 3 --out results/zarr_vm24.json

Nothing here writes to the store; every access is read-only.
"""

from __future__ import annotations

import argparse
import json
import math
import platform
import statistics
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import dask
import numpy as np
import polars as pl
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bench.queries import Query, select  # noqa: E402

GRID_DIRS = {"01": "1_degree", "04": "025_degree"}
MODES = ("numpy", "sync", "threads", "distributed")


# --- query resolution: kept identical to woa23_app.py so the comparison is fair ---

def to_lowest_grid_point(lon: float, lat: float, grid_size: float) -> tuple[float, float]:
    offset = 0.5 * grid_size
    return (math.floor(lon / grid_size) * grid_size) + offset, \
           (math.floor(lat / grid_size) * grid_size) + offset


def determine_subgroup(param: str, period: str) -> str:
    if param in ("temperature", "salinity"):
        param_group = "TS"
    elif param in ("oxygen", "o2sat", "AOU"):
        param_group = "Oxy"
    else:
        param_group = "Nutrients"

    if period == "0":
        return f"annual/{param_group}"
    if period in [str(i) for i in range(1, 13)]:
        return f"monthly/{param_group}"
    return f"seasonal/{param_group}"


def resolve(store: Path, q: Query) -> dict:
    """Turn a Query into the concrete slices and group paths the app would use."""
    grid = "04" if "25" in str(q.grid) else "01"
    grid_sz = 0.25 if grid == "04" else 1.0

    pars = sorted({p.strip() for p in q.parameter.split(",") if p.strip()})
    periods = sorted({p.strip() for p in str(q.time_period).split(",") if p.strip()})
    variables = sorted({v.strip() for v in q.append.split(",") if v.strip()})

    paths = sorted({
        store / GRID_DIRS[grid] / determine_subgroup(param, period)
        for param in pars for period in periods
    })

    dep0 = 0 if q.dep0 is None else q.dep0
    dep1 = 5501 if q.dep1 is None else q.dep1
    depth_min, depth_max = min(dep0, dep1), max(dep0, dep1)

    if q.lon1 is None or q.lat1 is None or (q.lon0 == q.lon1 and q.lat0 == q.lat1):
        lon0, lat0 = to_lowest_grid_point(q.lon0, q.lat0, grid_sz)
        lon_min, lon_max = lon0, lon0 + 0.1
        lat_min, lat_max = lat0, lat0 + 0.1
    else:
        lon0, lat0 = to_lowest_grid_point(q.lon0, q.lat0, grid_sz)
        lon1, lat1 = to_lowest_grid_point(q.lon1, q.lat1, grid_sz)
        lon_min, lon_max = min(lon0, lon1), max(lon0, lon1) + 0.1
        lat_min, lat_max = min(lat0, lat1), max(lat0, lat1) + 0.1

    return {
        "grid": grid, "pars": pars, "periods": periods, "variables": variables,
        "paths": paths,
        "sel": {
            "lon": slice(lon_min, lon_max),
            "lat": slice(lat_min, lat_max),
            "depth": slice(depth_min, depth_max),
        },
    }


# --- read amplification ---

def positions(coord: np.ndarray, chosen: np.ndarray) -> np.ndarray:
    """Index positions of `chosen` within `coord`, for numeric or string coords."""
    if coord.dtype.kind in "US" or coord.dtype == object:
        lookup = {str(v): i for i, v in enumerate(coord.tolist())}
        return np.array([lookup[str(v)] for v in chosen.tolist()], dtype=int)
    return np.searchsorted(coord, chosen)


def amplification(ds: xr.Dataset, sub: xr.Dataset, var: str) -> dict | None:
    """Elements that must be decompressed vs elements actually wanted."""
    da = ds[var]
    chunks = da.encoding.get("chunks")
    if not chunks:
        return None

    wanted = 1
    read = 1
    per_dim = {}
    for dim, chunk in zip(da.dims, chunks):
        if dim not in sub.sizes:
            return None
        n_sel = int(sub.sizes[dim])
        if n_sel == 0:
            return None
        pos = positions(np.asarray(ds[dim].values), np.asarray(sub[dim].values))
        touched = int(np.unique(pos // chunk).size)
        # Approximate: assume full chunks (edge chunks make this slightly pessimistic).
        read_dim = min(touched * chunk, int(da.sizes[dim]))
        wanted *= n_sel
        read *= read_dim
        per_dim[str(dim)] = {"selected": n_sel, "chunk": int(chunk),
                             "chunks_touched": touched, "elements_read": read_dim}

    itemsize = np.dtype(da.encoding.get("dtype", da.dtype)).itemsize
    return {
        "per_dim": per_dim,
        "elements_wanted": wanted,
        "elements_read": read,
        "factor": round(read / wanted, 1),
        "uncompressed_bytes_read": read * itemsize,
    }


# --- timing ---

class Timer:
    def __init__(self) -> None:
        self.stages: dict[str, float] = {}

    @contextmanager
    def stage(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self.stages[name] = self.stages.get(name, 0.0) + (time.perf_counter() - t0) * 1000

    def as_dict(self) -> dict:
        d = {k: round(v, 2) for k, v in self.stages.items()}
        d["total_ms"] = round(sum(self.stages.values()), 2)
        return d


def run_once(store: Path, q: Query, plan: dict, mode: str,
             cache: dict | None, collect_amp: bool) -> tuple[dict, dict, int]:
    """One full pipeline execution. Returns (stage timings, amplification, row count)."""
    t = Timer()
    result_list: list[pl.DataFrame] = []
    amp: dict = {}
    open_kwargs = {"chunks": None} if mode == "numpy" else {}

    for path in plan["paths"]:
        with t.stage("open"):
            if cache is not None and path in cache:
                ds = cache[path]
            else:
                ds = xr.open_zarr(path, **open_kwargs)
                if cache is not None:
                    cache[path] = ds

        sel_params = sorted(set(ds.coords["parameters"].values.tolist()) & set(plan["pars"]))
        sel_periods = sorted(set(ds.coords["time_periods"].values.tolist()) & set(plan["periods"]))
        if not sel_params or not sel_periods:
            continue

        with t.stage("sel"):
            sub = ds.sel(**plan["sel"], parameters=sel_params, time_periods=sel_periods)

        for var in plan["variables"]:
            if var not in sub:
                continue
            # Key on the full group path, not path.name: annual/TS, monthly/TS and
            # seasonal/TS all have name "TS" and would collide, silently dropping
            # every group but the last.
            key = f"{path.parent.parent.name}/{path.parent.name}/{path.name}:{var}"
            if collect_amp and key not in amp:
                a = amplification(ds, sub, var)
                if a:
                    amp[key] = a
            # Exactly what the app does today.
            with t.stage("to_dataframe"):
                data = sub[var].to_dataframe().reset_index()
            with t.stage("to_polars"):
                df = pl.from_pandas(data).with_columns([
                    pl.lit(var).alias("variable_type"),
                    pl.col(var).alias("value"),
                ]).drop(var)
            result_list.append(df)

    if not result_list:
        return t.as_dict(), amp, 0

    with t.stage("concat"):
        result_df = pl.concat(result_list, how="vertical").with_columns(
            (pl.col("parameters") + "_" + pl.col("variable_type")).alias("parameter_variable")
        )
    with t.stage("pivot"):
        result_df = result_df.pivot(
            index=["lon", "lat", "depth", "time_periods"],
            on="parameter_variable", values="value",
        )
    with t.stage("to_dicts"):
        rows = result_df.to_dicts()

    return t.as_dict(), amp, len(rows)


@contextmanager
def scheduler(mode: str, address: str):
    """Put dask into the requested execution mode for the duration of the block."""
    if mode in ("numpy", "sync"):
        with dask.config.set(scheduler="synchronous"):
            yield None
    elif mode == "threads":
        with dask.config.set(scheduler="threads"):
            yield None
    elif mode == "distributed":
        from dask.distributed import Client
        client = Client(address, set_as_default=True, name="woa23-bench")
        try:
            yield client
        finally:
            client.close()
    else:
        raise SystemExit(f"unknown mode: {mode}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store", type=Path, required=True)
    ap.add_argument("--mode", action="append", dest="modes", choices=MODES,
                    help="execution mode (repeatable); default: numpy, threads")
    ap.add_argument("--scheduler-address", default="tcp://localhost:8786")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--query", action="append", dest="queries")
    ap.add_argument("--include-heavy", action="store_true")
    ap.add_argument("--cache-stores", action="store_true",
                    help="open each Zarr group once and reuse it, i.e. simulate the "
                         "fix of hoisting open_zarr out of the request path")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    modes = args.modes or ["numpy", "threads"]
    queries = select(args.queries, args.include_heavy)

    print(f"store  : {args.store}")
    print(f"modes  : {', '.join(modes)}   repeat: {args.repeat}   "
          f"store cache: {'on' if args.cache_stores else 'off'}\n")

    results = []
    for mode in modes:
        with scheduler(mode, args.scheduler_address):
            for q in queries:
                plan = resolve(args.store, q)
                cache: dict | None = {} if args.cache_stores else None
                runs = []
                amp: dict = {}
                rows = 0
                for i in range(args.repeat):
                    try:
                        stages, a, rows = run_once(args.store, q, plan, mode, cache, collect_amp=(i == 0))
                    except Exception as exc:
                        runs.append({"error": repr(exc)})
                        break
                    amp = amp or a
                    runs.append(stages)
                ok = [r for r in runs if "error" not in r]
                if not ok:
                    print(f"[{mode}] {q.id:26s} FAILED: {runs[0]['error']}")
                    results.append({"mode": mode, "id": q.id, "runs": runs})
                    continue

                totals = [r["total_ms"] for r in ok]
                worst = max(amp.values(), key=lambda a: a["factor"], default=None)
                chunks_total = sum(
                    math.prod(d["chunks_touched"] for d in a["per_dim"].values())
                    for a in amp.values()
                )
                summary = {
                    "chunks_read": chunks_total,
                    "mib_decompressed": round(
                        sum(a["uncompressed_bytes_read"] for a in amp.values()) / 2**20, 1),
                    "cold_ms": ok[0]["total_ms"],
                    "warm_median_ms": round(statistics.median(totals[1:] or totals), 2),
                    "rows": rows,
                    "stage_median_ms": {
                        k: round(statistics.median([r.get(k, 0.0) for r in ok]), 2)
                        for k in ok[0] if k != "total_ms"
                    },
                    "worst_amplification": worst["factor"] if worst else None,
                }
                results.append({"mode": mode, "id": q.id, "intent": q.intent,
                                "paths": [str(p) for p in plan["paths"]],
                                "runs": runs, "amplification": amp, "summary": summary})
                amp_txt = f"x{worst['factor']:<8.1f}" if worst else "n/a      "
                print(f"[{mode:11s}] {q.id:26s} cold {summary['cold_ms']:8.1f} ms | "
                      f"warm {summary['warm_median_ms']:8.1f} ms | {rows:7d} rows | "
                      f"{chunks_total:5d} chunks / {summary['mib_decompressed']:7.1f} MiB | amp {amp_txt}")

    payload = {
        "kind": "zarr",
        "store": str(args.store),
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "host": platform.node(),
        "platform": platform.platform(),
        "versions": {"xarray": xr.__version__, "dask": dask.__version__,
                     "polars": pl.__version__, "numpy": np.__version__},
        "cache_stores": args.cache_stores,
        "repeat": args.repeat,
        "results": results,
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2, default=str))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
