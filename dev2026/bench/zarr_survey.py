"""Read-only inventory of the WOA23 Zarr store.

Answers the structural questions that decide everything downstream, without
touching or timing anything:

  * are the groups consolidated (.zmetadata present)?  -- if not, every
    xr.open_zarr in the API pays for a full key listing
  * what is the chunk shape per variable, and how big is one chunk?
  * what compressor / dtype / fill value is in use?
  * how much disk does each group take, and how many chunk files does it hold?

Usage:
    uv run python -m bench.zarr_survey --store /path/to/data --out results/survey.json
"""

from __future__ import annotations

import argparse
import json
import math
import time
from pathlib import Path

import numpy as np
import xarray as xr

GRID_DIRS = {"01": "1_degree", "04": "025_degree"}
PERIOD_DIRS = ["annual", "monthly", "seasonal"]
PARAM_GROUPS = ["TS", "Oxy", "Nutrients"]


def find_groups(store: Path) -> list[Path]:
    """Locate every leaf Zarr group, i.e. <store>/<grid>/<period>/<param_group>."""
    groups = []
    for grid_dir in GRID_DIRS.values():
        for period in PERIOD_DIRS:
            for pg in PARAM_GROUPS:
                path = store / grid_dir / period / pg
                if (path / ".zgroup").exists() or (path / "zarr.json").exists():
                    groups.append(path)
    return groups


def dir_stats(path: Path) -> dict:
    """Bytes on disk and file count, walked once."""
    nbytes = 0
    nfiles = 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                nbytes += p.stat().st_size
            except OSError:
                pass
            nfiles += 1
    return {"disk_bytes": nbytes, "file_count": nfiles}


def describe_group(path: Path) -> dict:
    consolidated = (path / ".zmetadata").exists()
    info: dict = {
        "path": str(path),
        "consolidated": consolidated,
        **dir_stats(path),
    }

    t0 = time.perf_counter()
    # consolidated=False forces the slow path so the two numbers are comparable.
    ds = xr.open_zarr(path, consolidated=consolidated or None)
    info["open_ms"] = round((time.perf_counter() - t0) * 1000, 2)

    if consolidated:
        t0 = time.perf_counter()
        xr.open_zarr(path, consolidated=False)
        info["open_unconsolidated_ms"] = round((time.perf_counter() - t0) * 1000, 2)

    info["dims"] = {k: int(v) for k, v in ds.sizes.items()}
    info["coords"] = {}
    for name in ds.coords:
        vals = ds[name].values
        if vals.dtype.kind in "US" or vals.dtype == object:
            info["coords"][name] = [str(v) for v in vals.tolist()]
        else:
            info["coords"][name] = {
                "min": float(np.min(vals)), "max": float(np.max(vals)),
                "n": int(vals.size),
                "step": float(vals[1] - vals[0]) if vals.size > 1 else None,
            }

    info["variables"] = {}
    for name, da in ds.data_vars.items():
        enc = da.encoding
        chunks = enc.get("chunks")
        itemsize = np.dtype(enc.get("dtype", da.dtype)).itemsize
        chunk_elems = int(math.prod(chunks)) if chunks else None
        compressor = enc.get("compressor")
        info["variables"][str(name)] = {
            "dims": list(da.dims),
            "shape": [int(s) for s in da.shape],
            "dtype": str(da.dtype),
            "chunks": list(chunks) if chunks else None,
            "chunk_elems": chunk_elems,
            "chunk_uncompressed_bytes": chunk_elems * itemsize if chunk_elems else None,
            "n_chunks": (
                int(math.prod(math.ceil(s / c) for s, c in zip(da.shape, chunks)))
                if chunks else None
            ),
            "compressor": str(compressor) if compressor is not None else None,
            "filters": [str(f) for f in (enc.get("filters") or [])],
            "fill_value": (
                None if enc.get("_FillValue") is None else float(enc["_FillValue"])
            ),
        }
    ds.close()
    return info


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store", type=Path, required=True,
                    help="root of the Zarr store (the directory holding 1_degree/ and 025_degree/)")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    groups = find_groups(args.store)
    if not groups:
        raise SystemExit(f"no Zarr groups found under {args.store}")

    out = []
    total_bytes = 0
    for path in groups:
        info = describe_group(path)
        total_bytes += info["disk_bytes"]
        out.append(info)
        rel = path.relative_to(args.store)
        flag = "consolidated" if info["consolidated"] else "NOT consolidated"
        print(f"{str(rel):34s} {info['disk_bytes'] / 2**30:7.2f} GiB  "
              f"{info['file_count']:8d} files  open {info['open_ms']:7.1f} ms  [{flag}]")
        for vname, v in info["variables"].items():
            if v["chunks"]:
                print(f"    {vname:5s} {str(v['shape']):28s} chunks {str(v['chunks']):26s} "
                      f"{(v['chunk_uncompressed_bytes'] or 0) / 2**20:6.2f} MiB/chunk  "
                      f"x{v['n_chunks']}")

    print(f"\ntotal on disk: {total_bytes / 2**30:.2f} GiB across {len(out)} groups")

    payload = {
        "kind": "zarr_survey",
        "store": str(args.store),
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "xarray": xr.__version__,
        "total_disk_bytes": total_bytes,
        "groups": out,
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2))
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
