"""Offline reproducer for the C16 / C16-csv byte difference in the D2b 5.2A gate.

Touches no host, needs no Zarr store, sends no request. It replays the one step
where the two arms provably diverge — the iteration order of `zarr_group_paths` —
and then runs synthetic rows through the *real* concat/pivot/serialise path so the
downstream consequence can be observed rather than asserted.

**What this does and does not establish.** The group iteration order is computed
from the arms' actual path strings and is exact. Everything after that is a
*mechanism demonstration on synthetic rows*: it shows what a reversed group order
does to the output, in the same library versions, but it is not the bytes C16
actually produced. Confirming those needs the raw responses from a diagnostic run
against the real store.

    PYTHONHASHSEED=0 uv run python -m bench.repro_c16

The seed matters: without it the group order is not reproducible at all, and the
whole comparison is meaningless. The module refuses to run unpinned.
"""

import csv
import io
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REQUIRED_PY = (3, 11)
REQUIRED_POLARS = "1.27.1"

# C16: lon0=135, lat0=15, parameter=salinity,temperature, time_period=13,0, append=mn,an
C16_PARAMS = {"lon0": 135, "lat0": 15, "parameter": "salinity,temperature",
              "time_period": "13,0", "append": "mn,an"}

# The two arms build this string differently. Both are verbatim from source:
#   reference  woa23_app.py:63   zarr_store_path = "data/"
#   candidate  api/config.py:31  zarr_store_path = os.environ["WOA23_ZARR_STORE"]
# and both then do  f"{zarr_store_path}/{grid_path}/{subgroup}", so the reference
# emits a double slash.
ARMS = {
    "reference": "data/",
    "candidate": "/home/odbadmin/python/woa23/data",
}


def preconditions() -> list[str]:
    problems = []
    if sys.version_info[:2] != REQUIRED_PY:
        problems.append(f"Python {'.'.join(map(str, REQUIRED_PY))} required; "
                        f"running {sys.version.split()[0]} — string hashing, and so "
                        f"set iteration order, is version-dependent")
    if os.environ.get("PYTHONHASHSEED") != "0":
        problems.append("PYTHONHASHSEED=0 required; without it set iteration order "
                        "is randomised per process and nothing here reproduces")
    try:
        import polars as pl
        if pl.__version__ != REQUIRED_POLARS:
            problems.append(f"polars {REQUIRED_POLARS} required (the version both "
                            f"arms ran); found {pl.__version__}")
    except ImportError:
        problems.append("polars is not importable")
    return problems


def group_order(root: str) -> list[str]:
    """The arm's `zarr_group_paths` iteration order for C16, exactly as built."""
    from api.query import determine_subgroup
    pars = list(set(c.strip() for c in C16_PARAMS["parameter"].split(",")))
    periods = sorted(set(p.strip() for p in str(C16_PARAMS["time_period"]).split(",")))
    paths = set()
    for p in pars:
        for t in periods:
            paths.add(f"{root}/1_degree/{determine_subgroup(p, t)}")
    return list(paths)


def synthetic_rows(order: list[str]):
    """Rows in the order this arm would append them to `result_list`.

    Two coordinates, so the row multiset is identical between arms and only the
    order can differ. `variables` is `list(set(...))` of the same two strings on
    both arms, so it contributes no divergence and is used as-is.
    """
    import polars as pl
    variables = list(set(v.strip() for v in C16_PARAMS["append"].split(",")))
    pars = list(set(c.strip() for c in C16_PARAMS["parameter"].split(",")))
    frames = []
    for path in order:
        subgroup = "annual" if "/annual/" in path else "seasonal"
        period = 0 if subgroup == "annual" else 13
        for var in variables:
            for i, par in enumerate(pars):
                frames.append(pl.DataFrame({
                    "lon": [135.5, 135.5], "lat": [15.5, 15.5],
                    "depth": [0.0, 10.0], "time_periods": [period, period],
                    "parameters": [par, par],
                    "variable_type": [var, var],
                    "value": [1.0 + i, 2.0 + i],
                }))
    return pl.concat(frames, how="vertical")


def pipeline(df):
    """The candidate's concat -> parameter_variable -> pivot, unmodified."""
    import polars as pl
    df = df.with_columns(
        (pl.col("parameters") + "_" + pl.col("variable_type")).alias("parameter_variable"))
    return df.pivot(index=["lon", "lat", "depth", "time_periods"],
                    on="parameter_variable", values="value")


def as_json(df) -> bytes:
    return json.dumps(df.to_dicts()).encode()


def as_csv(df) -> bytes:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=df.columns)
    w.writeheader()
    for row in df.to_dicts():
        w.writerow(row)
    return buf.getvalue().encode()


def first_difference(a: bytes, b: bytes) -> str:
    if a == b:
        return "identical"
    n = min(len(a), len(b))
    i = next((k for k in range(n) if a[k] != b[k]), n)
    lo = max(0, i - 30)
    return (f"byte {i} of {len(a)}/{len(b)}\n"
            f"      reference …{a[lo:i + 30].decode(errors='replace')}…\n"
            f"      candidate …{b[lo:i + 30].decode(errors='replace')}…")


def main() -> int:
    problems = preconditions()
    if problems:
        print("cannot run:")
        for p in problems:
            print(f"  - {p}")
        return 2

    print("C16 reproducer — offline, synthetic rows, no host contact\n")
    print(f"  params: {C16_PARAMS}\n")

    print("1. group iteration order (exact — computed from the real path strings)")
    orders = {}
    for arm, root in ARMS.items():
        orders[arm] = group_order(root)
        print(f"   {arm:10s} root={root!r}")
        for p in orders[arm]:
            print(f"              {p}")
    差 = orders["reference"] != orders["candidate"]
    print(f"\n   orders differ: {差}")
    if not 差:
        print("   -> the divergence does not reproduce; the rest is not meaningful")
        return 1
    print("   -> `result_list` is concatenated in a different order on each arm\n")

    print("2. what that does downstream (synthetic rows, real polars pipeline)")
    frames = {arm: pipeline(synthetic_rows(o)) for arm, o in orders.items()}
    for arm, df in frames.items():
        print(f"   {arm:10s} columns: {df.columns}")
    same_set = set(frames["reference"].columns) == set(frames["candidate"].columns)
    same_order = frames["reference"].columns == frames["candidate"].columns
    print(f"\n   same column set:   {same_set}")
    print(f"   same column order: {same_order}\n")

    print("3. serialised difference")
    for label, fn in (("JSON", as_json), ("CSV", as_csv)):
        a, b = fn(frames["reference"]), fn(frames["candidate"])
        print(f"   {label}: reference {len(a)} bytes, candidate {len(b)} bytes, "
              f"equal length: {len(a) == len(b)}")
        print(f"      first difference: {first_difference(a, b)}")

    print("\n4. first differing key, per row")
    ra, rb = frames["reference"].to_dicts(), frames["candidate"].to_dicts()
    for i, (x, y) in enumerate(zip(ra, rb)):
        if list(x) != list(y):
            print(f"   row {i}: key order {list(x)}")
            print(f"          vs        {list(y)}")
            print(f"   values equal: {x == y}")
            break
    else:
        print("   no row differs in key order")

    print("\n   NOTE: steps 2-4 are a mechanism demonstration on synthetic rows.")
    print("   They are not C16's actual bytes; confirming those needs the raw")
    print("   responses from a diagnostic run against the real store.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
