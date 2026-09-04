#!/usr/bin/env python3
"""What does spec 008's row sort cost, in isolation? Offline, no store, no host.

**This is an informative measurement, not a gate and not a performance result.** Spec
009 §3 says what it is for: an optimisation input and a first-order sanity check, so
that "the sort's cost is unmeasured" stops being the only thing anyone can say about
it. It is **not** a production SLA, **not** an end-to-end latency, and **not** a
substitute for the VM24 measurement spec 009 requests.

**What it measures.** The exact expression `api/query.py` runs —

    df.sort([pl.col("time_period").cast(pl.Int32), "depth", "lat", "lon"],
            nulls_last=True)

— against frames shaped like the ones the read path produces, at row counts taken from
real contract cases. The frame is already built, so this is the sort **and nothing
else**: no Zarr read, no decompression, no pivot, no serialisation.

**What it therefore cannot tell you.**

- **It is not the request's cost.** A sort that takes 8 ms inside a request that takes
  200 ms is a 4% effect; the same 8 ms inside a 12 ms request is not. This file
  measures the numerator only. The denominator needs the real store, which needs VM24.
- **It is not this machine's answer for VM24.** Different CPU, different memory
  bandwidth, different polars build. The ordering of magnitudes should carry; the
  numbers should not be quoted as VM24's.
- **It is not a comparison of two candidates.** The old tree does no sort at all, so
  its sort cost is zero by construction and a ratio would be meaningless. The
  comparison that matters is end-to-end, and that is spec 009's.

    uv run python -m bench.sort_cost
"""

import statistics
import sys
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

#: Row counts from real cases. 214,812 is the largest candidate response C1 (`c1f`)
#: recorded; 204 is C16, the multi-group case spec 003 is about; the small ones are
#: point and small-bbox shapes.
SHAPES = [
    ("point profile", 102, 1),
    ("C16 multi-group", 204, 3),
    ("small bbox", 2_914, 1),
    ("regional bbox", 32_400, 2),
    ("large result", 214_812, 3),
]

#: Repeats per shape. Enough to see the spread; this is not a gated measurement and no
#: bootstrap interval is computed, because none would be quotable anyway.
REPEATS = 15


def build(n_rows: int, n_value_cols: int, *, reversed_input: bool) -> pl.DataFrame:
    """A frame shaped like `process_woa23_data`'s output, just before the sort.

    `time_period` is a **string** column, as it is in the real frame — that is what
    makes the cast part of the cost rather than a free annotation.

    `reversed_input` decides the input order, and TWO orders are measured because one
    would invite a claim neither supports.

    **Neither is established as the worst case, and the first draft of this file said
    the reversed one was.** It is not: a fully descending frame measured FASTER than
    the modulo-cyclic one below, which is what a comparison sort on already-monotonic
    input tends to do. So the two are reported as "two input orders", the spread
    between them is small, and no adversarial input has been constructed or claimed.
    """
    periods = ["0", "1", "2", "13"]
    lat = [-90 + (i % 180) * 0.5 for i in range(n_rows)]
    lon = [-180 + (i % 360) * 0.5 for i in range(n_rows)]
    depth = [float((i * 7) % 5500) for i in range(n_rows)]
    tp = [periods[i % len(periods)] for i in range(n_rows)]
    data = {"lon": lon, "lat": lat, "depth": depth, "time_period": tp}
    for c in range(n_value_cols):
        data[f"value_{c}"] = [float(i) + c for i in range(n_rows)]
    df = pl.DataFrame(data)
    if reversed_input:
        df = df.sort([pl.col("time_period").cast(pl.Int32), "depth", "lat", "lon"],
                     descending=True)
    return df


def time_sort(df: pl.DataFrame, repeats: int) -> list:
    out = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        df.sort([pl.col("time_period").cast(pl.Int32), "depth", "lat", "lon"],
                nulls_last=True)
        out.append((time.perf_counter() - t0) * 1000.0)
    return out


def main() -> int:
    print(f"polars {pl.__version__} | python {sys.version.split()[0]} | "
          f"{REPEATS} repeats per shape")
    print("\nOFFLINE, SORT ONLY. Not an end-to-end latency, not a production figure,")
    print("not an SLA, and not this machine speaking for VM24.\n")
    print(f"{'shape':<20}{'rows':>9}{'cols':>6}  {'median ms':>10}{'min':>9}"
          f"{'max':>9}   input order")
    rows_out = []
    for name, n, vcols in SHAPES:
        for reversed_input in (False, True):
            df = build(n, vcols, reversed_input=reversed_input)
            # One untimed pass: the first sort on a fresh frame pays allocation costs
            # the steady state does not, and this measures the steady state.
            df.sort([pl.col("time_period").cast(pl.Int32), "depth", "lat", "lon"],
                    nulls_last=True)
            ms = time_sort(df, REPEATS)
            med = statistics.median(ms)
            label = "descending" if reversed_input else "cyclic"
            print(f"{name:<20}{n:>9,}{df.width:>6}  {med:>10.3f}{min(ms):>9.3f}"
                  f"{max(ms):>9.3f}   {label}")
            rows_out.append((name, n, df.width, med, label))

    print("\nPer-row cost at the largest shape, for scale:")
    big = [r for r in rows_out if r[1] == 214_812]
    for name, n, w, med, label in big:
        print(f"  {label:<16} {med:.3f} ms for {n:,} rows "
              f"= {med * 1000 / n:.3f} µs/row")

    print("\nHow to read this, and how not to:")
    print("  - the sort is one operation on an in-memory frame the pipeline has")
    print("    ALREADY built; the store read, decompression and pivot that precede it")
    print("    are not measured here and are known to dominate;")
    print("  - a fraction-of-request figure needs the denominator, which needs the")
    print("    real store on VM24 — spec 009 requests exactly that and this file")
    print("    does not substitute for it;")
    print("  - s2pB measured a tree WITHOUT this sort and may not be used to infer")
    print("    the new candidate's cost;")
    print("  - neither input order below is a demonstrated worst case. They are two")
    print("    orders, and the descending one measured FASTER, which is why no")
    print("    'worst case' is claimed here.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
