#!/usr/bin/env python3
"""Spec 008's row-order contract, through the real query path. Offline.

**The contract** (spec 008 §1, decided by the PI 2026-08-19): response rows ascend by
`(time_period, depth, lat, lon)`, `time_period` **numerically**, binding JSON and CSV
alike.

**What this drives.** `api.app.get_woa23` and `api.app.get_woa23_csv` — the real
handlers — against a synthetic WOA23-shaped Zarr store in a temp directory, and it
reads the bytes each one actually produces: the `ORJSONResponse` body, and the file
the `FileResponse` points at. Testing `process_woa23_data`'s frame alone would leave
the endpoint wiring unverified, and this campaign has twice had a run die on wiring
every unit test walked past.

**In-process, no socket.** Spec 008 §6 requires this stay offline, and it does so
literally: no HTTP, no listener, no VM24, no production store. The handlers are
`async def`, so they are awaited directly.

**The one thing a careless version of this file would get wrong.** Sorting
`time_period` as text yields `0, 1, 10, 13, 2`. Every key comparison here is
**numeric**, and test 7 exists solely to catch a string sort.

    uv run python -m bench.test_row_order
"""

import asyncio
import csv
import io
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import xarray as xr
from bench.suite_summary import summary, summary_line   # noqa: E402

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

PASS = FAIL = 0


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


# ---------------------------------------------------------------- the fixture ---
def build_group(path, *, params, periods, depths=(0.0, 10.0, 100.0)):
    """A group shaped like WOA23's.

    Coordinates ascend, as the real store's do — `xarray`'s `.sel(slice(lo, hi))`
    selects nothing from a descending index, so a "cleverly" reversed fixture would
    return zero rows and every assertion below would pass over an empty list.

    That does **not** make the fixture pre-sorted into the contract order. The frame
    that reaches the serialiser is `lon`-major, because `to_dataframe()` unrolls the
    dims in declaration order and `pl.concat` then stacks the groups in whatever order
    the `zarr_group_paths` set yielded. The contract is `time_period`-major. Test 2a
    asserts the difference rather than assuming it, so a fixture that ever did arrive
    pre-sorted would be caught instead of quietly making this suite vacuous.
    """
    lon = [134.5, 135.5, 136.5]
    lat = [14.5, 15.5, 16.5]
    dep = sorted(depths)
    shape = (len(lon), len(lat), len(dep), len(params), len(periods))
    data = np.arange(np.prod(shape), dtype="float32").reshape(shape)
    path.parent.mkdir(parents=True, exist_ok=True)
    xr.Dataset(
        {"an": (("lon", "lat", "depth", "parameters", "time_periods"), data),
         "mn": (("lon", "lat", "depth", "parameters", "time_periods"), data + 0.5)},
        coords={"lon": lon, "lat": lat, "depth": list(dep),
                "parameters": list(params), "time_periods": list(periods)},
    ).to_zarr(str(path), mode="w", consolidated=True)


def make_store(root):
    """Enough groups that a multi-group query really spans several.

    The subgroup layout is `api.query.determine_subgroup`'s: annual for period 0,
    monthly for 1-12, seasonal otherwise; TS for temperature/salinity, Nutrients for
    the rest. Periods 0, 1, 2 and 13 are all reachable, which is what test 7 needs.
    """
    store = Path(root)
    g = store / "1_degree"
    build_group(g / "annual" / "TS", params=["temperature", "salinity"], periods=["0"])
    build_group(g / "monthly" / "TS", params=["temperature", "salinity"],
                periods=["1", "2"])
    build_group(g / "seasonal" / "TS", params=["temperature", "salinity"],
                periods=["13"])
    build_group(g / "annual" / "Nutrients", params=["nitrate", "phosphate"],
                periods=["0"])
    return str(store)


# ------------------------------------------------------------ driving the app ---
def _handlers(store):
    """Import the real app with the store configured. Once per process.

    `api.config` reads WOA23_ZARR_STORE and validates it at import, so the variable
    has to be set before the first import and cannot usefully change afterwards.
    """
    os.environ["WOA23_ZARR_STORE"] = store
    from api.app import get_woa23, get_woa23_csv
    return get_woa23, get_woa23_csv


def json_rows(handler, **q):
    """The bytes the JSON endpoint actually returns, parsed."""
    resp = asyncio.run(handler(
        q.get("lon0", 134.0), q.get("lat0", 14.0), q.get("lon1", 137.0),
        q.get("lat1", 17.0), q.get("dep0"), q.get("dep1"), q.get("grid"),
        q.get("append"), q.get("parameter"), q.get("time_period")))
    return json.loads(bytes(resp.body))


def csv_rows(handler, **q):
    """The bytes the CSV endpoint actually writes, parsed.

    The handler returns a `FileResponse` over a `NamedTemporaryFile(delete=False)` —
    the known leak `api/app.py` preserves verbatim. The file is read and then removed
    here, so this suite does not add to it.
    """
    resp = asyncio.run(handler(
        q.get("lon0", 134.0), q.get("lat0", 14.0), q.get("lon1", 137.0),
        q.get("lat1", 17.0), q.get("dep0"), q.get("dep1"), q.get("grid"),
        q.get("append"), q.get("parameter"), q.get("time_period")))
    try:
        body = Path(resp.path).read_bytes()
    finally:
        try:
            os.unlink(resp.path)
        except OSError:
            pass
    return list(csv.DictReader(io.StringIO(body.decode())))


# ------------------------------------------------------------------- the key ---
def key(row):
    """The contract key, NUMERIC in every component.

    `time_period` arrives as a string from both serialisers; `int()` is the whole
    point of this function. `lon`, `lat` and `depth` are floats in JSON and strings
    in CSV, so they are floated rather than compared as text — '-10' < '-9' is true
    of strings and false of the coordinates they spell.
    """
    return (int(row["time_period"]), float(row["depth"]),
            float(row["lat"]), float(row["lon"]))


def ascending(rows):
    keys = [key(r) for r in rows]
    return keys == sorted(keys)


def strictly_ascending(rows):
    keys = [key(r) for r in rows]
    return all(a < b for a, b in zip(keys, keys[1:]))


# ===================================================================== tests ===
def main():
    tmp = tempfile.mkdtemp(prefix="woa23-roworder-")
    store = make_store(tmp)
    jhandler, chandler = _handlers(store)

    multi = dict(parameter="temperature,salinity,nitrate", time_period="0")
    spread = dict(parameter="temperature", time_period="0,1,2,13")

    print("1-2. both serialisers emit the contract order")
    jr = json_rows(jhandler, **spread)
    cr = csv_rows(chandler, **spread)
    check("the JSON rows ascend by (time_period, depth, lat, lon)", True, ascending(jr))
    check("and there are rows to have an order at all", True, len(jr) > 10)
    check("the CSV rows ascend by the same key", True, ascending(cr))
    check("and the CSV carried the same number of rows", len(jr), len(cr))

    print("2a. the fixture discriminates — the contract order is not the natural one")
    # If the pipeline's own order already satisfied the contract, everything below
    # would pass with the sort deleted. The natural frame is lon-major; the contract
    # is time_period-major, and these must therefore differ.
    natural = sorted(jr, key=lambda r: (float(r["lon"]), float(r["lat"]),
                                        float(r["depth"]), int(r["time_period"])))
    check("lon-major order is NOT the contract order", False,
          [key(r) for r in natural] == [key(r) for r in jr])
    check("so the response order is the sort's doing, not the pipeline's", True,
          ascending(jr) and not ascending(natural))

    print("3. JSON and CSV agree with each other")
    check("the two row-key sequences are identical", [key(r) for r in jr],
          [key(r) for r in cr])

    print("4. the parameter ORDER in the query does not move a row")
    a = [key(r) for r in json_rows(jhandler, parameter="temperature,salinity,nitrate",
                                   time_period="0")]
    b = [key(r) for r in json_rows(jhandler, parameter="nitrate,temperature,salinity",
                                   time_period="0")]
    c = [key(r) for r in json_rows(jhandler, parameter="salinity,nitrate,temperature",
                                   time_period="0")]
    check("three parameter orders, one row order", True, a == b == c)
    check("and the query really returned rows", True, len(a) > 0)

    print("5. the time_period INPUT order does not move a row")
    d = [key(r) for r in json_rows(jhandler, parameter="temperature",
                                   time_period="0,1,2,13")]
    e = [key(r) for r in json_rows(jhandler, parameter="temperature",
                                   time_period="13,2,1,0")]
    f = [key(r) for r in json_rows(jhandler, parameter="temperature",
                                   time_period="2,13,0,1")]
    check("three input orders, one row order", True, d == e == f)

    print("6. hash seed and group-set iteration do not move a row")
    # PYTHONHASHSEED is read at interpreter start, so this needs subprocesses. The
    # zarr_group_paths SET and the two list(set(...)) calls are exactly what varies
    # with it — the mechanism spec 003 identified.
    seeds, digests = ["0", "1", "12345"], []
    for seed in seeds:
        out = subprocess.run(
            [sys.executable, "-c", ORDER_PROBE, store],
            env=dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=str(REPO),
                     WOA23_ZARR_STORE=store),
            capture_output=True, text=True)
        if out.returncode != 0:
            check(f"the probe ran under PYTHONHASHSEED={seed}", 0, out.returncode)
            print("       " + out.stderr.strip()[-500:])
            digests.append(f"FAILED-{seed}")
            continue
        # The app prints "Handling parameters ..." and an ELAPSED TIME to stdout, so
        # the whole stream is not the answer — an earlier version of this test hashed
        # the timing line and reported three "different orders" that were three
        # different durations. Only the marked line counts.
        marked = [l.split(" ", 1)[1] for l in out.stdout.splitlines()
                  if l.startswith("ROWORDER ")]
        digests.append(marked[0] if len(marked) == 1 else f"NO-MARKER-{seed}")
    check("three seeds, one multi-group row order", 1, len(set(digests)))
    check("and the probe produced a real ordering, not an empty one", True,
          bool(digests[0]) and not digests[0].startswith(("FAILED", "NO-MARKER"))
          and int(digests[0].split()[1]) > 10)

    print("7. numeric, not lexicographic — the 0/1/2/13 trap")
    tp = [int(r["time_period"]) for r in json_rows(jhandler, parameter="temperature",
                                                   time_period="0,1,2,13")]
    check("time_period never decreases", True, tp == sorted(tp))
    check("the distinct periods come out 0, 1, 2, 13", [0, 1, 2, 13],
          sorted(set(tp)))
    # The explicit refutation: a string sort would put '13' before '2'.
    check("13 does not precede 2, as a string sort would have it", True,
          tp.index(13) > tp.index(2))
    check("and CSV agrees", [0, 1, 2, 13],
          sorted({int(r["time_period"]) for r in csv_rows(chandler,
                                                          parameter="temperature",
                                                          time_period="0,1,2,13")}))

    print("8. sorting moved rows and nothing else")
    from api.query import process_woa23_data
    frame = asyncio.run(process_woa23_data(134.0, 14.0, 137.0, 17.0, None, None,
                                           None, None, "temperature,salinity", "0"))
    unsorted_rows = json_rows(jhandler, parameter="temperature,salinity",
                              time_period="0")
    check("the column SEQUENCE is unchanged by the sort", frame.columns,
          list(unsorted_rows[0]))
    check("the row COUNT is unchanged", frame.height, len(unsorted_rows))
    # Values: the multiset of full rows must be identical to the frame's, so no value
    # was altered, dropped or duplicated by reordering.
    def canon(rows):
        return sorted(json.dumps({k: r[k] for k in sorted(r)}, sort_keys=True,
                                 default=str) for r in rows)
    check("the row multiset is unchanged", canon(frame.to_dicts()),
          canon(unsorted_rows))

    print("9. no duplicate contract key, and duplicates still raise at the pivot")
    keys = [key(r) for r in json_rows(jhandler, **multi)]
    check("every (time_period, depth, lat, lon) appears once", len(keys),
          len(set(keys)))
    check("so the order is STRICTLY ascending, not merely non-decreasing", True,
          strictly_ascending(json_rows(jhandler, **spread)))
    # Spec 008 §4.4: this is polars 1.27.1's OBSERVED behaviour on the current pivot
    # path, pinned so an upgrade that weakens it fails here rather than silently
    # changing what a response means. It is not claimed to hold for future versions.
    import polars as pl
    dup = pl.DataFrame({"lon": [1.0, 1.0], "lat": [2.0, 2.0], "depth": [0.0, 0.0],
                        "time_periods": ["0", "0"],
                        "parameter_variable": ["t_mn", "t_mn"], "value": [10.0, 99.0]})
    try:
        dup.pivot(index=["lon", "lat", "depth", "time_periods"],
                  on="parameter_variable", values="value")
        raised = False
    except Exception:                                          # noqa: BLE001
        raised = True
    check("a duplicated pivot group raises rather than picking a winner", True, raised)

    print("10. negative control — the checker must be able to fail")
    good = json_rows(jhandler, **spread)
    check("the unsorted fixture order is NOT accepted", False,
          ascending(list(reversed(good))))
    # A string sort over time_period: correct-looking, and wrong.
    string_sorted = sorted(good, key=lambda r: (str(r["time_period"]),
                                                float(r["depth"]),
                                                float(r["lat"]), float(r["lon"])))
    check("a STRING-sorted order is rejected", False, ascending(string_sorted))
    check("and it really was a different order, not a no-op", False,
          [key(r) for r in string_sorted] == [key(r) for r in good])
    # One row out of place is enough.
    nudged = list(good)
    nudged[0], nudged[-1] = nudged[-1], nudged[0]
    check("one swapped pair is rejected", False, ascending(nudged))

    print()
    return summary(PASS, FAIL)
#: Runs in its own interpreter so PYTHONHASHSEED takes effect. Prints a digest of the
#: row-key sequence for a query spanning several Zarr groups — the case whose order
#: used to follow the group set's iteration.
ORDER_PROBE = """
import asyncio, hashlib, sys
from api.app import get_woa23
import json
resp = asyncio.run(get_woa23(134.0, 14.0, 137.0, 17.0, None, None, None, None,
                             "temperature,salinity,nitrate", "0"))
rows = json.loads(bytes(resp.body))
seq = "|".join(f'{r["time_period"]},{r["depth"]},{r["lat"]},{r["lon"]}' for r in rows)
if not rows:
    raise SystemExit("the probe query returned no rows; it would prove nothing")
print("ROWORDER", hashlib.sha256(seq.encode()).hexdigest(), len(rows))
"""


if __name__ == "__main__":
    sys.exit(main())
