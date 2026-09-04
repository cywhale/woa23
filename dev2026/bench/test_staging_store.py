#!/usr/bin/env python3
"""Does the synthetic staging store actually carry the candidate? Offline.

The PM2 staging request says the fixture is enough to start `api.app`, satisfy its
lifespan, answer readiness, serve JSON and CSV, and produce a checkable row order. This
proves each of those **before** anyone is asked to authorise a VM24 run — a request that
turns out to be unrunnable wastes an authorisation, and finding that out on the host is
the expensive way.

It builds the store with the **real builder** (`deploy/make_staging_store.py`), runs the
**real lifespan** and the **real handlers**, and reads the bytes they produce.

**What a pass here means and does not mean.** It means the deployment machinery works
against this fixture. It says **nothing** about real WOA23 data: `c1f` and `c2g` are
that evidence, against the real store, and no number from this file may be reported as
theirs.

In-process, no socket, no PM2, no VM24.

    uv run python -m bench.test_staging_store
"""

import asyncio
import csv
import importlib
import io
import json
import os
import sys
import tempfile
from pathlib import Path
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


def key(row):
    """Spec 008's contract key, numeric in every component."""
    return (int(row["time_period"]), float(row["depth"]),
            float(row["lat"]), float(row["lon"]))


def main():
    from deploy.make_staging_store import GROUPS, file_list_digest, main as build_main

    root = Path(tempfile.mkdtemp()) / "store"
    rc = build_main.__wrapped__(root) if hasattr(build_main, "__wrapped__") else None
    # The builder is a CLI; call it the way the request will.
    sys.argv = ["make_staging_store", str(root)]
    rc = build_main()
    check("the builder succeeds", 0, rc)

    print("\nthe fixture is what the request describes")
    digest, n_files, total = file_list_digest(root)
    check("it has the three groups the query path needs", 3, len(GROUPS))
    check("the anchor group exists and is readable", True,
          (root / "1_degree/annual/TS/.zgroup").is_file())
    check("it is small", True, total < 1_000_000)
    check("and non-empty", True, n_files > 0)
    print(f"       files={n_files} bytes={total:,} file_list_sha256={digest}")

    print("\nrebuilding it elsewhere gives the SAME digest — so the request's digest "
          "is checkable")
    root2 = Path(tempfile.mkdtemp()) / "store"
    sys.argv = ["make_staging_store", str(root2)]
    build_main()
    digest2, n2, total2 = file_list_digest(root2)
    check("same file count", n_files, n2)
    check("same byte total", total, total2)
    check("same file-list digest", digest, digest2)

    print("\nthe builder refuses to overwrite an existing path")
    sys.argv = ["make_staging_store", str(root)]
    check("a second build on the same path is refused", 2, build_main())

    print("\nthe candidate's LIFESPAN opens the anchor from this store")
    os.environ["WOA23_ZARR_STORE"] = str(root)
    # Imported after the variable is set: api.config validates the store at import.
    app_mod = importlib.import_module("api.app")
    opened = []

    async def run_lifespan():
        async with app_mod.lifespan(app_mod.app):
            opened.append(True)
    asyncio.run(run_lifespan())
    check("the lifespan completed without raising", [True], opened)

    print("\nthe endpoints serve JSON and CSV from it")
    q = dict(lon0=134.0, lat0=14.0, lon1=138.0, lat1=17.0, dep0=None, dep1=None,
             grid=None, append=None, parameter="temperature,salinity",
             time_period="0,1,2,13")
    resp = asyncio.run(app_mod.get_woa23(
        q["lon0"], q["lat0"], q["lon1"], q["lat1"], q["dep0"], q["dep1"],
        q["grid"], q["append"], q["parameter"], q["time_period"]))
    rows = json.loads(bytes(resp.body))
    check("JSON returns rows", True, len(rows) > 0)
    check("and spans all four requested time_periods", [0, 1, 2, 13],
          sorted({int(r["time_period"]) for r in rows}))
    check("with the bare parameter columns the `mn` default produces", True,
          "temperature" in rows[0] and "salinity" in rows[0])

    cresp = asyncio.run(app_mod.get_woa23_csv(
        q["lon0"], q["lat0"], q["lon1"], q["lat1"], q["dep0"], q["dep1"],
        q["grid"], q["append"], q["parameter"], q["time_period"]))
    body = Path(cresp.path).read_bytes()
    try:
        os.unlink(cresp.path)      # the known NamedTemporaryFile leak; not ours to add to
    except OSError:
        pass
    crows = list(csv.DictReader(io.StringIO(body.decode())))
    check("CSV returns the same number of rows", len(rows), len(crows))

    print("\nthe row order is checkable, and it spans several groups")
    keys = [key(r) for r in rows]
    check("JSON rows ascend by (time_period, depth, lat, lon)", True,
          keys == sorted(keys))
    check("CSV rows carry the identical key sequence", keys, [key(r) for r in crows])
    check("strictly ascending — no duplicate contract key", True,
          all(a < b for a, b in zip(keys, keys[1:])))
    check("13 does not precede 2, as a string sort would have it", True,
          [k[0] for k in keys].index(13) > [k[0] for k in keys].index(2))
    # The fixture must not make the check trivial: the natural, unsorted order of this
    # store is lon-major, so a candidate that sorted nothing would fail the assertions
    # above rather than pass them by accident.
    natural = sorted(rows, key=lambda r: (float(r["lon"]), float(r["lat"]),
                                          float(r["depth"]), int(r["time_period"])))
    check("the contract order is NOT the store's natural order", False,
          [key(r) for r in natural] == keys)

    print("\nthe OpenAPI document is the 1.1.0 one")
    schema = app_mod.generate_custom_openapi()
    check("info.version is 1.1.0", "1.1.0", schema["info"]["version"])
    check("the description carries the row-order statement", True,
          "time_period numeric ascending" in schema["info"]["description"])
    for path in ("/api/woa23", "/api/woa23/csv"):
        desc = schema["paths"][path]["get"].get("description", "")
        check(f"{path} documents the row order", True,
              "time_period numeric ascending" in desc)
    check("the field/header exclusion is stated in the description", True,
          "JSON field order and CSV header order are unchanged"
          in schema["info"]["description"])

    print()
    # THE CAVEAT COMES FIRST, and that ordering is the whole point of this change. It used
    # to be printed AFTER the summary, so the caveat was this suite's last stdout line --
    # the runner displayed it, and these 24 assertions were invisible to every batch total.
    # Nothing may follow the contract line.
    print("Deployment machinery only. NOT evidence about real WOA23 data.")
    return summary(PASS, FAIL)


if __name__ == "__main__":
    sys.exit(main())
