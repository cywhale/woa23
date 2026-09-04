"""The empty-result contract: JSON and CSV must agree that "no rows" is a RESULT.

WHY THIS EXISTS. D-3 asked the same question of both routes -- winter nitrate
(`time_period=13`) at 3000-4000 m, a well-formed query whose depth range lies outside
seasonal nitrate's 0-800 m extent -- and they disagreed:

    /api/woa23      200  []                                     a result
    /api/woa23/csv  400  {"detail":"No data available ..."}     an error

The CSV body was `application/json`, so a CSV client received neither CSV nor a status it
could treat as an empty response. The JSON route has always expressed "no rows" as 200 with
an empty result; the CSV route now does the same, with a header-only body.

WHAT IS **NOT** CHANGED, and is asserted here rather than asserted in prose: genuine errors
keep their behaviour. `process_woa23_data` raises `HTTPException` itself for unusable
parameters and for a query matching no data arrays at all (404); `ValueError` becomes 400
and anything else becomes 500. None of those reach the removed branch, so none of them is
converted into a 200.

OFFLINE ONLY. `process_woa23_data` is replaced per case, so no store -- real or synthetic --
is opened, nothing is read from production, and no network call is made. That is what lets
this run beside the unit suites instead of needing a served instance.
"""

from __future__ import annotations

import csv
import io
import os
import sys
import tempfile
from pathlib import Path

# THE STORE IS A LOCAL EMPTY DIRECTORY, and it is never opened.
#
# `api.config` requires WOA23_ZARR_STORE at import and checks that it resolves to an
# existing directory. That check is about configuration, not about data, so a temporary
# directory satisfies it -- and because every case below replaces `process_woa23_data`,
# nothing ever reads from it. No real store, synthetic or production, is involved.
_STORE = tempfile.mkdtemp(prefix="csvempty-store-")
os.environ.setdefault("WOA23_ZARR_STORE", _STORE)

import polars as pl
from fastapi import HTTPException
from starlette.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from api import app as app_module                                    # noqa: E402
from bench.suite_summary import summary                              # noqa: E402

PASS = 0
FAIL = 0


def ok(label: str) -> None:
    global PASS
    PASS += 1
    print(f"  ok   {label}")


def bad(label: str, detail: str = "") -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {label}")
    if detail:
        print(f"       {detail}")


def is_(label: str, got, want) -> None:
    ok(label) if got == want else bad(label, f"expected {want!r}, got {got!r}")


#: The schema the pivot produces: the index columns plus one column per parameter.
COLUMNS = ["lon", "lat", "depth", "time_period", "nitrate"]


def frame(rows: list[dict]) -> pl.DataFrame:
    """A frame with the real schema, empty or not. An empty frame KEEPS its columns,
    which is what makes a header-only CSV possible at all."""
    if rows:
        return pl.DataFrame(rows)
    return pl.DataFrame({c: [] for c in COLUMNS},
                        schema={c: (pl.Utf8 if c == "time_period" else pl.Float64)
                                for c in COLUMNS})


def patch(fn):
    app_module.process_woa23_data = fn


ORIGINAL = app_module.process_woa23_data
client = TestClient(app_module.app)
Q = "/api/woa23?lon0=135&lat0=15&grid=1&parameter=nitrate&time_period=13&dep0=3000&dep1=4000"
QC = Q.replace("/api/woa23?", "/api/woa23/csv?")

print("=== the empty-result contract ===")
print()

# ============================================================ 1. valid, zero rows ======
print("1. a valid query matching zero rows")


async def empty(*a, **k):
    return frame([])


patch(empty)

r = client.get(Q)
is_("1a  JSON: status 200", r.status_code, 200)
is_("1b  JSON: empty result", r.json(), [])
is_("1c  JSON: content-type", r.headers["content-type"].split(";")[0], "application/json")

r = client.get(QC)
is_("1d  CSV: status 200 — the same answer as JSON", r.status_code, 200)
is_("1e  CSV: content-type is CSV, not JSON",
    r.headers["content-type"].split(";")[0], "text/csv")
body = r.text
rows = list(csv.reader(io.StringIO(body)))
is_("1f  CSV: exactly one line — the header", len(rows), 1)
is_("1g  CSV: the header is the real schema", rows[0], COLUMNS)
# 5: the body must not be a JSON error object.
if body.lstrip().startswith("{") or "detail" in body:
    bad("1h  CSV: the body is NOT a JSON error body", f"got {body[:80]!r}")
else:
    ok("1h  CSV: the body is NOT a JSON error body")
is_("1i  CSV: and not zero-byte", len(body) > 0, True)
# The two routes now agree, which is the whole point.
is_("1j  JSON and CSV agree on status for the same condition",
    client.get(Q).status_code, client.get(QC).status_code)
print()

# ============================================================ 2. normal, non-empty =====
print("2. a normal non-empty response is unchanged")

ROWS = [
    {"lon": 135.5, "lat": 15.5, "depth": 0.0, "time_period": "13", "nitrate": 1.25},
    {"lon": 135.5, "lat": 15.5, "depth": 5.0, "time_period": "13", "nitrate": 1.5},
]


async def full(*a, **k):
    return frame(ROWS)


patch(full)

r = client.get(Q)
is_("2a  JSON: status 200", r.status_code, 200)
is_("2b  JSON: two rows", len(r.json()), 2)
is_("2c  JSON: values intact", r.json()[0]["nitrate"], 1.25)

r = client.get(QC)
is_("2d  CSV: status 200", r.status_code, 200)
is_("2e  CSV: content-type", r.headers["content-type"].split(";")[0], "text/csv")
rows = list(csv.reader(io.StringIO(r.text)))
is_("2f  CSV: header plus two rows", len(rows), 3)
is_("2g  CSV: same header as the empty case", rows[0], COLUMNS)
is_("2h  CSV: values intact", rows[1][4], "1.25")
print()

# ====================================================== 3. real errors stay errors =====
#
# The branch was REMOVED, not widened. Each of these raises before or instead of returning
# a frame, so none reaches the removed line -- and none may become a 200.
print("3. genuine errors are NOT converted to 200")


def raises(exc):
    async def _fn(*a, **k):
        raise exc
    return _fn


for label, exc, want in [
    ("404 no data arrays at all", HTTPException(status_code=404, detail="No data found for the specified query parameters"), 404),
    ("400 from the query layer", HTTPException(status_code=400, detail="bad parameter"), 400),
    ("400 from a ValueError", ValueError("unparseable time_period"), 400),
    ("500 from an unexpected error", RuntimeError("store unavailable"), 500),
    ("500 column-order guard", HTTPException(status_code=500, detail="internal column-order error"), 500),
]:
    patch(raises(exc))
    rj = client.get(Q)
    rc = client.get(QC)
    is_(f"3a  JSON {label}", rj.status_code, want)
    is_(f"3b  CSV  {label}", rc.status_code, want)
    if rc.status_code == 200:
        bad(f"3c  CSV {label} was NOT turned into 200")
    else:
        ok(f"3c  CSV {label} was NOT turned into 200")

# A store failure must not be reported as an empty result.
patch(raises(RuntimeError("zarr store unreadable")))
r = client.get(QC)
is_("3d  a store failure is 500, never a header-only 200", r.status_code, 500)
if "text/csv" in r.headers.get("content-type", ""):
    bad("3e  a store failure does not return CSV")
else:
    ok("3e  a store failure does not return CSV")
print()

# =============================================== 4. status consistency, both routes ====
print("4. JSON and CSV are status-consistent for every condition tested")
cases = {
    "empty": empty,
    "full": full,
    "404": raises(HTTPException(status_code=404, detail="none")),
    "400": raises(HTTPException(status_code=400, detail="bad")),
    "500": raises(RuntimeError("boom")),
}
for name, fn in cases.items():
    patch(fn)
    a, b = client.get(Q).status_code, client.get(QC).status_code
    is_(f"4a  {name}: JSON {a} == CSV {b}", a, b)

app_module.process_woa23_data = ORIGINAL
print()

# ================================= 5. THE REAL PATH, against a local synthetic store ====
#
# Sections 1-4 replace `process_woa23_data`, so they prove the ROUTE's contract but never
# touch the pivot -- and the pivot is where the header was actually lost. This section
# drives the real query path end to end: real zarr open, real filtering, real pivot, real
# rename, real ordering, real `write_csv`.
#
# The store is the subject's OWN deterministic synthetic fixture, built here into a
# temporary directory. It is NOT the production store and NOT VM24: it proves deployment
# machinery and response shape only, never WOA23 data correctness, which is c1f/c2g's.
print("5. the REAL serialization path, against a local synthetic store")

import subprocess                                                    # noqa: E402

# The builder refuses a path that already exists -- it will not write into a directory
# it did not create -- so a name is reserved and the directory is left for it to make.
REAL_STORE = os.path.join(tempfile.mkdtemp(prefix="csvempty-real-"), "store")
_build = subprocess.run(
    [sys.executable, str(Path(__file__).resolve().parents[1] / "deploy" / "make_staging_store.py"), REAL_STORE],
    capture_output=True, text=True)
if _build.returncode != 0:
    bad("5a  the synthetic store could not be built", _build.stderr[-300:])
else:
    ok("5a  the synthetic store built (deterministic, offline, not production)")
    # A SECOND app instance bound to the real store, so sections 1-4 keep their own.
    import importlib                                                 # noqa: E402
    os.environ["WOA23_ZARR_STORE"] = REAL_STORE
    for _m in ("api.config", "api.query", "api.app"):
        sys.modules.pop(_m, None)
    real_app = importlib.import_module("api.app")
    rc_ = TestClient(real_app.app)

    BASE = "?lon0=135&lat0=15&grid=1&parameter=temperature&time_period=0"
    FULL = BASE + "&dep0=0&dep1=100"        # inside the fixture's depths
    NONE = BASE + "&dep0=3000&dep1=4000"    # valid, and matches zero rows

    # -- the canonical header, taken from the NON-EMPTY response of the same query ------
    r = rc_.get("/api/woa23/csv" + FULL)
    is_("5b  non-empty CSV is 200", r.status_code, 200)
    canonical = list(csv.reader(io.StringIO(r.text)))[0]
    print(f"       canonical header (from data): {canonical}")
    is_("5c  non-empty CSV has data rows", len(r.text.strip().splitlines()) - 1 > 0, True)
    rj = rc_.get("/api/woa23" + FULL)
    is_("5d  non-empty JSON is 200 with rows", (rj.status_code, len(rj.json()) > 0), (200, True))

    # -- 1,2,3,4,5: the valid empty result ----------------------------------------------
    rj = rc_.get("/api/woa23" + NONE)
    is_("5e  (1) valid empty JSON is 200", rj.status_code, 200)
    is_("5f  JSON keeps the existing empty shape", rj.json(), [])

    r = rc_.get("/api/woa23/csv" + NONE)
    is_("5g  (2) the same valid empty CSV is 200", r.status_code, 200)
    is_("5h  (3) CSV content type", r.headers["content-type"].split(";")[0], "text/csv")
    body = r.text
    rows = list(csv.reader(io.StringIO(body)))
    is_("5i  (4) the empty header IS the canonical header", rows[0], canonical)
    is_("5j  ... and it carries the parameter column", "temperature" in rows[0], True)
    is_("5k  (5) zero data rows", len(rows) - 1, 0)
    if body.lstrip().startswith("{") or "detail" in body:
        bad("5l  the empty CSV body is not a JSON error body", repr(body[:80]))
    else:
        ok("5l  the empty CSV body is not a JSON error body")

    # -- 7: real errors on the real path ------------------------------------------------
    # A parameter with no data array anywhere: process_woa23_data raises 404 itself.
    r404j = rc_.get("/api/woa23"     + "?lon0=135&lat0=15&grid=1&parameter=nitrate&time_period=0")
    r404c = rc_.get("/api/woa23/csv" + "?lon0=135&lat0=15&grid=1&parameter=nitrate&time_period=0")
    is_("5m  (7) a parameter with no array is not 200 (JSON)", r404j.status_code != 200, True)
    is_("5n  (7) a parameter with no array is not 200 (CSV)", r404c.status_code != 200, True)
    is_("5o  ... and both agree", r404j.status_code, r404c.status_code)
    # A malformed request: lon0 is required and must be a float.
    rbadj = rc_.get("/api/woa23"     + "?lat0=15&grid=1&parameter=temperature")
    rbadc = rc_.get("/api/woa23/csv" + "?lat0=15&grid=1&parameter=temperature")
    is_("5p  (7) a malformed request is not 200 (JSON)", rbadj.status_code != 200, True)
    is_("5q  (7) a malformed request is not 200 (CSV)", rbadc.status_code != 200, True)
    is_("5r  ... and both agree", rbadj.status_code, rbadc.status_code)
    # A store failure: point a fresh app at a directory that is not a store.
    broken = tempfile.mkdtemp(prefix="csvempty-broken-")   # a real dir, but not a store
    os.environ["WOA23_ZARR_STORE"] = broken
    for _m in ("api.config", "api.query", "api.app"):
        sys.modules.pop(_m, None)
    broken_app = importlib.import_module("api.app")
    rb = TestClient(broken_app.app, raise_server_exceptions=False)
    rbc = rb.get("/api/woa23/csv" + FULL)
    is_("5s  (7) a store failure is not 200", rbc.status_code != 200, True)
    if "text/csv" in rbc.headers.get("content-type", ""):
        bad("5t  a store failure does not return CSV", f"got {rbc.status_code}")
    else:
        ok("5t  a store failure does not return CSV")

print()

raise SystemExit(summary(PASS, FAIL))
