"""G1 — the offline empty-CSV coverage gate for subject 143bf8c.

NOT part of the subject. This helper lives outside `dev2026/` and is streamed to the
interpreter, so the proposed deployment artifact is unchanged: running G1 adds no file to
the archive and needs no successor subject.

OFFLINE. A synthetic store is built by the subject's own deterministic builder into a
temporary directory. No VM24, no production store, no request to 8050, no network, no
performance measurement, no C1/C2.

WHAT IT PROVES. For each of the four empty-result shapes, the CSV header of a valid query
matching ZERO rows is byte-identical to the header the SAME query returns WITH data. The
canonical header is taken from the non-empty response at run time, never written down here
-- a header typed into a gate is a second place for it to be wrong.
"""
import csv
import io
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

DEV = Path(sys.argv[1]).resolve()          # the subject worktree's dev2026
OUT = Path(sys.argv[2]).resolve()          # where the evidence json is written

PASS = 0
FAIL = 0
RECORDS = []


def ok(label):
    global PASS
    PASS += 1
    print(f"  ok   {label}")


def bad(label, detail=""):
    global FAIL
    FAIL += 1
    print(f"  FAIL {label}")
    if detail:
        print(f"       {detail}")


def is_(label, got, want):
    ok(label) if got == want else bad(label, f"expected {want!r}, got {got!r}")


# ---------------------------------------------------------------- the offline fixture
STORE = os.path.join(tempfile.mkdtemp(prefix="g1-"), "store")
build = subprocess.run([sys.executable, str(DEV / "deploy" / "make_staging_store.py"), STORE],
                       capture_output=True, text=True)
print("=== G1 — offline empty-CSV coverage gate ===")
print(f"  subject dev2026 : {DEV}")
print(f"  synthetic store : {STORE}  (built by the subject's own builder)")
if build.returncode != 0:
    bad("G0  the synthetic store built", build.stderr[-300:])
    raise SystemExit(2)
ok("G0  the synthetic store built — offline, deterministic, NOT production")

os.environ["WOA23_ZARR_STORE"] = STORE
sys.path.insert(0, str(DEV))
from starlette.testclient import TestClient                       # noqa: E402
from api import app as app_module                                 # noqa: E402

c = TestClient(app_module.app)

BASE = "?lon0=135&lat0=15&grid=1"
INSIDE = "&dep0=0&dep1=100"        # inside the fixture's depths -> rows
OUTSIDE = "&dep0=3000&dep1=4000"   # valid, and matches zero rows

SHAPES = [
    ("S1  one parameter  / one statistic",   "&parameter=temperature&time_period=0"),
    ("S2  two parameters / one statistic",   "&parameter=temperature,salinity&time_period=0"),
    ("S3  one parameter  / two statistics",  "&parameter=temperature&append=mn,an&time_period=0"),
    ("S4  two parameters / two statistics",  "&parameter=temperature,salinity&append=mn,an&time_period=0"),
]

print()
for name, q in SHAPES:
    print(name)
    # --- the canonical header, from the NON-EMPTY response of this same query ---------
    rf = c.get("/api/woa23/csv" + BASE + q + INSIDE)
    if rf.status_code != 200 or not rf.text:
        bad(f"{name}: non-empty CSV is 200", f"got {rf.status_code}")
        continue
    canonical = list(csv.reader(io.StringIO(rf.text)))[0]
    full_rows = len(rf.text.strip().splitlines()) - 1
    is_(f"{name} :: non-empty CSV 200 with rows", (rf.status_code, full_rows > 0), (200, True))
    print(f"       canonical header: {canonical}")

    # --- 1. JSON 200 -----------------------------------------------------------------
    rj = c.get("/api/woa23" + BASE + q + OUTSIDE)
    is_(f"{name} :: (1) empty JSON status 200", rj.status_code, 200)
    is_(f"{name} ::     JSON keeps the empty shape", rj.json(), [])

    # --- 2,3,4,5,6 the empty CSV -----------------------------------------------------
    rc = c.get("/api/woa23/csv" + BASE + q + OUTSIDE)
    is_(f"{name} :: (2) empty CSV status 200", rc.status_code, 200)
    is_(f"{name} :: (3) CSV content type",
        rc.headers.get("content-type", "").split(";")[0], "text/csv")
    rows = list(csv.reader(io.StringIO(rc.text))) if rc.text else []
    empty_header = rows[0] if rows else []
    is_(f"{name} :: (4) header BYTE-IDENTICAL to the canonical header", empty_header, canonical)
    is_(f"{name} :: (5) zero data rows", max(0, len(rows) - 1), 0)
    # 6: every value column the non-empty header carried is still present.
    value_cols = [c_ for c_ in canonical if c_ not in ("lon", "lat", "depth", "time_period")]
    missing = [c_ for c_ in value_cols if c_ not in empty_header]
    is_(f"{name} :: (6) no parameter/statistic column missing ({len(value_cols)} value cols)",
        missing, [])
    if rc.text.lstrip().startswith("{") or "detail" in rc.text:
        bad(f"{name} ::     the empty body is not a JSON error body", repr(rc.text[:80]))
    else:
        ok(f"{name} ::     the empty body is not a JSON error body")

    RECORDS.append({
        "shape": name.strip(), "query": BASE + q,
        "canonical_header": canonical, "empty_header": empty_header,
        "headers_identical": empty_header == canonical,
        "non_empty_rows": full_rows, "empty_rows": max(0, len(rows) - 1),
        "json_empty_status": rj.status_code, "csv_empty_status": rc.status_code,
        "csv_content_type": rc.headers.get("content-type"),
        "value_columns": value_cols, "missing_value_columns": missing,
    })
    print()

# ------------------------------------------------------- 7. errors are not 200 --------
print("7. invalid requests and store failures are NOT converted to 200")
# A parameter with no data array anywhere in the fixture.
for label, q in [("a parameter with no data array", "&parameter=nitrate&time_period=0"),
                 ("an unsupported time_period", "&parameter=temperature&time_period=99")]:
    rj = c.get("/api/woa23" + BASE + q)
    rc = c.get("/api/woa23/csv" + BASE + q)
    is_(f"  (7) {label}: JSON not 200", rj.status_code != 200, True)
    is_(f"  (7) {label}: CSV  not 200", rc.status_code != 200, True)
    is_(f"  (7) {label}: both agree", rj.status_code, rc.status_code)
    RECORDS.append({"error_case": label, "json_status": rj.status_code, "csv_status": rc.status_code})

# A malformed request: lon0 is required.
rj = c.get("/api/woa23?lat0=15&grid=1&parameter=temperature")
rc = c.get("/api/woa23/csv?lat0=15&grid=1&parameter=temperature")
is_("  (7) malformed request: JSON not 200", rj.status_code != 200, True)
is_("  (7) malformed request: CSV  not 200", rc.status_code != 200, True)
is_("  (7) malformed request: both agree", rj.status_code, rc.status_code)
RECORDS.append({"error_case": "malformed request (lon0 missing)",
                "json_status": rj.status_code, "csv_status": rc.status_code})

# A store failure: a fresh app pointed at a directory that is not a store.
import importlib                                                   # noqa: E402
broken = tempfile.mkdtemp(prefix="g1-broken-")
os.environ["WOA23_ZARR_STORE"] = broken
for m in ("api.config", "api.query", "api.app"):
    sys.modules.pop(m, None)
bapp = importlib.import_module("api.app")
bc = TestClient(bapp.app, raise_server_exceptions=False)
rb = bc.get("/api/woa23/csv" + BASE + SHAPES[0][1] + INSIDE)
is_("  (7) store failure: CSV not 200", rb.status_code != 200, True)
if "text/csv" in rb.headers.get("content-type", ""):
    bad("  (7) store failure does not return CSV", f"status {rb.status_code}")
else:
    ok("  (7) store failure does not return CSV")
RECORDS.append({"error_case": "store failure (dir is not a store)", "csv_status": rb.status_code})

print()
OUT.write_text(json.dumps(
    {"gate": "G1", "subject": "143bf8caae4aaa4cd4d4ef9ec0ddcab9ada1174d",
     "offline": True, "vm24_contacted": False, "production_store_used": False,
     "requests_to_8050": 0, "performance_measured": False,
     "passed": PASS, "failed": FAIL, "records": RECORDS}, indent=2))
print(f"evidence written: {OUT}")
print(f"G1_PASSED={PASS} G1_FAILED={FAIL}")
raise SystemExit(1 if FAIL else 0)
