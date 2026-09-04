"""One-shot single-arm issuer for the 8 concrete D-3 characterization requests.

Streamed through stdin. Nothing is written into the staging tree; evidence goes to
~/d3evidence-dep3s/cases/.

CASE DEFINITIONS AND REQUEST SEMANTICS COME FROM THE STAGED SUBJECT, not from here:
`bench.d1_cases.scheduled()` supplies the eight slots, and `bench.d1_probe.fetch()`
performs each request, so URL construction and status handling are the subject's own.
Nothing is retyped and no parameter is substituted.

NO REDIRECT FOLLOWING. urllib follows redirects by default; a global opener is installed
that refuses them, so a redirect becomes a recorded observation rather than a second
silent request. That is a change to this process's urllib, never to a subject file.

EXACT ONCE-ONLY ACCOUNTING. Every attempt increments a counter before it is made, and the
run refuses to report success unless exactly 8 attempts were made for 8 slots. A transport
failure is recorded as its one attempt and is NEVER retried.
"""
import base64
import hashlib
import json
import os
import sys
import time
import urllib.request

BASE = "http://127.0.0.1:19387"
OUT = "/home/woa23c1ro/d3evidence-dep3s/cases"

sys.path.insert(0, "/home/woa23c1ro/woa23-dep3s/dev2026")
from bench import d1_cases as C          # noqa: E402
from bench import d1_probe as P          # noqa: E402


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(
            req.full_url, code, f"redirect refused -> {newurl}", headers, fp)


urllib.request.install_opener(urllib.request.build_opener(NoRedirect))


def sha(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


T = "/home/woa23c1ro/woa23-dep3s/dev2026"
src = {f: sha(f"{T}/bench/{f}") for f in ("d1_cases.py", "d1_probe.py")}

slots = C.scheduled()

# ---------------------------------------------------------------- 1. THE MANIFEST
print("=== 1. THE 8-SLOT MANIFEST (printed BEFORE any request) ===")
print(f"  source bench/d1_cases.py : {src['d1_cases.py']}")
print(f"  source bench/d1_probe.py : {src['d1_probe.py']}")
print(f"  base url                 : {BASE}")
print()
rec_n = 0
manifest = []
for i, c in enumerate(slots, 1):
    ident = c.id
    if ident == "D1-ANCHOR-RECOVER":
        rec_n += 1
        ident = f"D1-ANCHOR-RECOVER #{rec_n}"      # an ordered slot, NOT a retry
    row = {"slot": i, "identity": ident, "case_id": c.id, "endpoint": c.path,
           "group": c.group, "params": dict(c.params)}
    manifest.append(row)
    print(f"  slot {i}  {ident}")
    print(f"          endpoint : {c.path}")
    print(f"          group    : {c.group}")
    print(f"          params   : {json.dumps(row['params'], sort_keys=True)}")

# ---------------------------------------------------------------- 2. ASSERTIONS
print()
print("=== 2. ASSERT the manifest matches the corrected request ===")
fail = 0


def ck(label, got, want):
    global fail
    ok = got == want
    if not ok:
        fail += 1
    print(f"  {'ok  ' if ok else 'FAIL'} {label}: {got!r}" + ("" if ok else f" != {want!r}"))


ck("8 slots", len(slots), 8)
ck("characterization_requests_per_arm()", C.characterization_requests_per_arm(), 8)
ck("STORE_PROBE_REQUESTS_PER_ARM (not issued as HTTP)", C.STORE_PROBE_REQUESTS_PER_ARM, 2)
ck("countable budget", C.countable_requests_per_arm(), 10)
ck("seasonal period", C.REQUIRED_SEASONAL_PERIOD, "13")
ck("four anchor-recovery slots", rec_n, 4)
ck("case ids in order", [m["case_id"] for m in manifest],
   ["D1-DEPTH-SUP", "D1-ANCHOR-RECOVER", "D1-DEPTH-SUP-csv", "D1-ANCHOR-RECOVER",
    "D1-DEPTH-OOR-tp13", "D1-ANCHOR-RECOVER", "D1-DEPTH-OOR-tp13-csv", "D1-ANCHOR-RECOVER"])
ck("no expect_status field on D1Case", hasattr(slots[0], "expect_status"), False)
ck("every endpoint is an api path", sorted({m["endpoint"] for m in manifest}),
   ["/api/woa23", "/api/woa23/csv"])
if fail:
    print(f"\n*** {fail} manifest assertion(s) FAILED — no request issued ***")
    raise SystemExit(2)

# ---------------------------------------------------------------- 3. NOT YET SENT
print()
print("=== 3. CONFIRM no request has been sent yet ===")
os.makedirs(OUT, exist_ok=True)
existing = [f for f in os.listdir(OUT) if f.endswith(".json")]
print(f"  evidence dir     : {OUT}")
print(f"  pre-existing recs: {len(existing)}  (must be 0)")
if existing:
    print("*** evidence already present — refusing to overwrite ***")
    raise SystemExit(2)
ATTEMPTS = 0
print(f"  attempts so far  : {ATTEMPTS}")

# ---------------------------------------------------------------- 4. ISSUE
print()
print("=== 4. ISSUING 8 SLOTS, SEQUENTIALLY, ONCE EACH ===")
records = []
for row, case in zip(manifest, slots):
    ATTEMPTS += 1
    t0 = time.monotonic()
    raw = P.fetch(BASE, case.path, case.params)          # the subject's own fetch
    dt = time.monotonic() - t0
    body = raw["body"] or b""
    url = f"{BASE}{case.path}?" + urllib.parse.urlencode(dict(case.params))
    rec = {
        "slot": row["slot"], "identity": row["identity"], "case_id": case.id,
        "url": url, "endpoint": case.path, "group": case.group,
        "params": row["params"],
        "http_status": raw["http_status"],
        "transport_error": raw["transport_error"],
        "content_type": raw["content_type"],
        "content_disposition": raw["content_disposition"],
        "elapsed_seconds": round(dt, 4),
        "body_bytes": len(body),
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "body_b64_full": base64.b64encode(body).decode("ascii"),
        "attempt_number": ATTEMPTS,
    }
    records.append(rec)
    with open(f"{OUT}/slot{row['slot']:02d}_{case.id}.json", "w") as fh:
        json.dump(rec, fh, indent=2, sort_keys=True)
    with open(f"{OUT}/slot{row['slot']:02d}_{case.id}.body", "wb") as fh:
        fh.write(body)
    st = rec["http_status"]
    te = rec["transport_error"]
    print(f"  slot {row['slot']}  {row['identity']:<24} status={st:<4} "
          f"bytes={rec['body_bytes']:<8} {rec['elapsed_seconds']:>7.3f}s"
          + (f"  TRANSPORT_ERROR={te}" if te else ""))

# ---------------------------------------------------------------- 5. ACCOUNTING
print()
print("=== 5. ONCE-ONLY ACCOUNTING ===")
print(f"  slots           : {len(slots)}")
print(f"  attempts made   : {ATTEMPTS}")
print(f"  records written : {len(records)}")
if not (ATTEMPTS == len(slots) == len(records) == 8):
    print("*** once-only accounting FAILED — reporting INCOMPLETE ***")
    raise SystemExit(2)
print("  exactly one attempt per slot, 8 of 8 — no retry, no replay")
with open(f"{OUT}/manifest.json", "w") as fh:
    json.dump({"base_url": BASE, "sources": src, "manifest": manifest,
               "attempts": ATTEMPTS}, fh, indent=2, sort_keys=True)
print()
print("=== observed statuses (recorded, NOT judged against any expectation) ===")
for r in records:
    print(f"  slot {r['slot']}  {r['identity']:<24} {r['http_status']}  "
          f"{r['content_type']}  sha256={r['body_sha256'][:16]}…")
print("ISSUER_OK")
