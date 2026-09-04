#!/usr/bin/env python3
"""C1's decided differences: column order (spec 015) and the 1.1.0 docs (spec 008).

`c1p` reached the contract gate and returned FAIL on 5 of 64 — and could produce no
verdict, because the harness had no way to say "this difference was decided". Four cases
were spec 015's parameter-major column order; the fifth, `C20a`, was spec 008 rev 6
publishing API 1.1.0 in the OpenAPI document.

Neither may be counted as a regression. Neither may be waved through either: the
permission is narrow and PROVEN each time —

  column order   the reference's own rows, permuted into the candidate's column
                 sequence, must reproduce the candidate's bytes EXACTLY
  documentation  the two OpenAPI documents must be identical once version, summary and
                 description strings are removed — a route, parameter, schema or
                 response that moved still fails

Every allowance below is therefore tested BOTH ways: the decided change is accepted, and
a difference that merely resembles it is still refused.

    uv run python -m bench.test_c1_decided_differences
"""

import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

_SCRATCH = Path(os.environ.get("TMPDIR", "/tmp")) / "woa23-c1-decided-import"
_SCRATCH.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("WOA23_ZARR_STORE", str(_SCRATCH))
os.environ.setdefault("WOA23_ANCHOR_REL", ".")

from bench.contract_diff import (  # noqa: E402
    canonical_column_rule, classify_raw_difference, column_order_difference,
    compare_canonical, openapi_docs_only_difference, summarise_5_2C)
from bench.suite_summary import summary, summary_line   # noqa: E402

PASS = FAIL = 0


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


def resp(body, status=200):
    return {"status": status, "body": body if isinstance(body, bytes) else body.encode()}


def jrows(rows):
    return json.dumps(rows, separators=(",", ":")).encode()


def csvrows(rows):
    cols = list(rows[0])
    out = [",".join(cols)]
    out += [",".join(str(r[c]) for c in cols) for r in rows]
    return ("\n".join(out) + "\n").encode()


NOCONTRACT = {"applies": False}

# The request these rows came from. Conformance is a separate finding from
# reconstruction (see bench/column_contract) and needs the REQUEST, not just the
# bytes; passing None instead would mean "the request is unknown" and would --
# correctly -- come back UNVERIFIED rather than ruled against invented defaults.
# Named in the request's own reversed order on purpose: spec 015 section 3 says the
# canonical order must not follow it.
REQ = {"parameter": "salinity,temperature", "append": "mn,an"}

# The shape c1p actually saw: same columns, different sequence. The reference is
# unmodified woa23_app emitting hash-seeded order; the candidate is parameter-major.
REF_ROWS = [{"lon": 1.5, "lat": 2.5, "depth": 0.0, "time_period": "0",
             "temperature_an": 0.0, "salinity_an": 1.0, "temperature": 0.5,
             "salinity": 2.0},
            {"lon": 2.5, "lat": 2.5, "depth": 0.0, "time_period": "0",
             "temperature_an": 3.0, "salinity_an": 4.0, "temperature": 5.0,
             "salinity": 6.0}]
CAND_COLS = ["lon", "lat", "depth", "time_period",
             "temperature_an", "temperature", "salinity_an", "salinity"]
CAND_ROWS = [{k: r[k] for k in CAND_COLS} for r in REF_ROWS]

print("the decided COLUMN ORDER change (spec 015) — the c1p shape")
d = column_order_difference(resp(jrows(REF_ROWS)), resp(jrows(CAND_ROWS)), False)
check("it is recognised as a column-order difference", True, d["applies"])
check("  the reference sequence is recorded", "salinity_an", d["ref_columns"][5])
check("  the candidate sequence is recorded", "temperature", d["cand_columns"][5])
check("  RECONSTRUCTION succeeds (JSON)", True, d["reconstructed"])

d = column_order_difference(resp(csvrows(REF_ROWS)), resp(csvrows(CAND_ROWS)), True)
check("  RECONSTRUCTION succeeds (CSV)", True, d["reconstructed"])

r = classify_raw_difference(resp(jrows(REF_ROWS)), resp(jrows(CAND_ROWS)), False,
                            NOCONTRACT, REQ)
check("classified EXPECTED_COLUMN_ORDER_CHANGE", "EXPECTED_COLUMN_ORDER_CHANGE",
      r["classification"])
check("  reconstruction proven", True, r["columns"]["reconstructed"])
check("  conformance CHECKED, not assumed", True, r["conformance"]["verified"])
check("  and conformant", True, r["conformance"]["conformant"])

# Without the request, the SAME bytes must not be ruled a regression against
# defaults -- they must be UNVERIFIED. A comparator that invents a request it was
# never given reports conformant candidates as regressions.
u = classify_raw_difference(resp(jrows(REF_ROWS)), resp(jrows(CAND_ROWS)), False,
                            NOCONTRACT, None)
check("  with no request supplied it is UNVERIFIED", "UNRECONSTRUCTED",
      u["classification"])
check("    not a regression", False,
      u["classification"] == "REGRESSION_BYTES_MUST_MATCH")
check("    and reconstruction still succeeded", True, u["columns"]["reconstructed"])
check("  and NOT a regression", False,
      r["classification"] == "REGRESSION_BYTES_MUST_MATCH")

print()
print("SENSITIVITY: differences that merely RESEMBLE the decided one are still refused")

# Same column set, permuted — but a VALUE also changed. Reconstruction must fail.
bad = [dict(r) for r in CAND_ROWS]
bad[1]["temperature"] = 99.0
d = column_order_difference(resp(jrows(REF_ROWS)), resp(jrows(bad)), False)
check("a value change hiding behind a column permutation is NOT reconstructed",
      False, d["reconstructed"])
r = classify_raw_difference(resp(jrows(REF_ROWS)), resp(jrows(bad)), False,
                            NOCONTRACT, REQ)
check("  so it is a REGRESSION", "REGRESSION_BYTES_MUST_MATCH", r["classification"])

# A column SET difference is a real defect, not an ordering one.
missing = [{k: v for k, v in r.items() if k != "salinity"} for r in CAND_ROWS]
d = column_order_difference(resp(jrows(REF_ROWS)), resp(jrows(missing)), False)
check("a column SET difference does not qualify as column order", False, d["applies"])
check("  and says the sets differ", True, "SETS differ" in (d["why"] or ""))
r = classify_raw_difference(resp(jrows(REF_ROWS)), resp(jrows(missing)), False,
                            NOCONTRACT, REQ)
check("  it is a REGRESSION", "REGRESSION_BYTES_MUST_MATCH", r["classification"])

# A ROW count difference is not a column-order difference either.
r = classify_raw_difference(resp(jrows(REF_ROWS)), resp(jrows(CAND_ROWS[:1])), False,
                            NOCONTRACT)
check("a row-count difference is not column order",
      True, r["classification"] != "EXPECTED_COLUMN_ORDER_CHANGE")

print()
print("canonical comparison: a SEQUENCE difference is no longer a canonical mismatch")
probs = compare_canonical(resp(jrows(REF_ROWS)), resp(jrows(CAND_ROWS)), False)
check("no canonical problem for a pure column permutation", [], probs)
probs = compare_canonical(resp(jrows(REF_ROWS)), resp(jrows(missing)), False)
check("  but a column SET difference IS a canonical problem", True, len(probs) > 0)
check("  and it is named as a SET difference", True,
      any("SET differs" in p for p in probs))

print()
print("the decided DOCUMENTATION change (spec 008 rev 6, API 1.1.0) — the C20a shape")
BASE = {"openapi": "3.1.0",
        "info": {"title": "woa23", "version": "1.0.0", "description": "old"},
        "paths": {"/api/woa23": {"get": {"summary": "s", "description": "d",
                                         "responses": {"200": {"description": "ok"}}}}}}
NEWDOC = json.loads(json.dumps(BASE))
NEWDOC["info"]["version"] = "1.1.0"
NEWDOC["info"]["description"] = ("Row order (since 1.1.0): rows are ordered by "
                                 "(time_period, depth, lat, lon). JSON field order and "
                                 "CSV header order are unchanged.")
NEWDOC["paths"]["/api/woa23"]["get"]["description"] = "d, plus the row-order statement"

d = openapi_docs_only_difference(resp(json.dumps(BASE)), resp(json.dumps(NEWDOC)))
check("recognised as OpenAPI documents", True, d["applies"])
check("  versions are recorded", ("1.0.0", "1.1.0"), (d["ref_version"], d["cand_version"]))
check("  DOCS-ONLY: nothing but version/summary/description moved", True, d["docs_only"])
r = classify_raw_difference(resp(json.dumps(BASE)), resp(json.dumps(NEWDOC)), False,
                            NOCONTRACT)
check("classified EXPECTED_DOCUMENTATION_CHANGE", "EXPECTED_DOCUMENTATION_CHANGE",
      r["classification"])

print()
print("SENSITIVITY: a documentation-sized change that is NOT documentation")
ROUTE = json.loads(json.dumps(NEWDOC))
ROUTE["paths"]["/api/woa23/NEW"] = {"get": {"responses": {"200": {"description": "x"}}}}
d = openapi_docs_only_difference(resp(json.dumps(BASE)), resp(json.dumps(ROUTE)))
check("a NEW ROUTE is not docs-only", False, d["docs_only"])
r = classify_raw_difference(resp(json.dumps(BASE)), resp(json.dumps(ROUTE)), False,
                            NOCONTRACT)
check("  so it is a REGRESSION", "REGRESSION_BYTES_MUST_MATCH", r["classification"])

SCHEMA = json.loads(json.dumps(NEWDOC))
SCHEMA["paths"]["/api/woa23"]["get"]["responses"]["404"] = {"description": "gone"}
d = openapi_docs_only_difference(resp(json.dumps(BASE)), resp(json.dumps(SCHEMA)))
check("a NEW RESPONSE CODE is not docs-only", False, d["docs_only"])

PARAM = json.loads(json.dumps(NEWDOC))
PARAM["paths"]["/api/woa23"]["get"]["parameters"] = [{"name": "sneaky", "in": "query"}]
d = openapi_docs_only_difference(resp(json.dumps(BASE)), resp(json.dumps(PARAM)))
check("a NEW PARAMETER is not docs-only", False, d["docs_only"])

d = openapi_docs_only_difference(resp(b'{"not":"openapi"}'), resp(b'{"not":"openapi2"}'))
check("a non-OpenAPI JSON object does not qualify", False, d["applies"])
r = classify_raw_difference(resp(b'{"a":1}'), resp(b'{"a":2}'), False, NOCONTRACT)
check("  and is a REGRESSION", "REGRESSION_BYTES_MUST_MATCH", r["classification"])

print()
print("the conformance rule must be VERIFIED, never assumed")
# SUPERSEDED, not deleted. This asserted that `canonical_column_rule()` returns a bare
# callable over (present, pars, variables) -- the api.query signature. After c1q it
# returns (comparator, failure): the comparator is pure, takes the candidate's columns
# and the REQUEST, and does the canonical normalisation itself, so no caller can hand
# it an uncanonicalised sequence. What the original protected is kept exactly: the rule
# must be reachable, and it must produce the parameter-major order c1p observed.
comparator, failure = canonical_column_rule()
check("the spec 015 comparator is reachable here", True, comparator is not None)
check("  with no setup failure", True, failure is None)
if comparator is not None:
    from bench.column_contract import expected_column_order
    order = expected_column_order(
        ["salinity", "lon", "temperature_an", "lat", "depth", "time_period",
         "temperature", "salinity_an"],
        {"parameter": "salinity,temperature", "append": "mn,an"})
    check("  and it produces the parameter-major order",
          ["lon", "lat", "depth", "time_period", "temperature_an", "temperature",
           "salinity_an", "salinity"], order)
    check("  which is exactly the candidate sequence c1p observed", CAND_COLS, order)
    # The request deliberately named salinity and mn FIRST: the canonical order must
    # not follow it. That is spec 015 section 3, and it is the property the pure
    # comparator adds over the old callable, which trusted its caller's ordering.
    got = comparator(CAND_COLS, {"parameter": "salinity,temperature", "append": "mn,an"})
    check("  the candidate order is VERIFIED against the rule", True, got["verified"])
    check("  and conforms", True, got["conformant"])
    scrambled = ["lon", "lat", "depth", "time_period", "temperature",
                 "temperature_an", "salinity_an", "salinity"]
    bad = comparator(scrambled, {"parameter": "salinity,temperature", "append": "mn,an"})
    check("  a non-conformant permutation is still caught", False, bad["conformant"])
    check("  and it was checked, not skipped", True, bad["verified"])

print()
print("the gate: decided differences do not fail it, unexplained ones do")


def result(cid, cls, notes=None, order_applies=False):
    return {"id": cid, "verdict": "DIFFER", "notes": notes or [],
            "candidate_row_order_contract": {"applies": order_applies, "ok": True},
            "raw_difference": {"classification": cls,
                               "columns": {"reconstructed": True,
                                           "ref_columns": [], "cand_columns": []}}}


s = summarise_5_2C([result("C1", "EXPECTED_COLUMN_ORDER_CHANGE"),
                    result("C16", "EXPECTED_COLUMN_ORDER_CHANGE"),
                    result("C20a", "EXPECTED_DOCUMENTATION_CHANGE"),
                    result("C2", "BYTE_IDENTICAL")])
check("a run of only decided differences PASSES", "PASS", s["gate"])
check("  column-order diffs are counted apart", ["C1", "C16"],
      s["expected_column_order_diffs"])
check("  documentation diffs are counted apart", ["C20a"],
      s["expected_documentation_diffs"])
check("  and reconstruction is reported", "RECONSTRUCTED", s["column_reconstruction"])
check("  regressions is empty", [], s["regressions"])

s = summarise_5_2C([result("C1", "EXPECTED_COLUMN_ORDER_CHANGE"),
                    result("C9", "REGRESSION_BYTES_MUST_MATCH")])
check("one unexplained difference still FAILS the gate", "FAIL", s["gate"])
check("  and names it", ["C9"], s["regressions"])

s = summarise_5_2C([result("C1", "UNRECONSTRUCTED")])
check("an unprovable reconstruction is INCOMPLETE_VALIDATION, not a pass",
      "INCOMPLETE_VALIDATION", s["gate"])

s = summarise_5_2C([result("C2", "BYTE_IDENTICAL")])
check("with no column-order case, reconstruction is N/A not 'proven'",
      "N/A_NOT_EXERCISED", s["column_reconstruction"])

print()
sys.exit(summary(PASS, FAIL))