#!/usr/bin/env python3
"""C2's expected-documentation layer: narrow, separate, and above an unchanged gate.

`c2j` produced NO C2 RESULT. Cycle 1 failed the 5.2B gate on `C20a` -- the decided
OpenAPI 1.0.0 -> 1.1.0 publication that `c1r` had already proved is documentation-only.
`c1r` taught the 5.2C path; the 5.2B path was never taught, and `compare_semantic`
falls back to raw bytes for a non-row payload and never classifies at all.

The fix does NOT touch `compare_semantic`. It adds a classification layer above it:
the comparator still says exactly what it said, and `c2_expected_documentation` decides,
separately, whether what it said is the one known change.

The permission here is NARROWER than C1's. C1 accepted any version pair that was
docs-only. This accepts exactly 1.0.0 -> 1.1.0 and nothing else.

Every allowance is tested both ways: the decided change is classified, and everything
that merely resembles it is still a regression.

    uv run python -m bench.test_c2_expected_docs
"""

import copy
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from bench.contract_diff import (  # noqa: E402
    C2_EXPECTED_DOC_VERSIONS, c2_expected_documentation, compare_semantic,
    retained_evidence)

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
    if not isinstance(body, bytes):
        body = json.dumps(body, separators=(",", ":")).encode()
    return {"status": status, "body": body}


BASE_DOC = {
    "openapi": "3.1.0",
    "info": {"title": "ODB WOA23 API", "version": "1.0.0",
             "description": "Open API to query WOA2023 data."},
    "paths": {"/api/woa23": {"get": {
        "summary": "Query WOA23",
        "parameters": [{"name": "lon0", "in": "query"}],
        "responses": {"200": {"description": "ok", "content": {
            "application/json": {"schema": {"type": "object"}}}}}}}},
}


def doc(version="1.1.0", **over):
    d = copy.deepcopy(BASE_DOC)
    d["info"]["version"] = version
    g = d["paths"]["/api/woa23"]["get"]
    if over.get("row_order_text"):
        g["summary"] = "Query WOA23; rows are returned in contract order"
        g["responses"]["200"]["description"] = "ok; rows ascend by time_period"
        d["info"]["description"] += " Rows are returned in a deterministic order."
    if over.get("new_route"):
        d["paths"]["/api/woa23/new"] = {"get": {"responses": {"200": {}}}}
    if over.get("new_response"):
        g["responses"]["503"] = {"description": "unavailable"}
    if over.get("new_parameter"):
        g["parameters"].append({"name": "sneaky", "in": "query"})
    if over.get("dropped_parameter"):
        g["parameters"] = []
    if over.get("changed_schema"):
        g["responses"]["200"]["content"]["application/json"]["schema"] = {"type": "array"}
    if over.get("changed_route"):
        d["paths"]["/api/woa23_v2"] = d["paths"].pop("/api/woa23")
    return d


REF = resp(BASE_DOC)


def semantic(a, b):
    """The unchanged 5.2B comparator, used exactly as the runner uses it."""
    return compare_semantic(a, b, False)


# ============================================================ the decided change ===
print("C20a: the decided 1.0.0 -> 1.1.0 documentation change")

cand = resp(doc(row_order_text=True))
notes = semantic(REF, cand)
check("5.2B still reports it as differing (the comparator is UNCHANGED)",
      True, len(notes) == 1 and notes[0].startswith("non-row payload differs"))
got = c2_expected_documentation(REF, cand, notes)
check("  the layer classifies it EXPECTED", True, got["expected"])
check("  it applies", True, got["applies"])
check("  docs_only is true", True, got["docs"]["docs_only"])
check("  and the versions are recorded", ("1.0.0", "1.1.0"),
      (got["docs"]["ref_version"], got["docs"]["cand_version"]))
check("  the decided pair is exactly 1.0.0 -> 1.1.0", ("1.0.0", "1.1.0"),
      C2_EXPECTED_DOC_VERSIONS)

print()
print("  the exact bodies are retained and structurally re-comparable")
ev = retained_evidence(REF, cand, None)
check("bodies retained", True, ev is not None)
check("  the reference body EXACTLY", REF["body"].decode(), ev["reference"]["text"])
check("  the candidate body EXACTLY", cand["body"].decode(), ev["candidate"]["text"])
check("  with both digests", True,
      bool(ev["reference"]["sha256"]) and bool(ev["candidate"]["sha256"]))
# Re-audit from the artefact alone, not merely re-derive from a digest.
again = c2_expected_documentation(
    resp(ev["reference"]["text"].encode()), resp(ev["candidate"]["text"].encode()),
    semantic(resp(ev["reference"]["text"].encode()),
             resp(ev["candidate"]["text"].encode())))
check("  and the artefact alone reproduces the classification", True, again["expected"])

# ================================================== everything that resembles it ===
print()
print("SENSITIVITY: a real documentation change is STILL a regression")

for label, kwargs in [
        ("a new route", {"new_route": True}),
        ("a changed route", {"changed_route": True}),
        ("a new response code", {"new_response": True}),
        ("a new parameter", {"new_parameter": True}),
        ("a dropped parameter", {"dropped_parameter": True}),
        ("a changed schema", {"changed_schema": True}),
]:
    c = resp(doc(row_order_text=True, **kwargs))
    g = c2_expected_documentation(REF, c, semantic(REF, c))
    check(f"{label} is NOT expected", False, g["expected"])
    check(f"  and is reported as not docs-only", False, g["docs"]["docs_only"])

print()
print("SENSITIVITY: only the decided VERSION PAIR qualifies")

for label, ref_v, cand_v in [
        ("1.1.0 -> 1.2.0", "1.1.0", "1.2.0"),
        ("1.0.0 -> 2.0.0", "1.0.0", "2.0.0"),
        ("a downgrade 1.1.0 -> 1.0.0", "1.1.0", "1.0.0"),
        ("1.0.0 -> 1.1.0-rc1", "1.0.0", "1.1.0-rc1"),
]:
    r = resp(doc(version=ref_v))
    c = resp(doc(version=cand_v, row_order_text=True))
    g = c2_expected_documentation(r, c, semantic(r, c))
    check(f"{label} is NOT expected", False, g["expected"])
    check(f"  and says so", True, "is not the decided change" in (g["why"] or ""))

print()
print("SENSITIVITY: it must be the documentation surface, and nothing else")

not_json = resp(b"<html>a swagger page</html>")
g = c2_expected_documentation(REF, not_json, semantic(REF, not_json))
check("a non-JSON payload is NOT expected", False, g["expected"])
check("  and does not even apply", False, g["applies"])

not_openapi = resp({"hello": "world"})
g = c2_expected_documentation(REF, not_openapi, semantic(REF, not_openapi))
check("a JSON object that is not an OpenAPI document is NOT expected", False, g["expected"])
check("  and does not apply", False, g["applies"])

err_a, err_b = resp({"detail": "x"}, 400), resp({"detail": "y"}, 400)
g = c2_expected_documentation(err_a, err_b, semantic(err_a, err_b))
check("a non-200 pair is NOT expected", False, g["expected"])

rows_a = resp([{"lon": 1.0, "temperature": 2.0}])
rows_b = resp([{"lon": 1.0, "temperature": 9.9}])
g = c2_expected_documentation(rows_a, rows_b, semantic(rows_a, rows_b))
check("a ROW payload whose values changed is NOT expected", False, g["expected"])
check("  and the note it carries is not the non-row one", True,
      "not the decided documentation difference" in (g["why"] or ""))

g = c2_expected_documentation(REF, cand, semantic(REF, cand) + ["something else broke"])
check("a case carrying a SECOND note is NOT expected", False, g["expected"])
check("  a second problem is never forgiven", True,
      "expected exactly one" in (g["why"] or ""))

# ========================================= 5.2B is unchanged for ordinary cases ===
print()
print("the 5.2B semantic comparator itself is UNCHANGED")

same = resp([{"lon": 1.0, "lat": 2.0, "depth": 0.0, "time_period": "0", "t": 3.0}])
check("identical row payloads still MATCH", [], semantic(same, copy.deepcopy(same)))

a = resp([{"lon": 1.0, "lat": 2.0, "depth": 0.0, "time_period": "0", "t": 3.0},
          {"lon": 2.0, "lat": 2.0, "depth": 0.0, "time_period": "0", "t": 4.0}])
b = resp(list(reversed(json.loads(a["body"].decode()))))
check("a row-ORDER difference is still semantically equal (5.2B's whole point)",
      [], semantic(a, b))

c = resp([{"lon": 1.0, "lat": 2.0, "depth": 0.0, "time_period": "0", "t": 3.0},
          {"lon": 2.0, "lat": 2.0, "depth": 0.0, "time_period": "0", "t": 99.0}])
check("a VALUE difference still fails", True, len(semantic(a, c)) > 0)

d = resp([{"lon": 1.0, "lat": 2.0, "depth": 0.0, "time_period": "0", "extra": 3.0},
          {"lon": 2.0, "lat": 2.0, "depth": 0.0, "time_period": "0", "extra": 4.0}])
check("a COLUMN SET difference still fails", True,
      any("column sets differ" in p for p in semantic(a, d)))

check("a STATUS difference still fails", True,
      any("status" in p for p in semantic(resp({}, 200), resp({}, 400))))

identical_docs = resp(BASE_DOC)
check("two IDENTICAL non-row payloads still match", [], semantic(REF, identical_docs))

# ================================================== C2 still needs three cycles ===
print()
print("C2 still requires all three cycles, and the verdicts stay verdicts")

import bench.c2_summary as c2s  # noqa: E402
from bench.suite_summary import summary          # noqa: E402

src = (REPO / "scripts" / "run_c2_cycles.sh").read_text()
check("CYCLES is fixed at 3", True, "CYCLES=3" in src)
check("  and --cycles is refused as a flag", True,
      "--cycles is not a flag" in src)
check("  a failing cycle stops the run", True,
      "two" in src and "not two thirds of an answer" in src)

check("ROW_ORDER_CONTRACT_FAILURE has its own exit code", 6,
      c2s.OUTCOME_EXIT.get("ROW_ORDER_CONTRACT_FAILURE"))
check("  and is not the semantic PASS code", True,
      c2s.OUTCOME_EXIT.get("ROW_ORDER_CONTRACT_FAILURE") != c2s.OUTCOME_EXIT.get("PASS"))

# Functional, not by docstring text: what these return is the contract.
def cyc(label, results):
    return {"label": label, "contract": {"results": results}}


def rowres(cid, applies=True, ok=True, cand_digest="c", ref_digest="r", omit=False):
    r = {"id": cid,
         "candidate_order": {"row_order_sha256": cand_digest},
         "reference_order": {"row_order_sha256": ref_digest}}
    if not omit:
        r["candidate_row_order_contract"] = {"applies": applies, "ok": ok,
                                             "violation": None if ok else "out of order"}
    return r


conforming = [cyc(f"c{i}", [rowres("C1")]) for i in (1, 2, 3)]
check("three conforming cycles: row-order contract PASSES", "PASS",
      c2s.row_order_contract(conforming)["status"])

missing_rec = [cyc("c1", [rowres("C1")]), cyc("c2", [rowres("C1", omit=True)]),
               cyc("c3", [rowres("C1")])]
got = c2s.row_order_contract(missing_rec)
check("MISSING conformance evidence is INDETERMINATE", "INDETERMINATE", got["status"])
check("  and it is NOT a pass", True, got["status"] != "PASS")
check("  the missing record is COUNTED", 1, got.get("n_missing_records"))
check("  and named, so it can be found", True,
      any("c2/C1" in m for m in got.get("missing_records") or []))

violating = [cyc("c1", [rowres("C1")]), cyc("c2", [rowres("C1", ok=False)]),
             cyc("c3", [rowres("C1")])]
check("a conformance VIOLATION is ROW_ORDER_CONTRACT_FAILURE",
      "ROW_ORDER_CONTRACT_FAILURE", c2s.row_order_contract(violating)["status"])

no_artefact = [cyc("c1", [rowres("C1")]), {"label": "c2"}, cyc("c3", [rowres("C1")])]
check("a cycle with no contract artefact is INDETERMINATE", "INDETERMINATE",
      c2s.row_order_contract(no_artefact)["status"])

# Candidate variation across cycles is a VERDICT; reference variation an OBSERVATION.
cand_varies = [cyc("c1", [rowres("C1", cand_digest="a")]),
               cyc("c2", [rowres("C1", cand_digest="b")]),
               cyc("c3", [rowres("C1", cand_digest="a")])]
st = c2s.order_stability(cand_varies)
check("CANDIDATE order varying across cycles is flagged", True,
      bool(st.get("candidate_varied")))

ref_varies = [cyc("c1", [rowres("C1", ref_digest="a")]),
              cyc("c2", [rowres("C1", ref_digest="b")]),
              cyc("c3", [rowres("C1", ref_digest="c")])]
st = c2s.order_stability(ref_varies)
check("REFERENCE order varying is NOT a candidate verdict", [],
      list(st.get("candidate_varied") or []))
check("  but it IS recorded as an observation", True, bool(st.get("varied")))

# Seed diversity: an observation that stops, never a fourth cycle.
def seedcyc(label, digest):
    return {"label": label,
            "interp": {"candidate": {"seed_digest": digest, "hashseed_env": None},
                       "reference": {"seed_digest": digest, "hashseed_env": None}}}


same_seeds = [seedcyc(f"c{i}", "7" * 64) for i in (1, 2, 3)]
sdv = c2s.seed_diversity(same_seeds)
check("identical seeds across cycles is INSUFFICIENT, not a failure", True,
      sdv["status"] != "PASS")
check("  and n_cycles stays 3 -- no fourth is ever suggested", 3, sdv["n_cycles"])

print()
sys.exit(summary(PASS, FAIL))