#!/usr/bin/env python3
"""The C1 canonical comparison and the C2 candidate-only order gate. Offline.

Spec 008 §7a and §7b. Two gates that did not exist before the row-order contract did:

- **C1 (variant 5.2C).** The candidate deliberately reorders rows, so raw byte
  equality is no longer the verdict. Three findings are kept apart — canonical values
  and columns, the candidate's own conformance, and the raw-order difference recorded
  as the decided change with a byte-level reconstruction to prove nothing else moved.
- **C2.** The same conformance record, aggregated across three cycles, plus the
  requirement that the candidate order a case identically under three different seeds.

**Every gate here is also exercised failing.** A gate that cannot fail proves nothing,
and each of these can be made to pass by accident — by canonicalising away a real
difference, by scoring order-less responses as conforming, or by reading a run that
predates the record as a clean one. Those are the cases with the longest names below.

    uv run python -m bench.test_contract_row_order
"""

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from bench.c2_summary import (  # noqa: E402
    order_stability, overall_outcome, row_order_contract as c2_row_order_contract)
from bench.contract_diff import (  # noqa: E402
    canonicalise, classify_raw_difference, compare_canonical, contract_key,
    reconstruct_from_reference, row_order_contract, summarise_5_2C)
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


# --------------------------------------------------------------- tiny fixtures ---
def row(tp, depth, lat, lon, t=1.0):
    return {"lon": lon, "lat": lat, "depth": depth, "time_period": tp,
            "temperature": t}


ORDERED = [row("0", 0.0, 10.0, 100.0), row("0", 0.0, 10.0, 101.0),
           row("0", 0.0, 11.0, 100.0), row("1", 0.0, 10.0, 100.0),
           row("2", 0.0, 10.0, 100.0), row("13", 0.0, 10.0, 100.0)]
#: The same rows as a string sort would leave them: '13' before '2'.
STRING_SORTED = sorted(ORDERED, key=lambda r: (str(r["time_period"]), r["depth"],
                                               r["lat"], r["lon"]))


def as_json(rows):
    # Separators without spaces, as orjson emits.
    return {"status": 200,
            "body": json.dumps(rows, separators=(",", ":")).encode()}


def as_csv(rows, newline="\n"):
    cols = list(rows[0])
    lines = [",".join(cols)]
    lines += [",".join(str(r[c]) for c in cols) for r in rows]
    return {"status": 200, "body": (newline.join(lines) + newline).encode()}


def main():
    print("the key is numeric, and that is the whole point")
    check("13 sorts after 2", True,
          contract_key(row("2", 0, 0, 0)) < contract_key(row("13", 0, 0, 0)))
    check("a string key would have got it wrong", True, str(13) < str(2))
    check("-10 sorts before -9 as a number", True,
          contract_key(row("0", 0, 0, -10.0)) < contract_key(row("0", 0, 0, -9.0)))

    print("\nthe candidate order gate — conformance")
    check("an ordered JSON response conforms", True,
          row_order_contract(as_json(ORDERED), False)["ok"])
    check("an ordered CSV response conforms", True,
          row_order_contract(as_csv(ORDERED), True)["ok"])
    bad = row_order_contract(as_json(list(reversed(ORDERED))), False)
    check("a reversed response does not", False, bad["ok"])
    check("and the failure names the offending pair", True,
          "precedes" in (bad["violation"] or ""))
    ss = row_order_contract(as_json(STRING_SORTED), False)
    check("a STRING-sorted response is rejected — the 0/1/2/13 trap", False, ss["ok"])
    check("and it really was a different order, not a no-op", False,
          STRING_SORTED == ORDERED)
    one_swap = list(ORDERED)
    one_swap[2], one_swap[3] = one_swap[3], one_swap[2]
    check("one swapped pair is enough to fail", False,
          row_order_contract(as_json(one_swap), False)["ok"])
    dup = [ORDERED[0], dict(ORDERED[0])]
    check("a duplicated contract key fails", False,
          row_order_contract(as_json(dup), False)["ok"])
    check("and says so, rather than calling it an order problem", True,
          "at most one row per key" in
          (row_order_contract(as_json(dup), False)["violation"] or ""))

    print("\nthe gate must not score things it cannot judge")
    check("a 400 body carries no row order", False,
          row_order_contract({"status": 400, "body": b'{"detail":"no"}'},
                             False)["applies"])
    check("and is not scored as conforming", None,
          row_order_contract({"status": 400, "body": b'{"detail":"no"}'},
                             False)["ok"])
    check("a non-row payload carries no row order", False,
          row_order_contract({"status": 200, "body": b'{"openapi":"3.1.0"}'},
                             False)["applies"])
    check("an empty array carries no row order", False,
          row_order_contract(as_json([])["status"] and {"status": 200, "body": b"[]"},
                             False)["applies"])

    print("\nthe canonical comparison — C1's correctness verdict")
    shuffled = as_json(list(reversed(ORDERED)))
    check("the same rows in a different order compare equal", [],
          compare_canonical(shuffled, as_json(ORDERED), False))
    changed = [dict(r) for r in ORDERED]
    changed[3]["temperature"] = 99.0
    check("a changed VALUE is still caught", True,
          bool(compare_canonical(as_json(changed), as_json(ORDERED), False)))
    check("and it is localised to the row and key", True,
          "temperature" in compare_canonical(as_json(changed),
                                             as_json(ORDERED), False)[0])
    dropped = ORDERED[:-1]
    check("a dropped row is caught", True,
          bool(compare_canonical(as_json(dropped), as_json(ORDERED), False)))
    recol = [{"lon": r["lon"], "lat": r["lat"], "depth": r["depth"],
              "time_period": r["time_period"], "temperature": r["temperature"]}
             for r in ORDERED]
    moved = [{"temperature": r["temperature"], "lon": r["lon"], "lat": r["lat"],
              "depth": r["depth"], "time_period": r["time_period"]} for r in ORDERED]
    notes = compare_canonical(as_json(recol), as_json(moved), False)
    # SUPERSEDED BY SPEC 015, and updated rather than deleted. This assertion read
    # "a moved COLUMN is caught — column order is out of scope and must not move",
    # which was spec 008's decision and was correct until 015 made the candidate's
    # parameter-major order a contract. A permutation of the SAME columns is now a
    # decided difference, classified by classify_raw_difference and proven by
    # reconstruction (bench/test_c1_decided_differences.py).
    #
    # What this test still protects is the part 015 did NOT change: a column SET
    # difference is a real defect and must still be caught here.
    check("a permuted COLUMN SEQUENCE is no longer a canonical mismatch (spec 015)",
          [], notes)
    fewer = [{k: v for k, v in r.items() if k != "temperature"} for r in recol]
    setnotes = compare_canonical(as_json(recol), as_json(fewer), False)
    check("  but a column SET difference still is", True,
          bool(setnotes) and "column SET differs" in setnotes[0])
    check("canonicalise is stable and total", [contract_key(r) for r in ORDERED],
          [contract_key(r) for r in canonicalise(list(reversed(ORDERED)))])

    print("\nreconstruction — 'only the order changed', demonstrated")
    ref_j, cand_j = as_json(STRING_SORTED), as_json(ORDERED)
    rec = reconstruct_from_reference(ref_j, cand_j, False)
    check("the reference's own JSON rows, reordered, are the candidate's bytes",
          (True, True), (rec["available"], rec["matches"]))
    ref_c, cand_c = as_csv(STRING_SORTED), as_csv(ORDERED)
    rec_c = reconstruct_from_reference(ref_c, cand_c, True)
    check("and the same holds for CSV", (True, True),
          (rec_c["available"], rec_c["matches"]))
    check("CRLF is honoured rather than rewritten", (True, True),
          (lambda r: (r["available"], r["matches"]))(
              reconstruct_from_reference(as_csv(STRING_SORTED, "\r\n"),
                                         as_csv(ORDERED, "\r\n"), True)))
    # The check that stops this from proving too much: if a VALUE also changed, the
    # reordered reference no longer reproduces the candidate.
    cand_changed = as_json(canonicalise(changed))
    rec_bad = reconstruct_from_reference(ref_j, cand_changed, False)
    check("a value change makes reconstruction FAIL to match", (True, False),
          (rec_bad["available"], rec_bad["matches"]))
    check("a body that is not a JSON array refuses rather than guesses", False,
          reconstruct_from_reference({"status": 200, "body": b'{"a":1}'},
                                     {"status": 200, "body": b'{"a":1}'},
                                     False)["available"])

    print("\nclassifying a raw byte difference")
    oc_ok = {"applies": True, "ok": True}
    check("identical bytes are identical bytes", "BYTE_IDENTICAL",
          classify_raw_difference(cand_j, cand_j, False, oc_ok)["classification"])
    check("a reordered multi-row 200 is the EXPECTED change",
          "EXPECTED_ROW_ORDER_CHANGE",
          classify_raw_difference(ref_j, cand_j, False, oc_ok)["classification"])
    err_a = {"status": 400, "body": b'{"detail":"a"}'}
    err_b = {"status": 400, "body": b'{"detail":"b"}'}
    check("a differing ERROR body is still a regression",
          "REGRESSION_BYTES_MUST_MATCH",
          classify_raw_difference(err_a, err_b, False, oc_ok)["classification"])
    one_a = as_json([ORDERED[0]])
    one_b = as_json([dict(ORDERED[0], temperature=2.0)])
    check("a single-row 200 has only one order, so a difference is a regression",
          "REGRESSION_BYTES_MUST_MATCH",
          classify_raw_difference(one_a, one_b, False, oc_ok)["classification"])
    check("a reference ALREADY in contract order cannot explain a difference",
          "REGRESSION_BYTES_MUST_MATCH",
          classify_raw_difference(as_json(ORDERED), as_json(changed), False,
                                  oc_ok)["classification"])
    check("a non-row payload difference is a regression",
          "REGRESSION_BYTES_MUST_MATCH",
          classify_raw_difference({"status": 200, "body": b'{"openapi":"3.1.0"}'},
                                  {"status": 200, "body": b'{"openapi":"3.1.1"}'},
                                  False, oc_ok)["classification"])
    check("reordering that does NOT reproduce the bytes is a regression, not expected",
          "REGRESSION_BYTES_MUST_MATCH",
          classify_raw_difference(ref_j, cand_changed, False,
                                  oc_ok)["classification"])

    print("\nthe 5.2C summary — PASS needs all three findings")
    def result(cid, notes=(), order_ok=True, applies=True, cls="BYTE_IDENTICAL"):
        return {"id": cid, "notes": list(notes),
                "candidate_row_order_contract": {"applies": applies, "ok": order_ok,
                                                 "violation": None},
                "raw_difference": {"classification": cls}}

    clean = [result("C1"), result("C2", cls="EXPECTED_ROW_ORDER_CHANGE")]
    f = summarise_5_2C(clean)
    check("a clean run passes", "PASS", f["gate"])
    check("and the headline names all three findings", True,
          all(t in f["headline"] for t in ("canonical values/columns match",
                                           "candidate row-order contract PASS",
                                           "required byte checks pass",
                                           "no unexpected differences",
                                           "expected raw-order differences:")))
    check("and it refuses the old sentence", True,
          "may NOT be reported as '5.2A byte-exact PASS'" in f["reporting_rule"])
    check("the expected difference was counted, not ignored", 1, f["expected_diffs"])
    check("with a difference present, reconstruction is reported as done",
          "RECONSTRUCTED", f["reconstruction"])

    # The correction the c1f run forced. A count of zero and a successful proof are
    # different findings: the first headline read "0 (reconstructed from the
    # reference's own rows)", which described a proof that never ran. c1f produced
    # exactly this shape — 64 byte-identical cases, zero raw-order differences — so a
    # reader could have taken it as evidence the reconstruction path works. It is not.
    none_diff = summarise_5_2C([result("C1"), result("C2")])
    check("with NO difference, the gate still passes", "PASS", none_diff["gate"])
    check("expected_diffs is zero", 0, none_diff["expected_diffs"])
    check("and reconstruction is N/A, NOT a success", "N/A_NOT_EXERCISED",
          none_diff["reconstruction"])
    check("the headline says the two things separately", True,
          "expected raw-order differences: 0" in none_diff["headline"]
          and "raw-order reconstruction: N/A, NOT EXERCISED" in none_diff["headline"])
    check("and it does not claim anything was reconstructed", False,
          "0, reconstructed" in none_diff["headline"])
    check("the note says the path was not evidenced by this run", True,
          "no evidence that" in none_diff["reconstruction_note"])
    check("the headline reports the required byte checks", True,
          "required byte checks pass" in none_diff["headline"])

    unrec = summarise_5_2C([result("C1"), result("C2", cls="UNRECONSTRUCTED")])
    check("an unreconstructed case is INCOMPLETE_VALIDATION, not PASS",
          "INCOMPLETE_VALIDATION", unrec["gate"])
    check("and the headline forbids a deployment claim", True,
          "may not be carried into a deployment claim" in unrec["headline"])

    ordfail = summarise_5_2C([
        result("C1"),
        result("C2", notes=["ROW_ORDER_CONTRACT_FAILURE: row 3 …"], order_ok=False)])
    check("an order failure fails the gate", "FAIL", ordfail["gate"])
    check("and is named as a contract failure, not a semantic divergence", True,
          "ROW_ORDER_CONTRACT_FAILURE" in ordfail["headline"])

    reg = summarise_5_2C([result("C1", notes=["bytes differ where they must not: x"],
                                 cls="REGRESSION_BYTES_MUST_MATCH")])
    check("a byte regression fails the gate", "FAIL", reg["gate"])
    check("order-less responses are counted apart from conforming ones", 1,
          summarise_5_2C([result("C1", applies=False, order_ok=None)])
          ["order_inapplicable"])

    print("\nC2 — the same record, aggregated over three cycles")
    def cycle(label, per_case):
        return {"label": label,
                "contract": {"results": [
                    {"id": cid,
                     "candidate_row_order_contract": {"applies": ap, "ok": ok,
                                                      "violation": v},
                     "candidate_order": {"row_order_sha256": dig},
                     "reference_order": {"row_order_sha256": rdig}}
                    for cid, ap, ok, v, dig, rdig in per_case]}}

    good = [cycle(f"c{i}", [("C16", True, True, None, "aaa", f"r{i}"),
                            ("C1", True, True, None, "bbb", f"s{i}")])
            for i in (1, 2, 3)]
    roc = c2_row_order_contract(good)
    check("three conforming cycles pass", "PASS", roc["status"])
    check("and it counted the responses it actually judged", 6,
          roc["n_with_row_order"])
    o = order_stability(good)
    check("the candidate is stable across three seeds", True, o["candidate_stable"])
    check("the reference varied, and that is fine", ["C1", "C16"],
          sorted(o["reference_varied"]))
    check("a reference-only variation does not fail the run", "PASS",
          overall_outcome({"gate": "PASS"}, {"status": "OBSERVED"},
                          {"status": "CONSISTENT"}, roc, o)["outcome"])

    drift = [cycle(f"c{i}", [("C16", True, True, None, f"aaa{i}", "r")])
             for i in (1, 2, 3)]
    od = order_stability(drift)
    check("a candidate that orders one case differently per cycle is NOT stable",
          False, od["candidate_stable"])
    out = overall_outcome({"gate": "PASS"}, {"status": "OBSERVED"},
                          {"status": "CONSISTENT"},
                          c2_row_order_contract(drift), od)
    check("and that is a ROW_ORDER_CONTRACT_FAILURE", "ROW_ORDER_CONTRACT_FAILURE",
          out["outcome"])
    check("with its own exit code, not FAIL's", 6, out["exit_code"])
    check("and it is not attributed to the semantic gate", "PASS",
          out["contract_gate"])

    viol = [cycle("c1", [("C16", True, False, "row 3 precedes row 4", "a", "r")]),
            cycle("c2", [("C16", True, True, None, "a", "r")]),
            cycle("c3", [("C16", True, True, None, "a", "r")])]
    rv = c2_row_order_contract(viol)
    check("one non-conforming response fails the whole gate",
          "ROW_ORDER_CONTRACT_FAILURE", rv["status"])
    check("and the violation is quoted with its cycle and case", True,
          rv["violations"] and rv["violations"][0].startswith("c1/C16"))

    print("\nC2 fails closed on evidence it does not have")
    stale = [{"label": f"c{i}", "contract": {"results": [{"id": "C16"}]}}
             for i in (1, 2, 3)]
    check("a run predating the record is INDETERMINATE, not a pass", "INDETERMINATE",
          c2_row_order_contract(stale)["status"])
    check("and INDETERMINATE does not become an overall PASS", "FAIL",
          overall_outcome({"gate": "PASS"}, {"status": "OBSERVED"},
                          {"status": "CONSISTENT"},
                          c2_row_order_contract(stale), order_stability(stale))
          ["outcome"])
    check("a cycle with no contract artefact at all is INDETERMINATE",
          "INDETERMINATE",
          c2_row_order_contract([{"label": "c1", "contract": None}])["status"])
    check("a failing semantic gate still fails, order contract or not", "FAIL",
          overall_outcome({"gate": "FAIL"}, {"status": "OBSERVED"},
                          {"status": "CONSISTENT"},
                          c2_row_order_contract(good), order_stability(good))
          ["outcome"])
    # Order of precedence: a contract failure is reported as itself even when the
    # seed observation is INSUFFICIENT, which would otherwise rename the outcome.
    check("a contract failure outranks the seed-diversity rename",
          "ROW_ORDER_CONTRACT_FAILURE",
          overall_outcome({"gate": "PASS"}, {"status": "INSUFFICIENT"},
                          {"status": "CONSISTENT"},
                          c2_row_order_contract(viol), order_stability(viol))
          ["outcome"])

    print()
    return summary(PASS, FAIL)
if __name__ == "__main__":
    sys.exit(main())
