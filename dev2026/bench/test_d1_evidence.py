"""What the D1 evidence records, and the three narrow things it judges.

The temptation this file exists to resist is an assertion about a status code. The
run is a characterization: what the real store returns for these queries is not known
and is the reason for running it. So the tests below check that a 200 and a 400 and a
500 are all recorded and none of them is called wrong — and that the three things
that ARE decided cannot be quietly passed.

    uv run python -m bench.test_d1_evidence
"""

import hashlib
import json
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.d1_evidence import (                               # noqa: E402
    OUTCOMES, REQUIRED_FIELDS, build, compare, decide, isolation, request_level,
    validate,
)
from bench.suite_summary import summary          # noqa: E402

PASS = 0
FAIL = 0
REPO = Path(__file__).resolve().parent.parent


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


def rec(case_id, arm, *, status=200, ctype="application/json", body=b"[]",
        order="RC", request_level_=True, alive=True, isolated=True, rows=0,
        err=None, disposition=None):
    import base64
    return {
        "case_id": case_id, "arm": arm, "endpoint": "/api/woa23",
        "params": {"lon0": 135, "lat0": 15}, "request_order": order,
        "http_status": status, "content_type": ctype,
        "content_disposition": disposition,
        "body_bytes": len(body), "body_sha256": hashlib.sha256(body).hexdigest(),
        "body_head_b64": base64.b64encode(body[:512]).decode("ascii"),
        "error_text": err, "row_count": rows,
        "request_level": request_level_, "process_alive": alive,
        "depth_isolated": isolated,
    }


def pair(**kw):
    return ([rec("D1-DEPTH-OOR", "candidate", **kw)],
            [rec("D1-DEPTH-OOR", "reference", **kw)])


GOOD_SURVEY = {"preconditions_met": True, "depth_isolated": True,
               "verdict": "PRECONDITIONS_MET", "chosen_seasonal_period": "13"}


def outcome(cand, ref, survey=GOOD_SURVEY):
    ev = validate(cand, "candidate") + validate(ref, "reference")
    cmp_ = compare(cand, ref)
    lvl = request_level(cand, "candidate") + request_level(ref, "reference")
    iso = isolation(cand, survey) + isolation(ref, survey)
    return decide(evidence_problems=ev, comparison=cmp_, level_problems=lvl,
                  isolation_problems=iso, survey=survey)["outcome"]


print("nothing here judges a status code")
for status, ctype, body, rows in ((200, "application/json", b"[]", 0),
                                  (200, "application/json", b'[{"a":1}]', 1),
                                  (400, "application/json",
                                   b'{"detail":"No data available"}', None),
                                  (404, "application/json",
                                   b'{"detail":"No data found"}', None),
                                  (500, "application/json",
                                   b'{"detail":"Internal server error"}', None),
                                  (200, "text/csv", b"lon,lat\n1,2\n", None)):
    c, r = pair(status=status, ctype=ctype, body=body, rows=rows)
    check(f"HTTP {status} {ctype} is recorded, not judged",
          "CHARACTERIZATION_RECORDED", outcome(c, r))

print()
print("what IS judged: the arms must return the same bytes")
c = [rec("D1-DEPTH-OOR", "candidate", body=b'[{"a":1}]')]
r = [rec("D1-DEPTH-OOR", "reference", body=b'[{"a":2}]')]
check("different bodies are a DIVERGENCE", "DIVERGENCE", outcome(c, r))
check("and the case is named", True,
      any("D1-DEPTH-OOR" in p for p in compare(c, r)["problems"]))

c = [rec("D1-DEPTH-OOR", "candidate", status=200)]
r = [rec("D1-DEPTH-OOR", "reference", status=400)]
check("a different status is a DIVERGENCE even with the same body length",
      "DIVERGENCE", outcome(c, r))
c = [rec("D1-DEPTH-SUP-csv", "candidate", ctype="text/csv")]
r = [rec("D1-DEPTH-SUP-csv", "reference", ctype="application/json")]
check("a different content type is a DIVERGENCE too", "DIVERGENCE", outcome(c, r))

c, _ = pair()
check("a case only one arm recorded is a DIVERGENCE, not a pass",
      "DIVERGENCE", outcome(c, []))
check("and the count difference is named", True,
      any("different numbers of requests" in p_ for p_ in compare(c, [])["problems"]))
# The recovery probe is issued four times per arm, so a case id is not a key and
# pairing is by position. Arms asked different things at the same position is a
# problem in itself.
c3 = [rec("D1-DEPTH-SUP", "candidate"), rec("D1-ANCHOR-RECOVER", "candidate")]
r3 = [rec("D1-ANCHOR-RECOVER", "reference"), rec("D1-DEPTH-SUP", "reference")]
check("arms asked the same cases in a different order are refused", False,
      compare(c3, r3)["ok"])
check("and it says they were not asked the same thing", True,
      any("not asked the same thing" in p_ for p_ in compare(c3, r3)["problems"]))
c4 = [rec("D1-ANCHOR-RECOVER", "candidate") for _ in range(4)]
r4 = [rec("D1-ANCHOR-RECOVER", "reference") for _ in range(4)]
check("four identical recovery probes per arm are fine, not a duplicate", True,
      compare(c4, r4)["ok"])

print()
print("what IS judged: each case must leave the process serving")
c, r = pair(request_level_=False)
check("a failed recovery probe is a REQUEST_LEVEL_VIOLATION",
      "REQUEST_LEVEL_VIOLATION", outcome(c, r))
c, r = pair(request_level_=None)
check("an undetermined recovery is NOT treated as success",
      "REQUEST_LEVEL_VIOLATION", outcome(c, r))
c, r = pair(alive=False)
check("a dead process in the tree is a violation", "REQUEST_LEVEL_VIOLATION",
      outcome(c, r))
c, r = pair(alive=None)
check("an undetermined liveness is not success either",
      "REQUEST_LEVEL_VIOLATION", outcome(c, r))
check("the recovery probe itself is not held to the rule", [],
      request_level([rec("D1-ANCHOR-RECOVER", "candidate", request_level_=None,
                         alive=None)], "candidate"))

print()
print("what IS judged: a depth case may not be issued without isolation")
UNMET = {"preconditions_met": False, "depth_isolated": False,
         "verdict": "PRECONDITION_UNMET",
         "verdict_note": "depth behaviour cannot be isolated on the real store"}
c, r = pair()
check("an unmet survey outranks everything else", "PRECONDITION_UNMET",
      outcome(c, r, UNMET))
MET_BUT_NOT_ISOLATED = {"preconditions_met": True, "depth_isolated": False}
c, r = pair(isolated=True)
check("a record claiming isolation the survey denies is INVALID_EVIDENCE",
      "INVALID_EVIDENCE", outcome(c, r, MET_BUT_NOT_ISOLATED))
c, r = pair(isolated=False)
check("and the OOR case issued anyway is caught", "INVALID_EVIDENCE",
      outcome(c, r, MET_BUT_NOT_ISOLATED))
c = [rec("D1-DEPTH-SUP", "candidate", isolated=False)]
r = [rec("D1-DEPTH-SUP", "reference", isolated=False)]
check("but the supported case does not need isolation",
      "CHARACTERIZATION_RECORDED", outcome(c, r, MET_BUT_NOT_ISOLATED))

print()
print("an incomplete record is INVALID_EVIDENCE, never 'mostly fine'")
for field, _typ in REQUIRED_FIELDS:
    c = [rec("D1-DEPTH-OOR", "candidate")]
    del c[0][field]
    problems = validate(c, "candidate")
    if not any(field in p for p in problems):
        FAIL += 1
        print(f"  FAIL a record missing {field!r} was accepted")
    else:
        PASS += 1
print(f"  ok   every one of the {len(REQUIRED_FIELDS)} required fields is checked")
c = [rec("D1-DEPTH-OOR", "candidate")]
c[0]["http_status"] = "200"
check("a string status is rejected", True,
      any("http_status" in p for p in validate(c, "candidate")))
c = [rec("D1-DEPTH-OOR", "candidate")]
c[0]["arm"] = "reference"
check("a record labelled for the other arm is rejected", True,
      any("labelled" in p for p in validate(c, "candidate")))

print()
print("the outcomes have distinct exit codes, and the success one is 0")
check("CHARACTERIZATION_RECORDED exits 0", 0, OUTCOMES["CHARACTERIZATION_RECORDED"])
check("PRECONDITION_UNMET has its own code, not FAIL's", 3,
      OUTCOMES["PRECONDITION_UNMET"])
check("DIVERGENCE is non-zero", True, OUTCOMES["DIVERGENCE"] != 0)
check("REQUEST_LEVEL_VIOLATION is non-zero", True,
      OUTCOMES["REQUEST_LEVEL_VIOLATION"] != 0)

print()
print("end to end through the CLI, on files")
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    cand = [rec("D1-DEPTH-SUP", "candidate", status=200, body=b'[{"a":1}]', rows=1),
            rec("D1-ANCHOR-RECOVER", "candidate"),
            rec("D1-DEPTH-OOR", "candidate", status=200, body=b"[]", rows=0),
            rec("D1-ANCHOR-RECOVER", "candidate")]
    ref = [rec(r_["case_id"], "reference", status=r_["http_status"],
               body=b'[{"a":1}]' if r_["case_id"] == "D1-DEPTH-SUP" else b"[]",
               rows=r_["row_count"]) for r_ in cand]
    (d / "cand.jsonl").write_text("".join(json.dumps(x) + "\n" for x in cand))
    (d / "ref.jsonl").write_text("".join(json.dumps(x) + "\n" for x in ref))
    (d / "survey.json").write_text(json.dumps(GOOD_SURVEY))

    result = build(d / "cand.jsonl", d / "ref.jsonl", d / "survey.json")
    check("the outcome is recorded", "CHARACTERIZATION_RECORDED", result["outcome"])
    check("both arms' records are counted", (4, 4),
          (result["n_candidate_records"], result["n_reference_records"]))
    check("the observations are carried, not just the verdict", 8,
          len(result["observations"]))
    check("it says out loud that nothing was judged", True,
          "judged right or wrong" in result["not_judged"])
    check("JSON and CSV are never compared with each other", True,
          "never compared with each other" in result["not_judged"])
    check("the readiness counting gap travels with the result", True,
          any("not counted" in x for x in result["limitations"]))
    check("and so does the coordinate-chunk distinction", True,
          any("coordinate chunks" in x for x in result["limitations"]))
    check("and that no latency was measured", True,
          any("latency" in x for x in result["limitations"]))

    out = d / "d1.json"
    r = subprocess.run(
        [sys.executable, "-m", "bench.d1_evidence", "--candidate", str(d / "cand.jsonl"),
         "--reference", str(d / "ref.jsonl"), "--survey", str(d / "survey.json"),
         "--out", str(out)], capture_output=True, text=True, cwd=str(REPO))
    check("the CLI exits 0", 0, r.returncode)
    check("and wrote the artefact", True, out.exists())
    check("the printed output shows what was observed", True,
          "what was observed (not judged)" in r.stdout)

    # A broken line is a problem, not a line to skip.
    (d / "cand.jsonl").write_text('{"case_id": "x"\n')
    result = build(d / "cand.jsonl", d / "ref.jsonl", d / "survey.json")
    check("a malformed record file is INVALID_EVIDENCE", "INVALID_EVIDENCE",
          result["outcome"])

    (d / "survey.json").write_text("not json")
    result = build(d / "ref.jsonl", d / "ref.jsonl", d / "survey.json")
    check("an unreadable survey fails closed to PRECONDITION_UNMET",
          "PRECONDITION_UNMET", result["outcome"])

print()
raise SystemExit(summary(PASS, FAIL))