"""The symmetric warm-up pass, against real loopback servers.

The pass exists because the candidate reaches its first request having read the
anchor group's metadata and the reference has not. If it warmed a different case set
from the one about to be measured, or if its requests went uncounted, it would be
doing the opposite of its job — so those are what these check, over real HTTP.

    uv run python -m bench.test_symmetric_warmup
"""

import json
import subprocess
import sys
import tempfile
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.queries import select                                  # noqa: E402
from bench.symmetric_warmup import (                              # noqa: E402
    ENDPOINT, ORDERS, request_count_per_arm, run,
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


def make_server(seen: list, status: int = 200):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            seen.append(self.path)
            body = b"[]"
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


print("the pass covers every case the gate will measure, on both arms")
cand_seen, ref_seen = [], []
csrv, curl = make_server(cand_seen)
rsrv, rurl = make_server(ref_seen)
try:
    result = run(curl, rurl, include_heavy=True)
    expected_cases = [q.id for q in select(None, True)]
    check("eight cases with --include-heavy", 8, len(expected_cases))
    check("sixteen requests to the candidate", 16, len(cand_seen))
    check("sixteen to the reference", 16, len(ref_seen))
    check("which is what the budget expects", 16,
          request_count_per_arm(len(expected_cases)))
    check("and what the record reports", {"candidate": 16, "reference": 16},
          result["requests_per_arm"])
    check("the record agrees with its own expectation", True,
          result["counts_match_expected"])

    for seen, arm in ((cand_seen, "candidate"), (ref_seen, "reference")):
        check(f"{arm}: every request went to the data endpoint", True,
              all(p.startswith(ENDPOINT + "?") for p in seen))
    check("both orders were used", {"RC", "CR"},
          {s["order"] for s in result["statuses"]})
    per_case = Counter((s["case"], s["arm"]) for s in result["statuses"])
    check("each case hit each arm exactly twice", {2},
          set(per_case.values()))
    check("and that is 8 cases x 2 arms x 2 orders", 32, sum(per_case.values()))
finally:
    csrv.shutdown(); rsrv.shutdown()

print()
print("nothing here is timed, and nothing enters a statistic")
check("the record says so", False, result["enters_statistics"])
check("no duration field exists anywhere in it", [],
      [k for k in result if "second" in k or "duration" in k or "latency" in k])
check("and it explains why", True, "decided not to trust" in result["not_timed"])
check("it names the other warm-up it is not", True,
      "per-case leading sample" in result["note"])

print()
print("a failing arm is still an attempt, and is still recorded")
cand_seen2, ref_seen2 = [], []
csrv2, curl2 = make_server(cand_seen2)
rsrv2, rurl2 = make_server(ref_seen2, status=500)
try:
    result2 = run(curl2, rurl2, include_heavy=True)
    check("the pass still issued its full count", {"candidate": 16, "reference": 16},
          result2["requests_per_arm"])
    check("the failing arm's statuses are recorded", {500},
          {s["http_status"] for s in result2["statuses"] if s["arm"] == "reference"})
    check("and the healthy arm's", {200},
          {s["http_status"] for s in result2["statuses"] if s["arm"] == "candidate"})
finally:
    csrv2.shutdown(); rsrv2.shutdown()

# A dead backend: the attempt happened, and the count must reflect it.
result3 = run("http://127.0.0.1:1", "http://127.0.0.1:1", queries=select(None, False))
check("a refused connection still counts as issued", True,
      all(v == request_count_per_arm(len(select(None, False)))
          for v in result3["requests_per_arm"].values()))
check("and is recorded as a transport error, not a status", {0},
      {s["http_status"] for s in result3["statuses"]})

print()
print("the CLI writes the artefact and reports the count")
with tempfile.TemporaryDirectory() as td:
    cand_seen3, ref_seen3 = [], []
    c3, cu3 = make_server(cand_seen3)
    r3, ru3 = make_server(ref_seen3)
    try:
        out = Path(td) / "warmup.json"
        proc = subprocess.run(
            [sys.executable, "-m", "bench.symmetric_warmup", "--candidate", cu3,
             "--reference", ru3, "--include-heavy", "--out", str(out)],
            capture_output=True, text=True, cwd=str(REPO))
        check("exit 0", 0, proc.returncode)
        check("the artefact was written", True, out.exists())
        rec = json.loads(out.read_text())
        check("with the per-arm counts", {"candidate": 16, "reference": 16},
              rec["requests_per_arm"])
        check("and the output says they are discarded", True,
              "discarded" in proc.stdout)
    finally:
        c3.shutdown(); r3.shutdown()

print()
print("FAIL CLOSED: an arm that did not answer 200 was not warmed")
# The defect: `main()` returned 0 whenever the COUNT was right, so a warm-up whose
# every response was a 500 — or refused, or timed out — reported success and the
# chain went on to measure latency against an arm that had never served a request.
for label, status, expect_transport in (("500s", 500, False),
                                        ("404s", 404, False)):
    cs, cu = make_server([], status=200)
    rs, ru = make_server([], status=status)
    try:
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "w.json"
            jr = Path(td) / "j" / "symmetric_warmup.jsonl"
            proc = subprocess.run(
                [sys.executable, "-m", "bench.symmetric_warmup", "--candidate", cu,
                 "--reference", ru, "--include-heavy", "--request-log", str(jr),
                 "--out", str(out)], capture_output=True, text=True, cwd=str(REPO))
            check(f"{label}: the warm-up exits non-zero", True, proc.returncode != 0)
            check(f"{label}: and says the latency gate must not run", True,
                  "latency gate must not run" in proc.stderr)
            check(f"{label}: the artefact was still written", True, out.exists())
            doc = json.loads(out.read_text())
            check(f"{label}: marked not complete", False, doc["complete"])
            check(f"{label}: the unusable responses are recorded", 16,
                  doc["unusable_per_arm"]["reference"])
            check(f"{label}: and the healthy arm is not blamed", 0,
                  doc["unusable_per_arm"]["candidate"])
            check(f"{label}: every attempt was still issued", 16,
                  doc["requests_per_arm"]["reference"])
            check(f"{label}: and journalled", True, jr.exists())
    finally:
        cs.shutdown(); rs.shutdown()

# A refused connection: recorded as status 0 with a transport error.
with tempfile.TemporaryDirectory() as td:
    cs, cu = make_server([])
    try:
        out = Path(td) / "w.json"
        proc = subprocess.run(
            [sys.executable, "-m", "bench.symmetric_warmup", "--candidate", cu,
             "--reference", "http://127.0.0.1:1", "--include-heavy",
             "--out", str(out)], capture_output=True, text=True, cwd=str(REPO))
        check("a refused arm fails the warm-up too", True, proc.returncode != 0)
        doc = json.loads(out.read_text())
        check("marked not complete", False, doc["complete"])
        check("and the transport errors are what made it so", 16,
              sum(1 for st in doc["unusable_responses"]
                  if st["transport_error"] is not None))
    finally:
        cs.shutdown()

# The healthy case must still pass, or the guard would simply block everything.
cs2, cu2 = make_server([])
rs2, ru2 = make_server([])
try:
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "w.json"
        proc = subprocess.run(
            [sys.executable, "-m", "bench.symmetric_warmup", "--candidate", cu2,
             "--reference", ru2, "--include-heavy", "--out", str(out)],
            capture_output=True, text=True, cwd=str(REPO))
        check("two healthy arms still pass", 0, proc.returncode)
        check("and the record says complete", True,
              json.loads(out.read_text())["complete"])
finally:
    cs2.shutdown(); rs2.shutdown()

print()
print("the runner budgets exactly what this issues, and records what it issued")
runner = ((REPO / "scripts" / "run_controlled.sh").read_text()
          + (REPO / "scripts" / "lib_s2perf.sh").read_text())
check("BUDGET_WARMUP is 16", True, "BUDGET_WARMUP=16" in runner)
check("the module and the runner agree", 16, request_count_per_arm(8))
# The budget is the ceiling the run is authorised against; the COUNT comes from this
# module's own record, so a pass whose last requests were refused still reports them.
check("the recorded count is measured, not the constant", False,
      'symmetric_warmup "$BUDGET_WARMUP"' in runner)
check("it is read back from this artefact", True,
      "bench.perf_counts" in runner
      and "for stage in symmetric_warmup latency noise_pilot" in runner)
check("the runner passes --include-heavy, so the sets match", True,
      "bench.symmetric_warmup" in runner and "--include-heavy" in runner)

print()
raise SystemExit(summary(PASS, FAIL))