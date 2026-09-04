"""The request journal, and what the two sampling stages leave behind when they die.

The journal exists because `paired_bench` and `noise_pilot` used to end a refused or
timed-out stage with no artefact at all, making the requests they had already issued
uncountable. So the assertions here are about the failure, not the success: what is
on disk when the arm goes away mid-stage, and whether the run can still say what it
put on the host.

Both stages are driven over real HTTP against a stand-in that refuses, hangs, or
stops answering partway.

    uv run python -m bench.test_request_log
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.queries import select                                    # noqa: E402
from bench.request_log import (                                     # noqa: E402
    RequestLog, integrity, read_counts,
)

PASS = 0
FAIL = 0
REPO = Path(__file__).resolve().parent.parent

STUB = """
import sys
import {mod} as m
for _n in ("validate_meta", "validate_store_agreement",
           "verify_group_path_agreement", "post_run_runtime_check"):
    if hasattr(m, _n):
        setattr(m, _n, lambda *a, **k: [])
if hasattr(m, "load_meta"):
    m.load_meta = lambda path, label: ({{"stub_provenance": True}}, [])
sys.argv = ["{mod}"] + sys.argv[1:]
raise SystemExit(m.main())
"""


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


def stubbed(module, *args, **kw):
    return subprocess.run([sys.executable, "-c", STUB.format(mod=module), *args],
                          capture_output=True, text=True, cwd=str(REPO), **kw)


def serve(die_after=None, delay=0.0):
    """A stand-in that can stop answering after N requests, or answer too slowly."""
    state = {"n": 0}

    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            state["n"] += 1
            if die_after is not None and state["n"] > die_after:
                # The socket closes with no response: what a backend that has just
                # died looks like from the client side.
                self.close_connection = True
                self.wfile.close()
                return
            if delay:
                time.sleep(delay)
            body = b"[]"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}", state


print("the journal records the attempt before the request, not the result")
with tempfile.TemporaryDirectory() as td:
    j = Path(td) / "sub" / "latency.jsonl"
    with RequestLog(j, "latency") as log:
        log.attempt("candidate", "readme_example")
        log.attempt("reference", "readme_example")
        # Readable from another process while this one is still writing: that is the
        # whole point — the writer may never get to close the file.
        mid, _ = read_counts(j)
        check("visible on disk before the stage ends", 1, mid["candidate"])
        log.attempt("candidate", "point_profile")
    got, probs = read_counts(j)
    check("every attempt counted, per arm", {"candidate": 2, "reference": 1}, got)
    check("no problems", [], probs)
    check("the parent directory was created", True, j.parent.is_dir())
    lines = [json.loads(x) for x in j.read_text().splitlines()]
    check("each line names its stage, arm and case", True,
          all({"stage", "arm", "case", "seq"} <= set(x) for x in lines))
    check("and its sequence number", [1, 2, 3], [x["seq"] for x in lines])

    # A second stage appends rather than truncating a journal it did not write.
    with RequestLog(j, "latency") as log2:
        log2.attempt("reference", "x")
    check("a second writer appends", 4, sum(read_counts(j)[0].values()))

with tempfile.TemporaryDirectory() as td:
    j = Path(td) / "latency.jsonl"
    j.write_text('{"stage":"latency","arm":"candidate","case":"a","seq":1}\n'
                 '{"stage":"latency","arm":"candi')          # killed mid-write
    got, probs = read_counts(j)
    check("a truncated last line does not lose the whole journal", 1, got["candidate"])
    check("and is reported rather than silently dropped", True,
          any("truncated" in p for p in probs))
    check("no journal at all is a problem, not a zero", True,
          any("no request journal" in p for p in read_counts(Path(td) / "none")[1]))

print()
print("a journal is optional: nothing breaks when the runner does not pass one")
log = RequestLog(None, "latency")
log.attempt("candidate", "x")
log.close()
check("no file, no error", True, True)

print()
print("the latency gate keeps a partial artefact when an arm goes away")
srv, url, state = serve(die_after=30)
alive_srv, alive_url, _ = serve()
try:
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "paired.json"
        jr = Path(td) / "journals" / "latency.jsonl"
        r = stubbed("bench.paired_bench", "--candidate", url,
                    "--reference", alive_url, "--gate-variant", "5.2A",
                    "--warm", "21", "--include-heavy", "--margin", "0.05",
                    "--pause", "0", "--request-log", str(jr), "--out", str(out))
        check("the stage exits non-zero", True, r.returncode != 0)
        check("it says what happened", True,
              "STAGE_ABORTED_TRANSPORT_FAILURE" in r.stdout)
        check("the artefact exists", True, out.exists())
        doc = json.loads(out.read_text())
        check("marked incomplete", False, doc["complete"])
        check("with no gate verdict invented from the cases that finished",
              "INVALID_TRANSPORT_FAILURE", doc["gate"])
        check("the classification names the stage and the case", "latency",
              doc["aborted"]["stage"])
        check("and the transport error", True,
              "Error" in doc["aborted"]["error"] or "Timeout" in doc["aborted"]["error"]
              or "Remote" in doc["aborted"]["error"])
        check("the artefact refuses an exact total", False, doc["request_total_exact"])
        # But its ATTEMPT count is exact: this process caught the failure and closed
        # its own journal. That is a different evidence strength from a hard kill.
        check("while its attempt count is exact", True,
              doc["host_attempt_count_exact"])
        check("and says which evidence that is", "journal_writer_exited_normally",
              doc["attempt_evidence"])
        check("and says what may be reported instead", True,
              "authorised ceiling" in doc["reporting_rule"])
        check("and that it is not a latency result", True,
              "NOT a latency result" in doc["aborted"]["not"])

        journal, _ = read_counts(jr)
        check("the journal recorded every attempt the server saw",
              state["n"], journal["candidate"])
        check("which is more than the artefact's samples can show", True,
              journal["candidate"] >
              sum(len(x["samples_ms"]["candidate"]) for x in doc["results"]))
        check("the artefact reports the journal's attempt count", journal,
              doc["journaled_attempts_per_arm"])
finally:
    srv.shutdown(); srv.server_close()
    alive_srv.shutdown(); alive_srv.server_close()

print()
print("the noise pilot does the same")
srv, url, state = serve(die_after=40)
try:
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "pilot.json"
        jr = Path(td) / "journals" / "noise_pilot.jsonl"
        r = subprocess.run(
            [sys.executable, "-m", "bench.noise_pilot", "--base-url", url,
             "--warm", "25", "--pause", "0", "--arm", "candidate",
             "--request-log", str(jr), "--out", str(out)],
            capture_output=True, text=True, cwd=str(REPO))
        check("the stage exits non-zero", True, r.returncode != 0)
        check("the artefact exists", True, out.exists())
        doc = json.loads(out.read_text())
        check("marked incomplete", False, doc["complete"])
        check("and inexact", False, doc["request_total_exact"])
        check("it names the arm it was sampling", "candidate", doc["arm"])
        check("the classification is the same one", "STAGE_ABORTED_TRANSPORT_FAILURE",
              doc["aborted"]["classification"])
        check("and it refuses to be read as a noise floor", True,
              "NOT a noise floor" in doc["aborted"]["not"])
        journal, _ = read_counts(jr)
        check("the journal has every attempt the server saw", state["n"],
              journal["candidate"])
        check("recorded against the arm it was told it was sampling", 0,
              journal["reference"])
finally:
    srv.shutdown(); srv.server_close()

print()
print("a process killed outright still leaves its attempts on disk")
srv, url, state = serve(delay=0.05)
try:
    with tempfile.TemporaryDirectory() as td:
        jr = Path(td) / "journals" / "noise_pilot.jsonl"
        proc = subprocess.Popen(
            [sys.executable, "-m", "bench.noise_pilot", "--base-url", url,
             "--warm", "25", "--pause", "0", "--request-log", str(jr),
             "--out", str(Path(td) / "pilot.json")],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=str(REPO))
        # Long enough to be well into sampling, then killed with no chance to write
        # an artefact — the case the journal exists for.
        time.sleep(3.0)
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)
        check("no artefact was written", False, (Path(td) / "pilot.json").exists())
        journal, probs = read_counts(jr)
        check("but the attempts are on disk", True, journal["candidate"] > 0)
        check("and they match what the server was asked", state["n"],
              journal["candidate"] + journal["reference"])
finally:
    srv.shutdown(); srv.server_close()

print()
print("what a journal count IS, and what may not be claimed from it")
# The line is written BEFORE the call, which is what makes the journal useful and is
# also why its count is not a bound once the writer is killed asynchronously.
with tempfile.TemporaryDirectory() as td:
    j = Path(td) / "noise_pilot.jsonl"

    # (a) A record whose request was never sent. On disk it is indistinguishable
    # from one whose request completed — which is the whole point: the file cannot
    # tell them apart, so no bound may be derived from it.
    with RequestLog(j, "noise_pilot") as log:
        for _ in range(5):
            log.attempt("reference", "point_profile")
        log.attempt("reference", "point_profile")     # flushed; call never made
    got, probs = read_counts(j)
    check("the unsent attempt is in the file like any other", 6, got["reference"])
    check("nothing marks it as unsent, because nothing can", [], probs)

    # (b) A truncated final record: present, unattributable, NOT counted.
    with j.open("a") as fh:
        fh.write('{"stage":"noise_pilot","arm":"refer')
    got2, probs2 = read_counts(j)
    check("the truncated record is not counted", 6, got2["reference"])
    check("and it is reported rather than dropped", True,
          any("truncated journal record" in p for p in probs2))
    check("the message says the count does not include it", True,
          any("does not include it" in p for p in probs2))
    integ = integrity(j)
    check("integrity counts the attributable records", 6,
          integ["attributable_records"])
    check("and the truncated one separately", 1, integ["truncated_records"])

    # So the same file can be one too high (a) and one too low (b) at once. Neither
    # direction is bounded, which is why the wording matters more than the number.
    check("a journal can over- and under-count at the same time", True,
          integ["attributable_records"] == 6 and integ["truncated_records"] == 1)

print()
print("the two evidence strengths are not interchangeable")
from bench.perf_counts import (                                     # noqa: E402
    EVIDENCE_JOURNAL_ABRUPT, EVIDENCE_JOURNAL_CLEAN, counts as perf_counts_of,
)
from bench.suite_summary import summary          # noqa: E402

with tempfile.TemporaryDirectory() as td:
    res, jr = Path(td) / "results", Path(td) / "journals"
    res.mkdir()
    for name, doc in (
        ("k_symmetric_warmup.json", {"requests_per_arm": {"candidate": 16,
                                                          "reference": 16},
                                     "expected_per_arm": 16,
                                     "counts_match_expected": True}),
        ("k_contract.json", {"gate": "PASS",
                             "results": [{"id": "C1", "verdict": "MATCH"}]}),
    ):
        (res / name).write_text(json.dumps(doc))
    # A stage that CAUGHT its own failure: it wrote the artefact, so its process was
    # alive to close the journal.
    (res / "k_paired.json").write_text(json.dumps({
        "complete": False,
        "aborted": {"classification": "STAGE_ABORTED_TRANSPORT_FAILURE"},
        "results": [{"id": "C1", "samples_ms": {"candidate": [1.0], "reference": [1.0]}}],
    }))
    with RequestLog(jr / "latency.jsonl", "latency") as log:
        for i in range(10):
            log.attempt("candidate" if i % 2 else "reference", "C1")

    rec = perf_counts_of(res, "k", jr)
    check("a caught failure is journal_writer_exited_normally",
          EVIDENCE_JOURNAL_CLEAN, rec["attempt_evidence"]["latency"])
    check("a stage the caller watched die is not", EVIDENCE_JOURNAL_ABRUPT,
          perf_counts_of(res, "k", jr, ("noise_pilot",))
          ["attempt_evidence"]["noise_pilot"])
    # The attempt count IS exact here — the writer closed its own journal. What is
    # not complete is the measurement, and those are separate answers.
    check("the attempt count is exact", True, rec["host_attempt_count_exact"])
    check("while the measurement is not complete", False,
          rec["measurement_complete"])
    check("and a hard kill is the case that is not exact", False,
          perf_counts_of(res, "k", jr, ("noise_pilot",))["host_attempt_count_exact"])
    check("and carries the journal's own shape", 10,
          rec["journal_integrity"]["latency"]["attributable_records"])

    # The reporting rule accompanies an INEXACT attempt count, so it belongs to the
    # hard-kill record rather than to this one, whose count is exact.
    killed_rec = perf_counts_of(res, "k", jr, ("noise_pilot",))
    check("the exact record carries no reporting rule", False,
          "reporting_rule" in rec)
    rule = killed_rec["reporting_rule"]
    for banned in ("floor", "at least", "actually issued"):
        check(f"the reporting rule does not say {banned!r}", False, banned in rule)
    check("it says what to report instead", True,
          "host_attempt_count_exact" in rule and "ceiling" in rule)
    check("and that an abrupt kill bounds nothing", True,
          "NOT a bound in either direction" in rule)

print()
print("a stage that finishes is unaffected: same counts, marked exact")
c_srv, c_url, c_state = serve()
r_srv, r_url, r_state = serve()
try:
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "warm.json"
        jr = Path(td) / "journals" / "symmetric_warmup.jsonl"
        r = subprocess.run(
            [sys.executable, "-m", "bench.symmetric_warmup", "--candidate", c_url,
             "--reference", r_url, "--include-heavy", "--request-log", str(jr),
             "--out", str(out)], capture_output=True, text=True, cwd=str(REPO))
        check("exit 0", 0, r.returncode)
        doc = json.loads(out.read_text())
        journal, _ = read_counts(jr)
        check("the journal and the artefact agree", doc["requests_per_arm"], journal)
        check("and both agree with the server", c_state["n"], journal["candidate"])
finally:
    c_srv.shutdown(); c_srv.server_close()
    r_srv.shutdown(); r_srv.server_close()

print()
raise SystemExit(summary(PASS, FAIL))