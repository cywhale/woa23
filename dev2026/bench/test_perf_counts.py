"""What `bench.perf_counts` reports is what the artefacts say was issued.

The module exists because the runner used to record budget constants as traffic. So
the assertions here are mostly about the difference between the two: a stage that
stopped early must report fewer, a stage that visited every case must report all of
them, and a bootstrap round must never turn into a request.

    uv run python -m bench.test_perf_counts
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.paired_stats import WARMUP_REQUESTS                      # noqa: E402
from bench.perf_counts import (                                     # noqa: E402
    contract_exact, counts, latency_counts, pilot_counts, warmup_counts,
)

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


def write(d: Path, name: str, doc) -> None:
    (d / name).write_text(json.dumps(doc))


def full_run(d: Path, label="s2p", *, cases=8, n=21, pilot_warm=25,
             warmup_per_arm=16, pilot_cases=None, contract_errors=0) -> None:
    """The artefacts a complete rung-21 pass would leave behind."""
    # The contract gate's own artefact. It is not counted here — the shell owns that
    # number — but its verdicts say whether the derived count describes what was
    # issued, so a complete pass has to have one.
    write(d, f"{label}_contract.json", {"gate": "PASS", "results": [
        {"id": f"C{i}", "verdict": "ERROR" if i < contract_errors else "MATCH"}
        for i in range(64)
    ]})
    write(d, f"{label}_symmetric_warmup.json", {
        "requests_per_arm": {"candidate": warmup_per_arm, "reference": warmup_per_arm},
        "expected_per_arm": warmup_per_arm,
        "counts_match_expected": True,
    })
    # RAW, as `paired_bench` writes it: n measured plus the discarded leading one.
    raw = n + WARMUP_REQUESTS
    write(d, f"{label}_paired.json", {"results": [
        {"id": f"C{i}",
         "samples_ms": {"candidate": [1.0] * raw, "reference": [1.0] * raw}}
        for i in range(cases)
    ]})
    for arm in ("candidate", "reference"):
        write(d, f"{label}_noise_pilot_{arm}.json", {"results": [
            {"id": f"C{i}", "samples_ms": [1.0] * (pilot_warm + WARMUP_REQUESTS)}
            for i in range(pilot_cases if pilot_cases is not None else cases)
        ]})


print("a complete rung-21 pass counts what spec 007 section 5.4.1 budgets")
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)
    r = counts(d, "s2p")
    check("no problems", [], r["problems"])
    check("the warm-up is 16 per arm", 16, r["per_arm"]["candidate"]["symmetric_warmup"])
    check("the latency gate is 8 x (1 + 21) = 176", 176,
          r["per_arm"]["candidate"]["latency"])
    # The discarded leading sample is IN samples_ms. Adding WARMUP_REQUESTS on top
    # counted it twice per case and reported 184 for a run that issued 176.
    check("and the discarded sample is not counted twice", True,
          r["per_arm"]["candidate"]["latency"] != 176 + 8)
    check("the pilot is 8 x (25 + 1) = 208, not 26", 208,
          r["per_arm"]["candidate"]["noise_pilot"])
    check("and the reference arm matches", r["per_arm"]["candidate"],
          r["per_arm"]["reference"])
    check("the record says these are attempts", True,
          r["counts_attempts_not_successes"])
    check("and that bootstrap rounds are excluded", True,
          "bootstrap" in r["excludes"])
    check("the three stages sum to the budgeted 400", 400,
          sum(r["per_arm"]["candidate"].values()))
    # 400, not 464: the contract stage's 64 is recorded by the shell before the gate
    # runs, and counting it here as well is the double count that reported 528 per
    # arm against an authorised 496.
    check("the contract stage is not among them", False,
          "contract" in r["per_arm"]["candidate"])
    check("but its derived count is confirmed usable", True,
          r["exact_per_stage"]["contract"])
    check("and the record says who owns it", True,
          "record_contract_count" in r["contract_note"])

print()
print("the pilot's per-case figure is not its per-arm figure")
# This is the arithmetic the budget constant originally got wrong. One case would
# have been 26; eight cases are 208, and no reading of the artefact yields 26.
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d, pilot_cases=1)
    check("one case is 26", {"candidate": 26, "reference": 26}, pilot_counts(d, "s2p")["per_arm"])
    full_run(d, pilot_cases=8)
    check("eight cases are 208", {"candidate": 208, "reference": 208},
          pilot_counts(d, "s2p")["per_arm"])
    check("the discarded leading sample is counted once per case, not once per arm",
          208, pilot_counts(d, "s2p")["per_arm"]["candidate"])

print()
print("a stage that stopped early reports what it issued, not what it planned")
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)
    # The gate aborted after three cases.
    write(d, "s2p_paired.json", {"results": [
        {"id": f"C{i}", "samples_ms": {"candidate": [1.0] * 22, "reference": [1.0] * 22}}
        for i in range(3)
    ]})
    check("three cases, not eight", 66, latency_counts(d, "s2p")["per_arm"]["candidate"])
    check("which is below the 176 ceiling", True,
          latency_counts(d, "s2p")["per_arm"]["candidate"] < 176)
    # A case whose reference samples came up short still counts what it took.
    write(d, "s2p_paired.json", {"results": [
        {"id": "C1", "samples_ms": {"candidate": [1.0] * 22, "reference": [1.0] * 5}}
    ]})
    got = latency_counts(d, "s2p")["per_arm"]
    check("the arms are counted apart", {"candidate": 22, "reference": 5}, got)

print()
print("the pilot is allowed to be absent, and is then zero")
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)
    for arm in ("candidate", "reference"):
        (d / f"s2p_noise_pilot_{arm}.json").unlink()
    r = counts(d, "s2p")
    # Spec 007 section 5.4.3: rung 150 runs no pilot. Absent is a state, not a fault.
    check("zero", {"candidate": 0, "reference": 0},
          {a: r["per_arm"][a]["noise_pilot"] for a in ("candidate", "reference")})
    check("and it is not reported as a problem", [], r["problems"])

print()
print("a missing or malformed artefact is a problem, and the count is still emitted")
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    r = counts(d, "s2p")
    check("nothing is invented", 0, r["per_arm"]["candidate"]["latency"])
    check("the missing artefacts are named", 2,
          len([p for p in r["problems"] if "_paired.json" in p
               or "_symmetric_warmup.json" in p]))

with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)
    (d / "s2p_paired.json").write_text("{ not json")
    check("unparseable is a problem, not a crash", True,
          any("s2p_paired.json" in p for p in counts(d, "s2p")["problems"]))
    check("and it still yields a number to record", 0,
          latency_counts(d, "s2p")["per_arm"]["candidate"])

with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)
    write(d, "s2p_symmetric_warmup.json", {
        "requests_per_arm": {"candidate": 14, "reference": 16},
        "expected_per_arm": 16, "counts_match_expected": False,
    })
    rec_w = warmup_counts(d, "s2p")
    w, probs = rec_w["per_arm"], rec_w["problems"]
    check("a warm-up that disagrees with itself is reported", True,
          any("disagree" in p for p in probs))
    check("but the artefact is still complete, so the count is exact", True,
          rec_w["attempt_count_exact"])
    check("and the measurement itself finished", True,
          rec_w["measurement_complete"])
    check("but the count recorded is the one it issued", 14, w["candidate"])

print()
print("the CLI prints one number for the shell, and says what went wrong on stderr")
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)

    def cli(*args):
        return subprocess.run(
            [sys.executable, "-m", "bench.perf_counts", "--label", "s2p",
             "--results", str(d), *args],
            capture_output=True, text=True, cwd=str(REPO))

    p = cli("--stage", "noise_pilot", "--arm", "candidate")
    check("exit 0", 0, p.returncode)
    check("one bare integer on stdout", "208", p.stdout.strip())
    check("nothing else on stdout", 1, len(p.stdout.strip().splitlines()))
    check("--stage without --arm is exit 2", 2,
          cli("--stage", "latency").returncode)
    whole = cli()
    check("the whole record is valid JSON", "perf_stage_counts",
          json.loads(whole.stdout)["kind"])
    check("and a clean record exits 0", 0, whole.returncode)

    (d / "s2p_paired.json").unlink()
    bad = cli("--stage", "latency", "--arm", "candidate")
    check("a problem still prints the count on stdout", "0", bad.stdout.strip())
    check("with the problem on stderr", True, "perf_counts:" in bad.stderr)
    check("so the shell records a number and the run's gate decides", 0, bad.returncode)
    check("the whole record exits 1 when something is wrong", 1, cli().returncode)

print()
print("a contract gate with an ERROR case may not report its derived count as issued")
# `contract_diff` records a case whose request raised as ERROR and moves on. Under RC
# order that case never issued the candidate's request at all, so len(all_cases()) is
# an upper bound on attempts, not a count of them.
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)
    check("a clean gate confirms the derived count", (True, []),
          contract_exact(d, "s2p"))

    full_run(d, contract_errors=3)
    ok, probs = contract_exact(d, "s2p")
    check("three ERROR cases do not", False, ok)
    check("and it says why, in the direction it errs", True,
          any("UPPER BOUND" in p for p in probs))
    check("naming the cases", True, any("C0" in p for p in probs))
    r = counts(d, "s2p")
    check("the whole record goes inexact", False, r["counts_exact"])
    check("the measured stages are untouched by it", True,
          all(r["exact_per_stage"][k] for k in
              ("symmetric_warmup", "latency", "noise_pilot")))
    check("and the reporting rule applies", True,
          "do NOT present a single exact total" in r["reporting_rule"])

with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)
    (d / "s2p_contract.json").unlink()
    ok, probs = contract_exact(d, "s2p")
    check("no contract artefact at all is also unconfirmable", False, ok)
    check("and is reported rather than assumed fine", 1, len(probs))

print()
print("a stage that did not finish reports a FLOOR, and says so")
from bench.request_log import RequestLog                            # noqa: E402
from bench.suite_summary import summary          # noqa: E402

with tempfile.TemporaryDirectory() as td:
    d = Path(td) / "results"
    d.mkdir()
    j = Path(td) / "journals"
    full_run(d)

    # The latency gate died on its fourth case. Its artefact says so, and the journal
    # holds every attempt it had issued by then — including the one that failed and
    # kept no sample, which is exactly the request the samples cannot show.
    # Three cases finished (3 x 22 x 2 = 132 attempts) and the fourth got eight in
    # before the arm refused: 140 attempts, 70 per arm.
    with RequestLog(j / "latency.jsonl", "latency") as log:
        for i in range(140):
            log.attempt("candidate" if i % 2 else "reference", f"C{i // 44}")
    write(d, "s2p_paired.json", {
        "complete": False,
        "aborted": {"classification": "STAGE_ABORTED_TRANSPORT_FAILURE",
                    "case": "readme_example"},
        "results": [{"id": f"C{i}",
                     "samples_ms": {"candidate": [1.0] * 22, "reference": [1.0] * 22}}
                    for i in range(3)],
    })
    r = counts(d, "s2p", j)
    check("the count comes from the journal, not from the samples", 70,
          r["per_arm"]["candidate"]["latency"])
    check("which is more than the finished cases kept", True,
          r["per_arm"]["candidate"]["latency"] > 3 * 22)
    # A stage that CAUGHT its failure wrote this artefact, so its process was alive
    # to close its journal: the ATTEMPT count is exact even though the MEASUREMENT
    # is not. The two answers are separate, and this is the case that shows why.
    check("the attempt count stays exact", True, r["counts_exact"])
    check("because the writer closed its own journal",
          "journal_writer_exited_normally", r["attempt_evidence"]["latency"])
    check("but the measurement is not complete", False, r["measurement_complete"])
    check("and it is the latency stage that is incomplete",
          {"symmetric_warmup": True, "latency": False, "noise_pilot": True},
          r["measurement_complete_per_stage"])
    check("every stage's attempt count is still exact",
          {"symmetric_warmup": True, "latency": True, "noise_pilot": True,
           "contract": True},
          r["exact_per_stage"])
    check("the abort is named in the problems", True,
          any("STAGE_ABORTED_TRANSPORT_FAILURE" in p for p in r["problems"]))
    # The reporting rule accompanies an INEXACT attempt count. This run's attempt
    # count is exact, so it does not carry one — the rule is about what may be said
    # of a number, not about whether a measurement finished.
    check("no reporting rule, because the count is exact", False,
          "reporting_rule" in r)

    # No artefact at all — the process died before writing one.
    (d / "s2p_paired.json").unlink()
    r = counts(d, "s2p", j)
    check("a missing artefact still yields the journal's floor", 70,
          r["per_arm"]["candidate"]["latency"])
    check("still inexact", False, r["counts_exact"])

    # No journal either: nothing may be claimed, and the shortfall is a problem.
    r = counts(d, "s2p", Path(td) / "nowhere")
    check("with no journal the count is zero", 0, r["per_arm"]["candidate"]["latency"])
    check("and that is reported, not passed off as no traffic", True,
          any("no request journal" in p for p in r["problems"]))
    check("still inexact", False, r["counts_exact"])

with tempfile.TemporaryDirectory() as td:
    d = Path(td) / "results"
    d.mkdir()
    j = Path(td) / "journals"
    full_run(d)
    with RequestLog(j / "noise_pilot.jsonl", "noise_pilot") as log:
        for i in range(40):
            log.attempt("candidate", f"C{i // 5}")
    doc = json.loads((d / "s2p_noise_pilot_candidate.json").read_text())
    doc["complete"] = False
    doc["aborted"] = {"classification": "STAGE_ABORTED_TRANSPORT_FAILURE"}
    write(d, "s2p_noise_pilot_candidate.json", doc)
    r = counts(d, "s2p", j)
    check("a partial pilot falls back to the journal", 40,
          r["per_arm"]["candidate"]["noise_pilot"])
    # A partial pilot artefact means its writer was alive to produce it, so the
    # ATTEMPT count is exact; what is incomplete is the pilot's measurement.
    check("the attempt count stays exact", True, r["counts_exact"])
    check("the pilot's measurement is not complete", False,
          r["measurement_complete_per_stage"]["noise_pilot"])
    check("and the evidence is the writer's own, not a hard kill",
          "journal_writer_exited_normally", r["attempt_evidence"]["noise_pilot"])
    check("the latency stage is untouched by it", True,
          r["exact_per_stage"]["latency"])

print()
print("--exact answers the one question a report has to ask first")
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)

    def ex(*args):
        return subprocess.run(
            [sys.executable, "-m", "bench.perf_counts", "--label", "s2p",
             "--results", str(d), "--exact", *args],
            capture_output=True, text=True, cwd=str(REPO)).stdout.strip()

    check("a complete run is exact", "yes", ex())
    (d / "s2p_paired.json").unlink()
    check("a run missing a stage artefact is not", "no", ex())

print()
print("a tampered artefact cannot inject shell into the record")
# `--emit-shell` output is eval'd by scripts/lib_s2perf.sh. `attempt_evidence` was
# taken from the artefact verbatim, so an artefact containing `clean; some-command`
# produced a line that eval would execute. Artefacts are files this harness READS.
with tempfile.TemporaryDirectory() as td:
    d = Path(td)
    full_run(d)
    write(d, "s2p_paired.json", {
        "complete": False,
        "aborted": {"classification": "STAGE_ABORTED_TRANSPORT_FAILURE"},
        "host_attempt_count_exact": True,
        "attempt_evidence": "clean; echo INJECTED >&2; :",
        "results": [],
    })
    r = counts(d, "s2p", d)
    check("the injected value is not passed on", "unrecognised_evidence_value",
          r["attempt_evidence"]["latency"])
    check("and the artefact's text appears nowhere in the evidence", False,
          any("echo" in v for v in r["attempt_evidence"].values()))
    check("it is reported as a problem, not swallowed", True,
          any("not a value this harness recognises" in p for p in r["problems"]))

    out = subprocess.run(
        [sys.executable, "-m", "bench.perf_counts", "--label", "s2p",
         "--results", str(d), "--journals", str(d), "--emit-shell"],
        capture_output=True, text=True, cwd=str(REPO))
    check("emit-shell exits 0", 0, out.returncode)
    check("and every line it prints is a plain assignment", True,
          all(("=" in ln and ";" not in ln and "$" not in ln and "`" not in ln)
              for ln in out.stdout.splitlines() if ln.strip()))
    check("nothing that could run a command survives", False,
          "echo INJECTED" in out.stdout)

    # And the shell refuses anything that is not an assignment, independently.
    guard = subprocess.run(
        ["bash", "-c",
         'set -u\n'
         'RUN=$(mktemp -d); . scripts/lib_requests.sh; . scripts/lib_s2perf.sh\n'
         'printf "%s\\n" "PERF_X=1" "EVIL=\\$(echo pwned)" | {\n'
         '  while IFS= read -r line; do\n'
         '    case "$line" in\n'
         '      PERF_FAILED_STAGES=\\\'*\\\') ;;\n'
         '      [A-Z_][A-Za-z0-9_]*=[A-Za-z0-9_]*) ;;\n'
         '      *) echo REFUSED; exit 0 ;;\n'
         '    esac\n'
         '  done\n'
         '}\n'],
        capture_output=True, text=True, cwd=str(REPO))
    check("the shell's own guard refuses a non-assignment line", True,
          "REFUSED" in guard.stdout)

print()
print("the harness takes its performance counts from here, not from the constants")
# The runner plus the stage chain it sources: the invocations moved into
# lib_s2perf.sh, and the claim is about both files together.
runner = ((REPO / "scripts" / "run_controlled.sh").read_text()
          + (REPO / "scripts" / "lib_s2perf.sh").read_text())
check("BUDGET_PILOT is the corrected 208", True, "BUDGET_PILOT=208" in runner)
check("and it is documented as per-case x cases", True,
      "8 cases x (25 measured + 1 discarded)" in runner)
for stage in ("symmetric_warmup", "latency", "noise_pilot"):
    check(f"{stage} is no longer recorded from its constant", False,
          f'{stage} "$BUDGET_' in runner)
check("perf_counts is what the runner calls", True, "bench.perf_counts" in runner)
# The contract stage stays derived: it is the one stage with no per-request artefact,
# and lib_requests.sh states the condition under which that is safe.
# Exactly one code location may record the contract stage. Two is what produced
# 528 per arm against an authorised 496, and grepping for the call is the only
# check that catches a second one being added back.
owners = [ln for ln in runner.splitlines()
          if "request_add" in ln and " contract " in ln]
check("exactly one place records the contract stage", 1, len(owners))
check("and it is the named owner", True,
      'request_add "$arm" contract "$n"' in owners[0])
check("which the runner calls once, before the gate", 1,
      runner.count("\nrecord_contract_count "))

print()
raise SystemExit(summary(PASS, FAIL))