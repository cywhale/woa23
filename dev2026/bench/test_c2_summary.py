"""Offline tests for the C2 conclusion.

Every case builds the three cycles' artefacts on disk in a temporary directory, so
the loading, the verdict and both observations are exercised end to end without a
host, a process or a request.

    uv run python -m bench.test_c2_summary
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.c2_summary import (  # noqa: E402
    OUTCOME_EXIT, load_cycle, order_stability, overall_outcome, seed_diversity,
    shutdown_budget, verdict,
)

PASS = 0
FAIL = 0
LABELS = ["c2_cycle1", "c2_cycle2", "c2_cycle3"]


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


def case(cid, ref_order, cand_order, verdict_="MATCH"):
    return {"id": cid, "verdict": verdict_,
            "reference_order": {"body_sha256": "b" * 64, "row_order_sha256": ref_order,
                                "columns": ["lon", "lat"], "n_rows": 2},
            "candidate_order": {"body_sha256": "b" * 64, "row_order_sha256": cand_order,
                                "columns": ["lon", "lat"], "n_rows": 2}}


def write_cycle(root, label, *, gate="PASS", variant="5.2B", seed=None,
                cases=None, skip=(), hashseed_env=None, randomization=1,
                n_strings=11, stop_wait=20, grace=10, arm_grace=None,
                stop_wait_source="default"):
    root.mkdir(parents=True, exist_ok=True)
    if "shutdown_budget" not in skip:
        (root / f"{label}_shutdown_budget.json").write_text(json.dumps({
            "kind": "shutdown_budget", "label": label,
            "arm_graceful_timeout": grace, "stop_wait_secs": stop_wait,
            "stop_wait_source": stop_wait_source, "holds": True}))
    for arm in ("candidate", "reference"):
        if f"meta_{arm}" in skip:
            continue
        # The launch argv is what the summary reads the arms' budget out of, so the
        # fixture carries a real one rather than the number on its own.
        app = "api.app:app" if arm == "candidate" else "woa23_app:app"
        argv = ["python3.11", "-S", "-m", "gunicorn", app, "-w", "2",
                "-k", "uvicorn.workers.UvicornWorker", "--timeout", "120"]
        launched = arm_grace if arm_grace is not None else grace
        if launched is not None:
            argv += ["--graceful-timeout", str(launched)]
        (root / f"{label}_meta_{arm}.json").write_text(json.dumps({
            "kind": "backend_meta", "label": arm, "launch_argv": argv}))
    if "contract" not in skip:
        (root / f"{label}_contract.json").write_text(json.dumps({
            "kind": "contract_diff", "gate": gate, "variant": variant,
            "results": cases if cases is not None else [case("C1", "r1", "c1")]}))
    for arm in ("candidate", "reference"):
        if f"interp_{arm}" in skip:
            continue
        (root / f"{label}_interp_{arm}.json").write_text(json.dumps({
            "label": arm, "seed_digest": seed, "problems": [],
            "hashseed_env": hashseed_env,
            "flags": {"no_site": 1, "ignore_environment": 0,
                      "hash_randomization": randomization},
            "hash_probe": {"strings": ["s"] * n_strings,
                           "hashes": [1] * n_strings}}))
    if "environment" not in skip:
        (root / f"{label}_environment.json").write_text(json.dumps(
            {"kind": "s2_package_clone_environment"}))


def build(root, **per_cycle):
    for i, label in enumerate(LABELS, start=1):
        write_cycle(root, label, **per_cycle.get(f"c{i}", {}))
    return [load_cycle(root, label) for label in LABELS]


print("the verdict is all three cycles, not a majority")
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64},
                   c3={"seed": "c" * 64})
    check("three passing cycles pass", "PASS", verdict(cycles)["gate"])

    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64, "gate": "FAIL"},
                   c3={"seed": "c" * 64})
    v = verdict(cycles)
    check("one failing cycle fails the result", "FAIL", v["gate"])
    check("and the failing cycle is named", True,
          any("c2_cycle2" in p for p in v["problems"]))
    check("the per-cycle gates are kept", ["PASS", "FAIL", "PASS"], v["per_cycle"])

    cycles = build(root, c1={"seed": "a" * 64, "variant": "5.2A"},
                   c2={"seed": "b" * 64}, c3={"seed": "c" * 64})
    v = verdict(cycles)
    check("a cycle compared byte-exact is refused", "FAIL", v["gate"])
    check("and says which variant it found", True,
          any("5.2A" in p for p in v["problems"]))

with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    for i, label in enumerate(LABELS, start=1):
        write_cycle(root, label, seed=chr(96 + i) * 64,
                    skip=("contract",) if i == 2 else ())
    cycles = [load_cycle(root, label) for label in LABELS]
    v = verdict(cycles)
    check("a cycle whose contract file is missing fails closed", "FAIL", v["gate"])
    check("and says the file was never written", True,
          any("never written" in p for p in v["problems"]))


print()
print("seed diversity is observed and reported, never escalated")
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64},
                   c3={"seed": "c" * 64})
    s = seed_diversity(cycles)
    check("three distinct seeds are OBSERVED", "OBSERVED", s["status"])
    check("and counted", 3, s["n_distinct"])

    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "a" * 64},
                   c3={"seed": "c" * 64})
    s = seed_diversity(cycles)
    check("two identical seeds is INSUFFICIENT", "INSUFFICIENT", s["status"])
    check("and it says so is not a candidate failure", True,
          "NOT a failure of the candidate" in s["note"])
    check("and refuses a fourth cycle in writing", True,
          "fourth cycle" in s["note"])

    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "a" * 64},
                   c3={"seed": "a" * 64})
    check("three identical seeds is INSUFFICIENT", "INSUFFICIENT",
          seed_diversity(cycles)["status"])

    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": None},
                   c3={"seed": "c" * 64})
    s = seed_diversity(cycles)
    check("a missing seed is INSUFFICIENT, not two-out-of-three", "INSUFFICIENT",
          s["status"])
    check("and the cycle that did not report one is named", True,
          "c2_cycle2" in s["note"])

    check("the sibling-interpreter limitation travels with the observation", True,
          "NOT of the gunicorn master or worker" in s["limitation"])
    check("and it is labelled sibling/launch-environment diversity", True,
          "SIBLING / LAUNCH-ENVIRONMENT" in s["limitation"])
    check("the measurement method is recorded with the number", True,
          "fixed 11-string tuple" in s["measurement"])


print()
print("the preconditions are checked, and are not themselves the evidence")
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64},
                   c3={"seed": "c" * 64})
    s = seed_diversity(cycles)
    check("clean preconditions raise no problem", [], s["precondition_problems"])
    check("and the statement that they are not evidence is carried", True,
          "not evidence" in s["preconditions_are_not_evidence"].lower())
    check("it names both preconditions", True,
          "PYTHONHASHSEED unset" in s["preconditions_are_not_evidence"]
          and "hash_randomization=1" in s["preconditions_are_not_evidence"])

    # Three DISTINCT digests, but the seed was pinned. Reporting OBSERVED here would
    # attribute variation to an arrangement that was not in force.
    cycles = build(root, c1={"seed": "a" * 64, "hashseed_env": "0"},
                   c2={"seed": "b" * 64}, c3={"seed": "c" * 64})
    s = seed_diversity(cycles)
    check("a pinned seed makes distinct digests INSUFFICIENT", "INSUFFICIENT",
          s["status"])
    check("and says the cycle observed nothing about unpinned behaviour", True,
          any("nothing about unpinned" in p for p in s["precondition_problems"]))
    check("the digests are still counted and reported", 3, s["n_distinct"])

    cycles = build(root, c1={"seed": "a" * 64, "randomization": 0},
                   c2={"seed": "b" * 64}, c3={"seed": "c" * 64})
    s = seed_diversity(cycles)
    check("hash_randomization=0 makes it INSUFFICIENT", "INSUFFICIENT", s["status"])
    check("and says identical digests would say nothing", True,
          any("would say nothing" in p for p in s["precondition_problems"]))

    cycles = build(root, c1={"seed": "a" * 64, "n_strings": 0},
                   c2={"seed": "b" * 64}, c3={"seed": "c" * 64})
    s = seed_diversity(cycles)
    check("an empty probe string set is INSUFFICIENT", "INSUFFICIENT", s["status"])
    check("and says the digest measures nothing", True,
          any("not a measurement" in p for p in s["precondition_problems"]))

    # The preconditions holding is not, on its own, diversity.
    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "a" * 64},
                   c3={"seed": "a" * 64})
    s = seed_diversity(cycles)
    check("clean preconditions with identical digests are still INSUFFICIENT",
          "INSUFFICIENT", s["status"])
    check("and raise no precondition problem, because none is wrong", [],
          s["precondition_problems"])

    # The preconditions as observed are recorded per cycle, for reading later.
    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64},
                   c3={"seed": "c" * 64})
    s = seed_diversity(cycles)
    check("each cycle records the seed variable as observed", [None, None, None],
          [e["hashseed_env"] for e in s["per_cycle"]])
    check("and the randomization flag as observed", [1, 1, 1],
          [e["hash_randomization"] for e in s["per_cycle"]])


print()
print("the two results combine into one named outcome, never into 'it passed'")
with tempfile.TemporaryDirectory() as td:
    root = Path(td)

    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64},
                   c3={"seed": "c" * 64})
    out = overall_outcome(verdict(cycles), seed_diversity(cycles))
    check("gates pass + seeds observed is PASS", "PASS", out["outcome"])
    check("and exits 0", 0, out["exit_code"])

    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "a" * 64},
                   c3={"seed": "a" * 64})
    out = overall_outcome(verdict(cycles), seed_diversity(cycles))
    check("gates pass + seeds identical is its own outcome",
          "PASS_WITH_INSUFFICIENT_SEED_DIVERSITY", out["outcome"])
    check("which is not the string PASS", False, out["outcome"] == "PASS")
    # Not exit 0: a caller checking only the status would read it as a plain pass,
    # which is the misreport this outcome exists to prevent. Not exit 1 either: the
    # candidate did not fail.
    check("and does not share PASS's exit code", 5, out["exit_code"])
    check("nor FAIL's", False, out["exit_code"] == OUTCOME_EXIT["FAIL"])
    check("it says it is not a plain PASS", True, "NOT a plain PASS" in out["because"])
    check("nor a candidate failure", True, "NOT a candidate failure" in out["because"])
    check("nor a reason for a fourth cycle", True, "fourth cycle" in out["because"])

    # A broken precondition reaches the same outcome by the same route.
    cycles = build(root, c1={"seed": "a" * 64, "hashseed_env": "0"},
                   c2={"seed": "b" * 64}, c3={"seed": "c" * 64})
    check("a pinned-seed cycle also yields the INSUFFICIENT outcome",
          "PASS_WITH_INSUFFICIENT_SEED_DIVERSITY",
          overall_outcome(verdict(cycles), seed_diversity(cycles))["outcome"])

    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64, "gate": "FAIL"},
                   c3={"seed": "c" * 64})
    out = overall_outcome(verdict(cycles), seed_diversity(cycles))
    check("any failing gate is FAIL", "FAIL", out["outcome"])
    check("and exits 1", 1, out["exit_code"])
    check("the seed observation is not consulted for a failing gate", True,
          "not consulted" in out["because"])

    # A failing gate with identical seeds is still FAIL, not the hybrid.
    cycles = build(root, c1={"seed": "a" * 64}, c2={"seed": "a" * 64, "gate": "FAIL"},
                   c3={"seed": "a" * 64})
    check("a failing gate outranks INSUFFICIENT", "FAIL",
          overall_outcome(verdict(cycles), seed_diversity(cycles))["outcome"])

    check("the three outcomes have three distinct exit codes", 3,
          len(set(OUTCOME_EXIT.values())))


print()
print("order stability is recorded per (case, arm) and never gates")
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    same = [case("C1", "r1", "c1"), case("C2", "r2", "c2")]
    cycles = build(root, c1={"seed": "a" * 64, "cases": same},
                   c2={"seed": "b" * 64, "cases": same},
                   c3={"seed": "c" * 64, "cases": same})
    o = order_stability(cycles)
    check("identical ordering in every cycle is stable", True, o["stable"])
    check("both arms of both cases are comparable", 4, o["n_comparable"])
    check("nothing varied", 0, o["n_varied"])

    moved = [case("C1", "r1", "DIFFERENT"), case("C2", "r2", "c2")]
    cycles = build(root, c1={"seed": "a" * 64, "cases": same},
                   c2={"seed": "b" * 64, "cases": moved},
                   c3={"seed": "c" * 64, "cases": same})
    o = order_stability(cycles)
    check("a changed row order is detected", 1, o["n_varied"])
    check("and attributed to the right case and arm", ["C1/candidate"], o["varied"])
    check("it is still not stable", False, o["stable"])
    # The point of the separation: the verdict is unaffected.
    check("but the verdict is untouched by it", "PASS", verdict(cycles)["gate"])

    orderless = [{"id": "OPENAPI", "verdict": "MATCH",
                  "reference_order": {"body_sha256": "x" * 64,
                                      "row_order_sha256": None, "columns": None,
                                      "n_rows": None},
                  "candidate_order": {"body_sha256": "x" * 64,
                                      "row_order_sha256": None, "columns": None,
                                      "n_rows": None}}]
    cycles = build(root, c1={"seed": "a" * 64, "cases": orderless},
                   c2={"seed": "b" * 64, "cases": orderless},
                   c3={"seed": "c" * 64, "cases": orderless})
    o = order_stability(cycles)
    check("a response with no rows is counted, not called stable", 6,
          o["n_orderless_responses"])
    check("and contributes no comparable pair", 0, o["n_comparable"])
    check("so 'stable' is not claimed on no evidence", False, o["stable"])


print()
print("the CLI refuses anything that is not three distinct cycles")
with tempfile.TemporaryDirectory() as td:
    root = Path(td) / "results"
    build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64}, c3={"seed": "c" * 64})
    repo = str(Path(__file__).resolve().parent.parent)

    def cli(*labels, out=None):
        cmd = [sys.executable, "-m", "bench.c2_summary", "--results", str(root)]
        if out:
            cmd += ["--out", str(out)]
        return subprocess.run(cmd + list(labels), cwd=repo,
                              capture_output=True, text=True)

    r = cli(*LABELS, out=Path(td) / "sum.json")
    check("three passing cycles exit 0", 0, r.returncode)
    payload = json.loads((Path(td) / "sum.json").read_text())
    check("the summary names the outcome at the top level", "PASS", payload["outcome"])
    check("and records the exit code it returned", 0, payload["exit_code"])
    check("the outcome is printed unmissably", True, "C2 OUTCOME: PASS" in r.stdout)
    check("the summary records the verdict", "PASS", payload["contract"]["gate"])
    check("and the observation separately", "OBSERVED",
          payload["seed_diversity"]["status"])
    check("and order stability separately again", True,
          "order_stability" in payload)
    check("and the -S limitation", True,
          "site.py did not run" in payload["site_limitation"])
    check("the printed output keeps the observation out of the gate line", True,
          "not a gate" in r.stdout)

    check("two cycles are refused", 2, cli(*LABELS[:2]).returncode)
    check("four are refused", 2, cli(*LABELS, "c2_cycle4").returncode)
    check("a repeated label is refused", 2,
          cli("c2_cycle1", "c2_cycle1", "c2_cycle3").returncode)
    check("and explains that one cycle overwrote another", True,
          "overwrote" in cli("c2_cycle1", "c2_cycle1", "c2_cycle3").stderr)

    # INSUFFICIENT diversity must not become a non-zero exit: that is exactly the
    # pressure that would push a run towards a fourth cycle.
    build(root, c1={"seed": "a" * 64}, c2={"seed": "a" * 64}, c3={"seed": "a" * 64})
    r = cli(*LABELS, out=Path(td) / "sum2.json")
    check("identical seeds exit 5, not 0", 5, r.returncode)
    check("and name the outcome in full", True,
          "C2 OUTCOME: PASS_WITH_INSUFFICIENT_SEED_DIVERSITY" in r.stdout)
    check("the outcome is in the record too",
          "PASS_WITH_INSUFFICIENT_SEED_DIVERSITY",
          json.loads((Path(td) / "sum2.json").read_text())["outcome"])
    check("the semantic gate is still reported as having passed", "PASS",
          json.loads((Path(td) / "sum2.json").read_text())["contract"]["gate"])

    build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64, "gate": "FAIL"},
          c3={"seed": "c" * 64})
    check("a failed gate exits 1", 1, cli(*LABELS).returncode)


print()
print("the shutdown budget each cycle ran under, read back from its own evidence")
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    SEEDS = {"c1": {"seed": "a" * 64}, "c2": {"seed": "b" * 64},
             "c3": {"seed": "c" * 64}}

    cycles = build(root, **SEEDS)
    b = shutdown_budget(cycles)
    check("three well-configured cycles are CONSISTENT", "CONSISTENT", b["status"])
    check("and the arms' launched value is read from the argv, not the record",
          [10, 10, 10], [e["candidate"] for e in b["per_cycle"]])
    check("the reference is read too", [10, 10, 10],
          [e["reference"] for e in b["per_cycle"]])
    check("and where STOP_WAIT_SECS came from is carried",
          ["default"] * 3, [e["stop_wait_source"] for e in b["per_cycle"]])

    # The C2 cycle-1 shape: the arms take longer than the harness waits. Here it is
    # a configuration that could produce it, caught from the record instead of from
    # a stranded process.
    cycles = build(root, **dict(SEEDS, c2={"seed": "b" * 64, "stop_wait": 20,
                                           "grace": 30}))
    b = shutdown_budget(cycles)
    check("a stop window no longer than the arms' budget is INCONSISTENT",
          "INCONSISTENT", b["status"])
    check("and the cycle is named", True,
          any("c2_cycle2" in p_ for p_ in b["problems"]))
    check("with both numbers in the message", True,
          any("STOP_WAIT_SECS=20" in p_ and "30" in p_ for p_ in b["problems"]))

    cycles = build(root, **dict(SEEDS, c3={"seed": "c" * 64, "grace": None}))
    b = shutdown_budget(cycles)
    check("an arm launched with no --graceful-timeout is INCONSISTENT",
          "INCONSISTENT", b["status"])
    check("and it is not silently read as gunicorn's default", True,
          all(e.get("candidate") != 30 for e in b["per_cycle"]))

    # The record and the process disagreeing is the case that matters most: the
    # budget file says what the run intended, the argv says what it did.
    cycles = build(root, **dict(SEEDS, c1={"seed": "a" * 64, "grace": 10,
                                           "arm_grace": 30}))
    b = shutdown_budget(cycles)
    check("an arm launched with a value the cycle did not record is INCONSISTENT",
          "INCONSISTENT", b["status"])
    check("and the message contrasts the two", True,
          any("30s" in p_ and "10s" in p_ for p_ in b["problems"]))

# A fresh directory: `build` writes into whatever is already there, so skipping a
# file in a reused root leaves the previous case's copy of it in place and the test
# would pass for the wrong reason.
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    for i, label in enumerate(LABELS, start=1):
        write_cycle(root, label, seed=chr(96 + i) * 64,
                    skip=("shutdown_budget",) if i == 2 else ())
    cycles = [load_cycle(root, label) for label in LABELS]
    b = shutdown_budget(cycles)
    check("a cycle with no recorded budget is INCONSISTENT", "INCONSISTENT",
          b["status"])
    check("and the missing artefact also fails the verdict closed", "FAIL",
          verdict(cycles)["gate"])

print()
print("an unconfirmed shutdown budget outranks the gates that ran inside it")
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    SEEDS = {"c1": {"seed": "a" * 64}, "c2": {"seed": "b" * 64},
             "c3": {"seed": "c" * 64}}

    cycles = build(root, **SEEDS)
    v, sd, ok = verdict(cycles), seed_diversity(cycles), shutdown_budget(cycles)
    check("a clean run is unaffected by the new input", "PASS",
          overall_outcome(v, sd, ok)["outcome"])
    check("and the budget status is carried in the outcome", "CONSISTENT",
          overall_outcome(v, sd, ok)["shutdown_budget"])

    bad = {"status": "INCONSISTENT", "per_cycle": [], "problems": ["x"]}
    o = overall_outcome(v, sd, bad)
    check("three passing gates do not survive an unconfirmed budget", "FAIL",
          o["outcome"])
    check("and it exits non-zero", 1, o["exit_code"])
    check("the reason names the configuration, not the contract", True,
          "authorised" in o["because"])
    check("the semantic gate is still reported as having passed", "PASS",
          o["contract_gate"])

    # Downward only. There is no arrangement of budget records that turns a failing
    # contract into a pass.
    vfail = dict(v, gate="FAIL")
    check("a consistent budget cannot rescue a failed gate", "FAIL",
          overall_outcome(vfail, sd, ok)["outcome"])
    check("omitting the argument keeps the old two-input behaviour", "PASS",
          overall_outcome(v, sd)["outcome"])
    check("and records no budget status when none was supplied", None,
          overall_outcome(v, sd)["shutdown_budget"])


print()
if FAIL:
    print(f"FAILED {FAIL}/{PASS + FAIL}")
    raise SystemExit(1)
print(f"all passed ({PASS} assertions)")
