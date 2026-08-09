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
    load_cycle, order_stability, seed_diversity, verdict,
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
                cases=None, skip=()):
    root.mkdir(parents=True, exist_ok=True)
    if "contract" not in skip:
        (root / f"{label}_contract.json").write_text(json.dumps({
            "kind": "contract_diff", "gate": gate, "variant": variant,
            "results": cases if cases is not None else [case("C1", "r1", "c1")]}))
    for arm in ("candidate", "reference"):
        if f"interp_{arm}" in skip:
            continue
        (root / f"{label}_interp_{arm}.json").write_text(json.dumps({
            "label": arm, "seed_digest": seed, "problems": []}))
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
          "not of the gunicorn worker" in s["limitation"])


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
    r = cli(*LABELS)
    check("identical seeds still exit 0 when the gate passed", 0, r.returncode)
    check("while reporting INSUFFICIENT", True, "INSUFFICIENT" in r.stdout)

    build(root, c1={"seed": "a" * 64}, c2={"seed": "b" * 64, "gate": "FAIL"},
          c3={"seed": "c" * 64})
    check("a failed gate exits 1", 1, cli(*LABELS).returncode)


print()
if FAIL:
    print(f"FAILED {FAIL}/{PASS + FAIL}")
    raise SystemExit(1)
print(f"all passed ({PASS} assertions)")
