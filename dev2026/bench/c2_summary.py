"""The C2 conclusion: three cycles in, one verdict and two observations out.

Kept separate from the cycles that produced it, and kept pure, because the three
statements it makes are easy to blur together and each has a different standing:

**The verdict** is the 5.2B semantic gate, and it is PASS only if every cycle
passed. Two passes and a failure is not a pass.

The two are combined into one named outcome, because reporting them separately
invites the summary "it passed":

| all three semantic gates | seed diversity | outcome | exit |
|---|---|---|---|
| PASS | `OBSERVED` | `PASS` | 0 |
| PASS | `INSUFFICIENT` | `PASS_WITH_INSUFFICIENT_SEED_DIVERSITY` | 5 |
| any FAIL | (not consulted) | `FAIL` | 1 |

**The shutdown budget** sits outside that table and can only move it downward. If
any cycle cannot show, from its own evidence, that its stop window outlasted what
its arms were allowed at shutdown, the outcome is `FAIL` whatever the gates said —
that cycle's cleanup was not held to the configuration the run was authorised for.
Nothing here can turn a failure into a pass.

**Seed diversity** is an *observation*, reported and never acted on. If the cycles
did not produce distinct seeds the answer is INSUFFICIENT — which is not a failure
of the candidate, and not a reason to run a fourth cycle. It means this run cannot
say what unpinned seeds do, and saying so is the whole of the obligation.

It also carries a limitation that must travel with it: the seed recorded per cycle
comes from a **sibling interpreter** launched by the same procedure as the arms, not
from the gunicorn worker that served the requests. It is evidence about the launch,
not about the worker. The arms' own behaviour is visible in the order fingerprints
instead, and those are the stronger observation precisely because they come from the
processes that answered.

**Order stability** is recorded and is deliberately not part of the verdict. Without
a pinned seed, two cycles ordering rows differently is the expected consequence of
the thing being observed, not a defect.

    uv run python -m bench.c2_summary --out results/c2_summary.json \\
        c2_cycle1 c2_cycle2 c2_cycle3
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# The same parser the collector asserts with, so the summary cannot disagree with
# the check that ran at collection time by reimplementing it.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.collect_backend_meta import graceful_timeout  # noqa: E402


def load_cycle(results: Path, label: str) -> dict:
    out: dict = {"label": label}
    for key, name in (("contract", f"{label}_contract.json"),
                      ("interp_candidate", f"{label}_interp_candidate.json"),
                      ("interp_reference", f"{label}_interp_reference.json"),
                      ("environment", f"{label}_environment.json"),
                      # The shutdown budget this cycle ran under, and the arms' own
                      # command lines to check it against. A missing one is already
                      # a problem by the rule below: `verdict` fails closed on any
                      # artefact a cycle was supposed to write and did not.
                      ("shutdown_budget", f"{label}_shutdown_budget.json"),
                      ("meta_candidate", f"{label}_meta_candidate.json"),
                      ("meta_reference", f"{label}_meta_reference.json")):
        path = results / name
        if not path.exists():
            out.setdefault("missing", []).append(str(path))
            continue
        try:
            out[key] = json.loads(path.read_text())
        except Exception as exc:
            out.setdefault("unreadable", []).append(f"{path}: {exc!r}")
    return out


def verdict(cycles: list[dict]) -> dict:
    """5.2B across every cycle. Fails closed on anything it cannot read."""
    problems, gates = [], []
    for c in cycles:
        label = c["label"]
        for m in c.get("missing", []):
            problems.append(f"{label}: {m} was never written")
        for u in c.get("unreadable", []):
            problems.append(f"{label}: {u}")
        contract = c.get("contract")
        if not isinstance(contract, dict):
            problems.append(f"{label}: no contract result to judge")
            continue
        if contract.get("variant") != "5.2B":
            problems.append(f"{label}: contract variant is {contract.get('variant')!r}, "
                            f"expected 5.2B — a C2 cycle compared the wrong way")
        gate = contract.get("gate")
        gates.append(gate)
        if gate != "PASS":
            failed = [r["id"] for r in contract.get("results", [])
                      if r.get("verdict") != "MATCH"]
            problems.append(f"{label}: contract gate {gate}"
                            + (f" ({len(failed)} cases: {', '.join(failed[:8])})"
                               if failed else ""))
    return {"gate": "PASS" if gates and not problems else "FAIL",
            "per_cycle": gates, "problems": problems}


def seed_diversity(cycles: list[dict]) -> dict:
    """Did independent starts hash differently? Observed, never forced.

    **`PYTHONHASHSEED` being unset and `hash_randomization` being 1 are
    preconditions, not evidence.** Together they say only that the interpreter was
    *permitted* to choose a seed per process. They are equally true of three starts
    that happened to choose the same one, and of a kernel or container configuration
    that makes the choice degenerate. What distinguishes those cases is the measured
    hash of a fixed string tuple, so the preconditions are checked and reported
    separately from the digests, and only the digests decide the status.
    """
    per_cycle = []
    precondition_problems = []
    for c in cycles:
        entry = {"label": c["label"]}
        for arm in ("candidate", "reference"):
            rec = c.get(f"interp_{arm}")
            entry[arm] = rec.get("seed_digest") if isinstance(rec, dict) else None
        # The preconditions, read from the same record as the digest.
        rec = c.get("interp_candidate")
        flags = (rec or {}).get("flags") or {}
        entry["hashseed_env"] = (rec or {}).get("hashseed_env")
        entry["hash_randomization"] = flags.get("hash_randomization")
        entry["n_strings"] = len(((rec or {}).get("hash_probe") or {}).get("strings") or [])
        if entry["hashseed_env"] is not None:
            precondition_problems.append(
                f"{c['label']}: PYTHONHASHSEED was set to {entry['hashseed_env']!r}. "
                f"A pinned seed is C1's arrangement, not C2's — this cycle observed "
                f"nothing about unpinned behaviour.")
        if entry["hash_randomization"] != 1:
            precondition_problems.append(
                f"{c['label']}: sys.flags.hash_randomization is "
                f"{entry['hash_randomization']!r}, not 1. The interpreter was not "
                f"free to choose a seed, so identical digests would say nothing.")
        if not entry["n_strings"]:
            precondition_problems.append(
                f"{c['label']}: the fixed hash probe recorded no strings, so its "
                f"digest is not a measurement of anything.")
        per_cycle.append(entry)

    digests = [e["candidate"] for e in per_cycle if e.get("candidate")]
    missing = [e["label"] for e in per_cycle if not e.get("candidate")]
    distinct = len(set(digests))

    if missing or len(digests) < len(cycles):
        status = "INSUFFICIENT"
        note = (f"no seed was recorded for {missing}; diversity cannot be observed "
                f"from cycles that did not report one")
    elif precondition_problems:
        # Fail closed. Three distinct digests under a broken precondition would still
        # be three distinct digests, and reporting OBSERVED from them would attribute
        # the variation to something that was not actually in force.
        status = "INSUFFICIENT"
        note = (f"{distinct} distinct digest(s) across {len(digests)} starts, but the "
                f"preconditions for reading them as unpinned-seed diversity did not "
                f"hold. See precondition_problems.")
    elif distinct == len(digests):
        status = "OBSERVED"
        note = (f"{distinct} distinct seeds in {len(digests)} independent starts — "
                f"this run observed the seed varying")
    else:
        status = "INSUFFICIENT"
        note = (f"only {distinct} distinct seed(s) across {len(digests)} starts. "
                f"This run cannot say what an unpinned seed does. It is NOT a "
                f"failure of the candidate, and it is NOT a reason to run a fourth "
                f"cycle: three was what was authorised.")
    return {"status": status, "n_distinct": distinct, "n_cycles": len(cycles),
            "per_cycle": per_cycle, "note": note,
            "precondition_problems": precondition_problems,
            "preconditions_are_not_evidence": (
                "PYTHONHASHSEED unset and hash_randomization=1 mean the interpreter "
                "was PERMITTED to choose a seed per process. They are not evidence "
                "that three starts chose different ones; three identical starts "
                "satisfy both. Only the measured digests below distinguish them."),
            "measurement": (
                "per cycle, hash() over a fixed 11-string tuple, run by the same "
                "binary with the same -S, PYTHONPATH, cwd and environment the arm "
                "was launched with"),
            "limitation": (
                "SIBLING / LAUNCH-ENVIRONMENT seed diversity. The recorded seed is "
                "that of a sibling interpreter launched by the same procedure as the "
                "arm, NOT of the gunicorn master or worker that served the requests. "
                "Measuring it inside those would need a worker observation mechanism "
                "this harness does not have. The arms' own behaviour is in "
                "order_stability below.")}


#: The three outcomes a C2 run can have, and their exit codes.
#:
#: `PASS_WITH_INSUFFICIENT_SEED_DIVERSITY` exists because the alternative is to call
#: it `PASS`, and that would be a false report: the semantic gate passed, and the
#: question C2 was run to answer — what an unpinned seed does across independent
#: starts — was not answered. Two different results must not share a name.
#:
#: It is deliberately **not** exit 0. A caller that checks only the exit status would
#: otherwise read it as a plain pass, which is the misreport this outcome exists to
#: prevent. It is equally deliberately **not** exit 1: the candidate did not fail, and
#: nothing about this outcome licenses a fourth cycle — there is no mechanism for one
#: and adding one is not a decision this code may take.
OUTCOME_EXIT = {
    "PASS": 0,
    "FAIL": 1,
    "PASS_WITH_INSUFFICIENT_SEED_DIVERSITY": 5,
    # Spec 008 §7b.4: named apart from FAIL on purpose. "The arms disagree
    # semantically" and "the candidate does not implement the contract we decided"
    # have different causes and different fixes, and one exit code for both would
    # send the reader to the wrong question.
    "ROW_ORDER_CONTRACT_FAILURE": 6,
}


def shutdown_budget(cycles: list[dict]) -> dict:
    """What each cycle allowed its arms at stop time, read back from the evidence.

    Three cycles' cleanups are three chances to strand a process on the host, and
    C2 cycle 1 on 2026-08-10 took one of them: the arms inherited gunicorn's 30 s
    `graceful_timeout` default while `STOP_WAIT_SECS` was 20. Both numbers are now
    fixed by the harness, asserted before a cycle starts, and asserted again against
    the arms' own `/proc/<pid>/cmdline` — but an assertion that ran is only visible
    if something reads its result afterwards, which is what this is.

    The arms' value is taken from `launch_argv` in each arm's provenance record, so
    it is what the process was launched with, not what the runner intended.
    """
    per_cycle, problems = [], []
    for c in cycles:
        label = c["label"]
        rec = c.get("shutdown_budget")
        entry: dict = {"label": label}
        if isinstance(rec, dict):
            entry["stop_wait_secs"] = rec.get("stop_wait_secs")
            entry["stop_wait_source"] = rec.get("stop_wait_source")
            entry["arm_graceful_timeout"] = rec.get("arm_graceful_timeout")
        else:
            problems.append(f"{label}: no shutdown budget was recorded")
        for arm in ("candidate", "reference"):
            meta = c.get(f"meta_{arm}")
            argv = meta.get("launch_argv") if isinstance(meta, dict) else None
            if not isinstance(argv, list):
                entry[arm] = None
                problems.append(f"{label}: no launch argv recorded for the {arm}")
                continue
            seconds, err = graceful_timeout([a for a in argv if isinstance(a, str)])
            entry[arm] = seconds
            if seconds is None:
                problems.append(f"{label}: the {arm}'s launch argv has no usable "
                                f"--graceful-timeout ({err})")
            elif (entry.get("arm_graceful_timeout") is not None
                  and seconds != entry["arm_graceful_timeout"]):
                problems.append(
                    f"{label}: the {arm} was launched with --graceful-timeout "
                    f"{seconds}s, but the cycle recorded a budget of "
                    f"{entry['arm_graceful_timeout']}s")
        wait, grace = entry.get("stop_wait_secs"), entry.get("arm_graceful_timeout")
        if isinstance(wait, int) and isinstance(grace, int) and wait <= grace:
            problems.append(
                f"{label}: STOP_WAIT_SECS={wait} does not exceed the arms' "
                f"--graceful-timeout={grace}. This cycle's cleanup could report a "
                f"survivor that was still inside its own budget.")
        per_cycle.append(entry)
    return {"status": "CONSISTENT" if not problems else "INCONSISTENT",
            "per_cycle": per_cycle, "problems": problems}


def overall_outcome(v: dict, s: dict, b: dict | None = None,
                    roc: dict | None = None, o: dict | None = None) -> dict:
    """Combine the semantic verdict and the seed observation into one named result.

    The shutdown budget is admitted here as a third input, and only ever downward:
    it cannot turn a failure into a pass. A cycle whose stop window was not sized
    against its arms' own budget did not run the authorised configuration, and its
    cleanup result is not the one that was asked for — so the run is not reported as
    a pass on the strength of gates that ran inside it.

    The row-order contract (spec 008 §7b) is a fourth input, also only ever downward,
    and it is **named separately** rather than folded into `FAIL`. Both its parts
    count: per-response conformance, and the candidate ordering a case identically in
    all three cycles. `INDETERMINATE` is not a pass either — a cycle predating the
    gate and a cycle that failed it must not look alike.
    """
    if roc is not None and roc.get("status") == "ROW_ORDER_CONTRACT_FAILURE":
        return {"outcome": "ROW_ORDER_CONTRACT_FAILURE",
                "exit_code": OUTCOME_EXIT["ROW_ORDER_CONTRACT_FAILURE"],
                "because": ("the candidate did not implement spec 008's row order in "
                            "at least one response. This is a contract failure, not a "
                            "semantic divergence, and it blocks a deployment claim on "
                            "its own"),
                "contract_gate": v.get("gate"), "seed_diversity": s.get("status"),
                "shutdown_budget": (b or {}).get("status"),
                "row_order_contract": roc.get("status")}
    if o is not None and o.get("candidate_varied"):
        return {"outcome": "ROW_ORDER_CONTRACT_FAILURE",
                "exit_code": OUTCOME_EXIT["ROW_ORDER_CONTRACT_FAILURE"],
                "because": ("the candidate ordered the same case differently in "
                            "different cycles: " + ", ".join(o["candidate_varied"][:5])
                            + ". Under the contract its order must not depend on the "
                            "process, and three independent seeds are exactly what "
                            "would expose it if it did"),
                "contract_gate": v.get("gate"), "seed_diversity": s.get("status"),
                "shutdown_budget": (b or {}).get("status"),
                "row_order_contract": (roc or {}).get("status")}
    if roc is not None and roc.get("status") == "INDETERMINATE":
        return {"outcome": "FAIL", "exit_code": OUTCOME_EXIT["FAIL"],
                "because": ("the row-order contract could not be established from "
                            "the cycles' own evidence — some response carried no "
                            "conformance record. A run that cannot show the contract "
                            "held is not a run that showed it"),
                "contract_gate": v.get("gate"), "seed_diversity": s.get("status"),
                "shutdown_budget": (b or {}).get("status"),
                "row_order_contract": roc.get("status")}
    if b is not None and b.get("status") != "CONSISTENT":
        return {"outcome": "FAIL", "exit_code": OUTCOME_EXIT["FAIL"],
                "because": ("at least one cycle's shutdown budget could not be "
                            "confirmed from its own evidence; cleanup was not held "
                            "to the configuration this run was authorised for"),
                "contract_gate": v.get("gate"), "seed_diversity": s.get("status"),
                "shutdown_budget": b.get("status")}
    if v.get("gate") != "PASS":
        name = "FAIL"
        because = ("at least one cycle's 5.2B semantic gate did not pass; the seed "
                   "observation is not consulted, because a failing contract is the "
                   "answer regardless of what the seeds did")
    elif s.get("status") == "OBSERVED":
        name = "PASS"
        because = ("all three cycles passed the 5.2B semantic gate, and three "
                   "independent starts were observed to hash differently")
    else:
        name = "PASS_WITH_INSUFFICIENT_SEED_DIVERSITY"
        because = ("all three cycles passed the 5.2B semantic gate, but this run did "
                   "not observe the seed varying, so it says nothing about unpinned "
                   "behaviour. This is NOT a plain PASS and must not be reported as "
                   "one. It is NOT a candidate failure. It is NOT a reason to run a "
                   "fourth cycle.")
    return {"outcome": name, "exit_code": OUTCOME_EXIT[name], "because": because,
            "contract_gate": v.get("gate"), "seed_diversity": s.get("status"),
            "shutdown_budget": (b or {}).get("status"),
            "row_order_contract": (roc or {}).get("status")}


def order_stability(cycles: list[dict]) -> dict:
    """Whether each arm ordered rows the same way in every cycle.

    **It is a verdict for the CANDIDATE and an observation for the REFERENCE.** Spec
    008 §7b.3 splits them, and the asymmetry is the point:

    - the **reference** does not implement spec 008's row order. With no pinned seed
      its order is a property of its process, so variation across cycles is expected
      and is **recorded, not gated** — exactly as it always was;
    - the **candidate** does implement it. Three cycles are three independent starts
      with three different seeds, which is precisely the circumstance under which the
      old order varied. So candidate variation is **`ROW_ORDER_CONTRACT_FAILURE`**,
      not an observation.

    An earlier version of this docstring said "Not a verdict" for both arms. That was
    true before the contract existed and false after it; a function whose comment
    disclaims a verdict while producing one is how a gate gets ignored.

    Compared per (case, arm) across cycles. Cases with no row structure — the
    OpenAPI document, error bodies — have no ordering and are counted separately
    rather than silently treated as stable.
    """
    seen: dict[tuple[str, str], set] = {}
    n_orderless = 0
    cases: set[str] = set()
    for c in cycles:
        contract = c.get("contract")
        if not isinstance(contract, dict):
            continue
        for r in contract.get("results", []):
            cid = r.get("id")
            if cid is None:
                continue
            cases.add(cid)
            for arm in ("reference", "candidate"):
                fp = r.get(f"{arm}_order")
                if not isinstance(fp, dict):
                    continue
                digest = fp.get("row_order_sha256")
                if digest is None:
                    n_orderless += 1
                    continue
                seen.setdefault((cid, arm), set()).add(digest)

    varied = sorted(f"{cid}/{arm}" for (cid, arm), d in seen.items() if len(d) > 1)
    cand_varied = sorted(cid for (cid, arm), d in seen.items()
                         if arm == "candidate" and len(d) > 1)
    ref_varied = sorted(cid for (cid, arm), d in seen.items()
                        if arm == "reference" and len(d) > 1)
    return {
        "n_cases": len(cases),
        "n_comparable": len(seen),
        "n_orderless_responses": n_orderless,
        "n_varied": len(varied),
        "varied": varied,
        "stable": not varied and bool(seen),
        # Split by arm, because the two arms are held to different standards.
        "candidate_varied": cand_varied,
        "candidate_stable": not cand_varied and any(
            arm == "candidate" for _, arm in seen),
        "reference_varied": ref_varied,
        "note": ("The REFERENCE line is recorded, not gated: with no pinned seed a "
                 "row-order difference across its cycles is the expected consequence "
                 "of what C2 observes, not a defect. The CANDIDATE line IS gated — "
                 "spec 008 made its row order a contract, and three cycles are three "
                 "independent seeds, so three-way agreement is what shows the sort "
                 "rather than the seed decides the order."),
    }


def row_order_contract(cycles: list[dict]) -> dict:
    """The candidate-only row-order gate for C2. Spec 008 §7b.

    Two questions, both answered from responses the cycles already fetched — **no
    extra HTTP request and no change to any budget** (§7b.2):

    1. did every candidate response conform to `(time_period, depth, lat, lon)`?
    2. was the candidate's order for a case identical in all three cycles?

    It is deliberately NOT folded into the 5.2B semantic verdict. "The arms disagree
    semantically" and "the candidate does not implement the contract we decided" have
    different causes and different fixes, and a report that blurs them sends the
    reader to the wrong question. A failure here blocks a deployment claim on its own.

    Fails closed: a cycle whose contract artefact carries no per-case conformance
    record cannot be treated as conforming, because a run predating this gate and a
    run that failed it would otherwise look identical.
    """
    violations, missing, problems = [], [], []
    n_checked = n_applicable = 0
    for c in cycles:
        contract = c.get("contract")
        if not isinstance(contract, dict):
            problems.append(f"{c.get('label')}: no contract artefact to read")
            continue
        results = contract.get("results", [])
        if not results:
            problems.append(f"{c.get('label')}: contract artefact carries no results")
            continue
        for r in results:
            cid = r.get("id", "?")
            n_checked += 1
            oc = r.get("candidate_row_order_contract")
            if not isinstance(oc, dict):
                missing.append(f"{c.get('label')}/{cid}")
                continue
            if not oc.get("applies"):
                continue
            n_applicable += 1
            if not oc.get("ok"):
                violations.append(f"{c.get('label')}/{cid}: {oc.get('violation')}")

    status = "PASS"
    if problems or missing:
        status = "INDETERMINATE"
    if violations:
        status = "ROW_ORDER_CONTRACT_FAILURE"
    return {
        "status": status,
        "n_responses_checked": n_checked,
        "n_with_row_order": n_applicable,
        "n_violations": len(violations),
        "violations": violations[:10],
        "n_missing_records": len(missing),
        "missing_records": missing[:10],
        "problems": problems,
        "note": ("Candidate only. The reference is not held to a contract it does "
                 "not implement. Computed from responses the cycles already fetched: "
                 "no additional request was issued and no budget changed."),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("labels", nargs="+", help="the cycle labels, in order")
    ap.add_argument("--results", type=Path, default=Path("results"))
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    if len(args.labels) != 3:
        print(f"C2 is three cycles; got {len(args.labels)}: {args.labels}. Refusing "
              f"to summarise a different number as a C2 result.", file=sys.stderr)
        return 2
    if len(set(args.labels)) != len(args.labels):
        print(f"the cycle labels repeat: {args.labels}. Each cycle writes to files "
              f"named after its label, so a repeat means one cycle overwrote another "
              f"and the same result would be counted twice.", file=sys.stderr)
        return 2

    cycles = [load_cycle(args.results, label) for label in args.labels]
    v = verdict(cycles)
    s = seed_diversity(cycles)
    o = order_stability(cycles)
    roc = row_order_contract(cycles)
    b = shutdown_budget(cycles)

    print("contract, 5.2B semantic, all three cycles")
    print(f"  gate: {v['gate']}   per cycle: {v['per_cycle']}")
    for p in v["problems"]:
        print(f"    - {p}")
    print()
    print("seed diversity — an observation, not a gate")
    print(f"  status: {s['status']}  ({s['n_distinct']} distinct / {s['n_cycles']} cycles)")
    print(f"  measured by: {s['measurement']}")
    print(f"  {s['note']}")
    print(f"  NOT EVIDENCE {s['preconditions_are_not_evidence']}")
    print(f"  LIMITATION {s['limitation']}")
    for p in s["precondition_problems"]:
        print(f"    - {p}")
    for e in s["per_cycle"]:
        cand = (e.get("candidate") or "<none>")[:16]
        ref = (e.get("reference") or "<none>")[:16]
        print(f"    {e['label']:12s} candidate {cand}  reference {ref}  "
              f"PYTHONHASHSEED={e.get('hashseed_env')!r} "
              f"hash_randomization={e.get('hash_randomization')!r}")
    print()
    print("order stability — GATED for the candidate, recorded for the reference")
    print(f"  {o['n_comparable']} (case, arm) pairs comparable across cycles; "
          f"{o['n_varied']} varied")
    print(f"  {o['n_orderless_responses']} responses had no row structure to order")
    print(f"  reference varied on {len(o['reference_varied'])} case(s) — an "
          f"observation, not a defect")
    print(f"  candidate varied on {len(o['candidate_varied'])} case(s) — "
          f"{'stable' if o['candidate_stable'] else 'ROW_ORDER_CONTRACT_FAILURE'}")
    for name in o["varied"][:10]:
        print(f"    varied: {name}")
    print(f"  {o['note']}")

    print()
    print("row-order contract, candidate only — spec 008 §7b")
    print(f"  status: {roc['status']}")
    print(f"  {roc['n_with_row_order']} of {roc['n_responses_checked']} candidate "
          f"responses carried a row order; {roc['n_violations']} violated it")
    if roc["n_missing_records"]:
        print(f"  {roc['n_missing_records']} response(s) carried NO conformance "
              f"record — INDETERMINATE, not a pass")
    for line in roc["violations"]:
        print(f"    - {line}")
    for line in roc["problems"]:
        print(f"    - {line}")
    print(f"  {roc['note']}")
    if roc["status"] != "PASS" or not o["candidate_stable"]:
        print("  A row-order contract failure is NOT a semantic divergence, is not "
              "folded into the 5.2B verdict above, and blocks a deployment claim on "
              "its own.")

    print()
    print("shutdown budget — what each cycle allowed its arms at stop time")
    print(f"  status: {b['status']}")
    for e in b["per_cycle"]:
        print(f"    {e['label']:12s} STOP_WAIT_SECS={e.get('stop_wait_secs')!r} "
              f"({e.get('stop_wait_source')}) vs --graceful-timeout: "
              f"recorded {e.get('arm_graceful_timeout')!r}, "
              f"candidate {e.get('candidate')!r}, reference {e.get('reference')!r}")
    for p_ in b["problems"]:
        print(f"    - {p_}")

    outcome = overall_outcome(v, s, b, roc, o)
    print()
    print("=" * 70)
    print(f"C2 OUTCOME: {outcome['outcome']}   (exit {outcome['exit_code']})")
    print(f"  {outcome['because']}")
    print(f"  semantic gate {outcome['contract_gate']}, "
          f"seed diversity {outcome['seed_diversity']}, "
          f"row-order contract {outcome.get('row_order_contract')}")
    print("=" * 70)

    payload = {"kind": "c2_summary", "labels": args.labels,
               "outcome": outcome["outcome"], "exit_code": outcome["exit_code"],
               "outcome_detail": outcome,
               "contract": v, "seed_diversity": s, "order_stability": o,
               "row_order_contract": roc,
               "shutdown_budget": b,
               "site_limitation": (
                   "every cycle ran under -S: site.py did not run and no .pth in the "
                   "clone was processed. Isolated package-tree import correctness, "
                   "not production's site/.pth startup semantics (spec 002 s4.3.1)."),
               "worker_provenance_limitation": (
                   "no worker-level Python provenance was collected in any cycle. "
                   "Import evidence comes from a sibling interpreter's launch "
                   "configuration and from /proc/<pid>/maps, which can refute "
                   "isolation but cannot establish what a gunicorn worker imported.")}
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2))
        print(f"\nwrote {args.out}")

    return outcome["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
