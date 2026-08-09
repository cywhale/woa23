"""The C2 conclusion: three cycles in, one verdict and two observations out.

Kept separate from the cycles that produced it, and kept pure, because the three
statements it makes are easy to blur together and each has a different standing:

**The verdict** is the 5.2B semantic gate, and it is PASS only if every cycle
passed. Two passes and a failure is not a pass.

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


def load_cycle(results: Path, label: str) -> dict:
    out: dict = {"label": label}
    for key, name in (("contract", f"{label}_contract.json"),
                      ("interp_candidate", f"{label}_interp_candidate.json"),
                      ("interp_reference", f"{label}_interp_reference.json"),
                      ("environment", f"{label}_environment.json")):
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
    """Did independent starts hash differently? Observed, never forced."""
    per_cycle = []
    for c in cycles:
        entry = {"label": c["label"]}
        for arm in ("candidate", "reference"):
            rec = c.get(f"interp_{arm}")
            entry[arm] = rec.get("seed_digest") if isinstance(rec, dict) else None
        per_cycle.append(entry)

    digests = [e["candidate"] for e in per_cycle if e.get("candidate")]
    missing = [e["label"] for e in per_cycle if not e.get("candidate")]
    distinct = len(set(digests))

    if missing or len(digests) < len(cycles):
        status = "INSUFFICIENT"
        note = (f"no seed was recorded for {missing}; diversity cannot be observed "
                f"from cycles that did not report one")
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
            "limitation": (
                "the recorded seed is that of a sibling interpreter launched by the "
                "same procedure as the arm, not of the gunicorn worker that served "
                "the requests. It is evidence about the launch procedure. The arms' "
                "own ordering is in order_stability below.")}


def order_stability(cycles: list[dict]) -> dict:
    """Whether each arm ordered rows the same way in every cycle. Not a verdict.

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
    return {
        "n_cases": len(cases),
        "n_comparable": len(seen),
        "n_orderless_responses": n_orderless,
        "n_varied": len(varied),
        "varied": varied,
        "stable": not varied and bool(seen),
        "note": ("Recorded, not gated. With no pinned seed a row-order difference "
                 "across cycles is the expected consequence of what C2 observes, "
                 "not a defect. A difference here is the arms' own behaviour, "
                 "unlike the seed digests above."),
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

    print("contract, 5.2B semantic, all three cycles")
    print(f"  gate: {v['gate']}   per cycle: {v['per_cycle']}")
    for p in v["problems"]:
        print(f"    - {p}")
    print()
    print("seed diversity — an observation, not a gate")
    print(f"  status: {s['status']}  ({s['n_distinct']} distinct / {s['n_cycles']} cycles)")
    print(f"  {s['note']}")
    print(f"  LIMITATION {s['limitation']}")
    for e in s["per_cycle"]:
        cand = (e.get("candidate") or "<none>")[:16]
        ref = (e.get("reference") or "<none>")[:16]
        print(f"    {e['label']:12s} candidate {cand}  reference {ref}")
    print()
    print("order stability — recorded, deliberately not part of the verdict")
    print(f"  {o['n_comparable']} (case, arm) pairs comparable across cycles; "
          f"{o['n_varied']} varied")
    print(f"  {o['n_orderless_responses']} responses had no row structure to order")
    for name in o["varied"][:10]:
        print(f"    varied: {name}")
    print(f"  {o['note']}")

    payload = {"kind": "c2_summary", "labels": args.labels,
               "contract": v, "seed_diversity": s, "order_stability": o,
               "site_limitation": (
                   "every cycle ran under -S: site.py did not run and no .pth in the "
                   "clone was processed. Isolated package-tree import correctness, "
                   "not production's site/.pth startup semantics (spec 002 s4.3.1).")}
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2))
        print(f"\nwrote {args.out}")

    # The gate decides the exit status. INSUFFICIENT seed diversity does not: it is
    # a statement about what this run could observe, and turning it into a failure
    # would create exactly the pressure to keep running cycles that the fixed count
    # exists to remove.
    if v["gate"] != "PASS":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
