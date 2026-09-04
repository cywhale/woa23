"""The latency gate: two backends, interleaved, with a three-state verdict.

`http_bench.py` measures one backend at a time — it runs every case against A, then
every case against B. Over a run that long, host drift lands unevenly on the two
arms. This tool interleaves A and B request by request within each case, so drift
hits both equally, and decides with a bootstrap confidence interval rather than by
eyeballing two medians.

Verdicts are three-state. An interval that straddles the threshold is
`INCONCLUSIVE`, not a pass: the honest output of an underpowered comparison is
"collect more samples", never "close enough".

Usage:
    uv run python -m bench.paired_bench \
        --candidate http://127.0.0.1:8051 \
        --reference http://127.0.0.1:8052 \
        --warm 21 --include-heavy --margin 0.05 \
        --out results/paired_s1.json

Raw per-request latencies are kept in the output so any statistic here can be
recomputed independently.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bench.request_log import RequestLog, read_counts as read_journal  # noqa: E402
from bench.paired_stats import (  # noqa: E402
    DEFAULT_SEED, ESCALATION_LADDER, PRACTICAL_MARGIN, WARMUP_REQUESTS,
    bootstrap_ratio, improvement_verdict, next_rung, plan_rung,
    regression_verdict, required_n, warm,
)
from bench.collect_backend_meta import (  # noqa: E402
    boot_id, cmdline, compare_store, master_of, pids_on_port, proc_starttime,
    zmetadata_fingerprints,
)
from bench.provenance import (  # noqa: E402
    _verify_source_set, load_meta, validate_meta, validate_store_agreement,
    verify_group_path_agreement,
)
from bench.queries import Query, select  # noqa: E402

ENDPOINT = "/api/woa23"

# Cases that must show an established speedup for S1 to pass; they are the
# work-dominated, low-noise ones. See spec 001 section 6.1.2.
IMPROVEMENT_REQUIRED = ("readme_example", "point_profile_multiparam")

def request_once(client: httpx.Client, base: str, params: dict,
                 timeout: float) -> tuple[float, int, int]:
    q = dict(params)
    q["_cb"] = uuid.uuid4().hex
    t0 = time.perf_counter()
    r = client.get(base.rstrip("/") + ENDPOINT, params=q, timeout=timeout)
    body = r.read()
    return (time.perf_counter() - t0) * 1000, r.status_code, len(body)


def source_hashes(root: Path) -> dict:
    """Full SHA-256 of every harness source file. Truncated hashes are not
    verifiable evidence, and a git commit does not cover uncommitted edits."""
    out = {}
    for p in sorted(root.glob("bench/*.py")):
        out[str(p.relative_to(root))] = hashlib.sha256(p.read_bytes()).hexdigest()
    return out


def post_run_runtime_check(meta: dict | None, label: str) -> list[str]:
    """Re-check everything the run depends on staying still: process, port, sources, store.

    All four are verified before sampling and checked again here; without this
    second look the whole sampling window would be unobserved. What can change in
    it, or become unverifiable:

    * the **process** — restarted, died, or its PID recycled into another;
    * the **port** — handed to a different process while the original still lives;
    * the **sources** — a file edited after its digest was recorded;
    * the **store** — `.zmetadata` rewritten underneath the reads.

    Doing this here rather than as a separate command afterwards is deliberate: a
    detached check can exit non-zero while the gate file it was meant to qualify
    already says PASS, and whoever reads the artefact later sees only the PASS.

    Everything needed comes from the pre-run record itself — store path, source
    digests, master PID, port, and the argv the sidecar was told to expect — so this
    asks the operator for nothing new.
    """
    if not meta:
        return []
    problems = []

    pid = meta.get("master_pid", -1)
    argv, _ = cmdline(pid)
    if argv is None:
        problems.append(f"{label}: process {pid} is gone — it restarted or died "
                        f"during sampling, so the timings do not all come from one "
                        f"process")
    else:
        # Start time first: a restart under the same launch command produces
        # identical argv, so argv alone cannot tell a recycled PID from the original.
        if boot_id() != meta.get("boot_id"):
            problems.append(f"{label}: the host rebooted during the run")
        elif proc_starttime(pid) != meta.get("proc_starttime"):
            problems.append(f"{label}: PID {pid} has a different start time — this is "
                            f"a different process wearing the same PID, not the one "
                            f"that was sampled")
        for expected in meta.get("expect_argv_contains") or []:
            if not any(expected in a for a in argv):
                problems.append(f"{label}: process {pid} no longer matches "
                                f"{expected!r}")
        if argv != meta.get("launch_argv"):
            problems.append(f"{label}: argv changed during sampling")

    # The socket can change hands without the original process dying: another
    # process binds the port after the first releases it, and PID, start time and
    # argv all still match while the HTTP traffic went somewhere else entirely.
    # Nothing above would notice, so the listener set is re-checked directly.
    port = meta.get("port")
    if isinstance(port, int):
        listeners = pids_on_port(port)
        if not listeners:
            problems.append(f"{label}: nothing is listening on port {port} any more")
        elif pid not in listeners:
            problems.append(f"{label}: port {port} is now held by {listeners}, not by "
                            f"PID {pid} — the requests may have gone to a different "
                            f"process")
        elif master_of(listeners) != pid:
            # Strict: an ambiguous listener set (master_of -> None) is a failure, not
            # a pass. "We could not tell" and "it is still the same process" are
            # different answers and only one of them clears this gate.
            problems.append(f"{label}: port {port} is no longer mastered by PID "
                            f"{pid} (master now: {master_of(listeners)}, "
                            f"listeners: {listeners})")

    # The pre-run verification was the last observation of these files before this
    # one, so anything between the two is only visible here. A file edited while the
    # benchmark ran would leave the pre-run digests describing code that stopped
    # being what served requests partway through.
    problems.extend(f"{label}: source changed during sampling — {p.split(': ', 1)[-1]}"
                    for p in _verify_source_set(meta, label))

    after = zmetadata_fingerprints(meta.get("store_path"))
    if after is None:
        problems.append(f"{label}: store path missing from the record")
    elif isinstance(after, dict) and "error" in after:
        problems.append(f"{label}: store unreadable after sampling: {after['error']}")
    else:
        problems.extend(f"{label}: {d}" for d in
                        compare_store(meta, {"store_path": meta.get("store_path"),
                                             "zmetadata_fingerprints": after}))
    return problems


def decide_gate(*, meta_problems: list, drift: list, invalid: list,
                hard_fail: list, unproven: list, undecided: list) -> str:
    """The run's single verdict, in strict precedence order.

    Extracted from `main()` so the precedence can be pinned by a test. Every check
    upstream of this has one, but the mapping from findings to verdict had none —
    so a future edit could route drift to a passing status and only the prose would
    notice.

    Order matters and is not arbitrary: the validity questions come first, because a
    run whose provenance or runtime cannot be trusted has no performance result to
    report at all. Only then do the performance verdicts apply, worst first.
    """
    if meta_problems:
        return "INVALID_METADATA"
    if drift:
        return "INVALID_RUNTIME_DRIFT"
    if invalid:
        return "INVALID"
    if hard_fail:
        return "FAIL"
    if unproven:
        return "FAIL_NO_ESTABLISHED_IMPROVEMENT"
    if undecided:
        return "INCONCLUSIVE"
    return "PASS"


def host_facts() -> dict:
    """Machine state that changes timings, read at the start of the run."""
    out = {"cpu_count": os.cpu_count()}
    try:
        mem = Path("/proc/meminfo").read_text().splitlines()
        out["meminfo"] = {k.strip(): v.strip() for k, v in
                          (line.split(":", 1) for line in mem[:5])}
    except Exception:
        out["meminfo"] = None          # not Linux, or unreadable
    try:
        out["loadavg"] = os.getloadavg()
    except Exception:
        out["loadavg"] = None
    return out


def git_head(root: Path) -> str | None:
    try:
        r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root,
                           capture_output=True, text=True, timeout=5)
        return r.stdout.strip() or None
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--candidate", required=True, help="base URL of the candidate (A arm)")
    ap.add_argument("--reference", required=True, help="base URL of the reference (B arm)")
    ap.add_argument("--warm", type=int, default=21,
                    help="warm samples per backend per case; "
                         f"{WARMUP_REQUESTS} extra cold request(s) are issued and discarded")
    ap.add_argument("--pause", type=float, default=0.2,
                    help="seconds between AB/BA **pairs**, not between individual "
                         "requests. Each iteration issues one request to each arm "
                         "back to back — that adjacency is what pairs them in time — "
                         "and then pauses once. A run of N warm samples therefore "
                         "issues 2*(N+1) requests and pauses N+1 times.")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--query", action="append", dest="queries")
    ap.add_argument("--include-heavy", action="store_true")
    ap.add_argument("--margin", type=float, default=PRACTICAL_MARGIN,
                    help="practical-significance margin: how much slower is worth "
                         "calling a regression (0.05 = 5%%). This is a judgement "
                         "about what matters, not a noise estimate — the confidence "
                         "interval already carries the noise.")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--insecure", action="store_true",
                    help="skip TLS verification (loopback to a known process only)")
    ap.add_argument("--candidate-meta", type=Path, default=None,
                    help="backend provenance JSON from bench.collect_backend_meta, "
                         "run on the backend's own host. The harness only speaks "
                         "HTTP and cannot see source hashes, PYTHONHASHSEED, or the "
                         "resolved dependencies; without this the record is "
                         "incomplete and says so.")
    ap.add_argument("--reference-meta", type=Path, default=None,
                    help="the same, for the reference backend")
    ap.add_argument("--gate-variant", choices=("5.2A", "5.2B", "5.2C"), required=True,
                    help="which contract-gate variant is in force (spec 001): 5.2A "
                         "byte-exact against a controlled reference, 5.2B semantic "
                         "against live production. The number means different things "
                         "under each, so it is recorded, not inferred.")
    ap.add_argument("--expect-status", type=int, default=200,
                    help="status every request must return; anything else invalidates "
                         "the case rather than being timed")
    ap.add_argument("--request-log", type=Path, default=None,
                    help="append one line per request attempt BEFORE it is issued, "
                         "so a stage killed by a transport failure still has "
                         "a record of the attempts it made. Under abrupt\n"
                         "termination that record is not a bound on what\n"
                         "reached the host")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    queries: list[Query] = select(args.queries, args.include_heavy)
    total = args.warm + WARMUP_REQUESTS

    print(f"candidate : {args.candidate}")
    print(f"reference : {args.reference}")
    print(f"cases     : {len(queries)}   warm samples/arm: {args.warm} "
          f"(+{WARMUP_REQUESTS} discarded)")
    print(f"margin    : +/-{args.margin:.0%} practical significance")
    print()

    # A file that failed to load has already said everything useful; re-running the
    # schema check on None would just repeat it.
    meta_problems = []
    unpinned_note: list[str] = []
    metas = {}
    for label, path in (("candidate", args.candidate_meta),
                        ("reference", args.reference_meta)):
        meta, errs = load_meta(path, label)
        metas[label] = meta
        # 5.2B's reference is live production: unpinned by necessity, which is why
        # that variant compares semantically. The candidate is ours and is always
        # required to be pinned.
        pinned = not (args.gate_variant == "5.2B" and label == "reference")
        meta_problems.extend(
            errs if errs else validate_meta(
                meta, label, seed_requirement="pinned" if pinned else "any"))
        if not pinned:
            seed = ((meta or {}).get("env") or {}).get("PYTHONHASHSEED")
            unpinned_note.append(
                f"{label}: PYTHONHASHSEED is {seed!r} — expected under variant 5.2B, "
                f"where the reference is a process we may not restart. Output "
                f"ordering from it is therefore not reproducible, which is why this "
                f"variant compares semantically rather than byte for byte.")
    cand_meta, ref_meta = metas["candidate"], metas["reference"]
    meta_problems.extend(validate_store_agreement(cand_meta, ref_meta))
    if args.gate_variant in ("5.2A", "5.2C"):
        # Not 5.2B. There the reference is live production, whose store
        # literal is whatever it is and cannot be aligned — which is the reason that
        # variant compares semantically in the first place.
        #
        # This is checked here as well as in the contract gate because the two tools
        # are run separately and a latency result carries its own provenance. A
        # latency number produced from arms that build their group paths differently
        # would be measuring an ordering difference alongside the change under test.
        meta_problems.extend(verify_group_path_agreement(cand_meta, ref_meta))
    if meta_problems:
        # Abort before the first request. Sampling against a run we already know is
        # unpublishable wastes the operator's time, puts avoidable load on a
        # production host, and produces numbers that invite being quoted anyway.
        print("INVALID_METADATA — refusing to sample:")
        for m in meta_problems:
            print(f"  - {m}")
        payload = {
            "kind": "paired_latency",
            "gate": "INVALID_METADATA",
            "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "gate_variant": args.gate_variant,
            "metadata_complete": False,
            "metadata_problems": meta_problems,
            "candidate_meta": cand_meta,
            "reference_meta": ref_meta,
            "harness_invocation": sys.argv,
            "note": "No requests were issued; provenance failed validation first.",
            "results": [],
        }
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(payload, indent=2))
            print(f"wrote {args.out}")
        return 1

    results = []
    verify = not args.insecure
    # Written before each request leaves, and read back by `bench.perf_counts` when
    # this process does not survive to write its artefact.
    journal = RequestLog(args.request_log, "latency")
    aborted: dict | None = None
    with httpx.Client(verify=verify, follow_redirects=True) as client:
      try:
        for q in queries:
            a_samples, b_samples = [], []
            a_status, b_status = set(), set()
            order_log = []
            for i in range(total):
                # Interleave so host drift is shared, and counterbalance AB/BA so
                # neither arm systematically gets the first (warmer-connection,
                # colder-cache) slot within a pair.
                pair = ((args.candidate, a_samples, a_status),
                        (args.reference, b_samples, b_status))
                if i % 2:
                    pair = pair[::-1]
                order_log.append("AB" if i % 2 == 0 else "BA")
                for base, bucket, seen in pair:
                    journal.attempt("candidate" if base == args.candidate
                                    else "reference", q.id)
                    ms, code, _ = request_once(client, base, q.params(), args.timeout)
                    bucket.append(ms)
                    seen.add(code)
                time.sleep(args.pause)

            # A backend that errors fast would otherwise look like a huge speedup.
            bad_status = (a_status != {args.expect_status}
                          or b_status != {args.expect_status})

            aw, bw = warm(a_samples), warm(b_samples)
            stats = bootstrap_ratio(aw, bw, seed=args.seed)
            reg = regression_verdict(stats["ci95_low"], stats["ci95_high"], args.margin)
            imp = improvement_verdict(stats["ci95_low"], stats["ci95_high"])
            if bad_status:
                reg = imp = "INVALID_STATUS"
            need = required_n(len(aw), stats["median_ratio"],
                              stats["ci95_low"], stats["ci95_high"], args.margin)
            rung = plan_rung(len(aw), need)

            results.append({
                "id": q.id,
                "params": q.params(),
                "statuses": {"candidate": sorted(a_status), "reference": sorted(b_status)},
                "expected_status": args.expect_status,
                "status_ok": not bad_status,
                "order_log": order_log,
                "margin": args.margin,
                # The estimate and the executable step are different things: 23 is a
                # valid estimate and an invalid instruction, because sample sizes are
                # pre-specified and 23 is not on the ladder.
                "samples_needed_estimate": need,
                "next_ladder_rung": rung,
                "stats": stats,
                "regression_verdict": reg,
                "improvement_verdict": imp,
                "improvement_required": q.id in IMPROVEMENT_REQUIRED,
                # Raw samples kept so every number above can be recomputed.
                "samples_ms": {"candidate": a_samples, "reference": b_samples},
            })
            hint = ""
            if reg == "INCONCLUSIVE":
                if need is None:
                    hint = "  unresolvable: effect sits exactly on the margin"
                elif rung is None:
                    hint = (f"  estimate ~{need}/arm exceeds the ladder "
                            f"(top {ESCALATION_LADDER[-1]}) -> refer to PI")
                else:
                    hint = f"  estimate ~{need}/arm -> next rung {rung}"
            print(f"{q.id:26s} ratio {stats['median_ratio']:6.3f} "
                  f"CI [{stats['ci95_low']:.3f}, {stats['ci95_high']:.3f}]  "
                  f"{reg:14s} {imp:14s}{hint}")
      except Exception as exc:                     # noqa: BLE001
        # A refused connection or a timeout. Every request already issued is in the
        # journal, and the cases already finished are in `results` — both are kept
        # and BOTH are marked partial. The alternative, which is what this replaced,
        # was to die with no artefact at all and leave the run unable to say how
        # many requests it had sent.
        aborted = {
            "classification": "STAGE_ABORTED_TRANSPORT_FAILURE",
            "stage": "latency",
            "case": q.id,
            "error": f"{type(exc).__name__}: {exc}",
            "cases_completed": len(results),
            "cases_planned": len(queries),
            "meaning": ("the latency stage stopped on a transport failure. The cases "
                        "below completed; the rest were never measured"),
            "not": ("NOT a latency result and NOT a gate verdict. No case here may "
                    "be quoted as performance, and the run's request total is not "
                    "exact — report the observed count and the authorised ceiling"),
            "request_journal": str(args.request_log) if args.request_log else None,
        }
        print(f"\nSTAGE_ABORTED_TRANSPORT_FAILURE at {q.id}: "
              f"{type(exc).__name__}: {exc}")
      finally:
        # Closed on every path out, including the ones no handler here catches.
        journal.close()

    if aborted is not None:
        # No gate. A run that could not finish sampling has no verdict to give, and
        # `decide_gate` would happily compute one from the cases that did finish.
        observed, jproblems = ({"candidate": 0, "reference": 0}, [])
        if args.request_log:
            observed, jproblems = read_journal(args.request_log)
        payload = {
            "kind": "paired_latency",
            "gate": "INVALID_TRANSPORT_FAILURE",
            "complete": False,
            "request_total_exact": False,
            "journaled_attempts_per_arm": observed,
            # This stage CAUGHT its own failure and is writing this
            # file, so its process was alive to close the journal:
            # every attempt() that returned was followed by a call
            # that completed or raised. The attempt count is exact;
            # the MEASUREMENT is what is incomplete.
            "host_attempt_count_exact": True,
            "attempt_evidence": "journal_writer_exited_normally",
            "request_journal_problems": jproblems,
            "reporting_rule": ("the MEASUREMENT is incomplete. The attempt count "
                               "above is exact — this process closed its own "
                               "journal — but no latency figure may be quoted, and "
                               "the authorised ceiling must be reported with it"),
            "aborted": aborted,
            "candidate_url": args.candidate,
            "reference_url": args.reference,
            "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "warm_samples_per_arm": args.warm,
            "warmup_discarded": WARMUP_REQUESTS,
            "gate_variant": args.gate_variant,
            "practical_margin": args.margin,
            "harness_invocation": sys.argv,
            # The cases that DID complete, with their raw samples. They are evidence
            # of what was issued, not of how the change performs.
            "results": results,
        }
        if args.out:
            args.out.parent.mkdir(parents=True, exist_ok=True)
            args.out.write_text(json.dumps(payload, indent=2))
            print(f"wrote {args.out} (partial: {len(results)}/{len(queries)} cases)")
        return 1

    invalid = [r for r in results if not r["status_ok"]]
    # The window the pre-run records cannot see. Folded into this run's verdict so
    # one artefact cannot say PASS while a separate command says something moved.
    drift = (post_run_runtime_check(cand_meta, "candidate")
             + post_run_runtime_check(ref_meta, "reference"))
    if drift:
        print("\nPOST-RUN DRIFT:")
        for d in drift:
            print(f"  - {d}")

    hard_fail = [r for r in results if r["regression_verdict"] == "REGRESSION"]
    unproven = [r for r in results
                if r["improvement_required"] and r["improvement_verdict"] != "IMPROVED"]
    undecided = [r for r in results if r["regression_verdict"] == "INCONCLUSIVE"]

    gate = decide_gate(meta_problems=meta_problems, drift=drift, invalid=invalid,
                       hard_fail=hard_fail, unproven=unproven, undecided=undecided)

    print(f"\ngate: {gate}")
    if meta_problems:
        print("  metadata: " + "; ".join(meta_problems))
    if drift:
        print("  post-run drift: " + "; ".join(drift))
    for label, rows in (("unexpected status", invalid), ("regressions", hard_fail),
                        ("undecided", undecided),
                        ("improvement not established", unproven)):
        if rows:
            print(f"  {label}: {', '.join(r['id'] for r in rows)}")

    root = Path(__file__).resolve().parent.parent
    payload = {
        "kind": "paired_latency",
        "gate": gate,
        "candidate_url": args.candidate,
        "reference_url": args.reference,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "warm_samples_per_arm": args.warm,
        "warmup_discarded": WARMUP_REQUESTS,
        "interleaved": True,
        "practical_margin": args.margin,
        "escalation_ladder": list(ESCALATION_LADDER),
        "next_rung": next_rung(args.warm),
        "candidate_meta": cand_meta,
        "reference_meta": ref_meta,
        "metadata_complete": not meta_problems,
        "metadata_problems": meta_problems,
        "unpinned_seed_notes": unpinned_note,
        "post_run_drift": drift,
        "counterbalanced": "AB/BA alternating per iteration",
        "gate_variant": args.gate_variant,
        "host": platform.node(),
        "platform": platform.platform(),
        "host_facts": host_facts(),
        "python": sys.version,
        "python_executable": sys.executable,
        "harness_invocation": sys.argv,
        "git_head": git_head(root),
        "harness_sha256": source_hashes(root),
        "results": results,
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2))
        print(f"wrote {args.out}")
    return 0 if gate == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
