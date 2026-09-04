"""Measure the noise floor of the latency harness before trusting any threshold.

A rule like "no case may be more than 5% slower" is only meaningful if 5% is
outside what two runs of the *same* backend produce by chance. This samples one
backend repeatedly, then asks by bootstrap: split those samples into two arms of
`k`, compare their medians — how far from 1.0 does the ratio wander?

Both arms come from one backend, so the **null hypothesis** is a ratio of 1.0. That
is not a guarantee of 1.0: the same backend still drifts with GC, page cache, and
co-tenant load, and those effects are inside these samples rather than controlled
away. The output is a preliminary estimate of the noise floor, not a constant.

Its output is used for **sample-size planning**, not for setting the gate's
threshold: a floor measured in one window does not transfer to another (see
`paired_stats.regression_verdict`).

**Whichever backend it samples, it describes only that backend.** In the S1 campaign
it targets the candidate, which keeps traffic off production — but that means the
resulting sample sizes are planned from the candidate's noise, and say nothing about
production's. That does not affect the gate's validity, because the gate's own
confidence interval is computed from both arms' actual samples. It does mean the
planning evidence is narrower than the thing being planned for, and a case sized
from a quiet candidate may still come back INCONCLUSIVE against a noisier
production — which the ladder then handles. Sampling follows `paired_stats.WARMUP_REQUESTS`,
the same rule `paired_bench.py` and `http_bench.py` use, so the quantity estimated
here is the quantity the gate compares.

Usage:
    uv run python -m bench.noise_pilot --base-url http://127.0.0.1:8051 \
        --warm 25 --out results/noise_pilot_candidate.json

Raw per-request latencies are kept in the output so the bootstrap can be redone.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import time
import uuid
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bench.paired_stats import (  # noqa: E402
    BOOTSTRAP_ROUNDS, DEFAULT_SEED, WARMUP_REQUESTS, noise_floor, warm,
)
from bench.queries import Query, select  # noqa: E402
from bench.request_log import RequestLog, read_counts as read_journal  # noqa: E402

class UnexpectedStatus(Exception):
    """A sampled case answered with a status the run did not expect.

    An exception rather than `raise SystemExit`, which is what this used to be.
    `SystemExit` derives from BaseException, so it passed straight through the
    `except Exception` that writes the partial artefact: a pilot that had already
    issued hundreds of requests exited with no artefact, no `complete: false` and
    no journal close, and `perf_counts` then read the absent artefact as the
    legitimate "no pilot at this rung" state and called the run exact.
    """


ENDPOINT = "/api/woa23"
REPEAT_LEVELS = (3, 5, 11, 21)
# The gate's planned sample size; the level whose estimate is reported as the
# headline. Must match the gate's --warm setting in spec 001 section 6.1.1.
SELECTED_K = 21


def sample(client: httpx.Client, base: str, params: dict, n: int,
           pause: float, timeout: float, journal=None, arm: str = "candidate",
           case: str = "") -> tuple[list[float], set[int]]:
    out, statuses = [], set()
    url = base.rstrip("/") + ENDPOINT
    for _ in range(n):
        q = dict(params)
        q["_cb"] = uuid.uuid4().hex
        if journal is not None:
            journal.attempt(arm, case)
        t0 = time.perf_counter()
        r = client.get(url, params=q, timeout=timeout)
        r.read()
        out.append((time.perf_counter() - t0) * 1000)
        statuses.add(r.status_code)
        time.sleep(pause)
    return out, statuses


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default="https://eco.odb.ntu.edu.tw")
    ap.add_argument("--warm", type=int, default=25,
                    help="warm samples per case; "
                         f"{WARMUP_REQUESTS} extra cold request(s) are discarded")
    ap.add_argument("--pause", type=float, default=0.3)
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--query", action="append", dest="queries",
                    help="restrict to these case ids; default is the full gate set")
    ap.add_argument("--skip-heavy", action="store_true",
                    help="omit the heavy cases. Off by default: the pilot must cover "
                         "every case the gate judges, and the noise floor varies ~7x "
                         "across them, so a subset cannot stand in for the rest.")
    ap.add_argument("--expect-status", type=int, default=200,
                    help="status every request must return; anything else means the "
                         "timings are not measuring the thing under test")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--insecure", action="store_true")
    ap.add_argument("--arm", default="candidate", choices=("candidate", "reference"),
                    help="which arm this invocation is sampling. The pilot targets "
                         "one arm per invocation; the name is what the request "
                         "journal records the attempts against")
    ap.add_argument("--request-log", type=Path, default=None,
                    help="append one line per request attempt BEFORE it is issued, "
                         "so a stage killed by a transport failure still has "
                         "a record of the attempts it made. Under abrupt\n"
                         "termination that record is not a bound on what\n"
                         "reached the host")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    # Default is the whole gate case list, heavy cases included, not a hand-picked
    # subset: the noise floor varies by ~7x across cases, so a subset cannot supply
    # planning numbers for the rest.
    queries: list[Query] = select(args.queries, include_heavy=not args.skip_heavy)
    total = args.warm + WARMUP_REQUESTS

    # Resampling k draws from a pool smaller than k cannot see noise the pool never
    # captured. A 4-sample run reported +/-1.3% for a case whose real floor is
    # +/-3.9%, so this is refused outright rather than warned about.
    if args.warm < SELECTED_K:
        raise SystemExit(
            f"--warm {args.warm} is below the gate's k={SELECTED_K}; the k={SELECTED_K} "
            f"estimate would resample from too small a pool and report a floor that is "
            f"confidently too tight. Use --warm {SELECTED_K} or more."
        )

    print(f"target: {args.base_url}   cases: {len(queries)}   "
          f"warm samples/case: {args.warm} (+{WARMUP_REQUESTS} discarded)\n")

    results = []
    journal = RequestLog(args.request_log, "noise_pilot")
    aborted = None
    with httpx.Client(verify=not args.insecure, follow_redirects=True) as client:
      try:
        for q in queries:
            raw, statuses = sample(client, args.base_url, q.params(), total,
                                   args.pause, args.timeout, journal=journal,
                                   arm=args.arm, case=q.id)
            if statuses != {args.expect_status}:
                raise UnexpectedStatus(
                    f"{q.id}: saw status {sorted(statuses)}, expected "
                    f"{args.expect_status}. Timings from error responses do not "
                    f"describe the workload; fix the backend or the case first."
                )
            s = warm(raw)
            levels = []
            for k in REPEAT_LEVELS:
                lv = noise_floor(s, k, BOOTSTRAP_ROUNDS, args.seed)
                lv["selected"] = (k == SELECTED_K)
                levels.append(lv)
            med = statistics.median(s)
            results.append({
                "id": q.id,
                "params": q.params(),
                "statuses_seen": sorted(statuses),
                "warm_n": len(s),
                "median_ms": round(med, 2),
                "min_ms": round(min(s), 2),
                "max_ms": round(max(s), 2),
                "spread_pct": round((max(s) - min(s)) / med * 100, 1),
                "levels": levels,
                "samples_ms": raw,          # includes the discarded cold sample(s)
            })
            sel = next(lv for lv in levels if lv["selected"])
            print(f"{q.id:26s} median {med:8.1f} ms  spread {results[-1]['spread_pct']:6.1f}%"
                  f"   k={SELECTED_K} tolerance +/-{sel['tolerance_pct']:.1f}%")
            for lv in levels:
                mark = " <-- gate" if lv["selected"] else ""
                print(f"      k={lv['k']:2d}  CI [{lv['ci95_low']:.3f}, {lv['ci95_high']:.3f}]"
                      f"  tol +/-{lv['tolerance_pct']:5.1f}%{mark}")

      except Exception as exc:                   # noqa: BLE001
        # Same rule as the latency gate: the attempts are in the journal, the cases
        # that finished are in `results`, and both are kept and marked partial. A
        # pilot that dies with no artefact leaves its requests uncountable.
        aborted = {
            "classification": ("STAGE_ABORTED_UNEXPECTED_STATUS"
                               if isinstance(exc, UnexpectedStatus)
                               else "STAGE_ABORTED_TRANSPORT_FAILURE"),
            "stage": "noise_pilot",
            "case": q.id,
            "error": f"{type(exc).__name__}: {exc}",
            "cases_completed": len(results),
            "cases_planned": len(queries),
            "not": ("NOT a noise floor and NOT sample-size planning. The run's "
                    "request total is not exact — report the observed count and "
                    "the authorised ceiling"),
        }
        print(f"\nSTAGE_ABORTED_{'UNEXPECTED_STATUS' if isinstance(exc, UnexpectedStatus) else 'TRANSPORT_FAILURE'} at {q.id}: "
              f"{type(exc).__name__}: {exc}")
      finally:
        # Every path out, including a KeyboardInterrupt or a SystemExit raised by
        # something below this frame. The lines are already flushed, so this is
        # about the descriptor rather than the data — but a stage that leaves an
        # open journal has left a loose end where its evidence lives.
        journal.close()

    observed, jproblems = ({"candidate": 0, "reference": 0}, [])
    if args.request_log:
        observed, jproblems = read_journal(args.request_log)
    payload = {
        "kind": "noise_pilot",
        "arm": args.arm,
        "complete": aborted is None,
        "request_total_exact": aborted is None,
        "journaled_attempts_per_arm": observed,
        # This stage CAUGHT its own failure and is writing this file, so its process
        # was alive to close the journal: every attempt() that returned was followed
        # by a call that completed or raised. The ATTEMPT COUNT is exact; the
        # MEASUREMENT is what is incomplete. A stage killed asynchronously writes no
        # artefact at all, and its journal is not evidence of the same strength.
        "host_attempt_count_exact": True,
        "attempt_evidence": "journal_writer_exited_normally",
        "request_journal_problems": jproblems,
        "aborted": aborted,
        "base_url": args.base_url,
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "host": platform.node(),
        "python": sys.version,
        "null_hypothesis": "Both bootstrap arms are drawn from the SAME backend, so "
                           "the null hypothesis is a ratio of 1.0. Drift, GC, cache "
                           "and co-tenant load are inside these samples, not "
                           "controlled away — this is a preliminary estimate of the "
                           "noise floor, not a measured constant.",
        "warmup_discarded": WARMUP_REQUESTS,
        "selected_k": SELECTED_K,
        "seed": args.seed,
        "bootstrap_rounds": BOOTSTRAP_ROUNDS,
        "results": results,
    }
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2))
        print(f"\nwrote {args.out}"
              + ("" if aborted is None
                 else f" (partial: {len(results)}/{len(queries)} cases)"))
    return 0 if aborted is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
