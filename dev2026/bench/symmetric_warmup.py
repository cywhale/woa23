"""The symmetric warm-up pass — spec 007 section 4.1.

**Why it exists.** The candidate reaches its first request having validated the store
at import and read the anchor group's metadata in its lifespan; the reference has
done neither (spec 004 sections 48-51). For a byte comparison that is harmless, which
is why C1 and C2 ignored it. For a *latency* comparison it is a head start, and
`paired_stats.WARMUP_REQUESTS` does not cover it: that discards one sample **per
case**, inside the sequence, and this asymmetry precedes the sequence.

So before any sampling: **every case, once to each arm, in both orders, discarded.**

**This is not `per_case_leading_sample` and must not be confused with it.** Two
warm-ups at two scopes:

    symmetric_warmup_pass    16 per arm, once per run, HERE
    per_case_leading_sample   1 per case per arm, inside the latency sequence,
                              dropped by paired_stats.warm()

**Neither enters any statistic. Both are HTTP requests and both are counted** —
discarded from the statistics is not the same as free. This module records what it
issued so the run's request accounting can include it (spec 007 section 5.4.1).

**Nothing here is timed.** Recording a duration would invite its use, and a warm-up
sample is by construction the one measurement you have decided not to trust.

    uv run python -m bench.symmetric_warmup \\
        --candidate http://127.0.0.1:18161 --reference http://127.0.0.1:18162 \\
        --out results/<label>_symmetric_warmup.json
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.queries import Query, select  # noqa: E402
from bench.request_log import RequestLog  # noqa: E402

#: The same endpoint and the same case selection the gate uses. If the warm-up
#: covered a different set from the one about to be measured, it would be warming
#: something else.
ENDPOINT = "/api/woa23"

#: What every warm-up response must be. Anything else — a 500, a 404, a transport
#: error recorded as 0 — means that arm was not warmed, and the pass has not done
#: its job whatever the request count says.
EXPECT_STATUS = 200

#: Each case is issued once to each arm under each of the two arm orders.
ORDERS = ("RC", "CR")


def request_count_per_arm(n_cases: int) -> int:
    """`K x 2` — the figure spec 007 section 5.4.1 budgets as 16 for eight cases."""
    return n_cases * len(ORDERS)


def fetch(base_url: str, path: str, params: dict, timeout: float = 120.0) -> dict:
    """One request. The body is read and dropped; only the status is recorded."""
    query = urllib.parse.urlencode(dict(params))
    url = f"{base_url}{path}?{query}" if query else f"{base_url}{path}"
    try:
        with urllib.request.urlopen(urllib.request.Request(url), timeout=timeout) as r:
            r.read()
            return {"http_status": r.status, "transport_error": None}
    except urllib.error.HTTPError as exc:
        exc.read()
        return {"http_status": exc.code, "transport_error": None}
    except Exception as exc:                       # noqa: BLE001
        # An attempt that failed is still an attempt, and still traffic.
        return {"http_status": 0, "transport_error": f"{type(exc).__name__}: {exc}"}


def run(candidate_url: str, reference_url: str, queries=None,
        include_heavy: bool = True, journal=None) -> dict:
    """Both orders over every case, both arms. Returns what was issued.

    `queries` defaults to exactly what `paired_bench` will measure — same selection,
    same `--include-heavy` decision — because warming a different set from the one
    about to be sampled would leave the sampled set cold.
    """
    qs = list(select(None, include_heavy) if queries is None else queries)
    arms = {"candidate": candidate_url, "reference": reference_url}
    issued = {"candidate": 0, "reference": 0}
    statuses: list[dict] = []

    for order in ORDERS:
        sequence = ("reference", "candidate") if order == "RC" else \
                   ("candidate", "reference")
        for q in qs:
            for arm in sequence:
                # Before the request, not after: this pass already survives a
                # transport error, but a process killed mid-pass would leave no
                # artefact at all and its attempts would be uncountable.
                if journal is not None:
                    journal.attempt(arm, q.id)
                res = fetch(arms[arm], ENDPOINT, q.params())
                issued[arm] += 1
                statuses.append({"order": order, "case": q.id, "arm": arm,
                                 "http_status": res["http_status"],
                                 "transport_error": res["transport_error"]})

    expected = request_count_per_arm(len(qs))
    # WHICH RESPONSES WERE NOT WHAT THE PASS NEEDS. A warm-up exists to leave both
    # arms in the same state; an arm that answered 500, refused the connection or
    # timed out is not warm, however many requests were sent to it. Counting the
    # attempts and calling that success was the whole defect: the run then measured
    # latency against an arm that had never served a request.
    bad = [st for st in statuses
           if st["transport_error"] is not None
           or st["http_status"] != EXPECT_STATUS]
    return {
        "kind": "symmetric_warmup",
        # Complete means: every request in the pass came back with the status the
        # pass requires, on BOTH arms. Not "the right number were sent".
        "complete": not bad,
        "unusable_responses": bad,
        "unusable_per_arm": {arm: sum(1 for st in bad if st["arm"] == arm)
                             for arm in ("candidate", "reference")},
        "expected_status": EXPECT_STATUS,
        "n_cases": len(qs),
        "orders": list(ORDERS),
        "requests_per_arm": issued,
        "expected_per_arm": expected,
        "counts_match_expected": all(v == expected for v in issued.values()),
        "enters_statistics": False,
        "note": ("Every response here is discarded. This pass exists to erase the "
                 "candidate's startup head start (it has read the anchor group's "
                 "metadata; the reference has not) before any sample is taken. It "
                 "is NOT the per-case leading sample, which is dropped separately "
                 "by paired_stats.warm()."),
        "not_timed": ("No duration is recorded. A warm-up sample is the measurement "
                      "you have decided not to trust, and recording its time would "
                      "invite using it."),
        "statuses": statuses,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--candidate", required=True)
    ap.add_argument("--reference", required=True)
    ap.add_argument("--include-heavy", action="store_true",
                    help="match the gate's selection; the warm-up must cover the "
                         "same cases that are about to be measured")
    ap.add_argument("--request-log", type=Path, default=None,
                    help="append one line per request attempt BEFORE it is "
                         "issued")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    with RequestLog(args.request_log, "symmetric_warmup") as journal:
        result = run(args.candidate, args.reference,
                     include_heavy=args.include_heavy, journal=journal)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2))
    print(f"   symmetric warm-up: {result['requests_per_arm']} per arm "
          f"({result['n_cases']} cases x {len(ORDERS)} orders), all discarded")
    if not result["counts_match_expected"]:
        print("   the counts do not match what the budget expects", file=sys.stderr)
        return 1
    if not result["complete"]:
        # FAIL CLOSED. The artefact and the journal are already written above, so the
        # evidence survives; what must not happen is the chain continuing into a
        # latency measurement of arms that are not in a common state.
        n = result["unusable_per_arm"]
        print(f"   WARM-UP FAILED: {n['candidate']} candidate and {n['reference']} "
              f"reference response(s) were not {EXPECT_STATUS}.", file=sys.stderr)
        print("   Every request was issued and is recorded, but an arm that answered "
              "an error, refused or timed out is NOT warm.", file=sys.stderr)
        print("   The latency gate must not run: it would compare an arm that has "
              "served requests against one that has not.", file=sys.stderr)
        for st in result["unusable_responses"][:5]:
            print(f"     {st['arm']:9s} {st['case']:24s} status={st['http_status']}"
                  f"{' ' + st['transport_error'] if st['transport_error'] else ''}",
                  file=sys.stderr)
        return 1
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
