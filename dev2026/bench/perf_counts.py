"""How many requests the performance stages ACTUALLY issued, read from their output.

Spec 007 section 5.4 budgets the stages, and the first version of the runner recorded
those budget constants into the request counter. **A constant is a plan, not a
count.** A warm-up whose last four requests were refused still issued them, and a
gate that aborted partway issued fewer than its ceiling; recording 16 and 176
regardless would report the plan and call it traffic.

So the counts come from what each stage wrote down:

| stage | source | what makes it a count |
|---|---|---|
| `symmetric_warmup` | `<label>_symmetric_warmup.json` | the module records every attempt, including the ones that returned 0 with a transport error |
| `latency` | `<label>_paired.json` | the length of each case's raw `samples_ms[arm]` — kept raw, discarded leading sample included, precisely so every number can be recomputed |
| `noise_pilot` | `<label>_noise_pilot_<arm>.json` | the raw sample list of **every case it sampled** — the pilot loops over the whole case set, and its `samples_ms` already includes the discarded leading request |

The **contract** stage is not counted here at all: `record_contract_count` in
`scripts/lib_s2perf.sh` owns it, derives it from the case list and records it before
the gate runs. What IS checked here is whether that derived number may stand in for
measured attempts — see `contract_exact` below.

**These are attempts, not successes**, on the same rule as `lib_requests.sh`.

**Nothing here counts a bootstrap round.** The 5,000 rounds resample numbers already
collected; they issue no HTTP and must never appear in a request total.

## When a stage did not finish

A stage killed by a refused connection or a timeout has no complete artefact, and its
samples are not what it attempted. Each stage therefore also writes a **request
journal** (`bench/request_log.py`), one line appended before each request leaves.
When the artefact is missing or marks itself partial, the number comes from the
journal and the result is marked `counts_exact: false`.

A stage that exited non-zero without leaving any artefact is invisible here — for
the pilot, an absent artefact is the legitimate "no pilot at this rung" state. The
caller passes `--stage-failed <stage>` for a stage it watched fail. **The record and
the run's own report must agree**, so this flag exists rather than the shell
overriding a file that says otherwise.

### Two evidence strengths, not one

`attempt_evidence` says which applies to each stage, because they are not
interchangeable:

| value | when | what the number is |
|---|---|---|
| `stage_artefact` | the stage finished | attempts, exactly |
| `journal_writer_exited_normally` | the stage caught its own failure and wrote a partial artefact | attempts, exactly: every `attempt()` that returned was followed by a call that completed or raised, and the file was closed |
| `journal_after_abrupt_termination` | the caller watched it die and no artefact exists | **`journaled_attempts` — the number of records the journal contains.** Not a bound |

Under abrupt termination the count is **neither** a floor **nor** exact: the writer
can die between the flush and the call, so it may be one too high; a truncated final
record cannot be attributed to an arm, so it may be one too low. `host_attempt_count_exact`
is false and **no lower bound is claimed**. A real bound would need a derivation over
the single-threaded request loop, the flush point and the truncated-record case,
tested as such.

**`counts_exact: false` forbids reporting a single request total.** Report the
journaled attempt counts, the exactness flag and the authorised ceiling; the run is
not a complete latency result. `reporting_rule` carries that in the artefact so a
reader who never saw this docstring still gets the constraint.

## One record, three consumers

The shell counter, the reconciliation step and this artefact must never disagree
about what a stage issued, and they did: `--stage-failed` reached only the artefact,
so a hard-killed pilot was recorded as 0 by the counter, reconciled 0 against 0, and
reported as no traffic while the journal held the attempts it had made.

So the record is computed ONCE and the shell consumes that computation rather than
repeating it:

    uv run python -m bench.perf_counts --label s2p --results results \
        --journals run/journals --stage-failed noise_pilot \
        --out results/s2p_perf_counts.json --emit-shell

`--out` writes the canonical JSON; `--emit-shell` prints the same numbers as shell
assignments for the caller to `eval`. Both come from one call, so no flag can be
passed to one consumer and forgotten for another.

    uv run python -m bench.perf_counts --label s2p --results results
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.request_log import (                              # noqa: E402
    integrity as read_integrity, read_counts as read_journal,
)

ARMS = ("candidate", "reference")

#: What a run may say when any stage's count is inexact. Carried in the artefact.
INEXACT_REPORTING_RULE = (
    "A stage did not finish, so its request count is not exact. Report the "
    "journaled attempt counts WITH host_attempt_count_exact and the authorised "
    "ceiling, and do NOT present a single exact total. Where the evidence is "
    "journal_after_abrupt_termination the count is the number of journal records "
    "and is NOT a bound in either direction: the writer can die between the flush "
    "and the call, and a truncated final record cannot be attributed to an arm. "
    "The run is not a complete latency result."
)

#: How a stage's number was obtained. See the module docstring.
EVIDENCE_ARTEFACT = "stage_artefact"
EVIDENCE_JOURNAL_CLEAN = "journal_writer_exited_normally"
EVIDENCE_JOURNAL_ABRUPT = "journal_after_abrupt_termination"

#: The only values `attempt_evidence` may take. Artefacts are read from disk and a
#: tampered or corrupt one must not be able to introduce a new one — `--emit-shell`
#: is consumed by the shell, and an arbitrary string there was an injection point:
#: `attempt_evidence: "clean; rm -rf ..."` became a command.
EVIDENCE_VALUES = (EVIDENCE_ARTEFACT, EVIDENCE_JOURNAL_CLEAN, EVIDENCE_JOURNAL_ABRUPT)

#: What an artefact's unrecognised evidence value becomes. Never the artefact's own
#: text: a value this module does not know is not one it may pass on.
EVIDENCE_UNRECOGNISED = "unrecognised_evidence_value"


def _known_evidence(value, problems: list, where: str) -> str:
    """Whitelist. Anything else is replaced and reported, never propagated."""
    if value in EVIDENCE_VALUES:
        return value
    problems.append(f"{where}: attempt_evidence is not one of "
                    f"{', '.join(EVIDENCE_VALUES)} — the artefact says {value!r}, "
                    f"which is not a value this harness recognises and is not "
                    f"passed on")
    return EVIDENCE_UNRECOGNISED


def _stage(per_arm: dict, problems: list, *, measurement_complete: bool,
           attempt_count_exact: bool, evidence: str) -> dict:
    """One stage's record. THREE separate answers, deliberately not one.

    They used to be a single `exact` flag, and they are different questions:

    * `measurement_complete` — did the stage finish what it was measuring? A caught
      transport failure makes this false while the attempt count is perfectly good.
    * `attempt_count_exact` — is the number of attempts exact? False only when the
      writer may have died between a journal flush and the call it described.
    * `evidence` — how the number was obtained, so a reader can see WHY.

    Collapsing them cost a real defect: a caught failure and a hard kill both set
    `exact = False`, so the run could not tell a count it could check against the
    ceiling from one that bounds nothing.
    """
    return {"per_arm": per_arm, "problems": problems,
            "measurement_complete": measurement_complete,
            "attempt_count_exact": attempt_count_exact, "evidence": evidence}


def _load(path: Path):
    try:
        return json.loads(path.read_text()), None
    except Exception as exc:                       # noqa: BLE001
        return None, f"{path}: {type(exc).__name__}: {exc}"


def journal_integrity(journals: Path | None, stage: str) -> dict:
    """The journal's own shape: records, truncated, unattributed. No interpretation."""
    if journals is None:
        return {"exists": False, "attributable_records": 0,
                "truncated_records": 0, "unattributed_records": 0}
    return read_integrity(journals / f"{stage}.jsonl")


def _journal(journals: Path | None, stage: str) -> tuple[dict, list[str]]:
    """What the stage's journal says it issued. Absent journal is zero and a problem."""
    if journals is None:
        return {a: 0 for a in ARMS}, [f"{stage}: no request-journal directory given, "
                                      f"so the attempts it issued cannot be recovered"]
    return read_journal(journals / f"{stage}.jsonl")


def warmup_counts(results: Path, label: str, journals: Path | None = None,
                  failed: bool = False) -> dict:
    doc, err = _load(results / f"{label}_symmetric_warmup.json")
    if doc is None:
        got, jp = _journal(journals, "symmetric_warmup")
        return _stage(got, [err, *jp], measurement_complete=False,
                      attempt_count_exact=False, evidence=EVIDENCE_JOURNAL_ABRUPT)
    issued = doc.get("requests_per_arm") or {}
    problems = []
    if not doc.get("counts_match_expected", False):
        problems.append(f"{label}: the warm-up's own counts disagree with its "
                        f"expectation ({issued} vs {doc.get('expected_per_arm')})")
    return _stage({a: int(issued.get(a, 0)) for a in ARMS}, problems,
                  measurement_complete=True, attempt_count_exact=True,
                  evidence=EVIDENCE_ARTEFACT)


def latency_counts(results: Path, label: str, journals: Path | None = None,
                   failed: bool = False) -> dict:
    """Per arm: the raw samples of every case, which already include the discarded one.

    `paired_bench` keeps `samples_ms` RAW — `warm(raw)` drops the leading sample for
    the statistics, but the artefact stores all of it "so every number above can be
    recomputed". A 21-sample rung therefore leaves 22 entries per arm per case, and
    adding `WARMUP_REQUESTS` on top counted the discarded request twice, once per
    case: 184 where the run issued 176. The same rule as the pilot, for the same
    reason.

    A stage that aborted writes `complete: false`. Its samples then undercount what
    it issued — the request that failed was issued and kept no sample — so the count
    comes from the journal instead and is marked inexact.
    """
    doc, err = _load(results / f"{label}_paired.json")
    if doc is None:
        got, jp = _journal(journals, "latency")
        return _stage(got, [err, *jp], measurement_complete=False,
                      attempt_count_exact=False, evidence=EVIDENCE_JOURNAL_ABRUPT)
    if doc.get("complete") is False:
        got, jp = _journal(journals, "latency")
        cls = (doc.get("aborted") or {}).get("classification", "INCOMPLETE")
        # THE ARTEFACT IS AUTHORITATIVE about how its writer ended. It exists, so a
        # process was alive to write it, and it says which evidence its journal is —
        # but only from the closed set above.
        problems = [f"{label}: the latency stage did not finish ({cls}); its "
                    f"MEASUREMENT is incomplete and its attempt count comes from "
                    f"the journal", *jp]
        return _stage(
            got,
            problems,
            measurement_complete=False,
            attempt_count_exact=bool(doc.get("host_attempt_count_exact", True)),
            evidence=_known_evidence(
                doc.get("attempt_evidence", EVIDENCE_JOURNAL_CLEAN),
                problems, f"{label}: the latency artefact"))
    out = {a: 0 for a in ARMS}
    problems = []
    rows = doc.get("results") or []
    if not rows:
        problems.append(f"{label}: the latency artefact records no case")
    for row in rows:
        samples = row.get("samples_ms") or {}
        for arm in ARMS:
            got = samples.get(arm)
            if not isinstance(got, list):
                problems.append(f"{label}: {row.get('id')} has no {arm} samples to "
                                f"count")
                continue
            out[arm] += len(got)
    return _stage(out, problems, measurement_complete=True,
                  attempt_count_exact=True, evidence=EVIDENCE_ARTEFACT)


def pilot_counts(results: Path, label: str, journals: Path | None = None,
                 failed: bool = False) -> dict:
    """Per arm: the raw samples of every case the pilot visited.

    `bench.noise_pilot` writes one row per case and each row's `samples_ms` is the
    RAW list — it already contains the leading request the analysis discards. So
    nothing is added here; adding `WARMUP_REQUESTS` again would count that request
    twice, once per case.

    **`failed` is not the same as "killed".** It means only that the caller watched
    this stage exit non-zero, which a stage does for two very different reasons: it
    caught its own failure, wrote a partial artefact and closed its journal; or it
    was killed and left nothing. The ARTEFACT decides which — it exists only if a
    process was alive to write it, and it records its own `attempt_evidence`. Taking
    `failed` as proof of an abrupt death mislabelled a partial artefact from an HTTP
    500 as a hard kill, and threw the artefact away unread.
    """
    out, problems = {a: 0 for a in ARMS}, []
    partial_doc = None
    seen_artefact = False
    complete = True

    for arm in ARMS:
        path = results / f"{label}_noise_pilot_{arm}.json"
        if not path.exists():
            # Spec 007 section 5.4.3: the pilot does not run at every rung, and
            # section 5.4.6a stops the second arm after the first fails. Absent is a
            # legitimate state on its own.
            continue
        seen_artefact = True
        doc, err = _load(path)
        if doc is None:
            problems.append(err)
            complete = False
            continue
        if doc.get("complete") is False:
            partial_doc = doc
            complete = False
            cls = (doc.get("aborted") or {}).get("classification", "INCOMPLETE")
            problems.append(f"{label}: the {arm} pilot did not finish ({cls}); its "
                            f"MEASUREMENT is incomplete and its attempt count comes "
                            f"from the journal its own process closed")
            continue
        rows = doc.get("results") or []
        if not rows:
            problems.append(f"{label}: the {arm} pilot records no case to count")
            continue
        for row in rows:
            samples = row.get("samples_ms")
            if not isinstance(samples, list) or not samples:
                problems.append(f"{label}: the {arm} pilot's {row.get('id')} has no "
                                f"samples to count")
                continue
            out[arm] += len(samples)

    if partial_doc is not None:
        # A partial artefact exists, so its writer was alive to produce it. One
        # journal covers both invocations, each recording its own arm, so the journal
        # replaces the whole stage rather than one arm of it — and the artefact says
        # how strong that evidence is.
        got, jp = _journal(journals, "noise_pilot")
        return _stage(
            got, [*problems, *jp], measurement_complete=False,
            attempt_count_exact=bool(partial_doc.get("host_attempt_count_exact",
                                                     True)),
            evidence=_known_evidence(
                partial_doc.get("attempt_evidence", EVIDENCE_JOURNAL_CLEAN),
                problems, f"{label}: the pilot artefact"))

    if failed and not seen_artefact:
        # Non-zero, and NOTHING was written. That is the case where the writer may
        # have died between a journal flush and the call it described, so its record
        # count bounds nothing in either direction.
        got, jp = _journal(journals, "noise_pilot")
        return _stage(
            got,
            [f"{label}: the noise pilot exited non-zero and left no artefact. The "
             f"number reported for it is the count of records in its journal, which "
             f"is NOT a bound on what reached the host", *jp],
            measurement_complete=False, attempt_count_exact=False,
            evidence=EVIDENCE_JOURNAL_ABRUPT)

    if failed:
        # Non-zero with a COMPLETE artefact: the stage finished its measurement and
        # still exited non-zero. Recorded rather than reconciled away.
        problems.append(f"{label}: the noise pilot exited non-zero but its artefacts "
                        f"are complete; the measurement stands and the exit status "
                        f"is reported as a problem")
        complete = False

    return _stage(out, problems, measurement_complete=complete,
                  attempt_count_exact=True, evidence=EVIDENCE_ARTEFACT)


def contract_exact(results: Path, label: str) -> tuple[bool, list[str]]:
    """Whether the DERIVED contract count may stand in for measured attempts.

    `record_contract_count` records `len(all_cases())` per arm before the gate runs.
    That equals what reached the host only if every case issued both of its requests
    — and `contract_diff` records a case whose request raised as `verdict: ERROR`
    and moves on. Under RC order a case that failed on the reference never issued
    the candidate request at all, so a gate with any ERROR case issued FEWER
    requests than the derived number claims.

    An over-count is not the harmless direction. It would let a run report traffic
    it did not send, and — since the ceiling is checked against the recorded total —
    could fail a run for requests that were never issued. Either way the derived
    number is no longer a count, so the whole record is marked inexact and says so.
    """
    doc, err = _load(results / f"{label}_contract.json")
    if doc is None:
        # No contract artefact: the gate did not get far enough to write one, so
        # nothing here can confirm the derived number describes what was issued.
        return False, [err]
    rows = doc.get("results") or []
    if not rows:
        return False, [f"{label}: the contract artefact records no case, so its "
                       f"derived count cannot be confirmed against what was issued"]
    errored = [r.get("id") for r in rows if r.get("verdict") == "ERROR"]
    if errored:
        return False, [f"{label}: the contract gate recorded {len(errored)} case(s) "
                       f"as ERROR ({', '.join(str(e) for e in errored[:5])}"
                       f"{'...' if len(errored) > 5 else ''}). Its derived count is "
                       f"an UPPER BOUND, not measured attempts: a case that raised "
                       f"on the first arm never issued the second arm's request"]
    return True, []


def counts(results: Path, label: str, journals: Path | None = None,
           failed_stages: tuple = ()) -> dict:
    stages = {
        "symmetric_warmup": warmup_counts(results, label, journals,
                                          "symmetric_warmup" in failed_stages),
        "latency": latency_counts(results, label, journals,
                                  "latency" in failed_stages),
        "noise_pilot": pilot_counts(results, label, journals,
                                    "noise_pilot" in failed_stages),
    }
    ce, cp = contract_exact(results, label)

    # The two questions, answered apart. `host_attempt_count_exact` is what decides
    # whether a request total may be compared against an authorisation;
    # `measurement_complete` is what decides whether there is a latency result.
    attempts_exact = all(st["attempt_count_exact"] for st in stages.values()) and ce
    measurement_complete = all(st["measurement_complete"] for st in stages.values())

    out = {
        "kind": "perf_stage_counts",
        "label": label,
        "counts_attempts_not_successes": True,
        # Kept as the name the shell and the older artefacts use; it answers the
        # attempt-exactness question and nothing else.
        "counts_exact": attempts_exact,
        "host_attempt_count_exact": attempts_exact,
        "measurement_complete": measurement_complete,
        "excludes": ("bootstrap resampling, which issues no HTTP and is never "
                     "counted as a request"),
        "per_arm": {a: {stage: st["per_arm"][a] for stage, st in stages.items()}
                    for a in ARMS},
        "exact_per_stage": {**{stage: st["attempt_count_exact"]
                               for stage, st in stages.items()},
                            "contract": ce},
        "measurement_complete_per_stage": {stage: st["measurement_complete"]
                                           for stage, st in stages.items()},
        "attempt_evidence": {stage: st["evidence"] for stage, st in stages.items()},
        "contract_note": ("the contract stage is counted by record_contract_count "
                          "before the gate runs, not here; `contract` above says "
                          "only whether that derived number may stand in for "
                          "measured attempts"),
        "journal_integrity": {stage: journal_integrity(journals, stage)
                              for stage in stages},
        "failed_stages": list(failed_stages),
        "problems": [p for st in stages.values() for p in st["problems"]] + cp,
    }
    if not attempts_exact:
        out["reporting_rule"] = INEXACT_REPORTING_RULE
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--label", required=True)
    ap.add_argument("--results", type=Path, default=Path("results"))
    ap.add_argument("--stage-failed", action="append", default=[],
                    choices=("symmetric_warmup", "latency", "noise_pilot"),
                    dest="failed_stages",
                    help="a stage the CALLER watched exit non-zero. Absence of an "
                         "artefact is a legitimate state for the pilot, so only the "
                         "caller's exit status can distinguish 'did not run' from "
                         "'ran and died' — pass it and this record says inexact "
                         "instead of counting the stage as no traffic")
    ap.add_argument("--journals", type=Path, default=None,
                    help="directory of <stage>.jsonl request journals, used when a "
                         "stage's artefact is missing or partial")
    ap.add_argument("--stage", choices=("symmetric_warmup", "latency", "noise_pilot"),
                    help="print one stage's count for one arm, for the shell to add")
    ap.add_argument("--arm", choices=ARMS)
    ap.add_argument("--out", type=Path, default=None,
                    help="write the canonical record here. The same call can also "
                         "--emit-shell, so the file and the shell's numbers are one "
                         "computation and cannot diverge")
    ap.add_argument("--emit-shell", action="store_true",
                    help="print the record as shell assignments for eval: "
                         "PERF_<arm>_<stage>, PERF_COUNTS_EXACT, "
                         "PERF_FAILED_STAGES, PERF_EVIDENCE_<stage>, "
                         "PERF_MEASUREMENT_COMPLETE")
    ap.add_argument("--exact", action="store_true",
                    help="print `yes` or `no`: whether every stage's count is an "
                         "exact attempt count")
    args = ap.parse_args()

    result = counts(args.results, args.label, args.journals,
                    tuple(args.failed_stages))

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=2))

    if args.emit_shell:
        # Deliberately not a general serialiser: every value below is an integer or
        # a fixed word, so nothing here needs quoting and an eval of it cannot run
        # anything the caller did not ask for.
        for arm, stages in result["per_arm"].items():
            for stage, n in stages.items():
                print(f"PERF_{arm}_{stage}={int(n)}")
        print(f"PERF_COUNTS_EXACT={'yes' if result['counts_exact'] else 'no'}")
        print("PERF_MEASUREMENT_COMPLETE="
              f"{'yes' if result['measurement_complete'] else 'no'}")
        print("PERF_FAILED_STAGES='" + " ".join(result["failed_stages"]) + "'")
        for stage, ok in result["exact_per_stage"].items():
            print(f"PERF_EXACT_{stage}={'yes' if ok else 'no'}")
        for stage, ev in result["attempt_evidence"].items():
            # Belt and braces at the boundary that feeds the shell. The whitelist
            # above should make this unreachable; if it ever is not, nothing leaves
            # here that a shell could interpret.
            if ev not in EVIDENCE_VALUES + (EVIDENCE_UNRECOGNISED,):
                print(f"perf_counts: refusing to emit unrecognised evidence "
                      f"{ev!r} for {stage}", file=sys.stderr)
                return 2
            print(f"PERF_EVIDENCE_{stage}={ev}")
        return 0

    if args.exact:
        print("yes" if result["counts_exact"] else "no")
        return 0
    if args.stage:
        if not args.arm:
            print("--stage needs --arm", file=sys.stderr)
            return 2
        # Problems are reported on stderr and the count is still printed: the caller
        # records what was issued, and the run's own gate decides what to do about a
        # malformed artefact.
        for p in result["problems"]:
            print(f"perf_counts: {p}", file=sys.stderr)
        print(result["per_arm"][args.arm][args.stage])
        return 0

    print(json.dumps(result, indent=2))
    return 1 if result["problems"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
