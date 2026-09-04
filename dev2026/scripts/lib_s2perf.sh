#!/usr/bin/env bash
#
# The stage chain: contract gate -> symmetric warm-up -> latency gate -> noise pilot
# -> request counting -> finalisation -> report.
#
# Sourced, never executed. It exists for the reason `lib_d1_finalize.sh` says in its
# own header, and which has now been paid for three times:
#
#   > it is a sourced function, so a test can execute the real path rather than a
#   > reconstruction of it.
#
# `bench/test_s2perf_integration.py` walks these stages by invoking the same Python
# modules, which proves the modules compose. It cannot prove that THIS chain — the
# order, the stop-on-failure clauses, which status reaches the caller, what is
# skipped when a stage dies — behaves as intended, because that logic lived inline
# in a script that needs Linux /proc, a production interpreter and a real store to
# reach it. `scripts/test_s2perf_driver.sh` sources this file and runs these
# functions for real, against stand-in servers.
#
# Every function returns a status; none of them calls `exit`. The caller decides
# what a failure means, so the same chain can be driven by a test that wants to see
# the failure rather than inherit it.
#
# Requires lib_requests.sh (the counters) and lib_d1_finalize.sh (finalisation).

#: Exit status for a run whose stages did not all finish, so it has no exact attempt
#: count to report. Distinct from 1 (a gate verdict) and from
#: $D1_FINALIZE_FAILED (artefacts not finalised): three different states, and a
#: caller must be able to tell them apart without parsing text.
S2PERF_INCOMPLETE=7

#: Exit status for a run that issued more requests than its authorisation covered.
#: Above every other status: a run outside its budget has no quotable result at all,
#: so this must not be confused with a gate verdict (1), unfinalised artefacts (6) or
#: an unfinished stage (7).
S2PERF_CEILING_EXCEEDED=8

#: Exit status for a run whose COUNTERS disagree with its evidence. Distinct from
#: $S2PERF_CEILING_EXCEEDED because they are different findings and only one of them
#: is about the host: a duplicate count owner issues no HTTP at all, it double-records
#: what one request already did. Calling that "ceiling exceeded" would report traffic
#: that was never sent. `CEILING_EXCEEDED` is reserved for attempts the evidence can
#: demonstrate, and anything else is an accounting fault.
S2PERF_ACCOUNTING_INCONSISTENT=9

# ------------------------------------------------- the contract stage's count ---

# record_contract_count <arm>...
#
# THE SINGLE OWNER of the contract stage's request count, for every mode. Called by
# whoever drives the gate, BEFORE the gate issues anything, exactly once per run.
#
# It is an owner rather than a line of shell because it was two lines of shell: the
# runner recorded it before the gate and `s2perf_record_counts` recorded it again
# after, so the s2perf path reported 528 per arm against an authorised 496. One
# function, called from one place per execution path, is what makes that arithmetic
# checkable instead of a thing to remember.
#
# LIMITATION, and the condition under which this number is the truth: this is a
# DERIVED count, not a measured one. It equals what reached the host only while
#
#   * the transport does not retry — `contract_diff` builds `httpx.Client()` with no
#     `transport=`, and httpx's default has `retries=0`, so a connection failure
#     raises instead of being tried again; and
#   * no response is a redirect — `follow_redirects=True` is set, so a 3xx would
#     make httpx issue a second request this number would not include. No contract
#     case declares a 3xx.
#
# Both are pinned behaviourally by test_the_contract_stage_count_is_safe_to_derive
# in bench/test_contract.py. **If either stops holding, the contract stage must be
# counted by a transport-level counter instead of derived here.**
#
# AND IT IS AN UPPER BOUND, NOT AN ATTEMPT COUNT, WHEN THE GATE DOES NOT COMPLETE.
# `contract_diff` records a case whose request raised as `verdict: ERROR` and moves
# on, and under RC order a case that failed on the reference never issued the
# candidate request at all. So a gate with any ERROR case issued FEWER requests than
# this number claims. `bench/perf_counts.py` reads those verdicts back and marks the
# whole record inexact when it finds any: a derived number may not pose as measured
# attempts. The readiness and store-probe stages are measured rather than derived,
# which is why they need no such condition.
record_contract_count() {   # record_contract_count <arm>...
  local n arm marker="${REQUEST_COUNTS_DIR:-}/.contract_recorded"
  # Once, and it refuses to be talked into twice. The owner being a function stops
  # a SECOND call site being added; this stops the SAME site being reached twice,
  # which no amount of grepping would catch.
  if [ -e "$marker" ]; then
    echo "record_contract_count: the contract stage has already been recorded for" >&2
    echo "  this run (see $marker). Recording it again would not issue a single" >&2
    echo "  extra request — it would double the COUNT of requests one gate made," >&2
    echo "  which is an accounting fault, not traffic. Refusing." >&2
    return 1
  fi
  n="$(uv run python -c '
import sys; sys.path.insert(0, ".")
from bench.contract_cases import all_cases
print(len(all_cases()))')" || return 1
  for arm in "$@"; do
    request_add "$arm" contract "$n" || return 1
  done
  # The marker IS the guarantee. If it cannot be written, a second call would not
  # be refused — and this function's whole contract is that it is. Fail closed
  # rather than leave a promise the filesystem did not keep.
  if ! mkdir -p "${REQUEST_COUNTS_DIR:-.}" 2>/dev/null \
     || ! printf '%s\n' "$n" > "$marker" 2>/dev/null; then
    echo "record_contract_count: the count was recorded but its once-only marker" >&2
    echo "  ($marker) could not be written, so a second call could not be refused." >&2
    echo "  Failing: an unenforceable guard is not a guard." >&2
    return 1
  fi
}

# ---------------------------------------------------------------- the stages ---

# s2perf_contract <cand-url> <ref-url> <variant> <seed-policy> <label> <results>
#
# One request per case per arm. Returns non-zero if any case differs, which is what
# stops the chain before a single timing is taken: a latency figure for an arm that
# has not been shown to answer correctly is not worth having.
s2perf_contract() {
  local cand="$1" ref="$2" variant="$3" policy="$4" label="$5" results="${6:-results}"
  uv run python -m bench.contract_diff \
    --candidate "$cand" --reference "$ref" --variant "$variant" \
    --seed-policy "$policy" \
    --candidate-meta "$results/${label}_meta_candidate.json" \
    --reference-meta "$results/${label}_meta_reference.json" \
    --out "$results/${label}_contract.json"
}

# s2perf_warmup <cand-url> <ref-url> <label> <results> <journals>
#
# Spec 007 section 4.1. The candidate reaches its first request having read the
# anchor group's metadata and the reference has not; `WARMUP_REQUESTS` does not
# cover that, because it discards one sample per case INSIDE the sequence and this
# asymmetry precedes the sequence. Every response is discarded, every request is
# counted, and it warms exactly the set the gate is about to measure.
s2perf_warmup() {
  local cand="$1" ref="$2" label="$3" results="${4:-results}" journals="$5"
  uv run python -m bench.symmetric_warmup \
    --candidate "$cand" --reference "$ref" --include-heavy \
    --request-log "$journals/symmetric_warmup.jsonl" \
    --out "$results/${label}_symmetric_warmup.json"
}

# s2perf_latency <cand-url> <ref-url> <label> <results> <journals>
#
# Rung 21, contract variant 5.2C, margin 0.05. Returns the gate's own status: 0 for PASS and
# non-zero for every other verdict. A non-zero status here is a RESULT, not a crash,
# and the caller must not treat it as one.
s2perf_latency() {
  local cand="$1" ref="$2" label="$3" results="${4:-results}" journals="$5"
  uv run python -m bench.paired_bench \
    --candidate "$cand" --reference "$ref" \
    --gate-variant 5.2C --warm 21 --include-heavy --margin 0.05 \
    --candidate-meta "$results/${label}_meta_candidate.json" \
    --reference-meta "$results/${label}_meta_reference.json" \
    --request-log "$journals/latency.jsonl" \
    --out "$results/${label}_paired.json"
}

# s2perf_latency_complete <label> [results]
#
# Echoes yes or no. A gate that stopped on a transport failure writes an artefact
# marked `complete: false`; a gate that returned a verdict writes a complete one.
# The two are different states and the chain treats them differently.
s2perf_latency_complete() {
  local label="$1" results="${2:-results}" f="${2:-results}/${1}_paired.json"
  if [ ! -f "$f" ]; then echo no; return 0; fi
  uv run python -c '
import json, sys
try:
    print("no" if json.load(open(sys.argv[1])).get("complete") is False else "yes")
except Exception:
    print("no")
' "$f"
}

# s2perf_pilot <cand-url> <ref-url> <label> <results> <journals>
#
# LAST, and against both arms. Before the latency gate it would have sampled one arm
# 208 times and the other not at all, warming one side's page cache and connection
# state ahead of a paired measurement — an asymmetry introduced by the very tool
# meant to characterise noise. Its output is sample-size planning for the NEXT rung.
# FAIL-FAST ACROSS THE ARMS, decided rather than inherited. The pilot's output is a
# noise floor OF THE PAIR: a figure from one arm plans nothing on its own, because
# the escalation estimate it feeds is a property of the comparison. So if the first
# arm does not complete, the second arm's 208 requests would buy a number that
# cannot be used — and spending them anyway is the opposite of a budget.
#
# What is kept when it stops: the journal (every attempt, both arms), the first
# arm's partial artefact, and its journaled attempt counts. What is not produced:
# any noise floor, for either arm.
s2perf_pilot() {
  local cand="$1" ref="$2" label="$3" results="${4:-results}" journals="$5" rc=0
  uv run python -m bench.noise_pilot --base-url "$ref" \
    --warm 25 --arm reference --request-log "$journals/noise_pilot.jsonl" \
    --out "$results/${label}_noise_pilot_reference.json" || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "the reference pilot exited $rc; NOT sampling the candidate arm." >&2
    echo "  The noise floor is a property of the pair, so a candidate-only figure" >&2
    echo "  would plan nothing — and 208 more requests to produce it would be" >&2
    echo "  spending the budget on a number that cannot be used." >&2
    return "$rc"
  fi
  uv run python -m bench.noise_pilot --base-url "$cand" \
    --warm 25 --arm candidate --request-log "$journals/noise_pilot.jsonl" \
    --out "$results/${label}_noise_pilot_candidate.json" || rc=$?
  return "$rc"
}

# ------------------------------------------------------------------ counting ---

# s2perf_record_counts <label> <results> <journals>
#
# From what each stage RECORDED, not from the budget constants. A constant is a
# plan: a warm-up whose last requests were refused still issued them, and recording
# 16 regardless would report the plan and call it traffic.
#
# **The contract stage is NOT recorded here.** `record_contract_count` owns it and
# has already run, before the gate. Recording it here as well is precisely the
# double count this function used to contain.
# s2perf_record_counts <label> <results> <journals>
#
# Reads the CANONICAL record — the one `s2perf_stage_record` computed, with whatever
# failed-stage information the caller had — rather than asking `perf_counts` again
# with its own flags. Three consumers asking three times is how a hard-killed pilot
# came to be counted as 0 by the shell and from the journal by the artefact: the
# `--stage-failed` flag reached one call and not the others.
s2perf_record_counts() {
  local label="$1" results="${2:-results}" journals="$3" arm stage n v
  local marker="${REQUEST_COUNTS_DIR:-}/.perf_counts_recorded"
  # Once per run, on the same rule as record_contract_count. A second call would
  # add each measured stage's count on top of itself — no extra request, twice the
  # bookkeeping. `s2perf_reconcile` would catch it afterwards, but a fault that can
  # be refused outright should not have to be detected.
  if [ -e "$marker" ]; then
    echo "s2perf_record_counts: the measured stages have already been recorded for" >&2
    echo "  this run (see $marker). Recording them again would double the count of" >&2
    echo "  requests that were issued once. Refusing." >&2
    return 1
  fi
  if [ -z "${PERF_COUNTS_EXACT:-}" ]; then
    echo "s2perf_record_counts: no canonical stage record in scope. Call" >&2
    echo "  s2perf_stage_record first; recomputing here would be a second, " >&2
    echo "  differently-flagged answer to the same question." >&2
    return 1
  fi
  for arm in candidate reference; do
    for stage in symmetric_warmup latency noise_pilot; do
      v="PERF_${arm}_${stage}"
      n="${!v:-}"
      case "$n" in ''|*[!0-9]*)
        echo "s2perf_record_counts: the canonical record has no count for" >&2
        echo "  $arm/$stage." >&2
        return 1 ;;
      esac
      request_add "$arm" "$stage" "$n" || return 1
    done
  done
  if ! mkdir -p "${REQUEST_COUNTS_DIR:-.}" 2>/dev/null \
     || ! : > "$marker" 2>/dev/null; then
    echo "s2perf_record_counts: the counts were recorded but the once-only marker" >&2
    echo "  ($marker) could not be written, so a second call could not be refused." >&2
    echo "  Failing: an unenforceable guard is not a guard." >&2
    return 1
  fi
}

# s2perf_counts_exact <label> <results> <journals>
#
# Echoes yes or no: whether every stage's number is an exact attempt count.
s2perf_counts_exact() {
  uv run python -m bench.perf_counts --label "$1" --results "${2:-results}" \
    --journals "$3" --exact
}

# s2perf_classify_incomplete <label> <results> <budget-arm>
#
# The classification a run gets when a stage did not finish. Written down rather
# than left to an exit code: the measurements that DID happen are real, the request
# attempt count is not exact, and neither fact may be read as the other.
# The run-level classification is NEUTRAL — `INCOMPLETE_STAGE_FAILURE` — and the
# stage's own reason is carried beside it. It used to be hardcoded
# `INCOMPLETE_TRANSPORT_FAILURE`, so a backend answering HTTP 500 was reported at run
# level as a transport failure: the stage artefact said one thing and the run's
# headline said another, and the headline is what gets quoted.
#
# `stage_failure_classification` is read from the stage artefacts themselves, so it
# cannot drift from what the stage recorded.
s2perf_classify_incomplete() {
  local label="$1" results="${2:-results}" budget="$3"
  local cand ref cause failed_json="" st v failed_json="" st
  cand="$(request_arm_total candidate)"
  ref="$(request_arm_total reference)"
  for st in ${PERF_FAILED_STAGES:-}; do
    [ -n "$failed_json" ] && failed_json="$failed_json, "
    failed_json="$failed_json\"$st\""
  done
  cause="$(uv run python -m bench.stage_cause --label "$label" --results "$results")" || cause="UNAVAILABLE"
  # `failed_stages` comes from the canonical record, so the run-level classification
  # names the stage on its own. `NO_STAGE_ARTEFACT_RECORDS_A_CAUSE` used to defer to
  # "the run report names which", which is no use to anyone holding only this file.
  # host_attempt_count_exact comes from the CANONICAL RECORD, not from a constant.
  # It was hardcoded false, so a caught stage failure — measurement incomplete,
  # attempt count perfectly exact — produced a classification contradicting both
  # perf_counts.json and the stage artefact beside it. Three files, two answers.
  local exact_json="${PERF_COUNTS_EXACT:-no}"
  case "$exact_json" in yes) exact_json=true ;; *) exact_json=false ;; esac
  local rule
  if [ "$exact_json" = true ]; then
    rule="the attempt count IS exact — every failing stage closed its own journal — so it may be compared against the authorised ceiling. What is incomplete is the MEASUREMENT: no latency figure may be quoted from this run"
  else
    rule="report the journaled attempt counts WITH host_attempt_count_exact and the authorised ceiling; never a single exact total. Where a stage was killed asynchronously its number is the count of records in its journal and is NOT a bound on what reached the host"
  fi
  printf '{\n  "kind": "s2perf_classification",\n  "label": "%s",\n  "classification": "INCOMPLETE_STAGE_FAILURE",\n  "stage_failure_classification": "%s",\n  "failed_stages": [%s],\n  "measurement_complete": false,\n  "host_attempt_count_exact": %s,\n  "journaled_attempts_per_arm": {"candidate": %s, "reference": %s},\n  "ceiling_per_arm": %s,\n  "ceiling_total": %s,\n  "reporting_rule": "%s",\n  "not": "%s"\n}\n' \
    "$label" "$cause" "$failed_json" "$exact_json" "$cand" "$ref" "$budget" \
    "$((budget * 2))" "$rule" \
    "NOT a complete latency result, NOT a gate verdict, and no case may be quoted as performance" \
    > "$results/${label}_perf_classification.json"

  echo "S2 performance classification: INCOMPLETE_STAGE_FAILURE" >&2
  echo "   failed stage(s), as the run observed them: ${PERF_FAILED_STAGES:-<none recorded>}" >&2
  echo "   cause, as the stages themselves recorded it: $cause" >&2
  echo "   A stage did not finish. The MEASUREMENT is incomplete either way; whether" >&2
  echo "   the attempt count is exact depends on HOW the stage ended:" >&2
  echo "      journaled : $cand candidate, $ref reference" >&2
  echo "      exact     : ${PERF_COUNTS_EXACT:-no} (host_attempt_count_exact=$exact_json)" >&2
  echo "      ceiling   : $budget per arm, $((budget * 2)) total" >&2
  for st in ${PERF_FAILED_STAGES:-}; do
    v="PERF_EVIDENCE_${st}"
    if [ "${!v:-}" = "journal_after_abrupt_termination" ]; then
      echo "   $st was killed asynchronously. Its number is the count of RECORDS in" >&2
      echo "   its journal, and that is NOT a lower bound on what reached the host:" >&2
      echo "   the writer can die between the flush and the call, so the count may" >&2
      echo "   be one too high, and a truncated final record cannot be attributed" >&2
      echo "   to an arm, so it may be one too low. No bound is claimed." >&2
    fi
  done
  echo "   This run is NOT a complete latency result and NO case in it may be" >&2
  echo "   quoted as performance. See $results/${label}_perf_counts.json for" >&2
  echo "   which stage, and $results/${label}_paired.json for its classification." >&2
  return "$S2PERF_INCOMPLETE"
}

# s2perf_stage_record <label> <results> <journals> [failed-stage...]
#
# THE canonical stage-count record for this run: written to
# `<label>_perf_counts.json` and, from the SAME call, exported into this shell as
# PERF_<arm>_<stage>, PERF_COUNTS_EXACT, PERF_EXACT_<stage> and PERF_FAILED_STAGES.
# The counter, the reconciliation and the artefact all read that one computation.
#
# The failed-stage list is the caller's knowledge and cannot be recovered from disk:
# an absent pilot artefact is the legitimate "no pilot at this rung" state, so only
# the exit status the caller watched tells that from a pilot that was killed after
# issuing 200 requests.
s2perf_stage_record() {
  local label="$1" results="${2:-results}" journals="$3"; shift 3
  local flags="" st shell_form
  for st in "$@"; do flags="$flags --stage-failed $st"; done
  # shellcheck disable=SC2086
  shell_form="$(uv run python -m bench.perf_counts --label "$label" \
    --results "$results" --journals "$journals" $flags \
    --out "$results/${label}_perf_counts.json" --emit-shell)" || return 1

  # EVERY LINE IS VALIDATED BEFORE IT IS EVALUATED.
  #
  # This text is derived from JSON artefacts on disk. One of the fields it carried,
  # `attempt_evidence`, was taken from an artefact verbatim — so an artefact
  # containing `clean; some-command` produced a line that `eval` would execute.
  # bench/perf_counts.py now emits only values from a closed set, and this refuses
  # anything that is not NAME=<digits|word> regardless. Two independent checks,
  # because the consequence of missing one is arbitrary command execution from a
  # file this harness only ever meant to read.
  local line
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    case "$line" in
      PERF_FAILED_STAGES=\'*\') ;;                 # a quoted, space-separated list
      [A-Z_][A-Za-z0-9_]*=[A-Za-z0-9_]*) ;;        # digits or a bare word
      *)
        echo "s2perf_stage_record: refusing to evaluate a stage-record line that is" >&2
        echo "  not a plain assignment: $line" >&2
        echo "  This text comes from artefacts on disk; anything else in it is not" >&2
        echo "  data this harness will execute." >&2
        return 1 ;;
    esac
  done <<EOF
$shell_form
EOF

  eval "$shell_form" || return 1
  [ -n "${PERF_COUNTS_EXACT:-}" ] || return 1
}

# s2perf_reconcile <label> <results> <journals> <arm>...
#
# Does the COUNTER agree with the EVIDENCE? Every stage's recorded number is checked
# against the artefact or journal it was supposed to come from:
#
#   contract              len(all_cases()) per arm, the derived figure
#   symmetric_warmup      the pass's own record
#   latency, noise_pilot  the samples or the journal
#
# A mismatch is an accounting fault and is reported as one. This is what catches a
# stage recorded twice — the case that put 64 extra into the contract counter without
# a single extra request reaching the host, and which every other check passed,
# because each stage's own evidence was perfectly correct.
s2perf_reconcile() {
  local label="$1" results="${2:-results}" journals="$3"; shift 3
  local arm stage want got bad=0 contract_n v
  contract_n="$(uv run python -c '
import sys; sys.path.insert(0, ".")
from bench.contract_cases import all_cases
print(len(all_cases()))')" || return 1
  for arm in "$@"; do
    got="$(request_count "$arm" contract)"
    if [ "$got" != "$contract_n" ]; then
      echo "REQUEST ACCOUNTING INCONSISTENT: $arm contract counter says $got, the" >&2
      echo "  case list says $contract_n. The counter and the evidence disagree." >&2
      bad=1
    fi
    for stage in symmetric_warmup latency noise_pilot; do
      # From the canonical record, not from a fresh call with different flags. A
      # reconciliation that recomputes its own expectation can agree with itself
      # while both halves are wrong — which is exactly what happened: 0 was
      # compared against 0 for a pilot whose journal held its attempts.
      v="PERF_${arm}_${stage}"
      want="${!v:-}"
      got="$(request_count "$arm" "$stage")"
      if [ -n "$want" ] && [ "$got" != "$want" ]; then
        echo "REQUEST ACCOUNTING INCONSISTENT: $arm $stage counter says $got, its" >&2
        echo "  own artefact says $want." >&2
        bad=1
      fi
    done
  done
  [ "$bad" -eq 0 ]
}

# ------------------------------------------------------------------ the tail ---

# s2perf_finish <label> <results> <journals> <budget-arm> <latency-rc> <pilot-rc>
#
# Everything after the last request: counting, finalisation, the ceiling assertion,
# the exactness gate and the closing statement. Returns, in this precedence:
#
#   $D1_FINALIZE_FAILED           the measurements are complete, the artefacts are not
#   $S2PERF_ACCOUNTING_INCONSISTENT the counters disagree with the evidence
#   $S2PERF_CEILING_EXCEEDED      an EXACT attempt count exceeds the authorisation
#   $S2PERF_INCOMPLETE            a stage did not finish; no exact attempt count
#   <latency-rc>                  the gate returned a verdict other than PASS
#   0                             PASS
#
# The order is not arbitrary, and it matches the implementation below.
#
#   * artefacts not written -> the run cannot be reported at all;
#   * counters disagree with evidence -> an unreconciled counter is not a
#     measurement of anything, so comparing it against an authorisation would be
#     comparing the wrong number. It comes BEFORE the ceiling for that reason, and
#     it makes no claim about what reached the host;
#   * over the ceiling -> "we issued more than we were allowed to" outranks "we are
#     unsure exactly how many". This rank is only reachable when the attempt count
#     is EXACT: an inexact count cannot demonstrate a breach, so it is never
#     compared against the ceiling and falls through to the rank below;
#   * a stage did not finish -> no exact attempt count, so no latency result;
#   * only then, the gate's own verdict.
#
# `run_controlled.sh` used to reach none of this on a non-PASS gate, because `set -e`
# ended the run at the gate — a REGRESSION, which is a RESULT, skipped the counting,
# the finalisation and the report and surfaced as a bare non-zero exit. That is the
# c2d and d1a failure shape for a third time.
s2perf_finish() {
  local label="$1" results="${2:-results}" journals="$3" budget="$4" latency_rc="$5"
  local pilot_rc="${6:-0}"
  local rc exact

  # The canonical record first, carrying whatever the caller knows about failed
  # stages, and everything below reads it: the counter, the reconciliation and the
  # artefact are one computation. Built BEFORE the counts are recorded, because the
  # counter is the first consumer.
  local failed=""
  [ "$pilot_rc" -ne 0 ] && failed="noise_pilot"
  # shellcheck disable=SC2086
  if ! s2perf_stage_record "$label" "$results" "$journals" $failed; then
    echo "s2perf_finish: the canonical stage record could not be built; refusing to" >&2
    echo "  record counts from an answer this run does not have." >&2
    return 1
  fi
  if [ -n "$failed" ]; then
    echo
    echo "the noise pilot exited $pilot_rc: it did not complete, so the request" >&2
    echo "  numbers below are not an exact attempt count, and no noise floor may" >&2
    echo "  be quoted from this run." >&2
  fi

  s2perf_record_counts "$label" "$results" "$journals" || return 1

  # Save and restore errexit rather than assuming it was on. A bare `set -e` here
  # turned it ON for a caller that had it off — the driver test — and the next
  # non-zero command ended that test silently, mid-run, with no failing assertion.
  local had_e=no
  case "$-" in *e*) had_e=yes ;; esac
  set +e
  finalize_run "$label" "$results" s2perf
  rc=$?
  [ "$had_e" = yes ] && set -e
  if [ "$rc" -ne 0 ]; then
    echo
    echo "S2 performance classification: INVALID_POST_MEASUREMENT_HARNESS"
    echo "  The gate's result IS recorded in $results/${label}_paired.json."
    return "$rc"
  fi

  # From the canonical record built above. Not a second call with its own flags.
  # TWO answers, not one: whether the attempt count may be compared against an
  # authorisation, and whether there is a measurement to report.
  exact="$PERF_COUNTS_EXACT"
  local measured="${PERF_MEASUREMENT_COMPLETE:-no}"

  echo
  echo "== requests recorded (attempts, not successes) =="
  request_counts_report candidate reference

  # Do the counters agree with the evidence? Asked BEFORE the ceiling, because a
  # counter that disagrees with its evidence cannot be compared against anything —
  # and because the two findings are different. A duplicate count owner issues no
  # HTTP; reporting it as a ceiling breach would claim traffic that never happened.
  if ! s2perf_reconcile "$label" "$results" "$journals" candidate reference; then
    printf '{\n  "kind": "s2perf_classification",\n  "label": "%s",\n  "classification": "REQUEST_ACCOUNTING_INCONSISTENT",\n  "request_total_exact": false,\n  "counted_per_arm": {"candidate": %s, "reference": %s},\n  "meaning": "%s",\n  "not": "%s"\n}\n' \
      "$label" "$(request_arm_total candidate)" "$(request_arm_total reference)" \
      "the run request counter disagrees with the evidence the stages left behind" \
      "NOT a demonstrated ceiling breach: a miscount issues no HTTP, and no claim is made here about how many requests reached the host" \
      > "$results/${label}_perf_classification.json"
    echo "   This run has no reportable request total and no quotable result." >&2
    return "$S2PERF_ACCOUNTING_INCONSISTENT"
  fi

  # THE CEILING IS ONLY CHECKABLE AGAINST AN EXACT COUNT.
  #
  # A journal written before each call bounds nothing once its writer is killed
  # asynchronously — it can be one too high (a record for a request never sent) and
  # one too low (a truncated final record) at once. Comparing such a number against
  # an authorisation and calling the result a breach would report traffic that may
  # never have happened, which is the same error as calling a miscount traffic.
  #
  # So an inexact run is NOT ceiling-checked at all. It records what its journals
  # hold, the exactness flag and the authorised ceiling, and is classified
  # INCOMPLETE_STAGE_FAILURE below. A CEILING_EXCEEDED verdict requires either an
  # exact attempt count or a formally derived lower bound above the ceiling, and no
  # such derivation exists.
  if [ "$exact" != yes ]; then
    echo
    echo "   the request counts are NOT exact, so the authorised ceiling is not" >&2
    echo "   applied to them: a count that bounds nothing cannot demonstrate a" >&2
    echo "   breach. The ceiling is recorded beside them instead." >&2
  elif ! assert_request_ceiling "$budget" candidate reference; then
    printf '{\n  "kind": "s2perf_classification",\n  "label": "%s",\n  "classification": "CEILING_EXCEEDED",\n  "authorised_per_arm": %s,\n  "authorised_total": %s,\n  "issued_per_arm": {"candidate": %s, "reference": %s},\n  "issued_total": %s,\n  "not": "%s"\n}\n' \
      "$label" "$budget" "$((budget * 2))" \
      "$(request_arm_total candidate)" "$(request_arm_total reference)" \
      "$(request_total candidate reference)" \
      "NOT a latency result and NOT a quotable run of any kind: it issued more requests than its authorisation covered" \
      > "$results/${label}_perf_classification.json"
    return "$S2PERF_CEILING_EXCEEDED"
  fi

  if [ "$exact" != yes ] || [ "$measured" != yes ]; then
    echo
    s2perf_classify_incomplete "$label" "$results" "$budget"
    return "$S2PERF_INCOMPLETE"
  fi

  echo
  echo "== S2 single-worker steady-state request-path performance validation =="
  echo "   APPROVED ENGINEERING THRESHOLD: 0.05 — approved by the PI on 2026-08-19"
  echo "   as a DECISION threshold, which is not a claim about the data: it is"
  echo "   not a statistical property of the data, not a confidence guarantee,"
  echo "   and not a production SLA."
  echo "   Every regression verdict above is relative to it."
  echo "   Per case: eight cases, eight ratios, eight bootstraps, eight verdicts."
  echo "   Samples are NEVER pooled across cases and there is no total ratio."
  echo "   IMPROVED is a GATE for the improvement-required cases only"
  echo "   (readme_example, point_profile_multiparam). Any other case whose"
  echo "   interval sits below 1.0 is an OBSERVATION about its measured ratio,"
  echo "   not a case that passed an improvement gate."
  echo "   The interval is the 2.5th-97.5th percentile range of 5,000 bootstrap"
  echo "   ratios, not a proven 95% coverage interval — the samples may be"
  echo "   autocorrelated and no independence argument has been made."
  echo "   NOT a production SLA, not throughput, not multi-worker, not cold-cache,"
  echo "   not PM2/TLS, not a production API measurement."
  echo "   Scope: S2 steady-state latency validation, rung 21 ONLY. This is not"
  echo "   the whole of spec 007 and contains NO startup measurement."
  if [ "$latency_rc" -ne 0 ]; then
    echo
    echo "   the latency gate did not return PASS; its verdict is in"
    echo "   $results/${label}_paired.json and every artefact above is complete."
    return "$latency_rc"
  fi
  return 0
}
