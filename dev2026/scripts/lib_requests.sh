#!/usr/bin/env bash
#
# Counting HTTP requests, per arm and per stage — attempts, not successes.
#
# Every run so far has declared a request *ceiling* before starting and then had no
# way to say what it actually issued. `process_ready` loops up to thirty times and
# swallows every failure with `|| true`, so a run that took nineteen attempts to
# become ready and a run that took one were indistinguishable afterwards, and both
# C1's and C2's reports had to give a range where a number belonged.
#
# **The counter counts attempts.** It is incremented BEFORE the request is issued,
# not after it succeeds, and that ordering is the whole design:
#
#   * a readiness probe that times out is a request. The socket was opened, the
#     server was asked, and the host saw traffic. Counting only the successful one
#     would report `1` for a start that actually took twenty.
#   * a connection refused is a request too. Nothing answered, but the attempt was
#     made, and a report that omits it understates what this run did to the host.
#   * a retry is not a free request.
#
# Anything that counts on the way out — on a 200, or after a non-empty body — is
# counting successes and calling them requests. `scripts/test_requests.sh` drives
# the real functions against a server that fails, hangs and refuses, and asserts the
# counts against what was actually sent.
#
# Counts live in files rather than shell variables because several of the callers
# run inside `$( )`, and a variable incremented in a subshell is lost when it exits
# — silently, and in the direction that under-reports.
#
# **Measured stages and one derived stage.** `readiness`, `store_probe`,
# `characterization` and `recovery` are counted from the attempts themselves —
# `request_attempt` before each request, or one record per attempt including the
# failed ones. `contract` is DERIVED from the case list by the runner, and that is
# only equal to what was issued while `bench.contract_diff`'s transport does not
# retry and no response is a redirect. Both conditions are pinned behaviourally in
# `bench/test_contract.py`; if either stops holding, that stage must move to a
# transport-level counter rather than staying derived.
#
# Requires $RUN. Sourced, never executed.

: "${REQUEST_COUNTS_DIR:=$RUN/requests}"

#: The stages a request can belong to. A fixed set, because a typo in a stage name
#: would otherwise create a new counter nobody reads and quietly drop those requests
#: out of the total.
REQUEST_STAGES="readiness store_probe contract characterization recovery symmetric_warmup latency noise_pilot startup"

_request_valid() {          # _request_valid <arm> <stage>
  case "$1" in
    ''|*[!A-Za-z0-9_-]*)
      echo "request counter: '$1' is not a usable arm name" >&2; return 1 ;;
  esac
  case " $REQUEST_STAGES " in
    *" $2 "*) return 0 ;;
    *) echo "request counter: '$2' is not one of: $REQUEST_STAGES" >&2; return 1 ;;
  esac
}

# Record that a request is ABOUT TO BE ISSUED. Call it before the request, never
# after, and never conditionally on the outcome.
request_attempt() {         # request_attempt <arm> <stage>
  _request_valid "$1" "$2" || return 1
  request_add "$1" "$2" 1
}

# Add a count measured elsewhere — for a stage whose requests are issued by a Python
# module that already records one line per attempt, including the failed ones.
request_add() {             # request_add <arm> <stage> <n>
  local arm="$1" stage="$2" n="$3" f cur
  _request_valid "$arm" "$stage" || return 1
  case "$n" in
    ''|*[!0-9]*) echo "request counter: '$n' is not a count" >&2; return 1 ;;
  esac
  if ! mkdir -p "$REQUEST_COUNTS_DIR" 2>/dev/null; then
    echo "request counter: cannot create $REQUEST_COUNTS_DIR — the count of" >&2
    echo "  requests this run issues cannot be kept, so the run must not proceed." >&2
    return 1
  fi
  f="$REQUEST_COUNTS_DIR/${arm}.${stage}"
  cur="$(cat "$f" 2>/dev/null || echo 0)"
  case "$cur" in ''|*[!0-9]*) cur=0 ;; esac
  # Checked, not assumed. An unwritable counter is not a cosmetic problem: the run
  # would issue requests it could not account for, and a request total is the one
  # number an authorisation is granted against.
  if ! echo "$((cur + n))" > "$f" 2>/dev/null; then
    echo "request counter: cannot write $f — $n request(s) for $arm/$stage would" >&2
    echo "  go unaccounted. Refusing: a run that cannot count what it issues has" >&2
    echo "  no reportable total and must not proceed." >&2
    return 1
  fi
}

request_count() {           # request_count <arm> <stage>
  local v
  v="$(cat "$REQUEST_COUNTS_DIR/${1}.${2}" 2>/dev/null || echo 0)"
  case "$v" in ''|*[!0-9]*) echo 0 ;; *) echo "$v" ;; esac
}

request_arm_total() {       # request_arm_total <arm>
  local total=0 stage
  for stage in $REQUEST_STAGES; do
    total=$((total + $(request_count "$1" "$stage")))
  done
  echo "$total"
}

request_total() {           # request_total <arm>...
  local total=0 arm
  for arm in "$@"; do total=$((total + $(request_arm_total "$arm"))); done
  echo "$total"
}

# The record. Per arm, per stage, per-arm totals and the grand total — all four,
# because a grand total alone cannot be checked against anything and a per-stage
# breakdown alone leaves the reader to add up numbers the run already knows.
request_counts_json() {     # request_counts_json <label> <out> <arm>...
  local label="$1" out="$2"; shift 2
  local arm stage first_arm=1 first_stage
  {
    printf '{\n  "kind": "request_counts",\n  "label": "%s",\n' "$label"
    printf '  "counts_attempts_not_successes": true,\n'
    printf '  "note": "Incremented before each request is issued. A readiness probe that timed out, a connection that was refused and a retry each count as one request, because each one asked the host.",\n'
    printf '  "stages": ['
    first_stage=1
    for stage in $REQUEST_STAGES; do
      [ "$first_stage" = 1 ] || printf ', '
      printf '"%s"' "$stage"; first_stage=0
    done
    printf '],\n  "per_arm": {\n'
    for arm in "$@"; do
      [ "$first_arm" = 1 ] || printf ',\n'
      printf '    "%s": {' "$arm"
      first_stage=1
      for stage in $REQUEST_STAGES; do
        [ "$first_stage" = 1 ] || printf ', '
        printf '"%s": %s' "$stage" "$(request_count "$arm" "$stage")"
        first_stage=0
      done
      printf ', "total": %s}' "$(request_arm_total "$arm")"
      first_arm=0
    done
    printf '\n  },\n  "total": %s\n}\n' "$(request_total "$@")"
  } > "$out"
}

# assert_request_ceiling <ceiling-per-arm> <arm>...
#
# The authorised ceiling is a LIMIT, not a label. A run that issued more than it was
# authorised to issue has exceeded its authorisation whatever its gates said, and
# must not be able to produce a quotable result — so this fails, loudly, rather than
# printing a number nobody compares against the grant.
#
# It exists because a double count is invisible to every other check: the s2perf path
# recorded the contract stage twice, once in the runner before the gate and once in
# the finaliser, and reported 528 per arm against an authorised 496. Nothing failed.
# Every stage's own test passed, because no test held the ceiling against the total.
assert_request_ceiling() {  # assert_request_ceiling <ceiling-per-arm> <arm>...
  local ceiling="$1"; shift
  local arm n total bad=0
  case "$ceiling" in ''|*[!0-9]*)
    echo "request ceiling: '$ceiling' is not a count" >&2; return 1 ;;
  esac
  for arm in "$@"; do
    n="$(request_arm_total "$arm")"
    if [ "$n" -gt "$ceiling" ]; then
      echo "REQUEST CEILING EXCEEDED: $arm issued $n, authorised $ceiling" >&2
      bad=1
    fi
  done
  total="$(request_total "$@")"
  if [ "$total" -gt "$((ceiling * $#))" ]; then
    echo "REQUEST CEILING EXCEEDED: $total in total, authorised $((ceiling * $#))" >&2
    bad=1
  fi
  if [ "$bad" -ne 0 ]; then
    echo "  The run put more requests on the host than it was authorised to put." >&2
    echo "  It has NO quotable result: not the gate's verdict, not a latency figure," >&2
    echo "  and not a request total, because the authorisation it ran under did not" >&2
    echo "  cover what it did. Report the counts and the ceiling, and nothing else." >&2
    return 1
  fi
  return 0
}

request_counts_report() {   # request_counts_report <arm>...
  local arm stage
  echo "   attempts, not successes — a timeout, a refused connection and a retry"
  echo "   each count as one request, because each one asked the host"
  for arm in "$@"; do
    printf '   %-10s' "$arm"
    for stage in $REQUEST_STAGES; do
      printf ' %s=%s' "$stage" "$(request_count "$arm" "$stage")"
    done
    printf '  TOTAL=%s\n' "$(request_arm_total "$arm")"
  done
  echo "   both arms: $(request_total "$@")"
}
