#!/usr/bin/env bash
#
# The offline suite batch, run SERIALLY, keeping everything a failure needs.
#
# It exists because a batch run of `test_procs.sh` failed 3 of 167 assertions twice and
# the loop that ran it printed only each suite's LAST LINE. The failures were real and
# the evidence was thrown away, so neither occurrence can be diagnosed. That is the
# defect this file fixes: a batch that cannot explain its own failure is not a batch
# worth running.
#
# Per suite it keeps: stdout and stderr SEPARATELY, the exit code, start and end time,
# wall duration, the full text of every failing assertion, an environment fingerprint,
# and the processes present before and after — so a stray left by suite N is visible in
# suite N+1's record rather than inferred.
#
# Each suite also gets its OWN TMPDIR under the batch root. Every suite already uses
# `mktemp -d`, so this does not change their behaviour; it makes the directories they
# create traceable to the suite that created them, and it means one suite's temporary
# files cannot be mistaken for another's.
#
#   ./scripts/run_suites.sh                  # all suites
#   ./scripts/run_suites.sh procs cli        # only suites whose name matches
#   WOA23_SUITE_REPEAT=3 ./scripts/run_suites.sh
#
# Artefacts land in a fresh directory printed at the end; nothing is deleted.

set -uo pipefail
# THE SUMMARY CONTRACT, read from the same file the suites write it with. The runner used
# to display each suite's LAST stdout line and the totals were read off that display, which
# is not parsing -- it is looking. `test_staging_store.py` printed its summary and then a
# caveat, so the caveat was displayed and its 24 assertions were invisible;
# `test_bootstrap_delivery.sh` used a private shape and lost 100 more. The batch reported
# 4667 as though it were the total, when the total was 4791.
# shellcheck source=/dev/null
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE" || exit 2

REPEAT="${WOA23_SUITE_REPEAT:-1}"
BATCH_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/woa23-suites-XXXXXX")"

# SERIAL BY CONSTRUCTION. There is no `&`, no `wait`, no job control and no xargs -P
# anywhere in this file: each suite runs to completion before the next starts. That is
# asserted rather than described — `scripts/test_run_suites.sh` greps this file for
# background operators, so the property cannot be lost in an edit.
CONCURRENCY="serial"

python_suites() { ls bench/test_*.py 2>/dev/null; }
shell_suites()  { ls scripts/test_*.sh 2>/dev/null; }

# WOA23_SUITE_SKIP: newline-separated BASENAMES that must not be LAUNCHED at all.
#
# Classifying a result after the fact is not the same as not running the suite, and for
# some suites the difference is destructive: `test_c1_readonly_account.sh` drives
# `run_controlled.sh`, which runs `uv sync --locked --python $PROD_PY` against the SHARED
# `dev2026/.venv` and rebuilds it onto a different interpreter mid-run. A suite that
# rewrites the runtime the other suites are being measured on has to be skipped BEFORE it
# starts, not excused afterwards.
#
# Unset or empty means EVERY suite runs, so the default behaviour is byte-identical to
# before. The caller that sets it is responsible for reporting what it skipped and why --
# nothing here silently drops a suite.
suite_skipped() {
  [ -n "${WOA23_SUITE_SKIP:-}" ] || return 1
  printf '%s\n' "$WOA23_SUITE_SKIP" | grep -qxF -- "$(basename "$1")"
}

select_suites() {
  local all
  all="$(python_suites; shell_suites)"
  if [ -n "${WOA23_SUITE_SKIP:-}" ]; then
    local kept="" s
    while IFS= read -r s; do
      [ -n "$s" ] || continue
      if suite_skipped "$s"; then continue; fi
      kept="$kept$s
"
    done <<< "$all"
    all="$(printf '%s' "$kept")"
  fi
  if [ "$#" -eq 0 ]; then printf '%s\n' "$all"; return; fi
  local pat keep=""
  for pat in "$@"; do
    keep="$keep$(printf '%s\n' "$all" | grep -- "$pat")
"
  done
  printf '%s\n' "$keep" | grep -v '^$' | sort -u
}

#: A snapshot of this user's processes, so a stray from one suite is visible in the
#: next suite's record. Commands with COUNTS and no PIDs: PIDs differ every run, so a
#: PID diff would flag everything, while a count diff shows that N more `sleep 900`
#: exist after the suite than before — which is what "left behind" means.
proc_snapshot() {
  # The snapshot's OWN pipeline is excluded. Without this the `sed` and `sort` below
  # appear in every "after" listing and are reported as leaked by the suite that had
  # just run — a leak detector whose first finding is itself.
  ps -o pid=,command= -u "$(id -un)" 2>/dev/null \
    | grep -vE 'ps -o pid=|grep -vE|sed s/\^|^[[:space:]]*[0-9]+[[:space:]]+(sed|sort)( |$)' \
    | sed 's/^[[:space:]]*//' \
    | awk '{$1=""; sub(/^ /,""); print}' | sort | uniq -c | sed 's/^[[:space:]]*//'
}

fingerprint() {  # fingerprint <suite-tmpdir>
  echo "suite_tmpdir  = $1"
  echo "TMPDIR        = ${TMPDIR:-<unset>}"
  echo "PROC_ROOT     = ${PROC_ROOT:-<unset>}"
  echo "STOP_WAIT_SECS= ${STOP_WAIT_SECS:-<unset>}"
  echo "PYTHONHASHSEED= ${PYTHONHASHSEED:-<unset>}"
  echo "PYTHONPATH    = ${PYTHONPATH:-<unset>}"
  echo "PATH          = $PATH"
  echo "SHELL/bash    = $BASH_VERSION"
  echo "uname         = $(uname -srm)"
  echo "cwd           = $(pwd)"
  echo "ulimit -n     = $(ulimit -n 2>/dev/null)"
  echo "load          = $(uptime | sed 's/.*load/load/')"
}

TOTAL=0; FAILED=0; FAILED_NAMES=""
# THE BATCH TOTALS ARE DERIVED FROM THE SUITE SUMMARIES, one addition per suite, and from
# nothing else. There is no second source for them to disagree with.
ASSERT_TOTAL=0; ASSERT_FAILED=0; CONTRACT_BAD=0; CONTRACT_NAMES=""
MANIFEST="$BATCH_ROOT/manifest.tsv"
printf 'iteration\tsuite\texit\tstart\tend\tseconds\tstdout_bytes\tstderr_bytes\tfail_lines\n' \
  > "$MANIFEST"

# THE BATCH RECORDS ITS OWN SUBJECT. It did not, and that was a real provenance gap:
# results were reported as attesting a particular HEAD with a clean tree, but nothing in
# this file ever asked git anything. The claim was made ABOUT the batch, from outside it,
# which is exactly the kind of unverified premise the rest of this harness refuses.
#
# Tracked and untracked are counted SEPARATELY and both are shown. An untracked scratch
# directory is not the same as a modified source file, and collapsing them into one
# "dirty" number would either cry wolf or hide a real edit. Neither is silently ignored.
GIT_HEAD="$(git rev-parse HEAD 2>/dev/null || echo '<not a git repo>')"
GIT_SUBJ="$(git log -1 --format=%s 2>/dev/null || echo '<none>')"
GIT_TRACKED_DIRTY="$(git status --porcelain --untracked-files=no 2>/dev/null | wc -l | tr -d ' ')"
GIT_UNTRACKED="$(git status --porcelain --untracked-files=all 2>/dev/null \
                 | grep -c '^??' || true)"

echo "batch root : $BATCH_ROOT"
echo "concurrency: $CONCURRENCY"
echo "repeats    : $REPEAT"
echo "git head   : $GIT_HEAD"
echo "git subject: $GIT_SUBJ"
echo "tracked dirty : $GIT_TRACKED_DIRTY   (0 means every tracked file matches the commit)"
echo "untracked     : $GIT_UNTRACKED"
[ "$GIT_TRACKED_DIRTY" = "0" ] || echo "WARNING: the tree does NOT match $GIT_HEAD — this batch does not attest that commit"
echo
{ echo "head=$GIT_HEAD"; echo "subject=$GIT_SUBJ"
  echo "tracked_dirty=$GIT_TRACKED_DIRTY"; echo "untracked=$GIT_UNTRACKED"
  git status --porcelain --untracked-files=all 2>/dev/null
} > "$BATCH_ROOT/git.txt"

for iter in $(seq 1 "$REPEAT"); do
  [ "$REPEAT" -gt 1 ] && echo "===== iteration $iter of $REPEAT ====="
  while IFS= read -r suite; do
    [ -n "$suite" ] || continue
    name="$(basename "$suite")"
    slot="$BATCH_ROOT/iter$iter/$name"
    mkdir -p "$slot/tmp"
    TOTAL=$((TOTAL + 1))

    proc_snapshot > "$slot/procs.before"
    ( TMPDIR="$slot/tmp"; fingerprint "$slot/tmp" ) > "$slot/env.txt"
    start_epoch="$(date +%s)"; start_iso="$(date +%FT%T%z)"

    # The suite runs with its own TMPDIR and its stdout and stderr kept APART. An
    # earlier runner merged them with 2>&1, which is why "the DIAG line reached
    # stop_tracked's output" could not be checked against what actually went where.
    if [ "${suite##*.}" = "py" ]; then
      ( cd "$HERE" && TMPDIR="$slot/tmp" PYTHONPATH=. .venv/bin/python "$suite" ) \
        > "$slot/stdout.txt" 2> "$slot/stderr.txt"
    else
      ( cd "$HERE" && TMPDIR="$slot/tmp" bash "$suite" ) \
        > "$slot/stdout.txt" 2> "$slot/stderr.txt"
    fi
    rc=$?

    end_epoch="$(date +%s)"; end_iso="$(date +%FT%T%z)"
    proc_snapshot > "$slot/procs.after"
    # What this suite LEFT behind: present after, absent before.
    comm -13 "$slot/procs.before" "$slot/procs.after" > "$slot/procs.leaked" 2>/dev/null

    # The full text of every failing assertion, from both streams.
    grep -h -A3 -E '^[[:space:]]*FAIL' "$slot/stdout.txt" "$slot/stderr.txt" \
      > "$slot/failures.txt" 2>/dev/null
    fail_lines="$(grep -c -E '^[[:space:]]*FAIL' "$slot/stdout.txt" "$slot/stderr.txt" \
                  2>/dev/null | awk -F: '{s+=$NF} END {print s+0}')"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$iter" "$name" "$rc" "$start_iso" "$end_iso" \
      "$((end_epoch - start_epoch))" \
      "$(wc -c < "$slot/stdout.txt" | tr -d ' ')" \
      "$(wc -c < "$slot/stderr.txt" | tr -d ' ')" \
      "$fail_lines" >> "$MANIFEST"

    # THE CONTRACT IS CHECKED BEFORE ANYTHING IS COUNTED. A suite whose summary is
    # missing, malformed, duplicated, or displaced by a trailing caveat has its result
    # REFUSED -- it is not silently skipped, and its assertions are not guessed at from
    # whatever else it printed. That refusal fails the batch.
    problem="$(suite_summary_problem "$slot/stdout.txt" "$rc")"
    if [ -n "$problem" ]; then
      CONTRACT_BAD=$((CONTRACT_BAD + 1))
      CONTRACT_NAMES="$CONTRACT_NAMES $name(iter$iter)"
      FAILED=$((FAILED + 1)); FAILED_NAMES="$FAILED_NAMES $name(iter$iter)"
      printf 'exit=%-3d %-34s %4ss  SUMMARY CONTRACT VIOLATION: %s\n' "$rc" "$name" \
        "$((end_epoch - start_epoch))" "$problem"
      echo "         --- last 3 stdout lines ---"
      tail -3 "$slot/stdout.txt" 2>/dev/null | sed 's/^/         /'
      # STDERR TOO. When a suite dies before printing its summary, the reason is almost
      # always on stderr, and a violation report that withholds it sends the reader to the
      # evidence directory for something the runner already had.
      echo "         --- stderr (last 20 lines) ---"
      tail -20 "$slot/stderr.txt" 2>/dev/null | sed 's/^/         /'
      echo "         --- evidence kept at: $slot ---"
      if [ -s "$slot/procs.leaked" ]; then
        echo "         NOTE: this suite left processes behind:"
        sed 's/^/           /' "$slot/procs.leaked" | head -10
      fi
      continue
    fi
    # NOT `set --`: the script's positional parameters are the suite-name filters, and
    # `select_suites "$@"` is re-evaluated on every repeat iteration. Overwriting them here
    # would make `run_suites.sh procs cli` run everything from the second repeat onward.
    counts="$(suite_summary_counts "$slot/stdout.txt")"
    s_assert="${counts%% *}"; s_failed="${counts##* }"
    ASSERT_TOTAL=$((ASSERT_TOTAL + s_assert)); ASSERT_FAILED=$((ASSERT_FAILED + s_failed))

    if [ "$rc" -eq 0 ]; then
      printf 'exit=0   %-34s %4ss  %s\n' "$name" "$((end_epoch - start_epoch))" \
        "$(tail -2 "$slot/stdout.txt" | head -1)   [$s_assert assertions]"
    else
      FAILED=$((FAILED + 1)); FAILED_NAMES="$FAILED_NAMES $name(iter$iter)"
      printf 'exit=%-3d %-34s %4ss  %s\n' "$rc" "$name" \
        "$((end_epoch - start_epoch))" "$(tail -2 "$slot/stdout.txt" | head -1)   [$s_assert assertions, $s_failed failed]"
      echo "         --- failing assertions, in full ---"
      sed 's/^/         /' "$slot/failures.txt" | head -40
      echo "         --- stderr (last 20 lines) ---"
      tail -20 "$slot/stderr.txt" | sed 's/^/         /'
      echo "         --- evidence kept at: $slot ---"
    fi

    if [ -s "$slot/procs.leaked" ]; then
      echo "         NOTE: this suite left processes behind:"
      sed 's/^/           /' "$slot/procs.leaked" | head -10
    fi
  done <<< "$(select_suites "$@")"
done

echo
echo "manifest   : $MANIFEST"
echo "batch root : $BATCH_ROOT   (nothing deleted)"
echo "=== TOTAL: $TOTAL | NON-ZERO: $FAILED ==="
# DERIVED FROM THE SUITE SUMMARIES, and from nothing else. Every suite that contributed to
# these numbers passed the contract check above; every suite that did not is counted in
# CONTRACT-VIOLATIONS and fails the batch, so a violation can never be mistaken for a zero.
echo "=== ASSERTIONS: $ASSERT_TOTAL | ASSERTION FAILURES: $ASSERT_FAILED | CONTRACT-VIOLATIONS: $CONTRACT_BAD ==="
[ -n "$CONTRACT_NAMES" ] && echo "=== CONTRACT VIOLATIONS:$CONTRACT_NAMES ==="
[ -n "$FAILED_NAMES" ] && echo "=== FAILED:$FAILED_NAMES ==="
[ "$FAILED" -eq 0 ] || exit 1
[ "$CONTRACT_BAD" -eq 0 ] || exit 1
exit 0
