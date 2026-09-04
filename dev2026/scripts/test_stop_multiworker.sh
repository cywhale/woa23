#!/usr/bin/env bash
#
# Cleanup against a multi-worker arbiter — the shape that failed C2 cycle 1.
#
# On 2026-08-10 the candidate arm's gunicorn arbiter took **30 seconds** to exit after
# SIGTERM while `STOP_WAIT_SECS` was 20, so `stop_tracked` reported a survivor and the
# run failed. The arbiter's own log shows why it took 30 s, and it is worth separating
# from what this file tests:
#
#   * gunicorn's `graceful_timeout` **defaults to 30 s** and the arms never set it. We
#     were waiting 20 s for something the library gives 30 s to finish. That mismatch
#     is ours and is real regardless of anything else.
#   * On that occasion the arbiter also hit
#     `RuntimeError: reentrant call inside <_io.BufferedWriter name='<stderr>'>` —
#     SIGCHLD arrived while it was mid-write to the redirected log, `reap_workers()`
#     raised out of the signal handler, and one worker was never reaped. That is a
#     race in gunicorn's logging, not in this harness, and the reference arm in the
#     same cycle did not hit it.
#
# What is tested here is the *cleanup contract*, with a deterministic fixture that
# reproduces the shape — an arbiter that outlives its workers by a controlled delay —
# without depending on gunicorn or on a race:
#
#   1. arbiter exits within the window            -> PASS, state removed
#   2. arbiter outlives the window                -> FAIL, state PRESERVED
#   3. arbiter still alive mid-window             -> reported as a survivor, never clean
#   4. everything gone                            -> empty, never "cannot determine"
#
#     ./scripts/test_stop_multiworker.sh
set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export RUN="$(mktemp -d)"
# shellcheck source=lib_ports.sh
. "$HERE/lib_ports.sh"
# shellcheck source=lib_procs.sh
. "$HERE/lib_procs.sh"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}
# `case` inside a command substitution breaks bash's parser here, as it has several
# times in this suite; a helper keeps it out of `$( )`.
contains() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }
STRAYS=""
cleanup_strays() {
  for p in $STRAYS; do kill -9 "$p" 2>/dev/null || true; done
}
trap cleanup_strays EXIT

# An arbiter that forks two workers and, on SIGTERM, stops them and then takes
# $ARB_EXIT_DELAY seconds to exit itself. That delay is the whole point: it is the
# gap gunicorn's graceful_timeout produced, made deterministic and adjustable.
# The idle wait blocks on a fifo rather than looping over `sleep`. A polling loop
# forks a short-lived child every iteration, so the recorded tree was sometimes four
# processes instead of three — a property of the fixture, not of anything under test,
# and exactly the kind of nondeterminism this file is supposed not to have.
FIXTURE='
  workers=""
  for _ in 1 2; do
    sleep "$W_LIFE" & workers="$workers $!"
  done
  trap "for w in \$workers; do kill \$w 2>/dev/null; done; sleep \$ARB_EXIT_DELAY; exit 0" TERM
  exec 9<>"$FIFO"
  read -u 9 _
'
export W_LIFE=900
export FIFO="$RUN/arbiter.fifo"
mkfifo "$FIFO"

start_arbiter() {           # start_arbiter <name> <exit-delay>
  ARB_EXIT_DELAY="$2" start_tracked "$1" "" bash -c "$FIXTURE" >/dev/null
  sleep 1                   # let the two workers appear
  record_tree "$1" >/dev/null
}

echo "the fixture really is a three-process tree, arbiter plus two workers"
export ARB_EXIT_DELAY=0
start_arbiter fast 0
n="$(tree_pids fast | wc -w | tr -d ' ')"
check "arbiter + 2 workers recorded" "3" "$n"
for p in $(tree_pids fast); do STRAYS="$STRAYS $p"; done

echo
echo "1. the arbiter exits inside the window -> clean, and state is removed"
STOP_WAIT_SECS=5 stop_tracked fast "" >/dev/null 2>&1
rc=$?
check "stop_tracked reports success" "0" "$rc"
check "no survivors remain" "0" \
      "$(for p in $STRAYS; do kill -0 "$p" 2>/dev/null && echo x; done | wc -l | tr -d ' ')"
check "the pidfile was removed" "no" "$([ -f "$RUN/fast.pid" ] && echo yes || echo no)"
check "the tree file was removed" "no" "$([ -f "$RUN/fast.tree" ] && echo yes || echo no)"

echo
echo "2. the arbiter outlives the window -> FAIL, and state is PRESERVED"
STRAYS=""
start_arbiter slow 8
for p in $(tree_pids slow); do STRAYS="$STRAYS $p"; done
arb="$(cat "$RUN/slow.pid")"
out="$(STOP_WAIT_SECS=3 stop_tracked slow "" 2>&1)"
rc=$?
check "stop_tracked reports failure" "1" "$rc"
check "and names the surviving arbiter" "yes" \
      "$(contains "$out" "still running after stop")"
check "the survivor named is the arbiter" "yes" \
      "$(contains "$out" "$arb")"
check "the pidfile is PRESERVED for inspection" "yes" \
      "$([ -f "$RUN/slow.pid" ] && echo yes || echo no)"
check "the tree file is PRESERVED" "yes" "$([ -f "$RUN/slow.tree" ] && echo yes || echo no)"

echo
echo "3. an arbiter that is temporarily alive is never reported clean"
# The workers are already gone by now; only the arbiter remains, which is exactly
# the C2 shape. tree_survivors must report it rather than concluding the tree drained.
surv="$(tree_survivors slow "mid")"
st=$?
check "tree_survivors succeeds (the state IS determinable)" "0" "$st"
check "and reports exactly the arbiter" "$arb" "$(printf '%s' "$surv" | tr -d ' ')"
check "the two workers are gone" "0" \
      "$(for p in $(tree_pids slow); do [ "$p" = "$arb" ] && continue; kill -0 "$p" 2>/dev/null && echo x; done | wc -l | tr -d ' ')"

echo
echo "4. once everything has gone, the answer is empty — not 'cannot determine'"
for i in $(seq 1 30); do kill -0 "$arb" 2>/dev/null || break; sleep 1; done
check "the arbiter has now exited on its own" "no" \
      "$(kill -0 "$arb" 2>/dev/null && echo yes || echo no)"
surv="$(tree_survivors slow "after")"
st=$?
check "tree_survivors still succeeds" "0" "$st"
check "with an empty survivor list" "" "$(printf '%s' "$surv" | tr -d ' ')"
check "no .diag was written — nothing was indeterminate" "no" \
      "$([ -f "$RUN/slow.diag" ] && echo yes || echo no)"

echo
echo "5. a late exit does not retroactively make the earlier stop clean"
# The arbiter is gone now, but the failure already happened and its state is still on
# disk. This is the rule the C2 run turned on: cleanup is judged in its window, and a
# process exiting afterwards does not change the verdict.
check "the preserved pidfile is still there" "yes" \
      "$([ -f "$RUN/slow.pid" ] && echo yes || echo no)"
check "and still names the arbiter that survived" "$arb" "$(cat "$RUN/slow.pid")"

echo
echo "6. the harness waits at least as long as the arms are allowed to take"
# The mismatch that made this reachable: gunicorn's graceful_timeout defaults to 30 s
# and STOP_WAIT_SECS was 20, so the harness gave up before the library was obliged to
# finish. Whatever the arms are configured with, the wait must not be shorter.
default_wait="$(bash -c 'unset STOP_WAIT_SECS; . '"$HERE"'/lib_procs.sh 2>/dev/null; echo "$STOP_WAIT_SECS"')"
check "the arms' graceful timeout is defined once, beside the wait it constrains" \
      "yes" "$([ -n "${ARM_GRACEFUL_TIMEOUT:-}" ] && echo yes || echo no)"
check "STOP_WAIT_SECS ($default_wait) exceeds it ($ARM_GRACEFUL_TIMEOUT)" "yes" \
      "$([ "$default_wait" -gt "$ARM_GRACEFUL_TIMEOUT" ] && echo yes || echo no)"

# Every launch line must take the number from that one definition. Six launches
# each carrying their own literal is six chances for one of them to be edited alone,
# and the one that was wrong would be indistinguishable from the five that were not.
runner="$(cat "$HERE/run_controlled.sh")"
n_var="$(grep -c -- '--graceful-timeout "\$ARM_GRACEFUL_TIMEOUT"' "$HERE/run_controlled.sh")"
n_literal="$(grep -cE -- '--graceful-timeout[= ][0-9]' "$HERE/run_controlled.sh" || true)"
check "there are six arm launches carrying it" "6" "$n_var"
check "and not one of them spells the number out" "0" "$n_literal"
check "the runner asserts the relationship at run time, not just in this file" "yes" \
      "$(contains "$runner" "assert_shutdown_budget || exit")"
check "and records what it actually ran with" "yes" \
      "$(contains "$runner" "_shutdown_budget.json")"
check "the arms' own command lines are checked against it" "yes" \
      "$(contains "$runner" '--expect-graceful-timeout "$ARM_GRACEFUL_TIMEOUT"')"

echo
echo "7. the runtime assertion, which is what a wrong environment actually hits"
# STOP_WAIT_SECS has a default; a default is not what a run used. An exported value
# in the invoking shell silently replaces it, and that is a live path to exactly the
# configuration that failed C2 — so the check is on the effective value.
budget() {                  # budget <env-assignment...> -> rc, message on stdout
  env "$@" bash -c '. '"$HERE"'/lib_procs.sh; assert_shutdown_budget' 2>&1
}
budget_rc() {
  env "$@" bash -c '. '"$HERE"'/lib_procs.sh; assert_shutdown_budget' >/dev/null 2>&1
  echo $?
}
check "the default configuration passes" "0" "$(budget_rc STOP_WAIT_SECS=)"
check "an inherited value below the arms' budget is refused" "1" \
      "$(budget_rc STOP_WAIT_SECS=5)"
check "and equal is refused too — the wait must EXCEED it" "1" \
      "$(budget_rc STOP_WAIT_SECS=10)"
check "one second more is accepted" "0" "$(budget_rc STOP_WAIT_SECS=11)"
check "the refusal reports the effective value, not the default" "yes" \
      "$(contains "$(budget STOP_WAIT_SECS=5)" "STOP_WAIT_SECS=5")"
check "and says the value came from the environment" "yes" \
      "$(contains "$(budget STOP_WAIT_SECS=5)" "source: environment")"
check "an unset value is reported as the default it fell back to" "yes" \
      "$(contains "$(budget STOP_WAIT_SECS=3)" "environment")"
check "a non-numeric value is refused rather than passed to seq" "1" \
      "$(budget_rc STOP_WAIT_SECS=soon)"
check "and says what it received" "yes" \
      "$(contains "$(budget STOP_WAIT_SECS=soon)" "not a number")"
check "an empty value falls back to the default and passes" "0" \
      "$(budget_rc STOP_WAIT_SECS=)"

# The arms' side is fixed on purpose. If the environment could move both sides, the
# inequality would hold trivially against whatever the environment wanted.
check "the arms' budget cannot be moved from the environment" "10" \
      "$(env ARM_GRACEFUL_TIMEOUT=999 bash -c '. '"$HERE"'/lib_procs.sh; echo "$ARM_GRACEFUL_TIMEOUT"')"

echo
suite_summary "$pass" "$fail"
