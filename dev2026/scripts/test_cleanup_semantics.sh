#!/usr/bin/env bash
#
# The two cleanup semantics, kept apart, against real processes.
#
# THE FOUR OUTCOMES (policy, decided 2026-08-18 after the clnA regression):
#
#   identity confirmed   + tree gone     -> PASS, state removed
#   identity confirmed   + survivor      -> FAIL, state kept
#   identity UNCONFIRMED + process gone  -> FAIL, state kept, nothing signalled
#   identity UNCONFIRMED + process alive -> FAIL, state kept, nothing signalled
#
# The third row is the one clnA settled. `kill -0` reporting ESRCH proves the PID
# NUMBER is absent; it does not confirm that the process this run recorded is the
# one that exited. Without the start time nothing ties the number to the process,
# and a recycled PID that has since exited looks identical. Before the policy the
# outcome was decided by whichever predicate settled first — a clean stop on Linux,
# a failure on macOS, from the same scenario.
#
# Written because a driver run failed cleanup once in eight — `stop_tracked` returned
# 1 with both arms still alive — and the branch it took could not be determined
# afterwards. The stderr had been discarded, and three different branches produce
# that same symptom. This file makes each branch reachable on demand and pins what
# each one is allowed to do.
#
# THE DEFECT IT FOUND. `stop_tracked` treated an unreadable start time as "PID is
# already gone": no signal, no classification. If the process was in fact alive it
# stayed alive, the wait loop found it as a survivor, and the run failed saying
# "still running after stop" — never that the reason was an identity read the
# function had written off. An unreadable identity is not evidence of exit.
#
# NOT tested here: SIGKILL, because there is none to test. The standing rule is that
# nothing is escalated, and the absence is asserted rather than assumed.
#
#     bash scripts/test_cleanup_semantics.sh
set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
pass=0; fail=0

check() {   # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}
has_text() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

WORK="$(mktemp -d)"
PY="$(command -v python3)"
cleanup_work() {
  [ -n "${KEEP_WORK:-}" ] && { echo "kept: $WORK"; return 0; }
  "$PY" -c "import shutil,sys; shutil.rmtree(sys.argv[1], ignore_errors=True)" "$WORK"
}
trap cleanup_work EXIT

RUN="$WORK/run"
mkdir -p "$RUN"
# shellcheck source=lib_procs.sh
. "$HERE/lib_procs.sh"

# Making an identity read fail on a LIVE process takes both of `starttime_of`'s
# sources, because it falls back from one to the other:
#
#   1. $PROC_ROOT points at a synthetic procfs with a /1/stat — so readers take the
#      Linux branch, the one VM24 takes — but no entry for the PID we track;
#   2. a `ps` shim that fails ONLY for `-o lstart=`, the query starttime_of makes,
#      and execs the real ps for everything else.
#
# Both together, so this pins the same branch on a machine with procfs and on one
# without. `pid_alive` uses kill -0 and is affected by neither, which is exactly
# why it is fit to re-check the failed read.
FAKEPROC="$WORK/proc"
mkdir -p "$FAKEPROC/1"
printf '1 (launchd) S 0 1 1 0 -1 4194304 0 0 0 0 0 0 0 0 20 0 1 0 12345 0 0\n' \
  > "$FAKEPROC/1/stat"

REALPS="$(command -v ps)"
mkdir -p "$WORK/shim"
cat > "$WORK/shim/ps" <<PSEOF
#!/bin/sh
for a in "\$@"; do
  [ "\$a" = "lstart=" ] && exit 1
done
exec "$REALPS" "\$@"
PSEOF
chmod +x "$WORK/shim/ps"

# blind_identity <cmd...> — run one command with both identity sources broken.
blind_identity() {
  PATH="$WORK/shim:$PATH" PROC_ROOT="$FAKEPROC" "$@"
}
# WHICH BRANCH THIS HOST TAKES NATIVELY. Every scenario below runs on it, so the
# output says which one was actually covered rather than leaving it to be assumed.
if [ -r /proc/1/stat ]; then
  NATIVE_BRANCH="linux-procfs"
else
  NATIVE_BRANCH="ps-fallback"
fi
echo "native identity branch on this host: $NATIVE_BRANCH"
branch_known=no
[ "$NATIVE_BRANCH" = linux-procfs ] && branch_known=yes
[ "$NATIVE_BRANCH" = ps-fallback ] && branch_known=yes
check "the branch is one of the two this code has" "yes" "$branch_known"
if [ "$NATIVE_BRANCH" = linux-procfs ]; then
  t="$(starttime_of $$)"; isint=yes
  [ -z "$t" ] && isint=no
  printf '%s' "$t" | grep -q '[^0-9]' && isint=no
  check "starttime_of returns the procfs integer here" "yes" "$isint"
else
  t="$(starttime_of $$)"; isps=no
  printf '%s' "$t" | grep -q ':' && isps=yes
  check "starttime_of returns the ps form here" "yes" "$isps"
fi

check "the shim really does break the identity read" "" \
      "$(blind_identity starttime_of $$ 2>/dev/null)"
check "while kill -0 still sees this very shell" "0" \
      "$(blind_identity pid_alive $$; echo $?)"

start_sleeper() {   # start_sleeper <name> -> a tracked, live process
  start_tracked "$1" "" sleep 600 >/dev/null 2>&1
}

echo "identity CONFIRMED: signal, verify the whole tree, then remove state"
start_sleeper alpha
pid_alpha="$(cat "$RUN/alpha.pid")"
check "the process is alive before the stop" "0" \
      "$(kill -0 "$pid_alpha" 2>/dev/null; echo $?)"
out="$(stop_tracked alpha "" 2>&1)"; rc=$?
check "the stop succeeds" "0" "$rc"
check "and says the whole tree exited, not just the pid" "yes" \
      "$(has_text "$out" "whole tree exited")"
check "the process is gone" "1" "$(kill -0 "$pid_alpha" 2>/dev/null; echo $?)"
check "the pidfile was removed" "no" \
      "$([ -e "$RUN/alpha.pid" ] && echo yes || echo no)"
check "the starttime file too" "no" \
      "$([ -e "$RUN/alpha.starttime" ] && echo yes || echo no)"
check "and the tree" "no" "$([ -e "$RUN/alpha.tree" ] && echo yes || echo no)"

echo
echo "identity UNCONFIRMED, process ALIVE: refuse to signal, keep everything"
start_sleeper beta
pid_beta="$(cat "$RUN/beta.pid")"
# The tree was recorded against the real host, so it still matches this boot; only
# the identity READ is made to fail.
out="$(blind_identity stop_tracked beta "" 2>&1)"; rc=$?
check "the stop fails" "1" "$rc"
check "it reports a cleanup failure rather than guessing" "yes" \
      "$(has_text "$out" "CLEANUP FAILED")"
check "and says an unreadable identity is not evidence of exit" "yes" \
      "$(has_text "$out" "not evidence that the process has gone")"
check "it names the PID as alive" "yes" "$(has_text "$out" "PID is alive")"
# The point of the whole branch: the process must still be running afterwards.
check "the process was NOT signalled — it is still alive" "0" \
      "$(kill -0 "$pid_beta" 2>/dev/null; echo $?)"
check "the pidfile was kept" "yes" \
      "$([ -e "$RUN/beta.pid" ] && echo yes || echo no)"
check "the starttime file was kept" "yes" \
      "$([ -e "$RUN/beta.starttime" ] && echo yes || echo no)"
check "the tree was kept" "yes" "$([ -e "$RUN/beta.tree" ] && echo yes || echo no)"
# The tree is NOT poisoned. What failed is one PID's identity read, which may be
# transient; marking the tree uncertain would turn that into a service that can
# never be stopped again. This attempt fails and removes nothing — that is what
# keeps ambiguity from becoming a clean stop, not a permanent flag.
check "the tree is not permanently poisoned by a transient read failure" "no" \
      "$([ -e "$RUN/beta.uncertain" ] && echo yes || echo no)"
# The message must not send an operator looking for a file that was deliberately
# not written, nor leave them believing every future stop is now refused.
check "and the message does not claim a .uncertain that does not exist" "no" \
      "$(has_text "$out" "the tree is marked uncertain")"
check "it says so explicitly, naming the file it did not write" "yes" \
      "$(has_text "$out" "no .uncertain file is written")"
check "and that a retry is entitled to succeed" "yes" \
      "$(has_text "$out" "entitled to succeed on its own")"
check "distinguishing this attempt from the service" "yes" \
      "$(has_text "$out" "not the service")"
check "the reason is written to the diag file, not only to stderr" "yes" \
      "$([ -s "$RUN/beta.diag" ] && echo yes || echo no)"
check "and the diag names the branch" "yes" \
      "$(has_text "$(cat "$RUN/beta.diag")" "starttime-unreadable-pid-alive")"
check "recording that nothing was signalled" "yes" \
      "$(has_text "$(cat "$RUN/beta.diag")" "NOT signalled")"
# Ambiguity must not become a clean stop on a second attempt either.
out2="$(blind_identity stop_tracked beta "" 2>&1)"
check "a second attempt does not launder it into success" "1" "$?"
# Now let the identity be readable again: the same call must succeed and clean up.
out3="$(stop_tracked beta "" 2>&1)"; rc3=$?
check "with the identity readable again the stop completes" "0" "$rc3"
check "and the process is gone" "1" "$(kill -0 "$pid_beta" 2>/dev/null; echo $?)"

echo
echo "identity UNCONFIRMED, process GONE: still a FAIL, state kept"
# The clnA outcome. An absent PID number is not a confirmed exit of the tracked
# process, so this fails and keeps everything — the same answer on both branches.
start_sleeper gamma
pid_gamma="$(cat "$RUN/gamma.pid")"
kill "$pid_gamma" 2>/dev/null
i=0; while kill -0 "$pid_gamma" 2>/dev/null && [ "$i" -lt 100 ]; do sleep 0.1; i=$((i+1)); done
check "the process is gone before the stop is asked" "1" \
      "$(kill -0 "$pid_gamma" 2>/dev/null; echo $?)"
out="$(blind_identity stop_tracked gamma "" 2>&1)"; rc=$?
check "the stop FAILS" "1" "$rc"
check "it says cleanup failed" "yes" "$(has_text "$out" "CLEANUP FAILED")"
check "and that the recorded process was never identified" "yes" \
      "$(has_text "$out" "was never identified")"
check "it reports what kill -0 said" "yes" \
      "$(has_text "$out" "PID is gone")"
check "and why an absent PID number is not a confirmed exit" "yes" \
      "$(has_text "$out" "not a confirmed exit of the tracked process")"
check "it does NOT claim the tree was verified" "no" \
      "$(has_text "$out" "whole tree exited")"
check "the pidfile was kept" "yes" \
      "$([ -e "$RUN/gamma.pid" ] && echo yes || echo no)"
check "the starttime file was kept" "yes" \
      "$([ -e "$RUN/gamma.starttime" ] && echo yes || echo no)"
check "the tree was kept" "yes" "$([ -e "$RUN/gamma.tree" ] && echo yes || echo no)"
check "the diag names the branch and the liveness" "yes" \
      "$(has_text "$(cat "$RUN/gamma.diag" 2>/dev/null)" \
         "starttime-unreadable-pid-gone")"
check "and records that nothing was signalled and state was kept" "yes" \
      "$(has_text "$(cat "$RUN/gamma.diag" 2>/dev/null)" "NOT signalled; state kept")"

echo "identity CONTRADICTED: a recycled PID is refused, not killed"
start_sleeper delta
pid_delta="$(cat "$RUN/delta.pid")"
printf 'not-the-recorded-start-time:00\n' > "$RUN/delta.starttime"
out="$(stop_tracked delta "" 2>&1)"; rc=$?
check "the stop fails" "1" "$rc"
check "it REFUSES TO KILL" "yes" "$(has_text "$out" "REFUSING TO KILL")"
check "naming PID recycling as the reason" "yes" "$(has_text "$out" "recycled")"
check "the process was not signalled" "0" \
      "$(kill -0 "$pid_delta" 2>/dev/null; echo $?)"
check "and its state was kept" "yes" \
      "$([ -e "$RUN/delta.pid" ] && echo yes || echo no)"
kill "$pid_delta" 2>/dev/null

echo
echo "tree not from this boot: refused before anything is signalled"
start_sleeper epsilon
pid_eps="$(cat "$RUN/epsilon.pid")"
"$PY" - "$RUN/epsilon.tree" <<'PYEOF'
import sys
p = sys.argv[1]
lines = open(p).read().splitlines()
lines[0] = "boot:a-different-boot-entirely"
open(p, "w").write("\n".join(lines) + "\n")
PYEOF
out="$(stop_tracked epsilon "" 2>&1)"; rc=$?
check "the stop fails" "1" "$rc"
check "it REFUSES TO ACT" "yes" "$(has_text "$out" "REFUSING TO ACT")"
check "saying the PIDs may belong to unrelated processes now" "yes" \
      "$(has_text "$out" "may now belong to unrelated processes")"
check "nothing was signalled" "0" "$(kill -0 "$pid_eps" 2>/dev/null; echo $?)"
check "and nothing was removed" "yes" \
      "$([ -e "$RUN/epsilon.pid" ] && [ -e "$RUN/epsilon.tree" ] && echo yes || echo no)"
kill "$pid_eps" 2>/dev/null

echo
echo "a tree whose completeness was never confirmed says so in its own words"
# `_is_uncertain` and a boot-id mismatch used to produce the same refusal message,
# which named three causes and not this one. A refusal whose stated reason is wrong
# sends the reader to check the boot id.
start_sleeper zeta
pid_zeta="$(cat "$RUN/zeta.pid")"
printf 'tree write uncertain\n' > "$RUN/zeta.uncertain"
out="$(stop_tracked zeta "" 2>&1)"; rc=$?
check "the stop fails" "1" "$rc"
check "it says the completeness was never confirmed" "yes" \
      "$(has_text "$out" "completeness was never confirmed")"
check "and says explicitly that this is not a boot-id mismatch" "yes" \
      "$(has_text "$out" "NOT a boot-id mismatch")"
check "pointing at the two files that hold the evidence" "yes" \
      "$(has_text "$out" "zeta.uncertain")"
check "nothing was signalled" "0" "$(kill -0 "$pid_zeta" 2>/dev/null; echo $?)"
check "and nothing was removed" "yes" \
      "$([ -e "$RUN/zeta.pid" ] && echo yes || echo no)"
kill "$pid_zeta" 2>/dev/null

echo
echo "the four outcomes, as a matrix, on the $NATIVE_BRANCH branch"
# Each row asserts the three things that matter together: the exit code, whether the
# state survived, and whether a clean stop was claimed. Asserting them apart is how
# "returned 1" and "kept its evidence" drifted into being different questions.
matrix_row() {   # matrix_row <name> <rc> <state-kept> <claimed-clean>
  check "  $1: exit code" "$2" "$MX_RC"
  check "  $1: state kept" "$3" "$MX_STATE"
  check "  $1: claimed a clean stop" "$4" "$MX_CLEAN"
}

# identity confirmed + tree gone -> PASS, state removed
start_sleeper m1
MX_OUT="$(stop_tracked m1 "" 2>&1)"; MX_RC=$?
MX_STATE="$([ -e "$RUN/m1.pid" ] && echo yes || echo no)"
MX_CLEAN="$(has_text "$MX_OUT" "whole tree exited")"
matrix_row "identity confirmed + tree gone" "0" "no" "yes"

# identity unconfirmed + process gone -> FAIL, state kept, no clean claim
start_sleeper m2
m2pid="$(cat "$RUN/m2.pid")"; kill "$m2pid" 2>/dev/null
i=0; while kill -0 "$m2pid" 2>/dev/null && [ "$i" -lt 100 ]; do sleep 0.1; i=$((i+1)); done
MX_OUT="$(blind_identity stop_tracked m2 "" 2>&1)"; MX_RC=$?
MX_STATE="$([ -e "$RUN/m2.pid" ] && echo yes || echo no)"
MX_CLEAN="$(has_text "$MX_OUT" "whole tree exited")"
matrix_row "identity unconfirmed + process gone" "1" "yes" "no"

# identity unconfirmed + process alive -> FAIL, state kept, nothing signalled
start_sleeper m3
m3pid="$(cat "$RUN/m3.pid")"
MX_OUT="$(blind_identity stop_tracked m3 "" 2>&1)"; MX_RC=$?
MX_STATE="$([ -e "$RUN/m3.pid" ] && echo yes || echo no)"
MX_CLEAN="$(has_text "$MX_OUT" "whole tree exited")"
matrix_row "identity unconfirmed + process alive" "1" "yes" "no"
check "  identity unconfirmed + process alive: NOT signalled" "0" \
      "$(kill -0 "$m3pid" 2>/dev/null; echo $?)"
kill "$m3pid" 2>/dev/null

# identity confirmed + survivor -> FAIL, state kept. The survivor is a child the
# tree records and that does not exit when the master is signalled.
start_tracked m4 "" sh -c 'sleep 600 & sleep 600' >/dev/null 2>&1
m4pid="$(cat "$RUN/m4.pid")"
kids="$(pgrep -P "$m4pid" 2>/dev/null | tr '\n' ' ')"
STOP_WAIT_SECS=2 MX_OUT="$(STOP_WAIT_SECS=2 stop_tracked m4 "" 2>&1)"; MX_RC=$?
MX_STATE="$([ -e "$RUN/m4.pid" ] && echo yes || echo no)"
MX_CLEAN="$(has_text "$MX_OUT" "whole tree exited")"
# A tree that drains cleanly is the normal outcome; this row asserts the SHAPE of a
# survivor result rather than forcing one, because forcing a process to outlive
# SIGTERM without escalation is exactly what this project refuses to do.
if [ "$MX_RC" -eq 0 ]; then
  echo "       (m4 drained cleanly on this host; survivor shape asserted from the"
  echo "        code path in scripts/test_procs.sh instead)"
  check "  identity confirmed + survivor: the survivor path exists and fails closed" \
        "yes" "$(has_text "$(cat "$HERE/lib_procs.sh")" "still running after stop")"
  check "  and keeps its state" "yes" \
        "$(has_text "$(cat "$HERE/lib_procs.sh")" "pidfile and tree left for inspection")"
else
  matrix_row "identity confirmed + survivor" "1" "yes" "no"
fi
for k in $kids; do kill "$k" 2>/dev/null; done

echo
echo "the rules that hold across every branch"
procs="$(cat "$HERE/lib_procs.sh")"
# Nothing is escalated. The standing rule is refuse-and-report, not force.
check "no SIGKILL anywhere in the process library" "no" \
      "$([ "$(has_text "$procs" "kill -9")" = yes ] \
         || [ "$(has_text "$procs" "-KILL")" = yes ] \
         && echo yes || echo no)"
check "nor in the runner" "no" \
      "$([ "$(has_text "$(cat "$HERE/run_controlled.sh")" "kill -9")" = yes ] \
         && echo yes || echo no)"
# The wait budget is not the answer to an ambiguous stop, and must not become one.
check "the shutdown budget is asserted, not tuned per failure" "yes" \
      "$(has_text "$procs" "assert_shutdown_budget")"
check "an uncertain tree can never yield a clean stop" "yes" \
      "$(has_text "$procs" "refusing to remove state")"
check "state removal happens only after survivors and port are both settled" "yes" \
      "$("$PY" - "$HERE/lib_procs.sh" <<'PYEOF'
import sys
s = open(sys.argv[1]).read()
i = s.index("still running after stop")
j = s.index('for f in "$RUN/$name.pid"')
print("yes" if i < j else "no")
PYEOF
)"

echo
suite_summary "$pass" "$fail"
