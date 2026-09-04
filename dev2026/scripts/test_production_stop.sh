#!/usr/bin/env bash
#
# The identity-based stop path (B1), checked offline against a FAKE /proc and a FAKE pm2.
# No real PM2 is invoked, no real process is signalled, and production is never touched.
#
# The central property under test is the one the grep-based pre_stop got wrong:
# **a process is stopped because of who it is, not because of what its command line says.**
# So the fixtures deliberately include a decoy from another project whose command line
# would match any string-based rule, and the test requires it to be untouched.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STOP="$HERE/deploy/production_stop.sh"
PROD_ECOSYSTEM="$HERE/../conf/ecosystem.config.js"
SIMU="$HERE/../conf/simu.sh"
LEGACY_FIXTURE="$HERE/scripts/fixtures/legacy-pre-stop.fixture.js"
# A HELPER, not `case` inside $( ). This suite asserts elsewhere that no shell file in the
# tree puts `case` in a command substitution -- it breaks under bash 3.2 -- and my first
# version of the fixture check did exactly that. Caught by the rule it would have violated.
not_deployable() {   # not_deployable <path>
  case "$1" in
    */deploy/*|*ecosystem.*.config.js) echo no ;;
    *) echo yes ;;
  esac
}

PASS=0; FAIL=0
check() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}
code() { sed -e 's/^[[:space:]]*#.*$//' "$1"; }

# `has` MUST NOT PIPE INTO `grep -q` — this file runs under `set -o pipefail`.
#
# The bug this replaces, measured rather than reasoned about: `code "$1" | grep -qF -- "$2"`
# made `grep -q` exit 0 the moment it matched, which closed the pipe, which killed `sed`
# with SIGPIPE. Under `pipefail` the pipeline then reports the SIGPIPE status:
#
#     code "$STOP" | grep -qF -- stat   ->  status 141   (grep matched 21 times)
#
# So the pipeline FAILED precisely BECAUSE the pattern was found early. Every
# `has ... -> expect yes` assertion in this suite was therefore unpassable, and
# `-> expect no` assertions returned the right answer for the wrong reason: grep read to
# EOF, sed exited 0, and no SIGPIPE occurred. `it reads starttime from /proc stat` is the
# only `yes` case here, which is why exactly one assertion failed while its three
# neighbours passed.
#
# The fix materialises the filtered text first, so nothing can be killed by a closing
# reader. THE ASSERTION IS UNCHANGED: the stop script must still contain `stat`, and a
# stop script that stopped reading starttime from /proc would still fail this suite.
has() {
  local _text
  _text="$(code "$1")"
  case "$_text" in
    *"$2"*) echo yes ;;
    *)      echo no  ;;
  esac
}

WORK="$(mktemp -d)"
cleanup() { chmod -R u+w "$WORK" 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

# ------------------------------------------------------------------ a synthetic /proc
# BOTH FILES, because the script now reads both and for different reasons: ppid comes from
# the LABELLED `PPid:` line of `status`, which cannot shift no matter what `comm` contains,
# and starttime comes from field 22 of `stat`, read only after the parenthesised comm has
# been removed. A fixture that wrote `stat` alone would test half of the real read path and
# would have gone green on a script that could no longer find any child at all.
mkproc() {   # mkproc <root> <pid> <ppid> <starttime> <comm>
  mkdir -p "$1/$2"
  printf '%s (%s) S %s 0 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 %s 0 0\n' "$2" "$5" "$3" "$4" \
    > "$1/$2/stat"
  printf 'Name:\t%s\nState:\tS (sleeping)\nTgid:\t%s\nPid:\t%s\nPPid:\t%s\n' \
    "$5" "$2" "$2" "$3" > "$1/$2/status"
}
FP="$WORK/proc"
mkproc "$FP" 4296 1    14214 gunicorn     # our master
mkproc "$FP" 5040 4296 15825 gunicorn     # our worker
mkproc "$FP" 5041 4296 15829 gunicorn     # our worker
# The decoy: ANOTHER project's process, same program name, not our child. A command-line
# rule would kill it. An identity rule must never see it.
mkproc "$FP" 9999 1    99999 gunicorn

# ------------------------------------------------------------------------- a fake pm2
# Records what it was asked to do, so the test can assert the ARGUMENTS as well as the
# outcome — 'stop woa23' must never become 'stop all'.
PM2LOG="$WORK/pm2.log"
FAKEPM2="$WORK/pm2"
cat > "$FAKEPM2" <<'PM2EOF'
#!/usr/bin/env bash
echo "$@" >> "$PM2LOG"
case "$1" in
  jlist) cat "$JLIST_FILE" ;;
  stop)  if [ -n "${STOP_EFFECT:-}" ]; then bash -c "$STOP_EFFECT"; fi; exit 0 ;;
esac
PM2EOF
chmod +x "$FAKEPM2"

JLIST_RUNNING="$WORK/jlist-running.json"
cat > "$JLIST_RUNNING" <<'JEOF'
[{"name":"woa23","pm_id":0,"pid":4296,"pm2_env":{"status":"online"}}]
JEOF
JLIST_STOPPED="$WORK/jlist-stopped.json"
cat > "$JLIST_STOPPED" <<'JEOF'
[{"name":"woa23","pm_id":0,"pid":0,"pm2_env":{"status":"stopped"}}]
JEOF

run_stop() {   # run_stop <app> <jlist> <stop-effect> [extra env...]
  local app="$1" jlist="$2" effect="$3"; shift 3
  : > "$PM2LOG"
  # The B1 grant is supplied here because every case below is testing something OTHER
  # than the grant. The grant's own behaviour -- refused when missing, empty, wrong, or
  # accompanied by another run's grant -- is tested on its own further down.
  env PM2LOG="$PM2LOG" JLIST_FILE="$jlist" STOP_EFFECT="$effect" \
      WOA23_B1_GRANTED=yes \
      WOA23_PM2_HOME="$WORK" WOA23_PM2_BIN="$FAKEPM2" PROC_ROOT="$FP" \
      WOA23_STOP_GRACE="${GRACE_OVERRIDE:-3}" "$@" \
      bash "$STOP" "$app" 2>&1
}

echo "the stop script exists and parses"
check "production_stop.sh exists" "yes" "$([ -f "$STOP" ] && echo yes || echo no)"
check "it is valid bash" "yes" "$(bash -n "$STOP" 2>/dev/null && echo yes || echo no)"
check "it is executable" "yes" "$([ -x "$STOP" ] && echo yes || echo no)"

echo
echo "B1 — it never matches a process by its command line"
check "no 'ps -ef' anywhere" "no" "$(has "$STOP" 'ps -ef')"
check "no grep of a process listing" "no" "$(has "$STOP" 'ps -eo')"
check "no 'kill -9' / SIGKILL" "no" "$(has "$STOP" 'kill -9')"
check "no '-KILL' either" "no" "$(has "$STOP" '-KILL')"
check "it reads starttime from /proc stat" "yes" "$(has "$STOP" 'stat')"
# THE PRODUCTION CONFIG IS NOW CLEAN, and that is what is asserted. It used to be
# asserted DIRTY -- "the defect is real" -- which was true and useful while the hook was
# still in place. Stage B removed it on VM24 and the repository source was reconciled to
# match, so asserting its presence would now assert a state we deliberately left behind.
# EXISTENCE IS ASSERTED FIRST, and this is not ceremony. `conf/` is OUTSIDE the subject
# archive -- the tree ships `dev2026/` only -- so on an extracted subject this file is
# simply not there. The old assertion expected "yes" and would have gone red loudly if it
# ever went missing. The flipped one expects "no", which a MISSING FILE also produces: it
# would pass while checking nothing. Asserting existence first is what keeps the flip from
# quietly becoming a vacuous pass.
check "the production config is present to be checked at all" "yes" \
      "$([ -f "$PROD_ECOSYSTEM" ] && echo yes || echo no)"
check "  it is non-empty" "yes" \
      "$([ -s "$PROD_ECOSYSTEM" ] && echo yes || echo no)"
check "  and it still defines the woa23 app (so we are reading the right file)" "yes" \
      "$(grep -qF "name: 'woa23'" "$PROD_ECOSYSTEM" && echo yes || echo no)"
check "the production config no longer carries pre_stop" "no" \
      "$(grep -qE 'pre_stop' "$PROD_ECOSYSTEM" && echo yes || echo no)"
check "  and carries no kill -9 at all" "no" \
      "$(grep -qF 'kill -9' "$PROD_ECOSYSTEM" && echo yes || echo no)"
# The pattern itself is not lost: it lives in one fixture, which is what the refusal guard
# is driven with. Deleting the assertion outright would have deleted the knowledge with it.
check "the historical pattern is preserved in the regression fixture" "yes" \
      "$([ -f "$LEGACY_FIXTURE" ] && grep -qF 'kill -9' "$LEGACY_FIXTURE" && echo yes || echo no)"
check "  and the fixture is not deployable (outside deploy/, not an ecosystem config)" "yes" \
      "$(not_deployable "$LEGACY_FIXTURE")"
# conf/simu.sh is UNTOUCHED and still documents the same technique against another
# project. That is a separate issue, deliberately not addressed by Stage B, and it is
# still asserted so it cannot quietly disappear.
check "conf/simu.sh is present to be checked" "yes" "$([ -f "$SIMU" ] && echo yes || echo no)"
check "conf/simu.sh STILL documents killing ANOTHER project by grep (separate issue)" "yes" \
      "$(grep -qF 'tide_app' "$SIMU" && echo yes || echo no)"

echo
echo "B1 — the grant is checked BEFORE anything else, including the app name"
# Ordering matters and is asserted, not assumed: a grant checked after argument parsing
# would still refuse, but it would already have read whatever it was pointed at. These
# invocations pass NO grant, so each must fail on the grant and not on the later rule.
G="WOA23_B1_GRANTED=yes"
out="$(bash "$STOP" 2>&1)"
check "no grant, no app name -> refused for the GRANT, not the usage" "yes" \
      "$(echo "$out" | grep -qF "WOA23_B1_GRANTED is not 'yes'" && echo yes || echo no)"
out="$(bash "$STOP" all 2>&1)"
check "no grant, app 'all'   -> still refused for the grant first" "yes" \
      "$(echo "$out" | grep -qF "WOA23_B1_GRANTED is not 'yes'" && echo yes || echo no)"
out="$(env WOA23_B1_GRANTED= bash "$STOP" woa23 2>&1)"
check "an EMPTY grant is refused" "yes" \
      "$(echo "$out" | grep -qF "WOA23_B1_GRANTED is not 'yes'" && echo yes || echo no)"
out="$(env WOA23_B1_GRANTED=1 bash "$STOP" woa23 2>&1)"
check "a WRONG value is refused" "yes" \
      "$(echo "$out" | grep -qF "WOA23_B1_GRANTED is not 'yes'" && echo yes || echo no)"
out="$(env WOA23_B1_GRANTED=YES bash "$STOP" woa23 2>&1)"
check "  and it is case-sensitive" "yes" \
      "$(echo "$out" | grep -qF "WOA23_B1_GRANTED is not 'yes'" && echo yes || echo no)"
out="$(env WOA23_PM2C_GRANTED=yes bash "$STOP" woa23 2>&1)"
check "the STAGING grant does NOT substitute for it" "yes" \
      "$(echo "$out" | grep -qF "WOA23_B1_GRANTED is not 'yes'" && echo yes || echo no)"
check "  and the refusal says a staging grant never authorised a stop" "yes" \
      "$(echo "$out" | grep -qF "has never authorised a stop" && echo yes || echo no)"
for g in WOA23_PM2C_GRANTED WOA23_S2PERF_GRANTED WOA23_D1_GRANTED; do
  out="$(env WOA23_B1_GRANTED=yes "$g=yes" bash "$STOP" woa23 2>&1)"
  check "  B1 grant + $g together is refused" "yes" \
        "$(echo "$out" | grep -qF "is set alongside WOA23_B1_GRANTED" && echo yes || echo no)"
done
out="$(env WOA23_B1_GRANTED=yes WOA23_PM2_HOME="$WORK" WOA23_PM2_BIN="$FAKEPM2" \
        JLIST_FILE="$JLIST_STOPPED" PM2LOG="$PM2LOG" PROC_ROOT="$FP" bash "$STOP" woa23 2>&1)"
check "the grant is CONSUMED, not passed on" "yes" \
      "$(grep -qF 'unset WOA23_B1_GRANTED' "$STOP" && echo yes || echo no)"

echo
echo "B1 — a stop is for ONE named app, under an explicit PM2_HOME"
out="$(env $G bash "$STOP" 2>&1)"
check "no app name is refused" "yes" \
      "$(echo "$out" | grep -qF "usage: production_stop.sh" && echo yes || echo no)"
out="$(env $G bash "$STOP" all 2>&1)"
check "'all' is refused by name" "yes" \
      "$(echo "$out" | grep -qF "refusing app name 'all'" && echo yes || echo no)"
out="$(env $G bash "$STOP" 'woa*' 2>&1)"
check "a wildcard is refused too" "yes" \
      "$(echo "$out" | grep -qF "refusing app name" && echo yes || echo no)"
out="$(env $G bash "$STOP" woa23 2>&1)"
check "a missing PM2_HOME is refused, not defaulted to production's" "yes" \
      "$(echo "$out" | grep -qF "WOA23_PM2_HOME is required" && echo yes || echo no)"
check "and the refusal says why that matters" "yes" \
      "$(echo "$out" | grep -qF "production's daemon" && echo yes || echo no)"

echo
echo "graceful stop: the recorded tree exits, and the script proves it"
# The stop 'succeeds': master and both workers vanish from the fake /proc.
out="$(run_stop woa23 "$JLIST_RUNNING" "rm -rf '$FP/4296' '$FP/5040' '$FP/5041'")"; rc=$?
check "it exits 0" "0" "$rc"
check "it identified the master with a starttime" "yes" \
      "$(echo "$out" | grep -qE "master   : pid=4296 starttime=14214" && echo yes || echo no)"
check "and both workers, from /proc by ppid" "2" \
      "$(echo "$out" | grep -c "worker   : pid=")"
check "it reports the whole tree gone" "yes" \
      "$(echo "$out" | grep -qF "STOPPED: every recorded process is gone" && echo yes || echo no)"
check "pm2 was asked to stop the NAMED app" "yes" \
      "$(grep -qx "stop woa23" "$PM2LOG" && echo yes || echo no)"
check "and never 'all'" "no" "$(grep -q "stop all" "$PM2LOG" && echo yes || echo no)"

echo
echo "THE CENTRAL PROPERTY — another project's process is never touched"
mkproc "$FP" 4296 1    14214 gunicorn
mkproc "$FP" 5040 4296 15825 gunicorn
mkproc "$FP" 5041 4296 15829 gunicorn
out="$(run_stop woa23 "$JLIST_RUNNING" "rm -rf '$FP/4296' '$FP/5040' '$FP/5041'")"
check "the decoy from another project still exists" "yes" \
      "$([ -d "$FP/9999" ] && echo yes || echo no)"
check "and it is named nowhere in the output" "no" \
      "$(echo "$out" | grep -q "9999" && echo yes || echo no)"

echo
echo "a pid recycled into a DIFFERENT process is not a survivor"
mkproc "$FP" 4296 1    14214 gunicorn
mkproc "$FP" 5040 4296 15825 gunicorn
mkproc "$FP" 5041 4296 15829 gunicorn
# 4296 still exists but with a different starttime: the number was reused by something
# else. Reporting that as a stranded worker would be a false alarm.
out="$(run_stop woa23 "$JLIST_RUNNING" \
  "rm -rf '$FP/5040' '$FP/5041'; printf '4296 (other) S 1 0 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 77777 0 0\n' > '$FP/4296/stat'")"; rc=$?
check "it exits 0 — the recycled pid is not ours" "0" "$rc"
check "and says so rather than claiming a kill" "yes" \
      "$(echo "$out" | grep -qF "belongs to something else" && echo yes || echo no)"


echo
echo "INDETERMINATE — a LIVE process that cannot be identified is NOT a clean stop"
# THE AUDIT FINDING, end to end. Before the fix, a worker whose stat became truncated was
# read as GONE: starttime_of returned rc 0 with an empty string, the survivor test
# `[ -n "$now" ]` failed, and the script printed "every recorded process is gone" and
# exited 0 -- over a process that was still there. Reporting a clean stop over a live
# process is exactly what B1 exists to prevent.
#
# `vanish` is used instead of writing the removal inline, so the effect string stays
# readable and every case removes exactly the paths it names.
vanish() { for d in "$@"; do rm -r "$FP/$d" 2>/dev/null; done; }
export -f vanish 2>/dev/null || true

mkproc "$FP" 4296 1    14214 gunicorn
mkproc "$FP" 5040 4296 15825 gunicorn
mkproc "$FP" 5041 4296 15829 gunicorn
# The master and one worker really do exit. The other worker STAYS, but its stat is
# truncated so its identity cannot be read.
out="$(run_stop woa23 "$JLIST_RUNNING" \
  "rm -r '$FP/4296' '$FP/5040'; printf '5041 (gunicorn) S 4296 0 0\n' > '$FP/5041/stat'")"; rc=$?
check "it does NOT exit 0" "yes" "$([ "$rc" != 0 ] && echo yes || echo no)"
check "  it exits 8 — indeterminate, distinct from CLEANUP_FAIL's 7" "8" "$rc"
check "  and does NOT claim everything is gone" "no" \
      "$(echo "$out" | grep -qF "every recorded process is gone" && echo yes || echo no)"
check "  it says INDETERMINATE" "yes" \
      "$(echo "$out" | grep -qF "INDETERMINATE" && echo yes || echo no)"
check "  it names the process it could not identify" "yes" \
      "$(echo "$out" | grep -qE "pid=5041" && echo yes || echo no)"
check "  it says /proc is present so they are NOT gone" "yes" \
      "$(echo "$out" | grep -qF "they are NOT gone" && echo yes || echo no)"
check "  and it preserved state rather than escalating" "yes" \
      "$(echo "$out" | grep -qF "State is preserved" && echo yes || echo no)"
check "  no SIGKILL was sent" "no" "$(grep -q "kill" "$PM2LOG" && echo yes || echo no)"
check "  the live process is STILL THERE, untouched" "yes" \
      "$([ -d "$FP/5041" ] && echo yes || echo no)"

echo
echo "  the same process with a NON-NUMERIC starttime is equally indeterminate"
mkproc "$FP" 4296 1    14214 gunicorn
mkproc "$FP" 5040 4296 15825 gunicorn
mkproc "$FP" 5041 4296 15829 gunicorn
out="$(run_stop woa23 "$JLIST_RUNNING" \
  "rm -r '$FP/4296' '$FP/5040'; printf '5041 (gunicorn) S 4296 0 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 zzz 0 0\n' > '$FP/5041/stat'")"; rc=$?
check "a non-numeric starttime is indeterminate, not gone" "8" "$rc"

echo
echo "  and a genuinely VANISHED process is still a clean stop"
mkproc "$FP" 4296 1    14214 gunicorn
mkproc "$FP" 5040 4296 15825 gunicorn
mkproc "$FP" 5041 4296 15829 gunicorn
out="$(run_stop woa23 "$JLIST_RUNNING" "rm -r '$FP/4296' '$FP/5040' '$FP/5041'")"; rc=$?
check "all three gone -> exit 0" "0" "$rc"
check "  the distinction is REAL: gone passes, unreadable does not" "yes" \
      "$(echo "$out" | grep -qF "every recorded process is gone" && echo yes || echo no)"

echo
echo "  a partially-unreadable DESCENDANT is not dropped from the recorded tree"
mkproc "$FP" 4296 1    14214 gunicorn
mkproc "$FP" 5040 4296 15825 gunicorn
mkproc "$FP" 5041 4296 15829 gunicorn
# Truncate a worker's stat BEFORE the stop, so the tree cannot even be recorded.
printf '5041 (gunicorn) S 4296 0\n' > "$FP/5041/stat"
out="$(run_stop woa23 "$JLIST_RUNNING" "true")"; rc=$?
check "recording refuses rather than silently omitting it" "yes" \
      "$([ "$rc" != 0 ] && echo yes || echo no)"
check "  and says the starttime could not be read" "yes" \
      "$(echo "$out" | grep -qF "starttime cannot be read" && echo yes || echo no)"
check "  naming the descendant" "yes" "$(echo "$out" | grep -qF "5041" && echo yes || echo no)"
mkproc "$FP" 5041 4296 15829 gunicorn
echo
echo "fail closed: a survivor is CLEANUP_FAIL, never an escalation"
mkproc "$FP" 4296 1    14214 gunicorn
mkproc "$FP" 5040 4296 15825 gunicorn
mkproc "$FP" 5041 4296 15829 gunicorn
out="$(run_stop woa23 "$JLIST_RUNNING" "rm -rf '$FP/5041'")"; rc=$?
check "it exits 7, not 0" "7" "$rc"
check "it says CLEANUP_FAIL" "yes" \
      "$(echo "$out" | grep -qF "CLEANUP_FAIL" && echo yes || echo no)"
check "it names the surviving identity" "yes" \
      "$(echo "$out" | grep -qE "pid=(4296|5040) starttime=" && echo yes || echo no)"
check "it states it is NOT escalating" "yes" \
      "$(echo "$out" | grep -qF "NOT ESCALATING" && echo yes || echo no)"
check "the survivors are still alive — nothing was force-killed" "yes" \
      "$([ -d "$FP/4296" ] && [ -d "$FP/5040" ] && echo yes || echo no)"

echo
echo "an app PM2 reports as stopped is not an error"
out="$(run_stop woa23 "$JLIST_STOPPED" "true")"; rc=$?
check "pid 0 exits 0" "0" "$rc"
check "and says there is nothing to stop" "yes" \
      "$(echo "$out" | grep -qF "nothing to stop" && echo yes || echo no)"
check "no stop was issued for an already-stopped app" "no" \
      "$(grep -q "^stop" "$PM2LOG" && echo yes || echo no)"

echo
suite_summary "$PASS" "$FAIL"
