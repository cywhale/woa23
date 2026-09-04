#!/usr/bin/env bash
#
# `production_stop.sh`'s pid resolver: order-independent, and FAIL CLOSED.
#
# WHY THIS EXISTS. In `bs3v1` this script printed
#     "pm2 reports no running pid for 'woa23-bs3v1-candidate' — nothing to stop."
# and exited 0 WHILE THE SERVICE WAS STILL RUNNING AND HOLDING PORT 18283.
#
# The old parser walked `pm2 jlist` with awk, setting a flag on `"name"` and taking the
# NEXT `"pid"`. pm2 5.4.2 emits `"pid"` FIRST:
#     [{"pid":1709484 , "name":"woa23-bs3v1-candidate" ...
# so the only pid passed before the flag was set, the parser returned empty, and empty
# was read as "there is nothing to stop". A stop path that fails OPEN is worse than one
# that refuses: everything downstream -- the (pid, starttime) identity, the survivor
# check -- is built on a pid it never obtained.
#
# THE RULE UNDER TEST: "nothing to stop" may be reported ONLY when a well-formed listing
# is read and the named app is genuinely absent from it. Every other outcome -- missing
# pid, null pid, pid 0, multiple matches, malformed JSON, unreadable output -- must FAIL
# CLOSED with a non-zero exit.
#
#     ./scripts/test_stop_jlist_parser.sh

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
STOP="$REPO/deploy/production_stop.sh"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then pass=$((pass + 1)); echo "  ok   $1"
  else fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}
has() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

# shellcheck disable=SC1090
WOA23_STOP_LIB_ONLY=1 . "$STOP"

r() { printf '%s' "$1" | jlist_resolve "$2"; }

echo "ORDER INDEPENDENCE — the bs3v1 defect, both ways round"
check "pid BEFORE name resolves (the exact bs3v1 shape)" "OK 1709484" \
      "$(r '[{"pid":1709484,"name":"woa23-bs3v1-candidate","pm2_env":{"status":"online"}}]' woa23-bs3v1-candidate)"
check "name BEFORE pid resolves" "OK 4242" \
      "$(r '[{"name":"app","pid":4242,"pm2_env":{"status":"online"}}]' app)"
check "  fields separated by other keys still resolve" "OK 7" \
      "$(r '[{"pid":7,"pm_id":0,"monit":{"memory":1},"name":"app"}]' app)"
check "  and the right app is picked out of several" "OK 55" \
      "$(r '[{"pid":11,"name":"other"},{"pid":55,"name":"app"},{"pid":99,"name":"third"}]' app)"
check "  regardless of position in the array" "OK 11" \
      "$(r '[{"pid":11,"name":"app"},{"pid":55,"name":"other"}]' app)"

echo
echo "NOTFOUND — the ONLY case that may report nothing to stop"
check "a well-formed listing without the app is NOTFOUND" "NOTFOUND" \
      "$(r '[{"pid":11,"name":"other"}]' app)"
check "  an empty array is NOTFOUND" "NOTFOUND" "$(r '[]' app)"
check "WRONG APP: asking for one name must not return another's pid" "NOTFOUND" \
      "$(r '[{"pid":1709484,"name":"woa23-bs3v1-candidate"}]' woa23)"
check "  a name that is a PREFIX of the real one does not match" "NOTFOUND" \
      "$(r '[{"pid":5,"name":"woa23-bs3v1-candidate"}]' woa23-bs3v1)"
check "  nor a SUFFIX" "NOTFOUND" \
      "$(r '[{"pid":5,"name":"woa23-bs3v1-candidate"}]' candidate)"

echo
echo "FAIL CLOSED — app present but no usable pid"
check "missing pid key is a PROBLEM, not NOTFOUND" yes \
      "$(has "$(r '[{"name":"app","pm2_env":{"status":"online"}}]' app)" 'PROBLEM')"
check "  and says the app exists" yes \
      "$(has "$(r '[{"name":"app"}]' app)" 'exists in the listing but carries no pid')"
check "null pid is a PROBLEM" yes "$(has "$(r '[{"name":"app","pid":null}]' app)" 'PROBLEM')"
check "empty-string pid is a PROBLEM" yes "$(has "$(r '[{"name":"app","pid":""}]' app)" 'PROBLEM')"
# pid 0 needs a FINER distinction than "always a problem", and the existing
# test_production_stop.sh suite was right to insist on it: pm2 represents a CLEANLY
# STOPPED app as pid 0 with status "stopped". That is a POSITIVE statement, and stopping
# an already-stopped app must stay idempotent. Treating it as a problem -- my first
# draft did -- breaks a second `stop` in a row, which is a real operational regression.
#
# The distinction that actually matters is between "pm2 says it is stopped" and
# "I could not determine anything", which is what bs3v1 conflated.
check "pid 0 WITH status stopped is STOPPED, and idempotent" "STOPPED" \
      "$(r '[{"name":"app","pid":0,"pm2_env":{"status":"stopped"}}]' app)"
check "pid 0 with status ONLINE is a PROBLEM — that is a contradiction" yes \
      "$(has "$(r '[{"name":"app","pid":0,"pm2_env":{"status":"online"}}]' app)" 'PROBLEM')"
check "pid 0 with status errored is a PROBLEM" yes \
      "$(has "$(r '[{"name":"app","pid":0,"pm2_env":{"status":"errored"}}]' app)" 'PROBLEM')"
check "pid 0 with NO status at all is a PROBLEM" yes \
      "$(has "$(r '[{"name":"app","pid":0}]' app)" 'PROBLEM')"
check "  and says pid 0 is only credible with status stopped" yes \
      "$(has "$(r '[{"name":"app","pid":0}]' app)" 'only credible with status')"
check "a negative pid is a PROBLEM" yes "$(has "$(r '[{"name":"app","pid":-1}]' app)" 'PROBLEM')"
check "a string pid is a PROBLEM" yes "$(has "$(r '[{"name":"app","pid":"1709484"}]' app)" 'PROBLEM')"
check "a float pid is a PROBLEM" yes "$(has "$(r '[{"name":"app","pid":17.5}]' app)" 'PROBLEM')"

echo
echo "FAIL CLOSED — ambiguity"
check "two apps with the same name is a PROBLEM" yes \
      "$(has "$(r '[{"pid":1,"name":"app"},{"pid":2,"name":"app"}]' app)" 'PROBLEM')"
check "  and it refuses to guess" yes \
      "$(has "$(r '[{"pid":1,"name":"app"},{"pid":2,"name":"app"}]' app)" 'refusing to guess')"
check "  even when one of them has no pid" yes \
      "$(has "$(r '[{"name":"app"},{"pid":2,"name":"app"}]' app)" 'PROBLEM')"

echo
echo "FAIL CLOSED — malformed or unexpected output"
check "malformed JSON is a PROBLEM" yes "$(has "$(r '[{"pid":1,' app)" 'PROBLEM')"
check "  and names it as invalid JSON" yes "$(has "$(r 'not json at all' app)" 'not valid JSON')"
check "empty output is a PROBLEM" yes "$(has "$(r '' app)" 'PROBLEM')"
check "a JSON object (not array) is a PROBLEM" yes \
      "$(has "$(r '{"pid":1,"name":"app"}' app)" 'not a JSON array')"
# The first draft wrote \" inside single quotes, which is a literal backslash -- the
# resolver saw invalid JSON, not a JSON string, so the test passed for the wrong reason.
check "a JSON string is a PROBLEM" yes "$(has "$(r '"hello"' app)" 'not a JSON array')"
check "a JSON number is a PROBLEM" yes "$(has "$(r '42' app)" 'not a JSON array')"
check "an array of nulls does not crash, and is NOTFOUND" "NOTFOUND" "$(r '[null,null]' app)"
check "pm2 warning text before the JSON is a PROBLEM, not a silent pass" yes \
      "$(has "$(r '[PM2][WARN] something
[{"pid":1,"name":"app"}]' app)" 'PROBLEM')"

echo
echo "THE CORE INVARIANT: a parser failure is NEVER 'nothing to stop'"
for bad in '[{"name":"app"}]' '[{"name":"app","pid":null}]' '[{"name":"app","pid":0}]' \
           '[{"name":"app","pid":0,"pm2_env":{"status":"online"}}]' \
           '[{"pid":1,"name":"app"},{"pid":2,"name":"app"}]' 'garbage' '' '{"a":1}'; do
  v="$(r "$bad" app)"
  check "  '$(printf '%.28s' "$bad")' -> not NOTFOUND" no "$([ "$v" = NOTFOUND ] && echo yes || echo no)"
done

echo
echo "SOURCE PROPERTIES — the script's own handling of those verdicts"
code="$(grep -v '^[[:space:]]*#' "$STOP")"
check "the old awk parser is GONE" 0 \
      "$(printf '%s' "$code" | grep -cE 'inapp && /"pid"/' || true)"
check "  and no awk walks the jlist at all" 0 \
      "$(printf '%s' "$code" | grep -cE 'JLIST.*awk|awk.*JLIST' || true)"
check "the resolver is used" 1 "$(printf '%s' "$code" | grep -cE 'jlist_resolve "\$APP"' || true)"
check "NOTFOUND may exit 0 without stopping" 1 \
      "$(printf '%s' "$code" | grep -cE '^[[:space:]]*NOTFOUND\)' || true)"
check "  and STOPPED may too, but ONLY on pm2's positive statement" 1 \
      "$(printf '%s' "$code" | grep -cE '^[[:space:]]*STOPPED\)' || true)"
check "  those are the only two exit-0-without-stopping branches" 2 \
      "$(printf '%s' "$code" | grep -cE '^[[:space:]]*(NOTFOUND|STOPPED)\)' || true)"
check "  a PROBLEM verdict calls die" 1 \
      "$(printf '%s' "$code" | grep -c 'cannot determine the pid for' || true)"
check "  and says it is NOT nothing-to-stop" 1 \
      "$(printf '%s' "$code" | grep -c "NOT reported as 'nothing to stop'" || true)"
check "an empty verdict is refused, not treated as absence" 1 \
      "$(printf '%s' "$code" | grep -c 'the pid resolver produced no verdict at all' || true)"
check "the resolved pid is re-checked as a positive integer" 1 \
      "$(printf '%s' "$code" | grep -cE '\[ "\$PID" -gt 0 \]' || true)"

echo
echo "THE EXISTING SAFETY PROPERTIES ARE UNCHANGED"
check "'all' is still refused" yes "$(has "$code" "'all' would reach every app")"
check "WOA23_PM2_HOME is still required with no default" yes \
      "$(has "$code" 'is required and has no default')"
check "SIGKILL is still never sent" yes "$(has "$code" 'NOT ESCALATING')"
check "  and the policy sentence survives" yes "$(has "$code" 'SIGKILL is not sent here')"
check "identity is still (pid, starttime)" yes "$(has "$code" 'starttime_of')"
check "children still come from /proc, never ps" 0 \
      "$(printf '%s' "$code" | grep -cE 'children_of.*ps |ps -ef' || true)"
check "survivors still fail closed" yes "$(has "$code" 'CLEANUP_FAIL')"
check "no kill -9 anywhere" 0 "$(printf '%s' "$code" | grep -cE 'kill -9|kill -KILL' || true)"

echo
echo "the script parses"
check "bash -n" 0 "$(bash -n "$STOP" >/dev/null 2>&1; echo $?)"
check "no 'case' inside a command substitution" 0 \
      "$(grep -cE '\$\([^)]*case ' "$STOP" || true)"

echo
suite_summary "$pass" "$fail"
