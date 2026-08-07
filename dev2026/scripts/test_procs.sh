#!/usr/bin/env bash
#
# Offline tests for scripts/lib_procs.sh, using real processes.
#
# The central case cannot be tested with fixtures: a parent that dies while the
# child it forked keeps running. So this builds exactly that — a "master" that
# forks a child, then exits — and asserts that stop_tracked notices, keeps its
# state files and returns non-zero.
#
# No ports, no network, no privileges. Port state is stubbed, so this runs on any
# machine including the development laptop.
#
#     ./scripts/test_procs.sh

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export RUN="$(mktemp -d)"
STRAYS=""
cleanup_strays() {
  local p
  for p in $STRAYS; do kill -9 "$p" 2>/dev/null || true; done
}
trap cleanup_strays EXIT

# `case` inside a command substitution confuses bash's parser, so word/substring
# membership gets its own function.
contains() { case " $1 " in *" $2 "*) echo yes ;; *) echo no ;; esac; }
has_text() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

pass=0; fail=0
check() {                   # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}

# Port state is not what is under test here; stub it so the tree logic is isolated.
PORT_IS_FREE=yes
port_released() { [ "$PORT_IS_FREE" = "yes" ]; }
pid_holds_port() { return 0; }

# shellcheck source=lib_procs.sh
. "$HERE/lib_procs.sh"

echo "the tree is discovered, not assumed"
# A parent that forks a child and waits, like a gunicorn arbiter.
bash -c 'sleep 30 & echo $! > "$RUN/child.pid"; sleep 30' &
parent=$!; STRAYS="$STRAYS $parent"
sleep 1
child="$(cat "$RUN/child.pid")"; STRAYS="$STRAYS $child"
echo "$parent" > "$RUN/svc.pid"
starttime_of "$parent" > "$RUN/svc.starttime"

kids="$(descendants_of "$parent")"
check "the forked child is found" "yes" "$(contains "$kids" "$child")"
record_tree svc >/dev/null
tree_pids="$(cut -d: -f1 "$RUN/svc.tree" | tr '\n' ' ')"
check "the recorded tree holds the parent" "yes" "$(contains "$tree_pids" "$parent")"
check "the recorded tree holds the forked child" "yes" "$(contains "$tree_pids" "$child")"
check "every live survivor is recorded" "yes" \
      "$(contains "$(tree_survivors svc)" "$child")"

echo
echo "a stranded child fails the stop, even with the port free"
# The exact defect: kill the parent only. The child is orphaned and keeps running,
# and the listening socket — had there been one — is already released.
kill "$parent" 2>/dev/null || true
sleep 2
check "the parent is gone" "" "$(starttime_of "$parent" 2>/dev/null || true)"
check "the child is still alive" "alive" \
      "$(kill -0 "$child" 2>/dev/null && echo alive || echo gone)"

set +e
out="$(stop_tracked svc 8099 2>&1)"; st=$?
set -e
check "stop_tracked returns non-zero" "1" "$st"
check "it reports a survivor" "yes" "$(has_text "$out" "still running after stop")"
check "it names the surviving PID" "yes" "$(has_text "$out" "$child")"
check "the pidfile is kept for inspection" "yes" \
      "$([ -f "$RUN/svc.pid" ] && echo yes || echo no)"
check "the tree file is kept too" "yes" \
      "$([ -f "$RUN/svc.tree" ] && echo yes || echo no)"

echo
echo "and succeeds once the whole tree is gone"
kill "$child" 2>/dev/null || true
sleep 2
set +e
out="$(stop_tracked svc 8099 2>&1)"; st=$?
set -e
check "stop_tracked returns zero" "0" "$st"
check "the pidfile is removed" "no" "$([ -f "$RUN/svc.pid" ] && echo yes || echo no)"
check "the tree file is removed" "no" "$([ -f "$RUN/svc.tree" ] && echo yes || echo no)"

echo
echo "a released port is not proof on its own"
# Both halves of the claim, isolated: tree gone but port held must still fail.
sleep 60 & lone=$!; STRAYS="$STRAYS $lone"
sleep 1
echo "$lone" > "$RUN/p.pid"; starttime_of "$lone" > "$RUN/p.starttime"
record_tree p >/dev/null
kill "$lone" 2>/dev/null || true; sleep 1
PORT_IS_FREE=no
set +e
out="$(stop_tracked p 8099 2>&1)"; st=$?
set -e
check "tree gone but port not confirmed free still fails" "1" "$st"
check "and says 'could not be confirmed free', not 'failed to release'" "yes" \
      "$(has_text "$out" "could not be")"
check "the wording does not overclaim" "no" "$(has_text "$out" "FAILED TO RELEASE")"
PORT_IS_FREE=yes

echo
echo "a recycled PID is not a survivor"
sleep 60 & rec=$!; STRAYS="$STRAYS $rec"
sleep 1
echo "$rec" > "$RUN/r.pid"; starttime_of "$rec" > "$RUN/r.starttime"
printf '%s:%s\n' "$rec" "definitely-not-its-start-time" > "$RUN/r.tree"
check "a live PID whose start time differs is not counted" "" "$(tree_survivors r)"
kill "$rec" 2>/dev/null || true

echo
if [ "$fail" -gt 0 ]; then
  echo "FAILED $fail/$((pass + fail))"
  exit 1
fi
echo "all passed ($pass assertions)"
