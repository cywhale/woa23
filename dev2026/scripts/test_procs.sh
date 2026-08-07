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
PORT_HELD_BY_PID=yes
pid_holds_port() { [ "$PORT_HELD_BY_PID" = "yes" ]; }

# shellcheck source=lib_procs.sh
. "$HERE/lib_procs.sh"

# ---------------------------------------------------------------------------
# The Linux parsing path, verified anywhere.
#
# VM24 reads /proc and never invokes `ps`; this laptop has no /proc, so without
# this the production path would go untested on the machine where the tests are
# actually run. A synthetic procfs exercises the parsing, including the case that
# breaks naive field splitting: `comm` is parenthesised and may itself contain
# spaces and brackets.
echo "the Linux /proc parsing path"
FAKE="$RUN/fakeproc"
mkdir -p "$FAKE/1" "$FAKE/4320" "$FAKE/4321" "$FAKE/999"
printf '1 (systemd) S 0 1 1 0 -1 4194560 %s\n' "$(seq -s' ' 1 30)" > "$FAKE/1/stat"
printf '4320 (gunicorn) S 1 4320 0 0 -1 0 0 0 0 0 1 2 0 0 20 0 1 0 987654301 1 2\n' \
  > "$FAKE/4320/stat"
# A comm with a space and nested parentheses — the reason the strip is greedy.
printf '4321 (my (weird) app) S 4320 4320 0 0 -1 0 0 0 0 0 1 2 0 0 20 0 1 0 987654321 1 2\n' \
  > "$FAKE/4321/stat"
printf '999 (dask (worker) x) S 4321 999 0 0 -1 0 0 0 0 0 1 2 0 0 20 0 1 0 987654399 1 2\n' \
  > "$FAKE/999/stat"

# starttime_of / ppid_of read the same line and must agree with the snapshot.
# 4321 is deliberately named `my (weird) app`: comm is parenthesised but not
# escaped, so it can contain `) `, and a shortest-match strip truncates there.
check "ppid_of parses a plain comm" "1" "$(PROC_ROOT="$FAKE" ppid_of 4320)"
check "ppid_of parses a comm containing ') '" "4320" "$(PROC_ROOT="$FAKE" ppid_of 4321)"
check "starttime_of parses a plain comm" "987654301" \
      "$(PROC_ROOT="$FAKE" starttime_of 4320)"
check "starttime_of parses a comm containing ') '" "987654321" \
      "$(PROC_ROOT="$FAKE" starttime_of 4321)"
check "the two parenthesised processes get DIFFERENT start times" "differ" \
      "$([ "$(PROC_ROOT="$FAKE" starttime_of 4321)" \
           != "$(PROC_ROOT="$FAKE" starttime_of 999)" ] && echo differ || echo same)"
check "an unreadable pid is an error, not a value" "1" \
      "$(PROC_ROOT="$FAKE" starttime_of 424242 >/dev/null 2>&1; echo $?)"

snap="$(PROC_ROOT="$FAKE" _pid_ppid_snapshot)"
check "every process is listed" "4" "$(printf '%s\n' "$snap" | wc -l | tr -d ' ')"
check "a plain comm parses" "yes" "$(has_text "$snap" "4320 1")"
check "a comm containing spaces and parens parses" "yes" "$(has_text "$snap" "4321 4320")"
check "the init entry parses" "yes" "$(has_text "$snap" "1 0")"
kids="$(PROC_ROOT="$FAKE" descendants_of 4320)"
check "descendants are found through /proc" "yes" "$(contains "$kids" "4321")"
check "and recursively, at depth 2" "yes" "$(contains "$kids" "999")"
check "an unrelated process is not included" "no" "$(contains "$kids" "1")"

echo
# These tests need to enumerate processes and read their start times. A sandbox that
# denies `ps` cannot do that, and the run would otherwise die before the first
# assertion and read as a failure. Exit 77 — the conventional "skipped" code — so a
# harness can tell "cannot verify here" from "verified and wrong".
#
# On Linux this reads /proc directly and never invokes `ps`, so a denial here does
# not say anything about the path taken on VM24.
if ! can_enumerate_processes; then
  echo "SKIPPED: this machine cannot enumerate processes (no readable /proc, and"
  echo "  \`ps\` is unavailable or denied). These tests verify nothing here."
  echo "  Source: $([ -r /proc/1/stat ] && echo /proc || echo ps)"
  exit 77
fi
echo "process enumeration: $([ -r /proc/1/stat ] && echo "/proc (the VM24 path)" \
                             || echo "ps (no procfs on this machine)")"

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
echo "a tracked service always has an interpretable tree"
# The window this closes: the trap is armed the moment a service is tracked, and
# stop_tracked refuses to signal anything whose tree it cannot interpret. If
# start_tracked did not record one, an abort before the first explicit record_tree —
# a port that never binds, a readiness timeout — would leave the process running.
start_tracked early "" sleep 45 >/dev/null
early_pid="$(cat "$RUN/early.pid")"; STRAYS="$STRAYS $early_pid"
check "start_tracked leaves a tree file behind" "yes" \
      "$([ -f "$RUN/early.tree" ] && echo yes || echo no)"
check "it carries a boot header" "yes" \
      "$(has_text "$(head -1 "$RUN/early.tree")" "boot:")"
set +e; tree_survivors early >/dev/null 2>&1; st=$?; set -e
check "so the tree is interpretable straight away" "0" "$st"
set +e; out="$(stop_tracked early "" 2>&1)"; st=$?; set -e
check "an immediate stop succeeds rather than refusing" "0" "$st"
check "and the process is actually gone" "gone" \
      "$(kill -0 "$early_pid" 2>/dev/null && echo alive || echo gone)"

echo
echo "a service that never bound is still stopped"
# The abort paths where cleanup matters most: a readiness timeout, a provenance
# check that failed, a scheduler that never came up. The PID is live and holds
# nothing. Requiring port ownership before signalling left it running.
PORT_HELD_BY_PID=no
start_tracked unbound 8099 sleep 45 >/dev/null
unbound_pid="$(cat "$RUN/unbound.pid")"; STRAYS="$STRAYS $unbound_pid"
set +e; out="$(stop_tracked unbound 8099 2>&1)"; st=$?; set -e
check "stop_tracked signals it rather than refusing" "0" "$st"
check "the process is actually gone" "gone" \
      "$(kill -0 "$unbound_pid" 2>/dev/null && echo alive || echo gone)"
check "it does not claim to be refusing" "no" "$(has_text "$out" "REFUSING TO KILL")"
check "it says why it proceeded" "yes" "$(has_text "$out" "does not currently hold port")"
check "state is removed on success" "no" \
      "$([ -f "$RUN/unbound.pid" ] && echo yes || echo no)"
PORT_HELD_BY_PID=yes

echo
echo "a live PID whose identity cannot be read is not 'exited'"
# A restricted /proc: the process directory exists, its stat does not. Reading that
# as "gone" is what would let cleanup delete the state files over a running process.
mkdir -p "$FAKE/5555"        # a pid directory with no stat file
set +e; PROC_ROOT="$FAKE" starttime_of 5555 >/dev/null 2>&1; st=$?; set -e
check "starttime_of reports failure, not a value" "1" "$st"
check "pid_exists still sees the process" "0" \
      "$(PROC_ROOT="$FAKE" pid_exists 5555; echo $?)"
{ printf 'boot:%s\n' "$(boot_id)"; printf '5555:12345\n'; } > "$RUN/unreadable.tree"
set +e; PROC_ROOT="$FAKE" tree_survivors unreadable >/dev/null 2>&1; st=$?; set -e
check "tree_survivors returns 2, not an empty all-clear" "2" "$st"
# The same, through stop_tracked. The tracked process is a real one that will exit;
# the unreadable 5555 is an extra entry in its tree, so the only thing keeping the
# stop from succeeding is the entry whose identity cannot be read.
sleep 45 & unread=$!; STRAYS="$STRAYS $unread"
sleep 1
echo "$unread" > "$RUN/unreadable.pid"
starttime_of "$unread" > "$RUN/unreadable.starttime"
{ printf 'boot:%s\n' "$(boot_id)"
  printf '%s:%s\n' "$unread" "$(starttime_of "$unread")"
  printf '5555:12345\n'; } > "$RUN/unreadable.tree"
set +e; out="$(PROC_ROOT="$FAKE" stop_tracked unreadable "" 2>&1)"; st=$?; set -e
check "stop_tracked fails rather than reporting a clean stop" "1" "$st"
check "it says the outcome is unknown, not that the boot id changed" "yes" \
      "$(has_text "$out" "cannot determine whether every tracked process exited")"
check "and the state files are kept" "yes" \
      "$([ -f "$RUN/unreadable.pid" ] && echo yes || echo no)"
kill "$unread" 2>/dev/null || true
rm "$RUN/unreadable.pid" "$RUN/unreadable.starttime" "$RUN/unreadable.tree"

echo
echo "a malformed tree line is uninterpretable, not empty"
for bad in "notapid:123" "4321" "4321:"; do
  { printf 'boot:%s\n' "$(boot_id)"; printf '%s\n' "$bad"; } > "$RUN/bad.tree"
  set +e; tree_survivors bad >/dev/null 2>&1; st=$?; set -e
  check "line '$bad' yields status 2" "2" "$st"
done
rm "$RUN/bad.tree"

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
echo "a tree from another boot is never acted on"
# After a reboot, PIDs are reused and start times are measured from boot — so the
# same pid:starttime pair can name someone else's process. The only safe action is
# none: do not signal, do not delete state.
sleep 60 & bootp=$!; STRAYS="$STRAYS $bootp"
sleep 1
echo "$bootp" > "$RUN/b.pid"; starttime_of "$bootp" > "$RUN/b.starttime"
record_tree b >/dev/null
real_boot="$(boot_id)"
# Rewrite the header as if the tree had been recorded before a reboot.
sed "s|^boot:.*|boot:0000-a-different-boot-0000|" "$RUN/b.tree" > "$RUN/b.tree.tmp"
mv "$RUN/b.tree.tmp" "$RUN/b.tree"

set +e; tree_survivors b >/dev/null 2>&1; st=$?; set -e
check "tree_survivors reports 2, not an empty all-clear" "2" "$st"
set +e; out="$(stop_tracked b 8099 2>&1)"; st=$?; set -e
check "stop_tracked refuses and returns non-zero" "1" "$st"
check "it says why" "yes" "$(has_text "$out" "not from the")"
check "nothing was signalled — the process is still alive" "alive" \
      "$(kill -0 "$bootp" 2>/dev/null && echo alive || echo gone)"
check "the pidfile is kept" "yes" "$([ -f "$RUN/b.pid" ] && echo yes || echo no)"
check "the tree file is kept" "yes" "$([ -f "$RUN/b.tree" ] && echo yes || echo no)"

# A tree with no header at all is equally uninterpretable.
grep -v '^boot:' "$RUN/b.tree" > "$RUN/b.tree.tmp"; mv "$RUN/b.tree.tmp" "$RUN/b.tree"
set +e; tree_survivors b >/dev/null 2>&1; st=$?; set -e
check "a tree with no boot header is also status 2" "2" "$st"

# And the matching header is what makes it usable again.
printf 'boot:%s\n' "$real_boot" > "$RUN/b.tree.tmp"
grep -v '^boot:' "$RUN/b.tree" >> "$RUN/b.tree.tmp"; mv "$RUN/b.tree.tmp" "$RUN/b.tree"
set +e; surv="$(tree_survivors b)"; st=$?; set -e
check "with the real boot id it is usable again" "0" "$st"
check "and the live process is seen as a survivor" "yes" "$(contains "$surv" "$bootp")"
kill "$bootp" 2>/dev/null || true

echo
echo "a recycled PID is not a survivor"
sleep 60 & rec=$!; STRAYS="$STRAYS $rec"
sleep 1
echo "$rec" > "$RUN/r.pid"; starttime_of "$rec" > "$RUN/r.starttime"
{ printf 'boot:%s\n' "$(boot_id)"
  printf '%s:%s\n' "$rec" "definitely-not-its-start-time"; } > "$RUN/r.tree"
check "a live PID whose start time differs is not counted" "" "$(tree_survivors r)"
kill "$rec" 2>/dev/null || true

echo
if [ "$fail" -gt 0 ]; then
  echo "FAILED $fail/$((pass + fail))"
  exit 1
fi
echo "all passed ($pass assertions)"
