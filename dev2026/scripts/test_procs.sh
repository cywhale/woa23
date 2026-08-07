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

# Fixture processes must outlive the entire suite. They used to be `sleep 30`, and
# once enough tests were added the suite ran longer than that: the "stranded child"
# fixture exited on its own and six assertions failed for a reason that had nothing
# to do with the code. The exit trap kills these regardless, so the value only needs
# to be comfortably larger than the run.
export FIXTURE_LIFE=900   # exported: the `bash -c` fixtures are separate shells
# Several cases are designed never to drain — a stranded child, a port that stays
# held — so the full production wait would be spent on each of them.
export STOP_WAIT_SECS=3

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

# The validator on the Linux branch, which is the one VM24 takes: field 22 is an
# integer, so anything else is a corrupt record rather than a different process.
check "procfs branch accepts an integer" "0" \
      "$(PROC_ROOT="$FAKE" _valid_starttime 987654321; echo $?)"
check "procfs branch rejects the ps-shaped token" "1" \
      "$(PROC_ROOT="$FAKE" _valid_starttime "Mon_Jan_1_00:00:01_2001"; echo $?)"
for bad in "definitely-not-its-start-time" "98765432a" "" "9876 5432"; do
  check "procfs branch rejects '$bad'" "1" \
        "$(PROC_ROOT="$FAKE" _valid_starttime "$bad"; echo $?)"
done
check "a real /proc start time validates" "0" \
      "$(PROC_ROOT="$FAKE" _valid_starttime "$(PROC_ROOT="$FAKE" starttime_of 4321)"; echo $?)"

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
# `wait` rather than a second `sleep`: the foreground sleep would be a THIRD
# process that survives the parent, and this block's last assertion needs the tree
# empty after exactly one child is killed. (It passed before only because the
# fixtures were short-lived enough to expire on their own.)
bash -c 'sleep "$FIXTURE_LIFE" & echo $! > "$RUN/child.pid"; wait' >/dev/null 2>&1 &
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
start_tracked early "" sleep "$FIXTURE_LIFE" >/dev/null
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
start_tracked unbound 8099 sleep "$FIXTURE_LIFE" >/dev/null
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
sleep "$FIXTURE_LIFE" >/dev/null 2>&1 & unread=$!; STRAYS="$STRAYS $unread"
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
echo "a live child is never silently dropped from the tree"
# Both writers could fail to see everything, and both used to swallow it: process
# enumeration failing vanished into a command substitution, and a PID that could not
# be recorded was skipped with `continue`. Either left a SHORT tree — worse than a
# corrupt one, because it looks entirely valid and reports every missing child as
# exited.

bash -c 'sleep "$FIXTURE_LIFE" & sleep "$FIXTURE_LIFE"' >/dev/null 2>&1 & dropp=$!; STRAYS="$STRAYS $dropp"
sleep 1
echo "$dropp" > "$RUN/drop.pid"; starttime_of "$dropp" > "$RUN/drop.starttime"
check "the parent really does have children" "yes" \
      "$([ -n "$(descendants_of "$dropp")" ] && echo yes || echo no)"

# (a) enumeration fails outright. Overridden here; re-sourcing the library below
# puts the real definition back.
_pid_ppid_snapshot() { return 1; }
set +e; record_tree drop >/dev/null 2>&1; st=$?; set -e
check "record_tree reports failure when enumeration fails" "1" "$st"
check "the tree records why" "1" \
      "$(grep -c '^incomplete:process-enumeration-failed' "$RUN/drop.tree")"
check "the tracked pid is still there — only completeness is in doubt" "yes" \
      "$(contains "$(tree_pids drop)" "$dropp")"
set +e; tree_survivors drop >/dev/null 2>&1; st=$?; set -e
check "tree_survivors cannot determine, rather than reporting none" "2" "$st"
set +e; out="$(stop_tracked drop "" 2>&1)"; st=$?; set -e
check "stop_tracked fails rather than reporting a clean stop" "1" "$st"
check "state is kept" "yes" "$([ -f "$RUN/drop.pid" ] && echo yes || echo no)"
# shellcheck source=lib_procs.sh
. "$HERE/lib_procs.sh"      # restores the real definitions
check "the real enumeration is back" "yes" \
      "$([ -n "$(_pid_ppid_snapshot | head -1)" ] && echo yes || echo no)"
kill "$dropp" 2>/dev/null || true
rm "$RUN/drop.pid" "$RUN/drop.starttime" "$RUN/drop.tree"

# (b) a process that is live but cannot be recorded. $FAKE/5555 is a pid directory
# with no stat file: pid_exists sees it, starttime_of cannot read it.
: > "$RUN/one.tree"
set +e; PROC_ROOT="$FAKE" _record_one one 5555 >/dev/null 2>&1; st=$?; set -e
check "_record_one fails on a live PID it cannot read" "1" "$st"
check "and marks the tree, naming the PID" "1" \
      "$(grep -c '^incomplete:unreadable-identity-5555' "$RUN/one.tree")"
check "the unrecordable PID is absent from the pid lines" "no" \
      "$(contains "$(tree_pids one)" "5555")"

# (c) a process whose recorded identity would be malformed is also not dropped.
mkdir -p "$FAKE/6666"
printf '6666 (bad) S 4320 6666 0 0 -1 0 0 0 0 0 1 2 0 0 20 0 1 0 not-a-number 1 2\n' \
  > "$FAKE/6666/stat"
: > "$RUN/two.tree"
set +e; PROC_ROOT="$FAKE" _record_one two 6666 >/dev/null 2>&1; st=$?; set -e
check "_record_one fails on a malformed start time" "1" "$st"
check "and marks the tree" "1" "$(grep -c '^incomplete:malformed-token-6666' "$RUN/two.tree")"

# (d) the benign race: a child that exited between the snapshot and the read is
# not a gap, and must not fail the write.
: > "$RUN/three.tree"
set +e; _record_one three 999999 >/dev/null 2>&1; st=$?; set -e
check "a PID that has exited is not an error" "0" "$st"
check "and leaves no marker" "0" "$(grep -c '^incomplete:' "$RUN/three.tree")"
rm "$RUN/one.tree" "$RUN/two.tree" "$RUN/three.tree"

echo
echo "an unwritable marker never leaves a tree that looks complete"
# Every function in lib_procs.sh is called in a context that tests its status, and
# bash switches `set -e` OFF inside such a function body — so an unchecked write
# failure there does not abort anything, it just carries on. _mark_incomplete was
# unchecked, which meant a short tree could end up with no marker at all.
: > "$RUN/ro.tree"
printf 'boot:%s\n' "$(boot_id)" > "$RUN/ro.tree"
chmod 0444 "$RUN/ro.tree"
if printf 'probe\n' >> "$RUN/ro.tree" 2>/dev/null; then
  echo "  (skipped: this user can write a 0444 file, so the failure cannot be staged)"
  chmod 0644 "$RUN/ro.tree"; rm "$RUN/ro.tree"
else
  set +e; _mark_incomplete ro "process-enumeration-failed" >/dev/null 2>&1; st=$?; set -e
  check "_mark_incomplete reports the failed write" "1" "$st"
  check "and the unmarked tree is gone, not left looking complete" "no" \
        "$([ -f "$RUN/ro.tree" ] && echo yes || echo no)"
  # A missing tree is the fail-closed answer for every later reader.
  set +e; tree_survivors ro >/dev/null 2>&1; st=$?; set -e
  check "a later read of the removed tree cannot determine" "2" "$st"
fi

echo
echo "when the marker cannot be written AND the tree cannot be removed"
# The residual hole: an unwritable marker plus a failed removal used to leave a
# tree on disk carrying a valid boot header and pid lines but no `incomplete:`
# line — a legal-looking tree that a later cleanup would trust.
LOCKED="$RUN/locked"
mkdir -p "$LOCKED"
bash -c 'sleep "$FIXTURE_LIFE" & echo $! > "'"$LOCKED"'/lk.child"; wait' \
  >/dev/null 2>&1 &
lockp=$!; STRAYS="$STRAYS $lockp"
sleep 1
lockc="$(cat "$LOCKED/lk.child")"; STRAYS="$STRAYS $lockc"
RUN_REAL="$RUN"; RUN="$LOCKED"
echo "$lockp" > "$RUN/lk.pid"; starttime_of "$lockp" > "$RUN/lk.starttime"
record_tree lk >/dev/null
check "the tree looks complete before the failure is staged" "0" \
      "$(grep -c '^incomplete:' "$RUN/lk.tree")"
# Both permissions are needed, and they are not the same thing: appending needs
# write on the FILE, while removing it and creating the sentinel need write on the
# DIRECTORY. Locking only the directory left the append working.
chmod 0444 "$RUN/lk.tree"
chmod 0555 "$LOCKED"
if printf 'probe\n' >> "$RUN/lk.tree" 2>/dev/null \
   || printf 'probe\n' > "$RUN/probe" 2>/dev/null; then
  echo "  (skipped: this user can write a read-only file in a 0555 directory)"
  chmod 0755 "$LOCKED"; chmod 0644 "$RUN/lk.tree"
  rm -- "$RUN/probe" 2>/dev/null || true
else
  set +e; _mark_incomplete lk "process-enumeration-failed" >/dev/null 2>&1; st=$?; set -e
  check "_mark_incomplete reports failure" "1" "$st"
  check "the tree really could not be removed" "yes" \
        "$([ -f "$RUN/lk.tree" ] && echo yes || echo no)"
  check "and it really has no marker in it" "0" "$(grep -c '^incomplete:' "$RUN/lk.tree")"
  check "the .uncertain sentinel could not be written either" "no" \
        "$([ -f "$RUN/lk.uncertain" ] && echo yes || echo no)"

  # With nothing on disk to say so, the runtime layer is what must hold.
  set +e; tree_boot_matches lk >/dev/null 2>&1; st=$?; set -e
  check "tree_boot_matches refuses the tree anyway" "2" "$st"
  set +e; tree_survivors lk >/dev/null 2>&1; st=$?; set -e
  check "tree_survivors cannot determine" "2" "$st"
  set +e; out="$(stop_tracked lk "" 2>&1)"; st=$?; set -e
  check "stop_tracked returns non-zero" "1" "$st"
  check "it refuses before signalling, so the process is untouched" "alive" \
        "$(kill -0 "$lockp" 2>/dev/null && echo alive || echo gone)"
  check "state is not removed" "yes" "$([ -f "$RUN/lk.pid" ] && echo yes || echo no)"
  chmod 0755 "$LOCKED"; chmod 0644 "$RUN/lk.tree"
fi
RUN="$RUN_REAL"
kill "$lockp" "$lockc" 2>/dev/null || true

echo
echo "a persisted .uncertain sentinel outlives the process that wrote it"
# The layer that covers a *later* invocation, which has no runtime flag at all.
bash -c 'sleep "$FIXTURE_LIFE" & wait' >/dev/null 2>&1 &
sentp=$!; STRAYS="$STRAYS $sentp"
sleep 1
echo "$sentp" > "$RUN/sent.pid"; starttime_of "$sentp" > "$RUN/sent.starttime"
record_tree sent >/dev/null
check "the tree is usable to begin with" "0" \
      "$(tree_boot_matches sent >/dev/null 2>&1; echo $?)"
printf 'tree write uncertain\n' > "$RUN/sent.uncertain"
check "a sentinel alone makes it uninterpretable" "2" \
      "$(tree_boot_matches sent >/dev/null 2>&1; echo $?)"
check "even though the tree itself still looks complete" "0" \
      "$(grep -c '^incomplete:' "$RUN/sent.tree")"
set +e; out="$(stop_tracked sent "" 2>&1)"; st=$?; set -e
check "stop_tracked refuses" "1" "$st"
check "state is kept" "yes" "$([ -f "$RUN/sent.pid" ] && echo yes || echo no)"
kill "$sentp" 2>/dev/null || true
rm "$RUN/sent.pid" "$RUN/sent.starttime" "$RUN/sent.tree" "$RUN/sent.uncertain"

echo
echo "a stop that cannot clear its state does not report a clean stop"
# errexit is off inside stop_tracked — it is always called with its status tested —
# so an unchecked `rm` that failed fell through to `return 0`, reporting success
# with the state files still on disk. The next preflight would then block on state
# this run claimed to have cleared.
UNRM="$RUN/unrm"
mkdir -p "$UNRM"
sleep "$FIXTURE_LIFE" >/dev/null 2>&1 & unrmp=$!; STRAYS="$STRAYS $unrmp"
sleep 1
RUN_REAL="$RUN"; RUN="$UNRM"
echo "$unrmp" > "$RUN/u.pid"; starttime_of "$unrmp" > "$RUN/u.starttime"
record_tree u >/dev/null
kill "$unrmp" 2>/dev/null || true
sleep 1
chmod 0555 "$UNRM"          # files may be read, but not unlinked
if rm -- "$UNRM/u.starttime" 2>/dev/null; then
  echo "  (skipped: this user can unlink inside a 0555 directory)"
  chmod 0755 "$UNRM"
else
  set +e; out="$(stop_tracked u "" 2>&1)"; st=$?; set -e
  check "the tracked process really did exit" "gone" \
        "$(kill -0 "$unrmp" 2>/dev/null && echo alive || echo gone)"
  check "stop_tracked does NOT report a clean stop" "1" "$st"
  check "it names the files it could not remove" "yes" \
        "$(has_text "$out" "could not be removed")"
  check "the pidfile is still there, as the message says" "yes" \
        "$([ -f "$RUN/u.pid" ] && echo yes || echo no)"
  chmod 0755 "$UNRM"
fi
RUN="$RUN_REAL"

echo
echo "an incomplete tree still stops the process it does know about"
# The point of marking rather than refusing: what is provably ours is still stopped,
# so the host is left cleaner, while the run still fails because completeness is
# unknown. Both halves are asserted here.
bash -c 'sleep "$FIXTURE_LIFE" & sleep "$FIXTURE_LIFE"' >/dev/null 2>&1 & inc=$!; STRAYS="$STRAYS $inc"
sleep 1
echo "$inc" > "$RUN/inc.pid"; starttime_of "$inc" > "$RUN/inc.starttime"
record_tree inc >/dev/null
_mark_incomplete inc "staged-for-test" >/dev/null
check "the tree is marked incomplete" "1" "$(grep -c '^incomplete:' "$RUN/inc.tree")"
check "the known process is alive before the stop" "alive" \
      "$(kill -0 "$inc" 2>/dev/null && echo alive || echo gone)"
set +e; out="$(stop_tracked inc "" 2>&1)"; st=$?; set -e
check "stop_tracked returns non-zero" "1" "$st"
check "THE KNOWN PROCESS WAS ACTUALLY STOPPED" "gone" \
      "$(kill -0 "$inc" 2>/dev/null && echo alive || echo gone)"
check "it reports the outcome as undetermined" "yes" \
      "$(has_text "$out" "cannot determine whether every tracked process exited")"
check "state is kept for inspection" "yes" \
      "$([ -f "$RUN/inc.pid" ] && echo yes || echo no)"
rm "$RUN/inc.pid" "$RUN/inc.starttime" "$RUN/inc.tree"

echo
echo "a refresh that fails is carried even if its marker never lands"
bash -c 'sleep "$FIXTURE_LIFE" & sleep "$FIXTURE_LIFE"' >/dev/null 2>&1 & ref=$!; STRAYS="$STRAYS $ref"
sleep 1
echo "$ref" > "$RUN/ref.pid"; starttime_of "$ref" > "$RUN/ref.starttime"
record_tree ref >/dev/null
# refresh_tree fails, and its marker write is staged to fail too, so the only
# surviving signal is stop_tracked's own local flag.
_pid_ppid_snapshot() { return 1; }
_mark_incomplete() { return 1; }
set +e; out="$(stop_tracked ref "" 2>&1)"; st=$?; set -e
check "stop_tracked still returns non-zero" "1" "$st"
check "the known process was still stopped" "gone" \
      "$(kill -0 "$ref" 2>/dev/null && echo alive || echo gone)"
check "and it says the stop could not be verified" "yes" \
      "$(has_text "$out" "could not be")"
# shellcheck source=lib_procs.sh
. "$HERE/lib_procs.sh"      # restores the real definitions
rm "$RUN/ref.pid" "$RUN/ref.starttime" "$RUN/ref.tree"

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
sleep "$FIXTURE_LIFE" >/dev/null 2>&1 & lone=$!; STRAYS="$STRAYS $lone"
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
sleep "$FIXTURE_LIFE" >/dev/null 2>&1 & bootp=$!; STRAYS="$STRAYS $bootp"
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
echo "a legal-but-different start time is a recycled PID; garbage is a corrupt tree"
# These two must not collapse into each other. The previous version of this test
# used a garbage value to stand in for "a different start time", so it asserted the
# fail-open behaviour — a malformed tree reporting no survivors — as correct.
sleep "$FIXTURE_LIFE" >/dev/null 2>&1 & rec=$!; STRAYS="$STRAYS $rec"
sleep 1
echo "$rec" > "$RUN/r.pid"; starttime_of "$rec" > "$RUN/r.starttime"
real_st="$(starttime_of "$rec")"

# A well-formed token this host could have produced, but not this process's.
if [ -r "${PROC_ROOT:-/proc}/1/stat" ]; then other_st=$(( ${real_st} + 1 ))
else other_st="Mon_Jan_1_00:00:01_2001"; fi
check "the substitute token is well formed" "0" \
      "$(_valid_starttime "$other_st"; echo $?)"
check "and is not the real one" "differ" \
      "$([ "$other_st" != "$real_st" ] && echo differ || echo same)"

{ printf 'boot:%s\n' "$(boot_id)"
  printf '%s:%s\n' "$rec" "$other_st"; } > "$RUN/r.tree"
set +e; surv="$(tree_survivors r)"; st=$?; set -e
check "a recycled PID is interpretable" "0" "$st"
check "and is not counted as a survivor" "" "$surv"

# The same PID, still alive, with a value that is not a token at all.
{ printf 'boot:%s\n' "$(boot_id)"
  printf '%s:%s\n' "$rec" "definitely-not-its-start-time"; } > "$RUN/r.tree"
set +e; tree_survivors r >/dev/null 2>&1; st=$?; set -e
check "a malformed start time is status 2, not 'no survivors'" "2" "$st"

# ...and it must not be possible to reach a clean stop through it.
starttime_of "$rec" > "$RUN/r.starttime"
set +e; out="$(stop_tracked r "" 2>&1)"; st=$?; set -e
check "stop_tracked fails on a corrupt tree" "1" "$st"
check "the state files are kept" "yes" "$([ -f "$RUN/r.pid" ] && echo yes || echo no)"

# $rec was stopped above, so ask a process that is certainly alive: this shell.
check "a real live process yields a token the validator accepts" "0" \
      "$(_valid_starttime "$(starttime_of $$)"; echo $?)"
for bad in "" "12 34" "abc" "-5" "1e6"; do
  check "'$bad' is rejected as a start time" "1" "$(_valid_starttime "$bad"; echo $?)"
done
kill "$rec" 2>/dev/null || true

echo
if [ "$fail" -gt 0 ]; then
  echo "FAILED $fail/$((pass + fail))"
  exit 1
fi
echo "all passed ($pass assertions)"
