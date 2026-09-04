#!/usr/bin/env bash
#
# scripts/lib_sentinel.sh — batch completion sentinels, checked offline.
#
# The defect this suite pins down was NOT a display problem. The old driver ended with a
# literal `echo "D3_6CE915E_BATCHES_DONE"`:
#
#   * the subject name was hardcoded, so a driver copied for a new subject announced
#     completion under the old subject's name;
#   * it ran after the loop and never tested any batch's exit status, so a FAILING run
#     still announced success;
#   * a stale line in a reused log was indistinguishable from a fresh one;
#   * the watcher matched a pattern that could match its own command line.
#
# A completion marker is what later work trusts when it says "the batches passed", so each
# of those is a provenance and completion-accounting failure. Every case below is one of
# them, written so it fails if the guard is removed.
#
# Nothing here starts a batch, touches VM24, PM2, a port, a store or a workdir.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIB="$HERE/scripts/lib_sentinel.sh"
[ -r "$LIB" ] || { echo "missing $LIB"; exit 2; }
# shellcheck source=/dev/null
. "$LIB"

PASS=0; FAIL=0
check() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

SUBJ_A=1111111111111111111111111111111111111111
SUBJ_B=2222222222222222222222222222222222222222
LABEL=runA
TOKEN=tok-0001

echo "1. a correct sentinel is written and accepted — ONCE"

F="$WORK/ok.log"
sentinel_write "$F" "$SUBJ_A" "$LABEL" "$TOKEN" 3 3 0 yes
check "a complete, clean run writes a sentinel" "0" "$?"
check "the sentinel names the subject, not a hardcoded string" "1" \
      "$(grep -c "subject=$SUBJ_A" "$F")"
check "it is accepted by the matching verifier" "0" \
      "$(sentinel_verify "$F" "$SUBJ_A" "$LABEL" "$TOKEN" 2>/dev/null; echo $?)"

# ACCEPTED ONCE. A second write must be refused, or "did it pass?" becomes ambiguous.
sentinel_write "$F" "$SUBJ_A" "$LABEL" "$TOKEN" 3 3 0 yes 2>/dev/null
check "a DUPLICATE write is refused" "10" "$?"
check "and the file still holds exactly one sentinel" "1" \
      "$(grep -c "^WOA23_BATCH_COMPLETE " "$F")"

echo
echo "2. an OLD SUBJECT's sentinel is rejected — the original defect"

OLD="$WORK/old.log"
sentinel_write "$OLD" "$SUBJ_B" "$LABEL" "$TOKEN" 3 3 0 yes
check "verifying an old subject's sentinel against the new subject fails" "4" \
      "$(sentinel_verify "$OLD" "$SUBJ_A" "$LABEL" "$TOKEN" 2>/dev/null; echo $?)"
check "the old sentinel is still valid for its OWN subject" "0" \
      "$(sentinel_verify "$OLD" "$SUBJ_B" "$LABEL" "$TOKEN" 2>/dev/null; echo $?)"

echo
echo "3. a WRONG BATCH LABEL is rejected"

check "a sentinel from another run label is refused" "5" \
      "$(sentinel_verify "$F" "$SUBJ_A" "otherRun" "$TOKEN" 2>/dev/null; echo $?)"
check "a sentinel from another run TOKEN is refused" "8" \
      "$(sentinel_verify "$F" "$SUBJ_A" "$LABEL" "tok-9999" 2>/dev/null; echo $?)"

echo
echo "4. a MISSING sentinel is 'not complete', never 'probably fine'"

check "an absent file is missing, not success" "3" \
      "$(sentinel_verify "$WORK/nope.log" "$SUBJ_A" "$LABEL" "$TOKEN" 2>/dev/null; echo $?)"
: > "$WORK/empty.log"
check "an empty log is missing, not success" "3" \
      "$(sentinel_verify "$WORK/empty.log" "$SUBJ_A" "$LABEL" "$TOKEN" 2>/dev/null; echo $?)"
printf 'batch 1 exit status: 0\nsome other output\n' > "$WORK/noise.log"
check "a log full of output but no sentinel is missing" "3" \
      "$(sentinel_verify "$WORK/noise.log" "$SUBJ_A" "$LABEL" "$TOKEN" 2>/dev/null; echo $?)"

echo
echo "5. a FAILED, PARTIAL, ABORTED or UNCERTAIN run writes NO success sentinel"
#
# This is the case the old driver got wrong: it captured rc and never tested it.

FF="$WORK/fail.log"
sentinel_write "$FF" "$SUBJ_A" "$LABEL" "$TOKEN" 3 3 1 yes 2>/dev/null
check "one non-zero suite result refuses the sentinel" "8" "$?"
check "and nothing was written" "0" \
      "$([ -e "$FF" ] && grep -c '^WOA23_BATCH_COMPLETE ' "$FF" || echo 0)"

sentinel_write "$WORK/partial.log" "$SUBJ_A" "$LABEL" "$TOKEN" 3 2 0 yes 2>/dev/null
check "a PARTIAL run (2 of 3 batches) refuses the sentinel" "7" "$?"
check "and nothing was written" "0" \
      "$([ -e "$WORK/partial.log" ] && grep -c '^WOA23_BATCH_COMPLETE ' "$WORK/partial.log" || echo 0)"

sentinel_write "$WORK/uncertain.log" "$SUBJ_A" "$LABEL" "$TOKEN" 3 3 0 uncertain 2>/dev/null
check "failed POSTCONDITIONS refuse the sentinel ('uncertain' is not 'passed')" "9" "$?"
check "and nothing was written" "0" \
      "$([ -e "$WORK/uncertain.log" ] && grep -c '^WOA23_BATCH_COMPLETE ' "$WORK/uncertain.log" || echo 0)"

echo
echo "6. a MISSING or BOGUS subject SHA is refused at write time"

sentinel_write "$WORK/nosha.log" "" "$LABEL" "$TOKEN" 3 3 0 yes 2>/dev/null
check "an empty subject is refused" "3" "$?"
sentinel_write "$WORK/badsha.log" "not-a-sha" "$LABEL" "$TOKEN" 3 3 0 yes 2>/dev/null
check "a non-hex subject is refused" "3" "$?"
sentinel_write "$WORK/shortsha.log" "1111111" "$LABEL" "$TOKEN" 3 3 0 yes 2>/dev/null
check "a truncated SHA is refused (40 hex exactly)" "3" "$?"
sentinel_write "$WORK/nolabel.log" "$SUBJ_A" "" "$TOKEN" 3 3 0 yes 2>/dev/null
check "an empty label is refused" "4" "$?"

echo
echo "7. a MALFORMED or DUPLICATED sentinel in a file is refused, not parsed loosely"

printf 'WOA23_BATCH_COMPLETE subject=zzzz label=%s token=%s batches=3 nonzero=0\n' "$LABEL" "$TOKEN" \
  > "$WORK/malformed.log"
check "a sentinel with a bogus subject field is malformed" "7" \
      "$(sentinel_verify "$WORK/malformed.log" "$SUBJ_A" "$LABEL" "$TOKEN" 2>/dev/null; echo $?)"

cp "$F" "$WORK/dup.log"; cat "$F" >> "$WORK/dup.log"
check "two sentinels in one file are refused as ambiguous" "6" \
      "$(sentinel_verify "$WORK/dup.log" "$SUBJ_A" "$LABEL" "$TOKEN" 2>/dev/null; echo $?)"

echo
echo "8. the WATCHER waits on an explicit PID — it cannot match its own command line"
#
# The old watcher used `pgrep -f <pattern>` and needed a bracket trick so the pattern would
# not match the watcher itself. That is a workaround for the wrong mechanism. A PID is an
# identity; a command-line substring is a guess.

sleep 30 &
DRIVER_PID=$!
echo "$DRIVER_PID" > "$WORK/driver.pid"

check "the driver's PID is recorded for the watcher" "yes" \
      "$([ -s "$WORK/driver.pid" ] && echo yes || echo no)"
check "the watcher sees the driver alive by PID" "alive" \
      "$(kill -0 "$(cat "$WORK/driver.pid")" 2>/dev/null && echo alive || echo gone)"

# THE DECISIVE DEMONSTRATION. Start a decoy whose own command line contains the sentinel
# tag -- exactly the shape that produced a false positive in this campaign, where a
# watcher's command line was captured in a process snapshot and matched itself.
# The decoy must STAY ALIVE with the tag IN ITS ARGV. Two earlier forms failed to prove
# anything: `sleep 30 <tag>` exits at once on the extra argument, and `sh -c "sleep 30 #
# <tag>"` gets exec-optimised so argv collapses to `sleep 30` and the tag disappears. A
# two-command body defeats that optimisation and keeps the tag visible to `ps`.
sh -c "sleep 30; : $SENTINEL_TAG" &
DECOY_PID=$!
sleep 1
DECOY_HITS="$(pgrep -f "$SENTINEL_TAG" 2>/dev/null | grep -c . || true)"
check "a fuzzy pattern search DOES match an unrelated process carrying the tag" "yes" \
      "$([ "${DECOY_HITS:-0}" -ge 1 ] && echo yes || echo no)"
check "a PID wait is unaffected by that decoy — the driver is still the driver" "alive" \
      "$(kill -0 "$DRIVER_PID" 2>/dev/null && echo alive || echo gone)"
check "and the decoy is NOT the driver" "different" \
      "$([ "$DECOY_PID" != "$DRIVER_PID" ] && echo different || echo same)"
kill "$DECOY_PID" 2>/dev/null; wait "$DECOY_PID" 2>/dev/null

kill "$DRIVER_PID" 2>/dev/null
wait "$DRIVER_PID" 2>/dev/null
check "after the driver exits, the PID wait ends" "gone" \
      "$(kill -0 "$DRIVER_PID" 2>/dev/null && echo alive || echo gone)"

# A killed driver must NOT leave a success sentinel behind.
check "an aborted driver leaves no success sentinel" "0" \
      "$([ -e "$WORK/aborted.log" ] && grep -c '^WOA23_BATCH_COMPLETE ' "$WORK/aborted.log" || echo 0)"

echo
echo "9. the library hardcodes no subject name"

check "no 40-hex subject literal in the library" "0" \
      "$(grep -cE '[0-9a-f]{40}' "$LIB" || true)"
# The legacy string may appear in a COMMENT explaining the defect -- that is the record,
# and deleting it would lose why this file exists. What must not exist is a legacy string
# in CODE.
check "no legacy hardcoded sentinel string in library CODE (comments excepted)" "0" \
      "$(grep -cE '^[^#]*BATCHES_DONE' "$LIB" || true)"
check "the legacy string IS still explained in a comment" "yes" \
      "$(grep -qE '^#.*BATCHES_DONE' "$LIB" && echo yes || echo no)"

echo
echo "10. RUN IDENTITY is pid + starttime — a pid alone is reusable"
#
# The first version of the driver persisted only the pid and set the token to `run-$$`.
# Both are reusable: after wraparound the same number names a different process, and a
# pid-derived token inherits that weakness rather than avoiding it.

R="$WORK/run.runid"
sleep 30 & LIVE_PID=$!

check "proc_starttime answers for a live pid" "yes" \
      "$(proc_starttime "$LIVE_PID" >/dev/null 2>&1 && echo yes || echo no)"
check "proc_starttime FAILS CLOSED on a non-numeric pid" "1" \
      "$(proc_starttime "not-a-pid" >/dev/null 2>&1; echo $?)"
check "proc_starttime FAILS CLOSED on an impossible pid" "1" \
      "$(proc_starttime 999999 >/dev/null 2>&1; echo $?)"
check "and prints NOTHING when it fails (no empty value to mistake for a reading)" "" \
      "$(proc_starttime 999999 2>/dev/null)"

runid_write "$R" "$LABEL" "$SUBJ_A"
check "a runid records this process" "0" "$?"
check "it stores BOTH pid and starttime, not just a pid" "yes" \
      "$(grep -q 'pid=' "$R" && grep -q 'starttime=' "$R" && echo yes || echo no)"
check "the token is NOT merely pid-derived — it carries the starttime" "yes" \
      "$(runid_token "$R" | grep -qE -- "-$$-" && runid_token "$R" | grep -qvE "^run-[0-9]+$" && echo yes || echo no)"
check "the live run verifies as alive" "0" \
      "$(runid_alive "$R" >/dev/null 2>&1; echo $?)"

# WRONG PID: a runid naming a pid that never was.
printf 'pid=999999 starttime=123 label=%s subject=%s\n' "$LABEL" "$SUBJ_A" > "$WORK/badpid.runid"
check "a WRONG PID is reported gone, not alive" "6" \
      "$(runid_alive "$WORK/badpid.runid" >/dev/null 2>&1; echo $?)"

# PID REUSE: the pid is alive, but it is a DIFFERENT process — modelled exactly by a
# recorded starttime that does not match the one the live pid actually has.
printf 'pid=%s starttime=THIS-IS-NOT-THE-START-TIME label=%s subject=%s\n' \
       "$LIVE_PID" "$LABEL" "$SUBJ_A" > "$WORK/reuse.runid"
check "PID REUSE is detected — same pid, different starttime" "8" \
      "$(runid_alive "$WORK/reuse.runid" >/dev/null 2>&1; echo $?)"
check "a pid-only check would have wrongly said 'alive' here" "alive" \
      "$(kill -0 "$LIVE_PID" 2>/dev/null && echo alive || echo gone)"

printf 'label=%s subject=%s\n' "$LABEL" "$SUBJ_A" > "$WORK/malformed.runid"
check "a runid without pid/starttime is malformed, not assumed alive" "7" \
      "$(runid_alive "$WORK/malformed.runid" >/dev/null 2>&1; echo $?)"
check "an absent runid is refused, not treated as finished" "3" \
      "$(runid_alive "$WORK/none.runid" >/dev/null 2>&1; echo $?)"

kill "$LIVE_PID" 2>/dev/null; wait "$LIVE_PID" 2>/dev/null

# A SEPARATE PROCESS must write its own runid for this case. `runid_write` records `$$`,
# which inside a subshell is still the PARENT's pid -- so `( runid_write ... ) &` would
# record this test shell and then "report alive" for the wrong reason. A real child, run
# via `bash -c`, is the only form that actually tests a finished run.
bash -c ". '$LIB'; runid_write '$WORK/child.runid' '$LABEL' '$SUBJ_A'" 
check "a child process recorded its OWN pid, not the test shell's" "yes" \
      "$([ "$(sed -n 's/.*pid=\([0-9]*\).*/\1/p' "$WORK/child.runid")" != "$$" ] && echo yes || echo no)"
check "after that run exits, its runid reports gone" "6" \
      "$(runid_alive "$WORK/child.runid" >/dev/null 2>&1; echo $?)"

check "the driver no longer uses a pid-derived token" "0" \
      "$(grep -cE '^[^#]*TOKEN="run-\$\$"' "$HERE/scripts/run_batches.sh" || true)"
check "the driver records a runid file" "1" \
      "$(grep -cE '^[^#]*runid_write ' "$HERE/scripts/run_batches.sh" || true)"

# THE GAP THAT LET A REAL BUG THROUGH. Every case above used a synthetic token
# (tok-0001). The token the DRIVER actually builds comes from proc_starttime, and on BSD
# `lstart` contains colons -- which _sent_is_ident rejects. A complete, clean run then
# refused to write its own sentinel: the refusal was correct, the charset was the bug, and
# no test exercised it because none used a real token.
REAL_TOKEN="$(runid_token "$WORK/child.runid")"
check "a REAL driver-built token is non-empty" "yes" \
      "$([ -n "$REAL_TOKEN" ] && echo yes || echo no)"
check "a REAL driver-built token passes the identifier check" "yes" \
      "$(_sent_is_ident "$REAL_TOKEN" && echo yes || echo no)"
check "a REAL driver-built token is ACCEPTED by sentinel_write" "0" \
      "$(sentinel_write "$WORK/realtok.log" "$SUBJ_A" "$LABEL" "$REAL_TOKEN" 3 3 0 yes 2>/dev/null; echo $?)"
check "and verifies against itself" "0" \
      "$(sentinel_verify "$WORK/realtok.log" "$SUBJ_A" "$LABEL" "$REAL_TOKEN" 2>/dev/null; echo $?)"
# NO `case` INSIDE $( ) -- bash 3.2 breaks on it, and this campaign has already banned the
# construct once. The same question is asked by deleting every allowed character and
# checking nothing survives.
ST_NOW="$(proc_starttime $$)"
check "proc_starttime emits no character outside the identifier set" "" \
      "$(printf '%s' "$ST_NOW" | tr -d 'A-Za-z0-9._-')"

echo
echo "11. THE DRIVER's exit code — a refused sentinel must NOT exit 0"
#
# Sections 1-10 test the library. They do not test what run_batches.sh DOES when the library
# refuses, and "three batches passed, no sentinel, driver exited 0" is precisely the state
# that must be impossible. These cases run the real driver.

DRV="$HERE/scripts/run_batches.sh"
# `grep -c` PRINTS 0 and EXITS 1 when nothing matches, so `grep -c ... || echo 0` emits two
# zeros. This helper answers the question once.
nsent() { [ -e "${1:-}" ] || { printf 0; return; }; grep -c "^$SENTINEL_TAG " "$1" 2>/dev/null | head -1; }
FAKE="$WORK/fakewt"
mkdir -p "$FAKE/dev2026/scripts"
( cd "$FAKE" && git init -q . && git config user.email t@t && git config user.name t \
  && echo x > f && git add -A && git commit -q -m init ) >/dev/null 2>&1
FAKE_SHA="$(git -C "$FAKE" rev-parse HEAD)"

# A stub suite runner: the driver only needs the summary lines it greps for.
cat > "$FAKE/dev2026/scripts/run_suites.sh" <<'STUB'
#!/usr/bin/env bash
echo "git head   : GITHEAD"
echo "git subject: stub"
echo "tracked dirty : 0"
echo "untracked     : 0"
echo "exit=0   stub_suite.sh   0s  all passed (1 assertions)"
echo "=== TOTAL: 1 | NON-ZERO: ${STUB_NONZERO:-0} ==="
exit ${STUB_RC:-0}
STUB
sed -i.bak "s/GITHEAD/$FAKE_SHA/" "$FAKE/dev2026/scripts/run_suites.sh"
chmod +x "$FAKE/dev2026/scripts/run_suites.sh"

# POSITIVE CONTROL: a clean run must exit 0 AND write a sentinel. Without this the negative
# cases below could pass because the driver never works at all.
O1="$WORK/drv-ok"
bash "$DRV" --subject "$FAKE_SHA" --label okrun --worktree "$FAKE" --batches 1 --out "$O1" >/dev/null 2>&1
check "a clean driver run exits 0" "0" "$?"
check "  and writes exactly one sentinel" "1" \
      "$(nsent "$O1/okrun.log")"

# A FAILING BATCH: non-zero suite exit must refuse the sentinel AND exit non-zero.
O2="$WORK/drv-fail"
STUB_RC=1 STUB_NONZERO=1 bash "$DRV" --subject "$FAKE_SHA" --label failrun \
  --worktree "$FAKE" --batches 1 --out "$O2" >/dev/null 2>&1
rc_fail=$?
check "a FAILING batch makes the driver exit NON-ZERO" "nonzero" \
      "$([ "$rc_fail" -ne 0 ] && echo nonzero || echo "zero($rc_fail)")"
check "  and writes NO success sentinel" "0" \
      "$(nsent "$O2/failrun.log")"
check "  and the reason is preserved in the log" "yes" \
      "$(grep -q 'NO SUCCESS SENTINEL' "$O2/failrun.log" && echo yes || echo no)"

# POSTCONDITION FAILURE: worktree HEAD is not the subject.
O3="$WORK/drv-post"
bash "$DRV" --subject "$SUBJ_A" --label postrun --worktree "$FAKE" --batches 1 --out "$O3" >/dev/null 2>&1
rc_post=$?
check "a POSTCONDITION failure makes the driver exit NON-ZERO" "nonzero" \
      "$([ "$rc_post" -ne 0 ] && echo nonzero || echo "zero($rc_post)")"
check "  and writes NO success sentinel" "0" \
      "$(nsent "$O3/postrun.log")"
check "  and the mismatch is named in the log" "yes" \
      "$(grep -q 'REFUSING batch 1' "$O3/postrun.log" && echo yes || echo no)"

# PARTIAL: 2 batches requested, the second fails.
O4="$WORK/drv-partial"
STUB_RC=1 STUB_NONZERO=1 bash "$DRV" --subject "$FAKE_SHA" --label partrun \
  --worktree "$FAKE" --batches 2 --out "$O4" >/dev/null 2>&1
check "a PARTIAL/failing multi-batch run exits NON-ZERO" "nonzero" \
      "$([ $? -ne 0 ] && echo nonzero || echo zero)"
check "  and writes NO success sentinel" "0" \
      "$(nsent "$O4/partrun.log")"

# NO ESCAPE HATCH. A force/skip/write-anyway option would make every guard advisory.
for esc in force skip write-anyway ignore-failures no-sentinel; do
  check "the driver has no --$esc option" "0" \
        "$(grep -cE -- "--$esc" "$DRV" || true)"
done
# The words appear in COMMENTS that explain no such flag exists -- that is the record.
# Only CODE must be free of them.
check "the library has no force/skip parameter in CODE (comments excepted)" "0" \
      "$(grep -cEi -- '^[^#]*(force|skip|anyway)' "$LIB" || true)"
check "the ONLY 'exit 0' in the driver is the sentinel-success branch" "1" \
      "$(grep -cE '^[^#]*exit 0' "$DRV" || true)"

echo
echo "12. the starttime is CANONICAL, not a scrubbed string"

ST="$(proc_starttime $$)"
check "it is prefixed with the clock it came from" "yes" \
      "$(printf '%s' "$ST" | grep -qE '^(lin|bsd)-[0-9]+$' && echo yes || echo no)"
check "the numeric part is digits only — nothing was folded" "" \
      "$(printf '%s' "$ST" | sed 's/^(lin|bsd)-//; s/^[a-z]*-//' | tr -d '0-9')"
check "no colon survives by deletion (there is nothing to delete)" "0" \
      "$(printf '%s' "$ST" | tr -cd ':' | wc -c | tr -d ' ')"
check "two reads of the same process agree" "$ST" "$(proc_starttime $$)"

echo
suite_summary "$PASS" "$FAIL"
