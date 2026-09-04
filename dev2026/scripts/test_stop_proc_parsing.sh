#!/usr/bin/env bash
#
# How `production_stop.sh` reads /proc: ppid, starttime, and the descendant tree.
#
# WHY THIS EXISTS. Both readers used to take fixed fields out of `/proc/<pid>/stat`:
#
#     <pid> (<comm>) <state> <ppid> ... <starttime> ...
#
# `comm` is the executable name IN PARENTHESES and MAY CONTAIN SPACES AND PARENTHESES, so
# every field after the second shifts. `awk '{print $4}'` then returns the WRONG NUMBER --
# not an error, a plausible wrong answer -- and the two consequences are exactly the two
# things this script exists to guarantee:
#
#   children_of   a real child is missed, so a survivor is never checked;
#   starttime_of  the (pid, starttime) identity compares unrelated numbers, so PID reuse
#                 stops being detectable.
#
# ppid now comes from the LABELLED `PPid:` line of `/proc/<pid>/status`, which cannot
# shift. starttime still comes from `stat`, but the comm is removed at the LAST ')' first.
#
# THE SECOND RULE UNDER TEST: a pid whose parentage cannot be determined is NOT "not a
# child". The old loop skipped unreadable entries silently -- the same fail-OPEN shape as
# the jlist parser reading empty output as "nothing to stop".
#
#     ./scripts/test_stop_proc_parsing.sh

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
is_unknown() { case "$1" in UNKNOWN*) echo yes ;; *) echo no ;; esac; }

W="$(mktemp -d)"
trap 'rm -rf "$W"' EXIT

# A synthetic /proc. BOTH files, as the real one has: `status` carries the labelled PPid,
# `stat` carries starttime behind the parenthesised comm.
mkproc() {   # mkproc <root> <pid> <ppid> <starttime> <comm>
  mkdir -p "$1/$2"
  printf '%s (%s) S %s 0 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 %s 0 0\n' "$2" "$5" "$3" "$4" \
    > "$1/$2/stat"
  printf 'Name:\t%s\nState:\tS (sleeping)\nTgid:\t%s\nPid:\t%s\nPPid:\t%s\n' \
    "$5" "$2" "$2" "$3" > "$1/$2/status"
}

# SOURCED ONCE, IN THE PARENT SHELL. The first draft wrapped each case in ( ... ), which
# reads naturally and silently threw away every pass/fail increment -- the counters live in
# the subshell and die with it, so the suite would have reported a total that omitted most
# of its own assertions. `PROC` is an ordinary variable read at call time, so retargeting it
# per case gives the same isolation without the subshell.
WOA23_STOP_LIB_ONLY=1 . "$STOP"
use_proc() { PROC="$1"; : > "$SCAN_UNRESOLVED"; }

# `die` exits, so it must be called in a subshell for its status to be observable; the
# status of $( ) IS the status of the command inside it.
scan_rc() { ( unresolved_scan_must_be_empty ) >/dev/null 2>&1; echo $?; }
scan_out() { ( unresolved_scan_must_be_empty ) 2>&1; }

echo "COMM CONTAINING A SPACE — the case that shifted every later field"
P1="$W/p1"
mkproc "$P1" 100 1   1000 "gunicorn"
mkproc "$P1" 200 100 2000 "my prog"           # a space in comm
mkproc "$P1" 300 100 3000 "gunicorn"
use_proc "$P1"
# CAPTURED FIRST, then matched. `children_of 100 | grep -qx 200` looks natural and is
# wrong under `set -o pipefail`: grep -q exits at the first match, children_of takes
# SIGPIPE, and the PIPELINE reports failure -- so the assertion went red precisely when the
# pid it wanted came FIRST, and passed when it came last. The bug was in the test, not the
# code, and it was the more dangerous direction: a match read as a miss.
kids="$(children_of 100)"
  check "the space-named child is still found" "yes" \
        "$(printf '%s\n' "$kids" | grep -qx 200 && echo yes || echo no)"
  check "  and so is its ordinary sibling" "yes" \
        "$(printf '%s\n' "$kids" | grep -qx 300 && echo yes || echo no)"
  check "  exactly two children, no strangers" 2 "$(printf '%s\n' "$kids" | grep -c . )"
  check "its starttime is read correctly despite the space" 2000 "$(starttime_of 200)"
  check "  and the ordinary sibling's too" 3000 "$(starttime_of 300)"
  # The old code would have taken field 4 of a shifted line. Show the shift is real.
  check "PROOF the naive read WOULD have been wrong" "no" \
        "$(awk '{print $4}' "$P1/200/stat" | grep -qx 100 && echo yes || echo no)"

echo
echo "COMM CONTAINING PARENTHESES — the greedy cut must take the LAST one"
P2="$W/p2"
mkproc "$P2" 100 1   1000 "gunicorn"
mkproc "$P2" 210 100 2100 "weird)name"
mkproc "$P2" 220 100 2200 "(nested (paren) thing)"
use_proc "$P2"
kids2="$(children_of 100)"
  check "a child whose comm holds ')' is found" "yes" \
        "$(printf '%s\n' "$kids2" | grep -qx 210 && echo yes || echo no)"
  check "  and one with nested parens and spaces" "yes" \
        "$(printf '%s\n' "$kids2" | grep -qx 220 && echo yes || echo no)"
  check "starttime survives an embedded ')'" 2100 "$(starttime_of 210)"
  check "  and nested parens" 2200 "$(starttime_of 220)"

echo
echo "MISSING, EMPTY OR MALFORMED PPid — unresolved, and NEVER 'no child'"
for case_name in missing empty malformed nonnumeric; do
  P="$W/ppid-$case_name"
  mkproc "$P" 100 1   1000 gunicorn
  mkproc "$P" 400 100 4000 gunicorn
  case "$case_name" in
    missing)    grep -v '^PPid:' "$P/400/status" > "$P/400/s.tmp"; mv "$P/400/s.tmp" "$P/400/status" ;;
    empty)      printf 'Name:\tg\nPPid:\t\n'        > "$P/400/status" ;;
    malformed)  printf 'Name:\tg\nPPid:\tnot-a-pid\n' > "$P/400/status" ;;
    nonnumeric) printf 'Name:\tg\nPPid:\t12x4\n'    > "$P/400/status" ;;
  esac
  use_proc "$P"
    ppid_of 400 >/dev/null 2>&1; rc=$?
    check "PPid $case_name -> a non-zero, non-vanished code" "yes" \
          "$([ "$rc" -eq 4 ] && echo yes || echo no)"
    : > "$SCAN_UNRESOLVED"
    kids="$(children_of 100)"
    check "  it is NOT silently reported as a child" "no" \
          "$(printf '%s' "$kids" | grep -qx 400 && echo yes || echo no)"
    check "  it IS recorded as unresolved" "yes" \
          "$(grep -q '^400 ' "$SCAN_UNRESOLVED" && echo yes || echo no)"
    out="$(scan_out)"
    check "  and the scan then FAILS CLOSED" 2 "$(scan_rc)"
    check "  saying it is not 'no children'" "yes" "$(has "$out" "NOT reported as 'no children'")"
done

echo
echo "AN UNREADABLE status — exists but cannot be classified"
P3="$W/p3"
mkproc "$P3" 100 1   1000 gunicorn
mkproc "$P3" 500 100 5000 gunicorn
chmod 000 "$P3/500/status" 2>/dev/null
if [ -r "$P3/500/status" ]; then
  echo "  (skipped: this filesystem/uid ignores chmod 000 — cannot build the case)"
else
  use_proc "$P3"
    ppid_of 500 >/dev/null 2>&1
    check "an unreadable status returns the 'unreadable' code" 3 "$?"
    : > "$SCAN_UNRESOLVED"
    children_of 100 >/dev/null
    check "  it is recorded as unresolved, not skipped" "yes" \
          "$(grep -q '^500 rc=3' "$SCAN_UNRESOLVED" && echo yes || echo no)"
    check "  and the scan fails closed" 2 "$(scan_rc)"
fi
chmod 755 "$P3/500/status" 2>/dev/null

echo
echo "A VANISHED pid — genuinely gone, and NOT an unresolved failure"
P4="$W/p4"
mkproc "$P4" 100 1   1000 gunicorn
mkproc "$P4" 600 100 6000 gunicorn
rm -r "$P4/600"
use_proc "$P4"
  ppid_of 600 >/dev/null 2>&1
  check "a vanished pid returns the 'vanished' code" 2 "$?"
  : > "$SCAN_UNRESOLVED"
  children_of 100 >/dev/null
  check "  and is NOT recorded as unresolved" 0 "$(wc -l < "$SCAN_UNRESOLVED" | tr -d ' ')"
  check "  so a race with an exiting process does not fail the stop" 0 "$(scan_rc)"

echo
echo "A MULTI-LEVEL TREE — descendants, not just direct children"
P5="$W/p5"
mkproc "$P5" 100 1   1000 gunicorn        # master
mkproc "$P5" 110 100 1100 gunicorn        # worker
mkproc "$P5" 120 100 1200 "my prog"       # worker, awkward comm
mkproc "$P5" 111 110 1110 helper          # grandchild
mkproc "$P5" 112 111 1120 "deep (one)"    # great-grandchild
mkproc "$P5" 900 1   9000 gunicorn        # a stranger, same program name
use_proc "$P5"
  check "direct children are 110 and 120 only" 2 "$(children_of 100 | wc -l | tr -d ' ')"
  d="$(descendants_of 100 | LC_ALL=C sort | tr '\n' ' ')"
  check "descendants reach every level" "110 111 112 120 " "$d"
  check "  the grandchild is included" "yes" "$(has "$d" '111')"
  check "  and the great-grandchild" "yes" "$(has "$d" '112')"
  check "  the stranger is NOT" "no" "$(has "$d" '900')"
  check "each descendant keeps a readable starttime" "1110" "$(starttime_of 111)"
  check "  including the one with parens and a space" "1120" "$(starttime_of 112)"

echo
echo "  a pid claimed by two parents is emitted ONCE, and the walk terminates"
# The first draft built a "cycle" by pointing 130 at 131 -- which only removed 130 from the
# master's children, so the walk had nothing to traverse and the test proved nothing. A
# reachable repeat is what actually exercises the `seen` set.
P6="$W/p6"
mkproc "$P6" 100 1   1000 gunicorn
mkproc "$P6" 130 100 1300 gunicorn
mkproc "$P6" 131 130 1310 gunicorn
mkproc "$P6" 132 131 1320 gunicorn
printf 'Name:\tg\nPPid:\t130\n' > "$P6/132/status"   # 132 now claims 130 as its parent too
use_proc "$P6"
d="$(descendants_of 100 | LC_ALL=C sort | tr '\n' ' ')"
check "every reachable descendant appears" "130 131 132 " "$d"
n="$(descendants_of 100 | wc -l | tr -d ' ')"
u="$(descendants_of 100 | sort -u | wc -l | tr -d ' ')"
check "  and none is emitted twice" "$u" "$n"

echo
echo "  a tree deeper than the bound is REFUSED, not silently truncated"
P8="$W/p8"
mkproc "$P8" 100 1   1000 gunicorn
prev=100
for i in 1 2 3 4 5 6; do
  pid=$((150 + i)); mkproc "$P8" "$pid" "$prev" "$((1500 + i))" gunicorn; prev=$pid
done
use_proc "$P8"
out="$( ( WOA23_STOP_MAX_DEPTH=3 descendants_of 100 ) 2>&1 )"
rc="$( ( WOA23_STOP_MAX_DEPTH=3 descendants_of 100 ) >/dev/null 2>&1; echo $? )"
check "the bounded walk fails rather than reporting a partial tree" 2 "$rc"
check "  and says the tree is deeper than the bound" "yes" "$(has "$out" 'deeper than 3 levels')"
check "  the default bound comfortably covers a 6-deep tree" 6 \
      "$(descendants_of 100 | wc -l | tr -d ' ')"

echo
echo "PID REUSE — the identity is (pid, starttime), and starttime must be read right"
P7="$W/p7"
mkproc "$P7" 100 1   1000 gunicorn
mkproc "$P7" 140 100 1400 gunicorn
use_proc "$P7"
  before="$(starttime_of 140)"
  # The pid is recycled: same number, different process, different starttime -- and a comm
  # that would have shifted the naive field read.
  mkproc "$P7" 140 1 7777 "other prog"
  after="$(starttime_of 140)"
  check "starttime before reuse" 1400 "$before"
  check "starttime after reuse"  7777 "$after"
  check "  so the identity CHANGES and reuse is detectable" "yes" \
        "$([ "$before" != "$after" ] && echo yes || echo no)"
  check "  and the recycled pid is no longer a child of the master" "no" \
        "$(printf '%s\n' "$(children_of 100)" | grep -qx 140 && echo yes || echo no)"

echo
echo "THE CORE INVARIANT: a parse failure is never read as 'this pid is not a child'"
# Stated as a table, the way the jlist suite states its own: every failure mode must end
# up recorded, and none of them may quietly resolve to "not mine".
for bad in missing empty malformed nonnumeric; do
  P="$W/inv-$bad"
  mkproc "$P" 100 1   1000 gunicorn
  mkproc "$P" 700 100 7000 gunicorn
  case "$bad" in
    missing)    grep -v '^PPid:' "$P/700/status" > "$P/700/t"; mv "$P/700/t" "$P/700/status" ;;
    empty)      printf 'PPid:\t\n'        > "$P/700/status" ;;
    malformed)  printf 'PPid:\tzzz\n'     > "$P/700/status" ;;
    nonnumeric) printf 'PPid:\t9y9\n'     > "$P/700/status" ;;
  esac
  use_proc "$P"; : > "$SCAN_UNRESOLVED"; children_of 100 >/dev/null
    check "  '$bad' -> recorded, so the stop cannot proceed silently" "yes" \
          "$(grep -q '^700 ' "$SCAN_UNRESOLVED" && echo yes || echo no)"
done


echo
echo "STARTTIME MUST FAIL CLOSED — an unknown starttime is never a usable identity"
# THE DEFECT THIS BLOCK EXISTS FOR. starttime_of used to return rc 0 with an EMPTY string
# whenever stat was truncated. `MASTER_START="$(starttime_of "$PID")" || die` therefore
# never fired, the identity became "<pid>:" with no starttime, and the survivor check --
# which tested `[ -n "$now" ]` -- read a LIVE process as GONE and reported a clean stop.
# Success on an unverified premise: the bs3v1 shape, in a second place.
P9="$W/p9"
mkproc "$P9" 100 1 1000 gunicorn
mk_raw() { mkdir -p "$P9/$1"; printf '%s\n' "$2" > "$P9/$1/stat"; printf 'PPid:\t100\n' > "$P9/$1/status"; }
mk_raw 801 '801 (gunicorn) S 100 0 0 0 -1 0 0 0'                                            # truncated
mk_raw 802 '802 (gunicorn) S 100 0 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 notanumber 0 0'         # non-numeric
mk_raw 803 '803 (gunicorn) S 100 0 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0'                        # exactly short
mk_raw 804 ''                                                                                # empty
mk_raw 805 '805 gunicorn S 100 0 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 5000 0 0'                  # no parens
mk_raw 806 '806 (gunicorn) S 100 0 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 0 0  0 0'                    # a plausible 0
use_proc "$P9"
for bad in 801 802 803 804 805; do
  v="$(starttime_of "$bad" 2>/dev/null)"; rc=$?
  check "  $bad: rc is 1, not 0" 1 "$rc"
  check "  $bad: and NOTHING is printed" "" "$v"
done
# 806 is the subtle one: a short field list can put a REAL token in position 20.
v806="$(starttime_of 806 2>/dev/null)"; rc806=$?
check "  806: a short line that yields a plausible '0' is still refused" 1 "$rc806"
check "  806: and prints nothing" "" "$v806"
check "a WELL-FORMED stat still yields its starttime" 1000 "$(starttime_of 100)"
use_proc "$P1"
check "  a comm with a space still parses" 2000 "$(starttime_of 200)"
use_proc "$P2"
check "  and a comm with parentheses" 2100 "$(starttime_of 210)"

echo
echo "PROC_STATE — three outcomes, and 'I cannot tell' is one of them"
use_proc "$P9"
check "a well-formed live process is ALIVE with its starttime" "ALIVE 1000" "$(proc_state 100)"
check "a truncated stat on a LIVE process is UNKNOWN, not GONE" "yes" \
      "$(is_unknown "$(proc_state 801)")"
check "  and it names the reason" "yes" "$(has "$(proc_state 801)" 'stat-unparsable')"
check "a non-numeric starttime is UNKNOWN, not GONE" "yes" \
      "$(is_unknown "$(proc_state 802)")"
mkdir -p "$P9/810"; printf 'x\n' > "$P9/810/stat"; chmod 000 "$P9/810/stat" 2>/dev/null
if [ -r "$P9/810/stat" ]; then
  echo "  (skipped: this uid ignores chmod 000)"
else
  check "an unreadable stat on a LIVE process is UNKNOWN" "yes" \
        "$(is_unknown "$(proc_state 810)")"
  check "  reported as unreadable, not unparsable" "yes" "$(has "$(proc_state 810)" 'stat-unreadable')"
fi
chmod 644 "$P9/810/stat" 2>/dev/null
check "ONLY a missing /proc/<pid> is GONE" "GONE" "$(proc_state 99999)"
check "  a live-but-unreadable process is NEVER classified GONE" "no" \
      "$(has "$(proc_state 801)" 'GONE')"

echo
echo "SOURCE PROPERTIES — the old field reads are gone, the guarantees are not"
code="$(grep -v '^[[:space:]]*#' "$STOP")"
check "no naive field-4 ppid read remains" 0 \
      "$(printf '%s' "$code" | grep -cE "awk '\{print \\\$4\}'" || true)"
check "ppid comes from the labelled PPid: line" 1 \
      "$(printf '%s' "$code" | grep -c "s/\^PPid:" || true)"
check "starttime removes the comm before taking a field" 1 \
      "$(printf '%s' "$code" | grep -c "sed 's/\.\*) //'" || true)"
check "children still come from /proc, never ps" 0 \
      "$(printf '%s' "$code" | grep -cE 'ps -ef|ps aux' || true)"
check "the unresolved record fails closed" 1 \
      "$(printf '%s' "$code" | grep -c 'unresolved_scan_must_be_empty$' || true)"
check "  and it is actually called after the tree is built" 1 \
      "$(printf '%s' "$code" | grep -cE '^unresolved_scan_must_be_empty$' || true)"

echo
echo "  and every guarantee the review listed is still asserted"
check "(pid, starttime) identity" "yes" "$(has "$code" 'starttime_of')"
check "named-app only; 'all' refused" "yes" "$(has "$code" "'all' would reach every app")"
check "exact app name matching" "yes" "$(has "$code" 'p.name === app')"
check "PM2_HOME required, no default" "yes" "$(has "$code" 'is required and has no default')"
check "app absent -> NOTFOUND" 1 "$(printf '%s' "$code" | grep -cE '^[[:space:]]*NOTFOUND\)' || true)"
check "pid 0 + stopped -> STOPPED" 1 "$(printf '%s' "$code" | grep -cE '^[[:space:]]*STOPPED\)' || true)"
check "survivor -> CLEANUP_FAIL" "yes" "$(has "$code" 'CLEANUP_FAIL')"
check "no SIGKILL" 0 "$(printf '%s' "$code" | grep -cE 'kill -9|kill -KILL' || true)"
check "  and the policy sentence survives" "yes" "$(has "$code" 'SIGKILL is not sent here')"
check "no global save" 0 "$(printf '%s' "$code" | grep -cE '\bpm2 save\b|"\$PM2" save' || true)"
check "no resurrect" 0 "$(printf '%s' "$code" | grep -cE 'resurrect' || true)"
check "no 'pm2 ... all'" 0 "$(printf '%s' "$code" | grep -cE '"\$PM2" (stop|delete|restart) all' || true)"

echo
echo "THE JLIST PARSER IS UNTOUCHED BY THIS CHANGE"
# The review asked for this explicitly. The resolver's own suite is the behavioural
# evidence; here the point is that the new /proc code did not reach into it.
# NOT a grep for "status": pm2_env.status is legitimately part of the resolver's own
# logic, and the first version of this assertion counted those and reported 6.
check "jlist_resolve reads no /proc and calls no /proc helper" 0 \
      "$(awk '/^jlist_resolve\(\)/{i=1; next} i && /^}/{i=0}
              i && /\$PROC|ppid_of|starttime_of|children_of|descendants_of/{n++}
              END{print n+0}' "$STOP")"
check "the resolver still returns the four verdicts" 4 \
      "$(awk '/^jlist_resolve\(\)/{i=1} i{ if (/console\.log\("OK /) o++;
                                           if (/console\.log\("NOTFOUND"\)/) o++;
                                           if (/console\.log\("STOPPED"\)/) o++;
                                           if (/PROBLEM/) p=1 } END{print o+p}' "$STOP")"

echo
check "the script still parses" 0 "$(bash -n "$STOP" >/dev/null 2>&1; echo $?)"

echo
suite_summary "$pass" "$fail"
