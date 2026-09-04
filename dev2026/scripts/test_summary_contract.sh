#!/usr/bin/env bash
#
# THE SUITE SUMMARY CONTRACT, both halves of it.
#
# WHY THIS FILE EXISTS. The batch runner did not parse anything. It displayed each suite's
# LAST stdout line, and the totals were read off that display -- which is looking, not
# parsing, and it is wrong in a way that cannot announce itself:
#
#   test_staging_store.py       printed its summary and then a CAVEAT, so the caveat was
#                               the final line, the caveat was displayed, and its 24
#                               assertions were invisible to every total ever taken.
#   test_bootstrap_delivery.sh  printed `=== 100 passed, 0 failed ===`, a private shape
#                               nothing could tally, losing 100 more.
#
# The batch log therefore said 4667 where the truth was 4791, and said it in the position
# where a reader expects the total. A count that is wrong is worse than a count that is
# absent, because nobody goes looking for it.
#
# The contract is one line, last, exactly once:
#
#     ASSERTIONS=<n> FAILED=<m>
#
# WHAT IS TESTED HERE IS BEHAVIOUR, at three levels:
#
#   1. the EMITTERS -- shell and Python -- produce byte-identical lines and derive the exit
#      status from the count rather than being told it;
#   2. the READER refuses every way the contract can be broken;
#   3. the REAL RUNNER, executed against a fixture tree of deliberately broken suites,
#      refuses them, says why, counts them, and exits non-zero.
#
# Level 3 matters because levels 1 and 2 can both be perfect while nothing calls them.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV="$(cd "$HERE/.." && pwd)"
RUNNER="$HERE/run_suites.sh"
LIB="$HERE/lib_suite_summary.sh"
PYLIB="$DEV/bench/suite_summary.py"

PASS=0; FAIL=0
ok()  { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad() { FAIL=$((FAIL+1)); printf '  FAIL %s\n' "$1"; [ $# -gt 1 ] && printf '       %s\n' "$2"; }
is()  { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "expected [$3], got [$2]"; fi; }
has() { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1" "output does not contain [$3]" ;; esac; }
hasnt(){
  # An empty haystack is not an absence; see test_bootstrap_delivery.sh for the same guard.
  if [ -z "$2" ]; then bad "$1" "the output was EMPTY — nothing was checked"; return; fi
  case "$2" in *"$3"*) bad "$1" "output unexpectedly contains [$3]" ;; *) ok "$1" ;; esac
}

T="$(mktemp -d "${TMPDIR:-/tmp}/summary.XXXXXX")"
trap 'chmod -R u+rwX "$T" 2>/dev/null; rm -rf "$T"' EXIT

#: A stdout file with the given lines.
mkout() { local f="$T/$1"; shift; printf '%s\n' "$@" > "$f"; printf '%s' "$f"; }

echo "=== the suite summary contract ==="
echo

# ============================================================== 1. the shell emitter ===
echo "1. the shell emitter"
# `emit` does NOT try to set a variable for its caller: it runs inside `$( )`, and a
# variable written in a command substitution is lost to the parent -- the same subshell
# trap that leaked 15 processes out of test_requests.sh. The caller reads `$?` instead.
emit() {  # <passed> <failed> -> stdout, exits with the emitter's status
  bash -c '. "$1"; suite_summary "$2" "$3"' _ "$LIB" "$1" "$2" 2>&1
}
o="$(emit 100 0)"; rc=$?
is "1a  a clean suite exits 0"                     "$rc" "0"
is "1b  ... its final line is the contract"        "$(printf '%s\n' "$o" | tail -1)" "ASSERTIONS=100 FAILED=0"
is "1c  ... with the prose line before it"         "$(printf '%s\n' "$o" | head -1)" "all passed (100 assertions)"
o="$(emit 97 3)"; rc=$?
is "1d  a failing suite exits 1"                   "$rc" "1"
is "1e  ... ASSERTIONS is the TOTAL, not the passes" \
   "$(printf '%s\n' "$o" | tail -1)" "ASSERTIONS=100 FAILED=3"
is "1f  ... and the prose names both"              "$(printf '%s\n' "$o" | head -1)" "3 FAILED, 97 passed"
o="$(emit 0 0)"; rc=$?
is "1g  zero assertions is legal"                  "$o" "all passed (0 assertions)
ASSERTIONS=0 FAILED=0"
is "1g2 ... and exits 0"                           "$rc" "0"
# THE EXIT STATUS IS DERIVED, never supplied. There is no argument a suite could pass to
# claim FAILED=3 and exit 0, which is why that combination cannot be produced by accident.
is "1h  the emitter takes no exit-status argument" "0" \
   "$(grep -c 'suite_summary() {   # <passed> <failed> <' "$LIB")"

o="$(emit x 0)"; rc=$?; is "1i  a non-numeric assertion count is refused" "$rc" "2"
has "1j  ... by name"                                                "$o" "passed is not a number"
o="$(emit 10 y)"; rc=$?; is "1k  a non-numeric failure count is refused" "$rc" "2"
o="$(bash -c '. "$1"; suite_summary_line 5 9' _ "$LIB" 2>&1)"; rc=$?
is "1l  FAILED exceeding ASSERTIONS is refused"                      "$rc" "2"
has "1m  ... by name"                                                "$o" "exceeds ASSERTIONS"
o="$(bash -c '. "$1"; suite_summary_line 12 3' _ "$LIB" 2>&1)"
is "1n  the line-only form prints only the line"                     "$o" "ASSERTIONS=12 FAILED=3"
echo

# ============================================================= 2. the Python emitter ===
#
# THE TWO EMITTERS MUST AGREE BYTE FOR BYTE. The runner parses one format and does not know
# which language produced it; two nearly-identical formats would be the original defect
# again, with better manners.
echo "2. the Python emitter, and that it agrees with the shell one"
PY="$DEV/.venv/bin/python"
if [ ! -x "$PY" ]; then
  for i in a b c d e f g; do ok "2$i  Python emitter (skipped: no .venv/bin/python)"; done
else
  pyemit() {  # <passed> <failed> -> stdout, exits with the emitter's status
    ( cd "$DEV" && PYTHONPATH=. "$PY" -c '
import sys
from bench.suite_summary import summary
raise SystemExit(summary(int(sys.argv[1]), int(sys.argv[2])))' "$1" "$2" 2>&1 )
  }
  o="$(pyemit 100 0)"; rc=$?
  is "2a  a clean Python suite exits 0"        "$rc" "0"
  is "2b  ... byte-identical to the shell form" "$o" "$(emit 100 0)"
  o="$(pyemit 97 3)"; rc=$?
  is "2c  a failing Python suite exits 1"      "$rc" "1"
  is "2d  ... byte-identical to the shell form" "$o" "$(emit 97 3)"
  o="$(pyemit 0 0)"
  is "2e  zero assertions agrees too"          "$o" "$(emit 0 0)"
  o="$( cd "$DEV" && PYTHONPATH=. "$PY" -c '
from bench.suite_summary import summary_line
summary_line(5, 9)' 2>&1 )"; rc=$?
  is "2f  FAILED exceeding ASSERTIONS is refused"  "$rc" "1"
  has "2g  ... by name"                            "$o" "exceeds ASSERTIONS"
fi
echo

# ================================================================== 3. the reader =====
#
# One case per way the contract can be broken, each against a REAL file rather than a
# string, because the reader reads files.
echo "3. the reader — every way the contract can be broken"

f="$(mkout normal 'some output' 'all passed (10 assertions)' 'ASSERTIONS=10 FAILED=0')"
is  "3a  NORMAL: a well-formed summary is accepted" "$(suite_summary_problem "$f" 0)" ""
is  "3b  ... and its counts are read back"          "$(suite_summary_counts "$f")" "10 0"

f="$(mkout caveat 'all passed (24 assertions)' 'ASSERTIONS=24 FAILED=0' \
     'Deployment machinery only. NOT evidence about real WOA23 data.')"
has "3c  CAVEAT AFTER SUMMARY is refused"  "$(suite_summary_problem "$f" 0)" "not the final line"
has "3d  ... and the offending line is quoted" "$(suite_summary_problem "$f" 0)" "Deployment machinery only"

f="$(mkout blankafter 'ASSERTIONS=10 FAILED=0' '')"
has "3e  even a BLANK line after the summary is refused" \
    "$(suite_summary_problem "$f" 0)" "not the final line"

f="$(mkout missing 'ran some things' 'all passed (10 assertions)')"
has "3f  MISSING: no contract line at all is refused" \
    "$(suite_summary_problem "$f" 0)" "no ASSERTIONS=<n> FAILED=<m> summary line"

for badline in 'ASSERTIONS=x FAILED=0' 'ASSERTIONS=10 FAILED=' 'ASSERTIONS= FAILED=0' \
               'ASSERTIONS=10  FAILED=0' 'assertions=10 failed=0' 'ASSERTIONS=10 FAILED=0 extra' \
               'ASSERTIONS=-1 FAILED=0' 'ASSERTIONS=10,FAILED=0'; do
  f="$(mkout "malformed" 'output' "$badline")"
  has "3g  MALFORMED refused: $badline" "$(suite_summary_problem "$f" 0)" "no ASSERTIONS=<n>"
done

f="$(mkout dup 'ASSERTIONS=10 FAILED=0' 'more output' 'ASSERTIONS=20 FAILED=0')"
has "3h  DUPLICATED is refused"  "$(suite_summary_problem "$f" 0)" "appears 2 times"
f="$(mkout dup3 'ASSERTIONS=1 FAILED=0' 'ASSERTIONS=2 FAILED=0' 'ASSERTIONS=3 FAILED=0')"
has "3i  ... three times too"    "$(suite_summary_problem "$f" 0)" "appears 3 times"
# The duplicate check must not be satisfied by the LAST one being well placed -- that is
# precisely the shape where a helper emits a summary and the suite emits another.
f="$(mkout dupsame 'ASSERTIONS=10 FAILED=0' 'ASSERTIONS=10 FAILED=0')"
has "3j  ... even when both are identical" "$(suite_summary_problem "$f" 0)" "appears 2 times"

f="$(mkout mm1 'ASSERTIONS=10 FAILED=0')"
has "3k  MISMATCH: FAILED=0 but exit 1 is refused" \
    "$(suite_summary_problem "$f" 1)" "reports FAILED=0 but exited 1"
f="$(mkout mm2 'ASSERTIONS=10 FAILED=2')"
has "3l  MISMATCH: FAILED=2 but exit 0 is refused" \
    "$(suite_summary_problem "$f" 0)" "reports FAILED=2 but exited 0"
is  "3m  ... FAILED=2 with exit 1 is accepted" "$(suite_summary_problem "$f" 1)" ""
f="$(mkout mm3 'ASSERTIONS=10 FAILED=0')"
is  "3n  ... FAILED=0 with exit 0 is accepted" "$(suite_summary_problem "$f" 0)" ""

f="$(mkout over 'ASSERTIONS=3 FAILED=9')"
has "3o  FAILED exceeding ASSERTIONS is refused" "$(suite_summary_problem "$f" 1)" "exceeds ASSERTIONS"

f="$(mkout rc 'ASSERTIONS=10 FAILED=0')"
has "3p  a non-numeric exit status is refused" "$(suite_summary_problem "$f" '')" "exit status is not a number"
has "3q  a stdout file that does not exist is refused" \
    "$(suite_summary_problem "$T/no-such-file" 0)" "no stdout was captured"
echo

# ================================================= 4. the REAL RUNNER, end to end =====
#
# A fixture tree holding the real `run_suites.sh` and the real library, with fake suites
# beside them. The runner derives its own root from BASH_SOURCE, so it globs the fixture's
# `scripts/` and `bench/` and nothing of the real tree. This is what proves the reader is
# actually WIRED IN: every assertion above could pass while the runner ignored it.
echo "4. the real runner, against a fixture tree of deliberately broken suites"
FX="$T/fixture"
mkdir -p "$FX/scripts" "$FX/bench"
cp "$RUNNER" "$FX/scripts/run_suites.sh"
cp "$LIB"    "$FX/scripts/lib_suite_summary.sh"
chmod 755 "$FX/scripts/run_suites.sh"

mkfake() {  # <name> <exit> <line...>
  local n="$1" rc="$2"; shift 2
  { printf '#!/usr/bin/env bash\n'
    for l in "$@"; do printf 'printf %s\\\\n %s\n' "'%s'" "'$l'"; done
    printf 'exit %s\n' "$rc"
  } > "$FX/scripts/test_$n.sh"
  chmod 755 "$FX/scripts/test_$n.sh"
}

runfx() { ( cd "$T" && bash "$FX/scripts/run_suites.sh" "$@" 2>&1 ); }

# -- 4A. the positive path: three good suites, totals derived from their summaries.
mkfake good1 0 'all passed (10 assertions)' 'ASSERTIONS=10 FAILED=0'
mkfake good2 0 'all passed (32 assertions)' 'ASSERTIONS=32 FAILED=0'
mkfake good3 0 'all passed (8 assertions)'  'ASSERTIONS=8 FAILED=0'
o="$(runfx good)"; rc=$?
is  "4a  three clean fixture suites -> exit 0"        "$rc" "0"
has "4b  ... ASSERTIONS is the SUM of their summaries" "$o" "ASSERTIONS: 50 |"
has "4c  ... with no assertion failures"               "$o" "ASSERTION FAILURES: 0"
has "4d  ... and no contract violations"               "$o" "CONTRACT-VIOLATIONS: 0"

# -- 4B. one broken suite of each kind, run ALONE so its effect is unambiguous.
mkfake caveatlast 0 'all passed (24 assertions)' 'ASSERTIONS=24 FAILED=0' 'a trailing caveat'
o="$(runfx caveatlast)"; rc=$?
is  "4e  CAVEAT AFTER SUMMARY -> the batch exits non-zero" "$rc" "1"
has "4f  ... named as a contract violation"                "$o" "SUMMARY CONTRACT VIOLATION"
has "4g  ... explaining that the summary is not last"      "$o" "not the final line"
has "4h  ... counted as a violation"                       "$o" "CONTRACT-VIOLATIONS: 1"
# AND ITS ASSERTIONS ARE NOT COUNTED. This is the whole defect: a suite whose summary
# cannot be trusted must not contribute a number that looks trustworthy.
has "4i  ... and its 24 assertions are NOT added to the total" "$o" "ASSERTIONS: 0 |"

mkfake nosummary 0 'all passed (10 assertions)'
o="$(runfx nosummary)"; rc=$?
is  "4j  MISSING summary -> exit 1"        "$rc" "1"
has "4k  ... named"                        "$o" "no ASSERTIONS=<n>"
has "4l  ... contributes nothing"          "$o" "ASSERTIONS: 0 |"

mkfake malformed 0 'ASSERTIONS=ten FAILED=0'
o="$(runfx malformed)"; rc=$?
is  "4m  MALFORMED summary -> exit 1"      "$rc" "1"
has "4n  ... counted as a violation"       "$o" "CONTRACT-VIOLATIONS: 1"

mkfake duplicated 0 'ASSERTIONS=10 FAILED=0' 'ASSERTIONS=10 FAILED=0'
o="$(runfx duplicated)"; rc=$?
is  "4o  DUPLICATED summary -> exit 1"     "$rc" "1"
has "4p  ... named as a duplicate"         "$o" "appears 2 times"
has "4q  ... and 10 is not counted once anyway" "$o" "ASSERTIONS: 0 |"

mkfake liar 0 'ASSERTIONS=10 FAILED=4'
o="$(runfx liar)"; rc=$?
is  "4r  MISMATCH FAILED=4 with exit 0 -> exit 1" "$rc" "1"
has "4s  ... named"                        "$o" "reports FAILED=4 but exited 0"
mkfake liar2 3 'ASSERTIONS=10 FAILED=0'
o="$(runfx liar2)"; rc=$?
is  "4t  MISMATCH FAILED=0 with exit 3 -> exit 1" "$rc" "1"
has "4u  ... named"                        "$o" "reports FAILED=0 but exited 3"

# -- 4C. a genuinely failing suite is NOT a contract violation: it reports honestly.
mkfake honest 1 '3 FAILED, 7 passed' 'ASSERTIONS=10 FAILED=3'
o="$(runfx honest)"; rc=$?
is  "4v  an honestly failing suite -> exit 1"          "$rc" "1"
hasnt "4w  ... is NOT a contract violation"            "$o" "SUMMARY CONTRACT VIOLATION"
has "4x  ... its assertions ARE counted"               "$o" "ASSERTIONS: 10 |"
has "4y  ... and its failures are counted"             "$o" "ASSERTION FAILURES: 3"
has "4z  ... with zero contract violations"            "$o" "CONTRACT-VIOLATIONS: 0"

# -- 4D. one broken suite among good ones must not be absorbed by them.
o="$(runfx good caveatlast)"; rc=$?
is  "4aa a violation beside clean suites still fails the batch" "$rc" "1"
has "4ab ... the clean 50 are counted"      "$o" "ASSERTIONS: 50 |"
has "4ac ... the violator's 24 are not"     "$o" "CONTRACT-VIOLATIONS: 1"
echo

# ============================================ 5. the positive control, at 4791 ========
#
# REQUIREMENT: the corrected tally, given the assertion counts the 56 suites of subject
# 594737c actually reported, must come to EXACTLY 4791 -- the true total the old display
# under-reported as 4667.
#
# THESE ARE THE RECORDED FIGURES, transcribed from that run's per-suite stdout. The first
# draft of this block had 56 numbers I had written from memory; they summed to something
# else, and the assertion caught it. Invented data that happens to sum correctly would have
# been worse than invented data that does not, so it is worth saying plainly that these are
# transcribed and not composed.
#
# They are a fixture for the ARITHMETIC, not an expectation about any suite's content:
# nothing here asserts what a suite should count, only that summing what 56 suites report
# gives 4791 rather than something 124 short.
echo "5. positive control — the recorded 594737c counts must sum to exactly 4791"
COUNTS='34 100 49 145 90 63 106 21 76 305 76 146 42 79 61 69 98 59 56 64 55 27 79 30 
41 29 84 33 58 101 181 113 66 297 82 61 67 26 59 72 92 221 138 77 106 148 102 
84 24 61 37 100 165 50 67 19'
n=0; FIRSTC=""
for c in $COUNTS; do
  n=$((n+1)); [ -n "$FIRSTC" ] || FIRSTC="$c"
  mkfake "ctl$n" 0 "all passed ($c assertions)" "ASSERTIONS=$c FAILED=0"
done
is "5a  the control has 56 suites, as the batch did" "$n" "56"
o="$(runfx ctl)"; rc=$?
is  "5b  the positive control exits 0"                "$rc" "0"
has "5c  and totals EXACTLY 4791"                     "$o" "ASSERTIONS: 4791 |"
has "5d  with no assertion failures"                  "$o" "ASSERTION FAILURES: 0"
has "5e  and no contract violations"                  "$o" "CONTRACT-VIOLATIONS: 0"
hasnt "5f  and never reports the old, wrong 4667"     "$o" "ASSERTIONS: 4667"
# NON-VACUITY: if ONE suite's summary is displaced by a caveat, the total must FALL by
# exactly that suite's count and the batch must fail. That is precisely what happened
# silently at 4667 -- except that nothing failed and the shortfall was reported as the
# total. The expected figure is DERIVED from the fixture, not written down: a hand-typed
# expectation here would be one more number nobody can check.
mkfake ctl1 0 "all passed ($FIRSTC assertions)" "ASSERTIONS=$FIRSTC FAILED=0" 'a trailing caveat'
o="$(runfx ctl)"; rc=$?
is  "5g  displacing ONE summary fails the batch"      "$rc" "1"
has "5h  ... and the total falls by exactly that suite's count"     "$o" "ASSERTIONS: $((4791 - FIRSTC)) |"
hasnt "5i  ... so 4791 is no longer claimed"          "$o" "ASSERTIONS: 4791"
has "5j  ... and it is reported as a contract violation" "$o" "CONTRACT-VIOLATIONS: 1"
echo

# ================================================ 6. every real suite obeys it ========
#
# A structural sweep, and it is honest about being one: it proves each suite CALLS the
# contract, not that its count is right. The counts are proved by running them, which the
# batch does 56 times.
echo "6. every real suite is wired to the contract"
missing=""
for f in "$HERE"/test_*.sh; do
  grep -q 'lib_suite_summary.sh' "$f" || missing="$missing $(basename "$f")"
done
is "6a  every shell suite sources the summary library" "${missing:- none}" " none"
missing=""
for f in "$DEV"/bench/test_*.py; do
  grep -q 'bench.suite_summary' "$f" || missing="$missing $(basename "$f")"
done
is "6b  every Python suite imports the summary module" "${missing:- none}" " none"
is "6c  the runner reads the contract, not the last line it happens to see" "1" \
   "$(grep -c 'suite_summary_problem "\$slot/stdout.txt" "\$rc"' "$RUNNER")"
is "6d  the runner's totals come from the summaries" "1" \
   "$(grep -c 'ASSERT_TOTAL=\$((ASSERT_TOTAL + s_assert))' "$RUNNER")"
is "6e  a contract violation fails the batch" "1" \
   "$(grep -c '\[ "\$CONTRACT_BAD" -eq 0 \] || exit 1' "$RUNNER")"
is "6f  the format is defined once, not per suite" "1" \
   "$(grep -c "^SUITE_SUMMARY_RE=" "$LIB")"
is "6g  the Python module emits the same format" "1" \
   "$(grep -c 'print(f"ASSERTIONS={assertions} FAILED={failed}")' "$PYLIB")"
echo

suite_summary "$PASS" "$FAIL"
