#!/usr/bin/env bash
#
# The summary contract, asserted on the ORDERING that an EXIT trap can break.
#
# WHY THIS EXISTS. `test_s2perf_driver.sh` kept its work directory on failure and announced
# it from an EXIT trap. A trap runs after the script's last command, so the announcement
# landed AFTER `ASSERTIONS=<n> FAILED=<m>`. The runner reported
#
#     SUMMARY CONTRACT VIOLATION: the summary is not the final line
#
# and printed that INSTEAD of the failing assertions. The fault fired only when `fail > 0`,
# so a failing run hid its own failures behind a formatting fault — the one case where the
# detail was needed. `test_summary_contract.sh` checks the contract's shape; nothing checked
# that a trap could not append to it.
#
# WHAT IS ASSERTED HERE is behaviour, not source text: two harnesses are built that use the
# REAL `lib_suite_summary.sh` and the same keep-notice/trap structure, one passing and one
# failing, and their actual output is inspected. A source-grep would pass on a script that
# printed the line and then broke it again from a second trap.
set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then pass=$((pass+1)); echo "  ok   $1"
  else fail=$((fail+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}

# Helpers rather than an inline `case` inside $( ): a `)` in a case pattern can terminate
# the command substitution during parsing, which silently turned four of the assertions
# below into syntax errors the first time this suite was run. Caught by running it.
is_contract_line() { case "$1" in ASSERTIONS=*" "FAILED=*) echo yes ;; *) echo no ;; esac; }
contains_text()    { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

WORK="$(mktemp -d)"
trap 'chmod -R u+w "$WORK" 2>/dev/null; find "$WORK" -mindepth 1 -delete; rmdir "$WORK"' EXIT

# A harness with the SAME shape as the fixed suite: a keep-notice emitted before the
# summary, and a trap that only speaks if the notice did not.
make_harness() {   # make_harness <path> <failcount>
  cat > "$1" <<HARNESS
#!/usr/bin/env bash
set -uo pipefail
. "$HERE/scripts/lib_suite_summary.sh"
pass=3; fail=$2
WORK=/tmp/harness-work-\$\$
_keep_noticed=no
keep_notice() {
  if [ "\$fail" -gt 0 ]; then echo "state kept for inspection: \$WORK"; _keep_noticed=yes; fi
}
cleanup_work() {
  if [ "\$fail" -gt 0 ]; then
    [ "\$_keep_noticed" = yes ] || echo "state kept for inspection: \$WORK"
    return 0
  fi
}
trap cleanup_work EXIT
echo "  ok   something"
[ "\$fail" -gt 0 ] && echo "  FAIL something else"
keep_notice
suite_summary "\$pass" "\$fail"
HARNESS
  chmod +x "$1"
}

echo "1. the SUCCESS path"
make_harness "$WORK/pass.sh" 0
out_pass="$("$WORK/pass.sh" 2>&1)"; rc_pass=$?
last_pass="$(printf '%s\n' "$out_pass" | tail -1)"
check "success: exits 0" "0" "$rc_pass"
check "success: the last line is the contract line" "yes" \
      "$(is_contract_line "$last_pass")"
check "success: the contract line appears exactly once" "1" \
      "$(printf '%s\n' "$out_pass" | grep -c '^ASSERTIONS=')"
check "success: no state-kept notice at all" "0" \
      "$(printf '%s\n' "$out_pass" | grep -c 'state kept for inspection')"

echo
echo "2. the FAILURE path — the one that used to break"
make_harness "$WORK/fail.sh" 1
out_fail="$("$WORK/fail.sh" 2>&1)"; rc_fail=$?
last_fail="$(printf '%s\n' "$out_fail" | tail -1)"
check "failure: exits non-zero" "yes" "$([ "$rc_fail" -ne 0 ] && echo yes || echo no)"
check "failure: the last line is STILL the contract line" "yes" \
      "$(is_contract_line "$last_fail")"
check "failure: the contract line appears exactly once" "1" \
      "$(printf '%s\n' "$out_fail" | grep -c '^ASSERTIONS=')"
check "failure: the state-kept notice IS emitted" "1" \
      "$(printf '%s\n' "$out_fail" | grep -c 'state kept for inspection')"

n_notice="$(printf '%s\n' "$out_fail" | grep -n 'state kept for inspection' | cut -d: -f1 | head -1)"
n_summary="$(printf '%s\n' "$out_fail" | grep -n '^ASSERTIONS=' | cut -d: -f1 | head -1)"
check "failure: the notice comes BEFORE the summary" "yes" \
      "$([ -n "$n_notice" ] && [ -n "$n_summary" ] && [ "$n_notice" -lt "$n_summary" ] \
         && echo yes || echo no)"
check "failure: the failing assertion is still visible" "yes" \
      "$(contains_text "$out_fail" "FAIL something else")"

echo
echo "3. the defect this replaces is CAUGHT, not merely absent"
# A harness that announces from the trap only — the original shape. If this were still
# accepted, the assertions above would be proving nothing.
cat > "$WORK/broken.sh" <<BROKEN
#!/usr/bin/env bash
set -uo pipefail
. "$HERE/scripts/lib_suite_summary.sh"
pass=3; fail=1
cleanup_work() { echo "state kept for inspection: /tmp/x"; }
trap cleanup_work EXIT
echo "  FAIL something else"
suite_summary "\$pass" "\$fail"
BROKEN
chmod +x "$WORK/broken.sh"
out_broken="$("$WORK/broken.sh" 2>&1)"
last_broken="$(printf '%s\n' "$out_broken" | tail -1)"
check "the original trap-only shape DOES break the contract" "no" \
      "$(is_contract_line "$last_broken")"
check "and this suite's checker detects that" "yes" \
      "$(contains_text "$last_broken" "state kept for inspection")"

echo
suite_summary "$pass" "$fail"
