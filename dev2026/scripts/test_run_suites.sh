#!/usr/bin/env bash
#
# The batch runner's own properties. Offline; it runs the runner over a single fast
# suite and inspects what it kept.
#
# A batch runner is a piece of evidence-handling machinery, and the failure it can have
# is silent: it reports a red suite without keeping what would explain it. That already
# happened twice — `test_procs.sh` failed 3 of 167 assertions in a batch and neither
# occurrence can be diagnosed, because the loop printed only each suite's last line.
# So the runner is checked for the properties that make its output usable.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$HERE/scripts/run_suites.sh"
REPO="$HERE"

PASS=0; FAIL=0
check() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}

echo "the runner is serial BY CONSTRUCTION, not by description"
# Comments are stripped: this file's own prose names `&` and `wait` while explaining
# that the runner uses neither.
code="$(sed -e 's/^[[:space:]]*#.*$//' "$RUNNER")"
check "no background operator on any command" "no" \
      "$(printf '%s' "$code" | grep -qE '[^&|]& *$' && echo yes || echo no)"
check "no shell 'wait'" "no" \
      "$(printf '%s' "$code" | grep -qE '(^|;| )wait( |$|;)' && echo yes || echo no)"
check "no xargs -P" "no" \
      "$(printf '%s' "$code" | grep -q 'xargs.*-P' && echo yes || echo no)"
check "no GNU parallel" "no" \
      "$(printf '%s' "$code" | grep -qE '(^| )parallel ' && echo yes || echo no)"
check "and it declares itself serial" "yes" \
      "$(printf '%s' "$code" | grep -q 'CONCURRENCY="serial"' && echo yes || echo no)"

echo
echo "it runs a suite and keeps what a failure would need"
# WOA23_SUITE_REPEAT is PINNED for every invocation below. It is not paranoia: this
# suite failed 2 of 45 inside a batch launched as `WOA23_SUITE_REPEAT=3
# run_suites.sh`, because that variable was in the environment and the runner spawned
# here INHERITED it — the inner runner ran three iterations, so the manifest carried
# four lines instead of two and the summary counted three failures instead of one.
# The suite was asserting against a number the caller's environment could change.
OUT="$(WOA23_SUITE_REPEAT=1 bash "$RUNNER" test_labels 2>&1)"
ROOT="$(printf '%s' "$OUT" | sed -n 's/^batch root : \(.*\)   (nothing deleted)$/\1/p' \
        | head -1)"
check "the batch root is reported" "yes" "$([ -n "$ROOT" ] && echo yes || echo no)"
SLOT="$ROOT/iter1/test_labels.sh"
for f in stdout.txt stderr.txt env.txt procs.before procs.after failures.txt; do
  check "it kept $f" "yes" "$([ -f "$SLOT/$f" ] && echo yes || echo no)"
done
check "stdout is NOT empty" "yes" \
      "$([ -s "$SLOT/stdout.txt" ] && echo yes || echo no)"
check "stdout and stderr are separate files" "yes" \
      "$([ "$SLOT/stdout.txt" != "$SLOT/stderr.txt" ] && echo yes || echo no)"
check "the suite's own result is in the kept stdout" "yes" \
      "$(grep -q 'all passed' "$SLOT/stdout.txt" && echo yes || echo no)"

echo
echo "the batch records its OWN subject, rather than being described from outside"
# This is here because results were once reported as attesting a particular HEAD with a
# clean tree while nothing in the runner ever asked git anything. A provenance claim the
# batch cannot make for itself is not evidence.
check "the batch reports git head" "yes" \
      "$(printf '%s' "$OUT" | grep -q '^git head   : [0-9a-f]\{40\}$' && echo yes || echo no)"
check "  and it is THIS repository's HEAD" "yes" \
      "$(printf '%s' "$OUT" | grep -q "^git head   : $(git -C "$REPO" rev-parse HEAD)$" \
         && echo yes || echo no)"
check "the batch reports the commit subject" "yes" \
      "$(printf '%s' "$OUT" | grep -q '^git subject: ' && echo yes || echo no)"
check "tracked and untracked are counted SEPARATELY" "yes" \
      "$(printf '%s' "$OUT" | grep -q '^tracked dirty : ' \
         && printf '%s' "$OUT" | grep -q '^untracked     : ' && echo yes || echo no)"
# NOT "the count is 0" — that would assert the developer's working tree is clean, which
# is a fact about whoever is running the suite, not about the runner. The property is
# that the reported counts AGREE WITH GIT, whatever the tree happens to look like.
TRACKED_SEEN="$(printf '%s' "$OUT" | sed -n 's/^tracked dirty : \([0-9]*\).*/\1/p' | head -1)"
check "  the tracked count agrees with git" \
      "$(git -C "$REPO" status --porcelain --untracked-files=no | wc -l | tr -d ' ')" \
      "$TRACKED_SEEN"
check "git.txt is kept in the batch root" "yes" \
      "$([ -f "$ROOT/git.txt" ] && echo yes || echo no)"
check "  and it carries the head" "yes" \
      "$(grep -q "^head=$(git -C "$REPO" rev-parse HEAD)$" "$ROOT/git.txt" && echo yes || echo no)"
check "  and the separated counts" "2" \
      "$(grep -cE '^(tracked_dirty|untracked)=' "$ROOT/git.txt" || true)"
check "a mismatched tree would be WARNED about, not passed over" 1 \
      "$(grep -c 'does NOT match' "$RUNNER" || true)"

echo
echo "the manifest records one row per suite run, with timings and exit code"
MAN="$ROOT/manifest.tsv"
check "the manifest exists" "yes" "$([ -f "$MAN" ] && echo yes || echo no)"
check "it has a header and one data row" "2" "$(wc -l < "$MAN" | tr -d ' ')"
hdr="$(head -1 "$MAN")"
for col in iteration suite exit start end seconds stdout_bytes stderr_bytes fail_lines; do
  check "the manifest records '$col'" "yes" \
        "$(printf '%s' "$hdr" | grep -q "$col" && echo yes || echo no)"
done
check "the data row carries exit=0 for a passing suite" "0" \
      "$(awk -F'\t' 'NR==2{print $3}' "$MAN")"
check "and a start timestamp" "yes" \
      "$(awk -F'\t' 'NR==2{print $4}' "$MAN" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' \
         && echo yes || echo no)"

echo
echo "each suite gets its OWN tmpdir, and the fingerprint records the environment"
check "the suite's TMPDIR is under its own slot" "yes" \
      "$(grep -q "suite_tmpdir  = $SLOT/tmp" "$SLOT/env.txt" && echo yes || echo no)"
check "the tmpdir was created" "yes" "$([ -d "$SLOT/tmp" ] && echo yes || echo no)"
for key in TMPDIR PROC_ROOT STOP_WAIT_SECS PYTHONHASHSEED PYTHONPATH PATH uname load; do
  check "the fingerprint records $key" "yes" \
        "$(grep -q "^$key" "$SLOT/env.txt" && echo yes || echo no)"
done

echo
# The fixture obeys the summary contract, because this block is about what the runner keeps
# for an HONESTLY failing suite. A suite that fails AND breaks the contract is a different
# case with a different report, and it is covered in test_summary_contract.sh section 4.
echo "a FAILING suite has its assertions kept in full — the property that was missing"
FAKE="$(mktemp -d)"
cat > "$FAKE/test_zzfake.sh" <<'FIXTURE'
echo "  ok   something fine"
echo "  FAIL a deliberately failing assertion — expected [yes], got [no]"
echo "       with a second line of detail"
echo "and a line on stderr" >&2
echo "1 FAILED, 1 passed"
echo "ASSERTIONS=2 FAILED=1"
exit 1
FIXTURE
cp "$FAKE/test_zzfake.sh" "$HERE/scripts/test_zzfake.sh"
OUT2="$(WOA23_SUITE_REPEAT=1 bash "$RUNNER" test_zzfake 2>&1)"; rc2=$?
unlink "$HERE/scripts/test_zzfake.sh"
check "the runner exits non-zero when a suite fails" "yes" \
      "$([ "$rc2" -ne 0 ] && echo yes || echo no)"
check "it prints the failing assertion text, not just the last line" "yes" \
      "$(printf '%s' "$OUT2" | grep -q 'a deliberately failing assertion' \
         && echo yes || echo no)"
check "including the following detail line" "yes" \
      "$(printf '%s' "$OUT2" | grep -q 'with a second line of detail' \
         && echo yes || echo no)"
check "it shows stderr separately" "yes" \
      "$(printf '%s' "$OUT2" | grep -q 'and a line on stderr' && echo yes || echo no)"
check "it names the suite" "yes" \
      "$(printf '%s' "$OUT2" | grep -q 'test_zzfake.sh' && echo yes || echo no)"
check "it points at the retained evidence" "yes" \
      "$(printf '%s' "$OUT2" | grep -q 'evidence kept at' && echo yes || echo no)"
check "and the summary counts the failure" "yes" \
      "$(printf '%s' "$OUT2" | grep -q 'NON-ZERO: 1' && echo yes || echo no)"
# That run had an EXTRA UNTRACKED FILE in scripts/ (the fixture above), which is exactly
# the case the separated counts exist for: it must move `untracked` and leave `tracked
# dirty` alone. A single collapsed "dirty" number could not tell these apart.
check "an untracked fixture did NOT change the tracked count" "$TRACKED_SEEN" \
      "$(printf '%s' "$OUT2" | sed -n 's/^tracked dirty : \([0-9]*\).*/\1/p' | head -1)"
check "  but it DID raise the untracked count" "yes" \
      "$([ "$(printf '%s' "$OUT2" | sed -n 's/^untracked     : \([0-9]*\).*/\1/p' | head -1)" \
           -gt "$(printf '%s' "$OUT" | sed -n 's/^untracked     : \([0-9]*\).*/\1/p' | head -1)" ] \
         && echo yes || echo no)"

echo
echo "the repeat count is honoured — so pinning it above is meaningful"
OUT3="$(WOA23_SUITE_REPEAT=2 bash "$RUNNER" test_labels 2>&1)"
ROOT3="$(printf '%s' "$OUT3" | sed -n 's/^batch root : \(.*\)   (nothing deleted)$/\1/p' \
         | head -1)"
check "two iterations produce two data rows" "3" \
      "$(wc -l < "$ROOT3/manifest.tsv" | tr -d ' ')"
check "and the rows are numbered 1 and 2" "1 2" \
      "$(awk -F'\t' 'NR>1{printf "%s ", $1}' "$ROOT3/manifest.tsv" | sed 's/ $//')"
# The inherited-environment failure, reproduced deliberately: an unpinned invocation
# under WOA23_SUITE_REPEAT=3 really does run three times.
OUT4="$(WOA23_SUITE_REPEAT=3 bash -c 'bash "$1" test_labels' _ "$RUNNER" 2>&1)"
ROOT4="$(printf '%s' "$OUT4" | sed -n 's/^batch root : \(.*\)   (nothing deleted)$/\1/p' \
         | head -1)"
check "an UNPINNED invocation inherits the caller's repeat count" "4" \
      "$(wc -l < "$ROOT4/manifest.tsv" | tr -d ' ')"

echo
suite_summary "$PASS" "$FAIL"
