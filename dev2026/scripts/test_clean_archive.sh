#!/usr/bin/env bash
#
# The clean-archive verifier verifies the tree it exported — not the tree it made.
#
# Two defects, both found by a reviewer running it on a different machine:
#
#   1. it invoked the AMBIENT `python3` for `compileall`. On a host whose `python3`
#      is 3.9 that step failed and the verifier reported 10/13 for a commit that
#      was perfectly fine. The result depended on the reader's PATH.
#   2. it computed the file list LAST. `compileall` writes `__pycache__` into the
#      tree it compiles and `test_tracked.sh` imports from it, so the export held
#      166 files instead of 115 by the time it was counted, with a different
#      file-list digest. The archive tar was always right; the identity reported
#      beside it was measuring the verifier's own side effects.
#
# So this asserts the two properties that make the identity mean anything: it is
# taken BEFORE the tree is touched, and it is the same afterwards.
#
#     bash scripts/test_clean_archive.sh
set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
pass=0; fail=0

check() {   # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}
has_text() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

WORK="$(mktemp -d)"
PY="$ROOT/dev2026/.venv/bin/python"
trap '"$PY" -c "import shutil,sys; shutil.rmtree(sys.argv[1], ignore_errors=True)" "$WORK"' EXIT

SRC="$(cat "$HERE/verify_clean_archive.sh")"

echo "the identity is taken before anything can change the tree"
# Read from the source, because the ORDER is the property: a later file list is a
# measurement of the verifier's own side effects.
check "the file list is computed before compileall" "yes" \
      "$("$PY" - "$HERE/verify_clean_archive.sh" <<'PYEOF'
import sys
s = open(sys.argv[1]).read()
print("yes" if s.index('file_list_of "$TREE" "$LIST"')
      < s.index('-m compileall') else "no")
PYEOF
)"
check "and before the export is imported from" "yes" \
      "$("$PY" - "$HERE/verify_clean_archive.sh" <<'PYEOF'
import sys
s = open(sys.argv[1]).read()
print("yes" if s.index('file_list_of "$TREE" "$LIST"')
      < s.index("importlib.import_module") else "no")
PYEOF
)"
check "and before test_tracked runs inside it" "yes" \
      "$("$PY" - "$HERE/verify_clean_archive.sh" <<'PYEOF'
import sys
s = open(sys.argv[1]).read()
print("yes" if s.index('file_list_of "$TREE" "$LIST"')
      < s.index("./scripts/test_tracked.sh") else "no")
PYEOF
)"
check "bytecode is redirected out of the export" "yes" \
      "$(has_text "$SRC" "PYTHONPYCACHEPREFIX")"
check "and the tree is re-checked against the identity afterwards" "yes" \
      "$(has_text "$SRC" "the export is still the tree whose identity was taken")"

echo
echo "the interpreter is named, not inherited"
check "it uses the project venv" "yes" \
      "$(has_text "$SRC" 'PY_BIN="$ROOT/dev2026/.venv/bin/python"')"
check "it does NOT fall back to ambient python3" "no" \
      "$(has_text "$SRC" '[ -x "$PY_BIN" ] || PY_BIN="python3"')"
check "compileall runs on that interpreter" "yes" \
      "$(has_text "$SRC" '"$PY_BIN" -m compileall')"
check "and a missing project interpreter stops the run" "yes" \
      "$(has_text "$SRC" "will not")"

echo
echo "an older python3 first on PATH changes nothing"
# The exact trap the reviewer hit: a 3.9 ambient interpreter. A shim that IS 3.9 if
# one exists, and otherwise a stub that fails the way 3.9 did — either way, `python3`
# on PATH is not something this verifier may depend on.
mkdir -p "$WORK/bin"
OLDPY="$(command -v python3.9 || true)"
if [ -n "$OLDPY" ]; then
  ln -s "$OLDPY" "$WORK/bin/python3"
  shim_kind="real python3.9"
else
  printf '#!/bin/sh\necho "ambient python3 must not be used" >&2\nexit 1\n' \
    > "$WORK/bin/python3"
  chmod +x "$WORK/bin/python3"
  shim_kind="a python3 that always fails"
fi
echo "       PATH shim: $shim_kind"

base="$(bash "$HERE/verify_clean_archive.sh" HEAD 2>&1)"
base_rc=$?
shimmed="$(PATH="$WORK/bin:$PATH" bash "$HERE/verify_clean_archive.sh" HEAD 2>&1)"
shim_rc=$?
check "the verifier passes with the shim first on PATH" "$base_rc" "$shim_rc"
check "and it exits 0" "0" "$shim_rc"

ident() {   # ident <output> <field>
  printf '%s\n' "$1" | grep "^   $2 " | tr -s ' ' | cut -d' ' -f3
}
for field in archive_sha256 file_count file_list_sha256; do
  check "$field is identical either way" "$(ident "$base" "$field")" \
        "$(ident "$shimmed" "$field")"
done
check "the shimmed run names the project interpreter, not the shim" "yes" \
      "$(has_text "$shimmed" "/.venv/bin/python")"
check "and reports the tree unchanged by its own checks" "yes" \
      "$(has_text "$shimmed" "the file count is unchanged")"
check "with no bytecode left in the export" "yes" \
      "$(has_text "$shimmed" "no bytecode was written into the export")"

echo
echo "the identity is reproducible and derived from the commit, not from the run"
# Deliberately NOT a hardcoded digest: that would pin one commit and have to be
# edited by every commit after it, which is a test that tracks the code rather than
# checking it. What must hold is that the identity is REPRODUCIBLE and comes from
# what git has, so it is derived here independently and compared.
git_files="$(git -C "$ROOT" archive HEAD dev2026 | tar -t | grep -vc '/$')"
check "the file count matches what git archive contains" "$git_files" \
      "$(ident "$base" file_count)"
again="$(bash "$HERE/verify_clean_archive.sh" HEAD 2>&1)"
check "a second run reports the same archive digest" \
      "$(ident "$base" archive_sha256)" "$(ident "$again" archive_sha256)"
check "the same file count" "$(ident "$base" file_count)" \
      "$(ident "$again" file_count)"
check "and the same file-list digest" "$(ident "$base" file_list_sha256)" \
      "$(ident "$again" file_list_sha256)"
echo "       identity: $(ident "$base" file_count) files, \
file-list $(ident "$base" file_list_sha256)"

echo
suite_summary "$pass" "$fail"
