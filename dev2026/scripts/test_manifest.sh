#!/usr/bin/env bash
#
# deploy/record_manifest.py — the complete-manifest recorder, checked offline.
#
# Two properties matter and both are checked behaviourally rather than by reading the
# source:
#
#   1. it records EVERY distribution, not a chosen subset. B7 asked about eight packages
#      and that was not enough to describe a runtime — S1 was caught by the same gap, with
#      12 pinned packages matching while 23 transitive ones did not;
#   2. it IMPORTS NOTHING. On VM24 an import of polars emits the AVX2 warning of spec 012
#      and initialises the library, so an inventory tool that imported what it inventories
#      would trigger the very risk B6 accepted. Proved here with `-X importtime`, which
#      logs every module actually imported — a claim about behaviour, not about the source.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$HERE/deploy/record_manifest.py"
PY="$HERE/.venv/bin/python"

PASS=0; FAIL=0
check() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}

WORK="$(mktemp -d)"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

echo "the recorder exists and runs"
check "record_manifest.py exists" "yes" "$([ -f "$SCRIPT" ] && echo yes || echo no)"
check "the project interpreter exists" "yes" "$([ -x "$PY" ] && echo yes || echo no)"
[ -x "$PY" ] || { echo "cannot continue without $PY"; exit 2; }
check "it compiles" "yes" \
      "$("$PY" -m py_compile "$SCRIPT" 2>/dev/null && echo yes || echo no)"

"$PY" "$SCRIPT" > "$WORK/full.txt" 2> "$WORK/full.err"; rc=$?
check "it exits 0" "0" "$rc"
check "it writes nothing to stderr" "0" "$(wc -c < "$WORK/full.err" | tr -d ' ')"

echo
echo "it reports the interpreter it actually ran on"
check "interpreter line present" "yes" \
      "$(grep -q '^interpreter    : ' "$WORK/full.txt" && echo yes || echo no)"
check "and it is this venv's python" "yes" \
      "$(grep -q "^interpreter    : $PY\$" "$WORK/full.txt" && echo yes || echo no)"
check "python_version is reported" "yes" \
      "$(grep -qE '^python_version : [0-9]+\.[0-9]+\.[0-9]+$' "$WORK/full.txt" && echo yes || echo no)"
check "platform is reported" "yes" \
      "$(grep -q '^platform       : ' "$WORK/full.txt" && echo yes || echo no)"

echo
echo "it records EVERY distribution, not a subset"
N_REPORTED="$(grep -E '^distributions  : ' "$WORK/full.txt" | awk '{print $3}')"
N_LISTED="$(sed -n '/^== COMPLETE MANIFEST ==/,$p' "$WORK/full.txt" | grep -cE '^  [^ ]+==')"
check "the count and the listing agree" "$N_REPORTED" "$N_LISTED"
# Independent ground truth: ask pip, which enumerates the same environment a different way.
N_PIP="$("$PY" -m pip list --format=freeze 2>/dev/null | grep -c '==' || echo 0)"
if [ "${N_PIP:-0}" -gt 0 ]; then
  # pip omits itself in some layouts, so allow a difference of at most one and say so.
  DIFF=$(( N_REPORTED > N_PIP ? N_REPORTED - N_PIP : N_PIP - N_REPORTED ))
  check "the count agrees with pip to within 1 (pip=$N_PIP, ours=$N_REPORTED)" "yes" \
        "$([ "$DIFF" -le 1 ] && echo yes || echo no)"
else
  PASS=$((PASS+1)); echo "  ok   (skipped: pip unavailable in this venv)"
fi
check "more than the eight B7 asked about" "yes" \
      "$([ "${N_REPORTED:-0}" -gt 8 ] && echo yes || echo no)"
check "the manifest digest is present" "yes" \
      "$(grep -qE '^manifest_sha256: [0-9a-f]{64}$' "$WORK/full.txt" && echo yes || echo no)"

echo
echo "CORE is reported separately, and matches uv.lock"
check "a CORE section exists" "yes" \
      "$(grep -q '^== CORE' "$WORK/full.txt" && echo yes || echo no)"
check "with its own digest" "yes" \
      "$(grep -qE '^  core_sha256  [0-9a-f]{64}$' "$WORK/full.txt" && echo yes || echo no)"
# Every CORE package's reported version must equal uv.lock's. The lockfile is the portable
# source of truth: the local venv runs 3.11.14 and a VM24 venv runs 3.11.4, so a
# venv-derived expectation would not be comparable between them.
for pkg in polars orjson fastapi uvicorn gunicorn starlette pydantic zarr xarray numpy; do
  LOCKV="$("$PY" - "$pkg" <<'PYEOF'
import re, sys, pathlib
name = sys.argv[1]
text = pathlib.Path("uv.lock").read_text()
m = re.search(r'\[\[package\]\]\nname = "%s"\nversion = "([^"]+)"' % re.escape(name), text)
print(m.group(1) if m else "NOT-IN-LOCK")
PYEOF
)"
  GOT="$(grep -E "^  $pkg " "$WORK/full.txt" | head -1 | awk '{print $2}')"
  check "CORE $pkg matches uv.lock ($LOCKV)" "$LOCKV" "$GOT"
done
# B6 decided this one explicitly, so it gets its own assertion rather than resting on the
# loop above: a silent polars change is the thing that would invalidate the decision.
check "polars is mainline 1.27.1, per the B6 decision" "1.27.1" \
      "$(grep -E '^  polars ' "$WORK/full.txt" | head -1 | awk '{print $2}')"
check "polars-lts-cpu is NOT installed" "no" \
      "$(grep -qE '^  polars-lts-cpu==' "$WORK/full.txt" && echo yes || echo no)"

echo
echo "--core-only omits the full listing but keeps the core"
"$PY" "$SCRIPT" --core-only > "$WORK/core.txt" 2>/dev/null
check "the CORE section is still there" "yes" \
      "$(grep -q '^== CORE' "$WORK/core.txt" && echo yes || echo no)"
check "the complete listing is omitted" "no" \
      "$(grep -q '^== COMPLETE MANIFEST' "$WORK/core.txt" && echo yes || echo no)"
check "and the core digest is unchanged between the two runs" \
      "$(grep 'core_sha256' "$WORK/full.txt" | awk '{print $2}')" \
      "$(grep 'core_sha256' "$WORK/core.txt" | awk '{print $2}')"

echo
echo "IT IMPORTS NOTHING — proved by -X importtime, not by reading the source"
# importtime logs every module actually imported. If the recorder imported what it
# inventories, these would appear. On VM24 that would also emit the AVX2 warning.
"$PY" -X importtime "$SCRIPT" --core-only > /dev/null 2> "$WORK/imports.txt"
for mod in polars orjson fastapi uvicorn gunicorn starlette pydantic zarr xarray numpy; do
  check "$mod was never imported" "0" \
        "$(awk -F'|' '{print $3}' "$WORK/imports.txt" | tr -d ' ' | grep -cE "^${mod}(\.|\$)")"
done
check "the importtime log is non-empty, so the check really ran" "yes" \
      "$([ -s "$WORK/imports.txt" ] && echo yes || echo no)"

echo
suite_summary "$PASS" "$FAIL"
