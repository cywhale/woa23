#!/usr/bin/env bash
#
# Every source file the harness needs must be tracked by git, and none may be
# ignored.
#
# The third authorised C1 run died on `ModuleNotFoundError: bench.dist_digests`.
# The module existed, imported cleanly, and had passing tests — in the working tree.
# `.gitignore` line 38 is `**/dist_*`, a pattern meant for build artefacts, and it
# matched `dist_digests.py`. `git add -A` skips ignored files **silently**, and
# `git status --porcelain` does not list them, so nothing about the commit looked
# wrong. The remote then verified 72 of 72 files against the commit — correctly, and
# the commit was the thing that was incomplete.
#
# A test suite run from the working tree cannot see this: it imports the file that
# is there. What catches it is asking git, which is what this does.
#
#     ./scripts/test_tracked.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}

echo "every harness source file is tracked, and none is ignored"

# Source, not output. results/ and run/ hold artefacts; .venv and __pycache__ are
# build products. Everything else under these roots is something a run needs.
# `mapfile` is bash 4; the machine these tests are written on ships bash 3.2, and a
# test that cannot run where it is written is not much of a test.
sources=()
while IFS= read -r line; do sources+=("$line"); done < <(
  find bench scripts api specs -type f \
       \( -name '*.py' -o -name '*.sh' -o -name '*.md' -o -name '*.json' \) \
       -not -path '*/__pycache__/*' -not -path '*/.venv/*' | sort)
check "there is something to check" "yes" \
      "$([ "${#sources[@]}" -gt 20 ] && echo yes || echo no)"

untracked=0
ignored=0
for f in "${sources[@]}"; do
  if ! git ls-files --error-unmatch "$f" >/dev/null 2>&1; then
    untracked=$((untracked + 1))
    echo "       UNTRACKED: $f"
  fi
  if git check-ignore -q "$f" 2>/dev/null; then
    ignored=$((ignored + 1))
    echo "       IGNORED:   $f  ($(git check-ignore -v "$f" | cut -f1))"
  fi
done
check "no harness source file is untracked" "0" "$untracked"
check "no harness source file is git-ignored" "0" "$ignored"

# Every module the harness imports from bench must resolve inside a tracked file.
# Import errors are the shape this failure takes at run time, so the import graph is
# the right thing to check rather than the file list alone.
echo
echo "every bench module imported by the harness is tracked"
missing=0
imported=()
while IFS= read -r line; do imported+=("$line"); done < <(
  { grep -rhoE 'from bench\.[a-z_]+ import|import bench\.[a-z_]+' bench scripts \
      --include='*.py' --include='*.sh' || true; } \
    | grep -oE 'bench\.[a-z_]+' | sort -u)
check "some bench imports were found" "yes" \
      "$([ "${#imported[@]}" -gt 3 ] && echo yes || echo no)"
for mod in "${imported[@]}"; do
  path="${mod//.//}.py"
  if [ ! -f "$path" ]; then
    echo "       MISSING FILE: $mod -> $path"; missing=$((missing + 1)); continue
  fi
  if ! git ls-files --error-unmatch "$path" >/dev/null 2>&1; then
    echo "       IMPORTED BUT UNTRACKED: $mod -> $path"; missing=$((missing + 1))
  fi
done
check "every imported bench module exists and is tracked" "0" "$missing"

# The specific pattern that caused it, so a future module named dist_* is refused
# at the point it is added rather than at the point a run consumes it.
echo
echo "the pattern that swallowed it is still there, and still would"
check ".gitignore still ignores dist_*" "yes" \
      "$(git check-ignore -q -- 'bench/dist_example.py' && echo yes || echo no)"
check "so no harness module may be named dist_*" "0" \
      "$(find bench scripts -name 'dist_*' -not -path '*/__pycache__/*' | wc -l | tr -d ' ')"

# The committed tree must be able to run, not just the working tree. This is the
# check that would have failed before the third C1 run rather than during it.
echo
echo "the committed tree imports cleanly, from a clean export"
# From the repository root: `git archive`'s pathspec is relative to the current
# directory, so `HEAD dev2026` run from inside dev2026 matches nothing — and with
# the output piped into tar the failure shows up as an empty export rather than an
# error. The first version of this check "skipped" for exactly that reason, which is
# the same shape of silence the whole test exists to end.
tmp="$(mktemp -d)"
root="$(git rev-parse --show-toplevel)"
git -C "$root" archive --format=tar HEAD dev2026 | tar -x -C "$tmp"
check "the commit exports a dev2026 tree at all" "yes" \
      "$([ -d "$tmp/dev2026/bench" ] && echo yes || echo no)"
if [ -d "$tmp/dev2026/bench" ]; then
  n_export="$(find "$tmp/dev2026/bench" -name '*.py' | wc -l | tr -d ' ')"
  n_tree="$(find bench -name '*.py' -not -path '*/__pycache__/*' | wc -l | tr -d ' ')"
  check "the export has as many bench modules as the working tree" "$n_tree" "$n_export"
  for mod in "${imported[@]}"; do
    path="$tmp/dev2026/${mod//.//}.py"
    [ -f "$path" ] || { echo "       MISSING FROM EXPORT: $mod"; missing=$((missing + 1)); }
  done
  check "every imported module is present in the export" "0" "$missing"
fi
rm -r "$tmp"

echo
if [ "$fail" -gt 0 ]; then
  echo "FAILED $fail/$((pass + fail))"
  exit 1
fi
echo "all passed ($pass assertions)"
