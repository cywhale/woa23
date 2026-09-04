#!/usr/bin/env bash
#
# Verify a commit by exporting it and testing the export — never the working tree.
#
# The third authorised C1 run died on a module that existed, imported and passed its
# tests here, and was not in the commit: `.gitignore`'s `**/dist_*` matched it and
# `git add -A` skipped it without a word. Every check that consulted the working tree
# was green, and every one of them was answering about the wrong tree.
#
# So this exports the commit with `git archive` into a fresh directory that has no
# `.git`, and runs the checks there. A file that is missing from the commit is
# missing here, and nothing in the working tree can stand in for it.
#
#     ./scripts/verify_clean_archive.sh <commit>
#
# Prints the commit SHA, the archive digest, the file count, a per-file SHA-256
# listing, and the result of each check. Exits non-zero if any check fails.
set -euo pipefail

COMMIT="${1:?usage: verify_clean_archive.sh <commit>}"
ROOT="$(git rev-parse --show-toplevel)"
OUTDIR="${2:-}"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); printf '  ok   %s\n' "$1"
  else
    fail=$((fail + 1)); printf '  FAIL %s — expected [%s], got [%s]\n' "$1" "$2" "$3"
  fi
}

SHA="$(git -C "$ROOT" rev-parse --verify "${COMMIT}^{commit}")"
echo "== clean-archive verification =="
echo "   commit  : $SHA"
echo "   subject : $(git -C "$ROOT" log -1 --format=%s "$SHA")"

TMP="$(mktemp -d)"
TAR="$TMP/archive.tar"

# The archive, and its digest. `git archive` is deterministic for a given commit —
# content and recorded mtimes both come from the commit — so this digest identifies
# the exact bytes that will be shipped.
git -C "$ROOT" archive --format=tar "$SHA" dev2026 > "$TAR"
ARCHIVE_SHA="$(sha256sum "$TAR" 2>/dev/null | cut -d' ' -f1 \
               || shasum -a 256 "$TAR" | cut -d' ' -f1)"
echo "   archive : sha256 $ARCHIVE_SHA  ($(wc -c < "$TAR" | tr -d ' ') bytes)"

mkdir -p "$TMP/tree"
tar -x -f "$TAR" -C "$TMP/tree"
TREE="$TMP/tree/dev2026"
echo "   export  : $TREE"

# The interpreter, chosen and NAMED, never inherited from PATH. An ambient python3
# — 3.9 on the reviewer's machine — fails `compileall` on syntax this repository
# uses, and the verifier then reported 10/13 for a tree that was perfectly fine.
# Falling back to `python3` made the result depend on whoever ran it.
PY_BIN="$ROOT/dev2026/.venv/bin/python"
if [ ! -x "$PY_BIN" ]; then
  echo "the project interpreter $PY_BIN is missing." >&2
  echo "  This verification runs on the repository's own interpreter and will not" >&2
  echo "  fall back to whatever \`python3\` resolves to: the result would then" >&2
  echo "  depend on the reader's PATH rather than on the commit." >&2
  echo "  Run \`uv sync\` in dev2026 first." >&2
  exit 2
fi
PY_VER="$("$PY_BIN" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])')"
echo "   python  : $PY_BIN ($PY_VER)"

# ---------------------------------------------------------- pristine identity ---
# COMPUTED FIRST, before anything compiles, imports or runs inside the tree.
#
# It used to be computed last. `compileall` writes __pycache__ into the tree it
# compiles and `test_tracked.sh` imports from it, so by the time the file list was
# taken the export held 166 files instead of 115 and a different digest — the
# verifier was measuring the tree it had itself modified. The archive tar was
# always right; the identity reported beside it was not.
#
# LC_ALL=C on the sort and on the find that feeds it. Collation is locale
# dependent: this digest was computed on macOS, and VM24's default locale ordered
# `.gitignore` and `CODEX_REVIEWER.md` differently — same files, same digests, a
# different file-list digest. A digest that changes with the reader's locale cannot
# be checked by the reader, which is the only thing it is for.
file_list_of() {            # file_list_of <tree> <out>
  (cd "$1" && LC_ALL=C find . -type f | sed 's|^\./||' | LC_ALL=C sort \
     | while IFS= read -r f; do
     printf '%s  %s\n' "$( (sha256sum "$f" 2>/dev/null || shasum -a 256 "$f") \
       | cut -d' ' -f1)" "$f"
   done) > "$2"
}
digest_of() {               # digest_of <file>
  (sha256sum "$1" 2>/dev/null || shasum -a 256 "$1") | cut -d' ' -f1
}

LIST="$TMP/files.sha256"
file_list_of "$TREE" "$LIST"
N="$(wc -l < "$LIST" | tr -d ' ')"
LIST_SHA="$(digest_of "$LIST")"
echo "   files   : $N"
echo "   file-list digest : $LIST_SHA"
echo

# Nothing below may write into the export. Bytecode goes to a cache directory
# outside it, so `compileall` can do its job without changing what is being
# identified.
export PYTHONPYCACHEPREFIX="$TMP/pycache"
mkdir -p "$PYTHONPYCACHEPREFIX"

echo "the export has no repository of its own"
check "no .git in the exported tree" "yes" \
      "$([ -e "$TMP/tree/.git" ] && echo no || echo yes)"
check "git commands cannot reach the source repo from here" "yes" \
      "$(cd "$TMP" && git rev-parse --show-toplevel >/dev/null 2>&1 && echo no || echo yes)"

echo
echo "the module that was missing, and the one that replaced it"
check "bench/package_digests.py IS in the archive" "yes" \
      "$([ -f "$TREE/bench/package_digests.py" ] && echo yes || echo no)"
check "bench/dist_digests.py is NOT in the archive" "yes" \
      "$([ -f "$TREE/bench/dist_digests.py" ] && echo no || echo yes)"
check "no file in the archive is named dist_*" "0" \
      "$(find "$TREE" -name 'dist_*' | wc -l | tr -d ' ')"
check "nothing in the archive imports dist_digests" "0" \
      "$(grep -rlE '^[[:space:]]*(from|import)[[:space:]]+[^#]*dist_digests|-m[[:space:]]+bench\.dist_digests' \
         "$TREE" 2>/dev/null --include='*.py' --include='*.sh' | wc -l | tr -d ' ')"

echo
echo "every bench module imported anywhere in the archive exists in the archive"
missing=0
mods=()
while IFS= read -r m; do mods+=("$m"); done < <(
  { grep -rhoE 'from bench\.[a-zA-Z_][a-zA-Z0-9_]* import|import bench\.[a-zA-Z_][a-zA-Z0-9_]*' \
      "$TREE/bench" "$TREE/scripts" --include='*.py' --include='*.sh' || true; } \
    | grep -oE 'bench\.[a-zA-Z_][a-zA-Z0-9_]*' | LC_ALL=C sort -u)
check "imports were found" "yes" "$([ "${#mods[@]}" -gt 3 ] && echo yes || echo no)"
for m in "${mods[@]}"; do
  if [ ! -f "$TREE/${m//.//}.py" ]; then
    echo "       MISSING FROM ARCHIVE: $m"; missing=$((missing + 1))
  fi
done
check "every imported bench module is in the archive" "0" "$missing"
echo "       (${#mods[@]} distinct bench modules imported)"

echo
echo "the archive compiles and imports on its own"
if (cd "$TREE" && "$PY_BIN" -m compileall -q bench api >/dev/null 2>&1); then
  check "compileall over bench and api" "0" "0"
else
  check "compileall over bench and api" "0" "1"
fi
# Import each module and confirm the file it came from is inside the archive.
#
# The interpreter is the repository's venv, because these modules need httpx, polars
# and the rest, and the archive carries no environment. That is also the risk: the
# venv sits in the working tree, so an import could in principle be satisfied from
# there. Hence the check is not "did it import" but "where did it come from" —
# module.__file__ must be under the archive. Emptying sys.path instead, which is what
# the first version of this did, removes the standard library and fails everything
# uniformly, which is a result about the check rather than about the archive.
import_fail=0
for m in "${mods[@]}"; do
  got="$( (cd "$TREE" && PYTHONDONTWRITEBYTECODE=1 "$PY_BIN" -c \
      "import importlib,sys; sys.path.insert(0,'$TREE'); m=importlib.import_module('$m'); print(getattr(m,'__file__','') or '')" \
      2>/dev/null) || true)"
  case "$got" in
    "$TREE"/*) ;;
    "") echo "       CANNOT IMPORT FROM ARCHIVE: $m"; import_fail=$((import_fail + 1)) ;;
    *)  echo "       IMPORTED FROM OUTSIDE THE ARCHIVE: $m -> $got"
        import_fail=$((import_fail + 1)) ;;
  esac
done
check "every imported bench module loads from inside the archive" "0" "$import_fail"

echo
echo "test_tracked.sh runs inside the archive, without a repository"
set +e
(cd "$TREE" && VERIFY_PY="$PY_BIN" ./scripts/test_tracked.sh) \
  > "$TMP/tracked.out" 2>&1
tracked_rc=$?
set -e
check "it exits 0" "0" "$tracked_rc"
check "and reports it ran in clean-tree mode" "yes" \
      "$(grep -q 'clean tree' "$TMP/tracked.out" && echo yes || echo no)"
check "and did not silently skip everything" "yes" \
      "$(grep -q 'tree-only mode' "$TMP/tracked.out" && echo yes || echo no)"
sed 's/^/       /' "$TMP/tracked.out" | tail -14

echo
echo "the export is still the tree whose identity was taken"
# Recomputed AFTER every compile, import and test, and compared against the digest
# taken before any of them. The identity above is only meaningful if nothing here
# changed the tree, and that is a thing to demonstrate rather than to arrange and
# assume.
AFTER="$TMP/files.after.sha256"
file_list_of "$TREE" "$AFTER"
N_AFTER="$(wc -l < "$AFTER" | tr -d ' ')"
AFTER_SHA="$(digest_of "$AFTER")"
check "the file count is unchanged" "$N" "$N_AFTER"
check "and so is the file-list digest" "$LIST_SHA" "$AFTER_SHA"
if [ "$LIST_SHA" != "$AFTER_SHA" ]; then
  echo "       the verification modified the tree it was verifying:" >&2
  diff "$LIST" "$AFTER" | head -20 | sed 's/^/       /' >&2
fi
check "no bytecode was written into the export" "0" \
      "$(find "$TREE" -name '__pycache__' -o -name '*.pyc' | wc -l | tr -d ' ')"
echo "   files            : $N"
echo "   file-list digest : $LIST_SHA"
if [ -n "$OUTDIR" ]; then
  mkdir -p "$OUTDIR"
  cp "$LIST" "$OUTDIR/archive_files.sha256"
  {
    echo "commit            $SHA"
    echo "archive_sha256    $ARCHIVE_SHA"
    echo "file_count        $N"
    echo "file_list_sha256  $LIST_SHA"
  } > "$OUTDIR/archive_identity.txt"
  echo "   written to       : $OUTDIR/"
fi

echo
echo "== summary =="
echo "   commit           $SHA"
echo "   archive_sha256   $ARCHIVE_SHA"
echo "   file_count       $N"
echo "   file_list_sha256 $LIST_SHA"

rm -r "$TMP"
echo
if [ "$fail" -gt 0 ]; then
  echo "FAILED $fail/$((pass + fail))"
  exit 1
fi
echo "all passed ($pass assertions)"
