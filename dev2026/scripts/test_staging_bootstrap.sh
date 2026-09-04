#!/usr/bin/env bash
#
# The bootstrap lifecycle, and the confusion `b35a1` died of.
#
# b35a1 extracted the archive into the STAGING ROOT so the driver would be on disk, and
# the driver then refused because the staging root must not exist. The bootstrap had
# consumed the identity it was about to check.
#
# So the properties under test are:
#
#   1. a bootstrap OUTSIDE every identity path lets the real sequence run to completion
#   2. a bootstrap that IS the staging root fails closed
#   3. a bootstrap inside the workdir / store / PM2_HOME / TMPDIR fails closed
#   4. a pre-existing staging root is STILL refused — the guard is not relaxed
#   5. an archive/driver/subject mismatch is refused
#   6. none of this weakens the stale / foreign / modified-tree guards
#
# The sequence tested is the REAL one -- transfer, extract the driver to an external
# bootstrap, --phase stage creates the staging root, then --phase run -- and NOT
# "extract everything into the staging root, then run the driver", which is the shape
# that failed.
#
#     ./scripts/test_staging_bootstrap.sh

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
BOOTSTRAP="$REPO/deploy/staging_bootstrap.sh"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then pass=$((pass + 1)); echo "  ok   $1"
  else fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}
yn() { if "$@" >/dev/null 2>&1; then echo yes; else echo no; fi; }
has() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

# shellcheck disable=SC1090
WOA23_BOOTSTRAP_LIB_ONLY=1 . "$BOOTSTRAP"

echo "paths_overlap — a true path relation, not a string prefix"
check "identical paths overlap" yes "$(yn paths_overlap /a/b /a/b)"
check "  trailing slashes do not matter" yes "$(yn paths_overlap /a/b/ /a/b)"
check "a child overlaps its parent" yes "$(yn paths_overlap /a/b/c /a/b)"
check "a PARENT overlaps its child (containment counts both ways)" yes \
      "$(yn paths_overlap /a /a/b)"
check "a sibling sharing a prefix does NOT overlap" no "$(yn paths_overlap /a/bc /a/b)"
check "  nor a lookalike" no "$(yn paths_overlap /a/b-old /a/b)"
check "unrelated paths do not overlap" no "$(yn paths_overlap /x/y /a/b)"

echo
echo "bootstrap_path_problem — the b35a1 mistake, refused by name"
R=/home/u/woa23-x; W=/home/u/woa23-x-work; T=/home/u/tmp-x
P=/home/u/woa23-x-pm2; S="$R/store"; C="$R/dev2026/deploy"

check "a bootstrap OUTSIDE everything is fine" "" \
      "$(bootstrap_path_problem /home/u/x-bootstrap "$R" "$W" "$T" "$P" "$S" "$C")"
check "bootstrap == staging root is REFUSED (this is b35a1)" yes \
      "$(has "$(bootstrap_path_problem "$R" "$R" "$W" "$T" "$P" "$S" "$C")" 'staging root')"
check "bootstrap INSIDE the staging root is refused" yes \
      "$(has "$(bootstrap_path_problem "$R/boot" "$R" "$W" "$T" "$P" "$S" "$C")" 'staging root')"
check "bootstrap CONTAINING the staging root is refused" yes \
      "$(has "$(bootstrap_path_problem /home/u "$R" "$W" "$T" "$P" "$S" "$C")" 'staging root')"
check "bootstrap inside the workdir is refused" yes \
      "$(has "$(bootstrap_path_problem "$W/b" "$R" "$W" "$T" "$P" "$S" "$C")" 'workdir')"
check "bootstrap inside TMPDIR is refused" yes \
      "$(has "$(bootstrap_path_problem "$T/b" "$R" "$W" "$T" "$P" "$S" "$C")" 'TMPDIR')"
check "bootstrap inside PM2_HOME is refused" yes \
      "$(has "$(bootstrap_path_problem "$P/b" "$R" "$W" "$T" "$P" "$S" "$C")" 'PM2_HOME')"
# The default store is INSIDE the staging root, so an overlap there is attributed to the
# root -- the first identity path it matches. It is still refused, which is what matters;
# the first draft of this assertion looked for the word "store" and was simply wrong.
check "bootstrap inside the (root-nested) store is refused" yes \
      "$([ -n "$(bootstrap_path_problem "$S/b" "$R" "$W" "$T" "$P" "$S" "$C")" ] && echo yes || echo no)"
check "  attributed to the staging root that contains it" yes \
      "$(has "$(bootstrap_path_problem "$S/b" "$R" "$W" "$T" "$P" "$S" "$C")" 'staging root')"
# A store placed OUTSIDE the root is attributed to the store itself.
check "bootstrap inside an EXTERNAL store is refused by name" yes \
      "$(has "$(bootstrap_path_problem /home/u/ext-store/b "$R" "$W" "$T" "$P" /home/u/ext-store "$C")" 'store')"
check "bootstrap at a production path is refused" yes \
      "$(has "$(bootstrap_path_problem /home/odbadmin/python/woa23/b "$R" "$W" "$T" "$P" "$S" "$C")" 'production')"
check "  and production's PM2_HOME too" yes \
      "$(has "$(bootstrap_path_problem /home/odbadmin/.pm2 "$R" "$W" "$T" "$P" "$S" "$C")" 'production')"
check "a relative bootstrap path is refused" yes \
      "$(has "$(bootstrap_path_problem relative/dir "$R" "$W" "$T" "$P" "$S" "$C")" 'must be absolute')"
check "an empty bootstrap path is refused" yes \
      "$(has "$(bootstrap_path_problem '' "$R" "$W" "$T" "$P" "$S" "$C")" 'empty')"
check "a bootstrap merely NAMED like the root is fine" "" \
      "$(bootstrap_path_problem "${R}-bootstrap" "$R" "$W" "$T" "$P" "$S" "$C")"

# ------------------------------------------------------------------ end to end ---
# A fake archive holding a stub driver, so the REAL sequence is exercised: extract the
# driver to an external bootstrap, then hand over. The stub records the argv it received
# and creates the staging root, which is what --phase stage does.
mkarchive() {   # mkarchive <dir> [driver body file]
  local d="$1" body="${2:-}"
  mkdir -p "$d/src/dev2026/deploy"
  if [ -n "$body" ]; then cp "$body" "$d/src/dev2026/deploy/staging_execute.sh"
  else
    cat > "$d/src/dev2026/deploy/staging_execute.sh" <<'STUB'
#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
root=""; prev=""
for a in "$@"; do [ "$prev" = "--root" ] && root="$a"; prev="$a"; done
echo "DRIVER RAN from $HERE"
echo "DRIVER ARGV: $*"
[ -n "$root" ] && { [ -e "$root" ] && { echo "DRIVER REFUSES: $root exists" >&2; exit 2; }
                    mkdir -p "$root/dev2026"; echo "DRIVER CREATED $root"; }
exit 0
STUB
  fi
  chmod 755 "$d/src/dev2026/deploy/staging_execute.sh"
  # EVERY MANIFEST MEMBER, not just the driver. The bootstrap now refuses an archive that
  # cannot supply all of them, and the real library is used rather than a stub so these
  # fixtures cannot drift away from what is actually delivered.
  cp "$REPO/deploy/lib_store_guard.sh" "$d/src/dev2026/deploy/lib_store_guard.sh"
  ( cd "$d/src" && tar -cf "$d/subject.tar" dev2026 )
}

echo
echo "END TO END — the REAL sequence, not 'extract everything into the staging root'"
D="$(mktemp -d)"
mkarchive "$D"
BOOT="$D/bootstrap"; ROOT="$D/woa23-x"
out="$(bash "$BOOTSTRAP" --archive "$D/subject.tar" --archive-sha256 "$(sha256sum "$D/subject.tar" | cut -d' ' -f1)" --bootstrap "$BOOT" \
        --root "$ROOT" --pm2-home "$D/x-pm2" --tmpdir "$D/tmp-x" \
        -- --phase stage --label x 2>&1)"; rc=$?
check "the sequence exits 0" 0 "$rc"
check "  every member was verified against the archive" yes \
      "$(has "$out" "MATCH — every member is the archive's own")"
check "  it ran FROM the bootstrap, not the staging root" yes "$(has "$out" "DRIVER RAN from $BOOT")"
check "  the driver received --root and --archive" yes "$(has "$out" -- "--root $ROOT")"
check "  and the phase args passed through" yes "$(has "$out" -- '--phase stage --label x')"
check "  the DRIVER created the staging root" yes "$(has "$out" "DRIVER CREATED $ROOT")"
# It holds the MANIFEST, and nothing else. "Only the driver" was the assumption that
# let the store guard library go undelivered all the way to D-3; the invariant was never
# "one file", it was "exactly the files the driver needs and no others".
check "  the bootstrap holds exactly the manifest's members" 2 \
      "$(find "$BOOT" -type f | wc -l | tr -d ' ')"
check "  ... the driver" yes "$(yn test -f "$BOOT/deploy/staging_execute.sh")"
check "  ... and the store guard library" yes "$(yn test -f "$BOOT/deploy/lib_store_guard.sh")"
check "  and the bootstrap is NOT inside the staging root" no \
      "$(yn paths_overlap "$BOOT" "$ROOT")"
rm -r "$D"

echo
echo "FAIL CLOSED — bootstrap colliding with the identity"
for label in root workdir store pm2home tmpdir; do
  D="$(mktemp -d)"; mkarchive "$D"; ROOT="$D/woa23-x"
  case "$label" in
    root)    B="$ROOT" ;;
    workdir) B="$D/woa23-x-work/b" ;;
    store)   B="$ROOT/store/b" ;;
    pm2home) B="$D/x-pm2/b" ;;
    tmpdir)  B="$D/tmp-x/b" ;;
  esac
  out="$(bash "$BOOTSTRAP" --archive "$D/subject.tar" --archive-sha256 "$(sha256sum "$D/subject.tar" | cut -d' ' -f1)" --bootstrap "$B" \
          --root "$ROOT" --workdir "$D/woa23-x-work" --pm2-home "$D/x-pm2" \
          --tmpdir "$D/tmp-x" -- --phase stage --label x 2>&1)"; rc=$?
  check "bootstrap at the $label is REFUSED" 2 "$rc"
  check "  and says a bootstrap must be outside the identity" yes \
        "$(has "$out" 'must be OUTSIDE every identity path')"
  check "  and nothing was created there" no "$(yn test -e "$B")"
  rm -r "$D"
done

echo
echo "THE FRESHNESS GUARD IS NOT RELAXED"
D="$(mktemp -d)"; mkarchive "$D"; ROOT="$D/woa23-x"
mkdir -p "$ROOT"                      # a previous attempt got this far
out="$(bash "$BOOTSTRAP" --archive "$D/subject.tar" --archive-sha256 "$(sha256sum "$D/subject.tar" | cut -d' ' -f1)" --bootstrap "$D/bootstrap" \
        --root "$ROOT" --pm2-home "$D/x-pm2" -- --phase stage --label x 2>&1)"; rc=$?
check "a pre-existing staging root is STILL refused" 2 "$rc"
check "  with the not-deleted-or-reused wording intact" yes \
      "$(has "$out" 'NOT deleted, emptied or reused')"
check "  and it is refused BEFORE the driver is extracted" no \
      "$(yn test -e "$D/bootstrap/deploy/staging_execute.sh")"
rm -r "$D"

D="$(mktemp -d)"; mkarchive "$D"; ROOT="$D/woa23-x"
mkdir -p "$D/x-pm2"
out="$(bash "$BOOTSTRAP" --archive "$D/subject.tar" --archive-sha256 "$(sha256sum "$D/subject.tar" | cut -d' ' -f1)" --bootstrap "$D/bootstrap" \
        --root "$ROOT" --pm2-home "$D/x-pm2" -- --phase stage --label x 2>&1)"; rc=$?
check "a pre-existing PM2_HOME is refused too" 2 "$rc"
rm -r "$D"

echo
echo "ARCHIVE / DRIVER / SUBJECT MISMATCH"
D="$(mktemp -d)"; mkarchive "$D"
out="$(bash "$BOOTSTRAP" --archive "$D/nosuch.tar" --archive-sha256 deadbeef --bootstrap "$D/bootstrap" \
        --root "$D/woa23-x" --pm2-home "$D/x-pm2" -- --phase stage 2>&1)"; rc=$?
check "an unreadable archive is refused" 2 "$rc"
check "  by name" yes "$(has "$out" 'archive not readable')"

# an archive with no driver member at all
mkdir -p "$D/empty/dev2026/deploy"; : > "$D/empty/dev2026/deploy/other.sh"
( cd "$D/empty" && tar -cf "$D/nodriver.tar" dev2026 )
out="$(bash "$BOOTSTRAP" --archive "$D/nodriver.tar" --archive-sha256 "$(sha256sum "$D/nodriver.tar" | cut -d' ' -f1)" --bootstrap "$D/b2" \
        --root "$D/woa23-y" --pm2-home "$D/y-pm2" -- --phase stage 2>&1)"; rc=$?
check "an archive without the driver is refused" 2 "$rc"
check "  and names the member the archive does not carry" yes \
      "$(has "$out" 'contains no member')"
check "  and says every delivered file must come from the archive" yes \
      "$(has "$out" 'must come from the archive under test')"
rm -r "$D"

echo
echo "path_is_within — narrower than overlap, for ANCESTORS"
check "a path inside another is within it" yes "$(yn path_is_within /a/b/c /a/b)"
check "identical paths are within" yes "$(yn path_is_within /a/b /a/b)"
check "a PARENT is NOT within its child" no "$(yn path_is_within /a /a/b)"
check "  which is the whole point: a bootstrap's parent legitimately contains the root" no \
      "$(yn path_is_within /home/u /home/u/woa23-x)"
check "a sibling is not within" no "$(yn path_is_within /a/bc /a/b)"

echo
echo "ARCHIVE FRESHNESS — an old archive at the same path is refused"
D="$(mktemp -d)"; mkarchive "$D"
out="$(bash "$BOOTSTRAP" --archive "$D/subject.tar" --archive-sha256 \
        0000000000000000000000000000000000000000000000000000000000000000 \
        --bootstrap "$D/bootstrap" --root "$D/woa23-x" --pm2-home "$D/x-pm2" \
        -- --phase stage --label x 2>&1)"; rc=$?
check "a digest mismatch on the archive is refused" 2 "$rc"
check "  and names it as not the authorised archive" yes \
      "$(has "$out" 'not the one this run was authorised for')"
check "  and says a stale archive is what this catches" yes \
      "$(has "$out" 'stale archive left at the transfer path')"
check "  nothing was created" no "$(yn test -e "$D/bootstrap")"

check "an archive INSIDE the staging root is refused" 2 \
      "$(mkdir -p "$D/woa23-y" && cp "$D/subject.tar" "$D/woa23-y/a.tar"
         bash "$BOOTSTRAP" --archive "$D/woa23-y/a.tar" \
           --archive-sha256 "$(sha256sum "$D/woa23-y/a.tar" | cut -d' ' -f1)" \
           --bootstrap "$D/b3" --root "$D/woa23-y" --pm2-home "$D/y-pm2" \
           -- --phase stage --label y >/dev/null 2>&1; echo $?)"
rm -r "$D"

echo
echo "BOOTSTRAP FRESHNESS — a pre-existing bootstrap is refused"
D="$(mktemp -d)"; mkarchive "$D"
mkdir -p "$D/bootstrap"                       # a leftover from a previous attempt
out="$(bash "$BOOTSTRAP" --archive "$D/subject.tar" \
        --archive-sha256 "$(sha256sum "$D/subject.tar" | cut -d' ' -f1)" \
        --bootstrap "$D/bootstrap" --root "$D/woa23-x" --pm2-home "$D/x-pm2" \
        -- --phase stage --label x 2>&1)"; rc=$?
check "a pre-existing bootstrap path is refused" 2 "$rc"
check "  and is NOT emptied or reused" yes "$(has "$out" 'NOT emptied or reused')"
rm -r "$D"

echo
echo "SYMLINK ESCAPES — fail closed"
D="$(mktemp -d)"; mkarchive "$D"
mkdir -p "$D/woa23-x"                          # the identity the symlink points into
ln -s "$D/woa23-x" "$D/sneaky"                 # a bootstrap PARENT that resolves inside it
out="$(bash "$BOOTSTRAP" --archive "$D/subject.tar" \
        --archive-sha256 "$(sha256sum "$D/subject.tar" | cut -d' ' -f1)" \
        --bootstrap "$D/sneaky/boot" --root "$D/woa23-x" --pm2-home "$D/x-pm2" \
        -- --phase stage --label x 2>&1)"; rc=$?
check "a bootstrap whose PARENT is a symlink into the identity is refused" 2 "$rc"
# The symlinked parent is caught by the explicit symlink refusal in 1b, BEFORE the
# realpath comparison gets a turn -- an earlier and clearer refusal. Assert what
# actually happens rather than the message I first guessed at.
check "  and it is refused as a symlinked PARENT, explicitly" yes \
      "$(has "$out" "the bootstrap's PARENT is a SYMLINK")"
rm -r "$D"

D="$(mktemp -d)"; mkarchive "$D"
mkdir -p "$D/elsewhere"; ln -s "$D/elsewhere" "$D/bootlink"
out="$(bash "$BOOTSTRAP" --archive "$D/subject.tar" \
        --archive-sha256 "$(sha256sum "$D/subject.tar" | cut -d' ' -f1)" \
        --bootstrap "$D/bootlink" --root "$D/woa23-x" --pm2-home "$D/x-pm2" \
        -- --phase stage --label x 2>&1)"; rc=$?
check "a bootstrap path that IS a symlink is refused (it already exists)" 2 "$rc"
rm -r "$D"

echo
echo "ARCHIVE MEMBER — unique, expected path, REGULAR FILE only"
check "a plain regular member is accepted" "" \
      "$(archive_member_problem '-rwxr-xr-x 0 u g 17 Jan 1 00:00 dev2026/deploy/staging_execute.sh' \
         dev2026/deploy/staging_execute.sh)"
check "a DIRECTORY member is refused" yes \
      "$(has "$(archive_member_problem 'drwxr-xr-x 0 u g 0 Jan 1 00:00 dev2026/deploy/staging_execute.sh' \
         dev2026/deploy/staging_execute.sh)" 'DIRECTORY')"
check "a SYMLINK member is refused" yes \
      "$(has "$(archive_member_problem 'lrwxrwxrwx 0 u g 0 Jan 1 00:00 dev2026/deploy/staging_execute.sh -> /etc/passwd' \
         dev2026/deploy/staging_execute.sh)" 'SYMLINK')"
check "a HARD LINK member is refused" yes \
      "$(has "$(archive_member_problem 'hrw-r--r-- 0 u g 0 Jan 1 00:00 dev2026/deploy/staging_execute.sh' \
         dev2026/deploy/staging_execute.sh)" 'HARD LINK')"
check "a DUPLICATE member is refused" yes \
      "$(has "$(archive_member_problem '-rwxr-xr-x 0 u g 17 Jan 1 00:00 dev2026/deploy/staging_execute.sh
-rwxr-xr-x 0 u g 99 Jan 1 00:00 dev2026/deploy/staging_execute.sh' \
         dev2026/deploy/staging_execute.sh)" 'duplicate member')"
check "  and the reason names the overwrite risk" yes \
      "$(has "$(archive_member_problem '-rwxr-xr-x 0 u g 17 Jan 1 00:00 dev2026/deploy/staging_execute.sh
-rwxr-xr-x 0 u g 99 Jan 1 00:00 dev2026/deploy/staging_execute.sh' \
         dev2026/deploy/staging_execute.sh)" 'overwrite the verified first')"
check "a missing member is refused" yes \
      "$(has "$(archive_member_problem '-rw-r--r-- 0 u g 1 Jan 1 00:00 dev2026/deploy/other.sh' \
         dev2026/deploy/staging_execute.sh)" 'no member')"
check "a path-prefix lookalike does NOT satisfy the member check" yes \
      "$(has "$(archive_member_problem '-rwxr-xr-x 0 u g 17 Jan 1 00:00 dev2026/deploy/staging_execute.sh.bak' \
         dev2026/deploy/staging_execute.sh)" 'no member')"

echo
echo "THE RUN PHASE LOADS THE SUBJECT FROM THE STAGING ROOT — not bootstrap, not checkout"
# The real driver derives every subject path from --root ($TREE = $ROOT/dev2026), never
# from $HERE. This stub proves the handover carries --root through to the run phase, and
# that the tree the run would read is under the staging root.
D="$(mktemp -d)"
mkdir -p "$D/src/dev2026/deploy"
cat > "$D/src/dev2026/deploy/staging_execute.sh" <<'STUB2'
#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
root=""; phase=""; prev=""
for a in "$@"; do
  [ "$prev" = "--root" ] && root="$a"
  [ "$prev" = "--phase" ] && phase="$a"
  prev="$a"
done
TREE="$root/dev2026"
echo "PHASE=$phase HERE=$HERE TREE=$TREE"
if [ "$phase" = stage ]; then
  mkdir -p "$TREE/api" "$TREE/bench" "$TREE/scripts" "$TREE/deploy"
  echo "subject-api"    > "$TREE/api/marker"
  echo "subject-bench"  > "$TREE/bench/marker"
  echo "subject-deploy" > "$TREE/deploy/marker"
  echo "STAGED $TREE"
else
  for d in api bench scripts deploy; do
    [ -d "$TREE/$d" ] && echo "RUN READS $TREE/$d" || { echo "MISSING $TREE/$d" >&2; exit 3; }
  done
  echo "RUN MARKER api=$(cat "$TREE/api/marker")"
  echo "RUN SOURCE ROOT=$root"
fi
exit 0
STUB2
chmod 755 "$D/src/dev2026/deploy/staging_execute.sh"
cp "$REPO/deploy/lib_store_guard.sh" "$D/src/dev2026/deploy/lib_store_guard.sh"
( cd "$D/src" && tar -cf "$D/subject.tar" dev2026 )
SHA="$(sha256sum "$D/subject.tar" | cut -d' ' -f1)"
BOOT="$D/bootstrap"; ROOT="$D/woa23-x"

s1="$(bash "$BOOTSTRAP" --archive "$D/subject.tar" --archive-sha256 "$SHA" \
       --bootstrap "$BOOT" --root "$ROOT" --pm2-home "$D/x-pm2" \
       -- --phase stage --label x 2>&1)"; rc1=$?
check "stage exits 0" 0 "$rc1"
check "  and the DRIVER created the subject tree under the staging root" yes \
      "$(has "$s1" "STAGED $ROOT/dev2026")"

# The run phase reuses the SAME bootstrap driver -- already extracted and verified.
s2="$("$BOOT/deploy/staging_execute.sh" --root "$ROOT" --archive "$D/subject.tar" \
       --phase run --label x 2>&1)"; rc2=$?
check "run exits 0" 0 "$rc2"
check "  run reads api from the STAGING ROOT" yes "$(has "$s2" "RUN READS $ROOT/dev2026/api")"
check "  run reads bench from the STAGING ROOT" yes "$(has "$s2" "RUN READS $ROOT/dev2026/bench")"
check "  run reads scripts from the STAGING ROOT" yes "$(has "$s2" "RUN READS $ROOT/dev2026/scripts")"
check "  run reads deploy from the STAGING ROOT" yes "$(has "$s2" "RUN READS $ROOT/dev2026/deploy")"
check "  and the content is the staged subject's" yes "$(has "$s2" 'RUN MARKER api=subject-api')"
check "  the subject tree is NOT under the bootstrap" no "$(yn test -d "$BOOT/dev2026/api")"
check "  the bootstrap still holds exactly the manifest's members" 2 \
      "$(find "$BOOT" -type f | wc -l | tr -d ' ')"
check "  and TREE was derived from --root, not from HERE" yes \
      "$(has "$s2" "TREE=$ROOT/dev2026")"
rm -r "$D"

echo
echo "the REAL driver derives its tree from --root, never from \$HERE"
drvsrc="$(grep -v '^[[:space:]]*#' "$REPO/deploy/staging_execute.sh")"
check "TREE is derived from ROOT" 1 \
      "$(printf '%s' "$drvsrc" | grep -cE '^TREE="\$ROOT/' || true)"
check "  the generated config lives in the TREE, not HERE" 1 \
      "$(printf '%s' "$drvsrc" | grep -cE '^CONFIG="\$TREE/' || true)"
check "  \$HERE is never used to locate subject files" 0 \
      "$(printf '%s' "$drvsrc" | grep -cE '\$HERE/(api|bench|scripts|deploy)' || true)"

echo
echo "the driver is never taken from a checkout"
code="$(grep -v '^[[:space:]]*#' "$BOOTSTRAP")"
check "each member is extracted with tar -xO from the archive" 1 \
      "$(printf '%s' "$code" | grep -c 'tar -xOf "\$ARCHIVE" "\$m" > "\$BOOT/deploy/\$base"' || true)"
check "  its digest is taken from the archive FIRST" 1 \
      "$(printf '%s' "$code" | grep -c 'want="\$(tar -xOf' || true)"
check "  and compared after extraction" 1 \
      "$(printf '%s' "$code" | grep -cE 'if \[ "\$got" != "\$want" \]' || true)"
check "  over the MANIFEST, not one hard-coded member" 2 \
      "$(printf '%s' "$code" | grep -c 'BOOTSTRAP_MEMBERS" | while IFS= read -r m; do' || true)"
check "no copy from a repository path" 0 \
      "$(printf '%s' "$code" | grep -cE '(^|[^a-zA-Z_])cp .*(REPO|HERE)' || true)"

echo
echo "no guard was weakened to make this work"
drv="$(grep -v '^[[:space:]]*#' "$REPO/deploy/staging_execute.sh")"
check "the driver still refuses a pre-existing identity element" yes \
      "$(has "$drv" 'already exists')"
check "  still says it is NOT deleted or reused" yes \
      "$(has "$drv" 'NOT deleted, emptied or reused')"
check "  still refuses a tree containing .git" yes "$(has "$drv" 'contains .git')"
check "  still refuses a file-count mismatch" yes "$(has "$drv" 'SUBJECT MISMATCH')"
check "  still refuses a modified/stale/foreign tree" yes \
      "$(has "$drv" 'modified, stale or foreign')"
check "the bootstrap does not pass any override to the driver" 0 \
      "$(printf '%s' "$code" | grep -cE 'allow-reused|--force|skip' || true)"

echo
echo "the script parses"
check "bash -n" 0 "$(bash -n "$BOOTSTRAP" >/dev/null 2>&1; echo $?)"
check "no 'case' inside a command substitution" 0 \
      "$(grep -cE '\$\([^)]*case ' "$BOOTSTRAP" || true)"

echo
suite_summary "$pass" "$fail"
