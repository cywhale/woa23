#!/usr/bin/env bash
#
# The production store pre-flight for a C1/C2 run: identity FIRST, then a
# permission determination that NEVER WRITES.
#
# This replaces the pre-flight that ended `c1h`. That one did two things wrong and
# this file exists to do both the other way round:
#
#   1. IT WROTE. It created a file inside the production store to see whether a write
#      would be refused. The write was not refused -- the store is owned by the running
#      account -- and creating and removing that one empty file moved the STORE
#      DIRECTORY'S MTIME, which is production metadata. A pre-flight whose job is to
#      establish that nothing will be modified must not modify something to find out.
#
#   2. IT MEASURED TOO LATE. Identity was step 4 and the probe was step 3, and the
#      script exited at step 3 -- so the probe destroyed the chance to baseline the very
#      thing it might have changed. There is no pre-probe file count or byte total for
#      the production store, and there never can be for that moment.
#
# So: identity is taken before anything else, and writability is decided from `stat`
# and `access` alone. `test -w` asks the kernel whether THIS account could write; it
# does not attempt one. That is the whole difference.
#
#     ./scripts/store_readonly_preflight.sh [store-path]
#
# Exit 0  the store is present, resolves correctly, and this account CANNOT write it.
# Exit 4  the store is missing, or does not resolve to the expected path.
# Exit 5  this account CAN write the store -- a STOP, and no C1/C2 run may proceed.
#
# NOTHING under the store is created, removed, renamed, chmod-ed or chown-ed by this
# script, on any path through it, including the failure paths.
set -uo pipefail

STORE="${1:-/home/odbadmin/python/woa23/data}"

# `stat` takes different flags on GNU and BSD, and this script has to be RUNNABLE ON THE
# DEVELOPMENT MACHINE as well as on VM24. That is not tidiness: the pre-flight it replaces
# was never exercised offline, and the first time anyone saw its behaviour was against the
# production store, where its one mistake cost a metadata change. A pre-flight that cannot
# be rehearsed is the problem, not the platform difference.
if stat -c '%a' . >/dev/null 2>&1; then
  st_mode()  { stat -c '%a'     "$1"; }
  st_owner() { stat -c '%U:%G'  "$1"; }
  st_ids()   { stat -c '%u:%g'  "$1"; }
  st_uid()   { stat -c '%u'     "$1"; }
  st_mtime() { stat -c '%Y'     "$1"; }
  st_mtimeh(){ stat -c '%y'     "$1"; }
  st_links() { stat -c '%h'     "$1"; }
  st_size()  { stat -c '%s'     "$1"; }
else
  st_mode()  { stat -f '%Lp'      "$1"; }
  st_owner() { stat -f '%Su:%Sg'  "$1"; }
  st_ids()   { stat -f '%u:%g'    "$1"; }
  st_uid()   { stat -f '%u'       "$1"; }
  st_mtime() { stat -f '%m'       "$1"; }
  st_mtimeh(){ stat -f '%Sm'      "$1"; }
  st_links() { stat -f '%l'       "$1"; }
  st_size()  { stat -f '%z'       "$1"; }
fi

# `find -printf` is GNU-only too; BSD find has no equivalent, so the file list falls back
# to a per-entry stat. Slower, same output, and it means the digest is comparable between
# the two machines rather than only computable on one.
file_list() {   # file_list <dir>
  if find "$1" -maxdepth 0 -printf '' >/dev/null 2>&1; then
    ( cd "$1" && LC_ALL=C find . -printf '%p\t%s\t%T@\n' 2>/dev/null | LC_ALL=C sort )
  else
    ( cd "$1" && LC_ALL=C find . 2>/dev/null | LC_ALL=C sort \
        | while IFS= read -r e; do
            printf '%s\t%s\t%s\n' "$e" "$(st_size "$e" 2>/dev/null || echo 0)" \
                                       "$(st_mtime "$e" 2>/dev/null || echo 0)"
          done )
  fi
}

# A symlink inside the store that points outside it is a path the arms would follow out
# of the read-only area -- and whatever it lands on has its own permissions. Reported by
# where they RESOLVE, not by whether they exist.
#
# A FUNCTION, and defined up here with the other helpers, so the offline suite can
# exercise it directly. Reaching it through the whole script needs a tree that is
# readable-but-NOT-writable by the test user, which a developer machine cannot easily
# produce -- step 2 refuses first. An escape check that only ever runs on VM24 is one
# nobody has rehearsed, and unrehearsed pre-flight code is what ended c1h.
check_symlink_escapes() {   # check_symlink_escapes <dir>  -> 0 clean, 7 escapes found
  local root="$1" real n esc=0 l tgt
  real="$(realpath "$root" 2>/dev/null)" || return 7
  n="$(find "$root" -type l 2>/dev/null | wc -l | tr -d ' ')"
  echo "  symlinks under the store: $n"
  if [ "$n" != 0 ]; then
    while IFS= read -r l; do
      [ -n "$l" ] || continue
      tgt="$(realpath "$l" 2>/dev/null || echo '<broken>')"
      case "$tgt" in
        "$real"|"$real"/*) ;;
        *) echo "    ESCAPES: $l -> $tgt"; esc=$((esc + 1)) ;;
      esac
    done <<EOF
$(find "$root" -type l 2>/dev/null)
EOF
  fi
  echo "  escaping symlinks: $esc   (must be 0)"
  [ "$esc" = 0 ] || return 7
  return 0
}


# Sourcing this file with WOA23_PREFLIGHT_LIB_ONLY=1 defines the helpers and stops, so
# tests can call them without the script running its checks and exiting.
if [ "${WOA23_PREFLIGHT_LIB_ONLY:-}" = 1 ]; then
  return 0 2>/dev/null || exit 0
fi

echo "== production store pre-flight (read-only; nothing is written) =="
date -u "+timestamp UTC: %F %T"
echo "  store    : $STORE"
echo "  account  : $(id -un) (uid=$(id -u), groups=$(id -Gn))"
echo

# ---------------------------------------------------------------- 1. IDENTITY, FIRST
# Before permissions, before any decision, before anything that could touch the tree.
# If this script later refuses, the record of what was there is already taken.
echo "== 1. identity, captured BEFORE anything else =="
if [ ! -e "$STORE" ]; then
  echo "  STOP: $STORE does not exist"
  exit 4
fi
if [ ! -d "$STORE" ]; then
  echo "  STOP: $STORE is not a directory"
  exit 4
fi

RESOLVED="$(realpath "$STORE" 2>/dev/null || echo '')"
echo "  resolved path   : ${RESOLVED:-(unresolvable)}"
if [ -z "$RESOLVED" ]; then
  echo "  STOP: the store path does not resolve"
  exit 4
fi
if [ "$RESOLVED" != "$STORE" ]; then
  echo "  NOTE: the declared path resolves elsewhere."
  echo "        declared: $STORE"
  echo "        resolved: $RESOLVED"
  echo "  STOP: a store that resolves somewhere else is not the store that was authorised."
  exit 4
fi

echo "  ls -ld          : $(ls -ld "$STORE")"
echo "  mode            : $(st_mode "$STORE")"
echo "  owner:group     : $(st_owner "$STORE")"
echo "  uid:gid         : $(st_ids "$STORE")"
echo "  directory mtime : $(st_mtime "$STORE")  ($(st_mtimeh "$STORE"))"
echo "  hard links      : $(st_links "$STORE")"
echo "  dir size        : $(st_size "$STORE")"
echo "  top-level       : $(ls -1 "$STORE" | wc -l | tr -d ' ') entries"
ls -1 "$STORE" | sed 's/^/      /'
echo -n "  total files     : "; find "$STORE" -type f 2>/dev/null | wc -l | tr -d ' '
# `du -sb` is GNU-only. Falling back to `du -sk` would report BLOCKS rather than bytes and
# the two would not be comparable between machines, so the fallback sums real sizes.
echo -n "  total bytes     : "
if du -sb "$STORE" >/dev/null 2>&1; then
  du -sb "$STORE" 2>/dev/null | cut -f1
else
  find "$STORE" -type f -exec stat -f '%z' {} + 2>/dev/null \
    | awk '{s+=$1} END {print s+0}'
fi

# A METADATA FINGERPRINT -- names, sizes and mtimes. NOT a content baseline, and it must
# never be reported as one.
#
# What it can do: tell a later pre-flight that the tree looks the same as it did, or that
# something moved. What it CANNOT do: prove file contents are unchanged. A write that
# preserves size and mtime is invisible to it, and it says nothing at all about the bytes.
# Hashing 35 GB on every pre-flight would cost more than it tells, so this is the trade
# being made deliberately -- and named, so nobody later reads this digest as proof of
# content integrity.
#
# It is also NOT a baseline for what c1h disturbed. c1h had no baseline of any kind: its
# probe ran before its identity step, so no pre-probe file count, byte total or digest for
# the production store exists, and none can be reconstructed. The first value this script
# records is the first that has ever existed -- a starting point from here, not a recovery
# of what was lost. See specs/C1-result-c1h.md section 3.
#
# LC_ALL=C so the ordering is the reader's too, not the locale's.
echo -n "  file-list digest (METADATA fingerprint, not a content baseline): "
file_list "$STORE" | { sha256sum 2>/dev/null || shasum -a 256; } | cut -d' ' -f1
echo

# ------------------------------------------------- 2. WRITABILITY, WITHOUT WRITING
# `test -w` consults the kernel's access check for THIS process. It creates nothing,
# removes nothing, and leaves no trace -- unlike the probe that ended c1h.
echo "== 2. can THIS account write the store? (stat/access only -- no probe) =="
OWNER_UID="$(st_uid "$STORE")"
MY_UID="$(id -u)"
echo "  store uid       : $OWNER_UID"
echo "  my uid          : $MY_UID"
[ "$OWNER_UID" = "$MY_UID" ] && echo "  -> THIS ACCOUNT OWNS THE STORE" \
                             || echo "  -> this account does not own the store"

WRITABLE=no
if [ -w "$STORE" ]; then WRITABLE=yes; fi
echo "  kernel access check (test -w): $WRITABLE"

if [ "$WRITABLE" = yes ]; then
  echo
  echo "  STOP: this account CAN write the production store."
  echo
  echo "  C1/C2 require read-only access that is ENFORCED, not merely intended. An"
  echo "  account that owns the store can always write it, so no amount of care in the"
  echo "  arms makes the guarantee checkable -- which is the whole point of checking."
  echo
  echo "  This is NOT to be resolved by chmod, chown, or a bind mount. Those change"
  echo "  production, or need privilege this campaign does not hold, and both are"
  echo "  forbidden. The resolution is an account that does not own the store."
  exit 5
fi

echo "  ok: this account cannot write the store at its root."
echo

# ------------------------------------------- 3. THE WHOLE TREE, not just the root
# The root being read-only says nothing about what is under it. A single writable
# subdirectory is a place this run could leave a file in production's data, and a
# single unreadable one is a case the arms would fail on halfway through a run
# rather than here.
#
# `-readable` / `-writable` / `-executable` use access(2), so they respect the ACL
# that grants this account r-x -- which a mode-bits comparison would not.
echo "== 3. the COMPLETE tree, as this account sees it =="

# `-readable`, `-writable` and `-executable` are GNU predicates. BSD find rejects them
# with "unknown primary or operator" -- and a find that errors inside a counting pipeline
# produces 0, which reads exactly like "nothing is writable". That is the vacuous pass
# this project keeps designing against, and it is not hypothetical: the first version of
# this block was "verified working" against a Homebrew GNU find on PATH while the script
# itself was getting /usr/bin/find, which supports none of them.
#
# So the capability is DETECTED, and when it is missing the scan falls back to testing
# each entry with `[ -r ]` / `[ -w ]` / `[ -x ]`. Those use access(2), like the GNU
# predicates, so they respect the ACL that grants this account r-x -- which a mode-bits
# comparison would not. The fallback is slower and gives the same answer, which is the
# trade that keeps this script rehearsable on a developer machine.
if find "$STORE" -maxdepth 0 -writable >/dev/null 2>&1; then
  FIND_ACCESS=gnu
  echo "  scan mode: GNU find predicates (-readable/-writable/-executable)"
else
  FIND_ACCESS=portable
  echo "  scan mode: portable per-entry access checks (this find lacks -writable)"
fi

# count_bad <type> <test-flag> : entries of <type> for which the test FAILS
count_bad() {               # count_bad d|f  r|w|x
  local ty="$1" fl="$2" n=0 e
  if [ "$FIND_ACCESS" = gnu ]; then
    case "$fl" in
      r) find "$STORE" -type "$ty" ! -readable   2>/dev/null | wc -l | tr -d ' ' ;;
      x) find "$STORE" -type "$ty" ! -executable 2>/dev/null | wc -l | tr -d ' ' ;;
    esac
    return
  fi
  while IFS= read -r e; do
    [ -n "$e" ] || continue
    case "$fl" in
      r) [ -r "$e" ] || n=$((n + 1)) ;;
      x) [ -x "$e" ] || n=$((n + 1)) ;;
    esac
  done <<EOF
$(find "$STORE" -type "$ty" 2>/dev/null)
EOF
  printf '%s' "$n"
}

count_writable() {
  local n=0 e
  if [ "$FIND_ACCESS" = gnu ]; then
    find "$STORE" -writable 2>/dev/null | wc -l | tr -d ' '
    return
  fi
  while IFS= read -r e; do
    [ -n "$e" ] || continue
    [ -w "$e" ] && n=$((n + 1))
  done <<EOF
$(find "$STORE" 2>/dev/null)
EOF
  printf '%s' "$n"
}

list_writable() {
  if [ "$FIND_ACCESS" = gnu ]; then
    find "$STORE" -writable 2>/dev/null
    return
  fi
  find "$STORE" 2>/dev/null | while IFS= read -r e; do
    [ -w "$e" ] && printf '%s\n' "$e"
  done
}

not_trav="$(count_bad d x)"
not_rd_d="$(count_bad d r)"
not_rd_f="$(count_bad f r)"
writable="$(count_writable)"
echo "  directories NOT traversable : $not_trav   (must be 0)"
echo "  directories NOT readable    : $not_rd_d   (must be 0)"
echo "  files       NOT readable    : $not_rd_f   (must be 0)"
echo "  paths          WRITABLE     : $writable   (must be 0)"

if [ "$not_trav" != 0 ] || [ "$not_rd_d" != 0 ] || [ "$not_rd_f" != 0 ]; then
  echo
  echo "  offending paths (first 20):"
  { find "$STORE" -type d ! -executable 2>/dev/null
    find "$STORE" -type d ! -readable   2>/dev/null
    find "$STORE" -type f ! -readable   2>/dev/null; } | sort -u | head -20 | sed 's/^/    /'
  echo
  echo "  STOP: the arms cannot read the whole store as this account. A run that"
  echo "  fails partway through is worse than one that refuses at the start."
  exit 6
fi

if [ "$writable" != 0 ]; then
  echo
  echo "  writable paths (first 20):"
  list_writable | head -20 | sed 's/^/    /'
  echo
  echo "  STOP: some path under the production store is writable by this account."
  echo "  Read-only must hold for the WHOLE tree, not merely its root."
  exit 5
fi
echo "  ok: every directory traversable and readable, every file readable,"
echo "      and nothing under the store is writable by this account."
echo

# --------------------------------------------------- 4. SYMLINK ESCAPES
# A symlink inside the store that points outside it is a path the arms would follow
# out of the read-only area -- and whatever it lands on has its own permissions.
# Reported by where they RESOLVE, not by whether they exist.
echo "== 4. symlink escapes =="
if ! check_symlink_escapes "$STORE"; then
  echo "  STOP: a symlink inside the store resolves outside it. The arms would"
  echo "  follow it out of the read-only area."
  exit 7
fi
echo "  ok: no symlink inside the store resolves outside it."
echo

echo "PRE-FLIGHT COMPLETE -- enforceable read-only confirmed for the whole tree"
exit 0
