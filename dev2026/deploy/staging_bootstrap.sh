#!/usr/bin/env bash
#
# The bootstrap step, and the distinction `b35a1` did not have.
#
# WHAT WENT WRONG. `staging_execute.sh` lives INSIDE the archive and must be on disk
# before it can run — it cannot be piped over stdin, because it derives `$HERE` from
# `BASH_SOURCE`. To get it there, `b35a1` extracted the whole archive into
# `/home/woa23c1ro/woa23-b35a1` — which IS the staging root the driver's own freshness
# guard requires to be ABSENT. The bootstrap consumed the identity the driver was about
# to check, and the driver refused, correctly.
#
# TWO KINDS OF PATH, and the whole point of this file is that they never coincide:
#
#   BOOTSTRAP PATH      where the driver is placed so it can be RUN. Scratch. It holds
#                       one file. It is NOT part of the run's identity and nothing about
#                       the run is recorded there.
#
#   IDENTITY PATHS      staging root, workdir, TMPDIR, PM2_HOME, store, generated config.
#                       Every one of these MUST NOT EXIST before `--phase stage`, because
#                       their presence means a previous attempt got that far and its
#                       evidence must not be silently reused.
#
# THE FRESHNESS GUARD IS NOT RELAXED. Not by one path, not by one flag. The fix is that
# the bootstrap stops standing on the identity's toes, not that the identity stops
# checking.
#
# THE DRIVER IS TAKEN FROM THE ARCHIVE AND RE-VERIFIED AGAINST IT. Never from a checkout,
# never from a path an earlier run left behind: the bytes are streamed out of the archive
# with `tar -xO`, hashed, extracted, and the extracted file is hashed again and compared.
# A driver that does not match the archive it claims to come from is a refusal.
#
#   ./deploy/staging_bootstrap.sh \
#       --archive   /path/to/subject.tar \
#       --bootstrap /home/woa23c1ro/b35b1-bootstrap \
#       --root      /home/woa23c1ro/woa23-b35b1 \
#       --workdir   /home/woa23c1ro/woa23-b35b1-work \
#       --tmpdir    /home/woa23c1ro/tmp-b35b1 \
#       --pm2-home  /home/woa23c1ro/woa23-b35b1-pm2 \
#       -- --phase stage --label b35b1 --port 18291 --app woa23-b35b1-candidate \
#          --files 203 --filelist <sha256>
#
# Everything after `--` is passed to the driver unchanged, with `--root` and `--archive`
# supplied automatically.
#
# Sourcing with WOA23_BOOTSTRAP_LIB_ONLY=1 defines the pure guards and stops.

set -uo pipefail

#: The driver's path inside the archive -- the file that is EXECUTED once delivered.
DRIVER_MEMBER="dev2026/deploy/staging_execute.sh"

#: The store-ownership guard the driver refuses to run without.
GUARD_MEMBER="dev2026/deploy/lib_store_guard.sh"

#: EVERY member the bootstrap must deliver, driver included.
#:
#: This list exists because the driver stopped being one self-contained file. The store
#: ownership guard was moved into a sibling library so its decision could be tested, and
#: the bootstrap still carried only the driver -- so the guard function was simply absent
#: at run time. The driver's source was conditional, so nothing refused; it ran on and
#: died at the call site with "command not found". D-3 halted there.
#:
#: Members are named EXPLICITLY rather than globbed: a glob would silently start carrying
#: whatever else appeared in deploy/, and "whatever is there" is not a manifest.
#:
#: It is built FROM the two names above rather than repeating them, because the first
#: version repeated them and left DRIVER_MEMBER assigned and never read -- a constant that
#: looked like it decided where the driver came from while the real decision was made by a
#: literal somewhere else. This campaign has been bitten by things that look authoritative
#: and are not; a dead constant beside a live duplicate of itself is one of them.
BOOTSTRAP_MEMBERS="$DRIVER_MEMBER
$GUARD_MEMBER"

#: Paths that belong to production. A bootstrap location may never be at or inside one.
PRODUCTION_PATHS="/home/odbadmin/python/woa23 /home/odbadmin/.pm2 /root/.pm2
/home/odbadmin/conf"

# ---------------------------------------------------------------- pure guards ---

#: Is <a> the same as, inside, or containing <b>? A TRUE path relation, not a string
#: prefix: `/a/bc` neither contains nor is inside `/a/b`.
paths_overlap() {   # <a> <b>
  local a="${1:-}" b="${2:-}"
  [ -n "$a" ] && [ -n "$b" ] || return 1
  a="${a%/}"; b="${b%/}"
  [ "$a" = "$b" ] && return 0
  case "$a" in "$b"/*) return 0 ;; esac
  case "$b" in "$a"/*) return 0 ;; esac
  return 1
}

#: Is <a> the same as, or INSIDE, <b>? Narrower than paths_overlap: it does NOT count
#: containment. Used for the bootstrap's ANCESTORS, which legitimately contain the
#: identity paths -- a bootstrap and a staging root are normally siblings under a common
#: parent, and treating that parent as an overlap refuses every sane layout. The first
#: version of this guard did exactly that.
path_is_within() {   # <a> <b>
  local a="${1:-}" b="${2:-}"
  [ -n "$a" ] && [ -n "$b" ] || return 1
  a="${a%/}"; b="${b%/}"
  [ "$a" = "$b" ] && return 0
  case "$a" in "$b"/*) return 0 ;; esac
  return 1
}

#: Why this ancestor path is unusable, or empty if it is fine. Same identity list as
#: bootstrap_path_problem, but judged with path_is_within.
ancestor_path_problem() {   # <path> <root> <workdir> <tmpdir> <pm2home> <store> <config>
  local a="${1:-}" p name
  [ -n "$a" ] || return 0
  shift
  for name in "staging root" "workdir" "TMPDIR" "PM2_HOME" "store" "generated config"; do
    p="${1:-}"; shift || true
    [ -n "$p" ] || continue
    if path_is_within "$a" "$p"; then
      printf 'the bootstrap resolves through the %s (%s)' "$name" "$p"
      return 0
    fi
  done
  for p in $PRODUCTION_PATHS; do
    if path_is_within "$a" "$p"; then
      printf 'the bootstrap resolves through a production path (%s)' "$p"
      return 0
    fi
  done
  return 0
}

#: Why this bootstrap location is unusable, or empty if it is fine.
#:
#: It must not be, contain, or sit inside ANY identity path. Containing one matters as
#: much as being inside one: a bootstrap that contains the staging root would make the
#: identity a child of scratch space, which is the same conflation by another route.
bootstrap_path_problem() {   # <bootstrap> <root> <workdir> <tmpdir> <pm2home> <store> <config>
  local b="${1:-}" p name
  [ -n "$b" ] || { printf 'bootstrap path is empty'; return 0; }
  case "$b" in
    /*) ;;
    *) printf 'bootstrap path must be absolute: %s' "$b"; return 0 ;;
  esac
  shift
  for name in "staging root" "workdir" "TMPDIR" "PM2_HOME" "store" "generated config"; do
    p="${1:-}"; shift || true
    [ -n "$p" ] || continue
    if paths_overlap "$b" "$p"; then
      printf 'bootstrap path overlaps the %s (%s): a bootstrap must be OUTSIDE every identity path' \
        "$name" "$p"
      return 0
    fi
  done
  for p in $PRODUCTION_PATHS; do
    if paths_overlap "$b" "$p"; then
      printf 'bootstrap path overlaps a production path (%s)' "$p"
      return 0
    fi
  done
  return 0
}

#: Is the archive member exactly one REGULAR FILE at the expected path?
#:
#: `tar -tvf` lines start with a mode string whose first character is the type: `-`
#: regular, `d` directory, `l` symlink, `h` hard link. An archive may legally contain
#: the same name twice, and a later member overwrites an earlier one on extraction --
#: so "a driver exists" is not enough. It must be UNIQUE and it must be a regular file.
#: A symlinked member would extract to a link pointing anywhere the archive chose.
archive_member_problem() {   # <tar -tvf listing> <member path>
  local listing="${1:-}" member="${2:-}" matches n first type
  [ -n "$member" ] || { printf 'no member path given'; return 0; }
  # THREE SHAPES, because tar does not print one. A symlink line ends with `-> target`,
  # NOT with the member name; a DIRECTORY is printed with a TRAILING SLASH. Matching only
  # on a trailing bare name misses both -- and it misses them by reporting "the archive
  # contains no member", which refuses for the wrong reason and describes the archive
  # incorrectly on the way out. A directory standing where the library should be is a
  # directory, and must be said to be one.
  matches="$(printf '%s\n' "$listing" \
    | awk -v m1=" $member\$" -v m2=" $member -> " -v m3=" $member/\$" \
          '$0 ~ m1 || $0 ~ m2 || $0 ~ m3')"
  n="$(printf '%s' "$matches" | grep -c . )"
  if [ "$n" -eq 0 ]; then
    printf 'the archive contains no member %s' "$member"; return 0
  fi
  if [ "$n" -gt 1 ]; then
    printf 'the archive contains %s members named %s; a duplicate member would let a later one overwrite the verified first' "$n" "$member"
    return 0
  fi
  first="$(printf '%s\n' "$matches" | head -1)"
  type="$(printf '%s' "$first" | cut -c1)"
  case "$type" in
    -) return 0 ;;
    d) printf 'the archive member %s is a DIRECTORY, not a regular file' "$member" ;;
    l) printf 'the archive member %s is a SYMLINK; it would extract to a link, not the driver' "$member" ;;
    h) printf 'the archive member %s is a HARD LINK, not a regular file' "$member" ;;
    *) printf 'the archive member %s has file type %s, which is not a regular file' "$member" "$type" ;;
  esac
  return 0
}

#: The manifest basenames that appear more than once, or empty if all are distinct.
#:
#: A MANIFEST MUST NOT DELIVER TWO FILES TO ONE NAME. Members are extracted to
#: `$BOOT/deploy/<basename>`, so `dev2026/deploy/x.sh` and `dev2026/scripts/x.sh` would
#: both be written to `$BOOT/deploy/x.sh` -- the second silently overwriting the first,
#: and the extraction hash check passing anyway because it re-reads what it just wrote.
#:
#: archive_member_problem CANNOT see this: both members are perfectly unique IN THE
#: ARCHIVE. The collision is created by the flattening, not by the archive, so it has to
#: be checked where the flattening is decided -- here.
manifest_basename_collision() {   # <members, one per line>
  printf '%s\n' "${1:-}" \
    | while IFS= read -r m; do [ -n "$m" ] && printf '%s\n' "${m##*/}"; done \
    | LC_ALL=C sort | uniq -d
}

#: Every path that must be checked for a bootstrap location: the path itself, its
#: PARENT, and -- where they exist -- their resolved realpaths. A symlinked parent is
#: how a bootstrap that looks external lands inside the staging root anyway.
bootstrap_paths_to_check() {   # <bootstrap>
  local b="${1:-}" par rb rp
  [ -n "$b" ] || return 0
  b="${b%/}"
  printf '%s\n' "$b"
  par="$(dirname "$b")"
  printf '%s\n' "$par"
  rb="$(readlink -f "$b" 2>/dev/null || true)"
  [ -n "$rb" ] && [ "$rb" != "$b" ] && printf '%s\n' "$rb"
  rp="$(readlink -f "$par" 2>/dev/null || true)"
  [ -n "$rp" ] && [ "$rp" != "$par" ] && printf '%s\n' "$rp"
  return 0
}

if [ "${WOA23_BOOTSTRAP_LIB_ONLY:-}" = 1 ]; then
  return 0 2>/dev/null || exit 0
fi

# ------------------------------------------------------------------ the bootstrap ---
die() { printf 'REFUSING: %s\n' "$1" >&2; shift; for l in "$@"; do printf '  %s\n' "$l" >&2; done; exit 2; }

ARCHIVE=""; BOOT=""; ROOT=""; WORKDIR=""; TMPD=""; PM2HOME=""; ARCHIVE_SHA=""
while [ $# -gt 0 ]; do
  case "$1" in
    --archive)   ARCHIVE="${2:-}"; shift 2 ;;
    --archive-sha256) ARCHIVE_SHA="${2:-}"; shift 2 ;;
    --bootstrap) BOOT="${2:-}"; shift 2 ;;
    --root)      ROOT="${2:-}"; shift 2 ;;
    --workdir)   WORKDIR="${2:-}"; shift 2 ;;
    --tmpdir)    TMPD="${2:-}"; shift 2 ;;
    --pm2-home)  PM2HOME="${2:-}"; shift 2 ;;
    --)          shift; break ;;
    *) die "unknown argument: $1" ;;
  esac
done

for pair in "--archive:$ARCHIVE" "--archive-sha256:$ARCHIVE_SHA" "--bootstrap:$BOOT" \
            "--root:$ROOT" "--pm2-home:$PM2HOME"; do
  [ -n "${pair#*:}" ] || die "${pair%%:*} is required"
done
[ -n "$WORKDIR" ] || WORKDIR="${ROOT}-work"
STORE="$ROOT/store"
CONFIG="$ROOT/dev2026/deploy"      # the directory the generated config lands in

echo "=== staging bootstrap — the driver's home is NOT the run's identity ==="
date -u "+timestamp UTC: %F %T"
echo "  bootstrap : $BOOT"
echo "  identity  : root=$ROOT"
echo "              workdir=$WORKDIR"
echo "              tmpdir=${TMPD:-<none>}"
echo "              pm2home=$PM2HOME"
echo "              store=$STORE"

echo
echo "== 0. the manifest must be deliverable at all =="
echo "   Checked before any path work: a manifest that cannot be delivered without one"
echo "   member overwriting another is broken wherever it is pointed."
[ -n "${BOOTSTRAP_MEMBERS//[[:space:]]/}" ] || die "the bootstrap manifest is empty"
mdup="$(manifest_basename_collision "$BOOTSTRAP_MEMBERS")"
[ -z "$mdup" ] || die "the manifest delivers two members to one name: $mdup" \
  "Members are flattened into \$BOOT/deploy/<basename>, so the second would silently" \
  "overwrite the first and the hash check would pass on what it had just written."
printf '%s\n' "$BOOTSTRAP_MEMBERS" | sed 's/^/  member: /'
echo "  all basenames distinct — no member can overwrite another"

echo
echo "== 1. the bootstrap path, its PARENT and their realpaths must all be outside =="
echo "   the identity. A symlinked parent is how an apparently-external bootstrap"
echo "   lands inside the staging root anyway."
problem="$(bootstrap_path_problem "$BOOT" "$ROOT" "$WORKDIR" "$TMPD" "$PM2HOME" "$STORE" "$CONFIG")"
[ -z "$problem" ] || die "$problem" \
  "  offending path: $BOOT" \
  "The bootstrap is scratch space for ONE file. The identity paths must not exist yet," \
  "and creating the driver's home must not create any of them." \
  "b35a1 failed exactly here: the bootstrap WAS the staging root."
echo "  ok (bootstrap itself, incl. containment): $BOOT"
# Ancestors and realpaths are judged with the NARROWER rule. A parent that CONTAINS the
# staging root is the normal sibling layout; a parent that is INSIDE one is the escape.
for cand in $(bootstrap_paths_to_check "$BOOT" | tail -n +2); do
  problem="$(ancestor_path_problem "$cand" "$ROOT" "$WORKDIR" "$TMPD" "$PM2HOME" "$STORE" "$CONFIG")"
  [ -z "$problem" ] || die "$problem" \
    "  offending path: $cand   (derived from --bootstrap $BOOT)" \
    "A symlinked parent is how an apparently-external bootstrap lands inside the identity."
  echo "  ok (ancestor/realpath, not inside any identity path): $cand"
done
rb="$(readlink -f "$BOOT" 2>/dev/null || true)"
if [ -n "$rb" ] && [ "$rb" != "${BOOT%/}" ]; then
  problem="$(bootstrap_path_problem "$rb" "$ROOT" "$WORKDIR" "$TMPD" "$PM2HOME" "$STORE" "$CONFIG")"
  [ -z "$problem" ] || die "$problem" "  the bootstrap RESOLVES to: $rb"
  echo "  ok (bootstrap realpath): $rb"
fi

echo
echo "== 1b. the bootstrap path must NOT already exist =="
[ -e "$BOOT" ] && die "the bootstrap path already exists: $BOOT" \
  "A pre-existing bootstrap may hold a driver from another archive, or from a" \
  "previous attempt. It is NOT emptied or reused. Choose a fresh bootstrap path." \
  || echo "  absent (ok): $BOOT"
[ -L "$BOOT" ] && die "the bootstrap path is a SYMLINK: $BOOT" || true
par="$(dirname "$BOOT")"
[ -L "$par" ] && die "the bootstrap's PARENT is a SYMLINK: $par" \
  "Its target could place the bootstrap anywhere, including inside the identity." \
  || echo "  parent is not a symlink (ok): $par"
[ -d "$par" ] || die "the bootstrap's parent is not a directory: $par"

echo
echo "== 2. the identity must still be absent — the guard is NOT relaxed =="
for p in "$ROOT" "$WORKDIR" "$TMPD" "$PM2HOME" "$STORE"; do
  [ -n "$p" ] || continue
  [ -e "$p" ] && die "$p already exists" \
    "Every element of this run's identity must be absent before staging begins." \
    "It is NOT deleted, emptied or reused. Choose a new identity." \
    || echo "  absent (ok): $p"
done

echo
echo "== 3. the archive: identified by digest, so an old one at the same path is refused =="
[ -r "$ARCHIVE" ] || die "archive not readable: $ARCHIVE"
[ -f "$ARCHIVE" ] || die "archive is not a regular file: $ARCHIVE"
[ -L "$ARCHIVE" ] && die "archive path is a SYMLINK: $ARCHIVE" || true
for p in "$ROOT" "$WORKDIR" "$TMPD" "$PM2HOME" "$STORE" "$BOOT"; do
  [ -n "$p" ] || continue
  paths_overlap "$ARCHIVE" "$p" && die "the archive sits inside an identity or bootstrap path ($p)" \
    "The transfer location must be outside both, so a rerun cannot inherit it."
done
GOT_A="$(sha256sum "$ARCHIVE" | cut -d' ' -f1)"
echo "  archive          : $ARCHIVE"
echo "  sha256 on disk   : $GOT_A"
echo "  sha256 expected  : $ARCHIVE_SHA"
[ "$GOT_A" = "$ARCHIVE_SHA" ] || die "the archive is not the one this run was authorised for" \
  "  on disk  : $GOT_A" "  expected : $ARCHIVE_SHA" \
  "A stale archive left at the transfer path is exactly what this check exists to catch."
echo "  MATCH — this is the authorised archive, not a leftover"

echo
echo "== 3b. EVERY delivered member must be UNIQUE and a REGULAR FILE =="
LISTING="$(tar -tvf "$ARCHIVE" 2>/dev/null)"
[ -n "$LISTING" ] || die "cannot list the archive: $ARCHIVE"
printf '%s\n' "$BOOTSTRAP_MEMBERS" | while IFS= read -r m; do
  [ -n "$m" ] || continue
  p="$(archive_member_problem "$LISTING" "$m")"
  [ -z "$p" ] || { printf 'REFUSING: %s\n' "$p" >&2; exit 2; }
done || die "an archive member is missing, duplicated, a symlink or not a regular file" \
  "Every delivered file must come from the archive under test, and must be the one," \
  "plain file the archive claims it is."
printf '%s\n' "$BOOTSTRAP_MEMBERS" | sed 's/^/  ok — unique regular file: /'

echo
echo "== 3c. each member, taken from the archive and re-verified after extraction =="
mkdir -p "$BOOT/deploy" || die "cannot create the bootstrap directory: $BOOT"
printf '%s\n' "$BOOTSTRAP_MEMBERS" | while IFS= read -r m; do
  [ -n "$m" ] || continue
  base="${m##*/}"
  # DEFENCE IN DEPTH behind the manifest check in step 0: nothing may be written over.
  # $BOOT did not exist a moment ago, so anything already at this path was put there by an
  # earlier iteration of this very loop -- which is the collision step 0 refuses.
  if [ -e "$BOOT/deploy/$base" ] || [ -L "$BOOT/deploy/$base" ]; then
    printf 'REFUSING: %s would be overwritten in the bootstrap\n' "$base" >&2; exit 2; fi
  want="$(tar -xOf "$ARCHIVE" "$m" 2>/dev/null | sha256sum | cut -d' ' -f1)"
  [ -n "$want" ] || { printf 'REFUSING: cannot read %s from the archive\n' "$m" >&2; exit 2; }
  tar -xOf "$ARCHIVE" "$m" > "$BOOT/deploy/$base" \
    || { printf 'REFUSING: cannot extract %s into the bootstrap\n' "$m" >&2; exit 2; }
  chmod 700 "$BOOT/deploy/$base"
  if [ -L "$BOOT/deploy/$base" ]; then
    printf 'REFUSING: the extracted %s is a SYMLINK\n' "$base" >&2; exit 2; fi
  if [ ! -f "$BOOT/deploy/$base" ]; then
    printf 'REFUSING: the extracted %s is not a regular file\n' "$base" >&2; exit 2; fi
  got="$(sha256sum "$BOOT/deploy/$base" | cut -d' ' -f1)"
  if [ "$got" != "$want" ]; then
    printf 'REFUSING: extracted %s does not match the archive\n  in archive: %s\n  on disk   : %s\n' \
      "$base" "$want" "$got" >&2; exit 2; fi
  printf '  %s\n    in archive %s\n    on disk    %s   MATCH\n' "$m" "$want" "$got"
done || die "a delivered member failed extraction or verification" \
  "A file that does not match the archive it came from is never run."

# THE MEMBERS MUST ACTUALLY BE THERE. The loop above runs in a subshell per `while`, so its
# success is re-established here against the filesystem rather than assumed from an exit code.
DRIVER_LOCAL="$BOOT/deploy/${DRIVER_MEMBER##*/}"
GUARD_LOCAL="$BOOT/deploy/${GUARD_MEMBER##*/}"
[ -f "$DRIVER_LOCAL" ] || die "the driver was not delivered to $BOOT/deploy"
[ -f "$GUARD_LOCAL" ]  || die "the store guard library was not delivered to $BOOT/deploy" \
  "  The driver refuses to run without it, and it is refused here rather than there."

# AND IT MUST STILL BE THE ARCHIVE'S OWN. "The file is there" and "the file is the right
# one" are different claims, and only the first was re-established outside the subshell.
# The digests are re-taken here, in the parent, so the whole delivery holds without
# trusting a subshell's exit status for anything.
for _pair in "$DRIVER_MEMBER:$DRIVER_LOCAL" "$GUARD_MEMBER:$GUARD_LOCAL"; do
  _m="${_pair%%:*}"; _f="${_pair#*:}"
  [ -L "$_f" ] && die "the delivered $_m is a SYMLINK: $_f"
  _w="$(tar -xOf "$ARCHIVE" "$_m" 2>/dev/null | sha256sum | cut -d' ' -f1)"
  _g="$(sha256sum "$_f" | cut -d' ' -f1)"
  [ -n "$_w" ] || die "cannot re-read $_m from the archive to confirm the delivery"
  [ "$_w" = "$_g" ] || die "the delivered $_m does not match the archive" \
    "  in archive: $_w" "  on disk   : $_g" \
    "A file that does not match the archive it came from is never run."
done
echo "  MATCH — every member is the archive's own, verified after extraction"

echo
echo "== 4. handing over to the driver, from the bootstrap =="
echo "  $DRIVER_LOCAL --root $ROOT --archive $ARCHIVE $*"
echo
exec "$DRIVER_LOCAL" --root "$ROOT" --archive "$ARCHIVE" "$@"
