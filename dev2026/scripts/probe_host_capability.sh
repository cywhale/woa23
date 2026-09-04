#!/usr/bin/env bash
#
# Read-only host capability probe for the validation account. Creates NOTHING.
#
# WHY THIS EXISTS. `pm2G`, `pm2B` and `pm2F` all ran as **odbadmin** — every path under
# /home/odbadmin/woa23-pm2g/. Running the same staging harness as **woa23c1ro** (uid 994)
# is a new, unvalidated configuration, and it has two prerequisites nothing offline can
# check: `node` (staging_execute.sh:367) and `pm2` (:387) being reachable by uid 994.
# Both may live under odbadmin's home or an nvm tree uid 994 cannot read.
#
# The campaign's repeated cost has been runs that aborted AFTER creating state. This
# answers the unknown before any identity, tree, PM2_HOME or port is spent.
#
# WHY IT NEVER EXECUTES `pm2`. A pm2 subcommand — including, on some builds, `pm2 -v` —
# CONNECTS TO OR SPAWNS THE PM2 DAEMON, creating $PM2_HOME as a side effect. That is
# forbidden here, so pm2's version is read from its package's own `package.json` as TEXT.
# The binary is resolved and stat'ed; it is never run. `node --version` IS run: node is
# not a daemon and starting it creates nothing.
#
# WHAT IT WILL NOT DO, by construction — there is no code path for any of these:
#   pm2 start / stop / delete / kill / save / resurrect / jlist / list / ping
#   creating a PM2 daemon or any PM2_HOME
#   creating a staging tree, workdir or store
#   binding a port
#   contacting the production API
#   modifying production PM2, files, ACLs or permissions
#
# Every filesystem call below is `stat`, `test`, `readlink`, `ls` or a read of a regular
# file. There is no mkdir, no touch, no redirect into a file, no chmod, no chown.
#
#   ./scripts/probe_host_capability.sh
#
# Exit 0 = every capability present and every guard satisfied. Non-zero = a specific
# refusal, named. Sourcing with WOA23_PROBE_LIB_ONLY=1 defines the pure guards and stops.

set -uo pipefail

#: Any PM2_HOME at or under one of these is production's and is refused outright.
PRODUCTION_PM2_PATHS="/home/odbadmin/.pm2 /home/odbadmin/.pm2/ /root/.pm2"

# ---------------------------------------------------------------- pure guards ---
# Pure so the offline suite can exercise them without a host. Defined before anything
# that touches the filesystem, so sourcing for the library is inert.

#: Is this path production's PM2 home, or inside it? Exact match or a path prefix --
#: `/home/odbadmin/.pm2anything` is NOT under it and must not false-positive, while
#: `/home/odbadmin/.pm2/pids` must.
is_production_pm2_home() {   # <path>
  local p="${1:-}" prod
  [ -n "$p" ] || return 1
  p="${p%/}"
  for prod in $PRODUCTION_PM2_PATHS; do
    prod="${prod%/}"
    [ "$p" = "$prod" ] && return 0
    case "$p" in "$prod"/*) return 0 ;; esac
  done
  return 1
}

#: A second, broader net: anything under another account's home is not ours to use.
#: Refuses even a PM2 path production does not currently use -- the point is that this
#: account writes only under its own home.
is_foreign_home_path() {   # <path> <my_home>
  local p="${1:-}" home="${2:-}"
  [ -n "$p" ] || return 1
  case "$p" in
    /home/*) ;;
    *) return 1 ;;
  esac
  [ -n "$home" ] || return 0
  home="${home%/}"
  [ "$p" = "$home" ] && return 1
  case "$p" in "$home"/*) return 1 ;; esac
  return 0
}

#: What pm2 WOULD use with no PM2_HOME set. pm2's own default is $HOME/.pm2.
effective_pm2_home() {   # <PM2_HOME value or empty> <HOME>
  local set_val="${1:-}" home="${2:-}"
  if [ -n "$set_val" ]; then printf '%s' "$set_val"; else printf '%s/.pm2' "${home%/}"; fi
}

if [ "${WOA23_PROBE_LIB_ONLY:-}" = 1 ]; then
  return 0 2>/dev/null || exit 0
fi

# ------------------------------------------------------------------- the probe ---
fail=0
note() { printf '  %s\n' "$*"; }
bad()  { printf '  REFUSE: %s\n' "$*" >&2; fail=1; }

echo "=========== read-only host capability probe (creates nothing) ==========="
date -u "+timestamp UTC: %F %T"
echo "host: $(hostname)"

echo
echo "== 1. identity =="
note "id      : $(id)"
note "uid     : $(id -u)   gid: $(id -g)"
note "user    : $(id -un)"
note "HOME    : ${HOME:-<unset>}"
[ "$(id -u)" = "994" ] || bad "expected uid 994, got $(id -u)"
[ "$(id -un)" != "odbadmin" ] || bad "running as odbadmin; this probe is for the validation account"
[ "${HOME:-}" = "/home/woa23c1ro" ] || bad "unexpected HOME: ${HOME:-<unset>}"

echo
echo "== 2. PATH, as the account resolves it =="
note "PATH    : ${PATH:-<unset>}"
# `printf '%s'` here dropped the LAST PATH entry: with no trailing newline the final
# field is unterminated, `read` returns non-zero on it, and the loop body never runs.
# probeA silently omitted /snap/bin because of this. `%s\n' terminates it.
printf '%s\n' "${PATH:-}" | tr ':' '\n' | while IFS= read -r d; do
  [ -n "$d" ] || continue
  if [ -d "$d" ]; then printf '    dir  %s\n' "$d"
  else printf '    ABSENT %s\n' "$d"; fi
done

echo
echo "== 3. node — resolved, stat'ed, and RUN (node starts no daemon) =="
NODE="$(command -v node 2>/dev/null || true)"
if [ -z "$NODE" ]; then
  bad "node is NOT on PATH for this account. staging_execute.sh:367 requires it."
else
  NODE_REAL="$(readlink -f "$NODE" 2>/dev/null || printf '%s' "$NODE")"
  note "resolved : $NODE"
  note "realpath : $NODE_REAL"
  note "stat     : $(stat -c '%U:%G mode=%a size=%s' "$NODE_REAL" 2>/dev/null || echo '<unreadable>')"
  if [ -x "$NODE_REAL" ]; then
    note "version  : $("$NODE" --version 2>&1 | head -1)"
  else
    bad "node resolves to $NODE_REAL but is not executable by this account"
  fi
  case "$NODE_REAL" in
    /home/odbadmin/*) note "NOTE     : node lives under odbadmin's home; readable here, but that is a shared dependency" ;;
  esac
fi

echo
echo "== 4. pm2 — resolved and stat'ed, NEVER EXECUTED =="
echo "   (a pm2 subcommand can spawn the daemon and create PM2_HOME; that is forbidden"
echo "    here, so the version is read from package.json as text)"
PM2="$(command -v pm2 2>/dev/null || true)"
if [ -z "$PM2" ]; then
  bad "pm2 is NOT on PATH for this account. staging_execute.sh:387 requires it."
else
  PM2_REAL="$(readlink -f "$PM2" 2>/dev/null || printf '%s' "$PM2")"
  note "resolved : $PM2"
  note "realpath : $PM2_REAL"
  note "stat     : $(stat -c '%U:%G mode=%a size=%s' "$PM2_REAL" 2>/dev/null || echo '<unreadable>')"
  [ -x "$PM2_REAL" ] || bad "pm2 resolves to $PM2_REAL but is not executable by this account"
  # package.json sits beside the bin script in a node package: <pkg>/bin/pm2 -> <pkg>/package.json
  PKG_DIR="$(dirname "$(dirname "$PM2_REAL")")"
  PKG_JSON="$PKG_DIR/package.json"
  if [ -r "$PKG_JSON" ]; then
    note "package  : $PKG_JSON"
    note "version  : $(tr ',' '\n' < "$PKG_JSON" | grep -m1 '"version"' | tr -d ' "' | cut -d: -f2)"
  else
    note "package  : $PKG_JSON NOT readable — version not determined (pm2 still not executed)"
  fi
  case "$PM2_REAL" in
    /home/odbadmin/*) note "NOTE     : pm2 lives under odbadmin's home; readable here, but shared" ;;
  esac
fi

echo
echo "== 5. PM2_HOME — no fallback to production's, proven =="
SET_PM2_HOME="${PM2_HOME:-}"
note "PM2_HOME in environment : ${SET_PM2_HOME:-<unset>}"
EFFECTIVE="$(effective_pm2_home "$SET_PM2_HOME" "${HOME:-}")"
note "effective (what pm2 would use) : $EFFECTIVE"

if is_production_pm2_home "$EFFECTIVE"; then
  bad "the effective PM2_HOME IS a production PM2 path: $EFFECTIVE"
else
  note "not a production PM2 path (checked against: $PRODUCTION_PM2_PATHS)"
fi
if is_foreign_home_path "$EFFECTIVE" "${HOME:-}"; then
  bad "the effective PM2_HOME is under another account's home: $EFFECTIVE"
else
  note "under this account's own home, or not under /home at all"
fi

for prod in $PRODUCTION_PM2_PATHS; do
  if [ -e "$prod" ]; then
    r=no; w=no
    [ -r "$prod" ] && r=yes
    [ -w "$prod" ] && w=yes
    note "production path $prod : exists, readable=$r writable=$w"
    [ "$w" = no ] || bad "this account can WRITE $prod"
  else
    note "production path $prod : not present or not visible to this account"
  fi
done
note "this probe sets no PM2_HOME, runs no pm2, and creates no PM2 directory"

echo
echo "== 6. would-be staging paths — reported, NOT created =="
for p in /home/woa23c1ro/woa23-b35a1 /home/woa23c1ro/woa23-b35a1-work \
         /home/woa23c1ro/woa23-b35a1-pm2 /home/woa23c1ro/tmp-b35a1; do
  if [ -e "$p" ]; then note "PRESENT (would block a run): $p"; fail=1
  else note "absent (ok, and NOT created): $p"; fi
done
if [ -w "${HOME:-/nonexistent}" ]; then
  note "HOME is writable by this account (so a staging root could later be created there)"
else
  bad "HOME is not writable: a staging run could not create its tree"
fi

echo
echo "== 7. uv, already known-good — confirmed, not assumed =="
UV="$(command -v uv 2>/dev/null || true)"
if [ -n "$UV" ]; then
  note "resolved : $UV"
  note "version  : $("$UV" --version 2>&1 | head -1)"
  note "sha256   : $(sha256sum "$UV" 2>/dev/null | cut -d' ' -f1)"
else
  bad "uv is not on PATH for this account"
fi

echo
echo "== 8. host state observed read-only (nothing contacted) =="
note "boot id  : $(cat /proc/sys/kernel/random/boot_id 2>/dev/null || echo '<unreadable>')"
note "8050 listening (ss -ltn, presence only, never -p): $(ss -ltn 2>/dev/null | grep -c ':8050 ')"
note "18265 (pm2G) listening: $(ss -ltn 2>/dev/null | grep -c ':18265 ')"
for pid in 4296 5040 5041; do
  if [ -r "/proc/$pid/stat" ]; then
    note "production pid=$pid starttime=$(awk '{print $22}' "/proc/$pid/stat")"
  else
    note "production pid=$pid not present"
  fi
done
for pid in 1456369 1456373 1456374; do
  [ -r "/proc/$pid/stat" ] && note "pm2G pid=$pid RUNNING (observed, untouched)" \
                           || note "pm2G pid=$pid not present"
done

echo
if [ "$fail" -ne 0 ]; then
  echo "PROBE RESULT: REFUSED — see the REFUSE lines above. Nothing was created."
  exit 3
fi
echo "PROBE RESULT: ALL CAPABILITIES PRESENT AND ALL GUARDS SATISFIED."
echo "Nothing was created, no pm2 was executed, no port was bound, no request was sent."
exit 0
