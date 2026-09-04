#!/usr/bin/env bash
#
# Read-only PM2 DISCOVERY probe for uid 994. Creates NOTHING. Executes no pm2.
#
# probeA established that `pm2` is not on the validation account's default PATH. That is
# NOT the same as "the host has no pm2", and this probe exists to tell the difference.
# It distinguishes four outcomes, which are four different decisions:
#
#   A  ABSENT              no pm2 found under any searched root
#   B  NOT_ON_PATH         pm2 exists and uid 994 can execute it, but PATH omits it
#   C  NOT_ACCESSIBLE      pm2 exists but uid 994 cannot traverse to it, read it, or
#                          execute it
#   D  USABLE_FOR_STAGING  a candidate is readable and executable by uid 994 and lies
#                          outside production-owned state -- **and no startup has been
#                          attempted**, so this is a capability finding, not a B3/B5 result
#
# IT NEVER EXECUTES pm2. Not `pm2`, not `pm2 --version`, not `pm2 -v`, not `pm2 jlist`,
# not any subcommand. A pm2 invocation can connect to or SPAWN the daemon and create
# $PM2_HOME as a side effect. Versions come from `package.json` read as TEXT.
# `node --version` IS run: node is not a daemon and starting it creates nothing.
#
# SEARCH IS BOUNDED. Every root has an explicit -maxdepth and -xdev, and each root's
# traversability is RECORDED -- "not found here" and "could not look here" are different
# findings, and collapsing them is how outcome A gets reported when the truth is C.
#
# There is no code path for: creating or modifying PM2_HOME; start/stop/delete/kill/
# save/resurrect; creating a staging tree, workdir, store or any temp file; binding a
# port; sudo/su/setpriv; modifying PATH, ACLs, permissions, production PM2 or production
# files. Every filesystem call is find/stat/readlink/test or a read of a regular file.
#
#   ssh ... 'bash -s' < scripts/probe_pm2_discovery.sh
#
# Sourcing with WOA23_PM2DISC_LIB_ONLY=1 defines the pure helpers and stops.

set -uo pipefail

#: Paths that are production-owned state. A candidate at or under one of these is
#: reported as such and is NOT proposed for staging use, whatever its permissions.
PRODUCTION_STATE_PATHS="/home/odbadmin/.pm2 /root/.pm2 /home/odbadmin/python/woa23"

# ---------------------------------------------------------------- pure helpers ---

#: Is <path> at or under <root>? Exact match or a true path prefix -- `/a/bc` is NOT
#: under `/a/b`, which a naive prefix test would get wrong.
path_is_under() {   # <path> <root>
  local p="${1:-}" r="${2:-}"
  [ -n "$p" ] && [ -n "$r" ] || return 1
  p="${p%/}"; r="${r%/}"
  [ "$p" = "$r" ] && return 0
  case "$p" in "$r"/*) return 0 ;; esac
  return 1
}

#: Which production state path contains <path>, or empty.
production_state_owner() {   # <path>
  local p="${1:-}" r
  for r in $PRODUCTION_STATE_PATHS; do
    if path_is_under "$p" "$r"; then printf '%s' "$r"; return 0; fi
  done
  return 1
}

#: The four-way classification, from booleans a caller has already established.
#: found/executable/accessible are "yes"/"no"; on_path is "yes"/"no".
classify_pm2() {   # <found> <accessible> <executable> <on_path>
  local found="${1:-no}" acc="${2:-no}" exec="${3:-no}" onpath="${4:-no}"
  if [ "$found" != yes ]; then printf 'A_ABSENT'; return 0; fi
  if [ "$acc" != yes ] || [ "$exec" != yes ]; then printf 'C_NOT_ACCESSIBLE'; return 0; fi
  if [ "$onpath" = yes ]; then printf 'D_USABLE_FOR_STAGING'; return 0; fi
  printf 'B_NOT_ON_PATH'
}

#: What a `find -name pm2` hit actually IS, after symlinks are resolved.
#:
#: probeB reported a DIRECTORY as an executable candidate, because on a directory the
#: `x` bit means traversable, not runnable. A directory named pm2 is the PACKAGE, not a
#: binary, and must never be offered as something b35a1 could run. Only a REGULAR FILE
#: that is executable is an executable candidate.
candidate_kind() {   # <type: f|d|l|other> <is_regular_file yes/no> <is_executable yes/no>
  local t="${1:-other}" reg="${2:-no}" ex="${3:-no}"
  if [ "$t" = d ]; then printf 'DIRECTORY'; return 0; fi
  if [ "$reg" != yes ]; then printf 'NOT_A_REGULAR_FILE'; return 0; fi
  if [ "$ex" != yes ]; then printf 'NOT_EXECUTABLE'; return 0; fi
  printf 'EXECUTABLE'
}

#: The ancestor directories to look in for the package that owns <file>, nearest first.
#:
#: probeB derived the package as dirname(dirname(path)), which is right for
#: `<pkg>/bin/pm2` and WRONG for `<pkg>/pm2` -- it produced `.../lib` and
#: `.../node_modules`, then reported "package.json NOT readable" about them, which read
#: like a finding about the host and was an artefact of the derivation. Walking up and
#: taking the nearest ancestor that actually HAS a package.json removes the guess.
package_search_dirs() {   # <file path> [max levels, default 4]
  local f="${1:-}" max="${2:-4}" d i
  [ -n "$f" ] || return 0
  d="$(dirname "$f")"
  i=0
  while [ "$i" -lt "$max" ] && [ "$d" != / ] && [ -n "$d" ]; do
    printf '%s\n' "$d"
    d="$(dirname "$d")"
    i=$((i + 1))
  done
}

if [ "${WOA23_PM2DISC_LIB_ONLY:-}" = 1 ]; then
  return 0 2>/dev/null || exit 0
fi

# -------------------------------------------------------------------- the probe ---
note() { printf '  %s\n' "$*"; }

echo "======== read-only PM2 discovery probe (creates nothing, runs no pm2) ========"
date -u "+timestamp UTC: %F %T"
echo "host: $(hostname)"

echo
echo "== 1. identity =="
note "id   : $(id)"
note "uid  : $(id -u)  gid: $(id -g)  user: $(id -un)"
note "HOME : ${HOME:-<unset>}"

echo
echo "== 2. FULL PATH, every entry including the last =="
note "PATH : ${PATH:-<unset>}"
# printf '%s\n' -- NOT '%s'. Without the newline the final field is unterminated, `read`
# returns non-zero on it, and the last entry is silently dropped. probeA omitted
# /snap/bin exactly that way.
n=0
printf '%s\n' "${PATH:-}" | tr ':' '\n' | while IFS= read -r d; do
  [ -n "$d" ] || continue
  n=$((n + 1))
  if [ -d "$d" ]; then
    printf '    %2d dir     %s  (pm2 here: %s)\n' "$n" "$d" \
      "$([ -e "$d/pm2" ] && echo YES || echo no)"
  else
    printf '    %2d ABSENT  %s\n' "$n" "$d"
  fi
done
note "entries in PATH: $(printf '%s\n' "${PATH:-}" | tr ':' '\n' | grep -c . )"

echo
echo "== 3. node — the interpreter any pm2 would run under =="
NODE="$(command -v node 2>/dev/null || true)"
if [ -n "$NODE" ]; then
  NODE_REAL="$(readlink -f "$NODE" 2>/dev/null || printf '%s' "$NODE")"
  note "resolved : $NODE"
  note "realpath : $NODE_REAL"
  note "stat     : $(stat -c '%U:%G mode=%a size=%s' "$NODE_REAL" 2>/dev/null || echo '<unreadable>')"
  note "version  : $("$NODE" --version 2>&1 | head -1)"
  note "readable : $([ -r "$NODE_REAL" ] && echo yes || echo no)   executable: $([ -x "$NODE_REAL" ] && echo yes || echo no)"
  NODE_PREFIX="$(dirname "$(dirname "$NODE_REAL")")"
  note "prefix   : $NODE_PREFIX  (its lib/node_modules is searched below)"
else
  note "node NOT on PATH"
  NODE_REAL=""; NODE_PREFIX=""
fi

echo
echo "== 4. bounded search roots — traversability recorded per root =="
echo "   'not found here' and 'could not look here' are different findings."
ROOTS="/usr/local/bin:1 /usr/bin:1 /bin:1 /sbin:1 /usr/local/sbin:1 /snap/bin:1
/usr/local/lib/node_modules:3 /usr/lib/node_modules:3 /lib/node_modules:3
/opt:4 /usr/local/n:5
${HOME}/.local:4 ${HOME}/.npm-global:4 ${HOME}/node_modules:3 ${HOME}/.nvm:5
/home/odbadmin/.nvm:5 /home/odbadmin/.npm-global:4 /home/odbadmin/.local:4
/home/odbadmin/node_modules:3 /home/odbadmin/.config/yarn:5"
[ -n "$NODE_PREFIX" ] && ROOTS="$ROOTS ${NODE_PREFIX}/lib/node_modules:3"
# Test seam only, never set in real use: lets the offline suite point the search at a
# fixture tree so the classification fix is proven end to end rather than by inspection.
[ -n "${WOA23_PM2DISC_ROOTS:-}" ] && ROOTS="$WOA23_PM2DISC_ROOTS"

CANDIDATES=""
UNTRAVERSABLE=""
for spec in $ROOTS; do
  root="${spec%:*}"; depth="${spec##*:}"
  if [ ! -e "$root" ]; then
    printf '    %-42s ABSENT\n' "$root"
    continue
  fi
  if [ ! -x "$root" ] || [ ! -r "$root" ]; then
    printf '    %-42s NOT TRAVERSABLE by uid %s (r=%s x=%s owner=%s)\n' "$root" "$(id -u)" \
      "$([ -r "$root" ] && echo y || echo n)" "$([ -x "$root" ] && echo y || echo n)" \
      "$(stat -c '%U:%G mode=%a' "$root" 2>/dev/null || echo '?')"
    UNTRAVERSABLE="$UNTRAVERSABLE $root"
    continue
  fi
  hits="$(find "$root" -maxdepth "$depth" -xdev -name pm2 2>/dev/null | head -20)"
  cnt="$(printf '%s' "$hits" | grep -c . )"
  printf '    %-42s searched (maxdepth %s): %s hit(s)\n' "$root" "$depth" "$cnt"
  [ "$cnt" -gt 0 ] && CANDIDATES="$CANDIDATES $hits"
done

echo
echo "== 5. candidates, one report each — pm2 is NEVER executed =="
echo "   A hit is an EXECUTABLE CANDIDATE only if, after resolving symlinks, it is a"
echo "   REGULAR FILE that is executable. A directory named pm2 is the package, not a"
echo "   binary, and is reported as such. The package is derived ONLY for executables."
EXEC_CANDIDATES=""
if [ -z "$(printf '%s' "$CANDIDATES" | tr -d ' \n')" ]; then
  note "NO pm2 hit under any traversable root."
else
  for c in $(printf '%s\n' $CANDIDATES | sort -u); do
    echo
    real="$(readlink -f "$c" 2>/dev/null || printf '%s' "$c")"
    # Classify FIRST, from the resolved target, and report accordingly.
    t=other
    [ -d "$real" ] && t=d
    [ -f "$real" ] && t=f
    reg=no; [ -f "$real" ] && reg=yes
    ex=no;  [ -x "$real" ] && ex=yes
    kind="$(candidate_kind "$t" "$reg" "$ex")"

    if [ "$kind" != EXECUTABLE ]; then
      echo "  --- hit (NOT an executable candidate): $c"
      note "kind      : $kind"
      note "realpath  : $real"
      note "stat      : $(stat -c '%U:%G mode=%a size=%s' "$real" 2>/dev/null || echo '<unreadable>')"
      if [ "$kind" = DIRECTORY ]; then
        note "NOTE      : this is a DIRECTORY. Its x bit means TRAVERSABLE, not runnable."
        note "            No package.json is derived for a directory hit, and none is"
        note "            reported missing -- probeB emitted exactly such a misleading line."
      else
        note "NOTE      : not a regular executable file; no package derived."
      fi
      continue
    fi

    echo "  --- EXECUTABLE CANDIDATE: $c"
    note "kind      : EXECUTABLE (regular file, executable after symlink resolution)"
    note "absolute  : $c"
    note "realpath  : $real"
    note "type      : $([ -L "$c" ] && echo 'symlink -> '"$(readlink "$c" 2>/dev/null)" || echo 'regular file')"
    note "stat      : $(stat -c '%U:%G mode=%a size=%s' "$real" 2>/dev/null || echo '<unreadable>')"
    note "binary      r/w/x for uid $(id -u): $([ -r "$real" ] && echo r || echo -)$([ -w "$real" ] && echo w || echo -)$([ -x "$real" ] && echo x || echo -)"
    pdir="$(dirname "$real")"
    note "parent dir  $pdir"
    note "parent      r/w/x for uid $(id -u): $([ -r "$pdir" ] && echo r || echo -)$([ -w "$pdir" ] && echo w || echo -)$([ -x "$pdir" ] && echo x || echo -)"
    note "writable (binary)     : $([ -w "$real" ] && echo 'YES — this account could modify it' || echo 'no — cannot be modified by this account')"
    note "writable (parent dir) : $([ -w "$pdir" ] && echo 'YES — could be replaced' || echo 'no — cannot be replaced by this account')"
    note "sha256    : $(sha256sum "$real" 2>/dev/null | cut -d' ' -f1 || echo '<unreadable>')"

    # Package derivation, ONLY for an executable: the nearest ancestor that actually has
    # a package.json. No dirname(dirname) guess, so no invented "NOT readable" line.
    pkg=""; pj=""
    for d in $(package_search_dirs "$real" 4); do
      if [ -r "$d/package.json" ]; then pkg="$d"; pj="$d/package.json"; break; fi
    done
    if [ -n "$pj" ]; then
      note "package     $pkg  (nearest ancestor WITH a package.json)"
      note "package     r/w/x for uid $(id -u): $([ -r "$pkg" ] && echo r || echo -)$([ -w "$pkg" ] && echo w || echo -)$([ -x "$pkg" ] && echo x || echo -)"
      note "pkg stat  : $(stat -c '%U:%G mode=%a' "$pkg" 2>/dev/null || echo '<unreadable>')"
      note "pkg writable by this account: $([ -w "$pkg" ] && echo YES || echo no)"
      note "package.json : $pj"
      note "name      : $(tr ',' '\n' < "$pj" | grep -m1 '"name"' | tr -d ' "' | cut -d: -f2)"
      note "VERSION   : $(tr ',' '\n' < "$pj" | grep -m1 '"version"' | tr -d ' "' | cut -d: -f2)"
      note "pj writable: $([ -w "$pj" ] && echo YES || echo no)"
    else
      note "package   : no ancestor within 4 levels has a READABLE package.json"
      note "            (stated as a search outcome, not as a missing file)"
    fi

    owner="$(production_state_owner "$real" || true)"
    if [ -n "$owner" ]; then
      note "PRODUCTION STATE: under $owner — NOT proposed for staging"
    else
      note "production state: not under any known production path"
    fi
    onpath=no
    case ":$PATH:" in *":$(dirname "$c"):"*) onpath=yes ;; esac
    note "on this account's PATH: $onpath"
    acc=no; [ -r "$real" ] && acc=yes
    note "CLASSIFICATION: $(classify_pm2 yes "$acc" "$ex" "$onpath")"
    EXEC_CANDIDATES="$EXEC_CANDIDATES $c"
  done
  [ -z "$(printf '%s' "$EXEC_CANDIDATES" | tr -d ' \n')" ] && {
    echo; note "NO hit resolved to an executable regular file."; }
fi

echo "== 5b. SEARCH COMPLETENESS =="
if [ -z "$(printf '%s' "$UNTRAVERSABLE" | tr -d ' \n')" ]; then
  note "every listed root was either searched or absent — search COMPLETE over the"
  note "declared root set (which is bounded by design, not exhaustive over the host)"
else
  note "SEARCH IS INCOMPLETE. These roots could NOT be traversed by uid $(id -u) and"
  note "were NOT searched. Any pm2 inside them is invisible to this probe:"
  for r in $UNTRAVERSABLE; do
    note "    $r   $(stat -c '%U:%G mode=%a' "$r" 2>/dev/null || echo '?')"
  done
  note "This incompleteness is recorded for EVERY outcome below, not only for A_ABSENT."
  note "A result of A_ABSENT here would mean 'not found in what could be searched',"
  note "which is NOT the same as 'absent from the host', and must not be read as such."
fi

echo
echo "== 6. overall classification =="
if [ -z "$(printf '%s' "$EXEC_CANDIDATES" | tr -d ' \n')" ]; then
  echo "  A_ABSENT — no EXECUTABLE pm2 found under any root this account can traverse."
  echo "  NOTE: roots marked NOT TRAVERSABLE above were NOT searched. Absent-from-what-"
  echo "        we-could-see is not the same as absent-from-the-host."
else
  best=A_ABSENT
  for c in $(printf '%s\n' $EXEC_CANDIDATES | sort -u); do
    real="$(readlink -f "$c" 2>/dev/null || printf '%s' "$c")"
    production_state_owner "$real" >/dev/null 2>&1 && continue
    acc=no; [ -r "$real" ] && acc=yes
    ex=no;  [ -x "$real" ] && ex=yes
    onpath=no
    case ":$PATH:" in *":$(dirname "$c"):"*) onpath=yes ;; esac
    k="$(classify_pm2 yes "$acc" "$ex" "$onpath")"
    case "$k" in
      D_USABLE_FOR_STAGING) best=D_USABLE_FOR_STAGING ;;
      B_NOT_ON_PATH) [ "$best" = D_USABLE_FOR_STAGING ] || best=B_NOT_ON_PATH ;;
      C_NOT_ACCESSIBLE) [ "$best" = A_ABSENT ] && best=C_NOT_ACCESSIBLE ;;
    esac
  done
  echo "  $best"
  [ -n "$(printf '%s' "$UNTRAVERSABLE" | tr -d ' \n')" ] && \
    echo "  CAVEAT: the search was INCOMPLETE (see 5b) — roots were skipped unread."
  echo "  A capability finding ONLY. No pm2 was started, no PM2_HOME exists, and this"
  echo "  says NOTHING about B3 or B5 — the launcher argv PM2 actually starts is still"
  echo "  unverified and remains b35a1's job."
fi

echo
echo "== 7. PM2_HOME, unchanged and uncreated =="
note "PM2_HOME in environment : ${PM2_HOME:-<unset>}"
note "would default to        : ${HOME%/}/.pm2"
note "  exists?               : $([ -e "${HOME%/}/.pm2" ] && echo YES || echo 'no — and NOT created')"
for p in $PRODUCTION_STATE_PATHS; do
  if [ -e "$p" ]; then
    note "production state $p : exists r=$([ -r "$p" ] && echo y || echo n) w=$([ -w "$p" ] && echo y || echo n)"
  else
    note "production state $p : not present or not visible"
  fi
done
note "this probe set no PM2_HOME, ran no pm2, and created no directory"

echo
echo "PROBE COMPLETE — discovery only. Nothing created, nothing started, nothing changed."
