#!/usr/bin/env bash
#
# THE execution entry for a PM2C-mode staging validation. Nothing else may start one.
#
# TWO PHASES, because "fresh" means opposite things at two different moments.
#
#   --phase stage   the staging root must NOT exist. The authorised archive is extracted
#                   into it and the result is verified against the authorised digests.
#   --phase run     the staging root MUST exist and MUST still be that subject. Everything
#                   this run creates — PM2_HOME, workdir, store, generated config — must
#                   NOT exist. Then the config is generated and PM2 is started.
#
# WHY THIS SHAPE. An earlier version had ONE phase with a single freshness rule: refuse if
# the staging root exists. That rule is correct before extraction and impossible after it —
# the root holds the extracted subject by the time anything can be started. The run would
# have stopped at its own guard, having created a staging tree and done nothing else, and
# the offline suite never caught it because every test invoked the entry against a root
# that did not exist. Each piece was right; the composition was never exercised.
#
# WHY THE FILE IS NOT NAMED AFTER A RUN. It used to be `pm2d_execute.sh`, which invited
# exactly the confusion this campaign kept having to correct. The LABEL is a parameter:
#
#   PM2C     the validation MODE and the grant name (WOA23_PM2C_GRANTED). Not a run.
#   pm2E     an execution IDENTITY — label, staging tree, workdir, PM2_HOME, port, app.
#   pm2C/D   earlier identities, each stopped by a pre-start audit, NEITHER EVER EXECUTED.
#
#   WOA23_PM2C_GRANTED=yes ./deploy/staging_execute.sh --phase stage \
#     --root ~/woa23-pm2e --archive ~/subject.tar --files 160 --filelist <sha256>
#
#   WOA23_PM2C_GRANTED=yes ./deploy/staging_execute.sh --phase run \
#     --root ~/woa23-pm2e --label pm2E --pm2-home ~/woa23-pm2e-pm2 \
#     --port 18263 --app woa23-pm2e-candidate --files 160 --filelist <sha256> \
#     [--preflight-only]
#
# --preflight-only applies to the RUN phase and stops immediately before `pm2 start`, after
# every guard, the store build and the config generation. It is what the offline suite
# drives, so the whole sequence is exercised without PM2 existing.
set -uo pipefail

die() { printf '%s\n' "$@" >&2; exit 2; }

# ------------------------------------------------------------------- 1. THE GRANT, FIRST
# Before arguments, before paths, before anything that could have a side effect. Both
# phases: extraction creates state too, and unauthorised state is still unauthorised.
GRANT="${WOA23_PM2C_GRANTED:-}"
if [ "$GRANT" != "yes" ]; then
  die "REFUSING: WOA23_PM2C_GRANTED is not 'yes'." \
      "  This is the execution entry for a PM2C-mode staging validation on VM24." \
      "  It creates a staging tree, starts a PM2 app and binds a port, and it needs its" \
      "  own explicit grant. No other grant is accepted in its place."
fi
for other in WOA23_S2PERF_GRANTED WOA23_S2_C1_GRANTED WOA23_S2_C2_GRANTED \
             WOA23_D1_GRANTED WOA23_D2A_GRANTED WOA23_D2B_GRANTED \
             WOA23_BASH5_VERIFY_GRANTED; do
  eval "v=\${$other:-}"
  [ -n "$v" ] && die "REFUSING: $other is set in this environment." \
      "  A staging validation must not run beside another run's grant."
done

# THE STORE-OWNERSHIP GUARD IS A HARD DEPENDENCY, loaded ONLY from beside this file.
#
# It used to be sourced conditionally -- `if [ -r ... ]; then . ...; fi` -- so a MISSING
# library was silently tolerated. The bootstrap delivered only the driver, the function was
# never defined, and the run continued until it died at the call site with "command not
# found". D-3 halted there. A critical guard that is absent must stop the run AT LOAD, not
# produce an undefined function that some later branch may or may not reach.
#
# It is loaded from $_SE_HERE ONLY -- the directory this file was executed from. There is no
# search path and no checkout fallback: if the bootstrap did not deliver it, nothing else
# may satisfy it.
_SE_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_SE_GUARD="$_SE_HERE/lib_store_guard.sh"
# THE SYMLINK TEST COMES FIRST, because `-e` follows links: a BROKEN symlink fails `-e`
# and would be reported as "missing", which is not what is on disk. A link is a link
# whether or not it currently resolves, and the refusal must say so.
[ -L "$_SE_GUARD" ] && die \
  "REFUSING: the store guard library is a SYMLINK: $_SE_GUARD" \
  "  It would resolve somewhere this driver did not come from."
[ -e "$_SE_GUARD" ] || die \
  "REFUSING: the store guard library is missing: $_SE_GUARD" \
  "  It is a hard dependency and is loaded only from the directory this driver ran from." \
  "  The bootstrap must deliver it beside the driver."
[ -f "$_SE_GUARD" ] || die "REFUSING: the store guard library is not a regular file: $_SE_GUARD"
[ -r "$_SE_GUARD" ] || die "REFUSING: the store guard library is not readable: $_SE_GUARD"
# shellcheck source=/dev/null
. "$_SE_GUARD" || die "REFUSING: the store guard library failed to load: $_SE_GUARD"
# LOADED IS NOT THE SAME AS USABLE. A truncated or edited library can source cleanly and
# still not define the function, so the symbol itself is checked.
command -v store_owner_verdict >/dev/null 2>&1 || die \
  "REFUSING: $_SE_GUARD loaded but does not define store_owner_verdict." \
  "  A library that sources cleanly and defines nothing is not a guard."

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROC="${PROC_ROOT:-/proc}"     # test seam only; never set in a run

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# The identity of a staged tree: a sorted per-file digest listing, hashed.
#
# GENERATED PATHS ARE EXCLUDED BY NAME, and the list is exhaustive rather than a wildcard:
# `.venv` (uv sync), `__pycache__` (any import), `tmp-*` (PM2 logs) and the generated
# ecosystem config are the only things this procedure creates inside the tree. Everything
# else contributes, so a modified, missing, extra, partial or foreign file changes the
# digest.
#
# LC_ALL=C on find and sort: collation is locale dependent, and the authorised digest was
# computed elsewhere. A digest that changes with the reader's locale cannot be checked.
tree_filelist() {   # tree_filelist <tree> <out>
  ( cd "$1" || return 1
    LC_ALL=C find . -type f \
      -not -path './.venv/*' -not -path '*/__pycache__/*' \
      -not -path './tmp-*' -not -path './tmp-*/*' \
      -not -name 'ecosystem.*.config.js' -o \
      -type f -name 'ecosystem.production.config.js' -o \
      -type f -name 'ecosystem.staging.config.js' \
      | sed 's|^\./||' | LC_ALL=C sort -u \
      | while IFS= read -r f; do printf '%s  %s\n' "$(sha256_of "$f")" "$f"; done
  ) > "$2"
}

# ------------------------------------------------------------------------ 2. arguments
PHASE=""; ROOT=""; LABEL=""; PM2HOME=""; PORT=""; APP=""
ARCHIVE=""; WANT_FILES=""; WANT_LIST=""; PREFLIGHT_ONLY=no
# STORE MODE. Defaulting to `synthetic` is deliberate: every existing caller omits the
# flag and must keep the behaviour it already has, byte for byte. The real store is
# reachable ONLY by naming it, never by an omission or a path that happens to resolve
# there -- that is the difference between a mode and a reinterpretation.
STORE_MODE=synthetic; REAL_STORE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --phase)    PHASE="${2:-}"; shift 2 ;;
    --root)     ROOT="${2:-}"; shift 2 ;;
    --label)    LABEL="${2:-}"; shift 2 ;;
    --pm2-home) PM2HOME="${2:-}"; shift 2 ;;
    --port)     PORT="${2:-}"; shift 2 ;;
    --app)      APP="${2:-}"; shift 2 ;;
    --archive)  ARCHIVE="${2:-}"; shift 2 ;;
    --files)    WANT_FILES="${2:-}"; shift 2 ;;
    --filelist) WANT_LIST="${2:-}"; shift 2 ;;
    --store-mode)  STORE_MODE="${2:-}"; shift 2 ;;
    --real-store)  REAL_STORE="${2:-}"; shift 2 ;;
    --preflight-only) PREFLIGHT_ONLY=yes; shift ;;
    *) die "unexpected argument: $1" ;;
  esac
done
case "$STORE_MODE" in
  synthetic|real-readonly) ;;
  *) die "--store-mode must be 'synthetic' or 'real-readonly' (got '${STORE_MODE}')." ;;
esac
# The pairing is enforced BOTH ways. A --real-store left over from an edited command line
# must not sit unused beside a synthetic run, where it would read as if the real store had
# been involved when it was not.
if [ "$STORE_MODE" = real-readonly ]; then
  [ -n "${REAL_STORE//[[:space:]]/}" ] || die \
    "--real-store is required when --store-mode is real-readonly, and has no default." \
    "  The production store is named explicitly or not used at all."
else
  [ -z "${REAL_STORE//[[:space:]]/}" ] || die \
    "--real-store was given but --store-mode is '$STORE_MODE'." \
    "  Refusing rather than ignoring it: a real store named in a synthetic run is a" \
    "  command line that does not mean what it says."
fi
case "$PHASE" in
  stage|run) ;;
  *) die "--phase must be 'stage' or 'run' (got '${PHASE}')." \
         "  There is no default: the two phases have OPPOSITE rules about the staging" \
         "  root, and guessing which was meant is how the previous version broke." ;;
esac
[ -n "${ROOT//[[:space:]]/}" ]       || die "--root is required and has no default."
[ -n "${WANT_FILES//[[:space:]]/}" ] || die "--files is required: the authorised file count."
[ -n "${WANT_LIST//[[:space:]]/}" ]  || die "--filelist is required: the authorised file-list SHA-256."
case "$WANT_FILES" in ''|*[!0-9]*) die "--files is not a number: '$WANT_FILES'" ;; esac
case "$WANT_LIST" in
  *[!0-9a-f]*|"") die "--filelist is not a sha256 hex digest: '$WANT_LIST'" ;;
esac
[ "${#WANT_LIST}" -eq 64 ] || die "--filelist is not 64 hex characters: '$WANT_LIST'"

# ============================================================== PHASE: stage
if [ "$PHASE" = stage ]; then
  [ -n "${ARCHIVE//[[:space:]]/}" ] || die "--archive is required in the stage phase."
  [ -f "$ARCHIVE" ] || die "no such archive: $ARCHIVE"
  [ -n "${LABEL//[[:space:]]/}" ] || die \
    "--label is required in the stage phase too." \
    "  The whole identity is checked here, and the generated config's path depends on it."
  [ -n "${PM2HOME//[[:space:]]/}" ] || die "--pm2-home is required in the stage phase too."

  # A. BEFORE EXTRACTION, EVERY ELEMENT OF THE IDENTITY MUST BE ABSENT.
  #
  # Not just the staging root. `pm2E` failed because the WORKDIR was checked only at run
  # time, by which point the venv step had created it — the same shape of defect as `pm2D`,
  # where the root was checked after extraction had created it. Checking one path at the
  # right moment and the rest at the wrong one is not a lifecycle; it is a coincidence.
  #
  # Nothing here is deleted, emptied or reused. A path that exists belongs to a previous
  # attempt or to another run, and either way it is evidence.
  for existing in "$ROOT" "${ROOT}-work" "$PM2HOME" "$ROOT/store" \
                  "$ROOT/dev2026/deploy/ecosystem.$LABEL.config.js"; do
    [ -e "$existing" ] && die \
      "REFUSING: $existing already exists." \
      "  Every element of this run's identity must be absent before staging begins:" \
      "    staging root      $ROOT" \
      "    workdir           ${ROOT}-work" \
      "    PM2_HOME          $PM2HOME" \
      "    store             $ROOT/store" \
      "    generated config  $ROOT/dev2026/deploy/ecosystem.$LABEL.config.js" \
      "  It is NOT deleted, emptied or reused. Choose a new identity."
  done

  echo "PM2C-mode staging validation — phase STAGE"
  echo "  root    : $ROOT   (absent, as required)"
  echo "  archive : $ARCHIVE"
  echo "  archive sha256: $(sha256_of "$ARCHIVE")"
  mkdir -p "$ROOT" || die "cannot create $ROOT"
  tar -x -f "$ARCHIVE" -C "$ROOT" || die "extraction failed into $ROOT"

  TREE="$ROOT/dev2026"
  [ -d "$TREE" ] || die "the archive did not produce $TREE — is it the right archive?"
  [ -e "$TREE/.git" ] && die "REFUSING: the extracted tree contains .git; it is not a clean export."

  LIST="$ROOT/.staged-filelist.sha256"
  tree_filelist "$TREE" "$LIST" || die "cannot compute the staged tree's file list"
  GOT_FILES="$(wc -l < "$LIST" | tr -d ' ')"
  GOT_LIST="$(sha256_of "$LIST")"
  echo "  files     : $GOT_FILES   (authorised $WANT_FILES)"
  echo "  file-list : $GOT_LIST"
  echo "  authorised: $WANT_LIST"
  [ "$GOT_FILES" = "$WANT_FILES" ] || die \
    "SUBJECT MISMATCH: the staged tree has $GOT_FILES files, the authorised subject has $WANT_FILES." \
    "  A partial extraction, a truncated archive or the wrong archive all look like this." \
    "  Nothing is deleted; the tree is left for inspection."
  [ "$GOT_LIST" = "$WANT_LIST" ] || die \
    "SUBJECT MISMATCH: the staged tree's file-list digest is not the authorised one." \
    "  staged    : $GOT_LIST" \
    "  authorised: $WANT_LIST" \
    "  The file COUNT matched, so this is a modified, stale or foreign tree rather than a" \
    "  partial one. Nothing is deleted; the tree is left for inspection."
  echo
  echo "STAGED and VERIFIED: the tree is the authorised subject, file for file."
  echo
  echo "NEXT — build the venv, and note what must NOT happen while you do:"
  echo "  cd $TREE"
  echo "  UV_CACHE_DIR=${ROOT}-uvcache \\"
  # THE SHARED PRODUCTION INTERPRETER IS NOT AN OPTION HERE. This line used to name
  # /home/odbadmin/.pyenv/versions/py311/bin/python3.11 -- production's own 3.11.4 -- and
  # following it would have put a SHARED interpreter in the serving path, the failure
  # spec 016 exists to prevent. The venv is built against the run's own isolated
  # interpreter, offline, from the pinned cache.
  echo "    UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=never UV_CACHE_DIR=<this account's own cache> \\"
  echo "      uv sync --locked --offline        # never --python <a shared interpreter>"
  echo
  echo "  THE WORKDIR MUST STILL NOT EXIST when --phase run starts:"
  echo "    ${ROOT}-work"
  echo "  The run phase CREATES it and stamps it with this run's identity. Putting the uv"
  echo "  cache, a manifest or anything else there beforehand is what ended pm2E."
  echo "  Use a task-specific cache path — ${ROOT}-uvcache above — never the workdir."
  exit 0
fi

# ================================================================ PHASE: run
for pair in "label:$LABEL" "pm2-home:$PM2HOME" "port:$PORT" "app:$APP"; do
  name="${pair%%:*}"; value="${pair#*:}"
  [ -n "${value//[[:space:]]/}" ] || die "--$name is required in the run phase."
done

# B. AFTER EXTRACTION the staging root must EXIST and must still be the subject.
[ -d "$ROOT" ] || die \
  "REFUSING: the staging root does not exist: $ROOT" \
  "  The run phase operates on an already-staged subject. Run --phase stage first."
TREE="$ROOT/dev2026"
[ -d "$TREE" ] || die "REFUSING: $TREE is missing — this root is not a staged subject."

# FULL PROVENANCE, not a spot check.
# Checking that one file exists proves only that a file exists. A stale subject, a
# partially extracted tree, a single modified file and an entirely foreign tree all pass
# that. The whole file list is re-derived and compared against the AUTHORISED digest —
# taken from the command line, not from anything inside the tree, because a foreign tree
# can carry a forged manifest.
LIST="$ROOT/.run-filelist.sha256"
tree_filelist "$TREE" "$LIST" || die "cannot compute the staged tree's file list"
GOT_FILES="$(wc -l < "$LIST" | tr -d ' ')"
GOT_LIST="$(sha256_of "$LIST")"
if [ "$GOT_FILES" != "$WANT_FILES" ] || [ "$GOT_LIST" != "$WANT_LIST" ]; then
  die "SUBJECT PROVENANCE FAILED — refusing to start PM2." \
      "  files     : $GOT_FILES   (authorised $WANT_FILES)" \
      "  file-list : $GOT_LIST" \
      "  authorised: $WANT_LIST" \
      "" \
      "  The tree at $ROOT is not the authorised subject. It may be stale, partial," \
      "  modified or foreign. Nothing is deleted or repaired; it is left for inspection."
fi
echo "PM2C-mode staging validation — phase RUN"
echo "  root      : $ROOT   (present, and verified as the authorised subject)"
echo "  provenance: $GOT_FILES files, file-list $GOT_LIST"

# ------------------------------------------------- it cannot be aimed at production
[ "$APP" = "woa23" ] && die \
  "REFUSING: the staging app may not be named 'woa23' — that is production's app." \
  "  Even under an isolated PM2_HOME, sharing the name makes a mistyped PM2_HOME" \
  "  ambiguous in exactly the situation where ambiguity costs most."
case "$APP" in all|ALL|*'*'*) die "REFUSING app name '$APP': one named app, no wildcards." ;; esac
case "$PORT" in ''|*[!0-9]*) die "--port is not a number: '$PORT'" ;; esac
[ "$PORT" -ge 1024 ] && [ "$PORT" -le 65535 ] || die "--port out of range: $PORT"
case "$PORT" in 8050|8786|8787) die "REFUSING to bind $PORT: that is a PRODUCTION port." ;; esac

real_pm2home="$(cd "$(dirname "$PM2HOME")" 2>/dev/null && pwd -P)/$(basename "$PM2HOME")"
real_home="$(cd "$HOME" 2>/dev/null && pwd -P || printf '%s' "$HOME")"
case "$real_pm2home" in
  "$real_home/.pm2"|"$real_home/.pm2/"*) die \
    "REFUSING: PM2_HOME resolves to production's daemon home: $real_pm2home" ;;
esac

# BOOTSTRAP PATH vs IDENTITY PATH -- the distinction b35a1 did not have.
#
# This driver lives INSIDE the archive and cannot be piped over stdin, so it must be on
# disk before it can run. Where it is put is a BOOTSTRAP PATH: scratch, one file, no part
# of the run's identity. The paths below are IDENTITY PATHS, and every one must be ABSENT
# when staging begins.
#
# b35a1 conflated them. To get this script on disk it extracted the archive into the
# STAGING ROOT, which is the first identity path checked here -- so the bootstrap created
# the thing the guard requires absent, and the run was refused before it began.
#
# The fix is deploy/staging_bootstrap.sh, which places the driver OUTSIDE every identity
# path, verifies it against the archive it came from, and only then hands over. THE GUARD
# BELOW IS UNCHANGED and must stay that way: it cannot tell a deliberate bootstrap from
# an abandoned previous attempt, and it should not try.
#
# ------------------------------------- everything THIS run creates must not exist yet
# The freshness rule belongs here, on the things the run itself produces. None of them is
# created by extraction, so requiring their absence is compatible with a staged root.
STORE="$ROOT/store"
WORKDIR="${ROOT}-work"
# The generated config belongs to the STAGED TREE, not to whichever copy of this script
# was invoked. `$HERE` is derived from BASH_SOURCE, so running the repository's copy would
# have written the run's config into the repository's working tree — polluting it, and
# putting the config somewhere PM2's cwd is not. Caught by running the real sequence.
CONFIG="$TREE/deploy/ecosystem.$LABEL.config.js"
LOGDIR="tmp-$LABEL"
for d in "$PM2HOME" "$STORE" "$CONFIG" "$TREE/$LOGDIR"; do
  [ -e "$d" ] && die \
    "REFUSING: $d already exists." \
    "  This run creates it, so its presence means a previous attempt got this far." \
    "  It is NOT deleted or reused — that would destroy the evidence of what happened."
done

# ------------------------------------------- the workdir: created HERE, and owned by THIS run
#
# `pm2E` died because the workdir was required absent at this point while the venv step had
# legitimately created it. The answer is not to drop the check — an unowned workdir is
# exactly the stale/foreign state that must stop a run — but to make the RUN PHASE OWN THE
# WORKDIR'S LIFECYCLE: it creates it, and it stamps it.
#
# So there are three cases and only one of them proceeds:
#   absent            -> create it, write the identity marker, continue
#   marker matches    -> this run already created it (a re-entry); continue
#   anything else     -> stale, foreign, or created by a step that had no business
#                        creating it. STOP, and delete nothing.
WORK_MARKER="$WORKDIR/.run-identity"
marker_body() {
  printf 'run-identity-v1\nlabel=%s\nsubject-filelist=%s\napp=%s\nport=%s\n' \
    "$LABEL" "$WANT_LIST" "$APP" "$PORT"
}
if [ ! -e "$WORKDIR" ]; then
  mkdir -p "$WORKDIR" || die "cannot create the workdir: $WORKDIR"
  { marker_body; printf 'created=%s\ncreator-pid=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$$"; } \
    > "$WORK_MARKER" || die "cannot stamp the workdir: $WORK_MARKER"
  echo "  workdir   : $WORKDIR   (created by this run, stamped)"
else
  [ -f "$WORK_MARKER" ] || die \
    "REFUSING: the workdir exists but carries no run-identity marker: $WORKDIR" \
    "  The run phase creates and stamps the workdir. An unstamped one was made by" \
    "  something else — a uv cache, a manifest, a previous attempt, another run — and" \
    "  its contents are unverified." \
    "  This is exactly what ended pm2E: the venv step had put UV_CACHE_DIR here." \
    "  Use a task-specific cache path instead, and leave the workdir to the run phase." \
    "  Nothing is deleted."
  # head -5, not -4: marker_body emits FIVE lines — the format line plus four fields — and
  # comparing five against four made a correct marker never match, so the only reachable
  # outcome was "belongs to a DIFFERENT run". Counted by hand once; derived from the
  # function now.
  MARKER_LINES="$(marker_body | wc -l | tr -d ' ')"
  if ! diff <(marker_body) <(head -n "$MARKER_LINES" "$WORK_MARKER") >/dev/null 2>&1; then
    echo "  --- marker found ---" >&2; sed 's/^/    /' "$WORK_MARKER" >&2
    echo "  --- this run ---"     >&2; marker_body | sed 's/^/    /' >&2
    die "REFUSING: the workdir belongs to a DIFFERENT run." \
        "  Its marker does not match this run's label, subject, app and port." \
        "  A workdir is owned by one run; adopting another's would mix two runs' state" \
        "  in one place and make both unreadable. Nothing is deleted."
  fi
  echo "  workdir   : $WORKDIR   (already created by THIS run; marker matches)"
fi

# The venv must not live inside the workdir: the workdir is this run's scratch space and
# the venv is a staged-tree artefact. Crossing them is how the pm2E confusion started.
case "$(cd "$TREE/.venv" 2>/dev/null && pwd -P || echo /nonexistent)" in
  "$(cd "$WORKDIR" 2>/dev/null && pwd -P || echo /nowhere)"/*)
    die "REFUSING: the venv resolves inside the workdir." ;;
esac

# ---------------------------------------------------------------- the port must be free
LEDGER="$TREE/scripts/ports_used.tsv"
[ -r "$LEDGER" ] || die "cannot read the port ledger at $LEDGER"
if awk -F'\t' -v p="$PORT" '$1 == p { f = 1 } END { exit !f }' "$LEDGER"; then
  die "REFUSING: port $PORT is ALREADY IN $LEDGER — it is spent."
fi
if command -v ss >/dev/null 2>&1; then
  [ "$(ss -ltn 2>/dev/null | grep -c ":$PORT ")" -eq 0 ] \
    || die "REFUSING: port $PORT is already bound on this host."
fi

VENV="$TREE/.venv/bin/python"
[ -x "$VENV" ] || die \
  "REFUSING: no interpreter at $VENV" \
  "  The run phase needs the venv built between the phases:" \
  "    cd $TREE && UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=never uv sync --locked --offline" \
  "  The interpreter must be this account's own isolated build. A shared interpreter -- " \
  "  production's pyenv py311 in particular -- is refused by spec 016 and must not be named."

echo "  label     : $LABEL        (this run's execution identity)"
echo "  PM2_HOME  : $real_pm2home   (never production's $real_home/.pm2)"
echo "  app       : $APP        (never production's 'woa23')"
echo "  port      : $PORT        (absent from the ledger, unbound)"
if [ "$STORE_MODE" = synthetic ]; then
  echo "  store     : $STORE       (synthetic; production data is never copied)"
else
  echo "  store     : $STORE -> $REAL_STORE   (REAL, read-only; never written)"
fi
echo "  store mode: $STORE_MODE"
echo "  venv      : $VENV ($("$VENV" --version 2>&1))"
echo

if [ "$STORE_MODE" = synthetic ]; then
# ------------------------------------------------- build the store, with the subject's builder
echo "building the synthetic store with the subject's own builder"
( cd "$TREE" && ./.venv/bin/python deploy/make_staging_store.py "$STORE" ) 2>&1 \
  | grep -vE 'RuntimeWarning|warnings.warn|^\s*$' | sed 's/^/  /' \
  || die "the store builder failed"
STORE_FILES="$(find "$STORE" -type f | wc -l | tr -d ' ')"
[ "$STORE_FILES" = 72 ] || die "store has $STORE_FILES files, expected 72"
[ -r "$STORE/1_degree/annual/TS/.zgroup" ] || die "no anchor group in the built store"
chmod -R a-w "$STORE"
if touch "$STORE/.write-probe" 2>/dev/null; then
  die "REFUSING: the store is still writable after chmod — a read-only store is required."
fi
echo "  72 files, anchor present, read-only (write probe refused)"

else
# ================================================== REAL STORE, READ-ONLY. Nothing is built.
#
# EVERY SYNTHETIC ASSUMPTION IS GONE FROM THIS BRANCH, and their absence is the point:
#
#   no make_staging_store.py   nothing is generated into the store
#   no `= 72` assertion        that number describes the fixture, not real data
#   no chmod / chown           the run must not change production's permissions
#   no mkdir / rm / touch      not even a write probe: probing by writing is a write
#
# The ONLY filesystem write this branch performs is `ln -s` creating $STORE, and that
# symlink lives inside the STAGING ROOT. The production store is opened for reading and
# for nothing else.
echo "REAL STORE MODE — read-only. Nothing is built, nothing is written to the store."

# 0. THE TOOLING MUST BE ABLE TO ANSWER, or the guards below are theatre.
#    Every check in this branch uses GNU `find -writable / ! -readable / ! -executable /
#    -printf` and GNU `stat -c`. On a BSD userland those are unknown primaries: the command
#    errors, `2>/dev/null` swallows it, `wc -l` reports 0, and EVERY guard "passes" while
#    testing nothing. That is a vacuous pass, and it is refused here rather than discovered
#    later in a result that looked clean.
find "$REAL_STORE" -maxdepth 0 -printf '' >/dev/null 2>&1 || die \
  "REFUSING: this host's find(1) does not support -printf (GNU find is required)." \
  "  Without it the writability, readability and fingerprint checks below return 0" \
  "  because the command failed, not because the store is clean. A guard that cannot" \
  "  run must stop the run, not pass it."
stat -c %u "$REAL_STORE" >/dev/null 2>&1 || die \
  "REFUSING: this host's stat(1) does not support -c (GNU coreutils is required)." \
  "  The ownership check cannot run, and an ownership check that cannot run is the" \
  "  one guard this mode most depends on."

# 1. EXACT PATH. The real store is not "a path that resolves somewhere acceptable"; it is
#    one literal, and anything else stops. This is what keeps the mode from becoming a
#    general-purpose escape hatch.
EXPECT_REAL_STORE='/home/odbadmin/python/woa23/data'
[ "$REAL_STORE" = "$EXPECT_REAL_STORE" ] || die \
  "REFUSING: --real-store is not the authorised production store." \
  "  given    : $REAL_STORE" \
  "  required : $EXPECT_REAL_STORE"

# 2. It must exist, be a directory, and NOT itself be a symlink -- a symlinked store root
#    could be re-pointed between this check and the run.
[ -e "$REAL_STORE" ] || die "REFUSING: the real store does not exist: $REAL_STORE"
[ -d "$REAL_STORE" ] || die "REFUSING: the real store is not a directory: $REAL_STORE"
[ -L "$REAL_STORE" ] && die \
  "REFUSING: the real store path is itself a symlink." \
  "  A symlinked store root can be re-pointed after this check and before the run."

# 3. REALPATH must equal the literal. Checked because every other guard in this campaign
#    that compared paths lexically could be walked around with a link.
REAL_RESOLVED="$(readlink -f "$REAL_STORE" 2>/dev/null || true)"
[ "$REAL_RESOLVED" = "$EXPECT_REAL_STORE" ] || die \
  "REFUSING: the real store's realpath is not the authorised path." \
  "  realpath : ${REAL_RESOLVED:-<unresolvable>}" \
  "  required : $EXPECT_REAL_STORE"

# 4. OWNERSHIP. The store must be owned by the AUTHORISED owner, and never by the account
#    running this.
#
#    THIS GUARD WAS INVERTED and D-3 halted on it. It read
#      [ "$STORE_UID" != "$ME_UID" ] && die "...which is this account"
#    which fired when the uids DIFFER -- the safe state -- and would have passed silently
#    when they MATCH, the dangerous one. The decision now lives in a function so a test can
#    exercise every combination, instead of only a real production store being able to.
EXPECT_STORE_UID=1000
ME_UID="$(id -u)"
STORE_UID="$(stat -c %u "$REAL_STORE" 2>/dev/null || echo '')"
OWNER_VERDICT="$(store_owner_verdict "$STORE_UID" "$ME_UID" "$EXPECT_STORE_UID")"
case "$OWNER_VERDICT" in
  ok) : ;;
  self) die \
    "REFUSING: the real store is owned by uid $STORE_UID, which IS this account ($ME_UID)." \
    "  An owner may always restore its own write permission, so read-only could not hold." ;;
  root) die \
    "REFUSING: the real store is owned by root (uid 0), not the authorised owner $EXPECT_STORE_UID." ;;
  unreadable) die \
    "REFUSING: the real store's owner could not be read: $REAL_STORE" \
    "  An unreadable owner is a refusal, never a default." ;;
  nonnumeric) die \
    "REFUSING: the real store's owner is not a plain uid (got '$STORE_UID')." ;;
  unexpected) die \
    "REFUSING: the real store is owned by uid $STORE_UID, not the authorised owner $EXPECT_STORE_UID." ;;
  *) die "REFUSING: unrecognised ownership verdict '$OWNER_VERDICT' -- refusing rather than guessing." ;;
esac
echo "  owner uid $STORE_UID = authorised $EXPECT_STORE_UID, and not this account ($ME_UID)"

# 5. NOT WRITABLE, by the kernel's own answer. `test -w` uses access(2), which honours
#    ACLs -- so this catches a POSIX ACL grant that a mode-bit reading would miss.
[ -w "$REAL_STORE" ] && die \
  "REFUSING: the real store is WRITABLE by uid $ME_UID." \
  "  The whole basis of this mode is that a write is impossible, not merely unintended."
[ -r "$REAL_STORE" ] || die "REFUSING: the real store is not readable: $REAL_STORE"
[ -x "$REAL_STORE" ] || die "REFUSING: the real store is not traversable: $REAL_STORE"

# 6. NO WRITABLE PATH ANYWHERE BENEATH IT.
RS_WRITABLE="$(find "$REAL_STORE" -writable 2>/dev/null | wc -l | tr -d ' ')"
[ "$RS_WRITABLE" = 0 ] || die \
  "REFUSING: $RS_WRITABLE path(s) beneath the real store are writable by uid $ME_UID."

# 7. NO WRITABLE ANCESTOR up to /. An unwritable store under a writable parent is not
#    protected: the parent's owner can replace the store wholesale.
anc="$REAL_RESOLVED"
while [ "$anc" != "/" ]; do
  anc="$(dirname "$anc")"
  [ -w "$anc" ] && die \
    "REFUSING: ancestor '$anc' of the real store is writable by uid $ME_UID."
done

# 8. NO ESCAPING SYMLINKS. A link inside the store that resolves outside it is a path the
#    app would follow out of the read-only area, into something with other permissions.
RS_ESCAPES=0
while IFS= read -r lnk; do
  [ -n "$lnk" ] || continue
  t="$(readlink -f "$lnk" 2>/dev/null || true)"
  case "$t" in
    "$REAL_RESOLVED"|"$REAL_RESOLVED"/*) ;;
    *) RS_ESCAPES=$((RS_ESCAPES + 1)); printf '    escapes: %s -> %s\n' "$lnk" "${t:-<unresolvable>}" >&2 ;;
  esac
done <<EOF
$(find "$REAL_STORE" -type l 2>/dev/null)
EOF
[ "$RS_ESCAPES" = 0 ] || die \
  "REFUSING: $RS_ESCAPES symlink(s) inside the real store resolve outside it."

# 9. UNREADABLE / UNTRAVERSABLE paths would make the run's coverage silently partial.
RS_UNREADABLE="$(find "$REAL_STORE" -type f ! -readable 2>/dev/null | wc -l | tr -d ' ')"
RS_UNTRAVERSABLE="$(find "$REAL_STORE" -type d ! -executable 2>/dev/null | wc -l | tr -d ' ')"
[ "$RS_UNREADABLE" = 0 ]    || die "REFUSING: $RS_UNREADABLE unreadable file(s) in the real store."
[ "$RS_UNTRAVERSABLE" = 0 ] || die "REFUSING: $RS_UNTRAVERSABLE untraversable dir(s) in the real store."

# 10. ANCHOR, read-only. Existence and readability only -- never a write probe.
[ -r "$REAL_STORE/1_degree/annual/TS/.zgroup" ] || die \
  "REFUSING: no readable anchor group at 1_degree/annual/TS/.zgroup in the real store."

# 11. The staging-internal symlink. THIS is the only thing created, and it is created
#     inside the staging root, never inside the store.
[ -e "$STORE" ] || [ -L "$STORE" ] && die \
  "REFUSING: $STORE already exists; the run phase requires it absent."
ln -s "$REAL_STORE" "$STORE" || die "could not create the staging store symlink at $STORE"
[ -L "$STORE" ] || die "REFUSING: $STORE is not a symlink after creation."
LINK_RESOLVED="$(readlink -f "$STORE" 2>/dev/null || true)"
[ "$LINK_RESOLVED" = "$EXPECT_REAL_STORE" ] || die \
  "REFUSING: the staging symlink does not resolve to the authorised store." \
  "  resolves : ${LINK_RESOLVED:-<unresolvable>}" \
  "  required : $EXPECT_REAL_STORE"

# 12. The metadata fingerprint, by the Stage C / D-1 method, so a before/after comparison
#     within this run is possible. METADATA ONLY: it cannot detect a same-size, same-mtime
#     content change, and is never reported as content integrity.
RS_FILES="$(find "$REAL_STORE" -type f 2>/dev/null | wc -l | tr -d ' ')"
RS_BYTES="$(du -sb "$REAL_STORE" 2>/dev/null | cut -f1)"
RS_FPRINT="$(find "$REAL_STORE" -type f -printf '%p\t%s\t%T@\n' 2>/dev/null \
             | LC_ALL=C sort | sha256sum | cut -d' ' -f1)"
echo "  real store : $REAL_STORE"
echo "    owner uid $STORE_UID (not this account $ME_UID), not writable, no writable ancestor"
echo "    beneath: 0 writable, 0 unreadable, 0 untraversable, 0 escaping symlinks"
echo "    files $RS_FILES, bytes $RS_BYTES"
echo "    metadata fingerprint (path+size+mtime, LC_ALL=C): $RS_FPRINT"
echo "    NOT a content digest -- a same-size, same-mtime change is invisible to it"
echo "  staging symlink: $STORE -> $LINK_RESOLVED   (the only path this branch created)"
fi
echo

# --------------------------------------------------------- generate and diff the config
GENERATOR="$TREE/deploy/make_staging_override.js"
[ -f "$GENERATOR" ] || die "the override generator is missing: $GENERATOR"
command -v node >/dev/null 2>&1 || die "node is required to generate the config"
echo "generating the override config from the production config"
echo "  generator sha256 : $(sha256_of "$GENERATOR")"
echo "  source config    : $(sha256_of "$TREE/deploy/ecosystem.production.config.js")"
# The mode is passed through EXPLICITLY. In real mode $STORE is a symlink, and the
# generator's own production-store guard is lexical -- it would not have noticed. Telling
# it the mode is what turns "the guard happened not to fire" into "the guard was told what
# this is and checked it by realpath".
WOA23_PM2C_GRANTED=yes node "$GENERATOR" \
  --name "$APP" --port "$PORT" --store "$STORE" --logdir "$LOGDIR" --out "$CONFIG" \
  --python "$VENV" --store-mode "$STORE_MODE" \
  || die "the override generator refused; not starting anything."
CONFIG_SHA="$(sha256_of "$CONFIG")"
echo "  generated config sha256 : $CONFIG_SHA"
echo

if [ "$PREFLIGHT_ONLY" = yes ]; then
  echo "PREFLIGHT ONLY — stopping before 'pm2 start'."
  echo "  nothing was started, no port was bound, no PM2 command was issued."
  exit 0
fi

# ------------------------------------------------------------------------- start it
PM2="${WOA23_PM2_BIN:-pm2}"
command -v "$PM2" >/dev/null 2>&1 || die "no pm2 binary: $PM2"
export PM2_HOME="$real_pm2home"

# THE GRANT IS CONSUMED HERE and must not reach the service. `pm2 start` spawns the God
# Daemon with this shell's environment and the daemon passes it to the app, so without
# this the run's own authorisation token would sit in a serving process — and the strict
# allowlist below would then reject the run's own grant as an unexpected variable. A
# safety rule that fails correctly-authorised runs is one whoever hits it next weakens.
unset WOA23_PM2C_GRANTED
unset WOA23_PM2_BIN

# TLS VARIABLES ARE SET OR CLEARED **HERE**, BEFORE `pm2 start`, NOT CHECKED AFTERWARDS.
#
# Omitting the keys from the generated config was never sufficient and must not be
# described as though it were. PM2 merges an app's `env` OVER the God Daemon's own
# environment, and the daemon inherits THIS shell's environment at spawn. There is no
# "unset" directive in a PM2 config. So a value exported by an ancestor -- an operator's
# shell, a wrapper, a cron environment -- reaches the app no matter what the config omits.
#
# The /proc check further down is a DETECTOR, not a remedy: it catches a leak after the
# fact and fails the run. This block is what actually prevents one. Both are kept, because
# a guard that can only detect is worth less than one that also prevents, and a preventer
# with no detector cannot prove it worked.
#
# TLS ON is handled in the same breath and deliberately so: the paths are EXPORTED
# explicitly from the config rather than left to whatever happened to be inherited, so in
# neither mode does an ancestor decide what the service uses.
CFG_TLS="$(node -e "console.log(require('$CONFIG').apps[0].env.WOA23_TLS)")"
if [ "$CFG_TLS" = "off" ]; then
  unset WOA23_TLS_KEYFILE
  unset WOA23_TLS_CERTFILE
  echo "TLS off: WOA23_TLS_KEYFILE and WOA23_TLS_CERTFILE unset before pm2 start"
else
  WOA23_TLS_KEYFILE="$(node -e "console.log(require('$CONFIG').apps[0].env.WOA23_TLS_KEYFILE)")"
  WOA23_TLS_CERTFILE="$(node -e "console.log(require('$CONFIG').apps[0].env.WOA23_TLS_CERTFILE)")"
  [ -n "$WOA23_TLS_KEYFILE" ] && [ "$WOA23_TLS_KEYFILE" != "undefined" ] || die \
    "TLS is on but the config carries no WOA23_TLS_KEYFILE." \
    "  TLS on with no key is refused here rather than left to the launcher."
  [ -n "$WOA23_TLS_CERTFILE" ] && [ "$WOA23_TLS_CERTFILE" != "undefined" ] || die \
    "TLS is on but the config carries no WOA23_TLS_CERTFILE."
  export WOA23_TLS_KEYFILE WOA23_TLS_CERTFILE
  echo "TLS on: key and certificate exported explicitly from the config"
fi

NOW_SHA="$(sha256_of "$CONFIG")"
[ "$NOW_SHA" = "$CONFIG_SHA" ] || die \
  "REFUSING: the generated config changed between generation and start." \
  "  at generation: $CONFIG_SHA" "  now          : $NOW_SHA"
echo "config provenance re-verified: $NOW_SHA"
echo "starting: pm2 start $(basename "$CONFIG") --only $APP"
"$PM2" start "$CONFIG" --only "$APP" || die "pm2 start failed; nothing further is attempted."
echo

sleep "${WOA23_START_SETTLE:-8}"
PID="$("$PM2" jlist 2>/dev/null | node -e "
  const j = JSON.parse(require('fs').readFileSync(0, 'utf8'));
  const p = j.find(a => a.name === process.argv[1]);
  if (p && p.pid) process.stdout.write(String(p.pid));
" "$APP")"
[ -n "$PID" ] && [ "$PID" != "0" ] || die "pm2 reports no pid for $APP after start."
echo "pid: $PID"
echo
echo "environment READ BACK FROM THE RUNNING PROCESS ($PROC/$PID/environ):"
[ -r "$PROC/$PID/environ" ] || die \
  "cannot read $PROC/$PID/environ — refusing to report an unverified environment."
tr '\0' '\n' < "$PROC/$PID/environ" | grep -E '^WOA23_' | LC_ALL=C sort | sed 's/^/  /'
echo

env_value_of() {   # env_value_of <pid> <name>
  local v
  v="$(tr '\0' '\n' < "$PROC/$1/environ" | sed -n "s/^$2=//p" | head -1)"
  if tr '\0' '\n' < "$PROC/$1/environ" | grep -q "^$2="; then printf '%s' "$v"
  else printf 'ABSENT'; fi
}
env_value() { env_value_of "$PID" "$1"; }

# WORKERS ARE NOT COVERED BY CHECKING THE MASTER. gunicorn's master forks workers, and it
# is the WORKERS that serve. A leak present in a worker but read only from the master
# would be reported clean, which is the exact shape of mistake this file exists to refuse.
#
# ppid is taken from AFTER THE LAST ')' in /proc/<pid>/stat, not as field 4. The comm field
# is parenthesised and may itself contain spaces, so a fixed field index silently reads the
# wrong number for such a process. production_stop.sh had the same latent bug and it is now
# FIXED there too -- it reads the labelled PPid: line of /proc/<pid>/status, which cannot
# shift at all. This reader is kept as-is because it is checking an environment, not
# establishing a stop identity, and changing it is not this file's business.
children_of_pid() {   # children_of_pid <pid> — direct children, from /proc, never ps
  local parent="$1" p ppid
  for p in $(ls "$PROC" 2>/dev/null | grep '^[0-9][0-9]*$'); do
    [ -r "$PROC/$p/stat" ] || continue
    ppid="$(sed 's/.*) //' "$PROC/$p/stat" 2>/dev/null | awk '{print $2}')"
    [ "$ppid" = "$parent" ] && printf '%s\n' "$p"
  done
}
env_must() {
  local got; got="$(env_value "$1")"
  if [ "$got" = "$2" ]; then printf '  ok   %-22s = %s\n' "$1" "$got"
  else die "PROCESS ENV MISMATCH: $1 — want [$2], process has [$got]"; fi
}

# TEN variables either way, but the SPLIT depends on the TLS mode, and saying so is the
# point — a fixed "7 exact, 3 absent" would now be wrong half the time:
#
#   TLS off : 5 exact, 5 required ABSENT   (the key and certificate are omitted)
#   TLS on  : 7 exact, 3 required ABSENT   (the key and certificate must match the config)
#
# EIGHT are checked on the master by `env_must`. The remaining TWO — the TLS paths — are
# checked separately, on the master AND EVERY WORKER, because it is the workers that serve
# and a leak in a worker alone would otherwise be reported clean.
#
# The eight variables production_app.sh reads, plus two it does NOT read and which must be
# absent: WOA23_PRODUCTION_STORE (the staging guard's variable; the production launcher has
# no such guard, because production points AT production's store) and WOA23_PM2C_GRANTED
# (this run's authorisation token, consumed above).
echo "every variable the launcher reads, verified from the process:"
env_must WOA23_PORT              "$PORT"
env_must WOA23_ZARR_STORE        "$STORE"
env_must WOA23_TLS               "off"
env_must WOA23_WORKERS           "$(node -e "console.log(require('$CONFIG').apps[0].env.WOA23_WORKERS)")"
# TLS-AWARE, and verified on the MASTER AND EVERY WORKER. `CFG_TLS` was resolved before
# `pm2 start`, where the variables were unset (off) or exported explicitly (on); what
# follows confirms that took effect in the processes that actually serve.
#
# A leak here is NOT an ordinary mismatch. It means a production TLS path reached a staging
# process despite the config omitting it and the entry unsetting it, so the environment
# this run measured is not the environment it intended to measure. That is classified
# INVALID_ENVIRONMENT and the run yields NO RESULT -- not a failed check inside an
# otherwise clean pass, and never a clean PASS.
tls_invalid_environment() {   # tls_invalid_environment <pid> <role> <name> <value>
  die "INVALID_ENVIRONMENT: $3 is present on the $2 (pid $1) with WOA23_TLS=off." \
      "  value: $4" \
      "" \
      "  The staging config omits it and the entry unsets it before pm2 start, so this" \
      "  value was INHERITED from an ancestor process -- an operator shell, a wrapper, or" \
      "  an already-running PM2 daemon spawned with it in scope." \
      "" \
      "  This run is INVALID_ENVIRONMENT. It is NOT a clean PASS and yields NO B3/B5" \
      "  result: the environment measured is not the environment intended. State is" \
      "  PRESERVED for inspection; nothing is stopped, removed or retried from here." \
      "  Find the exporting ancestor before any re-run."
}
TLS_PIDS="$PID"
for w in $(children_of_pid "$PID"); do TLS_PIDS="$TLS_PIDS $w"; done
WORKER_COUNT=0
for tp in $TLS_PIDS; do
  role="master"; [ "$tp" = "$PID" ] || { role="worker"; WORKER_COUNT=$((WORKER_COUNT + 1)); }
  [ -r "$PROC/$tp/environ" ] || die \
    "cannot read $PROC/$tp/environ for the $role (pid $tp)." \
    "  An unreadable environment is not an absent one; refusing to report it as clean."
  for v in WOA23_TLS_KEYFILE WOA23_TLS_CERTFILE; do
    got="$(env_value_of "$tp" "$v")"
    if [ "$CFG_TLS" = "off" ]; then
      [ "$got" = "ABSENT" ] || tls_invalid_environment "$tp" "$role" "$v" "$got"
    else
      want="$(node -e "console.log(require('$CONFIG').apps[0].env.$v)")"
      [ "$got" = "$want" ] || die \
        "PROCESS ENV MISMATCH on the $role (pid $tp): $v — want [$want], got [$got]"
    fi
  done
done
if [ "$CFG_TLS" = "off" ]; then
  printf '  ok   %-22s = ABSENT on master and %d worker(s)\n' WOA23_TLS_KEYFILE  "$WORKER_COUNT"
  printf '  ok   %-22s = ABSENT on master and %d worker(s)\n' WOA23_TLS_CERTFILE "$WORKER_COUNT"
else
  printf '  ok   %-22s = config value on master and %d worker(s)\n' WOA23_TLS_KEYFILE  "$WORKER_COUNT"
  printf '  ok   %-22s = config value on master and %d worker(s)\n' WOA23_TLS_CERTFILE "$WORKER_COUNT"
fi
# ZERO WORKERS IS NOT A PASS. If the master forked none, the check above covered only the
# master and the workers that will serve were never inspected.
[ "$WORKER_COUNT" -gt 0 ] || die \
  "no worker processes were found under the master (pid $PID)." \
  "  The TLS environment check would then cover only the master, and the processes that" \
  "  actually serve would be unverified. Refusing to report that as a clean environment."
env_must WOA23_ANCHOR_REL        "ABSENT"
# WOA23_PYTHON is a REQUIRED EXACT VALUE now, not an absence. It used to be checked
# ABSENT, which forced production_app.sh onto its shared-pyenv default -- so pm2G built
# an isolated venv, manifested it, and served from the shared environment with zero
# libraries mapped from that venv. Absence was not neutral; it selected the wrong runtime.
env_must WOA23_PYTHON            "$VENV"
env_must WOA23_PRODUCTION_STORE  "ABSENT"
env_must WOA23_PM2C_GRANTED      "ABSENT"
if [ "$CFG_TLS" = "off" ]; then
  echo "  10 variables: TLS off — 5 with exact values, 5 required ABSENT"
  echo "  8 on the master; the 2 TLS paths on the master and every worker"
else
  echo "  10 variables: TLS on — 7 with exact values, 3 required ABSENT"
  echo "  8 on the master; the 2 TLS paths on the master and every worker"
fi
echo

# FAIL-CLOSED ON ANY UNLISTED WOA23_*. Listing an unexpected variable and carrying on is
# not a check — it is a note in a report that says PASS at the bottom. The case it would
# miss: a variable is added to production_app.sh, this entry is not updated, and the run
# reports a verified environment while the new variable's effect is unexamined.
ALLOWED_ENV='^(WOA23_PORT|WOA23_ZARR_STORE|WOA23_TLS|WOA23_WORKERS|WOA23_TLS_KEYFILE|WOA23_TLS_CERTFILE|WOA23_ANCHOR_REL|WOA23_PYTHON|WOA23_PRODUCTION_STORE|WOA23_PM2C_GRANTED)$'
UNLISTED="$(tr '\0' '\n' < "$PROC/$PID/environ" | grep -E '^WOA23_' | cut -d= -f1 \
            | LC_ALL=C sort -u | grep -vE "$ALLOWED_ENV" || true)"
if [ -n "$UNLISTED" ]; then
  printf '%s\n' "$UNLISTED" | sed 's/^/    /' >&2
  die "INVALID ENVIRONMENT: the process carries WOA23_* variables outside the allowlist" \
      "  of ten. This is a STOP and NO staging PASS is produced." \
      "  To allow one, add it to the request by name with its purpose and why it cannot" \
      "  affect the launcher, and add it here. Never at run time."
fi
echo "  allowlist check: no WOA23_* outside the ten — environment is valid"
echo
echo "argv READ BACK FROM $PROC/$PID/cmdline:"
CMD="$(tr '\0' ' ' < "$PROC/$PID/cmdline")"
echo "  $CMD"
case "$CMD" in *woa23_app*) die "STOP: woa23_app appears in argv" ;; esac
case "$CMD" in *--reload*)  die "STOP: --reload appears in argv" ;; esac
case "$CMD" in *api.app:app*) ;; *) die "STOP: api.app:app is not in argv" ;; esac
case "$CMD" in *--keyfile*) die "STOP: --keyfile in argv, but TLS is off" ;; esac
echo "  api.app:app present; no woa23_app, no --reload, no --keyfile"
echo

# ------------------------------------------------- THE RUNTIME IS THE VENV, PROVEN NOT ASSUMED
# pm2G is why this block exists. Every earlier check passed — the venv built, its
# interpreter was 3.11.4, its manifest recorded 58 packages, `WOA23_PYTHON` was verified
# absent exactly as the contract then demanded — and the service still served from the
# shared pyenv environment, with ZERO libraries mapped from the venv that had just been
# measured. Nothing in the run contradicted anything; the run simply never asked which
# libraries were loaded.
#
# So it asks. argv names the interpreter, and the memory maps say where the code came
# from — the second is what makes it evidence rather than intent.
echo "runtime provenance — the STAGED VENV must be what actually serves:"

# argv[0] is the interpreter PM2 was told to run: the venv's own python, by exact path.
ARGV0="$(tr '\0' '\n' < "$PROC/$PID/cmdline" | head -1)"
[ "$ARGV0" = "$VENV" ] || die \
  "STOP: argv[0] is not this run's venv interpreter." \
  "  want: $VENV" "  got : $ARGV0"
echo "  ok   argv[0] is the staged venv interpreter"

# `/proc/<pid>/exe` resolves THROUGH the venv symlink to the pyenv binary the venv was
# built from, so it is expected to differ from $VENV and is recorded, not asserted equal.
# Asserting equality here would fail every correct run.
echo "  exe  : $(readlink -f "$PROC/$PID/exe" 2>/dev/null || echo '(unreadable)')"
echo "         (resolves through the venv symlink to its base interpreter — expected)"

# THE ONE THAT WOULD HAVE CAUGHT pm2G: where the loaded libraries live.
VENV_SITE="$TREE/.venv/lib"
SHARED_RE='/\.pyenv/versions/[^/]*/envs/py311/'
prov_checked=0
for w in $(pgrep -P "$PID" 2>/dev/null); do
  [ -r "$PROC/$w/maps" ] || continue
  prov_checked=$((prov_checked + 1))
  from_venv="$(awk '{print $6}' "$PROC/$w/maps" | grep -c "^$VENV_SITE" || true)"
  from_shared="$(awk '{print $6}' "$PROC/$w/maps" | grep -cE "$SHARED_RE" || true)"
  from_prod="$(awk '{print $6}' "$PROC/$w/maps" | grep -c '/python/woa23/' || true)"
  echo "  worker $w: venv=$from_venv shared_py311=$from_shared production=$from_prod"
  [ "$from_shared" = 0 ] || die \
    "STOP: worker $w maps $from_shared librar(ies) from the SHARED py311 environment." \
    "  This is the pm2G failure exactly: an isolated venv is built and manifested, and" \
    "  the service runs on someone else's dependencies. The manifest would describe" \
    "  something that is not serving, which is worse than having no manifest."
  [ "$from_prod" = 0 ] || die \
    "STOP: worker $w maps $from_prod librar(ies) from the production tree."
  [ "$from_venv" -gt 0 ] || die \
    "STOP: worker $w maps NOTHING from this run's venv ($VENV_SITE)." \
    "  The venv was built and is not being used."
done
[ "$prov_checked" -gt 0 ] || die \
  "STOP: no worker maps could be read, so runtime provenance is unverified." \
  "  Refusing to report a venv as the runtime on the strength of argv alone."
echo "  $prov_checked worker(s): all libraries from this run's venv, none from the"
echo "  shared py311 environment, none from the production tree"
echo
echo "started and verified. Stop it with:"
# The stop needs its OWN grant, which this run's grant does not supply and must not be
# assumed to. It is shown here so the command is complete, not so it is automatic.
echo "  WOA23_B1_GRANTED=yes WOA23_PM2_HOME=$real_pm2home \\"
echo "    $TREE/deploy/production_stop.sh $APP"
