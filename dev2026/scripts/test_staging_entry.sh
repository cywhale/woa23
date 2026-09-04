#!/usr/bin/env bash
#
# deploy/staging_execute.sh — the two-phase execution entry, checked offline.
#
# Nothing is started, no PM2 is invoked, no port is bound. A FAKE pm2 on PATH records any
# invocation, so "PM2 was never called" is evidence rather than an assumption.
#
# THIS SUITE DRIVES THE REAL SEQUENCE, and that is the point of it existing in this shape.
# Its predecessor only ever invoked the entry against a staging root that did NOT exist,
# so it asserted "an existing root is refused" as correct while the actual run order —
# extract into the root, then start from it — was never exercised. The entry would have
# refused its own staged tree. Each piece was right; the composition was never tested.
#
# So every case here goes through: root absent -> extract the authorised archive -> verify
# the staged tree -> build the store -> generate and diff the config -> run the entry.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENTRY="$HERE/deploy/staging_execute.sh"
REPO="$(cd "$HERE/.." && pwd)"

PASS=0; FAIL=0
check() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}

if ! command -v node >/dev/null 2>&1; then
  echo "node unavailable; the entry cannot generate a config."
  echo "all skipped (0 assertions)"; suite_summary_line 0 0; exit 0
fi
if ! git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1; then
  echo "no repository; this suite stages from git archive."
  echo "all skipped (0 assertions)"; suite_summary_line 0 0; exit 0
fi

WORK="$(mktemp -d)"
cleanup() { chmod -R u+w "$WORK" 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# ------------------------------------------------------------- the authorised archive
# HEAD is used as the subject: this suite tests the MECHANISM, and the mechanism must work
# for whatever tree it is pointed at. The digests are derived here, exactly as the real
# procedure derives them, rather than pasted in — a pasted digest would go stale.
ARCHIVE="$WORK/subject.tar"
git -C "$REPO" archive --format=tar HEAD dev2026 > "$ARCHIVE" 2>/dev/null \
  || { echo "cannot build an archive from HEAD"; echo "all skipped (0 assertions)"; suite_summary_line 0 0; exit 0; }

# The authorised file count and file-list digest, computed the way the entry computes them.
ref_root="$WORK/ref"; mkdir -p "$ref_root"
tar -x -f "$ARCHIVE" -C "$ref_root"
REF_LIST="$WORK/ref.filelist"
( cd "$ref_root/dev2026" && LC_ALL=C find . -type f \
    -not -path './.venv/*' -not -path '*/__pycache__/*' \
    -not -path './tmp-*' -not -path './tmp-*/*' \
    -not -name 'ecosystem.*.config.js' -o \
    -type f -name 'ecosystem.production.config.js' -o \
    -type f -name 'ecosystem.staging.config.js' \
  | sed 's|^\./||' | LC_ALL=C sort -u \
  | while IFS= read -r f; do printf '%s  %s\n' "$(sha256_of "$f")" "$f"; done ) > "$REF_LIST"
FILES="$(wc -l < "$REF_LIST" | tr -d ' ')"
LISTSHA="$(sha256_of "$REF_LIST")"
echo "subject under test: $FILES files, file-list ${LISTSHA:0:16}…"
echo

# --------------------------------------------------------------------------- fake pm2
# The fake `pm2 jlist` shape is parameterized by FAKE_JLIST_MODE so the entry's
# real post-start pid parser can be driven through every case PM2 could produce.
# Default is `name_first` — the historical order, the one the old awk parser
# assumed and the one pm2E's launch used. `pid_first` is what PM2 5.4.2 actually
# emits and what stopped pm2F. The remaining modes exercise the refusal paths.
PM2LOG="$WORK/pm2-invocations.txt"; : > "$PM2LOG"
PM2ENV="$WORK/pm2-start-env.txt"; : > "$PM2ENV"
mkdir -p "$WORK/bin"
cat > "$WORK/bin/pm2" <<'PM2EOF'
#!/usr/bin/env bash
echo "$@" >> "PM2LOG_PATH"
# THE STUB RECORDS ITS OWN ENVIRONMENT ON `start`. pm2 start spawns the God Daemon with
# this environment and the daemon hands it to the app, so what the stub sees is exactly
# what an inherited variable would ride in on. This is the functional evidence that the
# entry's `unset` happens BEFORE the spawn, rather than being asserted from the source.
if [ "$1" = "start" ]; then
  env | LC_ALL=C sort > "PM2ENV_PATH"
fi
if [ "$1" = "jlist" ]; then
  APP="${FAKE_APP:-x}"
  case "${FAKE_JLIST_MODE:-name_first}" in
    name_first)  printf '[{"name":"%s","pid":4242,"pm2_env":{"status":"online"}}]\n' "$APP" ;;
    pid_first)   printf '[{"pid":4242,"name":"%s","pm2_env":{"status":"online"}}]\n' "$APP" ;;
    missing_pid) printf '[{"name":"%s","pm2_env":{"status":"online"}}]\n' "$APP" ;;
    empty)       printf '[]\n' ;;
    multiple)    printf '[{"pid":4242,"name":"%s"},{"pid":9999,"name":"%s"}]\n' "$APP" "$APP" ;;
    malformed)   printf 'not valid json' ;;
    wrong_app)   printf '[{"pid":4242,"name":"some-other-app","pm2_env":{"status":"online"}}]\n' ;;
  esac
fi
exit 0
PM2EOF
sed -i.bak -e "s|PM2LOG_PATH|$PM2LOG|" -e "s|PM2ENV_PATH|$PM2ENV|" "$WORK/bin/pm2" \
  && rm "$WORK/bin/pm2.bak"
chmod +x "$WORK/bin/pm2"

G=WOA23_PM2C_GRANTED=yes
# A UNIQUE, NON-EXISTENT path per call, and it must not depend on shell state: this is
# called inside $( ), which is a subshell, so an incrementing counter would never advance
# in the parent and every call would return the SAME path. That is precisely what happened
# — a dozen cases failed because they were all staging into one root. It is the same
# subshell trap that has now bitten this campaign three times.
newroot() { mktemp -u "$WORK/rXXXXXX"; }

stage() {   # stage <root> [archive] [label] -> rc ; output in $WORK/last.out
  local root="$1" arch="${2:-$ARCHIVE}" lbl="${3:-L$(basename "$root")}"
  env -i PATH="$PATH" HOME="$WORK" $G bash "$ENTRY" --phase stage \
      --root "$root" --label "$lbl" --pm2-home "$root-pm2" \
      --archive "$arch" --files "$FILES" --filelist "$LISTSHA" \
      > "$WORK/last.out" 2>&1
  echo $?
}

# A venv the store builder can actually use: the repository's own interpreter.
wire_venv() {   # wire_venv <root>
  local tree="$1/dev2026"
  mkdir -p "$tree/.venv/bin"
  printf '#!/usr/bin/env bash\nexec %s/dev2026/.venv/bin/python "$@"\n' "$REPO" > "$tree/.venv/bin/python"
  chmod +x "$tree/.venv/bin/python"
}

run() {     # run <root> <label> [extra args...] -> rc
  local root="$1" label="$2"; shift 2
  env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G FAKE_APP="c-$label" \
      PROC_ROOT="${RUN_PROC:-/proc}" WOA23_START_SETTLE=0 \
      bash "$ENTRY" --phase run --root "$root" --label "$label" \
      --pm2-home "$WORK/$label-pm2" --port 39263 --app "c-$label" \
      --files "$FILES" --filelist "$LISTSHA" "$@" \
      > "$WORK/last.out" 2>&1
  echo $?
}

echo "the two phases are named explicitly; there is no default"
r="$(newroot)"
out="$(env -i PATH="$PATH" HOME="$WORK" $G bash "$ENTRY" --root "$r" --files "$FILES" \
        --filelist "$LISTSHA" 2>&1)"; rc=$?
check "a missing --phase is refused" "2" "$rc"
check "  and says the two phases have opposite rules" "yes" \
      "$(echo "$out" | grep -q "OPPOSITE rules" && echo yes || echo no)"
out="$(env -i PATH="$PATH" HOME="$WORK" $G bash "$ENTRY" --phase go --root "$r" \
        --files "$FILES" --filelist "$LISTSHA" 2>&1)"; rc=$?
check "an unknown phase is refused" "2" "$rc"

echo
echo "the grant is enforced in BOTH phases"
r="$(newroot)"
out="$(env -i PATH="$PATH" HOME="$WORK" bash "$ENTRY" --phase stage --root "$r" \
        --archive "$ARCHIVE" --files "$FILES" --filelist "$LISTSHA" 2>&1)"; rc=$?
check "stage without the grant is refused" "2" "$rc"
check "  and the root was NOT created" "no" "$([ -e "$r" ] && echo yes || echo no)"
r2="$(newroot)"
out="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" bash "$ENTRY" --phase run --root "$r2" \
        --label x --pm2-home "$WORK/x-pm2" --port 39263 --app c-x \
        --files "$FILES" --filelist "$LISTSHA" 2>&1)"; rc=$?
check "run without the grant is refused" "2" "$rc"
check "  and pm2 was never invoked" "0" "$(wc -l < "$PM2LOG" | tr -d ' ')"
for other in WOA23_S2PERF_GRANTED WOA23_D1_GRANTED WOA23_BASH5_VERIFY_GRANTED; do
  r3="$(newroot)"
  out="$(env -i PATH="$PATH" HOME="$WORK" $G "$other=yes" bash "$ENTRY" --phase stage \
          --root "$r3" --archive "$ARCHIVE" --files "$FILES" --filelist "$LISTSHA" 2>&1)"; rc=$?
  check "$other alongside refuses (stage)" "2" "$rc"
done

echo
echo "PHASE A — before extraction the staging root must NOT exist"
r="$(newroot)"
check "a fresh root stages cleanly" "0" "$(stage "$r")"
check "  and reports it verified the subject file for file" "yes" \
      "$(grep -q 'STAGED and VERIFIED' "$WORK/last.out" && echo yes || echo no)"
check "  the tree is there" "yes" "$([ -d "$r/dev2026/deploy" ] && echo yes || echo no)"
# Staging the SAME root twice must refuse — and must not touch what is already there.
before="$(sha256_of "$r/dev2026/deploy/production_app.sh")"
check "staging an EXISTING root is refused" "2" "$(stage "$r")"
check "  it says the root is not deleted or reused" "yes" \
      "$(grep -q 'NOT deleted' "$WORK/last.out" && echo yes || echo no)"
check "  and the existing tree is untouched" "$before" \
      "$(sha256_of "$r/dev2026/deploy/production_app.sh")"

echo
echo "PHASE A — the staged tree must BE the authorised subject"
# partial extraction: an archive missing files
r="$(newroot)"
PART="$WORK/partial.tar"
( cd "$ref_root" && tar -c -f "$PART" dev2026/deploy dev2026/api )
check "a PARTIAL archive is refused" "2" "$(stage "$r" "$PART")"
check "  named as a file-count mismatch" "yes" \
      "$(grep -q 'SUBJECT MISMATCH' "$WORK/last.out" && echo yes || echo no)"
check "  and the partial tree is left for inspection" "yes" \
      "$([ -d "$r/dev2026" ] && echo yes || echo no)"
# stale subject: a DIFFERENT commit's archive, verified against this one's digests.
#
# HEAD~1 IS NOT NECESSARILY A DIFFERENT SUBJECT. This case used to archive HEAD~1 and
# assume it differed. It does not when the previous commit touched only paths OUTSIDE
# dev2026 -- a docs-only or runs-only commit leaves `git archive HEAD~1 dev2026` byte
# identical to HEAD's, so the "stale" archive is the authorised one and is correctly
# ACCEPTED. The case then failed for a reason that had nothing to do with the entry point:
# it was measuring repository history, not the guard.
#
# The commit is now SELECTED for the property the case needs -- the newest one whose
# dev2026 tree actually differs -- and if there is none the case SKIPS LOUDLY rather than
# passing on an assumption.
r="$(newroot)"
STALE="$WORK/stale.tar"
HEAD_TREE="$(git -C "$REPO" rev-parse 'HEAD:dev2026' 2>/dev/null || true)"
STALE_REV=""
if [ -n "$HEAD_TREE" ]; then
  for rev in $(git -C "$REPO" rev-list --max-count=40 HEAD~1 2>/dev/null); do
    t="$(git -C "$REPO" rev-parse "$rev:dev2026" 2>/dev/null || true)"
    [ -n "$t" ] && [ "$t" != "$HEAD_TREE" ] && { STALE_REV="$rev"; break; }
  done
fi
if [ -n "$STALE_REV" ]; then
  git -C "$REPO" archive --format=tar "$STALE_REV" dev2026 > "$STALE"
  check "a STALE subject (a commit with a DIFFERENT dev2026 tree) is refused" "2" \
        "$(stage "$r" "$STALE")"
  check "  named as a subject mismatch" "yes" \
        "$(grep -q 'SUBJECT MISMATCH' "$WORK/last.out" && echo yes || echo no)"
else
  FAIL=$((FAIL+1))
  echo "  FAIL no ancestor within 40 commits has a different dev2026 tree —"
  echo "       the stale-subject case could not be constructed and is NOT counted as passed"
fi
# foreign tree: right file COUNT, wrong contents
r="$(newroot)"
FOREIGN="$WORK/foreign"; mkdir -p "$FOREIGN"
tar -x -f "$ARCHIVE" -C "$FOREIGN"
printf 'not the subject\n' > "$FOREIGN/dev2026/deploy/production_app.sh"
FTAR="$WORK/foreign.tar"; ( cd "$FOREIGN" && tar -c -f "$FTAR" dev2026 )
check "a FOREIGN/modified tree with the right count is refused" "2" "$(stage "$r" "$FTAR")"
check "  and the message distinguishes it from a partial one" "yes" \
      "$(grep -q 'modified, stale or foreign' "$WORK/last.out" && echo yes || echo no)"

echo
echo "PHASE B — the root must EXIST and must still be the subject"
r="$(newroot)"
check "run against a NONEXISTENT root is refused" "2" "$(run "$r" nx)"
check "  and says to stage first" "yes" \
      "$(grep -q 'Run --phase stage first' "$WORK/last.out" && echo yes || echo no)"
# THE REAL SEQUENCE.
RROOT="$(newroot)"
check "stage the real subject" "0" "$(stage "$RROOT")"
wire_venv "$RROOT"
: > "$PM2LOG"
check "then RUN against that staged root — preflight" "0" "$(run "$RROOT" pm2T --preflight-only)"
check "  provenance verified against the authorised digest" "yes" \
      "$(grep -q 'verified as the authorised subject' "$WORK/last.out" && echo yes || echo no)"
check "  the store was built and made read-only" "yes" \
      "$(grep -q '72 files, anchor present, read-only' "$WORK/last.out" && echo yes || echo no)"
check "  the config diff showed the 6 permitted items" "yes" \
      "$(grep -q '6 permitted items' "$WORK/last.out" && echo yes || echo no)"
check "  it stopped before pm2 start" "yes" \
      "$(grep -q 'PREFLIGHT ONLY' "$WORK/last.out" && echo yes || echo no)"
check "  PM2 WAS NEVER INVOKED" "0" "$(wc -l < "$PM2LOG" | tr -d ' ')"
check "  the generated config is in the STAGED tree, not the repository" "yes" \
      "$([ -f "$RROOT/dev2026/deploy/ecosystem.pm2T.config.js" ] && echo yes || echo no)"
check "  and the repository's deploy/ was not written to" "no" \
      "$(ls "$HERE"/deploy/ecosystem.pm2T.config.js >/dev/null 2>&1 && echo yes || echo no)"

# A MODIFIED file after staging must fail the run phase, not just the stage phase.
MOD="$(newroot)"
check "stage a second tree" "0" "$(stage "$MOD")"
wire_venv "$MOD"
printf '\n# tampered\n' >> "$MOD/dev2026/deploy/production_app.sh"
check "a file MODIFIED after staging fails the run phase" "2" "$(run "$MOD" pm2M --preflight-only)"
check "  named as a provenance failure" "yes" \
      "$(grep -q 'SUBJECT PROVENANCE FAILED' "$WORK/last.out" && echo yes || echo no)"
check "  refusing to start PM2" "yes" \
      "$(grep -q 'refusing to start PM2' "$WORK/last.out" && echo yes || echo no)"
check "  and nothing is deleted or repaired" "yes" \
      "$(grep -q 'left for inspection' "$WORK/last.out" && echo yes || echo no)"
# A root that exists but is not a staged subject at all.
NOTSUB="$(newroot)"; mkdir -p "$NOTSUB"
check "a root that is not a staged subject is refused" "2" "$(run "$NOTSUB" pm2N --preflight-only)"

echo
echo "PHASE B — everything THIS run creates must be fresh"
for what in pm2-home store config logdir; do
  RR="$(newroot)"
  stage "$RR" >/dev/null; wire_venv "$RR"
  lbl="pre$(basename "$RR")"
  case "$what" in
    pm2-home) mkdir -p "$WORK/$lbl-pm2" ;;
    workdir)  mkdir -p "${RR}-work" ;;
    store)    mkdir -p "$RR/store" ;;
    config)   printf 'x\n' > "$RR/dev2026/deploy/ecosystem.$lbl.config.js" ;;
    logdir)   mkdir -p "$RR/dev2026/tmp-$lbl" ;;
  esac
  rc="$(run "$RR" "$lbl" --preflight-only)"
  check "an existing $what is refused" "2" "$rc"
  check "  and is NOT deleted or reused" "yes" \
        "$(grep -q 'NOT deleted or reused' "$WORK/last.out" && echo yes || echo no)"
done

echo
echo "the run phase cannot be aimed at production"
RR="$(newroot)"; stage "$RR" >/dev/null; wire_venv "$RR"
rc="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G bash "$ENTRY" --phase run --root "$RR" \
        --label pz --pm2-home "$WORK/pz-pm2" --port 39263 --app woa23 --files "$FILES" \
        --filelist "$LISTSHA" --preflight-only > "$WORK/last.out" 2>&1; echo $?)"
check "the production app name is refused" "2" "$rc"
for p in 8050 8786 8787; do
  RR="$(newroot)"; stage "$RR" >/dev/null; wire_venv "$RR"
  rc="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G bash "$ENTRY" --phase run --root "$RR" \
          --label "pp$p" --pm2-home "$WORK/pp$p-pm2" --port "$p" --app "c-pp$p" \
          --files "$FILES" --filelist "$LISTSHA" --preflight-only > "$WORK/last.out" 2>&1; echo $?)"
  check "production port $p is refused" "2" "$rc"
done
RR="$(newroot)"; stage "$RR" >/dev/null; wire_venv "$RR"; mkdir -p "$WORK/.pm2"
rc="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G bash "$ENTRY" --phase run --root "$RR" \
        --label ph --pm2-home "$WORK/.pm2" --port 39263 --app c-ph --files "$FILES" \
        --filelist "$LISTSHA" --preflight-only > "$WORK/last.out" 2>&1; echo $?)"
check "production's PM2_HOME is refused" "2" "$rc"

echo
echo "the authorised digests must be supplied, and must be well formed"
RR="$(newroot)"
rc="$(env -i PATH="$PATH" HOME="$WORK" $G bash "$ENTRY" --phase stage --root "$RR" \
        --archive "$ARCHIVE" --filelist "$LISTSHA" > "$WORK/last.out" 2>&1; echo $?)"
check "a missing --files is refused" "2" "$rc"
rc="$(env -i PATH="$PATH" HOME="$WORK" $G bash "$ENTRY" --phase stage --root "$RR" \
        --archive "$ARCHIVE" --files "$FILES" > "$WORK/last.out" 2>&1; echo $?)"
check "a missing --filelist is refused" "2" "$rc"
rc="$(env -i PATH="$PATH" HOME="$WORK" $G bash "$ENTRY" --phase stage --root "$RR" \
        --archive "$ARCHIVE" --files "$FILES" --filelist deadbeef > "$WORK/last.out" 2>&1; echo $?)"
check "a short --filelist is refused" "2" "$rc"
check "  and none of those created the root" "no" "$([ -e "$RR" ] && echo yes || echo no)"

echo
echo "THE REAL LIFECYCLE — extract, venv with a SEPARATE cache, then the entry"
# This is the sequence pm2E actually ran, and the one its predecessor never tested. The
# venv is built with UV_CACHE_DIR pointing OUTSIDE the workdir, and the entry then creates
# the workdir itself and stamps it.
LR="$(newroot)"
check "stage" "0" "$(stage "$LR" "$ARCHIVE" LIFE)"
mkdir -p "$LR-uvcache"                    # the task-specific cache, NOT the workdir
wire_venv "$LR"
check "  the workdir is still absent after the venv step" "no" \
      "$([ -e "$LR-work" ] && echo yes || echo no)"
: > "$PM2LOG"
rc="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G FAKE_APP=c-LIFE WOA23_START_SETTLE=0 \
        bash "$ENTRY" --phase run --root "$LR" --label LIFE --pm2-home "$LR-pm2" \
        --port 39264 --app c-LIFE --files "$FILES" --filelist "$LISTSHA" \
        --preflight-only > "$WORK/last.out" 2>&1; echo $?)"
check "the entry runs against that staged tree" "0" "$rc"
check "  and CREATED the workdir itself" "yes" "$([ -d "$LR-work" ] && echo yes || echo no)"
check "  stamping it with this run's identity" "yes" \
      "$([ -f "$LR-work/.run-identity" ] && echo yes || echo no)"
check "  the marker names this label" "yes" \
      "$(grep -q '^label=LIFE$' "$LR-work/.run-identity" && echo yes || echo no)"
check "  and this subject" "yes" \
      "$(grep -q "^subject-filelist=$LISTSHA\$" "$LR-work/.run-identity" && echo yes || echo no)"
check "  it says it created the workdir" "yes" \
      "$(grep -q 'created by this run, stamped' "$WORK/last.out" && echo yes || echo no)"
check "  PM2 was never invoked" "0" "$(wc -l < "$PM2LOG" | tr -d ' ')"
check "  the uv cache is outside the workdir" "no" \
      "$([ -e "$LR-work/uv-cache" ] && echo yes || echo no)"

echo
echo "the workdir is OWNED — a correctly stamped one is adopted, everything else refused"
# The run phase is NOT idempotent: the store and the generated config must be fresh, so a
# straight second run is refused by those guards — correctly. The marker-matches path is
# reachable only when a run was interrupted AFTER creating the workdir and BEFORE building
# the store, which is what this models: a stamped workdir and nothing else.
OR="$(newroot)"
stage "$OR" "$ARCHIVE" OWN >/dev/null; wire_venv "$OR"
mkdir -p "$OR-work"
printf 'run-identity-v1\nlabel=OWN\nsubject-filelist=%s\napp=c-OWN\nport=39264\n' \
  "$LISTSHA" > "$OR-work/.run-identity"
rc="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G FAKE_APP=c-OWN WOA23_START_SETTLE=0 \
        bash "$ENTRY" --phase run --root "$OR" --label OWN --pm2-home "$OR-pm2" \
        --port 39264 --app c-OWN --files "$FILES" --filelist "$LISTSHA" \
        --preflight-only > "$WORK/last.out" 2>&1; echo $?)"
check "a workdir correctly stamped for THIS run is adopted" "0" "$rc"
check "  and the entry says the marker matched" "yes" \
      "$(grep -q 'marker matches' "$WORK/last.out" && echo yes || echo no)"

# THE pm2E CASE: an UNSTAMPED workdir, exactly as a uv cache would leave it.
UR="$(newroot)"
stage "$UR" "$ARCHIVE" UNST >/dev/null; wire_venv "$UR"
mkdir -p "$UR-work/uv-cache"; printf 'x\n' > "$UR-work/manifest-full.txt"
rc="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G WOA23_START_SETTLE=0 \
        bash "$ENTRY" --phase run --root "$UR" --label UNST --pm2-home "$UR-pm2" \
        --port 39264 --app c-UNST --files "$FILES" --filelist "$LISTSHA" \
        --preflight-only > "$WORK/last.out" 2>&1; echo $?)"
check "an UNSTAMPED workdir (the pm2E case) is refused" "2" "$rc"
check "  named as carrying no run-identity marker" "yes" \
      "$(grep -q 'no run-identity marker' "$WORK/last.out" && echo yes || echo no)"
check "  it names pm2E as the precedent" "yes" \
      "$(grep -q 'ended pm2E' "$WORK/last.out" && echo yes || echo no)"
check "  and nothing in it was deleted" "yes" \
      "$([ -f "$UR-work/manifest-full.txt" ] && echo yes || echo no)"

# A workdir stamped by a DIFFERENT run.
FR="$(newroot)"
stage "$FR" "$ARCHIVE" FGN >/dev/null; wire_venv "$FR"
mkdir -p "$FR-work"
printf 'run-identity-v1\nlabel=SOMEONE_ELSE\nsubject-filelist=%s\napp=c-other\nport=19999\n' \
  "$LISTSHA" > "$FR-work/.run-identity"
rc="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G WOA23_START_SETTLE=0 \
        bash "$ENTRY" --phase run --root "$FR" --label FGN --pm2-home "$FR-pm2" \
        --port 39264 --app c-FGN --files "$FILES" --filelist "$LISTSHA" \
        --preflight-only > "$WORK/last.out" 2>&1; echo $?)"
check "a workdir stamped by ANOTHER run is refused" "2" "$rc"
check "  named as belonging to a different run" "yes" \
      "$(grep -q 'belongs to a DIFFERENT run' "$WORK/last.out" && echo yes || echo no)"
check "  and the foreign marker is left alone" "yes" \
      "$(grep -q 'SOMEONE_ELSE' "$FR-work/.run-identity" && echo yes || echo no)"

# A STALE marker: right label, wrong subject — a workdir from an earlier subject.
SR="$(newroot)"
stage "$SR" "$ARCHIVE" STL >/dev/null; wire_venv "$SR"
mkdir -p "$SR-work"
printf 'run-identity-v1\nlabel=STL\nsubject-filelist=%s\napp=c-STL\nport=39264\n' \
  0000000000000000000000000000000000000000000000000000000000000000 > "$SR-work/.run-identity"
rc="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G WOA23_START_SETTLE=0 \
        bash "$ENTRY" --phase run --root "$SR" --label STL --pm2-home "$SR-pm2" \
        --port 39264 --app c-STL --files "$FILES" --filelist "$LISTSHA" \
        --preflight-only > "$WORK/last.out" 2>&1; echo $?)"
check "a STALE marker (different subject) is refused" "2" "$rc"
# A PARTIAL marker: truncated file.
PR="$(newroot)"
stage "$PR" "$ARCHIVE" PRT >/dev/null; wire_venv "$PR"
mkdir -p "$PR-work"; printf 'run-identity-v1\nlabel=PRT\n' > "$PR-work/.run-identity"
rc="$(env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G WOA23_START_SETTLE=0 \
        bash "$ENTRY" --phase run --root "$PR" --label PRT --pm2-home "$PR-pm2" \
        --port 39264 --app c-PRT --files "$FILES" --filelist "$LISTSHA" \
        --preflight-only > "$WORK/last.out" 2>&1; echo $?)"
check "a PARTIAL/truncated marker is refused" "2" "$rc"

echo
echo "PHASE A now checks the WHOLE identity, not only the root"
for pre in work pm2; do
  XR="$(newroot)"
  case "$pre" in
    work) mkdir -p "$XR-work" ;;
    pm2)  mkdir -p "$XR-pm2" ;;
  esac
  rc="$(stage "$XR")"
  check "stage refuses when the $pre path already exists" "2" "$rc"
  check "  and lists the whole identity" "yes" \
        "$(grep -q 'Every element of this run.s identity' "$WORK/last.out" && echo yes || echo no)"
  check "  the root was NOT created" "no" "$([ -e "$XR" ] && echo yes || echo no)"
done
check "the stage phase tells the operator where to put the uv cache" "yes" \
      "$(grep -q 'uvcache' "$WORK/last.out" 2>/dev/null && echo yes ||          { XX="$(newroot)"; stage "$XX" >/dev/null; grep -q 'uvcache' "$WORK/last.out" && echo yes || echo no; })"
check "  and warns the workdir must still not exist" "yes" \
      "$(grep -q 'MUST STILL NOT EXIST' "$WORK/last.out" && echo yes || echo no)"

echo
echo "pid extraction from pm2 jlist — the pm2F defect, and beyond"
# The pm2F case: pm2 5.4.2 emits "pid" BEFORE "name" in each jlist object; the
# old awk parser needed "name" first to set an in-app flag, so it returned an
# empty pid and the entry died on "pm2 reports no pid" while the service was
# running fine. The parser was replaced with `node -e` that parses jlist as
# JSON and matches by `name`.
#
# These tests drive the ENTRY (not a helper): they go PAST --preflight-only so
# the entry actually reaches the post-start pid-extraction line. The fake pm2
# returns pid=4242, which does not correspond to any real process on the host;
# after extraction the entry tries to read /proc/4242/environ and dies there.
# That is FINE — the assertion is on what the parser produced BEFORE that
# /proc step: either "pid: 4242" was printed (extraction worked) or the entry
# refused earlier with "pm2 reports no pid" (extraction correctly returned
# empty). The /proc failure is stable across every case, so it does not skew
# the pass/fail signal.

run_extract() {   # run_extract <root> <label> <jlist-mode> -> rc
  local root="$1" label="$2" mode="$3"
  env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G FAKE_APP="c-$label" \
      FAKE_JLIST_MODE="$mode" \
      PROC_ROOT="${RUN_PROC:-/proc}" WOA23_START_SETTLE=0 \
      bash "$ENTRY" --phase run --root "$root" --label "$label" \
      --pm2-home "$WORK/$label-pm2" --port 39265 --app "c-$label" \
      --files "$FILES" --filelist "$LISTSHA" \
      > "$WORK/last.out" 2>&1
  echo $?
}

# THE pm2F CASE — pid appears before name in the jlist JSON.
JR="$(newroot)"; stage "$JR" "$ARCHIVE" JPID >/dev/null; wire_venv "$JR"
: > "$PM2LOG"; run_extract "$JR" JPID pid_first >/dev/null
check "pid-before-name: parser extracts the correct pid (the pm2F case)" "yes" \
      "$(grep -q '^pid: 4242$' "$WORK/last.out" && echo yes || echo no)"
check "  and the extraction ran AFTER pm2 start (so it was the entry, not a helper)" "yes" \
      "$(grep -qE '^start ' "$PM2LOG" && grep -qxE 'jlist' "$PM2LOG" && echo yes || echo no)"
check "  the entry did reach the /proc read (proves it got past extraction)" "yes" \
      "$(grep -q 'cannot read' "$WORK/last.out" && echo yes || echo no)"

# name-before-pid — the historical order, still handled by JSON.
JR2="$(newroot)"; stage "$JR2" "$ARCHIVE" JNM >/dev/null; wire_venv "$JR2"
run_extract "$JR2" JNM name_first >/dev/null
check "name-before-pid: parser extracts the correct pid" "yes" \
      "$(grep -q '^pid: 4242$' "$WORK/last.out" && echo yes || echo no)"

# missing pid — the object exists but has no pid.
JR3="$(newroot)"; stage "$JR3" "$ARCHIVE" JMS >/dev/null; wire_venv "$JR3"
rc="$(run_extract "$JR3" JMS missing_pid)"
check "missing pid: refused" "2" "$rc"
check "  named as 'pm2 reports no pid'" "yes" \
      "$(grep -q 'pm2 reports no pid' "$WORK/last.out" && echo yes || echo no)"
check "  and never printed 'pid:'" "no" \
      "$(grep -q '^pid: ' "$WORK/last.out" && echo yes || echo no)"

# empty jlist — no apps at all.
JR4="$(newroot)"; stage "$JR4" "$ARCHIVE" JEM >/dev/null; wire_venv "$JR4"
rc="$(run_extract "$JR4" JEM empty)"
check "empty jlist: refused" "2" "$rc"
check "  named as 'pm2 reports no pid'" "yes" \
      "$(grep -q 'pm2 reports no pid' "$WORK/last.out" && echo yes || echo no)"

# multiple matches — two apps share the intended name. The current parser
# takes the FIRST; this is a known limitation and is asserted explicitly so
# the day it changes, the test says so.
JR5="$(newroot)"; stage "$JR5" "$ARCHIVE" JML >/dev/null; wire_venv "$JR5"
run_extract "$JR5" JML multiple >/dev/null
check "multiple matches: parser takes the FIRST (documented limitation)" "yes" \
      "$(grep -q '^pid: 4242$' "$WORK/last.out" && echo yes || echo no)"
check "  and did not take the second (9999)" "no" \
      "$(grep -q '^pid: 9999$' "$WORK/last.out" && echo yes || echo no)"

# malformed JSON — JSON.parse throws, node exits non-zero, pid is empty.
JR6="$(newroot)"; stage "$JR6" "$ARCHIVE" JMF >/dev/null; wire_venv "$JR6"
rc="$(run_extract "$JR6" JMF malformed)"
check "malformed jlist: refused" "2" "$rc"
check "  named as 'pm2 reports no pid'" "yes" \
      "$(grep -q 'pm2 reports no pid' "$WORK/last.out" && echo yes || echo no)"

# wrong app name — an object present but its name does not match --app.
JR7="$(newroot)"; stage "$JR7" "$ARCHIVE" JWA >/dev/null; wire_venv "$JR7"
rc="$(run_extract "$JR7" JWA wrong_app)"
check "wrong app name: refused" "2" "$rc"
check "  named as 'pm2 reports no pid'" "yes" \
      "$(grep -q 'pm2 reports no pid' "$WORK/last.out" && echo yes || echo no)"

# Source-level hygiene: the parser must be node-based JSON, not the old awk.
check "the entry parses jlist with 'node -e' (JSON), not by field order" "yes" \
      "$(grep -qE 'jlist.*node -e|node -e.*JSON' "$ENTRY" && echo yes || echo no)"
check "  and it must not use the old awk-based field-order parser" "no" \
      "$(grep -qE 'awk.*inapp.*name|/\"name\"/.*inapp' "$ENTRY" && echo yes || echo no)"

echo
echo "WOA23_PYTHON is a required VALUE, and the venv is proven to serve (spec 016)"
# pm2G's other finding: the run built an isolated venv, manifested 58 packages for it,
# verified WOA23_PYTHON ABSENT exactly as the contract then demanded — and served from the
# shared py311 environment with ZERO libraries mapped from that venv. Absence was not
# neutral; absence SELECTED the shared env, and the contract required absence, so the
# fallback was the only reachable behaviour.
check "the entry passes --python to the generator" "yes" \
      "$(grep -q -- '--python "\$VENV"' "$ENTRY" && echo yes || echo no)"
check "WOA23_PYTHON is checked as an EXACT VALUE, not ABSENT" "yes" \
      "$(grep -qE 'env_must WOA23_PYTHON +"\$VENV"' "$ENTRY" && echo yes || echo no)"
check "  and is no longer required absent" "no" \
      "$(grep -qE 'env_must WOA23_PYTHON +"ABSENT"' "$ENTRY" && echo yes || echo no)"
# The split is now MODE-DEPENDENT: TLS off omits the key and certificate, so they move
# from "exact" to "required ABSENT". A single hard-coded string would be wrong in one of
# the two modes -- this assertion previously pinned the TLS-on split and went red the
# moment TLS-off omission landed.
#
# So both splits must be stated, AND the stated totals are checked against the env_must
# calls that are actually there, rather than being taken on the comment's word.
check "both TLS splits are stated" "yes" \
      "$(grep -q 'TLS off — 5 with exact values, 5 required ABSENT' "$ENTRY" \
         && grep -q 'TLS on — 7 with exact values, 3 required ABSENT' "$ENTRY" \
         && echo yes || echo no)"
# TEN variables, but they are NOT all checked the same way, and the test says so rather
# than counting one construct and calling it the total. EIGHT go through `env_must`, which
# reads the MASTER only. The two TLS paths are checked separately across the master and
# every worker, so counting `env_must` alone would report 8 and miss exactly the two the
# review is about.
check "  8 variables go through env_must on the master" 8 \
      "$(grep -cE '^ *env_must WOA23_' "$ENTRY")"
check "  and the TLS paths are NOT among them" 0 \
      "$(grep -cE '^ *env_must WOA23_TLS_(KEY|CERT)FILE' "$ENTRY")"
check "  because they are checked across master AND workers instead" "yes" \
      "$(grep -q 'for tp in \$TLS_PIDS' "$ENTRY" && echo yes || echo no)"
check "  over a list built from the master plus its children" "yes" \
      "$(grep -q 'TLS_PIDS="\$PID"' "$ENTRY" \
         && grep -q 'children_of_pid "\$PID"' "$ENTRY" && echo yes || echo no)"
check "  8 + 2 = the 10 the summary claims" 10 \
      "$(( $(grep -cE '^ *env_must WOA23_' "$ENTRY") + 2 ))"
check "WOA23_PYTHON is still inside the allowlist" "yes" \
      "$(grep -q 'WOA23_PYTHON' <<<"$(grep '^ALLOWED_ENV=' "$ENTRY")" && echo yes || echo no)"

echo
echo "TLS INHERITANCE — an ancestor's exported paths must NOT reach the child"
# THE POINT OF THIS BLOCK. Omitting the keys from the generated config never removed an
# inherited value: PM2 merges an app's `env` over the God Daemon's environment, and the
# daemon inherits the environment of whatever ran `pm2 start`. So the config can omit the
# paths while the process still carries them -- which is bs3v1's exposure shape.
#
# The /proc check is a DETECTOR. This tests the PREVENTER: the entry must unset both
# variables BEFORE spawning pm2. The fake pm2 records its own environment on `start`, and
# that environment is exactly what the daemon would pass down, so this is functional
# evidence rather than a source grep.
#
# The ancestor is simulated by exporting the production paths into the entry's environment
# -- NOT with `env -i`, which would wipe them and make the test pass for no reason.
run_with_ancestor() {   # run_with_ancestor <root> <label> -> rc
  local root="$1" label="$2"
  env -i PATH="$WORK/bin:$PATH" HOME="$WORK" $G FAKE_APP="c-$label" \
      PROC_ROOT="${RUN_PROC:-/proc}" WOA23_START_SETTLE=0 \
      WOA23_TLS_KEYFILE=/home/odbadmin/python/woa23/conf/privkey.pem \
      WOA23_TLS_CERTFILE=/home/odbadmin/python/woa23/conf/fullchain.pem \
      bash "$ENTRY" --phase run --root "$root" --label "$label" \
      --pm2-home "$WORK/$label-pm2" --port 39267 --app "c-$label" \
      --files "$FILES" --filelist "$LISTSHA" \
      > "$WORK/last.out" 2>&1
  echo $?
}
AR="$(newroot)"; stage "$AR" "$ARCHIVE" ANC >/dev/null; wire_venv "$AR"
: > "$PM2ENV"; : > "$PM2LOG"; run_with_ancestor "$AR" ANC >/dev/null

check "the run reached 'pm2 start' (so the evidence below is real)" "yes" \
      "$(grep -qE '^start ' "$PM2LOG" && echo yes || echo no)"
check "  and the stub recorded the environment it was spawned with" "yes" \
      "$([ -f "$PM2ENV" ] && echo yes || echo no)"
check "the INHERITED key did NOT reach the pm2 child" 0 \
      "$(grep -c '^WOA23_TLS_KEYFILE=' "$PM2ENV" || true)"
check "the INHERITED certificate did NOT reach the pm2 child" 0 \
      "$(grep -c '^WOA23_TLS_CERTFILE=' "$PM2ENV" || true)"
check "  no /home/odbadmin path survived into it at all" 0 \
      "$(grep -c '/home/odbadmin' "$PM2ENV" || true)"
# The capture must be shown NON-EMPTY, or "no key in it" would be satisfied by an empty
# file and would prove nothing. WOA23_TLS itself is NOT the control: the entry never
# exports it -- the generated config carries it, and pm2 hands it to the app, not to the
# pm2 CLI. HOME is passed by the fixture and so is genuinely expected here.
check "  the capture is a REAL environment, not an empty file" 1 \
      "$(grep -c '^HOME=' "$PM2ENV" || true)"
check "and the entry SAYS it unset them" "yes" \
      "$(grep -q 'unset before pm2 start' "$WORK/last.out" && echo yes || echo no)"

# A CONTROL. If the fixture could not carry the ancestor's variables through in the first
# place, every assertion above would pass while proving nothing. This confirms the
# variables really are exported into a child of that same shell.
env -i PATH="$PATH" \
    WOA23_TLS_KEYFILE=/home/odbadmin/python/woa23/conf/privkey.pem \
    bash -c 'env | grep -c "^WOA23_TLS_KEYFILE=" || true' > "$WORK/control.txt" 2>&1
check "CONTROL: an exported path DOES reach an ordinary child" 1 \
      "$(cat "$WORK/control.txt")"

echo
echo "the source-level rules behind that behaviour"
check "both TLS variables are unset before pm2 start" 2 \
      "$(awk '/^[[:space:]]*unset WOA23_TLS_(KEY|CERT)FILE[[:space:]]*$/{n++} END{print n+0}' "$ENTRY")"
check "  and that happens BEFORE the pm2 start line" "yes" \
      "$(awk '/^ *unset WOA23_TLS_KEYFILE$/{u=NR} /"\$PM2" start /{s=NR}
              END{print (u && s && u < s) ? "yes" : "no"}' "$ENTRY")"
check "TLS ON exports the paths explicitly from the config" "yes" \
      "$(grep -q 'export WOA23_TLS_KEYFILE WOA23_TLS_CERTFILE' "$ENTRY" && echo yes || echo no)"
check "  and refuses TLS on with no key in the config" "yes" \
      "$(grep -q 'TLS is on but the config carries no WOA23_TLS_KEYFILE' "$ENTRY" \
         && echo yes || echo no)"
check "a leak is classified INVALID_ENVIRONMENT" "yes" \
      "$(grep -q 'INVALID_ENVIRONMENT: ' "$ENTRY" && echo yes || echo no)"
check "  and is stated NOT to be a clean PASS" "yes" \
      "$(grep -q 'NOT a clean PASS and yields NO B3/B5' "$ENTRY" && echo yes || echo no)"
check "  and preserves state rather than cleaning up" "yes" \
      "$(grep -q 'State is' "$ENTRY" && grep -q 'PRESERVED for inspection' "$ENTRY" \
         && echo yes || echo no)"
check "an unreadable worker environment is refused, not read as absence" "yes" \
      "$(grep -q 'An unreadable environment is not an absent one' "$ENTRY" \
         && echo yes || echo no)"
check "zero workers is refused rather than passed" "yes" \
      "$(grep -q 'no worker processes were found under the master' "$ENTRY" \
         && echo yes || echo no)"
check "ppid is read after the last ')', not as a fixed field" "yes" \
      "$(grep -q "sed 's/.\*) //'" "$ENTRY" && echo yes || echo no)"

# The grant and the pm2 binary stay entry-only — unchanged by spec 016.
check "WOA23_PM2C_GRANTED is still unset before pm2 start" "yes" \
      "$(grep -q '^unset WOA23_PM2C_GRANTED' "$ENTRY" && echo yes || echo no)"
check "  and still required ABSENT from the child" "yes" \
      "$(grep -qE 'env_must WOA23_PM2C_GRANTED +"ABSENT"' "$ENTRY" && echo yes || echo no)"
check "WOA23_PM2_BIN is still unset before pm2 start" "yes" \
      "$(grep -q '^unset WOA23_PM2_BIN' "$ENTRY" && echo yes || echo no)"
check "  and is deliberately NOT in the allowlist (a leak stays INVALID)" "no" \
      "$(grep -q 'WOA23_PM2_BIN' <<<"$(grep '^ALLOWED_ENV=' "$ENTRY")" && echo yes || echo no)"

# The provenance block: argv, and — the part that would have caught pm2G — the maps.
check "the entry asserts argv[0] IS the staged venv" "yes" \
      "$(grep -q 'ARGV0" = "\$VENV' "$ENTRY" && echo yes || echo no)"
check "it reads worker maps for library provenance" "yes" \
      "$(grep -q 'PROC/\$w/maps' "$ENTRY" && echo yes || echo no)"
check "it REFUSES libraries from the shared py311 env" "yes" \
      "$(grep -q 'SHARED py311 environment' "$ENTRY" && echo yes || echo no)"
check "it REFUSES libraries from the production tree" "yes" \
      "$(grep -q 'from the production tree' "$ENTRY" && echo yes || echo no)"
check "it REFUSES a venv that contributes nothing" "yes" \
      "$(grep -q 'maps NOTHING from this run' "$ENTRY" && echo yes || echo no)"
check "unreadable maps are a STOP, not a shrug" "yes" \
      "$(grep -q 'runtime provenance is unverified' "$ENTRY" && echo yes || echo no)"
check "  /proc/<pid>/exe is recorded, NOT asserted equal to the venv symlink" "yes" \
      "$(grep -q 'resolves through the venv symlink' "$ENTRY" && echo yes || echo no)"

echo
echo "hygiene"
check "the entry is valid bash" "yes" "$(bash -n "$ENTRY" 2>/dev/null && echo yes || echo no)"
check "it is executable" "yes" "$([ -x "$ENTRY" ] && echo yes || echo no)"
check "no SIGKILL in it" "no" \
      "$(sed -e 's/^[[:space:]]*#.*$//' "$ENTRY" | grep -qE 'kill -9|-KILL' && echo yes || echo no)"
check "no 'pm2 ... all' in it" "no" \
      "$(sed -e 's/^[[:space:]]*#.*$//' "$ENTRY" | grep -qE '\bpm2.*\ball\b' && echo yes || echo no)"
check "it never rm/rmdir/cleans a refused path" "no" \
      "$(sed -e 's/^[[:space:]]*#.*$//' "$ENTRY" | grep -qE '\brm \-|\brmdir\b' && echo yes || echo no)"
check "the repository's deploy/ holds only the two real configs" "2" \
      "$(ls "$HERE"/deploy/ecosystem.*.config.js 2>/dev/null | wc -l | tr -d ' ')"

echo
suite_summary "$PASS" "$FAIL"
