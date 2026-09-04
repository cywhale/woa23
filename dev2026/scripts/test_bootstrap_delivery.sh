#!/usr/bin/env bash
#
# THE BOOTSTRAP MUST DELIVER EVERY FILE THE DRIVER NEEDS, AND THE DRIVER MUST REFUSE
# WHEN IT DID NOT.
#
# WHY THIS FILE EXISTS. The store-ownership guard was moved out of `staging_execute.sh`
# into `lib_store_guard.sh` so its decision could be tested. The bootstrap carried a
# single member -- the driver -- and the driver sourced the library CONDITIONALLY:
#
#     if [ -r "$_SE_HERE/lib_store_guard.sh" ]; then . "$_SE_HERE/lib_store_guard.sh"; fi
#
# So on VM24 the library was never delivered, nothing refused, and the run continued
# until it died at the call site with `store_owner_verdict: command not found`. D-3
# halted there. Two independent defects, each of which alone would have been caught by
# the other: an incomplete manifest, and a critical dependency that was optional.
#
# WHY IT USES THE REAL PACKAGING SHAPE. The previous suites tested the driver in the
# checkout, where `lib_store_guard.sh` is always sitting next to it -- so the missing
# library was UNREPRESENTABLE in the tests. Every case here goes through the real shape:
#
#     a tar archive  ->  a clean bootstrap path OUTSIDE the checkout
#                    ->  extraction by staging_bootstrap.sh itself
#                    ->  the driver EXECUTED from that bootstrap directory
#                    ->  with the working directory outside the checkout
#
# and several cases run with the working directory set TO the checkout precisely to
# prove that a source tree cannot satisfy the import either.
#
# WHAT IS NOT REACHED ON THIS HOST, stated rather than glossed. The ownership verdict
# is computed inside the stage phase's real-store branch, which sits AFTER the tree is
# extracted and the venv is built, and whose first act is to refuse a non-GNU
# find(1)/stat(1). Neither a staged tree nor GNU coreutils exists here, so no local
# test can execute that line. What is proved here instead is the thing that actually
# failed: that the library is delivered, is verified byte-for-byte against the archive,
# and that its absence or corruption stops the run AT LOAD -- before any staging root,
# workdir, PM2_HOME, store symlink, `pm2 start` or case request. The verdict function's
# own behaviour is exercised in full, from the DELIVERED copy, in group D.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEV="$(cd "$HERE/.." && pwd)"
BOOTSTRAP_SRC="$DEV/deploy/staging_bootstrap.sh"
DRIVER_SRC="$DEV/deploy/staging_execute.sh"
GUARD_SRC="$DEV/deploy/lib_store_guard.sh"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  FAIL %s\n' "$1"; [ $# -gt 1 ] && printf '       %s\n' "$2"; }
is()   { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "expected [$3], got [$2]"; fi; }
isnt() { if [ "$2" != "$3" ]; then ok "$1"; else bad "$1" "should not have been [$3]"; fi; }
has()  { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1" "output does not contain [$3]" ;; esac; }
# AN EMPTY HAYSTACK IS NOT AN ABSENCE. Every `hasnt` below asserts that a refusal did NOT
# appear in output that certainly exists. If the command produced nothing at all -- it never
# ran, or it died before writing a byte -- then "the string is not there" is true and means
# nothing, and every such assertion in this file would pass at once. That is a vacuous pass,
# and it is refused here rather than counted. `has` fails on empty output already, by
# construction; `hasnt` is the direction that needed saying.
hasnt(){
  if [ -z "$2" ]; then bad "$1" "the output was EMPTY — nothing was checked"; return; fi
  case "$2" in *"$3"*) bad "$1" "output unexpectedly contains [$3]" ;; *) ok "$1" ;; esac
}
absent(){ if [ -e "$2" ] || [ -L "$2" ]; then bad "$1" "$2 exists"; else ok "$1"; fi; }
present(){ if [ -e "$2" ]; then ok "$1"; else bad "$1" "$2 is missing"; fi; }

T="$(mktemp -d "${TMPDIR:-/tmp}/bootdeliv.XXXXXX")"
trap 'chmod -R u+rwX "$T" 2>/dev/null; rm -rf "$T"' EXIT
# The tests must not be able to reach the checkout by accident. `OUT` is the working
# directory for every driver invocation that is meant to have no source tree in sight.
OUT="$T/elsewhere"; mkdir -p "$OUT"

sha() { sha256sum "$1" | cut -d' ' -f1; }

# --------------------------------------------------------------- archive builders ---
#
# Archives are built in the shape `git archive <sha> dev2026` produces: paths rooted at
# `dev2026/`, with the two deploy members among them. Filler members are included so the
# member checks are matching within a real listing rather than a two-line one.
#
# variant:
#   full          both members, regular files
#   noguard       the library is simply absent -- the D-3 shape exactly
#   symguard      the library member is a SYMLINK
#   dupguard      the library member appears TWICE
#   dirguard      a DIRECTORY stands where the library member should be
mkarch() {  # <variant> -> prints archive path
  local variant="$1" s="$T/src.$1" a="$T/arch.$1.tar"
  rm -rf "$s"; mkdir -p "$s/dev2026/deploy" "$s/dev2026/api"
  cp "$DRIVER_SRC" "$s/dev2026/deploy/staging_execute.sh"
  printf 'filler\n' > "$s/dev2026/api/app.py"
  printf 'filler\n' > "$s/dev2026/deploy/make_staging_store.py"

  if [ "$variant" = full ] || [ "$variant" = dupguard ]; then
    cp "$GUARD_SRC" "$s/dev2026/deploy/lib_store_guard.sh"
  elif [ "$variant" = symguard ]; then
    printf 'elsewhere\n' > "$s/dev2026/deploy/other.sh"
    ln -s other.sh "$s/dev2026/deploy/lib_store_guard.sh"
  elif [ "$variant" = dirguard ]; then
    mkdir -p "$s/dev2026/deploy/lib_store_guard.sh"
    printf 'x\n' > "$s/dev2026/deploy/lib_store_guard.sh/inner"
  fi

  rm -f "$a"
  ( cd "$s" && tar -cf "$a" dev2026 ) || return 1
  if [ "$variant" = dupguard ]; then
    # Appended a second time: an archive may legally carry the same name twice, and on
    # extraction the later member overwrites the verified earlier one.
    ( cd "$s" && tar -rf "$a" dev2026/deploy/lib_store_guard.sh ) || return 1
  fi
  printf '%s\n' "$a"
}

#: Run staging_bootstrap.sh with a fresh identity under $T/<tag>. Everything after the
#: tag and archive is passed through to the driver.
run_boot() {  # <tag> <archive> [driver args...]
  local tag="$1" arch="$2"; shift 2
  local base="$T/id.$tag"
  mkdir -p "$base"
  BOOT="$base/boot"; ROOT="$base/root"; WORK="$base/root-work"
  TMPD="$base/tmp"; PM2H="$base/pm2"; STORE="$ROOT/store"
  ( cd "$OUT" && env -i PATH="$PATH" HOME="$HOME" TMPDIR="$T" \
      WOA23_PM2C_GRANTED=yes \
      bash "$BOOTSTRAP_SRC" \
        --archive "$arch" --archive-sha256 "$(sha "$arch")" \
        --bootstrap "$BOOT" --root "$ROOT" --workdir "$WORK" \
        --tmpdir "$TMPD" --pm2-home "$PM2H" \
        -- "$@" ) 2>&1
}

#: Run the DELIVERED driver directly out of a bootstrap directory. `cwd` is given
#: explicitly so a case can deliberately stand in the checkout.
run_driver() {  # <bootdir> <cwd> [args...]
  local bd="$1" cwd="$2"; shift 2
  ( cd "$cwd" && env -i PATH="$PATH" HOME="$HOME" TMPDIR="$T" \
      WOA23_PM2C_GRANTED=yes \
      bash "$bd/deploy/staging_execute.sh" "$@" ) 2>&1
}

#: A driver command line that is complete and valid, so the run's stopping point is
#: decided by the guards under test and not by a missing argument.
VALIDARGS() {  # <root>
  printf '%s\n' "--phase stage --root $1 --archive $T/arch.full.tar --label deliv \
--pm2-home $T/nowhere-pm2 --port 19999 --app woa23-deliv-candidate \
--files 4 --filelist 0000000000000000000000000000000000000000000000000000000000000000"
}

echo "=== bootstrap delivery + hard library dependency ==="
echo

# ============================================================ A. the member guards ===
#
# Sourced in library-only mode, so the pure predicates are exercised against REAL tar
# listings of REAL archives -- not against hand-written strings that could drift from
# what tar actually prints.
echo "A. archive_member_problem, against real tar listings"
WOA23_BOOTSTRAP_LIB_ONLY=1 . "$BOOTSTRAP_SRC"

M=dev2026/deploy/lib_store_guard.sh
A_full="$(mkarch full)";     L_full="$(tar -tvf "$A_full")"
A_no="$(mkarch noguard)";    L_no="$(tar -tvf "$A_no")"
A_sym="$(mkarch symguard)";  L_sym="$(tar -tvf "$A_sym")"
A_dup="$(mkarch dupguard)";  L_dup="$(tar -tvf "$A_dup")"
A_dir="$(mkarch dirguard)";  L_dir="$(tar -tvf "$A_dir")"

is  "A1  unique regular member is accepted"        "$(archive_member_problem "$L_full" "$M")" ""
is  "A2  the driver member is accepted too"        "$(archive_member_problem "$L_full" dev2026/deploy/staging_execute.sh)" ""
has "A3  a missing member is named"                "$(archive_member_problem "$L_no" "$M")"  "contains no member"
has "A4  a symlink member is refused as a symlink" "$(archive_member_problem "$L_sym" "$M")" "SYMLINK"
has "A5  a duplicate member is refused"            "$(archive_member_problem "$L_dup" "$M")" "duplicate"
has "A6  a directory member is refused"            "$(archive_member_problem "$L_dir" "$M")" "DIRECTORY"
# Non-vacuity: the duplicate archive must really carry two, or A5 proved nothing.
is  "A7  the duplicate archive really has 2 copies" \
    "$(tar -tf "$A_dup" | grep -c "^$M\$")" "2"
echo

# ====================================================== B. delivery, end to end =====
echo "B. staging_bootstrap.sh delivering from a real archive to a clean bootstrap"

OUTB1="$(run_boot b1 "$A_full" $(VALIDARGS "$T/id.b1/root"))"; RC=$?
BD="$T/id.b1/boot"
present "B1  the driver was delivered"        "$BD/deploy/staging_execute.sh"
present "B2  THE LIBRARY WAS DELIVERED"       "$BD/deploy/lib_store_guard.sh"
is  "B3  delivered driver matches the archive"  "$(sha "$BD/deploy/staging_execute.sh")" \
    "$(tar -xOf "$A_full" dev2026/deploy/staging_execute.sh | sha256sum | cut -d' ' -f1)"
is  "B4  delivered library matches the archive" "$(sha "$BD/deploy/lib_store_guard.sh")" \
    "$(tar -xOf "$A_full" "$M" | sha256sum | cut -d' ' -f1)"
if [ -L "$BD/deploy/lib_store_guard.sh" ]; then bad "B5  delivered library is not a symlink"
else ok "B5  delivered library is not a symlink"; fi
has "B6  both members reported verified" "$OUTB1" "every member is the archive's own"
# The run went on to the driver and stopped at a LATER guard, not at the library.
hasnt "B7  no library refusal on the full archive" "$OUTB1" "store guard library"

# --- the D-3 shape: the archive simply does not carry the library ---
OUTB8="$(run_boot b8 "$A_no" $(VALIDARGS "$T/id.b8/root"))"; RCB8=$?
isnt "B8  an archive without the library is refused" "$RCB8" "0"
has  "B9  the refusal names the missing member" "$OUTB8" "contains no member"
absent "B10 no staging root was created"  "$T/id.b8/root"
absent "B11 no workdir was created"       "$T/id.b8/root-work"
absent "B12 no PM2_HOME was created"      "$T/id.b8/pm2"
absent "B13 no store symlink was created" "$T/id.b8/root/store"
absent "B14 no driver was left runnable"  "$T/id.b8/boot/deploy/staging_execute.sh"

OUTB="$(run_boot bsym "$A_sym" $(VALIDARGS "$T/id.bsym/root"))"; RCS=$?
isnt "B15 a symlinked library member is refused" "$RCS" "0"
has  "B16 ... as a symlink"                      "$OUTB" "SYMLINK"
absent "B17 ... and created no staging root"     "$T/id.bsym/root"

OUTB="$(run_boot bdup "$A_dup" $(VALIDARGS "$T/id.bdup/root"))"; RCD=$?
isnt "B18 a duplicated library member is refused" "$RCD" "0"
has  "B19 ... as a duplicate"                     "$OUTB" "duplicate"
absent "B20 ... and created no staging root"      "$T/id.bdup/root"

OUTB="$(run_boot bdir "$A_dir" $(VALIDARGS "$T/id.bdir/root"))"; RCR=$?
isnt "B21 a non-regular (directory) member is refused" "$RCR" "0"
has  "B22 ... as a directory"                          "$OUTB" "DIRECTORY"
absent "B23 ... and created no staging root"           "$T/id.bdir/root"
echo

# ============================== B'. extraction that does not match the archive ======
#
# FAULT INJECTION, not a bypass. The bootstrap hashes each member's bytes IN the archive
# and hashes the extracted file again afterwards. Nothing in a normal run makes those
# differ, so the comparison could be permanently vacuous and never be noticed. A `tar`
# shim earlier on PATH corrupts the SECOND read of the library member -- the extraction --
# leaving the first -- the expected hash -- intact.
echo "B'. the archive-to-disk hash comparison is not vacuous"
SHIM="$T/shim"; mkdir -p "$SHIM"
cat > "$SHIM/tar" <<'SHIMEOF'
#!/usr/bin/env bash
real=/usr/bin/tar
want=dev2026/deploy/lib_store_guard.sh
n="$WOA23_SHIM_COUNTER"
for a in "$@"; do
  if [ "$a" = "$want" ]; then
    c=0; [ -r "$n" ] && c="$(cat "$n")"
    c=$((c+1)); printf '%s' "$c" > "$n"
    if [ "$c" -ge 2 ]; then "$real" "$@"; printf '# corrupted by the shim\n'; exit 0; fi
  fi
done
exec "$real" "$@"
SHIMEOF
chmod 700 "$SHIM/tar"
CNT="$T/shim.count"; printf '0' > "$CNT"
# The bootstrap refuses when its parent is not a directory, and the first draft of this
# case never created one -- so it stopped there, returned non-zero, and the "an extracted
# member that differs is refused" assertion passed WITHOUT the corruption ever being
# reached. A vacuous pass, of exactly the kind this campaign keeps finding. B27 is what
# makes it non-vacuous now; the mkdir is what makes B27 reachable.
mkdir -p "$T/id.bcor"
OUTB="$( cd "$OUT" && env -i PATH="$SHIM:$PATH" HOME="$HOME" TMPDIR="$T" \
    WOA23_PM2C_GRANTED=yes WOA23_SHIM_COUNTER="$CNT" \
    bash "$BOOTSTRAP_SRC" \
      --archive "$A_full" --archive-sha256 "$(sha "$A_full")" \
      --bootstrap "$T/id.bcor/boot" --root "$T/id.bcor/root" \
      --workdir "$T/id.bcor/root-work" --tmpdir "$T/id.bcor/tmp" \
      --pm2-home "$T/id.bcor/pm2" \
      -- $(VALIDARGS "$T/id.bcor/root") 2>&1 )"; RCC=$?
isnt "B24 an extracted member that differs from the archive is refused" "$RCC" "0"
has  "B25 ... naming the mismatch"        "$OUTB" "does not match the archive"
absent "B26 ... and created no staging root" "$T/id.bcor/root"
# The verification is done TWICE: once inside the per-member `while` subshell, and again in
# the parent against the archive, because a subshell's exit status is the one thing this
# campaign has already been burned by (`SERVERS="$SERVERS $!"` inside `$(...)`, 15 leaked
# processes, suite exit 0). Both refusals must be present in the source.
is  "B27a the member loop verifies the digest" \
    "$(grep -cF 'REFUSING: extracted %s does not match the archive' "$BOOTSTRAP_SRC")" "1"
is  "B27b ... and the parent re-verifies it outside the subshell" \
    "$(grep -c 'die \"the delivered \$_m does not match the archive\"' "$BOOTSTRAP_SRC")" "1"
is  "B27c ... re-reading the archive rather than trusting an exit code" \
    "$(grep -c 'to confirm the delivery' "$BOOTSTRAP_SRC")" "1"
# Non-vacuity of the injection itself: the shim must actually have fired twice.
SHOTS="$(cat "$CNT" 2>/dev/null)"; SHOTS="${SHOTS:-0}"
if [ "$SHOTS" -ge 2 ]; then
  ok "B27 the shim really did reach the extraction and corrupt it"
else bad "B27 the shim really did reach the extraction and corrupt it" \
     "member reads=$SHOTS, expected >=2 (hash, then extract)"; fi
echo

# ================================ C. the driver's hard dependency, from $BOOT =======
#
# Every case runs the DELIVERED driver out of the bootstrap directory produced by B1.
# The library is then damaged in the bootstrap directory in each of the ways a delivery
# can be wrong, and the driver must refuse at load in every one of them.
echo "C. the driver refuses to run without a valid store guard library, from the bootstrap"
GUARD="$BD/deploy/lib_store_guard.sh"
GOOD="$T/guard.good"; cp "$GUARD" "$GOOD"
restore() { rm -rf "$GUARD"; cp "$GOOD" "$GUARD"; chmod 700 "$GUARD"; }

# C1 -- absent. The exact D-3 condition.
rm -f "$GUARD"
OUTC="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.c1/root"))"; RC1=$?
is   "C1  absent library -> exit 2"           "$RC1" "2"
has  "C2  ... names the missing library"      "$OUTC" "store guard library is missing"
hasnt "C3  ... never reached the call site"   "$OUTC" "command not found"
absent "C4  ... no staging root"              "$T/id.c1/root"
absent "C5  ... no workdir"                   "$T/id.c1/root-work"
absent "C6  ... no store symlink"             "$T/id.c1/root/store"
absent "C7  ... no PM2_HOME"                  "$T/nowhere-pm2"
hasnt "C8  ... no pm2 was started"            "$OUTC" "pm2 start"

# C9 -- absent, but standing INSIDE the checkout, where the real library exists.
OUTC="$(run_driver "$BD" "$DEV" $(VALIDARGS "$T/id.c9/root"))"; RC9=$?
is   "C9  a checkout cwd cannot satisfy the import" "$RC9" "2"
has  "C10 ... still refuses for the missing library" "$OUTC" "store guard library is missing"
# ... and the same standing in deploy/ itself, where a bare-name source would resolve.
OUTC="$(run_driver "$BD" "$DEV/deploy" $(VALIDARGS "$T/id.c10/root"))"; RC10=$?
is   "C11 nor does standing in deploy/ itself" "$RC10" "2"
has  "C12 ... still refuses"                   "$OUTC" "store guard library is missing"

# C13 -- a symlink, even one pointing at a perfectly good library.
restore; rm -f "$GUARD"; ln -s "$GOOD" "$GUARD"
OUTC="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.c13/root"))"; RC=$?
is  "C13 a symlinked library -> exit 2" "$RC" "2"
has "C14 ... refused as a SYMLINK"      "$OUTC" "SYMLINK"
absent "C15 ... no staging root"        "$T/id.c13/root"

# C13b -- a BROKEN symlink. `-e` follows links and reports it absent, so an existence-first
# ordering would call this "missing" and describe the disk incorrectly. It is a symlink.
restore; rm -f "$GUARD"; ln -s "$T/no-such-library" "$GUARD"
OUTC="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.c13b/root"))"; RC=$?
is  "C13b a BROKEN symlink -> exit 2"          "$RC" "2"
has "C13c ... refused as a SYMLINK, not as missing" "$OUTC" "is a SYMLINK"
hasnt "C13d ... and not described as missing"  "$OUTC" "library is missing"

# C16 -- not a regular file.
restore; rm -f "$GUARD"; mkdir -p "$GUARD"
OUTC="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.c16/root"))"; RC=$?
is  "C16 a directory in its place -> exit 2" "$RC" "2"
has "C17 ... refused as not a regular file"  "$OUTC" "not a regular file"
absent "C18 ... no staging root"             "$T/id.c16/root"

# C19 -- unreadable. Skipped as root, for whom nothing is unreadable.
restore; chmod 000 "$GUARD"
if [ "$(id -u)" = 0 ]; then
  ok "C19 unreadable library (skipped: running as root)"
  ok "C20 unreadable library (skipped: running as root)"
else
  OUTC="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.c19/root"))"; RC=$?
  is  "C19 an unreadable library -> exit 2" "$RC" "2"
  has "C20 ... refused as not readable"     "$OUTC" "not readable"
fi
chmod 700 "$GUARD"

# C21 -- MODIFIED: present, readable, regular, sources without error, and defines
# nothing. `[ -r ]` was true here, which is exactly why `[ -r ]` was never enough.
restore; printf '#!/usr/bin/env bash\n# gutted\n' > "$GUARD"
OUTC="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.c21/root"))"; RC=$?
is  "C21 a gutted library -> exit 2"                 "$RC" "2"
has "C22 ... refused for defining no verdict"        "$OUTC" "does not define store_owner_verdict"
absent "C23 ... no staging root"                     "$T/id.c21/root"

# C24 -- POSITIVE CONTROL. Intact library, valid command line: the driver must get past
# the load and stop at a strictly later guard. The stopping point is made deterministic
# by pointing --root at a path that already exists, so the freshness guard -- which runs
# after the library, and long before anything is created -- is what refuses.
restore
mkdir -p "$T/id.c24/root"
OUTC="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.c24/root"))"; RC=$?
hasnt "C24 intact library -> no library refusal"  "$OUTC" "store guard library"
hasnt "C25 ... and no 'command not found'"        "$OUTC" "command not found"
has   "C26 ... execution continued to a later guard" "$OUTC" "already exists"
is    "C27 ... which is itself a refusal"         "$RC" "2"
echo

# ===================== D. the delivered library's verdict, exercised in full ========
#
# Sourced from the BOOTSTRAP copy, with the working directory outside the checkout, so
# what is tested is the file the bootstrap delivered rather than the one in the tree.
echo "D. store_owner_verdict, from the DELIVERED copy, at every uid combination"
restore
verdict() {  # <store_uid> <me_uid> <expect_uid> -> "<word> <rc>"
  ( cd "$OUT" && env -i PATH="$PATH" bash -c '
      . "$1/deploy/lib_store_guard.sh"
      v="$(store_owner_verdict "$2" "$3" "$4")"; rc=$?
      printf "%s %s" "$v" "$rc"' _ "$BD" "$2" "$3" "$4" )
}
# The D-3 case: production store owned by uid 1000, executing as uid 994.
is "D1  store 1000 / me 994 / expect 1000 -> ok"        "$(verdict x 1000 994 1000)" "ok 0"
is "D2  store 994  / me 994 -> self (the dangerous one)" "$(verdict x 994 994 1000)" "self 5"
is "D3  store 0    / me 994 -> root"                     "$(verdict x 0 994 1000)"    "root 6"
is "D4  store 1001 / me 994 -> unexpected"               "$(verdict x 1001 994 1000)" "unexpected 7"
is "D5  store ''   -> unreadable"                        "$(verdict x '' 994 1000)"   "unreadable 3"
is "D6  store 'abc' -> nonnumeric"                       "$(verdict x abc 994 1000)"  "nonnumeric 4"
is "D7  me ''      -> unreadable"                        "$(verdict x 1000 '' 1000)"  "unreadable 3"
is "D8  expect ''  -> unreadable"                        "$(verdict x 1000 994 '')"   "unreadable 3"
is "D9  self wins over root (store 0 = me 0)"            "$(verdict x 0 0 1000)"      "self 5"
is "D10 self wins over expected (store=me=expect)"       "$(verdict x 994 994 994)"   "self 5"
echo

# ============================ E. synthetic mode is unchanged =======================
#
# The hard dependency must not have made the synthetic path harder to reach: the guard
# is loaded in both modes, but synthetic runs never consult a real store.
echo "E. synthetic mode, run from the bootstrap, is unchanged"
mkdir -p "$T/id.e/root"
OUTE="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.e/root") --store-mode synthetic)"; RC=$?
hasnt "E1  synthetic needs no --real-store"          "$OUTE" "--real-store is required"
hasnt "E2  ... and no library refusal"               "$OUTE" "store guard library"
has   "E3  ... reaching the same later guard"        "$OUTE" "already exists"

OUTE="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.e2/root") --store-mode synthetic --real-store /home/odbadmin/python/woa23/data)"; RC=$?
is  "E4  synthetic + --real-store -> exit 2" "$RC" "2"
has "E5  ... refused, not ignored"           "$OUTE" "--real-store was given but --store-mode"

OUTE="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.e3/root") --store-mode real-readonly)"; RC=$?
is  "E6  real-readonly without --real-store -> exit 2" "$RC" "2"
has "E7  ... refused"                                  "$OUTE" "--real-store is required"

OUTE="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.e4/root") --store-mode wishful)"; RC=$?
is  "E8  an unknown store mode -> exit 2" "$RC" "2"
has "E9  ... refused, never defaulted"    "$OUTE" "must be 'synthetic' or 'real-readonly'"

# The default is synthetic, and the default must not silently become real.
OUTE="$(run_driver "$BD" "$OUT" $(VALIDARGS "$T/id.e/root"))"
hasnt "E10 the default mode never consults a real store" "$OUTE" "REAL STORE MODE"
echo

# ==================================== F. the manifest itself =======================
#
# One structural check, because it is the thing that was wrong: the bootstrap's manifest
# must name the library. This is a presence test and is worth exactly what a presence
# test is worth -- the behaviour is proved in B. It is here so that dropping the member
# fails loudly rather than only through a delivery test somebody might skip.
echo "F. the manifest"
MAN="$(WOA23_BOOTSTRAP_LIB_ONLY=1 bash -c '. "$1" >/dev/null 2>&1; printf "%s\n" "$BOOTSTRAP_MEMBERS"' _ "$BOOTSTRAP_SRC")"
has "F1  the manifest names the driver"  "$MAN" "dev2026/deploy/staging_execute.sh"
has "F2  the manifest names the library" "$MAN" "dev2026/deploy/lib_store_guard.sh"
is  "F3  the manifest has exactly 2 members" "$(printf '%s\n' "$MAN" | grep -c .)" "2"

# A MANIFEST MUST NOT DELIVER TWO FILES TO ONE NAME. Members are flattened into
# $BOOT/deploy/<basename>, so two members with the same basename would write to one path --
# the second overwriting the first, with the extraction hash check passing because it
# re-reads exactly what it just wrote. archive_member_problem cannot see this: both members
# are unique IN THE ARCHIVE. The collision is created by the flattening.
is  "F4  the real manifest has no basename collision" \
    "$(manifest_basename_collision "$MAN")" ""
is  "F5  a collision across directories IS caught" \
    "$(manifest_basename_collision 'dev2026/deploy/x.sh
dev2026/scripts/x.sh')" "x.sh"
is  "F6  distinct basenames are not a collision" \
    "$(manifest_basename_collision 'dev2026/deploy/a.sh
dev2026/scripts/b.sh')" ""
is  "F7  three-way collisions report the name once" \
    "$(manifest_basename_collision 'a/x.sh
b/x.sh
c/x.sh')" "x.sh"
is  "F8  an identical repeated member is a collision too" \
    "$(manifest_basename_collision 'dev2026/deploy/x.sh
dev2026/deploy/x.sh')" "x.sh"
is  "F9  blank lines do not invent a collision" \
    "$(manifest_basename_collision 'dev2026/deploy/a.sh

dev2026/deploy/b.sh')" ""
is  "F10 an empty manifest has no collision (emptiness is refused separately)" \
    "$(manifest_basename_collision '')" ""
# The bootstrap must actually CONSULT it, and must refuse an empty manifest -- the pure
# function returns "" for empty, which is indistinguishable from "fine" on its own.
has "F11 step 0 refuses an empty manifest" \
    "$(grep -A2 'BOOTSTRAP_MEMBERS//' "$BOOTSTRAP_SRC")" "manifest is empty"
is  "F12 step 0 refuses a collision before any path work" \
    "$(grep -c 'manifest_basename_collision "\$BOOTSTRAP_MEMBERS"' "$BOOTSTRAP_SRC")" "1"
# Defence in depth inside the loop: nothing may be written over. It is not reachable while
# step 0 holds -- that is the point of it -- so it is checked structurally, and said to be.
is  "F13 the extraction loop also refuses to overwrite" \
    "$(grep -c 'would be overwritten in the bootstrap' "$BOOTSTRAP_SRC")" "1"
echo

# THE SUMMARY LINE IS THE CAMPAIGN'S, not this file's own.
#
# It used to print `=== 100 passed, 0 failed ===`, which is a THIRD shape alongside the two
# every tally in this campaign knows how to read -- `all passed (N assertions)` and
# `N tests, M assertions, K failed`. A suite that reports its count in a private format has
# a count nobody can add up: these 100 assertions were invisible to the batch totals, which
# is the same undercount that once turned 4528 into 4382. A number that cannot be summed is
# not evidence, however correct it is.
suite_summary "$PASS" "$FAIL"
