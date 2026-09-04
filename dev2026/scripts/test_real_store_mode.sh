#!/usr/bin/env bash
#
# The real-production-store read-only mode, checked offline.
#
# WHAT THIS EXISTS TO PROVE. D-3 needs the candidate to serve the REAL production store,
# and the execution entry previously could not: it built a synthetic fixture, asserted it
# had exactly 72 files, and ran `chmod -R a-w` over it. Pointing that at production would
# have attempted a permission change on production data. The new mode removes those
# assumptions -- and this suite is what makes "removes" checkable rather than claimed.
#
# TWO KINDS OF EVIDENCE, and the second is the one that matters:
#
#   SOURCE   the real-mode branch contains no chmod/chown/mkdir/rm/touch against the
#            store, and no synthetic builder call.
#   FUNCTIONAL a real read-only directory is built here, the mode is driven against it,
#            and the directory is compared byte-for-byte afterwards. A source grep can be
#            defeated by an indirection; a before/after comparison cannot.
#
# Nothing here touches the real production store, VM24, PM2, or any port. The "production
# store" is a local fixture made read-only with chmod, and every check runs against that.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXEC="$HERE/deploy/staging_execute.sh"
GEN="$HERE/deploy/make_staging_override.js"

PASS=0; FAIL=0
check() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}

[ -r "$EXEC" ] || { echo "missing $EXEC"; exit 2; }
[ -r "$GEN" ]  || { echo "missing $GEN"; exit 2; }

WORK="$(mktemp -d)"
trap 'chmod -R u+w "$WORK" 2>/dev/null; rm -rf "$WORK"' EXIT

echo "1. the mode is EXPLICIT — never reached by default or by a resolving path"

check "the default store mode is synthetic" "1" \
      "$(grep -c '^STORE_MODE=synthetic' "$EXEC")"
check "--store-mode accepts exactly two values" "1" \
      "$(grep -c 'synthetic|real-readonly) ;;' "$EXEC")"
check "--real-store is required in real mode" "yes" \
      "$(grep -q 'real-store is required when --store-mode is real-readonly' "$EXEC" && echo yes || echo no)"
check "--real-store is REFUSED in synthetic mode, not ignored" "yes" \
      "$(grep -q 'real-store was given but --store-mode' "$EXEC" && echo yes || echo no)"
check "the generator defaults to synthetic too" "yes" \
      "$(grep -q "args\['store-mode'\] === undefined ? 'synthetic'" "$GEN" && echo yes || echo no)"

echo
echo "2. the synthetic branch is UNCHANGED and still fully guarded"

check "the synthetic builder is still called (non-comment)" "1" \
      "$(grep -cE '^[^#]*make_staging_store\.py' "$EXEC")"
check "the 72-file assertion still exists for synthetic" "1" \
      "$(grep -c 'expected 72' "$EXEC")"
check "the synthetic chmod still exists" "1" \
      "$(grep -c 'chmod -R a-w' "$EXEC")"
check "the synthetic write-probe still exists" "1" \
      "$(grep -c 'write-probe' "$EXEC")"

echo
echo "3. SOURCE — the real branch carries none of the synthetic assumptions"

# The real branch is the text between the `else` that opens it and the `fi` that closes it.
REAL_BRANCH="$(awk '/^# ==+ REAL STORE, READ-ONLY/,/^fi$/' "$EXEC")"
check "the real branch was located in the source" "yes" \
      "$([ -n "$REAL_BRANCH" ] && echo yes || echo no)"

for verb in chmod chown mkdir rmdir; do
  check "real branch contains no $verb" "0" \
        "$(printf '%s\n' "$REAL_BRANCH" | grep -cE "^[^#]*\\b$verb\\b" || true)"
done
check "real branch contains no rm" "0" \
      "$(printf '%s\n' "$REAL_BRANCH" | grep -cE '^[^#]*\brm\b' || true)"
check "real branch contains no touch (not even a write probe)" "0" \
      "$(printf '%s\n' "$REAL_BRANCH" | grep -cE '^[^#]*\btouch\b' || true)"
check "real branch never CALLS the synthetic builder (comments excluded)" "0" \
      "$(printf '%s\n' "$REAL_BRANCH" | grep -cE '^[^#]*make_staging_store' || true)"
check "real branch carries no 72-file assumption (comments excluded)" "0" \
      "$(printf '%s\n' "$REAL_BRANCH" | grep -cE '^[^#]*expected 72' || true)"
check "the ONLY creating command in the real branch is ln -s" "1" \
      "$(printf '%s\n' "$REAL_BRANCH" | grep -cE '^[^#]*\bln -s\b' || true)"

echo
echo "4. SOURCE — the real branch verifies path, ownership, permissions and boundaries"

for probe in \
  'EXPECT_REAL_STORE=' 'readlink -f' 'stat -c %u' '-writable' \
  'writable ancestor|writable by uid' 'type l' 'not readable' 'not traversable' '.zgroup'
do
  check "real branch checks: $probe" "yes" \
        "$(printf '%s\n' "$REAL_BRANCH" | grep -qE -- "$probe" && echo yes || echo no)"
done
check "the authorised store path is a literal, not a pattern" "yes" \
      "$(grep -q "EXPECT_REAL_STORE='/home/odbadmin/python/woa23/data'" "$EXEC" && echo yes || echo no)"

echo
echo "5. SOURCE — the generator's guard is realpath-based in BOTH modes"

check "the generator now resolves symlinks" "yes" \
      "$(grep -q 'fs.realpathSync' "$GEN" && echo yes || echo no)"
check "synthetic mode tests the RESOLVED path too" "yes" \
      "$(grep -q 'insideProd(store) || insideProd(realStore)' "$GEN" && echo yes || echo no)"
check "real mode requires an EXACT resolve to the production store" "yes" \
      "$(grep -q 'realStore !== PROD_STORE' "$GEN" && echo yes || echo no)"
check "real mode refuses a writable store" "yes" \
      "$(grep -q 'storeIsWritable' "$GEN" && echo yes || echo no)"

echo
echo "6. FUNCTIONAL — a real read-only store is UNCHANGED after the mode runs"
#
# This is the check a source grep cannot give. A fixture store is built, made read-only,
# fingerprinted, driven through the real-mode preconditions, and fingerprinted again.

FAKE_PROD="$WORK/prodstore"
mkdir -p "$FAKE_PROD/1_degree/annual/TS"
printf '{"zarr_format":2}' > "$FAKE_PROD/1_degree/annual/TS/.zgroup"
printf 'payload-a' > "$FAKE_PROD/a.bin"
printf 'payload-b' > "$FAKE_PROD/1_degree/b.bin"
chmod -R a-w "$FAKE_PROD"

# PORTABLE, because this suite runs on the developer machine (BSD find) as well as on a
# GNU host. The first version used `find -printf` and `find -writable`; on BSD both are
# unknown primaries, the error went to /dev/null, and the fingerprint became sha256 of an
# EMPTY string -- identical before and after, so "unchanged" passed while measuring
# nothing. That is the vacuous-pass shape, produced here by me, and it is why these
# helpers exist rather than the one-line GNU form.
if stat -c %a . >/dev/null 2>&1; then
  st_mode() { stat -c %a "$1"; }; st_size() { stat -c %s "$1"; }
else
  st_mode() { stat -f %Lp "$1"; }; st_size() { stat -f %z "$1"; }
fi
sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum | cut -d' ' -f1
        else shasum -a 256 | cut -d' ' -f1; fi; }

fingerprint() {
  ( cd "$1" && LC_ALL=C find . | LC_ALL=C sort | while IFS= read -r e; do
      t=f; [ -d "$e" ] && t=d; [ -L "$e" ] && t=l
      printf '%s\t%s\t%s\t%s\n' "$e" "$(st_size "$e" 2>/dev/null || echo -)" \
                                   "$(st_mode "$e" 2>/dev/null || echo -)" "$t"
    done ) | sha
}
# Effective-user writability, asked of the kernel one path at a time -- `find -writable`
# is GNU-only and would answer 0 on BSD by failing.
count_writable() {
  n=0
  while IFS= read -r p; do [ -n "$p" ] || continue; [ -w "$p" ] && n=$((n+1)); done <<EOF
$(find "$1")
EOF
  printf '%s' "$n"
}

# The fingerprint must be non-vacuous. An empty digest here means the helpers failed, and
# a comparison of two empty digests would "pass" without measuring anything.
EMPTY_SHA="$(printf '' | sha)"
BEFORE="$(fingerprint "$FAKE_PROD")"
BEFORE_N="$(find "$FAKE_PROD" | wc -l | tr -d ' ')"
check "the fingerprint is non-vacuous (not the digest of empty input)" "non-empty" \
      "$([ "$BEFORE" != "$EMPTY_SHA" ] && echo non-empty || echo EMPTY)"
check "the fixture has entries to measure" "7" "$BEFORE_N"

# Drive the real-mode preconditions exactly as the branch does, against the fixture.
ROOT="$WORK/root"; mkdir -p "$ROOT"
STORE="$ROOT/store"
ln -s "$FAKE_PROD" "$STORE"

RS_RESOLVED="$(cd "$FAKE_PROD" && pwd -P)"
check "fixture store is not writable by this account" "no" \
      "$([ -w "$FAKE_PROD" ] && echo yes || echo no)"
check "no writable path beneath it" "0" "$(count_writable "$FAKE_PROD")"
check "no escaping symlink inside it" "0" \
      "$(find "$FAKE_PROD" -type l 2>/dev/null | wc -l | tr -d ' ')"
check "the staging symlink resolves to the fixture store" "$RS_RESOLVED" \
      "$(cd "$STORE" && pwd -P)"
check "the anchor is readable" "yes" \
      "$([ -r "$STORE/1_degree/annual/TS/.zgroup" ] && echo yes || echo no)"

# Creating inside the store THROUGH the symlink must fail: the directory has no write bit.
check "creating a file through the staging symlink is refused" "refused" \
      "$(touch "$STORE/.probe" 2>/dev/null && echo 'WROTE' || echo refused)"

# OWNERSHIP IS THE REAL PROTECTION, and this records why -- it is not a detail.
#
# `chmod -R a-w` does NOT stop the file's OWNER: an owner may always change the mode back.
# The fixture here is owned by the account running the suite, so the chmod SUCCEEDS, and
# asserting otherwise would be asserting something false. On VM24 the production store is
# owned by odbadmin while the run is uid 994, and it is that NON-OWNERSHIP -- not the mode
# bits -- that makes a write impossible. Hence the branch's ownership guard, checked below.
OWNER_CAN_CHMOD="$(chmod u+w "$STORE/a.bin" 2>/dev/null && echo yes || echo no)"
check "an OWNER can chmod their own read-only file (mode bits alone are not protection)" \
      "yes" "$OWNER_CAN_CHMOD"
check "so the branch REFUSES a store owned by the running account" "yes" \
      "$(printf '%s\n' "$REAL_BRANCH" | grep -q 'which is this account' && echo yes || echo no)"
chmod a-w "$STORE/a.bin" 2>/dev/null || true   # restore, so the comparison below is honest

AFTER="$(fingerprint "$FAKE_PROD")"
AFTER_N="$(find "$FAKE_PROD" | wc -l | tr -d ' ')"
check "the store's entry count is unchanged" "$BEFORE_N" "$AFTER_N"
check "the store's path+size+mode+type fingerprint is unchanged" "$BEFORE" "$AFTER"

echo
echo "7. FUNCTIONAL — the generator refuses the bypasses"

if command -v node >/dev/null 2>&1; then
  # synthetic mode + a symlink pointing at the fixture "production" store: under the old
  # lexical guard this would have passed. It must now be refused on the resolved path.
  OUT="$(node -e '
    const fs=require("fs"), path=require("path");
    const store=process.argv[1], PROD=process.argv[2];
    const real=fs.existsSync(store)?fs.realpathSync(store):store;
    const inside=(p)=>p===PROD||p.startsWith(PROD+path.sep);
    process.stdout.write((inside(store)||inside(real))?"refused":"allowed");
  ' "$STORE" "$RS_RESOLVED")"
  check "synthetic mode refuses a symlink into the production store" "refused" "$OUT"

  OUT2="$(node -e '
    const fs=require("fs");
    const store=process.argv[1], PROD=process.argv[2];
    const real=fs.existsSync(store)?fs.realpathSync(store):store;
    process.stdout.write(real!==PROD?"refused":"accepted");
  ' "$STORE" "$RS_RESOLVED")"
  check "real mode accepts only an exact resolve to the store" "accepted" "$OUT2"

  OUT3="$(node -e '
    const fs=require("fs");
    const other=process.argv[1], PROD=process.argv[2];
    const real=fs.existsSync(other)?fs.realpathSync(other):other;
    process.stdout.write(real!==PROD?"refused":"accepted");
  ' "$WORK" "$RS_RESOLVED")"
  check "real mode refuses any OTHER path" "refused" "$OUT3"
else
  echo "  (node unavailable — generator functional checks skipped)"
fi

echo
echo "8. the mode is threaded through to the generator"

check "the executor passes --store-mode to the generator" "1" \
      "$(grep -c -- '--store-mode "\$STORE_MODE"' "$EXEC")"
check "no production lifecycle verb entered the executor" "0" \
      "$(grep -cE '^[^#]*pm2 (restart|reload|delete|save|resurrect)' "$EXEC" || true)"

echo
echo "9. THE OWNERSHIP GUARD — its DECISION is exercised, not its presence"
#
# The guard was written INVERTED and D-3 halted on it:
#     [ "$STORE_UID" != "$ME_UID" ] && die "...which is this account"
# It fired when the uids DIFFER -- the safe state -- and would have PASSED when they match,
# the dangerous one. The old tests grepped for the message and so could not see it. These
# call the decision with every combination.

GUARD="$HERE/deploy/lib_store_guard.sh"
check "the guard library exists" "yes" "$([ -r "$GUARD" ] && echo yes || echo no)"
# shellcheck source=/dev/null
. "$GUARD"

v() { store_owner_verdict "$1" "$2" "$3"; }          # verdict word
r() { store_owner_verdict "$1" "$2" "$3" >/dev/null; echo $?; }   # return code

# THE CASE D-3 ACTUALLY MET: store owned by 1000, running as 994, expecting 1000.
check "owner 1000 / me 994 / expect 1000 -> ALLOWED" "ok"   "$(v 1000 994 1000)"
check "  and returns success"                        "0"    "$(r 1000 994 1000)"

# THE DANGEROUS CASE the inverted guard would have let through.
check "owner 994 (== me) -> REFUSED as 'self'"       "self" "$(v 994 994 1000)"
check "  and returns non-zero"                       "5"    "$(r 994 994 1000)"

check "unreadable owner -> REFUSED"                  "unreadable" "$(v '' 994 1000)"
check "  and returns non-zero"                       "3"    "$(r '' 994 1000)"
check "non-numeric owner -> REFUSED"                 "nonnumeric" "$(v abc 994 1000)"
check "  and returns non-zero"                       "4"    "$(r abc 994 1000)"
check "root-owned (uid 0) -> REFUSED"                "root" "$(v 0 994 1000)"
check "  and returns non-zero"                       "6"    "$(r 0 994 1000)"
check "unexpected owner 1234 -> REFUSED"             "unexpected" "$(v 1234 994 1000)"
check "  and returns non-zero"                       "7"    "$(r 1234 994 1000)"

# Ordering: self is reported BEFORE root and before unexpected, so the most dangerous
# case is never described as merely surprising.
check "owner 0 AND me 0 -> reported as 'self', not 'root'" "self" "$(v 0 0 1000)"
check "an unreadable ME_UID is refused too"          "unreadable" "$(v 1000 '' 1000)"
check "an unreadable EXPECTED uid is refused too"    "unreadable" "$(v 1000 994 '')"

# The driver must actually CALL it -- a correct function nobody invokes is not a guard --
# and, since D-3, it must REFUSE when the library is not there. The behaviour is proved
# end to end, through the real bootstrap packaging shape, in test_bootstrap_delivery.sh.
# What is checked here is only that the dependency has not quietly gone optional again:
# it was `if [ -r ... ]; then . ...; fi`, the bootstrap never delivered the file, nothing
# refused, and the run died at the call site with "command not found".
check "the driver loads the library from beside itself, unconditionally" "1" \
      "$(grep -cE '^\. "\$_SE_GUARD"' "$EXEC")"
check "the load is NOT conditional on [ -r ] any more" "0" \
      "$(grep -cE '^[^#]*if \[ -r "\$_SE_HERE/lib_store_guard\.sh" \]' "$EXEC")"
check "a library that loads but defines nothing is refused" "1" \
      "$(grep -cE '^command -v store_owner_verdict ' "$EXEC")"
check "the driver calls store_owner_verdict on the real store" "1" \
      "$(grep -cE '^OWNER_VERDICT="\$\(store_owner_verdict ' "$EXEC")"
check "the driver no longer carries the inverted test" "0" \
      "$(grep -cE '^[^#]*STORE_UID.*!=.*ME_UID' "$EXEC")"
check "the authorised owner is a literal 1000" "1" \
      "$(grep -cE '^EXPECT_STORE_UID=1000' "$EXEC")"

echo
echo "9a. END-TO-END — a refusal creates NO store symlink and does not continue"

E2E="$WORK/e2e"; mkdir -p "$E2E"
RSTORE="$E2E/notthestore"; mkdir -p "$RSTORE"
OUT="$E2E/out.txt"
# Real mode against a store that is NOT the authorised path: the driver must refuse and
# must not create anything. This exercises the real branch through the real entry point.
( cd "$HERE" && WOA23_PM2C_GRANTED=yes bash "$EXEC" --phase run \
    --root "$E2E/root" --label e2e --pm2-home "$E2E/pm2home" \
    --port 65530 --app woa23-e2e-candidate --files 1 --filelist "$(printf '%064d' 0)" \
    --store-mode real-readonly --real-store "$RSTORE" ) > "$OUT" 2>&1
rc=$?
check "the driver REFUSED (non-zero exit)" "nonzero" "$([ "$rc" -ne 0 ] && echo nonzero || echo "zero")"
check "  no store symlink was created" "absent" \
      "$([ -e "$E2E/root/store" ] || [ -L "$E2E/root/store" ] && echo PRESENT || echo absent)"
check "  no PM2_HOME was created" "absent" \
      "$([ -e "$E2E/pm2home" ] && echo PRESENT || echo absent)"
check "  it did not reach 'pm2 start'" "0" "$(grep -c 'pm2 start' "$OUT" || true)"
check "  the refusal names a reason" "yes" \
      "$(grep -qi 'refusing' "$OUT" && echo yes || echo no)"

echo
echo "9b. the D-3 path cannot select the shared production interpreter"

PA="$HERE/deploy/production_app.sh"
check "production_app.sh requires WOA23_PYTHON with no default" "yes" \
      "$(grep -q 'WOA23_PYTHON is not set' "$PA" && echo yes || echo no)"
# FUNCTIONAL: with WOA23_PYTHON unset the launcher must refuse rather than fall back.
#
# The launcher checks the ANCHOR and then TLS before the interpreter, so a bare invocation
# refuses for the wrong reason and proves nothing about the fallback. A minimal store with
# a readable anchor plus WOA23_TLS=off clears both, so the interpreter check is the one
# actually reached -- otherwise this assertion would pass on an unrelated refusal.
FS="$WORK/fakestore"; mkdir -p "$FS/1_degree/annual/TS"
printf '{"zarr_format":2}' > "$FS/1_degree/annual/TS/.zgroup"
out="$( cd "$HERE" && env -u WOA23_PYTHON WOA23_PORT=65531 WOA23_ZARR_STORE="$FS" \
        WOA23_TLS=off bash "$PA" 2>&1 || true )"
check "  and REFUSES when it is unset (no silent fallback)" "yes" \
      "$(printf '%s' "$out" | grep -qi 'WOA23_PYTHON is not set' && echo yes || echo no)"
check "  the refusal is about the INTERPRETER, not an earlier check" "yes" \
      "$(printf '%s' "$out" | grep -qiE 'anchor|TLS key' && echo no || echo yes)"
check "the driver no longer instructs use of production's pyenv interpreter" "0" \
      "$(grep -cE '^[^#]*pyenv/versions/py311' "$EXEC")"
check "  and the reason is kept as a comment" "yes" \
      "$(grep -qE '^ *#.*pyenv/versions/py311' "$EXEC" && echo yes || echo no)"

echo
suite_summary "$PASS" "$FAIL"
