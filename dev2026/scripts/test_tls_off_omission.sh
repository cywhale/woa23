#!/usr/bin/env bash
#
# TLS off means the key and certificate paths are ABSENT — and TLS on is unchanged.
#
# WHY. `bs3v1`'s staging app process carried
#     WOA23_TLS_CERTFILE=/home/odbadmin/python/woa23/conf/fullchain.pem
#     WOA23_TLS_KEYFILE =/home/odbadmin/python/woa23/conf/privkey.pem
# — production paths in a staging process. They were never opened (zero file descriptors
# under /home/odbadmin), but NON-USE IS NOT NON-EXPOSURE. They were carried in.
#
# The fix omits them when TLS is off, where they are meaningless anyway. The thing that
# must NOT change, and is asserted here in both directions, is the TLS behaviour itself:
# TLS is ON by default, and when on both paths are required, defaulted and checked for
# readability before the port is claimed. Omission when off must never widen into
# "TLS can be skipped".
#
#     ./scripts/test_tls_off_omission.sh

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
GEN="$REPO/deploy/make_staging_override.js"
PROD="$REPO/deploy/ecosystem.production.config.js"
APP="$REPO/deploy/production_app.sh"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then pass=$((pass + 1)); echo "  ok   $1"
  else fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}
has() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

W="$(mktemp -d)"
STORE="$W/store"; mkdir -p "$STORE"
LOG=tmp-tlstest

gen() {   # gen <outfile>
  WOA23_PM2C_GRANTED=yes node "$GEN" \
    --source "$PROD" --out "$1" --name woa23-tlstest-candidate \
    --port 18299 --store "$STORE" --logdir "$LOG" \
    --python /usr/bin/python3 >/dev/null 2>&1
}

echo "1. TLS OFF — the key and certificate must not appear in the generated config"
gen "$W/off.js"
check "the generator succeeded" 0 "$?"
check "WOA23_TLS is off" "off" \
      "$(node -e "console.log(require('$W/off.js').apps[0].env.WOA23_TLS)")"
check "WOA23_TLS_KEYFILE is ABSENT" "absent" \
      "$(node -e "console.log(('WOA23_TLS_KEYFILE' in require('$W/off.js').apps[0].env)?'present':'absent')")"
check "WOA23_TLS_CERTFILE is ABSENT" "absent" \
      "$(node -e "console.log(('WOA23_TLS_CERTFILE' in require('$W/off.js').apps[0].env)?'present':'absent')")"
check "no /home/odbadmin conf path anywhere in the staging env" "clean" \
      "$(node -e "
        const e=require('$W/off.js').apps[0].env;
        const bad=Object.keys(e).filter(k=>String(e[k]).includes('/home/odbadmin/python/woa23/conf'));
        console.log(bad.length? 'LEAKED:'+bad.join(','):'clean')")"
check "  and none anywhere in the whole staging app object" "clean" \
      "$(node -e "
        const a=require('$W/off.js').apps[0];
        console.log(JSON.stringify(a).includes('/conf/privkey.pem')||
                    JSON.stringify(a).includes('/conf/fullchain.pem') ? 'LEAKED':'clean')")"

echo
echo "2. the PRODUCTION config is untouched — omission is staging-only"
check "production still declares a TLS key" "yes" \
      "$(node -e "console.log(require('$PROD').apps[0].env.WOA23_TLS_KEYFILE?'yes':'no')")"
check "production still declares a TLS certificate" "yes" \
      "$(node -e "console.log(require('$PROD').apps[0].env.WOA23_TLS_CERTFILE?'yes':'no')")"
check "production does NOT set WOA23_TLS=off" "yes" \
      "$(node -e "console.log(require('$PROD').apps[0].env.WOA23_TLS==='off'?'no':'yes')")"
check "  so nothing here disables TLS for production" "yes" \
      "$(has "$(cat "$PROD")" 'WOA23_TLS_KEYFILE')"

echo
echo "3. TLS ON in production_app.sh — default, required, checked. UNCHANGED."
code="$(grep -v '^[[:space:]]*#' "$APP")"
check "TLS defaults to ON when WOA23_TLS is unset" 1 \
      "$(printf '%s' "$code" | grep -cE '\$\{WOA23_TLS:-on\}' || true)"
check "  only an explicit 'off' disables it" 1 \
      "$(printf '%s' "$code" | grep -cE '\$\{WOA23_TLS:-on\}" = "off"' || true)"
check "the key is still defaulted when TLS is on" 1 \
      "$(printf '%s' "$code" | grep -cE 'KEYFILE="\$\{WOA23_TLS_KEYFILE:-conf/privkey.pem\}"' || true)"
check "the certificate is still defaulted when TLS is on" 1 \
      "$(printf '%s' "$code" | grep -cE 'CERTFILE="\$\{WOA23_TLS_CERTFILE:-conf/fullchain.pem\}"' || true)"
check "an unreadable key still fails closed" 1 \
      "$(printf '%s' "$code" | grep -c 'TLS key not readable' || true)"
check "an unreadable certificate still fails closed" 1 \
      "$(printf '%s' "$code" | grep -c 'TLS certificate not readable' || true)"
check "  both are readability-checked, not merely non-empty" 2 \
      "$(printf '%s' "$code" | grep -cE '\[ -r "\$(KEY|CERT)FILE" \]' || true)"
check "--keyfile/--certfile are still passed when TLS is on" 1 \
      "$(printf '%s' "$code" | grep -cE 'TLS_ARGS=\(--keyfile "\$KEYFILE" --certfile "\$CERTFILE"\)' || true)"
check "TLS_ARGS is empty when TLS is off" 1 \
      "$(printf '%s' "$code" | grep -cE '^TLS_ARGS=\(\)' || true)"

echo
echo "4. TLS OFF must not resolve, read or open a key or certificate"
check "the paths are NOT defaulted outside the TLS-on branch" 0 \
      "$(printf '%s' "$code" | grep -cE '^KEYFILE="\$\{WOA23_TLS_KEYFILE' || true)"
check "  they start empty" 1 \
      "$(printf '%s' "$code" | grep -cE '^KEYFILE=""; CERTFILE=""' || true)"
check "no readability test sits outside the TLS-on branch" 0 \
      "$(printf '%s' "$code" | awk '/\$\{WOA23_TLS:-on\}" = "off"/{inoff=1} /^else$/{inoff=0} inoff && /-r "\$(KEY|CERT)FILE"/{n++} END{print n+0}')"

echo
echo "5. BEHAVIOURAL: run the launcher with TLS off and read the argv it would exec"
STUB="$W/stub"; mkdir -p "$STUB"
cat > "$STUB/python" <<'PYEOF'
#!/bin/sh
echo "ARGV: $*"
exit 0
PYEOF
chmod +x "$STUB/python"
# The launcher validates the store's anchor group (B4) BEFORE it reaches the TLS branch.
# A bare empty directory dies at the anchor, which would make the TLS assertions below
# pass for the wrong reason — the first draft of this fixture did exactly that, and the
# §6 "it refuses" check was green because the ANCHOR was missing, not the key.
mkdir -p "$W/zstore/1_degree/annual/TS"
printf '{"zarr_format":2}\n' > "$W/zstore/1_degree/annual/TS/.zgroup"
( cd "$W" && WOA23_PORT=18299 WOA23_ZARR_STORE="$W/zstore" WOA23_TLS=off \
    WOA23_PYTHON="$STUB/python" WOA23_WORKERS=2 \
    bash "$APP" > "$W/off.argv" 2>"$W/off.err" ) || true
argv="$(grep '^ARGV:' "$W/off.argv" 2>/dev/null || true)"
check "the launcher produced an argv" yes "$([ -n "$argv" ] && echo yes || echo no)"
check "  it contains NO --keyfile" 0 "$(printf '%s' "$argv" | grep -c -- '--keyfile' || true)"
check "  it contains NO --certfile" 0 "$(printf '%s' "$argv" | grep -c -- '--certfile' || true)"
check "  it binds the requested port" 1 "$(printf '%s' "$argv" | grep -c '127.0.0.1:18299' || true)"
check "  and it warns that TLS was explicitly disabled" yes \
      "$(has "$(cat "$W/off.err")" 'TLS explicitly disabled')"
check "  no privkey.pem or fullchain.pem is named at all" 0 \
      "$(printf '%s' "$argv" | grep -cE 'privkey.pem|fullchain.pem' || true)"

echo
echo "6. BEHAVIOURAL: TLS ON with a missing key must FAIL CLOSED"
( cd "$W" && WOA23_PORT=18299 WOA23_ZARR_STORE="$W/zstore" \
    WOA23_TLS_KEYFILE="$W/nope-key.pem" WOA23_TLS_CERTFILE="$W/nope-cert.pem" \
    WOA23_PYTHON="$STUB/python" WOA23_WORKERS=2 \
    bash "$APP" > "$W/on.argv" 2>"$W/on.err" ); rc=$?
check "it refuses" yes "$([ "$rc" != 0 ] && echo yes || echo no)"
check "  naming the unreadable key" yes "$(has "$(cat "$W/on.err")" 'TLS key not readable')"
check "  and it is the TLS check that refused, not the store check" no \
      "$(has "$(cat "$W/on.err")" 'anchor group metadata')"
check "  and it started nothing" 0 "$(grep -c '^ARGV:' "$W/on.argv" || true)"

echo "  TLS UNSET (the default) must behave as ON, not as off:"
( cd "$W" && WOA23_PORT=18299 WOA23_ZARR_STORE="$W/zstore" \
    WOA23_TLS_KEYFILE="$W/nope-key.pem" WOA23_TLS_CERTFILE="$W/nope-cert.pem" \
    WOA23_PYTHON="$STUB/python" WOA23_WORKERS=2 \
    bash "$APP" > "$W/unset.argv" 2>"$W/unset.err" ); rc2=$?
check "unset WOA23_TLS refuses on a missing key" yes "$([ "$rc2" != 0 ] && echo yes || echo no)"
check "  it did NOT silently serve plain HTTP" 0 "$(grep -c '^ARGV:' "$W/unset.argv" || true)"

echo "  TLS ON with a READABLE key and certificate must pass them through:"
printf 'k\n' > "$W/k.pem"; printf 'c\n' > "$W/c.pem"
( cd "$W" && WOA23_PORT=18299 WOA23_ZARR_STORE="$W/zstore" \
    WOA23_TLS_KEYFILE="$W/k.pem" WOA23_TLS_CERTFILE="$W/c.pem" \
    WOA23_PYTHON="$STUB/python" WOA23_WORKERS=2 \
    bash "$APP" > "$W/onok.argv" 2>"$W/onok.err" ) || true
okargv="$(grep '^ARGV:' "$W/onok.argv" 2>/dev/null || true)"
check "TLS on passes --keyfile" 1 "$(printf '%s' "$okargv" | grep -c -- "--keyfile $W/k.pem" || true)"
check "  and --certfile" 1 "$(printf '%s' "$okargv" | grep -c -- "--certfile $W/c.pem" || true)"

echo
echo "7. the staging harness requires ABSENCE when off, and equality when on"
sec="$(grep -v '^[[:space:]]*#' "$REPO/deploy/staging_execute.sh")"
check "the env check is TLS-aware" 1 \
      "$(printf '%s' "$sec" | grep -cE 'CFG_TLS=' || true)"
# The TLS paths are NO LONGER checked by env_must, which reads the master only. They are
# checked over master + workers, so asserting env_must lines here would now pass on a
# script that had stopped checking the workers entirely.
check "  the TLS paths are NOT left to the master-only env_must" 0 \
      "$(printf '%s' "$sec" | grep -cE 'env_must WOA23_TLS_(KEY|CERT)FILE' || true)"
check "  both are checked over the master+worker pid list" "yes" \
      "$(printf '%s' "$sec" | grep -q 'for v in WOA23_TLS_KEYFILE WOA23_TLS_CERTFILE' \
         && echo yes || echo no)"
check "  TLS off requires ABSENT" "yes" \
      "$(printf '%s' "$sec" | grep -q '\[ "\$got" = "ABSENT" \] || tls_invalid_environment' \
         && echo yes || echo no)"
check "  TLS on still requires them to match the config" "yes" \
      "$(printf '%s' "$sec" | grep -q 'want="\$(node -e "console.log(require(.\$CONFIG.).apps\[0\].env.\$v)")"' \
         && echo yes || echo no)"
check "the WOA23_* allowlist is unchanged and still fails closed" 1 \
      "$(printf '%s' "$sec" | grep -c 'ALLOWED_ENV=' || true)"

echo
echo "8. B3 and B5's existing checks are NOT relaxed"
check "B5: --reload is still asserted absent from the argv" 1 \
      "$(printf '%s' "$sec" | grep -c 'no woa23_app, no --reload' || true)"
check "B5: the launcher still contains no --reload" 0 \
      "$(printf '%s' "$code" | grep -c -- '--reload' || true)"
check "B3: the port is still required with no default" 1 \
      "$(printf '%s' "$code" | grep -cE 'WOA23_PORT is missing or empty' || true)"
check "B3: no 8050 literal in the launcher" 0 "$(printf '%s' "$code" | grep -c '8050' || true)"
check "B2: the app is still api.app:app" 1 \
      "$(printf '%s' "$code" | grep -cE '^APP="api.app:app"' || true)"
check "B4: the store is still required and validated" 1 \
      "$(printf '%s' "$code" | grep -cE 'WOA23_ZARR_STORE is missing or empty' || true)"

echo
echo "9. BEHAVIOURAL: the ACTUAL child process environment, spawned as PM2 would"
# §1 proves the keys are absent from the generated config; §7 proves the VM24 harness
# checks /proc/<pid>/environ. Neither proves the config actually YIELDS a child without
# them. PM2 spawns the app with `env` merged over its own environment, so this reproduces
# that merge offline and reads the environment the child really received.
#
# The parent deliberately EXPORTS both variables first: if the staging env merely omits
# them, an inherited value would still reach the child. That is the bs3v1 exposure shape,
# and it must not survive.
node -e '
  const cfg = require(process.argv[1]).apps[0];
  const child = require("child_process").spawnSync(
    process.execPath, ["-e", "console.log(JSON.stringify(process.env))"],
    { env: Object.assign({}, process.env, cfg.env), encoding: "utf8" });
  process.stdout.write(child.stdout);
' "$W/off.js" > "$W/childenv.json" 2>"$W/childenv.err" \
  <<<"" || true
childenv() { node -e "
  const e=require('$W/childenv.json');
  console.log((process.argv[1] in e) ? 'present:'+e[process.argv[1]] : 'absent')" "$1"; }
# make the inherited-value case real
WOA23_TLS_KEYFILE=/home/odbadmin/python/woa23/conf/privkey.pem \
WOA23_TLS_CERTFILE=/home/odbadmin/python/woa23/conf/fullchain.pem \
node -e '
  const cfg = require(process.argv[1]).apps[0];
  const child = require("child_process").spawnSync(
    process.execPath, ["-e", "console.log(JSON.stringify(process.env))"],
    { env: Object.assign({}, process.env, cfg.env), encoding: "utf8" });
  process.stdout.write(child.stdout);
' "$W/off.js" > "$W/childenv2.json" 2>/dev/null || true

check "the child environment was captured" yes \
      "$([ -s "$W/childenv.json" ] && echo yes || echo no)"
check "WOA23_TLS reached the child as off" "present:off" "$(childenv WOA23_TLS)"
check "WOA23_TLS_KEYFILE is ABSENT from the child environment" "absent" \
      "$(childenv WOA23_TLS_KEYFILE)"
check "WOA23_TLS_CERTFILE is ABSENT from the child environment" "absent" \
      "$(childenv WOA23_TLS_CERTFILE)"
check "  the staging port DID reach the child (the merge really happened)" "present:18299" \
      "$(childenv WOA23_PORT)"
echo
echo "10. OMISSION IS NOT REMOVAL — and the /proc check is a DETECTOR, not a remedy"
# THE DISTINCTION THIS SECTION EXISTS TO KEEP. Omitting the keys from the generated config
# removes the STRUCTURAL source -- the carry-over in make_staging_override.js, which is
# where bs3v1's paths came from. It does NOT scrub a value an ancestor already exports:
# PM2 merges `env` over its own environment and has no "unset" directive, so an operator
# shell or an already-running daemon holding WOA23_TLS_KEYFILE still passes it down.
#
# There are therefore THREE separate things, and calling any of them by another's name is
# the error this file guards against:
#
#   omission    the config does not carry the paths           (necessary, not sufficient)
#   PREVENTION  staging_execute.sh UNSETS both before pm2 start  -- this is the remedy,
#               and it is tested functionally in test_staging_entry.sh, which reads the
#               environment the spawned pm2 child actually received
#   DETECTION   the /proc/<pid>/environ check on master AND workers, which FAILS THE RUN
#               as INVALID_ENVIRONMENT -- this catches a leak, it does not prevent one
#
# The assertions below cover omission and detection. Prevention is functional and lives
# with the entry, because only a real spawn can demonstrate it.
check "an inherited parent value DOES still reach the child of a CONFIG-ONLY omission" "inherited" \
      "$(node -e "
        const e=require('$W/childenv2.json');
        const v=e.WOA23_TLS_KEYFILE||'';
        console.log(v.includes('/home/odbadmin')? 'inherited' : 'clean')")"

# The three env readers are lifted VERBATIM from the real script so this tracks the source
# rather than a copy that can drift. env_value_of does the work; env_value is the
# master-scoped wrapper; env_must is the assertion.
sed -n '/^env_value_of()/,/^}/p;/^env_value()/p;/^env_must()/,/^}/p' \
    "$REPO/deploy/staging_execute.sh" > "$W/envlib.sh"
check "  all three readers were lifted from the real script" 3 \
      "$(grep -cE '^env_(value_of|value|must)\(\)' "$W/envlib.sh" || true)"
cat > "$W/envcase.sh" <<'EOS'
die() { printf '%s\n' "$@" >&2; exit 2; }
PROC="$FAKE_PROC"; PID=1
. "$ENVLIB"
env_must WOA23_TLS_KEYFILE "ABSENT"
EOS
mkdir -p "$W/fakeproc/1"

printf 'WOA23_TLS=off\0WOA23_PORT=18299\0' > "$W/fakeproc/1/environ"
out="$(FAKE_PROC="$W/fakeproc" ENVLIB="$W/envlib.sh" bash "$W/envcase.sh" 2>&1)"; rc=$?
check "a genuinely absent key PASSES the ABSENT check" 0 "$rc"

printf 'WOA23_TLS=off\0WOA23_TLS_KEYFILE=/home/odbadmin/python/woa23/conf/privkey.pem\0' \
  > "$W/fakeproc/1/environ"
out="$(FAKE_PROC="$W/fakeproc" ENVLIB="$W/envlib.sh" bash "$W/envcase.sh" 2>&1)"; rc=$?
check "an INHERITED key FAILS the run" 2 "$rc"
check "  and the failure names the leaked path" yes \
      "$(has "$out" '/home/odbadmin/python/woa23/conf/privkey.pem')"
check "  reported as a process env mismatch" yes "$(has "$out" 'PROCESS ENV MISMATCH')"

# An empty-but-present variable is NOT absence, and must not be read as absence.
printf 'WOA23_TLS=off\0WOA23_TLS_KEYFILE=\0' > "$W/fakeproc/1/environ"
out="$(FAKE_PROC="$W/fakeproc" ENVLIB="$W/envlib.sh" bash "$W/envcase.sh" 2>&1)"; rc=$?
check "a PRESENT-but-empty key is not absence, and fails" 2 "$rc"

echo
echo "11. the entry PREVENTS the leak; it does not only detect it"
sec2="$(grep -v '^[[:space:]]*#' "$REPO/deploy/staging_execute.sh")"
check "both TLS paths are unset before pm2 start" 2 \
      "$(printf '%s' "$sec2" | grep -cE '^[[:space:]]*unset WOA23_TLS_(KEY|CERT)FILE[[:space:]]*$' || true)"
check "  the unset precedes the start, not follows it" "yes" \
      "$(printf '%s' "$sec2" | awk '/^[[:space:]]*unset WOA23_TLS_KEYFILE/{u=NR} /"\$PM2" start /{s=NR}
                                    END{print (u && s && u < s) ? "yes" : "no"}')"
check "TLS on injects the paths explicitly rather than inheriting them" 1 \
      "$(printf '%s' "$sec2" | grep -c 'export WOA23_TLS_KEYFILE WOA23_TLS_CERTFILE' || true)"
check "the TLS check covers workers, not the master alone" "yes" \
      "$(printf '%s' "$sec2" | grep -q 'children_of_pid "\$PID"' && echo yes || echo no)"
check "  and zero workers is refused rather than counted as clean" 1 \
      "$(printf '%s' "$sec2" | grep -c 'no worker processes were found under the master' || true)"
check "a leak is classified INVALID_ENVIRONMENT" 1 \
      "$(printf '%s' "$sec2" | grep -c 'INVALID_ENVIRONMENT: ' || true)"
check "  and explicitly not a clean PASS" 1 \
      "$(printf '%s' "$sec2" | grep -c 'NOT a clean PASS and yields NO B3/B5' || true)"

rm -r "$W"
echo
suite_summary "$pass" "$fail"
