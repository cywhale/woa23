#!/usr/bin/env bash
#
# deploy/make_staging_override.js — the generator/differ, checked offline.
#
# Nothing is started, no PM2 is invoked, no port is bound, and no production data is
# copied or read. The "production config" in the failure cases is a DOCTORED COPY in a
# temporary directory: the generator resolves it from its own __dirname, so copying the
# generator beside a doctored config exercises the real code path without touching the
# real config.
#
# The property under test is narrow and load-bearing: a staging run must differ from the
# production config in exactly five items and in nothing else. If a sixth difference can
# slip through, the run validates a configuration production will not have — which is the
# failure the whole exercise exists to prevent.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GEN="$HERE/deploy/make_staging_override.js"
PROD="$HERE/deploy/ecosystem.production.config.js"

PASS=0; FAIL=0
check() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}

if ! command -v node >/dev/null 2>&1; then
  echo "node is unavailable; this suite cannot evaluate a PM2 config."
  echo "all skipped (0 assertions)"; suite_summary_line 0 0; exit 0
fi

WORK="$(mktemp -d)"
cleanup() { chmod -R u+w "$WORK" 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

STORE="$WORK/store"
mkdir -p "$STORE/1_degree/annual/TS"
printf '{"zarr_format":2}' > "$STORE/1_degree/annual/TS/.zgroup"

# A stand-in for the staged venv's interpreter. `--python` is required now (spec 016):
# the generator writes it into env.WOA23_PYTHON so the launcher runs THIS run's venv
# instead of falling back to the shared pyenv environment, which is what pm2G exposed.
PYBIN="$WORK/venv/bin/python"
mkdir -p "$WORK/venv/bin"
printf '#!/usr/bin/env bash\nexit 0\n' > "$PYBIN"
chmod +x "$PYBIN"

# gen <extra-env> -- <args...>  : run with the grant set, capture output and rc
gen() {
  env -i PATH="$PATH" HOME="$WORK" WOA23_PM2C_GRANTED=yes "$@" 2>&1
}

echo "the grant is enforced in both directions"
out="$(env -i PATH="$PATH" HOME="$WORK" node "$GEN" --name c --port 18271 \
        --store "$STORE" --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN" 2>&1)"; rc=$?
check "no grant is refused" "2" "$rc"
check "and it names the grant it wants" "yes" \
      "$(echo "$out" | grep -q 'WOA23_PM2C_GRANTED is not' && echo yes || echo no)"
for other in WOA23_S2PERF_GRANTED WOA23_S2_C1_GRANTED WOA23_D1_GRANTED WOA23_BASH5_VERIFY_GRANTED; do
  out="$(env -i PATH="$PATH" HOME="$WORK" WOA23_PM2C_GRANTED=yes "$other=yes" \
          node "$GEN" --name c --port 18271 --store "$STORE" --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN" 2>&1)"; rc=$?
  check "$other alongside is refused" "2" "$rc"
done
out="$(env -i PATH="$PATH" HOME="$WORK" WOA23_PM2C_GRANTED=maybe node "$GEN" --name c \
        --port 18271 --store "$STORE" --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN" 2>&1)"; rc=$?
check "a grant that is not exactly 'yes' is refused" "2" "$rc"

echo
echo "the happy path: exactly the six permitted items differ"
out="$(gen node "$GEN" --name woa23-test-candidate --port 18271 --store "$STORE" \
        --logdir tmp-test --out "$WORK/good.js" --python "$PYBIN")"; rc=$?
check "it exits 0" "0" "$rc"
check "the output states the 6 permitted items" "yes" \
      "$(echo "$out" | grep -q '6 permitted' && echo yes || echo no)"
# SUPERSEDED, not deleted. This asserted 8 differing keys. With TLS off the key and
# certificate variables are now REMOVED from the staging config, and their disappearance
# is itself a difference -- so there are 10. What the original protected is unchanged and
# still asserted: the diff is EXHAUSTIVE and BOUNDED, and an unexpected key still fails.
check "ten keys differ (3 log paths, and 2 TLS paths REMOVED)" "yes" \
      "$(echo "$out" | grep -q 'exactly 10 differing keys' && echo yes || echo no)"
check "the generated file exists" "yes" "$([ -f "$WORK/good.js" ] && echo yes || echo no)"
check "and node can evaluate it" "yes" \
      "$(node -e "require('$WORK/good.js')" 2>/dev/null && echo yes || echo no)"

# The generated config must carry PRODUCTION's cwd and script, unchanged. This is the
# reason the run is worth doing at all.
PROD_CWD="$(node -e "console.log(require('$PROD').apps[0].cwd)")"
PROD_SCRIPT="$(node -e "console.log(require('$PROD').apps[0].script)")"
GEN_CWD="$(node -e "console.log(require('$WORK/good.js').apps[0].cwd)")"
GEN_SCRIPT="$(node -e "console.log(require('$WORK/good.js').apps[0].script)")"
check "cwd is production's, unchanged" "$PROD_CWD" "$GEN_CWD"
check "script is production's, unchanged" "$PROD_SCRIPT" "$GEN_SCRIPT"
check "the script resolves from that cwd" "true" \
      "$(node -e "const p=require('path'),f=require('fs');console.log(f.existsSync(p.resolve('$GEN_CWD','$GEN_SCRIPT')))")"
check "api/app.py is importable from that cwd" "true" \
      "$(node -e "const p=require('path'),f=require('fs');console.log(f.existsSync(p.resolve('$GEN_CWD','api','app.py')))")"
check "the five overrides took effect: name" "woa23-test-candidate" \
      "$(node -e "console.log(require('$WORK/good.js').apps[0].name)")"
check "  port" "18271" "$(node -e "console.log(require('$WORK/good.js').apps[0].env.WOA23_PORT)")"
check "  store" "$STORE" "$(node -e "console.log(require('$WORK/good.js').apps[0].env.WOA23_ZARR_STORE)")"
check "  TLS off" "off" "$(node -e "console.log(require('$WORK/good.js').apps[0].env.WOA23_TLS)")"
check "  log path" "tmp-test/staging.log" \
      "$(node -e "console.log(require('$WORK/good.js').apps[0].out_file)")"
# SUPERSEDED, not deleted. This asserted the staging config carried production's TLS
# paths through unchanged. bs3v1 showed that meant a STAGING process ran with production
# paths in its environment -- never opened, but carried in, and non-use is not
# non-exposure. With TLS off they are now ABSENT. The property the original protected --
# staging never INVENTS or alters a TLS path -- is preserved and asserted below.
check "with TLS off, the key path is ABSENT from the staging config" "yes" \
      "$(node -e "
        const b=require('$WORK/good.js').apps[0].env;
        console.log(('WOA23_TLS_KEYFILE' in b) ? 'no':'yes')")"
check "  and the certificate path is ABSENT too" "yes" \
      "$(node -e "
        const b=require('$WORK/good.js').apps[0].env;
        console.log(('WOA23_TLS_CERTFILE' in b) ? 'no':'yes')")"
check "  no production path survives anywhere in the staging env" "yes" \
      "$(node -e "
        const b=require('$WORK/good.js').apps[0].env;
        const bad=Object.keys(b).filter(k=>String(b[k]).indexOf('/home/odbadmin/python/woa23/conf')>=0);
        console.log(bad.length===0 ? 'yes':'no ('+bad.join(',')+')')")"
check "  and staging still INVENTS no TLS path of its own" "yes" \
      "$(node -e "
        const b=require('$WORK/good.js').apps[0].env;
        const inv=Object.keys(b).filter(k=>/TLS_(KEY|CERT)FILE/.test(k));
        console.log(inv.length===0 ? 'yes':'no')")"
check "the PRODUCTION config's own TLS defaults are UNCHANGED" "yes" \
      "$(node -e "
        const a=require('$PROD').apps[0].env;
        console.log(a.WOA23_TLS_KEYFILE && a.WOA23_TLS_CERTFILE ? 'yes':'no')")"
check "worker count is production's, unchanged" "yes" \
      "$(node -e "
        const a=require('$PROD').apps[0].env, b=require('$WORK/good.js').apps[0].env;
        console.log(a.WOA23_WORKERS===b.WOA23_WORKERS ? 'yes':'no')")"
check "no pre_stop in the generated config" "false" \
      "$(node -e "console.log('pre_stop' in require('$WORK/good.js').apps[0])")"

echo
echo "argument validation"
for missing in name port store logdir out python; do
  A=(--name c --port 18271 --store "$STORE" --logdir tmp-x --out "$WORK/o.js" \
     --python "$PYBIN")
  B=(); i=0
  while [ $i -lt ${#A[@]} ]; do
    if [ "${A[$i]}" = "--$missing" ]; then i=$((i+2)); continue; fi
    B+=("${A[$i]}" "${A[$((i+1))]}"); i=$((i+2))
  done
  out="$(gen node "$GEN" "${B[@]}")"; rc=$?
  check "--$missing missing is refused" "2" "$rc"
done
out="$(gen node "$GEN" --name c --port '' --store "$STORE" --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "an empty --port is refused" "2" "$rc"
out="$(gen node "$GEN" --name c --port eighty --store "$STORE" --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "a non-numeric --port is refused" "2" "$rc"
out="$(gen node "$GEN" --name c --port 99999 --store "$STORE" --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "an out-of-range --port is refused" "2" "$rc"

echo
echo "it cannot be pointed at production"
out="$(gen node "$GEN" --name woa23 --port 18271 --store "$STORE" --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "the production app name is refused" "2" "$rc"
check "  and says why" "yes" "$(echo "$out" | grep -q "may not be named" && echo yes || echo no)"
for p in 8050 8786 8787; do
  out="$(gen node "$GEN" --name c --port "$p" --store "$STORE" --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
  check "port $p is refused" "2" "$rc"
done
out="$(gen node "$GEN" --name c --port 18271 --store /home/odbadmin/python/woa23/data \
        --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "the production store is refused" "2" "$rc"
out="$(gen node "$GEN" --name c --port 18271 --store /home/odbadmin/python/woa23/data/sub \
        --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "a path INSIDE the production store is refused" "2" "$rc"

echo
echo "path escape"
out="$(gen node "$GEN" --name c --port 18271 --store "relative/store" --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "a relative --store is refused" "2" "$rc"
out="$(gen node "$GEN" --name c --port 18271 --store "/home/odbadmin/python/woa23/data/../data" \
        --logdir tmp-x --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "a --store containing .. is refused" "2" "$rc"
out="$(gen node "$GEN" --name c --port 18271 --store "$STORE" --logdir /tmp/abs --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "an absolute --logdir is refused" "2" "$rc"
out="$(gen node "$GEN" --name c --port 18271 --store "$STORE" --logdir "../../tmp" --out "$WORK/o.js" --python "$PYBIN")"; rc=$?
check "a --logdir escaping the app dir is refused" "2" "$rc"
check "  and names the escape" "yes" "$(echo "$out" | grep -q "escapes the app directory" && echo yes || echo no)"

echo
echo "a DOCTORED production config: the sixth difference must fail closed"
# The generator resolves its config from its own __dirname, so a copy beside a doctored
# config exercises the real path without touching the real file.
doctor() {  # doctor <node-expression-mutating-`a`> -> prints rc
  local mutate="$1" dir="$WORK/d$RANDOM"
  mkdir -p "$dir"
  cp "$GEN" "$dir/gen.js"
  node -e "
    const c = require('$PROD');
    const a = c.apps[0];
    $mutate
    require('fs').writeFileSync('$dir/ecosystem.production.config.js',
      'module.exports = ' + JSON.stringify(c, null, 1) + '\n');
  " || { echo 99; return; }
  env -i PATH="$PATH" HOME="$WORK" WOA23_PM2C_GRANTED=yes \
      node "$dir/gen.js" --name c --port 18271 --store "$STORE" --logdir tmp-x \
      --out "$dir/out.js" --python "$PYBIN" > "$dir/log.txt" 2>&1
  echo $?
}
# Nothing mutated: the doctored copy is the real config, so it must still succeed. This is
# the control — without it, every case below would "pass" for the wrong reason.
check "an UNmutated copy still generates cleanly" "0" "$(doctor '')"
check "a missing cwd is refused" "2" "$(doctor 'delete a.cwd;')"
check "a missing script is refused" "2" "$(doctor 'delete a.script;')"
check "a missing env is refused" "2" "$(doctor 'delete a.env;')"
check "a missing log_file is refused" "2" "$(doctor 'delete a.log_file;')"
check "a missing env.WOA23_PORT is refused" "2" "$(doctor 'delete a.env.WOA23_PORT;')"
check "a numeric env.WOA23_PORT is refused (wrong type)" "2" "$(doctor 'a.env.WOA23_PORT = 8050;')"
check "an array cwd is refused (wrong type)" "2" "$(doctor 'a.cwd = ["/x"];')"
check "an object script is refused (wrong type)" "2" "$(doctor 'a.script = {};')"
check "two apps are refused" "2" "$(doctor 'c.apps.push(JSON.parse(JSON.stringify(a)));')"
# THE PATTERN COMES FROM THE FIXTURE, not from a copy typed here. It used to be an inline
# string, which meant the historical hook existed in two places that could drift apart --
# and once Stage B removed it from the real config, an inline copy would have been the only
# surviving record, in a single test, uncommented. The fixture is now the one source, and
# driving the guard from it is what keeps the fixture load-bearing rather than decorative.
LEGACY_FIXTURE="$HERE/scripts/fixtures/legacy-pre-stop.fixture.js"
check "the regression fixture exists" "yes" "$([ -f "$LEGACY_FIXTURE" ] && echo yes || echo no)"
LEGACY_PATTERN="$(node -e "process.stdout.write(require('$LEGACY_FIXTURE').LEGACY_PRE_STOP)")"
contains() { case "$2" in *"$1"*) echo yes ;; *) echo no ;; esac; }   # helper, not case-in-$( )
check "  and it carries the historical kill -9 hook" "yes" \
      "$(contains 'kill -9' "$LEGACY_PATTERN")"
check "a resurrected pre_stop is refused — driven FROM the fixture" "2" \
      "$(doctor "a.pre_stop = $(node -e "process.stdout.write(JSON.stringify(require('$LEGACY_FIXTURE').LEGACY_PRE_STOP))");")"
check "  and the fixture's own app object carries it, for a real refusal input" "yes" \
      "$(node -e "console.log(('pre_stop' in require('$LEGACY_FIXTURE').apps[0]) ? 'yes':'no')")"

echo
echo "the differ is not a formality — a SIXTH override fails closed"
# Grepping the generator's source for its own error strings would only prove the strings
# exist. What matters is whether the differ FIRES, so a COPY of the generator is patched
# to behave like a future careless edit, and the copy is run for real.
patched_gen() {   # patched_gen <sed-expression> -> rc
  local expr="$1" dir="$WORK/p$RANDOM"
  mkdir -p "$dir"
  cp "$PROD" "$dir/ecosystem.production.config.js"
  sed "$expr" "$GEN" > "$dir/gen.js"
  env -i PATH="$PATH" HOME="$WORK" WOA23_PM2C_GRANTED=yes \
      node "$dir/gen.js" --name c --port 18271 --store "$STORE" --logdir tmp-x \
      --out "$dir/out.js" --python "$PYBIN" > "$WORK/last.log" 2>&1
  # The log goes to a FIXED path, not one returned in a variable: this function is called
  # inside $( ), which is a subshell, so an assignment here would never reach the caller.
  # That is the same trap that once made a diagnostic variable read back empty.
  echo $?
}
# A sixth override — someone "helpfully" changes the worker count for staging.
rc="$(patched_gen "s|^app.env.WOA23_TLS = 'off'|app.env.WOA23_WORKERS = '9'; app.env.WOA23_TLS = 'off'|")"
check "a sixth override (WOA23_WORKERS) is refused" "2" "$rc"
check "  and the refusal names the offending key" "yes" \
      "$(grep -q 'WOA23_WORKERS' "$WORK/last.log" && echo yes || echo no)"
# A sixth override on a structural key — the one that would silently invalidate the run.
rc="$(patched_gen "s|^app.env.WOA23_TLS = 'off'|app.args = '--extra'; app.env.WOA23_TLS = 'off'|")"
check "a sixth override (args) is refused" "2" "$rc"
check "  and it is caught as an unexpected difference" "yes" \
      "$(grep -qE 'unexpected difference|differs, and it must not' "$WORK/last.log" && echo yes || echo no)"
# cwd is the key the missing-cwd defect turned on: changing it must never pass.
rc="$(patched_gen "s|^app.env.WOA23_TLS = 'off'|app.cwd = '/tmp'; app.env.WOA23_TLS = 'off'|")"
check "a sixth override (cwd) is refused" "2" "$rc"
# A MISSING override — someone removes one and staging silently keeps production's value.
rc="$(patched_gen "s|^app.env.WOA23_TLS = 'off'||")"
check "a REMOVED override is refused" "2" "$rc"
check "  and it says the override did not take effect" "yes" \
      "$(grep -q 'did not take effect' "$WORK/last.log" && echo yes || echo no)"
rc="$(patched_gen "s|^app.name = args.name||")"
check "a removed name override is refused too" "2" "$rc"

echo
echo "--python: the sixth override, and the fallback it exists to close (spec 016)"
# pm2G built an isolated venv, manifested it, and served from the shared py311 env with
# ZERO libraries mapped from that venv — because WOA23_PYTHON was required ABSENT and
# production_app.sh defaulted to the shared environment. The generator now writes the
# interpreter in, and refuses the values that would put the old behaviour back.

out="$(gen node "$GEN" --name c --port 18271 --store "$STORE" --logdir tmp-x \
        --out "$WORK/py-ok.js" --python "$PYBIN")"; rc=$?
check "a good --python generates cleanly" "0" "$rc"
check "  env.WOA23_PYTHON is written into the config" "$PYBIN" \
      "$(node -e "console.log(require('$WORK/py-ok.js').apps[0].env.WOA23_PYTHON)")"
check "  and the summary names it as the sixth item" "yes" \
      "$(echo "$out" | grep -q 'WOA23_PYTHON' && echo yes || echo no)"

out="$(gen node "$GEN" --name c --port 18271 --store "$STORE" --logdir tmp-x \
        --out "$WORK/py-rel.js" --python "venv/bin/python")"; rc=$?
check "a RELATIVE --python is refused" "2" "$rc"
check "  and says it must be absolute" "yes" \
      "$(echo "$out" | grep -q 'must be an absolute path' && echo yes || echo no)"

out="$(gen node "$GEN" --name c --port 18271 --store "$STORE" --logdir tmp-x \
        --out "$WORK/py-dots.js" --python "$WORK/venv/bin/../bin/python")"; rc=$?
check "a non-normal-form --python is refused" "2" "$rc"

out="$(gen node "$GEN" --name c --port 18271 --store "$STORE" --logdir tmp-x \
        --out "$WORK/py-missing.js" --python "$WORK/venv/bin/absent")"; rc=$?
check "a MISSING --python is refused" "2" "$rc"
check "  and says the venv must be built first" "yes" \
      "$(echo "$out" | grep -q 'must be built BEFORE' && echo yes || echo no)"

NOEXEC="$WORK/venv/bin/noexec"; printf '#!/bin/sh\n' > "$NOEXEC"; chmod 644 "$NOEXEC"
out="$(gen node "$GEN" --name c --port 18271 --store "$STORE" --logdir tmp-x \
        --out "$WORK/py-noexec.js" --python "$NOEXEC")"; rc=$?
check "a NON-EXECUTABLE --python is refused" "2" "$rc"

# THE ONE THAT MATTERS: naming the shared environment pm2G actually ran on.
SHARED="$WORK/.pyenv/versions/py311/bin/python3.11"
mkdir -p "$(dirname "$SHARED")"; printf '#!/bin/sh\n' > "$SHARED"; chmod +x "$SHARED"
out="$(gen node "$GEN" --name c --port 18271 --store "$STORE" --logdir tmp-x \
        --out "$WORK/py-shared.js" --python "$SHARED")"; rc=$?
check "the SHARED py311 interpreter is refused by name" "2" "$rc"
check "  and the refusal explains the pm2G failure" "yes" \
      "$(echo "$out" | grep -q 'builds an isolated venv' && echo yes || echo no)"

echo
echo "no production data is copied or read"
check "the generator never reads the production store" "no" \
      "$(grep -qE "readFileSync|readdirSync|cpSync|copyFile" "$GEN" && echo yes || echo no)"
check "it writes exactly one file, the config it was asked for" "1" \
      "$(grep -c "writeFileSync" "$GEN")"

echo
suite_summary "$PASS" "$FAIL"
