#!/usr/bin/env bash
#
# The PROPOSED production launcher and PM2 config, checked offline. Nothing is started,
# no port is bound, no PM2 is invoked, and conf/ is only ever READ.
#
# Every check here corresponds to a named cutover blocker (spec 010 §5a) or to a
# property that must survive the cutover. The behavioural checks run the launcher with
# a stub interpreter, so the argv it would hand to gunicorn is observed directly rather
# than inferred from reading the file.

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHER="$HERE/deploy/production_app.sh"
ECOSYSTEM="$HERE/deploy/ecosystem.production.config.js"
PROD_LAUNCHER="$HERE/../conf/start_app.sh"
PROD_ECOSYSTEM="$HERE/../conf/ecosystem.config.js"

PASS=0; FAIL=0
check() {  # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}
# Comments stripped before matching: this launcher explains at length what it does NOT
# do — it names woa23_app, --reload, kill -9 and pre_stop in prose precisely to say
# they are absent. A plain grep cannot tell a mention from a use.
code() {
  case "$1" in
    *.js) sed -e 's|//.*$||' "$1" ;;
    *)    sed -e 's/^[[:space:]]*#.*$//' "$1" ;;
  esac
}
has()   { code "$1" | grep -qF -- "$2" && echo yes || echo no; }
hasre() { code "$1" | grep -qE -- "$2" && echo yes || echo no; }

# ---------------------------------------------------------------- a sandbox to run in
WORK="$(mktemp -d)"
cleanup() { chmod -R u+w "$WORK" 2>/dev/null; rm -rf "$WORK"; }
trap cleanup EXIT

STORE="$WORK/store"
mkdir -p "$STORE/1_degree/annual/TS"
printf '{"zarr_format":2}' > "$STORE/1_degree/annual/TS/.zgroup"
printf 'key\n'  > "$WORK/privkey.pem"
printf 'cert\n' > "$WORK/fullchain.pem"

# A stub interpreter. It prints the argv it was given and exits 0, so `exec` is
# observable without gunicorn, a port, or the application's dependencies.
STUB="$WORK/python-stub"
cat > "$STUB" <<'STUBEOF'
#!/usr/bin/env bash
if [ "${1:-}" = "--version" ]; then echo "Python 3.11.4 (stub)"; exit 0; fi
printf 'ARGV:'; printf ' %s' "$@"; printf '\n'
STUBEOF
chmod +x "$STUB"

# run_launcher <expected-exit> — runs with a base environment, returns output
run_launcher() {
  env -i PATH="$PATH" HOME="$WORK" \
      WOA23_PYTHON="$STUB" \
      "$@" bash "$LAUNCHER" 2>&1
}

echo "the launcher exists, parses, and fails closed"
check "production_app.sh exists" "yes" "$([ -f "$LAUNCHER" ] && echo yes || echo no)"
check "it is valid bash" "yes" "$(bash -n "$LAUNCHER" 2>/dev/null && echo yes || echo no)"
check "it is executable" "yes" "$([ -x "$LAUNCHER" ] && echo yes || echo no)"
check "set -euo pipefail" "yes" "$(has "$LAUNCHER" 'set -euo pipefail')"
in_deploy=no
case "$LAUNCHER" in */dev2026/deploy/*) in_deploy=yes ;; esac
check "it lives in dev2026/deploy, not conf/" "yes" "$in_deploy"

echo
echo "B2 — it serves the candidate, never the old app"
check "it launches api.app:app" "yes" "$(has "$LAUNCHER" 'APP="api.app:app"')"
check "woa23_app:app appears in no effective line" "no" "$(has "$LAUNCHER" 'woa23_app:app')"
out="$(run_launcher WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "the argv it execs names api.app:app" "yes" \
      "$(printf '%s' "$out" | grep -q 'ARGV:.*api\.app:app' && echo yes || echo no)"
check "the argv it execs never names woa23_app" "no" \
      "$(printf '%s' "$out" | grep -q 'ARGV:.*woa23_app' && echo yes || echo no)"

echo
echo "B3 — the port is configuration, not a literal"
check "no bare 8050 in effective launcher code" "no" "$(hasre "$LAUNCHER" '127\.0\.0\.1:8050')"
check "the port comes from WOA23_PORT" "yes" "$(has "$LAUNCHER" 'PORT="${WOA23_PORT:-}"')"
out="$(run_launcher WOA23_ZARR_STORE="$STORE" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"; rc=$?
check "a missing port is refused" "yes" \
      "$(printf '%s' "$out" | grep -q 'WOA23_PORT is missing or empty' && echo yes || echo no)"
check "and nothing is started" "no" \
      "$(printf '%s' "$out" | grep -q 'ARGV:' && echo yes || echo no)"
out="$(run_launcher WOA23_PORT='' WOA23_ZARR_STORE="$STORE" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "an EMPTY port is refused too, not defaulted" "yes" \
      "$(printf '%s' "$out" | grep -q 'WOA23_PORT is missing or empty' && echo yes || echo no)"
out="$(run_launcher WOA23_PORT=notanumber WOA23_ZARR_STORE="$STORE" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "a non-numeric port is refused" "yes" \
      "$(printf '%s' "$out" | grep -q 'not a number' && echo yes || echo no)"
out="$(run_launcher WOA23_PORT=8051 WOA23_ZARR_STORE="$STORE" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "the configured port reaches the argv" "yes" \
      "$(printf '%s' "$out" | grep -q 'ARGV:.*127\.0\.0\.1:8051' && echo yes || echo no)"

echo
echo "B4 — WOA23_ZARR_STORE is required and validated"
check "it reads WOA23_ZARR_STORE" "yes" "$(has "$LAUNCHER" 'STORE="${WOA23_ZARR_STORE:-}"')"
out="$(run_launcher WOA23_PORT=8050 \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "a missing store is refused by name" "yes" \
      "$(printf '%s' "$out" | grep -q 'WOA23_ZARR_STORE is missing or empty' && echo yes || echo no)"
out="$(run_launcher WOA23_PORT=8050 WOA23_ZARR_STORE="$WORK/nope" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "a nonexistent store directory is refused" "yes" \
      "$(printf '%s' "$out" | grep -q 'is not a directory' && echo yes || echo no)"
mkdir -p "$WORK/anchorless"
out="$(run_launcher WOA23_PORT=8050 WOA23_ZARR_STORE="$WORK/anchorless" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "a store with no anchor group is refused BEFORE start" "yes" \
      "$(printf '%s' "$out" | grep -q 'no readable anchor group metadata' && echo yes || echo no)"
check "and that refusal starts nothing" "no" \
      "$(printf '%s' "$out" | grep -q 'ARGV:' && echo yes || echo no)"

echo
echo "B5 — --reload is gone"
check "no --reload in effective launcher code" "no" "$(has "$LAUNCHER" '--reload')"
out="$(run_launcher WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "and none in the argv it would exec" "no" \
      "$(printf '%s' "$out" | grep -q 'ARGV:.*--reload' && echo yes || echo no)"
check "production's launcher DOES carry it (the defect is real)" "yes" \
      "$(grep -qF -- '--reload' "$PROD_LAUNCHER" && echo yes || echo no)"

echo
echo "B1 — the grep-based kill -9 pre_stop is removed, not rewritten"
check "no pre_stop key in the proposed config" "no" "$(hasre "$ECOSYSTEM" 'pre_stop\s*:')"
check "no kill -9 anywhere in it" "no" "$(has "$ECOSYSTEM" 'kill -9')"
check "no ps -ef | grep pipeline in it" "no" "$(has "$ECOSYSTEM" 'ps -ef')"
# These two asserted the defect was still live in production's config. Stage B removed it
# on VM24 and the repository source was reconciled to the same bytes, so they now assert
# the opposite -- that the proposed config and the production config AGREE on being clean.
# Existence first -- `conf/` is outside the subject archive, and a missing file yields the
# same "no" the assertion wants. Asserted so the flip cannot become a vacuous pass.
check "production's config is present to be checked" "yes" \
      "$([ -f "$PROD_ECOSYSTEM" ] && echo yes || echo no)"
check "  and identifies itself as the woa23 app" "yes" \
      "$(grep -qF "name: 'woa23'" "$PROD_ECOSYSTEM" && echo yes || echo no)"
check "production's config no longer carries pre_stop" "no" \
      "$(grep -qE 'pre_stop\s*:' "$PROD_ECOSYSTEM" && echo yes || echo no)"
check "  nor any kill -9" "no" \
      "$(grep -qF 'kill -9' "$PROD_ECOSYSTEM" && echo yes || echo no)"
check "the launcher execs so PM2 tracks gunicorn itself" "yes" \
      "$(hasre "$LAUNCHER" '^exec "\$PY" -m gunicorn')"

echo
echo "TLS survives the cutover"
out="$(run_launcher WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "--keyfile is passed" "yes" \
      "$(printf '%s' "$out" | grep -q 'ARGV:.*--keyfile' && echo yes || echo no)"
check "--certfile is passed" "yes" \
      "$(printf '%s' "$out" | grep -q 'ARGV:.*--certfile' && echo yes || echo no)"
out="$(run_launcher WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" \
        WOA23_TLS_KEYFILE="$WORK/missing.pem" WOA23_TLS_CERTFILE="$WORK/fullchain.pem")"
check "an unreadable key is refused BEFORE the port is claimed" "yes" \
      "$(printf '%s' "$out" | grep -q 'TLS key not readable' && echo yes || echo no)"
check "and nothing is started" "no" \
      "$(printf '%s' "$out" | grep -q 'ARGV:' && echo yes || echo no)"
out="$(run_launcher WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" \
        WOA23_TLS_KEYFILE="$WORK/privkey.pem" WOA23_TLS_CERTFILE="$WORK/missing.pem")"
check "an unreadable certificate is refused" "yes" \
      "$(printf '%s' "$out" | grep -q 'TLS certificate not readable' && echo yes || echo no)"
# The important negative: a typo must not silently downgrade HTTPS to HTTP.
check "a bad cert path does NOT fall back to plain HTTP" "no" \
      "$(printf '%s' "$out" | grep -q 'ARGV:' && echo yes || echo no)"
out="$(run_launcher WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" WOA23_TLS=off)"
check "TLS off is possible, but only when asked explicitly" "yes" \
      "$(printf '%s' "$out" | grep -q 'ARGV:' && echo yes || echo no)"
check "and it warns when it does" "yes" \
      "$(printf '%s' "$out" | grep -q 'TLS explicitly disabled' && echo yes || echo no)"
check "with no --keyfile in the argv" "no" \
      "$(printf '%s' "$out" | grep -q 'ARGV:.*--keyfile' && echo yes || echo no)"

echo
echo "the interpreter is named, never resolved through PATH"
check "it never execs a bare gunicorn" "no" "$(hasre "$LAUNCHER" '^exec gunicorn')"
out="$(env -i PATH="$PATH" HOME="$WORK" WOA23_PYTHON="$WORK/absent" \
        WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" WOA23_TLS=off bash "$LAUNCHER" 2>&1)"
check "a missing interpreter is refused" "yes" \
      "$(printf '%s' "$out" | grep -q 'no interpreter at' && echo yes || echo no)"

echo
echo "WOA23_PYTHON IS REQUIRED — no shared-pyenv fallback (spec 016, the pm2G finding)"
# pm2G built an isolated venv, manifested 58 packages for it, and then served from the
# shared py311 env with ZERO libraries mapped from that venv. The cause was this default:
# absent was not neutral, absent SELECTED the shared environment -- and the environment
# contract required it absent, so the fallback was the only reachable behaviour.
check "there is no ':-' default for WOA23_PYTHON any more" "no" \
      "$(has "$LAUNCHER" 'PY="${WOA23_PYTHON:-/home')"
check "the shared pyenv path is not hardcoded as a fallback" "no" \
      "$(hasre "$LAUNCHER" 'WOA23_PYTHON:-.*versions/py311')"
check "WOA23_PYTHON is read with an empty default, then refused" "yes" \
      "$(has "$LAUNCHER" 'PY="${WOA23_PYTHON:-}"')"

# No arrays: under bash 3.2 with `set -u`, expanding an EMPTY array is an error, and the
# `unset` case needs exactly that. The suite asserts 3.2 compatibility a few blocks down,
# so this cannot use the 4.4+ `${arr[@]+...}` idiom either.
for label in unset empty spaces; do
  case "$label" in
    unset)  out="$(env -i PATH="$PATH" HOME="$WORK" \
                     WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" WOA23_TLS=off \
                     bash "$LAUNCHER" 2>&1)"; rc=$? ;;
    empty)  out="$(env -i PATH="$PATH" HOME="$WORK" WOA23_PYTHON= \
                     WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" WOA23_TLS=off \
                     bash "$LAUNCHER" 2>&1)"; rc=$? ;;
    spaces) out="$(env -i PATH="$PATH" HOME="$WORK" WOA23_PYTHON="   " \
                     WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" WOA23_TLS=off \
                     bash "$LAUNCHER" 2>&1)"; rc=$? ;;
  esac
  check "WOA23_PYTHON $label: the launcher refuses" "2" "$rc"
  check "  and names the variable" "yes" \
        "$(printf '%s' "$out" | grep -q 'WOA23_PYTHON is not set' && echo yes || echo no)"
  check "  and nothing was exec'd" "no" \
        "$(printf '%s' "$out" | grep -q '^ARGV:' && echo yes || echo no)"
done

# A path that exists but cannot be executed is not an interpreter.
NOEXEC="$WORK/not-executable"; printf '#!/bin/sh\n' > "$NOEXEC"; chmod 644 "$NOEXEC"
out="$(env -i PATH="$PATH" HOME="$WORK" WOA23_PYTHON="$NOEXEC" \
        WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" WOA23_TLS=off bash "$LAUNCHER" 2>&1)"
check "a non-executable interpreter is refused" "yes" \
      "$(printf '%s' "$out" | grep -q 'no interpreter at' && echo yes || echo no)"

# And the positive control: named explicitly, it runs and that interpreter is the one used.
out="$(env -i PATH="$PATH" HOME="$WORK" WOA23_PYTHON="$STUB" \
        WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" WOA23_TLS=off bash "$LAUNCHER" 2>&1)"
check "named explicitly, the launcher starts" "yes" \
      "$(printf '%s' "$out" | grep -q '^ARGV:' && echo yes || echo no)"
check "  and the named interpreter is the one that ran" "yes" \
      "$(printf '%s' "$out" | grep -q "gunicorn" && echo yes || echo no)"

echo
echo "the PM2 config: an env block with no placeholders (the pm2A lesson)"
check "the config exists" "yes" "$([ -f "$ECOSYSTEM" ] && echo yes || echo no)"
check "it keeps the production app name 'woa23'" "yes" "$(hasre "$ECOSYSTEM" "name:\s*'woa23'")"
# Text-matched the old absolute-ish path until the cwd fix made `script` relative to
# `cwd`. The node-evaluated check above now proves the stronger property — that the path
# actually RESOLVES to an existing production_app.sh — so this one only needs to confirm
# the file is named at all.
check "it points at the proposed launcher" "yes" "$(has "$ECOSYSTEM" 'production_app.sh')"
check "it never points at conf/start_app.sh" "no" "$(has "$ECOSYSTEM" 'conf/start_app.sh')"
check "append_env_to_name is false, so --env cannot fork the app name" "yes" \
      "$(hasre "$ECOSYSTEM" 'append_env_to_name:\s*false')"
check "autorestart is kept" "yes" "$(hasre "$ECOSYSTEM" 'autorestart:\s*true')"
check "the 4G memory ceiling is kept" "yes" "$(has "$ECOSYSTEM" "max_memory_restart: '4G'")"
check "kill_timeout leaves room for the graceful drain" "yes" "$(has "$ECOSYSTEM" 'kill_timeout: 20000')"
# Every env value must be non-empty: pm2A failed on an EMPTY placeholder that the
# config layered over a correct command-line value.
empty_env="$(code "$ECOSYSTEM" | sed -n "/env:\s*{/,/}/p" | grep -cE ":\s*''\s*,?$")"
check "no env value is an empty placeholder" "0" "$empty_env"
for v in WOA23_PORT WOA23_ZARR_STORE WOA23_TLS_KEYFILE WOA23_TLS_CERTFILE WOA23_WORKERS; do
  check "env carries $v" "yes" "$(has "$ECOSYSTEM" "$v:")"
done

echo
echo "conf/ is untouched by this proposal"
check "conf/start_app.sh still launches the OLD app" "yes" \
      "$(grep -qF 'woa23_app:app' "$PROD_LAUNCHER" && echo yes || echo no)"
check "conf/start_app.sh still hard-codes 8050" "yes" \
      "$(grep -qF '127.0.0.1:8050' "$PROD_LAUNCHER" && echo yes || echo no)"
check "conf/ecosystem.config.js still names ./conf/start_app.sh" "yes" \
      "$(grep -qF './conf/start_app.sh' "$PROD_ECOSYSTEM" && echo yes || echo no)"

echo
echo "the config's cwd makes BOTH the script and api.app resolvable"
STAGING_LAUNCHER="$HERE/deploy/start_staging.sh"
STAGING_ECOSYSTEM="$HERE/deploy/ecosystem.staging.config.js"
# This section exists because the first version of the production config could not work.
# It set `script: './dev2026/deploy/production_app.sh'` with no `cwd`, and two requirements
# pulled opposite ways: that path resolves only from the repository root, while
# `python -m gunicorn api.app:app` puts cwd on sys.path and `api/` lives under dev2026/.
# PM2 would have reported `online` and gunicorn would have died at import — the B4 failure
# mode through a different door. Reading the launcher could never have shown it; only
# resolving the config's own paths does.
#
# node evaluates the config, because node is what PM2 uses. Text-matching a .js file would
# be guessing at what it evaluates to.
if command -v node >/dev/null 2>&1; then
  NODE_OUT="$(node -e "
    const c = require('$ECOSYSTEM').apps[0];
    const path = require('path'), fs = require('fs');
    const cwd = c.cwd ? path.resolve(c.cwd) : null;
    console.log('has_cwd=' + (c.cwd ? 'yes' : 'no'));
    console.log('script_exists=' + (cwd && c.script ? fs.existsSync(path.resolve(cwd, c.script)) : false));
    console.log('api_importable_from_cwd=' + (cwd ? fs.existsSync(path.resolve(cwd, 'api', 'app.py')) : false));
    console.log('script_is_production_app=' + /production_app\.sh\$/.test(c.script || ''));
    console.log('has_pre_stop=' + ('pre_stop' in c));
    console.log('name=' + c.name);
  " 2>&1)"
  get() { printf '%s\n' "$NODE_OUT" | grep "^$1=" | cut -d= -f2; }
  check "the config sets an explicit cwd" "yes" "$(get has_cwd)"
  check "the script resolves from that cwd" "true" "$(get script_exists)"
  check "and api/app.py is importable from that same cwd" "true" "$(get api_importable_from_cwd)"
  check "the script is production_app.sh" "true" "$(get script_is_production_app)"
  check "no pre_stop key survives evaluation" "false" "$(get has_pre_stop)"
  check "the app name is production's" "woa23" "$(get name)"
  # The staging config must satisfy the same property — it is what pm2B proved.
  NODE_STG="$(node -e "
    const c = require('$STAGING_ECOSYSTEM').apps[0];
    const path = require('path'), fs = require('fs');
    const cwd = c.cwd ? path.resolve(c.cwd) : null;
    console.log('ok=' + (cwd && fs.existsSync(path.resolve(cwd, c.script)) && fs.existsSync(path.resolve(cwd, 'api', 'app.py'))));
  " 2>&1)"
  check "the staging config resolves both from its cwd too" "ok=true" "$NODE_STG"
else
  PASS=$((PASS+1)); echo "  ok   (skipped: node unavailable — cannot evaluate a PM2 config)"
fi
# TLS paths must be absolute: with cwd now dev2026/, a relative 'conf/privkey.pem' would
# resolve to dev2026/conf/privkey.pem, which does not exist. The launcher would refuse —
# correctly — but for a reason nobody would guess from the config.
check "TLS key path is absolute" "yes" \
      "$(hasre "$ECOSYSTEM" "WOA23_TLS_KEYFILE: '/" )"
check "TLS cert path is absolute" "yes" \
      "$(hasre "$ECOSYSTEM" "WOA23_TLS_CERTFILE: '/" )"

echo
echo "B3/B5 — staging and production configuration never merge"
# B3, the direction that matters most: staging must not be able to reach 8050. It has no
# port default at all, and refuses production's ports outright.
check "the staging launcher has NO port default" "no" \
      "$(hasre "$STAGING_LAUNCHER" 'WOA23_STAGING_PORT:-[0-9]')"
check "and refuses 8050 outright" "yes" "$(has "$STAGING_LAUNCHER" '8050|8786|8787')"
check "the staging config carries no port value at all" "no" \
      "$(hasre "$STAGING_ECOSYSTEM" '8050|WOA23_STAGING_PORT')"
# B3, the other direction: 8050 belongs to the production CONFIG, never to the launcher.
check "8050 is set in the production config" "yes" "$(has "$ECOSYSTEM" "WOA23_PORT: '8050'")"
check "and appears in no effective line of the production launcher" "no" \
      "$(hasre "$LAUNCHER" '8050')"
# Neither launcher may borrow the other's machinery.
check "the production launcher does not read the staging port ledger" "no" \
      "$(has "$LAUNCHER" 'ports_used.tsv')"
check "the staging launcher DOES read it (that rule is staging's)" "yes" \
      "$(has "$STAGING_LAUNCHER" 'ports_used.tsv')"
check "the production config never names the staging launcher" "no" \
      "$(has "$ECOSYSTEM" 'start_staging.sh')"
check "the staging config never names the production launcher" "no" \
      "$(has "$STAGING_ECOSYSTEM" 'production_app.sh')"
check "the two configs use different app names" "no" \
      "$(has "$STAGING_ECOSYSTEM" "name: 'woa23'")"
# B5 both ways: reload is absent from production AND cannot be defaulted on in staging.
check "no --reload in the staging launcher either" "no" "$(has "$STAGING_LAUNCHER" '--reload')"
check "no reload variable that could default to on, in staging" "no" \
      "$(hasre "$STAGING_LAUNCHER" 'RELOAD|reload=')"
check "nor in production" "no" "$(hasre "$LAUNCHER" 'RELOAD|reload=')"
# A behavioural back-stop for B5: no environment variable can turn reload on.
out="$(env -i PATH="$PATH" HOME="$WORK" WOA23_PYTHON="$STUB" WOA23_RELOAD=1 WOA23_TLS=off \
        WOA23_PORT=8050 WOA23_ZARR_STORE="$STORE" bash "$LAUNCHER" 2>&1)"
check "WOA23_RELOAD=1 does not put --reload in the argv" "no" \
      "$(printf '%s' "$out" | grep -q 'ARGV:.*--reload' && echo yes || echo no)"

echo
echo "B4 — an UNREADABLE store is refused, not just a missing one"
UNREADABLE="$WORK/unreadable-store"
mkdir -p "$UNREADABLE/1_degree/annual/TS"
printf '{}' > "$UNREADABLE/1_degree/annual/TS/.zgroup"
chmod 000 "$UNREADABLE" 2>/dev/null
if [ -r "$UNREADABLE" ]; then
  # running as root, or a filesystem that ignores the mode: the case cannot be posed
  PASS=$((PASS+1)); echo "  ok   (skipped: this user can read a 0000 directory)"
else
  out="$(run_launcher WOA23_PORT=8050 WOA23_ZARR_STORE="$UNREADABLE" WOA23_TLS=off)"
  check "an unreadable store is refused" "yes" \
        "$(printf '%s' "$out" | grep -qE 'is not readable|no readable anchor' && echo yes || echo no)"
  check "and nothing is started" "no" \
        "$(printf '%s' "$out" | grep -q 'ARGV:' && echo yes || echo no)"
fi
chmod 755 "$UNREADABLE" 2>/dev/null

echo
echo "every deploy file is tracked and archived — no launcher may be invisible to the commit"
for f in production_app.sh production_stop.sh start_staging.sh make_staging_store.py \
         verify_staging_env.sh ecosystem.production.config.js ecosystem.staging.config.js; do
  check "deploy/$f is tracked by git" "yes" \
        "$(git -C "$HERE" ls-files --error-unmatch "deploy/$f" >/dev/null 2>&1 && echo yes || echo no)"
done
# The failure this guards against is real: a .gitignore pattern once matched a module,
# `git add -A` skipped it silently, and a remote run died on a file that existed here.
# SOURCE files only. `deploy/__pycache__` is git-ignored and should be — it is bytecode.
# The first version of this check counted it and reported a tracking failure for a
# directory that must never be tracked.
check "no deploy SOURCE file is git-ignored" "0" \
      "$(git -C "$HERE" check-ignore deploy/*.sh deploy/*.js deploy/*.py 2>/dev/null \
         | wc -l | tr -d ' ')"
check "no untracked file in deploy/" "0" \
      "$(git -C "$HERE" ls-files --others --exclude-standard deploy/ | wc -l | tr -d ' ')"
check "no untracked shell/JS/spec anywhere in dev2026" "0" \
      "$(git -C "$HERE" ls-files --others --exclude-standard . \
         | grep -cE '\.(sh|js|md|py)$' | tr -d ' ')"

echo
echo "shell portability — bash 3.2 and bash 5.x both have to run these"
# VM24 runs bash 5.x; this repository is developed on macOS, whose /bin/bash is 3.2.57.
# Two constructs have already cost real time by differing between them, and both are
# checked here across EVERY shell file we own rather than at the site that broke.
#
#   1. `case` inside a command substitution — bash 3.2 cannot parse `$(case X in a) ...
#      esac)` and dies with "syntax error near unexpected token". bash 5 accepts it.
#      This bit twice in one day, in two different files.
#   2. an unguarded empty-array expansion under `set -u` — `"${arr[@]}"` on an empty
#      array is an unbound-variable error on 3.2 and fine on 5. That one hid in the
#      TLS-off path of this very launcher, reachable only when someone disabled TLS.
#
# Both are bash-3.2 failures, so a suite that runs green on 3.2 has exercised the
# stricter of the two parsers. That is NOT the same as having run on 5.x — see
# spec 011 §2.5 and the caveat in the pm2B follow-up report.
# Comments are stripped first, via the same `code` helper the rest of this suite uses.
# The first version of this check flagged the paragraph ABOVE, which merely NAMES the
# construct — the identical mention-vs-use trap that the comment-stripping exists for.
#
# Only the `case` construct is checked statically. Whether an array can be EMPTY is not
# decidable by grep, and a blanket ban on `"${arr[@]}"` flagged 45 correct iterations
# over arrays that are non-empty by construction. A check that would force 45 rewrites
# to prove nothing is worse than no check; that class is covered where it can actually
# be observed — the behavioural TLS-off run above, which executes the empty-array path.
shell_files() { ls "$HERE"/deploy/*.sh "$HERE"/scripts/*.sh 2>/dev/null; }
bad_case=0
for f in $(shell_files); do
  n="$(code "$f" | grep -cE '\$\([^)]*\bcase\b' || true)"
  [ "${n:-0}" -gt 0 ] && { bad_case=$((bad_case + n)); echo "       $f: case inside \$( )"; }
done
check "no \`case\` inside a command substitution in any shell file" "0" "$bad_case"
check "every shell file parses under bash 3.2" "yes" \
      "$(rc=yes; for f in $(shell_files); do bash -n "$f" 2>/dev/null || rc=no; done; echo "$rc")"
check "this host's bash really is the strict one (3.2)" "3" \
      "$(bash --version | head -1 | sed -E 's/.*version ([0-9]+).*/\1/')"

echo
suite_summary "$PASS" "$FAIL"
