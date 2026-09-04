#!/usr/bin/env bash
#
# Read-only Bash 5.x compatibility evidence for deploy/production_app.sh.
#
# WHAT THIS IS FOR. The offline suite proves the launcher's refusal logic on bash 3.2,
# which is what macOS ships and is the stricter parser. VM24 runs bash 5.x, and
# production_app.sh has never executed there. `pm2B` exercised start_staging.sh and
# verify_staging_env.sh under bash 5, but not this file. This closes that one gap and
# nothing else.
#
# WHAT THIS IS NOT. Not a deployment. Not PM2 validation. Not a test of the launcher's
# SUCCESSFUL start against a real store. It never starts gunicorn, PM2 or Dask, never
# binds a port, never sends an HTTP request, and never reads or writes production's
# store, PM2 state or conf/.
#
# HOW THE SUCCESS PATH IS HANDLED. One case does reach the `exec` — deliberately, because
# the empty-array expansion under `set -u` that this whole exercise exists for is ONLY
# reachable there. It is neutralised twice over: the interpreter is a STUB that prints
# its argv and exits, and the store is a synthetic temporary directory, never
# production's. No gunicorn binary is involved and no port is bound. That is argv
# verification, not a launch.
#
#   WOA23_BASH5_VERIFY_GRANTED=yes ./scripts/verify_bash5_refusals.sh <path-to-launcher>
#
# Every case records: name, bash version, exit code, stdout, stderr, the refusal expected,
# whether the stub was invoked, and whether any process or listener appeared.
set -uo pipefail

# --------------------------------------------------------------------------- the grant
# Its OWN grant. Reusing the PM2, C1, C2, D1 or S2 grant would let an authorisation for
# one kind of run start another kind, which is the thing per-action authorisation exists
# to prevent.
GRANT="${WOA23_BASH5_VERIFY_GRANTED:-}"
if [ "$GRANT" != "yes" ]; then
  echo "REFUSING: WOA23_BASH5_VERIFY_GRANTED is not 'yes'." >&2
  echo "  This runner performs a VM24 action and needs its own explicit grant." >&2
  echo "  It does NOT accept WOA23_S2PERF_GRANTED, WOA23_S2_C1_GRANTED," >&2
  echo "  WOA23_S2_C2_GRANTED, WOA23_D1_GRANTED, WOA23_D2A_GRANTED or" >&2
  echo "  WOA23_D2B_GRANTED: an authorisation for one run is not one for another." >&2
  exit 2
fi
for other in WOA23_S2PERF_GRANTED WOA23_S2_C1_GRANTED WOA23_S2_C2_GRANTED \
             WOA23_D1_GRANTED WOA23_D2A_GRANTED WOA23_D2B_GRANTED; do
  eval "v=\${$other:-}"
  if [ -n "$v" ]; then
    echo "REFUSING: $other is set in this environment." >&2
    echo "  A Bash 5 verification must not run beside another run's grant." >&2
    exit 2
  fi
done

TARGET="${1:-}"
[ -n "$TARGET" ] || { echo "usage: verify_bash5_refusals.sh <path-to-production_app.sh>" >&2; exit 2; }
[ -f "$TARGET" ] || { echo "no such file: $TARGET" >&2; exit 2; }

# The launcher under test must be OURS, never production's. conf/start_app.sh starts the
# old app on 8050; running it even once would be a production action.
case "$TARGET" in
  */conf/start_app.sh|*/conf/*)
    echo "REFUSING: $TARGET is under conf/. This runner never executes production's launcher." >&2
    exit 2 ;;
esac
grep -q 'api.app:app' "$TARGET" || {
  echo "REFUSING: $TARGET does not look like the candidate launcher (no api.app:app)." >&2; exit 2; }

WORK="${WOA23_BASH5_WORKDIR:-}"
[ -n "$WORK" ] || { echo "WOA23_BASH5_WORKDIR is required and has no default." >&2; exit 2; }
[ -e "$WORK" ] && { echo "REFUSING: workdir already exists: $WORK" >&2; exit 2; }
mkdir -p "$WORK/cases" || exit 2

BASH_VER="$(bash --version | head -1)"
BASH_MAJOR="$(printf '%s\n' "$BASH_VER" | sed -E 's/.*version ([0-9]+).*/\1/')"

echo "=== Bash 5.x read-only refusal verification ==="
echo "  target        : $TARGET"
echo "  target sha256 : $(sha256sum "$TARGET" 2>/dev/null | cut -d' ' -f1 || shasum -a 256 "$TARGET" | cut -d' ' -f1)"
echo "  bash          : $BASH_VER"
echo "  bash major    : $BASH_MAJOR"
echo "  workdir       : $WORK"
echo "  grant         : WOA23_BASH5_VERIFY_GRANTED=yes"
echo

# ------------------------------------------------------------------- the fixture world
FIX="$WORK/fixture"
mkdir -p "$FIX/store/1_degree/annual/TS"
printf '{"zarr_format":2}' > "$FIX/store/1_degree/annual/TS/.zgroup"
mkdir -p "$FIX/anchorless"
printf 'not-a-real-key\n'  > "$FIX/privkey.pem"
printf 'not-a-real-cert\n' > "$FIX/fullchain.pem"
printf 'x\n' > "$FIX/notadir"
mkdir -p "$FIX/unreadable"; chmod 000 "$FIX/unreadable" 2>/dev/null

# The stub interpreter. It CANNOT bind, serve or fork: it appends its argv to a marker
# file and exits. Its presence in that file is how "was anything launched?" is answered
# per case, rather than by trusting the launcher's own output.
STUB="$WORK/python-stub"
MARKER="$WORK/stub-invocations.txt"
: > "$MARKER"
cat > "$STUB" <<STUBEOF
#!/usr/bin/env bash
if [ "\${1:-}" = "--version" ]; then echo "Python 3.11.4 (stub, never executes anything)"; exit 0; fi
{ printf 'INVOKED:'; printf ' %s' "\$@"; printf '\n'; } >> "$MARKER"
exit 0
STUBEOF
chmod +x "$STUB"

# A port number used ONLY as a value to pass in. Nothing binds it; it is checked after
# every case precisely to prove that.
PROBE_PORT=18251

listeners_on() {   # portable: /proc-based ss on Linux, netstat elsewhere
  if command -v ss >/dev/null 2>&1; then
    ss -ltn 2>/dev/null | grep -c ":$1 "
  else
    netstat -an 2>/dev/null | grep -c "\.$1 .*LISTEN"
  fi
}

PASS=0; FAIL=0
BASELINE_LISTENERS="$(listeners_on "$PROBE_PORT")"

# run_case <name> <expect-substring> <expect-stub-invoked yes|no> <env assignments...>
run_case() {
  local name="$1" expect="$2" expect_stub="$3"; shift 3
  local dir="$WORK/cases/$name"; mkdir -p "$dir"
  local before_marker after_marker rc out err listeners procs_before procs_after
  before_marker="$(wc -l < "$MARKER" | tr -d ' ')"
  procs_before="$(ps -eo pid= | wc -l | tr -d ' ')"

  # env -i so nothing from this shell leaks in; the launcher must see only what a case
  # gives it. PATH is kept because the launcher calls sed/grep.
  env -i PATH="$PATH" HOME="$WORK" "$@" bash "$TARGET" \
      > "$dir/stdout.txt" 2> "$dir/stderr.txt"
  rc=$?

  after_marker="$(wc -l < "$MARKER" | tr -d ' ')"
  procs_after="$(ps -eo pid= | wc -l | tr -d ' ')"
  listeners="$(listeners_on "$PROBE_PORT")"
  local stub_invoked=no
  [ "$after_marker" -gt "$before_marker" ] && stub_invoked=yes

  {
    echo "case            : $name"
    echo "bash            : $BASH_VER"
    echo "exit_code       : $rc"
    echo "expected        : $expect"
    echo "expect_stub     : $expect_stub"
    echo "stub_invoked    : $stub_invoked"
    echo "listeners_$PROBE_PORT : $listeners (baseline $BASELINE_LISTENERS)"
    echo "proc_count      : before=$procs_before after=$procs_after"
    echo "--- stdout ---"; cat "$dir/stdout.txt"
    echo "--- stderr ---"; cat "$dir/stderr.txt"
  } > "$dir/record.txt"

  local ok=yes reason=""
  if [ "$expect_stub" = "no" ]; then
    [ "$rc" -eq 2 ] || { ok=no; reason="$reason exit=$rc not 2;"; }
  else
    [ "$rc" -eq 0 ] || { ok=no; reason="$reason exit=$rc not 0;"; }
  fi
  grep -qF -- "$expect" "$dir/stdout.txt" "$dir/stderr.txt" \
    || { ok=no; reason="$reason expected text absent;"; }
  [ "$stub_invoked" = "$expect_stub" ] \
    || { ok=no; reason="$reason stub_invoked=$stub_invoked want $expect_stub;"; }
  [ "$listeners" = "$BASELINE_LISTENERS" ] \
    || { ok=no; reason="$reason listeners changed;"; }

  if [ "$ok" = yes ]; then
    PASS=$((PASS+1)); printf '  ok   %-26s rc=%s stub=%-3s listeners=%s\n' \
      "$name" "$rc" "$stub_invoked" "$listeners"
  else
    FAIL=$((FAIL+1)); printf '  FAIL %-26s %s\n' "$name" "$reason"
  fi
}

echo "0. the file parses under this bash"
if bash -n "$TARGET" 2>"$WORK/parse.err"; then
  PASS=$((PASS+1)); echo "  ok   bash -n            (no syntax error under $BASH_VER)"
else
  FAIL=$((FAIL+1)); echo "  FAIL bash -n            $(cat "$WORK/parse.err")"
fi

echo
echo "1. refusal cases — each must fail closed, BEFORE anything is started"
run_case port-missing        "WOA23_PORT is missing or empty"   no \
  WOA23_ZARR_STORE="$FIX/store" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case port-empty          "WOA23_PORT is missing or empty"   no \
  WOA23_PORT="" WOA23_ZARR_STORE="$FIX/store" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case port-not-a-number   "is not a number"                  no \
  WOA23_PORT="eighty-fifty" WOA23_ZARR_STORE="$FIX/store" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case port-out-of-range   "out of range"                     no \
  WOA23_PORT="70000" WOA23_ZARR_STORE="$FIX/store" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case store-missing       "WOA23_ZARR_STORE is missing or empty" no \
  WOA23_PORT="$PROBE_PORT" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case store-empty         "WOA23_ZARR_STORE is missing or empty" no \
  WOA23_PORT="$PROBE_PORT" WOA23_ZARR_STORE="" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case store-not-a-dir     "is not a directory"               no \
  WOA23_PORT="$PROBE_PORT" WOA23_ZARR_STORE="$FIX/notadir" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case store-absent        "is not a directory"               no \
  WOA23_PORT="$PROBE_PORT" WOA23_ZARR_STORE="$FIX/no-such-store" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case store-no-anchor     "no readable anchor group metadata" no \
  WOA23_PORT="$PROBE_PORT" WOA23_ZARR_STORE="$FIX/anchorless" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case tls-key-unreadable  "TLS key not readable"             no \
  WOA23_PORT="$PROBE_PORT" WOA23_ZARR_STORE="$FIX/store" WOA23_PYTHON="$STUB" \
  WOA23_TLS_KEYFILE="$FIX/no-such-key.pem" WOA23_TLS_CERTFILE="$FIX/fullchain.pem"
run_case tls-cert-unreadable "TLS certificate not readable"     no \
  WOA23_PORT="$PROBE_PORT" WOA23_ZARR_STORE="$FIX/store" WOA23_PYTHON="$STUB" \
  WOA23_TLS_KEYFILE="$FIX/privkey.pem" WOA23_TLS_CERTFILE="$FIX/no-such-cert.pem"
run_case interpreter-absent  "no interpreter at"                no \
  WOA23_PORT="$PROBE_PORT" WOA23_ZARR_STORE="$FIX/store" WOA23_PYTHON="$WORK/not-here" WOA23_TLS=off

echo
echo "2. argv verification — a STUB interpreter, a SYNTHETIC store, no gunicorn, no bind"
# The expected text is the LAUNCHER's own banner, not the stub's marker line: the stub
# appends INVOKED to a shared marker file, never to the case's stdout. Expecting
# INVOKED here failed both cases while the argv underneath was perfectly correct.
#
# This is the only case that reaches `exec`, and it is the reason the exercise exists:
# `${TLS_ARGS[@]+"${TLS_ARGS[@]}"}` on an EMPTY array is the construct that differs
# between bash 3.2 and 5.x. It cannot be observed from a refusal.
run_case argv-tls-off        "woa23 production launcher"        yes \
  WOA23_PORT="$PROBE_PORT" WOA23_ZARR_STORE="$FIX/store" WOA23_PYTHON="$STUB" WOA23_TLS=off
run_case argv-tls-on         "woa23 production launcher"        yes \
  WOA23_PORT="$PROBE_PORT" WOA23_ZARR_STORE="$FIX/store" WOA23_PYTHON="$STUB" \
  WOA23_TLS_KEYFILE="$FIX/privkey.pem" WOA23_TLS_CERTFILE="$FIX/fullchain.pem"

echo
echo "3. what the stub was actually asked to run"
sed 's/^/  /' "$MARKER"
echo
gun=0; rel=0
grep -q 'INVOKED.*-m gunicorn api.app:app' "$MARKER" && gun=1
grep -q 'INVOKED.*--reload' "$MARKER" && rel=1
if [ "$gun" = 1 ]; then PASS=$((PASS+1)); echo "  ok   the argv names 'python -m gunicorn api.app:app'"
else FAIL=$((FAIL+1)); echo "  FAIL the argv does not name gunicorn api.app:app"; fi
if [ "$rel" = 0 ]; then PASS=$((PASS+1)); echo "  ok   no --reload in any argv"
else FAIL=$((FAIL+1)); echo "  FAIL --reload appeared in an argv"; fi
if grep -q 'INVOKED.*woa23_app' "$MARKER"; then FAIL=$((FAIL+1)); echo "  FAIL woa23_app appeared in an argv"
else PASS=$((PASS+1)); echo "  ok   woa23_app appears in no argv"; fi
# The empty-array case must carry NO --keyfile, and the TLS case must carry one. Both
# from the same file, so the guarded expansion is proven to work in both directions.
if grep 'INVOKED' "$MARKER" | grep -q -- '--keyfile'; then PASS=$((PASS+1)); echo "  ok   the TLS argv carries --keyfile"
else FAIL=$((FAIL+1)); echo "  FAIL no --keyfile in any argv"; fi
n_nokey="$(grep -c 'INVOKED' "$MARKER")"
n_key="$(grep 'INVOKED' "$MARKER" | grep -c -- '--keyfile')"
if [ "$n_key" -lt "$n_nokey" ]; then PASS=$((PASS+1))
  echo "  ok   and the TLS-off argv carries none — the empty array expanded to nothing"
else FAIL=$((FAIL+1)); echo "  FAIL every argv carried --keyfile; the TLS-off path did not run"; fi

echo
echo "4. nothing was started and nothing bound"
strays="$(ps -eo args= | grep -cE 'gunicorn|pm2|dask-scheduler|dask-worker' || true)"
echo "  gunicorn/pm2/dask processes matching now : $strays  (informational; see the report)"
echo "  listeners on $PROBE_PORT                     : $(listeners_on "$PROBE_PORT")  (baseline $BASELINE_LISTENERS)"
if [ "$(listeners_on "$PROBE_PORT")" = "$BASELINE_LISTENERS" ]; then
  PASS=$((PASS+1)); echo "  ok   the probe port was never bound"
else FAIL=$((FAIL+1)); echo "  FAIL the probe port changed state"; fi

chmod -R u+w "$FIX/unreadable" 2>/dev/null
echo
echo "evidence: $WORK"
if [ "$FAIL" -eq 0 ]; then echo "all passed ($PASS checks)"; exit 0
else echo "$FAIL FAILED, $PASS passed"; exit 1; fi
