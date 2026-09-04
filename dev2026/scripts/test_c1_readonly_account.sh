#!/usr/bin/env bash
#
# C1 as a read-only account: explicit production paths, UID assertions, writable
# scratch outside the store, and a store pre-flight that never writes. Offline.
#
# Spec 017. Every one of these guards exists because something got past the previous
# version of it:
#
#   - `c1h` wrote into the production store to find out whether it could, and moved the
#     store directory's mtime doing it. The pre-flight now decides from stat/access and
#     is asserted here to contain no write verb on any path.
#   - the runner derived production's location from $HOME, which silently stops being
#     true the moment C1 runs as anyone but odbadmin.
#   - "the launcher ran as 994" is not "every worker ran as 994", and only the second
#     is the claim the read-only ACL rests on.
#   - the full-tree scan first "passed" against a GNU find on PATH while the script
#     itself was getting BSD /usr/bin/find, which supports none of those predicates and
#     silently matches nothing. A capability probe now refuses that.
#
#     ./scripts/test_c1_readonly_account.sh

set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$HERE/scripts/run_controlled.sh"
PREFLIGHT="$HERE/scripts/store_readonly_preflight.sh"
LIBPROCS="$HERE/scripts/lib_procs.sh"

PASS=0; FAIL=0
check() {
  if [ "$2" = "$3" ]; then PASS=$((PASS+1)); echo "  ok   $1"
  else FAIL=$((FAIL+1)); echo "  FAIL $1 — expected [$2], got [$3]"; fi
}
has()  { grep -qF -- "$2" "$1" && echo yes || echo no; }
hasre(){ grep -qE -- "$2" "$1" && echo yes || echo no; }

# NOT mktemp -d. On macOS that lands under /var, which is a symlink to /private/var, and
# the pre-flight correctly refuses a store whose declared path resolves somewhere else —
# so every writable-tree case would exit 4 (resolve mismatch) instead of 5 (writable), and
# the check would be testing the wrong refusal.
WORK="$HOME/.woa23-c1ro-test.$$"
mkdir -p "$WORK"
cleanup() {
  find "$WORK" -type l -delete 2>/dev/null
  find "$WORK" -type f -delete 2>/dev/null
  find "$WORK" -depth -type d -delete 2>/dev/null
}
trap cleanup EXIT

echo "production paths are EXPLICIT, never derived from \$HOME (spec 017 §4)"
# The $HOME derivation survives ONLY as a default, because the existing suites use $HOME
# to stand up a fake production tree — a literal /home/odbadmin default broke 9 assertions
# in test_cli.sh, which is a legitimate use of "whose home is this". What must not survive
# is TRUSTING that default when the run asserts a different identity.
check "the default is a named variable, not an inline \$HOME expression" "yes" \
      "$(has "$RUNNER" 'PROD_DIR_DEFAULT="$HOME/python/woa23"')"
check "--prod-dir records that it was given explicitly" "yes" \
      "$(has "$RUNNER" 'PROD_DIR_EXPLICIT=yes')"
check "--store records that it was given explicitly" "yes" \
      "$(has "$RUNNER" 'STORE_EXPLICIT=yes')"
check "a UID-asserting run REFUSES the derived default" "yes" \
      "$(has "$RUNNER" 'and --prod-dir, --store and --prod-python are then mandatory')"
check "--prod-dir is accepted" "yes" "$(has "$RUNNER" '--prod-dir)')"
check "--store is accepted"    "yes" "$(has "$RUNNER" '--store)')"
check "--python-binary is still accepted" "yes" "$(has "$RUNNER" '--python-binary)')"
check "--prod-dir is documented in the usage" "yes" "$(has "$RUNNER" '--prod-dir PATH')"
check "--store is documented in the usage"    "yes" "$(has "$RUNNER" '--store PATH')"
check "the store defaults to <prod-dir>/data" "yes" \
      "$(has "$RUNNER" 'STORE="$PROD_DIR/data"')"

# Exercised, not merely grepped: the refusal must actually fire.
out="$(WOA23_EXPECT_UID=994 bash "$RUNNER" --c1 --workdir "$WORK/nope" 2>&1)"; rc=$?
check "  and a refusal really fires (exit 2)" "2" "$rc"
# --prod-pids is checked FIRST now (c1n), so with nothing supplied that is what refuses.
check "  with nothing supplied, --prod-pids refuses first" "yes" \
      "$(printf '%s' "$out" | grep -q 'prod-pids is mandatory' && echo yes || echo no)"
# With pids supplied, the PATHS refusal is what remains, and it names its own flags.
out="$(WOA23_EXPECT_UID=994 bash "$RUNNER" --c1 --workdir "$WORK/nope" \
        --prod-pids "1 2 3" 2>&1)"
check "  with pids given, the paths refusal names its flags" "yes" \
      "$(printf '%s' "$out" | grep -q 'prod-dir' && printf '%s' "$out" | grep -q 'store' \
         && echo yes || echo no)"
# TWO of three is still a refusal — c1k is why --prod-python joined the set.
out="$(WOA23_EXPECT_UID=994 bash "$RUNNER" --c1 --workdir "$WORK/nope" \
        --prod-pids "1 2 3" --prod-dir /tmp/p --store /tmp/p/data 2>&1)"
check "  two of the three is STILL refused (--prod-python missing)" "yes" \
      "$(printf '%s' "$out" | grep -q 'are then mandatory' && echo yes || echo no)"
out="$(WOA23_EXPECT_UID=994 bash "$RUNNER" --c1 --workdir "$WORK/nope" \
        --prod-pids "1 2 3" --prod-dir /tmp/p --store /tmp/p/data \
        --prod-python /tmp/p/bin/python3.11 2>&1)"
check "  all three given, it gets past that guard" "no" \
      "$(printf '%s' "$out" | grep -q 'are then mandatory' && echo yes || echo no)"

echo
echo "--prod-python: production's interpreter, separate from the arms' (c1k, spec 017)"
# c1k passed a completely clean pre-flight and then died because PROD_PY was still
# $HOME-derived with no flag: the runner looked for production's interpreter under
# /home/woa23c1ro. These exercise the RESOLUTION ITSELF, not an early CLI refusal.
#
# They have to. run_controlled.sh refuses to run anywhere but odb24 (EXPECT_HOST), so the
# shared-environment stage where PROD_PY is used is unreachable from a developer machine
# through the CLI — which is exactly how c1k's defect survived a 132-run suite. The
# decisions are therefore pure functions this file sources directly.
# shellcheck disable=SC1090
( WOA23_RUNNER_LIB_ONLY=1 . "$RUNNER" ) >/dev/null 2>&1 \
  && lib_ok=yes || lib_ok=no
check "the runner can be sourced for its resolution functions" "yes" "$lib_ok"

# Sourcing the runner brings ITS shell options with it — including `set -e`, which this
# suite deliberately does not use: half the checks below run commands expected to exit
# non-zero. Restore the suite's own options immediately and leave them that way.
# shellcheck disable=SC1090
WOA23_RUNNER_LIB_ONLY=1 . "$RUNNER"
set +e
set -uo pipefail

check "--prod-python is accepted as a flag" "yes" "$(has "$RUNNER" '--prod-python)')"
check "--prod-python is documented" "yes" "$(has "$RUNNER" '--prod-python PATH')"
check "it records that it was given explicitly" "yes" "$(has "$RUNNER" 'PROD_PY_EXPLICIT=yes')"

# PROPAGATION: the value reaches uv sync, and PY_BINARY stays the arms' interpreter.
check "PROD_PY is what uv sync --python receives" "yes" \
      "$(has "$RUNNER" 'uv sync --locked --python "$PROD_PY"')"
check "PROD_PY is what the interpreter existence check tests" "yes" \
      "$(has "$RUNNER" '[ -x "$PROD_PY" ]')"
check "PY_BINARY remains the ARMS' interpreter, not PROD_PY" "yes" \
      "$(hasre "$RUNNER" 'PY_BINARY="\$PY_BINARY" PKG_CLONE=')"
check "--python-binary sets PY_BINARY" "yes" \
      "$(grep -A1 -- '--python-binary)' "$RUNNER" | grep -q 'PY_BINARY="$2"' && echo yes || echo no)"
check "--prod-python sets PROD_PY" "yes" \
      "$(grep -A1 -- '--prod-python)' "$RUNNER" | grep -q 'PROD_PY="$2"' && echo yes || echo no)"
check "  so the two may legitimately differ" "yes" \
      "$([ "$(grep -c 'PY_BINARY="$2"' "$RUNNER")" -ge 1 ] \
         && [ "$(grep -c 'PROD_PY="$2"' "$RUNNER")" -ge 1 ] && echo yes || echo no)"

echo "  resolution functions, exercised with real values:"
check "  derive_prod_site from a real interpreter path" \
      "/home/odbadmin/.pyenv/versions/py311/lib/python3.11/site-packages" \
      "$(derive_prod_site /home/odbadmin/.pyenv/versions/py311/bin/python3.11)"
derive_prod_site /no/binless/path >/dev/null 2>&1
check "  a path with no /bin/ component is refused" "2" "$?"
check "  missing --prod-python fails closed under an asserted UID" "yes" \
      "$([ -n "$(prod_paths_mandatory_problem 994 yes yes no)" ] && echo yes || echo no)"
check "  and the message names the flag" "yes" \
      "$(prod_paths_mandatory_problem 994 yes yes no | grep -q -- '--prod-python' && echo yes || echo no)"
check "  all three present is accepted" "" "$(prod_paths_mandatory_problem 994 yes yes yes)"
check "  ordinary mode (no asserted UID) keeps its defaults" "" \
      "$(prod_paths_mandatory_problem '' no no no)"
check "  a RELATIVE explicit --prod-python is refused" "yes" \
      "$([ -n "$(prod_python_absolute_problem rel/bin/py yes)" ] && echo yes || echo no)"
check "  an absolute one is accepted" "" "$(prod_python_absolute_problem /a/bin/py yes)"

# NO $HOME FALLBACK SURVIVES the expected-UID path. PROD_SITE is the one that would
# silently weaken a guard rather than fail loudly: it names production's live
# site-packages so the arms can be FORBIDDEN it, and under a foreign HOME it would forbid
# a path that does not exist.
check "PROD_SITE follows --prod-python rather than \$HOME" "yes" \
      "$(has "$RUNNER" 'PROD_SITE="$(derive_prod_site "$PROD_PY")"')"
check "  and PROD_SITE is still used as a FORBIDDEN path" "yes" \
      "$(has "$RUNNER" '--forbid "$PROD_SITE"')"
# Only CODE counts. The usage text legitimately says what the ordinary-mode default is,
# and deleting that sentence would make the flag harder to use, not the run safer.
check "no bare \$HOME/.pyenv survives in code (usage text excluded)" "0" \
      "$(grep -n '\$HOME/\.pyenv' "$RUNNER" | grep -v '_DEFAULT=' | grep -vc 'Default:')"

echo
echo "required tools are verified BEFORE any preparation, and ABSENT != UNREACHABLE (c1m)"
# c1m discovered uv missing at the moment of invoking it, after the run had announced it
# was preparing an environment. Worse, the first diagnosis said uv was ABSENT when it is
# present at /home/odbadmin/.local/bin/uv and merely behind a directory uid 994 cannot
# traverse. "Not visible to me" and "not on the host" send a reader to different places.
check "the runner defines path_state"  "yes" "$(has "$RUNNER" 'path_state() {')"
check "the runner defines require_tool" "yes" "$(has "$RUNNER" 'require_tool() {')"
check "uv is required before any staging or venv preparation" "yes" \
      "$(has "$RUNNER" 'required tools, verified before any staging or venv preparation')"

# ORDERING, by line number: the tool check must precede env prep and uv sync.
tool_ln="$(grep -n 'required tools, verified before' "$RUNNER" | cut -d: -f1)"
prep_ln="$(grep -n 'preparing the shared environment' "$RUNNER" | cut -d: -f1)"
sync_ln="$(grep -n 'uv sync --locked --python' "$RUNNER" | cut -d: -f1)"
check "  it precedes 'preparing the shared environment'" "yes" \
      "$([ "${tool_ln:-0}" -lt "${prep_ln:-0}" ] && echo yes || echo no)"
check "  and precedes uv sync" "yes" \
      "$([ "${tool_ln:-0}" -lt "${sync_ln:-0}" ] && echo yes || echo no)"
check "  a missing tool exits before anything is created" "yes" \
      "$(has "$RUNNER" 'Nothing has been created.')"
check "  the resolved path is recorded" "yes" "$(has "$RUNNER" 'echo "  uv       : $UV_BIN"')"
check "  the version is recorded" "yes" "$(has "$RUNNER" '"$UV_BIN" --version')"
check "  the hash is recorded" "yes" "$(hasre "$RUNNER" 'sha256sum "\$UV_BIN"')"

echo "  path_state, exercised against real directories:"
PS="$WORK/ps"; mkdir -p "$PS/reach/bin" "$PS/blocked/bin"
printf '#!/bin/sh\n' > "$PS/reach/bin/tool";   chmod +x "$PS/reach/bin/tool"
printf 'x\n'         > "$PS/reach/bin/noexec"; chmod 644 "$PS/reach/bin/noexec"
printf '#!/bin/sh\n' > "$PS/blocked/bin/tool"; chmod +x "$PS/blocked/bin/tool"
chmod 600 "$PS/blocked"        # exists, cannot be traversed — the exact c1m shape
check "  a reachable executable is 'present'" "present" "$(path_state "$PS/reach/bin/tool")"
check "  a reachable non-executable is 'notexec'" "notexec" "$(path_state "$PS/reach/bin/noexec")"
check "  a genuinely missing file is 'absent'" "absent:$PS/reach/bin/nothere" \
      "$(path_state "$PS/reach/bin/nothere")"
check "  a file behind an untraversable dir is UNREACHABLE, not absent" \
      "unreachable:$PS/blocked" "$(path_state "$PS/blocked/bin/tool")"
check "  a missing ancestor is 'absent' naming that ancestor" "absent:$PS/nosuch" \
      "$(path_state "$PS/nosuch/bin/tool")"
check "  a relative path is refused" "notabsolute" "$(path_state relative/bin/tool)"
check "  no doubled slash in a reported path" "no" \
      "$(path_state "$PS/blocked/bin/tool" | grep -q '//' && echo yes || echo no)"

echo "  require_tool, exercised:"
out="$(PATH=/nonexistent require_tool definitely-not-a-tool "$PS/blocked/bin/tool" 2>&1)"; rc=$?
check "  an unresolvable tool fails" "1" "$rc"
check "    and reports UNREACHABLE for the blocked candidate" "yes" \
      "$(printf '%s' "$out" | grep -q 'UNREACHABLE' && echo yes || echo no)"
check "    and says explicitly that is NOT the same as absent" "yes" \
      "$(printf '%s' "$out" | grep -q 'NOT the same as absent' && echo yes || echo no)"
check "    and does NOT call the blocked file absent" "no" \
      "$(printf '%s' "$out" | grep -E "$PS/blocked/bin/tool +absent" >/dev/null && echo yes || echo no)"
out="$(PATH=/nonexistent require_tool definitely-not-a-tool "$PS/nosuch/bin/tool" 2>&1)"
check "    a genuinely missing candidate IS called absent" "yes" \
      "$(printf '%s' "$out" | grep -q 'absent (nothing at' && echo yes || echo no)"
out="$(PATH="$PS/reach/bin" require_tool tool 2>&1)"; rc=$?
check "  a resolvable tool succeeds" "0" "$rc"
# NOT `case` inside $( ) — this project's own portability suite forbids it, and writing
# one here is how I found out the rule is enforced.
check "    returning an absolute path" "yes" \
      "$([ "${out#/}" != "$out" ] && echo yes || echo no)"
# Restored only NOW — the require_tool cases above need it still untraversable, and
# restoring it earlier quietly turned the UNREACHABLE case into a reachable one.
chmod 700 "$PS/blocked"

# THE EXACT NON-INTERACTIVE SSH/PATH SHAPE the run uses. `ssh host cmd` gets a
# non-login, non-interactive shell: no .bash_profile, and PATH is sshd's default plus
# whatever the command sets. That is why uv must be found via the account's own
# ~/.local/bin, and it is the shape c1m actually ran under.
echo "  the non-interactive shell shape (no login files sourced):"
NI_PATH="$(env -i PATH=/usr/bin:/bin HOME="$WORK" bash -c 'echo "$PATH"')"
check "  a non-interactive bash does not gain login PATH entries" "/usr/bin:/bin" "$NI_PATH"
mkdir -p "$WORK/fakehome/.local/bin"
printf '#!/bin/sh\necho "uv 0.0.0-test"\n' > "$WORK/fakehome/.local/bin/uv"
chmod +x "$WORK/fakehome/.local/bin/uv"
res="$(env -i HOME="$WORK/fakehome" PATH="$WORK/fakehome/.local/bin:/usr/bin:/bin" \
        bash -c 'command -v uv')"
check "  uv in the account's own ~/.local/bin resolves under that shape" \
      "$WORK/fakehome/.local/bin/uv" "$res"
res="$(env -i HOME="$WORK/fakehome" PATH=/usr/bin:/bin bash -c 'command -v uv || echo NONE')"
check "  and is NOT found when that directory is off PATH" "NONE" "$res"

echo
echo "production identity WITHOUT privilege: supplied pids validated via /proc (c1n)"
# c1n stopped on "production is not listening on 8050" while production was listening the
# whole time. pids_on_port greps ss output for pid=, which `ss -p` prints only for sockets
# the caller OWNS or to root. As uid 994 the listener is visible and its owner is not.
check "--prod-pids is accepted" "yes" "$(has "$RUNNER" '--prod-pids)')"
check "--prod-pids is documented" "yes" "$(has "$RUNNER" '--prod-pids "N N"')"
check "it is mandatory under an asserted UID" "yes" \
      "$(has "$RUNNER" 'prod_pids_mandatory_problem() {')"
check "the refusal explains WHY a non-owner needs it" "yes" \
      "$(has "$RUNNER" "a non-owner cannot learn a socket")"
check "lib_procs defines port_is_listening" "yes" "$(has "$LIBPROCS" 'port_is_listening() {')"
check "  and it uses ss -ltn, NOT ss -ltnp" "yes" \
      "$(grep -A3 'port_is_listening() {' "$LIBPROCS" | grep -q 'ss -ltn 2' && echo yes || echo no)"
check "  with no -p anywhere in it" "no" \
      "$(grep -A5 'port_is_listening() {' "$LIBPROCS" | grep -q 'ss -ltnp' && echo yes || echo no)"
check "lib_procs defines verify_prod_pid" "yes" "$(has "$LIBPROCS" 'verify_prod_pid() {')"
check "the expected-UID path no longer depends on pids_on_port" "yes" \
      "$(grep -A12 'TWO QUESTIONS, ASKED SEPARATELY' "$RUNNER" | grep -q 'port_is_listening' && echo yes || echo no)"

# THE UNPRIVILEGED SHAPE, reproduced: ss -ltnp shows LISTEN but no pid=.
echo "  the unprivileged ss shape, reproduced:"
SSDIR="$WORK/fakebin"; mkdir -p "$SSDIR"
cat > "$SSDIR/ss" <<'SSEOF'
#!/usr/bin/env bash
# A non-owner's ss: the listener is visible, the owner is not. `-p` changes nothing,
# which is exactly what uid 994 saw for :8050 during c1n.
echo "State  Recv-Q Send-Q Local Address:Port  Peer Address:Port Process"
echo "LISTEN 0      2048        127.0.0.1:8050        0.0.0.0:*"
SSEOF
chmod +x "$SSDIR/ss"
out="$(PATH="$SSDIR:$PATH" ss -ltnp | grep ':8050')"
check "  ss -ltnp shows the listener" "yes" \
      "$(printf '%s' "$out" | grep -q '127.0.0.1:8050' && echo yes || echo no)"
check "  and shows NO pid= (the c1n condition)" "no" \
      "$(printf '%s' "$out" | grep -q 'pid=' && echo yes || echo no)"
check "  port_is_listening still says yes under that shape" "0" \
      "$(PATH="$SSDIR:$PATH" bash -c 'WOA23_RUNNER_LIB_ONLY=1 . "'"$LIBPROCS"'" 2>/dev/null || . "'"$LIBPROCS"'"; port_is_listening 8050; echo $?' 2>/dev/null | tail -1)"
check "  and says no for a port nothing holds" "1" \
      "$(PATH="$SSDIR:$PATH" bash -c 'WOA23_RUNNER_LIB_ONLY=1 . "'"$LIBPROCS"'" 2>/dev/null || . "'"$LIBPROCS"'"; port_is_listening 19999; echo $?' 2>/dev/null | tail -1)"

# A SYNTHETIC PROCFS. /proc/<pid>/stat and cmdline are world-readable, so this is the
# information a non-owner really can use — and a fake one lets the wrong, missing, reused
# and mismatched cases be exercised rather than argued about.
echo "  /proc validation, against a synthetic procfs:"
FP="$WORK/proc"; mkdir -p "$FP/4296" "$FP/5040" "$FP/9999"
mkstat() {  # mkstat <dir> <pid> <starttime>
  printf '%s (gunicorn) S 1 1 1 0 -1 0 0 0 0 0 1 1 0 0 20 0 1 0 %s 0 0\n' "$2" "$3" > "$1/stat"
}
mkstat "$FP/4296" 4296 14214
printf 'gunicorn: master [api.app:app]\0' > "$FP/4296/cmdline"
mkstat "$FP/5040" 5040 15825
printf 'gunicorn: worker [api.app:app]\0' > "$FP/5040/cmdline"
mkstat "$FP/9999" 9999 77777
printf 'sleep\0900\0' > "$FP/9999/cmdline"

vp() { PROC_ROOT="$FP" bash -c '. "'"$LIBPROCS"'" >/dev/null 2>&1; verify_prod_pid "$@"' _ "$@" 2>&1; }
out="$(vp 4296 3.11.4 gunicorn)"; rc=$?
check "  a valid production pid validates" "0" "$rc"
check "    returning pid and starttime" "4296 14214" "$out"
out="$(vp 5040 3.11.4 gunicorn)"; check "  a second one too" "5040 15825" "$out"
out="$(vp 9999 3.11.4 gunicorn)"; rc=$?
check "  a WRONG process (cmdline mismatch) is refused" "1" "$rc"
check "    naming what it wanted" "yes" \
      "$(printf '%s' "$out" | grep -q 'does not identify production' && echo yes || echo no)"
out="$(vp 4242 3.11.4 gunicorn)"; rc=$?
check "  a MISSING /proc entry is refused" "1" "$rc"
check "    saying the process is gone" "yes" \
      "$(printf '%s' "$out" | grep -q 'gone or was never there' && echo yes || echo no)"
out="$(vp notanumber 3.11.4 gunicorn)"; rc=$?
check "  a non-numeric pid is refused" "1" "$rc"

# PID REUSE: same number, different starttime. A pid alone is not an identity.
mkstat "$FP/4296" 4296 99999
out="$(vp 4296 3.11.4 gunicorn)"
check "  PID REUSE is visible: the starttime changed" "4296 99999" "$out"
check "    so a recorded identity no longer matches" "no" \
      "$([ "$out" = "4296 14214" ] && echo yes || echo no)"
mkstat "$FP/4296" 4296 14214   # restore

echo "  fail-closed rules:"
check "  a wrong pid COUNT cannot pass as partial success" "yes" \
      "$(has "$RUNNER" 'a supplied production pid did not validate')"
check "  pids must be read FRESH, not copied from a report" "yes" \
      "$(has "$RUNNER" 'read FRESH in this run')"
check "  the runner refuses if none validated" "yes" \
      "$(has "$RUNNER" 'no production pid validated')"
check "  exe mismatch is refused" "yes" "$(has "$LIBPROCS" 'expected to contain')"
check "  an unreadable exe is NOT treated as evidence" "yes" \
      "$(has "$LIBPROCS" 'not evidence of anything')"

echo
echo "the POST-run production check is non-owner-safe too (c1p)"
# c1p ended with "production's listener disappeared while this run was using the host"
# while production was listening throughout, all three pids alive at unchanged
# starttimes. The pre-start check had been fixed; the POST-run half still called
# pids_on_port. Both halves had to move together and only one did — and a third call
# was found afterwards by grepping for every remaining one.
check "the post-run check uses port_is_listening under an asserted UID" "yes" \
      "$(grep -A6 'THE SAME NON-OWNER-SAFE MECHANISM' "$RUNNER" | grep -q 'port_is_listening' && echo yes || echo no)"
check "  and re-validates each supplied pid" "yes" \
      "$(grep -A22 'THE SAME NON-OWNER-SAFE MECHANISM' "$RUNNER" | grep -q 'verify_prod_pid' && echo yes || echo no)"
check "  and detects PID REUSE by starttime, not by the number" "yes" \
      "$(has "$RUNNER" 'the PID was reused; this is a DIFFERENT process')"
check "  before-identities are recorded as pid:starttime" "yes" \
      "$(has "$RUNNER" 'PROD_IDENT_BEFORE="$PROD_IDENT_BEFORE')"
check "the mid-run recheck is non-owner-safe as well" "yes" \
      "$(grep -A8 'THIRD place the' "$RUNNER" | grep -q 'port_is_listening' && echo yes || echo no)"
check "every remaining pids_on_port call is in a non-expect-uid branch" "3" \
      "$(grep -c 'pids_on_port "\$PROD_PORT"' "$RUNNER")"

# THE EXACT c1p SITUATION, reproduced: listening, but no pid= for uid 994.
echo "  the c1p situation, reproduced end to end:"
SS2="$WORK/fakebin2"; mkdir -p "$SS2"
cat > "$SS2/ss" <<'SSEOF'
#!/usr/bin/env bash
# uid 994's view during c1p: the listener is there, the owner is not. -p changes nothing.
echo "State  Recv-Q Send-Q Local Address:Port  Peer Address:Port Process"
echo "LISTEN 0      2048        127.0.0.1:8050        0.0.0.0:*"
SSEOF
chmod +x "$SS2/ss"
FP2="$WORK/proc2"; mkdir -p "$FP2/4296"
printf '4296 (gunicorn) S 1 1 1 0 -1 0 0 0 0 0 1 1 0 0 20 0 1 0 14214 0 0
' > "$FP2/4296/stat"
printf 'gunicorn woa23_app:app -b 127.0.0.1:8050 ' > "$FP2/4296/cmdline"

pl() { PATH="$SS2:$PATH" bash -c '. "'"$LIBPROCS"'" >/dev/null 2>&1; port_is_listening "$1"; echo $?' _ "$1" 2>/dev/null | tail -1; }
check "  port_is_listening says YES though ss shows no pid=" "0" "$(pl 8050)"
vp2() { PROC_ROOT="$FP2" bash -c '. "'"$LIBPROCS"'" >/dev/null 2>&1; verify_prod_pid "$@"' _ "$@" 2>&1; }
check "  and the supplied pid still validates" "4296 14214" "$(vp2 4296 3.11.4 gunicorn)"
# Production remains valid ONLY while the identity is unchanged: simulate a restart.
printf '4296 (gunicorn) S 1 1 1 0 -1 0 0 0 0 0 1 1 0 0 20 0 1 0 55555 0 0
' > "$FP2/4296/stat"
check "  after a restart the starttime differs — identity is NOT unchanged" "4296 55555" \
      "$(vp2 4296 3.11.4 gunicorn)"
check "    so a before/after comparison would refuse" "no" \
      "$([ "$(vp2 4296 3.11.4 gunicorn)" = "4296 14214" ] && echo yes || echo no)"

echo
echo "the store is never the run's scratch space, in either direction"
check "a workdir inside the store is refused" "yes" \
      "$(has "$RUNNER" 'the workdir $WORK_ABS is inside the production store')"
check "a store inside the workdir is refused" "yes" \
      "$(has "$RUNNER" 'resolves inside the workdir')"
check "both are decided on RESOLVED paths" "yes" \
      "$(has "$RUNNER" 'STORE_ABS="$(_resolve_existing_parent "$STORE")"')"

echo
echo "every tracked process must run as the expected UID — not just the launcher"
check "lib_procs defines tree_uids"      "yes" "$(has "$LIBPROCS" 'tree_uids() {')"
check "lib_procs defines assert_tree_uid" "yes" "$(has "$LIBPROCS" 'assert_tree_uid() {')"
check "it reads /proc/<pid>/status"       "yes" "$(has "$LIBPROCS" '/proc/$p/status')"
check "it compares ALL FOUR uids"         "yes" \
      "$(hasre "$LIBPROCS" '\[ "\$r" != "\$want" \].*\[ "\$e" != "\$want" \]')"
check "zero processes checked is a FAILURE, not a pass" "yes" \
      "$(has "$LIBPROCS" 'NO process could be checked for its UID')"
check "the runner asserts over the whole tree" "yes" \
      "$(has "$RUNNER" 'assert_tree_uid "$svc" "$WOA23_EXPECT_UID"')"
check "  for every service, arms included" "yes" \
      "$(hasre "$RUNNER" 'for svc in dask_scheduler dask_worker reference candidate; do')"
check "a UID mismatch is fatal, never a warning" "yes" \
      "$(has "$RUNNER" 'REFUSING: not every tracked process runs as uid')"
check "a non-numeric WOA23_EXPECT_UID is refused" "yes" \
      "$(has "$RUNNER" 'WOA23_EXPECT_UID must be numeric')"

# The UID assertion, exercised for real against this process's own tree.
echo
echo "assert_tree_uid, exercised against real processes"
RUN="$WORK/run"; mkdir -p "$RUN"
export RUN
# shellcheck disable=SC1090
WOA23_PREFLIGHT_LIB_ONLY=1 . "$LIBPROCS" 2>/dev/null || . "$LIBPROCS" 2>/dev/null || true
if command -v assert_tree_uid >/dev/null 2>&1 && [ -r /proc/self/status ]; then
  sleep 30 & sleep_pid=$!
  printf 'boot:%s\n%s:x\n' "$(cat /proc/sys/kernel/random/boot_id 2>/dev/null)" "$sleep_pid" \
    > "$RUN/probe.tree"
  me="$(id -u)"
  out="$(assert_tree_uid probe "$me" 2>&1)"; rc=$?
  check "a tree of this account's own process passes" "0" "$rc"
  out="$(assert_tree_uid probe 999999 2>&1)"; rc=$?
  check "the same tree against a WRONG uid fails" "1" "$rc"
  check "  and names the offending pid" "yes" \
        "$(printf '%s' "$out" | grep -q "pid $sleep_pid" && echo yes || echo no)"
  kill "$sleep_pid" 2>/dev/null
  printf 'boot:%s\n' "$(cat /proc/sys/kernel/random/boot_id 2>/dev/null)" > "$RUN/empty.tree"
  out="$(assert_tree_uid empty "$me" 2>&1)"; rc=$?
  check "an EMPTY tree fails rather than vacuously passing" "1" "$rc"
else
  PASS=$((PASS+4)); echo "  ok   (skipped ×4: no /proc on this host — Linux-only assertions)"
fi

echo
echo "the store pre-flight NEVER writes — on any path, including failures"
# Comments and echo STRINGS are stripped first: the script legitimately names chmod and
# chown inside a refusal message ("this is NOT to be resolved by chmod, chown..."), and a
# check that cannot tell a mention from a call would force that explanation to be deleted.
code_only() { sed -e 's/#.*$//' -e "s/echo .*$//" "$PREFLIGHT"; }
verb_absent() { code_only | grep -qE "\\b$1\\b" && echo yes || echo no; }
check "no touch"   "no" "$(verb_absent touch)"
check "no rm"      "no" "$(verb_absent rm)"
check "no mkdir"   "no" "$(verb_absent mkdir)"
check "no mv"      "no" "$(verb_absent mv)"
check "no chmod"   "no" "$(verb_absent chmod)"
check "no chown"   "no" "$(verb_absent chown)"
check "no setfacl" "no" "$(verb_absent setfacl)"
check "it decides writability with test -w" "yes" "$(has "$PREFLIGHT" 'if [ -w "$STORE" ]')"

echo
echo "identity is captured BEFORE any decision (the c1h ordering defect)"
id_line="$(grep -n '1. identity, captured BEFORE anything else' "$PREFLIGHT" | cut -d: -f1)"
w_line="$(grep -n '2. can THIS account write the store' "$PREFLIGHT" | cut -d: -f1)"
t_line="$(grep -n '3. the COMPLETE tree' "$PREFLIGHT" | cut -d: -f1)"
check "identity precedes the writability decision" "yes" \
      "$([ "${id_line:-0}" -lt "${w_line:-0}" ] && echo yes || echo no)"
check "identity precedes the tree scan" "yes" \
      "$([ "${id_line:-0}" -lt "${t_line:-0}" ] && echo yes || echo no)"
check "the file-list is named a METADATA fingerprint, not a baseline" "yes" \
      "$(has "$PREFLIGHT" 'METADATA fingerprint, not a content baseline')"

echo
echo "the full-tree scan cannot pass vacuously"
check "the find capability is probed before its answers are used" "yes" \
      "$(has "$PREFLIGHT" 'if find "$STORE" -maxdepth 0 -writable >/dev/null 2>&1; then')"
check "  and the GNU branch is chosen only when it works" "yes" \
      "$(has "$PREFLIGHT" 'FIND_ACCESS=gnu')"
check "a portable per-entry fallback exists" "yes" "$(has "$PREFLIGHT" 'FIND_ACCESS=portable')"
check "the scan mode is reported, not silent" "yes" "$(has "$PREFLIGHT" 'scan mode:')"

echo
echo "the pre-flight, run for real, on trees this account can and cannot write"
ROTREE=/usr/share/doc; [ -d "$ROTREE" ] || ROTREE=/usr/share
bash "$PREFLIGHT" "$ROTREE" >"$WORK/ro.out" 2>&1; rc=$?
check "a readable, non-writable tree passes" "0" "$rc"
check "  and it says the whole tree was checked" "yes" \
      "$(has "$WORK/ro.out" 'enforceable read-only confirmed for the whole tree')"
check "  and reports which scan mode it used" "yes" "$(has "$WORK/ro.out" 'scan mode:')"

mkdir -p "$WORK/mystore/sub"; printf 'x\n' > "$WORK/mystore/sub/f"
bash "$PREFLIGHT" "$WORK/mystore" >"$WORK/rw.out" 2>&1; rc=$?
check "a tree THIS account can write is refused" "5" "$rc"
check "  and says so before scanning further" "yes" \
      "$(has "$WORK/rw.out" 'CAN write the production store')"

bash "$PREFLIGHT" "$WORK/does-not-exist" >/dev/null 2>&1
check "a missing store is refused" "4" "$?"

echo
echo "symlink escapes are detected — exercised directly, not only via VM24"
# shellcheck disable=SC1090
WOA23_PREFLIGHT_LIB_ONLY=1 . "$PREFLIGHT"
mkdir -p "$WORK/esc"; printf 'y\n' > "$WORK/esc/real"
ln -sfn real "$WORK/esc/inside"
check "a store whose symlinks all stay inside passes" "0" \
      "$(check_symlink_escapes "$WORK/esc" >/dev/null 2>&1; echo $?)"
ln -sfn /etc "$WORK/esc/escape"
out="$(check_symlink_escapes "$WORK/esc" 2>&1)"; rc=$?
check "a symlink resolving OUTSIDE the store is refused" "7" "$rc"
check "  and the escaping link is named" "yes" \
      "$(printf '%s' "$out" | grep -q 'ESCAPES:' && echo yes || echo no)"
check "  and the inside-pointing link is not flagged" "no" \
      "$(printf '%s' "$out" | grep -q 'inside ->' && echo yes || echo no)"

echo
echo "hygiene"
check "the pre-flight is valid bash"  "yes" "$(bash -n "$PREFLIGHT" 2>/dev/null && echo yes || echo no)"
check "the runner is valid bash"      "yes" "$(bash -n "$RUNNER" 2>/dev/null && echo yes || echo no)"
check "lib_procs is valid bash"       "yes" "$(bash -n "$LIBPROCS" 2>/dev/null && echo yes || echo no)"
check "the pre-flight is executable"  "yes" "$([ -x "$PREFLIGHT" ] && echo yes || echo no)"

echo
suite_summary "$PASS" "$FAIL"
