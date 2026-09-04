#!/usr/bin/env bash
#
# Offline tests for run_controlled.sh's argument handling and mode selection.
#
# Every case here stops at argument validation or at the authorisation gate, so
# nothing is started, no port is bound and no request is sent. That is the point:
# a configuration mistake must be refused before the script looks at the host, and
# the refusal must say which value was wrong.
#
# The exit codes are part of the contract:
#   2  configuration error (bad argument, bad port, bad workdir, conflicting modes)
#   3  authorisation not stated
#   4  wrong host, or a missing host prerequisite
#
#     ./scripts/test_cli.sh

set -euo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="$HERE/run_controlled.sh"

pass=0; fail=0
check() {                   # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}
has_text() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

# Runs the script with the grant set, so anything that still exits 2 was refused by
# argument validation rather than by the authorisation gate. Wrong-host (4) is the
# furthest a valid configuration can get on a development machine.
# Every invocation in this file exits non-zero by design: 2 for a refused
# configuration, 3 without the grant, 4 for the wrong host — which is as far as a
# valid configuration gets on a development machine. So `run` never propagates a
# failure, or `set -e` would abort the suite on the first well-formed invocation.
run() { WOA23_D2B_GRANTED=yes "$RUNNER" "$@" 2>&1 || true; }
code() { WOA23_D2B_GRANTED=yes "$RUNNER" "$@" >/dev/null 2>&1; echo $?; }

echo "argument validation happens before anything touches the host"
check "an unknown argument is refused" "2" "$(code --bogus)"
check "and names itself" "yes" "$(has_text "$(run --bogus)" "unknown argument: --bogus")"
check "usage is printed on an unknown argument" "yes" \
      "$(has_text "$(run --bogus)" "usage: run_controlled.sh")"
check "--help exits 0" "0" "$(code --help)"
check "a flag missing its value is refused" "2" "$(code --workdir)"
check "and says which flag" "yes" "$(has_text "$(run --workdir)" "--workdir needs a value")"
for f in --candidate-port --reference-port --scheduler-port; do
  check "$f without a value is refused" "2" "$(code "$f")"
done

echo
echo "the modes are exclusive"
check "--contract-only alone is accepted past validation" "4" \
      "$(code --contract-only)"
check "--cleanup-only alone is accepted past validation" "4" \
      "$(code --cleanup-only)"
check "both together are refused" "2" "$(code --contract-only --cleanup-only)"
check "and the refusal explains why" "yes" \
      "$(has_text "$(run --contract-only --cleanup-only)" "mutually exclusive")"

echo
echo "ports are validated, and production's are never bindable"
check "a non-numeric port is refused" "2" "$(code --candidate-port abc)"
check "and names the value" "yes" \
      "$(has_text "$(run --candidate-port abc)" "candidate port 'abc' is not a number")"
check "an empty port is refused" "2" "$(code --candidate-port '')"
check "a privileged port is refused" "2" "$(code --candidate-port 80)"
check "and says the range" "yes" \
      "$(has_text "$(run --candidate-port 80)" "outside 1024-65535")"
check "a port above 65535 is refused" "2" "$(code --candidate-port 70000)"
# The three that matter. Preflight would refuse a held port anyway, but a typo
# aimed at production deserves a refusal that names the reason.
for p in 8050 8786 8787; do
  check "port $p is refused outright" "2" "$(code --candidate-port $p)"
  check "and is identified as production's" "yes" \
        "$(has_text "$(run --candidate-port $p)" "belongs to production")"
done
check "production's port is refused on the reference too" "2" "$(code --reference-port 8050)"
check "and on the scheduler" "2" "$(code --scheduler-port 8786)"
check "duplicate candidate/reference ports are refused" "2" \
      "$(code --candidate-port 19001 --reference-port 19001)"
check "duplicate candidate/scheduler ports are refused" "2" \
      "$(code --candidate-port 19001 --scheduler-port 19001)"
check "duplicate reference/scheduler ports are refused" "2" \
      "$(code --reference-port 19001 --scheduler-port 19001)"
check "and the refusal lists all three" "yes" \
      "$(has_text "$(run --candidate-port 19001 --reference-port 19001)" "the three ports must differ")"
check "three distinct non-production ports get past validation" "4" \
      "$(code --candidate-port 19001 --reference-port 19002 --scheduler-port 19003)"

echo
echo "the workdir is never production"
check "production itself is refused" "2" "$(code --workdir "$HOME/python/woa23")"
check "and says why" "yes" \
      "$(has_text "$(run --workdir "$HOME/python/woa23")" "is inside production")"
check "a path inside production is refused" "2" \
      "$(code --workdir "$HOME/python/woa23/staging")"
check "a relative path resolving into production is refused" "2" \
      "$(code --workdir "$HOME/python/woa23/../woa23/x")"
check "a path merely sharing a prefix is NOT refused" "4" \
      "$(code --workdir "$HOME/python/woa23-staging")"
check "an ordinary staging path gets past validation" "4" \
      "$(code --workdir "$HOME/woa23-s1-staging-merged")"

echo
echo "the workdir boundary survives a symlinked parent"
# Lexical normalisation cannot see a symlink: --workdir /tmp/link/new-run, where
# /tmp/link points at ~/python/woa23, is lexically nowhere near production and
# physically inside it. The check resolves the deepest existing ancestor
# physically, so the symlink is followed before the comparison.
#
# $HOME is the symlink target because it exists on every machine, so the real
# end-to-end refusal runs here as well as on VM24 — the production directory
# itself need not exist for the boundary to be tested.
TMPD="$(mktemp -d)"
ln -s "$HOME" "$TMPD/homelink"
ln -s "$TMPD/nowhere" "$TMPD/dangling"
: > "$TMPD/afile"; ln -s "$TMPD/afile" "$TMPD/tofile"
ln -s "$TMPD" "$TMPD/selfish"

check "a symlinked parent into production is refused" "2" \
      "$(code --workdir "$TMPD/homelink/python/woa23/new-run")"
check "and says it is inside production" "yes" \
      "$(has_text "$(run --workdir "$TMPD/homelink/python/woa23/new-run")" "is inside production")"
check "a deeper non-existent tail is still caught" "2" \
      "$(code --workdir "$TMPD/homelink/python/woa23/a/b/c")"
check "production itself through the symlink is refused" "2" \
      "$(code --workdir "$TMPD/homelink/python/woa23")"

# The near-miss that must NOT be refused: same symlink, a sibling whose name merely
# shares the prefix.
check "a sibling sharing the prefix is allowed through the same symlink" "4" \
      "$(code --workdir "$TMPD/homelink/python/woa23-staging/new-run")"
check "an unrelated path through the symlink is allowed" "4" \
      "$(code --workdir "$TMPD/homelink/woa23-s1-staging-merged")"
check "a symlink to somewhere unrelated is allowed" "4" \
      "$(code --workdir "$TMPD/selfish/staging")"

# Fail closed: an ancestor that cannot be followed to a directory cannot be shown to
# be outside production.
check "a dangling symlink parent is refused" "2" "$(code --workdir "$TMPD/dangling/x")"
check "and says why it could not be resolved" "yes" \
      "$(has_text "$(run --workdir "$TMPD/dangling/x")" "does not lead to a directory")"
check "a symlink to a file as parent is refused" "2" "$(code --workdir "$TMPD/tofile/x")"

for leftover in homelink dangling tofile selfish afile; do
  [ -e "$TMPD/$leftover" ] || [ -L "$TMPD/$leftover" ] && rm "$TMPD/$leftover"
done

echo
echo "authorisation still gates everything"
check "no grant is exit 3, whatever the arguments" "3" \
      "$(WOA23_D2B_GRANTED= "$RUNNER" --contract-only --workdir /tmp/x >/dev/null 2>&1; echo $?)"
check "a bad argument outranks the missing grant" "2" \
      "$(WOA23_D2B_GRANTED= "$RUNNER" --bogus >/dev/null 2>&1; echo $?)"
check "the grant message names four services and six processes" "yes" \
      "$(WOA23_D2B_GRANTED= "$RUNNER" 2>&1 | { has_text "$(cat)" "SIX OS processes"; })"

echo
echo "reusing a port is a decision, so it is a flag rather than a default"
check "--allow-reused-ports is accepted" "no" \
      "$([ "$(WOA23_D2B_GRANTED= "$RUNNER" --allow-reused-ports --contract-only \
              --workdir /tmp/x >/dev/null 2>&1; echo $?)" = 2 ] && echo yes || echo no)"
# It takes no value. A flag that swallowed the next argument would silently drop
# whatever followed it — a label, a port — and the run would proceed with a default.
check "it does not swallow the argument after it" "yes" \
      "$(has_text "$(run --allow-reused-ports --contract-only \
                         --workdir "$HOME/woa23-s1-staging-merged" --label kept)" \
                  "label     : kept")"
check "and an unknown flag is still exit 2" "2" \
      "$(WOA23_D2B_GRANTED= "$RUNNER" --allow-reused-port >/dev/null 2>&1; echo $?)"

echo
echo "the resolved configuration is announced, not assumed"
out="$(run --contract-only --workdir "$HOME/woa23-s1-staging-merged" \
           --candidate-port 19001 --reference-port 19002 --scheduler-port 19003)" || true
check "the mode is printed" "yes" "$(has_text "$out" "mode      : contract-only")"
check "the workdir is printed" "yes" \
      "$(has_text "$out" "workdir   : $HOME/woa23-s1-staging-merged")"
check "each port is printed" "yes" \
      "$(has_text "$out" "candidate : 127.0.0.1:19001")"
check "the scheduler port is printed" "yes" \
      "$(has_text "$out" "scheduler : 127.0.0.1:19003")"
out2="$(run --cleanup-only)" || true
check "cleanup-only announces itself" "yes" "$(has_text "$out2" "mode      : cleanup-only")"
out3="$(run)" || true
check "the default mode announces all three phases" "yes" \
      "$(has_text "$out3" "full (contract + latency + pilot)")"

echo
echo "the S2 modes are separate modes, not options on the D2b ones"
check "--c1 and --c2-cycle together are refused" "2" "$(code --c1 --c2-cycle)"
check "--c1 with --contract-only is refused" "2" "$(code --c1 --contract-only)"
check "--c2-cycle with --cleanup-only is refused" "2" "$(code --c2-cycle --cleanup-only)"
check "three modes at once are refused" "2" "$(code --c1 --contract-only --cleanup-only)"
check "and the refusal lists what it got" "yes" \
      "$(has_text "$(run --c1 --contract-only)" "mutually exclusive; got: --contract-only --c1")"

echo
echo "the S2 arguments are required by the S2 modes and refused outside them"
# Real artefacts, because --python-binary, --package-clone and --clone-manifest are
# now checked with the arguments rather than behind the host gate. A case that is
# meant to reach the grant or the mode announcement must therefore name things that
# exist; placeholder paths remain only where the refusal happens earlier still, at
# argument shape.
S2FIX="$(mktemp -d)"
mkdir -p "$S2FIX/clone" "$S2FIX/work-clone"
# A real four-column line: relpath, sha256, size, mtime_ns. The runner rejects
# anything else before the grant, so an empty file here would stop every case that
# is meant to reach further.
printf 'a.py\t%s\t12\t1722906835056193900\n' "$(printf 'f%.0s' $(seq 64))" > "$S2FIX/manifest"
cp /bin/echo "$S2FIX/binary" 2>/dev/null || printf '#!/bin/sh\ntrue\n' > "$S2FIX/binary"
chmod +x "$S2FIX/binary"
chmod a-w "$S2FIX/clone" "$S2FIX/work-clone"
S2OK=(--python-binary "$S2FIX/binary" --package-clone "$S2FIX/clone"
      --clone-manifest "$S2FIX/manifest")
# The whole point: there is no fallback to dev2026/.venv. A missing flag must stop
# the run, never silently select the campaign's own environment.
check "--c1 without --python-binary is refused" "2" "$(code --c1)"
check "and says there is no fallback" "yes" \
      "$(has_text "$(run --c1)" "no fallback to dev2026/.venv")"
check "--c1 without --package-clone is refused" "2" \
      "$(code --c1 --python-binary /usr/bin/python3)"
check "--c1 without --clone-manifest is refused" "2" \
      "$(code --c1 --python-binary /usr/bin/python3 --package-clone /tmp/clone)"
check "and says why the manifest matters" "yes" \
      "$(has_text "$(run --c1 --python-binary /usr/bin/python3 --package-clone /tmp/clone)" \
         "distinguishes the verified clone")"
check "a relative --package-clone is refused" "2" \
      "$(code --c1 --python-binary /usr/bin/python3 --package-clone clone --clone-manifest /tmp/m)"
check "and says it would resolve against the invoking directory" "yes" \
      "$(has_text "$(run --c1 --python-binary /usr/bin/python3 --package-clone clone --clone-manifest /tmp/m)" \
         "must be an absolute path")"

# Accepting a flag that has no effect reads as the flag having taken effect.
check "--python-binary outside an S2 mode is refused" "2" \
      "$(code --python-binary /usr/bin/python3)"
check "and says it would have been ignored" "yes" \
      "$(has_text "$(run --package-clone /tmp/clone)" "have ignored it")"
check "--package-clone outside an S2 mode is refused" "2" "$(code --package-clone /tmp/c)"
check "--clone-manifest outside an S2 mode is refused" "2" "$(code --clone-manifest /tmp/m)"
check "--workers outside an S2 mode is refused" "2" "$(code --workers 2)"
check "--expected-workers is refused under --c1" "2" \
      "$(code --c1 "${S2OK[@]}" --expected-workers 2)"
check "and says C1 pins one worker per arm" "yes" \
      "$(has_text "$(run --c1 "${S2OK[@]}" --expected-workers 2)" "C1 pins one worker")"

# The old name is refused rather than aliased: it read like a setting, and the
# number it names is read from production and cannot be chosen.
check "the old --workers spelling is refused outright" "2" \
      "$(code --c2-cycle "${S2OK[@]}" --workers 2)"
check "and says it never set the arms worker count" "yes" \
      "$(has_text "$(run --c2-cycle "${S2OK[@]}" --workers 2)" "It never set")"
check "and that the count is read from production" "yes" \
      "$(has_text "$(run --c2-cycle "${S2OK[@]}" --workers 2)" "cannot be chosen")"

S2ARGS_C2=(--c2-cycle "${S2OK[@]}")
check "a non-numeric --expected-workers is refused" "2" "$(code "${S2ARGS_C2[@]}" --expected-workers two)"
check "--expected-workers 0 is refused" "2" "$(code "${S2ARGS_C2[@]}" --expected-workers 0)"
check "--expected-workers 99 is refused" "2" "$(code "${S2ARGS_C2[@]}" --expected-workers 99)"
check "--expected-workers 2 is accepted past validation" "3" "$(code "${S2ARGS_C2[@]}" --expected-workers 2)"

check "a label with a space is refused" "2" "$(code --label 'two words')"
check "a label with a slash is refused" "2" "$(code --label a/b)"
check "and says it becomes a filename" "yes" \
      "$(has_text "$(run --label 'a b')" "becomes part of a filename")"
check "an ordinary label is accepted" "4" "$(code --label c2_cycle1)"

echo
echo "the clone is subject to the production boundary too"
S2ARGS_C1=(--c1 --python-binary "$S2FIX/binary" --clone-manifest "$S2FIX/manifest")
check "a clone inside production is refused" "2" \
      "$(code "${S2ARGS_C1[@]}" --package-clone "$HOME/python/woa23/site-packages")"
check "and explains it would import the live tree" "yes" \
      "$(has_text "$(run "${S2ARGS_C1[@]}" --package-clone "$HOME/python/woa23/x")" \
         "is inside production")"
check "production's own site-packages as the clone is refused" "2" \
      "$(code "${S2ARGS_C1[@]}" \
              --package-clone "$HOME/.pyenv/versions/py311/lib/python3.11/site-packages")"
check "a clone inside the workdir is refused" "2" \
      "$(code "${S2ARGS_C1[@]}" --workdir "$S2FIX/work" \
              --package-clone "$S2FIX/work/clone")"
check "and says the workdir is written to" "yes" \
      "$(has_text "$(run "${S2ARGS_C1[@]}" --workdir "$S2FIX/work" \
                        --package-clone "$S2FIX/work/clone")" "must be immutable")"
check "a clone merely sharing the workdir's prefix reaches the grant" "3" \
      "$(code "${S2ARGS_C1[@]}" --workdir "$S2FIX/work" \
              --package-clone "$S2FIX/work-clone")"

echo
echo "the three named artefacts must exist, and the clone must be read-only"
# Checked with the arguments rather than with the host prerequisites: each names
# something given on the command line, so a wrong name is a configuration error —
# and behind the host gate none of these refusals could be exercised off VM24 at all.
S2D="$(mktemp -d)"
mkdir -p "$S2D/clone" "$S2D/rwclone"
printf 'a.py\t%s\t12\t1722906835056193900\n' "$(printf 'f%.0s' $(seq 64))" > "$S2D/manifest"
# The two files that get passed by mistake, side by side, as they are on VM24.
: > "$S2D/empty-manifest"
printf '3c14cfe70369d081b9b10467d258892654299cba360a3852b886274550630407  source.manifest\n' \
  > "$S2D/SHA256SUMS"
: > "$S2D/notabinary"
cp /bin/echo "$S2D/binary" 2>/dev/null || printf '#!/bin/sh\ntrue\n' > "$S2D/binary"
chmod +x "$S2D/binary"
chmod a-w "$S2D/clone"

ok_c1() { code --c1 --python-binary "$1" --package-clone "$2" --clone-manifest "$3" \
               "${@:4}"; }
ok_c1_run() { run --c1 --python-binary "$1" --package-clone "$2" --clone-manifest "$3"; }

check "a nonexistent --python-binary is refused" "2" \
      "$(ok_c1 "$S2D/no-such-python" "$S2D/clone" "$S2D/manifest")"
check "and says it does not exist" "yes" \
      "$(has_text "$(ok_c1_run "$S2D/no-such-python" "$S2D/clone" "$S2D/manifest")" \
         "does not exist")"
check "a --python-binary that is not executable is refused" "2" \
      "$(ok_c1 "$S2D/notabinary" "$S2D/clone" "$S2D/manifest")"
check "and says it is not an executable file" "yes" \
      "$(has_text "$(ok_c1_run "$S2D/notabinary" "$S2D/clone" "$S2D/manifest")" \
         "is not an executable file")"
check "a directory given as --python-binary is refused" "2" \
      "$(ok_c1 "$S2D/clone" "$S2D/clone" "$S2D/manifest")"

check "a nonexistent --package-clone is refused" "2" \
      "$(ok_c1 "$S2D/binary" "$S2D/no-such-clone" "$S2D/manifest")"
check "and says it does not exist" "yes" \
      "$(has_text "$(ok_c1_run "$S2D/binary" "$S2D/no-such-clone" "$S2D/manifest")" \
         "does not exist")"
check "a file given as --package-clone is refused" "2" \
      "$(ok_c1 "$S2D/binary" "$S2D/manifest" "$S2D/manifest")"
check "and says it is not a directory" "yes" \
      "$(has_text "$(ok_c1_run "$S2D/binary" "$S2D/manifest" "$S2D/manifest")" \
         "is not a directory")"

check "a nonexistent --clone-manifest is refused" "2" \
      "$(ok_c1 "$S2D/binary" "$S2D/clone" "$S2D/no-such-manifest")"
check "a directory given as --clone-manifest is refused" "2" \
      "$(ok_c1 "$S2D/binary" "$S2D/clone" "$S2D/clone")"

# The failure of 2026-08-10: the clone root's SHA256SUMS was passed instead of
# clone.manifest. It exists and is readable, so every check up to here passed it,
# and the run got as far as bootstrapping a venv and building an environment
# record before the integrity stage finally read the file. It is now refused in
# the same second the argument is read, and refused by NAME.
check "the clone root's SHA256SUMS is refused" "2" \
      "$(ok_c1 "$S2D/binary" "$S2D/clone" "$S2D/SHA256SUMS")"
out="$(ok_c1_run "$S2D/binary" "$S2D/clone" "$S2D/SHA256SUMS")"
check "and is identified as a digest list" "yes" \
      "$(has_text "$out" "sha256sum-style digest list")"
check "and the right file is named" "yes" "$(has_text "$out" "clone.manifest")"
check "and its four columns are spelled out" "yes" \
      "$(has_text "$out" "relpath, sha256, size, mtime_ns")"
check "and the field count it actually found is reported" "yes" \
      "$(has_text "$out" "1 tab-separated field")"

check "an empty file is refused too" "2" \
      "$(ok_c1 "$S2D/binary" "$S2D/clone" "$S2D/empty-manifest")"
check "without calling it a digest list" "no" \
      "$(has_text "$(ok_c1_run "$S2D/binary" "$S2D/clone" "$S2D/empty-manifest")" \
         "digest list")"

# The clone must be the immutable artefact. A writable one may already have been
# modified, and this run could modify it further.
check "a writable clone is refused" "2" \
      "$(ok_c1 "$S2D/binary" "$S2D/rwclone" "$S2D/manifest")"
check "and says it could be modified further" "yes" \
      "$(has_text "$(ok_c1_run "$S2D/binary" "$S2D/rwclone" "$S2D/manifest")" \
         "could modify it further")"

# All three good: the next thing that stops it is the missing grant, not the paths.
check "three valid artefacts get as far as the grant" "3" \
      "$(env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
           "$RUNNER" --c1 --python-binary "$S2D/binary" --package-clone "$S2D/clone" \
           --clone-manifest "$S2D/manifest" >/dev/null 2>&1; echo $?)"
check "and with the grant, as far as the host check" "4" \
      "$(env -u WOA23_D2B_GRANTED -u WOA23_S2_C2_GRANTED WOA23_S2_C1_GRANTED=yes \
           "$RUNNER" --c1 --python-binary "$S2D/binary" --package-clone "$S2D/clone" \
           --clone-manifest "$S2D/manifest" >/dev/null 2>&1; echo $?)"

chmod u+w "$S2D/clone"
rm -rf "$S2D"

echo
echo "the arms never fall back to dev2026/.venv — structurally, not just by message"
# The refusals above cover a missing flag. This covers the other half: that no S2
# launch line can reach the campaign's own venv even if one were added by accident.
RUNSRC="$(cat "$RUNNER")"
# The C1 branch now also carries D1, which launches the arms identically — one
# worker each, seed pinned, production's binary against the clone. The range still
# starts at that `elif`, whatever conditions were added to it.
s2_launch="$(awk '/^elif \[ "\$S2_MODE" = c1 \]/,/^fi$/' "$RUNNER")"
check "the C1 and C2 arm launches never mention \$VENV" "no" \
      "$(has_text "$s2_launch" 'VENV')"
check "they use the production binary" "yes" "$(has_text "$s2_launch" '"$PY_BINARY" -S -m gunicorn')"
check "with the clone as the only package source" "yes" \
      "$(has_text "$s2_launch" 'PYTHONPATH="$PKG_CLONE"')"
check "and VIRTUAL_ENV unset rather than overridden" "yes" \
      "$(has_text "$s2_launch" '-u VIRTUAL_ENV -u PYTHONHOME')"
check "C1 pins the seed" "yes" "$(has_text "$s2_launch" 'PYTHONHASHSEED=0 PYTHONPATH')"
# CPython rejects PYTHONHASHSEED="", so an empty value would stop the interpreter
# starting rather than unpin it — the arm would fail for the wrong reason.
check "C2 unsets the seed rather than emptying it" "yes" \
      "$(has_text "$s2_launch" '-u PYTHONHASHSEED')"
# Comment lines are stripped first: the runner explains this very rule in a comment
# that quotes the string being searched for, so a naive grep matches the warning
# against the mistake and then reports the mistake.
check "no S2 launch sets PYTHONHASHSEED to an empty value" "no" \
      "$(has_text "$(grep -v '^[[:space:]]*#' "$RUNNER")" 'PYTHONHASHSEED=""')"
check "bytecode writing is disabled on every S2 launch" "yes" \
      "$(has_text "$s2_launch" 'PYTHONDONTWRITEBYTECODE=1')"
check "user site-packages too" "yes" "$(has_text "$s2_launch" 'PYTHONNOUSERSITE=1')"

echo
echo "each experiment has its own grant, and none implies another"
S2ARGS_OK=("${S2OK[@]}")
# The first argument is the grant environment, as a space-separated string of
# VAR=value assignments; everything after it is the runner's own arguments. They
# have to be kept apart: `env` reads anything before the command name as its own
# option, so passing --c1 through in the same list makes env reject it and the test
# measures env's exit code instead of the runner's. Three of these read 127 before
# the split, which is "command not found" and not any refusal this script makes.
# All three grants are unset first so the caller's shell cannot leak one in.
s2run() {  # s2run "<VAR=val ...>" [args...]
  local envs="$1"; shift
  env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
      $envs "$RUNNER" "$@" 2>&1 || true
}
s2code() { # s2code "<VAR=val ...>" [args...]
  local envs="$1"; shift
  env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
      $envs "$RUNNER" "$@" >/dev/null 2>&1
  echo $?
}

check "--c1 with no grant at all is exit 3" "3" \
      "$(s2code "" --c1 "${S2ARGS_OK[@]}")"
check "--c1 with only the D2b grant is still exit 3" "3" \
      "$(s2code "WOA23_D2B_GRANTED=yes" --c1 "${S2ARGS_OK[@]}")"
check "and says explicitly that D2b does not authorise it" "yes" \
      "$(has_text "$(s2run "WOA23_D2B_GRANTED=yes" --c1 "${S2ARGS_OK[@]}")" \
         "does NOT authorise this")"
check "--c1 with its own grant gets past authorisation" "4" \
      "$(s2code "WOA23_S2_C1_GRANTED=yes" --c1 "${S2ARGS_OK[@]}")"
check "--c2-cycle with only the C1 grant is exit 3" "3" \
      "$(s2code "WOA23_S2_C1_GRANTED=yes" --c2-cycle "${S2ARGS_OK[@]}")"
check "--c2-cycle with its own grant gets past authorisation" "4" \
      "$(s2code "WOA23_S2_C2_GRANTED=yes" --c2-cycle "${S2ARGS_OK[@]}")"

# The converse, so a leftover export cannot widen what was granted.
check "a D2b run with a C1 grant in the environment is refused" "3" \
      "$(s2code "WOA23_D2B_GRANTED=yes WOA23_S2_C1_GRANTED=yes" --contract-only)"
check "and names the variable that does not apply" "yes" \
      "$(has_text "$(s2run "WOA23_D2B_GRANTED=yes WOA23_S2_C1_GRANTED=yes" --contract-only)" \
         "WOA23_S2_C1_GRANTED is set but this is a D2b mode")"
check "a D2b run with a C2 grant in the environment is refused" "3" \
      "$(s2code "WOA23_D2B_GRANTED=yes WOA23_S2_C2_GRANTED=yes")"
check "a C1 run with the C2 grant also set is refused" "3" \
      "$(s2code "WOA23_S2_C1_GRANTED=yes WOA23_S2_C2_GRANTED=yes" --c1 "${S2ARGS_OK[@]}")"
check "a C2 run with the C1 grant also set is refused" "3" \
      "$(s2code "WOA23_S2_C1_GRANTED=yes WOA23_S2_C2_GRANTED=yes" --c2-cycle "${S2ARGS_OK[@]}")"
# A bad argument still outranks a missing grant, as it does for D2b.
check "a bad S2 argument outranks the missing S2 grant" "2" \
      "$(s2code "" --c1 --python-binary relative/path --package-clone /tmp/c \
                --clone-manifest /tmp/m)"

echo
echo "an S2 invocation announces the environment it will use, and its budget"
s2out="$(s2run "WOA23_S2_C1_GRANTED=yes" --c1 "${S2ARGS_OK[@]}" \
                 --workdir "$HOME/woa23-s2-c1-work" \
                 --candidate-port 18061 --reference-port 18062 --scheduler-port 18798)"
check "the mode names the experiment" "yes" \
      "$(has_text "$s2out" "mode      : C1 (S2: production binary + package clone, 5.2C canonical row order)")"
check "the binary is printed" "yes" "$(has_text "$s2out" "binary    : $S2FIX/binary")"
# Resolved, not as typed. The boundary check follows symlinks before comparing, and
# the announcement has to show what was actually checked — otherwise a symlinked
# clone would be announced as one path and validated as another.
S2FIX_PHYS="$(cd "$S2FIX" && pwd -P)"
check "the clone is printed, physically resolved" "yes" \
      "$(has_text "$s2out" "clone     : $S2FIX_PHYS/clone")"
check "the manifest is printed" "yes" "$(has_text "$s2out" "manifest  : ")"
check "the pinned seed is stated" "yes" \
      "$(has_text "$s2out" "seed      : PYTHONHASHSEED=0 (pinned)")"
check "and that the venv is not on the arms' path" "yes" \
      "$(has_text "$s2out" "is not on any arm's import path")"
check "the per-arm request ceiling is stated" "yes" \
      "$(has_text "$s2out" "per arm  : <= 96")"
check "so is the total" "yes" "$(has_text "$s2out" "total    : <= 192")"
check "production is stated as zero requests" "yes" \
      "$(has_text "$s2out" "0 requests")"

s2out2="$(s2run "WOA23_S2_C2_GRANTED=yes" --c2-cycle "${S2ARGS_OK[@]}" \
                  --workdir "$HOME/woa23-s2-c2-work")"
check "a C2 cycle says the seed is unset on purpose" "yes" \
      "$(has_text "$s2out2" "seed      : unset — this is what C2 observes")"
check "and that the worker count comes from production at run time" "yes" \
      "$(has_text "$s2out2" "workers   : read from production at run time")"
check "and that nothing was asserted when nothing was given" "yes" \
      "$(has_text "$s2out2" "expected (asserted): <none asserted>")"
check "and states the three-cycle budget" "yes" \
      "$(has_text "$s2out2" "Three cycles make a C2 result: <= 288 per arm")"
check "and that each cycle is cleaned up before the next" "yes" \
      "$(has_text "$s2out2" "each verified before the next starts")"

d2bout="$(run --contract-only)"
check "a D2b invocation states its own ceiling" "yes" \
      "$(has_text "$d2bout" "per arm  : <= 96")"
check "and the full mode states the larger one" "yes" \
      "$(has_text "$(run)" "per arm  : <= 480")"
check "cleanup-only budgets readiness only" "yes" \
      "$(has_text "$(run --cleanup-only)" "per arm  : <= 30")"

echo
echo "the C2 cycle driver gates itself before it creates anything"
CYCLER="$HERE/run_c2_cycles.sh"
cyc() {    # cyc "<VAR=val ...>" [args...]
  local envs="$1"; shift
  env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
      $envs "$CYCLER" "$@" >/dev/null 2>&1
  echo $?
}
cycrun() { # cycrun "<VAR=val ...>" [args...]
  local envs="$1"; shift
  env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
      $envs "$CYCLER" "$@" 2>&1 || true
}
CYCARGS=(--python-binary /usr/bin/python3 --package-clone /tmp/clone
         --clone-manifest /tmp/m --workdir-base /tmp/c2 --candidate-port 18071
         --reference-port 18072 --scheduler-port 18798)
check "the driver refuses --cycles outright" "2" "$(cyc "" "${CYCARGS[@]}" --cycles 5)"
check "and says why the number is fixed" "yes" \
      "$(has_text "$(cycrun "" "${CYCARGS[@]}" --cycles 5)" "came out a particular way")"
check "a missing required argument is refused" "2" \
      "$(cyc "" --python-binary /usr/bin/python3)"
check "no grant is exit 3" "3" "$(cyc "" "${CYCARGS[@]}")"
check "the D2b grant does not authorise it" "3" \
      "$(cyc "WOA23_D2B_GRANTED=yes" "${CYCARGS[@]}")"
check "nor does the C1 grant" "3" "$(cyc "WOA23_S2_C1_GRANTED=yes" "${CYCARGS[@]}")"
check "and it says C1 is one cycle with a pinned seed" "yes" \
      "$(has_text "$(cycrun "WOA23_S2_C1_GRANTED=yes" "${CYCARGS[@]}")" \
         "C1 is one cycle")"
check "the refusal states it would start the arms three times" "yes" \
      "$(has_text "$(cycrun "" "${CYCARGS[@]}")" "THREE")"
check "--help exits 0" "0" "$(cyc "" --help)"

echo
echo "the embedded Python is fed to a quoted heredoc"
# An unquoted heredoc is shell-expanded before python ever sees it. A comment inside
# one of them contained backticks around a module name; the shell ran it as a
# command ("importlib.metadata: command not found") and handed python a source line
# with the name deleted. It happened to land in a comment, so the run continued and
# the only sign was one stray line on stderr. Quoting every heredoc removes the class
# rather than that instance; the values they used to interpolate now arrive as
# environment variables.
check "every embedded-python heredoc is quoted" "5" \
      "$(grep -c "uv run python - <<'PYEOF'" "$RUNNER")"
check "and none is left unquoted" "0" \
      "$(grep -c 'uv run python - <<PYEOF' "$RUNNER" || true)"
# Nothing in the runner may take an unquoted heredoc at all. The last one fed
# production's argv, converted to newline-delimited text, into a shell loop — which
# splits any argument containing a newline into two and shifts every position after
# it. argv is NUL-separated precisely because an argument may hold anything but NUL.
check "the runner has no unquoted heredoc of any kind" "0" \
      "$(grep -c '<<[A-Za-z]' "$RUNNER" || true)"
check "argv is no longer flattened to newline-delimited text" "no" \
      "$(has_text "$(grep -v '^[[:space:]]*#' "$RUNNER")" "tr '\\0' '\\n'")"
check "the worker count crosses the boundary as one integer" "yes" \
      "$(has_text "$(cat "$RUNNER")" 'from bench.collect_backend_meta import argv_of, worker_count')"

echo
echo "the harness bootstrap and the environment under test are recorded apart"
check "the bootstrap is announced as not the thing under test" "yes" \
      "$(has_text "$(cat "$RUNNER")" "NOT the environment under test")"
check "it writes its own artefact" "yes" \
      "$(has_text "$(cat "$RUNNER")" '_harness_bootstrap.json')"
check "which is a different file from the environment record" "yes" \
      "$(has_text "$(cat "$RUNNER")" '_environment.json')"
check "and the uv scope is stated in the record" "yes" \
      "$(has_text "$(cat "$RUNNER")" 'uv_authorisation_scope')"
check "naming dev2026/.venv as the only thing uv may touch" "yes" \
      "$(has_text "$(cat "$RUNNER")" 'create or sync dev2026/.venv only')"
check "no backticks survive inside the embedded python" "0" \
      "$(awk "/<<.PYEOF/,/^PYEOF\$/" "$RUNNER" | grep -c '\`' || true)"
check "the heredocs read their values from the environment" "yes" \
      "$(has_text "$(cat "$RUNNER")" 'os.environ["PKG_CLONE"]')"

echo
echo "an option value that begins with a dash is passed unambiguously"
# `--env-python-arg -S` makes argparse read -S as the next option and exit 2 with
# "expected one argument". That is what ended the first real C1 attempt, after both
# arms were up and the store had been probed — offline tests never reached it
# because they all stop at argument validation.
check "the runner uses the = form for -S" "yes" \
      "$(has_text "$(cat "$RUNNER")" '--env-python-arg=-S')"
# Comments stripped: the runner explains the rule in a comment that quotes the
# broken form, and matching the explanation against the mistake reports the mistake.
# Third time this exact trap has bitten in this file.
check "and not the separated form" "no" \
      "$(has_text "$(grep -v '^[[:space:]]*#' "$RUNNER")" '--env-python-arg -S')"

echo
echo "readiness does not claim the store is untouched"
# It used to. The candidate's lifespan now reads the anchor group's Zarr metadata
# during startup, before any HTTP is served, so "the store has not been touched" was
# false the moment spec 004's patch landed. The claim is split: this probe read
# nothing; startup already read metadata; no chunk was read either way.
RUNSRC3="$(cat "$RUNNER")"
check "the stale claim is gone" "no" \
      "$(has_text "$RUNSRC3" "The store has not been touched")"
check "the probe's own reach is stated" "yes" \
      "$(has_text "$RUNSRC3" "This probe read nothing from the store")"
check "and what startup already read is stated" "yes" \
      "$(has_text "$RUNSRC3" "has already read Zarr METADATA for 1_degree/annual/TS")"
# The zero-chunk property is NOT observed by this run — no audit hook is installed
# on the host — so the message must say where the evidence comes from rather than
# assert it as a local observation.
check "the zero-chunk claim names its evidence as offline" "yes" \
      "$(has_text "$RUNSRC3" "offline-audited")"
check "and as implementation-supported" "yes" \
      "$(has_text "$RUNSRC3" "implementation-supported")"
check "and admits this run observes no file opens" "yes" \
      "$(has_text "$RUNSRC3" "this run observes no file opens")"
check "it does not assert the property as locally observed" "no" \
      "$(has_text "$RUNSRC3" "and no
    data or coordinate chunk; the reference")"
check "the arms' asymmetry is named" "yes" \
      "$(has_text "$RUNSRC3" "the reference validates nothing at startup")"
check "and data readiness is still deferred to the probe" "yes" \
      "$(has_text "$RUNSRC3" "Neither arm is known to serve DATA yet")"
# The asymmetry is harmless to 5.2A and would not be to a latency comparison; the
# runner says so rather than leaving it for someone to notice later.
# Matched on a fragment that does not straddle the wrap. Fifth time this shape of
# self-inflicted miss has come up in this file: the phrase is in the source, split
# across two comment lines, and a contiguous match reports it absent.
check "the latency implication is recorded" "yes" \
      "$(has_text "$RUNSRC3" "would matter to a latency")"

echo
echo "the C2 process count is derived, never written down"
# 8 is what production's measured -w 2 implies, not a fact about the system. A
# reconfiguration to four workers makes it twelve, and a count that did not move
# with it would be verifying last week's deployment.
RUNSRC2="$(grep -v '^[[:space:]]*#' "$RUNNER")"
check "the authorised total is derived from the measured worker count" "yes" \
      "$(has_text "$RUNSRC2" 'EXPECTED_TOTAL=$((1 + 1 + 2 * (1 + ARM_WORKERS)))')"
check "and the per-service expectation is too" "yes" \
      "$(has_text "$RUNSRC2" 'echo $((1 + ARM_WORKERS))')"
check "no literal 8 is assigned as the authorised total" "no" \
      "$(has_text "$RUNSRC2" 'AUTHORISED_TOTAL=8')"
check "and no literal 6 either" "no" "$(has_text "$RUNSRC2" 'AUTHORISED_TOTAL=6')"
# Fail closed on the way in: an unreadable or implausible worker count stops the run
# rather than being replaced by an assumption.
check "an unreadable worker count refuses to assume one" "yes" \
      "$(has_text "$RUNSRC2" 'Refusing to assume')"
check "and says a guess would answer for a deployment that does not exist" "yes" \
      "$(has_text "$(cat "$RUNNER")" "deployment that does not exist")"
check "an asserted count that disagrees with production aborts" "yes" \
      "$(has_text "$(cat "$RUNNER")" "expected-workers mismatch")"
check "and says the flag is an assertion, never a setting" "yes" \
      "$(has_text "$(cat "$RUNNER")" "is an ASSERTION and never a setting")"
check "the arms always take the ACTUAL number" "yes" \
      "$(has_text "$(grep -v '"'"'^[[:space:]]*#'"'"' "$RUNNER")" 'ARM_WORKERS="$measured"')"
check "and never the asserted one" "no" \
      "$(has_text "$(grep -v '"'"'^[[:space:]]*#'"'"' "$RUNNER")" 'ARM_WORKERS="$WORKERS"')"

echo
echo "production must not move while its configuration is being read"
# The worker count, master PID, start time, listener set and boot id describe one
# process; using them together assumes production held still between the reads.
check "the identity is re-read after the worker count" "yes" \
      "$(has_text "$(cat "$RUNNER")" "PROD_PIDS_RECHECK")"
check "the master is re-identified, not just the port re-polled" "yes" \
      "$(has_text "$(cat "$RUNNER")" "PROD_MASTER_RECHECK")"
check "so is its start time" "yes" "$(has_text "$(cat "$RUNNER")" "PROD_START_RECHECK")"
check "and the boot id" "yes" "$(has_text "$(cat "$RUNNER")" "BOOT_RECHECK")"
check "a change stops the run before any test service starts" "yes" \
      "$(has_text "$(cat "$RUNNER")" "is started")"
check "the production PID is never hard-coded" "0" \
      "$(grep -cE '"'"'/proc/3960|PROD_MASTER=3960'"'"' "$RUNNER" || true)"
check "it is derived from the listener set on the port" "yes" \
      "$(has_text "$(cat "$RUNNER")" 'master_of "$PROD_PIDS_BEFORE"')"

echo
echo "each cycle keeps its own state, results and logs"
check "the run-state directory is per label" "yes" \
      "$(has_text "$(cat "$RUNNER")" 'RUN=$HERE/run/$LABEL')"
check "and the leftover check scans the whole run tree" "yes" \
      "$(has_text "$(cat "$RUNNER")" 'find "$HERE/run" -type f')"
check "so a neighbouring cycle unfinished cleanup is still seen" "yes" \
      "$(has_text "$(cat "$RUNNER")" "would also hide a neighbouring")"

echo
echo "the driver propagates the three C2 outcomes distinctly"
CYCSRC="$(cat "$CYCLER")"
check "exit 5 is handled as its own case" "yes" \
      "$(has_text "$CYCSRC" "PASS_WITH_INSUFFICIENT_SEED_DIVERSITY")"
check "and is not flattened to a plain pass" "yes" \
      "$(has_text "$CYCSRC" "be a false report")"
check "and triggers no fourth cycle" "yes" \
      "$(has_text "$CYCSRC" "there is no fourth cycle")"
check "the summary status is propagated, not discarded" "yes" \
      "$(has_text "$CYCSRC" 'exit "$summary_rc"')"

chmod u+w "$S2FIX/clone" "$S2FIX/work-clone"
rm -r "$S2FIX"

echo
echo "nothing above started a process or bound a port"
check "no candidate/reference/scheduler process was left behind" "0" \
      "$(pgrep -f 'gunicorn (api.app|woa23_app):app' 2>/dev/null | wc -l | tr -d ' ')"
check "no staging workdir was created" "no" \
      "$([ -e "$HOME/woa23-s1-staging-merged" ] && echo yes || echo no)"

echo
echo "--d1 is its own mode, with its own grant and its own gate"
# D1 issues requests no contract case covers, against the real store. It reuses C1's
# environment and nothing else, so every boundary is checked rather than inherited.
check "an unknown flag near it is still exit 2" "2" \
      "$(WOA23_D2B_GRANTED= "$RUNNER" --d1x >/dev/null 2>&1; echo $?)"
check "it is mutually exclusive with --c1" "2" \
      "$(code --d1 --c1)"
check "and with --c2-cycle" "2" "$(code --d1 --c2-cycle)"
check "and with --contract-only" "2" "$(code --d1 --contract-only)"
check "and with --cleanup-only" "2" "$(code --d1 --cleanup-only)"
check "the refusal names the modes it got" "yes" \
      "$(has_text "$(run --d1 --c1)" "mutually exclusive")"

check "--d1 requires --python-binary like the other S2 modes" "2" "$(code --d1)"
check "and says there is no fallback" "yes" \
      "$(has_text "$(run --d1)" "no fallback")"
check "--expected-workers is refused for D1, as for C1" "2" \
      "$(code --d1 --python-binary /bin/sh --package-clone /tmp \
              --clone-manifest /etc/hostname --expected-workers 2)"
check "and explains that one process per side is the point" "yes" \
      "$(has_text "$(run --d1 --python-binary /bin/sh --package-clone /tmp \
                         --clone-manifest /etc/hostname --expected-workers 2)" \
         "one process per side")"

# The grant boundary, in both directions.
D1D="$(mktemp -d)"
mkdir -p "$D1D/clone"
printf 'a.py\t%s\t12\t1722906835056193900\n' "$(printf 'f%.0s' $(seq 64))" \
  > "$D1D/manifest"
cp /bin/echo "$D1D/binary" 2>/dev/null || printf '#!/bin/sh\ntrue\n' > "$D1D/binary"
chmod +x "$D1D/binary"
chmod a-w "$D1D/clone"
d1_run() {
  env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
      -u WOA23_D1_GRANTED "$@" \
      "$RUNNER" --d1 --python-binary "$D1D/binary" --package-clone "$D1D/clone" \
      --clone-manifest "$D1D/manifest" 2>&1
}
d1_code() { d1_run "$@" >/dev/null 2>&1; echo $?; }
check "no grant is exit 3" "3" "$(d1_code)"
check "and it names the variable to set" "yes" \
      "$(has_text "$(d1_run)" "WOA23_D1_GRANTED")"
check "a C1 grant does not authorise D1" "3" "$(d1_code WOA23_S2_C1_GRANTED=yes)"
check "and says so" "yes" \
      "$(has_text "$(d1_run WOA23_S2_C1_GRANTED=yes)" "does NOT authorise")"
check "a C2 grant does not either" "3" "$(d1_code WOA23_S2_C2_GRANTED=yes)"
check "a D2b grant does not either" "3" "$(d1_code WOA23_D2B_GRANTED=yes)"
check "with the D1 grant it reaches the host check" "4" \
      "$(d1_code WOA23_D1_GRANTED=yes)"
# And the converse: a D1 grant must not widen a D2b run.
check "a D1 grant does not authorise a D2b mode" "3" \
      "$(env -u WOA23_D2B_GRANTED WOA23_D1_GRANTED=yes "$RUNNER" --contract-only \
           --workdir /tmp/x >/dev/null 2>&1; echo $?)"
chmod u+w "$D1D/clone"
rm -rf "$D1D"

echo
echo "the D1 mode's own wiring, read out of the runner"
check "the survey runs before either arm starts" "yes" \
      "$(python3 - "$RUNNER" <<'PY'
import sys
s = open(sys.argv[1]).read()
print("yes" if s.index("bench.store_survey") < s.index("start_tracked reference")
      else "no")
PY
)"
check "the D1 gate precedes the contract gate and exits before it" "yes" \
      "$(python3 - "$RUNNER" <<'PY'
import sys
s = open(sys.argv[1]).read()
print("yes" if s.index("bench.d1_evidence") < s.index('echo "== contract gate')
      else "no")
PY
)"
# The characterization subtotal and the per-arm countable total are different
# numbers, and the first harness commit gave only the first one a name — which is
# the number a reader then used as the second.
check "the characterization subtotal is named for what it counts" "yes" \
      "$(has_text "$RUNSRC" "BUDGET_D1_CHARACTERIZATION=8")"
check "the ambiguous name is gone" "no" "$(has_text "$RUNSRC" "BUDGET_D1=")"
check "the countable per-arm figure is printed" "yes" \
      "$(has_text "$RUNSRC" "COUNTABLE per arm")"
check "and it is derived, not a literal" "yes" \
      "$(has_text "$RUNSRC" "D1_COUNTABLE_PER_ARM=\$((BUDGET_PROBE + BUDGET_D1_CHARACTERIZATION))")"
check "the recovery probes are described as one per case" "yes" \
      "$(has_text "$RUNSRC" "ONE AFTER EACH CASE")"
check "and refuses to call the total precise" "yes" \
      "$(has_text "$RUNSRC" "a RANGE, and must be reported as one")"
check "a survey that cannot establish its preconditions stops the run" "yes" \
      "$(has_text "$RUNSRC" "CHARACTERIZATION PENDING")"
check "and says the store is not modified" "yes" \
      "$(has_text "$RUNSRC" "no substitute data is created")"
check "the D1 probe is invoked with both arms' urls" "yes" \
      "$(has_text "$RUNSRC" '--candidate-url "http://127.0.0.1:${CAND_PORT}"')"
check "and both arms' pids" "yes" \
      "$(has_text "$RUNSRC" "--candidate-pid")"

echo
echo "D1 fixes one worker per arm, and the record says that is a choice not a finding"
# C2 MEASURES production's worker count and runs the arms at it. Every other mode
# fixes it at one. "workers: 1" reads the same either way, so the difference is
# recorded rather than left to the reader.
check "the expectation is fixed for the non-C2 modes" "yes" \
      "$(has_text "$RUNSRC" "EXPECT_ARM_WORKERS=1")"
check "and C2 takes it from its measurement instead" "yes" \
      "$(has_text "$RUNSRC" 'EXPECT_ARM_WORKERS="$measured"')"
check "the source of the number is carried beside it" "yes" \
      "$(has_text "$RUNSRC" "NOT derived from production")"
check "each arm's argv is asserted to have that count" "yes" \
      "$(has_text "$RUNSRC" "--expect-worker-count")"
# The distinction --expected-workers already carries for C2: confusing a setting
# with an assertion is how a harness chooses a number and reports it as production's.
# Substrings only, and none containing a quote or a backtick — the escaping needed to
# match those through two layers of shell is its own source of bugs.
check "the flag is described as asserting, not setting" "yes" \
      "$(has_text "$RUNSRC" "the -w on the launch line sets it")"
check "the runner says which one is the setting" "yes" \
      "$(has_text "$RUNSRC" "on each launch line below is the setting")"
# The record itself moved into bench/d1_finalize.py when the finalisation was made
# testable; the distinction has to be asserted where it now lives.
check "the artefact carries the same distinction" "yes" \
      "$(has_text "$(cat "$HERE/../bench/d1_finalize.py")" \
         "It is an assertion and sets nothing")"
check "and the printed report separates SET from ASSERTED" "yes" \
      "$(has_text "$RUNSRC" "SET by this mode")"
check "the run writes a worker-count artefact" "yes" \
      "$(has_text "$RUNSRC" "_workers.json")"
check "carrying the sentence that separates D1 from C2" "yes" \
      "$(has_text "$RUNSRC" "makes no claim about")"
check "and naming it as not a production-worker-count validation" "yes" \
      "$(has_text "$RUNSRC" "production-worker-count validation")"
check "the banner says 'read from production' only for C2" "yes" \
      "$(has_text "$RUNSRC" 'if [ "$S2_MODE" = c2 ]; then
    echo "   workers   : read from production at run time"')"
check "and tells the other modes the number is fixed, not measured" "yes" \
      "$(has_text "$RUNSRC" "NOT measured from production")"
check "the artefact records both arms' full launch argv" "yes" \
      "$(has_text "$(cat "$HERE/../bench/d1_finalize.py")" '"launch_argv": argv')"
check "and reads the count from that argv, not from the shell variable" "yes" \
      "$(has_text "$(cat "$HERE/../bench/d1_finalize.py")" \
         "from bench.collect_backend_meta import worker_count")"
check "and the label reaches it as an argument" "yes" \
      "$(has_text "$(cat "$HERE/../bench/d1_finalize.py")" '--label", required=True')"

echo
echo "--s2-perf is its own mode, and its ceiling is one number"
check "an unknown neighbour is still exit 2" "2" \
      "$(WOA23_D2B_GRANTED= "$RUNNER" --s2-perfx >/dev/null 2>&1; echo $?)"
check "it is mutually exclusive with --c1" "2" "$(code --s2-perf --c1)"
check "and with --d1" "2" "$(code --s2-perf --d1)"
check "and with --contract-only" "2" "$(code --s2-perf --contract-only)"
check "it requires the S2 arguments" "2" "$(code --s2-perf)"
# One worker per arm is the mode's own choice, so asserting production's count is
# refused here exactly as it is for C1.
check "--expected-workers is refused" "2" \
      "$(code --s2-perf --python-binary /bin/sh --package-clone /tmp \
              --clone-manifest /etc/hostname --expected-workers 2)"
check "and it says the median would become a median over an allocation policy" "yes" \
      "$(has_text "$(run --s2-perf --python-binary /bin/sh --package-clone /tmp \
                         --clone-manifest /etc/hostname --expected-workers 2)" \
         "allocation policy")"

PERFD="$(mktemp -d)"
mkdir -p "$PERFD/clone"
printf 'a.py\t%s\t12\t1722906835056193900\n' "$(printf 'f%.0s' $(seq 64))" \
  > "$PERFD/manifest"
cp /bin/echo "$PERFD/binary" 2>/dev/null || printf '#!/bin/sh\ntrue\n' > "$PERFD/binary"
chmod +x "$PERFD/binary"
chmod a-w "$PERFD/clone"
perf_run() {
  env -u WOA23_D2B_GRANTED -u WOA23_S2_C1_GRANTED -u WOA23_S2_C2_GRANTED \
      -u WOA23_D1_GRANTED -u WOA23_S2PERF_GRANTED "$@" \
      "$RUNNER" --s2-perf --python-binary "$PERFD/binary" \
      --package-clone "$PERFD/clone" --clone-manifest "$PERFD/manifest" 2>&1
}
perf_code() { perf_run "$@" >/dev/null 2>&1; echo $?; }
check "no grant is exit 3" "3" "$(perf_code)"
check "and it names its own variable" "yes" \
      "$(has_text "$(perf_run)" "WOA23_S2PERF_GRANTED")"
check "a C1 grant does not authorise it" "3" "$(perf_code WOA23_S2_C1_GRANTED=yes)"
check "a D1 grant does not either" "3" "$(perf_code WOA23_D1_GRANTED=yes)"
check "a D2b grant does not either" "3" "$(perf_code WOA23_D2B_GRANTED=yes)"
check "with its own grant it reaches the host check" "4" \
      "$(perf_code WOA23_S2PERF_GRANTED=yes)"

# The budget banner, on a run that gets as far as printing it.
perf_banner="$(env -u WOA23_D2B_GRANTED WOA23_S2PERF_GRANTED=yes "$RUNNER" --s2-perf \
  --python-binary "$PERFD/binary" --package-clone "$PERFD/clone" \
  --clone-manifest "$PERFD/manifest" 2>&1 || true)"
check "the ceiling is stated as 496 per arm" "yes" \
      "$(has_text "$perf_banner" "496 per arm")"
check "and 992 in total" "yes" "$(has_text "$perf_banner" "992 total")"
# The pilot samples every case. 26 is its per-case count; reading it as the per-arm
# count understated the ceiling by 182 per arm.
check "the pilot is budgeted per case, not per arm" "yes" \
      "$(has_text "$perf_banner" "noise pilot 208")"
check "the contract gate is inside that number" "yes" \
      "$(has_text "$perf_banner" "+ contract 64")"
check "so is the symmetric warm-up" "yes" \
      "$(has_text "$perf_banner" "+ symmetric warm-up 16")"
check "and the latency samples" "yes" "$(has_text "$perf_banner" "+ latency 176")"
check "bootstrap resampling is excluded in writing" "yes" \
      "$(has_text "$perf_banner" "Bootstrap resampling issues NO HTTP")"
check "the margin is called an approved engineering threshold" "yes" \
      "$(has_text "$perf_banner" "APPROVED ENGINEERING THRESHOLD: 0.05")"
check "and the superseded pre-approval wording is gone" "no" \
      "$(has_text "$perf_banner" "PROPOSED ENGINEERING THRESHOLD")"
check "approval is stated as a decision threshold, not a data claim" "yes" \
      "$(has_text "$perf_banner" "not a confidence guarantee")"
check "and disclaimed as an SLA" "yes" "$(has_text "$perf_banner" "not an SLA")"
chmod u+w "$PERFD/clone"
rm -rf "$PERFD"

echo
CHAINSRC="$(cat "$(dirname "$RUNNER")/lib_s2perf.sh")"
echo "the performance path runs both gates in one execution, in order"
check "the C1/C2 stop does not swallow it" "yes" \
      "$(has_text "$RUNSRC" '[ "$S2_MODE" != none ] && [ "$S2_MODE" != s2perf ]')"
check "the warm-up precedes the latency gate" "yes" \
      "$(python3 - "$RUNNER" <<'PY'
import sys
s = open(sys.argv[1]).read()
print("yes" if s.index("s2perf_warmup ") < s.index("s2perf_latency ") else "no")
PY
)"
check "and the contract gate precedes the warm-up" "yes" \
      "$(python3 - "$RUNNER" <<'PY'
import sys
s = open(sys.argv[1]).read()
print("yes" if s.index("s2perf_contract ") < s.index("s2perf_warmup ") else "no")
PY
)"
check "the pilot runs after the latency gate" "yes" \
      "$(python3 - "$RUNNER" <<'PY'
import sys
s = open(sys.argv[1]).read()
print("yes" if s.index("s2perf_latency ") < s.index("s2perf_pilot ") else "no")
PY
)"
check "and the runner sources the chain rather than inlining it" "yes" \
      "$(has_text "$RUNSRC" "lib_s2perf.sh")"
# Every stage it issues is recorded — and the three measurable ones are recorded
# from their own artefacts, not from the ceiling they were authorised against.
check "the measured stages go through perf_counts" "yes" \
      "$([ "$(has_text "$CHAINSRC" 'for stage in symmetric_warmup latency noise_pilot')" = yes ] \
         && [ "$(has_text "$CHAINSRC" "bench.perf_counts")" = yes ] \
         && echo yes || echo no)"
check "and the contract stage is still derived from the case list" "yes" \
      "$(has_text "$CHAINSRC" 'request_add "$arm" contract "$n"')"
# Exactly one owner. Two is what put 528 per arm against an authorised 496.
check "which exactly one place records" "1" \
      "$(printf '%s\n' "$RUNSRC$CHAINSRC" | grep -c 'request_add.* contract ')"
check "the runner calls that owner once, before the gate" "1" \
      "$(printf '%s\n' "$RUNSRC" | grep -c '^record_contract_count ')"
check "and a run over its ceiling is refused a result" "yes" \
      "$([ "$(has_text "$CHAINSRC" "assert_request_ceiling")" = yes ] \
         && [ "$(has_text "$CHAINSRC" "CEILING_EXCEEDED")" = yes ] \
         && echo yes || echo no)"
check "no performance stage records a budget constant as traffic" "yes" \
      "$([ "$(has_text "$RUNSRC$CHAINSRC" 'symmetric_warmup "$BUDGET_WARMUP"')" = no ] \
         && [ "$(has_text "$RUNSRC$CHAINSRC" 'latency "$BUDGET_LATENCY"')" = no ] \
         && [ "$(has_text "$RUNSRC$CHAINSRC" 'noise_pilot "$BUDGET_PILOT"')" = no ] \
         && echo yes || echo no)"
check "every stage journals its attempts before issuing them" "yes" \
      "$([ "$(has_text "$CHAINSRC" "--request-log")" = yes ] \
         && [ "$(has_text "$CHAINSRC" "symmetric_warmup.jsonl")" = yes ] \
         && [ "$(has_text "$CHAINSRC" "latency.jsonl")" = yes ] \
         && [ "$(has_text "$CHAINSRC" "noise_pilot.jsonl")" = yes ] \
         && echo yes || echo no)"
check "an inexact count is classified, not reported as a total" "yes" \
      "$([ "$(has_text "$CHAINSRC" "INCOMPLETE_STAGE_FAILURE")" = yes ] \
         && [ "$(has_text "$CHAINSRC" "The MEASUREMENT is incomplete either way")" = yes ] \
         && [ "$(has_text "$CHAINSRC" "NOT a complete latency result")" = yes ] \
         && echo yes || echo no)"
# A journal written before each call bounds nothing once the writer is killed
# asynchronously, and the chain must not describe it as though it did.
check "and a killed stage's journal count is not called a bound" "yes" \
      "$([ "$(has_text "$CHAINSRC" "No bound is claimed")" = yes ] \
         && [ "$(has_text "$CHAINSRC" "a floor, not a total")" = no ] \
         && echo yes || echo no)"
check "the classification names the failed stage itself" "yes" \
      "$(has_text "$CHAINSRC" '"failed_stages": [%s]')"
check "it finalises through the same function D1 uses" "yes" \
      "$(has_text "$CHAINSRC" "S2 performance classification: INVALID_POST_MEASUREMENT_HARNESS")"
check "the closing report refuses to pool the cases" "yes" \
      "$(has_text "$CHAINSRC" "NEVER pooled across cases")"
check "and refuses the coverage claim" "yes" \
      "$(has_text "$CHAINSRC" "not a proven 95% coverage interval")"
check "and lists what the result is not" "yes" \
      "$(has_text "$CHAINSRC" "not multi-worker, not cold-cache")"
check "and states the scope as rung 21 only" "yes" \
      "$([ "$(has_text "$CHAINSRC" "rung 21 ONLY")" = yes ] \
         && [ "$(has_text "$CHAINSRC" "NO startup measurement")" = yes ] \
         && echo yes || echo no)"

echo
suite_summary "$pass" "$fail"
