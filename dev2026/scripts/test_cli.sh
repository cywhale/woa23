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
check "--workers is refused under --c1" "2" \
      "$(code --c1 --python-binary /usr/bin/python3 --package-clone /tmp/c \
              --clone-manifest /tmp/m --workers 2)"
check "and says C1 pins one worker per arm" "yes" \
      "$(has_text "$(run --c1 --python-binary /usr/bin/python3 --package-clone /tmp/c \
                        --clone-manifest /tmp/m --workers 2)" "C1 pins one worker")"

S2ARGS_C2=(--c2-cycle --python-binary /usr/bin/python3 --package-clone /tmp/clone
           --clone-manifest /tmp/manifest)
check "a non-numeric --workers is refused" "2" "$(code "${S2ARGS_C2[@]}" --workers two)"
check "--workers 0 is refused" "2" "$(code "${S2ARGS_C2[@]}" --workers 0)"
check "--workers 99 is refused" "2" "$(code "${S2ARGS_C2[@]}" --workers 99)"
check "--workers 2 is accepted past validation" "3" "$(code "${S2ARGS_C2[@]}" --workers 2)"

check "a label with a space is refused" "2" "$(code --label 'two words')"
check "a label with a slash is refused" "2" "$(code --label a/b)"
check "and says it becomes a filename" "yes" \
      "$(has_text "$(run --label 'a b')" "becomes part of a filename")"
check "an ordinary label is accepted" "4" "$(code --label c2_cycle1)"

echo
echo "the clone is subject to the production boundary too"
S2ARGS_C1=(--c1 --python-binary /usr/bin/python3 --clone-manifest /tmp/manifest)
check "a clone inside production is refused" "2" \
      "$(code "${S2ARGS_C1[@]}" --package-clone "$HOME/python/woa23/site-packages")"
check "and explains it would import the live tree" "yes" \
      "$(has_text "$(run "${S2ARGS_C1[@]}" --package-clone "$HOME/python/woa23/x")" \
         "is inside production")"
check "production's own site-packages as the clone is refused" "2" \
      "$(code "${S2ARGS_C1[@]}" \
              --package-clone "$HOME/.pyenv/versions/py311/lib/python3.11/site-packages")"
check "a clone inside the workdir is refused" "2" \
      "$(code "${S2ARGS_C1[@]}" --workdir "$HOME/woa23-s2-c1-work" \
              --package-clone "$HOME/woa23-s2-c1-work/clone")"
check "and says the workdir is written to" "yes" \
      "$(has_text "$(run "${S2ARGS_C1[@]}" --workdir "$HOME/woa23-s2-c1-work" \
                        --package-clone "$HOME/woa23-s2-c1-work/clone")" "must be immutable")"
check "a clone merely sharing the workdir's prefix is allowed" "3" \
      "$(code "${S2ARGS_C1[@]}" --workdir "$HOME/woa23-s2-c1-work" \
              --package-clone "$HOME/woa23-s2-c1-work-clone")"

echo
echo "each experiment has its own grant, and none implies another"
S2ARGS_OK=(--python-binary /usr/bin/python3 --package-clone "$HOME/woa23-s2-package-clone/dist"
           --clone-manifest "$HOME/woa23-s2-package-clone/SHA256SUMS")
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
      "$(has_text "$s2out" "mode      : C1 (S2: production binary + package clone, 5.2A byte-exact)")"
check "the binary is printed" "yes" "$(has_text "$s2out" "binary    : /usr/bin/python3")"
check "the clone is printed" "yes" \
      "$(has_text "$s2out" "clone     : $HOME/woa23-s2-package-clone/dist")"
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
      "$(has_text "$s2out2" "workers   : <read from production at run time>")"
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
echo "nothing above started a process or bound a port"
check "no candidate/reference/scheduler process was left behind" "0" \
      "$(pgrep -f 'gunicorn (api.app|woa23_app):app' 2>/dev/null | wc -l | tr -d ' ')"
check "no staging workdir was created" "no" \
      "$([ -e "$HOME/woa23-s1-staging-merged" ] && echo yes || echo no)"

echo
if [ "$fail" -gt 0 ]; then
  echo "FAILED $fail/$((pass + fail))"
  exit 1
fi
echo "all passed ($pass assertions)"
