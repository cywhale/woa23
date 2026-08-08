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
