#!/usr/bin/env bash
#
# D2b — the controlled two-arm comparison, variant 5.2A.
#
# The 2026-08-07 campaign compared the candidate against *live production*. That was
# the only thing D2a allowed, and it left two variables uncontrolled: production's
# hash seed could not be pinned, and its environment differed from the candidate's in
# 23 transitive packages. This script removes both by running an unmodified
# `woa23_app.py` as the reference, in isolation, out of the same environment as the
# candidate.
#
#   WOA23_D2B_GRANTED=yes ./scripts/run_controlled.sh
#
# Four services, six OS processes: `gunicorn -w 1` is an arbiter plus a forked
# worker, so each API arm is two. The Dask worker runs with --no-nanny, which is one
# rather than the two the default supervisor would make it.
#
# Requires D2b authorisation, which is separate from D2a and is not implied by it.
# Nothing here touches production: not the process on 8050, not its configuration,
# not the shared Dask scheduler on 8786, not ~/python/woa23. It does consume VM24's
# CPU, RAM, page cache and Zarr read I/O — see D2b-request.md, "What this costs".
# "Does not modify production" is not "does not affect the host".

set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

EXPECT_HOST=odb24
EXPECT_PY=3.11.4
PROD_DIR=$HOME/python/woa23
PROD_PY=$HOME/.pyenv/versions/py311/bin/python3.11
STORE=$PROD_DIR/data
# Production's live site-packages. Never read by an arm and never on any import
# path this script builds — it is here only so it can be named as forbidden. The
# S2 arms must not reach it, and `__editable__.src-1.0.pth` inside it points at
# $PROD_DIR/src, so an arm that found this directory would import production's
# live source as well as its packages.
PROD_SITE=$HOME/.pyenv/versions/py311/lib/python3.11/site-packages
# Defaults. Every one of these can be overridden on the command line, because a
# staging run has to be able to sit beside the evidence of previous runs rather than
# on top of it — but the defaults stay pointed at the last authorised D2b
# configuration so an argument-free invocation does not silently mean something new.
WORK=$HOME/woa23-s1-controlled-r6
CAND_PORT=8051
REF_PORT=8052
SCHED_PORT=18787           # 8787 is NOT free: it is production's own scheduler
                           # dashboard, same PID as 8786. See D2b-request.md §4.
PROD_PORT=8050
# Ports this script must never bind, whatever it is told. 8050 is production's API,
# 8786 its shared Dask scheduler, 8787 that scheduler's dashboard. Preflight would
# refuse a held port anyway, but a typo that aims at production deserves a refusal
# that names the reason rather than one that says "already in use".
FORBIDDEN_PORTS="8050 8786 8787"

# Modes. Exactly one may be selected.
#
#   default          contract gate, then latency gate, then the sample-size pilot
#   --contract-only  contract gate, then stop. No latency, no pilot, no rung
#                    escalation — so it produces no timing of any kind and nothing
#                    may be quoted from it as performance.
#   --cleanup-only   neither gate. Brings the processes up, records and verifies
#                    their trees, collects provenance, stops. For reproducing a
#                    cleanup failure.
#
# and two S2 modes, which are a different experiment entirely. D2b asks whether the
# candidate matches the reference in one environment this campaign built. C1 and C2
# ask whether that still holds in *production's* environment — production's Python
# binary, a read-only clone of production's package tree, and no `dev2026/.venv`
# anywhere on the arms' import path.
#
#   --c1             production binary + package clone, PYTHONHASHSEED=0, 5.2A
#                    byte-exact over 64 cases. One cycle. No latency, no pilot.
#   --c2-cycle       one cycle of C2: the same clone, production's own worker count
#                    read at run time, and **no** PYTHONHASHSEED, compared 5.2B
#                    semantically. Three independent cycles make a C2 result, and
#                    scripts/run_c2_cycles.sh is what runs them — this flag is one
#                    cycle and never draws the conclusion.
CLEANUP_ONLY=no
CONTRACT_ONLY=no
C1_MODE=no
C2_CYCLE=no
PY_BINARY=""
PKG_CLONE=""
CLONE_MANIFEST=""
WORKERS=""
LABEL=""

usage() {
  cat >&2 <<'USAGE'
usage: run_controlled.sh [--contract-only | --cleanup-only | --c1 | --c2-cycle]
                         [--workdir PATH]
                         [--candidate-port N] [--reference-port N]
                         [--scheduler-port N]
                         [--python-binary PATH] [--package-clone PATH]
                         [--clone-manifest PATH] [--workers N] [--label TAG]

  --contract-only   run the 5.2A contract gate and stop. Skips the latency gate,
                    the noise pilot and any rung escalation.
  --cleanup-only    start the services, verify their trees, stop. No gates.
  --c1              S2 C1: production binary + package clone, PYTHONHASHSEED=0,
                    5.2A byte-exact, 64 cases. No latency, no pilot.
  --c2-cycle        S2 C2, ONE cycle: same clone, production's measured worker
                    count, no PYTHONHASHSEED, 5.2B semantic. Three cycles make a
                    result; run scripts/run_c2_cycles.sh, not this flag directly.
  --workdir PATH    staging root. Must not already exist.
  --*-port N        loopback port. 8050, 8786 and 8787 are refused.

 S2 modes only (and refused outside them):
  --python-binary PATH   production's interpreter. No default, and no fallback to
                         dev2026/.venv: an S2 run that quietly used the campaign's
                         own venv would answer a question nobody asked.
  --package-clone PATH   root of the read-only production package-tree clone.
  --clone-manifest PATH  the manifest written when that clone was built and
                         verified. Its digest anchors the environment record.
  --expected-workers N   ASSERT production's worker count. This does NOT set the
                         arms' worker count — that is read from production's own
                         argv at run time. If the two disagree the run aborts
                         before any arm starts. C2 only.
  --label TAG            prefixes this invocation's result files. C2 cycles need
                         it so three cycles do not overwrite each other.

 Authorisation, by mode. Each is separate and none implies another:
  default/--contract-only/--cleanup-only   WOA23_D2B_GRANTED=yes
  --c1                                     WOA23_S2_C1_GRANTED=yes
  --c2-cycle                               WOA23_S2_C2_GRANTED=yes
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --contract-only) CONTRACT_ONLY=yes; shift ;;
    --cleanup-only)  CLEANUP_ONLY=yes; shift ;;
    --c1)            C1_MODE=yes; shift ;;
    --c2-cycle)      C2_CYCLE=yes; shift ;;
    --workdir)         [ $# -ge 2 ] || { echo "--workdir needs a value" >&2; exit 2; }
                       WORK="$2"; shift 2 ;;
    --candidate-port)  [ $# -ge 2 ] || { echo "--candidate-port needs a value" >&2; exit 2; }
                       CAND_PORT="$2"; shift 2 ;;
    --reference-port)  [ $# -ge 2 ] || { echo "--reference-port needs a value" >&2; exit 2; }
                       REF_PORT="$2"; shift 2 ;;
    --scheduler-port)  [ $# -ge 2 ] || { echo "--scheduler-port needs a value" >&2; exit 2; }
                       SCHED_PORT="$2"; shift 2 ;;
    --python-binary)   [ $# -ge 2 ] || { echo "--python-binary needs a value" >&2; exit 2; }
                       PY_BINARY="$2"; shift 2 ;;
    --package-clone)   [ $# -ge 2 ] || { echo "--package-clone needs a value" >&2; exit 2; }
                       PKG_CLONE="$2"; shift 2 ;;
    --clone-manifest)  [ $# -ge 2 ] || { echo "--clone-manifest needs a value" >&2; exit 2; }
                       CLONE_MANIFEST="$2"; shift 2 ;;
    --expected-workers) [ $# -ge 2 ] || { echo "--expected-workers needs a value" >&2; exit 2; }
                       WORKERS="$2"; shift 2 ;;
    --workers)         echo "--workers was renamed --expected-workers. It never set" >&2
                       echo "  the arms' worker count: that is read from production at" >&2
                       echo "  run time and cannot be chosen. The old name invited the" >&2
                       echo "  opposite reading, so it is refused rather than aliased." >&2
                       exit 2 ;;
    --label)           [ $# -ge 2 ] || { echo "--label needs a value" >&2; exit 2; }
                       LABEL="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

# ------------------------------------------------------------ mode selection ---
# Exactly one mode, checked by counting rather than by a chain of pairwise tests:
# four flags make six pairs, and the version of this that enumerated them missed
# three of the six.
selected=""
[ "$CONTRACT_ONLY" = yes ] && selected="$selected --contract-only"
[ "$CLEANUP_ONLY"  = yes ] && selected="$selected --cleanup-only"
[ "$C1_MODE"       = yes ] && selected="$selected --c1"
[ "$C2_CYCLE"      = yes ] && selected="$selected --c2-cycle"
n_modes=$(printf '%s' "$selected" | wc -w | tr -d ' ')
if [ "$n_modes" -gt 1 ]; then
  echo "the modes are mutually exclusive; got:$selected" >&2
  echo "  Each runs a different set of gates against a different environment, so" >&2
  echo "  combining them would produce a result belonging to neither." >&2
  exit 2
fi

S2_MODE=none
[ "$C1_MODE" = yes ] && S2_MODE=c1
[ "$C2_CYCLE" = yes ] && S2_MODE=c2

# The S2 arguments exist only for the S2 modes. Accepting them elsewhere would
# silently ignore them, and "the flag was accepted" reads as "the flag took
# effect" to everyone including the person who wrote it.
if [ "$S2_MODE" = none ]; then
  for pair in "--python-binary:$PY_BINARY" "--package-clone:$PKG_CLONE" \
              "--clone-manifest:$CLONE_MANIFEST" "--expected-workers:$WORKERS"; do
    [ -n "${pair#*:}" ] || continue
    echo "${pair%%:*} means nothing without --c1 or --c2-cycle, and this run would" >&2
    echo "  have ignored it. Refusing rather than accepting a flag that has no effect." >&2
    exit 2
  done
else
  # No default and no fallback. dev2026/.venv is this campaign's own environment;
  # an S2 run that reached for it because a flag was missing would produce a D2b
  # result wearing a C1 label, and it would pass.
  [ -n "$PY_BINARY" ] || {
    echo "$S2_MODE requires --python-binary: the arms run production's interpreter," >&2
    echo "  and there is deliberately no fallback to dev2026/.venv." >&2
    exit 2; }
  [ -n "$PKG_CLONE" ] || {
    echo "$S2_MODE requires --package-clone: the arms import from the read-only" >&2
    echo "  production package-tree clone, and there is deliberately no fallback to" >&2
    echo "  dev2026/.venv." >&2
    exit 2; }
  [ -n "$CLONE_MANIFEST" ] || {
    echo "$S2_MODE requires --clone-manifest: without the manifest digest nothing" >&2
    echo "  distinguishes the verified clone from a directory with the right name." >&2
    exit 2; }
  for pair in "--python-binary:$PY_BINARY" "--package-clone:$PKG_CLONE" \
              "--clone-manifest:$CLONE_MANIFEST"; do
    case "${pair#*:}" in
      /*) ;;
      *) echo "${pair%%:*} must be an absolute path; got '${pair#*:}'. A relative" >&2
         echo "  path would resolve against whichever directory this was invoked from." >&2
         exit 2 ;;
    esac
  done
fi
if [ "$C1_MODE" = yes ] && [ -n "$WORKERS" ]; then
  echo "--expected-workers is C2 only. C1 pins one worker per arm so that a byte-exact" >&2
  echo "  comparison has one process producing each side's bytes." >&2
  exit 2
fi
if [ -n "$WORKERS" ]; then
  case "$WORKERS" in
    ''|*[!0-9]*) echo "--expected-workers '$WORKERS' is not a number" >&2; exit 2 ;;
  esac
  if [ "$WORKERS" -lt 1 ] || [ "$WORKERS" -gt 16 ]; then
    echo "--expected-workers $WORKERS is outside 1-16" >&2; exit 2
  fi
fi
if [ -n "$LABEL" ]; then
  case "$LABEL" in
    *[!A-Za-z0-9_-]*|"")
      echo "--label '$LABEL' may contain only letters, digits, '-' and '_': it" >&2
      echo "  becomes part of a filename." >&2
      exit 2 ;;
  esac
fi
[ -n "$LABEL" ] || LABEL=$([ "$S2_MODE" = none ] && echo d2b || echo "$S2_MODE")

# Validate the ports before anything else looks at the host. A port that is not a
# port, or is production's, is a configuration error and not something to discover
# halfway through preflight.
for spec in "candidate:$CAND_PORT" "reference:$REF_PORT" "scheduler:$SCHED_PORT"; do
  name="${spec%%:*}"; val="${spec#*:}"
  case "$val" in
    ''|*[!0-9]*) echo "$name port '$val' is not a number" >&2; exit 2 ;;
  esac
  if [ "$val" -lt 1024 ] || [ "$val" -gt 65535 ]; then
    echo "$name port $val is outside 1024-65535" >&2; exit 2
  fi
  for bad in $FORBIDDEN_PORTS; do
    [ "$val" = "$bad" ] || continue
    echo "$name port $val belongs to production and will never be bound by this" >&2
    echo "  script: 8050 is its API, 8786 its shared Dask scheduler, 8787 that" >&2
    echo "  scheduler's dashboard." >&2
    exit 2
  done
done
if [ "$CAND_PORT" = "$REF_PORT" ] || [ "$CAND_PORT" = "$SCHED_PORT" ] \
   || [ "$REF_PORT" = "$SCHED_PORT" ]; then
  echo "the three ports must differ (candidate $CAND_PORT, reference $REF_PORT," >&2
  echo "  scheduler $SCHED_PORT)" >&2
  exit 2
fi

# The staging root must not be production, nor inside it.
#
# Normalised lexically, in shell, rather than with `realpath -m`: that flag is GNU
# coreutils only. VM24 has it; the machine the offline tests run on does not, and a
# check that cannot be exercised where it is written is not much of a check. The path
# normally does not exist yet, so nothing here may touch the filesystem.
_abspath() {                # lexical absolute path; the target need not exist
  local p="$1" out="" part oldIFS="$IFS"
  case "$p" in /*) ;; *) p="$PWD/$p" ;; esac
  IFS=/
  for part in $p; do
    case "$part" in
      ''|.) ;;
      ..)   out="${out%/*}" ;;
      *)    out="$out/$part" ;;
    esac
  done
  IFS="$oldIFS"
  printf '%s' "${out:-/}"
}
# Lexical normalisation alone is not a boundary check: it cannot see a symlink.
# `--workdir /tmp/link/new-run`, where `/tmp/link` points at ~/python/woa23, is
# lexically nowhere near production and physically inside it.
#
# So the deepest ancestor that actually exists is resolved physically — `cd -P`
# plus `pwd -P`, which is POSIX and needs no GNU realpath — and the components
# that do not exist yet are appended to that. Both sides are resolved the same
# way, so a symlinked production directory is caught as well as a symlinked
# workdir.
#
# Fails closed. A path whose ancestor is a symlink that does not resolve to a
# directory — dangling, or pointing at a file — cannot be shown to be outside
# production, so it is refused rather than assumed safe.
_resolve_existing_parent() {    # physical path; the leaf need not exist
  local p tail="" base phys
  p="$(_abspath "$1")"
  while [ ! -d "$p" ]; do
    if [ -L "$p" ]; then
      return 2                  # a symlink we cannot follow to a directory
    fi
    [ "$p" = "/" ] && break
    base="${p##*/}"
    tail="${base}${tail:+/$tail}"
    p="${p%/*}"
    [ -z "$p" ] && p=/
  done
  phys="$(cd -P "$p" 2>/dev/null && pwd -P)" || return 1
  printf '%s' "$phys${tail:+/$tail}"
}

WORK_ABS="$(_resolve_existing_parent "$WORK")" || {
  echo "cannot resolve $WORK to a physical path: its nearest existing ancestor is" >&2
  echo "  a symlink that does not lead to a directory, so it cannot be shown to be" >&2
  echo "  outside production. Refusing." >&2
  exit 2; }
PROD_ABS="$(_resolve_existing_parent "$PROD_DIR")" || {
  echo "cannot resolve the production directory $PROD_DIR to a physical path" >&2
  exit 2; }
case "$WORK_ABS" in
  "$PROD_ABS"|"$PROD_ABS"/*)
    echo "workdir $WORK_ABS is inside production ($PROD_ABS). This script never" >&2
    echo "  writes there." >&2
    exit 2 ;;
esac
WORK="$WORK_ABS"
REF_DIR=$WORK/reference
CAND_DIR=$WORK/candidate

# The clone is subject to the same boundary as the workdir, resolved the same way.
# A `--package-clone` that resolved into production would put production's live
# site-packages on the arms' PYTHONPATH — with `__editable__.src-1.0.pth` inside it
# pointing at $PROD_DIR/src — which is precisely the arrangement C1 exists to avoid,
# and it would be reported as isolation.
if [ "$S2_MODE" != none ]; then
  CLONE_ABS="$(_resolve_existing_parent "$PKG_CLONE")" || {
    echo "cannot resolve --package-clone $PKG_CLONE to a physical path" >&2
    exit 2; }
  PROD_SITE_ABS="$(_resolve_existing_parent "$PROD_SITE")" || PROD_SITE_ABS="$PROD_SITE"
  for bad in "$PROD_ABS" "$PROD_SITE_ABS"; do
    case "$CLONE_ABS" in
      "$bad"|"$bad"/*)
        echo "--package-clone $CLONE_ABS is inside production ($bad). The clone is" >&2
        echo "  a copy of production's packages, never production's own directory:" >&2
        echo "  pointing at the original would make every arm import from the live" >&2
        echo "  tree and report it as isolated." >&2
        exit 2 ;;
    esac
  done
  case "$CLONE_ABS" in
    "$WORK"|"$WORK"/*)
      echo "--package-clone $CLONE_ABS is inside the workdir $WORK, which this run" >&2
      echo "  creates and writes to. The clone must be immutable and outside it." >&2
      exit 2 ;;
  esac
  PKG_CLONE="$CLONE_ABS"

  # The three artefacts are checked here — with the arguments, not with the host
  # prerequisites — because each one describes something named on the command line
  # and a wrong name is a configuration error. Putting them after the host gate had
  # a second cost: on any machine that is not VM24 the wrong-host exit came first,
  # so none of these refusals could be exercised offline at all. They are read-only
  # stats; nothing is created and nothing is started.
  [ -e "$PY_BINARY" ] || {
    echo "--python-binary $PY_BINARY does not exist" >&2; exit 2; }
  [ -x "$PY_BINARY" ] && [ -f "$PY_BINARY" ] || {
    echo "--python-binary $PY_BINARY is not an executable file" >&2; exit 2; }
  [ -e "$PKG_CLONE" ] || {
    echo "--package-clone $PKG_CLONE does not exist" >&2; exit 2; }
  [ -d "$PKG_CLONE" ] || {
    echo "--package-clone $PKG_CLONE is not a directory" >&2; exit 2; }
  [ -f "$CLONE_MANIFEST" ] || {
    echo "--clone-manifest $CLONE_MANIFEST is not a readable file" >&2; exit 2; }
  # The clone is meant to be immutable. If this run can write to it, it is not the
  # artefact that was built and verified — and a stray .pyc would change it.
  #
  # This tests the clone's own inode and that is all it tests. It is NOT sufficient
  # for the immutability claim: unlinking a file needs write permission on its
  # DIRECTORY, so a writable parent lets the whole tree be renamed and replaced under
  # the same path while every mode inside it stays 555/444. The ancestor chain and
  # the manifest are checked by bench.clone_integrity, below and again before each
  # arm starts.
  if [ -w "$PKG_CLONE" ]; then
    echo "--package-clone $PKG_CLONE is writable by this user. The clone is meant to" >&2
    echo "  be read-only; a writable one may already have been modified, and this run" >&2
    echo "  could modify it further." >&2
    exit 2
  fi
fi

# The repository this script lives in — the source of the candidate's api/, the venv,
# and the run-state directory. Distinct from $WORK, which is the staging root.
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Per label, so three C2 cycles cannot overwrite one another. Every per-service file
# — pid, starttime, tree, uncertain, diag and crucially the service LOGS — is named
# `<service>.<ext>` inside this directory, so with a shared directory cycle 2 would
# silently replace cycle 1's evidence. The logs are the part that survives a
# successful cleanup, and therefore the part that would have been lost.
RUN=$HERE/run/$LABEL
VENV=$HERE/.venv

# Printed before the authorisation and host gates, not after: a refused invocation
# should still record what it was asked to do, and reading the resolved values back
# is how a wrong flag gets noticed.
case "$S2_MODE" in
  c1) MODE_NAME="C1 (S2: production binary + package clone, 5.2A byte-exact)" ;;
  c2) MODE_NAME="C2 cycle (S2: production binary + package clone, 5.2B semantic)" ;;
  *)  MODE_NAME="$([ "$CLEANUP_ONLY" = yes ] && echo cleanup-only \
                   || { [ "$CONTRACT_ONLY" = yes ] && echo contract-only \
                        || echo "full (contract + latency + pilot)"; })" ;;
esac
echo "== configuration for this invocation =="
echo "   mode      : $MODE_NAME"
echo "   workdir   : $WORK"
echo "   candidate : 127.0.0.1:$CAND_PORT"
echo "   reference : 127.0.0.1:$REF_PORT"
echo "   scheduler : 127.0.0.1:$SCHED_PORT"
echo "   repository: $HERE"
echo "   label     : $LABEL"
if [ "$S2_MODE" != none ]; then
  echo "   binary    : $PY_BINARY"
  echo "   clone     : $PKG_CLONE"
  echo "   manifest  : $CLONE_MANIFEST"
  echo "   workers   : read from production at run time"
  echo "               expected (asserted): ${WORKERS:-<none asserted>}"
  echo "   seed      : $([ "$S2_MODE" = c1 ] && echo "PYTHONHASHSEED=0 (pinned)" \
                          || echo "unset — this is what C2 observes")"
  echo "   arms run from the clone. dev2026/.venv is the harness's environment and"
  echo "   is not on any arm's import path."
fi

# ------------------------------------------------------------ request budget ---
# Stated before anything is sent, and stated as a ceiling rather than an estimate.
# Every number below is the maximum this invocation can issue: process readiness
# gives up at 30 attempts per arm, the data probe is exactly two per arm, and the
# contract gate is one request per case per arm.
BUDGET_READY=30            # per arm, worst case; normally 1-3
BUDGET_PROBE=2             # per arm, counterbalanced
BUDGET_CONTRACT=64         # per arm, one per case
case "$S2_MODE:$CLEANUP_ONLY:$CONTRACT_ONLY" in
  c1:*|c2:*)        BUDGET_ARM=$((BUDGET_READY + BUDGET_PROBE + BUDGET_CONTRACT)) ;;
  none:yes:*)       BUDGET_ARM=$BUDGET_READY ;;
  none:*:yes)       BUDGET_ARM=$((BUDGET_READY + BUDGET_PROBE + BUDGET_CONTRACT)) ;;
  *)                BUDGET_ARM=480 ;;      # plus the latency gate and the pilot
esac
echo "== request budget (ceilings, not estimates) =="
echo "   per arm  : <= $BUDGET_ARM  (readiness <= $BUDGET_READY, probe $BUDGET_PROBE,"
echo "              contract $([ "$CLEANUP_ONLY" = yes ] && echo 0 || echo "$BUDGET_CONTRACT")\
$([ "$S2_MODE" = none ] && [ "$CLEANUP_ONLY" = no ] && [ "$CONTRACT_ONLY" = no ] \
  && echo ", latency + pilot" || echo ""))"
echo "   total    : <= $((BUDGET_ARM * 2)) across both arms"
echo "   production 127.0.0.1:$PROD_PORT : 0 requests. Nothing in this script"
echo "              addresses it; it is read from /proc and ss only."
if [ "$S2_MODE" = c2 ]; then
  echo "   one cycle. Three cycles make a C2 result: <= $((BUDGET_ARM * 3)) per arm,"
  echo "              <= $((BUDGET_ARM * 6)) in total, and three full start/stop"
  echo "              cleanups — one per cycle, each verified before the next starts."
fi

# ------------------------------------------------------------- authorisation ---
# One grant per experiment, and no grant implies another. D2b authorised six
# processes in an environment this campaign built and can rebuild; C1 and C2 run
# production's own interpreter against a copy of production's packages, which is a
# different set of risks and was granted, if at all, in a different message.
#
# Everything above this point is argument handling, and everything below it starts
# something: nothing has been created, no port has been touched and no request has
# been sent when this gate is reached.
case "$S2_MODE" in
  c1) GRANT_VAR=WOA23_S2_C1_GRANTED; GRANT_VAL="${WOA23_S2_C1_GRANTED:-}" ;;
  c2) GRANT_VAR=WOA23_S2_C2_GRANTED; GRANT_VAL="${WOA23_S2_C2_GRANTED:-}" ;;
  *)  GRANT_VAR=WOA23_D2B_GRANTED;   GRANT_VAL="${WOA23_D2B_GRANTED:-}" ;;
esac
if [ "$GRANT_VAL" != "yes" ]; then
  if [ "$S2_MODE" = none ]; then
    echo "D2b authorisation not stated. This starts FOUR services on a production" >&2
    echo "host — a Dask scheduler, a Dask worker, an unmodified reference API and the" >&2
    echo "candidate API — which is SIX OS processes, because each of the two APIs is" >&2
    echo "a gunicorn arbiter plus the worker it forks." >&2
    echo "Re-run with WOA23_D2B_GRANTED=yes once it is granted. D2a does not imply D2b." >&2
  else
    echo "S2 $S2_MODE authorisation not stated. This starts four services from" >&2
    echo "production's own Python binary against a read-only clone of production's" >&2
    echo "package tree — a different experiment from D2b, on a different environment." >&2
    if [ "${WOA23_D2B_GRANTED:-}" = "yes" ]; then
      echo "WOA23_D2B_GRANTED is set and does NOT authorise this: it was granted for" >&2
      echo "  the controlled venv, not for production's interpreter and packages." >&2
    fi
    echo "Re-run with $GRANT_VAR=yes once that specific authorisation is granted." >&2
  fi
  exit 3
fi
# The converse, so a stray export cannot widen what was granted: an S2 grant does
# not authorise a D2b run either.
if [ "$S2_MODE" = none ]; then
  for v in WOA23_S2_C1_GRANTED WOA23_S2_C2_GRANTED; do
    eval "set_val=\${$v:-}"
    [ "$set_val" = "yes" ] || continue
    echo "$v is set but this is a D2b mode, which it does not authorise." >&2
    echo "  Select the mode the grant was issued for, or unset the variable." >&2
    exit 3
  done
fi
if [ "$S2_MODE" = c1 ] && [ "${WOA23_S2_C2_GRANTED:-}" = "yes" ]; then
  echo "WOA23_S2_C2_GRANTED is set during a C1 run. C1 and C2 are separately" >&2
  echo "  authorised; unset the one this run is not." >&2
  exit 3
fi
if [ "$S2_MODE" = c2 ] && [ "${WOA23_S2_C1_GRANTED:-}" = "yes" ]; then
  echo "WOA23_S2_C1_GRANTED is set during a C2 cycle. C1 and C2 are separately" >&2
  echo "  authorised; unset the one this run is not." >&2
  exit 3
fi

if [ "$(hostname -s)" != "$EXPECT_HOST" ]; then
  echo "this runs on $EXPECT_HOST only; hostname is $(hostname -s)" >&2
  exit 4
fi
[ -d "$STORE" ] || { echo "store $STORE not found" >&2; exit 4; }
env -C / true 2>/dev/null || { echo "env -C is required (coreutils >= 8.28)" >&2; exit 4; }

cd "$HERE"
mkdir -p "$RUN" results


# Port-state helpers. Shared with run_candidate.sh and covered offline by
# scripts/test_ports.sh, which exercises them against a captured `ss` fixture under
# these same shell options.
# shellcheck source=lib_ports.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib_ports.sh"
# Process identity and process-tree tracking. Every service started here is more
# than one OS process, so cleanup is verified against a recorded tree rather than a
# single PID. Covered offline by scripts/test_procs.sh.
# shellcheck source=lib_procs.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib_procs.sh"

# ============================================================== environment ===
# Built and verified before any process starts. A run that discovers its
# environment is wrong after the servers are up has already perturbed the host for
# nothing.
echo "== preparing the shared environment =="
[ -x "$PROD_PY" ] || { echo "production interpreter $PROD_PY not found" >&2; exit 1; }
prod_py_version="$("$PROD_PY" --version 2>&1 | awk '{print $2}')"
[ "$prod_py_version" = "$EXPECT_PY" ] || {
  echo "production interpreter is $prod_py_version, expected $EXPECT_PY" >&2; exit 1; }

# --locked, not --frozen: it fails if uv.lock does not match pyproject.toml, rather
# than quietly installing from a lock that has drifted from its inputs.
#
# Under the S2 modes this venv is the *harness's* environment and nothing else. It
# runs contract_diff, the provenance collectors and the checks; it is not on any
# arm's import path and no package in it can reach an arm. The harness only issues
# HTTP and compares bytes, so its own package set cannot change what an arm returns.
# It is still built and pinned, because a harness that cannot run is a run that
# produces nothing.
#
# **Two environments, recorded separately and never merged.** The authorisation to
# run `uv sync` covers this one directory and nothing else:
#
#   harness bootstrap   dev2026/.venv, created or synced here by uv from uv.lock.
#                       Runs contract_diff, the collectors and the checks. NOT on
#                       any arm's import path.
#   arm environment     under S2 the read-only package clone; under D2b the same
#                       venv. This is what is under test, and uv never touches it —
#                       the clone stays immutable and production is never written.
#
# They go to different artefacts, `<label>_harness_bootstrap.json` and
# `<label>_environment.json`, so the digest of one cannot be read as the other's.
echo "== harness bootstrap: dev2026/.venv — NOT the environment under test =="
uv sync --locked --python "$PROD_PY" >&2

if [ "$S2_MODE" = none ]; then
  ARM_ENV_KIND="dev2026/.venv (the same environment as the harness)"
else
  ARM_ENV_KIND="the read-only production package clone at $PKG_CLONE"
fi
VENV_PY="$VENV/bin/python" LABEL="$LABEL" ARM_ENV_KIND="$ARM_ENV_KIND" \
uv run python - <<'PYEOF' || exit 1
import json, os, sys
from pathlib import Path
sys.path.insert(0, ".")
from bench.collect_backend_meta import dependencies

label = os.environ["LABEL"]
deps = dependencies(os.environ["VENV_PY"], Path("uv.lock"))
if "distributions_error" in deps:
    print(f"the harness venv is unusable: {deps['distributions_error']}",
          file=sys.stderr)
    raise SystemExit(1)
json.dump({
    "kind": "harness_bootstrap",
    "what_this_is": ("the environment the measuring harness runs in, created by "
                     "'uv sync --locked'. NOT the environment under test, and "
                     "not "
                     "on any arm's import path."),
    "arm_environment_is": os.environ["ARM_ENV_KIND"],
    "uv_authorisation_scope": ("uv may create or sync dev2026/.venv only. It never "
                               "installs into the package clone or into production "
                               "site-packages; both remain immutable."),
    "env_python": os.environ["VENV_PY"],
    "python_version": deps["python_version"],
    "lockfile_sha256": deps["lockfile_sha256"],
    "name_version_set_sha256": deps["name_version_set_sha256"],
    "n_distributions": len(deps["distributions"]),
    "distributions": deps["distributions"],
}, open(f"results/{label}_harness_bootstrap.json", "w"), indent=2)
print(f"  harness venv: python {deps['python_version']}, "
      f"{len(deps['distributions'])} distributions, "
      f"lock {deps['lockfile_sha256'][:16]}")
print(f"  environment under test (separate): {os.environ['ARM_ENV_KIND']}")
PYEOF

if [ "$S2_MODE" = none ]; then
VENV_PY="$VENV/bin/python" WANT_PY="$EXPECT_PY" LABEL="$LABEL" \
uv run python - <<'PYEOF' || exit 1
import hashlib, json, os, subprocess, sys
sys.path.insert(0, ".")
from bench.collect_backend_meta import dependencies

venv_py = os.environ["VENV_PY"]
want_py = os.environ["WANT_PY"]
label = os.environ["LABEL"]

deps = dependencies(venv_py, __import__("pathlib").Path("uv.lock"))
if "distributions_error" in deps:
    print(f"cannot list the venv's distributions: {deps['distributions_error']}",
          file=sys.stderr)
    raise SystemExit(1)
if deps["python_version"] != want_py:
    print(f"venv interpreter is {deps['python_version']}, expected {want_py} — the "
          f"benchmark would not be measuring production's runtime", file=sys.stderr)
    raise SystemExit(1)

json.dump({"kind": "controlled_environment",
           "python_version": deps["python_version"],
           "env_python": venv_py,
           "lockfile_sha256": deps["lockfile_sha256"],
           "name_version_set_sha256": deps["name_version_set_sha256"],
           "name_version_set_canonicalization": deps["name_version_set_canonicalization"],
           "n_distributions": len(deps["distributions"]),
           "distributions": deps["distributions"]},
          open(f"results/{label}_environment.json", "w"), indent=2)
print(f"  python {deps['python_version']}  "
      f"{len(deps['distributions'])} distributions  "
      f"lock {deps['lockfile_sha256'][:16]}  "
      f"name==version set {deps['name_version_set_sha256'][:16]}")
PYEOF
else
# The S2 environment record describes the *clone*, not the venv, and it is produced
# by listing the clone exactly the way an arm will be started: production's binary,
# `-S`, the clone on PYTHONPATH, no VIRTUAL_ENV and no PYTHONHOME. Listing it any
# other way — including simply running the binary — reports production's own 236
# distributions, which is the environment this run exists to stay out of.
PY_BINARY="$PY_BINARY" PKG_CLONE="$PKG_CLONE" CLONE_MANIFEST="$CLONE_MANIFEST" \
WANT_PY="$EXPECT_PY" S2_MODE="$S2_MODE" LABEL="$LABEL" \
uv run python - <<'PYEOF' || exit 1
import json, os, sys
from pathlib import Path
sys.path.insert(0, ".")
from bench.collect_backend_meta import dependencies
from bench.package_digests import CANONICALIZATION
from bench.s2_provenance import launch_env

binary = os.environ["PY_BINARY"]
clone = os.environ["PKG_CLONE"]
manifest = Path(os.environ["CLONE_MANIFEST"])
want_py = os.environ["WANT_PY"]
label = os.environ["LABEL"]

# hashseed=None here regardless of mode: this listing is importlib.metadata, whose
# answer does not depend on the hash seed, and pinning it would suggest it did.
env = launch_env(clone, hashseed=None)
deps = dependencies(binary, None, interp_args=("-S",), env=env,
                    clone_manifest=manifest, clone_root=clone)
if "distributions_error" in deps:
    print(f"cannot list the clone's distributions: {deps['distributions_error']}",
          file=sys.stderr)
    raise SystemExit(1)
if "clone_manifest_error" in deps:
    print(f"clone manifest: {deps['clone_manifest_error']}", file=sys.stderr)
    raise SystemExit(1)
if "clone_digest_error" in deps:
    print(f"clone dist-info digests: {deps['clone_digest_error']}", file=sys.stderr)
    raise SystemExit(1)
if deps["python_version"] != want_py:
    print(f"the clone's interpreter reports {deps['python_version']}, expected "
          f"{want_py}", file=sys.stderr)
    raise SystemExit(1)
if not deps["distributions"]:
    print("the clone listed no distributions at all. A clone that imports nothing "
          "would let both arms fail identically and be recorded as agreement.",
          file=sys.stderr)
    raise SystemExit(1)

json.dump({"kind": "s2_package_clone_environment",
           "mode": os.environ["S2_MODE"],
           "python_version": deps["python_version"],
           "env_python": binary,
           "package_clone": clone,
           # All three digests over the same tree, each with what it hashes, so
           # none can be read as another. name_version_set_sha256 is the SUPERSEDED
           # rev 1-5 canonicalization and is kept because it is the only one an
           # arm's own interpreter can compute.
           "clone_manifest": deps["clone_manifest"],
           "clone_manifest_sha256": deps["clone_manifest_sha256"],
           "package_tree_digest": deps["package_tree_digest"],
           "runtime_distribution_digest": deps["runtime_distribution_digest"],
           "n_dist_info_directories": deps["n_dist_info_directories"],
           "n_runtime_distributions": deps["n_runtime_distributions"],
           "name_version_set_sha256": deps["name_version_set_sha256"],
           "digest_canonicalization": dict(CANONICALIZATION),
           "n_distributions": len(deps["distributions"]),
           "distributions": deps["distributions"],
           "interpreter_args": deps["interpreter_args"],
           "interpreter_env": deps["interpreter_env"],
           "site_limitation": (
               "-S: site.py does not run, so no .pth in the clone is processed. "
               "This is isolated package-tree import correctness, not production's "
               "site/.pth startup semantics (spec 002 section 4.3.1).")},
          open(f"results/{label}_environment.json", "w"), indent=2)
print(f"  clone python {deps['python_version']}  "
      f"{len(deps['distributions'])} distributions")
print(f"    clone_manifest_sha256       {deps['clone_manifest_sha256']}")
print(f"    package_tree_digest         {deps['package_tree_digest']} "
      f"({deps['n_dist_info_directories']} dist-info dirs)")
print(f"    runtime_distribution_digest {deps['runtime_distribution_digest']} "
      f"({deps['n_runtime_distributions']} with METADATA)")
print(f"    name_version_set_sha256     {deps['name_version_set_sha256']} "
      f"(SUPERSEDED rev 1-5 canonicalization; not the runtime digest)")
print(f"  LIMITATION -S: site.py does not run; no .pth in the clone is processed.")
PYEOF

# ------------------------------------------------ is the clone still the clone? ---
# Mode bits answer "could this be replaced?". Only re-hashing answers "is this still
# the tree that was verified?". Both are asked, here and again immediately before
# each arm is started, and a failure at either point stops the run.
clone_integrity() {         # clone_integrity <stage>
  uv run python -m bench.clone_integrity \
    --clone "$PKG_CLONE" --manifest "$CLONE_MANIFEST" --stage "$1" \
    --out "results/${LABEL}_clone_integrity_$1.json" \
    || { echo "clone integrity failed at stage '$1'; stopping" >&2; return 1; }
}
echo "== clone integrity: ancestors and full manifest =="
clone_integrity preflight || exit 1
fi

# ================================================================ preflight ===
# Leftover state first: a free port is not an all-clear, because cleanup
# deliberately leaves its pidfile when it refuses to kill.
shopt -s nullglob
# `.uncertain` too: it is written precisely when a previous run could not record
# what it had started, which is the state that most needs a person to look.
# `.tree` as well: a stop that cannot remove its state leaves one behind, and a
# tree naming PIDs from a previous run is exactly what must not be stepped over.
# `.diag` too: it is the record of why a previous cleanup could not confirm itself,
# and it is written into the same directory the next run would write over. Evidence
# that a run can silently destroy is evidence that will be destroyed.
# The whole run/ tree, not just this label's directory. State left by ANY previous
# run or cycle is a reason to stop: per-label directories isolate evidence, and they
# would also hide a neighbouring cycle's unfinished cleanup from a check that only
# looked at its own. Logs are deliberately not in this list — they are the evidence a
# clean cleanup leaves behind.
leftovers=()
while IFS= read -r _leftover; do leftovers+=("$_leftover"); done < <(
  find "$HERE/run" -type f \( -name '*.pid' -o -name '*.starttime' -o -name '*.tree' \
       -o -name '*.uncertain' -o -name '*.diag' \) 2>/dev/null | sort)
if [ ${#leftovers[@]} -gt 0 ]; then
  echo "leftover run state from a previous invocation:" >&2
  printf '  %s\n' "${leftovers[@]}" >&2
  echo "Inspect and remove it deliberately before starting anything." >&2
  exit 1
fi
shopt -u nullglob

for port in "$CAND_PORT" "$REF_PORT" "$SCHED_PORT"; do
  st=0; port_held "$port" || st=$?
  case "$st" in
    0) echo "port ${port} is already in use — aborting rather than touching it" >&2
       ss_rows_on_port "$port" >&2 || true
       exit 1 ;;
    2) echo "cannot read port state for ${port}; refusing to start rather than" >&2
       echo "  assume it is free" >&2
       exit 1 ;;
  esac
done

BOOT_ID="$(cat /proc/sys/kernel/random/boot_id)"

# ------------------------------------------- production's identity, recorded ---
# "Someone is still listening on 8050" is not the same as "production is the process
# it was". A restart between the two checks would leave the port occupied and every
# comparison in this run describing a different backend.
prod_st=0
PROD_PIDS_BEFORE="$(pids_on_port "$PROD_PORT")" || prod_st=$?
[ "$prod_st" -eq 2 ] && { echo "cannot read port state for $PROD_PORT" >&2; exit 1; }
[ -n "$PROD_PIDS_BEFORE" ] || { echo "production is not listening on $PROD_PORT" >&2; exit 1; }
PROD_MASTER_BEFORE="$(master_of "$PROD_PIDS_BEFORE")" || {
  echo "cannot identify production's master among [$PROD_PIDS_BEFORE]" >&2; exit 1; }
PROD_START_BEFORE="$(starttime_of "$PROD_MASTER_BEFORE")"
echo "production master $PROD_MASTER_BEFORE (start $PROD_START_BEFORE), listeners: $PROD_PIDS_BEFORE"

# ------------------------------------------ production's worker count, measured ---
# C2 asks what happens under production's actual concurrency, so the number is read
# from production's own argv at run time rather than written into this script. `-w 2`
# was true when it was last looked at; a script that hard-codes it answers for the
# deployment it was written against, not the one it is running beside.
ARM_WORKERS=1
if [ "$S2_MODE" = c2 ]; then
  # Parsed from the NUL-separated bytes, in Python, behind a quoted heredoc — never
  # by turning argv into newline-delimited text and reading it line by line.
  #
  # Two separate reasons, and the first is the one that bites without looking like a
  # security problem. An argument may contain anything but NUL, so `tr '\0' '\n'`
  # turns a single argument containing a newline into two, and every position after
  # it shifts — which for a scan that pairs `-w` with the following token means the
  # WRONG token becomes the worker count, and the run then verifies itself against a
  # process set it was never authorised for. The second is that production's command
  # line is external data: embedding it in a shell or Python source text at all is a
  # quoting problem waiting for an argument with a quote in it. It crosses this
  # boundary as one integer on stdout.
  measured="$(PROD_MASTER="$PROD_MASTER_BEFORE" uv run python - <<'PYEOF'
import os, sys
sys.path.insert(0, ".")
from bench.collect_backend_meta import argv_of, worker_count

pid = int(os.environ["PROD_MASTER"])
argv, err = argv_of(pid)
if err:
    print(err, file=sys.stderr)
    raise SystemExit(1)
n, err = worker_count(argv)
if err:
    print(f"{err}; argv has {len(argv)} arguments", file=sys.stderr)
    raise SystemExit(1)
print(n)
PYEOF
  )" || {
    echo "cannot read a worker count from production's argv. Refusing to assume" >&2
    echo "  one: C2's whole question is what production's concurrency does, and a" >&2
    echo "  guessed number would answer it for a deployment that does not exist." >&2
    exit 1; }
  case "$measured" in
    ''|*[!0-9]*)
      echo "the worker count came back as '$measured', which is not a number" >&2
      exit 1 ;;
  esac
  if [ "$measured" -lt 1 ] || [ "$measured" -gt 16 ]; then
    echo "production reports $measured workers, outside the 1-16 this run will start" >&2
    exit 1
  fi
  echo "production worker count: actual=$measured (read from pid $PROD_MASTER_BEFORE\'s argv)"
  echo "                        expected=${WORKERS:-<none asserted>}"
  if [ -n "$WORKERS" ] && [ "$WORKERS" != "$measured" ]; then
    echo "expected-workers mismatch: expected $WORKERS, production is running $measured." >&2
    echo "  --expected-workers is an ASSERTION and never a setting: the arms take the" >&2
    echo "  actual number, so a disagreement means production changed or the assertion" >&2
    echo "  is stale, and either way this run would not be measuring production's" >&2
    echo "  configuration. Stopping before any arm is started." >&2
    exit 1
  fi
  ARM_WORKERS="$measured"          # the ACTUAL number, always; never $WORKERS

  # -------------------------------- production must not have moved while we read ---
  # The worker count, the master PID, its start time, the listener set and the boot
  # id all describe one process. Reading them at different moments and using them
  # together assumes production held still in between, and a restart between the
  # preflight capture and here would leave this run sized for a deployment that no
  # longer exists — with the arms not yet started, which is the last moment stopping
  # is free.
  recheck_st=0
  PROD_PIDS_RECHECK="$(pids_on_port "$PROD_PORT")" || recheck_st=$?
  [ "$recheck_st" -eq 2 ] && {
    echo "cannot re-read production's port state; refusing to continue" >&2; exit 1; }
  PROD_MASTER_RECHECK="$(master_of "$PROD_PIDS_RECHECK")" || {
    echo "cannot re-identify production's master among [$PROD_PIDS_RECHECK]" >&2; exit 1; }
  PROD_START_RECHECK="$(starttime_of "$PROD_MASTER_RECHECK")" || {
    echo "cannot re-read production's master start time" >&2; exit 1; }
  BOOT_RECHECK="$(cat /proc/sys/kernel/random/boot_id)" || {
    echo "cannot re-read the boot id" >&2; exit 1; }
  if [ "$PROD_MASTER_RECHECK" != "$PROD_MASTER_BEFORE" ] \
     || [ "$PROD_START_RECHECK" != "$PROD_START_BEFORE" ] \
     || [ "$PROD_PIDS_RECHECK" != "$PROD_PIDS_BEFORE" ] \
     || [ "$BOOT_RECHECK" != "$BOOT_ID" ]; then
    echo "production changed while its configuration was being read:" >&2
    echo "  master     $PROD_MASTER_BEFORE -> $PROD_MASTER_RECHECK" >&2
    echo "  starttime  $PROD_START_BEFORE -> $PROD_START_RECHECK" >&2
    echo "  listeners  [$PROD_PIDS_BEFORE] -> [$PROD_PIDS_RECHECK]" >&2
    echo "  boot id    $BOOT_ID -> $BOOT_RECHECK" >&2
    echo "  The worker count just read may describe a different process than the one" >&2
    echo "  this run would compare itself against. Stopping before any test service" >&2
    echo "  is started." >&2
    exit 1
  fi
  echo "  production unchanged across the read (master, starttime, listeners, boot id)"
fi

# The authorised process count is DERIVED from the number just measured, never
# written down. Two Dask processes plus, per arm, a gunicorn arbiter and the workers
# it forks. With one worker that is 6, which is D2b's and C1's figure; with
# production's two it is 8 — but 8 is a consequence of this measurement and not a
# fact about the system. If production is reconfigured to four workers this run has
# twelve processes, and the count it verifies against must move with it or the
# verification is checking last week's deployment.
#
# Nothing here silently accommodates a surprise: --workers, when the authorisation
# supplies it, has already aborted above on any disagreement, and a worker count that
# could not be read aborted before that. This line only names the arithmetic.
EXPECTED_TOTAL=$((1 + 1 + 2 * (1 + ARM_WORKERS)))
echo "  processes this run will account for: 2 Dask + 2 x (1 arbiter + $ARM_WORKERS"
echo "    worker(s)) = $EXPECTED_TOTAL, derived from the worker count above"

# ================================================== tracked process handling ===
CLEANUP_FAILED=0

cleanup() {
  local rc=$?
  # Reverse order of start. Each refuses to signal anything it cannot prove it
  # started, and a refusal is a run failure — not something to swallow. An earlier
  # draft ended every one of these with `|| true`, which would have let a stranded
  # process or a held port exit zero and read as a clean run.
  stop_tracked candidate      "$CAND_PORT"  || CLEANUP_FAILED=1
  stop_tracked reference      "$REF_PORT"   || CLEANUP_FAILED=1
  stop_tracked dask_worker    ""            || CLEANUP_FAILED=1
  stop_tracked dask_scheduler "$SCHED_PORT" || CLEANUP_FAILED=1

  # Production must be the same process it was, not merely a process. Each way
  # this can go wrong is reported distinctly — "cannot tell" and "gone" and
  # "different process" are three different facts and only one of them is benign.
  local after boot_after master_after start_after st=0
  after="$(pids_on_port "$PROD_PORT")" || st=$?
  boot_after="$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || true)"
  if [ "$st" -eq 2 ]; then
    echo "WARNING: cannot read port state; whether production still holds" >&2
    echo "  $PROD_PORT is unknown, which is not the same as unchanged" >&2
    CLEANUP_FAILED=1
  elif [ "$boot_after" != "$BOOT_ID" ]; then
    echo "WARNING: the host rebooted during this run" >&2; CLEANUP_FAILED=1
  elif [ -z "$after" ]; then
    echo "WARNING: nothing is listening on $PROD_PORT any more — production's" >&2
    echo "  listener disappeared while this run was using the host" >&2
    echo "  before: [$PROD_PIDS_BEFORE]" >&2
    CLEANUP_FAILED=1
  else
    master_after="$(master_of "$after")" || master_after=""
    if [ -z "$master_after" ]; then
      echo "WARNING: $PROD_PORT is held by [$after] but the master PID cannot be" >&2
      echo "  resolved — a parent was unreadable, or the set has more than one root." >&2
      echo "  Production's identity can be neither confirmed nor denied." >&2
      CLEANUP_FAILED=1
    else
      start_after="$(starttime_of "$master_after")" || start_after=""
      if [ -z "$start_after" ]; then
        echo "WARNING: production's master $master_after has no readable start time;" >&2
        echo "  identity cannot be confirmed" >&2
        CLEANUP_FAILED=1
      elif [ "$master_after" != "$PROD_MASTER_BEFORE" ] || \
           [ "$start_after" != "$PROD_START_BEFORE" ]; then
        echo "WARNING: production's master on $PROD_PORT is not the process it was —" >&2
        echo "  production restarted during this run" >&2
        echo "  before: master $PROD_MASTER_BEFORE start $PROD_START_BEFORE [$PROD_PIDS_BEFORE]" >&2
        echo "  after : master $master_after start $start_after [$after]" >&2
        CLEANUP_FAILED=1
      elif [ "$after" != "$PROD_PIDS_BEFORE" ]; then
        # The master alone is not the whole picture. gunicorn's workers hold the
        # same inherited socket, so a worker that died and respawned changes the set
        # while leaving the master untouched — precisely the collateral effect this
        # run could cause by competing for the host's CPU and page cache.
        echo "WARNING: production's master is unchanged but its listener set is not —" >&2
        echo "  worker processes were recycled while this run was using the host" >&2
        echo "  before: [$PROD_PIDS_BEFORE]" >&2
        echo "  after : [$after]" >&2
        CLEANUP_FAILED=1
      else
        echo "production on $PROD_PORT unchanged (master $PROD_MASTER_BEFORE," \
             "listeners [$PROD_PIDS_BEFORE], boot id matches)"
      fi
    fi
  fi

  if [ "$CLEANUP_FAILED" != "0" ]; then
    echo "CLEANUP DID NOT COMPLETE — this run is a failure regardless of its gates" >&2
    [ "$rc" -eq 0 ] && rc=1
  fi
  return $rc
}
trap cleanup EXIT INT TERM

# ================================================== isolated reference source ===
# An unmodified copy of the production app, outside the production directory. The
# copy is read-only and its digests are checked against the originals, so "we ran the
# same code" is verified rather than assumed.
#
# The directory is refused if it exists rather than cleared: a forced recursive
# delete on a path built from $HOME is one substitution away from catastrophic, and
# the D2a deploy step already set the standard — verify the target, do not clean it.
if [ -e "$WORK" ]; then
  echo "$WORK already exists. Inspect and remove it deliberately before re-running;" >&2
  echo "this script will not clear a directory it did not create." >&2
  exit 1
fi
mkdir -p "$REF_DIR" "$CAND_DIR"
cp "$PROD_DIR/woa23_app.py" "$REF_DIR/"
cp -r "$PROD_DIR/src" "$REF_DIR/src"
cp -r "$HERE/api" "$CAND_DIR/api"

# Both arms get a `data` symlink and are started with their own staging directory as
# cwd, so both resolve the store through the identical *relative* path.
#
# This is what the 2026-08-08 run got wrong. `zarr_group_paths` is a set of path
# strings, so its iteration order depends on the hash of those strings; the
# reference interpolates the hard-coded "data/" from woa23_app.py:63 while the
# candidate interpolated whatever WOA23_ZARR_STORE said, which was an absolute path.
# Different strings, different set order, different `result_list` order — and the two
# cases spanning more than one Zarr group are exactly the two that differed. That is
# a strongly supported mechanism, not a proven one: the actual bodies were never
# captured, so how the difference decomposes is not established. The candidate is
# NOT modified: it still reads the store from
# WOA23_ZARR_STORE and keeps that configurability. It is simply given the same
# string the reference uses, so the benchmark stops introducing a difference of its
# own.
#
# The symlink gives both the real store without copying 31.9 GiB and without a
# writable path to it.
ln -s "$STORE" "$REF_DIR/data"
ln -s "$STORE" "$CAND_DIR/data"
STORE_LITERAL='data/'                 # byte-for-byte what woa23_app.py:63 sets
chmod -R a-w "$REF_DIR/woa23_app.py" "$REF_DIR/src" "$CAND_DIR/api"

echo "== verifying the reference copy is byte-identical to production's =="
for f in woa23_app.py src/__init__.py src/config.py src/dask_client_manager.py \
         src/woa23_utils.py; do
  a="$(sha256sum "$PROD_DIR/$f" | cut -d' ' -f1)"
  b="$(sha256sum "$REF_DIR/$f" | cut -d' ' -f1)"
  [ "$a" = "$b" ] || { echo "  $f DIFFERS from production ($a vs $b)" >&2; exit 1; }
  echo "  $f  $a"
done

echo "== verifying the candidate copy is byte-identical to the repository's =="
# Enumerated, not listed. This was a hard-coded four-file loop, and spec 004 added a
# fifth module — api/store_paths.py, which holds the path builder both arms' group
# paths now come from. `cp -r` copied it and the loop would not have checked it: the
# one file whose byte-identity matters most to this run would have been the one file
# unverified. A list of filenames drifts from the directory it describes; the
# directory does not.
cand_files_repo="$(cd "$HERE" && find api -name '*.py' -not -path '*/__pycache__/*' | sort)"
cand_files_stage="$(cd "$CAND_DIR" && find api -name '*.py' -not -path '*/__pycache__/*' | sort)"
if [ "$cand_files_repo" != "$cand_files_stage" ]; then
  echo "  the staged candidate does not have the same file set as the repository:" >&2
  diff <(printf '%s\n' "$cand_files_repo") <(printf '%s\n' "$cand_files_stage") >&2 || true
  exit 1
fi
printf '%s\n' "$cand_files_repo" | while IFS= read -r f; do
  [ -n "$f" ] || continue
  a="$(sha256sum "$HERE/$f" | cut -d' ' -f1)"
  b="$(sha256sum "$CAND_DIR/$f" | cut -d' ' -f1)"
  [ "$a" = "$b" ] || { echo "  $f DIFFERS from the repository ($a vs $b)" >&2; exit 1; }
  echo "  $f  $a"
done || exit 1
echo "  $(printf '%s\n' "$cand_files_repo" | grep -c .) candidate source files verified"

# woa23_app.py:63 is the source of the reference's literal. If that line ever
# changes, the string below is silently wrong, so it is checked rather than trusted.
if ! grep -qF 'zarr_store_path = "data/"' "$REF_DIR/woa23_app.py"; then
  echo "woa23_app.py no longer sets zarr_store_path = \"data/\"; the candidate" >&2
  echo "  cannot be given a matching literal without re-reading it" >&2
  exit 1
fi
echo "  reference store literal confirmed at woa23_app.py:63: '$STORE_LITERAL'"

# ===================================================== isolated Dask cluster ===
# `src/dask_client_manager.py` reads DASK_SCHEDULER_ADDRESS and falls back to
# tcp://localhost:8786 — production's shared scheduler, serving tide_app and
# mhw_app. The reference must never reach it, so it gets its own on $SCHED_PORT and
# the variable is set explicitly rather than relying on a default being overridden.
if [ "$S2_MODE" = none ]; then
  start_tracked dask_scheduler "$SCHED_PORT" \
    "$VENV/bin/dask" scheduler --host 127.0.0.1 --port "$SCHED_PORT" --no-dashboard
else
  # No console script: the clone is a package tree, not an installed environment, so
  # it has no bin/. `-m distributed.cli.dask_scheduler` is the same entry point the
  # `dask scheduler` wrapper calls, reached without needing a wrapper to exist.
  start_tracked dask_scheduler "$SCHED_PORT" \
    env -C "$WORK" -u VIRTUAL_ENV -u PYTHONHOME -u PYTHONHASHSEED \
      PYTHONPATH="$PKG_CLONE" PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
      "$PY_BINARY" -S -m distributed.cli.dask_scheduler \
      --host 127.0.0.1 --port "$SCHED_PORT" --no-dashboard
fi
for _ in $(seq 1 30); do port_held "$SCHED_PORT" && break; sleep 1; done
port_held "$SCHED_PORT" || { echo "scheduler did not bind" >&2; exit 1; }
# --no-nanny: `dask worker` defaults to --nanny, a supervisor process that forks
# the worker. For a single worker the nanny buys nothing here and costs an extra
# process to account for, so the worker runs in this process directly.
if [ "$S2_MODE" = none ]; then
  start_tracked dask_worker "" \
    "$VENV/bin/dask" worker "tcp://127.0.0.1:${SCHED_PORT}" \
    --nworkers 1 --nthreads 1 --memory-limit 8GB --no-dashboard --no-nanny
else
  start_tracked dask_worker "" \
    env -C "$WORK" -u VIRTUAL_ENV -u PYTHONHOME -u PYTHONHASHSEED \
      PYTHONPATH="$PKG_CLONE" PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
      "$PY_BINARY" -S -m distributed.cli.dask_worker "tcp://127.0.0.1:${SCHED_PORT}" \
      --nworkers 1 --nthreads 1 --memory-limit 8GB --no-dashboard --no-nanny
fi

# ==================================================================== the arms ===
# One environment, both arms. That is the whole point of the byte-exact comparison:
# the packages stop being a variable because there is only one set of them. Under
# D2b that set is dev2026/.venv; under C1 and C2 it is the read-only clone of
# production's package tree, reached the same way by both arms. Both go through
# start_tracked either way, so identity is recorded identically for every process
# this run owns.
#
# The S2 launch is spelled out rather than assembled from a variable, because the
# difference between the two forms is the difference between the two experiments
# and a reader should not have to expand anything to see which one is running:
#
#   VIRTUAL_ENV, PYTHONHOME  unset — either would redirect the interpreter to an
#                            environment other than the clone, and VIRTUAL_ENV is
#                            exactly what a leftover `source .venv/bin/activate`
#                            leaves behind in an interactive shell.
#   -S                       site.py does not run, so no .pth is processed. This is
#                            a real limitation and it is carried into the results.
#   PYTHONPATH=<clone>       the only package source.
#   PYTHONNOUSERSITE=1       ~/.local/lib/python3.11/site-packages is not a package
#                            source either.
#   PYTHONDONTWRITEBYTECODE  the clone is read-only and must stay byte-identical.
if [ "$S2_MODE" = none ]; then
  start_tracked reference "$REF_PORT" \
    env -C "$REF_DIR" PYTHONHASHSEED=0 VIRTUAL_ENV="$VENV" \
      DASK_SCHEDULER_ADDRESS="tcp://127.0.0.1:${SCHED_PORT}" \
      "$VENV/bin/gunicorn" woa23_app:app -w 1 -k uvicorn.workers.UvicornWorker \
      -b "127.0.0.1:${REF_PORT}" --timeout 120

  # Same cwd-relative literal as the reference, still taken from the environment so
  # the candidate's configurability is intact. PYTHONPATH keeps the venv's packages
  # importable from a cwd that is not the repository.
  start_tracked candidate "$CAND_PORT" \
    env -C "$CAND_DIR" PYTHONHASHSEED=0 VIRTUAL_ENV="$VENV" \
      WOA23_ZARR_STORE="$STORE_LITERAL" \
      "$VENV/bin/gunicorn" api.app:app -w 1 -k uvicorn.workers.UvicornWorker \
      -b "127.0.0.1:${CAND_PORT}" --timeout 120
elif [ "$S2_MODE" = c1 ]; then
  # Re-verified here rather than trusted from preflight. Between the two checks this
  # run created a staging tree, started a Dask scheduler and a worker, and waited for
  # a port — time in which a writable ancestor could have had the clone swapped. The
  # window is narrowed to the gap between this line and the arm's own imports; it is
  # not closed, and bench/clone_integrity.py says so in the record.
  clone_integrity before-reference || exit 1
  start_tracked reference "$REF_PORT" \
    env -C "$REF_DIR" -u VIRTUAL_ENV -u PYTHONHOME \
      PYTHONHASHSEED=0 PYTHONPATH="$PKG_CLONE" \
      PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
      DASK_SCHEDULER_ADDRESS="tcp://127.0.0.1:${SCHED_PORT}" \
      "$PY_BINARY" -S -m gunicorn woa23_app:app -w 1 \
      -k uvicorn.workers.UvicornWorker -b "127.0.0.1:${REF_PORT}" --timeout 120

  clone_integrity before-candidate || exit 1
  start_tracked candidate "$CAND_PORT" \
    env -C "$CAND_DIR" -u VIRTUAL_ENV -u PYTHONHOME \
      PYTHONHASHSEED=0 PYTHONPATH="$PKG_CLONE" \
      PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
      WOA23_ZARR_STORE="$STORE_LITERAL" \
      "$PY_BINARY" -S -m gunicorn api.app:app -w 1 \
      -k uvicorn.workers.UvicornWorker -b "127.0.0.1:${CAND_PORT}" --timeout 120
else
  # C2. `-u PYTHONHASHSEED` rather than an empty value: CPython rejects
  # PYTHONHASHSEED="" outright, so setting it empty would not mean "unset", it would
  # mean the interpreter refuses to start — and the arm would fail for a reason that
  # looks nothing like the one it actually had.
  clone_integrity before-reference || exit 1
  start_tracked reference "$REF_PORT" \
    env -C "$REF_DIR" -u VIRTUAL_ENV -u PYTHONHOME -u PYTHONHASHSEED \
      PYTHONPATH="$PKG_CLONE" PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
      DASK_SCHEDULER_ADDRESS="tcp://127.0.0.1:${SCHED_PORT}" \
      "$PY_BINARY" -S -m gunicorn woa23_app:app -w "$ARM_WORKERS" \
      -k uvicorn.workers.UvicornWorker -b "127.0.0.1:${REF_PORT}" --timeout 120

  clone_integrity before-candidate || exit 1
  start_tracked candidate "$CAND_PORT" \
    env -C "$CAND_DIR" -u VIRTUAL_ENV -u PYTHONHOME -u PYTHONHASHSEED \
      PYTHONPATH="$PKG_CLONE" PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
      WOA23_ZARR_STORE="$STORE_LITERAL" \
      "$PY_BINARY" -S -m gunicorn api.app:app -w "$ARM_WORKERS" \
      -k uvicorn.workers.UvicornWorker -b "127.0.0.1:${CAND_PORT}" --timeout 120
fi

# ------------------------------------------------------- PROCESS readiness only ---
# The OpenAPI document. It exercises the whole stack that has to be up — gunicorn,
# the uvicorn worker, FastAPI routing — and **this probe** reads nothing from the
# Zarr store, so waiting for the servers to appear costs neither arm a chunk read.
#
# What has already happened by the time this runs is NOT nothing, and saying so was
# wrong until spec 004 landed. Under the patched candidate the lifespan opens the
# anchor group's **Zarr metadata** during startup — before any HTTP is served — so
# by the time a 200 comes back the candidate has read metadata for
# `1_degree/annual/TS`.
#
# That it read **no data or coordinate chunk** is NOT observed here. This run
# installs no audit hook and records no file-open events; the property is
# established offline, by bench/test_d1_store_validation.py, against synthetic
# fixtures, and carries over only because this run executes the same code path.
# It is **implementation-supported, not observed on this host**.
#
# The reference does not do this: `woa23_app.py` is unmodified and validates nothing
# at startup. So the two arms differ in what they have read by this point — metadata
# for one group on the candidate, nothing on the reference. That cannot change any
# byte either returns, which is what 5.2A compares. It would matter to a latency
# comparison, where a warmed metadata cache is exactly the kind of asymmetry S1 went
# to trouble to remove; no latency is measured in --c1 or --c2-cycle.
#
# That is also this probe's limit, and the limit is the point. **A 200 here says the
# process is serving.** Under the UNPATCHED candidate it said nothing at all about
# the store — D1 measured that directly: with WOA23_ZARR_STORE pointing at a path
# that does not exist, an empty directory, or an ordinary file, the app imported,
# started, served this document with a 200, and failed only when a request reached
# the data path. Under the patched candidate those three cannot get this far, but a
# 200 still does not establish that a *data* read will succeed.
#
# STORE readiness — that the configured store can actually be opened and read for
# data — is established by the symmetric data probe below and nowhere else, and it
# is a *separate stage*. Nothing between here and there may be described as the
# store being ready to serve data.
#
# The earlier probe issued a real data query, to the reference first. That gave the
# reference a warm store handle and a populated page cache before the candidate had
# served anything, and it did so on the one path the whole experiment measures.
process_ready() {           # process_ready <port> — serving, NOT store-ready
  local out
  for _ in $(seq 1 30); do
    out="$(curl -s --max-time 5 -o /dev/null -w '%{http_code} %{size_download}' \
      "http://127.0.0.1:$1/api/swagger/woa23/openapi.json" || true)"
    [ "${out%% *}" = "200" ] && [ "${out##* }" -gt 0 ] && return 0
    sleep 1
  done
  return 1
}
process_ready "$REF_PORT"  || { echo "reference process not ready; see $RUN/reference.log" >&2; exit 1; }
process_ready "$CAND_PORT" || { echo "candidate process not ready; see $RUN/candidate.log" >&2; exit 1; }
echo "  both arms are PROCESS-ready (OpenAPI 200)."
echo "    This probe read nothing from the store. The candidate's startup anchor"
echo "    validation has already read Zarr METADATA for 1_degree/annual/TS;"
echo "    the reference validates nothing at startup."
echo "    That the anchor read touched NO data or coordinate chunk is offline-audited"
echo "    (bench/test_d1_store_validation.py, synthetic fixtures) and"
echo "    implementation-supported — this run observes no file opens."
echo "    Neither arm is known to serve DATA yet — that is the probe below."

# The authorisation is for a specific set of processes, so the set is *verified*,
# not merely printed. A run that has five or seven is outside what was granted —
# a nanny that reappeared, a second gunicorn worker, a scheduler that forked — and
# it stops here, before the gates, with the trap cleaning up what it started.
echo "== process trees (what the authorisation covers and cleanup is held to) =="
expected_procs() {
  case "$1" in
    dask_scheduler|dask_worker) echo 1 ;;   # --no-nanny; the default would be 2
    # gunicorn arbiter plus the workers it forks. One each under D2b and C1; under
    # C2 it is production's measured count, so the authorised total is derived from
    # the same number the arms were started with rather than written down twice.
    reference|candidate)        echo $((1 + ARM_WORKERS)) ;;
    *)                          echo 0 ;;
  esac
}
AUTHORISED_TOTAL="$EXPECTED_TOTAL"     # derived where ARM_WORKERS was established
n_procs=0
seen_pids=""
for svc in dask_scheduler dask_worker reference candidate; do
  record_tree "$svc" || {
    echo "cannot record a complete process tree for $svc — the authorised set" >&2
    echo "  cannot be verified, so this run stops here and the trap cleans up" >&2
    exit 1; }
  pids="$(tree_pids "$svc")"
  n="$(printf '%s' "$pids" | wc -w | tr -d ' ')"
  want="$(expected_procs "$svc")"
  if [ "$n" -ne "$want" ]; then
    echo "$svc has $n OS process(es), expected $want: [$pids]" >&2
    echo "  This run is outside the process count the authorisation was granted for." >&2
    exit 1
  fi
  for pid in $pids; do
    case " $seen_pids " in
      *" $pid "*) echo "PID $pid appears in more than one service tree — the trees" >&2
                  echo "  overlap, so cleanup cannot attribute processes correctly" >&2
                  exit 1 ;;
    esac
    seen_pids="$seen_pids $pid"
  done
  n_procs=$((n_procs + n))
done
if [ "$n_procs" -ne "$AUTHORISED_TOTAL" ]; then
  echo "this run has $n_procs OS processes; the authorisation is for $AUTHORISED_TOTAL" >&2
  exit 1
fi
echo "  $n_procs OS processes, matching the authorised set: [$seen_pids ]"

# ------------------------------------------------- STORE readiness, separately ---
# This is the stage that establishes the store can be opened and read, and it is the
# first one that touches it. The OpenAPI check above proved only that the process is
# serving: D1 showed a nonexistent path, an empty directory and an ordinary file all
# import, start and answer that document with a 200, failing only here.
#
# So a store the reference cannot open fails at this point rather than thirty
# requests into the contract gate. But the probe must not favour an arm either, so it
# runs once in each order: candidate-first, then reference-first. Two requests per
# arm, exactly counterbalanced.
probe() {                   # probe <label> <port>
  local out
  out="$(curl -s --max-time 60 -o /dev/null -w '%{http_code} %{size_download}' \
    "http://127.0.0.1:$2/api/woa23?lon0=135&lat0=15&parameter=temperature" || true)"
  if [ "${out%% *}" != "200" ] || [ "${out##* }" -le 0 ]; then
    echo "$1 cannot serve the data path (got '$out'); see $RUN/$1.log" >&2
    return 1
  fi
}
if [ "$CLEANUP_ONLY" = "yes" ]; then
  # Skipped deliberately. The probe exists to catch an unreadable store before
  # spending 64 contract cases; with no contract gate to protect there is nothing
  # for it to save, and this mode's request count is meant to be as close to zero as
  # the process tree allows.
  echo "  data-path probe skipped (--cleanup-only): 0 requests"
  echo "  STORE readiness is therefore NOT established by this run."
else
  for pair in "candidate:$CAND_PORT reference:$REF_PORT" \
              "reference:$REF_PORT candidate:$CAND_PORT"; do
    for entry in $pair; do
      probe "${entry%%:*}" "${entry#*:}" || exit 1
    done
  done
  echo "  both arms are STORE-ready: each served the data path twice, in both orders."
fi
echo "both arms ready (process readiness and, unless --cleanup-only, store readiness)"

# Re-record each tree now that the children exist. start_tracked already wrote one
# when it began tracking — it has to, because the trap is armed from that moment and
# stop refuses to signal a service whose tree it cannot interpret — but a gunicorn
# arbiter has not forked its worker in the first second after exec, so that first
# snapshot holds the arbiter alone. This is where the full set is established, and
# where it is checked against what the authorisation covers.

# ================================================================= provenance ===
echo "== provenance =="
if [ "$S2_MODE" = none ]; then
  uv run python -m bench.collect_backend_meta --port "$CAND_PORT" --manifest candidate \
    --expect-argv-contains api.app:app --lockfile uv.lock \
    --out "results/${LABEL}_meta_candidate.json"
  uv run python -m bench.collect_backend_meta --port "$REF_PORT" --manifest reference \
    --expect-argv-contains woa23_app:app --lockfile uv.lock \
    --out "results/${LABEL}_meta_reference.json"
else
  # --env-python, and not the derived answer. Under S2 the arm's argv[0] is
  # production's binary, whose sibling `python` *is* production's environment: the
  # heuristic would list production's 236 distributions and label them the clone's.
  # The listing is reproduced under the arms' own launch (-S, clone on PYTHONPATH),
  # because the same binary answers differently depending on how it is started.
  for arm in candidate reference; do
    if [ "$arm" = candidate ]; then p="$CAND_PORT"; expect=api.app:app
    else p="$REF_PORT"; expect=woa23_app:app; fi
    # --env-python-arg=-S, not `--env-python-arg -S`. argparse reads a value
    # beginning with a dash as the next *option*, so the separated form makes it
    # report "expected one argument" and exit 2 — which is what happened on the
    # first real C1 attempt, after both arms were up and the store had been probed.
    # The `=` form is unambiguous.
    uv run python -m bench.collect_backend_meta --port "$p" --manifest "$arm" \
      --expect-argv-contains "$expect" \
      --env-python "$PY_BINARY" --env-python-arg=-S \
      --env-python-pythonpath "$PKG_CLONE" \
      --clone-manifest "$CLONE_MANIFEST" --clone-root "$PKG_CLONE" \
      --out "results/${LABEL}_meta_${arm}.json"
  done

  # ------------------------------------------- what the arms actually imported ---
  # Two kinds of evidence, recorded separately because they establish different
  # things. The probe is an identically-launched sibling interpreter: exact about
  # the launch procedure, and not the gunicorn worker. /proc/<pid>/maps is the arm
  # itself: every native extension it really loaded, which is where polars, numpy,
  # zarr's codecs and h5py would show up if they had come from production — and
  # blind to a pure-Python module imported from the wrong place.
  echo "== the arms' interpreter and import paths =="
  for arm in candidate reference; do
    if [ "$arm" = candidate ]; then armdir="$CAND_DIR"; else armdir="$REF_DIR"; fi
    pids="$(tree_pids "$arm")"
    pid_args=""
    for pid in $pids; do pid_args="$pid_args --pid $pid"; done
    [ -n "$pid_args" ] || { echo "no tracked PIDs for $arm" >&2; exit 1; }
    # shellcheck disable=SC2086
    uv run python -m bench.s2_provenance \
      --python-binary "$PY_BINARY" --package-clone "$PKG_CLONE" \
      --cwd "$armdir" --label "$arm" \
      $([ "$S2_MODE" = c1 ] && echo "--hashseed 0" || echo "") \
      --allow "$PKG_CLONE" --allow "$WORK" \
      --forbid "$PROD_DIR" --forbid "$PROD_SITE" \
      $pid_args \
      --out "results/${LABEL}_interp_${arm}.json" \
      || { echo "$arm did not establish import isolation; stopping before any gate" >&2
           exit 1; }
  done
fi

echo "== the arms must share an environment, or the comparison proves nothing =="
LABEL="$LABEL" S2_MODE="$S2_MODE" \
uv run python - <<'PYEOF' || exit 1
import json, os, sys
sys.path.insert(0, ".")
# One call, into tested code. The composition used to be spelled out here, and the
# S2 branch computed the right field list into a variable it then never passed —
# a dead assignment that reads exactly like working code, and every S2 run failed
# on the D2b field list complaining about a lockfile this campaign does not have.
# Inline logic in a heredoc is logic no test can reach.
from bench.provenance import compare_arms

label = os.environ["LABEL"]
s2_mode = os.environ["S2_MODE"]
cand = json.load(open(f"results/{label}_meta_candidate.json"))
ref = json.load(open(f"results/{label}_meta_reference.json"))
env = json.load(open(f"results/{label}_environment.json"))
# C2 is the only mode whose arms are deliberately unpinned, and it requires the
# seed to be ABSENT rather than merely tolerating it: a cycle that ran pinned
# observed nothing about the thing C2 exists to observe.
seed_policy = "both-unpinned" if s2_mode == "c2" else "both-pinned"
problems = compare_arms(cand, ref, env, s2=(s2_mode != "none"),
                        seed_policy=seed_policy)
if problems:
    print("arms are not comparable:", file=sys.stderr)
    for p in problems:
        print(f"  - {p}", file=sys.stderr)
    raise SystemExit(1)
print(f"both arms: python {cand['env_python_version']}, "
      f"{len(cand['dependencies']['distributions'])} distributions, "
      f"name==version set {cand['dependencies']['name_version_set_sha256'][:16]}")
print(f"both arms build group paths from {cand['store_path_literal']!r}")
PYEOF

# =============================================================== contract first ===
# The latency gate is not run unless the contract gate passes. A speed number for a
# backend that returns different bytes is not a result.
if [ "$CLEANUP_ONLY" = "yes" ]; then
  echo
  echo "== --cleanup-only: stopping here =="
  echo "   No contract gate, no latency gate, no pilot. This run measured nothing"
  echo "   and nothing may be quoted from it. The trap now exercises cleanup, which"
  echo "   is the only thing under observation."
  exit 0
fi

# C1 and D2b compare bytes; C2 cannot. Under C2 neither arm has a pinned seed, so
# `set` iteration order — and with it the row order of any query spanning more than
# one Zarr group — is a property of the process, not of the code. Comparing bytes
# there would fail on a difference that is not a defect. 5.2B compares the row
# multiset and the column set instead, and order is recorded separately below rather
# than folded into the verdict.
VARIANT=5.2A
SEED_POLICY=both-pinned
if [ "$S2_MODE" = c2 ]; then
  VARIANT=5.2B
  # Not the 5.2B default. That default is "pinned candidate against live
  # production"; C2's arms are both ours and both unpinned, so the requirement is
  # stated rather than inferred from the variant.
  SEED_POLICY=both-unpinned
fi
echo "== contract gate, variant $VARIANT ($([ "$VARIANT" = 5.2A ] && echo byte-exact \
     || echo semantic)), 64 cases per arm =="
uv run python -m bench.contract_diff \
  --candidate "http://127.0.0.1:${CAND_PORT}" \
  --reference "http://127.0.0.1:${REF_PORT}" --variant "$VARIANT" \
  --seed-policy "$SEED_POLICY" \
  --candidate-meta "results/${LABEL}_meta_candidate.json" \
  --reference-meta "results/${LABEL}_meta_reference.json" \
  --out "results/${LABEL}_contract.json" \
  || { echo "contract gate did not pass — stopping before anything further" >&2; exit 1; }

if [ "$S2_MODE" != none ]; then
  echo
  echo "== --$([ "$S2_MODE" = c1 ] && echo c1 || echo c2-cycle): stopping here =="
  echo "   The contract gate above passed. No latency gate, no noise pilot, no rung"
  echo "   escalation: this invocation produced no timing of any kind and nothing may"
  echo "   be quoted from it as performance."
  echo "   LIMITATION -S: site.py did not run, so no .pth in the clone was processed."
  echo "   This is isolated package-tree import correctness (spec 002 section 4.3.1),"
  echo "   and the launcher is this script rather than production's PM2 path."
  if [ "$S2_MODE" = c2 ]; then
    echo "   This is ONE cycle. A C2 result needs three independent cycles and the"
    echo "   seed-diversity observation across them; scripts/run_c2_cycles.sh draws"
    echo "   that conclusion, and this invocation does not."
  fi
  echo "   artefacts: results/${LABEL}_contract.json"
  echo "              results/${LABEL}_meta_{candidate,reference}.json"
  echo "              results/${LABEL}_interp_{candidate,reference}.json"
  echo "              results/${LABEL}_environment.json"
  exit 0
fi

if [ "$CONTRACT_ONLY" = "yes" ]; then
  echo
  echo "== --contract-only: stopping here =="
  echo "   The contract gate above passed. No latency gate, no noise pilot, and no"
  echo "   rung escalation was run, so this invocation produced no timing of any"
  echo "   kind and nothing may be quoted from it as performance. The trap now"
  echo "   stops all four services."
  exit 0
fi

echo "== latency gate, rung 21, variant 5.2A =="
uv run python -m bench.paired_bench \
  --candidate "http://127.0.0.1:${CAND_PORT}" \
  --reference "http://127.0.0.1:${REF_PORT}" \
  --gate-variant 5.2A --warm 21 --include-heavy --margin 0.05 \
  --candidate-meta "results/${LABEL}_meta_candidate.json" \
  --reference-meta "results/${LABEL}_meta_reference.json" \
  --out "results/${LABEL}_paired.json"

# The pilot runs LAST, and against both arms.
#
# Before the latency gate it would have sampled one arm 208 times and the other not
# at all, warming one side's page cache and connection state ahead of a paired
# measurement — an asymmetry introduced by the very tool meant to characterise noise.
# Its output is sample-size planning for the *next* rung, which does not need to
# precede this one; the gate's own confidence interval carries the noise for this
# one. Running it against both arms keeps the recorded floor a property of the pair.
echo "== sample-size planning for any escalation, both arms, after the measurement =="
uv run python -m bench.noise_pilot --base-url "http://127.0.0.1:${REF_PORT}" \
  --warm 25 --out "results/${LABEL}_noise_pilot_reference.json"
uv run python -m bench.noise_pilot --base-url "http://127.0.0.1:${CAND_PORT}" \
  --warm 25 --out "results/${LABEL}_noise_pilot_candidate.json"

echo
echo "artefacts: results/${LABEL}_contract.json results/${LABEL}_paired.json"
echo "           results/${LABEL}_meta_{candidate,reference}.json"
echo "           results/${LABEL}_noise_pilot_{reference,candidate}.json"
echo "           results/${LABEL}_environment.json"
echo "== done; the trap now stops all four services, verifies every process in"
echo "   their recorded trees has exited, verifies the ports, and confirms"
echo "   production is the same process it was =="
