#!/usr/bin/env bash
#
# C2: three independent start/stop cycles of the S2 arms, and the one conclusion
# that needs all three.
#
# A single cycle is `run_controlled.sh --c2-cycle`, which starts the arms from the
# read-only package clone with no PYTHONHASHSEED, runs the 5.2B semantic gate over
# the 64 cases and stops everything. That cycle cannot answer C2's actual question,
# which is what an *unpinned* seed does across independent starts — one process has
# one seed, and one seed is not a distribution.
#
# So this runs three, each in its own workdir, each fully cleaned up before the next
# begins, and then makes exactly two statements:
#
#   1. the 5.2B verdict, which is PASS only if all three cycles passed;
#   2. the seed-diversity observation, which is a *report*, never an escalation.
#      If the three cycles do not produce three distinct seeds the answer is
#      INSUFFICIENT and this script stops. It does not add a fourth cycle. Three was
#      what was authorised, and quietly running more until the observation came out
#      the desired way would make the observation worthless.
#
# Order stability is recorded from the per-case fingerprints and is deliberately
# **not** part of the verdict: with no pinned seed a row-order difference is a
# property of the process, not a defect.
#
#   WOA23_S2_C2_GRANTED=yes ./scripts/run_c2_cycles.sh \
#       --python-binary /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
#       --package-clone /home/odbadmin/woa23-s2-package-clone/dist \
#       --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
#       --workdir-base /home/odbadmin/woa23-s2-c2-work \
#       --candidate-port 18071 --reference-port 18072 --scheduler-port 18798

set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$HERE/scripts/run_controlled.sh"
RUN="$HERE/run"

# Fixed, and not a flag. "Three independent cycles" is the authorised design; a
# --cycles flag is how three becomes five the first time three is inconvenient.
CYCLES=3

PY_BINARY=""; PKG_CLONE=""; CLONE_MANIFEST=""; WORKDIR_BASE=""
CAND_PORT=""; REF_PORT=""; SCHED_PORT=""; WORKERS=""
# Names this run's three cycles. A rerun is a separate body of evidence from the
# run before it, and evidence that shares a name with earlier evidence is one
# `cp -r` away from being confused with it — or from replacing it.
LABEL_PREFIX="c2"
# Passed straight through to every cycle. Deciding it here would let the wrapper
# soften a check that belongs to the runner.
ALLOW_REUSED=""

usage() {
  cat >&2 <<'USAGE'
usage: run_c2_cycles.sh --python-binary PATH --package-clone PATH
                        --clone-manifest PATH --workdir-base PATH
                        --candidate-port N --reference-port N --scheduler-port N
                        [--expected-workers N] [--label-prefix NAME]
                        [--allow-reused-ports]

Runs exactly three --c2-cycle invocations and reports the 5.2B verdict, the seed
diversity observed across them, and order stability. Never runs a fourth.

--clone-manifest is the FOUR-COLUMN manifest written when the clone was built,
normally <clone-root>/clone.manifest. It is NOT the clone root's SHA256SUMS: that
file lists the digests of the manifest files and cannot verify a tree. An earlier
version of the usage example above named a manifest/SHA256SUMS path that does not
exist, and a C2 run was launched against the wrong file because of it.

Requires WOA23_S2_C2_GRANTED=yes. WOA23_D2B_GRANTED does not authorise this.
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --python-binary)  [ $# -ge 2 ] || { echo "--python-binary needs a value" >&2; exit 2; }
                      PY_BINARY="$2"; shift 2 ;;
    --package-clone)  [ $# -ge 2 ] || { echo "--package-clone needs a value" >&2; exit 2; }
                      PKG_CLONE="$2"; shift 2 ;;
    --clone-manifest) [ $# -ge 2 ] || { echo "--clone-manifest needs a value" >&2; exit 2; }
                      CLONE_MANIFEST="$2"; shift 2 ;;
    --workdir-base)   [ $# -ge 2 ] || { echo "--workdir-base needs a value" >&2; exit 2; }
                      WORKDIR_BASE="$2"; shift 2 ;;
    --candidate-port) [ $# -ge 2 ] || { echo "--candidate-port needs a value" >&2; exit 2; }
                      CAND_PORT="$2"; shift 2 ;;
    --reference-port) [ $# -ge 2 ] || { echo "--reference-port needs a value" >&2; exit 2; }
                      REF_PORT="$2"; shift 2 ;;
    --scheduler-port) [ $# -ge 2 ] || { echo "--scheduler-port needs a value" >&2; exit 2; }
                      SCHED_PORT="$2"; shift 2 ;;
    --expected-workers) [ $# -ge 2 ] || { echo "--expected-workers needs a value" >&2; exit 2; }
                      WORKERS="$2"; shift 2 ;;
    --label-prefix)   [ $# -ge 2 ] || { echo "--label-prefix needs a value" >&2; exit 2; }
                      LABEL_PREFIX="$2"; shift 2 ;;
    --allow-reused-ports) ALLOW_REUSED=--allow-reused-ports; shift ;;
    --workers)        echo "--workers was renamed --expected-workers: it asserts" >&2
                      echo "  production's worker count and never sets the arms'." >&2
                      exit 2 ;;
    --cycles)         echo "--cycles is not a flag. C2 is three independent cycles;" >&2
                      echo "  a run that could choose its own number could keep going" >&2
                      echo "  until the seed observation came out a particular way." >&2
                      exit 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

for pair in "--python-binary:$PY_BINARY" "--package-clone:$PKG_CLONE" \
            "--clone-manifest:$CLONE_MANIFEST" "--workdir-base:$WORKDIR_BASE" \
            "--candidate-port:$CAND_PORT" "--reference-port:$REF_PORT" \
            "--scheduler-port:$SCHED_PORT"; do
  [ -n "${pair#*:}" ] || { echo "${pair%%:*} is required" >&2; usage; exit 2; }
done

# The prefix names files and a directory, so it is checked rather than trusted:
# anything else would let a stray value write outside results/ and run/.
case "$LABEL_PREFIX" in
  ''|*[!A-Za-z0-9_]*|[!A-Za-z]*)
    echo "--label-prefix must start with a letter and hold only letters, digits" >&2
    echo "  and underscores; got '$LABEL_PREFIX'" >&2
    exit 2 ;;
esac

# The grant is checked here as well as in the runner. This script's own effect is
# three start/stop cycles rather than one, and a wrapper that let itself be started
# without the authorisation — and only found out on the first inner invocation —
# would already have created a workdir by then.
if [ "${WOA23_S2_C2_GRANTED:-}" != "yes" ]; then
  echo "S2 C2 authorisation not stated. This starts and stops the S2 arms THREE" >&2
  echo "times, from production's Python binary against the read-only package clone." >&2
  if [ "${WOA23_D2B_GRANTED:-}" = "yes" ]; then
    echo "WOA23_D2B_GRANTED is set and does NOT authorise this." >&2
  fi
  if [ "${WOA23_S2_C1_GRANTED:-}" = "yes" ]; then
    echo "WOA23_S2_C1_GRANTED is set and does NOT authorise this: C1 is one cycle" >&2
    echo "  with a pinned seed, C2 is three with none." >&2
  fi
  echo "Re-run with WOA23_S2_C2_GRANTED=yes once it is granted." >&2
  exit 3
fi

# Refuse to write over an earlier run's evidence — all of it, not the results.
#
# The first version of this checked results/ only. That is the half that is easy to
# think of and the less costly half to lose: `run/<label>/` holds each service's log
# and any state a cleanup deliberately preserved when it refused to kill something,
# which is precisely the evidence a failed run exists to keep. Both trees, matched
# by prefix, so the three cycles AND the summary are covered by one check.
#
# New staging usually makes this impossible. "Usually" is doing the work in that
# sentence: the driver writes into whatever export it was launched from, so a rerun
# started from a previous export would overwrite the artefacts it was told to leave
# alone.
# shellcheck source=lib_labels.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib_labels.sh"
refuse_label_collision "$HERE" "${LABEL_PREFIX}_" \
  "Use a different --label-prefix, or a staging export with no C2 result in it." \
  || exit 1

echo "== C2: $CYCLES independent cycles =="
echo "   binary   : $PY_BINARY"
echo "   clone    : $PKG_CLONE"
echo "   manifest : $CLONE_MANIFEST"
echo "   workdirs : ${WORKDIR_BASE}-cycle1 .. ${WORKDIR_BASE}-cycle${CYCLES}"
echo "   ports    : candidate $CAND_PORT, reference $REF_PORT, scheduler $SCHED_PORT"
echo "   labels   : ${LABEL_PREFIX}_cycle1 .. ${LABEL_PREFIX}_cycle${CYCLES} — each cycle's results, provenance,"
echo "              state and service logs live under its own label, so a failing"
echo "              cycle keeps its evidence and no cycle overwrites another"
echo "   seed     : unset in every cycle. That is the thing under observation."
echo

labels=""
for i in $(seq 1 "$CYCLES"); do
  label="${LABEL_PREFIX}_cycle${i}"
  echo "======================================================================"
  echo "== cycle $i of $CYCLES  (label $label) =="
  echo "======================================================================"

  # Leftover state from the previous cycle is a hard stop, not something to clean.
  # The runner refuses to start on it too; checking here as well means a failed
  # cycle 1 does not get as far as creating cycle 2's staging directory.
  leftovers=()
  while IFS= read -r _l; do leftovers+=("$_l"); done < <(
    find "$RUN" -type f \( -name '*.pid' -o -name '*.starttime' -o -name '*.tree' \
         -o -name '*.uncertain' -o -name '*.diag' \) 2>/dev/null | sort)
  if [ ${#leftovers[@]} -gt 0 ]; then
    echo "cycle $((i - 1)) left run state behind:" >&2
    printf '  %s\n' "${leftovers[@]}" >&2
    echo "Its cleanup could not be confirmed, so no further cycle starts. Inspect" >&2
    echo "  this state; do not remove it to make the next cycle run." >&2
    exit 1
  fi

  set +e
  "$RUNNER" --c2-cycle \
    --python-binary "$PY_BINARY" --package-clone "$PKG_CLONE" \
    --clone-manifest "$CLONE_MANIFEST" \
    --workdir "${WORKDIR_BASE}-cycle${i}" \
    --candidate-port "$CAND_PORT" --reference-port "$REF_PORT" \
    --scheduler-port "$SCHED_PORT" \
    ${WORKERS:+--expected-workers "$WORKERS"} \
    ${ALLOW_REUSED:+$ALLOW_REUSED} \
    --label "$label"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    echo >&2
    echo "cycle $i exited $rc. Stopping: a C2 result is three cycles, and two" >&2
    echo "  cycles plus a failure is not two thirds of an answer." >&2
    exit "$rc"
  fi
  labels="$labels $label"
  echo
done

# ============================================================== the conclusion ===
echo "======================================================================"
echo "== C2 across $CYCLES cycles =="
echo "======================================================================"
cd "$HERE"
set +e
# shellcheck disable=SC2086
uv run python -m bench.c2_summary --out "results/${LABEL_PREFIX}_summary.json" $labels
summary_rc=$?
set -e

# Exit 5 is PASS_WITH_INSUFFICIENT_SEED_DIVERSITY: the semantic gate passed and the
# seed question was not answered. It is propagated rather than flattened to 0,
# because a caller checking only the status would otherwise read it as a plain pass —
# and it is not treated as a cycle failure either, because nothing failed. It is
# emphatically not a trigger for a fourth cycle: there is no fourth cycle.
case "$summary_rc" in
  0) echo "C2 complete: PASS" ;;
  5) echo "C2 complete: PASS_WITH_INSUFFICIENT_SEED_DIVERSITY"
     echo "  The three 5.2B gates passed. This run did not observe the seed varying,"
     echo "  so it says nothing about unpinned behaviour. Reporting it as PASS would"
     echo "  be a false report. No fourth cycle is run, and none may be added without"
     echo "  a new authorisation." ;;
  *) echo "C2 complete: FAIL (summary exit $summary_rc)" >&2 ;;
esac
exit "$summary_rc"
