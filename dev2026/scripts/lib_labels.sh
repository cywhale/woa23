#!/usr/bin/env bash
#
# What a label owns on disk, and the refusal to write over any of it.
#
# A label is not just a filename prefix in results/. It names, at minimum:
#
#   results/<label>_contract.json            the gate's own verdict
#   results/<label>_meta_{candidate,reference}.json
#   results/<label>_interp_{candidate,reference}.json
#   results/<label>_environment.json
#   results/<label>_shutdown_budget.json
#   results/<label>_ports.json
#   results/<label>_clone_integrity_*.json
#   results/<label>_paired.json, _noise_pilot_*.json   (D2b modes)
#   run/<label>/                             STATE and SERVICE LOGS:
#                                            *.pid, *.starttime, *.tree, *.diag,
#                                            *.uncertain, candidate.log,
#                                            reference.log, dask_*.log
#
# The first version of this guard checked results/ alone. That is the half of the
# evidence that is easy to think of and the less dangerous half to lose: `run/<label>/`
# holds the service logs a failure is diagnosed from, and the state files a cleanup
# deliberately leaves behind when it refuses to kill something. Overwriting those
# would destroy exactly the evidence that a preserved failure exists to keep.
#
# So the guard is a glob over both trees rather than a list of known suffixes: a
# suffix list goes stale the first time a runner writes a new artefact, and it goes
# stale silently, which is the same failure mode as not checking at all.
#
# Sourced, never executed. Covered by scripts/test_labels.sh.

# Every existing path a label (or a label prefix) owns, one per line, possibly empty.
#
# $1 = the repository root that holds results/ and run/
# $2 = a label ("c2e_cycle1") or a prefix ("c2e_") — matched as "starts with"
label_artifacts() {
  local root="$1" pre="$2" f
  # `if` rather than `[ -e ] &&`: an unmatched glob leaves the pattern itself in $f,
  # and under `set -e` a failing && list at the end of a loop body ends the script —
  # turning "nothing found", the good case, into an unexplained exit.
  for f in "$root/results/$pre"* "$root/run/$pre"*; do
    if [ -e "$f" ]; then printf '%s\n' "$f"; fi
  done
}

# Refuse to start if anything already answers to this label. Status 0 to proceed.
#
# $3 is what to suggest instead, since the two callers have different remedies.
refuse_label_collision() {
  local root="$1" pre="$2" advice="${3:-}" existing
  existing="$(label_artifacts "$root" "$pre")"
  [ -n "$existing" ] || return 0
  echo "'$pre' already names evidence on disk:" >&2
  printf '  %s\n' $existing >&2
  echo "Refusing to start: this run would write over it. Results, provenance," >&2
  echo "  service logs and any preserved cleanup state all live under the label," >&2
  echo "  and a failed run's state is the evidence it exists to keep." >&2
  [ -z "$advice" ] || echo "  $advice" >&2
  return 1
}
