#!/usr/bin/env bash
#
# Finalising a D1 run: the artefacts written after the last request.
#
# On 2026-08-11 the `d1a` run measured everything, wrote its characterization
# result, and then died writing this — a heredoc read `os.environ["LABEL"]` and the
# shell variable was never exported. `d1a_workers.json` was left at zero bytes,
# `d1a_requests.json` was never written, and the runner exited 1. Cleanup still ran
# and passed, so a complete set of measurements ended as a bare non-zero exit with
# no statement of what had failed.
#
# Three things follow, and this file is all three:
#
#   1. **the label is an argument**, so nothing depends on a variable having been
#      exported;
#   2. **it is a sourced function**, so `scripts/test_d1_finalize.sh` can execute
#      the real path rather than a reconstruction of it;
#   3. **a finalization failure is CLASSIFIED**, not merely non-zero. The run's
#      measurements are complete and its artefacts are not, and those are different
#      states from a failed measurement — `INVALID_POST_MEASUREMENT_HARNESS` says
#      so, in a file, rather than leaving a reader to infer it from an exit code.
#
# Requires lib_requests.sh to be sourced. Sourced, never executed.

#: Exit status for a run whose measurements completed and whose artefacts did not.
#: Distinct from 1 so a caller can tell it from a failed gate without parsing text.
D1_FINALIZE_FAILED=6

# finalize_run <label> [results-dir] [mode]
#
# Writes the worker-count record and the request counts. Returns 0, or
# $D1_FINALIZE_FAILED after writing the classification.
#
# The MODE selects the wording of the worker record. It exists because the S2
# performance chain called this and got a file saying "D1 uses one worker per arm by
# design" — D1-specific provenance attached to a latency run, describing neither.
# The shape, the reader and the failure classification are shared; only the sentences
# that say what the count is NOT differ.
finalize_run() {
  local label="$1" results="${2:-results}" mode="${3:-d1}" problems=""

  if ! uv run python -m bench.d1_finalize \
        --label "$label" --results "$results" --mode "$mode" \
        --out "$results/${label}_workers.json"; then
    problems="$problems worker-count-record"
  fi

  if ! request_counts_json "$label" "$results/${label}_requests.json" \
        candidate reference; then
    problems="$problems request-counts"
  fi

  # Both files must exist and be non-empty. `d1a_workers.json` was created and left
  # at zero bytes, which every "did the file appear" check would have passed.
  local f
  for f in "${label}_workers.json" "${label}_requests.json"; do
    if [ ! -s "$results/$f" ]; then
      problems="$problems missing-or-empty:$f"
    fi
  done

  if [ -z "$problems" ]; then
    return 0
  fi

  # The classification, written down. A run that measured everything and failed to
  # record it is not a failed measurement, and must not be reported as one. The
  # sentences below name what the run WAS, which differs by mode: D1 records
  # characterization observations, s2perf records a gate result.
  local kind meaning nots measurements
  case "$mode" in
    s2perf)
      kind="s2perf_finalization"
      measurements="results/${label}_paired.json"
      meaning="the latency gate ran and its result is recorded, but required post-run artefacts were not finalized"
      nots="NOT a gate FAIL and NOT a completed S2 result: the gate's verdict is in results/${label}_paired.json, and the run may not be reported as complete until the artefacts are" ;;
    *)
      kind="d1_finalization"
      measurements="results/${label}_d1.json"
      meaning="characterization observations recorded, but required post-run artefacts were not finalized"
      nots="NOT a D1 characterization FAIL and NOT a completed D1 result: the measurements are in results/${label}_d1.json, and the run may not be reported as complete until the artefacts are" ;;
  esac
  printf '{\n  "kind": "%s",\n  "label": "%s",\n  "mode": "%s",\n  "classification": "INVALID_POST_MEASUREMENT_HARNESS",\n  "problems": "%s",\n  "meaning": "%s",\n  "not": "%s"\n}\n' \
    "$kind" "$label" "$mode" "${problems# }" "$meaning" "$nots" \
    > "$results/${label}_finalization.json"

  echo "INVALID_POST_MEASUREMENT_HARNESS: the measurements were recorded, but" >&2
  echo "  required post-run artefacts were not finalized:" >&2
  echo "  ${problems# }" >&2
  echo "  This is NOT a measurement failure and NOT a completed result." >&2
  echo "  What DID get recorded: $measurements" >&2
  echo "  See $results/${label}_finalization.json" >&2
  return "$D1_FINALIZE_FAILED"
}

# The D1 name, kept because D1's evidence, its tests and its runner path all use it.
d1_finalize() {             # d1_finalize <label> [results-dir]
  finalize_run "$1" "${2:-results}" d1
}
