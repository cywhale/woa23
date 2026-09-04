#!/usr/bin/env bash
#
# What a label owns, and what refusing to overwrite it has to cover.
#
# The guard this exercises started out checking `results/` alone. Everything below
# that touches `run/<label>/` is there because that version would have passed while
# the service logs and the preserved cleanup state of an earlier run were replaced —
# and a failed run's state is the evidence it exists to keep.
#
#     ./scripts/test_labels.sh
set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_labels.sh
. "$HERE/lib_labels.sh"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}
contains() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

ROOT="$(mktemp -d)"
trap 'rm -rf "$ROOT"' EXIT
mkdir -p "$ROOT/results" "$ROOT/run"

found() { label_artifacts "$ROOT" "$1" | wc -l | tr -d ' '; }
refuse() { refuse_label_collision "$ROOT" "$1" 2>&1; }
refuse_rc() { refuse_label_collision "$ROOT" "$1" >/dev/null 2>&1; echo $?; }

echo "a label nothing answers to is not a collision"
check "no artefacts found" "0" "$(found c2e_cycle1)"
check "and starting is allowed" "0" "$(refuse_rc c2e_cycle1)"
check "with nothing printed" "" "$(refuse c2e_cycle1)"

echo
echo "every kind of artefact a label owns is found, not just results"
: > "$ROOT/results/c2e_cycle1_contract.json"
check "a results file blocks it" "1" "$(refuse_rc c2e_cycle1)"
check "and is named" "yes" "$(contains "$(refuse c2e_cycle1)" "c2e_cycle1_contract.json")"
rm "$ROOT/results/c2e_cycle1_contract.json"

# The half the first version missed. Each of these is checked on its own, with the
# results tree empty, so passing cannot come from finding something else.
for artefact in \
    "run/c2e_cycle1/candidate.log:a service log" \
    "run/c2e_cycle1/reference.log:the other arm's log" \
    "run/c2e_cycle1/candidate.pid:a preserved pidfile" \
    "run/c2e_cycle1/candidate.tree:a preserved process tree" \
    "run/c2e_cycle1/candidate.diag:a cleanup diagnosis" \
    "run/c2e_cycle1/candidate.uncertain:an unrecorded start" \
    "run/c2e_cycle1/candidate.starttime:a recorded identity" \
    "results/c2e_cycle1_meta_candidate.json:provenance" \
    "results/c2e_cycle1_interp_reference.json:import evidence" \
    "results/c2e_cycle1_environment.json:the environment record" \
    "results/c2e_cycle1_shutdown_budget.json:the shutdown budget" \
    "results/c2e_cycle1_ports.json:the port record" \
    "results/c2e_cycle1_clone_integrity_before-reference.json:a clone check"; do
  path="${artefact%%:*}"; what="${artefact#*:}"
  mkdir -p "$(dirname "$ROOT/$path")"
  : > "$ROOT/$path"
  check "$what blocks the label" "1" "$(refuse_rc c2e_cycle1)"
  rm "$ROOT/$path"
  rmdir "$ROOT/run/c2e_cycle1" 2>/dev/null || true
done

echo
echo "an empty state directory counts — it is the label's, and it will be written to"
mkdir -p "$ROOT/run/c2e_cycle1"
check "the directory alone blocks it" "1" "$(refuse_rc c2e_cycle1)"
check "and the message says state and logs are at stake" "yes" \
      "$(contains "$(refuse c2e_cycle1)" "service logs")"
check "and that a failed run's state is the point" "yes" \
      "$(contains "$(refuse c2e_cycle1)" "evidence it exists to keep")"
rmdir "$ROOT/run/c2e_cycle1"

echo
echo "a prefix covers a whole run: three cycles and the summary"
: > "$ROOT/results/c2e_summary.json"
check "the summary alone blocks the prefix" "1" "$(refuse_rc "c2e_")"
check "but not a different run" "0" "$(refuse_rc "c2f_")"
mkdir -p "$ROOT/run/c2e_cycle3"
: > "$ROOT/run/c2e_cycle3/dask_worker.log"
: > "$ROOT/results/c2e_cycle2_contract.json"
check "all three kinds are listed at once" "3" "$(found "c2e_")"
out="$(refuse "c2e_")"
check "the summary is named" "yes" "$(contains "$out" "c2e_summary.json")"
check "a cycle's results are named" "yes" "$(contains "$out" "c2e_cycle2_contract.json")"
check "and a cycle's log directory is named" "yes" "$(contains "$out" "c2e_cycle3")"

echo
echo "the advice is the caller's, because the two callers have different remedies"
check "the driver's advice is carried" "yes" \
      "$(contains "$(refuse_label_collision "$ROOT" "c2e_" "Use a different --label-prefix." 2>&1)" \
                  "different --label-prefix")"
check "the runner's advice is carried" "yes" \
      "$(contains "$(refuse_label_collision "$ROOT" "c2e_" "Choose a fresh staging export." 2>&1)" \
                  "fresh staging export")"

echo
echo "matching is by prefix, and deliberately errs towards refusing"
# `c2e_cycle1` also matches `c2e_cycle11`. There is no such cycle — C2 is three —
# and the alternative, an exact suffix list, goes stale silently the first time a
# runner writes a new artefact. Refusing too much is recoverable; overwriting
# evidence is not.
: > "$ROOT/results/c2e_cycle11_contract.json"
check "a longer label sharing the prefix is treated as a collision" "1" \
      "$(refuse_rc c2e_cycle1)"

echo
suite_summary "$pass" "$fail"
