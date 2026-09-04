#!/usr/bin/env bash
#
# The post-measurement finalisation, executed — not reconstructed.
#
# The `d1a` run of 2026-08-11 took every measurement, wrote its characterization
# result, and then died writing the two artefacts that come after: a heredoc read
# `os.environ["LABEL"]` and the shell variable was never exported. Every offline
# test passed, because no test ran that path. So these run the real function, and
# require the real files to appear with the real fields in them.
#
#   1. success  -> workers.json AND requests.json exist, are non-empty, and carry
#                  the fields and the sources they claim
#   2. failure  -> classified as INVALID_POST_MEASUREMENT_HARNESS in a FILE, not
#                  left as a bare non-zero exit
#   3. the empty-file case specifically, because that is what actually happened:
#                  the file was created and left at zero bytes
#
#     ./scripts/test_d1_finalize.sh
set -uo pipefail
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_suite_summary.sh"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
export RUN="$(mktemp -d)"
# shellcheck source=lib_requests.sh
. "$HERE/lib_requests.sh"
# shellcheck source=lib_d1_finalize.sh
. "$HERE/lib_d1_finalize.sh"

pass=0; fail=0
check() {
  if [ "$2" = "$3" ]; then
    pass=$((pass + 1)); echo "  ok   $1"
  else
    fail=$((fail + 1)); echo "  FAIL $1 — expected [$2], got [$3]"
  fi
}
contains() { case "$1" in *"$2"*) echo yes ;; *) echo no ;; esac; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$REPO"

make_meta() {               # make_meta <dir> <label> <arm> <workers>
  mkdir -p "$1"
  local app=woa23_app:app
  [ "$3" = candidate ] && app=api.app:app
  cat > "$1/${2}_meta_${3}.json" <<JSON
{"kind": "backend_meta", "label": "$3",
 "launch_argv": ["/home/odbadmin/.pyenv/versions/py311/bin/python3.11", "-S", "-m",
   "gunicorn", "$app", "-w", "$4", "-k", "uvicorn.workers.UvicornWorker",
   "--graceful-timeout", "10", "-b", "127.0.0.1:18141", "--timeout", "120"],
 "launch_command": "python3.11 -S -m gunicorn $app -w $4",
 "worker_pids": [3699659]}
JSON
}

echo "1. the happy path really produces both artefacts, with their fields"
R="$WORK/ok"
make_meta "$R" d1x candidate 1
make_meta "$R" d1x reference 1
REQUEST_COUNTS_DIR="$WORK/ok_counts"
request_add candidate readiness 1
request_add candidate store_probe 2
request_add candidate characterization 4
request_add candidate recovery 4
request_add reference readiness 3
request_add reference store_probe 2
request_add reference characterization 4
request_add reference recovery 4

d1_finalize d1x "$R" >/dev/null 2>&1
check "it returns success" "0" "$?"
check "workers.json exists and is NOT empty" "yes" \
      "$([ -s "$R/d1x_workers.json" ] && echo yes || echo no)"
check "requests.json exists and is NOT empty" "yes" \
      "$([ -s "$R/d1x_requests.json" ] && echo yes || echo no)"
check "no classification file was written" "no" \
      "$([ -e "$R/d1x_finalization.json" ] && echo yes || echo no)"

py() { python3 -c "import json,sys;d=json.load(open(sys.argv[1]));print($1)" "$2"; }
check "workers.json says one worker per arm" "1" "$(py "d['arm_workers']" "$R/d1x_workers.json")"
check "and that it was not derived from a production measurement" "False" \
      "$(py "d['derived_from_production_measurement']" "$R/d1x_workers.json")"
check "the count is read from the candidate's own argv" "1" \
      "$(py "d['per_arm']['candidate']['worker_count']" "$R/d1x_workers.json")"
check "and the reference's" "1" \
      "$(py "d['per_arm']['reference']['worker_count']" "$R/d1x_workers.json")"
check "the full launch argv is carried, not just the count" "yes" \
      "$(contains "$(py "' '.join(d['per_arm']['candidate']['launch_argv'])" "$R/d1x_workers.json")" "-w 1")"
check "the sentence that separates D1 from C2 travels with it" "yes" \
      "$(contains "$(py "d['note']" "$R/d1x_workers.json")" "makes no claim about multi-worker")"
check "and it says the flag asserts rather than sets" "yes" \
      "$(contains "$(py "d['assertion']" "$R/d1x_workers.json")" "sets nothing")"

check "requests.json counts attempts, not successes" "True" \
      "$(py "d['counts_attempts_not_successes']" "$R/d1x_requests.json")"
check "the readiness attempts differ per arm, as measured" "1 3" \
      "$(py "str(d['per_arm']['candidate']['readiness'])+' '+str(d['per_arm']['reference']['readiness'])" "$R/d1x_requests.json")"
check "each arm's countable total is its stages" "11 13" \
      "$(py "str(d['per_arm']['candidate']['total'])+' '+str(d['per_arm']['reference']['total'])" "$R/d1x_requests.json")"
check "and the grand total is both" "24" "$(py "d['total']" "$R/d1x_requests.json")"

echo
echo "2. the label arrives as an argument, not from the environment"
# The actual defect: `os.environ["LABEL"]` with LABEL never exported. With the
# variable unset entirely, finalisation must still work.
R2="$WORK/noenv"
make_meta "$R2" d1y candidate 1
make_meta "$R2" d1y reference 1
REQUEST_COUNTS_DIR="$WORK/noenv_counts"
request_add candidate readiness 1
env -u LABEL bash -c "cd '$REPO' && RUN='$RUN' REQUEST_COUNTS_DIR='$WORK/noenv_counts' \
  . scripts/lib_requests.sh && . scripts/lib_d1_finalize.sh && \
  d1_finalize d1y '$R2'" >/dev/null 2>&1
check "finalisation succeeds with LABEL unset" "0" "$?"
check "and the file names the label it was given" "d1y" \
      "$(py "d['label']" "$R2/d1y_workers.json")"
check "the artefact is not empty" "yes" \
      "$([ -s "$R2/d1y_workers.json" ] && echo yes || echo no)"

echo
echo "3. a failure is CLASSIFIED, not left as a bare exit code"
R3="$WORK/broken"
mkdir -p "$R3"                          # no meta files at all
REQUEST_COUNTS_DIR="$WORK/broken_counts"
request_add candidate readiness 1
out="$(d1_finalize d1z "$R3" 2>&1)"
rc=$?
check "it returns the finalisation status, not 1" "$D1_FINALIZE_FAILED" "$rc"
check "and that status is distinct from a failed gate" "yes" \
      "$([ "$D1_FINALIZE_FAILED" != 1 ] && echo yes || echo no)"
check "a classification file is written" "yes" \
      "$([ -s "$R3/d1z_finalization.json" ] && echo yes || echo no)"
check "naming the classification" "INVALID_POST_MEASUREMENT_HARNESS" \
      "$(py "d['classification']" "$R3/d1z_finalization.json")"
check "and saying the observations were recorded" "yes" \
      "$(contains "$(py "d['meaning']" "$R3/d1z_finalization.json")" \
                  "characterization observations recorded")"
check "and that the artefacts were not finalized" "yes" \
      "$(contains "$(py "d['meaning']" "$R3/d1z_finalization.json")" \
                  "were not finalized")"
check "it refuses to be read as a characterization FAIL" "yes" \
      "$(contains "$(py "d['not']" "$R3/d1z_finalization.json")" \
                  "NOT a D1 characterization FAIL")"
check "or as a completed D1 result" "yes" \
      "$(contains "$(py "d['not']" "$R3/d1z_finalization.json")" \
                  "NOT a completed D1 result")"
check "the message on stderr says the same" "yes" \
      "$(contains "$out" "INVALID_POST_MEASUREMENT_HARNESS")"

echo
echo "4. a file that exists and is EMPTY is a failure — which is what happened"
R4="$WORK/empty"
make_meta "$R4" d1w candidate 1
make_meta "$R4" d1w reference 1
REQUEST_COUNTS_DIR="$WORK/empty_counts"
request_add candidate readiness 1
# Simulate exactly the observed state: the file appears, with nothing in it.
d1_finalize d1w "$R4" >/dev/null 2>&1
: > "$R4/d1w_workers.json"
check "a zero-byte artefact is not accepted by the size test" "no" \
      "$([ -s "$R4/d1w_workers.json" ] && echo yes || echo no)"
# And through the function, with the module made to fail.
R5="$WORK/empty2"
mkdir -p "$R5"
printf 'not json\n' > "$R5/d1v_meta_candidate.json"
printf 'not json\n' > "$R5/d1v_meta_reference.json"
REQUEST_COUNTS_DIR="$WORK/empty2_counts"
request_add candidate readiness 1
d1_finalize d1v "$R5" >/dev/null 2>&1
check "unreadable provenance is a classified failure" "$D1_FINALIZE_FAILED" "$?"
check "and the record still names what went wrong" "yes" \
      "$(contains "$(py "d['problems']" "$R5/d1v_finalization.json")" "worker-count-record")"
# The worker record is written even on failure: a record of what went wrong beats
# no record, and an empty file is the thing this replaced.
check "workers.json is still written, with the problem in it" "yes" \
      "$([ -s "$R5/d1v_workers.json" ] && echo yes || echo no)"

echo
echo "5. the runner calls this, and classifies what it returns"
runner="$(cat "$HERE/run_controlled.sh")"
check "the runner sources the finalisation library" "yes" \
      "$(contains "$runner" "lib_d1_finalize.sh")"
check "it passes the label as an argument" "yes" \
      "$(contains "$runner" 'd1_finalize "$LABEL" results')"
# The precise invariant, rather than "no heredoc may read LABEL" — several do, and
# legitimately, because their invocation puts LABEL= in the command's environment.
# What was violated is the pairing: a heredoc that READS the variable whose
# invocation does not SET it. That is the check, and it is computed on its own lines
# rather than inside the check's argument, where a nested heredoc will not parse.
label_pairing="$(python3 - "$HERE/run_controlled.sh" <<'PY'
import re
import sys

lines = open(sys.argv[1]).read().splitlines()
bad = []
for i, line in enumerate(lines):
    if 'os.environ["LABEL"]' not in line:
        continue
    j = i
    while j >= 0 and "uv run python" not in lines[j]:
        j -= 1
    if j < 0:
        bad.append("line %d: no invocation found above it" % (i + 1))
        continue
    prefix = lines[j]
    k = j - 1
    while k >= 0 and lines[k].rstrip().endswith("\\"):
        prefix = lines[k] + " " + prefix
        k -= 1
    if not re.search(r"\bLABEL=", prefix):
        bad.append("line %d: its invocation does not set LABEL" % (i + 1))
print("ok" if not bad else "; ".join(bad))
PY
)"
check "every heredoc reading LABEL has LABEL= on its invocation" "ok" \
      "$label_pairing"
check "and it prints the classification on failure" "yes" \
      "$(contains "$runner" "D1 classification: INVALID_POST_MEASUREMENT_HARNESS")"
check "saying the observations are still recorded" "yes" \
      "$(contains "$runner" "observations ARE recorded")"

echo
echo "6. the status reaches the top level — the trap must not swallow it"
# The failure being guarded: a run whose measurements are complete and whose
# artefacts are not must not exit 0, and must not be flattened to a plain 1 that
# reads like a failed gate. `run_controlled.sh` runs cleanup from an EXIT trap, and
# an EXIT trap is exactly the sort of thing that quietly rewrites an exit status.
#
# The mechanism first, on a fixture that reproduces the runner's shape: a cleanup
# trap that inspects $?, does work, and returns — then `exit "$finalize_rc"`.
FIX="$WORK/exitprop.sh"
cat > "$FIX" <<'SH'
cleanup() {
  local rc=$?
  echo "trap saw rc=$rc"
  CLEANUP_FAILED=${FAKE_CLEANUP_FAILED:-0}
  if [ "$CLEANUP_FAILED" != "0" ]; then
    echo "CLEANUP DID NOT COMPLETE" >&2
    if [ "$rc" -eq 0 ]; then exit 1; fi
  fi
  return $rc
}
trap cleanup EXIT INT TERM
finalize_rc="$1"
if [ "$finalize_rc" -ne 0 ]; then
  echo "D1 classification: INVALID_POST_MEASUREMENT_HARNESS"
  exit "$finalize_rc"
fi
exit 0
SH

bash "$FIX" 6 >/dev/null 2>&1
check "a finalisation failure exits 6 at the top level" "6" "$?"
bash "$FIX" 0 >/dev/null 2>&1
check "and a clean run still exits 0" "0" "$?"
check "6 is not 1: a failed gate and an unfinalised run are distinguishable" "yes" \
      "$([ "$D1_FINALIZE_FAILED" -ne 1 ] && echo yes || echo no)"

# With cleanup ALSO failing. The 6 must survive: a cleanup failure may upgrade a
# zero, and must not overwrite a non-zero with something less specific.
FAKE_CLEANUP_FAILED=1 bash "$FIX" 6 >/dev/null 2>&1
check "a failing cleanup does not overwrite the 6" "6" "$?"
FAKE_CLEANUP_FAILED=1 bash "$FIX" 0 >/dev/null 2>&1
check "but a failing cleanup does turn a clean run into a failure" "1" "$?"

# And the property the fixture depends on, asserted directly: an EXIT trap that
# RETURNS cannot change the status. If it ever called `exit`, it could.
cat > "$WORK/trapret.sh" <<'SH'
t() { local rc=$?; true; return 0; }
trap t EXIT
exit 6
SH
bash "$WORK/trapret.sh" >/dev/null 2>&1
check "an EXIT trap that returns 0 leaves the status alone" "6" "$?"

echo
echo "7. the real runner has that shape, and its cleanup never calls exit"
check "the runner exits with the finalisation status" "yes" \
      "$(contains "$runner" 'exit "$finalize_rc"')"
check "and does not flatten it to 1" "no" \
      "$(contains "$runner" 'exit 1  # finalisation')"
cleanup_body="$(awk '/^cleanup\(\) \{/,/^\}/' "$HERE/run_controlled.sh")"
# It calls exit exactly once, and only to raise a zero. More than one, or an exit
# outside that branch, could replace a specific status with a less specific one.
check "the cleanup trap calls exit exactly once" "1" \
      "$(printf '%s\n' "$cleanup_body" | grep -cE '^[[:space:]]*exit( |$)' || true)"
check "and never exits with the finalisation status" "no" \
      "$(contains "$cleanup_body" "exit 6")"
check "it returns rc rather than replacing it" "yes" \
      "$(contains "$cleanup_body" "return \$rc")"
# `return 1` from an EXIT trap does not set the exit status — on bash 3.2 or 5.2.
# The trap has to EXIT to raise it, and only upward: a non-zero status is already a
# failure and is more specific than 1.
check "the cleanup trap raises a zero by exiting, not by returning" "yes" \
      "$(contains "$cleanup_body" "exit 1")"
check "and says why returning would not work" "yes" \
      "$(contains "$cleanup_body" "never sets the exit status")"
check "it still only raises a zero" "yes" \
      "$(contains "$cleanup_body" 'if [ "$rc" -eq 0 ]; then')"
check "the trap is armed for EXIT" "yes" \
      "$(contains "$runner" "trap cleanup EXIT")"

echo
echo "8. a failed finalisation keeps the D1 evidence it already wrote"
# The measurements are the point. An unfinalised run must leave them exactly where
# they are — nothing is cleaned up, retried or truncated on the way out.
R6="$WORK/evidence"
mkdir -p "$R6"
printf '{"kind":"d1_characterization","outcome":"CHARACTERIZATION_RECORDED"}\n' \
  > "$R6/d1u_d1.json"
printf '{"case_id":"D1-DEPTH-OOR-tp13"}\n' > "$R6/d1u_d1_candidate.jsonl"
printf '{"case_id":"D1-DEPTH-OOR-tp13"}\n' > "$R6/d1u_d1_reference.jsonl"
before="$( (sha256sum "$R6"/d1u_d1* 2>/dev/null || shasum -a 256 "$R6"/d1u_d1*) \
           | cut -d' ' -f1 | tr '\n' ' ')"
REQUEST_COUNTS_DIR="$WORK/evidence_counts"
request_add candidate readiness 1
d1_finalize d1u "$R6" >/dev/null 2>&1
check "finalisation failed, as set up" "$D1_FINALIZE_FAILED" "$?"
after="$( (sha256sum "$R6"/d1u_d1* 2>/dev/null || shasum -a 256 "$R6"/d1u_d1*) \
          | cut -d' ' -f1 | tr '\n' ' ')"
check "the characterization result is untouched" "$before" "$after"
check "and still present" "yes" \
      "$([ -s "$R6/d1u_d1.json" ] && echo yes || echo no)"
check "both arms' raw records too" "yes" \
      "$([ -s "$R6/d1u_d1_candidate.jsonl" ] && [ -s "$R6/d1u_d1_reference.jsonl" ] \
         && echo yes || echo no)"
check "and the classification names where they are" "yes" \
      "$(contains "$(py "d['not']" "$R6/d1u_finalization.json")" "results/d1u_d1.json")"

echo
suite_summary "$pass" "$fail"
