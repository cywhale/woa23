#!/usr/bin/env bash
#
# Run one rung of the S1 campaign against the candidate on 127.0.0.1:8051.
#
# A file rather than a block quoted in D2a-request.md: a start sequence that is only
# ever pasted cannot be syntax-checked, reviewed as a diff, or re-run identically.
#
#   WOA23_D2A_GRANTED=yes ./scripts/run_candidate.sh 21
#   WOA23_D2A_GRANTED=yes ./scripts/run_candidate.sh 60     # escalation only
#
# ONE RUNG PER INVOCATION, and one authorisation covers one invocation.
#
#   rung 21  full campaign: provenance, sample-size pilot, contract gate, latency
#            gate over all 8 cases.
#   rung 60  escalation ONLY. Requires the rung-21 result, re-runs the latency gate
#            for the cases it left INCONCLUSIVE, and skips the pilot and the contract
#            gate because those do not improve with more latency samples. Re-running
#            them would inflate production traffic for nothing — revision 2 of the
#            request undercounted exactly this way.
#
#   rung 150 refused. It needs a separate signature (D2a-request.md section 2).

set -euo pipefail

# ssh runs a non-login shell, whose PATH does not include ~/.local/bin — which is
# where uv lives on VM24. Without this, `which uv` reports nothing and the tooling
# looks absent when it is merely out of sight.
export PATH="$HOME/.local/bin:$PATH"

PORT=8051
PROD_PORT=8050
STORE=/home/odbadmin/python/woa23/data
EXPECT_HOST=odb24
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIDFILE="$HERE/run/candidate_${PORT}.pid"
STARTFILE="$HERE/run/candidate_${PORT}.starttime"
LOG="$HERE/run/candidate_${PORT}.log"

RUNG="${1:-}"
# Second argument selects which phases run. `full` is the default and is what a
# fresh authorisation buys. `latency-only` exists because the first campaign spent
# its contract-gate budget and then failed on a harness bug: re-running everything
# would charge production for 64 requests whose answer is already recorded and
# unaffected by the fix.
PHASES="${2:-full}"
case "$PHASES" in
  full|latency-only) ;;
  *) echo "second argument must be 'full' or 'latency-only'" >&2; exit 2 ;;
esac

case "$RUNG" in
  21|60) ;;
  150) echo "rung 150 needs a separate signature (D2a-request.md section 2)" >&2; exit 2 ;;
  *) echo "usage: $0 <21|60>" >&2; exit 2 ;;
esac

if [ "${WOA23_D2A_GRANTED:-}" != "yes" ]; then
  echo "D2a authorisation not stated. This starts a process on a production host." >&2
  echo "Re-run with WOA23_D2A_GRANTED=yes once it is granted." >&2
  exit 3
fi

# The authorisation is for VM24 specifically. Without this the same variable would
# let the script start a process anywhere it happened to be copied.
if [ "$(hostname -s)" != "$EXPECT_HOST" ]; then
  echo "this runs on $EXPECT_HOST only; hostname is $(hostname -s)" >&2
  exit 4
fi
if [ ! -d "$STORE" ]; then
  echo "store $STORE not found — wrong host, or the store has moved" >&2
  exit 4
fi

cd "$HERE"
mkdir -p run results

# Port-state helpers. Shared with run_controlled.sh and covered offline by
# scripts/test_ports.sh against a captured `ss` fixture.
# shellcheck source=lib_ports.sh
. "$(dirname "${BASH_SOURCE[0]}")/lib_ports.sh"

st=0; port_held "$PORT" || st=$?
case "$st" in
  0) echo "port ${PORT} is already in use — aborting rather than touching it" >&2
     ss_rows_on_port "$PORT" >&2 || true
     exit 1 ;;
  2) echo "cannot read port state for ${PORT}; refusing to start rather than" >&2
     echo "  assume it is free" >&2
     exit 1 ;;
esac

# A free port is not an all-clear. `stop_candidate` deliberately leaves the pidfile
# when it refuses to kill — a recycled PID, or a process that no longer holds the
# port — and checking only the port would let this run overwrite that record and
# orphan whatever it was pointing at. The leftover state has to be dealt with by a
# person, not stepped over.
for stale in "$PIDFILE" "$STARTFILE"; do
  if [ -e "$stale" ]; then
    echo "leftover state from a previous run: $stale" >&2
    if [ -f "$PIDFILE" ]; then
      old="$(cat "$PIDFILE")"
      if [ -d "/proc/$old" ]; then
        echo "  PID $old is STILL RUNNING: $(tr '\0' ' ' < "/proc/$old/cmdline" 2>/dev/null)" >&2
        echo "  it no longer holds port ${PORT}, which is why it was left alone." >&2
      else
        echo "  PID $old is gone; the file is stale." >&2
      fi
    fi
    echo "Inspect and remove it deliberately before starting another instance." >&2
    exit 1
  fi
done

# --- process identity -------------------------------------------------------
# A pidfile is a claim, not proof. PIDs are recycled, so a stale one can name a
# process that has nothing to do with us. Field 22 of /proc/<pid>/stat is the
# process start time, which distinguishes a recycled PID from the original.

starttime_of() {
  local pid="$1" raw
  raw="$(cat "/proc/$pid/stat" 2>/dev/null)" || return 1
  echo "${raw#*) }" | awk '{print $20}'
}

holds_port() {
  pid_holds_port "$1" "$PORT"
}

stop_candidate() {
  local rc=$? pid now
  [ -f "$PIDFILE" ] || return $rc
  pid="$(cat "$PIDFILE")"

  now="$(starttime_of "$pid" || true)"
  if [ -z "$now" ]; then
    # A dead PID is not a released socket: a surviving worker, or something else
    # that grabbed the port, can still hold it. Reporting success here would leave
    # the next preflight to discover it.
    echo "PID $pid is already gone — checking whether port ${PORT} is free" >&2
    local j
    for j in $(seq 1 20); do
      if port_released "$PORT"; then
        rm "$PIDFILE"
        [ -f "$STARTFILE" ] && rm "$STARTFILE"
        echo "port ${PORT} is free; pidfile removed"
        return $rc
      fi
      sleep 1
    done
    echo "PID $pid is gone but port ${PORT} is STILL HELD:" >&2
    ss_rows_on_port "$PORT" >&2 || true
    echo "pidfile left in place; do not start another instance until this is resolved" >&2
    return 1
  fi
  if [ "$now" != "$(cat "$STARTFILE" 2>/dev/null || echo none)" ]; then
    echo "REFUSING TO KILL: PID $pid has a different start time than the process we" >&2
    echo "started — the PID was recycled. Pidfile left in place for inspection." >&2
    return 1
  fi
  if ! holds_port "$pid"; then
    echo "REFUSING TO KILL: PID $pid no longer holds port ${PORT}. Left in place." >&2
    return 1
  fi

  kill "$pid" 2>/dev/null || true
  local i
  for i in $(seq 1 20); do
    if port_released "$PORT"; then
      # Only once the port is confirmed released. A stop that reports success while
      # something still holds the socket is worse than no stop at all.
      rm "$PIDFILE"
      [ -f "$STARTFILE" ] && rm "$STARTFILE"
      echo "port ${PORT} released, pidfile removed"
      return $rc
    fi
    sleep 1
  done
  echo "FAILED TO RELEASE port ${PORT} after 20 s — left in place for inspection" >&2
  return 1
}
# Cleans up THIS run only, and only after proving the PID is still the process it
# started. It never kills anything else: a preflight that finds the port bound
# aborts above and leaves it alone.
trap stop_candidate EXIT INT TERM

PYTHONHASHSEED=0 WOA23_ZARR_STORE="$STORE" \
  nohup .venv/bin/gunicorn api.app:app -w 1 -k uvicorn.workers.UvicornWorker \
  -b "127.0.0.1:${PORT}" --timeout 120 > "$LOG" 2>&1 &
CANDIDATE_PID=$!
echo "$CANDIDATE_PID" > "$PIDFILE"
sleep 1
starttime_of "$CANDIDATE_PID" > "$STARTFILE" || {
  echo "could not read the start time of PID $CANDIDATE_PID" >&2; exit 1; }

# Ready means 200 AND a non-empty body, polled to a 30 s ceiling. A timeout is a
# failure, not something to wait through.
ready=0
for _ in $(seq 1 30); do
  out="$(curl -s -o /dev/null -w '%{http_code} %{size_download}' \
        "http://127.0.0.1:${PORT}/api/woa23?lon0=135&lat0=15&parameter=temperature" \
        || true)"
  code="${out%% *}"; size="${out##* }"
  if [ "${code:-0}" = "200" ] && [ "${size:-0}" -gt 0 ]; then ready=1; break; fi
  sleep 1
done
if [ "$ready" != "1" ]; then
  echo "candidate not ready after 30 s (last: ${code:-none} ${size:-0}); see $LOG" >&2
  exit 1
fi
echo "candidate ready on ${PORT} (pid $CANDIDATE_PID)"

# State the two things the campaign's validity rests on, before spending a single
# production request on it.
CAND_PY="$(.venv/bin/python --version 2>&1)"
PROD_PY="$(~/.pyenv/versions/py311/bin/python3.11 --version 2>&1)"
echo "candidate interpreter : $CAND_PY"
echo "production interpreter: $PROD_PY"
[ "$CAND_PY" = "$PROD_PY" ] || {
  echo "interpreter mismatch — the candidate would not be measuring production's runtime" >&2
  exit 1; }
CAND_SEED="$(tr '\0' '\n' < "/proc/$CANDIDATE_PID/environ" | grep '^PYTHONHASHSEED=' || true)"
echo "candidate ${CAND_SEED:-PYTHONHASHSEED=<unset>}"
[ "$CAND_SEED" = "PYTHONHASHSEED=0" ] || {
  echo "the candidate's hash seed is not pinned; its output ordering is not reproducible" >&2
  exit 1; }

echo "== provenance =="
uv run python -m bench.collect_backend_meta --port "$PORT" --manifest candidate \
  --expect-argv-contains api.app:app --lockfile uv.lock \
  --out results/meta_candidate.json
uv run python -m bench.collect_backend_meta --port "$PROD_PORT" --manifest reference \
  --expect-argv-contains woa23_app:app \
  --out results/meta_reference.json

if [ "$RUNG" = "21" ] && [ "$PHASES" = "latency-only" ]; then
  # The contract gate and the pilot are not re-run, so the earlier result has to
  # earn being carried forward: same code, same data, same dependencies, and no
  # failure other than the known harness defect. File existence proves none of it.
  for required in results/contract_s1.json results/noise_pilot_candidate.json; do
    [ -f "$required" ] || {
      echo "latency-only needs $required from an earlier run; it is absent" >&2
      exit 1; }
  done
  echo "== validating the carried-over contract result =="
  uv run python - <<'PYEOF' || exit 1
import json, sys
sys.path.insert(0, ".")
from bench.provenance import verify_prior_contract
from bench.contract_cases import all_cases
prior = json.load(open("results/contract_s1.json"))
cand = json.load(open("results/meta_candidate.json"))
ref = json.load(open("results/meta_reference.json"))
problems = verify_prior_contract(prior, cand, expect_variant="5.2B", ref_meta=ref,
                                 expect_case_ids={c.id for c in all_cases()})
if problems:
    print("cannot reuse the earlier contract result:", file=sys.stderr)
    for p in problems:
        print(f"  - {p}", file=sys.stderr)
    raise SystemExit(1)
n = len(prior["results"])
ok = sum(1 for r in prior["results"] if r["verdict"] == "MATCH")
print(f"carried over: {ok}/{n} semantic match; the {n - ok} exception(s) are the "
      f"known harness defect and are recorded as such")
PYEOF

  echo "== latency gate only, rung 21 — pilot and contract gate carried over =="
  uv run python -m bench.paired_bench \
    --candidate "http://127.0.0.1:${PORT}" \
    --reference "https://127.0.0.1:${PROD_PORT}" --insecure \
    --gate-variant 5.2B --warm 21 --include-heavy --margin 0.05 \
    --candidate-meta results/meta_candidate.json \
    --reference-meta results/meta_reference.json \
    --out results/paired_s1_rung21.json
  cp results/paired_s1_rung21.json results/paired_s1.json

elif [ "$RUNG" = "21" ]; then
  echo "== sample-size planning (against the candidate, not production) =="
  uv run python -m bench.noise_pilot --base-url "http://127.0.0.1:${PORT}" \
    --warm 25 --out results/noise_pilot_candidate.json

  echo "== contract gate, variant 5.2B =="
  uv run python -m bench.contract_diff \
    --candidate "http://127.0.0.1:${PORT}" \
    --reference "https://127.0.0.1:${PROD_PORT}" --variant 5.2B --insecure \
    --candidate-meta results/meta_candidate.json \
    --reference-meta results/meta_reference.json \
    --out results/contract_s1.json

  echo "== latency gate, rung 21, all cases =="
  uv run python -m bench.paired_bench \
    --candidate "http://127.0.0.1:${PORT}" \
    --reference "https://127.0.0.1:${PROD_PORT}" --insecure \
    --gate-variant 5.2B --warm 21 --include-heavy --margin 0.05 \
    --candidate-meta results/meta_candidate.json \
    --reference-meta results/meta_reference.json \
    --out results/paired_s1_rung21.json
  cp results/paired_s1_rung21.json results/paired_s1.json

else
  PREV=results/paired_s1_rung21.json
  [ -f "$PREV" ] || { echo "rung 60 is an escalation; $PREV does not exist" >&2; exit 1; }

  # Escalation inherits everything rung 21 established, so everything it
  # established has to still hold: same variant, same rung, valid provenance, no
  # drift, and the same code and data on both arms. File existence proves none of
  # that.
  echo "== validating the rung-21 result before escalating from it =="
  uv run python - "$PREV" <<'PYEOF' || exit 1
import json, sys
sys.path.insert(0, ".")
from bench.provenance import verify_prior_rung
prior = json.load(open(sys.argv[1]))
cand = json.load(open("results/meta_candidate.json"))
ref = json.load(open("results/meta_reference.json"))
from bench.queries import select
expect = {q.id for q in select(None, include_heavy=True)}
problems = verify_prior_rung(
    prior, cand, ref, expect_rung=21, expect_variant="5.2B",
    expect_cases=expect,
    expect_candidate_url="http://127.0.0.1:8051",
    expect_reference_url="https://127.0.0.1:8050")
if problems:
    print("cannot escalate from this result:", file=sys.stderr)
    for p in problems:
        print(f"  - {p}", file=sys.stderr)
    raise SystemExit(1)
print("rung-21 result is a sound basis for escalation")
PYEOF

  mapfile -t UNDECIDED < <(uv run python - "$PREV" <<'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
for r in data.get("results", []):
    if r.get("regression_verdict") == "INCONCLUSIVE":
        print(r["id"])
PYEOF
)
  if [ "${#UNDECIDED[@]}" -eq 0 ]; then
    echo "nothing was INCONCLUSIVE at rung 21 — no escalation needed"
    exit 0
  fi
  echo "== latency gate, rung 60, escalating ${#UNDECIDED[@]} case(s): ${UNDECIDED[*]} =="
  QARGS=()
  for c in "${UNDECIDED[@]}"; do QARGS+=(--query "$c"); done
  uv run python -m bench.paired_bench \
    --candidate "http://127.0.0.1:${PORT}" \
    --reference "https://127.0.0.1:${PROD_PORT}" --insecure \
    --gate-variant 5.2B --warm 60 --include-heavy --margin 0.05 \
    "${QARGS[@]}" \
    --candidate-meta results/meta_candidate.json \
    --reference-meta results/meta_reference.json \
    --out results/paired_s1_rung60.json
  cp results/paired_s1_rung60.json results/paired_s1.json
fi

echo
echo "canonical artefacts:"
echo "  results/paired_s1.json      latency gate (the rung just run)"
echo "  results/contract_s1.json    contract gate (rung 21 only)"
echo "  results/meta_candidate.json results/meta_reference.json"
echo "== done; the trap now stops the candidate and verifies the port =="
