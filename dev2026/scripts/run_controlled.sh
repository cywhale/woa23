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
# Requires D2b authorisation, which is separate from D2a and is not implied by it.
# Nothing here touches production: not the process on 8050, not its configuration,
# not the shared Dask scheduler on 8786, not ~/python/woa23. It does consume VM24's
# CPU, RAM, page cache and Zarr read I/O — see D2b-request.md section "What this
# costs". "Does not modify production" is not "does not affect the host".

set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

EXPECT_HOST=odb24
PROD_DIR=$HOME/python/woa23
STORE=$PROD_DIR/data
WORK=$HOME/woa23-s1-controlled
REF_DIR=$WORK/reference
CAND_PORT=8051
REF_PORT=8052
SCHED_PORT=8787            # isolated; production's is 8786 and is never touched
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN=$HERE/run

if [ "${WOA23_D2B_GRANTED:-}" != "yes" ]; then
  echo "D2b authorisation not stated. This starts three processes on a production" >&2
  echo "host, including a Dask scheduler and worker. Re-run with" >&2
  echo "WOA23_D2B_GRANTED=yes once it is granted. D2a does not imply D2b." >&2
  exit 3
fi
if [ "$(hostname -s)" != "$EXPECT_HOST" ]; then
  echo "this runs on $EXPECT_HOST only; hostname is $(hostname -s)" >&2
  exit 4
fi
[ -d "$STORE" ] || { echo "store $STORE not found" >&2; exit 4; }

cd "$HERE"
mkdir -p "$RUN" results

# ---------------------------------------------------------------- preflight ---
# Leftover state first: a free port is not an all-clear, because cleanup
# deliberately leaves its pidfile when it refuses to kill.
shopt -s nullglob
leftovers=("$RUN"/*.pid "$RUN"/*.starttime)
if [ ${#leftovers[@]} -gt 0 ]; then
  echo "leftover run state from a previous invocation:" >&2
  printf '  %s\n' "${leftovers[@]}" >&2
  echo "Inspect and remove it deliberately before starting anything." >&2
  exit 1
fi
shopt -u nullglob

for port in "$CAND_PORT" "$REF_PORT" "$SCHED_PORT"; do
  if ss -lntp | grep -qE ":${port}\b"; then
    echo "port ${port} is already in use — aborting rather than touching it" >&2
    ss -lntp | grep -E ":${port}\b" >&2
    exit 1
  fi
done
# Production must be left exactly as it is. Assert it is up now so that the
# post-run check can assert it is still up, unchanged.
PROD_BEFORE="$(ss -lntp | grep -E ':8050\b' || true)"
[ -n "$PROD_BEFORE" ] || { echo "production is not listening on 8050; stopping" >&2; exit 1; }

# --------------------------------------------------------- process identity ---
starttime_of() {
  local raw; raw="$(cat "/proc/$1/stat" 2>/dev/null)" || return 1
  echo "${raw#*) }" | awk '{print $20}'
}
holds_port() { ss -lntp 2>/dev/null | grep -E ":$2\b" | grep -qE "pid=$1,"; }

# name -> pidfile/startfile under run/. Every start records both; every stop
# proves identity before signalling anything.
start_tracked() {           # start_tracked <name> <port> <cmd...>
  local name="$1" port="$2"; shift 2
  "$@" > "$RUN/$name.log" 2>&1 &
  local pid=$!
  echo "$pid" > "$RUN/$name.pid"
  sleep 1
  starttime_of "$pid" > "$RUN/$name.starttime" || {
    echo "could not read start time of $name (pid $pid)" >&2; return 1; }
  echo "$name started (pid $pid, port $port)"
}

stop_tracked() {            # stop_tracked <name> <port>
  local name="$1" port="$2" pid now
  [ -f "$RUN/$name.pid" ] || return 0
  pid="$(cat "$RUN/$name.pid")"
  now="$(starttime_of "$pid" || true)"

  if [ -z "$now" ]; then
    # A dead PID is not a released socket.
    for _ in $(seq 1 20); do
      ss -lntp | grep -qE ":${port}\b" || {
        rm "$RUN/$name.pid"; [ -f "$RUN/$name.starttime" ] && rm "$RUN/$name.starttime"
        echo "$name: process gone, port ${port} free"; return 0; }
      sleep 1
    done
    echo "$name: PID $pid gone but port ${port} STILL HELD — left for inspection" >&2
    return 1
  fi
  if [ "$now" != "$(cat "$RUN/$name.starttime" 2>/dev/null || echo none)" ]; then
    echo "$name: REFUSING TO KILL — PID $pid has a different start time; the PID was" >&2
    echo "  recycled. Left in place for inspection." >&2
    return 1
  fi
  if [ -n "$port" ] && ! holds_port "$pid" "$port"; then
    echo "$name: REFUSING TO KILL — PID $pid no longer holds port ${port}." >&2
    return 1
  fi

  kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 20); do
    if [ -z "$port" ] || ! ss -lntp | grep -qE ":${port}\b"; then
      rm "$RUN/$name.pid"; [ -f "$RUN/$name.starttime" ] && rm "$RUN/$name.starttime"
      echo "$name stopped, port ${port:-n/a} released"; return 0
    fi
    sleep 1
  done
  echo "$name: FAILED TO RELEASE port ${port} — left for inspection" >&2
  return 1
}

cleanup() {
  local rc=$?
  # Reverse order of start. Each refuses to signal anything it cannot prove it
  # started; none of them ever touches a process it did not launch.
  stop_tracked candidate "$CAND_PORT" || true
  stop_tracked reference "$REF_PORT"  || true
  stop_tracked dask_worker ""         || true
  stop_tracked dask_scheduler "$SCHED_PORT" || true

  local prod_after; prod_after="$(ss -lntp | grep -E ':8050\b' || true)"
  if [ -z "$prod_after" ]; then
    echo "WARNING: production is no longer listening on 8050 — investigate" >&2
  else
    echo "production on 8050 still listening, unchanged"
  fi
  return $rc
}
trap cleanup EXIT INT TERM

# ------------------------------------------------- isolated reference source ---
# An unmodified copy of the production app, outside the production directory. The
# copy is read-only and its digests are checked against the originals, so "we ran
# the same code" is verified rather than assumed.
# Refuse an existing directory rather than clearing it. A forced recursive delete on
# a path built from $HOME is one substitution away from being catastrophic, and the
# D2a deploy step already set the standard: verify the target, do not clean it.
if [ -e "$WORK" ]; then
  echo "$WORK already exists. Inspect and remove it deliberately before re-running;" >&2
  echo "this script will not clear a directory it did not create." >&2
  exit 1
fi
mkdir -p "$REF_DIR"
cp "$PROD_DIR/woa23_app.py" "$REF_DIR/"
cp -r "$PROD_DIR/src" "$REF_DIR/src"
# woa23_app.py:63 hard-codes the relative `data/`. A symlink gives it the real store
# without copying 32 GB and without write access through this path.
ln -s "$STORE" "$REF_DIR/data"
chmod -R a-w "$REF_DIR/woa23_app.py" "$REF_DIR/src"

echo "== verifying the reference copy is byte-identical to production's =="
for f in woa23_app.py src/__init__.py src/config.py src/dask_client_manager.py \
         src/woa23_utils.py; do
  a="$(sha256sum "$PROD_DIR/$f" | cut -d' ' -f1)"
  b="$(sha256sum "$REF_DIR/$f" | cut -d' ' -f1)"
  [ "$a" = "$b" ] || { echo "  $f DIFFERS from production ($a vs $b)" >&2; exit 1; }
  echo "  $f  $a"
done

# ------------------------------------------------------- isolated Dask cluster ---
# `src/dask_client_manager.py` reads DASK_SCHEDULER_ADDRESS and falls back to
# tcp://localhost:8786 — production's shared scheduler, serving tide_app and
# mhw_app. The reference must never reach it, so it gets its own on $SCHED_PORT and
# the variable is set explicitly rather than relying on a default.
start_tracked dask_scheduler "$SCHED_PORT" \
  "$HERE/.venv/bin/dask" scheduler --host 127.0.0.1 --port "$SCHED_PORT" --no-dashboard
for _ in $(seq 1 30); do ss -lntp | grep -qE ":${SCHED_PORT}\b" && break; sleep 1; done
ss -lntp | grep -qE ":${SCHED_PORT}\b" || { echo "scheduler did not bind" >&2; exit 1; }
start_tracked dask_worker "" \
  "$HERE/.venv/bin/dask" worker "tcp://127.0.0.1:${SCHED_PORT}" \
  --nworkers 1 --nthreads 1 --memory-limit 8GB --no-dashboard

# ------------------------------------------------------------------- the arms ---
# One venv, both arms. That is the whole point of 5.2A: the packages stop being a
# variable because there is only one set of them.
VENV="$HERE/.venv"

( cd "$REF_DIR" && \
  PYTHONHASHSEED=0 VIRTUAL_ENV="$VENV" \
  DASK_SCHEDULER_ADDRESS="tcp://127.0.0.1:${SCHED_PORT}" \
  exec "$VENV/bin/gunicorn" woa23_app:app -w 1 -k uvicorn.workers.UvicornWorker \
    -b "127.0.0.1:${REF_PORT}" --timeout 120 ) > "$RUN/reference.log" 2>&1 &
REF_PID=$!
echo "$REF_PID" > "$RUN/reference.pid"
sleep 1
starttime_of "$REF_PID" > "$RUN/reference.starttime"

PYTHONHASHSEED=0 VIRTUAL_ENV="$VENV" WOA23_ZARR_STORE="$STORE" \
  "$VENV/bin/gunicorn" api.app:app -w 1 -k uvicorn.workers.UvicornWorker \
  -b "127.0.0.1:${CAND_PORT}" --timeout 120 > "$RUN/candidate.log" 2>&1 &
CAND_PID=$!
echo "$CAND_PID" > "$RUN/candidate.pid"
sleep 1
starttime_of "$CAND_PID" > "$RUN/candidate.starttime"

ready() {                   # ready <port>
  for _ in $(seq 1 30); do
    out="$(curl -s -o /dev/null -w '%{http_code} %{size_download}' \
      "http://127.0.0.1:$1/api/woa23?lon0=135&lat0=15&parameter=temperature" || true)"
    [ "${out%% *}" = "200" ] && [ "${out##* }" -gt 0 ] && return 0
    sleep 1
  done
  return 1
}
ready "$REF_PORT"  || { echo "reference not ready; see $RUN/reference.log" >&2; exit 1; }
ready "$CAND_PORT" || { echo "candidate not ready; see $RUN/candidate.log" >&2; exit 1; }
echo "both arms ready"

# --------------------------------------------------------------- provenance ---
echo "== provenance =="
uv run python -m bench.collect_backend_meta --port "$CAND_PORT" --manifest candidate \
  --expect-argv-contains api.app:app --lockfile uv.lock \
  --out results/d2b_meta_candidate.json
uv run python -m bench.collect_backend_meta --port "$REF_PORT" --manifest reference \
  --expect-argv-contains woa23_app:app --lockfile uv.lock \
  --out results/d2b_meta_reference.json

echo "== the arms must share an environment, or 5.2A proves nothing =="
uv run python - <<'PYEOF' || exit 1
import json, sys
sys.path.insert(0, ".")
from bench.provenance import verify_environment_match, validate_meta
cand = json.load(open("results/d2b_meta_candidate.json"))
ref = json.load(open("results/d2b_meta_reference.json"))
problems = (validate_meta(cand, "candidate") + validate_meta(ref, "reference")
            + verify_environment_match(cand, ref))
if problems:
    print("arms are not comparable:", file=sys.stderr)
    for p in problems:
        print(f"  - {p}", file=sys.stderr)
    raise SystemExit(1)
print(f"both arms: python {cand['env_python_version']}, "
      f"{len(cand['dependencies']['distributions'])} distributions, "
      f"digest {cand['dependencies']['distributions_sha256'][:16]}")
PYEOF

# ------------------------------------------------------------ contract first ---
# The latency gate is not run unless the contract gate passes. A speed number for a
# backend that returns different bytes is not a result.
echo "== contract gate, variant 5.2A (byte-exact), 64 cases per arm =="
uv run python -m bench.contract_diff \
  --candidate "http://127.0.0.1:${CAND_PORT}" \
  --reference "http://127.0.0.1:${REF_PORT}" --variant 5.2A \
  --candidate-meta results/d2b_meta_candidate.json \
  --reference-meta results/d2b_meta_reference.json \
  --out results/d2b_contract.json \
  || { echo "contract gate did not pass — stopping before the latency gate" >&2; exit 1; }

echo "== sample-size planning =="
uv run python -m bench.noise_pilot --base-url "http://127.0.0.1:${REF_PORT}" \
  --warm 25 --out results/d2b_noise_pilot.json

echo "== latency gate, rung 21, variant 5.2A =="
uv run python -m bench.paired_bench \
  --candidate "http://127.0.0.1:${CAND_PORT}" \
  --reference "http://127.0.0.1:${REF_PORT}" \
  --gate-variant 5.2A --warm 21 --include-heavy --margin 0.05 \
  --candidate-meta results/d2b_meta_candidate.json \
  --reference-meta results/d2b_meta_reference.json \
  --out results/d2b_paired.json

echo
echo "artefacts: results/d2b_contract.json results/d2b_paired.json"
echo "           results/d2b_meta_{candidate,reference}.json results/d2b_noise_pilot.json"
echo "== done; the trap now stops all four processes and verifies the ports =="
