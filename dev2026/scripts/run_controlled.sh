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
# CPU, RAM, page cache and Zarr read I/O — see D2b-request.md, "What this costs".
# "Does not modify production" is not "does not affect the host".

set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

EXPECT_HOST=odb24
EXPECT_PY=3.11.4
PROD_DIR=$HOME/python/woa23
PROD_PY=$HOME/.pyenv/versions/py311/bin/python3.11
STORE=$PROD_DIR/data
WORK=$HOME/woa23-s1-controlled
REF_DIR=$WORK/reference
CAND_PORT=8051
REF_PORT=8052
SCHED_PORT=8787            # isolated; production's is 8786 and is never touched
PROD_PORT=8050
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN=$HERE/run
VENV=$HERE/.venv

if [ "${WOA23_D2B_GRANTED:-}" != "yes" ]; then
  echo "D2b authorisation not stated. This starts FOUR processes on a production" >&2
  echo "host: a Dask scheduler, a Dask worker, an unmodified reference API and the" >&2
  echo "candidate API. Re-run with WOA23_D2B_GRANTED=yes once it is granted." >&2
  echo "D2a does not imply D2b." >&2
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
uv sync --locked --python "$PROD_PY" >&2

uv run python - <<PYEOF || exit 1
import hashlib, json, subprocess, sys
sys.path.insert(0, ".")
from bench.collect_backend_meta import dependencies

venv_py = "$VENV/bin/python"
want_py = "$EXPECT_PY"

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
           "distributions_sha256": deps["distributions_sha256"],
           "n_distributions": len(deps["distributions"]),
           "distributions": deps["distributions"]},
          open("results/d2b_environment.json", "w"), indent=2)
print(f"  python {deps['python_version']}  "
      f"{len(deps['distributions'])} distributions  "
      f"lock {deps['lockfile_sha256'][:16]}  dists {deps['distributions_sha256'][:16]}")
PYEOF

# ================================================================ preflight ===
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

# --------------------------------------------------------- port parsing ---
# `ss` writes the local address as `addr:port`, and the address half may contain
# colons of its own. Every port test here used to be a substring match, under which
# `[fe80::8050]:9000` — an unrelated service — read as a listener on 8050, and a
# `\b`-anchored grep additionally matched an IPv6 address ending in `:50` when
# looking for port 50. The port is now taken from the local-address column, after
# the last colon, and compared as a number. Only LISTEN rows count, which also
# drops the header.
ss_rows_on_port() {
  ss -lntp 2>/dev/null | awk -v want="$1" '
    $1 == "LISTEN" && NF >= 4 {
      n = split($4, part, ":")
      if (n >= 2 && part[n] ~ /^[0-9]+$/ && part[n] + 0 == want + 0) print
    }'
}
port_held()      { [ -n "$(ss_rows_on_port "$1")" ]; }
pid_holds_port() { ss_rows_on_port "$2" | grep -qE "pid=$1,"; }
pids_on_port() {            # every PID holding the socket, across all ss rows
  ss_rows_on_port "$1" | grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -un | tr '\n' ' '
}

for port in "$CAND_PORT" "$REF_PORT" "$SCHED_PORT"; do
  if port_held "$port"; then
    echo "port ${port} is already in use — aborting rather than touching it" >&2
    ss_rows_on_port "$port" >&2
    exit 1
  fi
done

# ------------------------------------------------------------ /proc helpers ---
starttime_of() {
  local raw; raw="$(cat "/proc/$1/stat" 2>/dev/null)" || return 1
  echo "${raw#*) }" | awk '{print $20}'
}
ppid_of() {
  local raw; raw="$(cat "/proc/$1/stat" 2>/dev/null)" || return 1
  echo "${raw#*) }" | awk '{print $2}'
}
master_of() {               # the one PID whose parent is not in the set
  local pids="$1" roots="" p pp
  [ -n "$pids" ] || return 1
  for p in $pids; do
    pp="$(ppid_of "$p")" || return 1      # unreadable parent -> ambiguous
    case " $pids " in *" $pp "*) ;; *) roots="$roots $p" ;; esac
  done
  set -- $roots
  [ $# -eq 1 ] || return 1
  echo "$1"
}
BOOT_ID="$(cat /proc/sys/kernel/random/boot_id)"

# ------------------------------------------- production's identity, recorded ---
# "Someone is still listening on 8050" is not the same as "production is the process
# it was". A restart between the two checks would leave the port occupied and every
# comparison in this run describing a different backend.
PROD_PIDS_BEFORE="$(pids_on_port "$PROD_PORT")"
[ -n "$PROD_PIDS_BEFORE" ] || { echo "production is not listening on $PROD_PORT" >&2; exit 1; }
PROD_MASTER_BEFORE="$(master_of "$PROD_PIDS_BEFORE")" || {
  echo "cannot identify production's master among [$PROD_PIDS_BEFORE]" >&2; exit 1; }
PROD_START_BEFORE="$(starttime_of "$PROD_MASTER_BEFORE")"
echo "production master $PROD_MASTER_BEFORE (start $PROD_START_BEFORE), listeners: $PROD_PIDS_BEFORE"

# ================================================== tracked process handling ===
CLEANUP_FAILED=0

start_tracked() {           # start_tracked <name> <port|""> <cmd...>
  local name="$1" port="$2"; shift 2
  [ -f "$RUN/$name.pid" ] && { echo "$name already tracked" >&2; return 1; }
  "$@" > "$RUN/$name.log" 2>&1 &
  local pid=$!
  echo "$pid" > "$RUN/$name.pid"
  sleep 1
  starttime_of "$pid" > "$RUN/$name.starttime" || {
    echo "could not read the start time of $name (pid $pid)" >&2; return 1; }
  echo "$name started (pid $pid${port:+, port $port})"
}

stop_tracked() {            # stop_tracked <name> <port|"">
  local name="$1" port="${2:-}" pid now
  [ -f "$RUN/$name.pid" ] || return 0
  pid="$(cat "$RUN/$name.pid")"
  now="$(starttime_of "$pid" || true)"

  if [ -z "$now" ]; then
    # A dead PID is not a released socket.
    if [ -n "$port" ]; then
      local i
      for i in $(seq 1 20); do
        port_held "$port" || break
        sleep 1
      done
      if port_held "$port"; then
        echo "$name: PID $pid gone but port ${port} STILL HELD — left for inspection" >&2
        return 1
      fi
    fi
    rm "$RUN/$name.pid"; [ -f "$RUN/$name.starttime" ] && rm "$RUN/$name.starttime"
    echo "$name: process already gone, port ${port:-n/a} free"
    return 0
  fi
  if [ "$now" != "$(cat "$RUN/$name.starttime" 2>/dev/null || echo none)" ]; then
    echo "$name: REFUSING TO KILL — PID $pid has a different start time; the PID was" >&2
    echo "  recycled. Left in place for inspection." >&2
    return 1
  fi
  if [ -n "$port" ] && ! pid_holds_port "$pid" "$port"; then
    echo "$name: REFUSING TO KILL — PID $pid no longer holds port ${port}." >&2
    return 1
  fi

  kill "$pid" 2>/dev/null || true
  local i
  for i in $(seq 1 20); do
    if [ -z "$port" ]; then
      [ -d "/proc/$pid" ] || break
    elif ! port_held "$port"; then
      break
    fi
    sleep 1
  done
  if [ -n "$port" ] && port_held "$port"; then
    echo "$name: FAILED TO RELEASE port ${port} — left for inspection" >&2
    return 1
  fi
  if [ -z "$port" ] && [ -d "/proc/$pid" ]; then
    echo "$name: PID $pid did not exit — left for inspection" >&2
    return 1
  fi
  rm "$RUN/$name.pid"; [ -f "$RUN/$name.starttime" ] && rm "$RUN/$name.starttime"
  echo "$name stopped, port ${port:-n/a} released"
}

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

  # Production must be the same process it was, not merely a process.
  local after master_after start_after boot_after
  after="$(pids_on_port "$PROD_PORT")"
  boot_after="$(cat /proc/sys/kernel/random/boot_id)"
  if [ "$boot_after" != "$BOOT_ID" ]; then
    echo "WARNING: the host rebooted during this run" >&2; CLEANUP_FAILED=1
  elif [ -z "$after" ]; then
    echo "WARNING: nothing is listening on $PROD_PORT any more" >&2; CLEANUP_FAILED=1
  else
    master_after="$(master_of "$after" || true)"
    start_after="$(starttime_of "${master_after:-0}" || true)"
    # The master alone is not the whole picture. gunicorn's workers hold the same
    # socket, and a worker that died and respawned changes the PID set while leaving
    # the master untouched — which is exactly the collateral effect this run could
    # cause by competing for the host's CPU and page cache. Compare the full set.
    if [ "$master_after" != "$PROD_MASTER_BEFORE" ] || \
       [ "$start_after" != "$PROD_START_BEFORE" ]; then
      echo "WARNING: production's master on $PROD_PORT is not the process it was —" >&2
      echo "  production restarted during this run" >&2
      echo "  before: master $PROD_MASTER_BEFORE start $PROD_START_BEFORE [$PROD_PIDS_BEFORE]" >&2
      echo "  after : master ${master_after:-?} start ${start_after:-?} [$after]" >&2
      CLEANUP_FAILED=1
    elif [ "$after" != "$PROD_PIDS_BEFORE" ]; then
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
mkdir -p "$REF_DIR"
cp "$PROD_DIR/woa23_app.py" "$REF_DIR/"
cp -r "$PROD_DIR/src" "$REF_DIR/src"
# woa23_app.py:63 hard-codes the relative `data/`. A symlink gives it the real store
# without copying 31.9 GiB and without a writable path to it.
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

# ===================================================== isolated Dask cluster ===
# `src/dask_client_manager.py` reads DASK_SCHEDULER_ADDRESS and falls back to
# tcp://localhost:8786 — production's shared scheduler, serving tide_app and
# mhw_app. The reference must never reach it, so it gets its own on $SCHED_PORT and
# the variable is set explicitly rather than relying on a default being overridden.
start_tracked dask_scheduler "$SCHED_PORT" \
  "$VENV/bin/dask" scheduler --host 127.0.0.1 --port "$SCHED_PORT" --no-dashboard
for _ in $(seq 1 30); do port_held "$SCHED_PORT" && break; sleep 1; done
port_held "$SCHED_PORT" || { echo "scheduler did not bind" >&2; exit 1; }
start_tracked dask_worker "" \
  "$VENV/bin/dask" worker "tcp://127.0.0.1:${SCHED_PORT}" \
  --nworkers 1 --nthreads 1 --memory-limit 8GB --no-dashboard

# ==================================================================== the arms ===
# One venv, both arms. That is the whole point of 5.2A: the packages stop being a
# variable because there is only one set of them. Both go through start_tracked, so
# identity is recorded the same way for every process this run owns.
start_tracked reference "$REF_PORT" \
  env -C "$REF_DIR" PYTHONHASHSEED=0 VIRTUAL_ENV="$VENV" \
    DASK_SCHEDULER_ADDRESS="tcp://127.0.0.1:${SCHED_PORT}" \
    "$VENV/bin/gunicorn" woa23_app:app -w 1 -k uvicorn.workers.UvicornWorker \
    -b "127.0.0.1:${REF_PORT}" --timeout 120

start_tracked candidate "$CAND_PORT" \
  env -C "$HERE" PYTHONHASHSEED=0 VIRTUAL_ENV="$VENV" WOA23_ZARR_STORE="$STORE" \
    "$VENV/bin/gunicorn" api.app:app -w 1 -k uvicorn.workers.UvicornWorker \
    -b "127.0.0.1:${CAND_PORT}" --timeout 120

ready() {                   # ready <port>
  local out
  for _ in $(seq 1 30); do
    # Bounded: without --max-time a hung backend makes each poll block for the
    # default connect/read timeouts and the "30 s ceiling" is not one.
    out="$(curl -s --max-time 5 -o /dev/null -w '%{http_code} %{size_download}' \
      "http://127.0.0.1:$1/api/woa23?lon0=135&lat0=15&parameter=temperature" || true)"
    [ "${out%% *}" = "200" ] && [ "${out##* }" -gt 0 ] && return 0
    sleep 1
  done
  return 1
}
ready "$REF_PORT"  || { echo "reference not ready; see $RUN/reference.log" >&2; exit 1; }
ready "$CAND_PORT" || { echo "candidate not ready; see $RUN/candidate.log" >&2; exit 1; }
echo "both arms ready"

# ================================================================= provenance ===
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
from bench.provenance import (validate_meta, verify_environment_match,
                              verify_environment_record)
cand = json.load(open("results/d2b_meta_candidate.json"))
ref = json.load(open("results/d2b_meta_reference.json"))
env = json.load(open("results/d2b_environment.json"))
problems = (validate_meta(cand, "candidate") + validate_meta(ref, "reference")
            # do the two arms agree with each other?
            + verify_environment_match(cand, ref)
            # and is what they agree on the environment this run actually built?
            # Two arms sharing a stale .venv agree perfectly and prove nothing.
            + verify_environment_record(env, cand, "candidate")
            + verify_environment_record(env, ref, "reference"))
if problems:
    print("arms are not comparable:", file=sys.stderr)
    for p in problems:
        print(f"  - {p}", file=sys.stderr)
    raise SystemExit(1)
print(f"both arms: python {cand['env_python_version']}, "
      f"{len(cand['dependencies']['distributions'])} distributions, "
      f"digest {cand['dependencies']['distributions_sha256'][:16]}")
PYEOF

# =============================================================== contract first ===
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

echo "== latency gate, rung 21, variant 5.2A =="
uv run python -m bench.paired_bench \
  --candidate "http://127.0.0.1:${CAND_PORT}" \
  --reference "http://127.0.0.1:${REF_PORT}" \
  --gate-variant 5.2A --warm 21 --include-heavy --margin 0.05 \
  --candidate-meta results/d2b_meta_candidate.json \
  --reference-meta results/d2b_meta_reference.json \
  --out results/d2b_paired.json

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
  --warm 25 --out results/d2b_noise_pilot_reference.json
uv run python -m bench.noise_pilot --base-url "http://127.0.0.1:${CAND_PORT}" \
  --warm 25 --out results/d2b_noise_pilot_candidate.json

echo
echo "artefacts: results/d2b_contract.json results/d2b_paired.json"
echo "           results/d2b_meta_{candidate,reference}.json"
echo "           results/d2b_noise_pilot_{reference,candidate}.json"
echo "           results/d2b_environment.json"
echo "== done; the trap now stops all four processes, verifies the ports, and"
echo "   confirms production is the same process it was =="
