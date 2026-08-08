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
# A new directory per attempt. The script refuses to reuse one, and the
# 2026-08-08 attempts left ~/woa23-s1-controlled and -r2 behind, each holding the
# reference copy its run was scored against — evidence, not scratch space.
WORK=$HOME/woa23-s1-controlled-r4
REF_DIR=$WORK/reference
CAND_DIR=$WORK/candidate
CAND_PORT=8051
REF_PORT=8052
SCHED_PORT=18787           # see D2b-request.md §4: 8787 is NOT free — it is
                           # production's own scheduler dashboard, same PID as 8786
PROD_PORT=8050
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN=$HERE/run
VENV=$HERE/.venv

# --cleanup-only: bring the six processes up, record and verify their trees, collect
# provenance, then stop. No contract gate, no latency gate, no pilot — so it produces
# no measurement of any kind and none may be quoted from it. It exists to make a
# cleanup failure reproducible and self-describing, nothing more.
CLEANUP_ONLY=no
for arg in "$@"; do
  case "$arg" in
    --cleanup-only) CLEANUP_ONLY=yes ;;
    *) echo "unknown argument: $arg (only --cleanup-only is accepted)" >&2; exit 2 ;;
  esac
done

if [ "${WOA23_D2B_GRANTED:-}" != "yes" ]; then
  echo "D2b authorisation not stated. This starts FOUR services on a production" >&2
  echo "host — a Dask scheduler, a Dask worker, an unmodified reference API and the" >&2
  echo "candidate API — which is SIX OS processes, because each of the two APIs is" >&2
  echo "a gunicorn arbiter plus the worker it forks." >&2
  echo "Re-run with WOA23_D2B_GRANTED=yes once it is granted. D2a does not imply D2b." >&2
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
# `.uncertain` too: it is written precisely when a previous run could not record
# what it had started, which is the state that most needs a person to look.
# `.tree` as well: a stop that cannot remove its state leaves one behind, and a
# tree naming PIDs from a previous run is exactly what must not be stepped over.
# `.diag` too: it is the record of why a previous cleanup could not confirm itself,
# and it is written into the same directory the next run would write over. Evidence
# that a run can silently destroy is evidence that will be destroyed.
leftovers=("$RUN"/*.pid "$RUN"/*.starttime "$RUN"/*.tree "$RUN"/*.uncertain \
           "$RUN"/*.diag)
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
for f in api/__init__.py api/app.py api/config.py api/query.py; do
  a="$(sha256sum "$HERE/$f" | cut -d' ' -f1)"
  b="$(sha256sum "$CAND_DIR/$f" | cut -d' ' -f1)"
  [ "$a" = "$b" ] || { echo "  $f DIFFERS from the repository ($a vs $b)" >&2; exit 1; }
  echo "  $f  $a"
done

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
start_tracked dask_scheduler "$SCHED_PORT" \
  "$VENV/bin/dask" scheduler --host 127.0.0.1 --port "$SCHED_PORT" --no-dashboard
for _ in $(seq 1 30); do port_held "$SCHED_PORT" && break; sleep 1; done
port_held "$SCHED_PORT" || { echo "scheduler did not bind" >&2; exit 1; }
# --no-nanny: `dask worker` defaults to --nanny, a supervisor process that forks
# the worker. For a single worker the nanny buys nothing here and costs an extra
# process to account for, so the worker runs in this process directly.
start_tracked dask_worker "" \
  "$VENV/bin/dask" worker "tcp://127.0.0.1:${SCHED_PORT}" \
  --nworkers 1 --nthreads 1 --memory-limit 8GB --no-dashboard --no-nanny

# ==================================================================== the arms ===
# One venv, both arms. That is the whole point of 5.2A: the packages stop being a
# variable because there is only one set of them. Both go through start_tracked, so
# identity is recorded the same way for every process this run owns.
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

# Readiness: the OpenAPI document. It exercises the whole stack that has to be up —
# gunicorn, the uvicorn worker, FastAPI routing — and reads **nothing** from the
# Zarr store, so waiting for the servers to appear costs neither arm a chunk read.
#
# The previous probe issued a real data query, to the reference first. That gave the
# reference a warm store handle and a populated page cache before the candidate had
# served anything, and it did so on the one path the whole experiment measures.
ready() {                   # ready <port>
  local out
  for _ in $(seq 1 30); do
    out="$(curl -s --max-time 5 -o /dev/null -w '%{http_code} %{size_download}' \
      "http://127.0.0.1:$1/api/swagger/woa23/openapi.json" || true)"
    [ "${out%% *}" = "200" ] && [ "${out##* }" -gt 0 ] && return 0
    sleep 1
  done
  return 1
}
ready "$REF_PORT"  || { echo "reference not ready; see $RUN/reference.log" >&2; exit 1; }
ready "$CAND_PORT" || { echo "candidate not ready; see $RUN/candidate.log" >&2; exit 1; }

# The authorisation is for a specific set of processes, so the set is *verified*,
# not merely printed. A run that has five or seven is outside what was granted —
# a nanny that reappeared, a second gunicorn worker, a scheduler that forked — and
# it stops here, before the gates, with the trap cleaning up what it started.
echo "== process trees (what the authorisation covers and cleanup is held to) =="
expected_procs() {
  case "$1" in
    dask_scheduler|dask_worker) echo 1 ;;   # --no-nanny; the default would be 2
    reference|candidate)        echo 2 ;;   # gunicorn arbiter + one forked worker
    *)                          echo 0 ;;
  esac
}
AUTHORISED_TOTAL=6
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

# The data path still has to work before committing to 64 contract cases — a store
# the reference cannot open should fail here, not thirty requests in. But the probe
# must not favour an arm either, so it runs once in each order: candidate-first,
# then reference-first. Two requests per arm, exactly counterbalanced.
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
else
  for pair in "candidate:$CAND_PORT reference:$REF_PORT" \
              "reference:$REF_PORT candidate:$CAND_PORT"; do
    for entry in $pair; do
      probe "${entry%%:*}" "${entry#*:}" || exit 1
    done
  done
fi
echo "both arms ready"

# Re-record each tree now that the children exist. start_tracked already wrote one
# when it began tracking — it has to, because the trap is armed from that moment and
# stop refuses to signal a service whose tree it cannot interpret — but a gunicorn
# arbiter has not forked its worker in the first second after exec, so that first
# snapshot holds the arbiter alone. This is where the full set is established, and
# where it is checked against what the authorisation covers.

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
                              verify_environment_record, verify_group_path_agreement)
cand = json.load(open("results/d2b_meta_candidate.json"))
ref = json.load(open("results/d2b_meta_reference.json"))
env = json.load(open("results/d2b_environment.json"))
problems = (validate_meta(cand, "candidate") + validate_meta(ref, "reference")
            # do the two arms agree with each other?
            + verify_environment_match(cand, ref)
            # and is what they agree on the environment this run actually built?
            # Two arms sharing a stale .venv agree perfectly and prove nothing.
            + verify_environment_record(env, cand, "candidate")
            + verify_environment_record(env, ref, "reference")
            # and do they build zarr_group_paths from the same string? Different
            # strings hash differently, so the set iterates in a different order
            # for any query spanning more than one group.
            + verify_group_path_agreement(cand, ref))
if problems:
    print("arms are not comparable:", file=sys.stderr)
    for p in problems:
        print(f"  - {p}", file=sys.stderr)
    raise SystemExit(1)
print(f"both arms: python {cand['env_python_version']}, "
      f"{len(cand['dependencies']['distributions'])} distributions, "
      f"digest {cand['dependencies']['distributions_sha256'][:16]}")
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
echo "== done; the trap now stops all four services, verifies every process in"
echo "   their recorded trees has exited, verifies the ports, and confirms"
echo "   production is the same process it was =="
