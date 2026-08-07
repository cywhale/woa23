# D2b — request to run a controlled two-arm comparison on VM24

**Status:** **formally requested, 2026-08-07. Not granted. Nothing here has been
run.** Reviewed and cleared to request at `422e236`; the review explicitly did *not*
grant execution.
**Asks for:** four processes on VM24, all loopback, for the duration of one run.
**Independent of D2a.** D2a authorised one candidate against live production; this
authorises a self-contained pair. Neither implies the other.

## 1. Why this exists

The 2026-08-07 campaign produced a `PASS` with apparent speedups of 1.39×–5.99×. It
is directional evidence and it agrees with the Zarr-level measurement, but it is not
a clean isolation of the Dask change, for two reasons that variant 5.2B cannot fix:

- **The reference's hash seed could not be pinned.** Production may not be
  restarted, so output ordering was not reproducible and the contract gate had to
  compare semantically rather than byte for byte.
- **The two arms did not share an environment.** The twelve packages pinned in
  `pyproject.toml` matched, but **23 of the 58 shared distributions differed** —
  including `fsspec` (zarr/xarray storage access), `anyio` (starlette's async path)
  and `pyarrow` (the pandas interchange the query pipeline uses). Nothing shows they
  affected the measurement; the point is that nobody could show they did not.

D2b removes both. Both arms are processes we start, from **one venv**, with the seed
pinned, so the contract gate can be byte-exact and the only difference between the
arms is the two lines of §4.4.

## 2. Traffic — production receives none

| phase | → our reference 8052 | → our candidate 8051 | → **production 8050** |
|---|---|---|---|
| readiness (OpenAPI document, no store read) | ≤30 | ≤30 | **0** |
| data-path probe, counterbalanced | 2 | 2 | **0** |
| contract gate, 5.2A, 64 cases | 64 | 64 | **0** |
| latency gate, rung 21 | 176 | 176 | **0** |
| sample-size pilot, *after* the gate, both arms | 208 | 208 | **0** |
| **total** | **≤480** | **≤480** | **0** |

Every request goes to a process this run started. The public API and its backend are
never contacted.

**Readiness never touches the data path.** The probe that waits for a server to
come up requests the OpenAPI document, which exercises gunicorn, the uvicorn worker
and FastAPI routing while reading nothing from the Zarr store. The previous probe
issued a real data query, to the reference first — giving the reference a warm store
handle and populated page cache before the candidate had served anything, on the one
path the whole experiment measures. The data path is still confirmed before
committing to 64 contract cases, by a probe that runs once in each order
(candidate-first, then reference-first): two requests per arm, exactly balanced.

**The contract gate counterbalances its request order.** All 64 cases used to fetch
the reference first. `request_order(i)` alternates by case index, giving 32 RC and 32
CR, and the order chosen for each case is recorded in the artefact alongside a
`request_order_counts` summary — so the balance can be audited from the result file
rather than taken on trust.

**The pilot runs last, and against both arms.** Placed before the gate it would have
sampled one arm 208 times and the other not at all — warming one side's page cache
and connection state immediately before a paired latency measurement, an asymmetry
introduced by the very tool meant to characterise noise. Its output is sample-size
planning for a possible rung 60, which does not need to precede rung 21; the gate's
own bootstrap interval carries the noise for this run. Sampling both arms keeps the
recorded floor a property of the pair rather than of one side.

## 3. What this costs anyway

**"Does not modify production" is not "does not affect the host."** This request is
for a shared machine and the honest accounting is:

- **CPU.** Four processes: two gunicorn workers, a Dask scheduler and one Dask
  worker. VM24 has 20 cores and also runs `tide_app`, `mhw_app` and the shared Dask
  cluster.
- **Page cache.** The gate decompresses roughly 13.55 MiB per pass over the eight
  cases; across the pilot, contract and latency phases that is on the order of
  **1 GiB of Zarr reads per arm**. The 31.9 GiB store currently fits in VM24's page
  cache, and production's measured performance depends on that. Our reads compete
  for the same cache and may evict pages production would otherwise have hit.
- **RAM.** Two API workers plus a Dask worker capped at 8 GB.
- **Disk.** The isolated work directory holds a copy of `woa23_app.py` and `src/`
  only — a few hundred kilobytes. **The 31.9 GiB store is not copied**; the
  reference reaches it through a read-only symlink.

None of this changes production's data, configuration or processes. It does share
its machine.

## 4. Isolation design

| | |
|---|---|
| **work directory** | `~/woa23-s1-controlled/` — new, outside `~/python/woa23`, which is never written |
| **reference source** | an unmodified copy of `woa23_app.py` + `src/`, made read-only, with **every file's SHA-256 checked against production's original before anything starts** |
| **reference store** | `~/woa23-s1-controlled/reference/data` → symlink to `~/python/woa23/data`. `woa23_app.py:63` hard-codes the relative `data/`, so this gives it the real store without copying 31.9 GiB and without a writable path to it |
| **candidate store** | absolute `WOA23_ZARR_STORE=/home/odbadmin/python/woa23/data` |
| **environment** | **one venv, shared by both arms** — `dev2026/.venv`, Python 3.11.4, built from this branch's `uv.lock` |
| **ports** | candidate `127.0.0.1:8051`, reference `127.0.0.1:8052`, isolated Dask scheduler `127.0.0.1:8787` |
| **both arms** | `PYTHONHASHSEED=0`, `-w 1`, no `--reload`, plain HTTP, never contacting 8050 |

### The Dask scheduler is the subtle one

The reference is `woa23_app.py` unmodified, so it *creates a Dask client* — that is
the thing under test. `src/dask_client_manager.py` reads `DASK_SCHEDULER_ADDRESS`
and **falls back to `tcp://localhost:8786`**, which is production's shared scheduler
serving `tide_app` and `mhw_app`. Left to the default, the reference would join it.

So this run starts **its own scheduler and worker on 8787** and sets
`DASK_SCHEDULER_ADDRESS` explicitly rather than relying on the default being
overridden. Production's cluster on 8786 is never contacted.

### One venv, and what that does and does not establish

Sharing a venv makes the packages stop being a variable: there is only one set. What
it does **not** do is reproduce production's environment — production has 236
distributions to this venv's 58, and 23 of the shared ones are at different versions.

**These are different goals and this request pursues the first.** D2b answers "what
does removing Dask do, holding everything else equal". It does not answer "what will
this do on production's exact environment"; that would need a lock built from
production's full distribution set, and it is S2c's question, not S1's. Production's
environment is not modified either way.

## 5. Preflight — the run aborts before any HTTP request unless all of this holds

**The environment is built and verified before any process starts.** A run that
discovers its environment is wrong once the servers are up has already spent the
host's CPU and evicted production's page cache for nothing. In order:

1. Host is `odb24` and the store exists.
2. Production's interpreter is present and **is 3.11.4**. The venv is then created or
   updated with `uv sync --locked` against that interpreter — `--locked`, not
   `--frozen`, so a `uv.lock` that has drifted from `pyproject.toml` is an error
   rather than a silent install from stale inputs.
3. The venv is re-interrogated from **inside itself**: interpreter version equals
   3.11.4, the full distribution list and its digest, and the `uv.lock` digest are
   all recorded to `results/d2b_environment.json`. `requires-python = ">=3.11"` once
   let `uv` pick 3.14 on VM24; the version is checked, not requested.
4. **No leftover run state.** A free port is not an all-clear: cleanup deliberately
   leaves its pidfile when it refuses to kill, and starting over it would orphan
   whatever it names.
5. Ports 8051, 8052 and 8787 are free. If any is held, abort — never displace it.
6. Production **is** listening on 8050. Its **listener PID set, master PID and the
   master's `/proc` start time** are recorded, along with the host's **boot ID**, so
   the post-run check can compare identity rather than mere occupancy.
7. `~/woa23-s1-controlled` does not already exist. The script refuses rather than
   clearing it.
8. Every reference source file's SHA-256 equals production's original.
9. Both arms' provenance passes full schema validation, then **two separate
   questions** are asked:
   - `verify_environment_match()` — do the arms agree *with each other* on
     **interpreter version**, **`env_python`**, the **digest of the full
     distribution list** and the **`uv.lock` digest**? A subset comparison is how
     the last discrepancy stayed invisible, so this compares the whole list and
     names any package that differs.
   - `verify_environment_record()` — is what they agree on **the environment step 3
     built**? Every field of `d2b_environment.json` is compared against each arm:
     `env_python`, `python_version`, `lockfile_sha256`, `distributions_sha256`. Two
     arms sharing a stale `.venv` from an earlier invocation agree with each other
     perfectly, so the first check alone passes while both run the wrong
     environment. Both fail closed: a missing record, a missing field, or a
     non-string value is a problem, never a pass.

**Port state is read through one library, `scripts/lib_ports.sh`**, shared by both
runners and exercised offline by `scripts/test_ports.sh` against a captured `ss`
fixture under the same `set -euo pipefail` the runners use. Three properties it
guarantees, each of which was previously violated:

- **Ports are parsed, not pattern-matched.** Every port test in both runner scripts
was a substring match on `ss` output. `ss` writes the local address as `addr:port`
and the address half may contain colons, so `[fe80::8050]:9000` — an unrelated
service — read as a listener on 8050, and a `\b`-anchored grep for port 50 matched
any IPv6 address ending in `:50`. Production's listener set is what the whole
identity check rests on, so a false member there corrupts the master election and
every comparison downstream. The port is now taken from the local-address column,
after the last colon, compared as an integer, and only in `LISTEN` rows — which also
stops a *client* of 8050 being counted as holding it.
- **An empty result is an observation, not an error.** `grep` exits 1 when nothing
  matches and `pipefail` propagates it, so `x="$(pids_on_port 8050)"` under `set -e`
  aborted the script outright — preflight never reached its "production is not
  listening" branch, and cleanup aborted mid-function, losing the entire
  production-identity report and the `CLEANUP_FAILED` rollup.
- **"Cannot determine" is never "nothing there."** If `ss` itself fails, every
  function returns status **2**, distinct from 1. `port_released()` is true only for
  a port observed free, so an unreadable `ss` can never be reported as a released
  socket, and preflight refuses to start rather than assume a port is available.

Cleanup reports each production outcome distinctly — port state unreadable, listener
gone, master unresolvable, master start time unreadable, production restarted,
workers recycled, unchanged — because "cannot tell" and "gone" and "different
process" are three different facts and only one of them is benign.

Every process this run owns — scheduler, worker, reference, candidate — is started
through the **same tracked-start path**, which writes a pidfile, reads the PID's
`/proc` start time and refuses to start over an existing record. Identity is
established the same way for all four rather than inline per process.

## 6. Order of work

1. Contract gate, **variant 5.2A, byte-exact**, all 64 cases.
2. **Only if it passes**, the latency gate at rung 21. A speed number for a backend
   that returns different bytes is not a result.
3. The sample-size pilot, last, against both arms.
4. Post-run drift: process, port, sources and store re-checked in the same run
   (`post_run_runtime_check`).

**Nothing is reused.** The 2026-08-07 contract and latency artefacts are historical,
were produced by the pre-fix harness, and are not carried into this run in any form.

## 7. Cleanup

All four processes are tracked with a pidfile and a start time. The trap runs on
every exit path and, for each, **refuses to signal anything whose start time no
longer matches or which no longer holds its port**, leaving the pidfile for
inspection instead. A pidfile is removed only after its port is confirmed released.
A dead PID is not treated as a released socket.

**A cleanup failure fails the run.** Any process left running, any port still held,
any refusal to signal — each sets a failure flag and the script exits non-zero even
if both gates passed. An earlier draft ended each stop with `|| true`, under which a
stranded gunicorn worker or a held 8051 would have exited zero and read as a clean
run; the artefacts would have looked complete while the host was left dirty. A gate
result from a run that could not clean up after itself is not reportable.

Afterwards the script compares production against what it recorded at preflight —
**the full listener PID set, the master PID, the master's start time, and the host's
boot ID**. A restart between the two checks would leave 8050 occupied by a different
process, so "someone is listening" would pass while every comparison in the run
described a backend that no longer exists.

The full set matters and the master alone does not cover it. gunicorn's workers hold
the same inherited socket, so a worker that died and respawned changes the PID set
while leaving the master untouched — and that is precisely the collateral effect this
run could cause by competing for the host's CPU and page cache. The two are reported
differently ("production restarted" vs "workers were recycled while this run was
using the host") and **both fail the run**, as does a reboot or an empty 8050.

## 8. Not covered by this request

Restarting or reconfiguring production; touching the shared Dask cluster on 8786;
writing anything under `~/python/woa23`; any public cutover; rung 60 or rung 150;
any change to production's package environment.

## 9. What is ready now

`scripts/run_controlled.sh` is written, syntax-checked, and its refusal paths are
verified: it exits 3 without `WOA23_D2B_GRANTED=yes`, exits 4 off `odb24` or without
`env -C`, and exits 1 on a held port, leftover run state, an existing work directory,
an interpreter that is not 3.11.4, a `uv.lock` that does not match `pyproject.toml`,
a reference source digest that does not match production, arms whose environments
disagree with each other or with the environment this run built, a failed contract
gate, or a cleanup that did not complete.

Its refusal message names **four** processes, which is what it starts. The count is
worth stating correctly in the message the PI reads at the moment of granting: an
earlier draft said three, having omitted the Dask worker — the process that does the
actual reading on the reference arm.

## 10. Known imprecision, stated rather than discovered later

Two diagnostics are less specific than they should be. Both **fail safe** — the run
stops or is marked failed in every case — but the message a reader gets can be
narrower than the truth, and this request is partly a request to trust those
messages, so they are listed here rather than left to be found in a transcript.

1. **A `LISTEN` row with no readable `pid=`** (another user's process, no privilege
   to see it) yields an empty PID set. Preflight then reports "production is not
   listening on 8050" and aborts, when the accurate statement is "a listener exists
   but its PID cannot be resolved". The refusal is correct; the reason given is not.
2. **`stop_tracked` when `ss` cannot be read** describes the port as not released.
   Refusing to declare an unreadable socket released is the behaviour we want — the
   wording just says "FAILED TO RELEASE" when it means "could not confirm release".

Neither affects whether the run proceeds or how it is scored. They can be fixed
before the run if that is preferred; the review classified them as non-blocking.

## 11. The ask

Authorisation is requested for **one execution** of
`scripts/run_controlled.sh` on VM24 as `odbadmin`, gated on
`WOA23_D2B_GRANTED=yes`, with:

| | |
|---|---|
| **processes** | 4 — Dask scheduler, Dask worker, reference API, candidate API |
| **ports** | `127.0.0.1:8051`, `127.0.0.1:8052`, `127.0.0.1:8787` — all loopback |
| **requests, per arm** | **≤480** |
| **requests to production 8050** | **0** |
| **writes under `~/python/woa23`** | **none** — read-only symlink to the store |
| **duration** | one run; the trap stops all four processes on every exit path |

**Not authorised by this request:** rung 60, rung 150, any second execution, any
public cutover, restarting or reconfiguring production, contact with the shared Dask
cluster on 8786, or any change to production's package environment. Each is a
separate decision.

**On completion the report will state** the contract gate verdict, the latency gate
verdict with its bootstrap intervals, both arms' provenance, the environment digests,
the recorded `request_order_counts`, and the cleanup result including production's
listener set, master PID, start time and boot ID before and after — or, if cleanup
did not complete, that the run is a failure regardless of its gates.
