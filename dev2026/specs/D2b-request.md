# D2b — request to run a controlled two-arm comparison on VM24

**Status:** requested, not granted. Nothing here has been run.
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
| contract gate, 5.2A, 64 cases | 64 | 64 | **0** |
| sample-size pilot | 208 | 0 | **0** |
| latency gate, rung 21 | 176 | 176 | **0** |
| **total** | **448** | **240** | **0** |

Every request goes to a process this run started. The public API and its backend are
never contacted.

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

1. Host is `odb24` and the store exists.
2. **No leftover run state.** A free port is not an all-clear: cleanup deliberately
   leaves its pidfile when it refuses to kill, and starting over it would orphan
   whatever it names.
3. Ports 8051, 8052 and 8787 are free. If any is held, abort — never displace it.
4. Production **is** listening on 8050, recorded so the post-run check can confirm
   it still is.
5. `~/woa23-s1-controlled` does not already exist. The script refuses rather than
   clearing it.
6. Every reference source file's SHA-256 equals production's original.
7. Both arms' provenance passes full schema validation, and
   `verify_environment_match()` confirms they agree on **interpreter version**,
   **`env_python`**, the **digest of the full distribution list** and the
   **`uv.lock` digest**. A subset comparison is how the last discrepancy stayed
   invisible, so this compares the whole list and names any package that differs.

## 6. Order of work

1. Contract gate, **variant 5.2A, byte-exact**, all 64 cases.
2. **Only if it passes**, the sample-size pilot and then the latency gate at rung 21.
   A speed number for a backend that returns different bytes is not a result.
3. Post-run drift: process, port, sources and store re-checked in the same run
   (`post_run_runtime_check`).

**Nothing is reused.** The 2026-08-07 contract and latency artefacts are historical,
were produced by the pre-fix harness, and are not carried into this run in any form.

## 7. Cleanup

All four processes are tracked with a pidfile and a start time. The trap runs on
every exit path and, for each, **refuses to signal anything whose start time no
longer matches or which no longer holds its port**, leaving the pidfile for
inspection instead. A pidfile is removed only after its port is confirmed released.
A dead PID is not treated as a released socket.

Afterwards the script confirms production is still listening on 8050 and says so.

## 8. Not covered by this request

Restarting or reconfiguring production; touching the shared Dask cluster on 8786;
writing anything under `~/python/woa23`; any public cutover; rung 60 or rung 150;
any change to production's package environment.

## 9. What is ready now

`scripts/run_controlled.sh` is written, syntax-checked, and its refusal paths are
verified: it exits 3 without `WOA23_D2B_GRANTED=yes`, exits 4 off `odb24`, and exits
1 on a held port, leftover run state, an existing work directory, a reference source
digest that does not match production, or arms whose environments disagree.
