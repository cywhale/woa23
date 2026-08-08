# Spec 002 — S2: production-like correctness, in isolation

**Status:** draft, not implemented. No VM24 process has been started for this spec.
**Depends on:** spec 001 (S1), merged at `3562492`.
**Scope of this document:** correctness of the S1 candidate under **production's package
environment**, verified in isolation. Nothing in it touches production, and nothing in
it is a performance claim.

## Revision history

| rev | date | change |
|---|---|---|
| 1 | 2026-08-08 | First draft, from the S2 outline reviewed in-session. Split per PI direction: nginx/TLS, live observation, canary and rollback moved out to later specs. C1 fixed as 5.2A over an isolated venv built from production's distribution set; C2 defined as 5.2B. Readiness and data-path smoke separated. The `p50/p99` item that contradicted the performance non-goal removed. |

---

## 1. What S1 established, and what it did not

S1 showed that removing Dask from the read path is byte-for-byte correct and faster,
**under one venv of 58 distributions with `PYTHONHASHSEED=0` on both arms**. That was
the point of D2b: make the packages stop being a variable.

Production is not that environment. It has **236 distributions** (digest
`60236d7210c8c364`, Python 3.11.4), and the versions differ from S1's venv in ways
that touch the read path directly:

| package | S1's isolated venv | production |
|---|---|---|
| `fsspec` | 2026.7.0 | **2025.10.0** |
| `anyio` | 4.14.2 | **4.11.0** |
| `pyarrow` | (S1 lock) | **22.0.0** |
| `numpy` | (S1 lock) | **2.2.4** |
| `pandas` | (S1 lock) | **2.2.3** |

`fsspec` mediates zarr/xarray store access, `anyio` is starlette's async path, and
`pyarrow`/`pandas` sit in the interchange the query pipeline uses. S1's data says
nothing about them: **its whole validity rests on both arms having been identical
apart from the Dask change**, and re-introducing 236 packages re-introduces the
variable S1 removed.

S2 asks one question: **does the candidate still return exactly what the reference
returns, when both are given production's packages?** It does not ask how fast.

## 2. Scope

1. **C1 — production-environment contract.** Build an isolated venv from
   **production's own distribution set**, give it to both arms, pin
   `PYTHONHASHSEED=0`, and run the 64 contract cases as **5.2A byte-exact**.
2. **C2 — multi-worker contract.** `gunicorn -w N` forks N workers, each its own
   process with its own hash seed, and production does not pin the seed. Verify the
   contract holds as **5.2B semantic** under that configuration.
3. **D1 — startup failure modes.** `WOA23_ZARR_STORE` is mandatory with no fallback
   (spec 001 §4.2). Establish what a missing or wrong value actually does at start,
   and that it fails loudly rather than serving errors.
4. **D2 — readiness.** Establish what "ready" means for this service and that the
   readiness signal does not depend on the store being readable.

## 3. Non-goals

Each of these is real work that S2 deliberately does not do. Several have their own
spec; none of them may be smuggled in as "while we're here".

- **Performance of any kind.** No latency gate, no rung escalation, no timing
  comparison, no percentiles. S2 produces no number that may be quoted as
  performance. *(Rev 1 correction: the first draft listed p50/p99 observation under
  deployment acceptance while declaring performance a non-goal. That was a
  contradiction; the item moved out with the rest of live observation.)*
- **nginx, TLS, and the public path** — separate spec.
- **Live observation, canary and rollback** — separate spec. S2 runs in isolation and
  never puts the candidate in front of a user.
- **Dependency modernization** — S2 *measures against* production's package set; it
  does not change it, converge it or upgrade it.
- **Dataset cache** — chunk caching, page-cache strategy, warming.
- **Event-loop concurrency** — async conversion, worker-count tuning, concurrency
  models.

C2 will probably surface concurrency questions. **Surfacing them is in scope; fixing
them is the event-loop spec's job.** S2 names what it finds and stops.

## 4. Contract acceptance

### C1 — production's packages, byte-exact

| | |
|---|---|
| environment | one isolated venv built from production's 236 distributions, **shared by both arms** |
| interpreter | Python 3.11.4, matching production |
| seed | `PYTHONHASHSEED=0` on both arms |
| store literal | identical string on both arms (S1's finding: the string, not the directory, orders `zarr_group_paths`) |
| comparison | **5.2A byte-exact**, all 64 cases |
| pass | 64/64 `MATCH` |

**Why 5.2A and not 5.2B.** Production itself cannot be restarted to pin its seed, and
that is exactly why D2a had to compare semantically. But S2 does not need to run
*production* — it needs production's *packages*. Those can be installed into a venv we
start ourselves, where the seed is ours to pin. That keeps the strong comparison and
still answers the question. The cost is that this is production's package set, not
production's running process; §7 states what that does and does not license.

The 64 cases are unchanged from S1: **33 JSON-path and 31 CSV, which partition the
set**; within them, 15 expect a non-200 status and 2 are the OpenAPI document and the
Swagger page. 47 return row payloads. Request order is counterbalanced RC/CR and
recorded per case.

### C2 — multiple workers, unpinned seed, semantic

| | |
|---|---|
| configuration | `gunicorn -w N` on both arms, `PYTHONHASHSEED` **not** pinned |
| comparison | **5.2B semantic**: rows as a multiset keyed on `(lon, lat, depth, time_period)`, columns as a set, values exactly |
| pass | 64/64, and every difference that *is* found is characterised, not waived |

**What 5.2B cannot see, stated plainly:** column order, key order and float
formatting. That is the price of not pinning the seed, and it is why C1 exists
alongside C2 rather than being replaced by it. If C2 shows differences that C1 did
not, the difference is caused by the worker configuration and is a finding in its own
right.

## 5. Deployment acceptance

Only the two that can be established in isolation. Everything requiring a live
service moved to the observation spec.

### D1 — startup fails loudly

`api/config.py:31` reads `os.environ["WOA23_ZARR_STORE"]` at import time and raises
`KeyError` when unset. Establish, for the deployment form actually used:

- unset variable → the process fails to start, with the cause in the log;
- variable set to a path that does not exist → same;
- variable set to a directory that is not a Zarr store → detected, and where.

The failure must be distinguishable from "started but returning errors". A service
that starts and then 500s on every request is the worse outcome and must not be what
this looks like.

### D2 — readiness is not a data-path check

These are two different things and the first draft of this spec ran them together:

| | probes | reads the store | purpose |
|---|---|---|---|
| **readiness** | the OpenAPI document | **no** | is the process up and routing? |
| **data-path smoke** | one real query per arm, counterbalanced | **yes** | can it actually read the store? |

Readiness must not read the store: S1 found that a readiness probe issuing a real
query warmed one arm's page cache and store handle before the other had served
anything. The data-path smoke is a separate, deliberately symmetric step.

Establish that the readiness signal is meaningful for the deployment form — that it
turns green only once the worker can serve, and that it does not turn green when the
store is unreadable while the process is otherwise healthy.

## 6. Isolation requirements

Inherited from S1 and non-negotiable:

- a **new** staging directory per attempt; `~/python/woa23` is never a working
  directory and is never written;
- the reference is a **read-only** copy of production's source, its SHA-256 checked
  against the original before anything starts;
- the store is reached through a **read-only symlink**; it is never copied;
- all ports **loopback only**, and 8050, 8786 and 8787 are refused by the runner;
- **0 requests to production 8050**, no connection to the shared Dask scheduler;
- the process tree is recorded and the count enforced before any gate runs;
- cleanup must confirm itself — every tracked process exited, ports released, state
  removed, and production's listener set, master PID, start time and boot id
  unchanged. **A run whose cleanup cannot confirm itself is a failed run whatever its
  gates said.**

## 7. What a C1 pass would and would not license

**Would:** that the candidate and the unmodified reference produce byte-identical
responses across the 64 cases when both run on production's package set with a pinned
seed.

**Would not:**

- that production's *running process* behaves the same — same packages, different
  process, different uptime, different memory state;
- anything about latency, throughput or resource use;
- anything about nginx, TLS or the public path;
- anything about behaviour under concurrent load beyond what C2 examines;
- that deployment is safe. That is the observation and rollback spec's question, and
  it needs a canary, not a contract gate.

## 8. Open questions for the PI

1. **How is the 236-distribution venv built?** Production's environment is a pyenv
   install, not a lockfile. Options: freeze it to a requirements file and install
   from that (reproducible, but a freeze is a snapshot and may not resolve), or copy
   the interpreter's `site-packages` (exact, but not a build). Neither is obviously
   right and the choice affects what C1 proves. **This needs deciding before any
   implementation.**
2. **What is `N` in C2?** Production's current worker count should be read rather than
   assumed, and C2 should use that value.
3. **Which deployment form is D1/D2 about** — systemd unit, an existing deploy script,
   or the bare gunicorn invocation? The answer changes what "fails to start" means.
4. **Does reading production's package list or unit file need separate authorisation?**
   Reading has been treated as allowed throughout S1; C1 needs rather more of it.

## 9. What is not yet decided, and is not assumed

- The staging directory name, ports and workdir for any S2 run. They will be proposed
  with the run request, not fixed here.
- Whether C1 and C2 run as one invocation or two.
- Whether the existing runner can host these gates or needs a mode of its own. It has
  `--contract-only` and `--cleanup-only` today; C2's unpinned seed conflicts with the
  runner's current insistence on a pinned seed for both arms under 5.2A, so at minimum
  that interaction needs designing.

**No implementation has begun and no VM24 process has been started for this spec.**
