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
| 2 | 2026-08-08 | **Settled per PI direction.** C1's environment is a read-only forensic clone of production's Python environment, not a rebuild — §4.1 and the open question it replaces. C2's worker count is read from the host, not assumed: production runs `-w 2`, and the first draft's `-w 4` was a guess. C2's unpinned seed is verified across repeated independent starts rather than one. Added §5, separating production-like gunicorn validation from formal PM2/nginx deployment validation — production is supervised by **PM2**, not systemd, which the first draft assumed. Added §6, the boundary between byte-exact, semantic and order-stability. |

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

1. **C1 — production-environment contract.** A **read-only forensic clone** of
   production's Python environment, shared by both arms, `PYTHONHASHSEED=0`, and the
   64 contract cases as **5.2A byte-exact**. Cloned, not rebuilt — §4.
2. **C2 — multi-worker contract.** `gunicorn` with **production's own worker count,
   read from the host** (observed `-w 2`, re-read at run time), seed unpinned as
   production leaves it, verified as **5.2B semantic** across **repeated independent
   starts**. Order stability is recorded alongside it — §6.
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

#### The environment is a read-only forensic clone, not a rebuild

Two ways to get production's packages were considered in rev 1 and the difference
matters more than it looks: freezing to a requirements file and reinstalling produces
*a resolution of the same names*, which is not the same artefact — a re-resolve can
pick different wheels, different transitive pins, different build metadata, and every
one of those is a variable S2 exists to hold still.

So the environment is **cloned, not rebuilt**: production's `site-packages` is copied
byte for byte into an isolated tree, made read-only, and used as-is.

| | |
|---|---|
| direction | one-way, production → clone. Production is **read** and never written, never moved, never relinked |
| method | copy, not symlink: a symlink would leave both arms executing production's own files, so a stray write during the run would land in production |
| verification | per-file SHA-256 of the clone against the source, and the distribution-list digest (`60236d7210c8c364`) recomputed **from inside the clone** |
| permissions | the clone is read-only for the duration of the run |
| interpreter | production's own Python 3.11.4 is **not** copied; the clone is used with an interpreter of the same version, and that difference is recorded rather than glossed |
| what it is | a forensic artefact — a snapshot of what production has, faithful to the byte |
| what it is not | a build, a lockfile, or anything reproducible from a manifest. It cannot be recreated from this spec; it can only be re-cloned |

If the clone's recomputed digest does not equal production's, the clone is wrong and
the run does not proceed. That check is what makes this a clone rather than an
approximation.

| | |
|---|---|
| environment | the forensic clone above, **shared by both arms** |
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
production's running process; §9 states what that does and does not license.

The 64 cases are unchanged from S1: **33 JSON-path and 31 CSV, which partition the
set**; within them, 15 expect a non-200 status and 2 are the OpenAPI document and the
Swagger page. 47 return row payloads. Request order is counterbalanced RC/CR and
recorded per case.

### C2 — multiple workers, unpinned seed, semantic

**The worker count is read from the host, not chosen.** Rev 1 wrote `-w 4`; the
running master's argv says `-w 2`, and its two children confirm it. That number is
read again at the time of the run rather than carried from here, because it is a
property of production and may change.

Observed 2026-08-08, read-only from `/proc`:

```
pid 3960  gunicorn woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker ...
          children: 4334 4366          (two, matching -w 2)
          PYTHONHASHSEED: not set in the master environment
```

**One start is not evidence that the seed is unpinned.** A single process has one
seed, and any output it produces is self-consistent. Showing that the seed is *not*
fixed — and that the contract survives it — takes **repeated independent starts**:
the arms are started, the cases run, both are stopped, and the whole thing is
repeated. The seed's variation is established by observing it differ across those
starts, and the contract is asserted on every one of them.

| | |
|---|---|
| configuration | `gunicorn -w <production's count>` on both arms, `PYTHONHASHSEED` **not** pinned |
| repetitions | at least three independent start/stop cycles; the recorded seed must differ across them, or the premise is unverified and the result says nothing |
| comparison | **5.2B semantic**: rows as a multiset keyed on `(lon, lat, depth, time_period)`, columns as a set, values exactly |
| pass | 64/64 on **every** cycle, and every difference that is found is characterised, not waived |

**What 5.2B cannot see, stated plainly:** column order, key order and float
formatting. That is the price of not pinning the seed, and it is why C1 exists
alongside C2 rather than being replaced by it. If C2 shows differences that C1 did
not, the difference is caused by the worker configuration and is a finding in its own
right.

## 5. Two different things called "deployment"

The first draft ran these together and named the wrong supervisor. They are separate,
and only the first is in this spec.

| | **production-like gunicorn validation** (this spec) | **formal deployment validation** (later spec) |
|---|---|---|
| what runs | `gunicorn` invoked directly, in a staging directory | the real supervision path |
| supervisor | none — the runner starts and stops it | **PM2** (`pm2-odbadmin.service`) via `~/python/woa23/conf/start_app.sh` |
| what it establishes | the application behaves correctly when given production's packages and worker count | the *deployment* behaves correctly — restart policy, log handling, environment inheritance, ordering, failure recovery |
| nginx / TLS | not involved | involved |
| touches production config | never | by definition |

Rev 1 assumed systemd. The host says otherwise: production's gunicorn master is a
child of `bash ~/python/woa23/conf/start_app.sh`, itself under
`pm2-odbadmin.service`. **Anything about how PM2 starts, restarts or supervises the
service is out of scope here** — including whether `WOA23_ZARR_STORE` would even
reach the process through that path, which is exactly the kind of question the later
spec exists for.

D1 and D2 below are therefore about the *application's* behaviour, not the
deployment's.

## 6. Three properties, three different tests

These are routinely conflated and the distinction is the reason S1 took as long as it
did.

| property | question | test | what it cannot see |
|---|---|---|---|
| **byte-exactness** (C1) | do the two arms emit identical bytes? | 5.2A, seed pinned, one environment | nothing — it is the strongest form, but it needs a pinned seed, so it cannot be applied to a process we may not restart |
| **semantic equivalence** (C2) | do they emit the same *content*? | 5.2B, seed unpinned | column order, key order, float formatting |
| **order stability** | does one arm emit the same order **twice**? | the same arm, repeated independent starts, compared to itself | nothing about the other arm — it is a property of one implementation, not of the pair |

Order stability is neither of the other two and S1 never measured it. It matters
because S1 established that the candidate's output order depends on the *string* used
to reach the store, which means order is a function of configuration and not only of
code. A consumer that relies on column order — a CSV reader indexing by position, a
GeoServer SQL view — is exposed to that whether or not the two arms agree with each
other.

**C2's repeated cycles give order stability almost for free**: the same arm's
responses across cycles can be compared to each other as well as to the other arm.
Recording it costs nothing and answers a question no gate currently asks.

## 7. Deployment acceptance

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

## 8. Isolation requirements

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

## 9. What a C1 pass would and would not license

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

## 10. Open questions for the PI

Rev 1's first three questions are settled and recorded in §4, §5 and the revision
history. What remains:

1. **Does cloning production's `site-packages` need its own authorisation?** It is a
   bulk read of production followed by a write into a staging tree. Every read of
   production so far has been a handful of files; this is the whole environment, and
   it is the largest read S2 asks for. It writes nothing to production, but the size
   of the read is itself worth granting explicitly rather than assuming.
2. **How many repetitions for C2?** §4 says at least three. Three demonstrates the
   seed varies; it does not bound how often an ordering difference might appear. If a
   stronger claim is wanted, the number should come from the PI rather than from what
   is convenient.
3. **Is the interpreter difference acceptable?** The clone carries production's
   packages but is run under a same-version interpreter that is not production's own
   binary. If that gap matters, the alternative is running the clone under
   production's interpreter — which is a heavier ask, since it means invoking a
   production binary rather than only reading it.
4. **What happens if C1 fails?** A byte difference under production's packages would
   mean one of those 236 packages changes the output. S2 would then have found
   something real, and the next step — bisecting the package set — is not in this
   spec and would need its own.

## 11. What is not yet decided, and is not assumed

- The staging directory name, ports and workdir for any S2 run. They will be proposed
  with the run request, not fixed here.
- Whether C1 and C2 run as one invocation or two.
- Whether the existing runner can host these gates or needs a mode of its own. It has
  `--contract-only` and `--cleanup-only` today; C2's unpinned seed conflicts with the
  runner's current insistence on a pinned seed for both arms under 5.2A, so at minimum
  that interaction needs designing.

**No implementation has begun and no VM24 process has been started for this spec.**
