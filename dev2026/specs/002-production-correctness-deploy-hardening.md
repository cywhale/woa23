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
| 3 | 2026-08-08 | Renamed the artefact **production package-tree clone** — "forensic clone" over-claimed. §4.1 now enumerates acceptance per component (site-packages, dist-info, `.pth`, native libraries, symlinks, bytecode, secrets) and states plainly that **full runtime byte-equivalence is not claimed**: the stdlib is outside the tree and the interpreter is not cloned. Survey of the real tree recorded, including a **`.pth` editable install pointing at production's own `src`**, which the clone must neutralise. C2's three starts reframed as observing *this run's* seed diversity, with `INSUFFICIENT` as an outcome rather than a reason to add starts. §6 given a concrete order-stability acceptance and its relationship to C2's verdict. PI decisions on C2 repetitions and the interpreter recorded. |
| 2 | 2026-08-08 | **Settled per PI direction.** C1's environment is a read-only clone of production's package tree, not a rebuild — §4.1 and the open question it replaces. C2's worker count is read from the host, not assumed: production runs `-w 2`, and the first draft's `-w 4` was a guess. C2's unpinned seed is verified across repeated independent starts rather than one. Added §5, separating production-like gunicorn validation from formal PM2/nginx deployment validation — production is supervised by **PM2**, not systemd, which the first draft assumed. Added §6, the boundary between byte-exact, semantic and order-stability. |

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

1. **C1 — production-environment contract.** A **read-only package-tree clone** of
   production's installed packages, shared by both arms, `PYTHONHASHSEED=0`, and the
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

#### The environment is a production **package-tree clone**

Called what it is. It is a copy of production's installed package tree — not a
"forensic clone" of the runtime, which would imply an equivalence this does not
establish and §4.1.3 says it does not.

Rebuilding was considered and rejected: freezing to a requirements file and
reinstalling produces *a resolution of the same names*, and a re-resolve can pick
different wheels, different transitive pins and different build metadata. Every one
of those is a variable S2 exists to hold still. So the tree is **copied, not
rebuilt**.

##### 4.1.1 Source and scale, surveyed rather than assumed

Read-only from VM24 on 2026-08-08:

```
interpreter    /home/odbadmin/.pyenv/versions/py311/bin/python3.11
site-packages  /home/odbadmin/.pyenv/versions/py311/lib/python3.11/site-packages
               (= sysconfig purelib)
               33,567 files, 3,762 directories, 1.7 GB
stdlib         /home/odbadmin/.pyenv/versions/3.11.4/lib/python3.11   <- NOT in the tree
py311          symlink -> /home/odbadmin/.pyenv/versions/3.11.4/envs/py311
               pyvenv.cfg: base 3.11.4, include-system-site-packages = false
```

##### 4.1.2 Acceptance, per component

| component | observed | how the clone is accepted |
|---|---|---|
| **site-packages tree** | 33,567 files / 3,762 dirs / 1.7 GB | per-file SHA-256 against the source, and file and directory counts equal |
| **`*.dist-info`** | **240** directories, while `importlib.metadata` reports **236** distributions | all 240 copied; **the discrepancy is reconciled and named before the run** — four directories that do not yield a distribution are either stale, malformed or dual-named, and which it is decides whether the clone is faithful or the source is untidy |
| **`*.pth`** | 4: `easy-install.pth` (empty), `distutils-precedence.pth`, `basemap_data_hires-…-nspkg.pth`, **`__editable__.src-1.0.pth`** | see §4.1.4 — the editable one **must be neutralised** |
| **native libraries** | 431 `*.so` | copied byte for byte, **never rebuilt**; digests compared like any other file |
| **symlinks** | **0** in the tree today | the clone asserts zero symlinks. If any appear, the run stops: a symlink would leave the clone reading files it does not own |
| **bytecode** | 1,385 `__pycache__` dirs, 12,130 `*.pyc` | copied **with mtimes preserved**. `.pyc` validity is an mtime-and-size check against its source, so a copy that loses mtimes silently invalidates 12,130 caches and changes what the first request does |
| **build provenance** | 255 `RECORD`, 3 `direct_url.json` | copied; recorded, not interpreted |
| **secrets** | see §4.1.5 | scanned by filename, contents never read |

##### 4.1.3 What the clone does **not** establish

It is a copy of the package tree. It is **not** a byte-equivalent runtime, and no
result from it may be described as one:

- **the standard library is not in the tree.** It lives under
  `…/versions/3.11.4/lib/python3.11` and is reached through the venv's `pyvenv.cfg`.
  The clone carries production's *packages*, not production's *Python*.
- **the interpreter binary is not copied** — see the PI decision in §4.1.6.
- **the process is not production's.** Different uptime, different memory state,
  different page cache, no accumulated state from serving traffic.

##### 4.1.4 The editable install is a real hazard, not a formality

`__editable__.src-1.0.pth` runs `__editable___src_1_0_finder.install()`, and that
finder points at:

```
/home/odbadmin/python/woa23/src        exists: yes    inside site-packages: NO
```

That is **production's own source directory**. Copied as-is into the clone, the
`.pth` would still redirect `src.*` imports there — so both arms would import
production's live `src`, from outside the clone, outside the read-only reference
copy, and outside anything the provenance check hashes. The comparison would be
between two processes reading the same production files, which is not the
comparison this spec describes.

S1 was not exposed to this: it ran under `dev2026/.venv`, built from `uv.lock`, which
has no such `.pth`. The hazard appears only because C1 uses production's tree.

**Acceptance:** the editable `.pth` and its finder module are excluded from the clone,
their absence is asserted, and `sys.path` inside each arm is recorded in provenance
and checked to contain **no path outside the clone, the arm's own staging directory,
and the stdlib**. This is a deliberate deviation from "identical to production" and is
recorded as one — production really does load `src` that way, and the clone
deliberately does not.

##### 4.1.5 Secrets exclusion

Scanned by **filename only**; no file contents are read at any point, by the scan or
by this spec's author.

Patterns: `*.pem *.key *.crt *.p12 .env* *credential* *secret* *token* id_rsa*
id_ed25519* .netrc .pgpass`.

**68 filenames matched, and every one examined is a false positive by name** —
library source such as `packaging/_tokenizer.py`, `keyring/credentials.py`,
`dns/tokenizer.py`, and `pip/_vendor/certifi/cacert.pem`, which is a public CA bundle.
No credential material was found.

Acceptance: the scan is re-run against the **clone** after copying; any match that is
not on the reviewed false-positive list stops the run for a human to look at. A name
scan cannot prove the absence of secrets and is not claimed to — it is a cheap check
against the obvious, and the stronger control is that the tree copied is a package
directory, not a configuration or data directory.

##### 4.1.6 PI decision — the interpreter

**Preferred: run the clone under production's own Python binary.** That removes the
last interpreter variable, and the binary is read and executed but never modified.

Executing a production binary is a different act from reading production's files, and
every prior authorisation in this project has been for reading. **It needs granting
explicitly.**

**If it is not granted**, the clone runs under a separately installed Python 3.11.4
and the difference is carried as an explicit limitation: same version, different
build, and anything sensitive to interpreter build flags is outside what C1
establishes.

| | |
|---|---|
| environment | the package-tree clone above, **shared by both arms** |
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
seed and its output is self-consistent, so nothing about seed variation can be read
from it.

**PI decision: three independent start/stop cycles.** The arms are started, the cases
run, both are stopped, and the whole thing is repeated three times.

What those three cycles establish is **the seed diversity observed in this run** — not
that the seed is unpinned in general, and not a bound on how often an ordering
difference appears. Three observations of a randomised value is three observations.

The outcome is therefore one of three, and the third is not a failure:

| observed | verdict |
|---|---|
| seeds differ across cycles **and** all three cycles pass the contract | **PASS** |
| any cycle fails the contract | **FAIL** — reported with the cycle and the case |
| all three cycles record the **same** seed | **INSUFFICIENT** — the premise was not exercised, so the contract result says nothing about unpinned seeds |

`INSUFFICIENT` is reported as it stands. **The run does not add cycles to chase a
different answer**: deciding to sample more after seeing the sample is how a result
stops meaning what it appears to mean. If more cycles are wanted, the number comes
from the PI, before the next run.

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

### 6.1 Order-stability acceptance

Measured per arm, per case, across C2's three cycles. Two forms, and the second is
used only where the first cannot be:

| form | comparison | when |
|---|---|---|
| **raw byte** | the arm's response body from cycle *n* compared byte for byte with cycle 1 | the default. It is the strongest and needs nothing parsed |
| **order fingerprint** | SHA-256 over the ordered column-name list, followed by the ordered sequence of `(lon, lat, depth, time_period)` index tuples | when the bodies legitimately differ in something that is not order — a timestamp, a float rendered differently — so a raw comparison would report a difference that is not an ordering one |

The fingerprint deliberately excludes values: it answers "was the same content laid
out in the same sequence", not "was the same content returned". The latter is C2's
question and is already answered semantically.

**Per case, per arm, the outcome is:**

| | |
|---|---|
| all three cycles identical (bytes, or fingerprint) | `STABLE` |
| any cycle differs | `UNSTABLE`, recorded with the first differing cycle and whether it was column order, row order, or both |
| C2 returned `INSUFFICIENT` | order stability is `UNKNOWN` — three cycles that happened to share a seed cannot show order varying with the seed |

### 6.2 Order stability does **not** decide C2

They are separate verdicts and conflating them would make each less useful:

- **C2 is 5.2B semantic and is order-insensitive by construction.** An `UNSTABLE`
  case can and should still pass C2: the content is the same, the sequence is not.
  Failing C2 on it would be measuring the thing 5.2B explicitly does not measure.
- **`UNSTABLE` is reported as a finding in its own right**, because it is one. S1
  established that this candidate's output order depends on the *string* used to
  reach the store, which means order is a function of configuration, not only of
  code. A consumer indexing CSV columns by position, or a GeoServer SQL view bound to
  a column sequence, is exposed to that whether or not the two arms agree.
- **An `UNSTABLE` result blocks nothing in S2 and decides nothing about deployment.**
  What to do about it belongs to the deployment and consumer-compatibility work, not
  here. S2's obligation is to find it and say so.

A case that is `STABLE` on the reference and `UNSTABLE` on the candidate — or the
reverse — is the most interesting outcome available from C2 and must be reported
prominently rather than averaged into a pass.

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
2. **Executing production's Python binary** — the PI's preference for C1, recorded in
   §4.1.6. Every authorisation so far has been to *read* production; running its
   interpreter is a different act and needs granting on its own. If it is not
   granted, the interpreter difference stands as a stated limitation and C1 still
   runs.
3. **The 240 vs 236 discrepancy** in §4.1.2 — four `dist-info` directories that yield
   no distribution. Whether that is stale state in production, a packaging quirk, or
   something else is unknown, and it should be understood before the clone is called
   faithful.
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
