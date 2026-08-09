# WOA23 refactor roadmap (2026)

Two phases, matching the two consumers of the dataset. Phase 1 is broken into
steps; each step gets its own spec in this directory. Phase 2 is sketched only —
it will be broken down once phase 1 lands.

## How a step moves

```
spec (Claude)  →  review (Codex)  →  implement (Claude)  →  review (Codex)  →  merge
```

A step is not started until its spec has passed review. Every step that claims a
performance change ships a before/after benchmark JSON in `dev2026/results/`,
produced by the shared harness so the two runs are comparable.

## Standing constraints

- Nothing existing is overwritten. `woa23_app.py`, `src/`, `dev/`, `conf/`,
  `requirements.txt`, `Pipfile` are untouched. New code lives under `dev2026/`.
- The remote production directory on VM24 (`~/python/woa23`) is read-only to us.
- No load generation against production beyond light sequential probing.
- Disk and data cleanup is the PI's decision and the PI's action.
- The API is published with a DOI and has external users. Any change to returned
  values, column names, ordering, null handling, or the JSON/CSV contract requires
  an explicit versioning decision — it is not a side effect anyone may take.

## Reference

- [`D2a-request.md`](D2a-request.md) — granted and executed, rung 21
- [`D2b-request.md`](D2b-request.md) — **requested, not granted**: the controlled
  two-arm run that removes the hash-seed and environment variables 5.2B could not
- [`../CODEX_REVIEWER.md`](../CODEX_REVIEWER.md) — **canonical** reviewer briefing.
  It stays at `dev2026/CODEX_REVIEWER.md`; `BASELINE.md` supplements its
  measurements but never overrides its rules.
- [`docs/BASELINE.md`](docs/BASELINE.md) — the measurement record all specs cite
- `../README.md` — the benchmark harness
- `../results/` — raw measurement JSON

**Work happens on `perf/2026-s1-remove-dask`; `main` stays clean.** A spec is
committed once it passes review; implementation and benchmarks are separate
commits after that.

---

## Phase 1 — Web API / Zarr (VM24)

Ordered by measured value, not by convenience. Steps S1–S3 are independent of each
other in principle, but running them in order keeps benchmark attribution clean:
each step's before/after is measured against the previous step's end state.

### S1 — Remove Dask from the read path

**Spec:** [`001-remove-dask-read-path.md`](001-remove-dask-read-path.md) · **Status:**
**measured, with a caveat.** Latency gate **PASS** at rung 21 — every case
established both no-regression and improvement, 1.39×–5.99×, nothing inconclusive.
Contract gate **62/64 semantic match**; the two exceptions were a harness defect
since fixed, and C20's byte equality rests on local in-process evidence, not an HTTP
result. **The A/B is not a clean isolation of the Dask change**: the twelve pinned
packages matched on both arms but 23 shared transitive dependencies did not,
including `fsspec` and `anyio`. Quotable as directional evidence; not as a measured
Dask-only speedup.

The app installs a distributed Dask client as the *default* scheduler, so every
xarray compute round-trips through a scheduler backed by a single worker shared
with `tide_app` and `mhw_app`. Removing it is 1.35×–8.4× faster across every query
shape measured, from 102 rows to 64,800. Largest win, smallest change, no
data-format consequences.

**Gate as run:** variant 5.2B — *semantic* equality over the full contract case
list, because D2a alone cannot pin production's hash seed. Byte-identical output is
what variant 5.2A would establish, and that needs D2b.

### S2 — Production environment correctness

**Spec:** `dev2026/specs/002-production-correctness-deploy-hardening.md` (revision 15)
· **Status:** C1 done; D1, C2 and PM2 deployment validation still **open**

| step | what it establishes | status |
|---|---|---|
| **C1** | isolated package-tree contract correctness, 5.2A byte-exact | **PASS 2026-08-09** — see below |
| **C2** | multi-worker, unpinned seed, 5.2B semantic, seed diversity | **not run, not authorised** |
| **D1** | startup failure modes | **measured, open** — only a missing env var fails before serving; an invalid or non-Zarr store starts and fails per request. No candidate change proposed. |
| **PM2 deployment validation** | production's real launcher, site/`.pth` semantics, readiness | **open** — C1's launcher is a shell script and ran under `-S` |

C1's result is recorded below and in `specs/docs/BASELINE.md` under
*non-performance contract evidence*. **It carries no performance meaning and does not
mean the deployment is validated.**

The three host-configuration defects below are separate from C1 and remain open:

Three defects found on the host that are configuration, not code:

- Polars is running a build that requires AVX2/BMI1/BMI2/LZCNT on a VM where
  VMware masks those features. Polars itself warns it "will likely result in a
  crash". Both `polars` and `polars-lts-cpu` 1.27.1 are installed and the AVX2
  build is the active one. This sits in the API's hot path.
- `--reload` is live in production gunicorn (`conf/start_app.sh`), on WOA23 and on
  the neighbouring services.
- The CSV endpoint creates `NamedTemporaryFile(delete=False)` and never removes it
  (`woa23_app.py:440`).

Each needs its own before/after evidence — in particular, whether switching to the
LTS polars build changes throughput at all, and whether it changes any returned
value.

### S2b — Deterministic output ordering

**Spec:** not yet written · **Status:** blocked on S1 · **Needs a PI contract decision**

`woa23_app.py:160,171,190` build `list(set(...))` over strings. Python randomises
string hashing per interpreter, so `variables` (which drives **column order** after
the pivot) and `pars` (which drives **row order**) come out in an order that is
fixed for the life of a gunicorn master but **changes on every restart**. Verified
across seeds 0–7: `variables` alternates between `['an','mn']` and `['mn','an']`,
and `pars` takes three different orders.

Twenty-four live production requests all returned the same column order, because
gunicorn's workers are forked from one master and inherit its hash seed — so the
instability is invisible until a restart.

Whether any consumer depends on the current ordering is **unmeasured**, and this
roadmap does not assume an answer. Column and row order are user-visible output, the
API is published with a DOI, and we have no telemetry on how clients parse it.
Making the ordering deterministic is therefore a contract change and the PI's call.
If it is wanted, the honest first move is to find out who is affected — nginx access
logs would at least show the shape of real traffic — rather than reasoning about
what a consumer ought to be doing.

S1 deliberately preserves the existing behaviour verbatim so its before/after stays
about Dask.

### S2c — Dependency modernisation

**Spec:** not yet written · **Status:** blocked on S1

Production pins are a year or more old: FastAPI 0.115.12, Uvicorn 0.34.1,
Starlette 0.46.2, Gunicorn 23.0.0, ORJSON 3.11.4, Pydantic 2.11.3, polars 1.27.1,
xarray 2025.3.1, zarr 2.18.6, dask 2025.3.0. Newer releases may carry real
throughput gains — polars in particular, given S2's CPU-baseline problem, and
`polars-runtime-32 1.35.2` is *already installed* on VM24 alongside 1.27.1.
Zarr v3 is a larger question with data-format consequences.

Each upgrade needs its own paired benchmark and a contract re-verification; a
version bump that changes a returned value is a regression, not an upgrade. Kept
out of S1 so that S1's A/B isolates the Dask change alone.

### S3 — Cache opened Zarr datasets

**Spec:** not yet written · **Status:** blocked on S1

Worth ~10% measured. Deferred behind S1 because it is small, and because it
interacts with the gunicorn multi-worker model, pm2's `max_memory_restart: '4G'`,
and the OS page cache over a 32 GB store. The obvious implementation has
non-obvious failure modes.

### S4 — Concurrency: unblock the event loop

**Spec:** not yet written · **Status:** blocked on S1

`process_woa23_data` is `async def` but does blocking CPU and I/O work, so it
occupies the event loop; with `-w 2` that bounds real concurrency hard. **No
concurrency benchmark exists yet** — every number in `docs/BASELINE.md` is from
sequential probing. This step starts by building that measurement, against a
non-production target.

### S5 — Cold-cache measurement and the re-chunking decision

**Spec:** not yet written · **Status:** blocked on S4

Chunks are `{time_periods:1, parameters:1, depth:8, lat:90, lon:360}` for both
grids, so a single-cell profile decompresses 12.6 MiB to return 102 values
(×32,400 amplification). This is nearly free today only because the whole 31.9 GiB
store fits in VM24's page cache — cold, the same query takes ~2,140 ms against
19.6 ms warm.

**This step may legitimately conclude "do nothing".** Re-chunking costs a full
rebuild plus storage and optimises one access pattern at another's expense. The
decision needs a cold-cache benchmark that does not yet exist. Do not pre-commit.

### S6 — Cutover

**Spec:** not yet written · **Status:** blocked on S1–S5

Deploy the candidate, re-verify the contract against production, review the nginx
proxy cache policy for a dataset that never changes, and retire the old process.
The nginx cache is currently masking real performance from anyone measuring
casually — that is a measurement hazard, but for a static dataset it may also be
an under-used opportunity.

---

## Phase 2 — PostGIS / GeoServer (VM34)

Sketch only. To be broken into steps after phase 1. Evidence so far is in
`docs/BASELINE.md`; none of it is confirmed by `EXPLAIN ANALYZE`.

1. **Confirm or kill the `MIN/MAX(value) OVER ()` hypothesis.** All ten SQL views
   compute a global min/max inside the virtual table, which GeoServer's tile BBOX
   filter cannot be pushed past. If the hypothesis holds, every WMS tile scans a
   full global slice — 64,800 rows at 1-degree, ~1,036,800 at 0.25-degree.
2. **Precomputed statistics table** for the colour ramps, if (1) confirms.
3. **Storage model review.** `woa23` is 80 GB holding less data than the 32 GB
   Zarr store, because every cell stores a Polygon per depth per time period.
4. **Decide the fate of 54 GB of unreferenced tables.** `grd025_monthly_ts` and
   `grd025_seasonal_ts` (236 M rows) have no GeoServer layer and no mention in
   GeoServer's query logs. PI's decision.
5. **Rewrite the zarr→PostGIS ingest.** `dev/zarr2postgis.ipynb` inserts row by row
   through four nested Python loops, building a Shapely polygon per cell.
6. **GeoServer / GWC tile-cache review.**

## D2b controlled run, 2026-08-08 — the first result that may be quoted

**Commit `48ca10f`**, VM24, workdir `~/woa23-s1-controlled-r6`. Both gates PASS and
cleanup confirmed itself. Three earlier attempts (r2, r3, r4) are recorded as FAIL
and **none of their numbers are carried into this table** — they were produced by a
harness whose cleanup could not verify itself, and two of them by one that let the
benchmark introduce a difference of its own.

### Contract gate — PASS, 64/64 byte-exact

Variant 5.2A, all 64 verdicts `MATCH`, no `DIFFER`, no `ERROR`. Request order
counterbalanced **RC 32 / CR 32**, recorded per case in the artefact.

`C16` and `C16-csv` — the two multi-group cases that differed on 2026-08-08's first
attempt — match byte for byte here. The candidate's ordering logic was never
changed: `api/query.py` is `93b64fc0…`, the same blob as at `ee30084`. What changed
is that the benchmark stopped introducing a difference: both arms now interpolate the
identical store literal.

### Latency gate — PASS, 8/8

rung 21, 21 warm samples per arm (+1 discarded), ±5% practical-significance margin,
bootstrap 5,000 rounds, seed 20260805. `ratio` is candidate ÷ reference, so lower is
faster.

| case | ratio (median) | 95% CI | ≈ speedup | verdict |
|---|---|---|---|---|
| `point_profile_multiparam` | 0.0651 | [0.0641, 0.0660] | ~15.36× | NO_REGRESSION · IMPROVED |
| `point_profile` | 0.1045 | [0.1026, 0.1069] | ~9.57× | NO_REGRESSION · IMPROVED |
| `point_profile_025` | 0.1162 | [0.1120, 0.1209] | ~8.61× | NO_REGRESSION · IMPROVED |
| `readme_example` | 0.1524 | [0.1477, 0.1549] | ~6.56× | NO_REGRESSION · IMPROVED |
| `small_bbox_full_depth` | 0.2115 | [0.2094, 0.2147] | ~4.73× | NO_REGRESSION · IMPROVED |
| `regional_bbox` | 0.4357 | [0.4284, 0.4471] | ~2.30× | NO_REGRESSION · IMPROVED |
| `regional_bbox_025` | 0.4931 | [0.4414, 0.5077] | ~2.03× | NO_REGRESSION · IMPROVED |
| `surface_global` | 0.7795 | [0.7632, 0.7918] | ~1.28× | NO_REGRESSION · IMPROVED |

Every case establishes **both** `NO_REGRESSION` and `IMPROVED`, and every interval
lies wholly below 1.0.

### What made this run controlled

| variable | D2a (2026-08-07) | here |
|---|---|---|
| package environment | 23 shared distributions differed | **one venv**, `distributions_sha256 cca6aa8460ab175a` on both arms |
| interpreter | production's, unverified | **Python 3.11.4**, 58 distributions, both arms |
| hash seed | production's could not be pinned | **`PYTHONHASHSEED=0`** on both arms |
| store path string | absolute vs `data/` | **`'data/'` on both**, canonical store `/home/odbadmin/python/woa23/data` on both |
| comparison | semantic | **byte-exact 5.2A** |
| request order | reference always first | **RC 32 / CR 32** |
| boot | — | `7c674929…` on both arms |

`post_run_drift: []`, `metadata_complete: true`, `metadata_problems: []`.

### Traffic and isolation

- **production `8050`: 0 requests.** The harness was invoked against
  `http://127.0.0.1:8051` and `http://127.0.0.1:8052` only.
- production's shared Dask scheduler on `8786` was never contacted; this run used an
  isolated scheduler on `127.0.0.1:18787`.
- 4 services / **6 OS processes**, verified against the authorised set before the
  gates ran.
- `~/python/woa23` was never written; the reference read the store through a
  read-only symlink.

### Cleanup — PASS

All four services stopped, every process in every recorded tree confirmed exited,
all three ports confirmed free, state files removed, and production on `8050`
unchanged across the run — full listener set `[3960 4334 4366]`, master PID, start
time and boot id all matching what was recorded at preflight.

### What this result is, and what it is not

It is an **8-case, warm-cache, loopback, single-worker controlled comparison** of the
same code with and without Dask on the read path, at rung 21.

It is **not**:

- a public SLA or any statement about what users experience;
- a cold-cache measurement — the 31.9 GiB store was resident in VM24's page cache,
  and these ratios say nothing about a cold store;
- a concurrency result — both arms ran `-w 1` and requests were issued one at a
  time, so nothing here describes behaviour under load;
- a measurement over TLS, nginx or the public path — it is plain HTTP on loopback;
- generalisable beyond these eight queries.

`surface_global` at ~1.28× is the smallest gain and the closest to the margin, which
is expected: it is the largest query, so decompression and serialisation dominate and
Dask's scheduling overhead is proportionally smallest. It is the case to re-examine
first if the rung or the case set changes.

Rung 60 and rung 150 were **not** run. `next_rung: 60` in the artefact is the
ladder's suggestion, not an authorisation.

---

## C1 controlled run, 2026-08-09 — isolated package-tree contract correctness

**Result: C1 PASS — isolated package-tree contract correctness.**

Read the name in full. It is not "C1 PASS", and §"What this is not" below is part of
the result rather than a caveat attached to it.

| | |
|---|---|
| commit | **`c1166bfa7110ad7687360cab06e74fa35a2ac119`** |
| archive verified before shipping | `cc960eb760eef6927f8ddabaf3c6a75f3f70727e785179ac64222d4c8ad21490`, 75 files, file-list `4ee0d631…baac5b74` |
| staging | `~/woa23-s2-c1d/`, workdir `~/woa23-s2-c1d-work/`, both new |
| gate | **5.2A byte-exact**, 64 cases per arm |
| verdict | **64/64 MATCH, 0 DIFFER** |
| request order | **RC 32 / CR 32**, counterbalanced |
| bytes compared | reference 24,440,431 / candidate **24,440,431** |
| error-status cases | **15** (400 and 404) — **byte-exact MATCH as well**, not excluded |
| captured | 2026-08-09T19:01:40+0800 on odb24 |

### The environment both arms ran in

Production's own interpreter, against the read-only clone of production's package
tree. Identical on both arms, field for field:

| | |
|---|---|
| interpreter | `/home/odbadmin/.pyenv/versions/py311/bin/python3.11`, named explicitly, not derived |
| `PYTHONHASHSEED` | **0**, both arms |
| `store_path_literal` | `'data/'`, both arms |
| `clone_manifest_sha256` | `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4` |
| `package_tree_digest` | `b8754d32c8aaec6d2049de5d67d3f81aeff4f19effd1525d76b62955447c9b4b` (240 dist-info directories) |
| `runtime_distribution_digest` | `a26ca6c3cfe20ea643c30075d910bb03dbbc01eb3f9d2b4fd224b5d76701447b` (236 with `METADATA`) |
| `name_version_set_sha256` | `60236d7210c8c3647a32e7da55714e246ecc878d1d6296d10e9caf966d2b0b2a` — the **deprecated** name-keyed canonicalization (spec 002 §7a.3f). **It is not the runtime-distribution digest.** |

The **harness bootstrap is a separate record**: `dev2026/.venv`, 58 distributions,
lockfile `0d2980a5…`. It runs the comparator and is on no arm's import path. `uv`
was authorised for that directory and nothing else; the clone and production
site-packages were never written.

### Clone integrity — three full verifications, all MATCH

| stage | problems | entries / files | bytes | seconds |
|---|---|---|---|---|
| preflight | 0 | 33,565 / 33,565 | 1,690,025,002 | 29.59 |
| before-reference | 0 | 33,565 / 33,565 | 1,690,025,002 | 6.89 |
| before-candidate | 0 | 33,565 / 33,565 | 1,690,025,002 | 6.85 |

Clone parent mode **555**; ancestor chain checked; every file re-hashed each time,
not sampled.

### Import isolation

Both arms: no problems, `no_site=1`, `ignore_environment=0`. Two tracked processes
each; 342 and 343 mapped files respectively, **0 from production**.

### The two cases that differed in the D2b run of 2026-08-08

C16 (37,083 bytes, 204 rows) and C16-csv (10,843 bytes) — **both MATCH**, with
identical row-order digests (`56ccfd33…`) and identical column sequences on the two
arms. That difference was the benchmark handing the arms different store strings; here
both build group paths from `'data/'`.

### Traffic, processes and cleanup

- **6 OS processes**, derived from the measured worker count and verified against it.
- **Requests: 64 contract + 2 data probe + 1–30 readiness per arm = 67–96 per arm,
  134–192 total.** Ceilings were 96 and 192.
- **Production 8050: 0 requests.** 8786 and 8787 were never connected to.
- **Cleanup PASS.** All four services stopped, every process in every recorded tree
  exited, 18061/18062/18798 confirmed free, boot id matched, production unchanged
  (master 3960, start time 1874, listeners 3960/4334/4366). No `.pid`, `.tree`,
  `.uncertain` or `.diag` left behind.
- Afterwards: clone 33,565 files, **0 writable, 0 new `.pyc`**, manifest digest
  unchanged; production site-packages 33,567 files at mtime 2026-02-12;
  `~/python/woa23` at mtime 2026-08-05.

### What this result is

The candidate and the unmodified reference return **byte-identical responses across
all 64 contract cases** — including every error-status case — when both run on
production's interpreter and a read-only copy of production's package tree, with a
pinned hash seed, one worker each, in isolated staging.

### What this result is **not**

Each of these is a limitation of the run, not a hedge about it.

- **Not deployment validated, and not ready to deploy.** A contract gate compares two
  processes this harness started, in a staging directory, under `-S`, with a store
  symlink, launched by a shell script.
- **`-S` means `site.py` never ran**, so no `.pth` in the clone was processed —
  `distutils-precedence.pth` and the basemap nspkg `.pth` are present and did not
  execute. Production's site/`.pth` startup semantics were not exercised.
- **The launcher is not production's PM2 path.**
- **No worker-level Python import provenance exists.** The probe is a sibling
  interpreter launched by the same procedure; `/proc/<pid>/maps` can *refute*
  isolation but its silence proves nothing, because it lists mapped files and not
  imports. "The workers imported from the clone" is an inference.
- **`/home/odbadmin` is writable**, so this account can still re-point the clone's
  path. The three manifest verifications are **bounded detection, not immutability**.
- **D1 is not fixed.** A missing `WOA23_ZARR_STORE` fails at import and startup; an
  invalid or non-Zarr store still starts cleanly and fails on the first data request.
  No candidate change is proposed or made.
- **C2 has not been run.** Multi-worker behaviour and unpinned-seed ordering are
  unexamined.
- **No latency, throughput or resource conclusion of any kind.** None was measured;
  the latency gate and the noise pilot did not run.
