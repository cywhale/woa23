# Measurement record — WOA23 performance baseline

Captured 2026-08-05. Raw JSON lives in `dev2026/results/`; the harness that
produced it is documented in `dev2026/README.md`. Specs cite this file rather
than restating numbers.

## Baseline: production, 2026-08-05

Client on the same campus network (RTT 0.86 ms), so network time is negligible.
Median of 5 cache-busted requests.

> **Read the small-case numbers loosely.** A later noise pilot
> (`results/noise_pilot_prod.json`, `bench/noise_pilot.py`) sampled one backend 25
> times per case and bootstrapped the ratio between two arms of *k* draws. At
> `k=3` the noise floor is **±84%** for `point_profile` and **±77%** for
> `small_bbox_full_depth`; at `k=5`, ±27% and ±51%. So the sub-250 ms entries in
> this table and the next carry tens of percent of uncertainty and must not be
> quoted to three significant figures. `readme_example` is stable (±11% at k=3,
> ±1.6% at k=21) because it is dominated by real work rather than per-request
> jitter, and the Zarr-level measurements below are quieter still — they run
> on-host with no nginx, TLS, or network. The headline finding is unaffected; the
> precision of the small HTTP figures is not what it looks like.

| query | Zarr groups touched | rows | payload | median |
|---|---|---|---|---|
| `point_profile` | 1 | 102 | 8.8 KiB | **176 ms** |
| `point_profile_025` | 1 | 102 | 11.7 KiB | **152 ms** |
| `small_bbox_full_depth` | 1 | 9,792 | 832 KiB | **233 ms** |
| `regional_bbox` | 1 | 16,275 | 1.77 MiB | **211 ms** |
| `readme_example` | 6 | 4,992 | 1.16 MiB | **1,298 ms** |
| `point_profile_multiparam` | 6 | 318 | 72 KiB | **1,974 ms** |

The first reading of this table was that cost tracks the **number of Zarr groups
opened**, pointing at the uncached `xr.open_zarr` in the request loop
([`woa23_app.py:234`](../woa23_app.py#L234)). Measurement on VM24 refuted that.
The group count correlated only because the multi-group cases are also the
many-chunk cases. See below.

## White-box: VM24, 2026-08-05

`results/survey_vm24.json`, `results/zarr_vm24_all.json`, `results/zarr_vm24_cached.json`.

VM24: 20 cores, 62 GiB RAM (~54 GiB in page cache), VMware. Store is 31.9 GiB
across 12 groups — **it fits entirely in page cache**, which matters below.
Benchmarks ran under the production interpreter
(`~/.pyenv/versions/py311/bin/python3.11`), not a separate env, so the versions
are exactly production's.

Warm median, milliseconds:

| query | `numpy` (no dask) | `threads` | `distributed` (production) | prod HTTP | chunks read | decompressed | amplification |
|---|---|---|---|---|---|---|---|
| `point_profile` | **19.6** | 48.6 | 108.5 | 176 | 13 | 12.6 MiB | ×32,400 |
| `point_profile_025` | **29.1** | 114.7 | 244.0 | 152 | 26 | 25.2 MiB | ×32,400 |
| `small_bbox_full_depth` | **37.1** | 68.4 | 117.2 | 233 | 13 | 12.6 MiB | ×338 |
| `regional_bbox` | **56.7** | 87.5 | 197.2 | 211 | 8 | 7.9 MiB | ×64 |
| `readme_example` | **186.8** | 701.0 | 1,220.0 | 1,298 | 48 | 47.5 MiB | ×415 |
| `point_profile_multiparam` | **258.6** | 906.1 | 2,097.2 | 1,974 | 242 | 224.7 MiB | ×32,400 |

### 1. The harness reproduces production

The `distributed` column tracks the production HTTP column closely
(2,097 vs 1,974; 1,220 vs 1,298; 197 vs 211). So the whole cost lives in the data
pipeline — FastAPI, ORJSON serialisation and nginx together account for only tens
of milliseconds. Optimising the web layer would be wasted effort.

### 2. Dask is the bottleneck, and it is a pure loss here

Dropping Dask entirely (`chunks=None`, plain lazy Zarr reads) is **3–8× faster
than the production configuration** on every single case:

| | speedup vs `distributed` |
|---|---|
| `point_profile_multiparam` | **8.1×** |
| `point_profile_025` | 8.4× |
| `readme_example` | 6.5× |
| `point_profile` | 5.5× |
| `regional_bbox` | 3.5× |
| `small_bbox_full_depth` | 3.2× |

The two heavy cases were added later (`results/zarr_vm24_heavy.json`) precisely
because they are where Dask would be expected to win — more work per task
amortises the scheduling overhead. It does not win there either, though the
margin narrows:

| query | `numpy` | `threads` | `distributed` | rows | speedup vs `distributed` |
|---|---|---|---|---|---|
| `surface_global` | **122.8** | 128.7 | 165.9 | 64,800 | 1.35× |
| `regional_bbox_025` | **123.7** | 218.4 | 298.3 | 42,025 | 2.4× |

So across the whole size range measured — 102 rows to 64,800 rows — there is no
query shape for which Dask pays for itself.

Even the purely local threaded scheduler loses to no-Dask. The slices here are
small enough that graph construction and per-task scheduling dominate the actual
work, and `distributed` adds TCP round-trips per task on top. Production makes
this worse: `dask-scheduler` is backed by a **single** `dask-worker` process
(`--memory-limit 8GB`) shared with `tide_app` and `mhw_app` on the same host, so
WOA23 requests queue behind other services.

### 3. `open_zarr` per request is *not* the problem

Every group is consolidated (`.zmetadata` present in all 12), and `open_zarr`
costs 6.7–110 ms. Caching the opened datasets across requests
(`--cache-stores`) buys only **~10%**:

| query | no cache | store cache |
|---|---|---|
| `point_profile_multiparam` | 242.6 | 220.6 |
| `readme_example` | 185.1 | 163.7 |
| `regional_bbox` | 56.8 | 51.5 |

Worth doing eventually, but it is not the win.

### 4. Read amplification is real but currently hidden by the page cache

Chunks are `{time_periods:1, parameters:1, depth:8, lat:90, lon:360}` — about
1 MiB each — for **both** grids. At 1-degree that is half the globe per chunk;
at 0.25-degree it is a 22.5°×90° tile. A single-cell full-depth profile therefore
decompresses 13 chunks (12.6 MiB) to return 102 values: **×32,400**.
`point_profile_multiparam` decompresses **224.7 MiB to return 318 rows**.

This costs little today only because all 31.9 GiB fits in VM24's page cache. The
cold penalty is visible in the very first query of each run: `point_profile` cold
is ~2,140 ms against a 19.6 ms warm. Any growth in the dataset, memory pressure
from co-tenant services, or a host restart turns that cold number into the normal
one.

### 5. Production hygiene, confirmed on the host

- **`--reload` is live.** Three `gunicorn ... -w 2 ... --reload` processes,
  54 days uptime. Same flag on `tide_app` and `mhw_app`.
- **Polars is running on a CPU without the instructions its build requires.**
  The host is a Xeon Gold 6326, but VMware masks `avx2`, `bmi1`, `bmi2` and
  `lzcnt`. Polars emits *"Continuing to use this version of Polars on this
  processor will likely result in a crash"*. Both `polars` and `polars-lts-cpu`
  1.27.1 are installed (plus `polars-runtime-32` 1.35.2) and the active build is
  the one that wants AVX2. This is a latent crash risk in the API's hot path, and
  separately a performance question.
- Leftover `data/test` Zarr group in the store.

### Where this leaves the optimisation order

1. Remove Dask from the read path — largest win, smallest change, no data-format
   consequences.
2. Resolve the polars build so the hot path is not running on an unsupported CPU
   baseline.
3. Cache opened datasets (~10%, and it interacts with the multi-worker model).
4. Re-chunking: only justified by cold-read behaviour, not by warm numbers, and
   it costs a rebuild plus storage. Needs a cold-cache benchmark before anyone
   commits to it.

## Phase 2 first survey: PostGIS / GeoServer on VM34

VM34: 16 cores, 125 GiB RAM, PostgreSQL 16 on port 5433, PostGIS, GeoServer with
`GEOSERVER_DATA_DIR=/usr/share/geodata`. Server tuning looks reasonable
(`shared_buffers=16GB`, `effective_cache_size=72GB`, `work_mem=64MB`,
`random_page_cost=1.1`, `jit=off`).

The ten SQL view definitions are captured in
`results/geoserver_sqlviews_vm34.txt` — read straight from the GeoServer data
directory, no database credentials needed. All ten share one shape, ending in:

```sql
SELECT parameter_view.*,
  MIN(value) OVER () AS minvalue,
  MAX(value) OVER () AS maxvalue
FROM parameter_view
WHERE value IS NOT NULL AND value != 'NaN'
```

An empty `OVER ()` frame evaluates across the whole CTE — every grid cell globally
at that depth and time period. GeoServer applies the tile BBOX *outside* the
virtual table and cannot push it inside without changing the window semantics, so
the hypothesis is that **every WMS tile scans a full global slice**: 64,800 rows
at 1-degree, ~1,036,800 at 0.25-degree. Needs `EXPLAIN ANALYZE` before anyone acts
on it.

`woa23` is 80 GB. `grd025_monthly_ts` (32 GB, 141.7M rows) and
`grd025_seasonal_ts` (22 GB, 94.8M rows) have **no GeoServer layer referencing
them** — 54 GB and 236 million rows that the map portal never reads. A sweep of
the whole `/usr/share/geodata` tree, GeoServer's query logs included, found zero
mentions of either table, while `grd025_annual_ts` appears in six files. Whether
anything outside GeoServer uses them is unverified.

VM34's root filesystem had **72 GB free (93% full)** when first surveyed. The PI
cleaned up on 2026-08-05 and it is now **407 GB free (56% used)**, so a rebuild or
`VACUUM FULL` on those tables is no longer blocked on disk. The databases were not
touched — `woa23` is still 80 GB.

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

# Non-performance contract evidence

**Nothing in this section is a performance baseline.** It records contract
correctness only. No timing was collected, no latency or throughput claim may be
drawn from any of it, and none of it belongs in the measurement record above.
It lives in this file so the contract evidence and the measurement evidence can be
found together, and it is fenced off so they cannot be confused.

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

---

## C2 controlled run, 2026-08-09 — semantic correctness at production's worker count

**Result: C2 PASS — isolated package-tree semantic correctness at production worker
count, with observed sibling seed diversity.**

The name is the result. §"What this is not" is part of it, not a caveat appended
to it.

| | |
|---|---|
| commit | **`5cfbf0aa70883af28ffd2e03ce21b4497ac30891`** |
| archive verified before shipping | `3f642dd0…ace0ba`, 78 files, file-list `3be3d046…7fe5c909` |
| staging | `~/woa23-s2-c2c/`, workdirs `~/woa23-s2-c2c-work-cycle{1,2,3}`, all new |
| cycles | **three independent start/stop cycles** |
| gate | **5.2B semantic**, 64 cases per arm per cycle |
| seed policy | **`both-unpinned`** — `PYTHONHASHSEED` **unset on both arms**, stated explicitly rather than inherited from the variant |
| verdict | **PASS (exit 0)** |

### Production's worker count, measured not assumed

Read from production's own argv at run time in **every** cycle: **actual = 2**.
`--expected-workers 2` was an assertion only; the arms take the measured number and
a disagreement would have aborted before any arm started. Each cycle also re-read
production's listener set, master PID, start time and boot id after the measurement
and confirmed them unchanged.

Process count is derived from that measurement — `2 Dask + 2 × (1 arbiter + 2
workers)` = **8 per cycle**, verified against the tracked trees:

| cycle | the eight processes |
|---|---|
| 1 | 3631498 3631556 3631617 3631619 3631639 3631715 3631717 3631718 |
| 2 | 3633166 3633229 3633282 3633284 3633285 3633387 3633389 3633390 |
| 3 | 3634816 3634879 3634934 3634936 3634956 3635039 3635041 3635042 |

### 1. Contract — 5.2B semantic, three of three

| | |
|---|---|
| gate | **PASS** |
| per cycle | `PASS`, `PASS`, `PASS` |
| cases | **64/64 per cycle** |
| problems | none |

### 2. Seed diversity — `OBSERVED`

| cycle | candidate seed digest | reference seed digest | `PYTHONHASHSEED` | `hash_randomization` |
|---|---|---|---|---|
| 1 | `d327c5f70cf4c105…` | `93eda1b784141c14…` | unset | 1 |
| 2 | `a529e1a76865da1b…` | `14f58361cfd4d38a…` | unset | 1 |
| 3 | `9bccce43aaec4040…` | `e253f07863f7e86d…` | unset | 1 |

**3 distinct digests across 3 cycles**, with no precondition problems — the seed was
genuinely unset, hash randomisation was genuinely on, and the fixed 11-string probe
was complete in every cycle. The `PASS_WITH_INSUFFICIENT_SEED_DIVERSITY` branch was
**not** taken.

Measured per cycle by hashing a fixed eleven-string tuple with the same binary, the
same `-S`, `PYTHONPATH`, cwd and environment each arm was launched with.

**This is sibling / launch-environment seed diversity.** The interpreter measured is
a sibling launched by the same procedure — **not** the gunicorn master and not any
worker that served a request.

### 3. Order stability — recorded, deliberately outside the verdict

| | |
|---|---|
| cases | 64 |
| comparable (case, arm) pairs across cycles | 94 |
| **varied** | **4** |
| responses with no row structure | 102 (counted, not called stable) |

The four are `C16/candidate`, `C16/reference`, `C16-csv/candidate`,
`C16-csv/reference` — and they are a separate finding, below.

### Traffic, cleanup and host state

- **Requests:** 64 contract + 2 data probe + 1–30 readiness per arm per cycle =
  **67–96 per arm per cycle**, **201–288 per arm** and **402–576 total** across three
  cycles. Ceilings were 288 and 576.
- **Production 8050, 8786, 8787: 0 requests, never connected to.** 8050 was read from
  `/proc` and `ss` only.
- **Clone integrity: nine full verifications, 9/9 MATCH** — three per cycle
  (preflight, before reference, before candidate), 33,565 entries against 33,565
  files, 1,690,025,002 bytes each time.
- **Cleanup: PASS in all three cycles.** Every service stopped, every process in every
  recorded tree exited, 18091/18092/18819 confirmed free each time, production
  unchanged (master 3960, start time 1874, listeners 3960/4334/4366). **Zero blocking
  state files across the whole `run/` tree afterwards.**
- **Per-cycle evidence is isolated and complete:** 10 result artefacts and 4 service
  logs under each of `c2_cycle1`, `c2_cycle2`, `c2_cycle3`; no cycle overwrote
  another.
- **Afterwards:** clone 555/555, manifest digest unchanged, 33,565 files, **0
  writable, 0 new `.pyc`**; production site-packages 33,567 files at mtime
  2026-02-12; `~/python/woa23` at mtime 2026-08-05.

### Finding — C16 and C16-csv: row order varies across cycles, semantics hold

Of the 64 cases, **exactly `C16` and `C16-csv` showed row-order variation across
cycles on both arms.** The other 90 comparable (case, arm) pairs were stable.

**Effect directly observed; source-level mechanism strongly supported.**

What was observed at runtime: the row-order fingerprints of those two cases, and only
those two, differ between cycles on both arms, under an unpinned hash seed, while the
5.2B semantic comparison passed for them in every cycle.

What is strongly supported but **not** established step by step at runtime: that this
arises because `zarr_group_paths` is a `set` of path strings whose iteration order
depends on the process's hash seed, so a query spanning more than one Zarr group
concatenates its groups in a per-process order. The supporting evidence is that C16
and C16-csv are precisely the two cases in the suite that span more than one group,
that the same two cases were the only ones to differ in the 2026-08-08 D2b run when
the arms were given different store strings, and that `bench/repro_c16.py`
demonstrates the mechanism offline on synthetic rows. **No runtime instrumentation
observed the set iteration inside a worker**, and none was authorised; the internal
causal chain is inferred from the shape of the effect and from offline reproduction,
not proven in the running process.

#### Traceability of the two observations

Both are reproducible from the run's own artefacts, under
`~/woa23-s2-c2c/dev2026/results/` on odb24.

**Seed digests** — `c2_summary.json → seed_diversity.per_cycle` reproduces exactly
the `seed_digest` field of each `c2_cycle{1,2,3}_interp_{candidate,reference}.json`;
all six match. Each of those records also carries the probe itself — 11 strings and
11 hashes — so the digests can be recomputed rather than taken on trust. The raw
values differ per cycle, not merely their digests: `hash("1_degree")` was
`-4854500566350811133`, `4447478972511239299`, `-4704986505660421793` on the
candidate and `9026776351318122430`, `3388466108566623068`, `-714907535411415113`
on the reference.

**Order fingerprints** — each `c2_cycle{1,2,3}_contract.json` records, per case and
per arm, `reference_order` and `candidate_order` with `body_sha256`,
`row_order_sha256`, `columns` and `n_rows`. There are **128 distinct (case, arm)
pairs** — 64 cases on two arms — and **each pair was observed three times, once per
cycle**, so the artefacts hold 384 observations in total. Of the 128 pairs, **94
carry a row-order digest** and 34 are responses with no row structure (those 34
account for the 102 orderless responses `c2_summary.json` counts: 34 pairs x 3
cycles). Recomputing the varied set from those artefacts gives exactly the four
`c2_summary.json` reports.

Case by case, the row-order digest per cycle:

| case | arm | cycle 1 | cycle 2 | cycle 3 |
|---|---|---|---|---|
| `C16` | reference | `56ccfd332e27…` | `56ccfd332e27…` | `a35ae14d930a…` |
| `C16` | candidate | `56ccfd332e27…` | `a35ae14d930a…` | `56ccfd332e27…` |
| `C16-csv` | reference | `56ccfd332e27…` | `56ccfd332e27…` | `a35ae14d930a…` |
| `C16-csv` | candidate | `56ccfd332e27…` | `a35ae14d930a…` | `56ccfd332e27…` |
| `C1` (stable, for contrast) | both | `c994a1ff7849…` | `c994a1ff7849…` | `c994a1ff7849…` |

The two cases take exactly **two** distinct orderings and no more, which is what a
two-element set admits. That is consistent with the proposed mechanism and is not on
its own proof of it — the count would look the same for any two-valued cause.

**It is not a defect against this gate.** 5.2B compares the row multiset and the
column set; row order is not part of the criterion, and under an unpinned seed a
per-process ordering is the expected consequence of the arrangement C2 exists to
observe. It matters because **any consumer that depends on row order would see it**,
and because a byte-exact comparison of these two cases across unpinned processes
would fail for a reason that is not a correctness defect. Whether the candidate
should impose a deterministic order is a separate question and a separate decision.

### What this result is

The candidate and the unmodified reference return **semantically equivalent
responses across all 64 contract cases, in each of three independent start/stop
cycles**, when both run on production's interpreter and a read-only copy of
production's package tree, **at production's measured worker count of two**, with no
pinned hash seed — and three independent starts were observed to choose different
hash seeds.

### What this result is **not**

- **Not deployment validated, and not ready to deploy.**
- **No latency, throughput or resource conclusion of any kind.** None was measured;
  the latency gate, the noise pilot and every rung above 21 did not run.
- **`-S` means `site.py` never ran** in any cycle, so no `.pth` in the clone was
  processed. Production's site/`.pth` startup semantics were not exercised.
- **The launcher is not production's PM2 path.**
- **No worker-level Python import provenance exists.** The interpreter probe is a
  sibling process; `/proc/<pid>/maps` can refute isolation but its silence proves
  nothing, because it lists mapped files and not imports.
- **The seed diversity is sibling / launch-environment level, not worker level.** It
  says three starts of that launch procedure chose different seeds. It does not
  measure the seed of any gunicorn master or worker that served a request.
- **D1 is still open.** A missing `WOA23_ZARR_STORE` fails at import and startup; an
  invalid or non-Zarr store still starts cleanly and fails on the first data request.
  No candidate change is proposed or made.
- **Formal deployment validation is still open** — PM2, site/`.pth` semantics,
  readiness, nginx and TLS.
- **Performance validation is still open** in its entirety for S2.

## C2 controlled run, 2026-08-10 (`c2f`) — semantic correctness **after the D1 patch**

**C2 PASS — isolated package-tree semantic correctness after D1 patch at production
worker count, with observed sibling seed diversity.**

**The first complete, valid C2 result on the patched candidate.** The C2 of
2026-08-09 (`c2c`) ran the *unpatched* candidate. Two attempts on the patched one did
not produce a result and are recorded as such, not quietly dropped:

| attempt | outcome | why |
|---|---|---|
| `c2d` | **cleanup FAIL**, cycle 1 | the arms inherited gunicorn's 30 s `graceful_timeout` while `STOP_WAIT_SECS` was 20, and a logging reentrancy race left one worker unreaped. The contract had passed. |
| `c2e` | **INVALID_PRE_START** | `--clone-manifest` was pointed at the clone root's `SHA256SUMS`, which is a digest list, not a manifest. Stopped at clone-integrity preflight; **no service started, no request issued**. |
| `c2f` | **PASS** | below |

`c2d` and `c2e` keep their evidence. Neither was re-run in place, and neither was
backfilled into this result.

### What ran

| | |
|---|---|
| commit | `249aa2749bc14d13bf82b9af9cf5385ac2cb2334`, archive `d77b275f79ae3ce036ab3d3d0a6ea0eac77c7339ff37439426284d4a9835caba`, 87 files |
| candidate `api/` | byte-identical to the C1-tested `919095e8` — `app.py d0d8c781…`, `config.py b8066414…`, `query.py 4e28bdd9…`, `store_paths.py 00cb80c2…` |
| staging / workdir / label | `~/woa23-s2-c2f/`, `~/woa23-s2-c2f-work-cycle{1,2,3}`, `c2f` — all new |
| ports | 18131 / 18132 / 18859 — **first use**, checked against `scripts/ports_used.tsv` before starting, and free on the host |
| clone | the existing read-only clone, `clone.manifest` (four columns, 33,565 entries) |
| seed policy | `both-unpinned` — `PYTHONHASHSEED` unset on **both** arms |
| processes | **8 per cycle**: two arms of arbiter + 2 workers, plus an isolated Dask scheduler and worker |

**Prerequisites were read from the host before any service started**, not taken from
the request: `STOP_WAIT_SECS=20` (source `default`; unset in the environment),
`ARM_GRACEFUL_TIMEOUT=10`, `assert_shutdown_budget` PASS, and production's worker
count **measured as 2** from pid 3960's own argv.

### 1. Contract — 5.2B semantic, three of three

| cycle | gate | cases | verdicts | request order |
|---|---|---|---|---|
| `c2f_cycle1` | **PASS** | 64 | 64 MATCH | RC 32, CR 32 |
| `c2f_cycle2` | **PASS** | 64 | 64 MATCH | RC 32, CR 32 |
| `c2f_cycle3` | **PASS** | 64 | 64 MATCH | RC 32, CR 32 |

### 2. Seed diversity — `OBSERVED`

Six digests from three independent starts, **all distinct**:

| cycle | candidate | reference |
|---|---|---|
| 1 | `b43233d7642d2319` | `dafdbaa61a9bb6da` |
| 2 | `2e04ce4cb4b0348a` | `14f537d43115d1f9` |
| 3 | `e744e91b3df4bc59` | `f346808f0ad1fe09` |

`PYTHONHASHSEED` unset and `hash_randomization=1` are **preconditions, not
evidence** — they say the interpreter was permitted to choose a seed, and are equally
true of three starts that chose the same one. Only the measured digests distinguish
those cases.

### 3. Order stability — recorded, deliberately outside the verdict

| | `c2f` (2026-08-10, patched) | `c2c` (2026-08-09, unpatched) |
|---|---|---|
| comparable (case, arm) pairs | 94 | 94 |
| **varied** | **2** | **4** |
| which | `C16/candidate`, `C16-csv/candidate` | `C16/candidate`, `C16/reference`, `C16-csv/candidate`, `C16-csv/reference` |
| responses with no row structure | 102 | 102 |

**The two runs involve the same two cases; they did not behave identically.** In
`c2c` both arms varied; in `c2f` only the candidate pair did, and the reference
returned the same row order in all three cycles.

**That difference is not itself a finding, and must not be read as one.** With no
pinned seed, three cycles landing on one order is compatible with coincidence — the
reference has three samples, not a demonstrated property. Nothing in this run
attributes the difference to the D1 patch, to the arms' code, or to anything else,
and no mechanism for it was investigated. What is established is what the table
says: which pairs varied, in which run.

Semantics held for these cases in every cycle: they are 5.2B MATCH throughout.

### 4. Shutdown budget — read back from the evidence, not assumed

| cycle | `STOP_WAIT_SECS` | source | arms' `--graceful-timeout`, from each arm's own `/proc/<pid>/cmdline` |
|---|---|---|---|
| 1 | 20 | default | recorded 10 · candidate 10 · reference 10 |
| 2 | 20 | default | recorded 10 · candidate 10 · reference 10 |
| 3 | 20 | default | recorded 10 · candidate 10 · reference 10 |

Status **CONSISTENT**. This is the relationship whose absence failed `c2d`: the
harness's wait must exceed what the arms are entitled to take, and both numbers are
now asserted before a cycle starts and read back afterwards from the processes
themselves.

### Traffic, cleanup and host state

**Requests — actual where recorded, and bounded where not:**

| component | per arm per cycle | recorded? |
|---|---|---|
| contract gate | **64** | yes — `request_order_counts` RC 32 + CR 32 in each `c2f_cycle{1,2,3}_contract.json` |
| store-readiness probe | **2** | yes — two probes per arm, in both orders, no retry path |
| process-readiness probe | 1–30 | **no** — the loop does not count its attempts |

So **396 requests are recorded as actually issued** (66 per arm per cycle × 2 arms ×
3 cycles), and process readiness adds an unrecorded 1–30 per arm per cycle. **Total
actual: between 402 and 576.** The ceiling declared before the run was 576 per run,
96 per arm per cycle. *The unrecorded component is a gap in the harness, not in this
report: `process_ready` should return its attempt count so the actual total is exact.
No change has been made and no rerun is proposed for it.*

- **Production 8050, 8786, 8787: 0 requests, never connected to.** Read from `/proc`
  and `ss` only. Production **unchanged** at every check in all three cycles: master
  3960, start time 1874, listeners 3960/4334/4366, boot id matched.
- **Clone integrity: nine full verifications, 9/9 OK** — three per cycle (preflight,
  before reference, before candidate). Each: 33,565 manifest entries against 33,565
  files on disk, 1,690,025,002 bytes re-hashed in ~7.0 s, **0 missing, 0 extra, 0
  digest mismatch, 0 size mismatch, 0 mtime mismatch, 0 unreadable**; manifest
  `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4`.
- **Clone integrity is detection, not immutability.** `/home/odbadmin` is writable by
  this account — recorded as residual exposure at every check. The window between a
  verification and a worker opening a file is narrowed, not closed.
- **Cleanup: PASS in all three cycles.** Every service stopped, every process in every
  recorded tree exited, 18131/18132/18859 confirmed free each time. **Zero blocking
  state files across the whole `run/` tree afterwards** — no `.pid`, `.starttime`,
  `.tree`, `.diag` or `.uncertain`. Each `run/c2f_cycle{1,2,3}/` holds its four
  service logs, which is what a clean stop leaves behind.
- **The store was not written.** `~/python/woa23/data` mtime remains 2025-04-18
  14:31:47 — *no modification observed*; the basis for the claim is that the runner
  performs no write to it.
- **Earlier evidence untouched:** `c2c`, `c2d` and `c2e` show zero file changes.

### Archive

`~/woa23-s2-archive/2026-08-10-c2f-PASS/` — outside any deploy directory, **50 evidence
files** (37 result artefacts, 12 service logs, the run log) **plus a separate
`SHA256SUMS`**, which is the manifest of those 50 and is not one of them. Each
evidence file was hashed at the source, copied, re-hashed at the destination and
compared; `SHA256SUMS` is
`ffa68f58a6f8831ca4487ee787aef6cb4e8ffd5d8bd79716fe2cb8912e87e101`. Directories 555,
files 444, verified unwritable. **The originals under `~/woa23-s2-c2f/` were copied,
never moved, and are unchanged.**

### What this result is

The candidate — **with the D1 store-startup patch applied** — and the unmodified
reference return **semantically equivalent responses across all 64 contract cases, in
each of three independent start/stop cycles**, when both run on production's
interpreter and a read-only copy of production's package tree, **at production's
measured worker count of two**, with no pinned hash seed. Three independent starts
were observed to choose different hash seeds, and all three cycles stopped cleanly.

### What this result is **not**

- **Not deployment validated, and not ready to deploy.**
- **No latency, throughput or resource conclusion of any kind.** None was measured;
  the latency gate, the noise pilot and every rung above 21 did not run.
- **`-S` means `site.py` never ran** in any cycle, so no `.pth` in the clone was
  processed. Production's site/`.pth` startup semantics were not exercised.
- **The launcher is not production's PM2 path.**
- **No worker-level Python import provenance exists.** The interpreter probe is a
  sibling process; `/proc/<pid>/maps` can refute isolation but its silence proves
  nothing, because it lists mapped files and not imports.
- **The seed diversity is sibling / launch-environment level, not worker level.** It
  says three starts of that launch procedure chose different seeds. It does not
  measure the seed of any gunicorn master or worker that served a request.
- **The candidate's startup anchor validation reading no data or coordinate chunk is
  offline-audited and implementation-supported, not observed on this host.** This run
  installed no audit hook and recorded no file-open events.
- **D1 is only partly closed.** The patch makes an invalid or non-Zarr store fail
  before the service is ready; **real-store depth characterization is still
  CHARACTERIZATION PENDING** and was not run.
- **The row-order contract decision is still open** — no sorting, no pinned seed.
- **Formal deployment validation is still open** — PM2, site/`.pth` semantics,
  readiness, nginx and TLS.
- **Performance validation is still open** in its entirety for S2.

## S2 performance validation, 2026-08-19 (`s2pB`) — rung 21 request-path latency

**S2 rung 21: PASS under the approved 0.05 engineering threshold.** Commit
`c3b9398b`, label `s2pB`, exit status 0. The full execution record — preflight,
digests, provenance, request accounting, cleanup — is in
`specs/002-production-correctness-deploy-hardening.md`; the gate's own account is
`specs/007-s2-performance-validation.md` §12. This entry is the measurement record.

**Scope, and it is the whole of it:** single-worker (`-w 1` per arm, by design, not
production's count), steady-state, warm-cache, **request-path latency**, eight cases
judged individually. **Not** a production SLA; **no** throughput, multi-worker,
startup, deployment or PM2/TLS conclusion follows from it.

| case | candidate | reference | ratio | 95% percentile interval | regression | improvement gate |
|---|---|---|---|---|---|---|
| point_profile | 20.2 ms | 185.6 ms | 0.109 | [0.105, 0.112] | NO_REGRESSION | not required |
| **point_profile_multiparam** | 194.5 ms | 3121.5 ms | 0.062 | [0.061, 0.064] | NO_REGRESSION | **required — IMPROVED** |
| **readme_example** | 180.9 ms | 1401.6 ms | 0.129 | [0.124, 0.136] | NO_REGRESSION | **required — IMPROVED** |
| small_bbox_full_depth | 41.2 ms | 200.5 ms | 0.206 | [0.203, 0.208] | NO_REGRESSION | not required |
| regional_bbox | 65.4 ms | 146.0 ms | 0.448 | [0.433, 0.451] | NO_REGRESSION | not required |
| surface_global | 149.6 ms | 192.8 ms | 0.776 | [0.770, 0.787] | NO_REGRESSION | not required |
| point_profile_025 | 25.6 ms | 222.2 ms | 0.116 | [0.113, 0.117] | NO_REGRESSION | not required |
| regional_bbox_025 | 145.1 ms | 289.9 ms | 0.500 | [0.487, 0.510] | NO_REGRESSION | not required |

**All eight `NO_REGRESSION`.** The `IMPROVED` **gate** covers only the two
improvement-required cases (`bench/paired_bench.py`: `IMPROVEMENT_REQUIRED`), and both
passed. The other six have intervals below 1.0 — an **observation about their measured
ratios, not an improvement gate they passed**, and it may not be quoted as one.

**Protocol.** Rung 21: 21 warm samples per arm per case (+1 discarded), interleaved,
counterbalanced AB/BA per iteration, bootstrap 5,000 rounds, seed 20260805, **per-case
ratios, never pooled, no total ratio**. The contract gate passed **64/64 byte-exact**
(variant 5.2A, both seeds pinned) in the same execution, so these are timings for arms
both shown to answer correctly. Both arms ran the same read-only package clone
(`f3b66c49…`, 33,565 entries, MATCH) under `-S`, `PYTHONHASHSEED=0` read from each
master's `/proc/<pid>/environ`.

**Requests: 467 per arm, 934 total**, under the authorised **496 / 992** ceiling and
**exact** — `counts_exact`, `host_attempt_count_exact` and `measurement_complete` all
true, no failed stages, every stage's evidence a stage artefact, and the pre-request
journal agreeing with the artefacts record-for-record (0 truncated, 0 unattributed).
**Production 8050 received 0 requests**; 8786 and 8787 were never contacted. Cleanup
was clean: all four services stopped, every tracked process confirmed exited, all three
ports free, no retained state, no `SIGKILL`, production verified the same process it was.

**What it does not establish.** `0.05` is an **approved engineering decision threshold**
(PI, 2026-08-19) — not a statistical property of the data, not a confidence guarantee,
not a production SLA. The interval is the 2.5th–97.5th percentile range of 5,000
bootstrap ratios, **not a proven 95% coverage interval**: the samples may be
autocorrelated and no independence argument has been made. **No startup measurement**
exists. **Rung 60 is not scheduled** and is not entered because the pilot suggests it;
it requires its own authorisation.

## D1 characterization run, 2026-08-11 (`d1b`) — real-store nitrate depth behaviour

> **Evidence availability, recorded 2026-08-13.** The observations below were
> recorded when the run happened. **The primary artefacts — `d1b_d1.json`,
> `d1b_requests.json`, `d1b_workers.json`, the per-arm provenance records and the
> store survey, under `~/woa23-d1b/results/` on VM24 — are no longer obtainable**,
> the staging and workdir having gone with the 2026-08-11 snapshot rollback. What
> survives is **secondary**: this document and its counterparts in `ROADMAP.md` and
> `specs/docs/BASELINE.md`, and the session console transcript
> (`c5f5a13a-b21d-4abd-bd3c-71c34e797b02.jsonl`, SHA-256
> `e22c9dc0ed5b4f41b8967cd8cd66173df9df67d960ca8a609009da27f5424bfc`), which holds
> the run's output but no artefact bodies.
>
> **No artefact-shaped evidence has been or may be reconstructed from these
> records, and no result here has been back-filled.** A file rebuilt from a
> transcription would be indistinguishable in shape from one a run wrote, and that
> is precisely the distinction this note exists to keep. The figures below stand as
> what was recorded on the day, at secondary-record strength, and are not
> re-derivable.
>
> This does not block S2: no S2 gate depends on `d1b`.

**D1 CHARACTERIZATION RECORDED — real-store nitrate depth behavior under the
declared one-worker scope.**

**Not a D1 PASS, and D1 is not complete.** This records what the real store returns
for two nitrate depth queries. It closes neither D1 as a whole nor any of the parts
listed under *What is still open* below.

Commit `0780566e55da3ff7ba2ec874cd1996a492ed2794`, archive
`3089d8d91f9462bf5577b241fe1034670b8cc915f755ac796b2b55e2f3cae142`, 103 files,
file-list `7e377e5e65d314928291b833c540a79641e6269537a1594171b790062fe93736` —
all three verified on the host before anything started. Staging `~/woa23-d1b/`,
workdir `~/woa23-d1b-work`, label `d1b`, first-use ports 18151/18152/18879.
`api/` byte-identical to the C1-tested `919095e8`.

### The observations

**Recorded, not judged.** No status or body was compared with an expectation; what
follows is what the store and the API returned.

| case | JSON | CSV |
|---|---|---|
| **annual nitrate, 0–800 m** (`D1-DEPTH-SUP`) | **200**, 3075 B, 43 rows | **200**, 872 B, `text/csv; charset=utf-8` |
| **winter nitrate** (`time_period=13`), **3000–4000 m** (`D1-DEPTH-OOR-tp13`) | **200** with **`[]`**, 2 B | **400** with **`No data available for the given parameters.`**, 56 B |

- **Both arms returned identical bytes** for all four cases — the candidate and the
  unmodified reference agree, so none of this is a candidate defect.
- **8 of 8 case observations are byte-identical to the `d1a` run** of the same day
  (status, `Content-Type` and body digest), and the anchor probe's body matched too.
  The behaviour reproduced across two independent runs.
- **An anchor recovery probe followed each case and returned 200** in every instance,
  with every process in each arm's tree still alive. The failures observed are
  confined to their own request.

### Preconditions — P1–P6, before any service or HTTP

`PRECONDITIONS_MET`. The survey runs after the store symlink and before either arm
starts, so it precedes every HTTP request rather than only the data-path ones.

> **P4 Table 4 count/extent compatibility verified; full Table 3 level-list equality
> not verified.**

| group | levels | extent | dtype | units | `levels_sha256` | Table 4 row |
|---|---|---|---|---|---|---|
| `1_degree/seasonal/Nutrients` | **43** | 0.0–800.0 | `float32` | **none declared** | `fd44cd93df8b…` | Seasonal / Nitrate |
| `1_degree/annual/Nutrients` | **102** | 0.0–5500.0 | `float32` | **none declared** | `fc5db2e1330e…` | Annual / Nitrate |

**No depth level is selectable in 3000–4000 m** on the seasonal group — asked with
the API's own selection semantics, not by comparing maxima. The annual group has
**43 levels selectable in 0–800 m**. Both digests are identical to `d1a`'s, so the
store's axes did not move between the runs.

Each group was judged against **its own** Table 4 row. Seasonal nitrate's 43 levels
over 0–800 m is not applied to the annual group, to the other variables or to the
store.

### What was observed about the store but NOT characterized

- **Missing-group behaviour on the real store is not characterized.** All **12 of 12**
  query-reachable groups exist and open, so no real missing group was available to
  request. The synthetic D1-D3 evidence remains the only evidence for that path.
  `query_reachable` means *query-path reachable by API logic; no HTTP request was
  sent* — the survey runs before any service exists.
- **`mn` and the three nutrient variables are a schema observation only.** `mn` — the
  statistical-mean data field of WOA23 Table 2, not a coordinate and not an
  oceanographic variable — is present in both groups, and `parameters` holds
  `nitrate`, `phosphate` and `silicate`. **This run characterizes nitrate.** No
  request was issued for phosphate or silicate, so nothing is known about what the
  API returns for them.

### Traffic, isolation and cleanup

- **Requests: measured, not bounded.** 11 per arm — 1 readiness, 2 store probe, 4
  characterization, 4 anchor recovery — **22 in total**, counted as *attempts* and
  not successes. Recorded in `d1b_requests.json`; the 22–80 range is the ceiling and
  this is the number.
- **One worker per arm**, `-w 1`, asserted against each arm's own
  `/proc/<pid>/cmdline` and recorded with both full launch argv.
  **D1 uses one worker per arm by design. This is not a production-worker-count
  validation and makes no claim about multi-worker D1 behavior.**
- **Clone integrity 3/3**: 33,565 manifest entries, 0 mismatched, 0 missing, 0 extra.
  Detection with a bounded window, not immutability — `/home/odbadmin` is writable by
  the account and that is recorded at every check.
- **Production 8050 / 8786 / 8787: 0 requests.** Master 3960, listeners
  3960/4334/4366, boot id unchanged throughout.
- **Cleanup PASS.** Every service stopped, every process in every recorded tree
  exited, all three ports confirmed free, **zero blocking state left**.
- **All post-run artefacts finalised** — the step that failed in `d1a` and left it
  classified `INVALID_POST_MEASUREMENT_HARNESS`.

### What this result is

For the two queries it issued, against the real store: the API's current behaviour,
measured, with the candidate and the reference in agreement and the observations
reproduced across two runs.

### What this result is **not**

- **Not a D1 PASS and not D1 complete.**
- **Not a validation of the JSON/CSV divergence it observed.** That the CSV path
  answers 400 where JSON answers `200 []` is recorded as **current behaviour**. The
  adopted policy (spec 006) is that both should be 200 — **not implemented, not
  tested, and not what this run measured**. The recorded 400 stands as measured and
  is not to be rewritten.
- **Not a characterization of anything but winter nitrate and annual nitrate.** Not
  `time_period` 14, 15 or 16; not monthly nitrate; not phosphate or silicate; not TS
  or oxygen. Each has its own Table 4 row and would need its own case and evidence.
- **Not a real-store missing-group characterization** — no group was missing.
- **Not a production-worker-count validation** — one worker per arm is fixed by the
  mode.
- **No latency, throughput or resource conclusion.** None was measured.
- **`-S` means `site.py` never ran**, so no `.pth` was processed, and the launcher is
  not production's PM2 path.
- **No worker-level import provenance.** The interpreter evidence is a sibling
  process and `/proc/<pid>/maps`.
- **The zero-chunk property of the startup anchor validation is unchanged**:
  offline-audited and implementation-supported, **not observed on this host**. The
  P1–P6 survey **does** read coordinate chunks, which is a separate operation.

### What is still open in D1

Real-store missing-group behaviour (unavailable while every group exists), every
variable and climatology outside the two characterized, and deployment validation
under PM2 with `site.py` enabled.

### Follow-up noted, not a D1 item

The JSON/CSV divergence this run observed prompted a small API contract decision,
recorded in **`specs/006-json-csv-empty-result-consistency.md`**: for a valid query
with no matching data, both formats should return **200**, with CSV carrying the
same query's **header-only** schema (option B, adopted). **Not implemented.**

**It is not a D1 blocker and not a track of its own.** D1 measured behaviour; the
memo decides what the behaviour should become, and nothing in D1 waits on it.

---

## PM2 alternate-port staging, 2026-08-20 (`pm2B`) — deployment machinery, synthetic store

**PASS, bounded.** Subject `716fcc6cb18eb4fdb6f4bd732aba8f455b5b4975`. Full record in
`specs/PM2-staging-result-pm2B.md`.

### What was established

| | |
|---|---|
| PM2 under an isolated `PM2_HOME` | `~/woa23-pm2b-pm2`, God Daemon 1248938, one named app, `online`, 0 restarts |
| environment **in the process** | read from `/proc/1248949/environ` — all four `WOA23_*` values correct |
| Python runtime | **3.11.4**, `/proc/<pid>/exe` identical to production's own PID 4296 |
| store | **72 files, 25,191 bytes**, file-list `8fb70f2c64d7a3ee3d7fa451de08218c7a1740b19451cf015dd83394fd328624` |
| API | OpenAPI **1.1.0**; JSON and CSV each **144 rows**, both strictly ascending by numeric `(time_period, depth, lat, lon)`, same order, three period groups (0 / 1,2 / 13) |
| restart | master 1248949 → 1249486, response **byte-identical** |
| cleanup | stop + delete released every process and port 18241; no SIGKILL |
| production | boot id, daemon 3459, PIDs 4296/5040/5041/4357/4358 and their starttimes, and all listeners **identical before and after**; **0 requests** |

### What it does NOT establish — stated because the store makes it easy to overclaim

**The store was 72 synthetic files built by `deploy/make_staging_store.py`. It is not WOA23
data.** Therefore this run is:

- **not real-store correctness** — that is `c1f` and `c2g`, against the real store;
- **not a performance result** — that is `s2pB`, and only at rung 21;
- **not a production deployment PASS** — different launcher, different port, no TLS, no
  reverse proxy, and production's own launcher untouched;
- **not PM2 validation of production's config** — it validated
  `dev2026/deploy/ecosystem.staging.config.js`, a file production does not use.

### The failure that preceded it

`pm2A` (2026-08-20, same day) **FAILED AT START** and is retained, not cleaned. PM2 merges
a config's `env:` block over the environment `pm2 start` is given and **the config wins**,
so placeholder values replaced the supplied ones: the launcher received an empty store and
the spent port 18221 rather than 18231. The store guard refused the empty store, which is
the only reason a spent port was never bound. **A staging-configuration failure, not a
candidate failure, and not a staging PASS.** Its tree, store, PM2 home, logs and daemon
(1242814) are kept as evidence.

## Bash 5.x read-only verification, 2026-08-20 (`bash5A`) — compatibility only

**PASS, bounded.** Subject `343309b70dec3b37378bb824bd4b283bd3d5ed86`, verified on the host
by file-list digest so all **143** files are byte-identical. Full record in
`specs/Bash5-verification-result-bash5A.md`.

### What was established

`dev2026/deploy/production_app.sh` (SHA-256
`3c6737a817cc12b7a37a2e9954698d0ce285e98c64e6b7a0b5a6e89a64d38ef3`) under
**GNU bash 5.2.21(1)-release**:

- `bash -n` — no syntax error;
- **twelve refusal cases**, each `exit 2`, the stub interpreter **never invoked**, the probe
  port never a listener, host process count identical before and after (482 → 482);
- refusals occur **in order** — `port-not-a-number` never reaches the store, TLS or
  interpreter checks;
- **two argv cases** via a stub interpreter and a synthetic store: argv names
  `python -m gunicorn api.app:app`, carries **no `--reload`** and **no `woa23_app`**, and
  `--keyfile` is present with TLS on and **absent with TLS off** — the guarded empty-array
  expansion, which is unreachable from any refusal;
- **exactly 2 stub invocations recorded**, which is what proves the twelve refusals ran
  nothing;
- production identical before and after; **0 requests**; no process leak.

### What it does NOT establish

- **not a successful start of the production launcher.** No gunicorn was started, no port
  bound, no real store opened. The stub interpreter cannot bind, serve or fork;
- **not PM2 validation** — PM2 was not involved at all;
- **not a production deployment PASS**;
- it says nothing about whether the launcher *works* in production, only that it *parses
  and refuses correctly* on the bash VM24 runs.

Its runner, `scripts/verify_bash5_refusals.sh`, was written **after** the subject and is
**not part of it**; it ships alongside with its own digest
(`02e97553f6a26a34c2598cc2b5ba15b1437b945631dae4c66c1e714c57e7f341`).

## Standing limitations after `pm2B` and `bash5A`

Recorded together because two PASSes in one day is exactly when a reader starts rounding up.

| item | status |
|---|---|
| `pm2A` | **configuration failure, retained as evidence**, never repeated or cleaned |
| **B1–B5** | **all five still open.** Production still runs `conf/start_app.sh` — old app, hard-coded 8050, no store, `--reload`, behind a `grep`-based `kill -9` `pre_stop` |
| **B6 — the deployment's dependencies** | **open and unverified.** Nothing has shown production's pyenv interpreter carries `polars`, `orjson` or `fastapi`; every validated run used a `uv` venv |
| **B6 — AVX2 masking** | **DECIDED 2026-08-20: mainline polars 1.27.1 retained**, `polars-lts-cpu` ruled out of this campaign (spec 012 rev 3). Nothing changed, so **no C1/C2 re-run follows** and `c1f`/`c2g`/`s2pB`/`pm2B` all stand. The masking is carried as **accepted residual risk**: the warning is not silenced, the SIGILL risk is unquantified, it applies to production's `py311` too, and **no absolute performance figure may be presented as production-representative**. Reopens on a hardware change, a polars upgrade, a SIGILL, an incorrect result, or a significant performance regression |
| **B7 — deployment dependencies** | **factual half answered 2026-08-20** (`B7-dependency-check-result.md`): all eight modules present in `py311` at versions identical to the validated venv. **Still OPEN** — eight matching packages are not runtime equivalence, `py311` is shared with three other projects, and the candidate has never been run from it. The deployment-runtime decision is outstanding |
| 16 local `arm.py` strays | **on the development machine only, not VM24 evidence, and deliberately not cleared** |
