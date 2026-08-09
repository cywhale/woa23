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
