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
