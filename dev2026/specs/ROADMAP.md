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

**Spec:** not yet written · **Status:** blocked on S1

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
