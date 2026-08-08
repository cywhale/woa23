# WOA23 performance re-evaluation (2026)

Rebuild of the performance story for the two consumers of the WOA23 dataset:

1. **Web API** — FastAPI reading a ~33 GB Zarr store (VM24, `192.168.2.24`)
2. **PostGIS / GeoServer** — the same data flattened into Postgres for WMS/WMTS
   SQL views (VM34, `192.168.2.34`)

Nothing in here modifies `dev/`, `src/`, `woa23_app.py`, or any production path.
New code lives here; the old notebooks stay as the record of how the data was built.

## Where things are

| | |
|---|---|
| [`specs/ROADMAP.md`](specs/ROADMAP.md) | the two phases and phase 1's steps — **start here** |
| `specs/NNN-*.md` | one spec per step |
| [`specs/docs/BASELINE.md`](specs/docs/BASELINE.md) | the measurement record every spec cites |
| [`CODEX_REVIEWER.md`](CODEX_REVIEWER.md) | **canonical** reviewer briefing — stays at this path; `BASELINE.md` supplements its measurements but never overrides its rules |
| `bench/` | the benchmark harness (documented below) |
| `results/` | raw measurement JSON, kept in git as the record |

## Environment

Managed with `uv`, pinned to the same versions production runs (`../requirements.txt`)
so measurements are representative.

```bash
cd dev2026 && uv sync
```

## Benchmarks

### Black-box — `bench/http_bench.py`

End-to-end over HTTP; needs no server access.

```bash
uv run python -m bench.http_bench --repeat 5 --out results/http_prod_baseline.json
```

> **Cache busting is on by default and must stay on.** There is an nginx proxy
> cache in front of production that reports `x-api-cache: HIT|MISS`. Repeating an
> identical URL measures nginx (~3 ms), not the application. The harness appends
> an ignored `_cb=<uuid>` parameter so every request reaches the app. Use
> `--no-cache-bust` only when you deliberately want to measure the cache.

### Noise floor — `bench/noise_pilot.py`

Samples one backend repeatedly, then bootstraps the ratio of medians between two
arms of *k* draws taken from the same pool. Both arms are the same backend, so the
true ratio is 1.0 and every deviation is noise. Run this **before** trusting any
regression threshold.

```bash
uv run python -m bench.noise_pilot --n 25 --out results/noise_pilot_prod.json
```

At `--repeat 3` the public path's noise floor is ±84% for `point_profile`. Any
pass/fail rule tighter than the measured floor is theatre.

### Structural survey — `bench/zarr_survey.py`

Read-only inventory of the Zarr store: consolidation status, chunk shapes,
compressors, disk footprint. Run on VM24.

```bash
uv run python -m bench.zarr_survey --store ~/python/woa23/data --out results/survey_vm24.json
```

### White-box — `bench/zarr_bench.py`

Reproduces `process_woa23_data` stage by stage (open → sel → to_dataframe →
polars → concat → pivot), reports **read amplification**, and A/B tests the
execution mode. Run on VM24.

```bash
uv run python -m bench.zarr_bench --store ~/python/woa23/data \
    --mode numpy --mode threads --mode distributed \
    --repeat 3 --out results/zarr_vm24.json
```

`--cache-stores` opens each Zarr group once and reuses it, i.e. simulates
hoisting `open_zarr` out of the request path.

Query cases are shared between the two harnesses (`bench/queries.py`), so an
HTTP result and a Zarr result for the same `id` are directly comparable — the
difference is framework and serialisation overhead.
