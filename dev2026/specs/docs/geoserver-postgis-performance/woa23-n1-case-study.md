# WOA23 GeoServer N1 style case study

This case study records the evidence and reusable lessons from the WOA23 style-performance work. It is a
curated summary, not an operational runbook: historical runners were tied to particular hosts, inputs,
authorizations and time windows, so they must not be rerun as-is.

## Goal and mechanism

The experiment tested whether rewriting the repeated categorized thresholds in WOA23 YSLD styles could reduce
GeoServer rendering time while preserving the rendered image. N1 changes the style expression; it does not
change PostgreSQL, the SQL view, the datastore, the layer definition or the layer's default style. It expresses
the data value in a normalized input range and compares it with fixed palette thresholds, rather than expressing
each threshold as arithmetic over `inputmin` and `inputmax`.

The intended adoption model is opt-in: a client can request a named N1 style. Installing a style does not switch
any layer default or change client code. Whether a client adopts an N1 name, or an owner later changes defaults,
is a separate decision.

## Evidence summary

### VM34 isolated GeoServer: 12 layer results

The table reports each layer independently. Times are curl `time_total` seconds; “N1 reduction” is the reduction
in median time from A to N1 within the N1-after paired block. “A drift” compares A after N1 with the pre-N1 A
reference and is a run-stability check, not an effect attributed to N1. The project interpretation rule also
required identical images, A drift within 10%, and no errors. These are small, directional observations from an
isolated GeoServer instance on VM34. The GeoServer instance was isolated, but the database was not a separate
isolated copy; requests used the WOA23 datastore/database available to the lab at the time. These are not
statistical-significance claims or production guarantees.

| Layer | A family | A after N1 / N1 median (s) | N1 time reduction | A drift | Project reading |
|---|---|---:|---:|---:|---|
| TS_monthly_grid025 | rainbow | 6.145 / 1.248 | 79.7% | +0.2% | Worth proposing |
| TS_monthly_grid1 | rainbow | 2.592 / 0.182 | 93.0% | −2.4% | Worth proposing |
| TS_seasonal_grid025 | rainbow | 7.517 / 2.126 | 71.7% | +0.1% | Worth proposing |
| TS_seasonal_grid1 | rainbow | 2.709 / 0.244 | 91.0% | −4.2% | Worth proposing |
| TS_annual_grid1 | rainbow | 3.027 / 0.206 | 93.2% | −3.3% | Worth proposing |
| TS_annual_grid025 | viridis | 7.743 / 1.990 | 74.3% | +1.9% | Worth proposing |
| Nutrients_annual_grid1 | speed | 0.396 / 0.090 | 77.4% | −2.2% | Worth proposing |
| Nutrients_monthly_grid1 | speed | 0.076 / 0.051 | 33.6% | +0.1% | Worth proposing; ~25 ms saved |
| Nutrients_seasonal_grid1 | speed | 0.138 / 0.075 | 45.3% | +23.9% | Did not meet rule: A drift exceeded 10% |
| Oxy_annual_grid1 | ice | 1.511 / 0.156 | 89.7% | −2.4% | Worth proposing |
| Oxy_monthly_grid1 | ice | 0.613 / 0.091 | 85.1% | +1.0% | Worth proposing |
| Oxy_seasonal_grid1 | ice | 0.915 / 0.130 | 85.8% | +1.2% | Worth proposing |

The Nutrients seasonal result is not evidence that N1 failed to speed that layer: N1's paired median was lower,
but the A reference drifted by 23.9%, so the predeclared acceptance band was not met. The owner chose not to
repeat the measurement absent a practical issue. Two other historical events remain unresolved: one sampler
observation was missing for a Nutrients annual request, and one annual request had an extra `select now()` in
the sampler. A later isolated retest of Nutrients annual passed its correctness gates; neither event was
retroactively explained.

### Representative palette/render checks

One fixed VM34 layer, `TS_monthly_grid025`, was used to check three A/N1 pairs across normal-range, outlier,
`min=max`, and `mask=true` cases. All required PNG comparisons were byte-identical. This checked a representative
base palette, a different base palette, and a reversed palette; it was not a render sweep of every candidate.

| A / N1 pair | A / N1 median (s) | N1 time reduction | A drift |
|---|---:|---:|---:|
| rainbow_256 / rainbow_256_n1 | 6.035 / 1.250 | 79.3% | +0.2% |
| ocean_256 / ocean_256_n1 | 6.116 / 1.260 | 79.4% | +2.4% |
| ice_256_i / ice_256_n1_i | 6.043 / 1.233 | 79.6% | +1.9% |

The 44 other generated candidates received static checks, not GeoServer rendering or timing. Static checks
verified source color order, unchanged bytes outside the targeted expressions, reverse-palette relationships,
and deterministic generation. Static equivalence is not a substitute for rendering tests on actual data.

### Seaimage: loopback and public path

For `TS_monthly_grid025` with rainbow, the loopback comparison observed A 6.482 s and N1 1.366 s medians.
The public-path comparison separately observed four paired requests: end-to-end `time_total` medians were
6.499 s for A and 1.427 s for N1. Under the predeclared response-header conditions, the server-wait medians
were 6.490 s and 1.417 s (78.2% lower for N1 in that sample).

The server-wait value includes network round trip, nginx, and possible upstream work; it is not pure GeoServer
render time. `X-ocpu-cache: BYPASS` / upstream-fetch classifications do not prove that GeoServer rendered the
response or rule out an outer cache. This was a four-pair directional comparison for one layer/style, not a
general public-user latency guarantee.

### Installation and public smoke checks

The first N1 style was added to seaimage's `woa23` workspace, then 49 more were added. Thus 50 N1 styles are
available there in total. The additive deployment did not alter layer defaults or client code. A later public
smoke check requested three representative new style names once each and received valid PNGs. It did not compare
those images with their original-style counterparts and did not measure performance.

## Reusable method and pitfalls

1. **Keep correctness separate from timing.** Compare A/N1 images byte-for-byte for the same layer, request
   parameters, bounds, dimensions, CRS and format. Include normal, outlier and degenerate (`min=max`) cases,
   plus mask behavior when relevant. Record boundary coverage as unknown unless real data actually exercises
   the thresholds.
2. **Use paired, interleaved requests.** Alternate A and N1 order across pairs, keep requests serial, separate
   warm-ups from timed samples, and report medians, ranges and per-pair differences. Do not pool different
   layers, palettes, environments, or public and loopback paths.
3. **Measure the layer that answers the question.** Database executor time, GeoServer loopback time, and
   public-path time include different components. A faster HTTP response alone does not locate the cause.
4. **Treat cache headers as evidence with limits.** A bypass request header is only a request; classify each
   response from observed headers. An upstream-fetch header is not proof of GeoServer execution when proxies
   or outer caches are not observed.
5. **Make failures and unknowns explicit.** A missing sampler observation is not proof that no query ran.
   Unexpected extra SQL text should fail a strict correctness gate unless the measurement design is deliberately
   revised and reauthorized. Do not infer its source without evidence.
6. **Separate deployment from adoption.** Adding styles, changing a layer default, and changing a client are
   distinct changes and should have separate owners, evidence and authorization.
7. **Treat old operational code as historical.** The original scripts are pinned to specific hosts, paths,
   hashes and authorizations. Preserve or inspect them as evidence; extract a fresh, host-independent utility
   before reusing transformation logic in another project. The constrained local implementation and its limits
   are documented in the [N1 static transformer guide](n1-static-transformer.md).

## Provenance and limits

The observations above are summarized from the WOA23 Round 195 VM34 layer runs, Round 196 seaimage Stage 1,
loopback and public-path measurements, Round 199 static candidate generation, Round 200 representative palette
checks, and Round 201 additive deployment and public smoke requests. Those round directories are historical
working evidence and are not included in this reusable guide. Exact request manifests, digests, raw logs and
host-bound runners remain in the original evidence repository; do not treat this summary as authorization or as
a command to operate any host.

No production PostgreSQL index was changed or evaluated as part of the N1 style series. PostgreSQL index
experiments are a separate track; see the [VM4_248 lab case study](vm4_248-lab.md) and the PostgreSQL evaluation
guide.
