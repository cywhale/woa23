# Measurement and evidence

## Keep the clocks separate

| Measurement | Includes | Does not establish by itself |
|---|---|---|
| Client `time_total` | Observed request path, network, proxy/service, and body transfer | GeoServer-only or database-only runtime |
| Time to first byte after setup | Network and proxy/service work up to response start | Pure GeoServer rendering or origin execution |
| GeoServer loopback | GeoServer, datastore/database, image generation, local HTTP | Public proxy/client behavior |
| PostgreSQL plan/execution | The SQL and work visible to the chosen DB instrument | Full WMS latency or user experience |

A sampled database activity fingerprint is not a request ID or continuous trace. Poll gaps can miss short work;
absence from a sample is not proof that no query ran.

## Paired measurements and drift

- Predeclare warm-ups and correctness requests; keep them out of timed summaries if that is the design.
- Use balanced serial order such as A/B, B/A, B/A, A/B. Do not retry or resend unless explicitly designed.
- Report pair differences, median, range, sample count, and the A-only drift reference.
- Keep different layers, parameter slices, palettes, and paths separate. Small samples support directional
  observations, not statistical significance.

## Cache is part of the experiment

Record response cache headers, DNS/connection/redirect facts, and the request headers used. `Pragma: no-cache`
or a proxy-reported `BYPASS` does not guarantee every cache was bypassed or that the application rendered the
response. Avoid cache-busting parameters unless specifically authorized and validated; they can alter behavior
and create cache entries.

## Distinct acceptance gates

1. **Function:** a valid HTTP/image response.
2. **Correctness:** expected data and pixels for the request.
3. **Performance:** paired observations in the stated environment.
4. **Acceptance:** predeclared threshold plus correctness and regression checks.
5. **Adoption:** a default, catalog, or client begins using the candidate.

Passing one gate does not imply the next. Record evidence paths, digests, unknowns, deviations, and cleanup
alongside the result. Do not claim state is unchanged beyond the checkpoints actually observed.
