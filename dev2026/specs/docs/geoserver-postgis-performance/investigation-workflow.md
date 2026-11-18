# End-to-end investigation workflow

## 1. Define one falsifiable question

Examples: “Does this index reduce execution work for this exact SQL and parameter slice?” or “Does this style
change preserve rendered pixels while reducing GeoServer rendering time?” Do not combine index, SQL, style,
GeoServer configuration, and cache changes in one comparison.

Record the system owner, target environment, layer/relation, request slice, allowed contact, request/query caps,
stop conditions, evidence location, and explicit exclusions. A separate GeoServer process is not a database-
isolated lab if it still queries a shared database.

## 2. Map the execution path

Identify the client request, proxy/cache layers, GeoServer version and catalog, datastore and SQL view, database
relation, and image encoding. Pin versions or digests for inputs that can change. Keep credentials private.

Treat these as separate observation points:

- **Public endpoint:** client-to-proxy-to-service latency, including network and cache.
- **GeoServer loopback:** GeoServer, datastore/database, image generation, and local HTTP.
- **PostgreSQL:** plan, execution, buffers/I/O, waits, and backend activity, using a separately reviewed method.

No single timer or sampled query establishes another component's runtime or cause.

## 3. Freeze a comparable baseline

Hold constant the layer, style, parameters, time/depth, bbox, dimensions, CRS, format, headers, data slice, and
software. Save the exact request, response status, response bytes/digest, and timing components. If a lab copy
shares production data, state that database load is still shared.

Use A-only observations to learn warm-up and within-run drift. For A/B, use balanced or randomized paired order,
serial requests, and no unplanned retry. Do not compare a cold request in one arm with a warm cached request in
the other.

## 4. Apply one bounded change

Prefer an isolated copy or a separately approved reversible deployment. Pin the source, generated artifact,
tool, and input digests. Define exactly which files/settings may change and which must remain unchanged. Set
per-step timeouts, request caps, an overall time window, and fail-closed stop rules. A local timeout does not
prove remote work was cancelled.

## 5. Correctness before timing

Check the semantic contract first. For SQL, compare the relevant rows/values and expected predicates. For map
images, compare full bytes or a declared pixel comparator under identical requests. Include outliers, null/mask,
equal-min/max, and data-supported color boundaries when relevant. If a boundary is absent from the data, report
it as not covered.

If correctness fails or is unknown, do not interpret candidate timing as a successful result.

## 6. Measure and report limits

Report every pair, order, sample count, median, range, baseline drift, and timer definition. Keep each layer,
parameter slice, and request path separate. Use a predeclared acceptance rule; a small sample is directional,
not statistical significance.

Record cache headers per response. A proxy `BYPASS`/`MISS` label describes what that proxy reported; it does not
prove the application rendered the response or exclude an outer cache.

## 7. Close out accurately

Record contact, requests/statements sent, changed state, cleanup, evidence and digests, unknowns, and deviations.
Do not claim production was unchanged beyond the actual checks. A server accepting a style name does not prove
a client uses it. Additional layers, defaults, cache purges, DDL, or client changes require separate scope and
owner decisions.
