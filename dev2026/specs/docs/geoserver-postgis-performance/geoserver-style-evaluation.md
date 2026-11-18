# GeoServer and style evaluation

## 1. Preserve a faithful isolated target

Inventory GeoServer/runtime versions, extensions, data directory, workspace/catalog, layer and feature type,
datastore, styles, cache settings, process identity, and relevant file digests. Use a separate data-directory
copy and instance; do not edit production files to create a lab. State every difference from the target.

An isolated GeoServer can still query a shared database. That isolates catalog/process changes, not database
load. Use bounded serial requests and stop if work remains active or its outcome is unknown.

## 2. Change one style

Keep the original as A and give the candidate a separate name. For a mechanical transformation, pin source and
output bytes, parse only known structures, refuse unexpected formats, preserve colors and order, and assert
that filters, null/outlier rules, titles, line endings, and all non-target bytes remain unchanged. Test
deterministic generation and reverse-order properties where applicable.

Do not reuse a parser or transformation assumption for a different style language without fixtures for that
project's real files.

## 3. Verify rendering before performance

Fix the layer, parameter/depth/time, bbox, dimensions, CRS, format, and style. Compare output bytes or a declared
pixel-level comparator. Cover normal values, outliers, equal min/max, mask/null, and boundaries when the data
supports them. HTTP 200 proves a response exists, not that its pixels are correct.

For representative samples, list the tested families/layers/cases and the combinations not tested. Do not
generalize a sample pass to every style and layer.

## 4. Deploy additively and separate adoption

Where approved, add a new named style without changing defaults or the original style. Verify stored bytes,
descriptor, workspace, allowed catalog changes, and process identity. Do not assume a running GeoServer watches
data-directory files; determine reload behavior before choosing a method. Do not reload/restart production
without separate approval.

A public WMS smoke test can show that a name returns an image; it is not an A/B correctness or performance
result. Keep cache headers precise: a proxy-reported `BYPASS` is not proof of GeoServer execution.

Availability is not adoption. A default change, advertised alternate-style change, or client code change is a
separate decision by its owner. Preserve the original style for fallback.
