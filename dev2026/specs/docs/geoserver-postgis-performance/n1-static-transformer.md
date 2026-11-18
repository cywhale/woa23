# N1 static transformer for the observed WOA23 YSLD pattern

The adjacent [`tools/n1_ysld.py`](tools/n1_ysld.py) is a small, dependency-free local utility extracted from the
style transformation used in the WOA23 N1 work. It can generate a candidate and independently check a source /
candidate pair without connecting to GeoServer, PostgreSQL, or any host.

It is intentionally constrained, not a general YSLD parser. It accepts only the observed WOA23 pattern:

- ASCII text with consistent LF or CRLF line endings;
- exactly four identical `categorize(value, ...)` expressions;
- 256 quoted `#RRGGBB` colors in each expression (repeated colors are allowed);
- 255 thresholds in the form `k*(inputmax-inputmin)+inputmin`, where `k` is exactly `n/256` for `n=1..255`.

Anything outside that contract should be reviewed and handled as a new format, not forced through this tool.

## Use

Create a new output directory with owner-only access, then run:

```sh
mkdir -m 700 n1-output
python3 dev2026/specs/docs/geoserver-postgis-performance/tools/n1_ysld.py make source-style.yaml n1-output
python3 dev2026/specs/docs/geoserver-postgis-performance/tools/n1_ysld.py check source-style.yaml n1-output/source-style_n1.yaml
```

`make` writes a new file only; it refuses to overwrite an existing path. It preserves the source extension and
bytes outside the four categorized expressions, writes with mode 0600, completes partial writes, and reads the
file back to verify the bytes. Names follow the WOA23 convention: `palette_256` becomes `palette_256_n1`, and
`palette_256_i` becomes `palette_256_n1_i`. `check` reparses both inputs, verifies that every source color stays
in the same position, and reports structural facts and SHA-256 digests, not the style contents. If an operating
system write/read-back error occurs after file creation, a partial new file may remain; the tool will not
overwrite it on a retry. Inspect and remove that specific partial output before trying again. Keep source files
and generated styles in an appropriately protected local directory.

The test suite uses synthetic styles and runs with the Python standard library:

```sh
python3 -W default -m unittest -v dev2026/specs/docs/geoserver-postgis-performance/tools/test_n1_ysld.py
```

The tests cover the accepted pattern, deterministic output, unchanged non-target bytes, color sequence and
threshold validation, LF/CRLF handling, fail-closed behavior, private output permissions, no-overwrite behavior,
and short writes. In addition, the utility was run locally against 45 WOA23 source styles held in the protected
Round 198 evidence area. All 45 passed make/check, and all 45 generated outputs were byte-identical to their
Round 199 generated counterparts. Those source and generated style files are intentionally not included in this
repository, so that input-level exercise is not independently reproducible from this PR alone.

## Limits

`N1-STATIC-PASS` means only that the file structure and transformation satisfy the static contract. It does not
show that GeoServer or GeoTools evaluates the style as expected; it does not prove pixel equivalence, boundary
behavior, `min=max`, mask behavior, NaN handling, runtime safety, cache behavior, or performance. Those require
separately designed rendering tests on representative data. The WOA23 results and their limits are summarized
in the [N1 case study](woa23-n1-case-study.md).

This utility is not a deployment tool. It has no host, GeoServer REST, database, authorization, or GO logic.
Do not reuse the historical operational runners as deployment commands; any deployment requires target-specific
review and authorization.
