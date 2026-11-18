# VM4_248 WOA23 lab case study

This is a concise public-facing record of the isolated WOA23 SQL/index and GeoServer-rendering lab. The database
itself is not copied or backed up here. Host-specific addresses, credentials, private logs, and executable
deployment packages are intentionally omitted; this document preserves the method and useful measured results.

## What the lab established

- A four-arm SQL comparison separated a type-stable predicate, precomputed min/max statistics, and their
  combination.
- Round 33's A→C comparison showed lower warm medians on the two monthly TS layers where the type-stable
  predicate makes the existing composite index usable. This compares a reconstructed pre-cast expression to
  the type-stable/index-eligible expression; it is not a pure index-DDL-only effect. Other ten layers were
  approximately unchanged. The large A→D results came from the separate statistics arm, not the index.
- A separate 12-layer SQL comparison checked four arms across 30 relation/value-column pairs: 480 comparisons
  had no recorded mismatches. Its performance results were warm and used one representative parameter per
  layer; they do not establish cold-cache behavior.
- A subsequent isolated GeoServer copy performed rendering consistency checks. Database isolation was not
  implied merely by using a separate GeoServer process.

For the measured index values and attribution, see [PostgreSQL index evaluation](postgresql-index-evaluation.md).
The selected raw Round 33 performance rows and the result-table reproducer are included beside this guide under
[`phase2-lab/hgfi/round33`](../../evidence/phase2-lab/hgfi/round33/analysis/perf-by-layer.tsv).
From the `round33` directory, the table can be rebuilt and checked with:

```bash
python3 tools/summarise_pg_index_perf.py expansion/perf.json /tmp/perf-by-layer.tsv
cmp analysis/perf-by-layer.tsv /tmp/perf-by-layer.tsv
python3 -m unittest discover -s tools -p 'test_summarise_pg_index_perf.py' -v
```

## Lessons worth carrying forward

1. Capture the actual SQL and parameters before choosing an index.
2. Test the index and predicate type together, with a control that isolates each change.
3. Separate SQL correctness, query-plan evidence, GeoServer rendering correctness, and end-to-end timing.
4. State whether the lab shares a database with another service; catalog isolation is not load isolation.
5. Preserve scripts/results that can reproduce the analysis, but do not treat a historical host-bound runner as
   a current operational command.

All lab facts are historical. They do not establish current host state or authorize a rebuild/deployment.
