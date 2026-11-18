# PostgreSQL/PostGIS index evaluation

This guide distinguishes index effects from SQL-shape, statistics, GeoServer, and cache effects. A WMS response
getting faster does not by itself prove that PostgreSQL became faster.

## 1. Start from the real workload

Capture the SQL shape emitted by the layer/view and its bind or view parameters. Confirm relation, predicates,
joins, geometry operators, selected columns, data distribution, and whether the request is a tile, map, or point
query. Check existing indexes, constraints, table size, statistics freshness, and write cost. An index on a
plausible column is a hypothesis until the actual predicate and plan support it.

## 2. Read plans safely

- `EXPLAIN` reports planner estimates; it does not execute the query or measure runtime.
- `EXPLAIN (ANALYZE, BUFFERS)` executes the statement and may consume substantial CPU, I/O, memory, locks, and
  shared capacity, even for `SELECT`. Use representative lab data or a separately reviewed bounded run.
- Compare estimated and actual rows, scan/join nodes, loops, spills, buffers, and planning versus execution
  time. Make sure the plan uses the actual request parameters and SQL shape.
- For PostGIS, confirm the spatial predicate is index-aware and matches the index operator class. The existence
  of a GiST index does not prove that the query uses it.

## 3. WOA23 VM4_248 lab result

The isolated lab tested a composite btree on `(depth, time_period)` for monthly temperature/salinity relations.
The results below are warm-lab observations, not production estimates. Round 33's [per-layer benchmark table](../../evidence/phase2-lab/hgfi/round33/analysis/perf-by-layer.tsv)
and [raw performance rows](../../evidence/phase2-lab/hgfi/round33/expansion/perf.json) are included as the
compact supporting record. The [reproducer](../../evidence/phase2-lab/hgfi/round33/tools/summarise_pg_index_perf.py)
regenerates the table from those rows.

| Round 33 warm SQL comparison | A → C median | What it supports |
|---|---:|---|
| `TS_monthly_grid1`, temperature | 45.675 → 22.338 ms (−51.1%) | A reconstructed pre-cast expression vs C's type-stable predicate, under which the existing composite index is usable; this is not a pure DDL-only comparison |
| `TS_monthly_grid025`, temperature | 302.147 → 205.916 ms (−31.8%) | Same expression/index-eligibility comparison on the 0.25° layer; not a pure DDL-only comparison |
| Other ten layers | approximately 0% (−0.5% to +0.1%) | The A→C improvement did not generalize to the other measured layers |

The same experiment measured precomputed statistics separately: A→D was −76.5% to −95.0% across the twelve
layers. That is a different optimization and must not be attributed to the index. The recorded control with
the old numeric predicate did not use the composite index; the useful condition was a predicate type compatible
with the index. The included performance rows support these results and can regenerate the per-layer table.

The result is specific to the tested warm data, SQL, parameters, and lab. It does not establish cold-cache
behavior, production impact, every bbox, or a general WMS speedup.

## 4. Design a defensible comparison

Use representative staging data, schema, versions, statistics, and settings. Compare the same SQL, parameters,
cache state, concurrency, and hardware. Use repeated paired runs and report medians/ranges and load. Collect:

1. result equivalence and expected rows/values;
2. plan shape and estimate accuracy;
3. execution time, buffers/I/O and waits;
4. index size, build time, and write/maintenance cost;
5. behavior across representative selectivity and concurrency.

An index may improve a database plan while GeoServer rendering, serialization, network, or cache dominates the
end-to-end request. Attribute only what the measurements isolate.

## 5. Production guardrails

Do not issue `CREATE INDEX`, `DROP INDEX`, `REINDEX`, `VACUUM`, `ANALYZE`, or other DDL/maintenance from a
benchmark package without separate scope and approval. Review locks, disk headroom, build duration, replication,
maintenance, and rollback. Verify catalog state and query use separately after deployment; successful DDL is
not proof of a performance improvement.
