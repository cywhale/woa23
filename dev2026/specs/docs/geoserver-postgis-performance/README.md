# PostgreSQL/PostGIS and GeoServer performance guide

This is a reusable starting point for another geospatial service project. Reuse the investigation method, not
WOA23-specific SQL, schema assumptions, thresholds, or commands without re-validation.

## Start here

| Question | Guide |
|---|---|
| How should an end-to-end investigation be scoped? | [Investigation workflow](investigation-workflow.md) |
| Can a PostgreSQL/PostGIS index improve the actual workload? | [PostgreSQL index evaluation](postgresql-index-evaluation.md) |
| How should a GeoServer or style change be isolated and checked? | [GeoServer/style evaluation](geoserver-style-evaluation.md) |
| How should timing, pairing, cache and evidence be reported? | [Measurement and evidence](measurement-and-evidence.md) |
| What did the VM4_248 lab actually do? | [VM4_248 lab case study](vm4_248-lab.md) |
| What did the WOA23 GeoServer N1 style work establish? | [WOA23 N1 case study](woa23-n1-case-study.md) |

## Core principle

Trace the request through the full path:

`client → proxy/cache → GeoServer → datastore SQL/view → PostgreSQL/PostGIS → image encoding`

A faster response alone does not identify which component improved. Separate database execution, GeoServer
loopback, and public-path measurements wherever possible. Keep correctness, performance, deployment, and client
adoption as distinct decisions.

The [WOA23 case study](vm4_248-lab.md) is historical evidence from an isolated lab. It is not a production
guarantee or an authorization to operate a current host.
