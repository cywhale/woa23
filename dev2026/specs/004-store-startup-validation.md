# 004 — D1: store startup validation

**Status:** spec, acceptance cases, test plan and an **implementation proposal**
(§9-16). **Nothing is implemented**, no option is adopted, and nothing in this
document authorises a candidate change.

| rev | date | change |
|---|---|---|
| 2 | 2026-08-10 | Implementation proposal added — §9-16. Purpose restated as *fail before ready, not on first request*. Zarr v2 metadata scope; relative-path resolution and what a message must name; **metadata only, never a chunk**, with an acceptance case that asserts it; where validation runs under `-w 2` and what `preload_app` changes; the startup / readiness / data-path boundary. Import-time and lifespan compared, with a split recommendation and the case for lifespan-alone if one change is preferred. Four read-only questions listed as prerequisites. Nothing implemented. |
| 1 | 2026-08-09 | First draft. Measured behaviour carried over from spec 002 §7 D1; acceptance cases, test plan and authorisation boundaries added. |

---

## 1. The behaviour, as measured

Measured 2026-08-09 in isolated staging against the package clone, with no socket
bound and no request sent. **Six negative fixtures and one real-store control.**

| # | fixture | what is at the path | import | startup | first data request |
|---|---|---|---|---|---|
| N1 | `WOA23_ZARR_STORE` **unset** | nothing configured | **exit 1** `KeyError` | **exit 1** | not reached |
| N2 | path does not exist | nothing | 0 | 0 | `FileNotFoundError` on the group path |
| N3 | existing but **empty** directory | a directory, no contents | 0 | 0 | `FileNotFoundError` on the group path |
| N4 | a **regular file** | a file, not a directory | 0 | 0 | `FileNotFoundError` on the group path |
| N5 | group path present, `.zgroup` **not JSON** | right shape, unparseable metadata | 0 | 0 | **`JSONDecodeError`**: `Expecting value: line 1 column 1 (char 0)` |
| N6 | group path present, `zarr_format: 99` | right shape, impossible format | 0 | 0 | **`MetadataError`**: `unsupported zarr format: 99` |
| P1 | the real production store | — | 0 | 0 | **returns data** |

Three facts this establishes, each of which constrains what a fix may claim:

1. **Only N1 fails before serving.** Everything else imports, passes
   `gunicorn --check-config`, and fails per request.
2. **N2, N3 and N4 are indistinguishable at every stage.** All three produce the
   identical `FileNotFoundError` naming `<store>/1_degree/annual/TS`. **A missing
   group path is not proof that the target is not a Zarr store** — it reports that
   the expected subpath is absent and says nothing about the store root.
3. **The two genuinely malformed stores produce a different class of error, and
   neither names the store.** `JSONDecodeError` names **no file and no path at all**;
   from a log it does not identify the store, the group, or even that the failure is
   store-related. `MetadataError` names the problem but not the file.

**Readiness is a fourth stage and is not established.** Whether the OpenAPI endpoint
answers 200 while the store is unreadable needs a listening server, which nothing has
authorised. It is a strong inference from N2–N4 — they import and pass
`--check-config` — and an inference is what it remains.

## 2. Scope

**In scope:** whether, and where, the candidate should detect an unusable store
before it starts serving; what a fix would have to establish; how it would be tested.

**Out of scope:** the reference's behaviour beyond §3 (it is not ours to change); the
row-order question (spec 003); PM2 / deployment validation; performance.

## 3. The reference behaves differently, and the difference is structural

| | reference | candidate |
|---|---|---|
| store source | `woa23_app.py:63`, `zarr_store_path = "data/"`, hard-coded | `api/config.py:31`, `os.environ["WOA23_ZARR_STORE"]`, mandatory |
| a missing store at startup | import succeeds, `--check-config` exit 0 | **unset variable**: import fails, exit 1 |
| effect of `WOA23_ZARR_STORE` | **none** — it is ignored | it *is* the store |
| what "not configured" looks like | a relative path resolved against whatever the cwd is | an absent variable, which is loud |

The reference cannot be misconfigured by the environment because it ignores it; it
can be misconfigured by being started in the wrong directory, and nothing detects
that at startup either. **`WOA23_ZARR_STORE` is not in production's environment
today** — production's gunicorn master has no `WOA23_*` variable at all — so the
candidate requires something production does not currently set. **Whether PM2 would
pass it through is unknown and belongs to deployment validation, not here.**

## 4. Options

None is chosen. Each is a **candidate change requiring its own approval**.

### A. Do nothing; document the behaviour

- Cost: none. Risk: a misconfigured deployment serves errors per request rather than
  failing to start, and one of those errors does not mention the store.

### B. Validate at import time, in `api/config.py`

Check that the configured path exists and is a directory when the variable is read.

- Catches N2, N3, N4 at import — the same stage that already catches N1.
- **Does not** catch N5 or N6: a directory that exists is not necessarily a Zarr
  store, and this check does not claim otherwise.
- Cheapest, and the narrowest claim.

### C. Validate the store opens, at startup

Open the store — or one known group — during application startup.

- Catches N2–N6, i.e. every negative fixture.
- Costs a read at startup and makes startup depend on the store being reachable,
  which changes the failure mode of a restart during a storage outage from "serves
  errors" to "will not start". **That is a trade, not an improvement**, and which side
  is preferable is an operational decision.
- **Must not be conflated with readiness.** §1 stage 4 is still unestablished.

### D. Improve the error rather than the timing

Leave startup alone; wrap the per-request failure so the message names the store path
and the group it tried to open.

- Fixes the worst property found — `JSONDecodeError` naming nothing — without
  changing when the process fails.
- Does not stop a misconfigured process from starting.

## 5. Acceptance cases

Whatever is chosen, these are the cases, and the fixtures already exist as built for
the 2026-08-09 measurement.

| case | fixture | required outcome under A | under B | under C | under D |
|---|---|---|---|---|---|
| D1-1 | N1 unset | fail at import, exit 1 | unchanged | unchanged | unchanged |
| D1-2 | N2 nonexistent | error per request | **fail at import** | **fail at startup** | error per request, **naming the path** |
| D1-3 | N3 empty dir | error per request | **fail at import** | **fail at startup** | error per request, **naming the path** |
| D1-4 | N4 regular file | error per request | **fail at import** | **fail at startup** | error per request, **naming the path** |
| D1-5 | N5 `.zgroup` not JSON | error per request | error per request | **fail at startup** | error per request, **naming the path and group** |
| D1-6 | N6 `zarr_format: 99` | error per request | error per request | **fail at startup** | error per request, **naming the path and group** |
| D1-7 | P1 real store | serves data | serves data | serves data | serves data |

**D1-7 is not decoration.** Without it, "every fixture failed" is equally consistent
with the harness being broken, and six identical errors look like six findings rather
than one behaviour plus a harness that cannot read anything.

Two further acceptance conditions for any option that changes behaviour:

- **D1-8, no false positive:** the real store must not be rejected by the new check,
  under the same launch the arms use (`-S`, package clone on `PYTHONPATH`).
- **D1-9, the message is actionable:** every failure message must name the configured
  path. This is the one property the measured behaviour most clearly lacks.

## 6. Test plan

**Offline, no host:** every case above can be exercised with a temporary directory
and the interpreter alone, as the 2026-08-09 measurement was. The three stages are
distinguished by how they are invoked, and the distinction is part of the test:

| stage | how it is exercised |
|---|---|
| import | `python -S -c "import api.app"` |
| startup | `gunicorn api.app:app --check-config` — builds the app, binds nothing |
| first data request | the query coroutine called in-process, no socket, no HTTP |

A regression suite would assert the **full 7×3 matrix**, not just the changed cells:
an option that fixes D1-2 by breaking D1-7 passes any test that only looks at the
fixture it was written for.

**On a host:** only if readiness (stage 4) is to be established, and that needs a
listening server and its own authorisation. It is not part of this plan.

**Fixture handling:** the six negative fixtures are built under a staging directory
and are never placed inside the package clone, production's site-packages or
`~/python/woa23`.

## 7. Authorisation boundaries

- **This document authorises nothing.** It is a spec and a test plan.
- **No candidate change is implemented.** `api/` remains byte-identical to
  `origin/main`, and any of options B, C or D is a change to `api/config.py` or
  `api/app.py` needing its own spec revision, review and explicit approval.
- **No VM24 action is proposed.** The measurement in §1 is already done; nothing here
  requires re-running it.
- **No production contact.** Not 8050, 8786 or 8787; no PM2, no systemd.
- **Readiness stays out.** Stage 4 is named as unestablished and is not smuggled in
  under "startup validation" — they are different stages and conflating them is how
  a health check comes to mean less than it appears to.
- **No performance claim.** Option C adds a read to startup; its cost is unmeasured.

## 8. Open questions for the PI

1. Which option — A, B, C or D — or a combination? B and D compose; C subsumes B.
2. For C: is "will not start during a storage outage" preferable to "starts and
   serves errors"? This is an operational preference and not a technical one.
3. Should the reference be left entirely alone? §3 says it ignores the variable
   altogether, so any alignment between the two arms is a separate question.
4. Is D1-9 — every failure message names the configured path — required regardless of
   which option is chosen? It is the smallest change with the clearest benefit.

---

# Implementation proposal (revision 2)

**Still a proposal. Nothing here is implemented, `api/` remains byte-identical to
`origin/main`, and no option is adopted.**

## 9. The purpose, stated so it can be tested against

**An invalid or non-Zarr store must fail clearly before the service can be treated as
ready — not on the first data request.**

That sentence has three testable parts, and §13 keeps them apart: *fail clearly*
(names the path), *before ready* (not merely before the first request), and *invalid
or non-Zarr* (which is a wider class than "the group path is missing").

## 10. What the candidate actually does today

Read from the source, not from memory:

| | |
|---|---|
| store path | `api/config.py:31` — `zarr_store_path = os.environ["WOA23_ZARR_STORE"]`, evaluated **when `api.config` is imported** |
| group path | `api/query.py:139` — `f"{zarr_store_path}/{grid_path}/{subgroup}"` |
| open | `api/query.py:178` — `xr.open_zarr(zarr_group_path, chunks=None)` |
| grids | `api/config.py` `grid_dir` — `1_degree`, `025_degree` |
| subgroups | `api/query.py` `determine_subgroup` — `{annual,monthly,seasonal}/{TS,Oxy,Nutrients}` |
| lifespan | `api/app.py:57` — an `@asynccontextmanager` `lifespan` **already exists** and currently only prints |

So the group space the candidate can address is **2 grids × 3 periods × 3 parameter
groups = 18 group paths**. The candidate never opens the store *root*; it opens a
group path directly.

## 11. The five points

### 11.1 Supported Zarr format and metadata scope

The store is **Zarr v2**, read through `zarr 2.18.6` and `xarray 2025.3.1` (the
versions in the package clone, recorded in `c1_environment.json`). Validation is
therefore scoped to **Zarr v2 group metadata** and nothing else:

- what it reads: a group's `.zgroup` — and `.zmetadata` where consolidated metadata
  is present — sufficient to establish that the path is a readable Zarr v2 group;
- what it does **not** claim: that the arrays are complete, that chunks exist, that
  values are correct, or that the store is internally consistent;
- **Zarr v3 is out of scope.** The `zarr_format: 99` fixture (N6) shows the reader
  already rejects an unknown format with `MetadataError`; validation should surface
  that error, not reimplement the version check.

**Unresolved, and it decides the design — a read-only question:** does a
`.zmetadata` exist at the **store root**, or only at each of the 18 group paths?
The candidate only ever opens group paths, so "validate the store" may have no root
object to validate. §15 lists this as the first thing to determine, and the two
sub-options in §12.3 depend on the answer. **It is not assumed here.**

### 11.2 How a relative store path is resolved

`WOA23_ZARR_STORE` may be relative — C1 and C2 both ran the candidate with the
literal `'data/'`, deliberately, to match `woa23_app.py:63`.

A relative value resolves **against the process's current working directory**, and
nothing in the candidate normalises it. Two consequences the proposal must state
rather than discover:

- the same configuration means different stores depending on where the process was
  started. Production's reference works only because gunicorn runs from
  `~/python/woa23`;
- **any validation must resolve the path the same way the read path will**, and must
  **report the resolved absolute path, not the configured string**. "`data/` not
  found" is not actionable; "`/home/odbadmin/x/data/1_degree/annual/TS` not found
  (from `WOA23_ZARR_STORE='data/'`, cwd `/home/odbadmin/x`)" is.

Validation **must not** silently absolutise the value or change how the read path
resolves it. Reporting the resolution is in scope; changing it is a different change
and is not proposed.

Note the existing double slash: `'data/'` + `'/'` + grid gives `data//1_degree/...`.
It is harmless to the readers and it is what both arms have always produced. **Any
validation must build its paths with the same expression as `query.py:139`**, or it
will validate a path the read path never uses.

### 11.3 Metadata only — never a data chunk

**Validation reads metadata and must not read a single data chunk.** Three reasons,
and the first is the one that would otherwise be discovered later:

1. **It would warm the page cache before any measurement.** S1 already found that a
   readiness probe issuing a real query gave one arm a warm store handle and skewed
   the comparison. A startup chunk read does the same thing to every future latency
   measurement, and S2 performance validation has not run yet.
2. Chunk reads are unbounded in cost; metadata reads are small and fixed.
3. It is not needed for the stated purpose: N5 and N6 are metadata failures, and no
   fixture requires touching a chunk to detect.

Concretely: `xr.open_zarr(path, chunks=None)` opens lazily and reads metadata; it does
**not** materialise arrays. Validation may open and inspect; it must not index,
`.compute()`, `.load()`, or select values. **An acceptance test asserts this rather
than trusting it** — see §13, D1-10.

### 11.4 Where validation runs under multiple workers, and what it costs

Production runs `-w 2`. Where the check executes depends on gunicorn's `preload_app`,
and **which setting production uses is not established here**:

| `preload_app` | app imported | import-time check runs | lifespan check runs |
|---|---|---|---|
| off (gunicorn default) | in **each worker** after fork | **once per worker** (2×) | once per worker (2×) |
| on (`--preload`) | **once in the arbiter**, before fork | **once** | once per worker (2×) — lifespan is per ASGI app start |

**Open item for a read-only check:** whether `~/python/woa23/conf/start_app.sh`
passes `--preload`. It changes the multiplier and, more importantly, *which process*
fails: with preload on, an import-time failure kills the arbiter before any worker
exists; with it off, each worker fails to boot and gunicorn gives up after repeated
failures. Both end with no service, by different routes and different log output.

**Cost.** Metadata only, so the units are small reads, not chunk decompression:

- validating **one** known group: 1 metadata read per process;
- validating **all 18** group paths: 18 metadata reads per process — and this is only
  safe if all 18 exist, which is **not established**. If some combinations legitimately
  do not exist in the store, validating all of them would reject a healthy store,
  which is exactly the false positive D1-8 forbids.

**No cost figure is given here because none has been measured.** Bounding it is a
small offline measurement against the real store and belongs to whichever option is
adopted, not to this document.

### 11.5 The boundary between the three failure points

These are three different stages and the value of the change depends on not blurring
them. C1 and C2 both printed this distinction at run time for the same reason.

| stage | question it answers | today | what an option can change |
|---|---|---|---|
| **startup failure** | can this process serve at all? | only N1 (unset variable) fails here | B and C move N2–N4, and C moves N5–N6, into this stage |
| **OpenAPI readiness** | is the process up and routing? | **200 even when the store is unusable** (inferred from N2–N4 importing and passing `--check-config`; not measured, needs a listening server) | nothing here changes what readiness *means*; a startup failure removes the case where readiness could be green with a bad store, because there is no process |
| **data-path failure** | can this request be served? | every negative fixture except N1 fails here | D improves the *message* without moving the stage |

Two statements that must survive into any implementation:

- **A 200 on the OpenAPI document is process readiness, never store readiness.** It
  reads nothing from the store. This does not change.
- **"Fails before ready" is achieved by there being no process to be ready**, not by
  readiness learning about the store. Making the readiness endpoint check the store
  would put a store read on a health check that is polled — a different design with a
  different cost, and it is **not** proposed.

## 12. Import-time versus lifespan

### 12.1 Import-time (option B, extended to metadata)

Validation in `api/config.py`, or a function it calls, at the moment
`zarr_store_path` is read.

**For.** Same stage as the one failure the candidate already has (N1), so the model
stays "a misconfigured candidate does not import". **Visible to
`gunicorn --check-config`**, which loads the app without starting it — so a
deployment can test its configuration without binding a port. Runs before any ASGI
machinery exists, so the failure is a plain traceback naming the path.

**Against.** Import-time side effects are surprising: importing `api.config` would
touch the filesystem, and every test, tool or REPL that imports it inherits that. It
also runs under `--check-config` in environments where the store may legitimately be
absent. Under `preload_app` off it runs once per worker — cheap for metadata, but
still per worker. And there is no async context, so it must be synchronous.

### 12.2 Application startup / lifespan (option C)

Validation inside the `lifespan` context manager that `api/app.py:57` **already
has** — it currently only prints, so the hook exists and no structural change is
needed.

**For.** The conventional place for startup checks. Async context available. Runs
once per worker as part of serving, not as a side effect of importing a module. A
failure there is an ASGI startup failure: uvicorn refuses to start the app and the
worker exits, so the process never serves — which is precisely the stated purpose.

**Against.** **`gunicorn --check-config` does not run lifespan.** A configuration
check would pass while the deployment is unusable, which is the weaker property of
the two and matters most to whoever is deploying. Importing `api.app` also still
succeeds, so any tool that only imports sees nothing wrong.

### 12.3 Comparison

| | import-time | lifespan |
|---|---|---|
| catches N1 | already, unchanged | already, unchanged (at import) |
| catches N2–N4 | yes | yes |
| catches N5–N6 | yes, if it opens group metadata | yes |
| visible to `gunicorn --check-config` | **yes** | **no** |
| import has filesystem side effects | **yes** | no |
| async available | no | yes |
| runs per worker (`preload` off) | yes | yes |
| runs once (`preload` on) | yes | no — still per worker |
| failure shape | traceback at import; worker fails to boot | ASGI startup failure; worker exits |

### 12.4 Recommendation

**A split, and it is one proposal rather than two:**

- **at import time, in `api/config.py`: existence and shape only** — the configured
  path resolves, exists, and is a directory. Cheap, synchronous, no store read, and
  it keeps `--check-config` meaningful. This is what distinguishes N2, N3 and N4,
  which are today indistinguishable at every stage;
- **in `lifespan`: the metadata open** — one group path, or a stated set, opened for
  metadata only. This is what catches N5 and N6, and it belongs where a real
  dependency check belongs.

**Why not import-time alone:** doing the metadata open at import gives
`api.config` a store dependency that every importer inherits, including offline
tests and `bench/repro_c16.py`, which deliberately avoids importing `api.config` for
exactly this reason today.

**Why not lifespan alone:** it leaves `--check-config` reporting success on a
deployment that cannot serve, and that is the check a deployment is most likely to
run.

**The honest caveat.** The split is two changes in two files rather than one, and it
puts related logic in two places. If the PI prefers a single change, **lifespan alone
is the better single choice** — it achieves the stated purpose, and the
`--check-config` gap is a documentation matter rather than a correctness one. The
recommendation above is for the stronger property, not the smaller diff.

**Neither is implemented, and adopting either needs its own approval.**

## 13. Acceptance — unchanged, plus what the proposal adds

The full **7 × 4 matrix of §5 stands**, including **D1-7, the real-store control**,
**D1-8, no false positive on the real store**, and **D1-9, every failure message
names the resolved path**. The proposal adds:

| case | requirement |
|---|---|
| **D1-9a** | the message names the **resolved absolute path** *and* the configured value *and* the cwd it was resolved against — §11.2 |
| **D1-10** | **validation reads no data chunk.** Asserted, not trusted: run validation against the real store with chunk reads made observable — e.g. a store wrapper counting chunk-key reads, or comparing bytes read against a metadata-only baseline — and require zero |
| **D1-11** | under the split of §12.4, `gunicorn --check-config` **fails** for N2–N4 and **succeeds** for N5, N6 and P1 — stating exactly what the config check does and does not cover |
| **D1-12** | with the store valid, the OpenAPI endpoint still answers 200 and **no store read has occurred** — readiness is unchanged and has not quietly become a store check |
| **D1-13** | validation builds its group paths with the **same expression as `query.py:139`**, double slash included, so it cannot validate a path the read path never uses |

**The whole matrix is asserted on every change, not the changed cells.** An option
that fixes D1-2 by breaking D1-7 passes any test written only for the fixture it was
aimed at.

## 14. Test plan additions

All offline, no host, as in §6:

- the seven fixtures already exist and are rebuilt per run under a staging directory,
  never inside the package clone, production's site-packages or `~/python/woa23`;
- **`--check-config` behaviour is exercised per fixture**, since §12.3 makes it a
  distinguishing property rather than an incidental one;
- **D1-10 needs a chunk-read observer.** The cheapest honest form is a store wrapper
  that records every key read and asserts none matches a chunk key pattern; a
  bytes-read comparison is weaker because a metadata-only baseline is itself an
  assumption;
- a test that the two arms' behaviour is compared only where comparison is meaningful:
  the reference ignores `WOA23_ZARR_STORE` entirely (§3), so D1 cases are **candidate-
  only** and must not be run as a two-arm contract.

## 15. What must be determined before any option is finalised

Read-only, and **not requested or authorised by this document**:

1. **Is there a `.zmetadata` at the store root, or only per group path?** Decides
   whether "validate the store" has a root object at all, and therefore whether
   §12.4's lifespan check opens one group or something else.
2. **Do all 18 group paths exist in the real store?** Decides whether validating all
   of them is thoroughness or a false positive.
3. **Does production's `start_app.sh` pass `--preload`?** Decides where an
   import-time failure lands and how many times either check runs.
4. **Would PM2 pass `WOA23_ZARR_STORE` through at all?** It is absent from
   production's environment today (§3). If it would not, the candidate cannot start
   under PM2 regardless of what this spec decides — which is deployment validation's
   question, and it is a prerequisite for the candidate rather than for D1.

## 16. Boundaries, restated

- **Nothing implemented.** No candidate file is touched; `api/` is byte-identical to
  `origin/main`.
- **No VM24 action**, no production contact, no PM2, no systemd, no 8050/8786/8787.
- **No performance claim**, and no measurement of §11.4's cost.
- **Readiness is not redefined** and no store read is added to a health check.
- The four items in §15 are questions, not planned actions.
