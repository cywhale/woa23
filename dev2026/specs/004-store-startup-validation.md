# 004 — D1: store startup validation

**Status:** spec, acceptance cases, test plan and an **implementation proposal**
(§9-16). **Nothing is implemented**, no option is adopted, and nothing in this
document authorises a candidate change.

| rev | date | change |
|---|---|---|
| 8 | 2026-08-10 | **Three reporting corrections** — §48-51. The runner's readiness line claimed the store had not been touched; under the patched candidate the lifespan has already read the anchor group's metadata by then, so it is corrected in the runner, its comment, spec 002 and a CLI assertion, and the resulting **asymmetry between the arms** is named — harmless to 5.2A, relevant to any future latency work. **64/64 MATCH does not prove the path strings are identical**: different strings can denote the same location, so the builder equivalence is cited from the offline D1-13 test instead, as separate evidence for a separate claim. **An unchanged mtime is an observation, not a guarantee**: the basis for 'production's store was not written' is that the runner performs no write operation, with the mtime corroborating. §51 states how the 2026-08-10 C1 rerun may be cited and what it does not establish. |
| 7 | 2026-08-10 | **`source_time_span` and `climatology` separated** — §40. The WOA23 time spans (`all`, `decav`, …) are source provenance, not an API request parameter; `climatology` (`annual`/`seasonal`/`monthly`) is what a user selects. The Zarr group path carries **no source_time_span at all**, so §11.1's question about needing `all` in a path does not arise. Depth cases restated in full with source coverage 1965-2022 at fixture/documentation level and grid/parameter/climatology/depth as request dimensions. **D1's scope stated definitively** — §41. **Chunk wording corrected** — §42: 'reads Zarr metadata files only, no array data chunk including coordinate chunks'; no claim about *which* metadata files, since the audit hook observed exactly one (`.zgroup`) on a consolidated fixture and that is store-dependent; observer scope stated (it sees `open`, not `listdir`). **D1-D3's six requirements all demonstrated offline** — §43, including that the request reaches the group open rather than the upstream 400, and that a fixture too thin to serve a successful request cannot demonstrate request-level failure. **D1-11 tabulated per fixture as split-only** — §44. Builder input coverage enumerated — §45. Nothing applied. |
| 6 | 2026-08-10 | **Correction, measured: `xr.open_zarr(chunks=None)` does read chunks** — it is lazy about data variables but materialises the coordinate arrays, which are stored as chunks (`depth/0`, `lat/0`, `lon/0`). The proposed check would have satisfied 'metadata only' as I had written it and violated the condition as stated. **Replaced with `zarr.open_group(mode="r")`: zero chunk reads, identical detection** of an empty directory, an unparseable `.zgroup` and an unsupported format; patch regenerated (185 lines) and re-checked. **Three cases move offline** on a synthetic isolated fixture that never involves production's store — D1-10 (audit hook, 0 chunk reads on all seven fixtures **including the valid one**), D1-D3, and a synthetic positive control for D1-8 (the real store still required). Conditions checklist and revised step list: steps 1-3 are now entirely offline. Holding for authorisation. |
| 5 | 2026-08-10 | **Source citation** for the WOA23 documentation — URL, sha256 `140aa25f…`, retrieval date, page and table anchors for §23, and a note that the PDF's embedded `/Title` still says WOA18 while the body is WOA23. **`D1-D2` renamed `D1-depth-out-of-range` and marked CHARACTERIZATION PENDING**: the first measurement after implementation must pin status (JSON and CSV separately, since `C18`/`C18-csv` already differ), body/error text, and that the outcome is request-level — it may not stay unasserted. `D1-D1` renamed `D1-depth-supported`. **Proposed diff committed as `specs/patches/004-store-paths.patch`**, produced outside the repository and verified with `git apply --check` — not applied. Sandbox exercise of the patch **corrected this spec twice**: N3, an empty directory, passes the import check and is caught at lifespan, not at import as revision 3 claimed; and D1-11 is correspondingly narrower — `--check-config` fails for N2 and N4 only, succeeding for N3, N5, N6 and P1. Nothing implemented. |
| 4 | 2026-08-10 | **Upstream availability rules recorded from the official WOA23 documentation** (NCEI, pp11-12), supplied by the PI — oxygen and the inorganic nutrients are one-degree only, so `query.py:114` reflects the dataset rather than limiting the API, and **Table 4's depth ranges vary by variable AND climatology** (seasonal nitrate 0-800 m against annual nitrate 0-5500 m). **Purpose narrowed** to: the configured store resolves and its required anchor group is a readable Zarr v2 group — with explicit non-goals, chief among them that **a missing non-anchor request-specific group is request-level and must never prevent startup**. **Structural guarantee selected by the PI**: `query.py:139` uses the shared builder too, output byte-identical including the double slash, and **C1 and C2 must be re-run** because the read path changes. **Depth stays request-level**, pinned by a supported and an unsupported case, with the unsupported case's outcome left to be measured rather than asserted. Sequencing revised so the read-path change is re-validated before validation is layered on it. Nothing implemented. |
| 3 | 2026-08-10 | Clarifications and the candidate implementation plan — §17-22. **D1-12 split into D1-12a (real store: readiness completes, zero chunk reads) and D1-12b (invalid stores: fail before readiness is observable)**. **The reachable group set is 12, not 18** — revision 2's arithmetic ignored `query.py:114`, which restricts 0.25° to temperature and salinity, making six of the eighteen unreachable by construction. **Settled: the twelve are not a startup invariant**; only four are exercised by any contract case, so requiring twelve would require eight never observed. One stated anchor group instead, with an explicit store manifest named as the only acceptable route to wider coverage; §15 question 2 withdrawn. **D1-11 marked as an acceptance condition of the split option only.** Implementation plan added: a pure `api/store_paths.py` shared by both call sites, ~6 lines in `config.py`, ~8 in the existing `lifespan`, **`query.py` untouched** with D1-13 as an asserted equivalence rather than a structural one, and the trade stated. Empty public API diff. Test matrix extended to D1-15. Multi-worker behaviour under both `preload_app` settings. Nothing implemented. |
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

---

# Revision 3 — clarifications and the candidate implementation plan

**Still a plan. No candidate file is modified, `api/` remains byte-identical to
`origin/main`, and nothing here is executed.**

## 17. Three clarifications

### 17.1 D1-12 applies to the real-store control only

D1-12 was written as though it were general. It is not, and split into the two
statements it was conflating:

| case | store | required |
|---|---|---|
| **D1-12a** | **P1, the real store** | process readiness completes — the OpenAPI endpoint answers 200 — **and no data chunk has been read** at any point during startup or readiness |
| **D1-12b** | **N2–N6, every invalid store** | the process **fails before readiness can be observed**. There is no readiness to check, because there is no serving process |

D1-12a is the only one that asserts anything about a 200. Asserting readiness for an
invalid store would be asserting a state the design exists to prevent — and under the
split, N2–N6 never reach the point where an OpenAPI request could be made.

N1 remains as it is today: it already fails at import, before either.

### 17.2 The reachable group paths are **12, not 18** — and none is a startup invariant

**Revision 2 said 18. That was wrong**, and the error was arithmetic over the code's
combinatorial space rather than over what the code can reach. `api/query.py:114`
restricts the parameter set at 0.25°:

```
available_pars = ['temperature', 'salinity'] if gridSz == 0.25 else [... eight ...]
```

so `025_degree/*/Oxy` and `025_degree/*/Nutrients` — six of the eighteen — are
rejected with a 400 at `query.py:119` **before any store access**. They are
unreachable by construction. The reachable set is **twelve**:

```
1_degree/{annual,monthly,seasonal}/{TS,Oxy,Nutrients}      9
025_degree/{annual,monthly,seasonal}/TS                    3
```

**Of those twelve, the 64 contract cases exercise four.** Derived from
`bench/contract_cases.py` and `determine_subgroup`:

| group path | contract cases touching it |
|---|---|
| `1_degree/annual/TS` | 22 |
| `025_degree/annual/TS` | 4 |
| `1_degree/seasonal/Oxy` | 2 |
| `1_degree/seasonal/TS` | 2 |
| the other **eight** reachable paths | **0** |

**Decision: the twelve are not a required startup invariant, and validation must not
enumerate them.** Three reasons, in order of force:

1. **We have positive evidence that four exist.** Nothing we have run has ever read
   the other eight. Requiring twelve at startup would require eight things never
   observed — which is not thoroughness, it is the false failure D1-8 forbids, and it
   would be discovered on a deployment rather than here.
2. **Even if all twelve existed, coupling startup to data completeness is wrong.**
   The group is chosen per request from the query parameters. A store legitimately
   missing one combination should fail *those* requests, not refuse to start and take
   down the other eleven.
3. The set is derived from a combinatorial expansion of code constants. It changes
   whenever `grid_dir`, `time_periods` or `determine_subgroup` changes, so a startup
   invariant defined this way silently redefines itself with an unrelated edit.

**What the invariant is instead.** The narrowest thing that distinguishes "a readable
Zarr v2 store" from "not one": **one stated anchor group opens as Zarr v2 metadata**.

The anchor is **`<store>/1_degree/annual/TS`**, stated as a constant and not derived:
it is the API's default combination (grid `01`, period `0`, `temperature`/`salinity`),
it carries 22 of the exercised contract cases, and it is already the group named in
the `FileNotFoundError` that N2, N3 and N4 produce today.

**If wider coverage is ever wanted it must come from an explicit store schema or
manifest** — a declared list of required groups, versioned with the store and
justified against it — never from re-expanding the code's combinatorial space. That
would be its own spec. It is **not proposed here**, and §15's question 2 is therefore
withdrawn as a prerequisite: the answer no longer gates the design, because the
design does not depend on it.

### 17.3 D1-11 belongs to the split, not to every option

D1-11 — `gunicorn --check-config` fails for N2–N4 and succeeds for N5, N6 and P1 — is
an acceptance condition **of the split option only**. It is a consequence of putting
existence and shape at import time while the metadata open is in lifespan, and it is
false for the others:

| option | `--check-config` behaviour | is D1-11 applicable? |
|---|---|---|
| A do nothing | succeeds for N2–N6 | **no** |
| B import-time only | fails for N2–N4, and for N5/N6 if it opens metadata | **no** — different expectation |
| C lifespan only | succeeds for **all** of N2–N6 | **no** — lifespan does not run |
| **split (recommended, and now selected)** | fails N2–N4, succeeds N5/N6/P1 | **yes** |

Stated because the reverse reading is available and wrong: D1-11 is not evidence that
`--check-config` is a good store check. It is a precise statement of **what the config
check does and does not cover under this design** — it covers configuration, not the
store's contents, and it says so.

---

## 18. Candidate implementation plan

Per the PI's direction: import-time checks configuration, resolution and required
type; lifespan checks Zarr v2 metadata; both share **one pure resolver/path builder**;
**metadata only, never a chunk**.

### 18.1 Files

| file | change | why |
|---|---|---|
| **`api/store_paths.py`** | **new**, pure | the shared resolver and path builder. Pure — no filesystem access, no store access, no imports from `api.config` — so it is testable with no environment and no store, and so importing it has no side effect |
| **`api/config.py`** | ~6 lines added | after `zarr_store_path` is read, resolve it and check existence and type |
| **`api/app.py`** | ~8 lines added inside the existing `lifespan` | open the anchor group's metadata |
| `api/query.py` | **unchanged** — see §18.4 | the read path is what C1 and C2 just validated |

### 18.2 The shared pure module — interface only

```python
# api/store_paths.py — pure: no I/O, no env, no api.config import
ANCHOR_GROUP: str                       # "1_degree/annual/TS", stated not derived

def group_path(store: str, grid_path: str, subgroup: str) -> str: ...
    # EXACTLY query.py:139's expression: f"{store}/{grid_path}/{subgroup}"
    # including the double slash a trailing-slash store produces.

def resolve(store: str, cwd: str) -> str: ...
    # the absolute path the read path will use. Does NOT change resolution —
    # it reports it. os.path.join(cwd, store) semantics, no normalisation
    # beyond making it absolute.

def describe(store: str, cwd: str) -> str: ...
    # "<resolved absolute path> (WOA23_ZARR_STORE=<value!r>, cwd=<cwd>)"
    # the one string every failure message must contain — D1-9a.
```

Nothing in it touches the filesystem, so `import api.store_paths` is free of side
effects and the offline tests need neither a store nor an environment variable.

### 18.3 The two call sites — behaviour, not code

**`api/config.py`, at import**, immediately after line 31:

- resolve the configured value against the cwd;
- require the resolved path to **exist** and to be a **directory**;
- on failure raise with `describe(...)` in the message.

Catches N2, N3, N4 — the three that are indistinguishable today. Reads no store
metadata and opens nothing.

**`api/app.py`, inside the existing `lifespan`**, before `yield`:

- build `group_path(store, *ANCHOR_GROUP.split("/", 1))`;
- open it for **metadata only** — `xr.open_zarr(path, chunks=None)`, which is lazy —
  and discard the handle;
- on failure raise with `describe(...)` **and the anchor group path** in the message,
  chaining the original `JSONDecodeError` / `MetadataError` rather than replacing it.

Catches N5 and N6. Raising inside `lifespan` fails ASGI startup, so the worker exits
and never serves — which is the stated purpose.

### 18.4 The public API diff is empty, and `query.py` is not touched

**No endpoint, parameter, response schema or status code changes.** The OpenAPI
document is unchanged. The only externally visible difference is that a
**misconfigured** deployment fails to start instead of starting and returning errors.
For a correctly configured deployment there is **no observable difference at all** —
which is what D1-8 and D1-12a assert.

`api/query.py` is deliberately left alone. Two ways to guarantee D1-13 — that
validation builds the path the read path uses:

| | guarantee | cost |
|---|---|---|
| **(i) chosen** | validation calls `group_path`; `query.py:139` keeps its literal expression; **a test asserts the two produce identical strings** over a table of inputs | read path untouched, so C1 and C2 remain valid without re-running |
| (ii) not chosen | `query.py:139` calls `group_path` too — structural, not asserted | **changes the read path**, so C1's byte-exact result would have to be re-established before it could be relied on |

(i) is chosen because the read path is precisely what C1 and C2 just validated, and a
test that compares two expressions is cheap. **It is the weaker guarantee** — a future
edit to `query.py:139` breaks the equivalence and only the test catches it. That is
the trade, stated rather than hidden. If the PI prefers (ii), it needs a C1 re-run in
the same change.

### 18.5 What this does *not* do

- **No readiness change.** The OpenAPI endpoint still reads nothing from the store,
  and no store check is added to any polled path.
- **No chunk read.** `open_zarr(..., chunks=None)` is lazy; nothing indexes,
  `.compute()`s, `.load()`s or selects.
- **No change to path resolution**, only to whether it is reported.
- **No enumeration of the twelve.** One stated anchor.
- **The reference is untouched.** It ignores the variable (§3), so these are
  candidate-only cases and must never be run as a two-arm contract.

## 19. Test matrix

All offline. Every run asserts the whole matrix, not the changed cells.

### 19.1 The seven fixtures × three stages, under the split

| case | fixture | import | lifespan | first data request |
|---|---|---|---|---|
| D1-1 | N1 unset | **fail** (today, unchanged) | not reached | not reached |
| D1-2 | N2 nonexistent | **fail** | not reached | not reached |
| D1-3 | N3 empty dir | **fail** | not reached | not reached |
| D1-4 | N4 regular file | **fail** | not reached | not reached |
| D1-5 | N5 `.zgroup` not JSON | pass | **fail** | not reached |
| D1-6 | N6 `zarr_format: 99` | pass | **fail** | not reached |
| D1-7 | P1 real store | pass | pass | **returns data** |

### 19.2 The additional cases

| case | assertion |
|---|---|
| D1-8 | the real store is **not** rejected at either stage, under the arms' own launch (`-S`, clone on `PYTHONPATH`) |
| D1-9 / D1-9a | every failure message contains `describe(...)` — resolved absolute path, configured value, cwd — and the lifespan failures additionally name the anchor group |
| D1-10 | **zero chunk reads.** A store wrapper records every key read during import, lifespan and an OpenAPI request; assert no key matches a chunk-key pattern. Asserted, not trusted |
| D1-11 | *(split only — §17.3)* `--check-config` **fails** for N2–N4, **succeeds** for N5, N6, P1 |
| D1-12a | P1: OpenAPI answers 200 **and** the chunk-read count is zero |
| D1-12b | N2–N6: the process never reaches a state where readiness could be observed |
| D1-13 | `group_path(...)` equals `query.py:139`'s expression over a table of (store, grid, subgroup), including a trailing-slash store producing the double slash |
| D1-14 | `import api.store_paths` performs no filesystem access — the module is pure |
| D1-15 | the anchor is a **stated constant**, not derived from `grid_dir` / `time_periods` / `determine_subgroup` |

### 19.3 How each stage is exercised

| stage | invocation |
|---|---|
| import | `python -S -c "import api.config"` and `... import api.app` |
| `--check-config` | `gunicorn api.app:app --check-config` — imports, does **not** run lifespan |
| lifespan | the ASGI lifespan protocol driven directly, or `TestClient` as a context manager; **no socket bound** |
| first data request | the query coroutine in-process; no socket, no HTTP |

Fixtures are rebuilt per run under a staging directory and are **never** placed inside
the package clone, production's site-packages or `~/python/woa23`.

## 20. Multi-worker behaviour

Production runs `-w 2`. What each worker does under the split:

| | `preload_app` **off** (gunicorn default) | `preload_app` **on** |
|---|---|---|
| import-time check | **once per worker** — 2 filesystem stats total | **once in the arbiter**, before fork |
| lifespan check | **once per worker** — 2 metadata opens total | **once per worker** — still 2 |
| where a bad **configuration** fails | each worker fails to boot; gunicorn gives up after repeated failures | the **arbiter** fails before any worker exists |
| where a bad **store** fails | each worker's ASGI startup fails; worker exits | same — lifespan is per worker regardless |
| net effect | **no service, either way** | **no service, either way** |

Three things follow, and the third is the one to design for:

1. **Cost is bounded and small**: per worker, a `stat` plus one group's metadata open.
   No chunk read, no scan of the store, and nothing proportional to its size. **No
   figure is claimed** — none has been measured, and measuring it belongs to whichever
   change is approved.
2. **Neither setting changes the outcome**, only which process reports it and how many
   times. Both end with the service not running.
3. **Workers fail independently and identically.** The check is deterministic and
   depends on nothing per-process — no hash seed, no ordering, no shared state — so
   two workers cannot disagree. A partial failure, where one worker serves and another
   does not, is not reachable by this design; if it were ever observed it would mean
   the store changed between the two workers' startups, which is a different fault.

**Whether production passes `--preload` is still not established** (§15 question 3),
and under this design it does not change the outcome — only the log. It stays a
question for deployment validation rather than a prerequisite for this change.

## 21. Sequencing, if approved

1. `api/store_paths.py` plus its offline tests — pure, no store, no environment. **No
   behaviour change**, so it can land and be reviewed on its own.
2. The `api/config.py` call site and D1-1 to D1-4, D1-9a, D1-11, D1-14.
3. The `api/app.py` lifespan call site and D1-5, D1-6, D1-10, D1-12a/b.
4. The full 7-fixture matrix and D1-8 as a regression run.
5. **A C1 re-run is not required by (i)** — the read path is unchanged — but the PI
   may want one anyway as evidence that the contract is unaffected. That would be a
   separate authorisation.

## 22. Boundaries, restated

- **Nothing implemented.** `api/` is byte-identical to `origin/main`.
- **No VM24 action**, no production contact, no PM2, no systemd, no deployment
  validation, no performance measurement.
- **Row order is untouched** — no sorting, no pinned seed. Spec 003 stays independent
  and undecided.
- §15's questions 1, 3 and 4 stand; **question 2 is withdrawn** — §17.2 removes the
  design's dependence on it.

---

# Revision 4 — the upstream availability rules, a narrowed purpose, and the
# structural guarantee

**Still a plan. No candidate file is modified and `api/` is byte-identical to
`origin/main`.**

## 23. Why availability is conditional: the upstream data, not the code

Revision 3 called the unreachable combinations "unreachable by construction", which
was true and explained nothing. The PI supplied the authoritative source and it gives
the actual reason: **WOA23 does not publish those fields.** The restriction in
`api/query.py:114` is the API reflecting its upstream, not a limitation the API
invented.

**Source.** *WOA23 Product Documentation*, NOAA NCEI —
`https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DOCUMENTATION/WOA23_Product_Documentation.pdf`,
fetched and text-extracted 2026-08-10; 20 pages. Quotations below are from printed
pages 11 and 12.

### 23.1 Availability by variable, grid and time span (p11)

> One-degree and quarter-degree Temperature and Salinity fields are NOT available for
> the 'all' time span.
>
> Dissolved Oxygen (and related O2 fields) are available on a one-degree grid and for
> the 'decav71A0' and 'all' time span.
>
> Nitrate, Phosphate, and Silicate fields are available ONLY for one-degree grid and
> for the 'all' time span.
>
> The 'all' time span for oxygen and inorganic nutrients is the time span from
> 1965-2022.
>
> Five-degree grid statistics are available only for the 'all' time span.
>
> Quarter-degree monthly fields are ONLY available for A5B4, B5C2, 'decav71A0',
> 'decav81B0', 'decav91C0', and 'decav' time spans.

**This confirms `query.py:114` directly.** Oxygen and the inorganic nutrients are
**one-degree only**, so a quarter-degree request for them has no upstream data to
serve. The code restricting 0.25° to temperature and salinity is faithful to the
dataset; the 400 it returns is the correct answer, not a gap.

It also settles §17.2 on stronger ground than the argument given there. Enumerating
the twelve reachable paths as a startup invariant would not merely be requiring
things we have not observed — **it would be requiring combinations the upstream
dataset does not publish**, for any store built to WOA23's actual shape.

### 23.2 Depth ranges differ per variable *and* per climatology (p12, Table 4)

> **Table 4.** Depth ranges and standard depth level numbers for annual, seasonal, and
> monthly statistics of each available oceanographic variable.

| variable (code) | annual | seasonal | monthly |
|---|---|---|---|
| Temperature (t) | 0–5500 m (102 levels) | 0–5500 m (102) | 0–1500 m (57) |
| Salinity (s) | 0–5500 m (102) | 0–5500 m (102) | 0–1500 m (57) |
| Oxygen (o) | 0–5500 m (102) | **0–1500 m (57)** | 0–1500 m (57) |
| Nitrate (n) | 0–5500 m (102) | **0–800 m (43)** | **0–800 m (43)** |
| Phosphate (p) | 0–5500 m (102) | **0–800 m (43)** | **0–800 m (43)** |
| Silicate (i) | 0–5500 m (102) | **0–800 m (43)** | **0–800 m (43)** |

Table 3 (p11) gives the standard level numbers; the maximum depth of WOA23 is
**5500 m**, which is the figure `api/query.py:145` already encodes as its `dep1`
default of `5501`.

**The depth ceiling is therefore a function of (variable, climatology), not a
constant.** Seasonal nitrate stops at 800 m while annual nitrate reaches 5500 m. A
startup check cannot express that: the variable and the climatology are both chosen
per request.

## 24. The purpose, narrowed

Replacing §9's sentence:

> **D1 validates that the configured store resolves and that its required anchor
> group is a readable Zarr v2 group.** It does not validate, and does not claim to
> validate, the completeness of any variable / grid / time-span / depth combination.

### 24.1 Non-goals, stated explicitly

- **D1 does not verify that every reachable group exists.** §17.2 settled this, and
  §23.1 gives the upstream reason: the combinations are conditional by dataset
  design, not by accident.
- **A missing non-anchor, request-specific group must be handled by request-level
  criteria** — the 400 at `query.py:119` for an unavailable grid/parameter pairing,
  or the per-request failure for a group that is absent. **It must never prevent
  startup.** A store lacking seasonal silicate should fail seasonal-silicate requests
  and serve everything else.
- **D1 does not validate depth.** Depth availability varies by variable *and*
  climatology (§23.2) and is selected per request. It stays a request-level criterion
  and §26 pins it with regressions **so that it is not confused with startup
  validation**.
- **D1 does not validate time spans.** The API surfaces `time_period`, not WOA23's
  time-span axis; whether the two need reconciling is out of scope here.
- **D1 does not check data content.** Metadata only (§11.3), still.

### 24.2 What "required anchor group" means, precisely

`<store>/1_degree/annual/TS` — one-degree, annual, temperature/salinity. Per §23.1
this is the combination WOA23 publishes most broadly, and per §17.2 it is the API's
default and carries 22 of the exercised contract cases. **If that group cannot be
opened, no default request can be served and the store is not the store this service
is for.** That is the whole of the claim.

## 25. The structural guarantee (PI-selected)

Revision 3 offered (i) an asserted equivalence with `query.py` untouched, and (ii) a
structural guarantee. **The PI selected (ii).** Revision 3's §18.4 is superseded.

### 25.1 What changes

`api/store_paths.py` provides the builder, and **all three call sites use it**:
`api/config.py` (import-time), `api/app.py` (lifespan) and **`api/query.py:139`**.

**The output must be byte-identical to today's expression, including the double
slash.** `query.py:139` is `f"{zarr_store_path}/{grid_path}/{subgroup}"`, so with the
reference-matching literal `'data/'` it yields `data//1_degree/annual/TS`. The builder
reproduces that exactly — it **concatenates, it does not normalise**. Any tidying of
the double slash would be a behavioural change disguised as a refactor, and it would
change the string whose hash decides `set` iteration order — the very mechanism spec
003 is about.

### 25.2 What it costs, stated plainly

**This touches the read path**, which is what C1 and C2 validated. Therefore:

- **C1 must be re-run** — 5.2A byte-exact, 64 cases — and must still be 64/64 MATCH.
- **C2 must be re-run** — three cycles, 5.2B semantic — and must still pass, with the
  seed-diversity observation reported as it comes.
- Until both are re-run, **the existing C1 and C2 results do not describe the changed
  candidate** and must not be cited as if they did.

This is the price of the stronger guarantee and it is the PI's call to pay it. The
alternative was a test asserting two expressions agree, which a future edit to
`query.py` could break silently.

### 25.3 The property this buys

D1-13 stops being an assertion about two pieces of code agreeing and becomes
structural: there is **one** builder, so validation cannot check a path the read path
does not use. The equivalence test remains as a regression **against the old literal
expression**, which is what proves the refactor changed nothing:

> **D1-13a:** `group_path(store, grid, subgroup) == f"{store}/{grid}/{subgroup}"` for a
> table of inputs including trailing-slash, empty and absolute stores — pinning the
> builder to the literal it replaces, including the double slash.

## 26. Depth regressions — request level, and kept apart from startup

Per the PI: one supported and one unsupported depth, at request level, so depth is
never read as something startup validation covers.

### 26.1 What already exists

The contract suite already pins an out-of-range depth, and **asymmetrically**:

| case | params | recorded status |
|---|---|---|
| `C18` | `dep0=6000, dep1=7000` | **200** |
| `C18-csv` | `dep0=6000, dep1=7000` | **400** |

Both were **byte-exact MATCH in C1**. 6000–7000 m is beyond WOA23's 5500 m maximum
(§23.2), so this is the "beyond the dataset entirely" case, and the JSON/CSV
asymmetry is existing recorded behaviour rather than something this spec introduces.

### 26.2 What is missing, and is the more informative case

A depth that is **within** WOA23's overall maximum but **outside that variable and
climatology's range** — the distinction Table 4 makes and `C18` does not reach:

| new case | request | why |
|---|---|---|
| **D1-D1 (supported)** | annual nitrate, `dep0=0, dep1=800` | inside annual nitrate's 0–5500 m; must return data |
| **D1-D2 (unsupported)** | **seasonal** nitrate, `dep0=3000, dep1=4000` | annual nitrate reaches 5500 m but **seasonal nitrate stops at 800 m** (Table 4). Within WOA23's maximum, outside this combination's range |

**The required outcome of D1-D2 is not asserted here, because it has not been
measured.** It could be an empty 200, a 400, or an error, and each would be a
different finding. The regression's job is to **pin whatever it is** so that a later
change cannot alter it unnoticed — and to establish it as a **request-level**
behaviour. If it turns out to be something undesirable, that is a separate decision,
in the shape of spec 003's, not a licence for D1 to start rejecting stores.

### 26.3 The boundary this protects

| | decided at | by what |
|---|---|---|
| store resolves, anchor group opens | **startup** | D1 |
| grid/parameter pairing unavailable upstream | **request** | `query.py:119`, 400 |
| requested depth outside this variable+climatology's range | **request** | the depth slice, pinned by D1-D1/D1-D2 |
| a non-anchor group absent from the store | **request** | per-request failure — **never startup** |

Only the first row is D1's. **Three of the four rows are request-level, and the
service must remain able to start and serve everything else when any of them fails.**

## 27. Updated test matrix additions

Carrying forward §19 and replacing D1-13:

| case | assertion |
|---|---|
| **D1-13a** | the shared builder reproduces `f"{store}/{grid}/{subgroup}"` exactly, over trailing-slash / empty / absolute / relative stores, **including the double slash** |
| **D1-13b** | `api/query.py` calls the shared builder and contains no second path-building expression |
| **D1-D1** | annual nitrate at 0–800 m returns data (supported depth, request level) |
| **D1-D2** | seasonal nitrate at 3000–4000 m — **pin the observed behaviour**, whatever it is; assert only that it is a per-request outcome and that the service is still serving afterwards |
| **D1-D3** | a request for a group absent from the store fails **that request** and the service continues to serve the anchor group — the non-goal of §24.1, asserted |

`C18` / `C18-csv` stay as they are, unchanged, as the beyond-dataset case.

## 28. Sequencing, revised for the structural guarantee

1. `api/store_paths.py` + D1-13a — pure, offline, **no behaviour change**.
2. `api/query.py:139` switched to the builder + D1-13b. **This is the read-path
   change.** Offline suites must be green before anything else.
3. **C1 re-run** — 5.2A, 64/64 expected. Needs its own authorisation.
4. **C2 re-run** — three cycles, 5.2B. Needs its own authorisation.
5. Only then the `config.py` and `app.py` call sites, and the D1 matrix.

Steps 3 and 4 gate step 5: adding validation on top of an unverified read-path change
would leave two changes to disentangle if anything failed.

## 29. Boundaries

- **Nothing implemented.** `api/` byte-identical to `origin/main`.
- **No VM24 action**, no PM2, no deployment validation, no performance measurement.
- **Row order untouched** — spec 003 independent and undecided.
- The C1/C2 re-runs in §28 are **named as required, not requested**; each needs its
  own authorisation when the time comes.
- §23's quotations are from the official WOA23 documentation, extracted from the PDF
  at the URL above. The depth table is reproduced for the combinations the API can
  reach; the document contains more.

---

# Revision 5 — the source citation, the characterization case, and the proposed diff

**Still not implemented.** `api/` is byte-identical to `origin/main`. The diff below
exists as a patch file and has been validated in a throwaway sandbox; it has not been
applied to the repository.

## 30. Source citation

Everything in §23 about grid, variable, time-span and depth availability comes from
one document. Recorded so the basis is traceable rather than paraphrased.

| | |
|---|---|
| title | *World Ocean Atlas 2023 Product Documentation* |
| publisher | NOAA National Centers for Environmental Information (NCEI) |
| URL | `https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DOCUMENTATION/WOA23_Product_Documentation.pdf` |
| retrieved | 2026-08-10 |
| **sha256** | `140aa25f37bc68d72ac39b5e28aef17144d2b47cbbc995338f9bd48f9e0dd59d` |
| size / pages | 519,496 bytes / 20 pages |
| PDF `/CreationDate` | `D:20240211112101-05'00'` |
| extraction | `pypdf`, `extract_text()` on page indices 10 and 11 |

**Caveat on the metadata.** The PDF's embedded `/Title` reads *"This document
describes WOA18 data files"* and `/Author` is `boyer` — leftovers from the WOA18
template. **The body is WOA23**; the tables cited are headed WOA23 and give WOA23's
5500 m maximum. The stale title is recorded so nobody later reads it as evidence that
the wrong document was cited.

### 30.1 What was taken from which page

| citation | printed page | anchor text |
|---|---|---|
| grid / variable / time-span availability (§23.1) | **p11** | the bullet list beginning "Quarter-degree monthly fields are ONLY available for…" |
| standard depth levels | **p11** | *Table 3. Depths associated with each standard level number.* |
| **depth ranges per variable per climatology** (§23.2) | **p12** | *Table 4. Depth ranges and standard depth levels numbers for annual, seasonal, and monthly statistics of each available oceanographic variable.* |

Table 4 is reproduced in §23.2 for the six variables the API can address. The document
contains more than is reproduced, and §23.2 is a subset chosen for relevance, not a
complete transcription.

## 31. The depth cases, renamed and given a deadline

Renamed as directed, and the second is now **characterization pending** rather than
permanently unasserted.

| case | request | status |
|---|---|---|
| **D1-depth-supported** | annual nitrate, `dep0=0, dep1=800` | expected to return data — inside annual nitrate's 0–5500 m (Table 4) |
| **D1-depth-out-of-range** | **seasonal** nitrate, `dep0=3000, dep1=4000` | **CHARACTERIZATION PENDING** — within WOA23's 5500 m maximum, outside seasonal nitrate's 0–800 m |

**`D1-depth-out-of-range` must not stay unasserted.** The first measurement after
implementation **must** record and then pin:

1. the **HTTP status** for the JSON endpoint and for the CSV endpoint separately —
   `C18`/`C18-csv` already show these two can differ (200 against 400) for a
   beyond-dataset depth, so they must not be assumed equal here;
2. the **response body or error message**, exactly, as the assertion's expected value;
3. that the outcome is **request-level** — the process is still serving afterwards,
   and a subsequent anchor-group request succeeds.

Until that measurement, the case is marked pending and **the spec does not claim what
the behaviour is**. After it, the case is a normal regression with a fixed expected
value, and any change to it is a change to be justified. `C18`/`C18-csv` remain
unchanged as the beyond-dataset case.

## 32. The proposed diff

**`dev2026/specs/patches/004-store-paths.patch`** — 177 lines, four files.

Produced outside the repository: the files were copied to a scratch tree, edited
there, and diffed against the originals, so `api/` was never written to. Verified
with `git apply --check` from the repository root — **checked, not applied**.

| file | change |
|---|---|
| `api/store_paths.py` | **new**, pure — `group_path`, `anchor_path`, `resolve`, `describe`, and the anchor constants |
| `api/config.py` | +19 — resolve, then require the path to exist and be a directory |
| `api/app.py` | +21 — open the anchor group for metadata inside the existing `lifespan` |
| `api/query.py` | +1 −1 — line 139 calls `group_path`; one import added |

### 32.1 What the sandbox established

The patch was applied to a throwaway export of `HEAD` and exercised there. **Two of
these are corrections to this spec, found by running rather than by reading.**

**D1-13a holds.** `group_path(store, grid, subgroup)` is byte-identical to
`f"{store}/{grid}/{subgroup}"` for every input tried — trailing-slash, bare,
absolute, absolute-with-slash, empty and `./`-prefixed stores across two grids and two
subgroups. `'data/'` still yields `'data//1_degree/annual/TS'`, **double slash
preserved**.

**D1-14 holds.** `api/store_paths.py` contains no `open(`, `listdir`,
`os.path.exists` or `os.path.isdir`. Importing it does no filesystem I/O.

**Correction 1 — N3 is caught at lifespan, not at import.** Revision 3's §19.1 said
import would fail for N2, N3 and N4. It does not: **an empty directory exists and is
a directory**, so an existence-and-type check accepts it. Measured in the sandbox:

| fixture | import | lifespan |
|---|---|---|
| N1 unset | **rejected** — `KeyError: 'WOA23_ZARR_STORE'` | — |
| N2 nonexistent | **rejected** — "does not exist: …" | — |
| **N3 empty directory** | **passes** | **rejected** — "required anchor group could not be opened" |
| N4 regular file | **rejected** — "is not a directory: …" | — |
| N5 `.zgroup` not JSON | passes | **rejected** — anchor could not be opened |
| N6 `zarr_format: 99` | passes | **rejected** — anchor could not be opened |
| P1 real store | passes | *(needs the real store; not exercisable offline)* |

**Correction 2 — D1-11 is narrower than revision 3 claimed.** Since
`gunicorn --check-config` runs imports and not lifespan, and N3 now passes import:

> **D1-11 (revised):** `--check-config` **fails for N2 and N4**, and **succeeds for
> N3, N5, N6 and P1**.

The config check covers *configuration* — a path that is absent or of the wrong type.
It does **not** cover an empty directory, and stating otherwise would overstate what a
deployment learns from running it.

### 32.2 What the sandbox did not establish

- **P1, the real store**, at either stage. It needs the real store, which is on VM24.
  D1-7, D1-8 and D1-12a are therefore **unverified** and are the first things a run
  must check.
- **D1-10, zero chunk reads.** `open_zarr(..., chunks=None)` is lazy by construction,
  but "no chunk was read" is asserted by a chunk-read observer against a real store,
  not by reading the call.
- Anything about **C1 or C2** under the patch. §25.2 stands: the read path changes, so
  both must be re-run and the existing results do not describe the patched candidate.

## 33. Test plan for the implementation

In the order of §28, with what is offline and what is not.

| # | step | tests | offline? |
|---|---|---|---|
| 1 | `store_paths.py` | D1-13a (builder ≡ literal, incl. double slash), D1-14 (no I/O on import), D1-15 (anchor is a stated constant) | **yes** |
| 2 | `query.py` switched to the builder | D1-13b (no second path expression in `query.py`), full existing offline suite green | **yes** |
| 3 | **C1 re-run** | 5.2A, 64 cases, expect 64/64 MATCH | **no — needs authorisation** |
| 4 | **C2 re-run** | three cycles, 5.2B, expect PASS; seed diversity reported as it comes | **no — needs authorisation** |
| 5 | `config.py` + `app.py` call sites | D1-1..D1-6 at the corrected stages (§32.1), D1-9/9a (messages name resolved path, configured value, cwd), D1-11 revised, D1-12b | **yes** |
| 6 | real-store cases | D1-7, D1-8, D1-10, D1-12a | **no — needs a store** |
| 7 | request-level cases | D1-depth-supported, **D1-depth-out-of-range characterization**, D1-D3 (absent non-anchor group fails that request only) | **no — needs a store** |

Steps 3 and 4 gate step 5: layering validation onto an unverified read-path change
would leave two changes to disentangle if either failed.

**Old C1 and C2 evidence is not carried forward.** New artefacts under new labels and
new staging directories, as every previous run has done; nothing is backfilled.

## 34. Boundaries

- **Nothing implemented.** `api/` byte-identical to `origin/main`; the patch is a file
  under `specs/patches/`, checked but not applied.
- **No VM24 action**, no C1/C2 re-run, no deployment validation, no performance work.
- The sandbox used a throwaway export of `HEAD` outside the repository and touched
  nothing under `dev2026/api/`.
- **Row order untouched** — spec 003 independent and undecided.

---

# Revision 6 — a measured correction, and the conditions this implementation is held to

**Not applied. Not authorised. Holding.** `api/` is byte-identical to `origin/main`.

## 35. Correction: `xr.open_zarr(chunks=None)` does read chunks

§11.3 and revision 5 both stated that `xr.open_zarr(path, chunks=None)` "reads no
chunk". **That is false**, and it was measured rather than reasoned:

| open | rejects N3 / N5 / N6 | chunk files read |
|---|---|---|
| `xr.open_zarr(path, chunks=None)` | yes | **3** — `depth/0`, `lat/0`, `lon/0` |
| `zarr.open_group(path, mode="r")` | yes | **0** |

`open_zarr` is lazy about **data variables** and materialises the **coordinate
arrays**, which are themselves stored as chunks. So the accurate statement of the old
claim would have been "reads no *data-variable* chunk" — narrower than "metadata
only", and narrower than the property the PI's condition asks for.

**The proposal now opens the Zarr group directly.** `zarr.open_group(path, mode="r")`
reads `.zgroup` and `.zmetadata` and nothing else, and still rejects an empty
directory (`GroupNotFoundError`), an unparseable `.zgroup` (`JSONDecodeError`) and an
unsupported format (`MetadataError`). **Zero chunk reads, identical detection.** The
patch is regenerated accordingly — 185 lines — and re-checked with `git apply --check`.

This is worth stating plainly: the check I proposed would have satisfied the letter of
"metadata only" as I had written it and violated the condition as the PI stated it.

## 36. Three cases move offline

Measured in the sandbox, on a **synthetic isolated fixture** built with
`xr.Dataset(...).to_zarr(...)` — a real Zarr v2 store with a real openable anchor
group. **Production's store is not involved and is never read.**

| case | was | now | how |
|---|---|---|---|
| **D1-10** zero chunk reads | needs the real store | **offline** | `sys.addaudithook` records every `open` event; assert no path matching a chunk key under the store |
| **D1-D3** absent non-anchor group | needs the real store | **offline** | fixture has the anchor and deliberately lacks `1_degree/seasonal/Nutrients`; opening it raises `FileNotFoundError` — the request-level failure |
| **D1-8** no false positive | needs the real store | **partly offline** | a synthetic *valid* store must pass both stages. **The real store remains required** — a synthetic positive is weaker evidence, and only the real store proves the check does not reject the thing it exists to accept |

The whole matrix now runs offline, and every fixture — including the valid one —
records **0 chunk reads**:

| fixture | import | lifespan | chunk reads |
|---|---|---|---|
| N2 nonexistent | REJECT | — | 0 |
| N3 empty directory | passes | REJECT | 0 |
| N4 regular file | REJECT | — | 0 |
| N5 `.zgroup` not JSON | passes | REJECT | 0 |
| N6 `zarr_format: 99` | passes | REJECT | 0 |
| **P1 valid (synthetic)** | **passes** | **passes** | **0** |

## 37. Conditions this implementation is held to

Recorded as a checklist so each is auditable rather than remembered.

| # | condition | how it is verified |
|---|---|---|
| 1 | `query.py`'s path string preserved, **double slash included** | D1-13a — builder ≡ `f"{store}/{grid}/{subgroup}"` over trailing-slash, bare, absolute, empty and `./` stores |
| 2 | `config.py`, `lifespan` and `query.py` share **one** builder | D1-13b — no second path expression in `query.py`; all three import `store_paths` |
| 3 | **no data chunks read** | D1-10 — audit hook, 0 chunk reads across all seven fixtures (§36) |
| 4 | **D1-D3 uses an isolated fixture; production's store is not modified** | synthetic store under a staging directory; the suite never writes outside it and never touches the clone, production site-packages or `~/python/woa23` |
| 5 | `D1-depth-out-of-range` characterized on first measurement | JSON and CSV status pinned **separately**, body/error text pinned, request-level confirmed (§31) |
| 6 | full offline suite green | all 14 suites plus the new D1 suite, by exit status |
| 7 | **C1 and C2 re-run on the new candidate** | separate authorisations; new labels, new staging |
| 8 | **old C1/C2 results not backfilled** | new artefacts only; §33's step list |
| 9 | nothing before C1/C2 pass | no real-store D1 verification, no deployment validation, no performance work |

## 38. Revised step list

| # | step | offline? | authorisation |
|---|---|---|---|
| 1 | `store_paths.py` + D1-13a, D1-14, D1-15 | **yes** | implementation |
| 2 | `query.py` → builder + D1-13b + full suite | **yes** | implementation |
| 3 | `config.py` + `app.py` + D1-1..D1-6, D1-9/9a, D1-11, D1-12b, **D1-10**, **D1-D3**, synthetic D1-8 | **yes** | implementation |
| 4 | **C1 re-run** — 5.2A, 64 cases | no | **its own VM24 authorisation** |
| 5 | **C2 re-run** — three cycles, 5.2B | no | **its own VM24 authorisation** |
| 6 | real-store D1-7, D1-8, D1-12a; `D1-depth-supported`; **`D1-depth-out-of-range` characterization** | no | after 4 and 5 |

Steps 1–3 are now entirely offline — more than revision 5 expected, because §36 moved
three cases. Step 6 waits for C1 and C2, per condition 9.

## 39. Status

**Holding.** The patch is a file under `specs/patches/`, checked and not applied; no
VM24 action; production untouched; row order untouched. Awaiting explicit
authorisation to apply.

---

# Revision 7 — source_time_span vs climatology, and six review corrections

**Not applied, not authorised, holding.** `api/` byte-identical to `origin/main`.

## 40. `source_time_span` and `climatology` are different axes

Revision 4 quoted WOA23's time-span rules beside the API's dimensions without saying
they are not the same thing. They are not, and conflating them would put a WOA23
internal term into the API's vocabulary.

| | **`source_time_span`** | **`climatology`** |
|---|---|---|
| what it is | the period the source observations were averaged over — `all`, `decav`, `decav71A0`, `decav81B0`, `decav91C0`, `A5B4`, `B5C2` | the temporal aggregation the user asks for |
| values relevant here | nutrients and oxygen: `all` = **1965–2022** | `annual`, `seasonal`, `monthly` |
| where it lives | dataset provenance — fixture metadata and API documentation | the API's `time_period` parameter |
| **is it an API request parameter?** | **No.** A user cannot select it and it appears in no endpoint | **Yes** — `time_period`, mapped by `determine_subgroup` |
| in the Zarr group path | **absent** | present |

**The group path carries no source_time_span at all.** `query.py:139` builds
`{store}/{grid_path}/{subgroup}` where `subgroup` is
`{annual|monthly|seasonal}/{TS|Oxy|Nutrients}` — grid, climatology, parameter group.
So §11.1's open question about needing `all` in a path does not arise: nothing in the
candidate's path construction refers to a time span. Should a future store ever
encode one, it must be called **`source_time_span`** or **dataset coverage**, never
`climatology` and never `time_period`.

§23.1's quotations are unchanged and remain accurate — they are statements about
**source_time_span availability**, which is what determines whether a variable exists
on a grid at all. That is why they bear on D1: they explain why a store may
legitimately lack a group. They are not statements about anything a user can request.

### 40.1 The depth cases, described in full

Neither case names `all` as a request parameter, because it is not one.

| | `D1-depth-supported` | `D1-depth-out-of-range` |
|---|---|---|
| source coverage *(fixture metadata / documentation level)* | 1965–2022 | 1965–2022 |
| grid *(request)* | `1_degree` | `1_degree` |
| parameter *(request)* | `nitrate` | `nitrate` |
| **climatology** *(request)* | **`annual`** | **`seasonal`** |
| depth *(request)* | `dep0=0, dep1=800` | `dep0=3000, dep1=4000` |
| Table 4 range for that pairing | annual nitrate **0–5500 m** | seasonal nitrate **0–800 m** |
| expectation | returns data | **CHARACTERIZATION PENDING** |

The distinction is carried entirely by **climatology**: the same variable on the same
grid has a 5500 m ceiling annually and an 800 m ceiling seasonally.

## 41. D1's scope, stated once and definitively

> **D1 validates that the configured store resolves, and that the required anchor
> group `1_degree/annual/TS` is a readable Zarr v2 group. Nothing else.**

**D1 does not validate, and does not claim:**

- that every variable / grid / source_time_span combination exists;
- that every non-anchor group exists;
- that every depth query is valid;
- that the WOA store is complete in any sense.

**WOA23's grid, parameter, source_time_span and depth availability are conditional**
(§23, §40). Oxygen is one-degree only; nitrate, phosphate and silicate are one-degree
and `all` source coverage only; depth ceilings differ by variable *and* climatology. A
store built faithfully to WOA23 **will** lack combinations, and that is correct.

**A missing non-anchor, request-specific group is handled by that request and must
never cause startup to fail.** Demonstrated in §43.

## 42. Chunk validation — wording corrected

Replacing every earlier "metadata only" phrasing:

> The startup check reads **Zarr metadata files only. It reads no array data chunk,
> including coordinate chunks.**

`xr.open_zarr(chunks=None)` is **not** metadata-only and is no longer proposed: it
materialises coordinate arrays, opening `depth/0`, `lat/0`, `lon/0` on a small
fixture.

**No claim is made about which metadata files are read.** Measured with an audit hook,
`zarr.open_group(path, mode="r")` followed by `array_keys()` opened exactly **one**
file — `.zgroup` — on a consolidated fixture. That is an observation on one store, not
a contract: which metadata files a store needs depends on whether it is consolidated.
**The assertion is the property, not the file list**: no opened path is a data-array
or coordinate chunk.

**Observer scope, stated so it is not over-read:** the hook records `open` events. A
directory listing is not an `open`, so `listdir` traffic is invisible to it. This is
sound for the property in question — reading a chunk requires opening the chunk file —
but the assertion is "no chunk file was opened", not "no directory was consulted".

Detection is unchanged: **N2–N6 all remain identified**, and the synthetic valid Zarr
v2 anchor (P1) passes both stages with **zero chunk reads** (§36).

## 43. D1-D3 — all six requirements demonstrated offline

Run against the patched candidate in a throwaway export, on a synthetic isolated
fixture. The fixture carries the anchor group with the structure the read path
actually needs — coordinates `lon`, `lat`, `depth`, `parameters`, `time_periods` and
data variables from `available_vars` — and **deliberately lacks**
`1_degree/seasonal/Nutrients`.

| # | requirement | result |
|---|---|---|
| 1 | the missing group is a **query-reachable legal combination** | `1_degree` + `nitrate` is permitted by `query.py:114` (nutrients are one-degree, and this is one-degree) |
| 2 | the request **reaches the group open**, not the upstream 400 | raised `FileNotFoundError`, **not** `HTTPException` — so it passed `query.py:119` and failed at the open |
| 3 | the failure is **request-level** | an exception from that request only |
| 4 | a **subsequent anchor request still succeeds** | returned 3 rows |
| 5 | **lifespan did not fail** | started before, exited cleanly after |
| 6 | **production's store is not modified** | fixture built under a temporary directory; production's store is never opened, and the suite writes nothing outside staging |

Requirement 4 failed on the first attempt, against a **minimal** fixture: the real
groups carry `parameters` and `time_periods` coordinates that `query.py` requires, and
a fixture without them raises `KeyError: "No variable named 'parameters'"`. **The
fixture specification is therefore part of the acceptance**, not an implementation
detail — a fixture too thin to serve a successful request cannot demonstrate that a
failure was request-level.

## 44. D1-11 — split-only, per fixture

**`D1-11` is an acceptance condition of the split option and is not a general store
validation.** `gunicorn --check-config` imports the app and does **not** run lifespan:

| fixture | `--check-config` | why |
|---|---|---|
| N2 nonexistent | **fails** | import-time configuration error |
| N4 regular file | **fails** | import-time configuration error |
| **N3 empty directory** | **succeeds** | it exists and is a directory; caught at lifespan |
| N5 `.zgroup` not JSON | **succeeds** | lifespan does not run |
| N6 `zarr_format: 99` | **succeeds** | lifespan does not run |
| P1 valid | **succeeds** | — |

**A passing `--check-config` says the configuration is well-formed. It says nothing
about metadata validity**, and reading it as a store check would be reading it as
something it is not.

## 45. Shared builder — the required input coverage

Unchanged in design; the test inputs are now enumerated as acceptance rather than left
to judgement. `group_path` must equal `f"{store}/{grid}/{subgroup}"` for, at minimum:

| input class | example |
|---|---|
| trailing slash | `data/` → `data//1_degree/annual/TS` **(double slash preserved)** |
| bare relative | `data` |
| absolute | `/home/odbadmin/python/woa23/data` |
| absolute with trailing slash | `/home/odbadmin/python/woa23/data/` |
| empty | `""` |
| dot-relative | `./data/` |

and `import api.store_paths` must perform **no filesystem I/O** (D1-14).

## 46. Sequence after the patch is applied

1. **full offline suite** — all existing suites plus the D1 suite;
2. **C1 re-run** on the new candidate — its own VM24 authorisation;
3. **C2 re-run** on the new candidate — its own VM24 authorisation;
4. **old C1/C2 results are not backfilled** — new labels, new staging, new artefacts;
5. **not before 2 and 3 pass**: real-store D1 characterization, deployment validation,
   performance work.

## 47. This round's limits

Nothing was applied, nothing was started. No patch application, no candidate change,
no VM24, no HTTP to production or staging, no C1/C2, no deployment or performance
work. The measurements in §42 and §43 were made against a **throwaway export of
`HEAD` with the patch applied inside it**, in a temporary directory, using synthetic
fixtures; the repository's `api/` was never written to.

---

# Revision 8 — three reporting corrections, and how the C1 rerun may be cited

Offline. No VM24, no re-run, no D1 characterization, no deployment or performance
work. The C1 rerun result stands; what changes is how it is described.

## 48. "The store has not been touched" was false, and is fixed

The runner printed, at process readiness:

> both arms are PROCESS-ready (OpenAPI 200). The store has not been touched
> and is not known to be readable

**Under the patched candidate that is wrong.** The lifespan opens the anchor group's
Zarr metadata during startup — before any HTTP is served — so by the time a 200 comes
back the candidate has already read metadata for `1_degree/annual/TS`.

Corrected, in the runner's output, its comment, this spec and a CLI assertion that
pins it:

> This probe read nothing from the store. The candidate's startup anchor validation
> has already read Zarr **metadata** for `1_degree/annual/TS`, and **no data or
> coordinate chunk**; the reference validates nothing at startup. Neither arm is
> known to serve **data** yet — that is the probe below.

**The arms are now asymmetric at this point** and the runner says so: the candidate
has read one group's metadata, the reference nothing. It cannot change any byte
either returns, which is all 5.2A compares, so **the C1 result is unaffected**. It
would matter to a *latency* comparison — a warmed metadata cache is exactly the
asymmetry S1 removed from the readiness probe — and no latency is measured in `--c1`
or `--c2-cycle`. Whoever designs S2 performance validation inherits this and should
find it written down rather than discover it.

## 49. 64/64 MATCH does not prove the path strings are identical

The C1 rerun report argued that if `group_path` produced different strings the 64
cases could not have matched byte-for-byte. **That does not follow.** Two different
path strings can denote the same location — a doubled slash, a `./` segment, a
symlinked parent — and would yield identical responses. Byte-identical output is
evidence the arms read the same *data*, not that they built the same *string*.

The correct statement, and the one to cite:

> **5.2A: 64/64 byte-exact MATCH.** Separately, and offline, `bench/test_d1_store_validation.py`
> asserts that the shared builder reproduces `query.py`'s previous expression exactly —
> including the double slash — over trailing-slash, bare, absolute, absolute-with-slash,
> empty and dot-relative stores.

Two independent pieces of evidence for two different claims. Neither substitutes for
the other, and the contract result is not evidence about string construction.

## 50. An unchanged mtime is an observation, not a guarantee

The report cited production's store mtime as unchanged. That is worth recording as an
observation and is **not** proof that nothing could have been written.

- **What it shows:** no modification was observed at that path.
- **What it does not show:** that the run was incapable of writing. The account owns
  the store directory; the mtime of a directory does not change when a file inside a
  subdirectory is rewritten in place, and an unchanged timestamp is consistent with
  several things besides "nothing happened".
- **The actual basis** is behavioural: the runner performs no write operation against
  the store. Both arms open it for reading; the candidate's validation uses
  `zarr.open_group(mode="r")`; the staging `data` entry is a symlink to production's
  directory and nothing in the run writes through it.

So the claim is **"the run performs no writes to the store, and no modification was
observed"** — the first clause carrying the weight, the second corroborating. This is
the same distinction §7a.3c drew for the package clone: mode bits and behaviour are
different kinds of assurance, and an observation after the fact is the weakest of the
three.

## 51. How the C1 rerun of 2026-08-10 may be cited

**Scope: `C1 PASS — isolated package-tree contract correctness after D1 patch`.**

Established:

- the real `1_degree/annual/TS` opens under `zarr.open_group(mode="r")` — the first
  time the patched validation met the production store, so **D1-8 is no longer
  supported only by a synthetic fixture**;
- 5.2A **64/64 byte-exact MATCH**, RC/CR 32/32, 15 error-status cases included;
- `api/store_paths.py` is carried in the candidate's five-file source provenance;
- clone integrity 3/3, six processes, request ceilings respected, production
  untouched, cleanup PASS.

**Not established, and not to be written as if it were:** D1 completion; any depth
characterization; deployment or PM2 validation; any performance conclusion; and
anything about C2 under the patched candidate.
