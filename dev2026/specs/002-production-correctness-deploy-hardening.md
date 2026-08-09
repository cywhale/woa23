# Spec 002 — S2: production-like correctness, in isolation

**Status:** draft, not implemented. No VM24 process has been started for this spec.
**Depends on:** spec 001 (S1), merged at `3562492`.
**Scope of this document:** correctness of the S1 candidate under **production's package
environment**, verified in isolation. Nothing in it touches production, and nothing in
it is a performance claim.

## Revision history

| rev | date | change |
|---|---|---|
| 1 | 2026-08-08 | First draft, from the S2 outline reviewed in-session. Split per PI direction: nginx/TLS, live observation, canary and rollback moved out to later specs. C1 fixed as 5.2A over an isolated venv built from production's distribution set; C2 defined as 5.2B. Readiness and data-path smoke separated. The `p50/p99` item that contradicted the performance non-goal removed. |
| 10 | 2026-08-09 | **The clone manifest identified by content and verified** — §7a.3b. The directory holds two manifests; `clone.manifest` is the one, distinguished by the two excluded entries rather than by its name. All **33,565 files re-hashed from the tree: 0 mismatches** of digest, size or `mtime_ns`, 0 missing, 0 extra. Both digests recomputed under the current definition and reproduce `b8754d32…` / `a26ca6c3…`. **Discrepancy recorded, not reconciled:** `clone.provenance` carries the rev 1–5 name-keyed values, because it was written before the definition changed and the clone is immutable. **Read-only measured, not assumed:** `dist/` and the four artefacts are unwritable, **the parent directory is not**, so the artefacts could be replaced and `dist` renamed — the contents cannot. **C2 outcome semantics:** three named outcomes with three exit codes, `PASS_WITH_INSUFFICIENT_SEED_DIVERSITY` sharing neither `PASS`'s nor `FAIL`'s. **Process count derived** from the measured worker count with the arithmetic printed; 8 holds only for `-w 2`. |
| 9 | 2026-08-09 | **No worker-level Python provenance is claimed.** The sibling probe establishes the *launch environment and import configuration*; `/proc/<pid>/maps` is stated as a **refuter** — a production path in it disproves isolation, its absence proves nothing correspondingly strong, because maps lists mapped files and not imports. No mechanism observes `sys.path`/`sys.modules` inside a gunicorn worker; the only external route (a gunicorn `-c` `post_fork` hook) changes the arms' launch line and is not part of the C1 request. Recorded as an explicit C1 limitation in §7a.3, in every interpreter record, in the C2 summary and on stdout. **C2 seed preconditions separated from evidence** — `PYTHONHASHSEED` unset and `hash_randomization=1` only mean the interpreter was *permitted* to choose; a cycle with a broken precondition is `INSUFFICIENT` even when the three digests differ, and the result is labelled **sibling / launch-environment seed diversity**. **§9: a C1 pass is explicitly not deployment readiness.** **D1 restated as open** with a standing summary. Runner: the three S2 artefacts are now checked with the arguments (exit 2) rather than behind the host gate, where they could not be exercised offline; `check_maps` was using the un-expanded forbidden roots and now uses the same expanded list as the path check. |
| 8 | 2026-08-09 | **Process readiness and store readiness separated throughout** — §7 D2. A 200 on the OpenAPI document says the process is serving and nothing about the store; C1 may use it as a startup precondition and may not call the store ready. That the endpoint would answer 200 with an unreadable store is marked an **inference from D1, not a measurement**: no socket was bound. D1's fixtures restated as **six negative fixtures (N1–N6) and one real-store control (P1)**, with what each has at the store path. **§7a added: the runner as implemented** — the `--c1` / `--c2-cycle` modes, the separate `WOA23_S2_C1_GRANTED` / `WOA23_S2_C2_GRANTED` gates that no other grant implies in either direction, mandatory `--python-binary` / `--package-clone` / `--clone-manifest` with **no fallback to `dev2026/.venv`**, the interpreter probe plus `/proc/<pid>/maps` as two separate kinds of evidence, order fingerprints recorded outside the verdict, and the request ceilings. Offline only; nothing executed against VM24. |
| 7 | 2026-08-09 | **`-S` means the smoke did not reproduce production's startup semantics** — site.py never ran, so no `.pth` was processed. C1 as evidenced is downgraded to *isolated package-tree import correctness*, with launcher and site semantics listed as a limitation, and a site-enabled sanitised variant designed in §4.3 for separate authorisation. D1 fixtures strengthened: a missing group path is **not** proof of a non-Zarr store, and the three failure stages are now distinguished — §7. The **`-E` run of 2026-08-09 is marked invalid** and may not be cited. "evidence of interrupted installation" softened to "consistent with". The filename audit no longer offers any judgement about the six non-Python hits. |
| 6 | 2026-08-09 | Digests **recomputed keyed on the dist-info directory**, not the package name, so `h5py` and `netCDF4` at two versions each survive as distinct entries — §4.1.2b. The name-keyed values of rev 1–5 are superseded and marked as such, including `a796…`, which cannot be the package-tree digest under the new definition. The filename audit no longer calls its 68 hits false positives: contents were never read, so the paths are described and the judgement is withheld — §4.1.5. Provenance re-collected without `-E`, with the full `sys.flags` and the exact command recorded — §4.2. **§7 D1 replaced with measured behaviour**: of the three candidate failure modes only the unset variable fails at startup; a wrong or non-store path starts cleanly and fails per request. |
| 5 | 2026-08-09 | **Terminology corrected.** Rev 4 called 240 `dist-info` directories "240 distributions"; they are not the same thing and the spec now counts them separately — 240 directories, **236 runtime distributions** with usable `METADATA`, 4 stub directories, 234 distinct names. Two digests named for what they cover: **package-tree** (240 dist-info entries) and **runtime distribution** (236). §4.1.3 states which evidence the clone does *not* carry — binary, stdlib, kernel — and that each stays separate. §4.2: `sys.path` empty entries are expanded against the process's cwd before checking, the allowed stdlib paths are enumerated rather than described, and every named module's `__file__` is verified, not a sample. §4.1.5 renamed a **filename audit**, which is all it is. |
| 4 | 2026-08-08 | The 240/236 "discrepancy" resolved, and it was **mine**: my digest script filtered on `metadata["Name"]`, silently dropping four distributions whose `METADATA` does not exist. Production has 240 dist-info directories and 240 distributions, no orphans. Two real findings surfaced instead — four abandoned install stubs and two double-installed packages — recorded in §4.1.2. The recorded digest is corrected. §4.2 added: provenance for C1 must prove, from inside each arm, that nothing resolves to production's `site-packages` or `src`, with `PYTHONDONTWRITEBYTECODE=1`. §4.1.7 added: the clone's file scope, exclusions and manifest acceptance. |
| 3 | 2026-08-08 | Renamed the artefact **production package-tree clone** — "forensic clone" over-claimed. §4.1 now enumerates acceptance per component (site-packages, dist-info, `.pth`, native libraries, symlinks, bytecode, secrets) and states plainly that **full runtime byte-equivalence is not claimed**: the stdlib is outside the tree and the interpreter is not cloned. Survey of the real tree recorded, including a **`.pth` editable install pointing at production's own `src`**, which the clone must neutralise. C2's three starts reframed as observing *this run's* seed diversity, with `INSUFFICIENT` as an outcome rather than a reason to add starts. §6 given a concrete order-stability acceptance and its relationship to C2's verdict. PI decisions on C2 repetitions and the interpreter recorded. |
| 2 | 2026-08-08 | **Settled per PI direction.** C1's environment is a read-only clone of production's package tree, not a rebuild — §4.1 and the open question it replaces. C2's worker count is read from the host, not assumed: production runs `-w 2`, and the first draft's `-w 4` was a guess. C2's unpinned seed is verified across repeated independent starts rather than one. Added §5, separating production-like gunicorn validation from formal PM2/nginx deployment validation — production is supervised by **PM2**, not systemd, which the first draft assumed. Added §6, the boundary between byte-exact, semantic and order-stability. |

---

## 1. What S1 established, and what it did not

S1 showed that removing Dask from the read path is byte-for-byte correct and faster,
**under one venv of 58 distributions with `PYTHONHASHSEED=0` on both arms**. That was
the point of D2b: make the packages stop being a variable.

Production is not that environment. Its package tree holds **240 `dist-info`
directories**, of which **236 are runtime distributions** carrying usable metadata
(§4.1.2a), and the versions differ from S1's venv in ways that touch the read path
directly:

| package | S1's isolated venv | production |
|---|---|---|
| `fsspec` | 2026.7.0 | **2025.10.0** |
| `anyio` | 4.14.2 | **4.11.0** |
| `pyarrow` | (S1 lock) | **22.0.0** |
| `numpy` | (S1 lock) | **2.2.4** |
| `pandas` | (S1 lock) | **2.2.3** |

`fsspec` mediates zarr/xarray store access, `anyio` is starlette's async path, and
`pyarrow`/`pandas` sit in the interchange the query pipeline uses. S1's data says
nothing about them: **its whole validity rests on both arms having been identical
apart from the Dask change**, and re-introducing that tree re-introduces the
variable S1 removed.

S2 asks one question: **does the candidate still return exactly what the reference
returns, when both are given production's packages?** It does not ask how fast.

## 2. Scope

1. **C1 — production-environment contract.** A **read-only package-tree clone** of
   production's installed packages, shared by both arms, `PYTHONHASHSEED=0`, and the
   64 contract cases as **5.2A byte-exact**. Cloned, not rebuilt — §4.
2. **C2 — multi-worker contract.** `gunicorn` with **production's own worker count,
   read from the host** (observed `-w 2`, re-read at run time), seed unpinned as
   production leaves it, verified as **5.2B semantic** across **repeated independent
   starts**. Order stability is recorded alongside it — §6.
3. **D1 — startup failure modes.** `WOA23_ZARR_STORE` is mandatory with no fallback
   (spec 001 §4.2). Establish what a missing or wrong value actually does at start,
   and that it fails loudly rather than serving errors.
4. **D2 — readiness.** Establish what "ready" means for this service and that the
   readiness signal does not depend on the store being readable.

## 3. Non-goals

Each of these is real work that S2 deliberately does not do. Several have their own
spec; none of them may be smuggled in as "while we're here".

- **Performance of any kind.** No latency gate, no rung escalation, no timing
  comparison, no percentiles. S2 produces no number that may be quoted as
  performance. *(Rev 1 correction: the first draft listed p50/p99 observation under
  deployment acceptance while declaring performance a non-goal. That was a
  contradiction; the item moved out with the rest of live observation.)*
- **nginx, TLS, and the public path** — separate spec.
- **Live observation, canary and rollback** — separate spec. S2 runs in isolation and
  never puts the candidate in front of a user.
- **Dependency modernization** — S2 *measures against* production's package set; it
  does not change it, converge it or upgrade it.
- **Dataset cache** — chunk caching, page-cache strategy, warming.
- **Event-loop concurrency** — async conversion, worker-count tuning, concurrency
  models.

C2 will probably surface concurrency questions. **Surfacing them is in scope; fixing
them is the event-loop spec's job.** S2 names what it finds and stops.

## 4. Contract acceptance

### C1 — production's packages, byte-exact

#### The environment is a production **package-tree clone**

Called what it is. It is a copy of production's installed package tree — not a
"forensic clone" of the runtime, which would imply an equivalence this does not
establish and §4.1.3 says it does not.

Rebuilding was considered and rejected: freezing to a requirements file and
reinstalling produces *a resolution of the same names*, and a re-resolve can pick
different wheels, different transitive pins and different build metadata. Every one
of those is a variable S2 exists to hold still. So the tree is **copied, not
rebuilt**.

##### 4.1.1 Source and scale, surveyed rather than assumed

Read-only from VM24 on 2026-08-08:

```
interpreter    /home/odbadmin/.pyenv/versions/py311/bin/python3.11
site-packages  /home/odbadmin/.pyenv/versions/py311/lib/python3.11/site-packages
               (= sysconfig purelib)
               33,567 files, 3,762 directories, 1.7 GB
stdlib         /home/odbadmin/.pyenv/versions/3.11.4/lib/python3.11   <- NOT in the tree
py311          symlink -> /home/odbadmin/.pyenv/versions/3.11.4/envs/py311
               pyvenv.cfg: base 3.11.4, include-system-site-packages = false
```

##### 4.1.2 Acceptance, per component

| component | observed | how the clone is accepted |
|---|---|---|
| **site-packages tree** | 33,567 files / 3,762 dirs / 1.7 GB | per-file SHA-256 against the source, and file and directory counts equal |
| **`*.dist-info`** | **240 directories**; **236 runtime distributions**; **4 stubs**; **234 distinct names** — §4.1.2a | all 240 directories copied verbatim, stubs and duplicates included. The clone reproduces production's untidiness rather than tidying it |
| **`*.pth`** | 4: `easy-install.pth` (empty), `distutils-precedence.pth`, `basemap_data_hires-…-nspkg.pth`, **`__editable__.src-1.0.pth`** | see §4.1.4 — the editable one **must be neutralised** |
| **native libraries** | 431 `*.so` | copied byte for byte, **never rebuilt**; digests compared like any other file |
| **symlinks** | **0** in the tree today | the clone asserts zero symlinks. If any appear, the run stops: a symlink would leave the clone reading files it does not own |
| **bytecode** | 1,385 `__pycache__` dirs, 12,130 `*.pyc` | copied **with mtimes preserved**. `.pyc` validity is an mtime-and-size check against its source, so a copy that loses mtimes silently invalidates 12,130 caches and changes what the first request does |
| **build provenance** | 255 `RECORD`, 3 `direct_url.json` | copied; recorded, not interpreted |
| **secrets** | see §4.1.5 | scanned by filename, contents never read |

##### 4.1.2a Four counts, and they are four different things

Rev 4 wrote "240 distributions". A `dist-info` directory and a runtime distribution
are not the same object, and conflating them is how the earlier confusion started.
Counted separately, read-only from VM24 on 2026-08-09:

| count | value | what it means |
|---|---|---|
| `*.dist-info` **directories** | **240** | what is on disk. The package-tree manifest covers all of them |
| `Distribution` objects yielded | 240 | `importlib.metadata` returns one per directory, including directories with no metadata |
| **runtime distributions** | **236** | objects whose `METADATA` exists and carries a `Name`. This is the number that means "an installed package Python can tell you about" |
| distinct names among those 236 | **234** | two names appear twice — below |

The 4-directory difference between 240 and 236 is not a bookkeeping error. It is four
**abandoned install stubs**, each containing only an empty `REQUESTED` file — no
`METADATA`, no `RECORD` — which is what pip leaves when an install is superseded or
interrupted. Each names a version different from the one actually importable:

| stub directory | version the stub names | version actually importable |
|---|---|---|
| `pyproj-3.7.0.dist-info` | 3.7.0 | **3.6.1** |
| `lxml-5.3.0.dist-info` | 5.3.0 | **6.0.2** |
| `packaging-24.1.dist-info` | 24.1 | **25.0** |
| `timescale-0.0.5.dist-info` | 0.0.5 | **0.1.0** |

The 236-to-234 difference is **two packages installed twice**, each resolving to the
newer. Neither is on the read path:

| package | dist-info present | resolves to |
|---|---|---|
| `h5py` | `h5py-3.12.1.dist-info`, `h5py-3.9.0.dist-info` | **3.12.1** |
| `netCDF4` | `netcdf4-1.7.3.dist-info`, `netCDF4-1.7.1.post2.dist-info` | **1.7.3** |

**Neither the stubs nor the duplicates are cleaned or corrected.** The clone
reproduces them, because a clone that improves on its source is not a clone. They are
**production hygiene findings**, raised here and owned by whoever owns that
environment — S2 does not act on them.

##### 4.1.2b Two digests, keyed on the dist-info directory

Revisions 1–5 computed digests over `name==version`. That is not a key: `h5py`
appears twice and `netCDF4` appears twice, and a name-based digest cannot express
which directory each came from — two trees with the same names at the same versions
but different directory layouts would hash the same.

Each entry is therefore keyed on its **dist-info directory name**, which is unique
within the tree, and carries the fields that identify what the directory claims:

```
<dist-info directory>\t<Name>\t<Version>\t<PEP 503 normalised name>\tMETADATA=<0|1>\tRECORD=<0|1>
```

Entries are sorted by directory name, joined with newlines, and hashed. Both digests
are recomputed from inside the clone and compared with the source; both matched on
2026-08-09.

| digest | over | value |
|---|---|---|
| **package-tree digest** | all **240** dist-info directories | `b8754d32c8aaec6d2049de5d67d3f81aeff4f19effd1525d76b62955447c9b4b` |
| **runtime distribution digest** | the **236** directories with `METADATA` | `a26ca6c3cfe20ea643c30075d910bb03dbbc01eb3f9d2b4fd224b5d76701447b` |

**Superseded.** The rev 1–5 values were name-keyed and are retained only so earlier
reports stay traceable: `a79657270859880477055f9445401e04b3f4941343e332b85f0c87486914d1fa`
(240 entries) and `60236d7210c8c3647a32e7da55714e246ecc878d1d6296d10e9caf966d2b0b2a`
(236 entries). **`a796…` cannot be the package-tree digest under this definition** —
it is name-keyed, which is the thing being replaced — so the label moves to
`b8754d32…` rather than staying with the value.

The duplicates survive as separate entries, which is the point:

```
h5py-3.12.1.dist-info          h5py     3.12.1        h5py     METADATA=1 RECORD=1
h5py-3.9.0.dist-info           h5py     3.9.0         h5py     METADATA=1 RECORD=1
netCDF4-1.7.1.post2.dist-info  netCDF4  1.7.1.post2   netcdf4  METADATA=1 RECORD=1
netcdf4-1.7.3.dist-info        netCDF4  1.7.3         netcdf4  METADATA=1 RECORD=1
```

The four stubs carry empty `Name` and `Version` and are the only entries with
`METADATA=0`; **they are also the only four with `RECORD=0`**. That is **consistent with an
incomplete or interrupted installation**; it is not proof of one, and no cause is
asserted here. What is recorded is the observation.

A third digest, the **package-tree manifest digest**, is over the file-level manifest
of §4.1.7 and is the strongest: the two above describe metadata, that one describes
every byte. Its value for this clone is in §4.1.7.

##### 4.1.3 A package-tree clone, not a runtime clone

The clone reproduces production's **installed package tree**. It is not a
byte-identical copy of production's runtime, and no result from it may be described
as one. Three things stay outside it and remain **separate evidence**, each recorded
in its own right rather than inferred from the clone:

| outside the clone | where it actually lives | how it is evidenced |
|---|---|---|
| **the Python binary** | `/home/odbadmin/.pyenv/versions/py311/bin/python3.11` | its absolute path, SHA-256 and `--version`, recorded at run time (§4.2). Under the PI's authorisation it is *executed* in the staging tree; it is never copied and never modified |
| **the standard library** | `/home/odbadmin/.pyenv/versions/3.11.4/lib/python3.11` — reached through the venv's `pyvenv.cfg`, not through site-packages | the exact allowed paths are enumerated in §4.2 and every `sys.path` entry is checked against them |
| **the kernel and host runtime** | the machine | `uname`, boot id and host recorded alongside every run, as S1 already does |

So a C1 pass says: *these packages, this interpreter, this stdlib, on this host*. It
does not say *production's running process*, and §9 holds that line.

##### 4.1.4 The editable install is a real hazard, not a formality

`__editable__.src-1.0.pth` runs `__editable___src_1_0_finder.install()`, and that
finder points at:

```
/home/odbadmin/python/woa23/src        exists: yes    inside site-packages: NO
```

That is **production's own source directory**. Copied as-is into the clone, the
`.pth` would still redirect `src.*` imports there — so both arms would import
production's live `src`, from outside the clone, outside the read-only reference
copy, and outside anything the provenance check hashes. The comparison would be
between two processes reading the same production files, which is not the
comparison this spec describes.

S1 was not exposed to this: it ran under `dev2026/.venv`, built from `uv.lock`, which
has no such `.pth`. The hazard appears only because C1 uses production's tree.

**Acceptance:** the editable `.pth` and its finder module are excluded from the clone,
their absence is asserted, and `sys.path` inside each arm is recorded in provenance
and checked to contain **no path outside the clone, the arm's own staging directory,
and the stdlib**. This is a deliberate deviation from "identical to production" and is
recorded as one — production really does load `src` that way, and the clone
deliberately does not.

##### 4.1.5 Filename audit — hits, not verdicts

A **filename audit**: it matches directory entry names. It does not open files. It
therefore cannot say whether the tree contains secrets, and **nothing below should be
read as saying that it does not**.

Patterns: `*.pem *.key *.crt *.p12 .env* *credential* *secret* *token* id_rsa*
id_ed25519* .netrc .pgpass`.

**Result: 68 filename hits**, identical in the source and the clone, none new in the
clone. 62 are `.py`, `.pyc` or `.pyi`. The remaining 6, named individually:

| path | size | what the path suggests | contents |
|---|---|---|---|
| `certifi/cacert.pem` | 283,932 | a CA bundle | **not inspected** |
| `pip/_vendor/certifi/cacert.pem` | 291,366 | a vendored CA bundle | **not inspected** |
| `pipenv/patched/pip/_vendor/certifi/cacert.pem` | 281,617 | a vendored CA bundle | **not inspected** |
| `tornado/test/test.crt` | 1,042 | a test fixture certificate | **not inspected** |
| `tornado/test/test.key` | 1,708 | a test fixture key | **not inspected** |
| `jeepney/tests/secrets_introspect.xml` | 4,575 | a test fixture for a D-Bus secrets interface | **not inspected** |

Rev 3–5 called all 68 false positives. That was a claim about content, reached
without reading any content. **Withdrawn, and not replaced by a softer version of the
same claim.** The table records the path, the size and what the *path* suggests.
Whether any of these six is harmless is **not established** — no file was opened, so
the audit has nothing to say about it either way. "It sits in a test directory" is
where such a file would normally live; it is not a finding about the file.

**Acceptance:** the audit is re-run against the clone and any hit not on this recorded
list stops the run. Reading contents would need its own authorisation and has not been
requested.

##### 4.1.6 PI decision — the interpreter

**Preferred: run the clone under production's own Python binary.** That removes the
last interpreter variable, and the binary is read and executed but never modified.

Executing a production binary is a different act from reading production's files, and
every prior authorisation in this project has been for reading. **It needs granting
explicitly.**

**If it is not granted**, the clone runs under a separately installed Python 3.11.4
and the difference is carried as an explicit limitation: same version, different
build, and anything sensitive to interpreter build flags is outside what C1
establishes.

| | |
|---|---|
| environment | the package-tree clone above, **shared by both arms** |
| interpreter | Python 3.11.4, matching production |
| seed | `PYTHONHASHSEED=0` on both arms |
| store literal | identical string on both arms (S1's finding: the string, not the directory, orders `zarr_group_paths`) |
| comparison | **5.2A byte-exact**, all 64 cases |
| pass | 64/64 `MATCH` |

**Why 5.2A and not 5.2B.** Production itself cannot be restarted to pin its seed, and
that is exactly why D2a had to compare semantically. But S2 does not need to run
*production* — it needs production's *packages*. Those can be installed into a venv we
start ourselves, where the seed is ours to pin. That keeps the strong comparison and
still answers the question. The cost is that this is production's package set, not
production's running process; §9 states what that does and does not license.

The 64 cases are unchanged from S1: **33 JSON-path and 31 CSV, which partition the
set**; within them, 15 expect a non-200 status and 2 are the OpenAPI document and the
Swagger page. 47 return row payloads. Request order is counterbalanced RC/CR and
recorded per case.

##### 4.1.7 File scope, exclusions and manifest

**In scope** — everything under
`…/versions/py311/lib/python3.11/site-packages`, recursively: package directories,
`*.dist-info` (including the four stubs), `*.pth` except as excluded below, native
`*.so`, data files, `__pycache__` and `*.pyc` with mtimes preserved.

**Excluded, each for a stated reason:**

| excluded | reason |
|---|---|
| `__editable__.src-1.0.pth` | redirects `src.*` to production's own source — §4.1.4 |
| `__editable___src_1_0_finder.py` | the module that `.pth` invokes; excluding one without the other leaves a broken import |
| nothing else | no blanket exclusions. Anything else omitted would have to be justified here, and nothing is |

**Built 2026-08-09.** `~/woa23-s2-package-clone/dist`, from
`…/versions/py311/lib/python3.11/site-packages`. 33,567 source files → **33,565**
in the clone, the difference being exactly the two named exclusions and nothing else;
3,762 directories in both; zero symlinks. Manifest digests:

```
source.manifest  3c14cfe70369d081b9b10467d258892654299cba360a3852b886274550630407
clone.manifest   f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4
```

**The clone is immutable.** It is read-only — verified by attempting to create a file
and to append to an existing one, both refused, with zero writable files or
directories remaining — and it stays that way. **No package in it is updated,
installed or removed.** Its purpose is to be what production has; a clone that has
been maintained is a different environment and answers a different question.

**Manifest acceptance.** The clone is accepted only if all of these hold:

1. a manifest of `relative path → SHA-256 → size → mtime_ns` is written for the source
   tree and recomputed for the clone, and the two are **identical except for the two
   excluded paths**, which must be present in the source manifest and absent from the
   clone's;
2. file and directory counts match the source, less the exclusions;
3. **zero symlinks** in the clone;
4. the 240-entry distribution digest recomputed **from inside the clone** equals the
   source's;
5. the secrets audit (§4.1.5) re-run against the clone yields no match outside the
   reviewed false-positive list;
6. the clone is read-only when the manifest is taken, and stays read-only for the run.

Any failure stops the run. The manifest is an artefact of the run and is archived with
its results.

### 4.2 Runtime provenance — proving what was actually loaded

The manifest proves what is *on disk*. It says nothing about what the running process
*imported*, and those diverge the moment a `.pth`, a `PYTHONPATH`, a user site
directory or an inherited variable is involved. C1 is only worth running if that gap
is closed.

Collected **from inside each arm**, after startup and before the first contract case:

| recorded | must satisfy |
|---|---|
| the **exact command**, including every environment assignment | recorded verbatim, so the run can be reproduced and audited |
| Python binary: absolute path, symlink target, SHA-256 of the ELF, size, `--version` | recorded, not assumed. The invoked path is a symlink; `stat` without `-L` reports the *link's* 52 bytes, which is not the binary |
| the **whole** of `sys.flags` | recorded, not a selected few |
| `sys.executable`, `sys.prefix`, `sys.base_prefix` | mutually consistent; a surprise here means an unexpected environment is active |
| `sys.path`, **in order, after empty-entry expansion** | every entry within the allowed set below |
| `module.__file__` for **every** module in §4.2.2 | resolves inside the clone, or the arm's staging directory for the application module |
| `sys.dont_write_bytecode` | `True` — **the interpreter's state, not the variable**. `-E` makes Python ignore every `PYTHON*` variable while they remain visible in `os.environ`, so a check that reads the environment would report the request as satisfied while the interpreter ignored it. That happened on the first attempt |
| `sys.flags.no_user_site` | `1` |
| `sys.flags.hash_randomization` | `0` for C1 |
| `sys.flags.ignore_environment` | **must be `0`.** `-E` is not permitted for provenance: it silently disables the controls this table exists to establish |
| `PYTHONPATH` | recorded verbatim; anything outside the clone and staging stops the run |
| `PYTHONHASHSEED` | `0` for C1 |

##### 4.2.0 Observed, 2026-08-09 — both arms PASS

Collected with the production binary, no `-E`, from inside each arm:

```
command   env -i HOME=... PATH=/usr/bin:/bin POLARS_SKIP_CPU_CHECK=1
              PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 PYTHONHASHSEED=0
              WOA23_ZARR_STORE=data/ PYTHONPATH=<clone>:<arm>
              /home/odbadmin/.pyenv/versions/py311/bin/python3.11 -S <arm>/s2_smoke.py

binary    /home/odbadmin/.pyenv/versions/py311/bin/python3.11
          -> /home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11
          ELF, 36,576 bytes, sha256 1462f705accd72d86160338557dfcdb90d873fea578033579c8e274887a06a9c

sys.executable   /home/odbadmin/.pyenv/versions/py311/bin/python3.11
sys.prefix       /home/odbadmin/.pyenv/versions/3.11.4
sys.base_prefix  /home/odbadmin/.pyenv/versions/3.11.4

sys.flags        ignore_environment=0   no_site=1        no_user_site=1
                 dont_write_bytecode=1  hash_randomization=0
                 isolated=0  safe_path=False  utf8_mode=1  optimize=0  dev_mode=False
```

`ignore_environment=0` is the one that matters: it is the evidence that the flags
beside it were honoured rather than merely requested.

Every `sys.path` entry and every module in §4.2.2 resolved inside the clone, the
arm's staging directory or the enumerated stdlib. **`src.config` resolved to
`<staging>/reference/src/config.py`, not to `/home/odbadmin/python/woa23/src`** —
which is the evidence that excluding the editable `.pth` worked.

The empty-entry expansion was exercised on real data: invoked with `-c`, `sys.path[0]`
is `''` and expanded to `/home/odbadmin/woa23-s2-staging/candidate`, read from
`/proc/self/cwd`.

After both runs the clone still held 33,565 files, zero of them newly written, and
zero `.pyc` newer than the clone's creation.

##### 4.2.0a The `-E` run of 2026-08-09 is invalid and may not be cited

The first provenance attempt passed `-E`. That makes the interpreter ignore every
`PYTHON*` variable while they remain visible in `os.environ`, so `PYTHONPATH`,
`PYTHONNOUSERSITE`, `PYTHONDONTWRITEBYTECODE` and `PYTHONHASHSEED` were all set and
none was honoured — and the check, reading the environment, would have reported them
satisfied.

**That run is void.** Nothing from it appears in this spec and nothing from it may be
quoted. The evidence in §4.2.0 comes from the re-run, whose `sys.flags` records
`ignore_environment=0`. Any future provenance run that reports a non-zero
`ignore_environment` is void on the same grounds, automatically.

##### 4.2.1 `sys.path`: empty entries, and the allowed set

**An empty string in `sys.path` means the current working directory.** Checking it as
a literal would pass it silently while the process actually imports from wherever it
happens to be running. Every empty entry is therefore **expanded against that
process's own cwd** — read from `/proc/<pid>/cwd`, not assumed from the launcher —
and the expansion is checked like any other entry. The expansion is recorded next to
the raw value so the substitution is auditable.

Production's interpreter shows no empty entry today. That is a property of how it is
launched, not a guarantee, and the check does not depend on it.

**Allowed, exhaustively.** Anything else fails the run:

| # | path | why |
|---|---|---|
| 1 | the package clone root | the packages under test |
| 2 | the arm's own staging directory | its application code, `api/` or `src/` |
| 3 | `/home/odbadmin/.pyenv/versions/3.11.4/lib/python311.zip` | stdlib |
| 4 | `/home/odbadmin/.pyenv/versions/3.11.4/lib/python3.11` | stdlib |
| 5 | `/home/odbadmin/.pyenv/versions/3.11.4/lib/python3.11/lib-dynload` | stdlib extension modules |

**Explicitly not allowed**, and each is a stop:

- `/home/odbadmin/.pyenv/versions/py311/lib/python3.11/site-packages` — production's
  own packages. The clone exists so this path is never on `sys.path`;
- `/home/odbadmin/.pyenv/versions/py311/lib/python3.11` — the venv's lib directory,
  whose `site-packages` is the above. It is `sysconfig`'s `platstdlib` and is *not* a
  stdlib path for this purpose;
- anything under `/home/odbadmin/python/woa23` — production source;
- any user site directory;
- the empty string left unexpanded.

##### 4.2.2 Modules verified — every one, not a sample

`module.__file__` is checked for each of these after import. There is no sampling:
a module not on the list is one nobody looked at.

**Both arms:** `xarray`, `zarr`, `numpy`, `pandas`, `polars`, `pyarrow`, `fsspec`,
`fastapi`, `starlette`, `uvicorn`, `gunicorn`.

**Reference arm additionally:** `dask`, `distributed`, and the application package
`src` together with `src.config`, `src.woa23_utils`, `src.dask_client_manager`.

**Candidate arm additionally:** `api`, `api.config`, `api.query`, `api.app`.

`src` is the one that matters most. §4.1.4 excludes the editable `.pth` so that `src`
*cannot* resolve to `/home/odbadmin/python/woa23/src`; **this check is what proves the
exclusion worked** rather than assuming it did. An arm whose `src.config` resolves
under production has invalidated the run, and the run stops rather than producing a
comparison of production with itself.

If any module resolves outside the allowed set, the run stops and reports which
module and which path. **It is not corrected by changing anything in production.**

### 4.3 `-S`: what the smoke did not reproduce

The smoke ran with `-S`, recorded as `sys.flags.no_site=1`. That was deliberate — the
invoked interpreter is the venv's, and without `-S` `site.py` would add production's
own `site-packages`, which is the one path this whole exercise exists to keep off
`sys.path`. But it has a consequence that must not be left implicit.

**`site.py` never ran, so no `.pth` file was processed.** Production runs with site
enabled and processes all four. Three survive in the clone (the editable one is
excluded, §4.1.4), and two of them execute code at startup:

| `.pth` in the clone | what it does under production | under `-S` |
|---|---|---|
| `easy-install.pth` | empty | nothing |
| `distutils-precedence.pth` | imports `_distutils_hack` and installs a shim that decides whether `distutils` resolves to setuptools' copy or the stdlib's | **did not run** |
| `basemap_data_hires-…-nspkg.pth` | builds the `mpl_toolkits` / `mpl_toolkits.basemap_data` namespace packages | **did not run** |

`distutils-precedence.pth` is the one that could matter: which `distutils` a later
import resolves to is decided by whether that shim installed.

#### 4.3.1 C1 as evidenced today is downgraded

What §4.2.0 establishes is **isolated package-tree import correctness**: with the
clone on `sys.path` and site disabled, every module in §4.2.2 imports from the clone
and nothing resolves to production. That is a real result and it is what closed the
editable-`.pth` question.

It is **not** "the candidate under production's environment". These are limitations,
not caveats:

- **site and `.pth` semantics were not exercised** — the two active `.pth` files above
  did not run;
- **the launcher was not production's.** Production reaches gunicorn through PM2 →
  `conf/start_app.sh`; the smoke invoked the interpreter directly with a controlled
  environment;
- **`sys.prefix` differed.** Under `-S` it is `…/versions/3.11.4`; production's
  process reports `…/versions/py311`. The venv was not active in the sense production
  has it active.

#### 4.3.2 The site-enabled sanitised variant, for separate authorisation

To reproduce site semantics without ever putting production's `site-packages` on
`sys.path`, the clone is made the *only* site directory of a purpose-built venv:

```
~/woa23-s2-sitevenv/
  pyvenv.cfg                       home = …/versions/3.11.4/bin
                                   include-system-site-packages = false
  bin/python3.11                   symlink to production's interpreter
  lib/python3.11/site-packages     -> the read-only clone
```

Run **without** `-S`, `site.py` then processes exactly the clone's `.pth` files and
adds exactly the clone.

**Acceptance, and every item is a stop:**

1. `sys.flags.no_site == 0` — site actually ran, or the variant proves nothing;
2. `sys.prefix` is the new venv, `sys.base_prefix` is `…/3.11.4`;
3. every `sys.path` entry is in the venv, the clone, the arm's staging or the
   enumerated stdlib — production's `site-packages` appearing is a hard failure;
4. **every `.pth` in the clone is audited before the run**: its content recorded, and
   every path it adds or module it imports resolved and checked against the same
   allowed set. A `.pth` that adds an unauditable path stops the variant;
5. `_distutils_hack` is recorded as installed or not, and `distutils.__file__` is
   captured, since that is the observable difference it makes;
6. the clone stays read-only. The symlink is *into* it; nothing is written through it.

This needs its own authorisation. It is not covered by anything granted so far, and
it is **not** requested in the C1/C2 submission that follows — C1 is submitted as the
downgraded, `-S` form, with §4.3.1's limitations attached.

### C2 — multiple workers, unpinned seed, semantic

**The worker count is read from the host, not chosen.** Rev 1 wrote `-w 4`; the
running master's argv says `-w 2`, and its two children confirm it. That number is
read again at the time of the run rather than carried from here, because it is a
property of production and may change.

Observed 2026-08-08, read-only from `/proc`:

```
pid 3960  gunicorn woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker ...
          children: 4334 4366          (two, matching -w 2)
          PYTHONHASHSEED: not set in the master environment
```

**One start is not evidence that the seed is unpinned.** A single process has one
seed and its output is self-consistent, so nothing about seed variation can be read
from it.

**PI decision: three independent start/stop cycles.** The arms are started, the cases
run, both are stopped, and the whole thing is repeated three times.

What those three cycles establish is **the seed diversity observed in this run** — not
that the seed is unpinned in general, and not a bound on how often an ordering
difference appears. Three observations of a randomised value is three observations.

The outcome is therefore one of three, and the third is not a failure:

| observed | verdict |
|---|---|
| seeds differ across cycles **and** all three cycles pass the contract | **PASS** |
| any cycle fails the contract | **FAIL** — reported with the cycle and the case |
| all three cycles record the **same** seed | **INSUFFICIENT** — the premise was not exercised, so the contract result says nothing about unpinned seeds |

`INSUFFICIENT` is reported as it stands. **The run does not add cycles to chase a
different answer**: deciding to sample more after seeing the sample is how a result
stops meaning what it appears to mean. If more cycles are wanted, the number comes
from the PI, before the next run.

| | |
|---|---|
| configuration | `gunicorn -w <production's count>` on both arms, `PYTHONHASHSEED` **not** pinned |
| repetitions | at least three independent start/stop cycles; the recorded seed must differ across them, or the premise is unverified and the result says nothing |
| comparison | **5.2B semantic**: rows as a multiset keyed on `(lon, lat, depth, time_period)`, columns as a set, values exactly |
| pass | 64/64 on **every** cycle, and every difference that is found is characterised, not waived |

**What 5.2B cannot see, stated plainly:** column order, key order and float
formatting. That is the price of not pinning the seed, and it is why C1 exists
alongside C2 rather than being replaced by it. If C2 shows differences that C1 did
not, the difference is caused by the worker configuration and is a finding in its own
right.

## 5. Two different things called "deployment"

The first draft ran these together and named the wrong supervisor. They are separate,
and only the first is in this spec.

| | **production-like gunicorn validation** (this spec) | **formal deployment validation** (later spec) |
|---|---|---|
| what runs | `gunicorn` invoked directly, in a staging directory | the real supervision path |
| supervisor | none — the runner starts and stops it | **PM2** (`pm2-odbadmin.service`) via `~/python/woa23/conf/start_app.sh` |
| what it establishes | the application behaves correctly when given production's packages and worker count | the *deployment* behaves correctly — restart policy, log handling, environment inheritance, ordering, failure recovery |
| nginx / TLS | not involved | involved |
| touches production config | never | by definition |

Rev 1 assumed systemd. The host says otherwise: production's gunicorn master is a
child of `bash ~/python/woa23/conf/start_app.sh`, itself under
`pm2-odbadmin.service`. **Anything about how PM2 starts, restarts or supervises the
service is out of scope here** — including whether `WOA23_ZARR_STORE` would even
reach the process through that path, which is exactly the kind of question the later
spec exists for.

D1 and D2 below are therefore about the *application's* behaviour, not the
deployment's.

## 6. Three properties, three different tests

These are routinely conflated and the distinction is the reason S1 took as long as it
did.

| property | question | test | what it cannot see |
|---|---|---|---|
| **byte-exactness** (C1) | do the two arms emit identical bytes? | 5.2A, seed pinned, one environment | nothing — it is the strongest form, but it needs a pinned seed, so it cannot be applied to a process we may not restart |
| **semantic equivalence** (C2) | do they emit the same *content*? | 5.2B, seed unpinned | column order, key order, float formatting |
| **order stability** | does one arm emit the same order **twice**? | the same arm, repeated independent starts, compared to itself | nothing about the other arm — it is a property of one implementation, not of the pair |

### 6.1 Order-stability acceptance

Measured per arm, per case, across C2's three cycles. Two forms, and the second is
used only where the first cannot be:

| form | comparison | when |
|---|---|---|
| **raw byte** | the arm's response body from cycle *n* compared byte for byte with cycle 1 | the default. It is the strongest and needs nothing parsed |
| **order fingerprint** | SHA-256 over the ordered column-name list, followed by the ordered sequence of `(lon, lat, depth, time_period)` index tuples | when the bodies legitimately differ in something that is not order — a timestamp, a float rendered differently — so a raw comparison would report a difference that is not an ordering one |

The fingerprint deliberately excludes values: it answers "was the same content laid
out in the same sequence", not "was the same content returned". The latter is C2's
question and is already answered semantically.

**Per case, per arm, the outcome is:**

| | |
|---|---|
| all three cycles identical (bytes, or fingerprint) | `STABLE` |
| any cycle differs | `UNSTABLE`, recorded with the first differing cycle and whether it was column order, row order, or both |
| C2 returned `INSUFFICIENT` | order stability is `UNKNOWN` — three cycles that happened to share a seed cannot show order varying with the seed |

### 6.2 Order stability does **not** decide C2

They are separate verdicts and conflating them would make each less useful:

- **C2 is 5.2B semantic and is order-insensitive by construction.** An `UNSTABLE`
  case can and should still pass C2: the content is the same, the sequence is not.
  Failing C2 on it would be measuring the thing 5.2B explicitly does not measure.
- **`UNSTABLE` is reported as a finding in its own right**, because it is one. S1
  established that this candidate's output order depends on the *string* used to
  reach the store, which means order is a function of configuration, not only of
  code. A consumer indexing CSV columns by position, or a GeoServer SQL view bound to
  a column sequence, is exposed to that whether or not the two arms agree.
- **An `UNSTABLE` result blocks nothing in S2 and decides nothing about deployment.**
  What to do about it belongs to the deployment and consumer-compatibility work, not
  here. S2's obligation is to find it and say so.

A case that is `STABLE` on the reference and `UNSTABLE` on the candidate — or the
reverse — is the most interesting outcome available from C2 and must be reported
prominently rather than averaged into a pass.

Order stability is neither of the other two and S1 never measured it. It matters
because S1 established that the candidate's output order depends on the *string* used
to reach the store, which means order is a function of configuration and not only of
code. A consumer that relies on column order — a CSV reader indexing by position, a
GeoServer SQL view — is exposed to that whether or not the two arms agree with each
other.

**C2's repeated cycles give order stability almost for free**: the same arm's
responses across cycles can be compared to each other as well as to the other arm.
Recording it costs nothing and answers a question no gate currently asks.

## 7. Deployment acceptance

Only the two that can be established in isolation. Everything requiring a live
service moved to the observation spec.

### D1 — startup failure modes, measured across three stages

Rev 1–5 asserted the candidate "fails loudly". Measured 2026-08-09 in the isolated
staging against the package clone, with no socket bound and no request sent, that is
true of **one** of six fixtures.

Three stages are distinguished, because a failure at each has a different operational
meaning:

| stage | what it means | how it was exercised |
|---|---|---|
| **import** | the module cannot load; nothing can start | `python -S -c "import api.app"` |
| **startup** | the app object cannot be built; the server exits before serving | `gunicorn api.app:app --check-config` — loads the app, binds nothing |
| **first data request** | the server is up and answers with an error | the query coroutine called in-process, no socket, no HTTP |

**Readiness** is a fourth stage and is **not established**: it needs a server to be
listening, which no authorisation covers. Whether the OpenAPI endpoint would answer
200 while the store is unreadable is therefore an open question, not a finding.

The fixture set is **six negative fixtures and one real-store control**. The control
is not decoration: without it, "every fixture failed" is equally consistent with the
harness being broken, and six identical `FileNotFoundError`s would look like six
findings rather than one behaviour plus a harness that cannot read anything.

**The six negative fixtures**, in increasing order of how much of a store is present:

| # | negative fixture | what is at the path | import | startup | first data request |
|---|---|---|---|---|---|
| N1 | `WOA23_ZARR_STORE` **unset** | nothing is configured at all | **exit 1** `KeyError` | **exit 1** | not reached |
| N2 | path does not exist | nothing | exit 0 | exit 0 | `FileNotFoundError` on the group path |
| N3 | **existing but empty** directory | a directory, no contents | exit 0 | exit 0 | `FileNotFoundError` on the group path |
| N4 | a **regular file** where the store should be | a file, not a directory | exit 0 | exit 0 | `FileNotFoundError` on the group path |
| N5 | group path present, `.zgroup` **not JSON** | the right shape, unparseable metadata | exit 0 | exit 0 | **`JSONDecodeError`**: `Expecting value: line 1 column 1 (char 0)` |
| N6 | group path present, `.zgroup` valid JSON, `zarr_format: 99` | the right shape, a format that does not exist | exit 0 | exit 0 | **`MetadataError`**: `unsupported zarr format: 99` |

**The control:**

| # | control fixture | import | startup | first data request |
|---|---|---|---|---|
| P1 | the real production store | exit 0 | exit 0 | **returns data** |

`N1` is the only fixture of the seven that fails before serving. `N2`–`N4` are
indistinguishable from each other at every stage. `N5` and `N6` are the only ones
that reach Zarr's own parsing, and they are the only two that could be called
"malformed store" without over-claiming.

#### What the errors do and do not distinguish

**A missing group path is not proof that the target is not a Zarr store.** Rev 6 said
it was, from a fixture that was simply an unrelated directory. Three different
conditions — a path that does not exist, a directory that is empty, and a *regular
file* — all produce the identical `FileNotFoundError` naming
`<store>/1_degree/annual/TS`. The error reports that the expected subpath is absent.
It says nothing about what, if anything, is at the store root.

A target that genuinely **is** malformed Zarr produces a different class of error
entirely, and neither is caught:

- `JSONDecodeError: Expecting value: line 1 column 1 (char 0)` — **names no file and
  no path.** From a log, this does not identify the store, the group, or even that
  the failure is store-related;
- `MetadataError: unsupported zarr format: 99` — names the problem but not the file.

**Only the unset variable fails before serving.** Every other misconfiguration
produces a process that imports, starts, passes a check that asks only whether it came
up, and then fails on every data request — one of them with a message that does not
mention the store at all.

**No change to the candidate is proposed here.** A startup-time store validation would
be a change to `api/config.py` or `api/app.py`, and that is a **separate candidate
change requiring its own spec and approval**. S2's obligation is to establish the
behaviour; changing it is not S2's to decide, and the behaviour is stated above so the
decision can be made on evidence.

#### D1 stays open, and this is its standing summary

| | established |
|---|---|
| a **missing** `WOA23_ZARR_STORE` | fails at **import**, and again at startup — exit 1, before anything serves |
| an **invalid or non-Zarr** store (N2–N6) | **starts successfully** and fails on the **first data request** |
| **readiness** under those conditions | **not established** — needs a listening server, which nothing authorises |

Nothing in C1 or C2 changes any of this, and neither run is permitted to. The
candidate is unmodified — `api/` is byte-identical to `origin/main` — and any
startup-time validation remains a separate candidate-change decision. D1 is recorded
as **open**, not closed by the S2 runs.

#### D1a — the reference behaves differently, and the difference is structural

| | reference | candidate |
|---|---|---|
| where the store comes from | `woa23_app.py:63`, `zarr_store_path = "data/"`, hard-coded | `api/config.py:31`, `os.environ["WOA23_ZARR_STORE"]`, mandatory |
| a missing store at startup | import succeeds; `--check-config` exit 0 | **unset variable**: import fails, exit 1 |
| effect of `WOA23_ZARR_STORE` | **none** — set to `/completely/ignored`, `woa23_app.zarr_store_path` is still `'data/'` | it *is* the store |
| what "not configured" looks like | a relative path that resolves against whatever the cwd happens to be | an absent variable, which is loud |

The two are not the same failure surface and cannot be compared as though they were.
The reference cannot be misconfigured by the environment because it ignores it; it can
be misconfigured by being started in the wrong directory, and nothing detects that at
startup either.

**And `WOA23_ZARR_STORE` is not in production's environment.** Read from
`/proc/3960/environ` on 2026-08-09: production's gunicorn master has no `WOA23_*`,
`PYTHON*`, `POLARS_*`, `DASK_*` or `OMP_*` variable at all. The candidate requires one
that production does not currently set, and **whether PM2 would pass it through is
unknown and is the deployment spec's question**, not this one's.

### D2 — process readiness and store readiness are different signals

These are two different things, the first draft of this spec ran them together, and
D1 is what shows why that mattered.

| | **process readiness** | **store readiness** |
|---|---|---|
| what it probes | the OpenAPI document | one real query per arm, counterbalanced |
| reads the Zarr store | **no** | **yes** |
| the question it answers | is the process up, routed and serving? | can this process actually read its store? |
| what a green signal licenses | starting the gate | nothing beyond "the store opened for this query" |
| what a green signal does **not** license | any statement about the store | any statement about the whole store, or about latency |

**A 200 on the OpenAPI document says the process is serving and says nothing about
the store.** D1 measured this from the other end: fixtures N2, N3 and N4 — a
nonexistent path, an empty directory, and an ordinary file — all import cleanly, pass
`gunicorn --check-config`, and fail only when a request reaches the data path. A
process in any of those three states is one whose OpenAPI endpoint has every reason
to answer 200.

That last step is an **inference, not a measurement**. D1 bound no socket and sent no
HTTP, so what the OpenAPI endpoint actually returns under N2–N4 has not been
observed. Establishing it needs a listening server, which is the fourth stage below
and is not authorised. The inference is strong enough to design against and not
strong enough to report as a finding.

#### What C1 and C2 may therefore do with readiness

**Process readiness may be used as a startup precondition, and only as that.** The
runner waits for the OpenAPI document on each arm before proceeding, because a gate
that starts before the workers have forked measures the wrong thing. Two constraints
follow and both are implemented:

1. **Readiness must not read the store.** S1 found that a readiness probe issuing a
   real query warmed one arm's page cache and store handle before the other had
   served anything — on the exact path the experiment measures. The OpenAPI document
   touches no chunk.
2. **Nothing between the readiness check and the data probe may be described as the
   store being ready.** The runner's function is named `process_ready`, its comment
   says what it does not establish, and its success line says the store has not been
   touched. Store readiness is established by the symmetric data probe and nowhere
   else; under `--cleanup-only`, which skips the probe, the run states that store
   readiness was **not** established rather than leaving it implied.

**Readiness as a fourth D1 stage — whether the signal turns green while the store is
unreadable — remains unestablished** and stays out of scope until a spec authorises a
listening server. It is what a real deployment's health check would need to answer,
and it is the deployment spec's question.

## 7a. The runner, as implemented

Written and tested offline on 2026-08-09. **Nothing below has been executed against
VM24**; the whole of it is argument handling, launch construction and verification,
covered by 129 offline CLI assertions that all stop at validation or at the
authorisation gate, plus 121 Python assertions over the new pure logic.

### 7a.1 Modes and their authorisation

Four modes, exactly one per invocation, counted rather than compared pairwise — four
flags make six pairs and the version that enumerated them missed three.

| mode | environment | seed | gate | grant |
|---|---|---|---|---|
| default | `dev2026/.venv` | `PYTHONHASHSEED=0` | 5.2A + latency + pilot | `WOA23_D2B_GRANTED` |
| `--contract-only` | `dev2026/.venv` | `PYTHONHASHSEED=0` | 5.2A only | `WOA23_D2B_GRANTED` |
| `--cleanup-only` | `dev2026/.venv` | `PYTHONHASHSEED=0` | none | `WOA23_D2B_GRANTED` |
| **`--c1`** | **package clone** | `PYTHONHASHSEED=0` | **5.2A only** | **`WOA23_S2_C1_GRANTED`** |
| **`--c2-cycle`** | **package clone** | **unset** | **5.2B only** | **`WOA23_S2_C2_GRANTED`** |

**No grant implies another, in either direction.** `WOA23_D2B_GRANTED=yes` on a
`--c1` invocation is exit 3 with a message saying so explicitly; a C1 grant present
during a D2b run is also exit 3, so a leftover `export` cannot widen what was
granted. C1 and C2 refuse each other's grants as well.

**Every argument and authorisation check completes before anything is created.** The
order is: parse → mode exclusivity → S2 argument presence and shape → port validation
→ workdir and clone boundary → **print the resolved configuration and the request
budget** → authorisation → host and prerequisites → staging → processes → HTTP. At
the authorisation gate nothing has been written, no port has been touched and no
request has been sent. A malformed argument still outranks a missing grant, so a
refusal names the wrong value rather than the missing variable.

### 7a.2 The arms run from the clone, and there is no fallback

`--python-binary`, `--package-clone` and `--clone-manifest` are **required** by both
S2 modes, must be absolute, and are **refused outside them** — a flag that is
accepted but ignored reads as a flag that took effect.

**There is deliberately no default and no fallback to `dev2026/.venv`.** An S2 run
that reached for the campaign's own venv because a flag was missing would produce a
D2b result wearing a C1 label, and it would pass. The refusal says this in words.

The launch, spelled out in the runner rather than assembled from a variable:

```
env -C <arm dir> -u VIRTUAL_ENV -u PYTHONHOME [-u PYTHONHASHSEED | PYTHONHASHSEED=0] \
    PYTHONPATH=<clone> PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
    <production binary> -S -m gunicorn <app> -w <n> -k uvicorn.workers.UvicornWorker …
```

- `VIRTUAL_ENV` and `PYTHONHOME` are **unset**, not overridden. Either inherited from
  the invoking shell would redirect the interpreter's idea of where its packages
  live, and `VIRTUAL_ENV` is exactly what a leftover `source .venv/bin/activate`
  leaves behind.
- C2 uses `-u PYTHONHASHSEED` rather than an empty value: CPython **rejects**
  `PYTHONHASHSEED=""`, so setting it empty would not mean "unset", it would mean the
  interpreter refuses to start — and the arm would fail for a reason that looks
  nothing like the one it had.
- The clone has no `bin/`, so the Dask scheduler and worker are reached as
  `-m distributed.cli.dask_scheduler` / `-m distributed.cli.dask_worker`, the same
  entry points the console scripts call.

The clone is subject to the **same boundary check as the workdir**, resolved the same
way through symlinks: a `--package-clone` inside `~/python/woa23`, inside production's
`site-packages`, or inside the workdir is refused. Pointing at the original would put
production's live packages — and `__editable__.src-1.0.pth`, which names
`~/python/woa23/src` — on the arms' path, and it would be reported as isolation. The
clone is also required to be **non-writable**: a writable clone is not the artefact
that was built and verified.

### 7a.3 What the provenance now has to establish

Three artefacts per S2 arm, and they establish different things:

| artefact | what it is | what it establishes | what it cannot |
|---|---|---|---|
| `<label>_environment.json` | the clone listed **under the arms' own launch** — `-S`, clone on `PYTHONPATH`, no `VIRTUAL_ENV` | which distributions the arms can see, anchored to the clone manifest's digest | that any particular process loaded them |
| `<label>_meta_<arm>.json` | `/proc` provenance of the running arm | argv, cwd, whitelisted environment, store literal, master and worker PIDs | in-process `sys.path` |
| `<label>_interp_<arm>.json` | the interpreter probe **and** `/proc/<pid>/maps` of every tracked arm process | see below | see below |

Two changes were needed to make the first two honest under S2:

- **`--env-python` is now mandatory for the S2 collections.** The arm's `argv[0]` is
  production's binary, whose sibling `python` *is* production's environment: the
  existing `argv0_sibling` heuristic would have listed production's 236 distributions
  and labelled them the clone's. A confident wrong answer is the failure this
  function already exists to have fixed once.
- **The listing reproduces the launch.** The same binary lists production's packages
  when run plainly and the clone's when run as the arm is started, so
  `dependencies()` now takes the interpreter arguments and environment, and records
  both alongside the answer so it cannot be read without its question.
- **The environment anchor is the clone manifest, not a lockfile.** There is no
  lockfile; `S2_ENVIRONMENT_RECORD_FIELDS` puts `package_manifest_sha256` in exactly
  the position `lockfile_sha256` holds under D2b, compared with the same strictness.

**The interpreter probe and `/proc/<pid>/maps` are separate evidence, they establish
different kinds of thing, and neither is worker-level import provenance.**

- The probe is an **identically-launched sibling interpreter**: same binary, flags,
  `PYTHONPATH`, cwd and environment, reporting `sys.executable`, `sys.prefix`,
  `sys.base_prefix`, the full `sys.flags`, the whole ordered `sys.path` with empty
  entries expanded against the cwd, and `__file__` for fourteen named modules.

  **What it establishes is the launch environment and import configuration**: that a
  process started this way resolves these modules to these files. It is a sibling
  process, not the gunicorn worker, and it cannot say what the worker imported.

- `/proc/<pid>/maps`, read for every process in each arm's tracked tree, is **a
  refuter, not a verifier**. A production path appearing in it is direct evidence
  that the running process loaded a file from production, and that refutes isolation
  outright. Its *absence* proves nothing correspondingly strong: `maps` lists mapped
  files, not imports. No entry in it names a module, a `sys.path` or an import, and
  a pure-Python module read from the wrong directory leaves no trace at all.

#### The C1 limitation this leaves, stated rather than papered over

**Neither source observes `sys.path` or `sys.modules` inside a gunicorn worker, and
no such mechanism exists in this harness.** The candidate may not be modified, and
the only external route — a gunicorn `-c` config with a `post_fork` hook reporting
from inside each worker — **changes the arms' launch line** and therefore needs its
own decision before it is used. It is not part of the C1 request, and nothing here
should be read as if it were already in place.

So the honest statement of what a C1 pass establishes about imports is:

> Processes launched by this procedure resolve the fourteen named modules to files
> inside the read-only clone; every `sys.path` entry of such a process is inside the
> clone, the staging tree or the interpreter's own stdlib, in both its absolute and
> its resolved form; and no process in either arm's tracked tree mapped a single file
> from production.

and what it does **not** establish:

> that the gunicorn workers which actually served the 64 contract responses imported
> those modules from the clone. That is an inference from the launch configuration
> plus the absence of production paths in `maps` — a strong one, and an inference.

This limitation is carried in the output, not only here: every interpreter record
and the C2 summary contain a `worker_provenance_limitation` field saying it, and it
is printed at the end of every S2 run.

Both fail closed. Every `sys.path` entry and every module file is checked in **both**
its absolute and its resolved form — a clone path that is a symlink into production
passes the first and fails the second — and an entry under *no* allowed root is a
problem as well as an entry under a forbidden one. That second half is what catches
the path nobody thought to forbid. Allowed and forbidden roots are each expanded to
include their own `realpath`, because production's site-packages is reached as
`…/versions/py311/…` and *is* `…/versions/3.11.4/envs/py311/…`; a root recorded in
one form would not match a path resolved in the other. Containment is tested on
component boundaries, so `…/woa23-staging` is not inside `…/woa23`.

A run whose probe reports `sys.flags.ignore_environment` is refused outright (§4.2.0a),
a missing `-S` is refused for the C1 procedure, and a probe that could write bytecode
is refused because the clone must stay byte-identical.

**The `-S` limitation is carried into the output, not just the spec.** It appears in
the environment record, in every interpreter record, in the C2 summary, and on stdout
at the end of every S2 run.

### 7a.3b The clone manifest, identified by content and verified — 2026-08-09

`~/woa23-s2-package-clone/` holds **seven** artefacts, two of which are manifests.
Choosing by name or by glob order would have been a coin toss, so they were read:

| file | size | what it is |
|---|---|---|
| **`clone.manifest`** | 4,529,883 | **33,565 lines, one per file in `dist/`.** This is the clone manifest. |
| `source.manifest` | 4,530,117 | 33,567 lines — production's site-packages, the *source* |
| `SHA256SUMS` | 421 | digests of five artefacts; **not** a manifest |
| `clone.provenance` | 1,234 | how the clone was built |
| `{source,clone}.filename-audit` | 2,832 each | the filename audit of §4.1.5 |
| `dist/` | — | the tree itself |

The distinguishing evidence is not the names: `source.manifest`'s first two lines are
`__editable__.src-1.0.pth` and `__editable___src_1_0_finder.py`, and `set` difference
confirms those **two entries, and only those two**, are what `clone.manifest` lacks —
which is exactly the exclusion `clone.provenance` records.

```
--clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest
```

| | |
|---|---|
| sha256 | `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4` |
| size / mode / owner | 4,529,883 B / `-r--r--r--` (444) / `odbadmin:odbadmin` |
| mtime | 2026-08-09 01:31:13.567923779 +0800 |
| agrees with | `SHA256SUMS` **and** `clone.provenance: clone_manifest_sha256` |

**The manifest was verified against the tree, not assumed to match it.** All 33,565
files were re-hashed from `dist/` (1,690,025,002 bytes, 6.9 s, `nice -n 19`):

| check | result |
|---|---|
| manifest entries vs files on disk | 33,565 / 33,565 |
| in the manifest, missing from disk | **0** |
| on disk, absent from the manifest | **0** |
| content digest mismatches | **0** |
| size mismatches | **0** |
| `mtime_ns` mismatches | **0** |
| unreadable | **0** |

Both digests were recomputed from inside the clone under §4.1.2b's current
dist-info-keyed definition, and both reproduce §4.1's recorded values exactly: 240
dist-info directories → `b8754d32…`, 236 with `METADATA` → `a26ca6c3…`.

**One discrepancy, reported rather than reconciled.** `clone.provenance` records
`package_tree_digest: a796…` and `runtime_distribution_digest: 60236d…` — the
**name-keyed values superseded in revision 6**. The file was written when the clone
was built, before the definition changed, and it has not been rewritten, which is
correct: the clone and its provenance are immutable. So the artefact on disk carries
the old definition's numbers, this spec carries the current ones, and `a796…` is
**not** the package-tree digest. Both are recorded so earlier reports stay traceable.

**Read-only status, as measured rather than as intended:**

| path | mode | writable by `odbadmin` |
|---|---|---|
| `dist/` | `dr-xr-xr-x` (555) | **no** |
| `clone.manifest`, `SHA256SUMS`, `clone.provenance` | `-r--r--r--` (444) | **no** |
| `~/woa23-s2-package-clone/` (the parent) | `drwxrwxr-x` (775) | **YES** |

The tree's *contents* are protected: nothing inside `dist/` can be added, removed or
modified, and no `.pyc` has appeared since it was built (0 files newer than the build
time). The **parent directory is writable**, which is a weaker position than it
looks — unlinking a file needs write permission on its *directory*, not on the file —
so the four mode-444 artefacts could be replaced, and `dist` could be renamed, by
this user. Neither run does any of that, and the runner's own check (`[ -w
"$PKG_CLONE" ]`, which tests `dist`) is the right test for what it guards. Recorded
because "read-only" without saying read-only *against what* is the kind of claim
that goes stale silently.

### 7a.4 C2: three cycles, and the three statements they support

One `--c2-cycle` is one cycle and **never draws the conclusion** — one process has one
seed, and one seed is not a distribution. `scripts/run_c2_cycles.sh` runs exactly
three, each in its own workdir, each fully cleaned up and verified before the next
begins, and `bench/c2_summary.py` produces:

1. **The 5.2B verdict.** PASS only if every cycle passed. Two passes and a failure is
   not two thirds of an answer, and the driver stops at the first failing cycle.

   The verdict and the seed observation are then combined into **one named outcome**,
   because reporting them side by side invites the summary "it passed":

   | three semantic gates | seed diversity | outcome | exit |
   |---|---|---|---|
   | PASS | `OBSERVED` | `PASS` | 0 |
   | PASS | `INSUFFICIENT` | **`PASS_WITH_INSUFFICIENT_SEED_DIVERSITY`** | **5** |
   | any FAIL | not consulted | `FAIL` | 1 |

   The middle row is not exit 0: a caller checking only the status would read that as
   a plain pass, which is the misreport the outcome exists to prevent. It is not exit
   1 either — the candidate did not fail — and it is **not a trigger for a fourth
   cycle**. There is no fourth cycle and none may be added without a new
   authorisation.
2. **Seed diversity — an observation, never an escalation.**

   **`PYTHONHASHSEED` unset and `hash_randomization=1` are preconditions, not
   evidence.** Together they say only that the interpreter was *permitted* to choose
   a seed per process. They are exactly as true of three starts that happened to
   choose the same seed, and of a configuration in which the choice is degenerate.
   Reporting diversity from them would be reporting the arrangement instead of the
   result.

   The measurement is therefore separate: **each cycle runs `hash()` over a fixed
   eleven-string tuple, using the same binary with the same `-S`, `PYTHONPATH`, cwd
   and environment the arm was launched with**, and the digest of those values is
   what is compared across cycles. The preconditions are checked as well — a cycle
   that reports a set `PYTHONHASHSEED`, `hash_randomization != 1`, or an empty probe
   tuple makes the result `INSUFFICIENT` *even if the three digests differ*, because
   variation under a broken precondition cannot be attributed to the arrangement
   being tested.

   Three distinct digests with clean preconditions is `OBSERVED`; anything else is
   **`INSUFFICIENT`**, which is *not* a failure of the candidate and *not* a reason
   to run a fourth cycle. There is no `--cycles` flag, and passing one is an error
   naming the reason: a run that could choose its own number could keep going until
   the observation came out a particular way. `INSUFFICIENT` does not affect the exit
   status, so nothing pushes towards a fourth.

   **Limitation, carried with the number and labelled on it:** this is
   **sibling / launch-environment seed diversity**. The seed measured is that of a
   sibling interpreter launched by the same procedure, **not** of the gunicorn master
   or of any worker that served a request. Measuring it inside those needs the same
   worker observation mechanism §7a.3 says does not exist.
3. **Order stability — recorded, and deliberately not part of pass/fail.** Every case
   now carries an order fingerprint per arm: the raw body digest, a **row-order
   digest over the row keys only with values excluded**, and the column sequence. The
   row-order digest is what isolates order — a changed *value* moves the body digest
   and leaves it alone — so a cross-cycle value difference is not reported as an
   ordering change. Without a pinned seed, two cycles ordering rows differently is the
   expected consequence of what C2 observes, not a defect. Unlike the seed digests,
   this comes from the processes that actually answered.

### 7a.5 Request budgets, stated as ceilings before anything is sent

| mode | per arm | both arms | production 8050 |
|---|---|---|---|
| `--cleanup-only` | ≤ 30 | ≤ 60 | **0** |
| `--contract-only`, `--c1`, one `--c2-cycle` | ≤ 96 | ≤ 192 | **0** |
| default (D2b full) | ≤ 480 | ≤ 960 | **0** |
| **C2, all three cycles** | **≤ 288** | **≤ 576** | **0** |

96 = 30 readiness (worst case; normally 1–3) + 2 data probe + 64 contract. Production
is zero by construction: nothing in the script addresses port 8050, which is read from
`/proc` and `ss` only.

**The process count is derived, never written down.** Two Dask processes plus, per
arm, a gunicorn arbiter and the workers it forks: `2 + 2 × (1 + workers)`. One worker
gives 6, which is D2b's and C1's figure. Production's measured `-w 2` gives **8 — a
consequence of that measurement and not a fact about the system**. A reconfiguration
to four workers makes it twelve, and a count that did not move with it would be
verifying last week's deployment. The runner prints the arithmetic with the number,
and three ways in are closed: a worker count that cannot be read from production's
argv aborts rather than being assumed, a count outside 1–16 aborts, and a `--workers N`
supplied by the authorisation aborts the run if production disagrees with it.

**Cleanup budget for C2**: three complete start/stop cycles, each stopping four
services and verifying every process in its recorded tree, its ports, its boot ID and
its state files. The driver refuses to begin a cycle if the previous one left any run
state behind, and says that leftover state must be inspected rather than removed to
make the next cycle run.

## 8. Isolation requirements

Inherited from S1 and non-negotiable:

- a **new** staging directory per attempt; `~/python/woa23` is never a working
  directory and is never written;
- the reference is a **read-only** copy of production's source, its SHA-256 checked
  against the original before anything starts;
- the store is reached through a **read-only symlink**; it is never copied;
- all ports **loopback only**, and 8050, 8786 and 8787 are refused by the runner;
- **0 requests to production 8050**, no connection to the shared Dask scheduler;
- the process tree is recorded and the count enforced before any gate runs;
- cleanup must confirm itself — every tracked process exited, ports released, state
  removed, and production's listener set, master PID, start time and boot id
  unchanged. **A run whose cleanup cannot confirm itself is a failed run whatever its
  gates said.**

## 9. What a C1 pass would and would not license

**Would:** that the candidate and the unmodified reference produce byte-identical
responses across the 64 cases when both run on production's package set with a pinned
seed.

**Would not:**

- that production's *running process* behaves the same — same packages, different
  process, different uptime, different memory state;
- anything about latency, throughput or resource use;
- anything about nginx, TLS or the public path;
- anything about behaviour under concurrent load beyond what C2 examines;
- **that the workers which served those responses imported from the clone.** That is
  an inference from the launch configuration and the absence of production paths in
  `maps`; no worker-level Python provenance mechanism exists — §7a.3;
- **that the deployment is ready.** A contract gate compares two processes this
  harness started, in a staging directory, under `-S`, with a store symlink, launched
  by a shell script. It is not a deployment and a pass is not readiness. Readiness in
  the operational sense — that a health check turns green only when the service can
  serve, and does not turn green when the store is unreadable — is **D2's fourth
  stage and is not established** (§7 D2); the launcher is not production's PM2 path
  (§5); and the site/`.pth` startup semantics were never exercised (§4.3.1). **"C1
  PASS" must never be written as "ready to deploy" or "deployment validated."**
- that deployment is safe. That is the observation and rollback spec's question, and
  it needs a canary, not a contract gate.

## 9a. If a dependency is missing — what happens, and what it would cost

C1's premise is that the clone **is** production's package set. Installing into it, or
building a fresh environment with `uv`, would replace the thing under test with
something else and the result would answer a different question.

So nothing is installed. If a smoke, D1 or C1 prerequisite fails on a missing import,
the run **stops and reports**, and the report carries:

- the import that failed and the package it belongs to;
- the full traceback;
- whether the production package tree is still intact — file count, both digests, and
  whether the same import fails against production's own site-packages as well as
  against the clone. If it fails against both, the gap is production's; if only
  against the clone, the clone is wrong and must be rebuilt, not patched;
- a proposed `uv` command, the exact versions, and a **new** venv path — never the
  clone, never production;
- **what the variant becomes.** A run against a uv-built environment is not
  "C1 with a fix". It is a different test: *the candidate under a resolved dependency
  set*, which does not carry production's transitive pins and therefore cannot speak
  to production's behaviour. It would need its own name, its own acceptance, and its
  own authorisation.

**`uv` is not used unless a real import or dependency failure occurs.** No such
failure has occurred: both arms imported every module in §4.2.2 from the clone on
2026-08-09. Nothing is installed into the clone or into production under any
circumstances — if the clone is wrong it is rebuilt from the source, never patched.

## 10. Open questions for the PI

Rev 1's first three questions are settled and recorded in §4, §5 and the revision
history. What remains:

1. **Does cloning production's `site-packages` need its own authorisation?** It is a
   bulk read of production followed by a write into a staging tree. Every read of
   production so far has been a handful of files; this is the whole environment, and
   it is the largest read S2 asks for. It writes nothing to production, but the size
   of the read is itself worth granting explicitly rather than assuming.
2. **Executing production's Python binary** — the PI's preference for C1, recorded in
   §4.1.6. Every authorisation so far has been to *read* production; running its
   interpreter is a different act and needs granting on its own. If it is not
   granted, the interpreter difference stands as a stated limitation and C1 still
   runs.
3. **Production has four abandoned install stubs and two double-installed packages**
   (§4.1.2a). Neither affects the read path and the clone reproduces both faithfully,
   so nothing here blocks S2. They are production hygiene findings and belong to
   whoever owns that environment — raised, not acted on.
4. **What happens if C1 fails?** A byte difference under production's packages would
   mean one of those 236 packages changes the output. S2 would then have found
   something real, and the next step — bisecting the package set — is not in this
   spec and would need its own.

## 11. What is not yet decided, and is not assumed

- The staging directory name, ports and workdir for any S2 run. They will be proposed
  with the run request, not fixed here.
- Whether C1 and C2 run as one invocation or two.
- Whether the existing runner can host these gates or needs a mode of its own. It has
  `--contract-only` and `--cleanup-only` today; C2's unpinned seed conflicts with the
  runner's current insistence on a pinned seed for both arms under 5.2A, so at minimum
  that interaction needs designing.

**No implementation has begun and no VM24 process has been started for this spec.**
