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
`METADATA=0`; **they are also the only four with `RECORD=0`**, which is consistent
with an interrupted install and is recorded as evidence rather than inferred.

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
without reading any content. **Withdrawn.** What can be said is what the table says:
these paths sit inside library test directories and vendored CA bundles, which is
where files with those names normally live — and no file was opened to confirm it.

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

### D1 — startup failure modes, measured

Rev 1–5 asserted that the candidate "fails loudly". Measured on 2026-08-09 in the
isolated staging, against the package clone, with no socket bound and no request
sent, that is true of **one** of the three cases and false of the other two.

| case | `import api.app` | `gunicorn --check-config` | at request time |
|---|---|---|---|
| `WOA23_ZARR_STORE` **unset** | **fails**: `KeyError: 'WOA23_ZARR_STORE'` at `api/config.py:31` | **exit 1** | never reached |
| set to a path that **does not exist** | succeeds | **exit 0** | `FileNotFoundError: No such file or directory: '/no/such/store/1_degree/annual/TS'` |
| set to a directory that is **not a Zarr store** | succeeds | **exit 0** | `FileNotFoundError: …/not_a_store/1_degree/annual/TS'` |

**Only the unset case is a startup failure.** The other two produce a process that
starts cleanly, passes any check that only asks "did it come up", and then fails on
every data request with an unhandled `FileNotFoundError` — which FastAPI renders as a
500, and which a proxy in front of it may render as a 502. That is precisely the
outcome D1 exists to rule out, and it is not currently ruled out.

The `FileNotFoundError` names the resolved group path, so it is not silent in the log.
What it is not is a *startup* failure: a deployment that checks only whether the
process is running will call this healthy.

**No fix is proposed here.** Whether the candidate should validate the store at import
or at readiness — and whether that validation belongs in the application at all — is a
design decision, and S2's job is to establish the behaviour, not change it.

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

### D2 — readiness is not a data-path check

These are two different things and the first draft of this spec ran them together:

| | probes | reads the store | purpose |
|---|---|---|---|
| **readiness** | the OpenAPI document | **no** | is the process up and routing? |
| **data-path smoke** | one real query per arm, counterbalanced | **yes** | can it actually read the store? |

Readiness must not read the store: S1 found that a readiness probe issuing a real
query warmed one arm's page cache and store handle before the other had served
anything. The data-path smoke is a separate, deliberately symmetric step.

Establish that the readiness signal is meaningful for the deployment form — that it
turns green only once the worker can serve, and that it does not turn green when the
store is unreadable while the process is otherwise healthy.

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

No such failure has occurred. Both arms imported every module in §4.2.2 from the
clone on 2026-08-09.

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
