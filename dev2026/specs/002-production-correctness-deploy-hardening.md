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
| 17 | 2026-08-10 | **C2 re-executed on the patched candidate and PASSED — `C2 PASS — isolated package-tree semantic correctness after D1 patch at production worker count, with observed sibling seed diversity`** — §7a.5c. Commit `249aa274`, archive `d77b275f…` verified on arrival, 87 files; `api/` byte-identical to the C1-tested `919095e8`. New staging, workdir, label `c2f` and **first-use ports 18131/18132/18859**. Prerequisites read from the host **before any service started**: `STOP_WAIT_SECS=20` source `default`, `ARM_GRACEFUL_TIMEOUT=10`, production's worker count **measured = 2**, `both-unpinned`, `clone.manifest`. **Three cycles, 5.2B, 64/64 MATCH each, all PASS**; **seed diversity OBSERVED**, six distinct digests; **shutdown budget CONSISTENT** in all three cycles, the arms' `--graceful-timeout 10` read back from each arm's own `/proc/<pid>/cmdline`; **9/9 clone integrity**, 33,565 entries and 1,690,025,002 bytes each; **cleanup PASS ×3** with zero blocking state; **8050/8786/8787 zero requests**, production unchanged throughout. **Requests: 396 recorded as actually issued**, plus an unrecorded 1–30 per arm per cycle for process readiness — 402–576 total against a 576 ceiling. **Order-stability correction:** `c2f` and `c2c` involve the **same two cases**, `C16` and `C16-csv`, but did **not** behave identically — `c2c` varied on both arms (4 pairs), `c2f` on the candidate only (2 pairs); the difference is recorded and **not** explained or attributed. Two attempts preceding it are kept as evidence and not backfilled: `c2d` **cleanup FAIL** (arms inherited gunicorn's 30 s default while the wait was 20, plus a logging reentrancy race) and `c2e` **INVALID_PRE_START** (`--clone-manifest` pointed at the clone root's `SHA256SUMS`, a digest list; stopped at preflight, nothing started). Evidence archived read-only outside any deploy directory at `~/woa23-s2-archive/2026-08-10-c2f-PASS/`, **50 evidence files plus a separate `SHA256SUMS`** manifest of those 50, each evidence file hashed at source and destination and compared, `SHA256SUMS` `ffa68f58…`, originals untouched. Limitations retained unchanged: `-S` ran no `site.py` or `.pth`, no worker-level import provenance, seed diversity is **sibling / launch-environment level and not worker level**, the zero-chunk anchor read is **offline-audited and implementation-supported, not observed on this host**, the launcher is not PM2, and **D1 real-store depth characterization, the row-order decision, formal deployment validation and all performance validation remain open**. |
| 16 | 2026-08-09 | **C2 executed and PASSED — `C2 PASS — isolated package-tree semantic correctness at production worker count, with observed sibling seed diversity`** — §7a.5b. Commit `5cfbf0aa`, archive verified against its digest before shipping, 78/78 files. Production's worker count **measured at run time in every cycle: actual = 2**, `--expected-workers 2` an assertion only, production's identity re-checked after each measurement; **8 processes per cycle**, derived. **Three independent cycles, 5.2B semantic, 64/64 each, all PASS**, under `both-unpinned` with `PYTHONHASHSEED` unset on both arms. **Seed diversity OBSERVED** — three distinct digests, no precondition problems; the `PASS_WITH_INSUFFICIENT_SEED_DIVERSITY` branch was not taken. **Nine clone-integrity verifications, 9/9 MATCH**; **cleanup PASS in all three cycles** with zero blocking state left; **8050/8786/8787 zero requests and never connected**; 402–576 requests total against ceilings of 288 per arm and 576 overall; per-cycle evidence isolated and complete. **Separate finding:** `C16` and `C16-csv` — and only those two of sixty-four — varied their row order across cycles on both arms while remaining semantically equivalent; **effect directly observed, source-level mechanism strongly supported**, not proven step by step in the running process. Limitations retained unchanged: `-S` ran no `site.py` or `.pth`, no worker-level import provenance, seed diversity is **sibling / launch-environment level and not worker level**, the launcher is not PM2, and **D1, formal deployment validation and all performance validation remain open**. |
| 15 | 2026-08-09 | **C1 executed and PASSED — `C1 PASS — isolated package-tree contract correctness`** — §7a.5a. Commit `c1166bfa`, archive verified against its authorised digest before shipping, 75/75 files. **5.2A byte-exact, 64/64 MATCH, 0 DIFFER, RC/CR 32/32**, 24,440,431 bytes per arm, **15 error-status cases byte-exact as well**. Both arms on production's interpreter with `PYTHONHASHSEED=0` and `'data/'`; `clone_manifest_sha256` `f3b66c49…`, `package_tree_digest` `b8754d32…`, `runtime_distribution_digest` `a26ca6c3…`, with `60236d72…` recorded only as the deprecated `name_version_set_sha256`. Clone integrity **three MATCHes**; import isolation clean with 0 production-mapped files; **cleanup PASS**; **production 8050 = 0 requests**; 67–96 requests per arm, 134–192 total. C16 and C16-csv, the two cases that differed under D2b, now match with identical row-order digests. Limitations retained in full and unchanged: `-S` did not run `site.py` or any `.pth`, the launcher is not PM2, there is **no worker-level import provenance**, `/home/odbadmin` remains writable so the manifest checks are **bounded detection and not immutability**, **D1 is not fixed**, **C2 has not run**, and **no latency, throughput or deployment-readiness conclusion exists**. |
| 14 | 2026-08-09 | **The third C1 attempt is `INVALID_PRE_START`** — §7a.3h. It failed earlier than either predecessor: nothing was started, no port bound, no workdir created, zero requests of every kind. `ModuleNotFoundError: bench.dist_digests` — the module existed and its tests passed **in the working tree**, but `.gitignore`'s `**/dist_*` matched it, `git add -A` skipped it silently, `git status` did not list it, and the commit shipped without it. The per-file sync verified 72 of 72 files correctly; the commit was what was incomplete. Module renamed **`bench/package_digests.py`**, and `scripts/test_tracked.sh` added: every harness source must be tracked and unignored, every `bench.*` module imported anywhere must exist and be tracked, no harness module may be named `dist_*`, and **the committed tree is exported and checked to contain every imported module** — the check that would have failed before the run rather than during it. |
| 13 | 2026-08-09 | **`60236d72…` identified: it is the superseded rev 1–5 digest, recomputed live** — §7a.3f. `dependencies()` hashes a sorted `Name==Version` set, which is that canonicalization, so the field named `distributions_sha256` was silently reproducing the digest revision 6 replaced. Renamed **`name_version_set_sha256`**, kept because it is the only one of the three an arm's own interpreter can compute, and every record now carries `clone_manifest_sha256` `f3b66c49…`, `package_tree_digest` `b8754d32…`, `runtime_distribution_digest` `a26ca6c3…` and the name-keyed digest **each beside its canonicalization**; the S2 field and digest lists check all three clone digests, not one. New `bench/package_digests.py`. **§7a.3g: the offline S2 integration test** — real artefacts, materialised source trees with recomputed digests so `validate_meta` re-hashes for real and **nothing is filtered**, `compare_arms(s2=True)` at zero problems, and the 64 cases served over loopback through the gate **invoked with the argument list read out of the runner**, reaching PASS 64/64 and FAIL naming the one altered case. |
| 12 | 2026-08-09 | **The first C1 attempt is `INVALID_PRECONTRACT_HARNESS`, not a C1 FAIL** — §7a.3d. The contract gate was never reached: contract, latency, noise and production-8050 requests were all 0, while the process tree, store probe, three clone-integrity verifications and cleanup all completed. No C1 contract result may be written or cited from it. Two harness defects fixed (`--env-python-arg=-S`; all embedded-Python heredocs quoted), and a third found while fixing them: the worker-count scan flattened NUL-separated argv to newline-delimited text, so an argument containing a newline would shift every later position and make the wrong token the worker count — now parsed from the bytes and crossing into the shell as one integer, with regression tests over argv containing quotes, backticks, newlines, semicolons and `$(…)`. **§7a.3e added: harness bootstrap vs environment under test.** `uv sync --locked` is authorised for `dev2026/.venv` **only**; it is recorded in its own artefact with its scope stated, and the package clone and production site-packages stay immutable — enforced by mode bits, the ancestor check and three manifest re-verifications per run, not by uv's good behaviour. |
| 11 | 2026-08-09 | **The immutability claim was too strong and is withdrawn** — §7a.3c. `dist/` at 555 protects its contents; the 775 parent leaves the *path* replaceable, because unlinking needs write on the directory rather than the file, and the runner's `[ -w ]` check would not have noticed. The ancestor chain is now inspected and classified — a writable immediate parent is a **refusal** (so C1 would refuse to start today), a writable `/home/odbadmin` is **residual exposure** that cannot be fixed and is recorded — and the **full manifest is re-verified three times per run**, at preflight and immediately before each arm starts. What this gives is **detection with a bounded window, not immutability**, and that sentence travels in every record. `chmod a-w` on the parent is named as a **staging write action** needing its own authorisation; it was not performed. New `bench/clone_integrity.py` (63 offline assertions, including the real VM24 chain as a fixture) and `scripts/test_c2_driver.sh` (41 end-to-end assertions on the C2 driver). |
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

**A 200 on the OpenAPI document says the process is serving.** Read this as of the
**unpatched** candidate, which is what C1 and C2 ran: it then said nothing about the
store at all. Spec 004's patch changes that — the candidate's lifespan now reads the
anchor group's Zarr metadata before serving — so under the patched candidate a 200
implies the anchor group opened. It still does not imply a *data* read will succeed,
and the probe itself still reads nothing from the store. D1 measured this from the other end: fixtures N2, N3 and N4 — a
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

### 7a.3c Immutability was over-claimed, and what replaces the claim

§7a.3b reported `dist/` at 555 and the artefacts at 444 and called the clone
read-only. That was too strong. **Unlinking a file needs write permission on its
directory, not on the file.** The parent is 775, so the entire tree can be renamed
and a different one put in its place — under the same path, with every mode inside it
still 555 and 444, passing the runner's `[ -w "$PKG_CLONE" ]` check unchanged. The
*contents* of `dist/` are protected. The *binding of the path to those contents* was
not, and "immutable clone" described the first while being relied on for the second.

The chain, read on 2026-08-09 as uid 1000:

| mode | octal | uid | writable by us | path |
|---|---|---|---|---|
| `dr-xr-xr-x` | 555 | 1000 | no | `…/woa23-s2-package-clone/dist` |
| `drwxrwxr-x` | **775** | 1000 | **YES** | `…/woa23-s2-package-clone` |
| `drwxr-xr-x` | 755 | 1000 | **YES** | `/home/odbadmin` |
| `drwxr-xr-x` | 755 | 0 | no | `/home` |
| `drwxr-xr-x` | 755 | 0 | no | `/` |

Two links are writable and they are not the same kind of problem:

- **The parent is a refusal.** One `chmod a-w` fixes it, and until it is fixed the
  immutability claim is simply false. `bench/clone_integrity.py` treats it as a
  problem and the runner stops. **As things stand today, C1 would refuse to start.**
- **`/home/odbadmin` is residual exposure, recorded and lived with.** The account
  needs a writable home; a rule that refused on it could never be satisfied, and an
  unsatisfiable check gets disabled rather than passed. It means this account can
  still re-point the clone's path even after the parent is fixed.

So path-level immutability **against this account is not achievable**, and the spec
stops claiming it. What replaces it:

1. **The ancestor chain is inspected, resolved through symlinks**, with modes, owners
   and writability recorded in the run's artefacts. A world-writable non-sticky
   ancestor, or a group-writable one owned by someone else, is a refusal at any
   depth.
2. **The full manifest is re-verified three times per run** — at preflight, then
   again immediately before the reference starts and again immediately before the
   candidate starts. All 33,565 files, every time; not a sample, because the thing
   guarded against is a substituted tree and a substituted tree matches a sample as
   easily as it matches nothing. ~7 s per pass.
3. **The remaining window is stated, not glossed.** Between the last verification and
   the moment a worker opens a file, the path can still be re-pointed by anyone who
   can write to an ancestor. This is **detection with a bounded window, not
   immutability**, and `clone_integrity.py` carries that sentence in every record it
   writes.

**The fix to the parent directory is a write action on VM24.** It is `chmod a-w
~/woa23-s2-package-clone`, it changes nothing inside `dist/` and nothing in
production, and it must be authorised as a staging write — it is not part of any
read-only check and was not performed.

### 7a.3d The 2026-08-09 C1 attempt: `INVALID_PRECONTRACT_HARNESS`

The first authorised C1 attempt did not produce a C1 result of any kind. It is
classified **`INVALID_PRECONTRACT_HARNESS`** and this classification is load-bearing:

- **It is not a C1 contract FAIL** and must never be cited as one. The contract gate
  was never reached.
- **No C1 contract result may be written from it.** Contract requests: **0**. Latency
  and noise: **0**, as for every C1 run. Production 8050: **0**.
- The failure was in the harness, after the environment under test had been brought
  up correctly.

What *did* complete, and stands as evidence about the harness rather than about the
candidate: the six-process tree verified against the authorised set; process
readiness on both arms; the symmetric store probe (2 requests per arm, both orders);
all three clone-integrity checks — 33,565 entries against 33,565 files,
1,690,025,002 bytes, `ok=True`, at preflight and before each arm; and a clean
cleanup — every process in every recorded tree exited, 18061/18062/18798 confirmed
free, boot id matched, production unchanged at master 3960 with listeners
3960/4334/4366, and no `.pid`, `.tree`, `.uncertain` or `.diag` left behind.

Two harness defects, neither reachable by any offline test that existed:

| defect | what happened | why nothing caught it |
|---|---|---|
| `--env-python-arg -S` | argparse reads a dash-leading value as the next *option* and exits 2, "expected one argument" | every CLI test stops at argument validation, so none ever invoked `collect_backend_meta`; a grep for the right spelling only checks spelling |
| unquoted `<<PYEOF` | the shell expanded the body before Python saw it; backticks in a *comment* ran as a command and Python received the line with the name deleted | it landed in a comment, so the run continued with one stray line on stderr — the same fault in an expression would have been silent and wrong |

A third, found while fixing the second and never triggered: the worker-count scan
converted production's argv to newline-delimited text with `tr '\0' '\n'`. argv is
NUL-separated *because an argument may contain anything but NUL*, so a single
argument holding a newline becomes two and every position after it shifts — making
the token *after* the real worker count read as the worker count, and the run then
verifies itself against a process set it was never authorised for. It is now parsed
from the NUL-separated bytes and crosses into the shell as one integer. Every
heredoc in the runner is quoted, and the guards assert that none is unquoted and
that no backtick survives inside embedded Python.

### 7a.3e Harness bootstrap and the environment under test are different things

`uv sync --locked --python <production python>` is authorised, and its scope is
exactly `dev2026/.venv`.

| | **harness bootstrap** | **environment under test** |
|---|---|---|
| what | `dev2026/.venv`, from `uv.lock` | S2: the read-only package clone. D2b: the same venv. |
| built by | `uv sync --locked` | nothing — the clone is copied once and never written |
| on an arm's import path | **no** | **yes, exclusively** |
| what it can affect | which HTTP client and comparator the harness uses | what the arms return |
| artefact | `<label>_harness_bootstrap.json` | `<label>_environment.json` |

They are written to **separate files** so one digest cannot be read as the other's,
and the bootstrap record carries its own scope statement: *uv may create or sync
`dev2026/.venv` only; it never installs into the package clone or into production
site-packages, and both remain immutable.* Under D2b the two rows describe the same
venv, and the record says so rather than leaving the coincidence to be inferred.

The clone's immutability is not a matter of uv's good behaviour: it is enforced by
the mode bits, by the ancestor check, and by the full manifest re-verification at
three points in every run (§7a.3c).

### 7a.3f Three digests over one tree, and which is which

The C1 artefacts recorded `distributions 236, digest 60236d72…`, and `60236d72…` is
the value revision 6 marked **superseded**. That is not a coincidence and not a third
digest: `collect_backend_meta.dependencies` hashes a sorted set of `Name==Version`
from inside a running interpreter, which is the rev 1–5 canonicalization, so it
recomputes the old digest every time it runs. The field was called
`distributions_sha256`, a name that says nothing about what is hashed and fits all
three digests equally well.

| field | canonicalization | value for the clone |
|---|---|---|
| `package_tree_digest` | one row per `*.dist-info` directory — **all 240** — as `<dir>\t<Name>\t<Version>\t<PEP 503 name>\tMETADATA=<0\|1>\tRECORD=<0\|1>`, sorted by directory, newline-joined, SHA-256 | `b8754d32…` |
| `runtime_distribution_digest` | the same rows, restricted to the **236** with `METADATA` | `a26ca6c3…` |
| `name_version_set_sha256` | sorted set of `<Name>==<Version>` over `importlib.metadata.distributions()`, newline-joined, SHA-256 — keyed on name and version, **not** on directory | `60236d72…` |
| `clone_manifest_sha256` | SHA-256 of `clone.manifest` itself | `f3b66c49…` |

`name_version_set_sha256` is the **superseded rev 1–5 canonicalization**, renamed and
kept, not revived. Two dist-info directories claiming the same name and version
collapse into one entry under it — precisely the weakness §4.1.2b replaced. It is
retained because it is the **only one of the three an arm's own interpreter can
compute**, and therefore the only one that can show both arms *see* the same
packages rather than that the same directory is on both their paths.

Every S2 provenance record now carries all four, each beside its canonicalization,
and `S2_ENVIRONMENT_RECORD_FIELDS` and `S2_ARM_MATCH_DIGESTS` check the three clone
digests rather than one, so two cannot drift while the record validates on the third.

### 7a.3g The offline S2 integration test

Two authorised runs died before the contract gate on wiring every offline test walked
past. The units were right; the composition had no test, because it was not a
function and the gate was invoked from a shell heredoc. `bench/test_s2_integration.py`
closes that:

- the fixtures are the **real** `c1_meta_candidate.json`, `c1_meta_reference.json`
  and `c1_environment.json` from the 2026-08-09 run, not hand-built records that
  would agree with whatever the code does;
- the arms' source trees are **materialised and their digests recomputed**, so
  `validate_meta` re-hashes and compares for real. **Nothing is filtered.** Editing a
  staged file after collection is asserted to be caught, and restoring it to clear.
  The only mapping is *where* the trees live, which is a property of this machine and
  not of the records;
- `compare_arms(…, s2=True)` must return **zero** problems, and the D2b field list
  must reject the same records for the missing lockfile;
- the 64 contract cases are served from two loopback HTTP servers and the gate is run
  **with the argument list read out of `run_controlled.sh`**, so a change to the
  runner's flags changes what the test executes;
- the gate must reach **PASS with 64 MATCH and RC/CR 32/32**, and **FAIL naming the
  single altered case** with the other 63 still MATCH;
- and the two layers are held to their own jobs: an inter-arm environment drift is
  `compare_arms`'s to catch, a store disagreement is the gate's, and the test asserts
  the runner calls the first before the second and guards the second on its status.

### 7a.3h The third C1 attempt: `INVALID_PRE_START`

Earlier than either predecessor, and for a reason none of the offline suites could
see. Classification **`INVALID_PRE_START`**: no service was started, no port was
bound, no staging workdir was created, and every request count — contract, latency,
noise, production 8050 — is **0**. Only the harness venv and its bootstrap record
came into existence. Nothing may be cited from it, including as a harness failure of
the kind §7a.3d describes; it never reached the stage those did.

```
ModuleNotFoundError: No module named 'bench.dist_digests'
```

The module existed. It imported cleanly. Its tests passed. **In the working tree.**
`.gitignore` line 38 is `**/dist_*` — a pattern for build artefacts — and it matched
`dist_digests.py`. `git add -A` skips ignored files **without saying so**, and
`git status --porcelain` does not list them, so the commit looked complete from every
angle a person checks. The per-file SHA-256 sync then verified 72 of 72 files against
that commit, correctly: the sync was right and the commit was incomplete.

A test suite run from the working tree cannot catch this, because it imports the file
that is there. `scripts/test_tracked.sh` asks git instead:

- every `.py`, `.sh`, `.md` and `.json` under `bench/`, `scripts/`, `api/` and
  `specs/` must be **tracked** and must not be **ignored**;
- every `bench.*` module imported anywhere in `bench/` or `scripts/` must exist and
  be tracked — the import graph, because an import error is the shape this takes at
  run time;
- no harness module may be named `dist_*`, and the `.gitignore` pattern is asserted
  to still match such a name, so the collision is refused when a file is added rather
  than when a run consumes it;
- and **the committed tree is exported with `git archive` and checked to contain
  every imported module**. That is the check that fails before a run instead of
  during one, and it was red against the offending commit.

The module is now `bench/package_digests.py`.

### 7a.5a C1, executed 2026-08-09


**Result: C1 PASS — isolated package-tree contract correctness.**

Read the name in full. It is not "C1 PASS", and §"What this is not" below is part of
the result rather than a caveat attached to it.

| | |
|---|---|
| commit | **`c1166bfa7110ad7687360cab06e74fa35a2ac119`** |
| archive verified before shipping | `cc960eb760eef6927f8ddabaf3c6a75f3f70727e785179ac64222d4c8ad21490`, 75 files, file-list `4ee0d631…baac5b74` |
| staging | `~/woa23-s2-c1d/`, workdir `~/woa23-s2-c1d-work/`, both new |
| gate | **5.2A byte-exact**, 64 cases per arm |
| verdict | **64/64 MATCH, 0 DIFFER** |
| request order | **RC 32 / CR 32**, counterbalanced |
| bytes compared | reference 24,440,431 / candidate **24,440,431** |
| error-status cases | **15** (400 and 404) — **byte-exact MATCH as well**, not excluded |
| captured | 2026-08-09T19:01:40+0800 on odb24 |

#### The environment both arms ran in

Production's own interpreter, against the read-only clone of production's package
tree. Identical on both arms, field for field:

| | |
|---|---|
| interpreter | `/home/odbadmin/.pyenv/versions/py311/bin/python3.11`, named explicitly, not derived |
| `PYTHONHASHSEED` | **0**, both arms |
| `store_path_literal` | `'data/'`, both arms |
| `clone_manifest_sha256` | `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4` |
| `package_tree_digest` | `b8754d32c8aaec6d2049de5d67d3f81aeff4f19effd1525d76b62955447c9b4b` (240 dist-info directories) |
| `runtime_distribution_digest` | `a26ca6c3cfe20ea643c30075d910bb03dbbc01eb3f9d2b4fd224b5d76701447b` (236 with `METADATA`) |
| `name_version_set_sha256` | `60236d7210c8c3647a32e7da55714e246ecc878d1d6296d10e9caf966d2b0b2a` — the **deprecated** name-keyed canonicalization (spec 002 §7a.3f). **It is not the runtime-distribution digest.** |

The **harness bootstrap is a separate record**: `dev2026/.venv`, 58 distributions,
lockfile `0d2980a5…`. It runs the comparator and is on no arm's import path. `uv`
was authorised for that directory and nothing else; the clone and production
site-packages were never written.

#### Clone integrity — three full verifications, all MATCH

| stage | problems | entries / files | bytes | seconds |
|---|---|---|---|---|
| preflight | 0 | 33,565 / 33,565 | 1,690,025,002 | 29.59 |
| before-reference | 0 | 33,565 / 33,565 | 1,690,025,002 | 6.89 |
| before-candidate | 0 | 33,565 / 33,565 | 1,690,025,002 | 6.85 |

Clone parent mode **555**; ancestor chain checked; every file re-hashed each time,
not sampled.

#### Import isolation

Both arms: no problems, `no_site=1`, `ignore_environment=0`. Two tracked processes
each; 342 and 343 mapped files respectively, **0 from production**.

#### The two cases that differed in the D2b run of 2026-08-08

C16 (37,083 bytes, 204 rows) and C16-csv (10,843 bytes) — **both MATCH**, with
identical row-order digests (`56ccfd33…`) and identical column sequences on the two
arms. That difference was the benchmark handing the arms different store strings; here
both build group paths from `'data/'`.

#### Traffic, processes and cleanup

- **6 OS processes**, derived from the measured worker count and verified against it.
- **Requests: 64 contract + 2 data probe + 1–30 readiness per arm = 67–96 per arm,
  134–192 total.** Ceilings were 96 and 192.
- **Production 8050: 0 requests.** 8786 and 8787 were never connected to.
- **Cleanup PASS.** All four services stopped, every process in every recorded tree
  exited, 18061/18062/18798 confirmed free, boot id matched, production unchanged
  (master 3960, start time 1874, listeners 3960/4334/4366). No `.pid`, `.tree`,
  `.uncertain` or `.diag` left behind.
- Afterwards: clone 33,565 files, **0 writable, 0 new `.pyc`**, manifest digest
  unchanged; production site-packages 33,567 files at mtime 2026-02-12;
  `~/python/woa23` at mtime 2026-08-05.

#### What this result is

The candidate and the unmodified reference return **byte-identical responses across
all 64 contract cases** — including every error-status case — when both run on
production's interpreter and a read-only copy of production's package tree, with a
pinned hash seed, one worker each, in isolated staging.

#### What this result is **not**

Each of these is a limitation of the run, not a hedge about it.

- **Not deployment validated, and not ready to deploy.** A contract gate compares two
  processes this harness started, in a staging directory, under `-S`, with a store
  symlink, launched by a shell script.
- **`-S` means `site.py` never ran**, so no `.pth` in the clone was processed —
  `distutils-precedence.pth` and the basemap nspkg `.pth` are present and did not
  execute. Production's site/`.pth` startup semantics were not exercised.
- **The launcher is not production's PM2 path.**
- **No worker-level Python import provenance exists.** The probe is a sibling
  interpreter launched by the same procedure; `/proc/<pid>/maps` can *refute*
  isolation but its silence proves nothing, because it lists mapped files and not
  imports. "The workers imported from the clone" is an inference.
- **`/home/odbadmin` is writable**, so this account can still re-point the clone's
  path. The three manifest verifications are **bounded detection, not immutability**.
- **D1 is not fixed.** A missing `WOA23_ZARR_STORE` fails at import and startup; an
  invalid or non-Zarr store still starts cleanly and fails on the first data request.
  No candidate change is proposed or made.
- **C2 has not been run.** Multi-worker behaviour and unpinned-seed ordering are
  unexamined.
- **No latency, throughput or resource conclusion of any kind.** None was measured;
  the latency gate and the noise pilot did not run.

### 7a.5b C2, executed 2026-08-09


**Result: C2 PASS — isolated package-tree semantic correctness at production worker
count, with observed sibling seed diversity.**

The name is the result. §"What this is not" is part of it, not a caveat appended
to it.

| | |
|---|---|
| commit | **`5cfbf0aa70883af28ffd2e03ce21b4497ac30891`** |
| archive verified before shipping | `3f642dd0…ace0ba`, 78 files, file-list `3be3d046…7fe5c909` |
| staging | `~/woa23-s2-c2c/`, workdirs `~/woa23-s2-c2c-work-cycle{1,2,3}`, all new |
| cycles | **three independent start/stop cycles** |
| gate | **5.2B semantic**, 64 cases per arm per cycle |
| seed policy | **`both-unpinned`** — `PYTHONHASHSEED` **unset on both arms**, stated explicitly rather than inherited from the variant |
| verdict | **PASS (exit 0)** |

#### Production's worker count, measured not assumed

Read from production's own argv at run time in **every** cycle: **actual = 2**.
`--expected-workers 2` was an assertion only; the arms take the measured number and
a disagreement would have aborted before any arm started. Each cycle also re-read
production's listener set, master PID, start time and boot id after the measurement
and confirmed them unchanged.

Process count is derived from that measurement — `2 Dask + 2 × (1 arbiter + 2
workers)` = **8 per cycle**, verified against the tracked trees:

| cycle | the eight processes |
|---|---|
| 1 | 3631498 3631556 3631617 3631619 3631639 3631715 3631717 3631718 |
| 2 | 3633166 3633229 3633282 3633284 3633285 3633387 3633389 3633390 |
| 3 | 3634816 3634879 3634934 3634936 3634956 3635039 3635041 3635042 |

#### 1. Contract — 5.2B semantic, three of three

| | |
|---|---|
| gate | **PASS** |
| per cycle | `PASS`, `PASS`, `PASS` |
| cases | **64/64 per cycle** |
| problems | none |

#### 2. Seed diversity — `OBSERVED`

| cycle | candidate seed digest | reference seed digest | `PYTHONHASHSEED` | `hash_randomization` |
|---|---|---|---|---|
| 1 | `d327c5f70cf4c105…` | `93eda1b784141c14…` | unset | 1 |
| 2 | `a529e1a76865da1b…` | `14f58361cfd4d38a…` | unset | 1 |
| 3 | `9bccce43aaec4040…` | `e253f07863f7e86d…` | unset | 1 |

**3 distinct digests across 3 cycles**, with no precondition problems — the seed was
genuinely unset, hash randomisation was genuinely on, and the fixed 11-string probe
was complete in every cycle. The `PASS_WITH_INSUFFICIENT_SEED_DIVERSITY` branch was
**not** taken.

Measured per cycle by hashing a fixed eleven-string tuple with the same binary, the
same `-S`, `PYTHONPATH`, cwd and environment each arm was launched with.

**This is sibling / launch-environment seed diversity.** The interpreter measured is
a sibling launched by the same procedure — **not** the gunicorn master and not any
worker that served a request.

#### 3. Order stability — recorded, deliberately outside the verdict

| | |
|---|---|
| cases | 64 |
| comparable (case, arm) pairs across cycles | 94 |
| **varied** | **4** |
| responses with no row structure | 102 (counted, not called stable) |

The four are `C16/candidate`, `C16/reference`, `C16-csv/candidate`,
`C16-csv/reference` — and they are a separate finding, below.

#### Traffic, cleanup and host state

- **Requests:** 64 contract + 2 data probe + 1–30 readiness per arm per cycle =
  **67–96 per arm per cycle**, **201–288 per arm** and **402–576 total** across three
  cycles. Ceilings were 288 and 576.
- **Production 8050, 8786, 8787: 0 requests, never connected to.** 8050 was read from
  `/proc` and `ss` only.
- **Clone integrity: nine full verifications, 9/9 MATCH** — three per cycle
  (preflight, before reference, before candidate), 33,565 entries against 33,565
  files, 1,690,025,002 bytes each time.
- **Cleanup: PASS in all three cycles.** Every service stopped, every process in every
  recorded tree exited, 18091/18092/18819 confirmed free each time, production
  unchanged (master 3960, start time 1874, listeners 3960/4334/4366). **Zero blocking
  state files across the whole `run/` tree afterwards.**
- **Per-cycle evidence is isolated and complete:** 10 result artefacts and 4 service
  logs under each of `c2_cycle1`, `c2_cycle2`, `c2_cycle3`; no cycle overwrote
  another.
- **Afterwards:** clone 555/555, manifest digest unchanged, 33,565 files, **0
  writable, 0 new `.pyc`**; production site-packages 33,567 files at mtime
  2026-02-12; `~/python/woa23` at mtime 2026-08-05.

#### Finding — C16 and C16-csv: row order varies across cycles, semantics hold

Of the 64 cases, **exactly `C16` and `C16-csv` showed row-order variation across
cycles on both arms.** The other 90 comparable (case, arm) pairs were stable.

**Effect directly observed; source-level mechanism strongly supported.**

What was observed at runtime: the row-order fingerprints of those two cases, and only
those two, differ between cycles on both arms, under an unpinned hash seed, while the
5.2B semantic comparison passed for them in every cycle.

What is strongly supported but **not** established step by step at runtime: that this
arises because `zarr_group_paths` is a `set` of path strings whose iteration order
depends on the process's hash seed, so a query spanning more than one Zarr group
concatenates its groups in a per-process order. The supporting evidence is that C16
and C16-csv are precisely the two cases in the suite that span more than one group,
that the same two cases were the only ones to differ in the 2026-08-08 D2b run when
the arms were given different store strings, and that `bench/repro_c16.py`
demonstrates the mechanism offline on synthetic rows. **No runtime instrumentation
observed the set iteration inside a worker**, and none was authorised; the internal
causal chain is inferred from the shape of the effect and from offline reproduction,
not proven in the running process.

##### Traceability of the two observations

Both are reproducible from the run's own artefacts, under
`~/woa23-s2-c2c/dev2026/results/` on odb24.

**Seed digests** — `c2_summary.json → seed_diversity.per_cycle` reproduces exactly
the `seed_digest` field of each `c2_cycle{1,2,3}_interp_{candidate,reference}.json`;
all six match. Each of those records also carries the probe itself — 11 strings and
11 hashes — so the digests can be recomputed rather than taken on trust. The raw
values differ per cycle, not merely their digests: `hash("1_degree")` was
`-4854500566350811133`, `4447478972511239299`, `-4704986505660421793` on the
candidate and `9026776351318122430`, `3388466108566623068`, `-714907535411415113`
on the reference.

**Order fingerprints** — each `c2_cycle{1,2,3}_contract.json` records, per case and
per arm, `reference_order` and `candidate_order` with `body_sha256`,
`row_order_sha256`, `columns` and `n_rows`. There are **128 distinct (case, arm)
pairs** — 64 cases on two arms — and **each pair was observed three times, once per
cycle**, so the artefacts hold 384 observations in total. Of the 128 pairs, **94
carry a row-order digest** and 34 are responses with no row structure (those 34
account for the 102 orderless responses `c2_summary.json` counts: 34 pairs x 3
cycles). Recomputing the varied set from those artefacts gives exactly the four
`c2_summary.json` reports.

Case by case, the row-order digest per cycle:

| case | arm | cycle 1 | cycle 2 | cycle 3 |
|---|---|---|---|---|
| `C16` | reference | `56ccfd332e27…` | `56ccfd332e27…` | `a35ae14d930a…` |
| `C16` | candidate | `56ccfd332e27…` | `a35ae14d930a…` | `56ccfd332e27…` |
| `C16-csv` | reference | `56ccfd332e27…` | `56ccfd332e27…` | `a35ae14d930a…` |
| `C16-csv` | candidate | `56ccfd332e27…` | `a35ae14d930a…` | `56ccfd332e27…` |
| `C1` (stable, for contrast) | both | `c994a1ff7849…` | `c994a1ff7849…` | `c994a1ff7849…` |

The two cases take exactly **two** distinct orderings and no more, which is what a
two-element set admits. That is consistent with the proposed mechanism and is not on
its own proof of it — the count would look the same for any two-valued cause.

**It is not a defect against this gate.** 5.2B compares the row multiset and the
column set; row order is not part of the criterion, and under an unpinned seed a
per-process ordering is the expected consequence of the arrangement C2 exists to
observe. It matters because **any consumer that depends on row order would see it**,
and because a byte-exact comparison of these two cases across unpinned processes
would fail for a reason that is not a correctness defect. Whether the candidate
should impose a deterministic order is a separate question and a separate decision.

#### What this result is

The candidate and the unmodified reference return **semantically equivalent
responses across all 64 contract cases, in each of three independent start/stop
cycles**, when both run on production's interpreter and a read-only copy of
production's package tree, **at production's measured worker count of two**, with no
pinned hash seed — and three independent starts were observed to choose different
hash seeds.

#### What this result is **not**

- **Not deployment validated, and not ready to deploy.**
- **No latency, throughput or resource conclusion of any kind.** None was measured;
  the latency gate, the noise pilot and every rung above 21 did not run.
- **`-S` means `site.py` never ran** in any cycle, so no `.pth` in the clone was
  processed. Production's site/`.pth` startup semantics were not exercised.
- **The launcher is not production's PM2 path.**
- **No worker-level Python import provenance exists.** The interpreter probe is a
  sibling process; `/proc/<pid>/maps` can refute isolation but its silence proves
  nothing, because it lists mapped files and not imports.
- **The seed diversity is sibling / launch-environment level, not worker level.** It
  says three starts of that launch procedure chose different seeds. It does not
  measure the seed of any gunicorn master or worker that served a request.
- **D1 is still open.** A missing `WOA23_ZARR_STORE` fails at import and startup; an
  invalid or non-Zarr store still starts cleanly and fails on the first data request.
  No candidate change is proposed or made.
- **Formal deployment validation is still open** — PM2, site/`.pth` semantics,
  readiness, nginx and TLS.
- **Performance validation is still open** in its entirety for S2.

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

### 7a.5c C2, re-executed 2026-08-10 on the patched candidate (`c2f`)

**C2 PASS — isolated package-tree semantic correctness after D1 patch at production
worker count, with observed sibling seed diversity.**

**The first complete, valid C2 result on the patched candidate.** The C2 of
2026-08-09 (`c2c`) ran the *unpatched* candidate. Two attempts on the patched one did
not produce a result and are recorded as such, not quietly dropped:

| attempt | outcome | why |
|---|---|---|
| `c2d` | **cleanup FAIL**, cycle 1 | the arms inherited gunicorn's 30 s `graceful_timeout` while `STOP_WAIT_SECS` was 20, and a logging reentrancy race left one worker unreaped. The contract had passed. |
| `c2e` | **INVALID_PRE_START** | `--clone-manifest` was pointed at the clone root's `SHA256SUMS`, which is a digest list, not a manifest. Stopped at clone-integrity preflight; **no service started, no request issued**. |
| `c2f` | **PASS** | below |

`c2d` and `c2e` keep their evidence. Neither was re-run in place, and neither was
backfilled into this result.

#### What ran

| | |
|---|---|
| commit | `249aa2749bc14d13bf82b9af9cf5385ac2cb2334`, archive `d77b275f79ae3ce036ab3d3d0a6ea0eac77c7339ff37439426284d4a9835caba`, 87 files |
| candidate `api/` | byte-identical to the C1-tested `919095e8` — `app.py d0d8c781…`, `config.py b8066414…`, `query.py 4e28bdd9…`, `store_paths.py 00cb80c2…` |
| staging / workdir / label | `~/woa23-s2-c2f/`, `~/woa23-s2-c2f-work-cycle{1,2,3}`, `c2f` — all new |
| ports | 18131 / 18132 / 18859 — **first use**, checked against `scripts/ports_used.tsv` before starting, and free on the host |
| clone | the existing read-only clone, `clone.manifest` (four columns, 33,565 entries) |
| seed policy | `both-unpinned` — `PYTHONHASHSEED` unset on **both** arms |
| processes | **8 per cycle**: two arms of arbiter + 2 workers, plus an isolated Dask scheduler and worker |

**Prerequisites were read from the host before any service started**, not taken from
the request: `STOP_WAIT_SECS=20` (source `default`; unset in the environment),
`ARM_GRACEFUL_TIMEOUT=10`, `assert_shutdown_budget` PASS, and production's worker
count **measured as 2** from pid 3960's own argv.

#### 1. Contract — 5.2B semantic, three of three

| cycle | gate | cases | verdicts | request order |
|---|---|---|---|---|
| `c2f_cycle1` | **PASS** | 64 | 64 MATCH | RC 32, CR 32 |
| `c2f_cycle2` | **PASS** | 64 | 64 MATCH | RC 32, CR 32 |
| `c2f_cycle3` | **PASS** | 64 | 64 MATCH | RC 32, CR 32 |

#### 2. Seed diversity — `OBSERVED`

Six digests from three independent starts, **all distinct**:

| cycle | candidate | reference |
|---|---|---|
| 1 | `b43233d7642d2319` | `dafdbaa61a9bb6da` |
| 2 | `2e04ce4cb4b0348a` | `14f537d43115d1f9` |
| 3 | `e744e91b3df4bc59` | `f346808f0ad1fe09` |

`PYTHONHASHSEED` unset and `hash_randomization=1` are **preconditions, not
evidence** — they say the interpreter was permitted to choose a seed, and are equally
true of three starts that chose the same one. Only the measured digests distinguish
those cases.

#### 3. Order stability — recorded, deliberately outside the verdict

| | `c2f` (2026-08-10, patched) | `c2c` (2026-08-09, unpatched) |
|---|---|---|
| comparable (case, arm) pairs | 94 | 94 |
| **varied** | **2** | **4** |
| which | `C16/candidate`, `C16-csv/candidate` | `C16/candidate`, `C16/reference`, `C16-csv/candidate`, `C16-csv/reference` |
| responses with no row structure | 102 | 102 |

**The two runs involve the same two cases; they did not behave identically.** In
`c2c` both arms varied; in `c2f` only the candidate pair did, and the reference
returned the same row order in all three cycles.

**That difference is not itself a finding, and must not be read as one.** With no
pinned seed, three cycles landing on one order is compatible with coincidence — the
reference has three samples, not a demonstrated property. Nothing in this run
attributes the difference to the D1 patch, to the arms' code, or to anything else,
and no mechanism for it was investigated. What is established is what the table
says: which pairs varied, in which run.

Semantics held for these cases in every cycle: they are 5.2B MATCH throughout.

#### 4. Shutdown budget — read back from the evidence, not assumed

| cycle | `STOP_WAIT_SECS` | source | arms' `--graceful-timeout`, from each arm's own `/proc/<pid>/cmdline` |
|---|---|---|---|
| 1 | 20 | default | recorded 10 · candidate 10 · reference 10 |
| 2 | 20 | default | recorded 10 · candidate 10 · reference 10 |
| 3 | 20 | default | recorded 10 · candidate 10 · reference 10 |

Status **CONSISTENT**. This is the relationship whose absence failed `c2d`: the
harness's wait must exceed what the arms are entitled to take, and both numbers are
now asserted before a cycle starts and read back afterwards from the processes
themselves.

#### Traffic, cleanup and host state

**Requests — actual where recorded, and bounded where not:**

| component | per arm per cycle | recorded? |
|---|---|---|
| contract gate | **64** | yes — `request_order_counts` RC 32 + CR 32 in each `c2f_cycle{1,2,3}_contract.json` |
| store-readiness probe | **2** | yes — two probes per arm, in both orders, no retry path |
| process-readiness probe | 1–30 | **no** — the loop does not count its attempts |

So **396 requests are recorded as actually issued** (66 per arm per cycle × 2 arms ×
3 cycles), and process readiness adds an unrecorded 1–30 per arm per cycle. **Total
actual: between 402 and 576.** The ceiling declared before the run was 576 per run,
96 per arm per cycle. *The unrecorded component is a gap in the harness, not in this
report: `process_ready` should return its attempt count so the actual total is exact.
No change has been made and no rerun is proposed for it.*

- **Production 8050, 8786, 8787: 0 requests, never connected to.** Read from `/proc`
  and `ss` only. Production **unchanged** at every check in all three cycles: master
  3960, start time 1874, listeners 3960/4334/4366, boot id matched.
- **Clone integrity: nine full verifications, 9/9 OK** — three per cycle (preflight,
  before reference, before candidate). Each: 33,565 manifest entries against 33,565
  files on disk, 1,690,025,002 bytes re-hashed in ~7.0 s, **0 missing, 0 extra, 0
  digest mismatch, 0 size mismatch, 0 mtime mismatch, 0 unreadable**; manifest
  `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4`.
- **Clone integrity is detection, not immutability.** `/home/odbadmin` is writable by
  this account — recorded as residual exposure at every check. The window between a
  verification and a worker opening a file is narrowed, not closed.
- **Cleanup: PASS in all three cycles.** Every service stopped, every process in every
  recorded tree exited, 18131/18132/18859 confirmed free each time. **Zero blocking
  state files across the whole `run/` tree afterwards** — no `.pid`, `.starttime`,
  `.tree`, `.diag` or `.uncertain`. Each `run/c2f_cycle{1,2,3}/` holds its four
  service logs, which is what a clean stop leaves behind.
- **The store was not written.** `~/python/woa23/data` mtime remains 2025-04-18
  14:31:47 — *no modification observed*; the basis for the claim is that the runner
  performs no write to it.
- **Earlier evidence untouched:** `c2c`, `c2d` and `c2e` show zero file changes.

#### Archive

`~/woa23-s2-archive/2026-08-10-c2f-PASS/` — outside any deploy directory, **50 evidence
files** (37 result artefacts, 12 service logs, the run log) **plus a separate
`SHA256SUMS`**, which is the manifest of those 50 and is not one of them. Each
evidence file was hashed at the source, copied, re-hashed at the destination and
compared; `SHA256SUMS` is
`ffa68f58a6f8831ca4487ee787aef6cb4e8ffd5d8bd79716fe2cb8912e87e101`. Directories 555,
files 444, verified unwritable. **The originals under `~/woa23-s2-c2f/` were copied,
never moved, and are unchanged.**

#### What this result is

The candidate — **with the D1 store-startup patch applied** — and the unmodified
reference return **semantically equivalent responses across all 64 contract cases, in
each of three independent start/stop cycles**, when both run on production's
interpreter and a read-only copy of production's package tree, **at production's
measured worker count of two**, with no pinned hash seed. Three independent starts
were observed to choose different hash seeds, and all three cycles stopped cleanly.

#### What this result is **not**

- **Not deployment validated, and not ready to deploy.**
- **No latency, throughput or resource conclusion of any kind.** None was measured;
  the latency gate, the noise pilot and every rung above 21 did not run.
- **`-S` means `site.py` never ran** in any cycle, so no `.pth` in the clone was
  processed. Production's site/`.pth` startup semantics were not exercised.
- **The launcher is not production's PM2 path.**
- **No worker-level Python import provenance exists.** The interpreter probe is a
  sibling process; `/proc/<pid>/maps` can refute isolation but its silence proves
  nothing, because it lists mapped files and not imports.
- **The seed diversity is sibling / launch-environment level, not worker level.** It
  says three starts of that launch procedure chose different seeds. It does not
  measure the seed of any gunicorn master or worker that served a request.
- **The candidate's startup anchor validation reading no data or coordinate chunk is
  offline-audited and implementation-supported, not observed on this host.** This run
  installed no audit hook and recorded no file-open events.
- **D1 is only partly closed.** The patch makes an invalid or non-Zarr store fail
  before the service is ready; **real-store depth characterization is still
  CHARACTERIZATION PENDING** and was not run.
- **The row-order contract decision is still open** — no sorting, no pinned seed.
- **Formal deployment validation is still open** — PM2, site/`.pth` semantics,
  readiness, nginx and TLS.
- **Performance validation is still open** in its entirety for S2.

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
