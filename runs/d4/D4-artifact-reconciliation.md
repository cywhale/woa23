# D-4 offline readiness reconciliation — `143bf8c` vs `f66ddd8`

**Offline only.** VM24 was not contacted, no production command was run, and nothing under
`dev2026/` was modified: application, Polars, `pyproject.toml`, `uv.lock`, the profile and
every assertion are untouched, no suite was reclassified, and no new batch set was created.

**This is not a cutover authorization.**

---

## 1. `test_s2perf_driver.sh` — formally recorded

**REQUIRED_PASS harness / regression validation.**

- drives **local stand-in HTTP servers** with synthetic delays and statuses;
- starts **no real WOA23 API**, reads **no real store**, exercises **no Polars behaviour**,
  issues **no production request**;
- its latency and statistics validate the harness's **control flow, failure handling, request
  accounting, cleanup and fail-closed behaviour** — nothing else;
- **~18 minutes is the test's own cost** (repeated synthetic scenarios, per-case pauses
  ~0.2 s latency / ~0.3 s pilot, per-case bootstrap). Measured 1081 / 1080 / 1081 s, all
  passing. **It must not be read as an API stall or a performance regression**;
- **it must not be reclassified NOT_APPLICABLE because of its duration**, and **its synthetic
  timing must never be cited as production performance evidence**.

## 2. The two subjects, compared exactly

| | `143bf8c` | `f66ddd8` |
|---|---|---|
| full commit | `143bf8caae4aaa4cd4d4ef9ec0ddcab9ada1174d` | `f66ddd8cd18840213b086a03dba4545b0da8ad44` |
| archive sha256 | `0873a9708a992aaad373ec4ec4999b631deae30a5b0787d72d11dfbbf5c58d17` | **`2bea7db91bcf00270abe0c73beff33ab66692ee6749e57483eb165ef6373de90`** |
| tar members | 273 | **276** |
| regular files | 262 | **265** |
| **file-list digest** | `c436362ae4918d67e59c61b1a4d3328160d877d5da41f770bbb99aba78d74497` | **`ca461166312a61a3d338edf6211fbf6604facac3009a2c546ff54bee6e8ea169`** |

**The `143bf8c` file-list recomputed here to `c436362a…`, matching the authorised value** —
so the algorithm used for `f66ddd8`'s digest is the driver's own, not an approximation.

### 2.1 `git diff --stat`, and the complete changed-file list

```
 dev2026/scripts/d4_profile.tsv          |  38 +++++++++
 dev2026/scripts/run_d4_validation.sh    | 147 ++++++++++++++++++++++++++++++++
 dev2026/scripts/run_suites.sh           |  27 ++++++
 dev2026/scripts/test_d1_finalize.sh     |   0
 dev2026/scripts/test_ports.sh           |  38 ++++++++-
 dev2026/scripts/test_production_stop.sh |  28 +++++-
 dev2026/scripts/test_s2perf_driver.sh   | 104 +++++++++++++++++++++-
 dev2026/scripts/test_summary_order.sh   | 123 ++++++++++++++++++++++++++
 8 files changed, 501 insertions(+), 4 deletions(-)
```

| status | file |
|---|---|
| A | `dev2026/scripts/d4_profile.tsv` |
| A | `dev2026/scripts/run_d4_validation.sh` |
| A | `dev2026/scripts/test_summary_order.sh` |
| M | `dev2026/scripts/run_suites.sh` |
| M | `dev2026/scripts/test_d1_finalize.sh` — **mode only**, `100644` -> `100755` |
| M | `dev2026/scripts/test_ports.sh` |
| M | `dev2026/scripts/test_production_stop.sh` |
| M | `dev2026/scripts/test_s2perf_driver.sh` |

**Every changed path is under `dev2026/scripts/`.**

### 2.2 Application, dependencies and delivery files are BYTE-IDENTICAL

Proven by git object identity, not by inspection:

| subtree / file | `143bf8c` | `f66ddd8` | |
|---|---|---|---|
| `dev2026/api` | `6c256928100b6d51cf74da71e94e86e395ed6ad0` | `6c256928100b6d51cf74da71e94e86e395ed6ad0` | **identical** |
| `dev2026/deploy` | `d0c58fcfb367aa23d68fb149f520612f1d23e971` | `d0c58fcfb367aa23d68fb149f520612f1d23e971` | **identical** |
| `dev2026/bench` | `636f5c19ba7c510843779ca0f30f6a52f2c638a9` | `636f5c19ba7c510843779ca0f30f6a52f2c638a9` | **identical** |
| `dev2026/uv.lock` | `8abeba987811a446f69db5670c09d085dd660d06` | `8abeba987811a446f69db5670c09d085dd660d06` | **identical** |
| `dev2026/pyproject.toml` | `bdb233360dae7ad8120d098860515b4461133a2a` | `bdb233360dae7ad8120d098860515b4461133a2a` | **identical** |
| `dev2026/conf`, `dev2026/specs` | — | — | **identical** |

**`f66ddd8` is a harness / test / tooling change ONLY.** The served application, the
deployment delivery files (`deploy/production_app.sh`,
`deploy/ecosystem.production.config.js`), the dependency set and the lock are unchanged.

**One provenance nuance, recorded rather than glossed:** `test_d1_finalize.sh` changed **mode
only**. `tree_filelist` digests **content**, not mode, so that fix does **not** appear in the
file-list difference and a file-list match alone would not prove it present.

## 3. STOP — an artifact / provisioning provenance MISMATCH

**Reported rather than reasoned around, as instructed.**

| | |
|---|---|
| **validated subject** | **`f66ddd8`** — three clean batches, sentinel `subject=f66ddd8… batches=3 clean=yes` |
| **provisioned subject on VM24** | **`143bf8c`** — `$APP_ROOT=/home/odbadmin/python/woa23-143bf8c`, extracted from archive `0873a970…` and verified against file-list `c436362a…` |
| `143bf8c`'s own batch evidence | **none clean.** Its baseline run in this environment was 13 / 12 / 12 non-zero suites, no sentinel |

**These are two different artifacts.** Deploying the `143bf8c` tree while citing `f66ddd8`'s
clean batches would mix runtime evidence across subjects — exactly what this campaign has
refused to do since D-3.

**The two coherent resolutions, neither adopted here:**

| # | resolution | what it requires |
|---|---|---|
| **R1 — cut over `f66ddd8`** | the validated subject becomes the artifact | `$APP_ROOT` re-provisioned as `woa23-f66ddd8`; archive `2bea7db9…`, 276 members / 265 files, file-list `ca461166…`; §1 of the plan and every provenance reference updated to f66. **The existing `/home/odbadmin/python/woa23-143bf8c` provisioning does not carry over** |
| **R2 — cut over `143bf8c`** | the already-provisioned tree stays | then **`f66ddd8`'s clean batches are NOT deployment validation for it**, and `143bf8c` has no clean batch set of its own. That gap must be stated, not filled by inference |

**The venv is not the obstacle in either case:** `uv.lock` and `pyproject.toml` are identical,
so the 58-distribution environment is the same for both, and the standalone CPython 3.11.14
is subject-independent. **What differs is the extracted tree and its recorded identity.**

**I am not choosing.** R1 is the one that keeps evidence and artifact aligned, but it means
re-doing the extraction step of provisioning against the f66 archive.

## 4. `f66ddd8`'s validation, as it stands

Three batches, HEAD verified before **and** after each, tracked dirty **0** throughout,
identical totals in all three:

```
REQUIRED_PASS passed 50 · FAILED 0 · REQUIRED_FAIL held 0 · BROKEN 0
NOT_APPLICABLE 4 · ENVIRONMENT_BLOCKED 6   (skipped BEFORE launch, never a PASS)
UNRESOLVED 0 · TOTAL 50 · NON-ZERO 0
```

Sentinel: `D4_VALIDATION_SENTINEL subject=f66ddd8cd18840213b086a03dba4545b0da8ad44 batches=3
clean=yes`.

**THERE IS NO PRODUCTION PERFORMANCE EVIDENCE, and none is claimed.** No C1/C2 rerun, no real
performance test, and `test_s2perf_driver.sh`'s synthetic timings are harness measurements
(§1). **The D-3 observation for `a361f70` is NOT back-filled as deployment evidence for
`f66ddd8` or for `143bf8c`.**

## 5. Cutover prerequisites still genuinely open

| # | prerequisite | state |
|---|---|---|
| 1 | **cutover artifact identity** | **OPEN — §3.** `f66ddd8` is validated; `143bf8c` is provisioned. One must be chosen |
| 2 | **runtime provisioning / extraction for that artifact** | **PARTIAL.** Interpreter, venv and cache are done and subject-independent. **The extracted tree at `$APP_ROOT` belongs to `143bf8c`**; if R1 is chosen it must be re-extracted and re-verified against `ca461166…` |
| 3 | **`[root]` operator named and available** | **OPEN** — for the whole window, not only the nginx step |
| 4 | **S2 risk acceptance** | **OPEN** — public TLS key readable by `odbadmin`; explicit, recorded acceptance required |
| 5 | **three config-delta authorizations** | **OPEN** — add `WOA23_TLS='off'`, add `WOA23_PYTHON`, remove the two TLS path variables |
| 6 | **nginx: two locations to HTTP upstream** | **OPEN** — `/api/woa23` and `/api/swagger/woa23`, `https://woa23api` -> `http://woa23api`, root operator, in-window only, after the old app stops |
| 7 | **cache-bypass in-window verification** | **OPEN — `CACHE-BYPASS-UNPROVEN`.** Needs the `[root]` `nginx -T` dump and the first observed `X-api-cache: BYPASS` (R2 of the R1/R2/R3 smoke sequence) |
| 8 | **rollback + B1 stop/restart sequence** | **OPEN** — §10 steps 7–9 and §12; the new `exec`-based stop path is still unvalidated in production |

**Also unchanged and still true:** A11 qualified only; store **content** integrity unproven
(the fingerprint is metadata-only); `conf/simu.sh` separate; no cleanup of retained state; the
AVX2 masking remains **accepted, unresolved residual risk** under B6, never a CPU-safety PASS.
