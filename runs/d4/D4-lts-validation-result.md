# Successor `0d96f7a` — wheel delivered, focused validation **PASSED**, batches **HALTED**

**Outcome: the artifact is in place and the focused suite passes on VM24's own CPU. The three
clean batches did NOT complete — batch 1 failed with 15 non-zero suites, and the driver was
stopped at the first failure as instructed.**

No PM2 start/stop, no Nginx/TLS/config change, no API request, no production store access, no
D-4 provisioning or cutover, no cleanup of retained production state. No C1/C2 rerun, no
performance test. **`POLARS_SKIP_CPU_CHECK` was never set.**

---

## 1. The wheel — verified at both ends

URL taken **verbatim from the committed lock** of subject `0d96f7a`, not typed:

```
https://files.pythonhosted.org/packages/7a/8f/666d0fa45ec4df530b332704ec799614c11469102bfe0396a438be757345/
polars_lts_cpu-1.27.1-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
```

| | workstation | VM24, as `odbadmin` |
|---|---|---|
| **sha256** | `b3f5915b…5b76c` | `b3f5915b…5b76c` |
| **size** | 35 028 993 | 35 028 993 |
| verdict | **MATCH** | **MATCH** |

Staged at `/home/odbadmin/python/wheelhouse/`, `odbadmin:odbadmin` `644`. The target did not
pre-exist. **VM24 made no outbound connection.**

## 2. Cache placement — what did NOT work, and what did

**`uv cache` has no "add" subcommand**, so there is no supported way to hand-place a file and
have uv recognise it by URL. Two uv-native routes were tried before one succeeded; both
failures are recorded because they constrain how this must be repeated.

| # | attempt | result |
|---|---|---|
| 1 | `uv sync --locked --offline --find-links <wheelhouse>` | **FAILED (exit 1).** Under `--locked` uv fetches the exact registry URL from the lock; a flat find-links index does not substitute for it |
| 2 | `uv sync --locked --offline --no-index --find-links file://<wheelhouse>` | **FAILED (exit 1).** `--no-index` removed the registry, so the other 57 packages became unresolvable: *"no version of dask[complete]==2025.3.0"* |
| **3** | **`uv pip install --offline --no-index --find-links <wheelhouse> polars-lts-cpu==1.27.1`** | **exit 0** — uv itself ingested the verified local wheel, which populated its cache |

**This is uv performing the install from the approved local artifact, not a file copied into
the cache directory on the assumption uv would find it.** The distinction matters: attempts 1
and 2 show that assumption would have been wrong.

**Cache: 40 323 → 40 510 files** (`/home/odbadmin/.cache/uv`, `odbadmin`'s own). **No other
account's cache was used, and nothing was downloaded on VM24.**

## 3. The gate — `uv sync --locked --offline`, all conditions met

Run with **no** `--find-links` and **no** index override, in a worktree bound to the subject:

| # | condition | result |
|---|---|---|
| 1 | `uv sync --locked --offline` | **exit 0** |
| 2 | lock digest | **`d544b0e186e43283ea77bc01d27d17e4ea8a55027a10277c15726d2ed74ccfdf`** — as specified |
| 3 | LTS variant installed | **`polars-lts-cpu 1.27.1`** |
| 4 | standard polars | **NOT INSTALLED** |
| 5 | `packages_distributions()["polars"]` | **`['polars-lts-cpu']`** |
| 6 | dist-info present | `polars/` + **`polars_lts_cpu-1.27.1.dist-info`** only |
| 7 | distributions installed | **58** |
| 8 | `POLARS_SKIP_CPU_CHECK` | **unset** |

Worktree `/home/odbadmin/python/woa23-0d96f7a-val`, cloned from a git bundle and detached at
**`0d96f7a64b0538bc132eba23270638d9223290f0`**; `git status --porcelain` empty.

**This is a validation tree, not a deployment tree.** The provisioned `143bf8c` deployment at
`/home/odbadmin/python/woa23-143bf8c` was not touched.

## 4. Focused validation — **43 assertions, 0 failures, on VM24's own CPU**

```
ASSERTIONS=43 FAILED=0
```

| section | result |
|---|---|
| 1 `POLARS_SKIP_CPU_CHECK` unset | **ok** |
| 2 distribution identity is `polars-lts-cpu` | **ok** (2a–2d) |
| 3 **no AVX2 crash-risk warning** on a clean subprocess import | **ok** (3a–3e) |
| 4 `group_by`/`agg`, **pivot**, sort, filter, `with_columns`, empty filter | **ok** (4a–4k) |
| 5 CSV + JSON serialization, incl. header-only empty frame | **ok** (5a–5g) |
| 6 no-data CSV 200 + canonical header + header-only body; errors still errors | **ok** (6a–6o) |

**The warning that started this is gone**, measured on the CPU that produced it.

## 5. BATCHES HALTED — batch 1 failed

```
subject : 0d96f7a64b0538bc132eba23270638d9223290f0
label   : ltsb1        token: ltsb1-2094087-lin-171602171
precheck  HEAD == subject   OK
postcheck HEAD == subject   OK
tracked dirty: 0   untracked: 0
batch 1 exit status: 1
=== TOTAL: 60 | NON-ZERO: 15 ===
```

**Stopped at the first failure, per instruction. Batch 2 was in progress and was terminated;
batch 3 never started. No subject-bound sentinel was produced, and none may be claimed.**

### 5.1 Attribution — the tree difference is exactly three files

```
$ git diff --name-only 143bf8c 0d96f7a -- dev2026
dev2026/bench/test_polars_lts_cpu.py
dev2026/pyproject.toml
dev2026/uv.lock
```

**Class A — caused by the dependency change (3 suites).** These pin the polars
**distribution name**, which is now `polars-lts-cpu`:

| suite | failures | evidence |
|---|---|---|
| `test_environment.py` | 1 | `test_environment.py:342` asserts `("polars", "1.27.1")`; the check reports **`got None`**, because `importlib.metadata.version("polars")` no longer resolves |
| `test_s2_provenance.py` | 4 | references polars |
| `test_manifest.sh` | 3 | references polars |

**This is a real gap in the successor: the dependency was changed, but the suites that record
the expected distribution set were not.** It is not an environment problem and will not go
away on a re-run.

**Class B — not attributable to the change (12 suites).** `test_clone_integrity.py`,
`test_compare_arms.py`, `test_s2perf_integration.py`, `test_c1_readonly_account.sh`,
`test_cli.sh`, `test_d1_finalize.sh`, `test_ports.sh`, `test_production_launcher.sh`,
`test_production_stop.sh`, `test_staging_bootstrap.sh`, `test_staging_entry.sh`,
`test_staging_launcher.sh`.

**Each contains ZERO references to polars** (measured, `grep -c` = 0 for all twelve), and
**every one of their files is byte-identical between `143bf8c` and `0d96f7a`** — the diff
above lists only three files, none of them a suite these exercise.

Two observed causes are environmental rather than subject-related:

```
test_clone_integrity.py: '/tmp is group-writable (drwxrwxrwt) and owned by uid 121, not 1000
                          — an ancestor 5 levels up'
test_ports.sh          : 'every non-comment row starts with a port and a tab' — 88 rows
```

**I am NOT claiming these are pre-existing.** They are **not attributable to the dependency
change**, which is a weaker and provable statement. Establishing that they also fail on
`143bf8c` requires a baseline batch run in this same environment, under `odbadmin`, in
`/home/odbadmin/python/...` — the campaign's earlier batches ran under **`woa23c1ro`**, a
different account and a different tree. **That baseline has not been run and was not
authorised.**

## 6. Status

| | |
|---|---|
| wheel | **delivered and verified at both ends**; cache 40 323 -> 40 510 |
| cache placement | **uv-native** (`uv pip install` from the local wheelhouse); two other routes failed and are recorded |
| `uv sync --locked --offline` | **exit 0**, lock unchanged, LTS installed, standard polars absent, 58 distributions |
| focused validation | **PASSED — 43 assertions, 0 failures, on VM24's CPU, `POLARS_SKIP_CPU_CHECK` unset** |
| **three clean batches** | **NOT COMPLETED** — batch 1 exit 1, 15 non-zero suites; driver stopped at the failure |
| subject-bound sentinel | **NOT produced** |
| production | **untouched** — PM2 3459, 8050 held by 1828352/1828389/1828409 |
| C1/C2, performance | **not run** |
| D-4 cutover | **not performed, not prepared** |

**What the successor still needs, and it is a subject change:** `test_environment.py`,
`test_s2_provenance.py` and `test_manifest.sh` encode `polars` as the expected distribution
and must be updated to `polars-lts-cpu`. **That is a further successor subject** — this run
did not modify `0d96f7a`, its lock, or its dependencies.

**Before the batches can be called clean, the Class B failures also need attribution** — a
baseline run of `143bf8c` in this same environment would settle whether they are
environmental, and that needs its own authorization.
