# `f66ddd8` provisioned — R1 adopted, `143bf8c` provisioning SUPERSEDED

**Offline provisioning reconciliation only. No cutover was started.** No `PM2_HOME`, no store
symlink, no app started, no Nginx/TLS/production change, no reload, no purge, no API request.
**The `143bf8c` tree was not cleaned, reused, overwritten or read from** — it is preserved as
historical evidence.

**`f66ddd8cd18840213b086a03dba4545b0da8ad44` is the single cutover artifact.**

---

## 1. The artifact, verified end to end

| | |
|---|---|
| subject | **`f66ddd8cd18840213b086a03dba4545b0da8ad44`** |
| archive sha256 | **`2bea7db91bcf00270abe0c73beff33ab66692ee6749e57483eb165ef6373de90`** — matched on the workstation **and** on VM24 |
| members / files | **276 / 265** — both as authorised |
| **file-list** | **`ca461166312a61a3d338edf6211fbf6604facac3009a2c546ff54bee6e8ea169`** — recomputed on VM24 with the driver's own `tree_filelist` algorithm, **matched** |
| **`$APP_ROOT`** | **`/home/odbadmin/python/woa23-f66ddd8`** — newly created; did not previously exist |

### 1.1 Delivery members — archive digest vs on-disk digest

| member | digest | |
|---|---|---|
| `deploy/staging_execute.sh` | `a69b12658c9ca6d76f90bb7afc3d46ab0626582534ee670cb92b2c190fc52d2b` | **MATCH** |
| `deploy/lib_store_guard.sh` | `dc82e80f62177805d6e64500adebdaf435fca0d1512fd283d264a410ac23d23d` | **MATCH** |
| `deploy/production_app.sh` | `7f4da8d76ededc424c748d84e15b05750b3b1cb7fdb1b69fe8e4a47a217f7bea` | **MATCH** |
| `deploy/ecosystem.production.config.js` | `a7e4cb7fcb47e83b8192b225093e5dbe20aad2c673e5fed511b2328ee22410c4` | **MATCH** |

### 1.2 Modes — checked, because the file-list cannot see them

`tree_filelist` digests **content**, not mode, so a file-list match alone would not prove the
exec-bit fix present. Checked directly:

| | |
|---|---|
| `test_d1_finalize.sh` in the archive | `-rwxrwxr-x` |
| on disk after extraction | `-rwxrwxr-x` (775) |
| **direct invocation** | **exit 1 — a real test result, NOT 126** |
| executable members in archive / on disk | **48 / 48** |

### 1.3 No `143bf8c` residue

| | |
|---|---|
| the three f66-only files (`d4_profile.tsv`, `run_d4_validation.sh`, `test_summary_order.sh`) | **all present** |
| the abandoned LTS test `bench/test_polars_lts_cpu.py` | **absent** |
| polars pin | **`"polars==1.27.1"`** |
| `uv.lock` digest | **`0d2980a5…dccc69`** |
| any path naming `143bf8c` inside the tree | **none** |

## 2. Standalone CPython — reused, and re-verified as `odbadmin`

| | |
|---|---|
| uv | **0.9.22** |
| `uv python dir` | `/home/odbadmin/python/uv-pythons` |
| **uv recognition** | **`cpython-3.11.14-linux-x86_64-gnu` listed as uv-managed** |
| `-VV` | `Python 3.11.14 (main, Dec 17 2025, 21:07:37) [Clang 21.1.4]` |
| realpath | `…/uv-pythons/cpython-3.11.14-linux-x86_64-gnu/bin/python3.11` |
| alias -> target | `cpython-3.11.14-20251217` -> the real root |
| owner / mode | `odbadmin:odbadmin` `775` (root and binary) |
| binary sha256 | **`96d1b01675f2492922ec6f6ed8445791d2d3231ccae727cda521db30494b751e`** — matches the recorded value |
| readable + executable by `odbadmin` | **yes** |
| pyenv / `woa23c1ro` | **neither appears** in the realpath |

### 2.1 One halt, and what caused it — my check, not the installation

The first pass **halted at the recognition step**: `uv python list` did not show the
installation. That was **a defect in the check, not a provisioning inconsistency**.

**`uv python list` is scoped to the install directory.** The check ran without
`UV_PYTHON_INSTALL_DIR`, so uv looked in its default `~/.local/share/uv/python` and correctly
reported nothing. With the variable set — which is how the interpreter was installed and how
it must be queried — it is listed, and every other property matched.

**Recorded because the same omission would make a healthy runtime look unprovisioned**: the
recognition check must always carry `UV_PYTHON_INSTALL_DIR`. The halt was correct behaviour;
nothing was reused or assumed compatible while it stood.

## 3. A fresh venv in the new APP_ROOT

Built from the **real** interpreter path, not the alias. **Nothing was copied from the 143
tree.**

```
UV_CACHE_DIR=/home/odbadmin/.cache/uv  UV_PROJECT_ENVIRONMENT=$APP_ROOT/.venv
UV_OFFLINE=1  UV_PYTHON_DOWNLOADS=never
uv sync --locked --python <real>/bin/python3.11        ->  exit 0
```

| check | result |
|---|---|
| `uv sync --locked --offline` | **exit 0** |
| `uv.lock` digest after | **`0d2980a5…dccc69` — UNCHANGED** |
| distributions installed | **58** |
| `dev2026/.venv` | **absent** |
| `sys.prefix` | **`/home/odbadmin/python/woa23-f66ddd8/.venv`** = `$VENV` |
| `realpath(sys.base_prefix)` | **`…/uv-pythons/cpython-3.11.14-linux-x86_64-gnu`** = the real root |
| venv active | **True** |
| polars distribution | **`['polars']`, 1.27.1** — standard, not LTS |
| `WOA23_PYTHON` would be | **`/home/odbadmin/python/woa23-f66ddd8/.venv/bin/python3.11`** |
| cache files before / after | **40 706 / 40 706 — unchanged; nothing downloaded** |

## 4. Provenance status

| | |
|---|---|
| **cutover artifact** | **`f66ddd8` — the only one.** `$APP_ROOT=/home/odbadmin/python/woa23-f66ddd8` |
| **`143bf8c` provisioning** | **SUPERSEDED.** `/home/odbadmin/python/woa23-143bf8c` (515 MB) and its venv are **preserved as historical evidence** and **may NOT be used as deployment evidence for `f66ddd8`** |
| **`f66ddd8`'s batch sentinel** | proves **offline validation only** — three clean batches on a validation worktree. **It does NOT mean the artifact is deployed on VM24**, and it is not deployment evidence |
| D-3 observation for `a361f70` | **not back-filled** to either subject |
| production performance evidence | **none, and none is claimed** |
| new batch set | **none created**; application, Polars, `pyproject.toml`, `uv.lock`, the profile and every assertion are unchanged |

**What is now provisioned is the tree, the interpreter and the venv. Nothing is deployed:** no
PM2 app, no `PM2_HOME`, no store symlink, no config placed, no nginx change.

## 5. Cutover prerequisites still open

| # | prerequisite | state |
|---|---|---|
| 1 | cutover artifact identity | **CLOSED — `f66ddd8`** |
| 2 | runtime provisioning for that artifact | **CLOSED** — tree, interpreter and venv all verified above |
| 3 | **`[root]` operator** named and available for the whole window | **OPEN** |
| 4 | **S2 risk acceptance** — public TLS key readable by `odbadmin` | **OPEN** |
| 5 | **three config deltas** authorised (`WOA23_TLS='off'`, `WOA23_PYTHON`, drop the two TLS paths) | **OPEN** — note `WOA23_PYTHON` now resolves to the f66 venv |
| 6 | **nginx: `/api/woa23` and `/api/swagger/woa23` -> `http://woa23api`** | **OPEN** — root operator, in-window, after the old app stops |
| 7 | **`CACHE-BYPASS-UNPROVEN`** | **OPEN** — needs the `[root]` `nginx -T` dump and the first observed `X-api-cache: BYPASS` |
| 8 | **rollback + B1 stop/restart sequence** | **OPEN** — the `exec`-based stop path is still unvalidated in production |

Unchanged: A11 qualified only; store **content** integrity unproven; `conf/simu.sh` separate;
no retained-state cleanup; AVX2 masking remains **accepted, unresolved residual risk** under
B6 — never a CPU-safety PASS.
