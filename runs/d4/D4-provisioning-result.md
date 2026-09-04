# D-4 — production-side runtime provisioning: **COMPLETE**

**All ten success conditions met.** The standalone runtime and the application tree are
provisioned under `odbadmin`.

**No cutover.** No PM2 or app started, no `PM2_HOME`, no store symlink, no Nginx/TLS/production
config change, no reload, no purge, no API request, no cleanup of retained state, no C1/C2 or
performance test.

**A NEW BLOCKER was found while verifying the result — `POLARS-CPU-MISMATCH` (§7).** It does
not affect the provisioning conditions, and it is not fixable inside this subject.

---

## 1. The runner bug — fixed, and no new subject was needed

The previous run printed `install rc=0` for a **failed** install: `$?` had captured the exit of
the `sed` in the pipeline, not uv's.

**The runner is `prov2.sh` in the session scratchpad, streamed to `sh` over stdin. It is NOT
part of subject `143bf8c` and not under `dev2026/`.** Fixing it therefore required **no
successor subject**, and **the approved subject was not modified** — its file-list still
verifies to `c436362a…` (§5).

**The fix:** every uv invocation writes to a file and its own status is captured immediately;
nothing is piped, so `$?` can never be a `sed`/`tee` status.

```sh
uv ... > "$OUT" 2>&1
RC=$?              # uv's own status, captured before anything else runs
sed 's/^/  /' "$OUT"
```

All three uv calls now report a **true exit code**: `python install` = 0, `venv` = 0,
`sync --locked` = 0.

## 2. Preconditions re-confirmed before starting

| | |
|---|---|
| archive sha256 | **`49e99461…f330003a` — still matches the approved value** |
| `/home/odbadmin/python/uv-pythons` | existed with **bookkeeping only** — `.gitignore` (1 B), `.lock` (0 B), empty `.temp/`; **zero `cpython-*` directories** |
| alias, `$APP_ROOT`, `$VENV` | **all absent** |
| overwriting | **none** — the runner refuses if an interpreter directory or any target already exists |

## 3. Step 3 — standalone CPython installed offline

```
UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=manual \
  /home/odbadmin/.local/bin/uv python install 3.11.14 \
    --mirror "file:///home/odbadmin/python/py-mirror"
```

```
Downloading cpython-3.11.14-linux-x86_64-gnu (download) (29.1MiB)
Installed Python 3.11.14 in 952ms
 + cpython-3.11.14-linux-x86_64-gnu (python3.11)
uv python install TRUE exit code = 0
```

**"Downloading" is uv's generic wording for retrieving from its configured source.** The
configured source was the **`file://` mirror**, and **`UV_OFFLINE=1` makes network access
impossible** — uv fails rather than reaching out under that flag. **VM24 made no outbound
connection.**

**`UV_PYTHON_DOWNLOADS=manual` was applied to that single command only.** It was never
exported; **every later uv call ran with `UV_PYTHON_DOWNLOADS=never`**, as recorded in the
transcript.

## 4. Steps 4–5 — the real root, the alias, and interpreter identity

| | |
|---|---|
| `UV_PYTHON_REAL_ROOT` | `/home/odbadmin/python/uv-pythons/cpython-3.11.14-linux-x86_64-gnu` |
| alias | `/home/odbadmin/python/cpython-3.11.14-20251217` -> **the real root**, `readlink` verified |
| uv listing | recognised as **uv-managed** |
| `-VV` | **`Python 3.11.14 (main, Dec 17 2025, 21:07:37) [Clang 21.1.4]`** — build **2025-12-17**, the approved build |
| binary sha256 | `96d1b01675f2492922ec6f6ed8445791d2d3231ccae727cda521db30494b751e` |
| owner / mode | `odbadmin:odbadmin` `-rwxrwxr-x`, 21 333 768 bytes |
| realpath | inside the real root; **no `/home/odbadmin/.pyenv`, no `/home/woa23c1ro`** |

**The real directory was left where uv created it. Only the approved symlink alias was added.**

## 5. Step 6 — the application tree

| | |
|---|---|
| `$APP_ROOT` | `/home/odbadmin/python/woa23-143bf8c` |
| subject archive sha256 | `0873a9708a992aaad373ec4ec4999b631deae30a5b0787d72d11dfbbf5c58d17` |
| files | **262** (authorised 262) |
| **file-list** | **`c436362ae4918d67e59c61b1a4d3328160d877d5da41f770bbb99aba78d74497`** |
| authorised | **identical — SUBJECT VERIFIED** |

## 6. Steps 7–9 — venv, offline sync, and all ten conditions

```
UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=never uv venv --python $UV_PYTHON_ROOT/bin/python3.11 $VENV
UV_CACHE_DIR=... UV_PROJECT_ENVIRONMENT=$VENV UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=never \
  uv sync --locked
```

| # | condition | result |
|---|---|---|
| 1 | uv recognises the standalone CPython 3.11.14 | **yes** |
| 2 | alias -> uv real directory | **verified** |
| 3 | `$APP_ROOT` from subject `143bf8c` | **262 files** |
| 4 | file-list `c436362a…` | **match** |
| 5 | `$VENV/bin/python3.11` | **exists, executable** |
| 6 | `sys.prefix` | **`/home/odbadmin/python/woa23-143bf8c/.venv`** = `$VENV` |
| 7 | `sys.base_prefix` realpath | **`…/uv-pythons/cpython-3.11.14-linux-x86_64-gnu`** = `$UV_PYTHON_REAL_ROOT` |
| 8 | `$APP_ROOT/dev2026/.venv` | **absent** |
| 9 | `uv sync --locked` offline | **exit 0** |
| 10 | lock digest | **`0d2980a5…dccc69` — unchanged** |
| + | applicable distributions | **58** — as expected for Linux/cp311 |

`pyvenv.cfg`: `home = /home/odbadmin/python/cpython-3.11.14-20251217/bin`, `uv = 0.9.22`,
`version_info = 3.11.14`, `include-system-site-packages = false`.

**Package provenance, verified independently:** `numpy`, `polars`, `pyarrow`, `pydantic_core`,
`orjson`, `zarr`, `fastapi`, `gunicorn` all resolve to
`$VENV/lib/python3.11/site-packages/…`, and **`sys.path` contains no path under `.pyenv` or
`woa23c1ro`** — checked explicitly, result **NONE**.

`dask` and `distributed` are installed in the venv, as the lock requires. **They remain in the
package set; only the serving path does not import them.**

## 7. NEW BLOCKER — `POLARS-CPU-MISMATCH`

**Found while verifying the venv. It does not affect any provisioning condition, and it is a
real risk to the cutover.**

Importing `polars` from the new venv prints:

```
Missing required CPU features.
The following required CPU features were not detected:
    avx2, bmi1, bmi2, lzcnt
Continuing to use this version of Polars on this processor will likely result in a crash.
Install the `polars-lts-cpu` package instead of `polars`.
```

**Measured on VM24:**

| | |
|---|---|
| CPU | `Intel(R) Xeon(R) Gold 6326 CPU @ 2.90GHz` |
| `/proc/cpuinfo` flags | **`avx2` ABSENT · `bmi1` ABSENT · `bmi2` ABSENT · `lzcnt` ABSENT · `abm` ABSENT** — `avx` and `sse4_2` present |

The physical Xeon 6326 has AVX2; **the guest does not see it**, so the vCPU model exposed by
the hypervisor masks those features. **For anything running in this VM, they are absent.**

**Why production is unaffected today — and this is the decisive comparison:**

| environment | polars distribution installed |
|---|---|
| **production, shared pyenv** | `polars-1.27.1.dist-info` **and `polars_lts_cpu-1.27.1.dist-info`** — production runs **`polars-lts-cpu`** |
| **the new venv** | **`polars-1.27.1.dist-info` only** — the standard build, which requires AVX2 |

**`uv.lock` pins `polars`, not `polars-lts-cpu`.** So the subject's dependency set specifies a
build this host cannot guarantee to run, while production has quietly been on the LTS-CPU
build all along.

**Functional probe** (recorded honestly, and it does **not** clear the finding): with
`POLARS_SKIP_CPU_CHECK=1`, a basic frame, a `group_by`/`agg` and a `pivot` all executed
correctly. **That is not evidence of safety** — polars' own check calls a crash *likely*, the
failure would be an illegal instruction on whichever kernel dispatches to AVX2, and it would
appear at request time rather than at import.

**This cannot be fixed inside subject `143bf8c`.** Changing `polars` to `polars-lts-cpu`
means editing `pyproject.toml` and re-locking — **a successor subject**, with its own review.

**Also note:** every candidate run and **G1** used this same lock, so they too ran the
standard `polars` build. **They did not crash**, which is a fact about the paths they
exercised, not a guarantee about all paths.

**Not fixed, not worked around, and no package was changed.** The warning is emitted on every
import and would appear in production logs after cutover.

## 8. Recorded state

| | |
|---|---|
| `uv-pythons` | `odbadmin:odbadmin` `drwxrwxr-x` |
| real python root | `drwxrwxr-x`, **91 MB** |
| alias | `lrwxrwxrwx`, 65-byte target |
| `$APP_ROOT` | `drwxrwxr-x`, **505 MB** total |
| `$VENV` | `drwxrwxr-x`, **500 MB** |
| cache files before / after | **40 320 -> 40 323** (+3: interpreter/metadata records). **No package downloaded** — the sync ran offline from cache |
| lock | **unchanged** |
| mirror | archive retained at `/home/odbadmin/python/py-mirror/20251217/`, 30 507 253 B |

## 9. Production untouched

| | |
|---|---|
| PM2 daemon | pid **3459** — unchanged |
| 8050 listener | **1828352 / 1828389 / 1828409**, starttimes `131297235` / `131297318` / `131297327` — **identical to the baseline** |
| `conf/ecosystem.config.js` | sha256 `ed5dec6c…2159` — **unchanged** |
| old certificate and key | `644`, 5603 / 1704 bytes, mtime Jun 18 2024 — **preserved** |
| new-app `PM2_HOME` | **not created** |
| store symlink | **not created** |

---

## 10. Status

| | |
|---|---|
| provisioning | **COMPLETE — all ten conditions met** |
| runtime | standalone uv-managed **CPython 3.11.14**, build 2025-12-17, under `odbadmin` |
| `WOA23_PYTHON` | `/home/odbadmin/python/woa23-143bf8c/.venv/bin/python3.11` |
| pyenv / `woa23c1ro` | **take no part** — verified in `sys.path` and in every realpath |
| **`POLARS-CPU-MISMATCH`** | **OPEN BLOCKER (§7)** — needs a **successor subject**; not fixable here |
| `CACHE-BYPASS-UNPROVEN` | still open, still a cutover-window gate |
| D-4 cutover | **not performed, not prepared, not authorised** |

**Ready for D-4 cutover planning on the runtime axis only.** The runtime exists, is verified,
and is isolated from pyenv. **`POLARS-CPU-MISMATCH` must be settled first**, and settling it
changes the subject — which means the cutover artifact would no longer be `143bf8c`.
