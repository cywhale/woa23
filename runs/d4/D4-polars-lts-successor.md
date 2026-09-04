# `POLARS-CPU-MISMATCH` — successor subject prepared, **batches BLOCKED on a missing artifact**

**Offline only.** VM24 production, PM2, Nginx, TLS and the API were not touched. No
provisioning, no cleanup, no workaround. **`POLARS_SKIP_CPU_CHECK` was never set.** No package
was downloaded to VM24, no index was changed, no other account's cache was used.

**Subject `143bf8c` was NOT modified.** The change is a successor.

---

## 1. The LTS-CPU package — identified, not guessed

| question | answer | how it was established |
|---|---|---|
| distribution name | **`polars-lts-cpu`** | production's shared pyenv site-packages contains **`polars_lts_cpu-1.27.1.dist-info`** — measured during provisioning. `polars_lts_cpu` is the wheel-normalised form of `polars-lts-cpu` |
| version | **`1.27.1`** — unchanged | the same dist-info, and the resolution below succeeded at that exact pin |
| **import namespace** | **`polars`** — unchanged | production carries **both** dist-infos over a **single** `polars/` module directory, and `import polars` there reports `1.27.1`. The LTS build provides the same top-level module |
| resolvable at that version? | **yes** | `uv lock` resolved `polars-lts-cpu==1.27.1` cleanly |

**Nothing about the spelling was assumed.** Had the name been wrong, resolution would have
failed rather than silently producing a different package.

**No version or name compromise was needed** — the LTS variant exists at exactly `1.27.1`, so
the substitution is name-only.

## 2. The dependency change — one line

```diff
- "polars==1.27.1",
+ # LTS-CPU build: VM24 exposes no avx2/bmi1/bmi2/lzcnt to the guest, and the
+ # standard polars wheel requires them. Same import namespace (`polars`), same
+ # version; only the distribution differs. See runs/d4 POLARS-CPU-MISMATCH.
+ "polars-lts-cpu==1.27.1",
```

## 3. The lock diff — minimal, and it is the whole diff

| | |
|---|---|
| regenerated with | `uv lock` (index metadata only; **no package installed**) |
| result | `Resolved 60 packages · Removed polars v1.27.1 · Added polars-lts-cpu v1.27.1` |
| diff size | **46 lines, 3 hunks** |
| packages changed | **exactly two**: `-name = "polars"`, `+name = "polars-lts-cpu"` |
| every other dependency | **unchanged** — no version moved, nothing added or removed |
| old lock sha256 | `0d2980a5928d4d0964d6cb3b78bffae14aa11a70d3b51ca00f4cf39073dccc69` |
| **new lock sha256** | **`d544b0e186e43283ea77bc01d27d17e4ea8a55027a10277c15726d2ed74ccfdf`** |

**Source and hashes recorded in the lock** (`registry = "https://pypi.org/simple"`):

```
sdist  polars_lts_cpu-1.27.1.tar.gz
       sha256:ee4a39e875400ea908a207c02636c8ad0fa14736cc5ff26cffec5d9de55e1f9f   4 557 318 B

wheel  polars_lts_cpu-1.27.1-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl   <-- VM24
       sha256:b3f5915b798710f5a20cbac3b658a22ee9ac69f456d048c14ff6486328b5b76c  35 028 993 B
```

(The lock also carries the macOS x86_64/arm64, manylinux aarch64, win amd64 and win arm64
wheels; only the manylinux x86_64 one is installable on VM24.)

**The new lock installs the LTS-CPU variant and not the standard one** — verified by the
offline sync in §5, which failed naming `polars-lts-cpu` and **never mentioned `polars`**.

## 4. Focused validation — `dev2026/bench/test_polars_lts_cpu.py`

**43 assertions.** No C1/C2 rerun, no performance test.

| section | asserts |
|---|---|
| **1** | **`POLARS_SKIP_CPU_CHECK` is unset**, and the suite refuses to continue if it is set — a suppressed check would make every later assertion meaningless |
| **2** | the installed **DISTRIBUTION** is `polars-lts-cpu==1.27.1`; the standard `polars` distribution is **not also** installed; and `importlib.metadata.packages_distributions()["polars"]` maps to **`['polars-lts-cpu']`** |
| **3** | a **clean subprocess import** emits no `Missing required CPU features`, no `likely result in a crash`, no LTS remediation hint, no `avx2` |
| **4** | `group_by`/`agg`, **`pivot`**, multi-key sort, filter, `with_columns`, and an empty filter that keeps its columns |
| **5** | CSV serialization incl. **header-only for an empty frame**; JSON serialization incl. empty |
| **6** | the no-data CSV response is **200**, `text/csv`, **canonical header byte-identical**, **header-only body**, not a JSON error object; the JSON route agrees with `200 []`; and **`HTTPException` (404), `ValueError` (400) and a malformed request all remain errors** |

### 4.1 Why section 2 exists separately from section 3 — proved, not argued

Both builds install the same module at the same version, so `import polars` and
`polars.__version__` **cannot** tell them apart. Section 2 identifies the distribution.

**The suite was run as a NEGATIVE CONTROL against an environment carrying the STANDARD
polars** (this workstation, macOS/arm64, where the CPU-feature warning does **not** appear):

```
ASSERTIONS=43 FAILED=4
  FAIL 2a  distribution 'polars-lts-cpu' is installed
  FAIL 2b  ... at the pinned version                    got None, want '1.27.1'
  FAIL 2c  the standard 'polars' distribution is NOT also installed
  FAIL 2d  top-level 'polars' is provided by the LTS-CPU distribution
                                                        got ['polars'], want ['polars-lts-cpu']
  ok   3a-3e  (no CPU warning on this CPU)
  ok   1, 4, 5, 6  -- 39 passed
```

**All four failures are in section 2, and section 3 passed.** On a CPU that emits no warning,
**the warning check alone would have accepted the wrong build**; only the distribution check
catches it. That is exactly the property requested, and it is now demonstrated rather than
asserted.

### 4.2 Two defects found and fixed while exercising it

Recorded because an unexercised test is how a vacuous pass gets committed.

1. **The fakes were synchronous.** `process_woa23_data` is **awaited** by the route, so a
   plain function made the route await a `DataFrame` — every section-6 case returned **500**
   and looked like a behaviour regression. The replacements are now `async def`.
2. **6f/6g passed vacuously.** The canonical header was taken from the non-empty response
   without checking it succeeded; comparing two headers from two *failed* responses passes
   while proving nothing. Both are now guarded on 6a having returned 200.

**Neither defect was in the application. Both were in the test**, and both would have made the
suite report a false result.

## 5. BLOCKER — the LTS artifact is NOT in `odbadmin`'s cache

The gate was run on VM24 in a **throwaway tree**, offline, against `odbadmin`'s own cache. No
target path was involved.

```
UV_CACHE_DIR=/home/odbadmin/.cache/uv UV_PROJECT_ENVIRONMENT=/tmp/ltsprobe/.venv \
UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=never  uv sync --locked

  × Failed to download `polars-lts-cpu==1.27.1`
  ╰─▶ Network connectivity is disabled, but the requested data wasn't found in the cache for:
      https://files.pythonhosted.org/packages/7a/8f/666d0fa45ec4df530b332704ec799614c11469102bfe0396a438be757345/
      polars_lts_cpu-1.27.1-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
  TRUE exit code = 1
```

**Cache unchanged: 40 323 files before and after. Nothing was downloaded.**

**REQUIRED ARTIFACT — one wheel:**

| | |
|---|---|
| filename | `polars_lts_cpu-1.27.1-cp39-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl` |
| **sha256** | **`b3f5915b798710f5a20cbac3b658a22ee9ac69f456d048c14ff6486328b5b76c`** |
| size | **35 028 993** bytes |
| URL in the lock | `https://files.pythonhosted.org/packages/7a/8f/666d0fa45ec4df530b332704ec799614c11469102bfe0396a438be757345/…` |
| needed by | `odbadmin`'s cache, `/home/odbadmin/.cache/uv` |

`cp39-abi3` is a stable-ABI wheel, so it serves cp311. **All other 57 distributions are
already cached** — the sync reached polars-lts-cpu and stopped only there.

**It was NOT downloaded, the index was NOT changed, and no other account's cache was used.**
The same wheel is also absent from this workstation's cache, so the validation could not be
run against the LTS build anywhere.

## 6. Status

| | |
|---|---|
| successor subject | **created** — `dev2026/pyproject.toml`, `dev2026/uv.lock`, `dev2026/bench/test_polars_lts_cpu.py` |
| `143bf8c` | **not modified**; its evidence is **not** back-filled to the successor |
| lock diff | **2 packages, 46 lines, 3 hunks** — everything else unchanged |
| focused validation | **written, 43 assertions**, exercised as a **negative control** (4 failures, all in §2, as designed) |
| **validation on the LTS build** | **NOT RUN** — the artifact is unavailable |
| **clean batches for the successor** | **NOT RUN** — they require the LTS build; the subject currently has **no evidence** |
| C1/C2, performance | **not run** |
| VM24 production, PM2, Nginx, TLS, API | **untouched** |

**One authorization is needed:** place the wheel above, digest
`b3f5915b…5b76c`, into `odbadmin`'s cache from an approved source. Then the successor's
batches run offline, and §4's suite can be executed on VM24's own CPU without
`POLARS_SKIP_CPU_CHECK`.
