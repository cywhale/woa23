# D-3 — authorised, offline-only: **STOPPED AT THE CACHE GATE, before staging**

**Authorised 2026-08-30, offline-only. Executed on VM24 (`odb24`) as `woa23c1ro`, uid 994.**

> ## RESULT: **CACHE MISS — the offline-only gate FAILED. The run STOPPED before staging.**
>
> **No staging tree. No workdir. No `PM2_HOME`. No venv. No `pm2 start`. No case issued.**
> **`dep3a` is unstarted and `19161` is unbound.** Both remain **first-use and unburned.**

**This is the authorised outcome of a cache miss, not a failure of the run.** The gate was
written to answer this question before anything existed, and it did.

**Network egress: ZERO.** `UV_OFFLINE=1` and `UV_PYTHON_DOWNLOADS=never` were set on every
command; no interpreter and no package was fetched. The only connections made were the SSH
control session itself.

---

## 1. The gate, and where it failed

| # | condition | result |
|---|---|---|
| **1** | `cpython-3.11.14-linux-x86_64-gnu` present in `UV_PYTHON_INSTALL_DIR` | **FAIL** |
| **2** | every locked artifact present in the uv cache | **UNDETERMINED** — cannot be evaluated without a matching interpreter (§3) |
| **3** | selected interpreter reports 3.11.14 | **not reached** |

**Condition 1 is terminal on its own.** Under offline-only there is no permitted way to
obtain the interpreter, so the run stops.

### 1.1 The evidence

```
uv 0.9.22        UV_OFFLINE=1        UV_PYTHON_DOWNLOADS=never

uv python list --only-installed --offline:
  cpython-3.12.3-linux-x86_64-gnu    /usr/bin/python3.12
  cpython-3.12.3-linux-x86_64-gnu    /usr/bin/python3 -> python3.12

matches for cpython-3.11.14 : 0
/home/woa23c1ro/.local/share/uv/python : No such file or directory
/home/woa23c1ro/.local/share/uv        : does not exist
```

**The uv Python install directory has never existed for this account.** No uv-managed
interpreter has ever been installed here.

The one installed interpreter, **3.12.3**, is **excluded by the subject itself** —
`pyproject.toml` requires `>=3.11,<3.12` and `uv.lock` pins `==3.11.*`.

### 1.2 No permitted interpreter exists anywhere on the host

A read-only search found **no CPython 3.11.x available to uid 994 in any isolated form**: no
uv-managed directory, no `/usr/bin/python3.11`, no `/usr/local/bin/python3.11`, no pyenv
under this account.

**The only 3.11 on the host belongs to `odbadmin`, and all three are FORBIDDEN:**

```
/home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11        Python 3.11.4
/home/odbadmin/.pyenv/versions/py311/bin/python3.11         Python 3.11.4
/home/odbadmin/.pyenv/versions/py311numpy1/bin/python3.11   Python 3.11.4
```

**They were not used, and must not be.** Each is a **shared** interpreter; putting one in the
serving path is precisely the failure **spec 016** exists to prevent, and the authorisation
forbids falling back to production's 3.11.4 explicitly. **That the version constraint would
technically accept 3.11.4 does not make it permitted** — and using it would also erase the
very divergence D-3 is required to record.

**No fallback was taken. No constraint was relaxed. Nothing was edited.**

---

## 2. What the package cache does and does not show

The uv cache exists and looks substantial, but **this is indicative only and condition 2 is
NOT passed:**

| | |
|---|---|
| cache | `/home/woa23c1ro/.cache/uv`, **504 MB** |
| `archive-v0` | 503 MB, **59 entries** |
| `wheels-v5` / `sdists-v9` / `simple-v18` / `interpreter-v4` | 1 entry each |
| sampled locked packages with some cache entry | **10 of 10** — polars, xarray, zarr, numpy, orjson, fastapi, gunicorn, uvicorn, pydantic, starlette |
| `cp311` matches in the cache | **16** |
| `cp312` matches | **0** |

**A 3.11 environment was built by this account at some point** — the cp311 artefacts and the
zero cp312 artefacts say so — **yet no uv-managed interpreter directory exists today.**
**Where that interpreter went is not established**, and I did not guess.

**Why condition 2 stays UNDETERMINED rather than "probably fine":** the authoritative check is
`uv sync --frozen --locked --offline --dry-run`, and uv resolves an interpreter **before** it
evaluates packages. With no permitted 3.11 present it would fail at interpreter selection and
never report on the artifacts. **Finding filenames in a cache directory is not the same as uv
confirming the resolution is satisfiable**, and I will not report the weaker check as the
stronger one.

---

## 3. Nothing was created — verified, not asserted

| | before | after |
|---|---|---|
| `~/woa23-dep3a` | absent | **absent** |
| `~/woa23-dep3a-work` | absent | **absent** |
| `~/woa23-dep3a-pm2` | absent | **absent** |
| `~/tmp-dep3a` | absent | **absent** |
| anything matching `*dep3a*` under `$HOME` | 0 | **0** |
| **port 19161** | 0 listeners | **0 listeners** |
| `~/.local/share/uv` | absent | **absent** — proof no download occurred |

**Process inventory, observer excluded by PID/PGID:**

```
rows   : 12   (baseline 12)
sha256 : ecb0aeba3138fb9bfe03e073707683bdea078ae68ef369cb51154db6e55c6489
```

**Byte-identical to the preflight baseline.** Ten desktop-session processes plus the two
retained daemons — `1709473` (`bs3v1`) and `1761143` (`b1s1`) — **both alive and untouched**.

**Unexpected survivors: 0.** Production `PM2_HOME`, port 8050, the production daemon `3459`
and every other retained daemon were **never touched**. The `test_requests.sh` survivors and
the pre-existing processes were **not cleaned** — separate work, as instructed.

---

## 4. Limits that stand regardless

**D-3 did not run**, so it produced **no** deployment observation, **no** case responses and
**no** store before/after comparison. Nothing here is:

- a **production deployment**, **production equivalence**, a **PASS**, or a **cutover**;
- evidence about the candidate's data path — the ten cases were **never issued**;
- a store claim beyond preflight's **metadata-only** reading.

**The attribution limitation is unchanged and, if anything, reinforced:** a D-3 run would use
**3.11.14** against production's **3.11.4**, with a resolved package set from this run's
`uv.lock` rather than production's environment. **No response difference could be attributed
to the API code alone.** The gate stopping the run does not soften that; it is the reason the
interpreter question mattered enough to gate on.

---

## 5. The decision this leaves with the PI

**Offline-only and this host state are incompatible: there is no permitted interpreter, so
no offline-only D-3 is possible as things stand.** Three ways forward, none taken:

| option | what it needs |
|---|---|
| **A. Permit a scoped download** of `cpython-3.11.14-linux-x86_64-gnu` only | an explicit, separate authorisation. VM24's **own** uv 0.9.22 metadata must be read and compared first — the URL/digest in the [network policy](D3-network-access-policy.md) came from uv **0.9.27** locally and is **unverified for 0.9.22** |
| **B. Pre-populate the interpreter out of band**, then re-run D-3 fully offline | a separate authorised step; D-3 itself then needs no network |
| **C. Leave D-3 blocked** | no action |

**Not an option:** using `odbadmin`'s 3.11.4, using system 3.12.3, relaxing `==3.11.*`, or
editing `uv.lock` / `pyproject.toml`. The first two are forbidden and would corrupt the
result; the last changes the subject and would require a new subject with fresh batches.

**`dep3a` and `19161` are NOT burned.** Neither was created, bound, or written into the
subject — both remain available for the run whenever it is authorised to proceed.

---

## 6. Status

| | |
|---|---|
| preflight | **PASS** (identity, port, store, ACL, PM2, Node — see [preflight result](D3-preflight-result.md)) |
| **offline-only cache gate** | **FAIL — interpreter absent** |
| staging / `pm2 start` / cases | **NOT PERFORMED** |
| network egress | **ZERO** |
| state changed on VM24 | **NONE** — inventory digest identical |
| identity | **`dep3a` / `19161` unburned** |

**Awaiting the PI's decision.**
