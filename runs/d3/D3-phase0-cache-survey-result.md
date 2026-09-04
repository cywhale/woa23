# Phase 0 — read-only cache survey: **RESULT**

**Authorised 2026-08-30, read-only. Executed on VM24 (`odb24`) as `woa23c1ro`, uid 994.**

> ## STOPPED AT THE END OF PHASE 0, as instructed. **No provisioning. No Phase 1. No D-3.**
>
> **Nothing downloaded, installed, extracted, created or modified.** The cache is
> **byte-identical** before and after. `dep3a` and `19161` remain **unconsumed**.

---

## 1. The four required findings

### 1.1 CPython 3.11.14 — **MISSING**

```
/home/woa23c1ro/.local/share/uv/python   DOES NOT EXIST
/home/woa23c1ro/.local/share/uv          DOES NOT EXIST
uv python list --only-installed --offline:
    cpython-3.12.3-linux-x86_64-gnu    /usr/bin/python3.12
    cpython-3.12.3-linux-x86_64-gnu    /usr/bin/python3 -> python3.12
cpython-3.11.14 matches : 0
```

**Established two independent ways** — by filesystem (the directory does not exist) and by
uv's own offline listing. The only installed interpreter is **3.12.3**, which the subject's
`requires-python = ">=3.11,<3.12"` **excludes**.

### 1.2 Locked artifacts — **59 INDICATIVE_PRESENT, 1 INDICATIVE_MISSING**

All **60** artifacts surveyed. None sampled, none skipped.

| | |
|---|---|
| `INDICATIVE_PRESENT` | **59** |
| `INDICATIVE_MISSING` | **1** |
| surveyed | **60 of 60** |

### 1.3 The complete missing list

```
colorama  0.4.6  (wheel)  colorama-0.4.6-py2.py3-none-any.whl
```

**One entry. That is the whole list.**

#### 1.3a `colorama` is not required on this platform — and that is a defect in MY derivation

`uv.lock` declares it as a dependency of `click` under
**`marker = "sys_platform == 'win32'"`**. **On linux-x86_64 it is never installed.**

**My §5 derivation over-included it**, because it selected artifacts by **wheel tag** and
**never evaluated environment markers**. That gap was not stated plainly enough in the
request, and the survey exposed it.

**The gap is bounded, and I checked rather than assumed.** The lock contains exactly **three**
marker-conditional dependencies:

| dependency | marker | on linux |
|---|---|---|
| `colorama` | `sys_platform == 'win32'` | **not required** — the one reported missing |
| `fasteners` | `sys_platform != 'emscripten'` | **required** — `INDICATIVE_PRESENT` |
| `tornado` | `sys_platform != 'emscripten'` | **required** — `INDICATIVE_PRESENT` |

**So every artifact actually required for cp311 / linux-x86_64 is `INDICATIVE_PRESENT`, and
the single miss is a package that would not be installed.**

**This is stated as a finding, not used to wave the miss away.** The reported status stands as
measured: `colorama` is `INDICATIVE_MISSING`. What changed is my understanding of whether it
was ever needed — and **the authoritative answer still belongs to Phase 2**, not here.

### 1.4 Before / after state — **completely identical**

| | before | after |
|---|---|---|
| `~/.local/share/uv/python` | does not exist | **does not exist** |
| `~/.local/share/uv` | does not exist | **does not exist** |
| cache entries | 9 969 | **9 969** |
| cache bytes | 502 146 635 | **502 146 635** |
| **cache digest** (`path+size+mtime`, `LC_ALL=C`) | `a4c51b82…faca` | **`a4c51b82…faca`** |
| four identity paths | absent | **absent** |
| `*dep3a*` under `$HOME` | 0 | **0** |
| port **19161** | 0 listeners | **0 listeners** |
| process inventory | 12 rows, `ecb0aeba…6489` | **12 rows, `ecb0aeba…6489`** |

**The cache digest was taken three times — before, mid-survey and after — and all three are
identical:**

```
a4c51b82cdec618207336032260c2fa61254ce0d086200c0c85c11bdf100faca
```

**Measured, not assumed.** I checked specifically whether uv's own queries write to the cache
— some uv subcommands persist interpreter metadata under `interpreter-v4` — by digesting
around the invocation. **They did not:** the mid digest (after the filesystem survey) and the
after digest (following `uv --version`, `uv python list` and the metadata read) both equal the
before digest.

**Also verified untouched:** `/home/odbadmin/.pyenv` newest mtime unchanged, port **8050**
still serving, **both** retained daemons (`1709473` bs3v1, `1761143` b1s1) alive.

---

## 2. A finding that matters more than the survey: **VM24's metadata differs**

**VM24's own `uv 0.9.22` names a different interpreter artifact from the local `uv 0.9.27`.**
This is exactly the divergence the request guarded against, and it is real.

| | **VM24, `uv 0.9.22` — GOVERNS** | local `uv 0.9.27` — comparison only |
|---|---|---|
| build tag | **`20251217`** | `20260114` |
| **SHA-256** | **`49e99461d9c4ea3ee80ff0e5d00afa197f9d4c00ebf5fab51e70e507f330003a`** | `7fb42e7a…3950` |
| URL | `…/releases/download/20251217/cpython-3.11.14%2B20251217-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz` | `…/20260114/…%2B20260114-…` |

**Same Python version, different build.** Had the local 0.9.27 digest been used as Option B's
verification target, the transferred archive would have failed its check — or, worse, someone
might have "reconciled" the difference. **VM24's values are the ones that govern**, and they
are recorded here so any future fetch verifies against the right target.

**Size remains unpublished** in uv's metadata and is **not stated**.

---

## 3. What Phase 0 is NOT

| | |
|---|---|
| **not an authoritative resolution** | §1.2 is a **filename search of the cache**. It is `INDICATIVE` and labelled so on every row |
| **the authoritative check is Phase 2** | `uv sync --frozen --locked --offline --dry-run`, which **cannot run** until an interpreter exists — uv resolves the interpreter **before** evaluating artifacts. That is precisely what the D-3 gate hit |
| **no upgrade later** | this result is **not** re-labelled authoritative once Phase 2 runs. Phase 2 produces its own verdict, and any disagreement between them is a **finding about my derivation** |
| **no automatic provisioning** | a miss does **not** authorise a download, an install, or an inference that the run can proceed |
| **no inference of feasibility** | I am **not** concluding that D-3 will work once an interpreter exists. §1.3a explains why `colorama` would not be installed on linux, but the **authoritative** confirmation is Phase 2's and it has not run |

---

## 4. Compliance with the authorisation

| required | |
|---|---|
| ran as uid 994 `woa23c1ro` | ✓ `uid=994(woa23c1ro) gid=993` |
| read only the uv python dir and `/home/woa23c1ro/.cache/uv` | ✓ |
| complete 60-artifact indicative survey | ✓ |
| recorded paths, kinds, sizes, status | ✓ |
| uv used read-only / offline, no cache mutation | ✓ **verified by digest**, not asserted |

| prohibited | |
|---|---|
| download anything | **none** |
| install or extract Python | **none** |
| create/modify any file, dir, cache, venv or symlink | **none** — cache digest identical |
| run `pm2` or any lifecycle command | **never invoked** |
| touch production `PM2_HOME`, 8050, `/home/odbadmin/.pyenv`, production store | **not touched**; 8050 and pyenv observed read-only only |
| create staging root / workdir / `PM2_HOME` / TMPDIR, bind 19161 | **none created, 19161 unbound** |
| run D-3 or any case | **none** |
| enter Phase 1 | **not entered** |

---

## 5. Status and what is being asked

| | |
|---|---|
| CPython 3.11.14 | **MISSING** |
| locked artifacts | **59 INDICATIVE_PRESENT / 1 INDICATIVE_MISSING** (`colorama`, not required on linux — §1.3a) |
| VM24 state | **completely unchanged**, verified by digest |
| `dep3a` / `19161` | **unconsumed** |
| Phase 1 | **NOT entered, NOT requested in this document** |

**Phase 0 is complete and I have stopped.** Awaiting the PI's decision on whether to authorise
Phase 1 provisioning — and if so, **the interpreter to obtain is the one VM24's own metadata
names: build `20251217`, sha256 `49e99461…003a`**, not the values carried in the request from
the local uv.
