# Runtime provisioning — an isolated CPython 3.11.14 for uid 994: **request**

**Prepared entirely OFFLINE. VM24 not contacted. Nothing downloaded, nothing installed,
nothing executed. D-3 is not run by this document.**

**This is a standalone request.** It stands apart from D-3 and is **split into separately
authorised phases**. Nothing here bundles Phase 0, provisioning and D-3 execution into one
approval, and **each phase needs its own decision.**

| phase | what it does | status |
|---|---|---|
| **0** | read-only cache survey | **COMPLETE** — [result](D3-phase0-cache-survey-result.md). 3.11.14 **MISSING**; 59/60 artifacts `INDICATIVE_PRESENT` |
| **1** | **provisioning via Option B**, in **seven steps** (§6). Step 7 is the authoritative offline package check — **what earlier drafts called Phase 2** | **COMPLETE** — [result](D3-phase1-provisioning-result.md). 3.11.14 installed, uv recognises it, offline resolution passes. **Three items need review** |
| **3** | **D-3 execution** | separate, already defined, **unchanged** |

**Method: Option B — out-of-band placement.** The isolated runtime is prepared **outside**
D-3; **D-3 itself stays completely offline** and **VM24 never makes an outbound connection.**

**The old "Phase 2" is now step 7** and keeps the same authority. Nothing was dropped in the
rename; the numbering changed, the gate did not.

---

## 1. Fixed parameters

| | |
|---|---|
| interpreter | **CPython 3.11.14**, linux-x86_64, glibc |
| install location | **`/home/woa23c1ro/.local/share/uv/python`** — and nowhere else |
| uv cache | **`/home/woa23c1ro/.cache/uv`** — pinned via `UV_CACHE_DIR`, never left to the default, never inside a staging workdir |
| executed by | **uid 994 (`woa23c1ro`) only** |
| subject | **`6ce915e`** — unchanged. `pyproject.toml`, `uv.lock`, `dep3a`, `19161` all unchanged |

### 1.1 Prohibitions — refusals, not guidance

| # | |
|---|---|
| 1 | **no `odbadmin`**, **no privilege escalation** of any kind |
| 2 | **no write to `/home/odbadmin/.pyenv`**, or to any production path, at any point |
| 3 | **`pm2` is not invoked at all** — not `start`, `stop`, `restart`, `reload`, `delete`, `save`, `resurrect`, `jlist`, or `-v` |
| 4 | **production `PM2_HOME`, port 8050 and every service lifecycle operation are untouched** |
| 5 | **no staging root, workdir, `PM2_HOME` or tmpdir is created**; **no port is bound** |
| 6 | retained state untouched — `bs3v1`/`1709473`, `b1s1`/`1761143`, `pm2A/B/F/G`, production daemon `3459` |
| 7 | `test_requests.sh` survivors and the pre-existing processes are **not cleaned** — separate work |

---

## 2. Network: zero VM24 outbound egress, one inbound transfer

**These are different things and are not merged.**

| | |
|---|---|
| **VM24 outbound egress** | **ZERO.** VM24 initiates **no** connection — not to GitHub, not to PyPI, not to any index or mirror. `uv` is never permitted to fetch |
| **inbound to VM24** | **one SSH-borne file transfer.** The operator, from their own machine, pushes the interpreter archive over SSH (`scp`/`sftp`). The connection is **initiated by the operator toward VM24**; VM24's `sshd` is the passive endpoint |
| where the archive is obtained | **off VM24 entirely** — on the operator's machine, which is where any GitHub contact happens |
| D-3 itself | unchanged: `UV_OFFLINE=1`, and its only traffic is **loopback to `127.0.0.1:19161`** |

**Why the distinction matters:** "offline-only" for D-3 means **VM24 fetches nothing**. An
inbound transfer that the operator initiates does not violate that, but it is **still network
activity reaching the host** and is recorded as such rather than described as "no network".

---

## 3. PHASE 0 — read-only cache survey. **COMPLETE**

**Executed 2026-08-30 under its own authorisation. Full result:
[Phase 0 cache survey result](D3-phase0-cache-survey-result.md).**

| finding | |
|---|---|
| **CPython 3.11.14** | **MISSING** — the uv python install dir does not exist at all |
| **locked artifacts** | **59 `INDICATIVE_PRESENT`, 1 `INDICATIVE_MISSING`** (`colorama` — status **unchanged**, decided at step 7, §7a) |
| **VM24 state** | **completely unchanged** — cache digest `a4c51b82…faca` identical before, mid and after |
| **VM24 metadata** | names build **`20251217`**, **not** the local uv's `20260114` — §4 |

**The steps below are the record of what Phase 0 did.** It wrote nothing, downloaded nothing,
installed nothing and bound nothing.

| # | step |
|---|---|
| 0.1 | confirm `uid=994(woa23c1ro)`; refuse otherwise |
| 0.2 | record `uv --version` and read **VM24's own uv 0.9.22 embedded metadata** for `cpython-3.11.14-linux-x86_64-gnu`: **build tag, URL, SHA-256** |
| 0.3 | compare 0.2 against the local uv 0.9.27 values in §4. **A difference is a finding; VM24's values govern** |
| 0.4 | record whether `/home/woa23c1ro/.local/share/uv/python` exists, and `uv python list --only-installed --offline` |
| 0.5 | pin `UV_CACHE_DIR=/home/woa23c1ro/.cache/uv`; record its size, subdirectories and entry counts |
| 0.6 | **survey all 60 locked artifacts** (§5) against the cache, producing a complete **EXISTS / MISSING** table — every row, no sampling |
| 0.7 | re-confirm the four D-3 identity paths absent and `19161` unbound |

### 3.1 An ordering constraint I cannot engineer away — stated plainly

**The authoritative check is `uv sync --locked --offline --dry-run`, and it CANNOT
run in Phase 0.** uv resolves an interpreter **before** it evaluates artifacts; with no 3.11
present and downloads disabled, it fails at interpreter selection and never reports on
packages. That is exactly what the D-3 gate observed.

**So Phase 0's artifact survey is INDICATIVE — a filename search of the cache — and is
labelled as such.** The authoritative dry-run belongs to **step 7** (§6), after the interpreter
exists. **Phase 0 is not presented as authoritative and its result is not upgraded later.**

### 3.2 What Phase 0 must do if artifacts are MISSING

> **If any of the 60 artifacts is missing, that is reported as a BLOCKER, in full, with the
> missing rows named.**

**It is explicitly NOT treated as "provision the interpreter and see."** Provisioning an
interpreter would not make a missing wheel appear, and under `UV_OFFLINE=1` a missing artifact
stops D-3 exactly as the interpreter did. **Inferring that the run will work once the
interpreter is there is forbidden**; the missing list decides whether a further provisioning
question even arises, and that is the PI's decision, not an inference of mine.

**Phase 0 has three possible outcomes**, and all three are reported as found:

| outcome | consequence |
|---|---|
| interpreter absent, all 60 artifacts present | Phase 1 is worth proposing |
| interpreter absent, **some artifacts missing** | **Phase 1 alone is insufficient.** The missing list is reported and a separate decision is required |
| interpreter present | the D-3 gate is re-run; provisioning may be unnecessary |

---

## 4. The interpreter artifact — **VM24's metadata, and only VM24's**

**Read from VM24's own `uv 0.9.22` embedded metadata in
[Phase 0](D3-phase0-cache-survey-result.md). These are the operative values.**

```
identifier  cpython-3.11.14-linux-x86_64-gnu
version     3.11.14
build tag   20251217
sha256      49e99461d9c4ea3ee80ff0e5d00afa197f9d4c00ebf5fab51e70e507f330003a
url         https://github.com/astral-sh/python-build-standalone/releases/download/20251217/
            cpython-3.11.14%2B20251217-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz
size        NOT PUBLISHED in uv's metadata — not stated, not guessed
```

> ### The local `uv 0.9.27` values are FORBIDDEN as a source or a check
>
> `uv 0.9.27` on the development machine names **build `20260114`**, sha256
> `7fb42e7a…3950`, at a `…/20260114/…` URL. **Same Python version, different build.**
>
> **They may not be downloaded from, verified against, or substituted in.** They are retained
> nowhere in this section except this warning, so they cannot be picked up by mistake.
> **Had they been used, the transferred archive would have failed its digest check.**

**Every digest comparison in Phase 1 uses `49e99461…003a`.** A mismatch stops the work; it is
never reconciled, and the other build is never fetched "to see".

### 4.1 ONE ARCHIVE DOWNLOADED IS NOT ONE FILE INSTALLED

> **The download is a single `.tar.gz`. The installation is a complete CPython tree —
> `bin/`, `lib/`, `include/`, `share/`, the standard library, and shared objects.**

**These must never be conflated, and earlier wording that said "exactly one artifact" referred
to the download only.** An installed CPython is **thousands of files**.

**The installed file count and total size are RECORDED AT INSTALL TIME (§6), not estimated
here.** No number is stated in advance, because I do not have one and inventing it would be
the kind of unverified premise this campaign refuses.

---
## 5. The complete locked artifact set — all 60, enumerated

Derived offline from `uv.lock` (`0d2980a5…cc69`, verified against the subject) for
**cp311 / linux-x86_64 / glibc**: **59 real packages**, **60 artifacts**,
**151 074 275 bytes (144.1 MB)**, one host `files.pythonhosted.org`, one sdist-only
package (`asciitree`).

**Three limits, and Phase 0 exposed the third:**

1. **It is a wheel-tag derivation, not uv's own resolution.** **Step 7** is authoritative;
   this is checked *against* it, never instead of it.
2. **`wrapt` 2.3.0 matches two compatible artifacts**, so which uv selects is undetermined
   here; both are treated as possibly required.
3. **It does NOT evaluate environment markers** — the gap Phase 0 surfaced through
   `colorama`. The lock has exactly three marker-conditional dependencies (`colorama`
   win32-only; `fasteners` and `tornado` both `!= 'emscripten'`), so this derivation
   **over-includes** on Linux and does not under-include. **Which packages are actually
   required is uv's verdict at step 7, not mine** (§7a).

**A defect of mine, recorded:** the first derivation called `polars`, `psutil` and
`tornado` sdist-only because the tag regex read `-cp39-abi3-` as minor version **39**
rather than **9**, rejecting forward-compatible `abi3` wheels. Corrected, the total rose
from 115.1 MB to 144.1 MB.

| # | package | version | kind | size | sha256 (first 16) |
|---|---|---|---|---|---|
| 1 | `annotated-types` | 0.8.0 | wheel | 13,427 | `f072f4d804ea359e` |
| 2 | `anyio` | 4.14.2 | wheel | 125,813 | `9f505dda5ac9f0c8` |
| 3 | `asciitree` | 0.3.3 | sdist | 3,951 | `4aa4b9b649f85e3f` |
| 4 | `bokeh` | 3.9.2 | wheel | 6,409,477 | `448e07d5ee78231f` |
| 5 | `certifi` | 2026.7.22 | wheel | 136,983 | `62f22742b58a1a33` |
| 6 | `click` | 8.4.2 | wheel | 119,243 | `e6f9f66136c81674` |
| 7 | `cloudpickle` | 3.1.2 | wheel | 22,228 | `9acb47f6afd73f60` |
| 8 | `colorama` | 0.4.6 | wheel | 25,335 | `4f1d9991f5acc0ca` |
| 9 | `contourpy` | 1.3.3 | wheel | 355,238 | `51e79c1f7470158e` |
| 10 | `dask` | 2025.3.0 | wheel | 1,437,133 | `b5d72bb33788904a` |
| 11 | `deprecated` | 1.3.1 | wheel | 11,298 | `597bfef186b6f601` |
| 12 | `distributed` | 2025.3.0 | wheel | 1,018,811 | `ebdacd181873b39b` |
| 13 | `fastapi` | 0.115.12 | wheel | 95,164 | `e94613d6c05e27be` |
| 14 | `fasteners` | 0.20 | wheel | 18,702 | `9422c40d1e350e42` |
| 15 | `fsspec` | 2026.7.0 | wheel | 206,583 | `b57ddbafedfaef70` |
| 16 | `gunicorn` | 23.0.0 | wheel | 85,029 | `ec400d38950de4df` |
| 17 | `h11` | 0.16.0 | wheel | 37,515 | `63cf8bbe7522de3b` |
| 18 | `httpcore` | 1.0.9 | wheel | 78,784 | `2d400746a40668fc` |
| 19 | `httpx` | 0.28.1 | wheel | 73,517 | `d909fcccc110f8c7` |
| 20 | `idna` | 3.18 | wheel | 65,455 | `7f952cbe720b6880` |
| 21 | `importlib-metadata` | 9.0.0 | wheel | 27,789 | `2d21d1cc5a017bd0` |
| 22 | `jinja2` | 3.1.6 | wheel | 134,899 | `85ece4451f492d0c` |
| 23 | `locket` | 1.0.0 | wheel | 4,398 | `b6c819a722f7b6bd` |
| 24 | `lz4` | 4.4.5 | wheel | 1,368,674 | `75419bb1a559af00` |
| 25 | `markupsafe` | 3.0.3 | wheel | 22,940 | `0bf2a864d67e76e5` |
| 26 | `msgpack` | 1.2.1 | wheel | 423,843 | `a28d076ca7c82b9c` |
| 27 | `narwhals` | 2.24.0 | wheel | 461,030 | `42fdedf44e5b2ca7` |
| 28 | `numcodecs` | 0.15.1 | wheel | 8,891,971 | `cdfaef9f5f2ed8f6` |
| 29 | `numpy` | 2.2.4 | wheel | 16,428,819 | `f4162988a360a29a` |
| 30 | `orjson` | 3.11.4 | wheel | 136,160 | `95713e5fc8af84d8` |
| 31 | `packaging` | 26.3 | wheel | 129,956 | `d7193f7c8e4e93f4` |
| 32 | `pandas` | 2.2.3 | wheel | 13,058,505 | `c124333816c3a9b0` |
| 33 | `partd` | 1.4.2 | wheel | 18,905 | `978e4ac767ec4ba5` |
| 34 | `pillow` | 12.3.0 | wheel | 6,934,408 | `23d27a3e0307ec22` |
| 35 | `polars` | 1.27.1 | wheel | 35,388,976 | `f801e0d9da198eb9` |
| 36 | `psutil` | 7.2.2 | wheel | 155,560 | `076a2d2f923fd482` |
| 37 | `pyarrow` | 25.0.0 | wheel | 50,056,458 | `0222f0071d133139` |
| 38 | `pydantic` | 2.11.3 | wheel | 443,591 | `a082753436a07f9b` |
| 39 | `pydantic-core` | 2.33.1 | wheel | 2,005,794 | `c964fd24e6166420` |
| 40 | `python-dateutil` | 2.9.0.post0 | wheel | 229,892 | `a8b2bc7bffae2822` |
| 41 | `pytz` | 2026.3.post1 | wheel | 508,283 | `dd95840dd199baea` |
| 42 | `pyyaml` | 6.0.3 | wheel | 806,638 | `b8bb0864c5a28024` |
| 43 | `six` | 1.17.0 | wheel | 11,050 | `4721f391ed90541f` |
| 44 | `sortedcontainers` | 2.4.0 | wheel | 29,575 | `a163dcaede0f1c02` |
| 45 | `starlette` | 0.46.2 | wheel | 72,037 | `595633ce89f8ffa7` |
| 46 | `tblib` | 3.2.2 | wheel | 12,893 | `26bdccf339bcce6a` |
| 47 | `toolz` | 1.1.0 | wheel | 58,093 | `15ccc861ac51c536` |
| 48 | `tornado` | 6.5.7 | wheel | 449,774 | `8d759e71906ee783` |
| 49 | `typing-extensions` | 4.16.0 | wheel | 45,571 | `481caa481374e813` |
| 50 | `typing-inspection` | 0.4.2 | wheel | 14,611 | `4ed1cacbdc298c22` |
| 51 | `tzdata` | 2026.3 | wheel | 348,168 | `dc096730c87af6ca` |
| 52 | `urllib3` | 2.7.0 | wheel | 131,087 | `9fb4c81ebbb1ce95` |
| 53 | `uvicorn` | 0.34.1 | wheel | 62,404 | `984c3a8c7ca18eba` |
| 54 | `wrapt` | 2.3.0 | wheel | 161,700 | `fc648a335d7e01ad` |
| 55 | `wrapt` | 2.3.0 | wheel | 61,866 | `d8c7ed0847742975` |
| 56 | `xarray` | 2025.3.1 | wheel | 1,279,327 | `3404e313930c226d` |
| 57 | `xyzservices` | 2026.3.0 | wheel | 94,101 | `503183d4b322bfeb` |
| 58 | `zarr` | 2.18.6 | wheel | 211,273 | `a5334311fae88598` |
| 59 | `zict` | 3.0.0 | wheel | 43,332 | `5796e36bd0e0cc8c` |
| 60 | `zipp` | 4.1.0 | wheel | 10,238 | `25ad4e16390cd314` |

---

## 6. PHASE 1 — Option B, in seven steps. **Prepared; NOT authorised, NOT executed**

**Option B keeps D-3 completely offline.** The runtime is prepared **outside** D-3; D-3 itself
never fetches anything.

**A note on numbering, so nothing is lost in the rename.** What earlier drafts called *Phase 2*
is now **step 7** below — the authoritative offline package check. It is the same gate with
the same authority; only its label changed. Where later sections say "the Phase 2 gate", they
mean **step 7**.

**Egress, restated because it is the point of Option B:** **VM24 outbound egress remains
ZERO.** Steps 1's fetch happens on the **operator's** machine. What reaches VM24 is **one
inbound SSH-borne transfer**, initiated by the operator.

---

### Step 1 — archive transfer

| # | |
|---|---|
| 1.1 | **On the operator's machine, not VM24**: obtain the archive at the §4 URL — build **`20251217`**. VM24 is not involved and makes no connection |
| 1.2 | compute its SHA-256 **there**, and compare with **`49e99461…003a`**. **Mismatch ⇒ STOP; nothing is transferred** |
| 1.3 | record the archive's **real size in bytes**, from the file. uv publishes no size, so this is the first point at which a true figure exists |
| 1.4 | transfer it over SSH to a scratch path under `/home/woa23c1ro` — **inbound only** |

**The scratch path is a bootstrap path** ([spec 019](019-bootstrap-invocation-protocol-note.md)):
not a staging root, workdir, `PM2_HOME` or tmpdir, and its name contains **no D-3 identity
string**, so `dep3a` stays unconsumed.

### Step 2 — archive digest verification, **on VM24**

| # | |
|---|---|
| 2.1 | re-compute the SHA-256 **on VM24 after transfer** |
| 2.2 | compare with **`49e99461…003a`** — VM24's own metadata value |
| 2.3 | record the on-VM24 byte size and compare with step 1.3 |

**Mismatch at 2.2 ⇒ STOP.** The archive is not extracted, not retried, not re-fetched from
another build. **Verifying on both sides is deliberate**: it separates a bad source from a
damaged transfer.

### Step 3 — isolated interpreter placement

**Destination, and nowhere else:** `/home/woa23c1ro/.local/share/uv/python`.

**Two methods. 3a is preferred; 3b is the fallback.** Which applies is settled at execution,
recorded, not assumed now.

#### 3a — let uv place it, from a local mirror *(preferred)*

```
UV_OFFLINE=1  UV_PYTHON_DOWNLOADS=never  UV_CACHE_DIR=/home/woa23c1ro/.cache/uv
uv python install 3.11.14 --mirror file://<scratch-dir>
```

`uv python install` accepts **`--mirror <MIRROR>`** — *"Set the URL to use as the source for
downloading Python installations"* — and a `file://` URL is **local, not network**.

**Why this is preferred: uv performs its own placement**, so the directory name, internal
layout and any marker files are whatever uv 0.9.22 itself expects, rather than what I guessed.

**Two things about 3a that are NOT assumed and must be confirmed at execution:**

1. **whether `--offline` permits a `file://` mirror at all.** `--offline` disables *network*
   access, and a local file is not network — **but that is reasoning, not a tested fact on
   uv 0.9.22.** If uv refuses, fall back to 3b. **It must not be "fixed" by dropping
   `--offline`.**
2. **the path layout the mirror must present.** uv substitutes the mirror for the release base
   URL, so the archive is expected at
   `<scratch-dir>/20251217/cpython-3.11.14%2B20251217-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz`
   — **and whether the on-disk name carries the literal `+` or the encoded `%2B` is
   undetermined.** Both are tried; which one works is recorded.

#### 3b — manual extraction *(fallback)*

Extract into `/home/woa23c1ro/.local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu/`.

**The archive's top-level `python/` directory is not the install root** — its *contents*
(`bin/`, `include/`, `lib/`, `share/`) go directly into the uv-named directory.

> **A uv-managed install carries a `BUILD` file at its top level containing the build tag.**
> Verified on this machine's own uv-managed `cpython-3.11.14-macos-aarch64-none`, whose
> `BUILD` contains exactly `20260114`. **For VM24's build, `BUILD` must contain `20251217`.**
>
> **If 3b is used, that file must be created**, or uv may not recognise or may misidentify the
> install. **This is exactly why 3a is preferred** — uv writes it itself.

**Neither method uses:** `pip`, `pyenv`, compilation, `pm2`, privilege escalation, or any
write outside the install dir, the pinned cache and the scratch path.

### Step 4 — installed-tree inventory

> **ONE DOWNLOADED ARCHIVE IS NOT ONE INSTALLED FILE.** The download is a single `.tar.gz`;
> the installation is a full CPython tree.
>
> **Concrete reference, not a prediction:** this machine's uv-managed `cpython-3.11.14`
> install contains **3 054 files** across `BUILD`, `bin/`, `include/`, `lib/`, `share/`. That
> is **macOS/arm64 and uv 0.9.27** — the linux-x86_64 figure **will differ** and is **recorded
> at install time, never estimated**.

| recorded | before | after |
|---|---|---|
| **complete file listing** of the install dir — every path | ✓ | ✓ |
| **file count** and **total bytes** of the tree | ✓ | ✓ |
| top-level entries, and the `BUILD` file's contents | — | ✓ |
| tree fingerprint (`path+size+mode`, `LC_ALL=C` sorted, sha256) | ✓ | ✓ |

**The before-listing is what makes the after-listing mean anything.** Phase 0 established the
before-state precisely: **the directory does not exist at all.**

### Step 5 — realpath / version / owner / mode / SHA-256

| # | recorded | acceptance |
|---|---|---|
| 5.1 | **realpath** of the interpreter, resolving every symlink | under `/home/woa23c1ro/.local/share/uv/python` |
| 5.2 | **`--version`** | exactly **`Python 3.11.14`** |
| 5.3 | **owner** and **mode** of the binary and of every directory level | owner **uid 994**; nothing group- or world-writable |
| 5.4 | **SHA-256** of the interpreter binary | recorded — **no prior value exists to compare against**, so this is a first capture, not a check |
| 5.5 | `sys.version`, `sys.prefix`, `sys.executable` | prefix inside the install dir — **not** `/usr`, **not** `/home/odbadmin` |

**5.4 is stated carefully:** the archive digest is verified (step 2), but **no published digest
exists for the extracted binary**, so 5.4 establishes a baseline for future comparison and is
**not** presented as a verification.

### Step 6 — uv 0.9.22 recognition check

```
UV_OFFLINE=1  UV_PYTHON_DOWNLOADS=never  UV_CACHE_DIR=/home/woa23c1ro/.cache/uv
uv python list --only-installed --offline
```

| acceptance | |
|---|---|
| `cpython-3.11.14-linux-x86_64-gnu` appears | as a **filesystem path**, never `<download available>` |
| the path shown | is the one placed in step 3 |

> **If uv does not list it, provisioning is reported FAILED.**
>
> **It is NOT worked around** by pointing `WOA23_PYTHON` at the extracted binary, by
> `--python <path>`, by `UV_PYTHON`, or by any other bypass. An interpreter uv cannot see is
> an interpreter `uv sync --offline` cannot use, and disguising that would move the failure
> into D-3 where it would be harder to attribute.

### Step 7 — authoritative offline package check *(the gate formerly called Phase 2)*

```
UV_OFFLINE=1  UV_PYTHON_DOWNLOADS=never  UV_CACHE_DIR=/home/woa23c1ro/.cache/uv
uv sync --locked --offline --dry-run              # --no-cache remains forbidden
#  NOTE: `--frozen` and `--locked` are MUTUALLY EXCLUSIVE in uv 0.9.22. Earlier drafts of
#  this request paired them, which cannot execute. `--locked` is the correct one: it is the
#  flag that asserts uv.lock must not change. See the Phase 1 result, deviation 3.
```

Run in a **scratch bootstrap directory** holding only the subject's `pyproject.toml` and
`uv.lock` — **no D-3 identity string in its name**, so `dep3a` stays unconsumed.

| # | acceptance |
|---|---|
| 7.1 | the command **succeeds** with **no network access and no download** |
| 7.2 | the interpreter it selects is the **3.11.14** from step 3 |
| 7.3 | **every artifact it actually requires** resolves from the pinned cache |
| 7.4 | its result is reconciled against Phase 0's indicative table **and** §5's derivation; **any disagreement is recorded as a finding about my derivation**, not smoothed over |

#### 7z — carried into D-3 PREFLIGHT by review decision 4

The group-writable interpreter tree is an **accepted, recorded exception** — **no
`chmod -R g-w` is performed**. **D-3 preflight must therefore re-confirm:**

| # | re-confirmed at D-3 preflight |
|---|---|
| 1 | **GID** — group `993` still has no member other than `woa23c1ro`, and no other user holds it as a primary gid |
| 2 | **ACLs** — on the interpreter tree, and on the store, its ancestors and the symlink target |
| 3 | **world-writable = 0** — counted **separately** from group-writable, because `find -perm /022` conflates the two (a measurement error already made once) |
| 4 | **symlink boundaries** — no symlink in the interpreter tree or the store resolves outside it |

**And a STABILIZED BASELINE, not a fingerprint.** Because the tree is writable, **running the
interpreter writes `.pyc` into its own tree** — this already produced six entries during
Phase 1. A whole-tree digest is therefore guaranteed to move and would make every run look
like a finding.

**D-3 compares against the per-file manifest**
[`D3-phase1-interpreter-baseline.tsv`](D3-phase1-interpreter-baseline.tsv) — 5 567 entries,
sha256 `5e83e03bc24fd105073ec61ae38234c88fb23ad66d6bbfd64ffff317c84a455e`:

| observed | verdict |
|---|---|
| one of the **six exact paths** in the Phase 1 result §J, unchanged | **permitted** |
| a `.pyc` or `__pycache__` at **any other path** | **FINDING** — permission is by exact path, never by category |
| a symlink whose **link text** differs, or whose `realpath` leaves the tree or reaches a production path | **FINDING — REJECT** |
| any **other** content change | **FINDING** |
| any **symlink** added, removed or re-pointed | **FINDING** |
| any **owner** change away from `994:993` | **FINDING** |
| any **mode** change | **FINDING** |
| any entry **removed** | **FINDING** |

**Only the six enumerated generated entries are permitted by default** (Phase 1 result §F.1).

**`PYTHONDONTWRITEBYTECODE=1` is recommended**, which removes the drift entirely. It must be
**exported in the spawning shell**, never added to the ecosystem config — the generator diffs
against production's config and **refuses any unexpected key**, so the config route would
change the subject. `ALLOWED_ENV` scans only `^WOA23_`, so it cannot trip that guard.
**Production does not set it, so it is one more documented environment difference.**

#### 7a — `colorama` is decided HERE, and not before

**Phase 0's status for `colorama` stands as `INDICATIVE_MISSING` and is NOT pre-emptively
changed.** I observed that `uv.lock` declares it under `marker = "sys_platform == 'win32'"`,
which would exclude it on linux — **that is an observation, not the verdict.**

**Step 7's offline resolution decides**, because uv evaluates markers itself and I do not:

| step 7 outcome for `colorama` | meaning |
|---|---|
| not required | Phase 0's miss was **my derivation over-including it**; recorded as such |
| **required and absent** | **STOP** — a genuinely missing artifact |

**Either way the verdict comes from uv, not from my reading of the marker.**

#### 7b — what a failure at step 7 does NOT permit

| forbidden | |
|---|---|
| downloading the missing artifact | no network, under any framing |
| falling back to **Python 3.12.3** | excluded by `requires-python` |
| using production **3.11.4** | shared interpreter in the serving path — the **spec 016** failure |
| editing `uv.lock` or `pyproject.toml`, relaxing `==3.11.*` | changes the subject |
| any alternate index, mirror or source **for packages** | the step-3a mirror is for the **interpreter only**, and only from a local file |
| proceeding to D-3 anyway | the gate exists to stop exactly that |

**A stopped run with the missing artifacts named in full is the correct outcome.**

### Step 8 — closing evidence (every step, whether or not it succeeded)

| recorded | |
|---|---|
| `/home/odbadmin/.pyenv` listing and mtimes | **proving no write** |
| production `PM2_HOME`, port **8050**, production daemon `3459` | untouched |
| retained daemons `1709473`, `1761143` | alive |
| the four D-3 identity paths, and `19161` | **absent / unbound** |
| uid-994 process inventory | observer excluded **by PID/PGID**, with a **self-agreeing retake** |
| pinned cache digest | before and after — a change is **reported and attributed**, never assumed benign |

**`pm2` is not invoked at any point in Phase 1** — not `start`, `stop`, `restart`, `reload`,
`delete`, `save`, `resurrect`, `jlist`, or `-v`.

---

## 8. PHASE 3 — D-3, unchanged

```
UV_OFFLINE=1
UV_PYTHON_DOWNLOADS=never
uv sync --locked --offline                    # --no-cache remains forbidden
#  `--frozen` and `--locked` are mutually exclusive in uv 0.9.22 (Phase 1, deviation 3)
UV_CACHE_DIR=/home/woa23c1ro/.cache/uv
```

| still forbidden | why |
|---|---|
| fallback to system **3.12.3** | excluded by `requires-python = ">=3.11,<3.12"` |
| fallback to production **3.11.4** | a **shared** interpreter in the serving path — the **spec 016** failure |
| modifying the **subject**, `uv.lock`, `pyproject.toml`, **`dep3a`** or **`19161`** | changes what is under test; would require a new subject and fresh batches |
| relaxing `==3.11.*`; any alternate index or mirror | not in the subject, not authorised |

**D-3's decisions are unchanged:** real production store via the staging symlink with
`WOA23_ZARR_STORE=data/` · uid 994 with kernel-level unwritability across store, ancestors and
symlink target · **TLS off** · the **fixed ten cases**, one attempt each, read from the subject
archive · isolated `PM2_HOME`, staging tree, venv and alternate port · `api.app:app` verified
loaded from the staging root · **A11 is not a gate** · daemon, tree, workdir, archive and
artefacts **retained**, cleanup separate.

**It is called a candidate deployment rehearsal using the real production store** — **never**
production equivalence, a production PASS, or a cutover.

---

## 9. Attribution limitation — retained; C1/C2 NOT re-run

Provisioning does not remove the divergence, it makes it explicit: the interpreter will be
**3.11.14**, production runs **3.11.4**, and the resolved package set comes from this
`uv.lock` rather than production's environment.

> **No response difference D-3 observes may be attributed to the API code alone.** The
> interpreter and the dependency set remain equally available explanations, and D-3 cannot
> separate them. A **matching** response is likewise not proof of code equivalence.

**C1/C2 are not re-run**, by decision. Their evidence stands as recorded, within its own
limits, and is neither back-filled into D-3 nor extended by it.

**Forward policy for Python 3.12+** is recorded in
[roadmap 022 §6a](022-b2-b5-cutover-roadmap.md): a **runtime/dependency upgrade**, not an API
architecture rewrite — the historical C1/C2/B1 set is **not** re-run wholesale, but a **new
venv**, a **re-resolved lock**, a **dependency compatibility check**, a **trimmed staging
smoke test** and a **trimmed data-path validation** are all required, and **3.11 evidence may
never be claimed to cover 3.12**.

---

## 10. Status

| | |
|---|---|
| Phase 0 | **COMPLETE** — [result](D3-phase0-cache-survey-result.md); VM24 left byte-identical |
| VM24 | **not contacted** to prepare this Phase 1 update |
| download | **none** |
| installation | **none** |
| D-3 | **not executed** |
| `dep3a` / `19161` | **unconsumed**; subject `6ce915e`, `uv.lock` and `pyproject.toml` unchanged |

**The only thing put forward for a decision is PHASE 1 (§6), as seven steps.** It is
**prepared, not executed**, and needs its own authorisation. **Phase 1 and D-3 execution
remain separate approvals and are not bundled** — a Phase 1 authorisation does **not**
authorise D-3.

### 10.1 What will still be true after Phase 1 succeeds

| | |
|---|---|
| D-3 runs | `UV_OFFLINE=1`, `UV_PYTHON_DOWNLOADS=never`, `uv sync --locked --offline` |
| cases | the **fixed ten**, one attempt each, from the subject archive |
| store | the **real production store**, read-only under kernel enforcement as uid 994 |
| TLS | **off** |
| retained | daemon, tree, workdir, archive, artefacts — **no cleanup** |
| C1 / C2 | **not re-run** |
| wording | **candidate deployment rehearsal using the real production store** — never production equivalence, a production PASS, or a cutover |
| attribution | **3.11.14 vs production 3.11.4**, and a different resolved package set — **no response difference may be attributed to the API code alone** |

**Provisioning changes what runs. It does not change what may be claimed.**
