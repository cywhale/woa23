# D-3 — network access: **execution policy decision required**

**Prepared entirely OFFLINE. No VM24 contact. No network request was made to prepare this.**

Raised because preflight found that `uv` would **download** an interpreter: `uv.lock` pins
`requires-python = "==3.11.*"`, and the only 3.11 available to uv on VM24 is
`cpython-3.11.14` marked `<download available>`. **That is a new execution property, not a
detail** — D-3 as previously written would have reached out to the network mid-run.

> ## DEFAULT ADOPTED: **OFFLINE-ONLY.** No Python download, no package download.
> **If the interpreter or any locked dependency is not already in VM24's cache, D-3 STOPS
> before staging.** No fallback to system 3.12, none to production's 3.11.4, no relaxing of
> the version constraint, no alternate source.

---

## 1. The Python artifact

**Read from `uv`'s own embedded download metadata, offline** (`strings` over the binary; no
network, no VM24).

| | |
|---|---|
| identifier | `cpython-3.11.14-linux-x86_64-gnu` |
| version | **3.11.14** |
| build tag | `20260114` |
| source | `astral-sh/python-build-standalone`, a **GitHub release asset** |
| URL | `https://github.com/astral-sh/python-build-standalone/releases/download/20260114/cpython-3.11.14%2B20260114-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz` |
| **SHA-256** | **`7fb42e7ac220ec607c5eddbf0361e523279f8cee17fd994bf5fe521676a63950`** |
| **size** | **NOT AVAILABLE.** uv's metadata carries no size field. It is not stated, and not guessed |

For completeness, the musl variant — **not** the one that would be selected on VM24 (glibc):
`…-x86_64-unknown-linux-musl-install_only_stripped.tar.gz`, sha256
`6702202595c881184ea87600f38296208baf9f93415ea063de60a157244109b6`.

### 1.1 A provenance caveat that must not be glossed

**These values come from uv `0.9.27` on the development machine. VM24 runs uv `0.9.22`.**
The embedded metadata is **version-specific** — build tag, URL and digest can differ between
uv releases. VM24's `uv python list` did show a `cpython-3.11.14` row, so its metadata has an
entry; **whether that entry carries this URL and this digest is UNVERIFIED**, because
confirming it needs VM24 and no contact was permitted.

**So the digest above may not be checked against for the interpreter D-3 would install.**
Under the offline-only default this is moot — nothing is downloaded. It matters only if the
PI permits a download, in which case VM24's own metadata must be read first.

---

## 2. Will uv also download packages? **Yes — 60 of them, unless cached**

From the subject's `uv.lock` (`0d2980a5…cc69`):

| | |
|---|---|
| packages | **60** |
| artifacts with URL + sha256 | **271** |
| distinct host | **`files.pythonhosted.org`** — one, and only one |
| registry | **`https://pypi.org/simple`** |
| non-registry sources | **none** — no git, no path, no private index. The single non-registry entry is `source = { virtual = "." }`, the project itself, which is not a download |
| total size of **all** listed artifacts | **1 058 274 187 bytes (~1009 MB)** — an **upper bound**: the lock lists wheels for every platform and Python version, and a linux-x86_64 / cp311 run installs a subset |

**Every artifact is hash-pinned in the lock**, so a package download is verifiable — which is
a property of `uv.lock`, not a reason to allow the download.

---

> ### These hosts are POTENTIAL sources only, for a download that was never permitted
>
> **The authorised D-3 run was offline-only, and its actual network egress was ZERO** — the
> gate stopped it before staging ([execution result](D3-execution-result.md)). Every host
> below describes **where uv WOULD fetch from IF a download were ever allowed**. None was
> contacted. Under offline-only, **any non-loopback connection is a condition violation and
> stops the run.**

## 3. Every network access D-3 could make

| # | actor | destination | when | under offline-only |
|---|---|---|---|---|
| 1 | `uv` interpreter fetch | `github.com` release assets (redirects to `objects.githubusercontent.com`) | `uv sync` / venv creation, if 3.11.14 is not in the Python install dir | **BLOCKED** |
| 2 | `uv` package fetch | `files.pythonhosted.org` | `uv sync`, per artifact not in cache | **BLOCKED** |
| 3 | `uv` index metadata | `pypi.org/simple` | resolution | **BLOCKED** — and `--frozen`/`--locked` means no resolution is needed |
| 4 | `uv` self-update check | Astral endpoints | not triggered by `sync`; never invoked here | **not used** |
| 5 | the ten D-3 cases | **`127.0.0.1:19161`** — loopback only | during the run | **not network egress**; unaffected |
| 6 | `pm2` / `node` | none required for `start`/`jlist` | — | none |
| 7 | the app reading the store | **local filesystem** | during the run | none |

**Only 1–3 are real egress, and all three belong to `uv`.** The ten cases go to loopback and
are unaffected by the policy.

---

## 4. Cache and offline behaviour — verified from uv's own interface

Flags confirmed present in `uv`'s help output, not assumed:

| control | effect |
|---|---|
| `--offline` / `UV_OFFLINE=1` | **disable network access** |
| `--no-python-downloads` / `UV_PYTHON_DOWNLOADS=never` | **disable automatic downloads of Python** |
| `--locked` / `UV_LOCKED=1` | assert `uv.lock` will remain unchanged |
| `--frozen` / `UV_FROZEN=1` | sync without updating `uv.lock` |
| `--no-cache` / `UV_NO_CACHE=1` | **must NOT be used** — it forces a temporary cache, guaranteeing re-download |
| `--cache-dir` / `UV_CACHE_DIR` | package cache location (default `~/.cache/uv`) |
| `UV_PYTHON_INSTALL_DIR` | managed-interpreter location (default `~/.local/share/uv/python`) |

**Two separate caches, and both matter.** The interpreter lives in the Python install dir;
the wheels live in the package cache. **A hit in one does not imply a hit in the other**, so
both are checked independently.

---

## 5. The policy — offline-only, fail closed BEFORE staging

### 5.1 What the run does

```
export UV_OFFLINE=1
export UV_PYTHON_DOWNLOADS=never
uv sync --frozen --locked --offline
```

**`--no-cache` is forbidden**, since it would defeat the cache the policy depends on.

### 5.2 The gate, and it runs BEFORE anything is staged

**Cache presence is checked first, as a preflight step, not discovered by a failing `uv sync`
after the staging tree exists.**

| # | check | on failure |
|---|---|---|
| 1 | `cpython-3.11.14-linux-x86_64-gnu` present in `UV_PYTHON_INSTALL_DIR` — confirmed by `uv python list --only-installed --offline` showing it as a path, not `<download available>` | **STOP before staging** |
| 2 | every locked artifact the linux-x86_64 / cp311 resolution needs present in the uv cache — confirmed by a **dry run**: `uv sync --frozen --locked --offline --dry-run` | **STOP before staging** |
| 3 | the interpreter actually selected reports **3.11.14** | **STOP** |

**Failure is fail-closed and terminal**, and it is explicitly **not** a licence to:

| forbidden fallback | |
|---|---|
| system Python **3.12.3** | violates `requires-python = ">=3.11,<3.12"` |
| production's **3.11.4** at `/home/odbadmin/.pyenv/…` | puts a **shared** interpreter back in the serving path — the exact failure **spec 016** exists to prevent, and it would also silently erase the divergence D-3 is required to record |
| relaxing `==3.11.*`, editing `uv.lock` or `pyproject.toml` | changes the subject; a new subject and fresh batches would be required |
| any alternate index, mirror or source | not in the subject, not authorised |

**If the gate stops the run, the correct outcome is a STOPPED run and a report — not a
substitution.**

### 5.3 What is NOT decided here

**Whether the cache is already populated is UNKNOWN.** Checking it requires VM24, and no
contact was permitted while preparing this. The gate is written so the answer is obtained
**before** staging rather than assumed either way.

**Two outcomes, both acceptable, neither pre-judged:**

- **cache hit** → D-3 proceeds fully offline, and the policy costs nothing;
- **cache miss** → D-3 **stops before staging**, and the PI decides whether to permit a
  download as a separate authorisation.

### 5.4 OUTCOME — the gate ran, and it was a MISS

**`cpython-3.11.14` is not installed on VM24, and `~/.local/share/uv` does not exist at all.**
The run **stopped before staging**; nothing was created and `19161` was never bound. Full
evidence in the [execution result](D3-execution-result.md).

**Condition 2 (packages) is UNDETERMINED, not passed** — uv resolves an interpreter before it
evaluates artifacts, so the authoritative `--dry-run` could not report on them.

---

## 6. If a download is ever permitted — what that decision would need

**Not requested, and nothing below happens without an explicit, separate authorisation.**

1. VM24's **own** uv `0.9.22` metadata read first, and its URL/digest for
   `cpython-3.11.14-linux-x86_64-gnu` compared with §1 — **a difference is a finding**;
2. the downloaded archive's **sha256 verified against VM24's metadata** before use;
3. every package artifact verified against the `uv.lock` hash — already enforced by uv;
4. destinations restricted to the hosts in §3 (`github.com` /
   `objects.githubusercontent.com`, `files.pythonhosted.org`, `pypi.org`), with **the actual
   hosts contacted recorded**;
5. the fetch recorded as part of the run's provenance, since a downloaded interpreter becomes
   part of what served the ten cases.

---

## 7. Bearing on the D-3 result

**None of this softens the attribution limit — it sharpens it.** The interpreter is
**3.11.14** against production's **3.11.4**, and the package set comes from this run's
`uv.lock`, not production's environment. **No response difference may be attributed to the
API code alone**, whether the interpreter came from a cache or a download.

**D-3 remains a candidate deployment rehearsal using the real production store** — not
production equivalence, not a PASS, not a cutover.

**Awaiting the PI's decision. No VM24 contact, no staging, no `pm2 start`, no case issued.**
