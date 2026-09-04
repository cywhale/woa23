# 014 — The deployment venv, and recording the whole dependency manifest

**Status: offline plan, revision 1. Nothing built, nothing installed, no VM24 action.**
No dependency version is changed by this document — `pyproject.toml`, `uv.lock` and `api/`
are untouched, as the B6 decision requires.

| rev | date | change |
|---|---|---|
| 1 | 2026-08-20 | First draft, after the B6 decision and the B7 result. Specifies the isolated venv, the pinned interpreter, and the complete-manifest requirement. |

---

## 1. An isolated venv, not the shared `py311`

**Decision (PI, 2026-08-20): the deployment uses its own venv. `py311` is not used.**

B7 found all eight modules present in `/home/odbadmin/.pyenv/versions/py311` at versions
identical to the validated venv's — so the shared interpreter *would* work today. The
decision is not to use it anyway, and the reasons are worth keeping:

| | shared `py311` | isolated venv |
|---|---|---|
| who else depends on it | `mhwapi`, `ghrsst_mcp`, `tide`, and production's own `woa23` | nobody |
| what installed `polars` there | **unknown** — not this campaign | `uv sync` from our `uv.lock` |
| can another project change it? | **yes, silently** | no |
| does a rollback have to undo package installs? | **yes, in a shared environment** | no — the venv is simply not used |
| is it the configuration validated by `c1f`/`c2g`/`s2pB`/`pm2B`? | **no** | **yes** |

The last row is the decisive one. **Every validated run in this campaign used a
`dev2026/.venv` built by `uv sync`.** Deploying onto `py311` would mean production ran a
configuration nothing had validated, with the resemblance argued from eight package
versions rather than demonstrated.

**B7's favourable answer makes the shared interpreter workable, not preferable**, and that
was stated in the B7 request before the result was known.

## 2. The interpreter is pinned to 3.11.4

```
uv sync --python /home/odbadmin/.pyenv/versions/py311/bin/python3.11
```

**3.11.4 — production's own interpreter**, named explicitly rather than resolved. `pm2A`
silently took 3.11.14; `pm2B` pinned 3.11.4 and confirmed from the running process that
`/proc/<pid>/exe` matched production's PID 4296 byte for byte.

**A caveat that must not be lost.** The development machine's venv runs **3.11.14**, not
3.11.4 — macOS has no 3.11.4 available here. So:

- **the local venv is not a byte-equivalent reference** for a VM24 venv, and no manifest
  taken from it may be presented as one;
- the **lockfile** is the portable source of truth, and §3 grounds the expectation there
  rather than in any particular machine's venv;
- a `pm2C` run records its own manifest **on VM24, from the 3.11.4 venv**, and that is the
  artefact that describes what ran.

## 3. The complete manifest, not eight packages

**B7 answered the question it was asked — are these eight present — and that is not enough
to describe a runtime.**

This campaign has already been caught by exactly that gap. In **S1**, the twelve *pinned*
packages matched on both arms while **23 shared transitive dependencies did not**,
including `fsspec` and `anyio`; the A/B was consequently recorded as directional evidence
and never as a measured Dask-only speedup. Eight matching versions are the same kind of
partial view.

So `deploy/record_manifest.py` records **every installed distribution** and a digest over
the whole sorted list.

### 3.1 Two levels, with different rules

**CORE — must match exactly.** A difference is a **stop**, not a note:

| package | required version | why it is CORE |
|---|---|---|
| `polars` | **1.27.1** | **B6 decided this version explicitly.** A different polars is not the decided configuration, and it is the library that performs the row-order sort |
| `orjson` | 3.11.4 | JSON serialisation — response bytes |
| `fastapi` | 0.115.12 | the app framework; owns the OpenAPI document |
| `starlette` | 0.46.2 | response construction beneath FastAPI |
| `pydantic` | 2.11.3 | request validation |
| `uvicorn` | 0.34.1 | the worker class |
| `gunicorn` | 23.0.0 | the master |
| `zarr` | 2.18.6 | store reads |
| `xarray` | 2025.3.1 | dataset handling |
| `numpy` | 2.2.4 | beneath all of the above |

**All ten verified against `uv.lock` on 2026-08-20 — every one matches.** The expectation
is therefore **lock-derived and platform-independent**, not taken from a machine.

**FULL — recorded, and differences explained rather than ignored.** The complete manifest
can legitimately differ between a macOS 3.11.14 venv and a Linux 3.11.4 venv: `uv.lock`
resolves platform markers, so platform-specific distributions may be present on one and
absent on the other.

**"Explained" means named.** A `pm2C` report states, for each difference: the distribution,
both versions (or `ABSENT`), and the marker or platform reason. **A difference that cannot
be explained is a stop.** The purpose of recording the whole manifest is defeated if
unexpected entries can be waved through as "probably platform".

### 3.2 It imports nothing, and that is a property to protect

`record_manifest.py` uses **`importlib.metadata` only**, which reads distribution metadata
from disk. It does not import `polars`, so it does not initialise the library and does not
emit the AVX2 warning of spec 012.

That is not a nicety. An inventory tool that imports what it inventories would, on this
host, trigger the very risk B6 accepted — and would make the manifest step a source of the
warnings it is supposed to be describing. **The offline suite asserts the property
structurally** (via `-X importtime`), not by trusting the source.

## 4. Mainline polars is retained

Per the B6 decision (spec 012 rev 3): **mainline `polars 1.27.1`**. `polars-lts-cpu` is
**not installed and not evaluated in this campaign**. The AVX2 masking is accepted residual
risk, and it applies to any deployment venv on VM24 exactly as it does today.

**No dependency version is changed by this spec.**

## 5. Where the venv lives

For `pm2C`: **inside that run's own staging tree**, at `<staging>/dev2026/.venv`, built by
`uv sync` with the pinned interpreter — the same shape `pm2B` used and proved.

**It is not installed into production, not placed in a shared location, and not reused
between runs.** A later production deployment would need its own venv location decided as
part of the cutover, which is spec 011's business and is not settled here.

## 6. Boundaries

**Done:** the isolated-venv decision and its reasons, the pinned interpreter, the
complete-manifest requirement with CORE/FULL rules, and `deploy/record_manifest.py`.

**Not done, not authorised:** no venv built, no `uv sync` run anywhere but this
repository's own existing `.venv`, no package installed or upgraded, no dependency version
changed, no VM24 action, no PM2, no production change.
