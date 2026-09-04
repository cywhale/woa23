# B7 — production interpreter dependency check: result

**Executed 2026-08-20 on VM24 (`odb24`) under the authorisation of the same date.**

**Observation: all eight modules are present in production's `py311` interpreter, at
versions identical to the validated venv's.**

**This is a dependency-check observation and nothing more.** It is **not** a PM2 result,
**not** a deployment validation, **not** a performance or correctness result, and **not**
production approval. **B1–B5 remain open. B6 remains an open decision. B7 is answered but
not closed** — see §5.

---

## 1. Verification before execution

| item | authorised | verified |
|---|---|---|
| protocol request commit | `040ee63` | `040ee63594f5ad4dc7820f205f53e93a076766f0`, contains the request file |
| execution subject | `18080700a9b602b6c68d87350b694e756158143b` | re-derived, 16/16 |
| archive SHA-256 | `06b0183eec501f31ae0b2428ff9636fcb0bcb5dd4f7ceeaaccf4020c2967815b` | **exact** |
| file count | `149` | **149** |
| file-list SHA-256 | `ba4b07a3151d4c392833fa1fc84def479fb7370df41fb06b800d0c66067a6997` | **exact** |

**Nothing from the archive was shipped or executed**, exactly as the request states. No
file was transferred to VM24. The subject identifies the state of the work this result
attaches to.

## 2. The command, and its exit

The single command of request §4 was run **once**, unmodified, over `ssh vm24` with
`BatchMode=yes` and the configured key. **Exit code 0.**

### stdout — 11 lines, complete and unedited

```
interpreter  : /home/odbadmin/.pyenv/versions/py311/bin/python3.11
version_info : 3.11.4
version      : 3.11.4 (main, Jul 25 2023, 10:21:50) [GCC 9.4.0]
polars     1.27.1
orjson     3.11.4
fastapi    0.115.12
uvicorn    0.34.1
gunicorn   23.0.0
dask       2025.3.0
xarray     2025.3.1
zarr       2.18.6
```

### stderr — 0 bytes

**Empty.** In particular **no polars AVX2 warning**, which is the direct evidence that
`find_spec` located `polars` without importing it. Had the command imported anything, that
warning would be here — it appears on every real import of polars on this host.

## 3. Presence and version, stated as the authorisation requires

| module | present or absent | distribution version |
|---|---|---|
| `polars` | **present** | 1.27.1 |
| `orjson` | **present** | 3.11.4 |
| `fastapi` | **present** | 0.115.12 |
| `uvicorn` | **present** | 0.34.1 |
| `gunicorn` | **present** | 23.0.0 |
| `dask` | **present** | 2025.3.0 |
| `xarray` | **present** | 2025.3.1 |
| `zarr` | **present** | 2.18.6 |

**Interpreter:** `/home/odbadmin/.pyenv/versions/py311/bin/python3.11`, **Python 3.11.4**,
built 2023-07-25 with GCC 9.4.0 — the same interpreter production's own workers run
(`/proc/4296/exe` → `.pyenv/versions/3.11.4/bin/python3.11`).

## 4. The eight versions match the validated venv exactly — and what that is worth

| module | `py311` (production) | `dev2026/.venv` (every validated run) |
|---|---|---|
| `polars` | 1.27.1 | 1.27.1 |
| `orjson` | 3.11.4 | 3.11.4 |
| `fastapi` | 0.115.12 | 0.115.12 |
| `uvicorn` | 0.34.1 | 0.34.1 |
| `gunicorn` | 23.0.0 | 23.0.0 |
| `dask` | 2025.3.0 | 2025.3.0 |
| `xarray` | 2025.3.1 | 2025.3.1 |
| `zarr` | 2.18.6 | 2.18.6 |

**All eight identical. That is a stronger result than the check was designed to get, and it
is still not runtime equivalence.**

Eight named packages agreeing says nothing about the rest of either environment. **This
campaign has already been caught by exactly that distinction:** in S1 the twelve *pinned*
packages matched on both arms while **23 shared transitive dependencies did not**,
including `fsspec` and `anyio` — which is why that A/B was recorded as directional evidence
rather than a measured Dask-only speedup.

**So the correct reading is:** the eight dependencies the candidate needs are present at
the versions it was validated with. **Not:** `py311` and the venv are the same environment.
Establishing that would need a full distribution comparison, which was not authorised and
is not proposed here.

## 5. What this answers, and why B7 does not close

**Answered:** production's interpreter would not fail to import `api.app` for want of
`polars`, `orjson`, `fastapi` or `uvicorn`. The failure mode B7 exists to prevent — PM2
reports the app started, then gunicorn dies at import — **is not present for these
modules.**

**Not answered, so B7 stays OPEN:**

1. **No install decision is settled.** The request said in advance that a "present" result
   makes installing into `py311` *possible*, not *advisable*. What it actually shows is
   better than that: **no install is needed at all** for these eight. But the deployment
   runtime — shared `py311` versus a deployed venv — is still a decision, and it is the
   PI's.
2. **`py311` is shared.** `mhwapi`, `ghrsst_mcp`, `tide` and production's own `woa23` all
   run from it. Something else on this host installed `polars` there; a future change by
   another project could remove or move it, and nothing in this campaign would notice.
   That is the rollback and drift risk spec 011 §4 and §7 describe, and it is unchanged by
   a favourable reading today.
3. **The transitive set is unmeasured** (§4).
4. **This result cannot be read as a runtime PASS.** The candidate has never actually been
   *run* from `py311` — not once, in any run of this campaign.

**My recommendation is unchanged, and I said in the request it would be regardless of the
result: deploy a venv and point `WOA23_PYTHON` at it.** It is the configuration every
validated run used, it is unaffected by another project's package changes, and it keeps a
rollback from having to reason about a shared environment. Today's result makes the
alternative *workable*; it does not make it *preferable*.

## 6. A consequence for B6, which is new

**`polars` is present in `py311` at 1.27.1 — the mainline build.** So if the candidate ever
runs from that interpreter, **the AVX2 warning of spec 012 (B6) appears in production**,
with the same unquantified SIGILL risk, on the same masked CPU.

B6 was already open. This makes its scope concrete: it is not only a question about the
`dev2026` venv, it is a question about the interpreter production would actually use.
**B6 remains an open decision and nothing here pre-empts it.** `polars-lts-cpu` is **not**
installed anywhere.

## 7. Production before and after — identical

**0 HTTP requests.** Everything from `/proc`, `ss` and a read-only `pm2 jlist`.

| | before | after |
|---|---|---|
| boot id | `0b513a75-213b-40bf-8219-1c7cbc51a085` | **identical** |
| God Daemon 3459 | started 2026-08-14 13:25:09 | **identical** |
| 4296 / 5040 / 5041 starttime | 14214 / 15825 / 15829 | **identical** |
| 4357 / 4358 starttime | 14323 / 14330 | **identical** |
| every `exe` | `.pyenv/versions/3.11.4/bin/python3.11` | **identical** |
| listeners 8050 / 8786 / 8787 | present, same PIDs | **identical** |
| PM2 `woa23` | `online`, pid 4295, restarts 0 | **identical** |
| PM2 `dask-scheduler` / `dask-worker` | `online`, restarts 0 | **identical** |
| staging/candidate app in production PM2 | absent | **absent** |

**No production state changed.**

### Nothing was created, and nothing survives

- `~/woa23-b7a` and `~/woa23-b7a-work`: **absent** — the check creates no staging tree or
  workdir, as the request specifies;
- **no ledger entry** — nothing was bound;
- **no leftover process.** An intermediate reading said "2 processes still running"; that
  was **my own `ps | grep` command line matching itself**, the same self-match that
  appeared twice during `bash5A`. Re-checked with the pattern split: **none**;
- **God Daemons: exactly 3** — production's 3459 and the two retained staging daemons
  1242814 (`pm2A`) and 1248938 (`pm2B`), **untouched**. An intermediate count of "6" was
  the same self-matching artefact;
- the 16 local `arm.py` strays are on the **development machine**, are not VM24 evidence,
  and were **not touched**, per the authorisation.

## 8. Classification

**B7 dependency check — OBSERVATION RECORDED. All eight modules present at matching
versions. B7 remains OPEN pending the deployment-runtime decision.**

**Not** a PM2 result. **Not** a deployment validation. **Not** performance. **Not**
correctness. **Not** production approval. **Not** an installation recommendation.

**Out of scope and untouched:** `pm2C`, the B1–B5 cutover, `polars-lts-cpu` installation,
production deployment, and the retained-daemon cleanup.

## 9. Evidence

`scratchpad/b7A/` — `01-before.txt`, `02-stdout.txt`, `02-stderr.txt` (0 bytes),
`03-after.txt`, `04-verify.txt`.
