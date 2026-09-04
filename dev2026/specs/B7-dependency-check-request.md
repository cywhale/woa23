# B7 — production interpreter dependency check: formal authorisation request

**Status: REQUESTED, NOT GRANTED. Nothing here has been run.** No VM24 command has been
issued for this check, nothing installed, nothing modified. **This document is not an
authorisation.**

**It is a read-only question asked of one interpreter.** Nothing is started, nothing is
installed, no service is touched, and no HTTP request is sent.

---

## 1. The question, and why it blocks a cutover

**Does the interpreter that would serve production actually have the candidate's
dependencies?**

Production runs `woa23_app:app` from `/home/odbadmin/.pyenv/versions/py311`, whose
dependency set is `dask`, `xarray` and `zarr`. The candidate `api.app:app` needs **`polars`,
`orjson`, `fastapi`** and `uvicorn`. **Nothing in this campaign has ever run the candidate
from that interpreter** — `c1f`, `c2g`, `s2pB`, `pm2B` and `bash5A` all used a
`dev2026/.venv` built by `uv sync`.

If `py311` lacks them, `production_app.sh` starts, gunicorn imports `api.app`, and the
service **fails at import — after PM2 has reported it started.** That is the failure this
check exists to find before a cutover rather than during one.

**B7 is a hard prerequisite, like B2, B3 and B4.** It was introduced in spec 011 §4 as
"B6" and renumbered when the PI assigned B6 to the AVX2 decision (spec 012 §0).

## 2. Execution subject — and what it does and does not mean here

| item | value |
|---|---|
| commit | `18080700a9b602b6c68d87350b694e756158143b` |
| archive SHA-256 | `06b0183eec501f31ae0b2428ff9636fcb0bcb5dd4f7ceeaaccf4020c2967815b` |
| file count | `149` |
| file-list SHA-256 | `ba4b07a3151d4c392833fa1fc84def479fb7370df41fb06b800d0c66067a6997` |
| verifier | `verify_clean_archive.sh` 16/16 |
| offline evidence | **three independent serial batches, 39/39 each, 0 non-zero, 0 failing assertions** |

**Nothing from this archive is shipped or executed.** The check runs a single command
against an interpreter that already exists on the host. The subject identifies **the state
of the work this result attaches to** — it is not code being deployed, and no file from it
reaches VM24.

That is a deliberate difference from `pm2B` and `bash5A`, both of which shipped and ran a
tree. Recording it here prevents the result from later being read as "the subject was
validated on the host".

## 3. Identity

| | |
|---|---|
| label | **`b7A`** |
| staging | **none** — nothing is unpacked |
| workdir | **none** — nothing is written on VM24 |
| PM2_HOME | **none** — PM2 is not invoked |
| port | **none** |
| ledger | **no entry** — nothing is bound |

**Nothing is created on VM24 by this check.** Its evidence is the command's stdout, kept
locally.

## 4. The exact command

**One command, run once, exactly as written.** An earlier draft of this request used two —
a presence check and then a version check — and the second was written awkwardly enough
that it was hard to read and therefore hard to authorise. This is the tested replacement:

```
/home/odbadmin/.pyenv/versions/py311/bin/python3.11 -c "
import importlib.util as u, importlib.metadata as md, sys
print('interpreter  :', sys.executable)
print('version_info :', '%d.%d.%d' % sys.version_info[:3])
print('version      :', sys.version.replace(chr(10), ' '))
for m in ('polars','orjson','fastapi','uvicorn','gunicorn','dask','xarray','zarr'):
    if u.find_spec(m) is None:
        print('%-10s ABSENT' % m); continue
    try: v = md.version(m)
    except Exception as e: v = 'present, version unreadable (%s)' % type(e).__name__
    print('%-10s %s' % (m, v))
"
```

**Verified locally before being proposed**, against this project's own interpreter. It
prints the interpreter path, version and version_info, then one line per module. A module
that is not installed prints `ABSENT` — checked with a deliberately fake module name, which
reported `ABSENT` as required. A module whose metadata cannot be read prints `present,
version unreadable (…)` rather than raising, so a metadata problem cannot be mistaken for
an absence.

**It emitted no AVX2 warning**, which is the direct evidence that `find_spec` does not
import `polars`.

**Why `importlib.util.find_spec` and `importlib.metadata` rather than `import`:**
`find_spec` **locates** a module without executing it. Importing `polars` on this host
**emits the AVX2 warning and initialises the library** (spec 012); importing `fastapi` or
`dask` pulls in large dependency graphs. This check must answer "is it installed?" without
running anything, and `find_spec` is exactly that question. `importlib.metadata` reads
installed **distribution metadata from disk** — it does not import the package either.

**It also uses production's interpreter without touching production's service.** Running
`python3.11 -c` starts a short-lived process that exits immediately; it does not signal,
attach to, or share state with PIDs 4296 / 5040 / 5041.

## 5. Before and after — the same read-only snapshot as `pm2B` and `bash5A`

Taken **before** and **after**, and compared:

- `/proc/sys/kernel/random/boot_id`;
- PM2 God Daemon **3459** — presence and start time;
- **4296, 5040, 5041** (gunicorn) and **4357, 4358** (dask): existence, `starttime`
  (field 22 of `/proc/<pid>/stat`), and `readlink /proc/<pid>/exe`;
- listeners on **8050 / 8786 / 8787** via `ss -ltnp`;
- production's PM2 app list, read under `PM2_HOME=/home/odbadmin/.pm2` — **read only**, no
  action.

**Any difference stops the run and is reported as-is.**

## 6. Forbidden

- **no install, no upgrade, no `pip`, no `uv`, no venv creation or modification** — not to
  `py311`, not anywhere;
- no service, PM2, gunicorn or Dask started; no `pm2 start/stop/restart/delete`, no
  `pm2 * all`, no global `save`/`resurrect`;
- **no HTTP request** to any port, production or otherwise;
- production store not read, written or listed;
- `conf/` not read as input to a change and never modified;
- no `polars-lts-cpu` — **B6 is an open decision and this check does not pre-empt it**;
- `pm2A`, `pm2B` and `bash5A` evidence not cleared, moved or reused;
- retained PM2 daemons **1242814** and **1248938** not touched — their cleanup is a
  **separate** item;
- no SIGKILL; no self-rerun.

## 7. What each outcome would mean

| result | meaning | consequence |
|---|---|---|
| all four **present** | `py311` could import `api.app` | B7 is answerable, but **not automatically closed** — versions must still be compatible, and a shared interpreter remains a risk at rollback (spec 011 §7) |
| any **ABSENT** | production's interpreter **cannot run the candidate** | B7 stays open and the deployment-runtime decision of spec 011 §4 must be taken: install into `py311`, or **deploy a venv** and point `WOA23_PYTHON` at it |

**My recommendation is unchanged and independent of the result: deploy a venv.** It is the
configuration every validated run actually used, and it keeps a rollback from having to
undo package installs in a shared environment. A "present" answer would make installing
into `py311` *possible*, not advisable.

**This check does not close B7 by itself.** It answers one factual question; the runtime
decision that follows is the PI's, and it needs its own record.

## 8. What a result will NOT mean

Not a cutover approval. Not a deployment validation. Not a performance or correctness
result. **B1–B5 remain open** — none is installed or host-validated. **B6 remains an open
decision.** `pm2C`, the staging validation of the production launcher, is **not requested
here** and is deliberately not bundled with this.
