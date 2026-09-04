# PM2 staging validation (`pm2E`) — result: INVALID_PRE_START

**Classification: `INVALID_PRE_START` — a harness / pre-start lifecycle failure.**

Executed 2026-08-24 on VM24 under the authorisation of the same date, subject
`a54ec9eac3efe0f554e14c5980fb476a59e19d11`.

**This IS a VM24 execution**, and it is the first of the pm2C/pm2D/pm2E series that is.
`pm2C` and `pm2D` were stopped before any VM24 contact; **this one created state on the
host** and therefore consumed its execution identity.

---

## 1. What this is, and what it is not

| | |
|---|---|
| **a VM24 execution?** | **YES** — staging tree, venv and workdir were created on the host |
| **the `pm2E` identity** | **CONSUMED.** `pm2E`, `~/woa23-pm2e/`, `~/woa23-pm2e-work/`, `~/woa23-pm2e-pm2/` and the app name must never be reused |
| **a candidate failure?** | **NO.** Nothing about `api.app`, the launcher, the config or the contract was reached |
| **a service started?** | **NO.** No gunicorn, no PM2 daemon, no app |
| **a generated config?** | **NO.** Never generated |
| **any production request?** | **NO. Zero.** |
| **port 18263** | **NEVER BOUND.** It does **not** enter `scripts/ports_used.tsv` |
| **B1–B5** | **NOT VALIDATED.** The run never reached the launcher |
| **evidence** | **retained in full; nothing cleaned, nothing re-run** |

## 2. Where it stopped

```
PM2C-mode staging validation — phase RUN
  root      : /home/odbadmin/woa23-pm2e   (present, and verified as the authorised subject)
  provenance: 161 files, file-list 90a53cbcbff36ccf7c5fd8aa2c18a14b4a2ef13594a2ee82ad9053b0f4b2e41d
REFUSING: /home/odbadmin/woa23-pm2e-work already exists.
  This run creates it, so its presence means a previous attempt got this far.
  It is NOT deleted or reused — that would destroy the evidence of what happened.
RUN EXIT=2
```

## 3. The cause — mine, and the same class as `pm2D`

**The workdir was created by the venv step, by me**, which pointed `UV_CACHE_DIR` at
`~/woa23-pm2e-work/uv-cache`. The run phase then required that directory to be absent.

**This is the same defect as `pm2D`: a freshness check whose moment does not match the real
sequence.** In `pm2D` it was the staging root — created by extraction, then required absent
at run time. Here it is the workdir — created during the venv build, then required absent
at run time. When that entry was rewritten, the root check was moved and **the workdir check
was left where it was**.

The offline suite did not catch it for the same reason it missed the first one: its `run`
cases never populate a workdir before invoking the entry. A new blind spot in the same
shape.

**The guard behaved correctly.** It refused, named the path, and deleted nothing. The fault
is the lifecycle around it, not the refusal.

## 4. What was established before the stop — true, and not a staging PASS

**None of the following may be reported as, or rolled into, a staging PASS.**

| step | result |
|---|---|
| archive on the host | `d439e01f9ded94f141058921810f3650fbf90b0cb11dc1e5e042a7c297ec083f`, exact |
| **stage phase** | **PASS** — 161 files, file-list `90a53cbcbff36ccf7c5fd8aa2c18a14b4a2ef13594a2ee82ad9053b0f4b2e41d`, verified file for file |
| shipped entry vs staged copy | identical, `a75ffc2968ba2fdd0e00079c9a1e310be75bb921b92e860d0b68e018be8c6ae4` |
| ledger | `18263` absent from the staged `ports_used.tsv` |
| venv interpreter | **Python 3.11.4**, `readlink -f` = `/home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11` — **the same binary as production's PID 4296** |
| CORE manifest | all ten match `uv.lock`; **polars mainline 1.27.1** — B6 upheld, `polars-lts-cpu` not installed |
| full manifest | **58 distributions**, `3835c975859181f377de085c70f36a2c43a33d5b81bfc1c690be0ee59739d0d6` — identical to the development venv's digest, so **no platform differences to explain** |
| manifest recorder stderr | **0 bytes** — it imported nothing, and no AVX2 warning |

**A correction to an earlier report of mine:** I said "60 distributions". That was a bad
`grep -c "=="` counting two `==` section headers. The count is **58**.

## 5. State left on VM24 — retained, nothing deleted

| path | state |
|---|---|
| `~/woa23-pm2e/` | **present**, 8774 files — the verified subject tree |
| `~/woa23-pm2e/dev2026/.venv` | **present** — Python 3.11.4 |
| `~/woa23-pm2e-work/` | **present** — `uv-cache/`, `manifest-full.txt` |
| `~/woa23-pm2e/store` | **absent** — never built |
| `~/woa23-pm2e-pm2/` | **absent** — no daemon was ever created |
| `deploy/ecosystem.pm2E.config.js` | **absent** — never generated |
| listeners on `18263` | **0** |
| pm2E processes | **none** |
| God Daemons | 3459 (production), 1242814 (`pm2A`), 1248938 (`pm2B`) — **no new daemon** |

## 6. Production — identical before and after, 0 requests

| | before | after |
|---|---|---|
| boot id | `0b513a75-213b-40bf-8219-1c7cbc51a085` | **identical** |
| 4296 / 5040 / 5041 starttime | 14214 / 15825 / 15829 | **identical** |
| 4357 / 4358 starttime | 14323 / 14330 | **identical** |
| listeners 8050 / 8786 / 8787 | present, same PIDs | **identical** |
| PM2 `woa23` | `online`, pid 4295, restarts 0 | **identical** |
| `woa23-pm2e-candidate` in production's PM2 | absent | **absent** |

`conf/`, the production store and production's PM2 state were never written to. `pm2A` and
`pm2B` evidence is untouched.

## 7. Standing limits, unchanged

**B1–B5 remain open and are NOT validated by this run** — the launcher was never started.
**B6** remains decided (mainline polars 1.27.1). **B7** remains open. The synthetic-store
limit is untouched because no store was built. **Nothing here is a production closure, and
no part of it is a staging PASS.**

## 8. Evidence

Local: `scratchpad/pm2E/01-preflight.txt`, `02-stage.txt`, `03-venv.txt`, `04-run.txt`,
`05-stopped-state.txt`.

VM24, retained and not to be cleaned: `~/woa23-pm2e/`, `~/woa23-pm2e-work/`.
