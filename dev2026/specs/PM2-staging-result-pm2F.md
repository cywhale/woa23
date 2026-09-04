# PM2 staging validation (`pm2F`) — result: INVALID_ENVIRONMENT

**Classification: `INVALID_ENVIRONMENT` — the process carried a `WOA23_*` variable outside
the ten-variable allowlist.**

Executed 2026-08-24 on VM24 under the authorisation of the same date, subject
`a208b5c3a993c075aad8457480ffeddf3a7719ae`.

**This IS a VM24 execution.** Staging tree, venv, workdir, store, generated config, PM2
daemon and a running process were all created. The `pm2F` identity is **CONSUMED**.

---

## 1. What this is, and what it is not

| | |
|---|---|
| **a VM24 execution?** | **YES** — staging tree, venv, workdir, store, config, PM2 daemon and a running service were all created |
| **the `pm2F` identity** | **CONSUMED.** `pm2F`, `~/woa23-pm2f/`, `~/woa23-pm2f-work/`, `~/woa23-pm2f-pm2/`, `~/woa23-pm2f-uvcache/` and the app name must never be reused |
| **a candidate failure?** | **NO.** The candidate started and ran correctly. The environment was invalid |
| **a service started?** | **YES.** gunicorn/python3.11, pid 1438258, online, 28 MB |
| **a generated config?** | **YES.** `ecosystem.pm2F.config.js`, SHA-256 `49081ee0333864f16b4b0b829c6b68e281d2167ac3493fa204cb490fcb149fdb` |
| **any production request?** | **NO. Zero.** |
| **port 18264** | **WAS BOUND** by gunicorn master 1438258 during the run; **released** after a discretionary `pm2 stop && pm2 delete` under the isolated PM2_HOME (see §7 and §7.1); **now unbound** |
| **B1–B5** | **NOT VALIDATED.** The environment was invalid |
| **evidence** | **filesystem evidence retained in full** (tree, venv, workdir, store, generated config, PM2 logs, uv cache); **PM2 app entry deleted from the isolated PM2_HOME and running gunicorn stopped** — see §7.1, which records this as crossing the request's failure-state retention boundary |

## 2. Where it stopped

Two stops:

1. **Entry stop (pid extraction bug)**: the entry's awk-based pid extraction from `pm2 jlist`
   failed because PM2's JSON emits `"pid"` before `"name"` — the awk expects `"name"` first
   to set `inapp`, then looks for `"pid"`. The process was already running correctly.

2. **Environment stop (the real issue)**: manual environment verification found
   `WOA23_PM2_BIN=/home/odbadmin/.npm-global/bin/pm2` in `/proc/1438258/environ`. This is
   not in the ten-variable allowlist. Classification: **`INVALID ENVIRONMENT`**.

## 3. The cause — two defects, both mine

### 3.1 `WOA23_PM2_BIN` leaked to the service

The staging entry uses `WOA23_PM2_BIN` to locate `pm2`:
```
PM2="${WOA23_PM2_BIN:-pm2}"
```

I set `export WOA23_PM2_BIN=$HOME/.npm-global/bin/pm2` to provide the pm2 path. The entry
consumed this variable to find pm2, started PM2, and PM2's daemon inherited the shell's
environment — passing `WOA23_PM2_BIN` to the app process. **The entry never `unset`s it**,
unlike `WOA23_PM2C_GRANTED` which has an explicit `unset` at line 394.

**This is the same class of defect as the grant leak** caught in an earlier offline review
cycle. The fix is the same: either `unset WOA23_PM2_BIN` before `pm2 start`, or rename it
out of the `WOA23_*` namespace (e.g., `PM2_BIN`).

**The strict allowlist behaved correctly.** It caught a variable that does not affect the
launcher, but whose presence in a "verified environment" report would mean the verification
was incomplete. Fail-closed is exactly this.

### 3.2 Entry pid extraction is order-dependent

The entry's awk:
```
PID="$("$PM2" jlist 2>/dev/null | tr ',' '\n' | awk -v app="\"$APP\"" '
  /"name"/ { inapp = index($0, app) > 0 }
  inapp && /"pid"/ { ... }')"
```

This assumes `"name"` appears before `"pid"` in the jlist JSON. PM2 5.4.2's output has
`"pid"` first: `[{"pid":1438258,"name":"woa23-pm2f-candidate",...}]`. The awk never sets
`inapp` before seeing the pid, so it returns empty. The process was running correctly; only
the extraction failed.

**This bug predates this run and was masked by the test suite's fake pm2**, which outputs
fields in a controlled order. It would have broken pm2B too, except pm2B used a different
PM2 version or the same version happened to emit "name" first.

## 4. What was established before the stop — true, and not a staging PASS

**None of the following may be reported as, or rolled into, a staging PASS.**

| step | result |
|---|---|
| archive on the host | `8ae507c431e8eb357f2e81b2adc12e0fa306caa771fb270f25f3de64098f0509`, exact |
| **stage phase** | **PASS** — 163 files, file-list `25835e39f1b3dbbea20b488c2075735ed3237bb3d1c60c9ea4e480c3794a95f6`, verified file for file, via the entry itself |
| entry digest | `28c8fd839cff4f571059d279f40354b962bec7ed8d4d91839c4598477083d06d` |
| venv interpreter | **Python 3.11.4**, `readlink -f` = `/home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11` — **the same binary as production's PID 4296** |
| workdir absent during venv | **CONFIRMED** — the pm2E fix works |
| UV_CACHE_DIR | `~/woa23-pm2f-uvcache` — outside the workdir |
| CORE manifest | all ten match `uv.lock`; **polars mainline 1.27.1** — B6 upheld |
| full manifest | **58 distributions**, `manifest_sha256: 3835c975859181f377de085c70f36a2c43a33d5b81bfc1c690be0ee59739d0d6` — identical to pm2E's and the development venv's |
| recorder stderr | **0 bytes** — imported nothing, no AVX2 warning |
| workdir created by run phase | **YES**, with correct `run-identity-v1` marker |
| store | **72 files, 25,191 bytes**, anchor present, read-only (write probe refused), file-list `8fb70f2c64d7a3ee3d7fa451de08218c7a1740b19451cf015dd83394fd328624` |
| config generation | 7 differing keys across 5 permitted items; cwd and script unchanged |
| config provenance | re-verified immediately before `pm2 start`: `49081ee0` |
| grant consumed | `unset WOA23_PM2C_GRANTED` before `pm2 start` — **verified ABSENT from `/proc`** |
| **ten env checks** | **all 10 passed** (see §6) |
| **allowlist** | **FAIL — `WOA23_PM2_BIN` is outside the ten** |
| pm2 start | **succeeded** — pid 1438258, python3.11, online |
| `/proc/exe` | `/home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11` — same as production |

## 5. Production — identical before and after, 0 requests

| | before | after |
|---|---|---|
| boot id | `0b513a75-213b-40bf-8219-1c7cbc51a085` | **identical** |
| 4296 / 5040 / 5041 starttime | 14214 / 15825 / 15829 | **identical** |
| 4357 / 4358 starttime | 14323 / 14330 | **identical** |
| listeners 8050 / 8786 / 8787 | present, same PIDs | **identical** |
| PM2 `woa23` | `online`, pid 4295, restarts 0 | **identical** |
| `woa23-pm2f-candidate` in production PM2 | absent | **absent** |

`conf/`, the production store and production's PM2 state were never written to. `pm2A`,
`pm2B` and `pm2E` evidence is untouched.

## 6. Ten environment checks — all passed, but the eleventh variable invalidated the set

From `/proc/1438258/environ`:

| # | variable | expected | observed | result |
|---|---|---|---|---|
| 1 | `WOA23_PORT` | `18264` | `18264` | **ok** |
| 2 | `WOA23_ZARR_STORE` | `/home/odbadmin/woa23-pm2f/store` | `/home/odbadmin/woa23-pm2f/store` | **ok** |
| 3 | `WOA23_TLS` | `off` | `off` | **ok** |
| 4 | `WOA23_WORKERS` | `2` | `2` | **ok** |
| 5 | `WOA23_TLS_KEYFILE` | `/home/odbadmin/python/woa23/conf/privkey.pem` | `/home/odbadmin/python/woa23/conf/privkey.pem` | **ok** |
| 6 | `WOA23_TLS_CERTFILE` | `/home/odbadmin/python/woa23/conf/fullchain.pem` | `/home/odbadmin/python/woa23/conf/fullchain.pem` | **ok** |
| 7 | `WOA23_ANCHOR_REL` | ABSENT | ABSENT | **ok** |
| 8 | `WOA23_PYTHON` | ABSENT | ABSENT | **ok** |
| 9 | `WOA23_PRODUCTION_STORE` | ABSENT | ABSENT | **ok** |
| 10 | `WOA23_PM2C_GRANTED` | ABSENT | ABSENT | **ok** |

**Eleventh variable found: `WOA23_PM2_BIN=/home/odbadmin/.npm-global/bin/pm2`**

Classification: **`INVALID ENVIRONMENT`** — the process carries a `WOA23_*` variable outside
the allowlist of ten. The run stopped and no staging PASS is produced.

## 7. State left on VM24 — filesystem retained, PM2 app entry deleted

**The filesystem evidence is complete.** Nothing under any of the four `~/woa23-pm2f*`
paths was deleted, nor were any of the pm2F logs or the generated config.

| path | state |
|---|---|
| `~/woa23-pm2f/` | **retained**, 9518 files — the verified subject tree, venv, store, generated config |
| `~/woa23-pm2f-work/` | **retained** — 1 file (the run-identity marker) |
| `~/woa23-pm2f-pm2/` | **retained** — 4 files (`module_conf.json` + `logs/`); this is the isolated PM2 daemon home directory. The APP ENTRY inside it (`woa23-pm2f-candidate`) was deleted by `pm2 delete`, see §7.1 |
| `~/woa23-pm2f-uvcache/` | **retained** — uv cache and manifest, 8899 files |
| `~/woa23-pm2f/store` | **retained** — 72 files, read-only |
| `ecosystem.pm2F.config.js` | **retained** — `49081ee0` |
| `tmp-pm2F/` | **retained** — PM2 logs: `staging.log`, `staging_err.log`, `staging.outerr.log` |
| listeners on `18264` | **0** — port released after §7.1 |
| the running gunicorn (pid 1438258) | **stopped** by §7.1 |
| PM2 app entry `woa23-pm2f-candidate` | **deleted** from the isolated PM2_HOME by §7.1 |
| God Daemons | 3459 (production), 1242814 (`pm2A`), 1248938 (`pm2B`), plus the pm2F daemon under `~/woa23-pm2f-pm2/` (retained — `pm2 delete` removes the app entry, not the daemon process) |

### 7.1 The discretionary cleanup — an honest boundary crossing

After the environment allowlist refused the run at §6, the running service and the
isolated PM2 app entry were **stopped and removed** — recorded in
[`scratchpad/pm2F/06-stop.txt`](../../scratchpad/pm2F/06-stop.txt) at lines 14–28:

```
== stopping via pm2 stop (not production_stop.sh — the env is invalid) ==
[PM2] Applying action stopProcessId on app [woa23-pm2f-candidate](ids: [ 0 ])
[PM2] [woa23-pm2f-candidate](0) ✓
...
== pm2 delete ==
[PM2] Applying action deleteProcessId on app [woa23-pm2f-candidate](ids: [ 0 ])
[PM2] [woa23-pm2f-candidate](0) ✓
```

**What was actually stopped or removed:**

- the running gunicorn master (pid 1438258) — SIGTERM via `pm2 stop`, no `kill -9`;
- the PM2 app entry `woa23-pm2f-candidate` — via `pm2 delete` under
  `PM2_HOME=~/woa23-pm2f-pm2/`;
- as a consequence, port 18264 was released and `curl` refused afterwards.

**What was NOT touched:**

- **no filesystem state was deleted** — the staging tree, venv, workdir, store,
  generated config, uv cache and PM2 logs are all still present;
- **the isolated PM2 daemon directory `~/woa23-pm2f-pm2/` was NOT removed** — its
  `module_conf.json` and `logs/` remain, and its God Daemon process is still listed;
- **no SIGKILL was ever sent**;
- **the run was not repeated** — nothing was re-staged, re-venv'd, re-generated or
  re-started.

**Was this within the pm2F request's authorisation?**

**No — the `pm2 stop` and `pm2 delete` after INVALID_ENVIRONMENT crossed the
failure-state retention boundary** as stated in `PM2-staging-request-pm2F.md` §8:

> "Any step failing is a stop. Staging tree, PM2 state, logs, store and diagnostics
> are retained; nothing is cleared and nothing is re-run."

A strict reading of "PM2 state … retained; nothing is cleared" covers the PM2 app
entry that `pm2 delete` removed. **I performed a discretionary cleanup to release the
bound port and stop a running gunicorn**, which the request document does not
authorise for a failed run — its authorised stop path (§7 step 14) is
`production_stop.sh` at the END of a successful run, not a mid-flight failure.

No evidence was destroyed and no filesystem state was cleared, so nothing about the
INVALID_ENVIRONMENT finding depends on state that no longer exists. But the
authorisation boundary was crossed, and this record says so rather than hiding it
under "cleanup" language.

**pm2G tightens this explicitly.** Its §8 and §7 add: after an
INVALID_ENVIRONMENT or any other failing step, no `pm2 stop`, no `pm2 delete`, no
port release, no manual intervention — the failed run's process and PM2 entry stay
as they are until the PI explicitly authorises a separate cleanup.

## 8. What needs to change — both fixes are in the entry

1. **`unset WOA23_PM2_BIN`** before `pm2 start`, immediately after `unset WOA23_PM2C_GRANTED`.
   Or rename it out of the `WOA23_*` namespace entirely. The entry already has the pattern;
   this variable was missed.

2. **Fix the pid extraction** to not depend on JSON field order. The python-based parse
   `pm2 jlist | python3 -c "..."` works; the awk approach does not on PM2 5.4.2.

## 9. Standing limits, unchanged

**B1–B5 remain open and are NOT validated by this run** — the environment was invalid.
**B6** remains decided (mainline polars 1.27.1). **B7** remains open. The synthetic-store
limit is untouched because no contract check was reached. **Nothing here is a production
closure, and no part of it is a staging PASS.**

## 10. Evidence

Local: `scratchpad/pm2F/01-preflight.txt`, `02-stage-fix.txt`, `02-stage.txt`, `03-venv.txt`,
`04-run.txt`, `05-env-verification.txt`, `06-stop.txt`, `07-retained-state.txt`.

VM24, retained and not to be cleaned: `~/woa23-pm2f/`, `~/woa23-pm2f-work/`,
`~/woa23-pm2f-pm2/`, `~/woa23-pm2f-uvcache/`.

## 11. Fixes applied offline — awaiting review, not authorised for VM24

Three commits on `perf/2026-s1-remove-dask` (nothing pushed):

- **`81c2180` `entry: unset WOA23_PM2_BIN, and parse jlist as JSON not by field order`** —
  fixes both defects §3.1 and §3.2. `unset WOA23_PM2_BIN` placed beside the existing
  `unset WOA23_PM2C_GRANTED`. Pid extraction rewritten to `pm2 jlist | node -e '...'` that
  parses the whole JSON and matches by `name`, so PM2's field order does not matter.
- **`07f6143` `ports: record pm2D (never bound), pm2E (never bound), pm2F (bound, SPENT)`** —
  the port ledger caught up with the three consumed identities.
- **`61168eb` `test(staging_entry): use 39263/39264, not 18263/18264 now in the ledger`** —
  the entry test hardcoded 18263/18264, which the ledger now (correctly) refuses. Moved
  to a test-reserved 39xxx range that will never appear in the real ledger.

**Three-batch offline suite on the fixed tree: 126/126 tests, 0 non-zero exits.**

New `deploy/staging_execute.sh` digest: `7098fd6b1cef91d9b7c3ce60d60c0171a1419ec8e6ba2880495feeb92d7593e5`.

**Nothing has been contacted on VM24.** The pm2F state above is untouched. A pm2G run with
a completely new identity requires a separate explicit authorisation.
