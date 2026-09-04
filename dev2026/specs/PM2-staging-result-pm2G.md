# PM2 staging validation (`pm2G`) — result: NOT A PASS, two findings, state RETAINED

**Classification: `INVALID_EXPECTATION` at §7 step 10 + `CONTRACT_DEFECT` at §7 step 14.**
**No staging PASS is produced.**

Executed 2026-08-24 on VM24 under the authorisation of the same date, subject
`06661fd9361b2fb48052e1e61825a41483fc1c9f`.

**The `pm2G` identity is CONSUMED. Port `18265` is SPENT.**

**THE SERVICE IS STILL RUNNING AND THE PORT IS STILL BOUND.** Nothing was stopped,
deleted, released or cleaned — this is the retention discipline pm2G's §8 was written
to enforce after pm2F violated it. **A separate explicit authorisation is required to
clean up.**

---

## 1. Headline

| | |
|---|---|
| **staging PASS?** | **NO.** Two stated criteria of the authorised sequence were not met |
| **a candidate crash?** | **NO.** The service started, served correct data, restarted cleanly and is still serving |
| **the two pm2F defects** | **BOTH FIXED AND PROVEN ON VM24** — see §3 |
| **finding 1 (step 10)** | **the isolated venv is NOT the runtime.** The launcher runs the SHARED `py311` env; the pm2G venv contributes **zero** mapped libraries |
| **finding 2 (step 14)** | **JSON field order and CSV header order are NOT stable across a restart**, contradicting the API's own OpenAPI 1.1.0 promise. Row order is unaffected and correct |
| **row-order contract** | **PASS** — 144 rows JSON, 144 CSV, strictly ascending by numeric `(time_period, depth, lat, lon)`, `13` after `2`, lon varies fastest |
| **production** | **identical before and after. ZERO requests.** |
| **B1–B5** | **NOT closed.** Step 15 (`production_stop.sh`, the B1 path) was **never reached** |
| **B7** | **NOT closed — and now shown to be further away than assumed.** See §4.4 |
| **state** | **fully retained; service running; port bound; nothing cleaned** |

## 2. Execution subject — verified exactly

| item | authorised | derived on VM24 | result |
|---|---|---|---|
| commit | `06661fd9361b2fb48052e1e61825a41483fc1c9f` | — | — |
| archive SHA-256 | `597078731597e33554c4b30bc48ea573fb843c8d6154ad2ff3f97feee6259282` | identical | **ok** |
| file count | `164` | `164` | **ok** |
| file-list SHA-256 | `00f866a9bc9a9c9c6d04f4a035f69f41bb9c05c82da5548533b626277332f6bd` | identical | **ok** |
| entry SHA-256 | `7098fd6b1cef91d9b7c3ce60d60c0171a1419ec8e6ba2880495feeb92d7593e5` | identical | **ok** |

Local `verify_clean_archive.sh 06661fd`: **16/16**. Stage phase verified the extracted
tree **file for file** against the authorised file-list.

**The request document `PM2-staging-request-pm2G.md` and every commit after `06661fd`
are protocol references and were NOT used as the subject.**

## 3. Both pm2F defects are fixed — proven on VM24, not merely offline

### 3.1 `WOA23_PM2_BIN` no longer leaks

`WOA23_PM2_BIN` was exported into the entry's environment exactly as in pm2F (pm2 is
not on VM24's default SSH `PATH`). The complete set of `WOA23_*` in the running
service:

```
WOA23_PORT=18265
WOA23_TLS=off
WOA23_TLS_CERTFILE=/home/odbadmin/python/woa23/conf/fullchain.pem
WOA23_TLS_KEYFILE=/home/odbadmin/python/woa23/conf/privkey.pem
WOA23_WORKERS=2
WOA23_ZARR_STORE=/home/odbadmin/woa23-pm2g/store
```

**Six variables. `WOA23_PM2_BIN` is ABSENT.** The `unset` works.

```
allowlist check: no WOA23_* outside the ten — environment is valid
```

### 3.2 The pid parser no longer depends on field order

```
pid: 1455874
```

PM2 5.4.2 emits `"pid"` before `"name"`; the JSON parser extracted it correctly. In
pm2F this same jlist shape produced an empty pid and killed the run.

## 4. The ten environment checks — 10/10, on both the initial and restarted process

Read from `/proc/1455874/environ` (initial) and `/proc/1456369/environ` (after restart).
**Identical results on both.**

| # | variable | expected | observed | result |
|---|---|---|---|---|
| 1 | `WOA23_PORT` | `18265` | `18265` | **ok** |
| 2 | `WOA23_ZARR_STORE` | `/home/odbadmin/woa23-pm2g/store` | identical | **ok** |
| 3 | `WOA23_TLS` | `off` | `off` | **ok** |
| 4 | `WOA23_WORKERS` | `2` | `2` | **ok** |
| 5 | `WOA23_TLS_KEYFILE` | `/home/odbadmin/python/woa23/conf/privkey.pem` | identical | **ok** |
| 6 | `WOA23_TLS_CERTFILE` | `/home/odbadmin/python/woa23/conf/fullchain.pem` | identical | **ok** |
| 7 | `WOA23_ANCHOR_REL` | ABSENT | ABSENT | **ok** |
| 8 | `WOA23_PYTHON` | ABSENT | ABSENT | **ok** |
| 9 | `WOA23_PRODUCTION_STORE` | ABSENT | ABSENT | **ok** |
| 10 | `WOA23_PM2C_GRANTED` | ABSENT | ABSENT | **ok** |

**Allowlist: no `WOA23_*` outside the ten.** `WOA23_PM2_BIN`: **ABSENT**.

argv from `/proc/<pid>/cmdline`:
```
/home/odbadmin/.pyenv/versions/py311/bin/python3.11 -m gunicorn api.app:app \
  -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:18265 --timeout 120 --graceful-timeout 10
```
`api.app:app` present; no `woa23_app`, no `--reload`, no `--keyfile`.

### 4.4 FINDING 1 — the isolated venv is NOT the runtime (§7 step 10 fails)

**Step 10 of the authorised sequence states:** "`/proc/<worker>/maps` shows libraries
from the `pm2G` venv and **none** from production or `py311`."

**The opposite is true.** Worker library provenance:

| worker | total mapped paths | from pm2G venv | from **shared py311** | from production tree |
|---|---|---|---|---|
| 1456373 | 1565 | **0** | **1153** | 0 |
| 1456374 | 785 | **0** | **451** | 0 |

The single `site-packages` root in use by both workers:
```
/home/odbadmin/.pyenv/versions/3.11.4/envs/py311/lib/python3.11/site-packages
```

polars is memory-mapped from the shared env, not the venv:
```
/home/odbadmin/.pyenv/versions/3.11.4/envs/py311/lib/python3.11/site-packages/polars/polars.abi3.so
```

**Cause — deliberate, and in the launcher this run exists to validate.**
`deploy/production_app.sh:104`:

```
PY="${WOA23_PYTHON:-/home/odbadmin/.pyenv/versions/py311/bin/python3.11}"
```

`WOA23_PYTHON` is **required ABSENT** by row 8 of the ten-variable check, so the
launcher takes its default: the **shared py311 environment**. The launcher's own
comment says this is intentional — "it preserves today's behaviour".

**So the request document contradicts itself, and the run exposed it:**

- §5 asserts "shared `py311` — **not the runtime** — only the interpreter `uv sync`
  builds *from*". **False under this configuration.**
- §7 step 10 asserts maps show pm2G-venv libraries and none from py311.
  **The reverse is observed.**
- §6.3 row 8 requires `WOA23_PYTHON` ABSENT — **which is exactly what forces the
  shared env to be used.**

These three cannot all hold. Rows 8 and step 10 are mutually exclusive.

**What this means:**

- The pm2G venv was built correctly (Python 3.11.4, 58 distributions,
  `manifest_sha256 3835c975…`, polars mainline 1.27.1, recorder stderr 0 bytes) and
  then **played no part in serving a single request**.
- **B7 is not merely "still open" — this run shows the deployment does NOT use an
  isolated venv.** Spec 014 settles the *intent*; `production_app.sh` as written does
  not implement it unless `WOA23_PYTHON` is set, and the current environment contract
  forbids setting it.
- The CORE/FULL manifest evidence describes a venv that is not the runtime, so it does
  **not** characterise what actually serves. The dependency set that served these 144
  rows is the shared py311 env's, which this run did not manifest.

**No file was changed.** `api/`, `conf/` and the three production candidates are
untouched; resolving this contradiction is a spec decision for the PI, not something
to patch mid-run.

## 5. The store, the config — both correct

**Synthetic store**, built by the subject's own `deploy/make_staging_store.py`:

| | |
|---|---|
| path | `/home/odbadmin/woa23-pm2g/store` |
| files / bytes | **72 / 25,191** |
| file-list SHA-256 | **`8fb70f2c64d7a3ee3d7fa451de08218c7a1740b19451cf015dd83394fd328624`** — identical to pm2F's, as predicted (same builder bytes) |
| groups | `1_degree/annual/TS` (0), `monthly/TS` (1,2), `seasonal/TS` (13) |
| anchor | present |
| read-only | **write probe refused**, before and after |

**No production data copied. No symlink to the production store. The production store
was never read.**

**Generated config** — `deploy/ecosystem.pm2G.config.js`,
SHA-256 `6399a3dd5ec2fc9eae7edb0fbaabd11c884e09dd982786003c688879620735bb`,
re-verified unchanged immediately before `pm2 start`.

Exactly **7 differing keys across the 5 permitted items**:

```
env.WOA23_PORT      : "8050" -> "18265"
env.WOA23_TLS       : (absent) -> "off"
env.WOA23_ZARR_STORE: "/home/odbadmin/python/woa23/data" -> "/home/odbadmin/woa23-pm2g/store"
error_file          : "tmp/woa23_err.log"      -> "tmp-pm2G/staging_err.log"
log_file            : "tmp/woa23.outerr.log"   -> "tmp-pm2G/staging.outerr.log"
name                : "woa23" -> "woa23-pm2g-candidate"
out_file            : "tmp/woa23.log"          -> "tmp-pm2G/staging.log"
```

`cwd` and `script` carried through **unchanged** — the relationship `pm2C` would have
proved cannot work now demonstrably does.

## 6. Readiness and the row-order contract — PASS

**OpenAPI 1.1.0**, from `/api/swagger/woa23/openapi.json` on a freshly restarted
process:

| | |
|---|---|
| title | **ODB WOA23 API** |
| version | **1.1.0** |
| paths | `/api/woa23`, `/api/woa23/csv` |
| row-order statement | **present** |
| names the four keys in order | **yes** |
| states lon varies fastest | **yes** |

**A note on how this was obtained, because it is a real caching property.**
`generate_custom_openapi()` returns `app.openapi_schema` if already set. An earlier
request in this run hit FastAPI's default `/openapi.json`, which populated that cache
with the **default** schema (`title: FastAPI`, `version: 0.1.0`) — after which the
custom route returned the cached default. The 1.1.0 schema above was therefore read
from a fresh process with the custom path queried **first**. This is a pre-existing
property of `api/app.py` (unchanged since the C1/C2 subject), not a pm2G regression,
but it means **whichever OpenAPI path is requested first wins for the process's
lifetime.**

**Contract** — 144 rows expected from the synthetic grid
(4 lon × 3 lat × 3 depth × 4 time_periods):

| | JSON | CSV |
|---|---|---|
| HTTP | **200** | **200** |
| rows | **144** | **144** |
| counts identical | **yes** | |
| strictly ascending by **numeric** `(time_period, depth, lat, lon)` | **yes** | **yes** |
| `13` after `2` (numeric, not string) | **yes** — order of appearance `[0, 1, 2, 13]` | |
| lat outer, lon varies fastest | **yes** — `(14.5,134.5) (14.5,135.5) (14.5,136.5) (14.5,137.5) (15.5,134.5)` | |

**The row-order contract PASSES on both representations, on both processes.**

## 7. FINDING 2 — column order is not stable across a restart (§7 step 14 fails)

**Step 14 of the authorised sequence states:** "restart — named app only; re-check
9–13; **response byte-identical**."

The response is **not** byte-identical. Same 144 rows, same values, **different column
order**:

| | before restart (pid 1455874) | after restart (pid 1456369) |
|---|---|---|
| CSV header | `lon,lat,depth,time_period,`**`temperature_an,temperature`** | `lon,lat,depth,time_period,`**`temperature,temperature_an`** |
| CSV row 1 | `134.5,14.5,0.0,0,`**`0.0,0.5`** | `134.5,14.5,0.0,0,`**`0.5,0.0`** |
| JSON first object keys | `…,`**`temperature_an, temperature`** | `…,`**`temperature, temperature_an`** |

**As Python objects the JSON rows compare EQUAL** (`identical as python objects: True`)
— only the serialised key order differs. **The CSV differs as text, and the value
columns swap with the header**, so a positional CSV consumer reads `temperature_an`
where it previously read `temperature`.

**Characterisation** — 8 repeated identical requests to the *same* process:

| | result |
|---|---|
| CSV header across 8 requests to one process | **identical all 8 times** |
| JSON key order across 8 requests to one process | **identical all 8 times** |
| `append=an,mn` vs `append=mn,an` on one process | **same order both ways** — request order does not drive it |
| across a process restart | **CHANGED** |

**Stable within a process, different between processes, and independent of the request.**
That is the signature of iteration over a hash-seeded container (a `set` of the
appended variable names) whose order is fixed per interpreter by `PYTHONHASHSEED`.

**This contradicts the API's own documented promise**, in the OpenAPI 1.1.0
description this very run verified:

> "Row order (since 1.1.0): … **JSON field order and CSV header order are unchanged.**"

**Row order is not affected** and remains correct — this is a *column* order defect,
and it lives in `api/`. `api/` is unchanged since the C1/C2 subject, so **c1f/c2g
validated row order but evidently never compared column order across two processes.**

**No fix was attempted. `api/` must not be modified without separate authorisation,
and doing so would invalidate C1/C2 evidence.**

## 8. Production — identical before and after, ZERO requests

| | before | after |
|---|---|---|
| boot id | `0b513a75-213b-40bf-8219-1c7cbc51a085` | **identical** |
| 4296 / 5040 / 5041 starttime | 14214 / 15825 / 15829 | **identical** |
| 4357 / 4358 starttime | 14323 / 14330 | **identical** |
| listeners 8050 / 8786 / 8787 | present, same PIDs | **identical** |
| PM2 `woa23` | `online`, pid 4295, restarts 0 | **identical** |
| `conf/ecosystem.config.js` | `8db9a6ba…` | **identical** |
| `conf/start_app.sh` | `4aaed5b7…` | **identical** |
| `woa23-pm2g-candidate` in production PM2 | absent | **absent** |

**Production received ZERO requests.** Every HTTP call in this run went to
`127.0.0.1:18265`. `conf/` was never written to. The production store was never read.

## 9. State on VM24 — RETAINED, service RUNNING, port BOUND

**Nothing was stopped. Nothing was deleted. The port was not released.**

| item | state |
|---|---|
| `woa23-pm2g-candidate` | **RUNNING** — pid `1456369`, `online`, 1 restart, 28.4 MB |
| workers | `1456373`, `1456374` — **alive** |
| port `18265` | **STILL BOUND** by all three |
| PM2 app entry | **PRESENT** under `~/woa23-pm2g-pm2/` |
| pm2G God Daemon | `1455863` — **running** |
| `~/woa23-pm2g/` | **retained**, 9669 files |
| `~/woa23-pm2g-work/` | **retained**, 1 file, marker `label=pm2G`, `port=18265`, subject `00f866a9…` |
| `~/woa23-pm2g-pm2/` | **retained**, 5 files |
| `~/woa23-pm2g-uvcache/` | **retained**, 8898 files |
| store | **retained**, 72 files, read-only (probe refused) |
| generated config | **retained**, `6399a3dd…` |
| `tmp-pm2G/` logs | **retained** — `staging.log`, `staging_err.log`, `staging.outerr.log` |
| **pm2E** | **untouched** — `~/woa23-pm2e` (8774), `~/woa23-pm2e-work` (8898) |
| **pm2F** | **untouched** — `~/woa23-pm2f` (9518), `-work` (1), `-pm2` (4), `-uvcache` (8899) |
| **pm2A / pm2B daemons** | **untouched** — 1242814, 1248938 |

**This is deliberate.** pm2G §8 forbids `pm2 stop`, `pm2 delete`, port release, manual
termination and filesystem cleanup after any mid-flight failure — written precisely
because pm2F performed a discretionary cleanup that was not authorised. **A separate
explicit authorisation is required to clean up pm2G.**

**Step 15 (`production_stop.sh`) was NEVER REACHED**, so **B1 is not exercised by this
run** and the identity-based stop remains unvalidated.

## 10. What this run did and did not establish

**Did:**

- The subject `06661fd` stages and verifies exactly — 164 files, file for file.
- **Both pm2F defects are genuinely fixed on VM24**: no `WOA23_PM2_BIN` leak, and
  order-independent pid extraction.
- The ten-variable environment contract holds, twice, with a clean allowlist.
- The override generator produces exactly the 5 permitted items / 7 keys, and `cwd`
  and `script` carry through unchanged — the `pm2C` defect is definitively fixed.
- The synthetic store builds byte-identically to pm2F's and is read-only.
- **The row-order contract passes**: 144/144, numeric ordering, `13` after `2`.
- OpenAPI 1.1.0 with the row-order statement is served.
- Restart works; the process comes back `online` with an identical environment.
- **Production was untouched throughout and received zero requests.**

**Did NOT:**

- **NOT a staging PASS.** Steps 10 and 14 failed their stated criteria.
- **NOT a production cutover PASS.** Alternate port, 72-file synthetic store, no TLS,
  no reverse proxy, isolated PM2 daemon, `conf/` unmodified.
- **NOT real-store correctness** — `c1f`/`c2g` are that.
- **NOT a performance result.**
- **B1–B5 NOT closed.** Step 15 was never reached; the B1 stop path is unvalidated.
- **B7 NOT closed, and further from closed than assumed** — §4.4 shows the deployment
  does not use the isolated venv at all under the current environment contract.

## 11. What needs a decision before a `pm2H` can be proposed

1. **The `WOA23_PYTHON` contradiction (§4.4).** Either the launcher should point at
   the staged venv (which means `WOA23_PYTHON` must be *set*, not absent, and row 8 of
   the ten changes), or the request must stop claiming the venv is the runtime and the
   manifest evidence must be recognised as describing something that does not serve.
   **This is a spec decision, not a patch.**
2. **The column-order defect (§7).** It is in `api/`, it contradicts the API's own
   1.1.0 promise, and fixing it invalidates C1/C2 evidence. Needs its own authorisation
   and its own regression test — comparing two *processes*, not two requests.
3. **The OpenAPI caching order (§6).** Whichever schema path is requested first wins
   for the process lifetime. Benign in production (nothing requests `/openapi.json`
   before the custom path) but it makes verification order-sensitive.
4. **pm2G cleanup.** The service is running and the port is bound, awaiting explicit
   authorisation.

## 12. Evidence

Local, under `scratchpad/pm2G/`:
`00-local-preflight.txt`, `01-vm24-preflight.txt`, `02-stage.txt`, `03-venv.txt`,
`04-run.txt`, `05-verify.txt`, `06-restart.txt`, `07-diagnostics.txt`,
`08-column-order.txt`, `09-final-state.txt`, plus the driver scripts and
`pm2g-archive.tar`.

VM24, retained and **not** to be cleaned: `~/woa23-pm2g/`, `~/woa23-pm2g-work/`,
`~/woa23-pm2g-pm2/`, `~/woa23-pm2g-uvcache/`, `/home/odbadmin/pm2g-archive.tar`, and
the **running** `woa23-pm2g-candidate` (pid 1456369) holding port 18265.
