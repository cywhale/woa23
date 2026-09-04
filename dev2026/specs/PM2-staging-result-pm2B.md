# PM2 alternate-port staging (`pm2B`) — result

**Outcome: PASS**, in the bounded sense §7 of the request defines and §9 below repeats.
Executed 2026-08-20 on VM24 (`odb24`) under the authorisation of the same date.

**This is not a production deployment PASS.** B1–B5 remain open cutover blockers.

---

## 1. What ran

| | |
|---|---|
| execution subject | `716fcc6cb18eb4fdb6f4bd732aba8f455b5b4975` |
| archive SHA-256 | `f96c512036be47a490117b23ec4da2f51da2b89ea746c71d1d9d4960f40c5dd9` |
| file count / file-list SHA-256 | `137` / `78e9dac1cd9aa561dde81211acd76efac8fc5f9b7ffd83f9fd71dc39f5cf1a2a` |
| label / staging / workdir | `pm2B` / `~/woa23-pm2b/` / `~/woa23-pm2b-work/` |
| PM2_HOME | `~/woa23-pm2b-pm2/` (God Daemon **1248938**) |
| store | `/home/odbadmin/woa23-pm2b/store`, 72 files, 25,191 bytes |
| API port | **18241**, first use |
| Dask | none started; `api/` imports neither `dask` nor `distributed` |

All three digests were re-derived **locally** (16/16), re-derived again **on VM24 after
transfer**, and the nine named files compared one by one. Every value matched §1 of the
request. No step was taken on a digest that had not been re-checked on the host.

## 2. The thirteen steps

| # | step | result |
|---|---|---|
| 1 | `pm2B` identity absent; `pm2A` present and untouched | PASS — all four pm2B paths absent; pm2A 9798 + 4 files |
| 2 | 18241 absent from ledger, unbound on host | PASS — 0 in ledger, 0 listeners |
| 3 | archive re-derived on VM24, compared file by file | PASS — 137 files, digest exact, 9/9 files exact |
| 4 | Python 3.11.4, named not chosen | PASS with a wording correction — see §3 |
| 5 | store built by the archive's own builder | PASS |
| 6 | store verified, then read-only with a failing write probe | PASS — 72 / 25,191 / digest exact; probe refused; digest unchanged |
| 7 | start under isolated PM2_HOME | PASS — `online`, 0 restarts, master 1248949 |
| 8 | environment verified **in the process** via `/proc/<pid>/environ` | PASS — all four variables correct |
| 9 | argv is `api.app`, scoped to the staging tree | PASS — `woa23_app` absent |
| 10 | PM2 `online`, lifespan complete, readiness on 18241 | PASS — `Application startup complete`, HTTP 200 in 9 ms |
| 11 | OpenAPI 1.1.0; JSON/CSV 144 rows; contract row order | PASS — see §4 |
| 12 | restart → re-check 8–11; stop; delete; port released | PASS — see §5 |
| 13 | production unchanged, 0 requests | PASS — see §6 |

### The pm2A failure mode is gone, and was checked rather than assumed

`pm2A` died because PM2 merged the config's `env:` block over the environment `pm2 start`
was given and the config won, so the launcher received an empty store and a spent port.
This run read the environment **out of `/proc/1248949/environ`** — the running process,
not the starting shell:

```
ok   WOA23_STAGING_PORT     = 18241
ok   WOA23_STAGING_STORE    = /home/odbadmin/woa23-pm2b/store
ok   WOA23_PRODUCTION_STORE = /home/odbadmin/python/woa23/data
ok   WOA23_ZARR_STORE       = /home/odbadmin/woa23-pm2b/store
ok   the port is neither 18221 nor 18231
```

`pm2 jlist` independently showed the same three values on the app, and `WOA23_ZARR_STORE`
as `None` in PM2's own record — it is set by the launcher after its guards pass, which is
where it should be set.

## 3. Runtime — proven, and one correction to the request's wording

**Python 3.11.4 was used, and it is the same interpreter production runs.**

| | |
|---|---|
| `.venv/bin/python --version` | `Python 3.11.4` |
| its realpath | `/home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11` |
| argv[0] of the running master | `/home/odbadmin/woa23-pm2b/dev2026/.venv/bin/python` |
| `readlink /proc/1248949/exe` | `/home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11` |
| production's `readlink /proc/4296/exe` | `/home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11` |
| same interpreter as production | **YES** |

**Correction.** §4 of the request required that `readlink /proc/<pid>/exe` "point inside
`~/woa23-pm2b/dev2026/.venv`". **That check is unsatisfiable as written, and the fault is
in the request, not the run**: a venv's `bin/python` is a symlink to the base interpreter
and `/proc/<pid>/exe` always resolves to the real inode, so a correct venv can never
produce a path inside itself. The requirement should have been, and is here evidenced as,
two separate facts:

1. **argv[0] is inside the staging venv** — from `/proc/<pid>/cmdline`;
2. **the resolved binary is 3.11.4, byte-identical in path to production's** — from
   `/proc/<pid>/exe`, compared against production's own PID 4296.

That the venv actually supplies the libraries was checked separately, from
`/proc/<worker>/maps`: **416 mapped regions under
`/home/odbadmin/woa23-pm2b/dev2026/.venv`** (polars, numpy, pyarrow, orjson, zarr among
them) and **0 regions under any production path**.

Full command line of the master, unedited:

```
/home/odbadmin/woa23-pm2b/dev2026/.venv/bin/python -m gunicorn api.app:app \
  -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:18241 \
  --timeout 120 --graceful-timeout 10
```

No `--reload`, no TLS flags, no Dask, not a production port, `PYTHONHASHSEED` unset.
Gunicorn 23.0.0 and uvicorn 0.34.1, both resolved from the staging venv.

## 4. The 1.1.0 contract, served

`GET /api/swagger/woa23/openapi.json` → **`info.version = "1.1.0"`**, the row-order bullet
present in the description, and the `Row order (since 1.1.0)` section present in **both**
`/api/woa23` and `/api/woa23/csv`.

Query spanning all three store groups
(`lon0=134.5&lon1=137.5&lat0=14.5&lat1=16.5&dep0=0&dep1=100&time_period=0,1,2,13`):

| | JSON | CSV |
|---|---|---|
| HTTP | 200 | 200 |
| rows | **144** | **144** |
| strictly ascending by numeric `(time_period, depth, lat, lon)` | **yes** | **yes** |
| same order as the other format | **yes** | **yes** |

- **Three groups in one response** — `time_period` 0 (annual), 1 and 2 (monthly), 13
  (seasonal), in that order.
- **The lexical trap is avoided**: `13` sorts **after** `2`, which is the whole reason the
  key casts to `Int32` rather than comparing strings.
- **Latitude outer, longitude fastest**, verified inside a fixed `(time_period, depth)`
  block: `(14.5,134.5) (14.5,135.5) (14.5,136.5) (14.5,137.5) (15.5,134.5) …`
- JSON field order and CSV header order identical and unchanged:
  `lon, lat, depth, time_period, temperature`.

## 5. Restart, stop, cleanup

Restart of the **named app only**: master 1248949 → 1249486, old master and both old
workers confirmed gone, status `online`, restarts 1. Steps 8–11 re-run against the new
process — environment, argv, runtime, readiness all identical, and the response
**byte-identical** to the pre-restart one (JSON equal, CSV SHA-256 equal).

Stop, then delete, named app only. Never `all`, never `pm2 kill`, never a global
`save`/`resurrect`, **never SIGKILL**.

| after cleanup | |
|---|---|
| master and both workers | gone (verified by recorded PID) |
| apps under our PM2_HOME | 0 |
| listeners on 18241 | 0; `curl` → connection refused |
| processes matching the staging tree | none but the daemon |
| store | 72 files, still read-only, digest `8fb70f2c…` unchanged |

**The four `[ERROR]` lines in the log are gunicorn labelling its own normal SIGINT
shutdown.** Each reads `Worker (pid:…) was sent SIGINT!` and each is logged *after* that
worker's `Application shutdown complete` and `Finished server process`. SIGINT is PM2's
stop signal; no `SIGKILL` was sent, and `kill_timeout` never expired. There are no
tracebacks in the logs.

`pm2B`'s God Daemon **1248938 is deliberately left running**, exactly as `pm2A`'s is: the
request forbids `pm2 kill`, and the daemon plus its logs are the run's retained state.

## 6. Production — before and after, read-only

**0 requests were sent to 8050, 8786 or 8787.** Every fact below comes from `/proc` and
`ss` only.

| | before | after |
|---|---|---|
| boot id | `0b513a75-213b-40bf-8219-1c7cbc51a085` | identical |
| God Daemon | 3459, started 2026-08-14 13:25:09 | identical |
| gunicorn 4296 / 5040 / 5041 starttime | 14214 / 15825 / 15829 | identical |
| dask 4357 / 4358 starttime | 14323 / 14330 | identical |
| listeners 8050 / 8786 / 8787 | present, same PIDs | identical |

Production's own PM2 daemon was read separately: 9 apps, `woa23` `online` pid 4295 with
**0 restarts**, and **`woa23-staging-candidate` does not appear in it**.

`pm2A`'s tree, store, PM2 home and logs were neither deleted, moved, cleaned nor read as
inputs: 9798 and 4 files before and after, newest mtime `2026-08-20 11:05` — an hour
before this run started — and its daemon 1242814 still alive.

## 7. Two observations that are not failures, recorded because they are true

**7a. polars warns that this CPU lacks `avx2`.** Every worker logs
`RuntimeWarning: Missing required CPU features … avx2, bmi1, bmi2, lzcnt … will likely
result in a crash`. `avx2` is genuinely absent from `/proc/cpuinfo` — the host is a Xeon
Gold 6326, which has AVX2 in silicon, so the VM is masking it. **polars nevertheless works
correctly here**: a direct sort returned `['0','1','2','13']`, and 288 rows over two
formats and two process generations came out in exact contract order. The same warning was
present throughout `c1f` and `c2g`, which together issued far more requests without a
crash. **This is a pre-existing host property, not something pm2B introduced, and it is
worth a cutover decision** — `polars-lts-cpu` is the upstream remedy. It is not a blocker
this run can close and no change was made.

**7b. `dask` and `distributed` are importable in the staging venv.** They are pinned in
`pyproject.toml` because `bench/zarr_bench.py` drives the `distributed` mode for the
*before* arm. This does not weaken the S1 claim: **`api/` imports neither**, verified by
search over the staged tree, and no Dask process was started. My first check labelled them
"absent (expected)", which was the wrong expectation to state.

## 8. Ledger

`18241` was absent from `scripts/ports_used.tsv` for the whole run — that is what let the
launcher's spent-port guard accept it — and was written in **afterwards**, marked bound and
released. `18221` and `18231` remained in the ledger and would have been refused.

## 9. What this PASS means, and what it does not

**It means:** the deployment machinery works. PM2 starts the candidate under an isolated
daemon with the environment intact, the launcher's guards hold, the lifespan opens the
anchor, readiness answers, the documented 1.1.0 contract is served, rows come out in
contract order across three period groups in both formats and survive a restart
byte-identically, and stop/delete releases the port and every process.

**It does not mean:** real-WOA23-data correctness — that is `c1f` and `c2g`, against the
real store; this store is 72 synthetic files. Not a performance result. Not TLS or
reverse-proxy validation. Not production's launcher, which is still `conf/start_app.sh`.
**Not a production deployment PASS.** B1–B5 remain open.
