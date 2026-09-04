# 013 — Production launcher source audit: what actually starts what

**Status: audit only, revision 1. Nothing changed by this document.** `conf/` is read and
never written. No VM24 action, no PM2, no production change.

Its purpose is to establish, from the files themselves, **what production actually runs
today** — so that the cutover design in spec 011 is measured against the real thing rather
than against a remembered version of it.

| rev | date | change |
|---|---|---|
| 1 | 2026-08-20 | First audit. Covers all five launcher-related files and the PM2 / Gunicorn / TLS / store / Dask relationships. |

---

## 1. The five files, and what each one is

| file | role | in use? |
|---|---|---|
| `conf/ecosystem.config.js` | **production PM2 app definition.** The only thing PM2 reads | **YES — live** |
| `conf/start_app.sh` | **production launcher.** What that PM2 app executes | **YES — live** |
| `conf/simu.sh` | a manual runbook of copy-paste commands. Not referenced by PM2 or by any script | **no — reference only, and dangerous** (§5) |
| `dev2026/deploy/production_app.sh` | **proposed** replacement for `start_app.sh` | **no — not installed** |
| `dev2026/deploy/start_staging.sh` | the isolated staging launcher used by `pm2B` | **no — staging only, never production** |

## 2. What production actually does, in full

`conf/ecosystem.config.js` defines **one app**, `woa23`, whose `script` is
`./conf/start_app.sh`, and `conf/start_app.sh` is a **single command**:

```
gunicorn woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8050 \
         --keyfile conf/privkey.pem --certfile conf/fullchain.pem --timeout 120 --reload
```

Confirmed live on VM24 (read-only, `pm2B` and `bash5A` preflights): master **4296** and
workers **5040**, **5041**, all `python3.11` from `/home/odbadmin/.pyenv/versions/py311`,
resolving to `.pyenv/versions/3.11.4`.

**Six facts follow, and each one matters to the cutover:**

1. **The app is `woa23_app:app`** — the OLD application. Not `api.app`.
2. **The port is a literal** in the launcher: `-b 127.0.0.1:8050`.
3. **TLS terminates in gunicorn itself**, via `--keyfile`/`--certfile`. There is no proxy
   doing it. A cutover that drops these flags turns an HTTPS endpoint into an HTTP one.
4. **`--reload` is live in production.**
5. **`WOA23_ZARR_STORE` is never set.** The old app does not need it; `api.config` reads it
   at **import**, so the candidate cannot start from this launcher at all.
6. **`gunicorn` is resolved through `PATH`**, not named. It happens to resolve to
   `/home/odbadmin/.pyenv/versions/py311/bin/gunicorn`, but that is an accident of
   whatever environment PM2 was started in.

### The launcher does not `exec`

`start_app.sh` runs `gunicorn` as a **child** of the script's shell. PM2 therefore tracks
the *shell*, not the gunicorn master — which is very likely **why a `pre_stop` was written
at all**: stopping the tracked process would not reliably stop gunicorn, so someone reached
for a `kill` that would.

**That is the fault to fix at the root.** `deploy/production_app.sh` `exec`s, so PM2 tracks
the master directly and the `pre_stop` becomes unnecessary rather than merely rewritten.
`pm2B` demonstrated this end to end: SIGINT to the master, both workers drained and gone,
no `pre_stop` present anywhere.

## 3. Dask — separate apps, not started by the launcher

`start_app.sh` starts **no Dask**. The scheduler (**4357**, port 8786/8787) and worker
(**4358**) are **their own PM2 apps**, `dask-scheduler` and `dask-worker`, in production's
PM2 list.

**Consequence for the cutover:** the candidate imports neither `dask` nor `distributed`
(verified over the staged tree in `pm2B`), so after cutover those two apps become
**unused by woa23** — but they are **shared**: `tide_app` and `mhw_app` use the same
scheduler. **Stopping them is not part of this cutover and must not be bundled into it.**

## 4. The store

| | |
|---|---|
| production store | `/home/odbadmin/python/woa23/data` |
| set by the launcher? | **no** — `WOA23_ZARR_STORE` appears nowhere in `conf/` |
| needed by the candidate? | **yes, at import**, by `api.config` |
| read-only to the app? | the application only reads; nothing in the candidate writes to it |

`deploy/start_staging.sh` additionally takes `WOA23_PRODUCTION_STORE` purely so it can
**refuse** a staging store that resolves inside production's, comparing physical paths so a
symlink cannot slip past. That guard is a staging concern and is deliberately absent from
the production launcher, which is *supposed* to point at the production store.

## 5. `conf/simu.sh` — a runbook, and the most dangerous file in `conf/`

Not referenced by PM2 or any script; a human copy-pastes from it. It contains the same
`--reload` command as `start_app.sh`, plus **three** `grep | kill -9` pipelines:

```
ps -ef | grep 'woa23_app'      | grep -v grep | awk '{print $2}' | xargs -r kill -9
ps -ef | grep -w 'dask scheduler' | … | xargs -r kill -9
… | grep -w 'tide_app' | … | xargs -r kill -9
```

**The third one kills `tide_app` — a different project.** It is written down as a normal
step. This is the clearest evidence that the `grep | kill -9` habit in this configuration
is not confined to one line of one file, and it is why B1's fix must be *removal of the
pattern*, not a tidier version of it.

**`simu.sh` is not modified by this audit and is not part of the cutover.** It is recorded
here so that whoever performs the cutover does not reach for it. **Recommendation:** it
should eventually be replaced by a runbook that references the identity-based stop path
(§6), but that is a separate change with its own authorisation.

## 6. The stop path, compared

| | production today | staging (`pm2B`, proven) | proposed production |
|---|---|---|---|
| what PM2 tracks | the **shell**, not gunicorn | the gunicorn **master** (`exec`) | the master (`exec`) |
| stop mechanism | `pre_stop` = `grep` + `kill -9` | PM2's own signal | PM2's own signal |
| matches by | **command-line string** | process identity | process identity |
| can it hit another project? | **yes** | no | no |
| graceful? | **no — SIGKILL** | yes, SIGINT then drain | yes |
| verified? | never | **yes** — master and both workers gone, port released | by `deploy/production_stop.sh` (§7 of spec 011) |

## 7. Staging and production must not share configuration

`start_staging.sh` and `production_app.sh` are separate files **on purpose**, and each
refuses what the other requires:

| | `start_staging.sh` | `production_app.sh` |
|---|---|---|
| port | required; **8050/8786/8787 are refused outright**; ledgered ports refused | required; 8050 is the *configured* value in the production PM2 config |
| store | required; refuses one resolving inside production's | required; validated but production's store is the expected value |
| TLS | **absent** — staging is loopback-only, a stated gap | **present**, defaulting to production's certificate paths |
| `env:` block in its PM2 config | **none** — values are per-run and come from the operator | **present and explicit** — values are fixed properties of the deployment |
| Dask | never started | never started |

**Neither may adopt the other's defaults.** A staging launcher that could default to 8050
would be one edit away from binding production's port; a production launcher that read the
staging port ledger would refuse to start for a reason that has nothing to do with
production.

## 8. The conclusion this audit supports

**Production must start `api.app:app` and must never again start `woa23_app:app`.**

That single change is blocked by three of the five blockers at once — B2 names the app, B3
frees the port, B4 supplies the store — and none of them can be met by editing a value in
the existing launcher, because the existing launcher has no place to put a store and no
variable to hold a port. **A replacement file is the minimum change, not the ambitious
one.**

## 9. Boundaries

**Done here:** reading all five files, and confirming production's live process tree,
listeners and interpreter from the `pm2B`/`bash5A` read-only snapshots.

**Not done:** no file in `conf/` modified; nothing installed; no PM2 command; no VM24
action in this stage; no cutover.
