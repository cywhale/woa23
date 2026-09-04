# PM2 alternate-port staging of the PRODUCTION launcher (`pm2C`) — authorisation request

**Status: SUPERSEDED — REVIEWED, AUTHORISED, AND STOPPED BY A PRE-START AUDIT.
NEVER EXECUTED.**

An authorisation was issued for this request on 2026-08-21 and **execution did not begin.**
While preparing the override config — offline, before any VM24 contact — evaluating
`deploy/ecosystem.production.config.js` with node showed it **could not work**: `script`
resolved only from the repository root, `api.app` was importable only from `dev2026/`, and
the config set no `cwd` at all. PM2 would have reported the app `online` and gunicorn would
have died at import.

**Nothing on VM24 was touched.** No SSH session, no staging tree, no venv, no PM2 daemon,
no store, and **no port was bound**. In particular:

- **`18261` was never bound and is NOT spent.** It is not in `scripts/ports_used.tsv` and
  must not be recorded there. It is, however, now *named* in this document, so the
  superseding request derives a different first-use port.
- **This is not a VM24 execution** and must never be counted as one. There is no `pm2C`
  result document because there was no run.

**Superseded by `PM2-staging-request-pm2D.md`**, which carries a new subject, a fresh
execution identity, the dedicated grant, and the generator/differ this request assumed but
did not have.

**This document is retained as the record of a request that was stopped before it ran.**

---

*(Original request text follows, unchanged.)*

**Status when written: REQUESTED, NOT GRANTED.** No PM2 process started, no
port bound, no staging directory created, no venv built, no VM24 action taken.
**This document is not an authorisation.**

**What is new about this run:** `pm2B` validated `deploy/start_staging.sh` — a staging
launcher production does not use. **`pm2C` validates the three files that are actually
proposed for production**, as a set, on an alternate port:

| file | role |
|---|---|
| `deploy/production_app.sh` | the proposed replacement for `conf/start_app.sh` |
| `deploy/ecosystem.production.config.js` | the proposed replacement for `conf/ecosystem.config.js` |
| `deploy/production_stop.sh` | the identity-based stop that replaces the `pre_stop` line |

**It is still not a production deployment.** Alternate port, synthetic store, isolated PM2
daemon, `conf/` untouched. §9 states the bound.

---

## 1. Execution subject

| item | value |
|---|---|
| commit | `ecd4c445c67d3e085c601507a6e2c9e72a6d7a08` |
| archive SHA-256 | `1e97253587a8ea0020fe8af7124b36a00d20033fd99c09bcba2713190b24e332` |
| file count | `154` |
| file-list SHA-256 | `1eb0f0a0f642f317f8a47892b08900ed13c3a2fbb74503a790e1860676651679` |
| verifier | `verify_clean_archive.sh` 16/16 |
| offline evidence | **40/40 suites, 0 non-zero, 0 failing assertions**, at this commit, clean tree |

**This document commits AFTER the subject and is a protocol reference, never the subject.**
No later archive digest may be quoted as "the tree that ran".

The six files this run exercises. **Every digest is stated here and re-derived on VM24;
a mismatch on any one is a stop.**

| file | SHA-256 |
|---|---|
| `deploy/production_app.sh` | `3c6737a817cc12b7a37a2e9954698d0ce285e98c64e6b7a0b5a6e89a64d38ef3` |
| `deploy/ecosystem.production.config.js` | `0552aa3dd0e08102642ecd98f4a9fe4b7a1f23b2ccb69d4aa58487c49037188b` |
| `deploy/production_stop.sh` | `e86f07f18b38ee4b470e7049d15c5363cfe9c3f8847e2602aa0096c1e6b7803f` |
| `deploy/record_manifest.py` | `8e7a0aaba21ea7634033318297584bf9e756b765603a8626d934cafd7a91cd34` |
| `deploy/make_staging_store.py` | `cf121f7f41e15cd9a381d461772e2bc4a8b58281f7ba341baef14e9d3d5f69f1` |
| `api/query.py` | `8e980e5b60a004902e66e6cb86ed2352a5ec641a6ad4cd3173a5a2efc56cebce` |

`api/` is **unchanged since the C1/C2 subject** — all five digests identical, verified at
this commit. **No `api/`, query-logic, response-behaviour or dependency-version change has
occurred since `c1f`/`c2g`/`s2pB`**, so none of that evidence is disturbed by this run.

## 2. Identity — every element new

| | value | not reused from |
|---|---|---|
| label | **`pm2C`** | `pm2A`, `pm2B` |
| staging | **`~/woa23-pm2c/`** | `~/woa23-pm2a/`, `~/woa23-pm2b/` |
| workdir | **`~/woa23-pm2c-work/`** | — |
| PM2 home | **`~/woa23-pm2c-pm2/`** | `~/woa23-pm2a-pm2/`, `~/woa23-pm2b-pm2/` |
| store | **`/home/odbadmin/woa23-pm2c/store`** | — |
| API port | **`18261`** | 18221, 18231, 18241 (all spent) |
| PM2 app name | **`woa23-pm2c-candidate`** | **not `woa23`** — see §5 |

### Why `18261` and not `18251`

`18251` is **absent from `scripts/ports_used.tsv`** — `bash5A` passed it as a value and
never bound it, so the ledger correctly has no entry. **But it is named in three files** as
that run's probe port, so it is not *genuinely* unused. `18261` is absent from the ledger
**and** from every file in the tree, derived by scanning both at request time.

**It is deliberately not pre-recorded in the ledger.** The launcher refuses any ledgered
port, so entering a port before its run would make the guard reject the very run it was
chosen for — which is exactly what happened when 18241 was briefly recorded as PROPOSED.
The ledger records ports a run has **taken**; 18261 enters it afterwards.

### `pm2A` and `pm2B` are untouched

Their trees, stores, PM2 homes, logs and **retained daemons 1242814 and 1248938** are not
deleted, moved, cleaned, restarted or read as inputs. **The retained-daemon cleanup is a
separate item and is not part of this run.**

## 3. The venv — isolated, pinned, and fully recorded

Per spec 014 and the PI's instruction:

```
cd ~/woa23-pm2c/dev2026
uv sync --python /home/odbadmin/.pyenv/versions/py311/bin/python3.11
```

| | |
|---|---|
| location | `~/woa23-pm2c/dev2026/.venv` — **inside this run's own tree** |
| interpreter | **Python 3.11.4**, production's, named not resolved |
| shared `py311` | **not used as the runtime.** It is only the interpreter `uv sync` builds *from* |
| polars | **mainline 1.27.1** — the B6 decision. `polars-lts-cpu` is **not installed** |

**The complete manifest is recorded**, not the eight packages B7 asked about:

```
./.venv/bin/python deploy/record_manifest.py
```

- **CORE (ten packages) must match `uv.lock` exactly.** A difference is a **stop**.
- **FULL is recorded with its digest.** Differences from the development venv are expected
  — that venv runs 3.11.14 on Darwin and this one runs 3.11.4 on Linux, and `uv.lock`
  resolves platform markers — but **each difference must be NAMED with its reason in the
  result.** An unexplained difference is a stop.
- The recorder **imports nothing** (proved offline with `-X importtime`), so it does not
  initialise polars and does not emit the AVX2 warning.

## 4. The store — the existing small synthetic one

Built on VM24 by the archive's own `deploy/make_staging_store.py`, exactly as in `pm2B`:

| | |
|---|---|
| path | `/home/odbadmin/woa23-pm2c/store` |
| files / bytes | **72 / 25,191** |
| file-list SHA-256 | `8fb70f2c64d7a3ee3d7fa451de08218c7a1740b19451cf015dd83394fd328624` |
| groups | `1_degree/annual/TS` (period 0), `monthly/TS` (1, 2), `seasonal/TS` (13) |

**No production data is copied. No symlink to production's store. Made read-only after the
build, with a write probe required to fail.** The production store is never read.

## 5. The one deliberate deviation from the production config, and why

`ecosystem.production.config.js` names the app **`woa23`** — production's name — and sets
`WOA23_PORT: '8050'`, because that is what it will be when installed.

**Neither may be used here.** A `pm2C` run must not start an app called `woa23`, even under
an isolated `PM2_HOME`: the name is production's, and a stop or list command typed against
the wrong `PM2_HOME` would then be ambiguous in exactly the situation where ambiguity is
most costly. And 8050 is production's port.

**So `pm2C` supplies an override config generated at run time from the proposed one**,
identical except for:

| key | production value | `pm2C` value |
|---|---|---|
| `name` | `woa23` | `woa23-pm2c-candidate` |
| `env.WOA23_PORT` | `8050` | `18261` |
| `env.WOA23_ZARR_STORE` | `/home/odbadmin/python/woa23/data` | `/home/odbadmin/woa23-pm2c/store` |
| `env.WOA23_TLS` | *(absent — TLS on)* | `off` |
| log paths | `tmp/woa23*.log` | `tmp-pm2c/*.log` |

**Every other key is byte-identical, and the run must prove it** — the result reports a
diff of the generated config against the proposed one showing exactly these five keys and
nothing else. **A sixth difference is a stop.**

**TLS is off, and that is a stated GAP not a claim.** Production terminates TLS in gunicorn
with real certificates; staging has none and must not fabricate any. `production_app.sh`
requires an explicit `WOA23_TLS=off` to run without TLS and warns when it does — so this
also exercises that path. **TLS validation belongs to the cutover, not here.**

## 6. The sequence — each step a stop

1. **identity absent** — `~/woa23-pm2c/`, `~/woa23-pm2c-work/`, `~/woa23-pm2c-pm2/`; label
   `pm2C` has 0 artefacts. **`pm2A`/`pm2B` paths and daemons confirmed present, untouched.**
2. **port** — `18261` absent from the ledger **and** unbound on the host.
3. **archive** — digests and the six file hashes re-derived on VM24; compared file by file.
4. **venv** — `uv sync` with the pinned interpreter; `.venv/bin/python --version` is
   **3.11.4**; **complete manifest recorded**; CORE compared to `uv.lock`; FULL digest
   recorded and every difference from the development manifest named.
5. **store** — built by the archive's builder; **72 / 25,191 / digest** verified; anchor
   present; physical path not under production's; then read-only with a failing write probe.
6. **config** — the `pm2C` override generated; **diffed against the proposed production
   config**; exactly five keys differ.
7. **start** — isolated `PM2_HOME`, one named app:

   ```
   cd ~/woa23-pm2c/dev2026
   export PM2_HOME=~/woa23-pm2c-pm2
   pm2 start deploy/ecosystem.pm2c.config.js --only woa23-pm2c-candidate
   ```

8. **environment verified IN THE PROCESS** — `/proc/<pid>/environ` carries
   `WOA23_PORT=18261`, the staging store, and **no `--reload`-enabling variable**.
9. **argv** — from `/proc/<pid>/cmdline`: `api.app:app`, the staging venv's python,
   **no `--reload`**, **no `woa23_app`**, **no `--keyfile`** (TLS off). Scoped by path,
   since another project also runs `api.app:app` on this host.
10. **provenance** — `/proc/<pid>/exe` resolves to `.pyenv/versions/3.11.4/bin/python3.11`,
    equal to production's PID 4296; `/proc/<worker>/maps` shows libraries from the `pm2C`
    venv and **none** from production or from `py311`'s `site-packages`.
11. **readiness** — PM2 `online`, lifespan complete, **OpenAPI 1.1.0** on 18261 with the
    row-order statement in the description and both endpoints.
12. **contract** — JSON and CSV both 200, **144 rows each**, identical counts, strictly
    ascending by numeric `(time_period, depth, lat, lon)` across the three groups.
13. **restart** — named app only; re-check 8–12; response byte-identical.
14. **stop via `production_stop.sh`** — this is the B1 path and the point of the run:
    `WOA23_PM2_HOME=~/woa23-pm2c-pm2 ./deploy/production_stop.sh woa23-pm2c-candidate`.
    It must resolve the pid from PM2, record `(pid, starttime)` for master and workers,
    stop gracefully, and **verify by identity** that the tree is gone. **No SIGKILL.**
    **No process belonging to any other project may be signalled or named.**
15. **release** — 18261 free, `curl` refused, `pm2 delete` the named app, 0 apps left.
16. **production unchanged** — boot id, master/worker PIDs and starttimes, listeners on
    8050/8786/8787, and production's PM2 list read **before and after** and compared.
    **0 requests.** The staging app must not appear in production's PM2.

**If cleanup fails: `CLEANUP_FAIL`.** State and diagnostics kept, no repeat, nothing
cleared.

## 7. What `pm2C` must validate — the PI's list, mapped to steps

| requirement | step |
|---|---|
| candidate `api.app:app` | 9 |
| required `WOA23_ZARR_STORE` | 5, 8 |
| Python / Gunicorn provenance | 4, 10 |
| no `--reload` | 8, 9 |
| no grep / `kill -9` `pre_stop` | 6 (config diff shows no `pre_stop` key at all), 14 |
| graceful stop and survivor handling | 14 |
| alternate-port readiness | 11 |
| OpenAPI 1.1.0 | 11 |
| JSON / CSV response | 12 |
| row order | 12 |
| restart, stop, port release | 13, 14, 15 |
| production before/after unchanged | 16 |

**Survivor handling is exercised as a real outcome, not simulated.** If
`production_stop.sh` exits 7 with a survivor named, that is a genuine `CLEANUP_FAIL` for
this run — it is **not** re-run, and the surviving process is left for inspection.

## 8. Forbidden

Touching the production API, production PM2, production store or `conf/`; `pm2 * all`,
`pm2 kill`, global `save`/`resurrect`; production's app name `woa23`; production's port
8050; **SIGKILL**; **self-rerun**; installing or evaluating `polars-lts-cpu`; changing any
dependency version; clearing or reusing `pm2A`/`pm2B`/`bash5A` evidence; touching the
retained daemons 1242814 and 1248938; touching the 16 local `arm.py` strays on the
development machine.

## 9. What a PASS will and will not mean

**Will:** the three proposed production files work **together** — the launcher starts the
candidate with a validated environment under PM2, the config carries real values with no
`pre_stop`, the identity-based stop terminates the recorded tree and proves it, and the
port is released. The runtime is an isolated venv on production's 3.11.4 with a fully
recorded manifest.

**Will not:** **not a production deployment PASS.** Alternate port, **synthetic 72-file
store**, **no TLS**, no reverse proxy, isolated PM2 daemon, `conf/` unmodified, production
untouched. **Not real-store correctness** (`c1f`/`c2g` are that). **Not a performance
result** (`s2pB`, and B6's accepted masking means no absolute figure is
production-representative). **B1–B5 are not closed by this run** — they are *validated in
staging*; closing them requires installation and a cutover, each separately authorised.
**B7's runtime decision is settled by spec 014 as an isolated venv; B7 itself closes only
when a deployment actually uses one.**
