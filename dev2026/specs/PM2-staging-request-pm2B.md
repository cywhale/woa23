# PM2 alternate-port staging (`pm2B`) — formal authorisation request

**Status: REQUESTED, NOT GRANTED. Nothing here has been run.** No PM2 process started,
no port bound, no staging directory created, no VM24 action taken.
**This document is not an authorisation.**

**This is the re-attempt after `pm2A` failed at start.** `pm2A` is not repeated, reused
or cleaned: its staging tree, store, PM2 daemon and evidence are **left exactly as they
are**, and every element of this run's identity is new.

---

## 0. Why there is a second attempt, and what changed

### 0.1 What `pm2A` failed on

**PM2 merges a config's `env:` block over the environment `pm2 start` is given, and the
config wins.** The block's placeholders replaced the values the command supplied: the
process received `WOA23_STAGING_STORE=''` and `WOA23_STAGING_PORT='18221'` instead of
the store path and port 18231 it was started with. The launcher refused the empty store
and exited 2 — **which is the only reason a spent port was never bound**.

It was a **staging-configuration failure**. The candidate never started, so nothing
about the candidate was learned, and `pm2A` is **not** a staging PASS.

### 0.2 What is different this time

| | `pm2A` | now |
|---|---|---|
| PM2 `env:` block | present, with defaults | **removed entirely** — the starting environment passes through |
| required variables | store checked; port and production-store defaulted | **all three required, refused by name, no defaults** |
| spent ports | not checked | **refused from `scripts/ports_used.tsv` itself** — 18221 and 18231 are refused without being named in code |
| interpreter | `exec gunicorn` via `PATH` | **`exec "$PY" -m gunicorn`**, named |
| environment proof | the starting shell | **`/proc/<pid>/environ` of the running process** |
| Python | whatever `uv` chose (3.11.14) | **pinned to production's 3.11.4** |

Verified offline: `scripts/test_staging_launcher.sh`, **96 assertions**, including all
three variables absent/empty/correct, the `/proc` verifier catching the exact `pm2A`
shape, and — proved on a throwaway tree with its own ledger — that the spent-port rule
**reads the ledger** rather than hard-coding two numbers: an arbitrary port passes while
absent and is refused the moment it is written in, an unledgered neighbour still passes,
and an absent ledger stops the run outright.

## 1. Execution subject

```
716fcc6cb18eb4fdb6f4bd732aba8f455b5b4975
```

**Superseded subjects, and why each was left behind** — the list is kept because a
re-point is a deliberate act and each one had a reason:

| tree | why it is not the subject |
|---|---|
| `5a86f2fdc7997546734bc62a36e44732d86fa689` | lacks `deploy/make_staging_store.py`; the run would stop at the store build |
| `182fe8ebad3de4e945b8e71aa27a0c55b454683e` | carries the PM2 `env:` block that caused the `pm2A` failure |
| `f1837f1dfc2099484c440de112bc74a9af536f6d` | its launcher predates the ledger-corollary fix |
| `f7bfe03958b8a3921312b2fc3110ff6d0a4a9a82` | correct, but predates the dynamic-ledger regression that proves the spent-port rule reads the ledger |

| item | value |
|---|---|
| commit SHA | `716fcc6cb18eb4fdb6f4bd732aba8f455b5b4975` |
| archive SHA-256 | `f96c512036be47a490117b23ec4da2f51da2b89ea746c71d1d9d4960f40c5dd9` |
| archive file count | `137` |
| file-list SHA-256 | `78e9dac1cd9aa561dde81211acd76efac8fc5f9b7ffd83f9fd71dc39f5cf1a2a` |

`api/` — **byte-identical to the C1/C2 subject `1439194…` apart from `api/app.py`,
whose difference is documentation-only**, decided by `bench/docs_only_diff.py`:

| file | SHA-256 |
|---|---|
| `api/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `api/app.py` | `15f26444af9fbffbdef8a240833c1ff7f8bc99fdf84277317eb15f859d467ce2` |
| `api/config.py` | `b806641dc7478acaca375380b8e0f8575c1fa48f093362f7f917921a3adc94ca` |
| `api/query.py` | `8e980e5b60a004902e66e6cb86ed2352a5ec641a6ad4cd3173a5a2efc56cebce` |
| `api/store_paths.py` | `00cb80c2b1c4ef74f984f42026dcdc5736bfb841e34471bd882c3fd42e35b928` |

The four files that run the staging:

| file | SHA-256 |
|---|---|
| `deploy/start_staging.sh` | `ab256716c1a919b6322425db3ddba36131d0756bb3ac196e541ab25bd9c4ccd6` |
| `deploy/ecosystem.staging.config.js` | `47cf48cfc8125ef0cec9355906493493a53700e6a058b175303406301d71457a` |
| `deploy/make_staging_store.py` | `cf121f7f41e15cd9a381d461772e2bc4a8b58281f7ba341baef14e9d3d5f69f1` |
| `deploy/verify_staging_env.sh` | `e42c341509fc5d22c634d7947a505bec4df66cf2de18f1e3cddc1e9d31aac5cf` |

`verify_clean_archive.sh` at this commit: **16/16**, producing exactly the four values
above. **All re-derived on VM24 and compared file by file; a mismatch stops the run.**

**Later commits are protocol references, never the subject.** This document is itself
committed; the commit carrying it is **not** what runs, and no later archive digest may
be quoted as "the tree that ran". A subject move is a deliberate re-point stated here.

## 2. Execution identity — every element new

| | value | `pm2A`'s (untouched, not reused) |
|---|---|---|
| label | **`pm2B`** | `pm2A` |
| staging | **`~/woa23-pm2b/`** | `~/woa23-pm2a/` |
| workdir | **`~/woa23-pm2b-work/`** | `~/woa23-pm2a-work/` (never created) |
| PM2 home | **`~/woa23-pm2b-pm2/`** | `~/woa23-pm2a-pm2/` |
| store | **`/home/odbadmin/woa23-pm2b/store`** | `/home/odbadmin/woa23-pm2a/store` |
| API port | **`18241`** | `18231` — spent |
| PM2 app name | `woa23-staging-candidate` | same name, **different daemon** |

`18241` is **deliberately absent from `scripts/ports_used.tsv`**, and must stay absent
until this run has taken it. The launcher refuses any port the ledger lists, so entering
a port before its run would make the guard reject that run at its first step — which is
exactly what happened when `18241` was briefly recorded as PROPOSED and
`test_staging_launcher.sh` went red. **The ledger records ports a run has taken; a port
a request intends is recorded here, and enters the ledger afterwards.**

**18221 and 18231 are spent, are in the ledger, and are therefore refused by the
launcher itself** — neither can be used by accident.

**`pm2A`'s tree, store, PM2 daemon and logs are not deleted, moved, cleaned or read from
as inputs.** They are evidence of a failed run and stay that way.

**No Dask ports.** The candidate imports no `dask` and no `distributed`; their absence
is what the candidate requires, not an omission in this request.

## 3. The store — small, synthetic, guard armed

Built on VM24 by the archive's own `deploy/make_staging_store.py`. Expected, and
verified before use:

| | |
|---|---|
| path | `/home/odbadmin/woa23-pm2b/store` |
| files / bytes | **72 / 25,191** |
| file-list SHA-256 | `8fb70f2c64d7a3ee3d7fa451de08218c7a1740b19451cf015dd83394fd328624` |
| groups | `1_degree/annual/TS` (anchor, period 0), `monthly/TS` (1, 2), `seasonal/TS` (13) |

Deterministic, so the digest is checkable rather than decorative. **No production copy,
no symlink to production's store, and `WOA23_PRODUCTION_STORE` is set so the guard is
armed** — the launcher refuses any staging store resolving under production's, comparing
physical paths. Made **read-only** after the build, with a write probe required to fail.

## 4. Python runtime — production's, pinned

```
uv sync --python /home/odbadmin/.pyenv/versions/py311/bin/python3.11
```

**Python 3.11.4, production's interpreter**, named explicitly rather than chosen. `pm2A`
resolved **3.11.14** silently.

**If 3.11.4 cannot be used**, the run may proceed only if the report states, in the
result itself: *this run did not use production's Python 3.11.4; it is not a
production-runtime validation and no runtime-equivalence claim follows.* That sentence
is the price of proceeding and is not omitted for brevity.

The interpreter actually used is recorded from `"$PY" --version` and from the running
process.

## 5. The sequence — thirteen steps, each a stop

1. **identity absent** — `~/woa23-pm2b/`, `~/woa23-pm2b-work/`, `~/woa23-pm2b-pm2/`;
   label `pm2B` has 0 artefacts. **`pm2A`'s paths are checked to still exist and are not
   touched.**
2. **port** — `18241` absent from the ledger **and** unbound on the host.
3. **archive** — digests and the nine file hashes re-derived on VM24; staged tree
   compared **file by file** against §1.
4. **environment** — `uv sync --python /home/odbadmin/.pyenv/versions/py311/bin/python3.11`.
   Then **confirm `.venv/bin/python --version` is 3.11.4**, and after the service starts,
   confirm from the running process that it is that venv's interpreter —
   `readlink /proc/<pid>/exe` and `/proc/<pid>/cmdline` must both point inside
   `~/woa23-pm2b/dev2026/.venv`. **If 3.11.4 cannot be obtained, stop** — or continue
   only with §4's sentence written into the result.
5. **store** — built by the archive's own builder.
6. **store verified** — 72 files, 25,191 bytes, digest matching §3, anchor present,
   physical path **not** under `/home/odbadmin/python/woa23/data`; then **read-only**,
   with a write probe that must fail and the digest unchanged.
7. **start** — isolated `PM2_HOME`, one named app, all three variables supplied at
   `pm2 start`:

   ```
   cd ~/woa23-pm2b/dev2026
   export PM2_HOME=~/woa23-pm2b-pm2
   WOA23_STAGING_PORT=18241 \
   WOA23_STAGING_STORE=/home/odbadmin/woa23-pm2b/store \
   WOA23_PRODUCTION_STORE=/home/odbadmin/python/woa23/data \
     pm2 start deploy/ecosystem.staging.config.js --only woa23-staging-candidate
   ```

8. **environment verified IN THE PROCESS** — `deploy/verify_staging_env.sh <pid> 18241
   /home/odbadmin/woa23-pm2b/store /home/odbadmin/python/woa23/data`, reading
   `/proc/<pid>/environ`. **This is the check `pm2A` did not have**, and a mismatch is a
   stop, not a restart.
9. **argv is `api.app`** — from `/proc/<pid>/cmdline`, scoped to the staging tree.
   `woa23_app` must appear nowhere in it. *(Note: the ghrsst project also runs
   `api.app:app` on this host, so the check is scoped by path, not by module name.)*
10. **PM2 `online`**, lifespan completed, **readiness on 18241**.
11. **OpenAPI `1.1.0`** with the row-order statement in the description and both
    endpoints; **JSON and CSV both 200 with 144 rows**, identical counts, rows strictly
    ascending by numeric `(time_period, depth, lat, lon)` across three groups.
12. **restart → re-check 8–11; stop; delete** — named app only. Process gone, **18241
    free**, PM2 state and logs finalised. **Never `all`, never `pm2 kill`, never a global
    `save`/`resurrect`, never `SIGKILL`.**
13. **production unchanged** — master PID, worker PIDs, starttimes, listeners on
    8050/8786/8787, boot id: read **before and after** and compared. **0 requests.**
    Production's PM2 daemon read separately; the staging app must not appear in it.

**If cleanup fails: `CLEANUP_FAIL`.** State and diagnostics kept, no repeat, nothing
cleared.

## 6. Forbidden

Touching the production API, production PM2, or `conf/`; `pm2 * all`, `pm2 kill`, global
`save`/`resurrect`; production's app name; **`SIGKILL`**; **self-rerun**; clearing or
overwriting failure evidence — `pm2A`'s included; any production cutover conclusion.

## 7. What a PASS will and will not mean

**Will:** the deployment machinery works — PM2 starts the candidate under an isolated
daemon, the lifespan opens the anchor, readiness answers, the documented 1.1.0 contract
is served, rows come out in contract order, and stop/cleanup releases everything.

**Will not:** it is **not** real-WOA23-data correctness (`c1f` and `c2g` are that,
against the real store); **not** a performance result; **not** TLS or reverse-proxy
validation; **not** production's launcher, which is still `conf/start_app.sh`; and **not
a production deployment PASS**. **B1–B5 remain open cutover blockers.**
