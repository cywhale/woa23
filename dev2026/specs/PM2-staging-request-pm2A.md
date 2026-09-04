# PM2 alternate-port staging (`pm2A`) — formal authorisation request

**Status: GRANTED 2026-08-20 and EXECUTED once — `pm2A`, FAILED AT START (step 8 of
12).** The service exited immediately, exit code 2, and never listened: **PM2's `env:`
block overrode the shell environment**, handing the launcher an empty store and port
**18221** instead of the authorised 18231. The store guard refused the empty store and
stopped the run **before an unauthorised port could be bound**.

Steps 1–7 passed, including the archive verified file by file and the synthetic store
reproducing its digest exactly. The full record is in
`specs/002-production-correctness-deploy-hardening.md` under `pm2A`.

**This request is spent.** A re-attempt needs a corrected configuration, a **new
first-use port** (18231 is now spent too), and its own authorisation. No PM2 process started,
no port bound, no staging directory created, no VM24 action taken, nothing deployed.
**This document is not an authorisation and does not become one by being reviewed.**

**Asks for:** one PM2-managed candidate service on VM24, on an alternate loopback port,
in its own PM2 daemon, for the duration of one validation.

**Governed by:** spec 010 (design, checks, boundaries) and spec 008 §9 (the 1.1.0
documentation this validates is published).

---

## 0. The execution subject, and what is NOT in it

### 0.1 Subject — the only tree that runs

```
182fe8ebad3de4e945b8e71aa27a0c55b454683e
```

**Fixed by the PI on 2026-08-20.** It does not change if further commits are made before
authorisation is granted, and the HEAD at authorisation time is **not** the subject.

**It is NOT `5a86f2fdc7997546734bc62a36e44732d86fa689`.** That tree was this request's
subject in revisions 1–2 and **does not contain `deploy/make_staging_store.py`**, which
was added afterwards — an archive of it could not build the synthetic store, and the run
would stop at step 5 with the builder missing. The subject was re-pointed for exactly
that reason.

### 0.1a Later commits are protocol references, never the execution subject

**This document is itself committed.** The commit carrying this revision, and any
revision after it, is a **protocol reference**: it describes what the run must do and
is read by whoever authorises it. It is **not** the execution subject and its archive is
**not** what runs on VM24.

**The archive that runs is §1's, and only §1's.** No later commit's archive digest may
be substituted for it, quoted as "the tree that ran", or used to re-derive anything
after the fact. If a future revision of this request needs the subject to move — as it
did once already — that is a **deliberate re-point stated in §0.1**, not something that
happens because a document was edited.

A tree cannot record its own digest, so this document does not attempt to name the
commit that carries it.

### 0.2 What this tree is, relative to the validated one

`api/` differs from the C1/C2 subject `1439194a091a5c00ac9414ddd46a3898e51dad51` by
**`api/app.py` only**, and that difference is **documentation-only**, decided by
`bench/docs_only_diff.py` and not by assertion: the two revisions' ASTs are identical
once docstrings and the two allowlisted `get_openapi` keywords are removed, and the
other four `api/` files are byte-identical.

**So `c1f` and `c2g` remain the data-path evidence for this tree**, and this staging run
is **not** offered as C1/C2 evidence for anything.

### 0.3 Evidence that may NOT be back-filled

`c1e`, `c2f` and `s2pB` describe trees without the row sort. `c1f` and `c2g` describe
the data path under controlled two-arm conditions. **None of them says anything about
PM2**, and this run says nothing about them.

## 1. Digests — complete, none abbreviated

**These are the digests OF THE EXECUTION SUBJECT `182fe8e…`**, and of nothing else.

| item | value |
|---|---|
| commit SHA | `182fe8ebad3de4e945b8e71aa27a0c55b454683e` |
| archive SHA-256 | `0dc2a273f53538482ad90a52edb8df72dd5276e914d4d54f5f2d3f92e4f562aa` |
| archive file count | `135` |
| file-list SHA-256 | `8f32c62ec966bc5efeee97ee393b44b4541c41bf448a8073d597a613c3524f5c` |

The superseded subject `5a86f2fdc7997546734bc62a36e44732d86fa689` (archive
`d52ffdad…`, 132 files, file-list `69fda1b5…`) is recorded here **only** so a reader
who saw revisions 1–2 can tell the two apart. **It is not the subject and must not be
staged.**

`api/` source SHA-256, read from the subject commit itself. They are **identical** to
the superseded tree's — the re-point added `deploy/make_staging_store.py` and
`bench/test_staging_store.py` and changed no `api/` file:

| file | SHA-256 |
|---|---|
| `api/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `api/app.py` | `15f26444af9fbffbdef8a240833c1ff7f8bc99fdf84277317eb15f859d467ce2` |
| `api/config.py` | `b806641dc7478acaca375380b8e0f8575c1fa48f093362f7f917921a3adc94ca` |
| `api/query.py` | `8e980e5b60a004902e66e6cb86ed2352a5ec641a6ad4cd3173a5a2efc56cebce` |
| `api/store_paths.py` | `00cb80c2b1c4ef74f984f42026dcdc5736bfb841e34471bd882c3fd42e35b928` |

The two files that do the launching:

| file | SHA-256 |
|---|---|
| `deploy/start_staging.sh` | `6c9064e04503dca5692ed664375a37651505aeb3be6f549dfd838a4d7ad1efc6` |
| `deploy/ecosystem.staging.config.js` | `346d27d46a9fa1a7525c1eb28aa7ccffc8ba8dfb60541167f01459ae0533f3e7` |

Clean-archive verification at this commit, run locally 2026-08-20: **16/16 assertions**,
producing exactly the four values above.

**All of it is re-derived on VM24 after transfer and compared file by file. A mismatch
stops the run before PM2 is invoked.**

## 2. Execution identity — every element first-use

| | value |
|---|---|
| label | **`pm2A`** |
| staging directory | **`~/woa23-pm2a/`** |
| workdir | **`~/woa23-pm2a-work/`** |
| PM2 home | **`~/woa23-pm2a-pm2/`** |
| PM2 app name | **`woa23-staging-candidate`** |
| **API port** | **`18231`** |
| Dask scheduler / worker ports | **NONE — see §2.1** |

**`18221` is NOT used.** It is recorded in `scripts/ports_used.tsv` as `ALLOCATED,
NEVER BOUND` and is therefore spent; it survives only as the offline design default in
`ecosystem.staging.config.js` and is overridden by `WOA23_STAGING_PORT` at launch.

`18231` is recorded in `scripts/ports_used.tsv` as **PROPOSED, not granted**. **If this
run is authorised and executed, the ledger entry is updated to bound/consumed
afterwards** — not before, because a port the run never bound is not a port the run
used. **`18221` stays spent and is not reused.** No earlier identity — `c1d`, `c1f`, `c2c`, `c2e`,
`c2f`, `c2g`, `d1a`, `d1b`, `clnA`, `clnB`, `s2pA`, `s2pB` — is reused, reopened,
cleaned or overwritten.

### 2.1 No Dask ports, and why that is a statement of fact

**The candidate imports no `dask` and no `distributed`.** Checked in the subject tree:
the only occurrences under `dev2026/api/` are comments explaining their removal.
Production's `woa23_app.py:17` installs a client at import; the candidate does not.

So **no Dask scheduler or worker is started and no Dask port is allocated.** Reserving
one would stage something the candidate does not use.

**This is a positive finding, not a gap in the request.** A reviewer checking the
sequence for Dask ports and not finding any is seeing the correct outcome: their absence
is what the candidate's own imports require, and a run that started a scheduler would be
validating a component this candidate removed. If a *reference* arm is ever
staged beside the candidate, that arm needs Dask and its own alternate ports — a
different request.

## 3. The store — a small synthetic fixture, decided

**Option C, chosen by the PI on 2026-08-20.** No copy of production data, **no symlink
to production's store even read-only**, and **the store guard is not disabled**.

### 3.1 Source, size and digest

Built on VM24 by `deploy/make_staging_store.py`, which is in the execution archive.

| | |
|---|---|
| builder | `deploy/make_staging_store.py` (subject tree) |
| absolute path | **`/home/odbadmin/woa23-pm2a/store`** |
| `WOA23_ZARR_STORE` effective value | **`/home/odbadmin/woa23-pm2a/store`** — absolute, exported by the launcher from `WOA23_STAGING_STORE` |
| files | **72** |
| bytes | **25,191** (~25 KB) |
| file-list SHA-256 | **`8fb70f2c64d7a3ee3d7fa451de08218c7a1740b19451cf015dd83394fd328624`** |

**The digest is reproducible, and that is checked rather than hoped.** Values come from
`numpy.arange`, coordinates from fixed lists; nothing depends on the clock, the
filesystem or a seed. `bench/test_staging_store.py` builds it **twice, in two different
directories**, and requires the file count, byte total and file-list digest to match.
The VM24 build must reproduce the digest above; a mismatch is a stop.

**The builder refuses an existing path** rather than clearing it — the same rule the
runner applies to a workdir, for the same reason.

### 3.2 Contents — exactly what the query path touches

| group | time_periods | why it is there |
|---|---|---|
| `1_degree/annual/TS` | `0` | the **anchor group** `api.app`'s lifespan opens before serving |
| `1_degree/monthly/TS` | `1`, `2` | a second group, so a query spans more than one |
| `1_degree/seasonal/TS` | `13` | a third, and the case that catches a string sort |

Coordinates `lon` = 134.5, 135.5, 136.5, 137.5 · `lat` = 14.5, 15.5, 16.5 · `depth` =
0, 10, 100 · `parameters` = temperature, salinity · variables **`an` and `mn`**.

`mn` is not decoration: the default `append` is `mn`, and the rename to the bare
parameter name depends on it. The longitudes and latitudes sit on the 1-degree grid's
half-degree centres so `to_lowest_grid_point` snaps onto them.

The three groups are what `determine_subgroup` sends periods 0, 1, 2 and 13 to — so a
single query really does open three groups, which is what makes the row-order check
meaningful instead of trivially satisfied by one group's natural order.

### 3.3 It is not under the production store, and the guard stays armed

`/home/odbadmin/woa23-pm2a/store` is not inside `/home/odbadmin/python/woa23/data`. The
run sets

```
WOA23_PRODUCTION_STORE=/home/odbadmin/python/woa23/data
```

so `deploy/start_staging.sh` refuses outright if the staging store ever resolves under
production's, comparing **physical** paths so a symlink cannot slip past. The guard is
**enabled for this run**, not bypassed.

### 3.4 Already demonstrated to carry the candidate, offline

`bench/test_staging_store.py` — **24 assertions**, in-process, no socket — builds the
fixture with the real builder and then runs the **real lifespan** and the **real
handlers**:

- the **lifespan completes** without raising, having opened the anchor;
- the JSON endpoint returns rows spanning **all four** requested periods, with the bare
  `temperature` and `salinity` columns the `mn` default produces;
- the CSV endpoint returns the **same row count**;
- rows ascend by `(time_period, depth, lat, lon)`, **strictly**, with JSON and CSV
  carrying an identical key sequence, and **13 after 2**;
- **the contract order is NOT the store's natural order** — the fixture is lon-major, so
  a candidate that sorted nothing would fail rather than pass by accident;
- the OpenAPI document is the **1.1.0** one, with the row-order statement in the
  description and in **both** endpoint descriptions.

So the request is known to be runnable before anyone spends an authorisation on it.

### 3.5 What this fixture does NOT establish

**It is not WOA23 data.** A pass here is evidence about **deployment machinery** —
PM2, startup, lifespan, readiness, serialisation, ordering — and **nothing else**.

- **Startup succeeding on this store may not be reported as real-store correctness.**
  `c1f` and `c2g` are that evidence, against the real store, and no number from this
  fixture may be quoted as theirs.
- **No depth characterization.** That is D1's, against the real store.
- **No performance measurement.** Nothing here is timed, and the store is far too small
  to mean anything if it were.
- **No production API involvement** of any kind.

## 4. PM2 state isolation

**`PM2_HOME=~/woa23-pm2a-pm2` is exported for every PM2 command**, so the staging app
lives in its own daemon and process list and cannot appear in — or be reached from —
production's.

**Only `woa23-staging-candidate` is ever named.** `pm2 delete all`, `pm2 restart all`,
`pm2 stop all`, `pm2 kill` and a global `pm2 save`/`pm2 resurrect` are **forbidden**,
with or without `PM2_HOME` set: each acts on every app in whichever daemon is addressed,
and one missing `export` would make that production's.

**Isolation is verified, not assumed:** production's PM2 list is read once with
production's own `PM2_HOME` before and after, and the staging app must appear in the
staging daemon's list and **not** in production's.

## 5. What the run does — and nothing else

*(§9 is the ordered, executable form of this list, with each step's stop condition. If
the two ever disagree, §9 governs — it is what the operator follows.)*

1. start the PM2 candidate under the staging `PM2_HOME`;
2. **PM2 reports `online`**;
3. **the running process is `api.app`** — read from its own `/proc/<pid>/cmdline`;
   `woa23_app` must appear nowhere in the staging tree's argv;
4. startup / import / lifespan validation (`autorestart: false`, so a crash stays a
   crash);
5. **readiness on 18231**, served from the synthetic store;
6. **OpenAPI `info.version` = `1.1.0`** at `/api/swagger/woa23/openapi.json`;
7. the **row-order statement** present in the OpenAPI description and both endpoint
   descriptions;
8. **basic JSON and CSV requests** succeed. The concrete pair, chosen so both span the
   three groups:

   ```
   GET /api/woa23?lon0=134&lat0=14&lon1=138&lat1=17&parameter=temperature,salinity&time_period=0,1,2,13
   GET /api/woa23/csv?lon0=134&lat0=14&lon1=138&lat1=17&parameter=temperature,salinity&time_period=0,1,2,13
   ```

   Expected on this fixture: **200**, **144 rows** each (4 lon x 3 lat x 3 depth x 4
   periods), identical row counts between the two, and columns `lon`, `lat`, `depth`,
   `time_period`, `temperature`, `salinity` — the bare parameter names the `mn` default
   produces. Verified offline by `bench/test_staging_store.py` before this request was
   written;
9. **row order** ascends by numeric `(time_period, depth, lat, lon)` — over a query
   spanning periods **0, 1, 2 and 13**, which opens **three** groups, so the ordering is
   across groups and not one group's natural order;
10. **`pm2 restart woa23-staging-candidate`** — returns to online and still passes 3–9;
11. **precise stop**, then **cleanup**: process gone, **18231 free**, PM2 state and logs
    finalised;
12. **production unchanged** — master PID, worker PIDs, starttimes, listeners on
    8050/8786/8787, and boot id, read **before and after** from `/proc` and `ss` only.

**Production receives 0 requests.** Nothing addresses 8050, 8786 or 8787.

## 6. Forbidden in this run

- touching the production API, production PM2, or anything under `conf/`;
- `pm2 delete all` / `restart all` / `stop all` / `kill`, or a global `pm2 save`;
- using production's app name `woa23`;
- **`SIGKILL`** in any form;
- **self-rerun** of any kind;
- clearing or overwriting failure evidence;
- drawing any **production cutover** conclusion.

## 7. Failure handling

**If cleanup fails, the whole run is `CLEANUP_FAIL`.** State and diagnostics are
**kept**, the run is **not** repeated, nothing is cleared, and the result is reported as
`CLEANUP_FAIL` with the retained evidence named. Releasing that state is a separate
human decision, as spec 002's cleanup policy requires.

Any preflight, digest, identity, isolation or check failure **stops the run and
preserves everything**. No rule is relaxed to obtain a pass.

## 8. Pre-start re-confirmation — read-only, this run's own values only

*(Executed as §9 steps 1–3 and 12. Listed here as the acceptance criteria a reviewer
checks; §9 is the order they happen in.)*

Nothing from `c1f`, `c2g`, `s2pB` or any earlier run is carried forward.

1. `~/woa23-pm2a/`, `~/woa23-pm2a-work/`, `~/woa23-pm2a-pm2/` **absent**; label `pm2A`
   has **0** artefacts;
2. **`18231` absent from `scripts/ports_used.tsv`** and **actually unbound** (`ss`) —
   two different questions, both asked;
3. production **8050 / 8786 / 8787** listening exactly as before;
4. production **master PID, worker PIDs, starttimes, listeners** and **boot id**;
5. staging store **non-empty, readable, anchored, not under production's**;
6. archive SHA-256, file count, file-list SHA-256 and the five `api/` hashes re-derived
   on VM24, and the staged tree compared **file by file** against §1;
7. **`PM2_HOME` isolation confirmed** — production's PM2 list read separately, and the
   staging app absent from it;
8. after start, the candidate's **argv is `api.app`**, not `woa23_app`.

## 9. The execution sequence — ordered, and each step's stop condition

**Every step is a stop.** If a step's condition is not met, the run halts there, keeps
everything it has, and is reported at that step. Nothing later is attempted, and no
step is skipped to reach a later one.

### Step 1 — read-only: the identity does not exist

`~/woa23-pm2a/`, `~/woa23-pm2a-work/`, `~/woa23-pm2a-pm2/` **absent**; label `pm2A` has
**0** artefacts. **Any of them existing is a stop** — a pre-existing path means an
earlier attempt, and its evidence is not overwritten.

### Step 2 — read-only: the port

`18231` is **`PROPOSED` in `scripts/ports_used.tsv`** and **actually unbound** on the
host (`ss -ltn`). Two different questions; both asked. Either failing is a stop.

**The ledger is not updated here.** It is updated to bound/consumed **after** a run that
actually bound the port — a port a run never bound is not a port that run used.

### Step 3 — the archive, verified file by file on VM24

Re-derive on VM24 and compare with §1: archive SHA-256, file count **135**, file-list
SHA-256, the five `api/` hashes, and the two launcher hashes. Then compare the staged
tree **file by file** against the subject commit. **Any mismatch stops the run before
anything is created.**

### Step 4 — staging tree and its environment

Create `~/woa23-pm2a/` **only**, extract the archive, then create/sync the pinned uv
environment:

```
cd ~/woa23-pm2a/dev2026 && uv sync
```

`pyproject.toml` pins the runtime the service needs — gunicorn 23.0.0, uvicorn 0.34.1,
fastapi 0.115.12, polars 1.27.1, xarray 2025.3.1, zarr 2.18.6 — at production's
versions. **`uv` may create only this venv**, as the campaign's standing rule requires.

### Step 5 — build the synthetic store, from the archive's own builder

```
PYTHONPATH=. .venv/bin/python deploy/make_staging_store.py /home/odbadmin/woa23-pm2a/store
```

The builder is `deploy/make_staging_store.py` **from the verified archive** — not a copy,
not a re-typed script. It **refuses an existing path** rather than clearing it.

### Step 6 — verify the store before it is served

| check | expected |
|---|---|
| files | **72** |
| bytes | **25,191** (~25 KB) |
| file-list SHA-256 | `8fb70f2c64d7a3ee3d7fa451de08218c7a1740b19451cf015dd83394fd328624` |
| physical path | **not** under `/home/odbadmin/python/woa23/data` (`pwd -P` on both, compared) |
| anchor | `1_degree/annual/TS/.zgroup` present and readable |
| openable by the candidate | the lifespan opens it at step 8 without raising |

**Then make it read-only** — `chmod -R a-w` on the store — and confirm with a write
attempt that fails. A staging store the service cannot write is a staging store the
service cannot corrupt. **If the platform refuses the change, record that it was not
applied**; it is a hardening step, not a precondition, and it is not silently skipped.

### Step 7 — record the effective store

```
WOA23_ZARR_STORE = /home/odbadmin/woa23-pm2a/store
```

Absolute, exported by the launcher from `WOA23_STAGING_STORE`. Recorded as the value
that actually took effect, read back from the running process's environment.

### Step 8 — start, in an isolated PM2 daemon, one app only

```
cd ~/woa23-pm2a/dev2026
export PM2_HOME=~/woa23-pm2a-pm2
PATH=~/woa23-pm2a/dev2026/.venv/bin:$PATH \
WOA23_STAGING_PORT=18231 \
WOA23_STAGING_STORE=/home/odbadmin/woa23-pm2a/store \
WOA23_PRODUCTION_STORE=/home/odbadmin/python/woa23/data \
  pm2 start deploy/ecosystem.staging.config.js --only woa23-staging-candidate
```

**`PATH` is set deliberately.** `deploy/start_staging.sh` runs `exec gunicorn`, which
resolves through `PATH`, and PM2 inherits the environment of whoever starts it. Putting
the staging venv first is what guarantees the service runs **the pinned gunicorn from
the verified tree** rather than whatever happens to be on the operator's path. *(The
launcher would be more robust invoking `.venv/bin/python -m gunicorn` directly; that is
a follow-up improvement to `deploy/start_staging.sh`, not a change to be made inside
this run — it would move the execution subject again.)*

`WOA23_PRODUCTION_STORE` is set, so the **store guard is armed**: the launcher refuses
outright if the staging store ever resolves under production's.

### Step 9 — the running process is the CANDIDATE

Read from the process's own `/proc/<pid>/cmdline`: the argv contains **`api.app`**.
**`woa23_app` must appear nowhere** in the staging tree's argv. Reading the config that
was *supposed* to start it proves nothing; the process's own argv does.

### Step 10 — what the service returns

- PM2 reports **`online`**; the lifespan completed (`autorestart: false`, so a crash
  stays a crash);
- **readiness** answers on 18231;
- `/api/swagger/woa23/openapi.json` → **`info.version` = `1.1.0`**, and the row-order
  statement present in the description **and both endpoint descriptions**;
- both requests return **200** with **144 rows** each and identical row counts:

  ```
  GET /api/woa23?lon0=134&lat0=14&lon1=138&lat1=17&parameter=temperature,salinity&time_period=0,1,2,13
  GET /api/woa23/csv?lon0=134&lat0=14&lon1=138&lat1=17&parameter=temperature,salinity&time_period=0,1,2,13
  ```

- rows ascend **strictly** by numeric `(time_period, depth, lat, lon)`, JSON and CSV
  carrying an identical key sequence, with **13 after 2** — over a query that opens
  **three** groups.

### Step 11 — restart, stop, and cleanup

```
pm2 restart woa23-staging-candidate     # then re-check steps 9 and 10
pm2 stop    woa23-staging-candidate
pm2 delete  woa23-staging-candidate
```

Then: the process is **gone**, **18231 is free**, and PM2 state and logs are finalised.
**Only the named app is ever addressed** — never `all`, never `pm2 kill`, never a global
`save`/`resurrect`, and **never `SIGKILL`**.

### Step 12 — production unchanged, read before AND after

Production's **master PID, worker PIDs, their starttimes, the listeners on 8050 / 8786 /
8787, and the boot id** — read from `/proc` and `ss` only, **at step 1 and again after
step 11**, and compared. Production receives **0 requests** throughout.

**PM2 isolation is verified the same way**: production's PM2 list is read separately
under production's own `PM2_HOME`, before and after, and the staging app must appear in
the staging daemon's list and **not** in production's.

### Cleanup failure

If step 11's cleanup does not complete — process still present, port still bound, state
not finalised — the whole run is **`CLEANUP_FAIL`**. State and diagnostics are **kept**,
the run is **not** repeated, nothing is cleared, and it is reported as `CLEANUP_FAIL`
naming the retained evidence. Releasing that state is a separate human decision.

## 10. What a PASS here does NOT mean

- **It is not a production deployment validation.** Different launcher, alternate port,
  no TLS, no reverse proxy, no production process touched.
- **Blockers B1–B5 remain open** (spec 010 §5a) and are handled separately: production's
  `pre_stop` grep + `kill -9`, its launcher's old `woa23_app`, its hard-coded 8050, its
  missing `WOA23_ZARR_STORE`, and its `--reload`. **None is fixed by this run, and none
  may be fixed inside it.**
- **No performance claim.** Nothing here is timed, and the store is 25 KB.
- **No real-store correctness claim.** The fixture is synthetic (§3.5).
- **No cutover conclusion.** A production cutover is a separate request, only after this
  passes, and it must carry backup, rollback, health checks, TLS and reverse-proxy
  handling, and production listener checks.
