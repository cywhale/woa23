# D-3 — candidate deployment rehearsal observation, subject `a361f70`

**This is an OBSERVATION. It is not production equivalence, not a deployment PASS, not
data-path correctness PASS, not TLS validation, and not A11 validation. It does not
adjudicate anything and it back-fills nothing.**

**TLS UNVALIDATED** — the candidate ran with `WOA23_TLS=off` and production's key and
certificate verified ABSENT from master and every worker. No TLS claim of any kind is made.

**The candidate, its daemon, staging tree, workdir, `PM2_HOME`, store symlink and all
artefacts are RETAINED and still running. Cleanup is a separate authorisation.**

---

## 1. Identity and provenance

```
subject   a361f70668f28eaec49fabf078ca4c9c05d5d4ed
archive   92e93bd378af5835ab3d917e6a90ffbfbc40b2632dffbcabee99c70ac6da6ba3
          272 tar members, 261 regular files
file-list 699d0f55175db02fe0aa6083c7223685c4082bfe2e9f450968a182e47f7c43db
label     dep3s        port 19387        app woa23-dep3s-candidate
```

The archive was verified by **digest and member count at both ends** of a fresh transfer,
and the staged tree's file-list was re-derived and matched **before the run, before the
cases and after the cases**.

---

## 2. What ran

| | |
|---|---|
| PM2 daemon | `2035966`, `PM2_HOME=/home/woa23c1ro/woa23-dep3s-pm2` |
| master | `2035977`, starttime `lin-162712475`, uid 994 |
| workers | `2035986` `lin-162712520`, `2035987` `lin-162712525`, uid 994 |
| argv | `.venv/bin/python -m gunicorn api.app:app -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:19387 --timeout 120 --graceful-timeout 10` |
| interpreter | CPython **3.11.14**, uv-managed, under uid 994's own tree |
| store | `~/woa23-dep3s/store` → `/home/odbadmin/python/woa23/data`, **real, read-only** |

`api.app:app` present; **no `--reload`**, no `woa23_app`, no `--keyfile`. Both workers load
**every** library from this run's venv — `venv=416, shared_py311=0, production=0` each.

Environment read back from `/proc`: `WOA23_PORT=19387`, `WOA23_ZARR_STORE=<staging
symlink>`, `WOA23_TLS=off`, `WOA23_WORKERS=2`, `WOA23_PYTHON=<staged venv>`;
`WOA23_TLS_KEYFILE`, `WOA23_TLS_CERTFILE`, `WOA23_ANCHOR_REL`, `WOA23_PRODUCTION_STORE` and
`WOA23_PM2C_GRANTED` all **ABSENT**, on master and every worker. No `WOA23_*` outside the ten.

---

## 3. The eight observations

**Eight concrete HTTP requests, each issued exactly once, sequentially, to
`http://127.0.0.1:19387` only.** No retry, no replay, no parallelism, no redirect
following. **Not one request to production 8050.** The two remaining entries of the
countable budget of ten are store-readiness / filesystem probes, not HTTP cases — see the
request's §4B.

**No expected status exists for these cases and none was written.** The statuses below are
recorded as observed and are **not** labelled expected or unexpected.

| slot | case | endpoint | status | bytes | time | content-type |
|---|---|---|---|---|---|---|
| 1 | `D1-DEPTH-SUP` | `/api/woa23` | **200** | 3075 | 1.516 s | `application/json` |
| 2 | `D1-ANCHOR-RECOVER` **#1** | `/api/woa23` | **200** | 9033 | 0.030 s | `application/json` |
| 3 | `D1-DEPTH-SUP-csv` | `/api/woa23/csv` | **200** | 872 | 0.083 s | `text/csv; charset=utf-8` |
| 4 | `D1-ANCHOR-RECOVER` **#2** | `/api/woa23` | **200** | 9033 | 0.348 s | `application/json` |
| 5 | `D1-DEPTH-OOR-tp13` | `/api/woa23` | **200** | 2 | 0.161 s | `application/json` |
| 6 | `D1-ANCHOR-RECOVER` **#3** | `/api/woa23` | **200** | 9033 | 0.031 s | `application/json` |
| 7 | `D1-DEPTH-OOR-tp13-csv` | `/api/woa23/csv` | **400** | 56 | 0.011 s | `application/json` |
| 8 | `D1-ANCHOR-RECOVER` **#4** | `/api/woa23` | **200** | 9033 | 0.029 s | `application/json` |

**The four `D1-ANCHOR-RECOVER` entries are four ordered sequence slots, not retries.** Each
followed its own case before the next began. All four returned 200 with an identical body
digest `5568923220baf4bd…`, so the process served the anchor group after every case.

### 3.1 The bodies, as returned

- **slot 1** annual nitrate 0–800 m, JSON **200**: an array of depth rows,
  `{"lon":135.5,"lat":15.5,"depth":0.0,"time_period":"0","nitrate":null}` …
- **slot 3** the same request as CSV, **200**: header `lon,lat,depth,time_period,nitrate`
  then rows with an empty nitrate column
- **slot 5** winter nitrate `time_period=13`, 3000–4000 m, JSON **200**: body is `[]`
- **slot 7** the same request as CSV, **400**:
  `{"detail":"No data available for the given parameters."}`

**The JSON/CSV divergence at slots 5 and 7 is recorded as observed.** It is not rewritten
toward any target and it is not adjudicated here; D-1 `d1b` recorded the same pair, and
spec 006 carries it as observed current behaviour.

Full URL, status, timing, size, body sha256 and the complete body are retained for every
slot in [`invalid-attempt-a361f70/`'s sibling](exec-a361f70-cases) — outside the staging
tree.

---

## 4. Process and store evidence, before and after

### 4.1 The candidate did not restart

| | before the cases | after the cases |
|---|---|---|
| master | `2035977` `lin-162712475` | **identical** |
| worker | `2035986` `lin-162712520` | **identical** |
| worker | `2035987` `lin-162712525` | **identical** |

Same pids **and** same starttimes, so this is the same process and not a reused pid.

### 4.2 Survivor inventory — no unexpected survivor

Taken with the canonical tool (method recorded, digest recomputable from the rows it
prints), before and after:

```
before : 46a044e1a3b33ec66b258528b522534d77892b1cdc32b8d36efa5906cc5e886c   12 rows
after  : 7b09f8f61810a6a58b97404e5bc689d4a373996fced040db8d75e2b396b15420   16 rows
```

The delta is **exactly this run's own four processes**, and nothing else:

```
+ 2035966  PM2 v5.4.2: God Daemon (/home/woa23c1ro/woa23-dep3s-pm2)
+ 2035977  .../.venv/bin/python -m gunicorn api.app:app -w 2 ... -b 127.0.0.1:19387
+ 2035986  (worker)
+ 2035987  (worker)
```

The two retained daemons `1709473` (bs3v1) and `1761143` (b1s1) are still present and
untouched.

### 4.3 Production store — unchanged throughout

`/home/odbadmin/python/woa23/data`, metadata fingerprint at **every** checkpoint — before
the run, after the reversible move, before the cases and after the cases:

```
abe6c21221b5081eb352a1a549c9d1fd6399c74b1f06b8c2ce09952828c61806
123005 files, 35101630061 bytes, mode 775, owner 1000:1000
```

Guards at run time: owner uid **1000** ≠ execution uid **994**; not writable; no writable
ancestor; **0** writable entries, **0** symlinks, **0** unreadable, **0** untraversable
beneath it; ACL grants `woa23c1ro` `r-x` only. The **only** filesystem write the real-store
branch performed was `ln -s`, inside the staging root.

**This fingerprint is metadata-only — path, size, mtime.** It is not a content digest, and
a same-size, same-mtime change is invisible to it. It is used for within-run before/after
comparison and no cross-run equality is claimed from it.

### 4.4 Production untouched

| | |
|---|---|
| production port 8050 | its own listener, **no request issued to it by this run** |
| production `PM2_HOME` `/home/odbadmin/.pm2` | **never read, never written** |
| default `~/.pm2` | **absent** — never created |
| production PM2 lifecycle | never invoked; no `save`, `resurrect`, `delete`, wildcard, `all`, `kill` or signal |
| PM2 binary | the recorded production-owned installation, digest `bbb58671…256d` verified, **read-only to uid 994**, `pm2 -v` never invoked |

---

## 4A. FROZEN — and what this observation is NOT

**This result is FROZEN as a qualified candidate deployment rehearsal observation.** All
evidence is preserved: this document, the eight response bodies and records, the case
manifest and issuer, the before/after process inventories, the run-phase log, the PM2 /
config / store evidence, and the archive, transfer and bootstrap evidence.

**It is not back-filled into anything.** Specifically it is **not** production equivalence,
**not** data-path PASS, **not** TLS PASS, **not** A11 validation, and **not** performance
evidence. No timing figure here is a performance measurement: eight requests issued once
each, on one arm, with no reference and no repetition, cannot measure performance and are
not offered as if they could.

## 4B. API contract finding — JSON and CSV disagree on a valid no-result query

Recorded as an **API contract finding only**. It is not a defect adjudication and it does
not classify the run.

| the same semantic condition | route | observed |
|---|---|---|
| valid query, zero matching rows | JSON `/api/woa23` | **HTTP 200** with an empty result — `[]` |
| valid query, zero matching rows | CSV `/api/woa23/csv` | **HTTP 400** with an error body — `{"detail":"No data available for the given parameters."}` |

Both slots asked the same question — winter nitrate, `time_period=13`, 3000–4000 m — and
differ only in endpoint. The query is well-formed and its parameters are valid; the depth
range simply lies outside seasonal nitrate's 0–800 m extent, so the correct answer is *no
rows*, which the JSON route expresses as an empty result and the CSV route as an error.

The CSV body is also a **JSON** object (`application/json`), not CSV, so a CSV client
receives neither CSV nor a status it can treat as a normal empty response.

D-1 `d1b` recorded the same pair, and spec 006 already carries it as observed current
behaviour. **It is recorded here, not resolved here.**

---

## 4C. Cleanup, and an out-of-scope deviation in it

The dep3s candidate was stopped and its runtime state removed under a separate
authorisation. **The enumerated paths were not the only ones removed**, and that is
recorded here rather than glossed.

**Authorised and removed:** staging root, workdir, isolated `PM2_HOME`, tmpdir (never
created), store symlink (verified `-L` with target exactly
`/home/odbadmin/python/woa23/data`, then `unlink` on the **link only**), generated staging
config.

**Removed but NOT enumerated — a deviation:**

| path | what it was | impact |
|---|---|---|
| `~/woa23-dep3s-uvcache` | the empty task-specific uv cache created by this run's first, failed `uv sync` | **none on production**; it held no evidence and was never read successfully |

It was a dep3s runtime path this run created, which is why it was swept up, but it was
outside the list I was given and I should have asked before removing it. **It has not been
recreated**: manufacturing an empty directory to make the deviation disappear would be
falsifying the record, which is worse than the deviation.

**Not touched:** the shared cache `/home/woa23c1ro/.cache/uv` (9971 entries, 502150139
bytes — unchanged), all D-3 evidence, and the retained `dep3m`, `dep3h`, `bs3v1`, `b1s1`
and `pm2G` state.

**Post-cleanup:** port 19387 free, 0 dep3s processes, production 8050 still serving, both
retained daemons present with identical starttimes, production store `abe6c212…c61806` with
no new drift.

---

## 5. Standing limits

- **TLS UNVALIDATED.** `WOA23_TLS=off`; no TLS claim.
- **A11 is not a gate** and was not used as one.
- **Runtime divergence.** Staging built CPython **3.11.14**; production runs **3.11.4**.
  A rehearsal on 3.11.14 does not validate 3.11.4, and no response difference is
  attributable to the API code alone.
- **Eight requests, one arm.** There is no reference arm, so nothing here is a
  candidate-versus-reference comparison.
- **No expected status exists** for these cases, so nothing was compared against one.
- **The store fingerprint is metadata-only** (§4.3).

---

## 6. Status

| | |
|---|---|
| classification | **candidate deployment rehearsal observation** |
| requests issued | **8**, exactly once each, to `127.0.0.1:19387` only |
| requests to production 8050 | **0** |
| candidate | **online and RETAINED** — daemon `2035966`, master `2035977`, workers `2035986`/`2035987` |
| artefacts | staging tree, workdir, `PM2_HOME`, store symlink, config, evidence — **all retained** |
| unexpected survivor | **none** |
| production | **unchanged** |
| cleanup | **NOT performed — separately authorised** |
