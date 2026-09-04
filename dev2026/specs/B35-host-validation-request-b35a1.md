# B5 + B3 host validation `b35a1` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. Nothing has been run.**
No VM24 contact. This is **staging-only**. It **does not close B5 or B3** — §4 says
exactly what it can and cannot establish.

---

## 1. Scope — deliberately narrow

**B5** — the launcher passes no `--reload`.
**B3** — the port comes from `WOA23_PORT`; no `8050` literal is reachable.

Both are assertions on the **argv that actually reaches gunicorn** in one staging start.
They share a single start; running them separately would mean two identical runs for two
`grep`s on the same argv. **If you prefer strictly one blocker per run, say so and I will
split this into two requests.**

**Not in this request:** B1, B2, B4, B6, B7, any cutover, any production file, any
timing. No C1/C2 rerun, no latency, no rung-60 work.

---

## 2. Two findings from the offline audit that shape this request

### 2.1 The staging harness has never run as uid 994

`pm2G` — and `pm2B`, `pm2F` before it — ran as **`odbadmin`**: every path is under
`/home/odbadmin/woa23-pm2g/`. Running the same harness as **`woa23c1ro`** is a **new,
unvalidated configuration**.

Two prerequisites are **unverified and unverifiable offline**:

| tool | needed for | status for uid 994 |
|---|---|---|
| `node` | `staging_execute.sh:367` — generates the config, parses `pm2 jlist` | **UNKNOWN** |
| `pm2` | `staging_execute.sh:387` — starts and inspects the staging app | **UNKNOWN** |

Both may live under `odbadmin`'s home or an `nvm` tree unreadable to uid 994. **I have
not guessed.** Phase 0 below resolves them and aborts harmlessly if either is
unreachable.

### 2.2 `ss -p` is not relied on, and must not become so

The only `ss` in `deploy/` is `ss -ltn` (listener presence). No check here depends on
socket→PID ownership visibility, which uid 994 does not have.

---

## 3. Execution plan — four phases, each with its own abort

| phase | what it does | creates state? | abort behaviour |
|---|---|---|---|
| **0 — capability probe** | `command -v` for `node`, `pm2`, `uv`; their versions and paths; that the staging root's parent is writable by uid 994; production and pm2G baselines | **no** | if `node` or `pm2` is unreachable: **STOP**, report, create nothing. Identity retired NEVER-BOUND. |
| **1 — stage** | extract the authorised archive, verify file count and file-list digest | staging tree only | subject mismatch → stop, tree left for inspection, nothing deleted |
| **2 — preflight-only** | `staging_execute.sh --phase run … --preflight-only` | no PM2 app, no port bound | any refusal → stop |
| **3 — run** | start the staging app, **capture the argv**, assert B5 and B3, stop, verify release | PM2 app on the first-use port | any failure → stop, state preserved |

**Phase 0 is the point of this design.** The campaign's repeated cost has been runs that
aborted after creating state. Here, the one genuinely unknown thing is checked before
anything is created.

---

## 4. What this run can establish — and what it CANNOT

**It does NOT close B5 or B3.**

| | this run CAN establish | still REQUIRED, and only by cutover |
|---|---|---|
| **B5** | `deploy/production_app.sh`'s argv, as launched under PM2 by uid 994, contains **no `--reload`** | that **production's installed launcher** has none. The defect is live in `conf/start_app.sh` (`4aaed5b7…`), which this run never touches. |
| **B3** | the port comes from `WOA23_PORT`; the argv binds the authorised first-use port; **no `8050` literal** is reachable in the file | that production's **real** port and config are separated in the **installed** files, and that nginx/TLS in front still resolve |

**The trap, named:** *"B5 is closed — the launcher has no `--reload`"* is **false**.
A file that is not installed cannot close a blocker about the file that is.

Also not established: anything about the real 35 GB store (this uses a synthetic one),
TLS, nginx, real traffic, or timing of any kind.

---

## 5. Execution subject

```
commit           823a6edef5bb33c1e724131dafbf288f5af5e6f5
subject line     spec(013): B1-B5 host validation matrix, ownership audit,
                 evidence boundary
archive sha256   85d032e8c1358ef766fe26a0cbbf4a435643fcb4628d7f166e0e17fe08cf2fb3
files            203
file-list sha256 241f150bacce036745c314a4237caba1745cc84f1fb45b2cf0079aa8e71ab203
```

`verify_clean_archive.sh` — **all passed (16 assertions)**.

**No executable code changed** from the `c2k` subject `a5ca913`: the only additions are
spec documents. `api/query.py` unchanged at `50907dee…2ca8`.

### 5.1 Source hashes to verify in pre-flight

```
07bc37642d9c0f82ed709c91f973170b85b7df7ab98ef6058eb49eea3e71295a  deploy/production_app.sh
e86f07f18b38ee4b470e7049d15c5363cfe9c3f8847e2602aa0096c1e6b7803f  deploy/production_stop.sh
a7e4cb7fcb47e83b8192b225093e5dbe20aad2c673e5fed511b2328ee22410c4  deploy/ecosystem.production.config.js
47cf48cfc8125ef0cec9355906493493a53700e6a058b175303406301d71457a  deploy/ecosystem.staging.config.js
ab256716c1a919b6322425db3ddba36131d0756bb3ac196e541ab25bd9c4ccd6  deploy/start_staging.sh
bd505a4d23a4e434f2733f10da970d111d400cb4326105390921251348f154f3  deploy/staging_execute.sh
e42c341509fc5d22c634d7947a505bec4df66cf2de18f1e3cddc1e9d31aac5cf  deploy/verify_staging_env.sh
cf121f7f41e15cd9a381d461772e2bc4a8b58281f7ba341baef14e9d3d5f69f1  deploy/make_staging_store.py
143ffa6381750789e33cdea1004c680bd1e8dc83b49267da2d77d903317ccd57  deploy/make_staging_override.js
```

### 5.2 Offline evidence for this subject

Three serial batches in the main tree at `823a6ed`, each recording HEAD itself:

```
batch1 exit=0 head=823a6edef5bb33c1e724131dafbf288f5af5e6f5 dirty=0
batch2 exit=0 head=823a6edef5bb33c1e724131dafbf288f5af5e6f5 dirty=0
batch3 exit=0 head=823a6edef5bb33c1e724131dafbf288f5af5e6f5 dirty=0
```

**47 suites, 3962 assertions, 0 non-zero exits, 0 differences** across all three
pairings. Roots `BUtyJI`, `lC8IJZ`, `g7GVCo`.

Launcher suites: `test_production_launcher.sh` **111**, `test_production_stop.sh` **33**
(**144 total — the "121" figure was stale**), `test_staging_entry.sh` 125,
`test_staging_launcher.sh` 101, `test_staging_override.sh` 77.

**These are offline assertions and are NOT host validation.**

---

## 6. Execution identity — new label, first-use port

| | value |
|---|---|
| **grant** | `WOA23_PM2C_GRANTED=yes` (the staging validation MODE) |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993** |
| **connection** | direct SSH, campaign Ed25519 key, `BatchMode=yes`, `IdentitiesOnly=yes`, `RequestTTY=no` |
| **label** | **`b35a1`** |
| **staging root** | `/home/woa23c1ro/woa23-b35a1/` |
| **workdir** | `/home/woa23c1ro/woa23-b35a1-work/` |
| **`PM2_HOME`** | `/home/woa23c1ro/woa23-b35a1-pm2/` — **new, owned by uid 994** |
| **PM2 app name** | `woa23-b35a1-candidate` |
| **TMPDIR** | `/home/woa23c1ro/tmp-b35a1/` |
| **staging store** | `/home/woa23c1ro/woa23-b35a1/store` — **synthetic** |
| **port** | **`18281`** |

**Label and port screening.** `b35a1`: 0 occurrences anywhere in the subject.
`18281`: **0 ledger rows, 0 tree occurrences** — the two-way screen that rejected 19109,
19113, 19116, 19118, 19136, and here also `18271` and `18273`. `pm2H` and `b35A` were
both rejected: `pm2H` is already referenced in four files, `b35A` occurs inside
`uv.lock`. Not pre-recorded in the ledger.

**Nothing of pm2G is reused** — not its label, `PM2_HOME`, port 18265, app entry,
retained state or store path.

---

## 7. Production-impact statement

**Intended impact on production: NONE.**

| | |
|---|---|
| production files written | **none.** `conf/start_app.sh` and `conf/ecosystem.config.js` are read for their hashes only |
| production PM2 entry | **not touched.** This run's `PM2_HOME` is its own; `/home/odbadmin/.pm2` is never named, and uid 994 could not reach it anyway |
| production service | **not stopped, not restarted.** Its `(pid, starttime)` and boot id recorded before and after |
| production requests | **zero** |
| production store | **read-only.** Full scan as uid 994; the staging store is synthetic and under `woa23c1ro`'s home |
| pm2G / 18265 | **untouched** — not stopped, deleted, cleaned or destructively inspected; presence checked with `ss -ltn` and world-readable `/proc` only |
| ports | one first-use port, 18281 |
| store ACLs, runtime, `.lock`, permissions | **not modified** |

**The one way this could touch production** is a `PM2_HOME` pointing at
`/home/odbadmin/.pm2`. It is set explicitly to `/home/woa23c1ro/woa23-b35a1-pm2/`, is
never defaulted, and phase 0 asserts it is not production's.

---

## 8. Failure handling and stop conditions

**Abort before creating anything** (phase 0): `node` or `pm2` unreachable by uid 994;
identity not `uid=994`; the staging root's parent not writable; production pid/starttime
or boot id not readable; pm2G not in its expected retained state.

**Abort before starting** (phases 1–2): archive, file count, file-list or any per-file
hash mismatch; port bound or present in the subject's ledger; any identity path already
present; `PM2_HOME` resolving anywhere under `/home/odbadmin`.

**Fail the run** (phase 3): argv contains `--reload`; argv does not bind 18281; any
`8050` literal reachable; the app fails to start; the port is not released; production's
pid/starttime or boot id changed at any point; pm2G's state changed.

**On any failure:** stop immediately, preserve all state and logs for inspection, delete
nothing, and **do not retry**. The identity is consumed either way.

**Never, under any outcome:** `pm2 delete all`, `pm2 kill`, any write under
`/home/odbadmin`, any SIGKILL, or a retry with a "fixed" flag.

---

## 9. Submission

`b35a1` is submitted for **explicit authorisation**. It has not been executed and no
VM24 contact has been made.

Two things I would like your decision on:

1. **B5 and B3 together, or split into two runs?** They share one start; I propose
   together, and will split on request.
2. **Phase 0 as part of this run, or as a separate read-only probe first?** If `node` or
   `pm2` turns out to be unreachable, this identity is consumed for a capability check.
   A standalone read-only probe would answer it without spending an identity — say the
   word and I will submit that instead.
