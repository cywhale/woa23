# Stage A — production read-only inventory: **RESULT**

**Read-only. Executed under the Stage A authorisation and nothing else.** No `pm2 stop`,
`start`, `restart`, `reload`, `delete`, `kill`, `save` or `resurrect`. No file written on
VM24. No config modified. No API called. No daemon created. No cleanup. `b1s1` and `bs3v1`
retained state untouched.

Subject `787a72d0062ef581a31b752971cfdb5ecb8edc91`.

---

## 0. Execution identity and daemon identification — done BEFORE any `pm2` subcommand

| | |
|---|---|
| user / uid | **`odbadmin` / 1000** — production's PM2 owner |
| host | `odb24` |
| **boot id** | **`0b513a75-213b-40bf-8219-1c7cbc51a085`** |
| `PM2_HOME` | **`/home/odbadmin/.pm2`** |

**Daemon identified from `/proc` alone, before `pm2` was invoked at all:**

```
pid file      /home/odbadmin/.pm2/pm2.pid  ->  3459
/proc/3459/status   Name: PM2 v5.4.2: God   State: S   PPid: 1   Uid: 1000
/proc/3459/cmdline  PM2 v5.4.2: God Daemon (/home/odbadmin/.pm2)
(pid, starttime)    (3459, 13189)      fields after comm: 50
/proc/3459/environ  PM2_HOME=/home/odbadmin/.pm2   -> MATCHES the candidate
fds under PM2_HOME  2
```

Identity re-confirmed **immediately before and immediately after** `jlist`, unchanged both
times. Because the daemon already existed, `jlist` attached to it and **could not spawn
one**.

---

## 1. A1 — exact registered app name: **`woa23`**

**`append_env_to_name: true` produced NO suffix.** The registered name is the bare string
`woa23`, recorded verbatim from `jlist`. No guess, no wildcard.

**All nine registered apps** (A9):

| name | pm_id | pid | status |
|---|---|---|---|
| `gateway` | 0 | 0 | stopped |
| `odbbathy` | 1 | *(undefined)* | stopped |
| `mhwapi` | 2 | 1088291 | online |
| **`woa23`** | **3** | **4295** | **online** |
| `ghrsst` | 4 | 1751875 | online |
| `ghrsst_mcp` | 5 | 4337 | online |
| `dask-scheduler` | 6 | 4357 | online |
| `dask-worker` | 7 | 4358 | online |
| `tide` | 10 | 4393 | online |

---

## 2. A2 — production PM2 version: **5.4.2**

Obtained from two sources that **cannot spawn a daemon**. `pm2 -v` was **not** run.

- the running God Daemon's own `cmdline`: `PM2 v5.4.2: God Daemon`
- `.npm-global/lib/node_modules/pm2/package.json`: `name=pm2 version=5.4.2`

**Same version as `b1s1`'s staging daemon.** That does **not** transfer `b1s1`'s result to
production — it removes one of the differences between them, and nothing more.

---

## 3. A6 — `pre_stop`: **THE FILE HAS IT; THE DAEMON DOES NOT — AND PM2 5.4.2 DOES NOT KNOW THE KEY**

This is the finding of Stage A, and it **corrects my earlier claim**.

### 3.1 The file

```
/home/odbadmin/python/woa23/conf/ecosystem.config.js
  sha256 8db9a6ba1888821c833452ee9602871045e4017500800ae69b00aba9fdde4340
  mtime  2024-06-18 21:37
  line 16: pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
```

### 3.2 The daemon — four independent sources, all agreeing

| source | `pre_stop` |
|---|---|
| `pm2 jlist` → `pm2_env`, **all nine apps** | **absent** |
| `dump.pm2` (9 entries) | **absent**, top-level and env |
| `dump.pm2.bak` (10 entries) | **absent** |
| any file in `PM2_HOME` containing the string `pre_stop` | **none** |

`dump.pm2` contains **0** occurrences of `woa23_app`. The only hook-shaped keys in
`woa23`'s live definition are `treekill` and `kill_retry_time`.

### 3.3 Why — `pre_stop` is not a PM2 key at all

Read from PM2 5.4.2's own installed source:

```
occurrences of "pre_stop" under pm2/lib : 0
files under lib/ mentioning pre_stop    : 0
schema.json — "pre_stop" present        : 0   (of 65 documented app keys)
hooks PM2 actually defines              : post_update
```

**PM2 5.4.2 has no `pre_stop` hook. The key is silently ignored.** The running app was
started from that very config (`pm_exec_path` = `conf/start_app.sh`, `pm_cwd` =
`/home/odbadmin/python/woa23`) and the key still never entered the daemon's state.

### 3.4 What I got wrong, stated plainly

In `b1p1` I wrote that a production B1 run *"would execute the exact behaviour B1 exists to
replace"* and that the grep *"may reach `tide_app`"*. **Both claims are unsupported.**

- **PM2 will not run it.** The key is inert dead configuration.
- **The `tide_app` link was my conflation.** `conf/simu.sh:13` kills `tide_app` with a
  **separate** command of its own; the `woa23` `pre_stop` greps `woa23_app`, which is a
  different pattern. I joined two facts that are not joined.

### 3.5 The hook's ACTUAL match scope — evaluated, never executed

The selection half was run (`ps -ef | grep -w 'woa23_app' | grep -v grep`) with **no
`xargs`, no `kill`**:

```
match count: 3
  4296  gunicorn woa23_app:app ... -b 127.0.0.1:8050 ...   (master)
  5040  gunicorn woa23_app:app ...                          (worker)
  5041  gunicorn woa23_app:app ...                          (worker)
```

Processes whose command line contains `woa23_app` **and** belong to `tide`: **0**.

**Today, the pattern matches exactly `woa23`'s own three processes.** That is a statement
about the process table as observed, not a guarantee — any future process whose command
line contains `woa23_app` would match too.

---

## 4. A4 / A5 — target tree

```
pid=4295  comm=bash      ppid=3459(PM2)  starttime=14198   bash conf/start_app.sh   <- PM2's tracked pid
  pid=4296  comm=gunicorn  ppid=4295  starttime=14214   (master)
    pid=5040  comm=gunicorn  ppid=4296  starttime=15825   (worker)
    pid=5041  comm=gunicorn  ppid=4296  starttime=15829   (worker)
descendants: 3, depth 2
```

All four identities read cleanly — `PPid:` from `status`, `starttime` from `stat` past the
comm, every value non-zero and all-digits. **No `UNKNOWN`, no unreadable entry.**

**A structural fact Stage C must not trip over:** `start_app.sh` does **not** `exec`, so
**PM2 tracks the bash wrapper (4295), not the gunicorn master (4296)**. A stop signals the
wrapper; the master and workers are descendants. The descendant walk is therefore
load-bearing on production in a way it was not on `b1s1`, whose tree was one level deep.

---

## 5. A8 — listeners, and A10 — conf / store identity

**45 listeners.** `8050` (woa23 API) bound ×1; `18265` (`pm2G`, retained) bound ×1; `8786`
and `8787` (dask) bound; `18283` and `19157` **unbound**.

**`conf/` — 5 files, content digests, file-list sha256
`6840b8fc0cc6b106e6abd3314d7e5451ada1b4814f798892b99dee6e47856083`:**

```
8db9a6ba…4340  ecosystem.config.js      6e275f15…c0d9  fullchain.pem
5130653c…a4cc  privkey.pem              3bc46819…eef9  simu.sh
4aaed5b7…6d77  start_app.sh
```

**Store — METADATA ONLY, and the limit is the record.** `WOA23_ZARR_STORE` is **not set** in
the production app's environment; the inspected path is
`/home/odbadmin/python/woa23/data` — **33G apparent, 123,005 files**. A content digest was
**not** attempted: it would read the entire dataset. **Store identity is therefore
metadata-level only**, and any Stage C claim about it inherits that limit.

---

## 6. A11 — API request count: **QUALIFIED PROXY ONLY. NOT an exact count.**

**No HTTP request was made.** Calling the API would increment the very count being measured.

The app logs a per-query marker. Counts are identical in both files:

| marker | `woa23.log` | `woa23.outerr.log` |
|---|---|---|
| `Handling parameters and time_periods` | **10950** | **10950** |
| `Total time for this query taken` | **10967** | **10967** |

**Defined method, for a DELTA and nothing more:** count `Handling parameters and
time_periods` in `tmp/woa23.outerr.log` before and after, and diff. This measures *whether
traffic occurred*, not how many requests the API has served.

**Caveats that must travel with it:**

1. **The two markers disagree by 17.** They are not 1:1, so a single marker must be fixed as
   *the* counter and the other must not be averaged in. The 17 is unexplained.
2. **No rotation config was found** (nothing readable in `/etc/logrotate.d` names woa23), and
   the files run back to 2024-06-18. Counts are **cumulative since then**, not per-period.
3. `restart_time=0`, so no restart has truncated them.
4. `woa23.log` last changed **2026-08-07** while `woa23.outerr.log` changed **2026-08-14** —
   consistent with no API query for about three weeks, with only startup lines since. **Not
   verified**, and worth understanding before it is used as a baseline.

### 6.1 Classification — and what it cannot support

**A11 is a QUALIFIED PROXY, not an API request count.** The four caveats are not footnotes
on an otherwise exact number; they are the reason the number is not exact:

- the two markers **disagree by 17**, so the log does not define a single unambiguous
  per-request event;
- the counts are **cumulative since 2024** with **no rotation policy found**, so any
  "count" is a running total whose origin is unverified;
- log **freshness is inconsistent** between the two files, unexplained.

**What it CAN support:** a before/after **delta** on one fixed marker, read without calling
the API, sufficient to show *whether traffic occurred during a window* — with the 17-marker
ambiguity attached to every figure derived from it.

**What it CANNOT support:** any claim that the **exact** production API request count is
unchanged. Nothing here establishes that, and **no inference may stand in for it.**

> **If production B1 must prove the exact request count unchanged, A11 is a BLOCKER.**
> Meeting it needs a counter this inventory did not find — an instrumented endpoint
> excluded from its own count, or a proxy/access log with a defined rotation policy — and
> that source must be identified and verified before Stage C, not assumed.

---

## 7. A1–A11 summary

| | item | result |
|---|---|---|
| A1 | exact app name | **`woa23`** — verbatim, no suffix |
| A2 | production PM2 version | **5.4.2** |
| A3 | `PM2_HOME` | **`/home/odbadmin/.pm2`**, confirmed from the daemon's own environ |
| A4 | target PID / starttime | **(4295, 14198)** — the bash wrapper |
| A5 | descendants | **3**, depth 2, all identities clean |
| A6 | `pre_stop` live vs file | **DISCREPANCY, EXPLAINED**: file has it, daemon does not, PM2 5.4.2 has no such key |
| A7 | boot id | `0b513a75-…-1c7cbc51a085` |
| A8 | listeners | 45, captured |
| A9 | non-target apps | 8 others, captured |
| A10 | conf identity | **content digests**. store identity **metadata-only** (33G / 123,005 files) |
| A11 | API request count | **QUALIFIED PROXY ONLY** — not an exact count. **BLOCKER** if exactness is required |

**No item is unobtainable.** Two are **qualified, and the qualification is part of the
result**: A10's store identity is **metadata-level only**, and A11 is a **proxy, not a
count** — and A11 becomes a **BLOCKER** the moment production B1 is required to prove the
exact request count unchanged.

---

## 8. Incidental production observations — NOT B1, and NOT acted on

Visible in the command line captured for A6.5, recorded because they are live confirmations
of previously-specified defects:

```
gunicorn woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker \
  -b 127.0.0.1:8050 --keyfile conf/privkey.pem --certfile conf/fullchain.pem \
  --timeout 120 --reload
```

- **`woa23_app:app`** — the B2 app-path defect, live.
- **`-b 127.0.0.1:8050` hard-coded** — the B3 port defect, live.
- **`--reload` running in production** — the B5 defect, live.
- **no `WOA23_ZARR_STORE`** — the B4 store defect, live.
- TLS **on**, with `conf/privkey.pem` / `conf/fullchain.pem`.

**Nothing was changed, and none of this is authorised work.** It is recorded so it is not
rediscovered later as if new.

---

## 9. Can Stage B proceed? — **yes, as a SEPARATE request, and its premise has changed**

**Stage B is still required, and it is no longer urgent.**

| | before Stage A | after Stage A |
|---|---|---|
| does `pre_stop` run on stop? | assumed **yes** — the basis of Blocker 1 | **No.** PM2 5.4.2 does not know the key; four daemon-side sources agree it is absent |
| does it block Stage C? | **yes**, circularly | **No.** Stage C's blocker is removed |
| is removing it still worth doing? | yes | **Yes** — it is dead config that *reads* as an active SIGKILL safeguard. The next person to read that file will believe it runs |
| does removing it need downtime? | unknown | **No downtime expected** — the daemon never adopted it, so the file can be corrected without touching the running app. **To be established by reads in [Stage B §2](B1-stageB-remove-prestop-change-request.md), not assumed** |

**Stage B must still be its own authorised request**, and must still address downtime,
restart, rollback and responsibility — but on this evidence it is a **documentation-hygiene
change with no expected service impact**, not a dangerous one. **I am not proposing it now**
and nothing about it is executed.

**The production B1 Stage C precondition also changes:** the requirement that `pre_stop` be
absent from the live definition is **already satisfied**. That is Stage C's business, and
Stage C remains **unauthorised**.

> **Naming, to prevent a collision.** "Stage C" here is the **production B1** stage. It has
> nothing to do with the **C1 / C2 correctness-validation track** (`c1r`, `c2k`). **Stage A
> does not close C2, does not require C1 or C2 to be re-run, and says nothing about them.**

---

## 10. State unchanged

`b1v2` **WITHDRAWN** · `b1s1` **qualified staging-only stop-path PASS**, not back-filled ·
generated staging config digest **INCOMPLETE** · `b1s1` daemon `1761143` and `bs3v1` daemon
`1709473`, trees and bootstrap paths **retained, not cleaned** · `pm2G`/18265 untouched and
still bound · `CLEAN-bs3v1` still a separate authorisation · **production B1 unvalidated and
unauthorised**.
