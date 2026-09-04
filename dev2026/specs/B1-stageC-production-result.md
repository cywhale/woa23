# Stage C — production B1: **RESULT**

Executed under the Stage C authorisation, subject `6ce915e`, request `af69d73`.

```
owner   odbadmin (uid 1000)      PM2_HOME  /home/odbadmin/.pm2
app     woa23 (exact)            PM2       5.4.2
grant   WOA23_B1_GRANTED=yes     boot id   0b513a75-213b-40bf-8219-1c7cbc51a085 (unchanged)
```

**This was not a deployment.** No new artifact, code, venv, config, environment or store. The
run stopped the existing app, validated the stop path, and recovered the same app.

---

## 1. B1 STOP-PATH VALIDATION — **PASS**

> **Production B1 stop-path: PASS.** The identity-based stop terminated the named app and
> its full depth-2 tree on production, and verified it.

**Recorded before the stop, from the real `/proc`:**

```
L0  4295 : 14198 : bash        (PM2's tracked pid — start_app.sh does not exec)
  L1  4296 : 14214 : gunicorn  (master)
    L2  5040 : 15825 : gunicorn
    L2  5041 : 15829 : gunicorn
```

**Command issued — the only one:**

```
pm2 stop woa23        (named app only; never 'all'; no SIGKILL)
```

**Result: exit 0, `waited 0s of 30s`.**

| # | completion condition | result |
|---|---|---|
| 1 | resolver returned the pid for the exact name `woa23` | **4295** |
| 2 | wrapper + every depth-2 descendant recorded as `(pid, starttime)` **before** the stop | **4 processes** |
| 3 | after the stop every recorded process is `GONE` | **4/4 GONE** — none `ALIVE`, none `INDETERMINATE` |
| 4 | stop exited 0 | **0** |
| 5 | `8050` carries no listener | **0 listeners** |
| 6 | no non-target app changed | **all 8 unchanged** |

**All six hold. Neither exit 7 (`CLEANUP_FAIL`) nor exit 8 (`INDETERMINATE`) occurred.**

**What this validates, precisely.** The stop path resolves the exact app name against a real
PM2 5.4.2 daemon, records `(pid, starttime)` from real `/proc`, walks a **genuine depth-2
tree**, and verifies every process gone. `b1s1`'s tree was one level deep, so **the
descendant walk was exercised on production in a way staging could not exercise it.**

**What it does not validate.** `comm` special characters, truncated `stat`, non-numeric
starttime, unreadable `/proc`, PID reuse and `INDETERMINATE`/exit 8 remain **offline-only**
coverage. Production produced none of those conditions, and **a run that does not produce
them does not upgrade them.** Exit 8 has still never fired on a host.

---

## 2. RECOVERY — **SUCCESS on the first attempt**

**Command — the single pre-approved one, used exactly once:**

```
PM2_HOME=/home/odbadmin/.pm2 /home/odbadmin/.npm-global/bin/pm2 start woa23
```

**The G1–G6 retry gate was never reached**, because attempt 1 satisfied every condition.
No retry was issued.

| # | condition | result |
|---|---|---|
| 1 | `woa23` `online` with a positive pid | **1828351**, `online` |
| 2 | pid resolves in `/proc` with a readable non-zero starttime | **131297234** |
| 3 | depth-2 descendant tree restored | **rebuilt** — see below |
| 4 | `8050` listening again | **yes** |
| 5 | argv matches the pre-stop argv | **matches** |
| 6 | all eight non-target apps unchanged | **unchanged** |
| 7 | boot id unchanged | **unchanged** |

```
L0  1828351 : 131297234 : bash
  L1  1828352 : 131297235 : gunicorn
    L2  1828389 : 131297318 : gunicorn
    L2  1828409 : 131297327 : gunicorn
```

`restart_time` **0**, `unstable_restarts` **0**. New pids and new starttimes throughout, as
expected — this is a **new process tree**, not the old one resurrected.

**This is operational evidence, not B1 evidence.** A successful recovery says nothing about
whether the stop path is correct, and would not have rescued a failed validation.

---

## 3. OPERATOR CHECK — **200**, exactly one request

```
UTC        2026-08-29T10:06:14Z
command    curl -sS --insecure --max-time 10 -o <ev>/opcheck.body \
             -w '%{http_code}\t%{time_total}\t%{size_download}' \
             https://127.0.0.1:8050/api/swagger/woa23/openapi.json
curl rc    0
status     200          time_total  0.011862 s        size  8597 bytes
body       parses as JSON; "openapi": "3.1.0"; info.title "ODB WOA23 API"; info.version "1.0.0"
```

**Both acceptance conditions met:** status **200**, and the body is JSON containing an
`openapi` key.

> **LABEL: OPERATOR CHECK.** Attributed to this Stage C run. **Not organic traffic.**
> **Exactly one HTTP request was issued BY THIS RUN, and no further request will be.**
>
> **Scope of that claim:** this is *the only production HTTP request explicitly recorded by
> this campaign*. It is **not** a claim that no other request was ever issued to production
> — no access-log evidence exists that could support one, and the service serves real
> traffic.

**What it proves:** the app **answers HTTP** — ASGI stack, TLS termination and routing are
alive, not merely a socket in LISTEN.

**What it does not prove.** The endpoint never reaches the query handler, so **the Zarr store
was never opened and data queries are not exercised**. And `--insecure` means the
certificate was **not validated** — production's cert is for a public domain and cannot
match `127.0.0.1`. **This is a liveness check, not a TLS validation**, and must not be
reported as evidence that TLS is correctly configured.

---

## 4. A11 — **QUALIFIED PROXY ONLY**

```
marker "Handling parameters and time_periods"   before 11083   after 11083   delta 0
```

**Reported as:** *no new marker was observed in this window.*

**NOT reported as:** the exact API request count, unchanged or otherwise. **Nothing here
establishes that.**

The delta of 0 is **consistent with** the operator check not touching the query path — the
marker is emitted at `woa23_app.py:185`, inside the query handler after parameter
validation, and `/api/swagger/woa23/openapi.json` never reaches it. The check is therefore
**not** attributable to any marker, and none appeared.

**The proxy's limits are unchanged and remain part of the result:** the two markers disagree
by 17, counts are cumulative since 2024 with no rotation policy found, and log freshness is
inconsistent. **A delta of 0 is not evidence that no request was served.**

*(Note: the baseline moved from 10950 at Stage A to 11083 here — the service carried real
traffic between the two, which is expected and is why the baseline is re-read per run.)*

---

## 5. Before / after evidence

### 5.1 Production PM2

| | before | after |
|---|---|---|
| daemon | `(3459, 13189)`, `PM2 v5.4.2: God`, uid 1000 | **unchanged** |
| `PM2_HOME` | `/home/odbadmin/.pm2`, from the daemon's own environ | **unchanged** |
| apps registered | 9 | **9** |
| `woa23` | `4295` `online` | **`1828351` `online`**, `restart_time` 0 |
| non-target apps (8) | `gateway`, `odbbathy`, `mhwapi`, `ghrsst`, `ghrsst_mcp`, `dask-scheduler`, `dask-worker`, `tide` | **all unchanged** |

### 5.2 Listeners

**45 before, 45 after, and the address set is byte-identical** — `diff` produced no output.
`8050` bound before, 0 during the stop, bound after.

### 5.3 Production `conf/`

| | |
|---|---|
| `ecosystem.config.js` | **`ed5dec6c…2159`** — verified **before the stop** and again **after recovery**. Match both times |
| `simu.sh` | `3bc46819…eef9` — **never touched** |
| `conf/` file-list | `4b34f6e8…ce4c` **before and after, identical** |

### 5.4 Production store — METADATA-LEVEL ONLY

| | before | after |
|---|---|---|
| files | 123,005 | **123,005** |
| bytes | 35,101,630,061 | **35,101,630,061** |
| `path+size+mtime` fingerprint | `abe6c212…1806` | **`abe6c212…1806`** |

**This is a metadata fingerprint, not a content digest.** It detects added, removed, resized
and re-timestamped files. **It does not detect a same-size, same-mtime content change**, and
it may not be reported as "the 35 GB of content is unchanged".

#### 5.4a Cross-stage store comparison — **NOT ADJUDICATED, and a correction**

A cross-stage comparison against `1c89be47…b208f` was proposed, attributed to Stage A. **Two
things are wrong with that premise, and both matter:**

**1. `1c89be47…b208f` is not a Stage A value.** [Stage A](B1-stageA-production-inventory-result.md)
computed **no store fingerprint at all** — only `du -sh --apparent-size` (33G) and a file
count (123,005). The digest `1c89be47…b208f` belongs to the **C1 / C2 correctness track**
(`c1k`, `c1m`, `c1n`, `c1q`, `c1r`, `c2j`, `c2k`) — a different programme, and the same
`C`-label collision already flagged in [spec 021](021-subject-boundary-and-external-source-evidence.md).

**2. The two digests are NOT COMPARABLE.** They are computed by different methods, so a
different value is guaranteed *whether or not the store changed*:

| | C1/C2 (`store_readonly_preflight.sh:66`) | Stage C |
|---|---|---|
| command | `cd "$store" && find . -printf '%p\t%s\t%T@\n'` | `find "$store" -type f -printf '%p\t%s\t%T@\n'` |
| paths | **relative** (`./1_degree/…`) | **absolute** (`/home/odbadmin/…`) |
| entries | **all** — directories included | **files only** (`-type f`) |

**So there is no demonstrated drift.** Comparing them would produce a difference caused
entirely by the method, and reporting that as store drift would be a fabricated finding.

**What IS comparable, and does match:**

| | `c1k` (C1/C2 track) | Stage C |
|---|---|---|
| total files | 123,005 | **123,005** |
| total bytes | 35,101,630,061 | **35,101,630,061** |

**Classification: cross-stage store identity is NOT ADJUDICATED.** No continuity is claimed
between the C1/C2 reading and this one, and **none is denied**. Within this Stage C run,
before and after are identical.

**No inference in either direction.** It is not asserted that content changed. It is not
asserted that content is unchanged. The metadata-level limitation stands, and to it is added
that **no cross-stage fingerprint comparison exists at all** — only the file count and byte
total, which agree.

### 5.5 Protected retained state — untouched

`bs3v1` daemon **1709473 present** · `b1s1` daemon **1761143 present** · `18265` (`pm2G`)
**still bound** · `pm2A`, `pm2B` not opened · nothing cleaned, deleted or reused.

**Commands never used:** `pm2 all`, wildcards, `pm2 kill`, SIGKILL, `pm2 delete`,
`pm2 restart all`, `pm2 resurrect`, `pm2 save`. No signal was sent by hand. No fallback, no
improvised command, no second HTTP request.

---

## 6. What remains UNVALIDATED

| | |
|---|---|
| **data-path serving** | the operator check never reaches the query handler. **No production data query was made or verified** |
| **TLS correctness** | `--insecure`; the certificate was not validated |
| **exact API request count** | **A11 is a proxy.** No reliable exact counter exists |
| **store content** | metadata-level only; a same-size, same-mtime change would not be detected |
| **stop-path edge cases** | `comm` special characters, truncated `stat`, non-numeric starttime, unreadable `/proc`, PID reuse, `INDETERMINATE`/exit 8 — **offline-only**, none produced on production |
| **B2 / B3 / B4 / B5** | **not addressed.** The live argv still shows `woa23_app:app`, hard-coded `-b 127.0.0.1:8050`, `--reload` in production, and no `WOA23_ZARR_STORE` |
| **`conf/simu.sh`** | untouched; still greps `tide_app` directly. **Separate request** |
| **downtime duration** | the API was down between the stop and recovery. Bounded by the run, not measured as an SLA figure |

**No back-fill.** `b1s1` remains a **qualified staging-only stop-path PASS**; Stage A was an
**inventory** and Stage B a **configuration cleanup** — none of the three is production B1
evidence, individually or together. **This result stands on its own evidence, recorded
above.**

---

## 7. Verdicts

| | |
|---|---|
| **1. B1 stop-path validation** | **PASS** — production, PM2 5.4.2, exact app `woa23`, depth-2 tree, all six conditions |
| **2. recovery** | **SUCCESS**, first attempt, no retry, gate never reached |
| **3. operator check** | **200**, JSON with `openapi` key, one request |
| **4. A11** | **QUALIFIED PROXY** — delta 0, *no new marker observed*. **Not** an exact-count claim |
| **5. before/after evidence** | conf, store, PM2, listeners, boot id, non-target apps — **all consistent** |
| **6. limitations** | §6, unchanged and unabsorbed |
