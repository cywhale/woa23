# D-1 — production data-path characterisation: **RESULT (raw observations)**

**D-1 records. It does not adjudicate.** No PASS is claimed, no comparison is performed, and
no conclusion about correctness is drawn. **D-2 adjudicates, separately.**

```
subject 6ce915e72c1be4e1c76e2d1c88e115c2baee1649   archive 5dc056a4…472e   240 files
request 9acc66e (addenda §2.1 §2.2 §2.3)          owner odbadmin (uid 1000)
app     woa23   PM2_HOME /home/odbadmin/.pm2      PM2 5.4.2
```

**Headline: 10/10 requests issued with expected statuses. A11 delta = 0, expected 9 —
classified UNATTRIBUTED.**

---

## 1. The ten cases — as issued

Case parameters were read from `bench/contract_cases.py` **extracted from the subject
archive** (`cbe79942…6c8b`, identical to the archive listing), never retyped. Sequential,
one attempt each, **no retry, no additional request**.

| # | case | status | expect | bytes | s | sha256 (body) |
|---|---|---|---|---|---|---|
| 1 | C1 | **200** | 200 | 35790 | 5.207 | `c749c3aa…64c0` |
| 2 | C16 | **200** | 200 | 37083 | 0.831 | `0b94f7bf…8c87` |
| 3 | C20a | **200** | 200 | 8597 | 0.004 | `18649e2d…bc4d` |
| 4 | C1-csv | **200** | 200 | 10414 | 0.704 | `2fa819a3…7405` |
| 5 | C16-csv | **200** | 200 | 10843 | 0.571 | `bf2c8dcc…fb10` |
| 6 | C2 | **200** | 200 | 9414 | 0.100 | `5a61077f…62d3` |
| 7 | C3 | **200** | 200 | 9033 | 0.101 | `55689232…d607` |
| 8 | C4 | **200** | 200 | 2191 | 0.074 | `2c6ebd77…301d` |
| 9 | C5a | **404** | **404** | 61 | 2.106 | `89f01d54…7bc6` |
| 10 | C5b | **404** | **404** | 61 | 0.234 | `89f01d54…7bc6` |

**All ten statuses match the case definitions.** UTC timestamps span
`2026-08-29T12:01:08Z` → `12:01:18Z`. Full URLs, parameters and bodies are retained in
`d1-evidence/` (`case-NN-<label>.body`, `cases.json`).

**Observation, not adjudication:** C5a and C5b returned **byte-identical** 61-byte bodies
(`89f01d54…7bc6`) — the same 404 payload for two different missing-variable conditions.
Recorded; **not** evaluated here.

---

## 2. A11 — **UNATTRIBUTED**

```
marker "Handling parameters and time_periods"
  before all cases : 11083
  after  all cases : 11083
  DELTA            : 0
  EXPECTED         : 9   (nine marker-emitting cases; C20a does not reach the query handler)
```

**Per §2.2 of the request, `delta != 9` is classified UNATTRIBUTED.**

- **No retry** was issued.
- **The expectation is not revised.** It remains 9.
- **The difference is not explained away**, and no cause is asserted.
- **A11 remains QUALIFIED PROXY ONLY.** Nothing here is a claim about the exact request
  count, unchanged or otherwise.

**The per-case marker readings were 0 for every case**, taken immediately before and after
each request — so the shortfall is not a batching artefact of measuring only at the ends.

### 2.1 Raw observations bearing on it — recorded for D-2, NOT interpreted

| | |
|---|---|
| module production serves | `/home/odbadmin/python/woa23/woa23_app.py`, sha256 `9f7b3e44…7147`, 450 lines |
| the marker string in it | **present, exactly once, at line 185** — `print("Handling parameters and time_periods: ", pars, periods)` |
| `woa23.outerr.log` | 29830 lines, **11083 marker lines**, last write **2026-08-29 18:05:54 +0800**, last line `[1828389] [INFO] Application startup complete.` |
| `woa23.log` | 22231 lines, **11083 marker lines**, last write **2026-08-29 17:09:10 +0800**, last line `Total time for this query taken: 0.136741 seconds` |
| `woa23_err.log` | 7599 lines, **0** marker lines, last write **2026-08-29 18:05:54 +0800** |
| the D-1 query window | `12:01:08Z`–`12:01:18Z` UTC = **20:01 +0800** |

**Stated as fact and nothing more:** the newest log write in all three files **precedes the
D-1 query window**, and the last `outerr` line is a worker startup matching pid `1828389`,
one of the workers created during the Stage C recovery.

**No cause is asserted.** Buffering, log routing, a changed capture path at restart, and
other possibilities are **not investigated here and not claimed**. **D-1 does not adjudicate
this; D-2 does.**

---

## 3. Production state — before / after

| | before | after |
|---|---|---|
| `woa23` | `1828351` `online`, `restart_time` 0 | **identical**, `unstable_restarts` 0 |
| process tree | `1828351:131297234` → `1828352:131297235` → `1828389:131297318`, `1828409:131297327` | **identical, all four `(pid, starttime)`** |
| non-target apps (8) | recorded | **unchanged** |
| listeners | 45, `8050` bound | **45, `8050` bound** |
| boot id | `0b513a75-…-1c7cbc51a085` | **unchanged** |
| `conf/ecosystem.config.js` | `ed5dec6c…2159` | **`ed5dec6c…2159`** |
| store files / bytes | 123,005 / 35,101,630,061 | **identical** |
| store metadata fingerprint | `abe6c212…1806` | **`abe6c212…1806`** |

**No restart occurred during D-1.** All four `(pid, starttime)` pairs are unchanged, so the
responses came from the same processes throughout.

---

## 4. Store — APPLICATION-LEVEL read-only only

Per §2.3 of the request, and repeated because it is easy to overstate:

| | |
|---|---|
| executing uid | **1000 (`odbadmin`)** — **the store's owner**, mode 775 |
| kernel write check | **the store IS writable by this account.** Read-only was **not** enforced by permissions |
| what may be claimed | an **application-level read-only operation**: only the ten fixed GETs were issued, the production app opened the store through its own handler, and the metadata identity is unchanged |
| what may **NOT** be claimed | any filesystem/ACL-enforced read-only guarantee of the kind `c1r` had (uid 994, not the owner), or any **store content-integrity proof** |

A metadata fingerprint detects added, removed, resized and re-timestamped files. **It does
not detect a same-size, same-mtime content change.**

---

## 5. Scope — what this is and is not

| | |
|---|---|
| **is** | a **production deployment characterisation**: how the deployment responded to ten fixed cases, with bodies retained for offline comparison |
| **is NOT** | a data-correctness result. `c1r`/`c2k` already established **limited real-store contract evidence** — 64/64 cases accounted for, `regressions: []`, three-cycle stability — and **D-1 does not re-claim it, extend it, or back-fill onto it** |
| **does not touch** | B2–B5 · TLS correctness (`127.0.0.1` with certificate verification disabled) · deployment cutover · store content identity |
| **out of scope entirely** | `conf/simu.sh`, `b1s1` (`1761143`), `bs3v1` (`1709473`), `pm2G`/18265 — none touched, none cleaned |

**Nothing was modified.** No `pm2` lifecycle command, no config, PM2, environment or store
change, no request outside the ten, no cleanup.

---

## 6. Status

| | |
|---|---|
| **D-1 queries** | **COMPLETE** — 10/10 issued, statuses as defined, bodies retained |
| **A11** | **UNATTRIBUTED** — delta 0, expected 9 |
| **D-2** | **NOT STARTED.** No adjudication, no comparison, no PASS |
| B1 production stop path | **CLOSED** (unchanged by this) |
| B2–B5 | **OPEN** |

**The A11 result is the open item for review.** D-1 recorded it and stopped there, as the
authorisation requires.
