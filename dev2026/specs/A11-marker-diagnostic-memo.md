# A11 — marker diagnostic memo

**Offline memo. No VM24 contact, no new query, no diagnostic run.** Built only from D-1's
existing artefacts, the module source already read, and evidence already captured.

**No cause is selected. Every hypothesis below is unresolved.**

---

## 1. The observation

```
D-1: 10 requests issued, 9 of them reaching the query handler
     marker "Handling parameters and time_periods"
       before : 11083
       after  : 11083
       delta  : 0        EXPECTED 9      ->  UNATTRIBUTED
```

The per-case readings were taken **immediately before and after each individual request**
and were **0 every time** — so this is not an artefact of sampling only at the ends.

---

## 2. What is established, and what it does NOT mean

### 2.1 The queries executed. This is not in doubt.

**The strongest evidence is the responses themselves**, adjudicated in
[D-2](D2-offline-adjudication-result.md):

- C1 returned **102 rows × 13 columns** of real WOA23 data; C16 **204 × 8**; C4 **30 rows**;
- the CSV replays returned matching structures;
- C5a/C5b returned the expected **404** payloads;
- C20a returned a **8597-byte** OpenAPI document.

**A delta of 0 does NOT mean no query ran.** It means **no marker line appeared in the log
file being counted.** Those are different statements, and only the second is supported.

### 2.2 Where the expectation of +9 came from

**Derived from the handler path, not from prior measurement:**
`woa23_app.py:185` emits the marker inside `process_woa23_data`, which both `/api/woa23` and
`/api/woa23/csv` call. C20a's Swagger route never reaches it. Nine of the ten cases traverse
that path → **+9**. The derivation stands; **what it predicts about the log did not occur.**

---

## 3. Evidence assembled — facts only

| | |
|---|---|
| module production serves | `/home/odbadmin/python/woa23/woa23_app.py`, sha256 `9f7b3e44…7147`, 450 lines |
| the marker string in it | **present, exactly once, line 185**, inside `process_woa23_data` |
| emission mechanism | bare `print(...)` to **stdout** — one of 6 `print()` calls in the module |
| log capture | PM2 `log_file: tmp/woa23.outerr.log`, `out_file: tmp/woa23.log`, `error_file: tmp/woa23_err.log`, `merge_logs: true` |
| `woa23.outerr.log` | 29830 lines, **11083** markers, last write **2026-08-29 18:05:54 +0800** |
| `woa23.log` | 22231 lines, **11083** markers, last write **2026-08-29 17:09:10 +0800** |
| `woa23_err.log` | 7599 lines, **0** markers, last write **2026-08-29 18:05:54 +0800** |
| last `outerr` line | `[1828389] [INFO] Application startup complete.` |
| pid `1828389` | a **worker created during the Stage C recovery** (starttime `131297318`) |
| D-1 query window | `2026-08-29T12:01:08Z–12:01:18Z` = **20:01 +0800** |
| serving processes | wrapper `1828351` → master `1828352` → workers `1828389`, `1828409`; **all four unchanged across D-1** |
| `woa23.log`'s last content line | `Total time for this query taken: 0.136741 seconds` — a **query** line, at 17:09 +0800 |

**The single most concrete fact:** **every log file's newest write predates the D-1 query
window**, and the newest `outerr` content is a worker **startup** line from the Stage C
recovery — not query output.

---

## 4. Hypotheses — listed, NONE selected

**None of these is investigated, supported or preferred.** Distinguishing them requires
evidence this memo does not have and did not gather.

| # | hypothesis | what would test it |
|---|---|---|
| **H1** | **stdout buffering.** `print()` to a non-TTY pipe is block-buffered; output may sit unflushed until the buffer fills or the process exits | whether the worker's stdout is a pipe; whether `PYTHONUNBUFFERED`/`-u` is set; whether markers appear later without new traffic |
| **H2** | **log routing changed at the Stage C restart.** The recovered process may write somewhere other than the file being counted | the workers' open file descriptors for fd 1/2 |
| **H3** | **the counted file is no longer the live target** — rotation, replacement, or a stale path retained by the old process | inode of the log vs the fd target |
| **H4** | **PM2 log-capture behaviour after `pm2 start`** differs from the pre-existing process's | comparison against a controlled restart |
| **H5** | **an unnoticed pre-existing condition** — markers may have stopped being captured before D-1, and the `17:09` line is simply the last one that was | when marker writes actually stopped relative to the restart |

**H1 is not preferred over the others**, despite being the most familiar. It is listed first
only because it is the most commonly encountered; that is not evidence.

**Note bearing on several of them:** the Stage C recovery at `18:05` produced the newest
`outerr` line, and `woa23.log`'s newest **query** line is `17:09` — **before** that recovery.
Whether marker capture ended at the recovery or earlier is **not established**, and H5 exists
precisely because that ordering does not by itself decide it.

---

## 5. Consequences for A11 as a measure

| | |
|---|---|
| **A11 remains QUALIFIED PROXY ONLY** | unchanged, and now with an additional demonstrated failure mode: **it can read 0 while nine queries are served** |
| **the marker delta may NEVER be reported as an exact request count** | it was already qualified; D-1 shows it can be **wrong in the direction of undercounting**, not merely imprecise |
| **no reliable request counter exists** | Stage A found none; nothing since has produced one |
| **a delta of 0 may not be read as "no traffic"** | D-1 is the counter-example, in this record |

### 5.1 What this changes about earlier results

**Nothing is retracted, and one earlier statement is now weaker than it reads.**

Stage C reported a marker delta of **0** and described it as *"no new marker was observed in
this window"* — which was carefully worded and remains **literally true**. But D-1 shows a
delta of 0 is consistent with **nine served queries**, so Stage C's delta supports even less
than it appeared to. **It never claimed more**, and it is not revised; this memo records that
the qualification was load-bearing.

---

## 6. What this memo does NOT do

- **Does not select a cause.** H1–H5 are all open.
- **Does not propose or authorise any diagnostic on VM24.**
- **Does not revise D-1.** Its raw observations and `UNATTRIBUTED` classification stand.
- **Does not affect D-2.** The adjudication used the response bodies and **never** the marker
  delta.
- **Does not claim production is or is not logging correctly** — only that the counted file
  did not gain markers in the window.

**If a reliable request counter is ever required** — for a cutover, an SLA, or a stronger
before/after guarantee — this memo is the record that the log marker is **not** it, and that
the gap is unresolved.
