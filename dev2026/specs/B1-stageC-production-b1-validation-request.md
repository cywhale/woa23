# Stage C — **production B1 validation** request

> **A naming note, because the letters collide.** `P1`–`P7` below are **preconditions of
> this production B1 stage**. They are **unrelated to the C1 / C2 correctness-validation
> track** (`c1r`, `c2k`), which is a different programme entirely. **Nothing in the B1
> stages requires C1 or C2 to be re-run, and nothing here closes them.**

**Status: DRAFT SKELETON, OFFLINE. Not authorised, not executed, and NOT YET WRITABLE in
final form.** Third of three stages. **Depends on
[Stage A](B1-stageA-production-inventory-request.md) and
[Stage B](B1-stageB-remove-prestop-change-request.md), each completed and separately
authorised.**

> **This stage cannot be finalised yet, and that is the honest state.** Its three most
> important parameters — the exact app name, the production PM2 version, and whether
> `pre_stop` is actually gone from the **daemon's live definition** — are outputs of A and
> B. Writing them now would be inventing them.

---

## 1. Preconditions — all must hold, each shown, none assumed

| # | precondition | source |
|---|---|---|
| **P1** | Stage A complete; **every** A-item **OBTAINED**, none left a blocker | Stage A result |
| **P2** | Stage B complete and applied; `pre_stop` **absent from the daemon's live app definition**, read from `pm2 jlist` — not from the file | Stage B step 6 |
| **P3** | the **exact** registered app name | A1 |
| **P4** | the **production PM2 version**, and a statement of whether the resolver's order-independence has been established for it | A2 |
| **P5** | production `PM2_HOME`, confirmed from the daemon's own environment | A3 |
| **P6** | a reliable, **non-perturbing** API-request-count read | A11 |
| **P7** | your decisions on downtime, restart and recovery | §4 |

**If any precondition fails, Stage C does not run.** In particular, if **P2** fails, this
stage is exactly as blocked as it was before Stage B, and no partial version of it may be
attempted.

---

## 2. Execution parameters — no substitution

| | value |
|---|---|
| account | the **actual owner** of production PM2 state (expected `odbadmin`, uid 1000, confirmed in A). **uid 994 / `woa23c1ro` cannot substitute** |
| `PM2_HOME` | production's exact path from A3, passed **explicitly**, never defaulted |
| app name | the exact string from A1. **No wildcard, no prefix, no guess** |
| pm2 binary | absolute path |
| grant | **`WOA23_B1_GRANTED=yes`**, checked **before the app name, before `PM2_HOME`, and before any `pm2` invocation**; refused when missing, empty, miscased, wrongly valued, or accompanied by any other run's grant; `unset` immediately so it cannot reach a serving process |

---

## 3. The stop path — unchanged guarantees

Subject blob `d1e5630143fee2bc62fbdfc4df68b8dfeb57eb8dce6d529d11e5197013dd25ed`.

| | property |
|---|---|
| **stop target** | the **exact app NAME**. `pm2 stop <name>` is the only stop issued. **PM2 does not stop by pid or starttime** |
| **identity verification** | `(pid, starttime)` recorded **before** and re-checked **after** — verification, not targeting |
| `ppid` | from the labelled `PPid:` line of `/proc/<pid>/status` |
| `starttime` | field 22 of `stat`, read after the comm is cut at the last `)`; **fail-closed** on empty, `0`, non-numeric, truncated, or fewer than 20 fields |
| exists but unparsable | **`INDETERMINATE`, exit 8** — never "gone", never `NOTFOUND` |
| descendants | breadth-first, depth-bounded; exceeding the bound refuses rather than reporting a partial tree |
| unresolved scan | any pid whose parentage cannot be determined **fails the run** |
| **refused** | `all`, wildcards, `kill`, SIGKILL, global `save`, `resurrect` |
| survivors | `CLEANUP_FAIL`, exit 7, **state preserved** — no manual stop, no retry, no self-cleanup |

---

## 4. Downtime and recovery — recorded before the run, not after

**Stopping the production app takes the WOA23 API on `8050` down.** `autorestart: true`
does **not** restart a deliberately stopped app.

**The recovery method must be written into this request before authorisation**, including:
the exact command that brings the service back; who runs it; the maximum acceptable
downtime; who confirms it is serving; and the abort condition.

**No recovery strategy is proposed here.** It is a decision about live service availability,
and it belongs to you.

---

## 5. Evidence — before and after, each item separately

| # | item | note |
|---|---|---|
| 1 | **boot id** | captured **first**; a change makes every starttime comparison **invalid**, not merely different |
| 2 | production PM2 state | full `pm2 jlist` under production's `PM2_HOME` |
| 3 | **production PM2 version** | |
| 4 | target PID + starttime | |
| 5 | descendants | `comm` as it appears, `PPid:`, `starttime`, depth |
| 6 | listener inventory | `ss -ltn`, never `-ltnp` |
| 7 | **API request count** | by the P6 method only. **Never by calling the API**, which would increment what it measures |
| 8 | production `conf/` identity | file list + SHA-256, including proof `pre_stop` is gone |
| 9 | production store identity | file count + file-list SHA-256 |
| 10 | non-target PM2 apps | name, pid, status — each shown unchanged |
| 11 | stop outcome | verdict, exit code, survivors, indeterminates |

**Anything unobtainable is a BLOCKER, never inferred**, and never written as "no change
observed".

---

## 6. Protected set

`pm2G`/18265 · `pm2A`, `pm2B` and every other historical `PM2_HOME` · **`bs3v1` retained
daemon `1709473`**, tree, bootstrap, workdir · **`b1s1` retained daemon `1761143`**, tree,
workdir, bootstrap paths · all non-target PM2 apps · ports 18281, 18283, 19157.

**Cleanup of `bs3v1` and `b1s1` is a separate authorisation**
([`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md)) and is **not** bundled into this stage.

---

## 7. What must NOT be inherited

- **`b1s1`'s staging PASS is NOT production evidence.** It is a **qualified staging-only
  stop-path PASS** on PM2 5.4.2 under uid 994, cited only as background for which code
  paths have been exercised somewhere. **It is not back-filled into this stage**, and this
  stage's gates are re-established from zero.
- **Offline-only coverage stays offline-only**: `comm` special characters, truncated
  `stat`, non-numeric starttime, unreadable `/proc`, PID reuse, and `INDETERMINATE`/exit 8.
  A production run that does not produce those conditions does **not** upgrade them.
- **`b1v2` remains WITHDRAWN.**
- The **generated staging config digest stays INCOMPLETE** and is unrelated to this stage.

---

## 8. Subject

To be confirmed when this stage is finalised. As of writing, `787a72d` remains valid —
**no executable or harness file has changed since it**, and `production_stop.sh` is the
same blob. If Stage B or any later work changes executable code, a **new subject, archive,
file-list and three fresh offline batches** are required before this stage may be
authorised.

---

## 9. Status

**BLOCKED on Stage A and Stage B**, and on your decisions in §4. Not authorised, not
scheduled, not executed. This document is a skeleton by design and will be completed only
when A and B have supplied the values it deliberately leaves empty.
