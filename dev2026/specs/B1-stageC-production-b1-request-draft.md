# Stage C — **production B1 validation**: request draft

**Status: DRAFT. NOT AUTHORISED, NOT EXECUTED. No VM24 contact was made to write it.**
Supersedes the earlier skeleton
[`B1-stageC-production-b1-validation-request.md`](B1-stageC-production-b1-validation-request.md),
which was written before Stages A and B supplied its values.

**The four blockers of the earlier draft are now DECIDED (§6).** Their limits travel with
them, and the run is bounded by explicit completion and abort conditions rather than a clock.

> **Naming.** `P1`–`P7` are preconditions of **this production B1 stage**. They are unrelated
> to the **C1 / C2 correctness-validation track** (`c1r`, `c2k`), which is a different
> programme. Nothing here requires C1 or C2 to be re-run, and nothing here closes them.

---

## 1. Subject and protocol position

```
subject   6ce915e72c1be4e1c76e2d1c88e115c2baee1649
archive   5dc056a4b6b996adcb0d4b2efc61959fb5e538a5a22a01da8e333c430cc4472e
files     240   file-list 7821387167325e7ad5a7ed73db0d97cbd2c3828ac529ca704afd8b6ee86eb1c6
batches   3 serial, 53 suites, 4382 assertions, 0 non-zero, each recording its own HEAD
stop path deploy/production_stop.sh  d1e5630143fee2bc62fbdfc4df68b8dfeb57eb8dce6d529d11e5197013dd25ed
```

**This request postdates `6ce915e` and references it.** It is not part of the subject.

**The subject is not modified by this document.** If preparing or reviewing it turns out to
require an executable or harness change, **work stops** and a new subject with fresh
archive, file-list and three batches is cut **before** the request is revised — per
[spec 021](021-subject-boundary-and-external-source-evidence.md), which also records that
`conf/` is an **external input**, not archive content.

---

## 2. Execution parameters — all now known, none guessed

| | value | source |
|---|---|---|
| **owner** | **`odbadmin` (uid 1000)** — production PM2's actual owner | Stage A §0 |
| **`PM2_HOME`** | **`/home/odbadmin/.pm2`**, from the daemon's own `environ` | Stage A §0 |
| **app name** | **`woa23`** — exact, verbatim; `append_env_to_name` produced no suffix | Stage A A1 |
| **PM2 version** | **5.4.2** — same as `b1s1`'s staging daemon | Stage A A2 |
| **pm2 binary** | `/home/odbadmin/.npm-global/bin/pm2`, absolute | probeC |
| **grant** | **`WOA23_B1_GRANTED=yes`** | §4 |
| **uid 994 / staging `PM2_HOME`** | **NOT usable.** No substitution | — |

### 2.1 The target tree — and the structural fact that makes this different from `b1s1`

```
4295  bash      ppid=3459(PM2)  starttime=14198   bash conf/start_app.sh   ← PM2's tracked pid
  4296  gunicorn  ppid=4295  starttime=14214   (master)
    5040  gunicorn  ppid=4296  starttime=15825   (worker)
    5041  gunicorn  ppid=4296  starttime=15829   (worker)
```

**`start_app.sh` does not `exec`**, so PM2 tracks the **bash wrapper**, not the gunicorn
master. A stop signals the wrapper; master and workers are **depth-2 descendants**.

`b1s1`'s tree was one level deep, so **the descendant walk is load-bearing here in a way it
was not there.** Those pids and starttimes are Stage A's reading and **must be re-read at
execution time**, not taken from this document.

---

## 3. Stop-path guarantees

| | property |
|---|---|
| **stop target** | the **exact app NAME**: `pm2 stop woa23`. **PM2 does not stop by pid or starttime** |
| **identity verification** | `(pid, starttime)` recorded **before**, re-checked **after** — verification, not targeting |
| `ppid` | labelled `PPid:` line of `/proc/<pid>/status` |
| `starttime` | `stat` field 22, read after the comm is cut at the last `)`; **fail-closed** on empty, `0`, non-numeric, truncated, or fewer than 20 fields |
| exists but unparsable | **`INDETERMINATE`, exit 8** — never "gone", never `NOTFOUND` |
| descendants | breadth-first, depth-bounded; exceeding the bound **refuses** rather than reporting a partial tree |
| unresolved scan | any pid whose parentage cannot be determined **fails the run** |
| **refused** | `all`, wildcards, `kill`, SIGKILL, global `save`, `resurrect` |
| survivors | `CLEANUP_FAIL`, exit 7, **state preserved** — no manual stop, no retry, no self-cleanup |

**Exit 8 has never fired on a host**, and `comm` special characters, truncated `stat`,
non-numeric starttime, unreadable `/proc` and PID reuse remain **offline-only** coverage.
A production run that does not produce those conditions **does not upgrade them**.

---

## 4. Grant

`WOA23_B1_GRANTED=yes`, checked **before the app name, before `PM2_HOME`, and before any
`pm2` invocation**. Refused when missing, empty, miscased or wrongly valued, and refused
when **any** other run's grant is set alongside. `unset` immediately, so it cannot reach a
serving process.

Exercised **7/7** against a live daemon in `b1s1` — **staging background, not production
evidence**.

---

## 5. Protected set — named individually, untouched

| | protected | rule |
|---|---|---|
| 1 | **`pm2G` / port 18265** | still bound as of Stage A. Not started, stopped, read or bound |
| 2 | **`pm2A`, `pm2B`**, every other historical `PM2_HOME` | not opened |
| 3 | **`bs3v1` daemon `1709473`**, tree, bootstrap, workdir | not stopped, killed, deleted, reused |
| 4 | **`b1s1` daemon `1761143`**, tree, workdir, bootstrap paths | same |
| 5 | **non-target apps** — `gateway`, `odbbathy`, `mhwapi`, `ghrsst`, `ghrsst_mcp`, `dask-scheduler`, `dask-worker`, `tide` | observed only; `all`/wildcards refused |
| 6 | ports 18281, 18283, 19157 | not bound; ledger states unchanged |
| 7 | production store, `conf/simu.sh`, `conf/start_app.sh`, TLS key/cert | not modified |

**Cleanup of `bs3v1` and `b1s1` remains a separate authorisation**
([`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md)) and is **not** bundled here.

**`conf/simu.sh` is NOT part of this stage.** It still carries the same kill-by-grep
technique, including a line that greps `tide_app` — another project — directly, and unlike
the config's former `pre_stop` it is a **shell script that runs if invoked** rather than an
inert key PM2 never read. It is a **separate request** with its own risk assessment, and
folding it in here would widen a validation into a change.

---

## 6. Decisions taken — recorded, with the limits they carry

Four blockers are now decided. Each decision comes with a constraint, and the constraint is
part of the decision.

### 6.1 A11 — QUALIFIED PROXY ONLY *(accepted)*

The fixed marker is `Handling parameters and time_periods` in
`/home/odbadmin/python/woa23/tmp/woa23.outerr.log`, counted before and after.

| | |
|---|---|
| **what the delta may be reported as** | *"no new marker was observed in this window"* |
| **what it may NOT be reported as** | the exact API request count, unchanged or otherwise |
| **method** | **never** by calling the API — that would increment what it measures |

The reasons it is a proxy stand: the two markers **disagree by 17**, counts are
**cumulative since 2024** with no rotation policy found, and log freshness is inconsistent.
**A delta of 0 is not evidence that no request was served** — only that no marker appeared.

### 6.2 Downtime — permitted **in the weekend maintenance window** *(accepted)*

Stopping `woa23` on `8050` is authorised **within that window only**. **No fixed maximum
downtime in minutes is set** — instead the run is bounded by the explicit conditions in
§6.3, which is a stronger constraint than a clock: a timer that expires still leaves you
deciding what to do, whereas these say exactly when to stop.

### 6.3 Completion and abort conditions — bounded, and never open-ended

**Stop-path validation is COMPLETE when all of:**

1. `jlist_resolve` returned `OK <pid>` from the daemon's real listing for the exact name
   `woa23`;
2. the wrapper and **every** depth-2 descendant were recorded as `(pid, starttime)` **before**
   the stop;
3. after the stop, **every** recorded process is `GONE` — none `ALIVE` with our starttime,
   none `INDETERMINATE`;
4. the stop exited **0**;
5. `8050` carries **no listener**;
6. no non-target app changed.

**Any of these instead → the validation has FAILED and recovery begins immediately:**
exit **7** (`CLEANUP_FAIL`, a survivor), exit **8** (`INDETERMINATE`), any non-zero exit, or
any non-target app changing.

**Recovery is COMPLETE when all of:**

1. `pm2 jlist` shows `woa23` `status: "online"` with a **positive** pid;
2. that pid resolves in `/proc` with a **readable, non-zero** starttime;
3. its **depth-2 descendant tree is restored** — a gunicorn master under the wrapper, and
   workers under the master;
4. **`8050` is listening again**;
5. argv matches the pre-stop argv;
6. all eight non-target apps are unchanged;
7. boot id unchanged throughout.

**ABORT — preserve state and stop, no further commands:**

- the first recovery command fails **and** the one pre-authorised retry (§7.4) also fails;
- `woa23` reaches `errored`, or `restart_time` climbs, indicating a crash loop;
- `8050` does not listen after a successful-looking start;
- any non-target app changes at any point;
- boot id changes;
- **anything that cannot be determined.**

> **NO UNBOUNDED RETRY. NO IMPROVISED COMMAND.** `pm2 resurrect`, `pm2 restart all`,
> `pm2 kill`, `pm2 delete`, `pm2 save`, any SIGKILL, and any command not written in this
> request are **forbidden — including, and especially, when recovery is failing.** That is
> the moment the temptation exists and the moment an unreviewed command does the most harm.

### 6.4 Store identity — METADATA-LEVEL *(accepted)*

Recorded: **file count, total bytes, mtime range, and a metadata fingerprint** (sorted
`path + size + mtime` digest) for `/home/odbadmin/python/woa23/data`.

**May NOT be written as:** a content digest, or "the 33 GB of content is unchanged". A
metadata fingerprint detects added, removed, resized and re-timestamped files. **It does not
detect a same-size, same-mtime content change**, and that limit is part of the record.

---

## 7. This is NOT a deployment — and the recovery plan, reviewed offline

### 7.0 Three things this request keeps apart

They are separated here because collapsing any two of them is how a report ends up claiming
something it did not establish.

| | what it is | what it produces | what it is NOT |
|---|---|---|---|
| **A. stop-path validation** | the B1 objective: `production_stop.sh` stops the named app, verifies by `(pid, starttime)`, and fails closed | **B1 evidence** — the only part of this run that does | not a deployment, not a health check |
| **B. recovery of the existing app** | bringing back the **same** app, same code, venv, config, environment, store | **operational evidence** that service was restored | **not** B1 evidence. A successful recovery says nothing about whether the stop path is correct |
| **C. future deployment rollback** | reverting a **new version** to a previous one | **nothing here** | **NOT PART OF THIS RUN.** No new version is installed, so there is no artifact to roll back to — see §7.3 for what such a rollback would actually have to cover |

**This run is A followed by B. C does not occur.**

A failed restart is therefore **`RECOVERY_FAILURE`** — the app that was running is not
running — and never "rollback failed", which would imply a deployment step that was never
taken.

**And the converse matters just as much:** a clean recovery does **not** rescue a failed
stop-path validation. If the stop produced a survivor (exit 7) or an `INDETERMINATE` process
(exit 8), **B1 has failed** even if the app comes back perfectly afterwards. The two results
are reported separately and neither substitutes for the other.

### 7.1 What this run is

| | |
|---|---|
| **is** | stop the **existing** production app, validate the stop path, then bring the **same** app back with the **same** code, venv, config, environment and store |
| **is NOT** | a new-version deployment. **No** artifact, code, venv, config, environment or store change of any kind |
| **failure to restart is called** | **RECOVERY FAILURE** — not "rollback". Nothing new was installed, so there is nothing to roll back *to*; the task is restoring what was already running |

### 7.2 Why the wording matters

"Rollback" implies a previous version to return to. Here **the previous version never left**
— the same definition, binaries and files are on disk throughout. Calling a failed restart a
"rollback" would suggest a deployment step to reverse, and send whoever reads it looking for
one that does not exist. **The failure mode is: the app that was running is not running.**

### 7.3 What a real deployment rollback WOULD have to cover — for future reference only

**Not part of this Stage C.** Recorded so that copying a config back is never mistaken for a
rollback:

| # | scope |
|---|---|
| 1 | app code / artifact |
| 2 | Python interpreter and venv |
| 3 | config file **and** the daemon's live definition (they differ — see Stage A) |
| 4 | environment variables of the running process |
| 5 | store and paths |
| 6 | the restart command itself |
| 7 | verification that the restored version is the one intended |

### 7.4 The exact recovery plan

| | |
|---|---|
| **owner** | `odbadmin` (uid 1000) |
| **`PM2_HOME`** | `/home/odbadmin/.pm2`, explicit, never defaulted |
| **app** | `woa23`, exact |

**Primary recovery command — the ONLY one authorised:**

```
PM2_HOME=/home/odbadmin/.pm2 /home/odbadmin/.npm-global/bin/pm2 start woa23
```

**Why by name, and not from the config file.** `pm2 start woa23` restarts the daemon's
**stored definition** — the same one that has been running since December 2025.
`pm2 start ecosystem.config.js` would **re-read the file** and rewrite the live definition,
which is a configuration change smuggled into a recovery. `--update-env` is likewise
excluded: it refreshes the environment from the recovering shell, which is not the
environment the app was serving with.

**Expected state after recovery:**

```
pm2 jlist  woa23  status "online"   pid > 0   restart_time unchanged or +1
/proc/<pid>  readable, starttime non-zero and DIFFERENT from 14198 (a new process)
tree         wrapper -> gunicorn master -> 2 workers      (depth 2, as before)
ss -ltn      127.0.0.1:8050 LISTEN
argv         matches the pre-stop argv, recorded before the stop
non-target   all eight apps unchanged
boot id      unchanged
```

**Confirming `8050` is back.** `ss -ltn` showing a listener on `127.0.0.1:8050`, plus the app
reaching `online` with a live descendant tree. Whether anything is additionally **asked of
the application** is §7.6 — **your decision**, with both options written out.

**If the first recovery command fails — ONE retry, and only through a CLEAN-SLATE GATE.**

A retry is not a reflex. `pm2 start` on an app that is **partly** up can produce a second
process tree beside the first, and a second gunicorn contending for `8050` is a worse
outcome than the outage it was meant to fix. So the retry is gated on the target being
**demonstrably absent**, not merely on the first command having returned non-zero.

**GATE — all six must hold, each RE-READ after the failure. Not one may be assumed:**

| # | condition | how it is established |
|---|---|---|
| **G1** | the exact app `woa23` has **no running process** | `jlist` shows `woa23` not `online`, and its reported pid is `0`/absent — never a pid that still resolves in `/proc` |
| **G2** | **no recorded descendant survives** | every pid recorded before the stop — wrapper, master, workers — is `GONE` by `proc_state`, with **no** live `/proc` entry |
| **G3** | **no `UNKNOWN` / `INDETERMINATE` process** | `proc_state` returns `GONE` for every recorded pid. A process that exists but cannot be identified **blocks the retry** |
| **G4** | **no survivor with a mismatched `(pid, starttime)`** | no recorded pid is `ALIVE` with our starttime; a pid reused by something else is not ours and is left alone |
| **G5** | **no half-started or partial PM2 state** | `woa23` is not `launching`, `errored`, `waiting restart`, `one-launch-status`, or `online` with a pid that does not resolve. `restart_time` / `unstable_restarts` are re-read and recorded |
| **G6** | **`8050` has NO LISTEN socket at all** | `ss -ltn` shows **zero** LISTEN entries for `8050`. **ANY listener blocks the retry — known owner or unknown, ours or another process's.** The test is *"is the port free?"*, never *"is the owner unrecognised?"* |

**G6 is deliberately stricter than "no unknown owner".** An earlier wording asked whether
something *unaccounted-for* held the port, which would have let a listener through as long
as it could be explained — and the most easily explained listener is a half-started copy of
our own app, which is exactly the case the gate exists to catch. **A listener on `8050` is
a blocker whoever owns it.** If the port is occupied while the app is supposedly down, the
premise of the retry is already false.

**Only if all six hold:** wait 15s, re-read `jlist` and `/proc` once more, then re-issue the
**identical** command exactly once:

```
PM2_HOME=/home/odbadmin/.pm2 /home/odbadmin/.npm-global/bin/pm2 start woa23
```

**Identical, not adapted.** No flags added, no config path substituted, no `--update-env`.

### 7.5 If the gate does NOT hold — stop, and do not tidy up first

If **any** process, survivor, `UNKNOWN`/`INDETERMINATE`, or partial PM2 state is present:

- **DO NOT retry.**
- **DO NOT** `pm2 delete`, `pm2 kill`, `pm2 stop all`, `pm2 restart all`, `pm2 resurrect`,
  `pm2 save`, or send any signal.
- **DO NOT** free `8050` by any means.
- **PRESERVE the scene exactly** and report **`RECOVERY_FAILURE`**, naming which of G1–G6
  failed and with what evidence.

> **The forbidden commands are exactly the ones that would make the retry "work".** Deleting
> the app definition, killing the survivor, or freeing the port would each clear the gate —
> and destroy the evidence explaining why recovery failed, during a production outage, with
> no one able to reconstruct it afterwards. A blocked retry is a finding, not an obstacle.

**A second failure after a clean gate** ends the run the same way: state preserved, full
evidence retained, **`RECOVERY_FAILURE`** reported with `8050` down. **No third attempt, no
improvisation, no guessing.**

> One retry, and only through the gate, is deliberate. A transient failure with the target
> demonstrably absent is worth one identical re-try. A failure with anything still present is
> not transient — it is a different situation, and the right response to a situation you did
> not plan for is to stop, not to start issuing commands nobody reviewed.


### 7.6 Functional check after recovery — **OPTION B, exactly one operator check**

**Decided: Option B.** Option A (no HTTP request) is recorded in §7.6.1 as the rejected
alternative and is not performed.

#### The request — fixed here, not chosen at the keyboard

```
curl -sS --insecure --max-time 10 -o <evidence>/opcheck.body -w '%{http_code} %{time_total}' \
     https://127.0.0.1:8050/api/swagger/woa23/openapi.json
```

| | |
|---|---|
| **URL / path** | `https://127.0.0.1:8050/api/swagger/woa23/openapi.json` |
| **method** | `GET` |
| **parameters** | **none.** No query string at all |
| **scheme** | **HTTPS** — production serves TLS (`--keyfile conf/privkey.pem --certfile conf/fullchain.pem`) |
| **expected status** | **`200`** |
| **acceptable response** | body parses as JSON **and** contains an `openapi` key. Nothing else is asserted about content |
| **timeout** | **10 s** (`--max-time 10`) |
| **abort** | non-`200`, timeout, connection refused, TLS handshake failure, or a body that is not JSON → **stop**, preserve state, report. **No second request under any circumstance** |
| **attempts** | **exactly one.** Not retried even on timeout — a timeout is a result |

#### Why this endpoint, and what it does and does not prove

**It does not touch the data path.** The A11 marker
(`Handling parameters and time_periods`) is emitted at `woa23_app.py:185`, **inside the
query handler after parameter validation**. `/api/swagger/woa23/openapi.json` is served by
`custom_openapi()` and never reaches it.

| | |
|---|---|
| **proves** | the app is **answering HTTP** — the ASGI stack, TLS termination and routing are alive, not merely a socket in LISTEN |
| **does NOT prove** | that **data queries** work. The Zarr store is never opened, so a store fault would not be detected |
| **effect on A11** | **expected: none.** This route emits no marker |

**`--insecure` is deliberate and is a limitation, not an oversight.** Production's
certificate is issued for a public domain; a request to `127.0.0.1` cannot match it, so
verification would fail for a reason unrelated to whether the service is up. **This is a
liveness check, not a TLS validation**, and it must never be reported as evidence that TLS
is correctly configured.

#### Recording — operator check, never organic traffic

| | |
|---|---|
| label | **OPERATOR CHECK**, attributed to this run |
| recorded | UTC timestamp, the **full command verbatim**, HTTP status, `time_total`, response size, and the body kept as evidence |
| **never** | counted, described or reported as organic API traffic |

#### A11 under Option B — still QUALIFIED PROXY ONLY

The marker is counted **before the stop** and **after the operator check**.

| outcome | how it is reported |
|---|---|
| delta **0** | *"no new marker was observed in this window"* — **consistent with** the operator check not touching the query path, and **not** proof the count is unchanged |
| delta **≥ 1** | reported as **"includes the 1 operator check"** only if the marker can be attributed to it; otherwise the increase is reported as **unattributed** and **not explained away** |

**A11 remains a QUALIFIED PROXY ONLY under Option B**, exactly as under Option A. It may
never be reported as the exact API request count, unchanged or otherwise.

#### 7.6.1 Option A — the rejected alternative, recorded

No HTTP request: PM2 state, `(pid, starttime)`, descendant tree and `ss -ltn` only. It
claims **process and socket recovery** and nothing about whether the service answers. It is
**not performed**, and is kept here so the choice is visible rather than implicit.

---

## 8. Evidence plan — before and after, each item its own file

| # | item | note |
|---|---|---|
| 1 | **boot id** | captured **first**; a change makes every starttime comparison **invalid**, not merely different |
| 2 | production PM2 state | full `jlist` under production's `PM2_HOME` |
| 3 | production PM2 version | 5.4.2, re-read from the daemon cmdline / `package.json` — never `pm2 -v`, which can spawn a daemon |
| 4 | target PID + starttime | wrapper **and** master |
| 5 | descendants | `comm` as it appears, `PPid:`, `starttime`, depth |
| 6 | listener inventory | `ss -ltn`, never `-ltnp` |
| 7 | API request count | the §6.1 marker only — **QUALIFIED PROXY**, reportable as *"no new marker observed"* and nothing more. **Never by calling the API** |
| 8 | production `conf/` identity | file list + SHA-256, including that `pre_stop` is absent |
| 9 | production store identity | **metadata-level** per §6.4: file count, total bytes, mtime range, and a sorted `path+size+mtime` fingerprint. **Not** a content digest |
| 10 | non-target PM2 apps | all eight, each shown unchanged |
| 11 | stop outcome | verdict, exit code, survivors, indeterminates |
| 12 | **recovery outcome** | the command issued, whether the one retry was used and **which of G1–G6 permitted it**, the restored `(pid, starttime)`, the rebuilt descendant tree, and `8050` listening |
| 13 | **external `conf/ecosystem.config.js` digest** | `ed5dec6c…2159` re-verified **before** the stop and **after** recovery. External input, not archive content |
| 14 | **functional check** | **Option B**, §7.6: exactly one `GET https://127.0.0.1:8050/api/swagger/woa23/openapi.json`. Recorded with UTC timestamp, full command, status, `time_total`, size and body, labelled **OPERATOR CHECK** |

**Anything unobtainable is a BLOCKER, never inferred**, and never written as "no change
observed" — which implies an observation that did not happen.

**The external `conf/ecosystem.config.js` digest is RE-VERIFIED at execution time.** It is an
**external input**, not archive content ([spec 021](021-subject-boundary-and-external-source-evidence.md)),
so the subject's digests do not cover it. It must read `ed5dec6c…2159` — the post-Stage-B
value — **before** the stop and again **after** recovery. A different digest means the file
changed since Stage B and the run **does not start**; a change across the run is a finding.

**A daemon-safety precondition, from Stage A:** `pm2` **spawns a God Daemon if none is
running**. Before any `pm2` subcommand, the daemon must be confirmed alive and identified
from `/proc` — pid file, `status`, `cmdline`, `environ`, uid, `(pid, starttime)`. If it is
not already running, **STOP**; do not let a "read" create state.

---

## 9. What must NOT be inherited

- **`b1s1` is a qualified staging-only stop-path PASS.** Not production evidence, **not
  back-filled**. This stage's gates are re-established from zero.
- **Stage A is an inventory. It validates nothing.**
- **Stage B was a configuration cleanup with no PM2 lifecycle operation.** It validates
  nothing.
- **None of the three may be reported as a production B1 PASS**, individually or together.
- `b1v1`, `b1v2` remain **WITHDRAWN**; the `b1s1` staging config digest remains
  **INCOMPLETE**.

---

## 10. Status

**Planning decisions are made; EXECUTION IS NOT AUTHORISED.** This request is updated
offline only. Nothing has been run, VM24 has not been contacted, production has not been
stopped, started, restarted or reloaded, and no retained state has been cleaned.

**All planning decisions are settled**, including §7.6 (Option B, one operator check).

Production B1 remains **OPEN and unvalidated**. `conf/simu.sh` is **not** part of this
stage and remains a **separate request**.
