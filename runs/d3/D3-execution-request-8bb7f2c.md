# D-3 — candidate deployment rehearsal on the real production store: **execution request**

**Prepared entirely OFFLINE. No VM24 contact. D-3 NOT executed. Nothing staged: no PM2, no
store, no workdir, no port. Production untouched. C1/C2 not re-run.**

**This file lives in `runs/d3/`, OUTSIDE `git archive <sha> dev2026`, and is committed AFTER
the subject was cut.** That is what lets it name an identity without burning one.

> ## SUPERSEDED — see [`D3-execution-request-19aabf0.md`](D3-execution-request-19aabf0.md)
>
> This request is bound to subject **`8bb7f2c8`**, which was superseded by an executable
> change. **Its figures remain correct FOR ITS OWN SUBJECT** and are retained as a record.
>
> **The 4634 assertions below are `8bb7f2c8`'s.** The current subject `19aabf02` reports
> **4655**; the difference is **exactly 21**, being `test_sentinel.sh` growing from 56 to 77
> assertions. **Neither number is a miscount** — see §2.3 of the superseding request.
>
> **Its proposed identity `dep3f`/`19229` was never consumed** and is not used.

> ## Completing this request is NOT authorisation to execute D-3.

---

## 1. Part I — the watcher audit, and what it cost

**The audit FAILED against my own implementation.** Reported here first because it is the
reason two subjects were superseded.

### 1.1 The defect

`run_batches.sh` persisted **only the pid**, and set the run token to **`run-$$`** — derived
from the pid. **A pid is reused after wraparound**, so "is pid N alive?" can be answered
`yes` about an entirely different process, and a pid-derived token **inherits** that weakness
rather than avoiding it. My own comment said the watcher waits on a PID rather than a name
pattern; that was true and **insufficient**.

### 1.2 The fix — `(pid, starttime)`

| | |
|---|---|
| `proc_starttime` | `/proc/<pid>/stat` split after the **LAST `)`** — the comm field is parenthesised and may contain spaces and parens, the defect already fixed once in `production_stop.sh`. BSD fallback via `ps lstart`. **Fails closed and prints nothing** |
| `runid_write` | records **pid, starttime, label, subject** |
| `runid_token` | `label-pid-starttime` — **not** pid-derived |
| `runid_alive` | distinguishes **gone (6)**, **malformed (7)**, **PID REUSE (8)** — a live pid whose starttime no longer matches |

### 1.3 Then the fixed code refused its own run — correctly

The `fae5a417` batch run completed **3 of 3, zero non-zero, postconditions passed**, and
wrote **NO sentinel**.

**Cause:** BSD `lstart` contains **colons** (`22:19:03`); `_sent_is_ident` allows only
`[A-Za-z0-9._-]`; so `sentinel_write` **rejected the driver's own token** and wrote nothing.

**The refusal was correct behaviour** — the library will not write a token it cannot
validate. **The charset was my bug**, and it cost a clean run. `proc_starttime` now folds
everything outside `[A-Za-z0-9.]` to `-`.

### 1.4 The gap that let it through — the more important finding

**Every sentinel test used a synthetic token, `tok-0001`. None used the token the driver
actually builds.** The suite that exists to catch exactly this could not have.

**Five assertions now exercise a REAL driver-built token** end to end: non-empty, passes the
identifier check, accepted by `sentinel_write`, verifies against itself, and `proc_starttime`
emits nothing outside the identifier set.

Writing that last assertion I used **`case` inside `$( )`** — the construct this campaign has
already banned — and **its own suite caught me**. Rewritten.

### 1.5 A second, unrelated defect found while verifying

`test_staging_entry.sh` built its stale-subject archive from `HEAD~1` and **assumed that tree
differed**. It does not when the previous commit touched only paths **outside** `dev2026/`: a
`runs/`-only commit leaves `git archive HEAD~1 dev2026` **byte-identical** to HEAD's, so the
"stale" archive **is** the authorised one and is correctly accepted. **The case was measuring
repository history, not the guard.** It passed in earlier batches only because `HEAD~1`
happened to differ. It now selects the newest ancestor whose `dev2026` tree actually differs,
and **fails loudly** if none exists within 40 commits.

### 1.6 Audit answers

| # | required | status |
|---|---|---|
| 1 | PID + starttime, or a unique precise token | **both** — `(pid, starttime)` and a token built from the pair |
| 2 | no fuzzy grep that can match the watcher itself | **no pattern matching at all**; the watcher calls `runid_alive` |
| 3 | tests for PID reuse, wrong PID, wrong starttime, wrong token, old-subject sentinel, duplicate | **all present** — `test_sentinel.sh`, **56 assertions** |
| 4 | sentinel only after 3/3 complete, exit 0, postconditions passed | **enforced in the library**, not the caller |
| 5 | fail closed on failure, partial, missing, uncertain | **enforced**; demonstrated live in §1.3 |

---

## 2. Subject and provenance — re-derived at the final subject

```
subject   8bb7f2c82bc959a344391f1ecc660c9cba8b7bb8
archive   5070d3cdda068649b2fbb7cff230f68f328b7d5adb5a091c9e5a6073253856c4
files     253
file-list d48885c76e1eb8b68d7e213b2bcfa1bac5f99703f8360ec80a7184830fbd613c
```

**Superseded, in order, and NOT back-filled:** `ccfca934` → `9269dacb` → `fae5a417` →
**`8bb7f2c8`**. Each was superseded by an executable change; **their batches are records, not
evidence for this subject**, and the void `5ae8f49` set remains quarantined.

### 2.1 Files this run depends on, by digest at THIS subject

```
7f8e430b749b4a03f127788f7a57cbbe57d1ebb0b0905a02e494a25117cb4d02  deploy/staging_execute.sh
8c023ab6f0820a384ed6ed304b517bf0df57ac50e812e990935a11d391a54bc4  deploy/staging_bootstrap.sh
7f4da8d76ededc424c748d84e15b05750b3b1cb7fdb1b69fe8e4a47a217f7bea  deploy/production_app.sh
54bdc96b5d630e4e8fbc9168aa78188dcb7c10ff5617ba77d57231cb73fd7679  deploy/make_staging_override.js
d1e5630143fee2bc62fbdfc4df68b8dfeb57eb8dce6d529d11e5197013dd25ed  deploy/production_stop.sh
cbe799426cddabd7437839ef13e1319658f2fda0c14470409ab94e34336d6c8b  bench/contract_cases.py
```

`staging_execute.sh` and `make_staging_override.js` differ from earlier subjects because they
carry the real-store mode; the other four are unchanged.

### 2.2 Three serial batches at this subject

| batch | HEAD, as the batch recorded it | tracked dirty | untracked | suites | non-zero | assertions | exit |
|---|---|---|---|---|---|---|---|
| 1 | `8bb7f2c8…` | **0** | 1 | **55** | **0** | **4634** | **0** |
| 2 | `8bb7f2c8…` | **0** | 1 | **55** | **0** | **4634** | **0** |
| 3 | `8bb7f2c8…` | **0** | 1 | **55** | **0** | **4634** | **0** |

**All 55 result lines identical** across 1↔2, 1↔3, 2↔3.

**The sentinel, and its verification by the library rather than by eye:**

```
WOA23_BATCH_COMPLETE subject=8bb7f2c8… label=s8bb7f2c token=s8bb7f2c-16761-Sun-Aug-30-23-36-07-2026 batches=3 nonzero=0

correct subject + label + token   -> 0  accepted
superseded 9269dacb               -> 4  subject mismatch
superseded fae5a417               -> 4  subject mismatch
superseded ccfca934               -> 4  subject mismatch
wrong token                       -> 8
```

**All three superseded subjects are rejected** — the original defect, demonstrated against
real artefacts.

`untracked=1` is `dev2026/.venv`, the **external local helper**. `tracked_dirty=0` stands;
**"pristine" is not claimed**.

---

## 3. Identity — first-use, verified at the final subject

| | value | subject | ledger | repo@`HEAD` |
|---|---|---|---|---|
| **label** | **`dep3f`** | **0** | **0** | **0** |
| **port** | **`19229`** | **0** | **0** | **0** |
| app name | `woa23-dep3f-candidate` | **0** | **0** | **0** |
| staging root | `~/woa23-dep3f` | — | — | — |
| workdir | `~/woa23-dep3f-work` | — | — | — |
| `PM2_HOME` | `~/woa23-dep3f-pm2` | — | — | — |
| tmpdir | `~/tmp-dep3f` | — | — | — |

**Why not `dep3d`/`19211`.** They were **not consumed** — never executed, never bound, never
in a subject — but they now appear **at `HEAD`** in the superseded `runs/d3` proposal. The
requirement is absence from **subject, ledger and HEAD**, so rather than argue the boundary I
selected a pair that is clean on all three. **`dep3d`/`19211` remain unconsumed and
available**; they are simply not used here.

**Rejected candidate, recorded so the choice is visibly a selection:** `rso1`/**`19249`** —
`19249` occurs at `HEAD`. That is the coincidental-substring trap that already cost `19113`,
`19136`, `19061`, `19081`, `19173` and `19217`.

**At execution, re-checked live:** the ledger at the authorised subject, `ss -ltn` showing
`19229` unbound, and every identity path absent. **Any disagreement stops the run.**

---

## 4. Store — real, read-only, fail closed

**Mode: `--store-mode real-readonly --real-store /home/odbadmin/python/woa23/data`.**
Synthetic mode is the default and is **unchanged**; real mode must be **named**, and
`--real-store` in synthetic mode is **refused, not ignored**.

| # | verified before anything proceeds |
|---|---|
| 0 | GNU `find -printf` and `stat -c` available — else the guards would return 0 by failing |
| 1 | the path equals **one authorised literal** |
| 2 | exists, is a directory, and is **not itself a symlink** that could be re-pointed after the check |
| 3 | **realpath exactly equals** the authorised production store |
| 4 | **not owned** by uid 994 — non-ownership, not mode bits, is what makes it unwritable |
| 5 | **not writable**, readable, traversable — `access(2)`, which **honours ACLs** |
| 6 | **0** writable paths beneath |
| 7 | **0** writable ancestors, walked to `/` |
| 8 | **0** symlinks resolving outside the store |
| 9 | **0** unreadable files, **0** untraversable directories |
| 10 | readable anchor `1_degree/annual/TS/.zgroup` |
| 11 | the staging symlink resolves to the authorised store |
| 12 | metadata fingerprint, **metadata-only**, by the Stage C / D-1 method |

**No permission change, ownership change, directory creation or removal, deletion or write
touches the store — not even a write probe, because probing by writing is a write.** The only
filesystem write is the staging `ln -s`, inside the staging root.

**Also re-confirmed at preflight** (Phase 1 review decision 4): GID membership, ACLs on the
interpreter tree and the store, **world-writable counted separately from group-writable**, and
symlink boundaries.

---

## 5. Runtime and process

| | |
|---|---|
| module | **`api.app:app`**, a literal in `production_app.sh` — not from env, not from config |
| `PM2_HOME` | **isolated**, this run's own; production's never touched |
| venv | **isolated**, built by this run; `WOA23_PYTHON` a required exact value |
| interpreter | **CPython 3.11.14**, already provisioned (QUALIFIED PROVISIONING COMPLETE) |
| `UV_OFFLINE` | **1** |
| `UV_PYTHON_DOWNLOADS` | **never** |
| package sync | `uv sync --locked --offline` — **`--frozen` is mutually exclusive with `--locked`** and is not used |
| **`PYTHONDONTWRITEBYTECODE`** | **`1`, exported by the spawning shell**, never in the ecosystem config (the generator diffs against production's and refuses unexpected keys, which would change the subject). **Verified in the master AND every worker** from `/proc/<pid>/environ`; absent from any worker is a **finding** |
| workers | **2**, pinned by a four-link chain |
| TLS | **off**; `WOA23_TLS_KEYFILE`/`CERTFILE` verified **ABSENT** from every staging process. **Production credentials never enter a staging process** |
| verified per process | uid **994** (real, effective, saved, fs), `PM2_HOME`, argv, port, **no `--reload`** |

**No fallback to system 3.12.3 or production 3.11.4, no lock edit, no relaxed constraint, no
network access, no alternate index.**

**Interpreter tree**: compared against the stabilized baseline by **entry count, ownership and
mode** — never a whole-tree digest, which drifts. Only the **six enumerated** generated cache
paths are permitted; a `.pyc` anywhere else is a **finding**.

---

## 6. Cases, evidence and cleanup

**The fixed ten**, in order, read from `bench/contract_cases.py` (`cbe79942…6c8b`) **in the
subject archive**, never retyped. **One attempt each. No retry. No request outside the ten.**

Retained per case: UTC timestamp, full URL and parameters as issued, HTTP status, **body in
full**, size, elapsed time, body SHA-256.

**Process inventory** before and after, observer excluded **by PID/PGID** with a
self-agreeing retake; **any unexpected survivor ⇒ INCOMPLETE**, evidence retained, **nothing
killed**.

**Retained:** daemon, tree, workdir, archive, artefacts. **Cleanup requires separate
authorisation.** The `test_requests.sh` survivors and pre-existing processes are **not
cleaned**.

---

## 7. Result limits — fixed wording

> **D-3 is a CANDIDATE DEPLOYMENT REHEARSAL using the real production store.**

| it is NOT | |
|---|---|
| production equivalence | **no** |
| a deployment PASS | **no** |
| a data-path correctness PASS | **no** |
| TLS validation | **no** — TLS is off |
| A11 validation | **no** — **A11 is not a gate**, and a marker delta is never read as a request count |

| limits that travel with any result | |
|---|---|
| interpreter | **3.11.14** vs production **3.11.4** — the difference **still exists** |
| package set | resolved from this `uv.lock`, **not** production's — it **may affect results** |
| attribution | **no response difference may be attributed to the API code alone**; a *matching* response is not proof of equivalence either |
| store identity | **metadata-only** — a same-size, same-mtime change is invisible |
| C1 / C2 | **not re-run and not back-filled** |

---

## 8. Status

| | |
|---|---|
| VM24 | **not contacted** |
| D-3 | **not executed**; no staging, PM2, store, workdir or port created |
| production | **untouched** |
| subject | **`8bb7f2c8…`** — no D-3 request, no execution identity |
| batches | **3 clean, identical, 55 suites, 4634 assertions, 0 non-zero**, sentinel verified |
| identity | **`dep3f` / `19229`**, first-use, named only here |
| `dep3d` / `19211` | **not consumed**, still available, simply not used |

**Awaiting the PI's review. Completing this request is not authorisation to execute D-3, and
no VM24 contact will occur before an explicit authorisation.**
