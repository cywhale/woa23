# `b1p1` — execution request: **production B1 stop-path validation**

> # SUPERSEDED — SPLIT INTO THREE STAGES
>
> Review outcome: production B1 stays **BLOCKED**, and the work is split so that a
> production **change** is never bundled with a **validation**:
>
> | | | |
> |---|---|---|
> | **A** | [production read-only inventory](B1-stageA-production-inventory-request.md) — **[RESULT](B1-stageA-production-inventory-result.md), complete** | facts only; nothing stopped, restarted or written |
> | **B** | [remove production's `pre_stop`](B1-stageB-remove-prestop-change-request.md) | a production **config change**, its own authorisation |
> | **C** | [production B1 validation](B1-stageC-production-b1-validation-request.md) | only after A and B, each separately authorised |
>
> This document is kept as the record of how the blockers were found. **Do not authorise or
> execute it** — use the three stage requests instead.

**Status: SUPERSEDED by stages A/B/C. Not authorised, not executed. No VM24 contact
was made to write it.**

**This request is BLOCKED.** Three blockers must be resolved by you before it can be
authorised, and one of them cannot be resolved inside B1 at all — see §2. It is submitted
in this state deliberately: the blockers *are* the finding.

---

## 1. What this would validate, and what it must not inherit

> **B1 — a stop is a stop.** `production_stop.sh` terminates the named app and its
> workers, verifies they are gone, and **fails closed** when it cannot establish that.

**`b1s1` is background only.** It is a **qualified staging-only stop-path PASS** on PM2
5.4.2 under uid 994, and it is cited here **solely** to say which code paths have been
exercised somewhere. It is **not** production evidence, is **not** back-filled onto this
request, and its result must not be rewritten as a production B1 PASS. Every gate below is
re-established from zero on production.

---

## 2. BLOCKERS — all three are yours to decide

### BLOCKER 1 — **WITHDRAWN BY [STAGE A](B1-stageA-production-inventory-result.md) §3. THE CLAIM BELOW WAS WRONG.**

> **Stage A disproved this.** PM2 5.4.2 has **no `pre_stop` hook** — 0 occurrences in its
> source, absent from `schema.json`'s 65 keys — and the key is absent from `jlist`,
> `dump.pm2`, `dump.pm2.bak` and every `PM2_HOME` state file. **PM2 will not run it.**
>
> The `tide_app` link was also **my conflation**: `conf/simu.sh:13` kills `tide_app` with a
> separate command; the `woa23` hook greps `woa23_app`. Observed match scope today is
> exactly `woa23`'s own three processes, and **zero** `tide` processes.
>
> The original text is kept below unaltered, as the record of a claim I made and then
> disproved.

### ~~BLOCKER 1~~ (original text, DISPROVED) — production's `pre_stop` still `kill -9`s by grep, and B1 cannot suppress it

`conf/ecosystem.config.js:16`, in this repository, still carries:

```js
pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
```

**PM2 runs that hook itself when the app is stopped.** `production_stop.sh` issues
`pm2 stop <app>`; the daemon then executes `pre_stop` before stopping. Our script contains
no SIGKILL and cannot prevent one it does not issue.

**So a production B1 run would, during the validation, execute the exact behaviour B1
exists to replace:**

- a `kill -9` selected by **command-line grep**, not by identity;
- against **every** process whose command line matches `woa23_app` — `conf/simu.sh:13`
  documents the same technique killing `tide_app`, another project;
- with no `(pid, starttime)` check, no grace period, and no fail-closed.

**This is circular and cannot be resolved inside B1.** Validating the identity-based stop
requires the grep-based `kill -9` not to run; removing it is a **production config change**,
which is outside this request's scope and needs its own authorisation, its own review and
its own window.

Spec 013's resolution says `pre_stop` is to be **deleted, not rewritten**. That deletion
**has not happened**, and `test_production_stop.sh` still asserts its presence as a live
defect.

> **Nothing in this request proposes touching production's config.** The dependency is
> stated so it can be sequenced, not so it can be worked around.

### BLOCKER 2 — the exact production app name is NOT determinable offline

`conf/ecosystem.config.js` sets `name: 'woa23'` **and** `append_env_to_name: true`, so the
name PM2 actually registered depends on the environment the app was started with. It may be
`woa23`, `woa23-production`, or another suffixed form.

`production_stop.sh` requires an **exact** match (`p.name === app`) and refuses wildcards.
**An approximate name is not usable**, and guessing is precisely what the exact-match rule
exists to prevent.

**Resolution required before authorisation:** the exact registered name, read from
production's own `pm2 jlist` **by production's owner**. That read is itself a production PM2
access and is not performed here.

### BLOCKER 3 — downtime and restart policy are undecided, and I will not presume one

**Stopping the production app takes the WOA23 API down.** It serves on `8050` (ledger row:
*production api, live service*).

`autorestart: true` **does not** bring back a deliberately stopped app — PM2's autorestart
covers crashes, not an operator stop. **So after the stop, the service stays down until
someone starts it.**

Three decisions are yours, and none is assumed:

| | decision | if not granted |
|---|---|---|
| **3a** | may production be stopped at all? | the request cannot proceed |
| **3b** | may it be **automatically restarted** after the stop? | see 3c |
| **3c** | if no auto-restart, **who** restarts it and **when**? | **BLOCKER** — the API stays down for an undefined period, and this request must not be authorised until that is settled |

**No recovery strategy is proposed here.** Writing one would be presuming the answer to a
question about live service availability that is not mine to answer.

---

## 3. Execution account, `PM2_HOME`, app — no substitution

| | value | status |
|---|---|---|
| **owner** | **`odbadmin` (uid 1000)** — the owner of production's PM2 state | **to be confirmed by you** |
| **`PM2_HOME`** | **`/home/odbadmin/.pm2`** (the path `production_stop.sh`'s own header names) | **to be confirmed** — never defaulted, passed explicitly |
| **app name** | **UNKNOWN** — BLOCKER 2 | must be exact |
| **uid 994 / `woa23c1ro`** | **NOT usable.** It does not own production PM2 state; `pm2 jlist` as uid 994 returns a *staging* daemon | fixed |
| **staging PM2_HOME** | **NOT usable** as a substitute | fixed |

**A disclosure that belongs in this request.** The session that prepared it can reach VM24
as `odbadmin` — production's owner — over an existing key. **That access has not been used
for any production PM2 read, and will not be without your explicit authorisation.** It is
stated because "the account is available" must never be mistaken for "the action is
authorised".

---

## 4. Grant

`WOA23_B1_GRANTED=yes`, and nothing substitutes for it.

Enforced in `production_stop.sh` **before the app name, before `PM2_HOME`, and before any
`pm2` invocation**: refused when missing, empty, miscased or wrongly valued, and refused
when **any** other run's grant is set alongside. The grant is `unset` immediately after the
check so it cannot reach a serving process.

Exercised 7/7 against a live daemon in `b1s1` — **as staging background, not as production
evidence.**

---

## 5. The stop path — exactly what it does

Subject blob `d1e5630143fee2bc62fbdfc4df68b8dfeb57eb8dce6d529d11e5197013dd25ed`.

| | property |
|---|---|
| **stop target** | the **exact app NAME**. `pm2 stop <name>` is the only stop issued. **PM2 does not stop by pid or starttime** |
| **identity verification** | `(pid, starttime)` recorded **before** the stop and re-checked **after** — this is verification, not targeting |
| `ppid` | from the labelled `PPid:` line of `/proc/<pid>/status`, which cannot shift on a `comm` containing spaces |
| `starttime` | field 22 of `stat`, read only **after** the comm is removed at the last `)`; **fail-closed** — empty, `0`, non-numeric, truncated or fewer than 20 fields all return rc 1 and print nothing |
| **unparsable `stat`/`status`** | a process that **exists** but cannot be identified is `UNKNOWN`/`INDETERMINATE` — **exit 8**, never "gone", never `NOTFOUND` |
| descendants | breadth-first, depth-bounded; exceeding the bound refuses rather than reporting a partial tree |
| unresolved scan | any pid whose parentage cannot be determined **fails the run** |
| refused | `all`, wildcards, `kill`, SIGKILL, global `save`, `resurrect` |
| survivors | `CLEANUP_FAIL`, exit 7, **state preserved** |

**Exit 8 has never fired on a host.** Its coverage is offline only.

---

## 6. Before/after evidence plan

### 6.1 Obtainable **only by production's owner** — all mandatory here

Each captured before and after, to its own file, and diffed. Under `b1s1` these were
**blockers**; under production ownership they become obtainable and therefore **required**.

| # | evidence | source |
|---|---|---|
| 1 | **boot id** | `/proc/sys/kernel/random/boot_id` — captured **first**; it gates every starttime comparison, and a change makes them **invalid**, not merely different |
| 2 | **production PM2 state** | `pm2 jlist` under production's `PM2_HOME` — full app list, names, pids, statuses |
| 3 | **production PM2 version** | `pm2 --version` — currently **UNVERIFIED**; the resolver's order-independence is proven against 5.4.2 only |
| 4 | **target PID + starttime** | `/proc/<pid>/status` + `stat` |
| 5 | **descendants** | every pid, `comm` as it appears, `PPid:`, `starttime`, tree depth |
| 6 | **listener inventory** | `ss -ltn` — **never `-ltnp`** |
| 7 | **API request count** | production's own counter/endpoint — **method to be specified by you**; if none is readable, this becomes a **blocker**, not an estimate |
| 8 | **production `conf/` identity** | file list + SHA-256 |
| 9 | **production store identity** | file count + file-list SHA-256 |
| 10 | **non-target PM2 apps** | name, pid, status for `dask-scheduler`, `dask-worker`, `ghrsst`, `mhwapi`, `tide`, and anything else registered |

**Any item that cannot be obtained is recorded as a BLOCKER, never inferred**, and never
written as "no change observed" — which would imply an observation that did not happen.

### 6.2 Protected set — untouched, and named individually

| | protected | rule |
|---|---|---|
| 1 | **`pm2G` / port 18265** | not started, stopped, read or bound. Still bound as of `b1s1` |
| 2 | **`pm2A`, `pm2B`** and every other historical `PM2_HOME` | not opened |
| 3 | **`bs3v1` retained daemon `1709473`, tree, bootstrap, workdir** | not stopped, killed, deleted or reused |
| 4 | **`b1s1` retained daemon `1761143`, tree, workdir, bootstrap paths** | same |
| 5 | **all non-target PM2 apps** | observed only. `all` and wildcards refused |
| 6 | ports `18281`, `18283`, `19157` | not bound; ledger states unchanged |

**`bs3v1` and `b1s1` cleanup remain separate authorisations**
([`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md)) and are **not** bundled here.

---

## 7. Subject and provenance

```
subject   787a72d0062ef581a31b752971cfdb5ecb8edc91
archive   5d909f012b4efa3ddc3fefd4786c3751c9832b1d15d8e801a40d73817312134d
files     229    file-list 84f4096447eefb0d067be03fcbdeb48cef830b99a1ae5015faa33708a06ebba9
batches   3 serial, 53 suites, 4369 assertions, 0 non-zero, each recording its own HEAD
```

**This request postdates the subject** and is a protocol reference to it, not part of it.

### 7.1 Why the existing subject can be reused — and the one caveat

**No executable or harness file changed** since `787a72d`. `production_stop.sh` is the same
blob (`dcea7d3a…`, content `d1e56301…25ed`). Everything else changed is documentation.

**The one exception, stated rather than waved through:** `scripts/ports_used.tsv` gained the
`19157` row after the subject. It is **data, not executable code**, and it is read by
`start_staging.sh`, `staging_execute.sh` and `lib_ports.sh` — **none of which a production
B1 run invokes**. This run uses `production_stop.sh` only.

**So `787a72d` is sound for this request.** If you would rather the subject carry a current
ledger, say so and a new subject with fresh batches will be cut — the choice is yours, not
an assumption I should make.

---

## 8. Status

| | |
|---|---|
| **BLOCKED** | on B1 (`pre_stop`), B2 (exact app name), B3 (downtime/restart policy) |
| production B1 | **unvalidated and unauthorised** |
| `b1v2` | remains **WITHDRAWN** |
| `b1s1` | remains **qualified staging-only stop-path PASS**, not rewritten |
| retained state | `bs3v1` and `b1s1` daemons/trees **untouched**; cleanup separate |

**Nothing is executed until you authorise it explicitly, and BLOCKER 1 in particular is a
decision about production's configuration that this request does not make.**
