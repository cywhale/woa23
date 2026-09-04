# `b1v3` — execution request: B1 **staging stop-path validation**

**Status: DRAFT. Not authorised, not executed. No VM24 contact.**

Supersedes the withdrawn [`b1v2`](B1-host-validation-request-b1v2.md), which pinned a
subject that no longer contains the code under test.

---

## 1. What this request is, and what it explicitly is NOT

The title is deliberate. This is a **staging stop-path validation**, not "B1".

> **This run cannot close B1.** It exercises `production_stop.sh` against a **staging**
> PM2 daemon owned by an unprivileged account. It says nothing about the production PM2
> daemon, and **`uid 994` / `woa23c1ro` is not a substitute for production's owner.**

That assumption — that a staging run under a borrowed uid could stand in for production —
is not made anywhere in this document. §6 states what a production B1 would require and
why it is a separate, unrequested authorisation.

| | |
|---|---|
| **objective** | staging evidence that the fixed stop path works against a real PM2 daemon and a real `/proc` |
| **closes B1?** | **NO** |
| **touches production?** | **NO** — see §5 |

---

## 2. Why it is still worth running

Every fix below is covered offline and **none has met a real daemon or a real `/proc`**:

| defect | fixed in | what only a host can show |
|---|---|---|
| jlist field order (`pid` before `name`) | `cba1329` | the daemon's own emitted order |
| `ppid` from `stat` field 4 | `cba1329` | real `comm` values in a real gunicorn tree |
| `starttime` from `stat` field 22 | `cba1329` | as above — and this field is the PID-reuse identity |
| unreadable pid silently skipped | `cba1329` | a `/proc` containing other accounts' processes |
| direct children only | `cba1329` | the real worker tree, at its real depth |
| **`starttime_of` rc 0 on a truncated stat** | **`787a72d`** | that a live process is never read as gone |
| **no grant on the stop path** | **`787a72d`** | that the grant gates a real `pm2 stop` |

---

## 3. Scope, identity and the protected set

### 3.1 Which PM2 state is under test — named, not "staging" in the abstract

| | |
|---|---|
| **PM2 daemon** | a **new** daemon started by this run, under a **new** `PM2_HOME` created by this run |
| **owner** | the unprivileged staging account (uid 994), which **owns that daemon and nothing else** |
| **app** | exactly one, this run's own, named in the authorisation |
| **production PM2** | **not read, not listed, not contacted.** `PM2_HOME` is explicit and never defaulted |

### 3.2 The protected set — untouched, and named individually

Not "everything else". Each of these is named so its absence from the run is checkable:

| | protected item | rule |
|---|---|---|
| 1 | **`pm2G`** and **port 18265** | not started, not stopped, not read, not bound |
| 2 | **`pm2A`, `pm2B`** and every other historical `PM2_HOME` | not opened; this run's `PM2_HOME` is new |
| 3 | **`bs3v1` retained daemon (pid `1709473`), tree, bootstrap, workdir** | not stopped, killed, deleted, reused or adopted |
| 4 | **production store** | not read; a synthetic store is built |
| 5 | **production `conf/`** | not read or written; no TLS key or certificate is opened |
| 6 | **every other PM2 app** — `dask-scheduler`, `dask-worker`, `ghrsst`, `mhwapi`, `tide` | never reachable: `all` and wildcards are refused, `PM2_HOME` is this run's |
| 7 | **`18281` / `18283`** | not bound. Ledger states unchanged |

### 3.3 Identity — new, first-use, and reconciled against the ledger

**Proposed identity, named here and NOWHERE in the subject:**

| | value |
|---|---|
| label | **`b1s1`** |
| port | **`19157`** |
| app name | `woa23-b1s1-candidate` |
| staging root | `~/woa23-b1s1` |
| workdir | `~/woa23-b1s1-work` |
| `PM2_HOME` | `~/woa23-b1s1-pm2` |
| store | synthetic, built by the run inside its own root |

**Absence, checked rather than asserted** — against the extracted subject tree, the ledger
inside that tree, and every committed file at `HEAD`:

```
19157   subject-tree hits 0   ledger rows 0   ledger mentions 0   repo@HEAD hits 0
b1s1    subject-tree hits 0                                       repo@HEAD hits 0
b1v3    subject-tree hits 0   (this document is not in the subject)
```

The scan is a plain substring search over all **229** files of the extracted subject, not a
port-shaped-token search, because the failure it guards against is *coincidental
substrings*: `19113` and `19136` were both rejected pre-run for occurring inside a float
and inside the tree, and `19061`/`19081` for occurring inside a `uv.lock` URL. Three of the
candidates considered here were rejected the same way — `19151` (2 hits), `19153` (1),
`19173` (1) — and are recorded so the choice is visibly a selection, not a first guess.

### 3.4 Ledger provenance reconciliation

**Nothing in the ledger is edited.** Discrepancies are **listed**, not reconciled away.

**87 port rows**, classified:

| state | count |
|---|---|
| production ports, never touched by this campaign | 3 |
| **BOUND / SPENT** | 61 |
| **RETIRED-NEVER-BOUND** (explicit wording) | 7 |
| allocated-never-bound (older, pre-vocabulary wording) | 16 |

**Discrepancies found — recorded, not fixed:**

| # | discrepancy | disposition |
|---|---|---|
| **D-1** | **19 ports named only in the ledger's header prose, with no row of their own**: `c1i` 18321/18322/18969, `c1j` 18341/18342/18979, `c1k` 18361/18362/18989, `c1m` 19051/19052/19059, `c1n` 19071/19072/19079, plus 19061, 19081, 19113, 19136 rejected pre-run | **all treated as USED and excluded from reuse.** The prose is the record; the missing rows are a formatting gap, not evidence of availability. **Not added by this request** — editing the ledger to suit a new request is exactly what must not happen |
| **D-2** | the header says *"c1q's are added only after it runs"*; 19101/19102/19103 now **do** have rows | stale prose, harmless. The rows are correct and c1q did run. **Left as written** |
| **D-3** | `18282` and `18291` were reported in an earlier session as burned identities (`b35c1`, `b35b1`). They appear in **no ledger row and nowhere at `HEAD`** | **excluded from reuse anyway**, conservatively. The claim cannot be substantiated from the current tree, and this is recorded as an unresolved provenance gap rather than silently dropped or silently trusted |
| **D-4** | 16 rows use the older wording that conflates "spent" with "never bound" | **left as they are.** The ledger's own header says rewriting historical evidence to a newer vocabulary is an edit to the record, not a correction |

**Explicitly excluded from reuse**, beyond every row above: `b35a1`/18281, `b35c1`,
`b35b1`, `bs3v1`/18283, `b1v1`, `b1v2`, `pm2A`, `pm2B`, `pm2G`/18265, and every consumed
or withdrawn identity. **18281 stays RETIRED-NEVER-BOUND and 18283 stays SPENT.**

**At execution time, re-checked live and not taken from this document:**

1. the **live** ledger at the authorised subject — a port recorded since this reconciliation
   invalidates the choice;
2. **live `ss -ltn`** — `19157` must carry no listener before the run;
3. every identity path absent on the host.

If any of the three disagrees with this section, the run **does not start**.

### 3.5 Execution environment — fixed, and nothing borrowed from the host

| | value | why it is stated rather than defaulted |
|---|---|---|
| account | **uid 994** (staging), which owns this run's daemon **and nothing else** | it does not own production PM2 state, and §6 says what that costs |
| `PM2_HOME` | `~/woa23-b1s1-pm2`, **created by this run**, passed explicitly | `pm2` with no `PM2_HOME` silently uses `~/.pm2`. A defaulted value is the difference between stopping a staging app and stopping production |
| pm2 binary | an **absolute path**, passed as `WOA23_PM2_BIN` | resolving `pm2` through `PATH` means whichever copy comes first decides what gets stopped |
| `PATH` | **not modified.** No host `PATH` edit, no shim directory, no prepended bin | `pm2F` was invalidated because `WOA23_PM2_BIN` leaked into the service environment; a `PATH` edit is the same class of change with a wider blast radius |
| store | **synthetic**, built by the run inside its own root | production's store is never opened |
| app name | `woa23-b1s1-candidate`, exact | no wildcard, no prefix match |
| production `PM2_HOME` | **never referenced.** Not read, not listed, not passed | |

**Nothing on the host is reconfigured.** The run creates only paths inside its own
identity, and removes nothing.

---

## 4. Grants

| grant | authorises | required |
|---|---|---|
| `WOA23_PM2C_GRANTED=yes` | staging: create the tree, start the app, bind the port | yes |
| **`WOA23_B1_GRANTED=yes`** | **the stop, and nothing else** | **yes, separately** |

`production_stop.sh` checks `WOA23_B1_GRANTED` **before the app name, before `PM2_HOME`,
and before any `pm2` invocation**, refuses missing/empty/miscased/wrong values, refuses
when another run's grant is set alongside, and **unsets it immediately** so it cannot
reach a serving process. The staging grant does **not** substitute for it.

### 4.1 The stop-path guarantees this run exercises — none may be relaxed

Each is enforced in `production_stop.sh` at subject `787a72d` and asserted by the offline
suites. They are listed here so the run has a checklist, not a recollection.

| | guarantee |
|---|---|
| exact app name | matched with `p.name === app`. A prefix or suffix of the real name does not match |
| exact identity | `(pid, starttime)`, recorded **before** the stop; only matching processes are reported on |
| `all` / wildcard | **refused by name**, before `PM2_HOME` is even read |
| `kill` / SIGKILL | never sent. A graceful stop that does not finish is a `CLEANUP_FAIL` to inspect, not a signal to escalate |
| global `save` / `resurrect` | never invoked |
| parser failure | `PROBLEM`, never `NOTFOUND`. "Cannot determine" is not "absent" |
| unreadable `/proc` entry | recorded as unresolved; the scan **fails closed**. An unknown parent is not "not a child" |
| truncated / unparsable `stat` | `starttime_of` returns rc 1 and prints nothing. Empty, `0` and non-numeric are all refused |
| PID reuse | a differing starttime means the pid belongs to something else; it is not our survivor and not our kill |
| **UNKNOWN / INDETERMINATE** | **never** reported as `NOTFOUND` or as *gone*. Exit **8**, distinct from `CLEANUP_FAIL`'s **7** |
| cleanup wrapper failure | the run **stops and preserves state**. No manual `pm2 stop`, no port release by hand, no retry, no self-cleanup (spec 014 §2) |


---

## 5. Before/after evidence — and what this account CANNOT obtain

Captured before the run and again after, and diffed.

### 5.1 Obtainable by the execution account — captured BEFORE and AFTER, separately

Each is written to its own before-file and after-file and diffed. Nothing is summarised
into a single "unchanged" line, because a summary is where an unchecked item hides.

| # | evidence | how | before | after |
|---|---|---|---|---|
| 1 | **boot id** | `/proc/sys/kernel/random/boot_id` (world-readable) | yes | yes |
| 2 | **staging `PM2_HOME`** — path, existence, ownership | `stat` on this run's own path | yes | yes |
| 3 | **staging daemon** — its pid and `(pid, starttime)` | `pm2 jlist` under this run's `PM2_HOME` only | yes | yes |
| 4 | **staging app** — name, status, pid | same source | yes | yes |
| 5 | **target PID + starttime** | `/proc/<pid>/status` + `stat` | yes | yes |
| 6 | **descendants** — every pid, its `comm` as it appears, `PPid:`, `starttime`, tree depth | the fixed readers | yes | yes |
| 7 | **listener inventory** | `ss -ltn` — **never `-ltnp`** | yes | yes |
| 8 | **survivor / cleanup outcome** | the stop's own verdict and exit code | n/a | yes |
| 9 | **subject tree identity** | file count + file-list SHA-256 | yes | yes |
| 10 | **synthetic store identity** | file count + file-list SHA-256 | yes | yes |

**Boot id first, and it gates everything else.** Every `starttime` is jiffies since boot,
so a changed boot id makes every before/after identity comparison meaningless. If it
differs, the comparison is reported as **invalid**, not as a difference.

`ss -ltn` and never `-ltnp`: the `p` flag asks for process owners the execution account has
no right to, and a partial answer that looks complete is worse than no answer.

### 5.1a What must be shown UNTOUCHED — as absence of evidence of contact

Each is a **negative** claim this run can support, because it can show it never opened the
thing at all:

| | must be untouched | how it is shown |
|---|---|---|
| **`pm2G` / port 18265** | never started, stopped, read or bound | 18265 absent from this run's parameters; listener inventory before vs after |
| **`pm2A`, `pm2B`** and every other historical `PM2_HOME` | never opened | this run's `PM2_HOME` is new and passed explicitly; no other is referenced |
| **production PM2** | never contacted | `PM2_HOME` never defaults; `pm2` is an absolute path; no production path in any argument |
| **`bs3v1` daemon `1709473`, tree, bootstrap, workdir** | not stopped, killed, deleted, reused | none appears in any parameter; nothing is removed by this run |
| **other PM2 apps** (`dask-scheduler`, `dask-worker`, `ghrsst`, `mhwapi`, `tide`) | unreachable | `all` and wildcards refused; the daemon is this run's own |

**These are non-contact claims, not verification claims.** They say this run did not reach
those things. They do **not** say those things are unchanged — nothing else on the host is
inspected, and §5.2 explains why it cannot be.

### 5.2 **BLOCKERS — not obtainable, and not to be inferred**

Stated as blockers rather than approximated, because an inferred "unchanged" is worth
nothing:

| # | evidence | why this account cannot get it |
|---|---|---|
| **B-1** | **production PM2 state** | requires production's `PM2_HOME`, readable only by its owner. `pm2 jlist` as uid 994 returns **this run's** daemon, not production's — reporting it as "production unchanged" would be a category error |
| **B-2** | **production PID/starttime** | production's `/proc/<pid>/stat` is world-readable, **but identifying which pids are production's** requires its PM2 state (B-1). Without that the set is guesswork |
| **B-3** | **API request count** | held by the production service; no endpoint is readable without contacting it, and contacting it is out of scope |
| **B-4** | **production `conf/` and store identity** | not readable by this account by design; hashing them would require access this run must not have |
| **B-6** | **production PM2 version** | requires production's PM2 state (B-1). L-2 records that it stays unverified |
| **B-5** | **non-target PM2 apps' state** | same as B-1: they live under production's `PM2_HOME` |

**Production store, `conf/` and API request counts are therefore OUT OF SCOPE and must be
written as such** — never as "verified unchanged", and never as "no change observed",
which would imply an observation that did not happen.

**Consequence, stated plainly.** This run can demonstrate that it did not *reach*
production — its `PM2_HOME` is its own, its port is its own, `all`/wildcards are refused,
and no production path is opened. It **cannot** produce positive before/after proof that
production PM2 state, request counts or conf/store identity are unchanged. Anyone needing
that must run §6.

Listener inventory (5.1 #2) is the one cross-cutting check available: it shows every
listening socket on the host before and after, so a change to production's listeners
**would** be visible even though production's PM2 state is not.

---

## 6. What a **production** B1 would require — NOT requested here

Recorded so the gap is explicit and nobody closes it by assumption:

| | requirement |
|---|---|
| account | the **actual owner** of production's PM2 daemon and state. **Not uid 994, not `woa23c1ro`, no substitute** |
| `PM2_HOME` | production's exact path, stated in the authorisation, never defaulted |
| app | the exact production app name. `all`, wildcards, `kill` and SIGKILL all refused |
| targets | only processes matching a `(pid, starttime)` pair recorded **before** the stop |
| evidence | everything in §5.2 becomes obtainable and **mandatory** |
| blast radius | stopping a production app interrupts live service — it needs its own risk decision, its own window, and its own authorisation |

**This is not requested, not scheduled, and not implied by authorising `b1v3`.**

---

## 7. Procedure

1. **Bootstrap** via `staging_bootstrap.sh` with `--archive-sha256`; all refusals stand.
2. **Before-capture** — §5.1 items 1–7.
3. **Start** the app under this run's `PM2_HOME`.
4. **Real jlist** — capture `pm2 jlist` verbatim; record the field order emitted and the
   pid the resolver returns from those exact bytes.
5. **Real `/proc`** — for master and every descendant: `comm` as it actually appears, the
   `PPid:` line, the `starttime`, and the tree depth.
6. **TLS environment** — verified on master and every worker; **reported separately** (§8).
7. **The stop** — `WOA23_B1_GRANTED=yes`, named app, then re-check by `(pid, starttime)`.
8. **Idempotency** — run it again; record the verdict, exit code and raw jlist entry.
9. **After-capture** — §5.1 repeated, and diffed.

---

## 8. Results reported per objective, never merged

| objective | this run can establish | it cannot |
|---|---|---|
| **staging stop path** | the fixed reader and stop work against a real daemon and real `/proc` | **nothing about the production stop path** |
| **TLS environment isolation** | paths absent from master and every worker | nothing about production TLS |
| **B1 (production)** | **nothing** | remains **open**; see §6 |
| **B3 / B5** | **nothing.** Not exercised | `bs3v1`'s staging evidence stands with its five qualifications |

**Exit codes and what each means:** `0` clean; `7` `CLEANUP_FAIL` (a survivor); **`8`
`INDETERMINATE`** (a process exists but could not be identified — **not** a clean stop);
`INVALID_ENVIRONMENT` for a TLS path leak. Any non-zero outcome **preserves state**: no
manual `pm2 stop`, no port release, no retry (spec 014 §2).

`production_stop.sh` **is the thing under test**, so its success cannot double as this
run's cleanup. Anything left is preserved and reported. Daemon cleanup, and `bs3v1`
cleanup, remain separate authorisations.

---

## 9. Provenance — this request postdates the subject it names

```
subject commit   787a72d0062ef581a31b752971cfdb5ecb8edc91
subject line     fix(B1): starttime fails closed, "cannot tell" is not "gone", and a stop needs its own grant
archive sha256   5d909f012b4efa3ddc3fefd4786c3751c9832b1d15d8e801a40d73817312134d
files            229
file-list sha256 84f4096447eefb0d067be03fcbdeb48cef830b99a1ae5015faa33708a06ebba9
```

`verify_clean_archive.sh` — **16 assertions**, plus 9 tree-only inside the export.

**Three serial offline batches at that commit: 53 suites, 4369 assertions, 0 non-zero**,
identical across all three, each recording its own HEAD with `tracked dirty = 0`.

**Per-file digests — the code under test and its suites:**

```
d1e5630143fee2bc62fbdfc4df68b8dfeb57eb8dce6d529d11e5197013dd25ed  deploy/production_stop.sh
b6eb2e5540259222bb5a3ee446749d50ad7030c1728ac2701527c4642bad2630  deploy/staging_execute.sh
38cc18be81884241306d5e880f6cb6b0a88e201f4a4bf8ae81157edcd7062006  scripts/test_production_stop.sh
38f105402f723d11d14af7bd4f517bf1aaf72a8544579593d9c8c46edc6d8b49  scripts/test_stop_proc_parsing.sh
```

**Unchanged from `cba1329`, by blob identity rather than assertion:**

```
7f4da8d76ededc424c748d84e15b05750b3b1cb7fdb1b69fe8e4a47a217f7bea  deploy/production_app.sh
0b2e719aecb0fae13ec0271cf03dde9ff8fdd33dea2342bc1cf70481d9d04f60  deploy/make_staging_override.js
a7e4cb7fcb47e83b8192b225093e5dbe20aad2c673e5fed511b2328ee22410c4  deploy/ecosystem.production.config.js
50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8  api/query.py
```

**The jlist parser is byte-identical across all three subjects** — `e7243e4`, `cba1329`
and `787a72d` all give `jlist_resolve` = `fde549be…79d7`, and its suite passes the same
**61 assertions** at each. TLS Option A behaviour and the production config are likewise
untouched.

**Superseded, and not to be used for any run:** `cba1329`, `e7243e4`, `fe6d4e2`,
`a046d5ad`. Only `787a72d` is current.

### 9.1 Status of the pre-authorisation conditions

| | condition | status |
|---|---|---|
| 1 | ledger provenance reconciliation | **DONE** — §3.4, discrepancies listed and not fixed |
| 2 | new label and first-use port, named after the subject | **DONE** — `b1s1` / `19157`, §3.3, in this commit, which postdates `787a72d` |
| 3 | identity absent from subject, its ledger and the whole subject tree | **VERIFIED** — 0 hits across all 229 files |
| 4 | live re-check at execution time | **PENDING BY DESIGN** — §3.4, not substitutable by this document |
| 5 | your decision on the §5.2 blockers | **AWAITING YOU** |

**Still outstanding and yours alone:** whether a staging-only stop-path validation, with
production PM2 state unverifiable by the execution account, is worth running at all.

---

## 10. Limitations — recorded so they cannot be read past

| # | limitation |
|---|---|
| **L-1** | **The staging daemon is PM2 5.4.2.** Every result is against that version. The jlist field-order defect was a 5.4.2 behaviour, so the fix is being validated against the version that exposed it — which is right, and is also the limit of what it shows |
| **L-2** | **Production's PM2 version is NOT verified.** It has never been read by this campaign, and this run cannot read it. A parser proven order-independent against 5.4.2 is *expected* to hold elsewhere, but expectation is not evidence |
| **L-3** | **A staging PASS is NOT a production B1 PASS.** Different daemon, different version, different owner, different `PM2_HOME`, different app, different process tree. The staging result may not be cited as production evidence in any report |
| **L-4** | **This run carries no production-owner, production-PM2-state or production-cutover evidence.** All three are absent by construction, not by oversight — see §5.2 and §6 |
| **L-5** | **`bs3v1`'s retained daemon (pid `1709473`), tree, bootstrap and workdir are out of scope**, and so is their cleanup. That is [`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md), a separate authorisation, **not** merged into this request |
| **L-6** | **B3 and B5 are not exercised.** `bs3v1`'s staging-only evidence stands exactly as recorded, with its five qualifications, and is neither extended nor re-scored |
| **L-7** | **Nothing from this run is back-filled** onto `bs3v1`, `b1v1`, `b1v2`, or any earlier subject; and no earlier result is back-filled onto this one |

### 10.1 The one-line version, for any report that cites this run

> `b1s1` is a **staging-only stop-path validation on PM2 5.4.2 under uid 994**. It does not
> validate the production stop path, does not close **B1**, and does not establish that
> production is unchanged — only that this run did not reach it.
