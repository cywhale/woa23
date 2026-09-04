# D-3 — candidate deployment rehearsal on the real production store: **request**

**Status: OFFLINE EXECUTION REQUEST — PREPARED, NOT SUBMITTED FOR AUTHORISATION.
NOT AUTHORISED, NOT EXECUTED. No VM24 contact was made to prepare it, and none may occur
until the PI authorises it explicitly.**

> **This package is NOT fully clean, and is not described as such.** The three offline
> batches at `6ce915e` left **15 survivor processes** from `scripts/test_requests.sh`, which
> remains defective. The [survivor audit](D3-survivor-process-audit.md) root-causes it and
> shows the D-3 execution path does not use that code — but **the harness defect is open**,
> and **no VM24 execution authorisation is being requested while it is.**

Third stage of [`022`](022-b2-b5-cutover-roadmap.md). **Nothing beyond D-3 is requested.**

## 0. What D-3 IS, and what it is not

> ### D-3 = **candidate deployment rehearsal using the real production store**
>
> **It is NOT production equivalence. It is NOT a production PASS. It is NOT a cutover.**
> This wording is fixed and may not be upgraded in this document or anything citing it.

**The prohibition that shapes everything below.** D-1 observed **`woa23_app:app` — the
LEGACY module — running as production.** It is **not** candidate deployment evidence and
must never be used as any. `c1r`/`c2k` ran the candidate under the **benchmark harness**,
which is also not a deployment. **The candidate has never run as a deployment**, and D-3's
evidence must come from the process D-3 itself starts.

---

## 1. Decisions, fixed

| # | decision |
|---|---|
| 1 | **real production store**, via a staging-internal symlink with `WOA23_ZARR_STORE=data/`; **uid 994 re-verifies every store path, ancestor and symlink target is unwritable, or preflight STOPS** |
| 2 | **`WOA23_TLS=off`.** No TLS claim of any kind, and production's key/cert **verified ABSENT** from every staging process |
| 3 | the **same ten cases** as D-1, read from the **subject archive**, never retyped or substituted; **one attempt each**; full bodies retained |
| 4 | candidate **`api.app:app`**, **alternate first-use port**, isolated `PM2_HOME`, isolated staging tree; **production `PM2_HOME`, 8050 and lifecycle never touched** |
| 5 | Python/venv, package lock, Node, PM2 and worker count **pinned and recorded**; master and workers verified **uid 994**, **no `--reload`**, argv/`PM2_HOME`/port/source-root all verified. **Scheduled item by item in [Appendix B](#appendix-b--runtime-store-and-process-evidence-the-complete-schedule)**, which separates what is already settled offline from what only exists at execution |
| 6 | **A11 is NOT a gate.** A marker delta is never read as a request count — observation only |
| 7 | D-3 produces **candidate deployment observation only** — **no PASS**, no D-2 re-adjudication, no D-4 |
| 8 | daemon, workdir, tree, archive and artefacts **RETAINED**; **cleanup is a separate authorisation** |

---

## 2. Execution subject and provenance

```
subject   6ce915e72c1be4e1c76e2d1c88e115c2baee1649
archive   5dc056a4b6b996adcb0d4b2efc61959fb5e538a5a22a01da8e333c430cc4472e
files     240
file-list 7821387167325e7ad5a7ed73db0d97cbd2c3828ac529ca704afd8b6ee86eb1c6
verifier  verify_clean_archive.sh -- 16 assertions (+9 tree-only inside the export)
batches   3 serial, RUN IN A DETACHED WORKTREE PINNED TO 6ce915e — 53 suites,
          4528 assertions, 0 non-zero, exit 0; each batch records its own
          head=6ce915e..., tracked_dirty=0, untracked=1 (the venv symlink, A.1d)
```

**The batches attest the subject directly.** They are run on `6ce915e` itself, not on a later
commit whose difference is then argued away. See [A.1](#a1-three-serial-batches-run-on-the-subject).

**Separately true, and no longer relied upon:** no executable or harness file has changed
since `6ce915e` — every commit after it touches only `specs/`. The subject stands unchanged
and needs no re-cut.

**This request POSTDATES the subject** and is a protocol reference to it, not part of it.
`conf/` remains an **external input**, not archive content
([spec 021](021-subject-boundary-and-external-source-evidence.md)).

**If anything in preparing or reviewing D-3 requires an executable or harness change, work
STOPS** and a new subject with fresh archive, file-list and three batches is cut first.

### 2.1 Files this run depends on, by digest

```
b6eb2e5540259222bb5a3ee446749d50ad7030c1728ac2701527c4642bad2630  deploy/staging_execute.sh
8c023ab6f0820a384ed6ed304b517bf0df57ac50e812e990935a11d391a54bc4  deploy/staging_bootstrap.sh
7f4da8d76ededc424c748d84e15b05750b3b1cb7fdb1b69fe8e4a47a217f7bea  deploy/production_app.sh
0b2e719aecb0fae13ec0271cf03dde9ff8fdd33dea2342bc1cf70481d9d04f60  deploy/make_staging_override.js
d1e5630143fee2bc62fbdfc4df68b8dfeb57eb8dce6d529d11e5197013dd25ed  deploy/production_stop.sh
cbe799426cddabd7437839ef13e1319658f2fda0c14470409ab94e34336d6c8b  bench/contract_cases.py
```

---

## 3. Identity — new label, first-use port, reconciled

| | value |
|---|---|
| **label** | **`dep3a`** |
| **port** | **`19161`** — first use |
| app name | `woa23-dep3a-candidate` |
| staging root | `~/woa23-dep3a` |
| workdir | `~/woa23-dep3a-work` |
| `PM2_HOME` | `~/woa23-dep3a-pm2` |
| tmpdir | `~/tmp-dep3a` |
| bootstrap | a **fresh** path, distinct from the bootstrap **script** location ([spec 019](019-bootstrap-invocation-protocol-note.md)) |

**Absence verified — substring search over all 240 subject files, the 88-row ledger
(`scripts/ports_used.tsv`), and every committed file at `HEAD`:**

```
                       subject-tree   ledger   repo@HEAD outside this request
dep3a                       0            0              0
19161                       0            0              0
woa23-dep3a-candidate       0            0              0
```

**The third column is worded exactly.** Each string now occurs in **one** committed file —
**this request** — and in nothing else. That is not a leak: this request **postdates the
subject**, and placing the identity here rather than in the subject is precisely what keeps
it unburned. What must stay 0 is the **subject tree**, and it is.

**A rejected candidate, recorded so the choice is visibly a selection.** `d3r1` was
considered and **rejected**: it occurs inside base64 image data in a notebook at `HEAD`.
That is the same coincidental-substring trap that already cost `19113`, `19136`, `19061` and
`19081`, and "appears nowhere" is taken literally.

### 3.1 Ledger reconciliation

**88 port rows.** Nothing in the ledger is edited.

| state | count |
|---|---|
| production ports, never touched by this campaign | 3 |
| BOUND / SPENT | 62 |
| RETIRED-NEVER-BOUND (explicit) | 7 |
| allocated-never-bound (older wording, left as written) | 16 |

**Discrepancies carried forward unchanged** from [`b1v3` §3.4](B1-staging-stop-path-request-b1v3.md):
D-1 (19 ports named only in header prose, no row — all treated as **used**), D-2 (stale
`c1q` prose), D-3 (`18282`/`18291`, unsubstantiated, **excluded from reuse anyway**), D-4
(16 pre-vocabulary rows, **left as written**).

**Excluded from reuse:** `18265` (`pm2G`, **still bound**), `18281` (RETIRED-NEVER-BOUND),
`18283` (SPENT), `19157` (SPENT, `b1s1`), and every consumed or withdrawn identity —
`b1s1`, `bs3v1`, `b35a1`, `b1v1`, `b1v2`, `pm2A`, `pm2B`, `pm2G`.

**At execution time, re-checked live:** the ledger at the authorised subject, `ss -ltn`
showing `19161` unbound, and every identity path absent. **Any disagreement stops the run.**

---

## 4. Store — real, and unwritability RE-VERIFIED before anything starts

| | |
|---|---|
| store | `/home/odbadmin/python/woa23/data` |
| access | staging-internal symlink; `WOA23_ZARR_STORE=data/` — the literal `woa23_app.py:63` uses, so no absolute path is interpolated |
| account | **uid 994 (`woa23c1ro`)**, which does **not** own the store |

**Preflight — ALL must hold, or the run STOPS before any service starts:**

1. store owner is **not** uid 994; kernel `test -w` on the store root = **no**;
2. **every path** beneath the store: `0` writable;
3. **every ancestor** of the store, up to `/`: not writable by uid 994;
4. the **symlink target resolves to the real store** and to nothing else;
5. `0` escaping symlinks, `0` unreadable files, `0` untraversable directories;
6. file count, total bytes and `path+size+mtime` fingerprint recorded.

**This is stronger than D-1 could achieve.** D-1 ran as the store's **owner**, so its
read-only claim was application-level only. **Here the kernel refuses a write**, and the
ancestor check (3) is added so the guarantee cannot be defeated by a writable parent.

**Still not provided:** any **content-integrity proof**. The fingerprint is metadata-level
and a same-size, same-mtime change is undetectable.

---

## 5. TLS — off, verified absent, and NOT validated

| | |
|---|---|
| setting | `WOA23_TLS=off` |
| generated config | `WOA23_TLS_KEYFILE` / `WOA23_TLS_CERTFILE` **omitted entirely** |
| before `pm2 start` | both **unset in the spawning shell**, so an inherited value cannot reach the daemon |
| after start | both verified **ABSENT** from `/proc/<pid>/environ` on the **master and every worker** |
| argv | no `--keyfile`, no `--certfile` |
| additional | **no `/home/odbadmin/.../conf` path anywhere** in any staging process environment |
| **if either path appears** | **`INVALID_ENVIRONMENT`** — no result, state preserved |

**TLS is NOT validated by D-3 and the result may not claim otherwise.** Production serves
TLS; D-3 does not rehearse it. Provisioning a staging certificate is a separate decision;
**production's key and certificate are never copied into a staging process** — that was
`bs3v1`'s recorded deviation.

---

## 6. Runtime — pinned and recorded

**Every row below is scheduled with its exact command and acceptance criterion in
[Appendix B](#appendix-b--runtime-store-and-process-evidence-the-complete-schedule).**

| | |
|---|---|
| Python | the **isolated venv's** interpreter, recorded by absolute path, version and sha256 — **and compared with production's 3.11.4, which it is NOT expected to match** (B.1) |
| venv | **isolated**, built by this run; `WOA23_PYTHON` a **required exact value**, verified in the process environment |
| package lock | `uv.lock` `0d2980a5…cc69` and `pyproject.toml` `aa846b8b…e378e` re-verified against the subject; the resolved package set manifested |
| Node | **re-confirmed before start** — realpath, version and permissions, against the probe record `/usr/local/bin/node` → `/usr/bin/node`, **v22.14.0**, `root:root` `755` (B.2) |
| PM2 | version recorded from the God Daemon's own cmdline and `package.json` — **never `pm2 -v`**, which can spawn a daemon |
| workers | **pinned to 2** — settled offline by a four-link chain, and asserted in argv (B.4) |

**Verified after start, from `/proc`:**

- master **and every worker** run as **uid 994** (real, effective, saved, fs);
- argv contains **`api.app:app`** — the candidate — and **no `--reload`**;
- `WOA23_PORT` = `19161`, `WOA23_ZARR_STORE` = `data/`, `WOA23_PYTHON` = the staged venv;
- `PM2_HOME` is this run's own, from the daemon's own environ;
- source root is the staged tree, and worker library provenance is the run's venv — **not**
  the shared `py311` environment and **not** the production tree;
- process tree shape recorded (`production_app.sh` **`exec`s**, so **depth-1** is expected —
  recorded, not asserted as a requirement).

---

## 7. Case set — the same ten, from the subject

C1, C16, C20a, C1-csv, C16-csv, C2, C3, C4, C5a, C5b — **in that order**, parameters read
from `bench/contract_cases.py` (`cbe79942…6c8b`) **in the subject archive**, never retyped.
**One attempt each. No retry. No request outside the ten.**

Per case: UTC timestamp, full URL and parameters as issued, HTTP status, response **body
retained in full**, size, elapsed time, and body SHA-256.

**Expectations — to be CONFIRMED, not assumed.** The candidate should sit on the *post*-change
side of the decided differences: spec-015 column order for C1/C16 and their CSV replays, and
OpenAPI `info.version` **1.1.0** for C20a — the opposite side from D-1. **If it does not,
that is a finding, not an error to correct.**

**And a difference outside those decided classes is NOT attributable to the API code.** D-3's
interpreter and dependency set both diverge from the deployment D-1 observed, so any such
difference is recorded as **observed under a divergent runtime**, with the API code, the
interpreter and the resolved packages all left standing as candidate causes (B.1, §9).

**A11 does not apply.** These queries go to a **staging** app on `19161`. Production's marker
log is untouched and **production receives no request**. If any staging-side marker count is
recorded at all, it is an **observation only** and is never read as a request count (§1.6).

---

## 8. Protection and cleanup

**Never touched:** production `PM2_HOME`, the production app, port 8050, production
lifecycle, `conf/`, `conf/simu.sh`, `start_app.sh`, TLS key/cert, the store's **contents** ·
`pm2G`/18265 · `pm2A`, `pm2B` · `bs3v1` daemon `1709473` and tree · `b1s1` daemon `1761143`
and tree.

**Never used:** `pm2 all`, wildcards, `kill`, SIGKILL, `save`, `resurrect`, or any command
not written in this request.

### 8.0 Process inventory, and a FAIL-CLOSED gate on unexpected survivors

**Added because the offline batches at `6ce915e` left 15 survivor processes** — five per
batch, from `scripts/test_requests.sh`. Root cause, evidence and the reason D-3's own path is
unaffected are in the [survivor audit](D3-survivor-process-audit.md).

**Full inventory, twice, by the same method:** immediately **before** the run starts, and
**after** the ten cases and after stop. For every process owned by **uid 994**: **PID, PPID,
PGID, start time, uid and full argv** — not a count, not a command-name summary.

> ### The inventory MUST exclude its own pipeline — found by running the preflight
>
> A first inventory taken during preflight excluded the observing shell and its `sshd` by
> PID, and **still listed its own `ps` and `awk`**. Left uncorrected this breaks the gate
> below outright: the after-set's `ps`/`awk` have different PIDs from the before-set's, so
> **every run would report an UNEXPECTED SURVIVOR and fail closed on itself** — a gate whose
> first finding is the observer.
>
> **Required method:** exclude the whole observing pipeline — the login shell, its `sshd`,
> and every process in the inventory command's own process group — the way
> `run_suites.sh`'s `proc_snapshot` already does. **The exclusion is asserted, by taking a
> third inventory immediately after the "before" one and confirming the two agree**; if a
> bare re-take disagrees with itself, the method is measuring the observer and the run
> stops before it starts.

**The gate — after-set minus before-set, minus what D-3 is expected to have created:**

| expected, enumerated in advance | count |
|---|---|
| PM2 God Daemon for **this run's** `PM2_HOME` | 1 |
| gunicorn master | 1 |
| gunicorn workers | **2** (B.4) |

Anything in the after-set that is **not** in the before-set and **not** on that list is an
**UNEXPECTED SURVIVOR**.

| | |
|---|---|
| **on detection** | **FAIL CLOSED.** The run is reported **INCOMPLETE** and **no result is claimed from it** |
| **the survivor itself** | **left running and recorded** — PID, PPID, PGID, start time, argv. **Never killed**: killing it destroys the evidence needed to explain it |
| **zero unexpected survivors** | recorded as a **positive assertion**, never as silence — an absent finding and an unrun check must not look alike |

**The gate is deliberately stricter than the known defect requires.** The audit shows no
`deploy/` file launches a background process at all; this gate means that finding does not
have to be taken on trust.

**Not confused with the retained deployment:** the God Daemon, master and 2 workers are
**intended and retained by decision** (§8.1). They are inventoried, not flagged.

### 8.1 Cleanup — RETAINED by decision

The app is stopped with `production_stop.sh` under **`WOA23_B1_GRANTED=yes`** and this run's
own `PM2_HOME`.

**Everything else is RETAINED and NOT cleaned:** the staging **daemon**, workdir, tree,
bootstrap paths, archive and all artefacts.

**If the stop reports anything other than a clean verdict** — exit 7 (`CLEANUP_FAIL`),
exit 8 (`INDETERMINATE`), or any discrepancy between its claim and the process table — the
run **STOPS and PRESERVES state**: no manual stop, no port release, no retry (spec 014 §2).

**Daemon cleanup is its own authorisation** and is never implied by stopping an app.

---

## 9. What D-3 will and will not establish

| | |
|---|---|
| **will** | that the **candidate module runs as a deployment** — proposed launcher, isolated PM2, isolated venv, real store read-only under kernel enforcement, `WOA23_PORT`/`WOA23_ZARR_STORE` honoured, no `--reload`, uid 994 throughout — and **how it answers the ten cases**, bodies retained |
| **will NOT** | close **B2–B5** · validate **TLS** · establish **production equivalence** · provide **store content-integrity proof** · re-validate **B1** · claim any **PASS** · **attribute any response difference to the API code** |

**The attribution limit, restated here because it binds the result wording.** D-3's
interpreter and resolved package set both diverge from the deployment D-1 observed (B.1).
**A response difference therefore cannot be attributed to `api.app:app` alone** — the
interpreter and the dependency set are equally available explanations, and D-3 cannot
separate them. Differences are recorded as **observed under a divergent runtime**. A
*matching* response is likewise not proof of code equivalence.

**No back-fill in either direction.** `c1r`/`c2k` keep their limited real-store contract
evidence; D-1 keeps its raw observation; D-2 keeps its limited adjudication; **none becomes
candidate-deployment evidence, and none is replaced by D-3.**

---

## 10. Status — offline execution request, prepared but NOT submitted

> ## D-3 = **candidate deployment rehearsal using the real production store**
>
> **NOT production equivalence. NOT a production PASS. NOT a cutover. NOT an A11 gate.**
> This wording is fixed and may not be upgraded here or in anything citing it.

**The request content is complete. It is NOT submitted for execution authorisation.**

**One blocker is open**, and it is not in this request's scope to close:
`scripts/test_requests.sh` leaks five processes per offline batch run. The
[survivor audit](D3-survivor-process-audit.md) establishes that **no file D-3 executes uses
that code, or launches a background process at all**, and §8.0 adds a fail-closed gate so
that finding need not be trusted. **The harness defect itself is still open**, so the package
**is not called clean** and **no VM24 contact may occur** until the PI decides — either to fix
`test_requests.sh` first (new subject, fresh batches), or to authorise D-3 on the audit's
evidence.

| what is being asked for | |
|---|---|
| subject | **`6ce915e`** — archive `5dc056a4…472e`, 240 files, three batches run at this exact commit |
| identity | label **`dep3a`**, port **`19161`**, app `woa23-dep3a-candidate`, isolated `PM2_HOME` |
| store | the **real production store**, read-only, unwritability re-verified under **uid 994** across every path, ancestor and symlink target (§4, B.6) |
| TLS | **off**, key and certificate verified **ABSENT** from every staging process (§5) |
| cases | the **fixed ten**, read from the subject archive, **one attempt each**, full bodies retained (§7) |
| runtime | Python/venv, Node, PM2, worker count **re-confirmed before start** (Appendix B) |
| after the run | **daemon, tree, workdir, archive and artefacts RETAINED** — cleanup is a **separate** authorisation (§8.1) |
| production | `PM2_HOME`, port **8050** and lifecycle **never touched** |

**What it may produce:** candidate deployment **observation only** — how the candidate
answers the ten cases under a runtime that is **known to diverge** from production's (B.1,
§9). **No PASS of any kind.**

Standing state unchanged: B1 production stop path **CLOSED** · B2–B5 **OPEN** · A11
**QUALIFIED PROXY ONLY**, D-1 **UNATTRIBUTED**, cause **undetermined** · TLS, full store
identity and deployment equivalence **not established** · `conf/simu.sh`, `b1s1`, `bs3v1`,
`pm2G` **untouched**.

---

# APPENDIX A — Offline review package

**Prepared offline. No VM24 contact. Not authorised, not executed.**

## A.1 Three serial batches, run ON the subject

> **These three batches were executed at the EXACT subject commit
> `6ce915e72c1be4e1c76e2d1c88e115c2baee1649`**, in a **detached worktree** created solely for
> this purpose. No result below is carried across commits, inferred from a later tree, or
> argued from a documentation-only diff.

**The earlier batch set that ran at `5ae8f49` remains VOID and is quarantined.** It is kept
in A.1c as a record of what went wrong and why. **It must never be merged with, averaged
into, quoted alongside, or used to corroborate the clean set above** — not for suite counts,
not for assertion totals, not for timings. The two sets ran on different commits under
different conditions, and only the set in this table attests the subject.

The previous package ran its batches at `5ae8f49` and argued the difference away as
documentation-only; **that argument is withdrawn and replaced by evidence.**

| batch | HEAD, as the batch itself recorded it | tracked dirty | untracked | suites | non-zero | assertions | exit |
|---|---|---|---|---|---|---|---|
| 1 | `6ce915e72c1be4e1c76e2d1c88e115c2baee1649` | **0** | 1 | 53 | **0** | **4528** | **0** |
| 2 | `6ce915e72c1be4e1c76e2d1c88e115c2baee1649` | **0** | 1 | 53 | **0** | **4528** | **0** |
| 3 | `6ce915e72c1be4e1c76e2d1c88e115c2baee1649` | **0** | 1 | 53 | **0** | **4528** | **0** |

**Every value in that table is the batch's own record**, not a claim made about it from
outside. Each batch writes its own `git.txt`, and all three read identically:

```
head=6ce915e72c1be4e1c76e2d1c88e115c2baee1649
subject=fix(tests): assert the production config EXISTS before asserting it is clean
tracked_dirty=0
untracked=1
?? dev2026/.venv
```

**HEAD is additionally checked by the driver before AND after each batch**, and a batch whose
HEAD moved mid-run is declared VOID rather than reported. All six checks passed.

### A.1a Suite-by-suite identity — and what exactly is identical

**All 53 result lines — suite name, exit status, outcome and assertion count — are identical
across batches 1 vs 2, 1 vs 3 and 2 vs 3.** Wall-clock timings are excluded from the
comparison; they vary by design and are not a result.

**What is NOT identical, stated rather than glossed:** the batches' process-snapshot notes
differ — **20, 30 and 36** `left processes behind` notes respectively. These are
**observations, not assertions**; no suite failed in any batch, and every exit is 0. Part of
the difference is **whatever else the machine was doing**: macOS background services, and a
concurrent test run belonging to **another project entirely** (`~/proj/ghrsst/dev2026`) that
was active throughout. **That run was not touched.**

### A.1a-1 Survivors — proved by PID and start time, and my earlier claim was WRONG

**An earlier draft of this package said "no process was leaked by these batches." That is
false, and the method behind it was also wrong** — it reasoned from process *age* ("all the
stale ones are 11–18 days old"), which cannot show that a batch created nothing new.

**Enumerating every process by PID and absolute start time against the measured batch windows
shows 15 survivors, and they are ours.**

Batch windows, taken from each batch's own `manifest.tsv`:

```
batch 1   2026-08-30T08:15:04+0800 -> 08:39:24      batch 3   09:03:45 -> 09:28:13
batch 2   2026-08-30T08:39:24+0800 -> 09:03:45
```

Every process owned by this user whose **start time falls inside those windows** and which is
**still alive**:

| batch | PIDs | start times | cwd |
|---|---|---|---|
| 1 | `61767 61809 61864 62543 62625` | 08:19:11, :12, :12, :20, :20 | `…/wt-6ce915e/dev2026` |
| 2 | `97428 97452 97486 97557 97587` | 08:43:34, :34, :35, :43, :43 | `…/wt-6ce915e/dev2026` |
| 3 | `28059 28084 28118 28551 28598` | 09:07:59, :59, :59, 09:08:08, :08 | `…/wt-6ce915e/dev2026` |

**Five per batch, at the same offsets from each batch's start, all reparented to `launchd`
(ppid 1), all with cwd inside the detached worktree.** Two independent groupings agree: the
process table shows exactly **15** such processes started on Aug 30, and exactly **15** whose
cwd is `wt-6ce915e/dev2026`. **The attribution is by identity and timestamp, not by age.**

**Which suite:** mapping the timestamps onto each batch's manifest puts all of them inside
**`test_requests.sh`** (batch 1: 08:19:11→08:19:23), consistently in all three batches. The
batch's own `procs.leaked` for that suite records the same command line. They are
`/Library/…/Python3.framework/Versions/3.9/…/Python -` — the **system** Python 3.9, not the
project venv's 3.11.

### A.1a-2 The wider finding this uncovered — NOT a D-3 finding, but real

Counting every live process of that shape on this machine: **1065**, with start dates running
from **Aug 10** to today, **1025** of them with cwd `/Users/cywhale/proj/woa23/dev2026`.

**`test_requests.sh` leaks about five processes per run, and they have been accumulating for
roughly three weeks.** This is a **defect in the harness**, recorded here because the survivor
audit found it. **It is out of scope for D-3**, it did not affect any result above — all
53 suites passed in all three batches — and **nothing was killed or cleaned up**, so the
evidence stands as found.

**Bearing on D-3:** none directly, but it is the reason the survivor question must be
answered by **PID and start time** rather than by "the suite reported nothing unusual."

### A.1b A correction: the assertion total was previously UNDERCOUNTED

**The earlier packages reported 4382 assertions. The correct figure is 4528 — 146 short.**

**Cause: my counting regex, not the suites.** It matched `(N assertions)` with parentheses,
and `test_column_contract.py` reports its total in a different shape —
`test_column_contract.py: 46 tests, 146 assertions, 0 failed` — so its **146 assertions were
silently dropped from every previous total I reported.** A second suite,
`test_staging_store.py`, ends on a caveat line rather than a count and is read from its own
stdout (**24**).

**4504 counted from the 52 result lines that carry a count, plus 24 from
`test_staging_store.py`, = 4528.** Identical in all three batches.

### A.1c The VOID batch set — quarantined, never mixed with the clean set

> **STATUS: VOID.** Ran at `5ae8f49`, not at the subject. **Retained only as a record of a
> mistake.** Its numbers may not be combined with, compared against, or cited as support for
> the clean set in A.1 — they are not two samples of the same thing.

An earlier batch set was **discarded**, not reported. It showed 2 non-zero:

```
test_clean_archive.sh   FAIL a second run reports the same archive digest
                             expected 84d6d36a…  got 370dec8f…
test_request_log.py     FAIL and they match what the server was asked
```

**Cause: I committed `5ae8f49` at 21:36:42 while batch 1 was running.**
`test_clean_archive.sh` invokes `verify_clean_archive.sh HEAD` three times, so it archived
**two different HEADs** and correctly reported a mismatch.

**The detached worktree removes the failure mode rather than relying on my discipline.** Its
HEAD is pinned and detached, so a commit landing on `perf/2026-s1-remove-dask` cannot move it
— and the driver's before/after check would catch it anyway. Both suites pass in all three
batches above.

### A.1d `untracked=1` — an EXTERNAL LOCAL HELPER, and the tree is not called pristine

**`untracked=1` is `dev2026/.venv`: a symlink to a pre-existing virtual environment on this
development machine. It is an EXTERNAL LOCAL HELPER, not part of the subject.** The venv is
untracked, so a fresh worktree has none, and the Python suites need an interpreter.

**The tree is therefore NOT pristine, and is not described as such.** What is true is the
narrower, checkable statement:

| | |
|---|---|
| **`tracked_dirty=0`** | every **tracked** file matches `6ce915e` exactly — this stands |
| **`untracked=1`** | one external local helper is present: `dev2026/.venv` |
| **"pristine"** | **not claimed.** The two counts are recorded separately by the batch, precisely so they cannot be collapsed into one reassuring word |

**Verified that the helper supplies no application code and no editable install** — checked,
not assumed:

- `api` resolves to `…/wt-6ce915e/dev2026/api/__init__.py` — the **worktree**;
- `bench` resolves to `…/wt-6ce915e/dev2026/bench` — the **worktree**;
- site-packages contains **no** editable install, no `__editable__` finder, no egg-link and
  no `.pth` pointing at any checkout — the only `.pth` present is virtualenv's own
  `_virtualenv.pth`.

So the helper contributes **third-party packages only**; every line of `api/` and `bench/`
exercised by the batches comes from the subject tree.

**This helper is a property of the offline development machine and has NO counterpart on
VM24.** D-3's execution takes its code **only** from the **subject archive and the bootstrap
tree** — never from a developer venv, a worktree, or anything symlinked into one. The
interpreter D-3 serves with is the **isolated venv built by the run itself**, with
`WOA23_PYTHON` a required exact value (B.1). Like `conf/`, this helper is an **external
input** ([spec 021](021-subject-boundary-and-external-source-evidence.md)) and carries no
evidentiary weight beyond letting the offline suites run.

---

## A.2 Subject, archive, file-list, verifier — consistent

```
subject   6ce915e72c1be4e1c76e2d1c88e115c2baee1649
archive   5dc056a4b6b996adcb0d4b2efc61959fb5e538a5a22a01da8e333c430cc4472e
files     240
file-list 7821387167325e7ad5a7ed73db0d97cbd2c3828ac529ca704afd8b6ee86eb1c6
verifier  verify_clean_archive.sh — all passed (16 assertions)
```

Re-derived independently again for this package, identical each time, and all six per-file
digests in §2.1 re-confirmed.

**A misreading of my own, corrected.** A first attempt at the file-list digest produced
`42ecf306…` and **did not match**. The cause was **my command, not the content**: I hashed
*file names only, with the `dev2026/` prefix*, whereas the recorded digest is produced by
`file_list_of()` in `verify_clean_archive.sh` — a `LC_ALL=C`-sorted list of
`<sha256>  <path>` lines over the **extracted** tree, paths relative to `dev2026/`.
Reproducing the recorded method gives `7821387…b1c6` on 240 files, exactly.

**The distinction matters and is in the subject's favour:** the recorded file-list digest is
over **per-file content digests**, not over names. It detects a changed file; a digest of
names would not.

---

## A.3 Identity appears ONLY after the subject

| | subject `6ce915e` | 88-row ledger | repo@`HEAD` outside this request |
|---|---|---|---|
| `dep3a` | **0** | **0** | **0** |
| `19161` | **0** | **0** | **0** |
| `woa23-dep3a-candidate` | **0** | **0** | **0** |

Verified twice: by substring search over the **detached worktree** (240 files, 0 hits each)
and over every committed file at `HEAD`. Each string occurs in exactly **one** committed
file — **this request**, which postdates the subject.

### A.3a The `5ae8f49` subject conflict — RESOLVED, per review

**Resolution: the subject stays `6ce915e`, and the batches were re-run on it.**

`5ae8f49` is the **request** commit. Adopting it as the subject would have placed this
document inside the tree it references and **burned `dep3a` and `19161`**, since both appear
only here — the way `b35c1`/18282, `b35b1`/18291, `b35A` and `pm2H` were lost.

**The previous package's fallback argument is withdrawn.** It reported batches run at
`5ae8f49` and reasoned that, because every commit between the two touches only `specs/`
(10 files; **0** under `deploy/`, `scripts/`, `bench/`, `conf/`), the executable content was
identical and the batches therefore described the subject. **That reasoning was sound and is
still true — and it is no longer relied upon.** The batches in A.1 ran at `6ce915e` itself.

**`dep3a` and `19161` are not written into the subject.** They live only in this
post-subject request, which is what keeps them first-use.
## A.4 The runtime, store and process evidence — now scheduled in full

**This section used to be a list of what could not be reported. It has been replaced by
[Appendix B](#appendix-b--runtime-store-and-process-evidence-the-complete-schedule), which
does the work rather than deferring it.**

Appendix B takes each requested item and splits it into what is **settled offline now, from
the subject** — with the value — and what can **only** be observed at execution, with the
exact command and its acceptance criterion.

**Four items turned out to be decidable offline** and are now answered rather than deferred:

| item | settled offline | where |
|---|---|---|
| **worker count** | **2** — pinned by a four-link chain that refuses rather than defaults | B.4 |
| **no `--reload`** | the launcher **cannot emit it**; the exec line is a fixed literal list plus `TLS_ARGS`, which has exactly two assignments in the file | B.7 |
| **`api.app:app`** | a **literal** at `production_app.sh:42` — not from env, not from config, no default branch | B.7 |
| **package lock** | `uv.lock` `0d2980a5…cc69`, `pyproject.toml` `aa846b8b…e378e`, both at `6ce915e` | B.1 |

**A correction to an earlier draft of this package:** it claimed Node's version had **never**
been recorded in this campaign. **That was wrong.** `probeA`, `probeB` and `probeC` all
recorded it and agree — `/usr/local/bin/node` → `/usr/bin/node`, **v22.14.0**, `root:root`
`755`. What is outstanding is **re-verification in a D-3 execution**, not first capture: D-3
re-confirms realpath, version and permissions **before start**, and a deviation is a finding
(B.2).

**One divergence is predicted before the run rather than discovered after it:** `uv sync`
previously resolved **Python 3.11.14** while production runs **3.11.4** (B.1). D-3's
interpreter will probably not match production's patch version. That is recorded as a
**divergence**, is **not** a STOP, and is **not** to be "fixed" by pointing `WOA23_PYTHON` at
production's shared interpreter — the failure spec 016 exists to prevent.

**Reference points carried for re-verification, never as present-tense claims:** PM2
**5.4.2** (Stage A, two daemon-safe sources) and the `bin/pm2` sha256
`bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d` (probeC **§4.1**).
**Both are re-derived at execution**; a difference is a finding.

**An earlier draft cited `2c88ef6d…4aee` here. That was my error** — it is probeC **§4.2**'s
digest, for a different 87-byte file that probeC marked as *not* the path in use. Preflight
confirmed the correct value; see B.3.

## A.5 Package status

**Batches and provenance PASS**, and the batches now attest the subject **directly** — three
serial runs in a detached worktree pinned to `6ce915e`, all three identical suite-by-suite,
each recording its own `head=6ce915e…`, `tracked_dirty=0` and exit 0.

Subject **`6ce915e`** · label **`dep3a`** · port **`19161`** — **no identity or port
conflict**, and no new identity needed. The subject-conflict flagged in the previous package
is **resolved** (A.3a): the identity is written **only** into this post-subject request.

**Four errors of mine are corrected in place rather than left standing.** None changed a
suite result; all changed something I had reported:

| # | what I got wrong | correction |
|---|---|---|
| 1 | assertion total reported as **4382** | **4528** — my regex required `(N assertions)`, dropping `test_column_contract.py`'s 146 (A.1b) |
| 2 | a file-list derivation gave `42ecf306…` | my command hashed **names only**; the recorded digest is over **per-file content digests** — real method reproduces `7821387…b1c6` (A.2) |
| 3 | "Node has **never** been recorded in this campaign" | **false.** probeA/probeB/probeC all recorded it: `/usr/local/bin/node` → `/usr/bin/node`, **v22.14.0**, `root:root` `755`. I searched two files and generalised (B.2) |
| 4 | "**no process was leaked** by these batches" | **false, and the method was wrong too.** I argued from process *age*. By PID and start time, **15 survivors** are ours — 5 per batch, from `test_requests.sh` (A.1a-1) |

**Error 4 uncovered a larger pre-existing defect:** **1065** leaked processes of that shape
are alive on this machine, dating back to **Aug 10**. `test_requests.sh` leaks ~5 per run.
**Nothing killed** — recorded because the audit found it (A.1a-2).

### A.5a THIS PACKAGE IS NOT CLEAN — the open blocker

**`scripts/test_requests.sh` is defective and the defect is unfixed.** Full root cause,
per-process attribution and the D-3 exposure analysis are in the
[survivor audit](D3-survivor-process-audit.md). In short:

| | |
|---|---|
| cause | `SERVERS="$SERVERS $!"` runs inside `$(start_server …)` — a **command-substitution subshell**. The parent's `SERVERS` stays `""`, so the `EXIT` trap kills nothing. Reproduced, and confirmed on bash 3.2.57 |
| why exit was still 0 | **no assertion covers it**, `cleanup` **cannot fail**, it is a **vacuous pass**, and `run_suites.sh` records survivors as a diagnostic **NOTE** by design |
| D-3 exposure | **none found.** No `deploy/` file references `test_requests.sh`, sources its libraries, invokes any suite, or **launches a background process at all** |
| consequence for this package | **not clean, and not called clean.** A fail-closed survivor gate is added (§8.0) so the exposure finding does not have to be trusted |
| consequence for the subject | **unchanged.** No file D-3 executes is modified, so no new subject and no re-run of the batches |

**What D-3 still is:** a **candidate deployment rehearsal using the real production store**.
**Not** production equivalence, **not** a production PASS, **not** a cutover, and **not** an
A11 gate.

**NOT AUTHORISED. NOT EXECUTED. No VM24 contact. NO EXECUTION AUTHORISATION IS REQUESTED
while the harness blocker is open.**

---

# APPENDIX B — Runtime, store and process evidence: the complete schedule

Every item the review asked to be completed is below. Each is split into **what is settled
offline now, from the subject** — with its value — and **what can only be observed when D-3
runs**, with the exact method and acceptance criterion.

**The split is the point.** Some of these are properties of the subject's own source and are
decided already; the rest are properties of a process that does not exist yet. Recording a
value for the second kind would be invention, so those carry a **`PENDING EXECUTION`** slot
and nothing else.

**Refusal rule for the whole appendix:** any criterion below that fails is a **STOP**, not a
thing to adjust and retry. Where a value merely differs from a reference point, it is
recorded as a **divergence** — never silently corrected.

---

## B.1 Python and venv

### Settled offline

| | |
|---|---|
| interpreter is **mandatory, no default** | `production_app.sh:128–135`: `PY="${WOA23_PYTHON:-}"`, then `die` if blank, then `die` if not executable. The fallback that produced `pm2G` (spec 016) cannot recur |
| `uv.lock` @ `6ce915e` | `0d2980a5928d4d0964d6cb3b78bffae14aa11a70d3b51ca00f4cf39073dccc69` |
| `pyproject.toml` @ `6ce915e` | `aa846b8be70b0b5d466d0e2a0bbb1f4dfe6ccac5d28f795a57c1c6bbea7e378e` |

### A divergence already on record — expected, and NOT to be papered over

Production runs **Python 3.11.4** (`/home/odbadmin/.pyenv/versions/py311/bin/python3.11`,
spec 010:186). A previous **isolated venv built by `uv sync` resolved Python 3.11.14**
(spec 002:2106, 2113). **`uv sync` does not reproduce production's patch version**, and
there is no reason to expect D-3's venv to.

**Consequence, stated before the run rather than discovered after it:** D-3's interpreter
will probably **not** match production's patch version. That is **recorded as a divergence**
and it is one more reason D-3 is a rehearsal and not production equivalence. **It is not a
STOP**, and it is **not** to be fixed by pointing `WOA23_PYTHON` at production's interpreter
— that would put a shared environment back in the serving path, which spec 016 exists to
prevent.

### The attribution limit this imposes — binding on the D-3 result

> **D-3 runs a different interpreter, and a different resolved package set, from the
> deployment D-1 observed. Therefore NO response difference D-3 finds may be attributed to
> the API code alone.**

Any difference between a D-3 response and D-1's production response has **at least three**
candidate causes, and D-3 cannot separate them:

1. the **API code** — `api.app:app` versus the legacy `woa23_app:app`;
2. the **interpreter** — 3.11.x patch-level divergence;
3. the **resolved dependency set** — polars, xarray, zarr, numpy and orjson versions come
   from this run's `uv.lock`, not from production's environment, and they participate
   directly in reading, filtering, ordering and serialising the data.

**This binds the wording of any D-3 result.** A difference is recorded as *observed under a
divergent runtime*, never as "the candidate changed X". The already-decided classes
(spec-015 column order, the `1.0.0 → 1.1.0` documentation change) are the exception only
because they were decided **elsewhere, on other evidence** — D-3 confirms which side the
candidate sits on; it does not establish the cause.

**A matching response is likewise not proof of code equivalence**, since two differences
could cancel. D-3 observes; it does not attribute.

### OFFLINE-ONLY, and a cache gate BEFORE staging

**`uv` would otherwise DOWNLOAD the interpreter.** `uv.lock` pins `==3.11.*` and the only
3.11 available to uv on VM24 is `cpython-3.11.14`, marked `<download available>`. That is a
network access, so it is a policy decision, not an implementation detail — see the
[network access policy](D3-network-access-policy.md).

```
export UV_OFFLINE=1
export UV_PYTHON_DOWNLOADS=never
uv sync --frozen --locked --offline          # --no-cache is FORBIDDEN
```

| gate, run **before** anything is staged | on failure |
|---|---|
| `cpython-3.11.14-linux-x86_64-gnu` already in `UV_PYTHON_INSTALL_DIR` | **STOP before staging** |
| every locked artifact present in the uv cache (`--dry-run` proves it) | **STOP before staging** |
| selected interpreter reports **3.11.14** | **STOP** |

**No fallback of any kind:** not system **3.12.3**, not production's **3.11.4** (a shared
interpreter in the serving path is the spec 016 failure), not a relaxed constraint, not an
edited lock, not an alternate index. **A stopped run and a report is the correct outcome.**

### At execution

```
readlink -f "$WOA23_PYTHON"; "$WOA23_PYTHON" -V; sha256sum "$WOA23_PYTHON"
sha256sum dev2026/uv.lock dev2026/pyproject.toml      # must equal the two digests above
"$WOA23_PYTHON" -m pip list --format=freeze           # manifested, then hashed
```

| record | criterion |
|---|---|
| absolute path, version, sha256 | `PENDING EXECUTION` |
| `uv.lock` / `pyproject.toml` digests | **must equal** the subject values above, or **STOP** |
| resolved package set + its digest | `PENDING EXECUTION` |
| version vs production's 3.11.4 | recorded as **match** or **divergence**; divergence is not a STOP |

---

## B.2 Node

### Already on record — from three independent probes

**Node HAS been recorded in this campaign.** An earlier draft of this appendix said it never
had been; **that was my error**, produced by searching only the B1 Stage A inventory and the
Stage C draft and then generalising to "anywhere". `probeA`, `probeB` and `probeC` each
captured it, and they agree exactly:

| | value | recorded by |
|---|---|---|
| resolved | `/usr/local/bin/node` | probeA, probeB, probeC |
| realpath | `/usr/bin/node` | probeA, probeB, probeC |
| owner / mode / size | `root:root` `755` `120177224` | probeA, probeB, probeC |
| **version** | **v22.14.0** | probeA, probeB, probeC |

System-wide, root-owned, world-executable, reachable by uid 994 with no `PATH` change.

**What is actually outstanding is re-verification, not first capture.** **Node has not yet
been re-verified in a D-3 execution**; the values above are historical probe readings.
**D-3 must re-confirm realpath, version and permissions before start**, and any difference
from the table is a **finding**, not something to reconcile away.

It matters because `make_staging_override.js` is run by `node`
(`staging_execute.sh:387`) and `staging_execute.sh` reads the generated config through
`node -e` in several places, including the TLS-mode branch.

### At execution — re-confirmed before start

```
command -v node; readlink -f "$(command -v node)"; node -v
stat -c '%U:%G %a %s %n' "$(readlink -f "$(command -v node)")"
sha256sum "$(readlink -f "$(command -v node)")"
```

| record | criterion |
|---|---|
| resolved path → realpath | expected `/usr/local/bin/node` → `/usr/bin/node` |
| version | expected **v22.14.0** |
| owner / mode / size | expected `root:root` / `755` / `120177224` |
| sha256 | `PENDING EXECUTION` — **not captured by any probe**, so this genuinely is a first capture |
| any deviation from the probe values | recorded as a **divergence / finding** |

---

## B.3 PM2

### Reference points from earlier stages — to be RE-VERIFIED, not assumed current

| | value | source |
|---|---|---|
| version | **5.4.2** | Stage A, from **two** sources that cannot spawn a daemon: the God Daemon's own `cmdline` (`PM2 v5.4.2: God Daemon`) and `.npm-global/lib/node_modules/pm2/package.json` |
| binary | `/home/odbadmin/.npm-global/bin/pm2` — a **symlink** → `../lib/node_modules/pm2/bin/pm2` | probeC **§4.1** |
| resolved file | `.../lib/node_modules/pm2/bin/pm2`, `odbadmin:odbadmin` `775`, **56 bytes** — a shim that `require`s `../lib/binaries/CLI.js` | probeC **§4.1** |
| **binary sha256** | **`bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d`** | probeC **§4.1** |

> ### CORRECTION — a wrong digest of mine, caught by preflight
>
> Earlier drafts carried `2c88ef6dfbc924be2f9a3e3e8487f6d62427ffc44088d57b37f77e50da2c4aee`
> as the digest of `bin/pm2`. **It is not.** probeC recorded that value in **§4.2**, for a
> **different file** — `/home/odbadmin/.npm-global/lib/node_modules/pm2/pm2`, **87 bytes** —
> which probeC explicitly labelled *"not the path proposed for use; §4.1's `bin/pm2` is."*
> **I attached §4.2's digest to §4.1's path.**
>
> **The host was never inconsistent — the request was.** Preflight measured
> `bbb58671…256d`, matching probeC §4.1 **exactly**, along with the same realpath, symlink
> target, owner, mode and size.

**These are historical readings. They are carried as reference points and are re-derived at
execution; a difference is a finding, not something to reconcile away.**

### The constraint on how it is read

**`pm2 -v` is forbidden.** `pm2` spawns a God Daemon when none is running, so the version
command is not read-only. Version comes from the daemon's own `cmdline` and from
`package.json` on disk — the same two sources Stage A used.

### At execution

```
sha256sum /home/odbadmin/.npm-global/bin/pm2
node -e 'console.log(require("/home/odbadmin/.npm-global/lib/node_modules/pm2/package.json").version)'
tr '\0' ' ' < /proc/<god-pid>/cmdline          # for THIS run's PM2_HOME, once its daemon exists
```

| record | criterion |
|---|---|
| binary sha256 (follows the symlink) | compared with `bbb58671…256d`; **equal or divergence recorded** |
| realpath / owner / mode / size | expected `.../lib/node_modules/pm2/bin/pm2`, `odbadmin:odbadmin`, `775`, `56` |
| version from `package.json` | expected `5.4.2` |
| this run's daemon cmdline + its `PM2_HOME` | `PENDING EXECUTION` |

---

## B.4 Worker count — **settled offline: 2**

This one needs no execution to decide. The value is pinned by a chain in the subject, and
every link refuses rather than defaults:

| # | link | file |
|---|---|---|
| 1 | `WOA23_WORKERS: '2'` — the source value | `ecosystem.production.config.js:96` |
| 2 | the override generator **refuses** if the staging value differs from the source | `make_staging_override.js:271–273` |
| 3 | the executor asserts it in the **process environment** | `staging_execute.sh:521` |
| 4 | the launcher reads it and passes it to gunicorn | `production_app.sh:137`, `:164` (`-w "$WORKERS"`) |

**So the expected worker count is 2, and it is not a guess.**

### At execution

| record | criterion |
|---|---|
| workers found as children of the gunicorn master | **exactly 2** |
| zero workers | **refusal** — `staging_execute.sh:570` already refuses this, because a check that ran only against the master never inspected anything that serves |

---

## B.5 `data/` symlink target, and inode/metadata identity across the run

### Settled offline

`WOA23_ZARR_STORE=data/` is a **relative literal**. No absolute store path is interpolated
anywhere, so the process reaches the store only through the staging tree's own `data`
symlink.

### At execution — taken THREE times, identically

**Before start · after the ten cases · after stop.**

```
readlink data
readlink -f data
stat -c '%d %i %u %g %#a %h %s %Y %n' data          # the link itself (lstat)
stat -Lc '%d %i %u %g %#a %h %n' data               # the target
```

**Fingerprint method — FIXED, and it is the Stage C / D-1 method:**

```
find "$STORE" -type f -printf '%p\t%s\t%T@\n' | LC_ALL=C sort | sha256sum
      absolute paths · files only · TAB · LC_ALL=C sort
```

**Used for D-3's WITHIN-RUN before/after comparison only.** It is the method Stage C and D-1
used, and preflight's reconstruction agreed with their recorded `abe6c212…1806` — but that
value is recorded **only in truncated form**, so the agreement covers **12 of 64 characters**
and is a **partial check, not a cross-stage proof of an unchanged store**. No cross-stage
conclusion is drawn from it. The C1/C2 method (relative paths,
all entries) and the run-1 variant (space separator) are **different metrics** and are
recorded separately; **cross-method comparison is forbidden** — a difference so produced is a
method artefact, not drift.

| record | criterion |
|---|---|
| `readlink data` | the same target at all three points |
| device + inode of the target | **identical at all three points** |
| uid, gid, mode, nlink of the target | **identical at all three points** |
| any difference | a **finding**, recorded as such |

**The limit, stated with the method:** device+inode+mode+mtime identity shows the symlink was
not re-pointed and the target directory was not replaced. **It is metadata, not content.** A
change that preserves size and mtime is invisible to it, and D-3 does **not** claim store
content integrity.

---

## B.6 Unwritability scan — store path, every ancestor, and the symlink target

**Run as uid 994 (`woa23c1ro`), which does not own the store.** This is the check D-1 could
not make: D-1 ran as the store's **owner**, so its read-only claim was application-level
only. Here the kernel is the one refusing.

### At execution — before anything starts

```
T="$(readlink -f data)"
test -w "$T"                                        # must FAIL
find "$T" -writable            | wc -l              # must be 0
find "$T" ! -readable          | wc -l              # must be 0
find "$T" -type d ! -executable| wc -l              # must be 0
find "$T" -type l -exec sh -c 'case "$(readlink -f "$1")" in "$2"/*) ;; *) echo "$1";; esac' _ {} "$T" \; | wc -l   # escaping symlinks: 0
p="$T"; while [ "$p" != "/" ]; do p="$(dirname "$p")"; test -w "$p" && echo "WRITABLE ANCESTOR: $p"; done   # must print nothing
find "$T" -type f | wc -l ; du -sb "$T"             # count and bytes
find "$T" -printf '%p %s %T@\n' | LC_ALL=C sort | sha256sum    # path+size+mtime fingerprint
```

| # | criterion | on failure |
|---|---|---|
| 1 | store root **not writable** by uid 994 | **STOP before any service starts** |
| 2 | writable paths beneath the store: **0** | **STOP** |
| 3 | writable ancestors up to `/`: **0** | **STOP** |
| 4 | symlink target resolves to the real store and nothing else | **STOP** |
| 5 | escaping symlinks / unreadable files / untraversable dirs: **0 / 0 / 0** | **STOP** |
| 6 | file count, total bytes, `path+size+mtime` fingerprint | recorded — `PENDING EXECUTION` |

**Criterion 3 is why the ancestor walk is here at all:** an unwritable store under a writable
parent is not protected, and checking only the store root would have called that a pass.

---

## B.7 uid, `PM2_HOME`, argv, no `--reload`, and `api.app:app` from the staging root

### Settled offline — two of these are decided by the subject's source

**`api.app:app` cannot be anything else.** `production_app.sh:42` is the literal
`APP="api.app:app"`. It is not read from the environment, not from the config, and has no
default branch.

**`--reload` cannot be emitted.** The exec line is a fixed literal argument list:

```
exec "$PY" -m gunicorn "$APP" \
  -w "$WORKERS" -k uvicorn.workers.UvicornWorker -b "127.0.0.1:$PORT" \
  ${TLS_ARGS[@]+"${TLS_ARGS[@]}"} --timeout 120 --graceful-timeout 10
```

`TLS_ARGS` is the only variable-length part, and it is assigned in **exactly two places** in
the whole file — `TLS_ARGS=()` at `:90` and `TLS_ARGS=(--keyfile "$KEYFILE" --certfile
"$CERTFILE")` at `:105`. **There is no third assignment and no append**, so no flag other
than those two can enter argv. With `WOA23_TLS=off` the array stays empty.

**This is an offline proof about the launcher, not an observation of a process.** It says the
subject cannot produce `--reload`; the run still records what argv actually was.

### At execution — for the master AND every worker, from `/proc`

```
grep -E '^(Uid|Gid):' /proc/<pid>/status              # all four fields = 994
tr '\0' '\n' < /proc/<pid>/cmdline
tr '\0' '\n' < /proc/<pid>/environ | grep -E '^(PM2_HOME|WOA23_|PYTHONPATH)='
readlink /proc/<pid>/cwd
```

| # | record | criterion |
|---|---|---|
| 1 | `Uid:` and `Gid:` — real, effective, saved, fs | **994 on the master and on every worker** |
| 2 | argv contains | `api.app:app`, `-w 2`, `-b 127.0.0.1:19161`, `--timeout 120`, `--graceful-timeout 10` |
| 3 | argv does **not** contain | `--reload`, `--keyfile`, `--certfile` |
| 4 | `PM2_HOME` | **this run's own**, read from the daemon's own `environ` — never production's |
| 5 | `WOA23_PORT` / `WOA23_ZARR_STORE` / `WOA23_PYTHON` | `19161` / `data/` / the staged venv |
| 6 | `WOA23_TLS_KEYFILE`, `WOA23_TLS_CERTFILE` | **ABSENT** — if either appears, `INVALID_ENVIRONMENT`, no result |
| 7 | `WOA23_PM2C_GRANTED` | **ABSENT** in the served process — already asserted by `staging_execute.sh:583`, and unset at `:411` after the generator runs |
| 8 | `/proc/<pid>/cwd` | the **staging root** |
| 9 | process tree depth | `production_app.sh` **`exec`s** → depth-1 expected. **Recorded, not required** |

### How "loaded `api.app:app` from the staging root" is actually established

**Say plainly what the evidence is worth.** There is no way to read a running interpreter's
`sys.modules['api.app'].__file__` from outside it, so this is established by **three
independent lines**, and the third is the strongest:

1. **kernel-level** — `/proc/<pid>/cwd` is the staging root, and gunicorn resolves
   `api.app:app` from `sys.path[0]`, which is the cwd;
2. **path exclusivity** — every entry of the interpreter's `sys.path` is enumerated and
   checked for any *other* `api/app.py`; the staging one must be the only one reachable;
3. **behavioural** — **C20a returns `info.version` `1.1.0`.** The subject's
   `api/app.py:43` is the literal `version="1.1.0"`; D-1 observed production's legacy module
   serving **`1.0.0`**. A `1.1.0` document is served by the candidate and by nothing else in
   this campaign's record.

**Lines 1 and 2 are inference about which file would be found; line 3 is the served output
of the module that actually loaded.** If line 3 disagrees with lines 1 and 2, **line 3 wins
and the run has a finding**, not a correction.

---

## B.8 What this appendix still does not cover

- **Store content integrity** — B.5 is metadata only, and B.6 is permissions only.
- **TLS** — off by decision; nothing here validates a TLS path (§5).
- **Production equivalence** — the interpreter is expected to diverge (B.1), TLS is off, and
  the port and `PM2_HOME` are deliberately not production's.
- **Any A11 gate** — a staging marker count, if recorded at all, is an observation (§1.6).
