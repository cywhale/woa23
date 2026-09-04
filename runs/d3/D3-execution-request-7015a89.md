# D-3 — candidate deployment rehearsal on the real production store: **execution request**

**Prepared entirely OFFLINE. No VM24 contact this round. D-3 NOT retried. No PM2 started, no
store symlink created, no case issued, no process killed, no cleanup. Production untouched.
C1/C2 not re-run.**

**In `runs/d3/`, OUTSIDE `git archive <sha> dev2026`, committed AFTER the subject was fixed.**

> ## Completing this request is NOT authorisation to execute D-3.

---

## 1. Why the previous run halted — a fail-open defect in my own guard

**D-3 stopped at store guard 4 and the halt was correct.** The guard was written **inverted**:

```bash
[ "$STORE_UID" != "$ME_UID" ] && die "...owned by uid $STORE_UID, which is this account"
```

Store uid **1000**, account uid **994** → they differ → **die**. It fired on the **safe**
state, and would have **passed silently when they matched** — the dangerous one. **Fail-open
in the wrong direction.**

### 1.1 Why the tests did not catch it

**They asserted the guard's PRESENCE — a grep for its message — and never its BEHAVIOUR.**
That is the same gap class as the synthetic-token defect: a guard whose decision is never
exercised is only a comment with a syntax error's blast radius.

### 1.2 The fix

The decision now lives in **`deploy/lib_store_guard.sh`** as `store_owner_verdict`, sourced
by the driver, **so a test can call it with every combination of uids** instead of only a
real production store being able to reach it.

| store uid | verdict | return |
|---|---|---|
| **1000** (authorised), account 994 | **`ok`** | 0 |
| **equal to the running account** | **`self`** | 5 |
| `0` | `root` | 6 |
| empty / unreadable | `unreadable` | 3 |
| non-numeric | `nonnumeric` | 4 |
| any other uid | `unexpected` | 7 |

**The order is deliberate:** `self` is reported **before** `root` and **before** `unexpected`,
so the most dangerous case is never described as merely surprising. **An unreadable owner is
a refusal, never a default.** The authorised owner is the literal **`1000`**, beside the
existing literal store path, and the guard **runs before the store symlink is created**.

### 1.3 The tests are FUNCTIONAL, not source greps

| covered | how |
|---|---|
| owner **1000** / account **994** | decision called directly → `ok`, rc 0 |
| owner **994** (== account) | → `self`, rc 5 |
| **unreadable** owner | → `unreadable`, rc 3 |
| **non-numeric** owner | → `nonnumeric`, rc 4 |
| **root** owner | → `root`, rc 6 |
| **unexpected** uid 1234 | → `unexpected`, rc 7 |
| ordering | owner 0 **and** account 0 → **`self`**, not `root` |
| the driver **calls** it | sources the library, invokes `store_owner_verdict`, no longer carries the inverted test |
| **end-to-end refusal** | the **real driver**, real mode, non-authorised store → **non-zero exit**, **no store symlink**, **no `PM2_HOME`**, **never reaches `pm2 start`**, names a reason |

**Mutation test:** re-inverting the guard — the exact D-3 defect — is caught by **five**
assertions.

### 1.4 The stale interpreter instruction, removed

The driver told the operator to run
`uv sync --python /home/odbadmin/.pyenv/versions/py311/bin/python3.11`. **That is
production's shared 3.11.4**, and following it would have put a shared interpreter in the
serving path — the failure **spec 016** exists to prevent. **I refused it by hand during the
halted run**; it is now removed from both places and replaced with an offline locked sync
against the run's own isolated interpreter. The reason is kept as a comment.

**A functional test** confirms `production_app.sh` **refuses** when `WOA23_PYTHON` is unset
rather than falling back. **It needed two corrections of mine:** the launcher checks the
**anchor**, then **TLS**, before the interpreter — so my first two attempts were passing on
an *unrelated* refusal. It now supplies a valid anchor and `WOA23_TLS=off`, **plus an
assertion that the refusal is about the interpreter and not an earlier check**.

---

## 2. Subject and provenance

```
subject   7015a89589ac2ae3d09298fa21609928ef6b3275
archive   d577315d2fc53353ac0fd1a73b2086d96b21b96406067053089cc6f2369fc186
files     254
file-list 45b27a6fc3b286655a21a71fde52f7adff6a34f60880e0c83bb0ab7fac017aa4
```

**Superseded, in order, NOT back-filled:** `ccfca934` → `9269dacb` → `fae5a417` →
`8bb7f2c8` → `19aabf02` → **`7015a895`**. Their batches are **records, not evidence**, and
the void `5ae8f49` set stays quarantined.

### 2.1 Three serial batches at this subject

| batch | HEAD, as the batch recorded it | tracked dirty | untracked | suites | non-zero | assertions | exit |
|---|---|---|---|---|---|---|---|
| 1 | `7015a895…` | **0** | 1 | **55** | **0** | **4685** | **0** |
| 2 | `7015a895…` | **0** | 1 | **55** | **0** | **4685** | **0** |
| 3 | `7015a895…` | **0** | 1 | **55** | **0** | **4685** | **0** |

**All 55 result lines identical** across 1↔2, 1↔3, 2↔3; each log **ends on its own `TOTAL`
line**, so none is truncated.

**Assertion delta, reconciled:** 4655 → **4685**, exactly **+30**, being
`test_real_store_mode.sh` growing **50 → 80** with the ownership and interpreter tests. **No
other suite changed count**, and **no suite was added or removed**.

```
WOA23_BATCH_COMPLETE subject=7015a89589ac2ae3d09298fa21609928ef6b3275 label=s7015a89
                     token=s7015a89-81835-bsd-1788146401 batches=3 nonzero=0

correct subject + label + token  ->  0   accepted
superseded 19aabf02              ->  4   subject mismatch
superseded 8bb7f2c8              ->  4   subject mismatch
```

`untracked=1` is `dev2026/.venv`, the **external local helper**; `tracked_dirty=0` stands and
**"pristine" is not claimed**.

---

## 3. Identity — `dep3m` / `19301`, and the HEAD rule settled

| | value | subject tree | **archive** | ledger | repo@`HEAD` |
|---|---|---|---|---|---|
| **label** | **`dep3m`** | **0** | **0** | **0** | **only this request** |
| **port** | **`19301`** | **0** | **0** | **0** | **only this request** |
| app name | `woa23-dep3m-candidate` | **0** | **0** | **0** | only this request |
| staging root | `~/woa23-dep3m` | — | — | — | — |
| workdir | `~/woa23-dep3m-work` | — | — | — | — |
| `PM2_HOME` | `~/woa23-dep3m-pm2` | — | — | — | — |
| tmpdir | `~/tmp-dep3m` | — | — | — | — |

**Selected after subject `7015a895` was fixed**, and verified clean on subject, archive,
ledger and `HEAD` **at selection time**.

### 3.1 A literal "absent from HEAD" rule cannot be satisfied by ANY identity

**This is now demonstrated rather than argued.** Re-checking the three candidates from the
previous round:

```
dep3m  19301  ->  runs/d3/D3-execution-request-7015a89.md
dep3n  19309  ->  runs/d3/D3-execution-request-7015a89.md
dep3p  19319  ->  runs/d3/D3-execution-request-7015a89.md
```

**`dep3n`/`19309` and `dep3p`/`19319` were never selected.** They appear only because I
listed them as *"alternatives verified and unused"*. **Merely naming a candidate in a
committed document disqualifies it** under a literal reading — so the rule, applied
literally, admits no identity at all: the request that names one necessarily puts it at
`HEAD`.

**The coherent reading, and the one used here:** the check is made **at selection time,
before the request naming it is committed**, and what must stay zero afterwards is the
**subject tree and the archive** — which is what a burned identity actually means, because
`git archive <sha> dev2026` is what a run is verified against.

**Two consequences, applied:**

1. **`dep3m`/`19301` is kept**, and this request is **finalised IN PLACE** rather than
   reissued — a second document naming it would create a second `HEAD` occurrence for no
   benefit, which is exactly what stranded `dep3a`, `dep3c`, `dep3d`, `dep3f` and `dep3h`.
2. **No alternates are listed any more.** Naming rejected candidates burned two of them for
   nothing; that practice stops here.

**If the PI wants the literal reading enforced instead**, then no identity can ever be used
and the rule needs changing rather than another pair spent. **I am not deciding that
silently — the reasoning is above and the choice is stated.**

### 3.2 Old identities, none reused

**Excluded by instruction and not used:** `dep3a`, `dep3c`, `dep3d`, `dep3f`, `dep3h` and
their ports `19161`, `19187`, `19211`, `19229`, `19259`. **Zero occurrences of any of them
in the subject tree.**

**`dep3h`/`19259` remains PARTIALLY CONSUMED** — `~/woa23-dep3h` and `~/woa23-dep3h-work`
exist on VM24 from the halted run; `PM2_HOME`, tmpdir and the store symlink were never
created, and the port never bound. **That state is preserved untouched; cleanup is a separate
authorisation.**

**At execution, re-checked live:** the ledger at the authorised subject, `ss -ltn` showing
`19301` unbound, and every `dep3m` path absent. **Any disagreement stops the run.**

---

## 4. Store — real, read-only, fail closed

**`--store-mode real-readonly --real-store /home/odbadmin/python/woa23/data`.** Synthetic
mode remains the **default and unchanged** — builder, 72-file assertion, `chmod` and write
probe all still present and asserted.

| # | verified before anything proceeds |
|---|---|
| 0 | GNU `find -printf` / `stat -c` available — else the guards return 0 by failing |
| 1 | the path equals **one authorised literal** |
| 2 | exists, a directory, **not itself a symlink** |
| 3 | **realpath exactly equals** the authorised store |
| **4** | **owner is the authorised uid 1000, and NOT this account** — §1.2 |
| 5 | **not writable**, readable, traversable — `access(2)`, honouring ACLs |
| 6–7 | **0** writable paths beneath; **0** writable ancestors to `/` |
| 8–9 | **0** symlinks resolving outside; **0** unreadable / untraversable |
| 10–11 | readable anchor; staging symlink resolves to the authorised store |
| 12 | metadata fingerprint, **metadata-only** |

**No permission change, ownership change, directory creation or removal, deletion or write
touches the store** — not even a write probe. The only filesystem write is the staging
`ln -s`, inside the staging root.

**A recorded property, not a pass:** the store is **group-writable** (`0775`, group
`odbadmin`, 123 205 entries). uid 994 is **not** in that group, and effective writability
measures **0**. It grants this account nothing; it is production's own mode.

---

## 5. Runtime and process

| | |
|---|---|
| module | **`api.app:app`** — a literal in `production_app.sh` |
| `PM2_HOME` / venv | **isolated**, this run's own |
| PM2 | **absolute binary** `/home/odbadmin/.npm-global/bin/pm2`, sha256 `bbb58671…256d`; **`pm2 -v` never invoked** |
| interpreter | **CPython 3.11.14**, the provisioned isolated build — **never** production's 3.11.4, **never** system 3.12.3 |
| `UV_OFFLINE` / `UV_PYTHON_DOWNLOADS` | **1** / **never** |
| sync | `uv sync --locked --offline` |
| **`PYTHONDONTWRITEBYTECODE=1`** | **exported by the spawning shell**, verified in the **master AND every worker** from `/proc/<pid>/environ`; absent from any worker is a **finding** |
| workers | **2** |
| TLS | **OFF, and reported UNVALIDATED.** Key/cert **ABSENT** from every staging process; production credentials never enter one |
| per process | uid **994** (real, effective, saved, fs), `PM2_HOME`, argv, port, **no `--reload`** |

**Production `PM2_HOME`, production PM2 lifecycle and port 8050 are never touched.**

---

## 6. Cases, evidence, retention

**The fixed ten**, in order, from `bench/contract_cases.py` in the **subject archive**, never
retyped. **One attempt each. No retry.** Sent **only** to `127.0.0.1:19301`; **no request to
the production API**.

Retained per case: UTC timestamp, full URL and parameters, HTTP status, **body in full**,
size, elapsed time, body SHA-256.

**Process inventory** before and after, observer excluded **by PID/PGID identity** — never a
fuzzy command-line grep. **Unexpected survivor, UNKNOWN, INDETERMINATE or PID reuse ⇒
INCOMPLETE**, evidence retained.

**Never used:** `kill`, SIGKILL, `pm2 kill`, `pm2 delete`, `pm2 all`, wildcards, `save`,
`resurrect`, or any improvised cleanup. **Daemon, tree, workdir, `PM2_HOME`, store link and
artefacts are retained** — **cleanup is a separate authorisation.**

**A11 is not a gate**, and a marker delta is **never** written as an exact request count.

---

## 7. Result limits — fixed wording

> **D-3 produces a CANDIDATE DEPLOYMENT REHEARSAL OBSERVATION.**

| it is NOT | |
|---|---|
| production equivalence | **no** |
| a deployment PASS | **no** |
| a data-path correctness PASS | **no** |
| TLS validation | **no** — TLS is off and unvalidated |
| A11 validation | **no** |
| grounds to extend B1 or any production validation | **no** |

| disclosed with any result | |
|---|---|
| interpreter | **3.11.14** vs production **3.11.4** |
| package set | resolved from this `uv.lock`, **not** production's — it **may affect results** |
| attribution | **no response difference may be attributed to the API code alone**; a matching response is not proof of equivalence |
| store identity | **metadata-only** |
| C1 / C2 | **not re-run, not back-filled** |

---

## 7c. Batch status confirmation — read-only reconciliation

**Confirmed after the fact, not assumed.** No second batch set was started.

| # | check | result |
|---|---|---|
| 1–2 | is any batch still running? | **no** — driver pid `81835`, starttime `bsd-1788146401`, **gone**; `runid_alive` = **6** (gone, **not 8/PID-reuse**) |
| 3 | per-batch self-records | all three: `git head` **`7015a895…`**, tracked dirty **0**, untracked **1**, own `TOTAL: 55 \| NON-ZERO: 0`, **55** result lines, driver exit **0** |
| 4 | truncation | **each log ends on its own `TOTAL` line** — none partial |
| 5 | line-by-line | 1↔2, 1↔3, 2↔3 **IDENTICAL**; **0** non-zero exit lines |
| 6 | sentinel preconditions | `batches completed 3 of 3`, `non-zero total 0`, `postconditions yes`, **6 of 6** HEAD checks |
| 7 | sentinel | **exactly one**; token **matches the one derived independently from the runid file**; verify = **0**; **rejected (4)** for all five superseded subjects |
| 8 | INCOMPLETE / VOID | **none** |

**Assertion total, summed from the raw logs — 4685 per batch, independently:**

```
batch1: 54 of 55 result lines carry a count = 4661 ; + test_staging_store.py 24 = 4685
batch2: 54 of 55 result lines carry a count = 4661 ; + test_staging_store.py 24 = 4685
batch3: 54 of 55 result lines carry a count = 4661 ; + test_staging_store.py 24 = 4685
```

`test_staging_store.py` is the one suite whose result line ends on a caveat rather than a
count; it is read from its own stdout.

**No mixing:** the superseded subjects' batches (`ccfca934`, `9269dacb`, `fae5a417`,
`8bb7f2c8`, `19aabf02`) and the **void** `5ae8f49` set are neither merged, averaged nor cited
in support. The sentinel rejecting all five is the mechanical check of that.

**A self-observation correction:** a first pass reported "2 `run_batches.sh` processes" while
the driver pid was gone. A snapshot-first re-check found **none** — the match was **my own
tool-call shell**, whose argv contains the command text. That is the fourth instance of this
pattern in this campaign, and it is why the batch verdict rests on the self-recorded records
and the verified sentinel rather than on process counts.

---

## 8. Status

| | |
|---|---|
| VM24 | **not contacted this round** |
| D-3 | **not retried**; no PM2, no store symlink, no case, no kill, no cleanup |
| production | **untouched** |
| subject | **`7015a895…`** — no D-3 request, no execution identity |
| batches | **3 clean, identical, 55 suites, 4685 assertions, 0 non-zero**, sentinel verified |
| identity | **`dep3m` / `19301`**, first-use, named only here |
| `dep3h` / `19259` | **partially consumed, not reused; state preserved** |

**Awaiting review. Completing this request is not authorisation to execute D-3, and no VM24
contact will occur before an explicit new authorisation.**
