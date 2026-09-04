# D-3 — candidate deployment rehearsal on the real production store: **FINAL execution request**

**Prepared entirely OFFLINE. No VM24 contact. D-3 NOT executed. No staging, no `pm2 start`,
no store access, no case request, no cleanup. Production untouched. C1/C2 not re-run.**

**In `runs/d3/`, OUTSIDE `git archive <sha> dev2026`, committed AFTER the subject was fixed.**

> ## SUPERSEDED — see [`D3-execution-request-7015a89.md`](D3-execution-request-7015a89.md)
>
> **The D-3 run against this subject HALTED at the real-store owner guard**, which was
> written inverted and is a fail-open defect. Its figures (55 suites, **4655** assertions)
> remain correct **for this subject only** and are retained as a record; the current subject
> `7015a895` reports **4685**, the difference being `test_real_store_mode.sh` 50 → 80.
>
> **Its identity `dep3h`/`19259` is PARTIALLY CONSUMED** — the staging root and workdir exist
> on VM24 — **and is not reused**. That state is preserved; cleanup is separately authorised.
>
> ## FINAL — bound to subject `19aabf02860955b0e228918e451258341c6b49fd`
>
> **Completing this request is NOT authorisation to execute D-3.**

### Accepted batch evidence, fixed

| | |
|---|---|
| subject | **`19aabf02860955b0e228918e451258341c6b49fd`** |
| batches | **three complete, self-recorded** |
| suites | **55** |
| assertions | **4655 per batch** |
| non-zero exits | **zero** |
| comparison | **line-by-line identical** |
| sentinel | **exactly one**, bound to subject + label + run token |

**`8bb7f2c8` and its 4634 count are retained as a SUPERSEDED HISTORICAL RECORD.** The two
subjects are **never mixed or averaged**; each figure belongs to its own subject only.

---

## 1. The two final checks — **BOTH FAILED as implemented**

Reported first, because both required an executable change and therefore a new subject.

### 1.1 Check I — the driver's exit code on a refused sentinel was NOT TESTED

The `exit 1` path existed in `run_batches.sh`, but **nothing executed the driver to assert
it** — the suite only *grepped* the file. So the one state that must be impossible —
**"three batches passed, no sentinel, driver exited 0"** — was untested.

**§11 of `test_sentinel.sh` now runs the real driver** against a throwaway git worktree with a
stub suite runner:

| case | asserted |
|---|---|
| **positive control** — a clean run | **exit 0**, and **exactly one** sentinel |
| a **failing** batch | **exit non-zero**, **no** sentinel, **reason preserved** in the log |
| a **postcondition** failure (HEAD ≠ subject) | **exit non-zero**, **no** sentinel, mismatch named |
| a **partial** multi-batch run | **exit non-zero**, **no** sentinel |

**The positive control is load-bearing:** without it, the three negative cases could pass
because the driver never works at all.

**No escape hatch**, asserted: no `--force`, `--skip`, `--write-anyway`, `--ignore-failures`
or `--no-sentinel`; no such parameter in the library's **code**; and **the only `exit 0` in
the driver is the sentinel-success branch**.

**Mutation test:** flipping the refusal path to `exit 0` is caught by **four** independent
assertions.

### 1.2 Check II — the starttime was SCRUBBED, not canonical

The BSD branch built its value by **folding colons and spaces to `-`**. That is exactly the
lossy character cleaning the review forbids: two different start times can fold to the same
token, so an identity meant to **prevent** collisions could **create** one — and it could not
be parsed back into a time.

**Now canonical and self-describing, by conversion rather than cleaning:**

| platform | format | source |
|---|---|---|
| Linux (VM24) | **`lin-<ticks>`** | field 22 of `/proc/<pid>/stat`, split after the **LAST `)`** — the comm field is parenthesised and may contain spaces and parens |
| BSD/macOS | **`bsd-<epoch>`** | `ps -o lstart=` **converted** by `date(1)` to seconds since the epoch |

The prefix names **which clock** the number is on, so the two are never compared as the same
quantity, and either can be re-parsed. Verified round-trip: `bsd-1788135239` →
`Mon Aug 31 08:13:59 2026`.

**Tested:** shape, that nothing was folded, that no colon survives *by deletion*, that two
reads agree — plus PID reuse, wrong pid, malformed starttime, process vanished, and the
observer's own pid.

#### 1.2a Linux `lin-<ticks>` — the path VM24 will actually take, evidenced

Exercised **offline** against synthetic `/proc/<pid>/stat` lines (the repository was not
modified; the parsing expression was mirrored and driven directly):

| comm field | parsed |
|---|---|
| `bash` | `lin-131297318` |
| `my proc name` — **spaces** | `lin-999111` |
| `weird(name)` — **parens** | `lin-424242` |
| `a (b) c` — **spaces AND parens** | `lin-777777` |

**The decisive comparison:** on the spaces case a naive `awk '{print $22}'` returns **`200`**
— the wrong field — while the split after the **last `)`** returns **`lin-999111`**. That is
the defect this campaign already fixed once in `production_stop.sh`, and it is why the split
is written the way it is.

**Fail-closed, verified:** no `)` at all → refuse; fewer than 20 trailing fields → refuse; a
literal `0` starttime → refuse. In every refusal the function returns non-zero and prints
**nothing**, so there is no empty string to mistake for a reading.

#### 1.2b Precision limits — stated, and measured

| platform | granularity | measured |
|---|---|---|
| **Linux** `lin-<ticks>` | `1/CLK_TCK` s — **typically 10 ms** at 100 Hz | VM24's path |
| **BSD** `bsd-<epoch>` | **whole seconds** — `ps -o lstart=` publishes no sub-second field | two processes started in the same second produce the **same** token |

**Measured, not assumed:** two processes launched together returned the identical
`bsd-1788141903`.

> ### The BSD fallback is a LOCAL-TOOLING LIMITATION, and is not claimed as protection
>
> **`bsd-<epoch>` has whole-second precision and does NOT provide complete same-second
> PID-reuse protection.** `(pid, starttime)` separates the two processes measured above only
> because their **pids** differ; a pid recycled **within one second** on a BSD host would not
> be distinguished.
>
> **No cross-platform full PID-reuse protection is claimed.** The guarantee is stated per
> platform:
>
> | platform | claim |
> |---|---|
> | **Linux — `lin-<ticks>`** | ~10 ms granularity; same-second pid reuse **is** distinguished |
> | **BSD — `bsd-<epoch>`** | whole-second; same-second pid reuse **is NOT** distinguished — a **documented local-tooling limitation** |
>
> **VM24 is Linux and MUST use the `lin-<ticks>` path.** The BSD branch exists only for the
> development machine where the offline batches run.

### 1.3 Two defects in my own new tests

`grep -c` **prints `0` and exits 1** when nothing matches, so `grep -c … || echo 0` emitted
**two** zeros. And a force/skip check matched the **comments** that exist to record that no
such flag is offered. Both scoped correctly.

`test_sentinel.sh` is now **77 assertions**.

---

## 2. Subject and provenance — re-derived at the final subject

```
subject   19aabf02860955b0e228918e451258341c6b49fd
archive   52fe5806e421f693e0387be53cddff7d177a032e249884d00e2f32d6a373794f
files     253
file-list 7b179cdc5620b57d29a409da6c3421d32337628ea54b89800228acf36ab11c33
```

**Superseded, in order, NOT back-filled:** `ccfca934` → `9269dacb` → `fae5a417` →
`8bb7f2c8` → **`19aabf02`**. Their batches are **records, not evidence** for this subject, and
the void `5ae8f49` set stays quarantined.

### 2.1 Files this run depends on, by digest AT THIS SUBJECT

All **re-derived at this subject** for this final request:

```
7f8e430b749b4a03f127788f7a57cbbe57d1ebb0b0905a02e494a25117cb4d02  deploy/staging_execute.sh
8c023ab6f0820a384ed6ed304b517bf0df57ac50e812e990935a11d391a54bc4  deploy/staging_bootstrap.sh
7f4da8d76ededc424c748d84e15b05750b3b1cb7fdb1b69fe8e4a47a217f7bea  deploy/production_app.sh
54bdc96b5d630e4e8fbc9168aa78188dcb7c10ff5617ba77d57231cb73fd7679  deploy/make_staging_override.js
d1e5630143fee2bc62fbdfc4df68b8dfeb57eb8dce6d529d11e5197013dd25ed  deploy/production_stop.sh
cbe799426cddabd7437839ef13e1319658f2fda0c14470409ab94e34336d6c8b  bench/contract_cases.py
```

**The synthetic-mode files, unchanged and pinned so their preservation is checkable:**

```
cf121f7f41e15cd9a381d461772e2bc4a8b58281f7ba341baef14e9d3d5f69f1  deploy/make_staging_store.py
ab256716c1a919b6322425db3ddba36131d0756bb3ac196e541ab25bd9c4ccd6  deploy/start_staging.sh
```

**The batch-provenance machinery this subject's evidence rests on:**

```
0cea4aa158f431142e4118edd52bea7cc8134292a129c6b9160c7a6b332af679  scripts/lib_sentinel.sh
89e0256d01a6fda8b95d37e8651a4f44cabbe4af4f61388fd36188c1c94f44de  scripts/run_batches.sh
```

### 2.2 Three serial batches at this subject

| batch | HEAD, as the batch recorded it | tracked dirty | untracked | suites | non-zero | assertions | exit |
|---|---|---|---|---|---|---|---|
| 1 | `19aabf02…` | **0** | 1 | **55** | **0** | **4655** | **0** |
| 2 | `19aabf02…` | **0** | 1 | **55** | **0** | **4655** | **0** |
| 3 | `19aabf02…` | **0** | 1 | **55** | **0** | **4655** | **0** |

**All 55 result lines identical** across 1↔2, 1↔3, 2↔3.

**The sentinel, and its verification by the library rather than by eye:**

```
WOA23_BATCH_COMPLETE subject=19aabf02… label=s19aabf0 token=s19aabf0-80898-bsd-1788135508 batches=3 nonzero=0

correct subject + label + token  ->  0   accepted
superseded 8bb7f2c8              ->  4   subject mismatch
superseded fae5a417              ->  4   subject mismatch
superseded 9269dacb              ->  4   subject mismatch
superseded ccfca934              ->  4   subject mismatch
```

### 2.3 Assertion-count reconciliation: 4634 (`8bb7f2c8`) vs 4655 (`19aabf02`)

**Two figures exist in this campaign's records and BOTH are correct — they belong to
DIFFERENT SUBJECTS.** Neither is a miscount, and no subject carries both.

| subject | test_sentinel.sh | inline total | + `test_staging_store.py` | **total** |
|---|---|---|---|---|
| `8bb7f2c8` | **56** | 4610 | 24 | **4634** |
| **`19aabf02`** | **77** | 4631 | 24 | **4655** |

**Summed from the raw batch logs**, all three batches of this subject independently:

```
batch1: 55 result lines; 54 carry a count summing to 4631; + test_staging_store.py 24 = 4655
batch2: 55 result lines; 54 carry a count summing to 4631; + test_staging_store.py 24 = 4655
batch3: 55 result lines; 54 carry a count summing to 4631; + test_staging_store.py 24 = 4655
```

**Where the 21 went — a suite-by-suite diff of the two subjects' batch-1 logs:**

| | |
|---|---|
| suites present in one subject but not the other | **0** |
| suites whose count differs | **exactly one — `test_sentinel.sh`, 56 → 77, delta +21** |
| every other suite | **identical count in both** |

**The 21 are the assertions added by the last fix round**: §11 (the driver's exit code on a
refused sentinel — positive control, failing batch, postcondition failure, partial run, the
no-escape-hatch checks) and §12 (the canonical starttime). **Nothing was previously
under-counted**; the suite grew.

**One measurement error of my own, recorded because it nearly entered this table.** My first
comparison helper matched only the wording `assertions` as a bare word, so it missed
`test_column_contract.py`, whose line reads `46 tests, 146 assertions, 0 failed`. It
under-counted **both** subjects by 146 (4464 and 4485). The delta was unaffected, but the
absolute figures were wrong; the extractor now accepts every wording, and the numbers above
are the corrected ones.

**Only `test_staging_store.py` carries no inline count** — its result line ends on a caveat —
and it is read from its own stdout. That is the one suite requiring special handling, in both
subjects.

**The token now carries the canonical starttime** (`bsd-1788135508`), so the run is
identified by `(pid, starttime)` and not by a pid alone.

`untracked=1` is `dev2026/.venv`, the **external local helper**. `tracked_dirty=0` stands;
**"pristine" is not claimed**.

---

## 3. Identity — rechecked for this final request

| | value | subject tree | ledger | repo@`HEAD` |
|---|---|---|---|---|
| **label** | **`dep3h`** | **0** | **0** | **only this request** |
| **port** | **`19259`** | **0** | **0** | **only this request** |
| app name | `woa23-dep3h-candidate` | **0** | **0** | **only this request** |
| staging root | `~/woa23-dep3h` | — | — | — |
| workdir | `~/woa23-dep3h-work` | — | — | — |
| `PM2_HOME` | `~/woa23-dep3h-pm2` | — | — | — |
| tmpdir | `~/tmp-dep3h` | — | — | — |

### 3.1 The HEAD check, stated exactly rather than glossed

**`dep3h` and `19259` are absent from the subject tree and from the ledger.** At `HEAD` they
occur in **exactly one file — THIS request**, and in nothing else:

```
git grep -l dep3h HEAD  ->  runs/d3/D3-execution-request-19aabf0.md
git grep -l 19259 HEAD  ->  runs/d3/D3-execution-request-19aabf0.md
```

**That is the intended and only correct place for them.** A request must name its identity
somewhere, and `runs/d3/` is outside `git archive <sha> dev2026`, so naming it here does
**not** put it in the subject.

**This request is FINALISED IN PLACE rather than reissued as a new document.** Writing a
fresh file naming `dep3h` again would make the identity appear in a second committed
document, which is what turned `dep3a`, `dep3c`, `dep3d` and `dep3f` into unusable pairs:
each was named in a request that was then **superseded**, leaving the identity at `HEAD`
attached to a plan that no longer applied.

> **The distinction, offered for the PI to rule on rather than assumed:** an identity in the
> **current** request for the **current** subject is *allocated*, not *spent*. An identity in
> a **superseded** request is stranded. Both look identical to a plain "absent from HEAD"
> test. **I have not relaxed the rule** — I have kept `dep3h` in the single document that
> already carried it, so no new occurrence is created.

**Never bound, never executed:** `19259` has never been listened on, and no `dep3h` path has
ever existed on VM24.

**Rejected candidate, recorded so the choice is visibly a selection:** `rso1`/`19249` —
`19249` occurs at `HEAD`. Same trap as `19113`, `19136`, `19061`, `19081`, `19173`, `19217`.

**Also verified clean at selection time, unused:** `dep3j`/`19267`, `dep3k`/`19273`.

**At execution, re-checked live:** the ledger at the authorised subject, `ss -ltn` showing
`19259` unbound, and every identity path absent. **Any disagreement stops the run.**

---

## 4. Store — real, read-only, fail closed

**`--store-mode real-readonly --real-store /home/odbadmin/python/woa23/data`.** Synthetic
mode is the **default and unchanged**; real mode must be **named**; `--real-store` in
synthetic mode is **refused, not ignored**.

| # | verified before anything proceeds |
|---|---|
| 0 | GNU `find -printf` and `stat -c` available — else the guards would return 0 by failing |
| 1 | the path equals **one authorised literal**, not a pattern |
| 2 | exists, is a directory, **not itself a symlink** that could be re-pointed after the check |
| 3 | **realpath exactly equals** the authorised production store |
| 4 | **not owned** by uid 994 — non-ownership, not mode bits, is the protection |
| 5 | **not writable**, readable, traversable — `access(2)`, which **honours ACLs** |
| 6–7 | **0** writable paths beneath; **0** writable ancestors to `/` |
| 8–9 | **0** symlinks resolving outside; **0** unreadable files, **0** untraversable dirs |
| 10–11 | readable anchor; staging symlink resolves to the authorised store |
| 12 | metadata fingerprint, **metadata-only**, Stage C / D-1 method |

**No permission change, ownership change, directory creation or removal, deletion or write
touches the store — not even a write probe, because probing by writing is a write.** The only
filesystem write is the staging `ln -s`, inside the staging root.

**Also re-confirmed at preflight:** GID membership, ACLs on the interpreter tree and the
store, **world-writable counted separately from group-writable**, and symlink boundaries.

---

## 5. Runtime and process

| | |
|---|---|
| module | **`api.app:app`** — a literal in `production_app.sh`, not env, not config |
| `PM2_HOME` / venv | **isolated**, this run's own |
| interpreter | **CPython 3.11.14** (QUALIFIED PROVISIONING COMPLETE) |
| `UV_OFFLINE` / `UV_PYTHON_DOWNLOADS` | **1** / **never** |
| sync | `uv sync --locked --offline` — `--frozen` is **mutually exclusive** with `--locked` |
| **`PYTHONDONTWRITEBYTECODE=1`** | **exported by the spawning shell**, never in the config (the generator refuses unexpected keys, which would change the subject). **Verified in the master AND every worker** from `/proc/<pid>/environ`; absent from any worker is a **finding** |
| workers | **2** |
| TLS | **OFF, and reported as UNVALIDATED.** Key/cert verified **ABSENT** from every staging process; **production credentials never enter a staging process**. D-3 does **not** rehearse TLS and **no TLS claim of any kind** may be drawn from it — the gap is **stated, not closed** |
| per process | uid **994** (real, effective, saved, fs), `PM2_HOME`, argv, port, **no `--reload`** |

**No fallback to system 3.12.3 or production 3.11.4, no lock edit, no relaxed constraint, no
network, no alternate index.**

**Interpreter tree** compared against the stabilized baseline by **entry count, ownership and
mode** — never a whole-tree digest. Only the **six enumerated** cache paths are permitted.

---

## 6. Cases, evidence, retention

**The fixed ten**, in order, from `bench/contract_cases.py` (`cbe79942…6c8b`) **in the subject
archive**, never retyped. **One attempt each. No retry. No request outside the ten.**

Retained per case: UTC timestamp, full URL and parameters, HTTP status, **body in full**,
size, elapsed time, body SHA-256.

**Process inventory** before and after, observer excluded **by PID/PGID** with a
self-agreeing retake. **Any unexpected survivor ⇒ INCOMPLETE**, evidence retained, **nothing
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
| A11 validation | **no** — **A11 is not a gate**; a marker delta is never a request count |

| disclosed with any result | |
|---|---|
| interpreter | **3.11.14** vs production **3.11.4** — the difference **still exists** |
| package set | resolved from this `uv.lock`, **not** production's — it **may affect results** |
| attribution | **no response difference may be attributed to the API code alone**; a *matching* response is not proof of equivalence either |
| store identity | **metadata-only** |
| C1 / C2 | **not re-run, not back-filled** |

---

## 7a. Post-run reconciliation — read-only, nothing killed or cleaned

**Confirmed after the batches finished, without re-running anything.**

| | |
|---|---|
| driver pid / starttime | `80898` / `bsd-1788135508` — **gone**, `runid_alive` = **6** (gone, **not 8/pid-reuse**) |
| each batch's own final record | present, complete, **ends on its own `TOTAL` line** — no truncation, no partial output |
| every batch's `git head` | **`19aabf02…`**, tracked dirty **0**, untracked **1** |
| driver-recorded exit per batch | **0, 0, 0**; all six pre/post HEAD checks OK |
| success sentinel | **exactly one**, token **matches the one derived independently from the runid file**, `sentinel_verify` = **0** |
| line-by-line | 1↔2, 1↔3, 2↔3 **IDENTICAL**; **0** non-zero exit lines |

**"No process" was NOT treated as success.** The driver being gone says only that it exited;
the verdict rests on the three self-recorded final records and the verified sentinel.

**A log-size difference, attributed rather than left open:** the three batch logs are 33 780 /
15 836 / 14 042 bytes. The delta is entirely the **process-snapshot notes** (102 / 73 / 68
detail lines), which vary with what else the machine was doing. They are observations, not
assertions, and all three `TOTAL` lines agree.

### 7a.1 Survivors and artefacts — RECORDED ONLY

| | |
|---|---|
| survivors from **this** run | **15** processes with cwd inside `wt-19aabf0/dev2026` — five per batch, the known `test_requests.sh` leak |
| pre-existing leaked servers | **1143** `python3.9` processes, **18** stale `arm.py` |
| worktrees | **8** registered, including `wt-c731213` marked **prunable** |
| batch roots | three temp dirs retained under `/var/folders/.../woa23-suites-{oke6Cd,uY3noS,rdUUo7}` |

**Nothing was killed, pruned or cleaned.** Cleanup remains separately authorised work.

**Not mine, recorded for completeness:** pid `95967`, `ssh odbadmin@192.168.2.29 -X` — a
**different host** from VM24 (`.24`). Untouched.

**A counting caveat on my own scan:** a time-window filter found only 10 of the 15 survivors,
because one batch's group fell outside the window bound. **The cwd scan is the authoritative
one at 15**, and the weaker figure is not carried forward.

---

## 7b. What this request does NOT do

| | |
|---|---|
| VM24 | **not contacted** to prepare it |
| staging root / workdir / `PM2_HOME` / tmpdir | **not created** |
| `pm2 start` | **not run**; no PM2 daemon exists for this identity |
| the production store | **not accessed** |
| the ten cases | **not issued** |
| cleanup | **not performed** — survivors, worktrees and batch roots left as found |
| C1 / C2 | **not re-run, not back-filled** |
| `dep3h` / `19259` | **not consumed** — never bound, never listened on |

---

## 8. Status

| | |
|---|---|
| VM24 | **not contacted** |
| D-3 | **not executed**; no staging, PM2, store, workdir or port |
| production | **untouched** |
| subject | **`19aabf02…`** — no D-3 request, no execution identity |
| batches | **3 clean, identical, 55 suites, 4655 assertions, 0 non-zero**, sentinel verified |
| identity | **`dep3h` / `19259`**, first-use, named only here |
| `dep3f` / `19229` | **not consumed** — superseded by the HEAD rule, §3.1 |

### 8.1 The ask

**This is the FINAL offline request and provenance package, submitted for review.**

**It is not a request to execute.** No VM24 contact will occur, and no staging, `pm2 start`,
store access or case request will be made, until the PI issues an explicit execution
authorisation naming this subject and this identity.

**If anything in reviewing it requires an executable or harness change, work STOPS** — the
subject is superseded, a fresh archive, file-list and three batches are cut, and the identity
is re-selected. That has now happened four times, and each occurrence is recorded rather than
smoothed over.
