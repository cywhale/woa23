# Offline package — subject `9269dac`, sentinel fix, and a post-subject identity

**Offline only. No VM24 contact. D-3 NOT executed. No staging, PM2, store, workdir or port
created. Production untouched. C1/C2 not re-run.**

**This document lives in `runs/d3/`, OUTSIDE `git archive <sha> dev2026`, and is committed
AFTER the subject was cut.** That is what lets it name an identity without burning one.

---

## 1. The sentinel defect — not cosmetic, and I was wrong to call it that

I previously described the stale sentinel as cosmetic. **It was a provenance and completion
accounting defect**, and tracing every writer, reader and watcher found **three** faults, not
one.

**The only writer** was the scratchpad driver's last line,
`echo "D3_6CE915E_BATCHES_DONE"`:

| # | fault | consequence |
|---|---|---|
| 1 | the subject name was **hardcoded** | a driver copied for a new subject announced completion **under the old subject's name** — a false provenance claim |
| 2 | it sat after the loop; the driver captured `rc=$?`, **printed it, and never tested it** | **a run containing a failing batch still announced success** |
| 3 | the **watcher** matched a fuzzy pattern | it could match **its own command line** — a false positive this campaign had already hit once |

**Fault 2 is the serious one.** Verified in the source before fixing: `rc` appears only in an
assignment and an `echo`; there is no `if` on it anywhere.

---

## 2. What replaced it

### 2.1 `dev2026/scripts/lib_sentinel.sh`

A sentinel is now one line carrying **subject, label, run token and counts**:

```
WOA23_BATCH_COMPLETE subject=<40 hex> label=<run label> token=<run token> batches=<n> nonzero=0
```

**`sentinel_write` refuses** unless: the subject is a caller-supplied 40-hex SHA; label and
token are plain identifiers; **every** batch completed; the non-zero total is **0**;
postconditions passed; and **no sentinel already exists** in the file.

**There is no partial-success form and no write-anyway flag.** The **absence** of a sentinel
is how an incomplete run reports itself.

**`sentinel_verify` returns distinct codes**, so a caller cannot collapse them into
"probably fine":

| code | meaning |
|---|---|
| 0 | accepted |
| 3 | **missing** — absent file, empty file, or a log with no sentinel |
| 4 | **subject mismatch** — which is exactly what an *old subject's* sentinel is |
| 5 | label mismatch |
| 6 | **duplicate** — two sentinels are ambiguous, and ambiguity reads as success |
| 7 | malformed |
| 8 | token mismatch |

### 2.2 `dev2026/scripts/run_batches.sh`

The driver now lives **in the repository**, beside its library and tests, rather than in a
scratch file nobody could check.

| requirement | how |
|---|---|
| subject not hardcoded | `--subject` is **required**, validated as 40-hex |
| supplied or freshly obtained | passed by the caller; **never** defaulted, **never** read back from a previous run's artefacts |
| bound to subject **and** run | subject + label + per-run token in the sentinel |
| worktree agreement | HEAD checked against the subject **before and after every batch** |
| exit status **tested** | `if [ "$rc" -ne 0 ]` — the line the old driver did not have |
| watcher by identity | the driver writes **its own PID**; the watcher waits on that PID, never a name pattern |

### 2.3 `dev2026/scripts/test_sentinel.sh` — 34 assertions

Every case required by the review:

| required | covered |
|---|---|
| old sentinel rejected | §2 — an old subject's sentinel fails against the new subject, **and is still valid for its own** |
| wrong subject rejected | §2, code 4 |
| wrong batch label rejected | §3, code 5 (and wrong **token**, code 8) |
| missing sentinel = not complete | §4 — absent, empty, and noisy-but-sentinel-free logs |
| failed run writes no sentinel | §5 — failing, **partial** (2 of 3), and **uncertain postconditions** each write nothing |
| watcher cannot catch itself | §8 — demonstrated, see below |
| correct sentinel accepted **once** | §1 — a duplicate write is refused and the file still holds exactly one |

**§8 demonstrates the hazard rather than asserting it.** A decoy process carrying the tag in
its argv **is** matched by a fuzzy pattern search, while a PID wait is unaffected.

**Two earlier decoy forms proved nothing, and the working form is documented so it is not
simplified back:** `sleep 30 <tag>` exits immediately on the extra argument, and
`sh -c "sleep 30 # <tag>"` is **exec-optimised** so argv collapses to `sleep 30` and the tag
disappears. A two-command body defeats that.

---

## 3. Store mode — the interface re-confirmed, no ambiguity

| required | status |
|---|---|
| synthetic is the default | **yes** — `STORE_MODE=synthetic`, asserted |
| real mode must be explicit | **yes** — `--store-mode real-readonly` **and** `--real-store` |
| synthetic + real flag refused | **yes** — refused, **not ignored** |
| production store untouched unless real mode | **yes** — the synthetic branch never names it |
| real mode builds no synthetic store | **yes** — builder not called |
| no chmod / chown / directory creation or removal / deletion / write | **yes** — the only write is the staging `ln -s` |
| realpath exactly equals the authorised store | **yes**, in the executor **and** the generator |
| ACLs, ancestors, symlink boundary, uid-994 unwritability fail closed | **yes** |

`test_real_store_mode.sh` (50 assertions) carries these, and two mutation tests show it can
fail.

---

## 4. Subject, provenance and three batches

```
subject   9269dacb55a8abf63c56d538ed998a9e9a11d51d
archive   fc4c8a9e8a6a2f020890f4cd0fa08e752d2a004dfd004e6e2a9de8fd8272ec3d
files     253
file-list f81d93a7ad76807dcb9cc31782bedf38d1020b55c89ec183f5f59d05a3bb94be
```

| batch | HEAD, as the batch recorded it | tracked dirty | untracked | suites | non-zero | assertions | exit |
|---|---|---|---|---|---|---|---|
| 1 | `9269dacb…` | **0** | 1 | **55** | **0** | **4612** | **0** |
| 2 | `9269dacb…` | **0** | 1 | **55** | **0** | **4612** | **0** |
| 3 | `9269dacb…` | **0** | 1 | **55** | **0** | **4612** | **0** |

**All 55 result lines identical** across 1↔2, 1↔3 and 2↔3. Suites 54 → 55 and assertions
4578 → 4612, both accounted for by `test_sentinel.sh` at **34**.

**The sentinel this run wrote, and its verification by the library rather than by eye:**

```
WOA23_BATCH_COMPLETE subject=9269dacb… label=s9269dac token=run-67542 batches=3 nonzero=0

verify  correct subject + label + token        -> 0  accepted
verify  SUPERSEDED subject ccfca934            -> 4  subject mismatch
verify  wrong label                            -> 5
verify  wrong token                            -> 8
```

**The superseded subject's sentinel is rejected by the new mechanism** — the original defect,
now demonstrated against real artefacts.

`untracked=1` is `dev2026/.venv`, the **external local helper**. `tracked_dirty=0` stands;
**"pristine" is not claimed**.

### 4.1 The void set is quarantined

The earlier `5ae8f49` batch set remains **VOID** and is **not merged with, averaged into, or
cited alongside** these batches. The `ccfca934` batches are **superseded**, retained as a
record, and **not back-filled**.

---

## 5. Identity — the burn removed structurally

**Every previous candidate was burned by being written into a subject tree.** The fix is
structural, not a matter of remembering:

| | |
|---|---|
| all **11** D-3 execution documents | moved to **`runs/d3/`**, outside `git archive <sha> dev2026` |
| `dev2026/specs/023` | names **no** label, port, root, workdir, `PM2_HOME` or store identity |
| identity selection | happens **after** the subject is cut, in a **post-subject** commit — this file |

**Verified in the new subject:** `dep3a`, `dep3b`, `dep3c`, `19161`, `19187` — **0
occurrences each** anywhere under `dev2026`. **0** D-3 request documents in the subject.

### 5.1 The proposed first-use identity

| | value | subject tree | ledger | repo@`HEAD` |
|---|---|---|---|---|
| **label** | **`dep3d`** | **0** | **0** | **0** |
| **port** | **`19211`** | **0** | **0** | **0** |
| app name | `woa23-dep3d-candidate` | **0** | **0** | **0** |
| staging root | `~/woa23-dep3d` | — | — | — |
| workdir | `~/woa23-dep3d-work` | — | — | — |
| `PM2_HOME` | `~/woa23-dep3d-pm2` | — | — | — |
| tmpdir | `~/tmp-dep3d` | — | — | — |

**A rejected candidate, recorded so the choice is visibly a selection:** `rsr1`/**`19217`**
was considered and **rejected** — `19217` occurs at `HEAD` in `dev/sim_woa23_api01.ipynb`.
That is the coincidental-substring trap that already cost `19113`, `19136`, `19061`, `19081`
and `19173`.

**`dep3c`/`19187` is NOT used and NOT consumed**, as required — it was never written into a
subject and is left available.

---

## 6. Result classification — unchanged

| | |
|---|---|
| `ccfca934` | **superseded as an executable subject**, retained as a record, **not back-filled** |
| D-3 | a **candidate deployment rehearsal using the real production store** |
| never | production equivalence · deployment PASS · data-path correctness PASS · TLS validation · A11 validation |
| retained limits | Python **3.11.14** vs production **3.11.4** and a divergent package set — **no response difference attributable to the API code alone** · **TLS off** · the **fixed ten cases** · store identity **metadata-only** |
| this offline fix | **does not authorise D-3** |

---

## 7. Status

| | |
|---|---|
| VM24 | **not contacted** |
| D-3 | **not executed**; no staging, PM2, store, workdir or port created |
| production | **untouched** |
| C1 / C2 | **not re-run** |
| subject | **`9269dacb…`** — no D-3 request, no execution identity |
| batches | **3 clean, identical, 55 suites, 4612 assertions, 0 non-zero** |
| sentinel | **subject-bound, verified, and the superseded subject's is rejected** |
| proposed identity | **`dep3d` / `19211`**, first-use, named only here |
| `dep3c` / `19187` | **not consumed** |

**Awaiting the PI's review.** No D-3 request is submitted and none will be prepared before
that review.
