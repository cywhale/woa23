# 023 — real-production-store read-only mode: **offline package**

**Prepared entirely OFFLINE. No VM24 contact. D-3 NOT executed and NOT submitted for
execution authorisation.**

Option A, taken in principle: the execution entry gains an **explicit** real-store read-only
mode, so the D-3 blocker can be closed without disabling any guard.

---

## 1. What changed, and what deliberately did not

| file | change |
|---|---|
| `deploy/staging_execute.sh` | **new** `--store-mode synthetic\|real-readonly` (+ `--real-store`), and a real branch that builds nothing |
| `deploy/make_staging_override.js` | the same explicit mode, and its production-store guard made **realpath-based** |
| `scripts/test_real_store_mode.sh` | **new** suite, **50 assertions**, source **and** functional |
| `deploy/start_staging.sh` | **UNTOUCHED** — its protection is preserved, not disabled |
| `deploy/make_staging_store.py` | **UNTOUCHED** — the synthetic fixture still exists and is still the default |

### 1.1 The synthetic mode is unchanged (requirement 1)

**`STORE_MODE` defaults to `synthetic`.** Every existing caller omits the flag, and absence
means exactly what it always meant. The synthetic branch still calls the builder, still
asserts **72 files**, still runs `chmod -R a-w`, and still refuses on the write probe — all
four are asserted by the new suite so they cannot be lost in a later edit.

### 1.2 The real store is NAMED, never inferred (requirement 2)

**Two flags, and the pairing is enforced in both directions:**

- `--store-mode real-readonly` **requires** `--real-store`;
- `--real-store` **without** real mode is **REFUSED**, not ignored — *"a real store named in a
  synthetic run is a command line that does not mean what it says."*

**No path is silently reinterpreted.** The real store cannot be reached by omission, by
default, or by a path that happens to resolve there.

---

## 2. Requirement 3 — every synthetic assumption is gone from the real branch

| removed | |
|---|---|
| `make_staging_store.py` | not called |
| `[ "$STORE_FILES" = 72 ]` | absent — 72 describes the fixture, not real data |
| `chmod -R a-w` | absent |
| `chown` / directory creation / removal / deletion | absent |
| the write probe (`touch`) | absent — **probing by writing is a write** |

**The only filesystem write in the real branch is the `ln -s` that creates the staging
symlink, and it lands inside the staging root.** The production store is opened for reading
and nothing else. The suite asserts each of these by counting **non-comment** occurrences.

---

## 3. Requirement 4 — what real mode verifies before anything proceeds

| # | check |
|---|---|
| 0 | **GNU `find -printf` and `stat -c` are available** — see §5.2 |
| 1 | the store path equals **one authorised literal**, not a pattern |
| 2 | exists, is a directory, and is **not itself a symlink** that could be re-pointed after the check |
| 3 | **realpath** equals the literal |
| 4 | **not owned** by the running account |
| 5 | **not writable**, and readable and traversable — `test -w` uses `access(2)`, which **honours ACLs** |
| 6 | **0** writable paths beneath it |
| 7 | **0** writable ancestors, walked to `/` |
| 8 | **0** symlinks resolving outside it |
| 9 | **0** unreadable files, **0** untraversable directories |
| 10 | a **readable** anchor at `1_degree/annual/TS/.zgroup` |
| 11 | the staging symlink is created and **resolves to the authorised store** |
| 12 | metadata fingerprint by the **Stage C / D-1 method**, labelled **metadata-only** |

---

## 4. Requirement 6 — the existing protection is preserved, and one weakness found

**`start_staging.sh` is untouched.** Its `WOA23_PRODUCTION_STORE` guard still refuses a
staging store resolving inside production's, and **nothing disables it globally**.

### 4.1 A latent weakness in the generator, found while doing this

> **The generator's production-store guard was purely LEXICAL.**

`path.resolve` collapses `..` but **does not follow symlinks**, and the generator contained
**no `realpath` or `readlink` at all**. A symlink inside the staging root pointing at
production's store would have **sailed past the guard unnoticed** — which is exactly the
shape real mode would have taken had it been implemented by pointing `--store` at a link and
saying nothing.

**Fixed in both directions:**

- **synthetic mode** now tests the **resolved** path too, so a symlink into production is
  refused rather than quietly accepted;
- **real mode** does not *disable* the guard — it **inverts** it into an exact realpath
  equality (`realStore !== PROD_STORE` ⇒ refuse) plus a writability refusal, so the mode
  cannot be used to reach any other path.

**This is why the mode had to be explicit rather than permissive.** Reaching the real store
by slipping past a guard that cannot see symlinks would have been "working by accident".

---

## 5. Requirement 5 — the tests, and two defects of mine they exposed

`scripts/test_real_store_mode.sh` — **50 assertions**, in eight sections. It gives **source**
evidence (the real branch contains no mutating verb) **and functional** evidence (a real
read-only fixture is unchanged after the mode's preconditions run), because a source grep can
be defeated by an indirection and a before/after comparison cannot.

### 5.1 The suite's first version was VACUOUS — mine, found and fixed

It used `find -printf` and `find -writable`. **BSD `find` rejects both**, the error went to
`/dev/null`, and the fingerprint became **the digest of empty input** — identical before and
after, so *"the store is unchanged"* passed **while measuring nothing**.

**Fixed:** portable `stat`-based helpers, per-path kernel writability, and an explicit
assertion that **the fingerprint is not the digest of empty input** before any comparison is
trusted.

### 5.2 The same defect was in the PRODUCTION branch — also mine, also fixed

The real branch's guards use the same GNU-only predicates with stderr suppressed. On a BSD
userland **every one would have returned 0 and passed while testing nothing.**

**The branch now refuses to run** unless GNU `find -printf` and `stat -c` are available:

> *"A guard that cannot run must stop the run, not pass it."*

### 5.3 Mutation tests — the suite is shown able to fail

| mutation | caught |
|---|---|
| a `chmod` injected into the real branch | **yes** — *real branch contains no chmod* |
| the generator reverted to a lexical-only guard | **yes** — *synthetic mode tests the RESOLVED path too* |

### 5.4 A finding recorded instead of a false assertion

**Making a tree unwritable does not stop the file's OWNER** — an owner may always change the
mode back. On the suite's own fixture, owned by the account running it, the `chmod`
**succeeds**, and asserting otherwise would assert something false.

**Non-ownership — not the mode bits — is what makes the production store unwritable by
uid 994.** That is precisely why check 4 of §3 exists, and the suite asserts the branch
refuses a store owned by the running account.

---

## 6. Requirement 8 — execution identities are NOT named in this subject

**No execution identity appears anywhere in `dev2026/` at this subject, and none is proposed
here.** The previous candidate pair was burned by being written into a subject tree, and the
fix is structural rather than a matter of care:

| | |
|---|---|
| **D-3 execution documents** | moved **out of `dev2026/`** to `runs/d3/`, so they are outside `git archive <sha> dev2026` entirely |
| **identity selection** | happens **after** the subject is cut, in a **post-subject** commit, and never in `dev2026/` |
| this document | names **no** label, port, root, workdir, `PM2_HOME` or store identity |

**Why the move rather than more discipline:** the subject archive is
`git archive <sha> dev2026`. Any request document living under `dev2026/specs/` is *in* the
subject by construction, so an identity named in one is burned the moment the subject is cut
— no matter how careful the author intended to be. Moving execution records outside that
path removes the failure mode instead of relying on remembering it.

## 7. Requirement 7 — D-3's limitations are unchanged

| | |
|---|---|
| interpreter | **3.11.14** vs production's **3.11.4**, and a resolved package set from this `uv.lock` |
| attribution | **no response difference may be attributed to the API code alone** |
| TLS | **off**; production credentials absent |
| cases | the **fixed ten**, one attempt each |
| environment | `UV_OFFLINE=1`, `UV_PYTHON_DOWNLOADS=never`; no fallback, no lock edit, no network |
| C1 / C2 | **not re-run** |
| wording | **candidate deployment rehearsal using the real production store** — never production equivalence, a deployment PASS, a data-path correctness PASS, TLS validation or A11 validation |

---

## 8. Status

| | |
|---|---|
| VM24 | **not contacted** |
| D-3 | **not executed, not submitted for authorisation** |
| execution identities | **none named in this subject** — see §6 |
| `start_staging.sh` | **untouched** |

**Awaiting the PI's review of this offline package.** No D-3 request will be submitted or
executed before that review.

---

## 9. The new subject and three clean batches (requirement 9)

```
subject   ccfca9340b9aacd894e5e9ebab4791952205c12d
archive   ccce2758e44661fa7f96914c6618597d86218372347d855cc909fb41a23b2117
files     260
file-list 44e244f3624467d740ce3d5af5619cd81fda89ae5bdee10fb2bab82d80cc56f0
```

**Three serial batches, run in a DETACHED WORKTREE pinned to the new subject.** HEAD was
checked by the driver **before and after** each batch; all six checks passed.

| batch | HEAD, as the batch itself recorded it | tracked dirty | untracked | suites | non-zero | assertions | exit |
|---|---|---|---|---|---|---|---|
| 1 | `ccfca9340b9aacd894e5e9ebab4791952205c12d` | **0** | 1 | **54** | **0** | **4578** | **0** |
| 2 | `ccfca9340b9aacd894e5e9ebab4791952205c12d` | **0** | 1 | **54** | **0** | **4578** | **0** |
| 3 | `ccfca9340b9aacd894e5e9ebab4791952205c12d` | **0** | 1 | **54** | **0** | **4578** | **0** |

**All 54 result lines identical** across 1↔2, 1↔3 and 2↔3.

| | |
|---|---|
| suites | **54** — 53 before, plus `test_real_store_mode.sh` |
| assertions | **4578** — 4528 before, plus the new suite's **50** |
| the new suite, in every batch | `exit=0 test_real_store_mode.sh — all passed (50 assertions)` |
| `untracked=1` | `dev2026/.venv`, the **external local helper** (Phase 1 result §A.1d). `tracked_dirty=0` stands; **"pristine" is not claimed** |

**Two recording notes, so neither is discovered later as a surprise:**

- the driver's completion sentinel still prints `D3_6CE915E_BATCHES_DONE`. It was copied
  from the previous driver and the string was not renamed. **Cosmetic** — the HEAD checks in
  the same log are the real evidence, and they name `ccfca93` throughout;
- assertion totals are counted by the corrected method (all wordings, plus
  `test_staging_store.py` read from its own stdout), the one that fixed the earlier 4382
  undercount.

---

## 10. Status — package complete, nothing submitted

| | |
|---|---|
| executable change | **committed at `ccfca93`**, the new subject |
| new subject provenance | archive `ccce2758…2117`, 260 files, file-list `44e244f3…56f0` |
| three batches | **clean, identical, 54 suites, 4578 assertions, 0 non-zero** |
| execution identities | **none in `dev2026/`** — selection happens post-subject (§6) |
| VM24 | **not contacted** |
| D-3 | **not executed, and no D-3 request is submitted** |

**Awaiting the PI's review of this offline package** before any D-3 request is prepared or
submitted.
