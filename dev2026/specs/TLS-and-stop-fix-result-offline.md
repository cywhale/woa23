# Offline result — TLS omission, the stop-parser fix, and the new subject

> **SUPERSEDED IN PART.** The subject named in §4 is `e7243e4`, which is **no longer
> current**: the review then found `production_stop.sh:children_of` reading
> `/proc/<pid>/stat` field 4, and fixing it changed the script B1 validates. The current
> subject is **`cba1329`** — see
> [`b1v2` §7](B1-host-validation-request-b1v2.md). Everything else in this document stands;
> §9 records what changed and why.

**Status: OFFLINE RESULT. No VM24 contact. No staging run. No cleanup. Nothing here is
back-filled into `bs3v1`'s evidence, and nothing here is a host validation.**

Submitted against the five judgements you confirmed and the TLS decision.

---

## 1. Your five judgements — where each one landed

| # | judgement | where it is now enforced |
|---|---|---|
| 1 | the parser fix direction is correct | `production_stop.sh` parses `pm2 jlist` as **JSON by field name**; the awk walk is gone, and its absence is asserted |
| 2 | `pid 0` + status `stopped` → **STOPPED**, exit 0, idempotent | `jlist_resolve` returns `STOPPED`; `test_production_stop.sh`'s idempotency case is green again |
| 3 | `pid 0` with `online`/`errored`/missing status → still **PROBLEM** | four separate assertions, each on its own shape |
| 4 | "cannot determine" is **never NOTFOUND** | asserted over a table of eight bad inputs, plus a source assertion that only `NOTFOUND` and `STOPPED` can exit 0 without stopping — and that there are **exactly two** such branches |
| 5 | the `bs3v1` manual stop is recorded; future wrapper failure must **stop-and-preserve** | spec 014 §2, binding. `bs3v1`'s fallback is written down as **now disallowed**, not quietly dropped |

**Deliverable 2 — parser fix test results.** `test_stop_jlist_parser.sh` **61 assertions**,
`test_production_stop.sh` **33 assertions**, `test_stop_multiworker.sh` unchanged. All
pass in all three batches (§5).

**What this is not.** These are offline suites. The fix has **never run against a real PM2
daemon**, and the one piece of evidence a fixture structurally cannot produce is the
daemon's own `jlist` field order — which is precisely what broke the old parser. That gap
is what [`b1v2`](B1-host-validation-request-b1v2.md) is for — `b1v1` is **withdrawn**,
see §9 — and until it runs, B1 has **no host evidence**.

---

## 2. Deliverable 1 — TLS, implemented as described

Full detail in [spec 014 §1.5](014-staging-tls-paths-and-cleanup-policy.md). In short:
when `WOA23_TLS` is `off`, `WOA23_TLS_KEYFILE` and `WOA23_TLS_CERTFILE` are **not present
at all** — not in the generated config, not in the child's environment, not in argv — and
`production_app.sh` never resolves, reads or opens them.

**Omission alone was never enough, and §2.2 corrects how I described it.** The entry now
**unsets both variables before `pm2 start`**, which is the only point where an inherited
value can actually be removed, and then verifies their absence on the **master and every
worker**.

**Naming, flagged rather than smoothed over.** You said "Option A"; spec 014's table
labels **omission as B** and **blanking as A**. The description was unambiguous, so
omission is what I built. Blanking would have left `production_app.sh` defaulting to the
relative production-shaped path `conf/privkey.pem`, which is the weaker outcome.

### 2.1 Every test you asked for, and its result

| your requirement | assertions | result |
|---|---|---|
| key/cert **absent from the generated config** when TLS off | 6 | pass |
| key/cert **absent from the actual child process environment** | 5 | pass |
| **no `keyfile`/`certfile` in argv** when TLS off | 6 | pass |
| key/cert **still required** when TLS on | 2 behavioural + 8 source | pass |
| **missing/unreadable** key/cert **fails closed** when TLS on | 6 | pass |
| production config's **existing TLS defaults unchanged** | 4 | pass |
| **B3/B5 existing checks not loosened** | 6 | pass |
| **an ancestor's exported paths must not reach the child** | 9 functional + 1 control | pass |
| master **and workers** both verified | 5 | pass |

`scripts/test_tls_off_omission.sh` — **67 assertions**. `test_staging_entry.sh` 125 →
**148**. `test_staging_override.sh` 78 → **81**. `test_production_launcher.sh` **111**,
unchanged. All pass in all three batches.

TLS-on behaviour is asserted in **both** directions, because "off omits the paths" must
never widen into "TLS can be skipped": `${WOA23_TLS:-on}` is intact, so **unset still
means on**; a run with `WOA23_TLS` unset and a missing key **refuses** rather than serving
plain HTTP; both paths are still defaulted, still checked for **readability**, and still
passed through as `--keyfile`/`--certfile`.

### 2.2 Omission is not removal — and the `/proc` check is a DETECTOR, not a remedy

**A correction to how I described this.** An earlier version of this section said the
`/proc` check "closes the hole". It does not. It **detects** a leak and **fails the run**.
Detection after the fact is not removal, and describing it as removal was the same class
of overstated claim this programme exists to refuse.

Three distinct things, now kept apart in the code, the tests and this document:

| | | |
|---|---|---|
| **omission** | the generated config does not carry the paths | necessary, **not sufficient** |
| **PREVENTION** | `staging_execute.sh` **unsets both before `pm2 start`** | **this is the remedy** |
| **detection** | `/proc/<pid>/environ` on master **and every worker** → `INVALID_ENVIRONMENT` | catches a leak; does not prevent one |

**Why prevention has to happen exactly there.** `pm2 start` spawns the God Daemon with the
calling shell's environment, and the daemon hands that to the app. A PM2 config has **no
unset directive**, so nothing the config omits can remove what an ancestor exported. The
only place to break the chain is the shell that spawns pm2.

**TLS on is handled in the same block, deliberately.** The paths are **exported explicitly
from the config** rather than left to whatever was inherited, and a config claiming TLS on
while carrying no key or certificate is refused there instead of being left to the
launcher. In neither mode does an ancestor decide what the service uses.

### 2.3 What the earlier check missed: the workers

The `/proc` check read the **master only**. gunicorn's master forks the workers, and it is
the **workers that serve** — so a leak present in a worker alone would have been reported
clean. That is the same shape as reading the wrong thing and calling it evidence.

The check now covers the master **and every child**, and **zero workers found is a
refusal**, not a pass: a check that covered only the master is not a check on the processes
that serve. An unreadable worker environment is also a refusal — an unreadable environment
is not an absent one.

While writing it I found that `ppid` was being taken as **field 4** of `/proc/<pid>/stat`,
which is wrong whenever `comm` contains a space. The new code reads after the last `)`.

At the time I recorded that **`production_stop.sh`'s `children_of` has the same latent
bug** and deliberately did **not** edit it, on the grounds that this work should not change
the stop path it was not testing. **The review overruled that, correctly:** B1 must not be
validated against code carrying a known defect. It is fixed in `cba1329` — along with the
same defect in `starttime_of`, which I had not reported — and §9 covers it.

### 2.4 The evidence is functional, not a source grep

The fake `pm2` records **its own environment** on `start` — which is exactly what the
daemon would pass down. A case exports the production TLS paths into the entry's
environment (deliberately **not** under `env -i`, which would wipe them and let the test
pass for nothing) and asserts the spawned child received **neither**, with no
`/home/odbadmin` path at all.

A **control** asserts that an exported path *does* reach an ordinary child, so a fixture
that silently failed to set the variables could not masquerade as a pass.

Two of my own assertions were wrong on first writing and were corrected rather than
loosened: the stub captured only `WOA23_TLS*`, which is legitimately **empty** when TLS is
off — so "the key is not in it" was satisfiable by an empty file, and it now captures the
whole environment and proves the capture non-empty; and an `awk` pattern anchored `unset`
at column 0 while the real lines are indented.

### 2.5 A leak is `INVALID_ENVIRONMENT`, not a failed check inside a pass

If either path is present with TLS off, the run yields **no B3/B5 result**, is **not** a
clean PASS, and **state is preserved** — the exporting ancestor must be found before any
re-run. It is not an ordinary environment mismatch, because it means the environment
measured was not the environment intended.

### 2.6 Two pre-existing assertions were updated, not deleted

Both contradicted the new behaviour. Each was replaced by something that preserves what it
protected:

- *"eight keys differ"* → **ten** (three log paths, and two TLS paths **removed**). The
  property was that the diff is **exhaustive and bounded**, so the removals are **counted
  as differences** rather than excused.
- *"the TLS cert paths are production's, unchanged"* → five assertions: key **absent**,
  certificate **absent**, **no** `/home/odbadmin/python/woa23/conf` path anywhere in the
  staging environment, staging still **invents no TLS path of its own**, and
  **production's own TLS defaults are unchanged**.

---

## 3. Deliverable 3 — how the `bs3v1` environment deviation is handled

**It is not closed retroactively, and `bs3v1` is not re-scored.**

| | |
|---|---|
| `bs3v1`'s recorded qualification | **stands, unchanged.** It really did run with production TLS paths in its environment |
| `bs3v1`'s B3/B5 evidence | **unchanged**, with all five qualifications intact. Still staging-only, still not an unconditional or clean PASS |
| new code | **not back-filled** into `bs3v1`'s VM24 evidence in any form |
| the deviation | closed **for future staging runs only**, from `e80c6f1` forward |

No later code changes what already happened. Spec 014 §1.4 is marked **superseded for
future runs** and says so explicitly rather than being edited to look as though the
deviation never stood.

---

## 4. Deliverable 4 — the new subject

A new subject is required because **executable code changed**: `make_staging_override.js`,
`staging_execute.sh`, `production_app.sh`, and the batch runner `run_suites.sh`.

**Two earlier subjects were cut and are superseded.** `fe6d4e2` (226 files, archive
`069d6230…e2d4`) was clean across three batches, and `3334c31` before it was not — see
§5.1. `fe6d4e2` is superseded because the review required the TLS *prevention* work, which
changed executable code again. **Only `e7243e4` below is current**; the earlier two must
not be used for any run.

```
commit           e7243e4dea8b8a9a267a2d3ca297b02092acb697
subject line     fix(staging): PREVENT inherited TLS paths, not merely detect them; verify workers too
archive sha256   bc0cb2ab6515c67a5ba9fbc6075621bb08169fad99723a0a39d261b8c48de5c0
files            227
file-list sha256 baebc9c96a2f32deb05f8dd4c903a4b7820b136d1a80f90bd26ecf73e87167a4
```

`verify_clean_archive.sh` — **all passed (16 assertions)**, plus the tree-only re-check
inside the export (9 assertions). The export is verified as *the tree whose identity was
taken*: file count and file-list digest unchanged, and no bytecode written into it.

**Per-file digests — every file that changed since the previous subject:**

```
0b2e719aecb0fae13ec0271cf03dde9ff8fdd33dea2342bc1cf70481d9d04f60  deploy/make_staging_override.js
7f4da8d76ededc424c748d84e15b05750b3b1cb7fdb1b69fe8e4a47a217f7bea  deploy/production_app.sh
bb90652f2ed7966360c2b5b1150157405dc30090f2f15c200848615b184c5c56  deploy/staging_execute.sh
2161060a34d97de2fc94045989893616a43e397dfdf1ce430763d9a865c11698  scripts/run_suites.sh
3cdf97001f0dda4777f6377735fe4ae02024ce547cbbe5f963ff127c9ee086f1  scripts/test_run_suites.sh
f409bb100ea04daa80fecddf07bd3aff85266b8d794a1906dc8075bfe3959349  scripts/test_staging_entry.sh
675c1ef8faa17121b1b008addac99c86d835d9018938419a0ea6778cb62384e8  scripts/test_staging_override.sh
3496b09e83ecf9c8455dc9876e0995058b2933771e2037ee469f2a4b198c35c3  scripts/test_tls_off_omission.sh
```

The complete 227-line per-file listing is `archive_files.sha256`, whose digest is the
file-list SHA-256 above.

**Unchanged, and verified rather than asserted** — by blob identity between the previous
subject `a046d5ad` and this one:

```
api/query.py                            50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8
deploy/ecosystem.production.config.js   a7e4cb7fcb47e83b8192b225093e5dbe20aad2c673e5fed511b2328ee22410c4
deploy/production_stop.sh               1228eefff54a8cdb81cbc7866f45e68b064de718b8a1d46c9bfeccb7fcb8a773
```

`production_stop.sh` is unchanged **since `a046d5ad`** because the parser fix landed
before it; the digest is given so it can be checked, not so it can be assumed.

The first attempt at this check compared `api/query.py` at the repository root, where no
such file exists — `git diff` on a non-existent path is empty and **exits 0**, so it
"passed" while comparing nothing. The real path is `dev2026/api/query.py`. A vacuous pass
is recorded here rather than quietly replaced, because it is the same failure shape as the
rest of this report: a green result that answered no question.

The previous subject `a046d5ad4b8cda363d409f22d9f14a6dc382304c` **predates the TLS work**
and must not be used for any future run.

**No new future execution identity or port is named in the subject.** A *future* identity
named in the document that becomes the subject is burned — `b35c1`/18282, `b35b1`/18291,
`b35A` and `pm2H` were each lost that way. The identity for any authorised run is named in
a request committed **after** this subject.

That rule constrains **forward allocation only**. It does not reach backwards: the
historical ledger keeps every consumed and retired identity and port exactly as recorded,
and nothing is deleted from it to satisfy a form of words.

Ledger unchanged, and retained: **18281 RETIRED-NEVER-BOUND** (never bound, not SPENT),
**18283 SPENT**, and every earlier entry. Those are history, not reservations, and they
stay.

### 4.1 A provenance claim I had to correct first

Earlier subject records say the batches each recorded HEAD and attested a clean tree.
**`run_suites.sh` never asked git anything.** The commits and clean trees were real and I
did check them — but from *outside* the batch, and then wrote the sentence as though the
batch had attested them. That is a claim resting on an unverified premise, and provenance
is the worst place for one, because the reviewer cannot re-derive it later: the temp
directories are gone and only the sentence survives.

The runner now records HEAD, the subject line, and **separate** tracked-dirty and
untracked counts, keeps them in `git.txt` in the batch root, and prints a warning in terms
if the tracked tree does not match. `test_run_suites.sh` 45 → **59**.

---

## 5. Batch results — three serial batches at the subject

Serial by construction — no `&`, no `wait`, no `xargs -P`, asserted by
`test_run_suites.sh`. Each batch records its **own** HEAD (§4.1).

| batch | head | tracked dirty | suites | non-zero | assertions |
|---|---|---|---|---|---|
| 1 | `e7243e4` | **0** | 52 | **0** | 4243 |
| 2 | `e7243e4` | **0** | 52 | **0** | 4243 |
| 3 | `e7243e4` | **0** | 52 | **0** | 4243 |

**52 suites, 4243 assertions, 0 non-zero exits, identical across all three.**

Suites relevant to this work, from batch 1 and identical in 2 and 3:

```
test_tls_off_omission.sh      67      test_staging_entry.sh        148
test_stop_jlist_parser.sh     61      test_staging_override.sh      81
test_production_stop.sh       33      test_production_launcher.sh  111
test_run_suites.sh            59      test_tracked.sh               19
```

`untracked` reads 419 in each batch: the session scratch directory, outside `dev2026/` and
outside the archive. It is reported rather than suppressed, and counted **separately** from
tracked-dirty so it cannot be mistaken for a modified source file.

### 5.1 The first three batches were NOT clean, and why

An earlier set of three at `3334c31` reported **NON-ZERO: 3** in each. Recorded here
because a report that mentions only the green run is the same omission this programme
keeps refusing.

| suite | failure | cause |
|---|---|---|
| `test_staging_entry.sh` | *"the ten are now 7 exact + 3 absent"* | **a real regression from my TLS change** |
| `test_production_launcher.sh` | *"no untracked shell/JS/spec anywhere in dev2026"* | my process error |
| `test_tracked.sh` | *"no harness source file is untracked"* | same |

**The regression.** Making `env_must` TLS-aware moved the key and certificate from *exact
value* to *required ABSENT* when TLS is off, so the split is no longer fixed — **TLS off:
5 exact, 5 absent; TLS on: 7 exact, 3 absent**. `staging_execute.sh` still stated the
TLS-**on** split as though it were the only one, and the entry suite asserted that exact
string.

I missed it because I re-ran only the suites I *assumed* the change touched. The batch has
a claim to completeness; my judgement about which suites are relevant does not.

The fix does not re-pin a different literal, which would be equally brittle one mode over.
The test now **derives** the numbers from the `env_must` calls themselves: 8 unconditional
plus 2 on each arm, so **ten checks execute in either mode although twelve lines exist**.
My first version counted source lines, said 12, and was wrong — corrected before it
landed. `test_staging_entry.sh` 125 → **130**.

**The two untracked failures were mine, and the guards were right.** I created a document
under `dev2026/specs/` while a batch was running. Those guards exist because a module once
passed every working-tree check and was absent from the commit. A document that names a
subject hash cannot be inside that subject anyway, so this one is committed **after** the
subject is cut.

### 5.2 Two false signals of my own, recorded

Both are the defect class this programme keeps finding — **a check that matched its own
artifact** — and both were mine, in this session's tooling rather than in the harness:

1. **A completion watcher matched itself.** The batch records the process table; that
   capture contained my watcher's own command line, which contained the literal string it
   was watching for. The watcher found itself and reported three completed batches while
   batch 1 was still running. Fixed with a whole-line match the indented listing cannot
   satisfy.
2. **A vacuous provenance check.** `git diff` against a path that does not exist compares
   nothing and exits 0 — see §4.

Neither affected the harness or any recorded result. They are here because in both cases
the wrong answer was **green**, and green is the direction that does not prompt a second
look.

### 5.3 One reporting nuance, not a defect

The batch line for `test_staging_store.py` shows a caveat rather than a count, because the
runner prints each suite's **last** line and that suite ends with *"Deployment machinery
only. NOT evidence about real WOA23 data."* Its 24 assertions pass; `exit=0` is the
authoritative signal and is what the totals use. The 4217 above includes them.

---

## 6. Deliverables 5 and 6 — the two draft requests

Both are drafts. **Neither is authorised, neither has been executed, and neither names an
identity or a port.**

| | | |
|---|---|---|
| **5** | [`b1v2`](B1-host-validation-request-b1v2.md) — replaces the **withdrawn** [`b1v1`](B1-host-validation-request-b1v1.md) | B1 host validation — the stop fix meeting a real PM2 daemon. Staging only, one app started solely to be stopped, serving nothing and measuring nothing |
| **6** | [`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md) | cleanup of the retained `bs3v1` daemon (pid 1709473), tree, bootstrap and workdir, plus `~/woa23-b35a1/` |

**They are deliberately separate documents.** Spec 014 §2 says cleanup after a preserved
state needs its own authorisation and is never bundled into a validation run, and that
daemon removal is its own authorisation. Bundling them would have broken both rules in the
first documents written after making them.

There is also a substantive reason, not just a procedural one: **`production_stop.sh` is
the thing `b1v2` tests**, so its success cannot double as that run's cleanup without
assuming the conclusion.

`CLEAN-bs3v1` captures before it removes — the daemon's raw `jlist`, the logs, a digest
listing, and the daemon's `(pid, starttime)` — because deleting a preserved failure
destroys the record of the failure. A failed capture removes nothing.

---

## 7. Separated status — what is staging evidence, and what is not validated

Reported per objective and never merged, because a pass in one says nothing about another.

| objective | status | evidence | production validated? |
|---|---|---|---|
| **B1** — a stop is a stop | **NO HOST EVIDENCE** | offline suites only (61 + 33 assertions). Never met a real PM2 daemon | **NO** |
| **B3** | **staging evidence only**, from `bs3v1`, with its five qualifications intact | `bs3v1`, unchanged and not re-scored | **NO** |
| **B5** | as B3 | `bs3v1`, unchanged | **NO** |
| **TLS environment isolation** | **offline evidence only** — prevention and detection implemented and tested; never run on the host | 67 + 148 assertions | **NO** |
| **production TLS behaviour** | **unchanged, and not under test.** Default ON, key/cert still required and readability-checked | assertions only | **NO** |

**Nothing in this report is a production validation of anything.** No production config,
launcher, runtime, ACL, permission or PM2 state was read or modified. The only *host*
evidence that exists anywhere in this programme is `bs3v1`'s staging-only B3/B5, and it is
not extended, upgraded or re-interpreted here.

---

## 8. Still open, and not bundled into anything

The ghrsst assessor-approval record at
`~/.config/ghrsst/p5_assessor_approval_7126c7b.json` still awaits your keep-or-remove
decision. It is on the local machine, not VM24, and is mentioned only so it is not lost.


---

## 9. Addendum — the `/proc` read path, and subject `cba1329`

Added after review. **`e7243e4` is superseded; `cba1329` is current.**

### 9.1 The defect I flagged, and the one I had not

The review picked up `children_of` taking **field 4** of `/proc/<pid>/stat`. `starttime_of`
had the **identical defect on field 22** and I had not said so — which is the worse
omission, because that field **is** the `(pid, starttime)` identity, the whole defence
against PID reuse.

```
<pid> (<comm>) <state> <ppid> ... <starttime> ...
```

`comm` may contain spaces and parentheses, so every later field shifts and a fixed index
returns a **plausible wrong number**, not an error. A missed child is a survivor nobody
checks; a wrong starttime compares two unrelated processes and calls them the same one.

`ppid` now comes from the labelled `PPid:` line of `/proc/<pid>/status`, which cannot
shift. `starttime` still needs `stat`, so the comm is removed **first**, at the **last**
`)` — greedy, so a comm containing its own `)` is handled.

### 9.2 Two more fail-open paths, found while fixing it

Neither was a shifted field; both were **silent skips**, and neither was asked about.

1. The scan did `[ -r ... ] || continue`, so a pid it could not read was treated as **"not
   a child of mine"** — the same fail-open shape as the jlist parser reading empty output
   as "nothing to stop". An **unknown** parent is not a parent known not to be this app's.
   Unreadable and unparsable entries are now recorded and the run dies on them. A pid that
   **vanished** between listing and read is separated out and is *not* an error — that is
   an exiting process, and failing on it would make every stop flaky.
2. A child whose starttime could not be read was **dropped from the recorded tree**, so a
   process that existed but could not be identified would never be checked for survival.

The unresolved list is a **file, not a variable**: `children_of` is called inside `$( )`,
and a variable set in a subshell never reaches the caller. That trap has now cost this
campaign four defects.

### 9.3 Descendants, not only direct children

gunicorn's workers are direct children, but nothing guarantees the tree is two deep. The
walk is breadth-first with an explicit depth bound, and **exceeding the bound refuses**
rather than reporting a partial tree as the whole tree.

### 9.4 Tests — `test_stop_proc_parsing.sh`, 77 assertions

Every case the review listed: comm with a space; comm with parentheses, including nested;
missing / empty / malformed / non-numeric `PPid`; unreadable `status`; vanished pid; a
four-level tree; a pid claimed by two parents; a tree deeper than the bound; and PID reuse
where the recycled process also has an awkward comm. The core invariant is asserted over a
table: **a parse failure is never read as "this pid is not a child".**

`test_production_stop.sh`'s fixture wrote only `stat` and went red immediately — correctly,
because it was modelling half the real read path. It now writes both files.

**Four test bugs of my own**, corrected rather than worked around. The worst:
`children_of 100 | grep -qx 200` is wrong under `set -o pipefail` — `grep -q` exits at the
first match, `children_of` takes SIGPIPE, and the **pipeline** reports failure. So the
assertion went red exactly when the pid it wanted came **first**, and passed when it came
last: a match read as a miss. Also: every case was wrapped in `( )`, silently discarding
every pass/fail increment; a "cycle" fixture that merely detached the subtree and proved
nothing; and an assertion counting `pm2_env.status` as a `/proc` read.

### 9.5 What did NOT change, shown rather than argued

`jlist_resolve` is **byte-identical** between `e7243e4` and `cba1329` — `fde549be…79d7` —
and `test_stop_jlist_parser.sh` is the same blob (`3336adcb…6ffa`) passing the same **61
assertions**. A new assertion also requires that `jlist_resolve` reads no `/proc` and calls
none of the new helpers.

TLS Option A is untouched: `staging_execute.sh`, `production_app.sh`,
`make_staging_override.js` and the production config are all **unchanged blobs** from
`e7243e4`.

### 9.6 Subject `cba1329`

```
commit           cba1329cf444179636f6298e4d98112083c247ab
archive sha256   17044c0bd2a19563bb4c4178b8908509ca08d1da06ad1aabb08198e322dfc7b3
files            228
file-list sha256 4724ce1e8688ff07e4d101d087e32300f50b4a447af0ead162e32b2c74d0a844
```

Three serial batches: **53 suites, 4320 assertions, 0 non-zero**, identical, each recording
its own HEAD with `tracked dirty = 0`. Per-file digests in [`b1v2` §7](B1-host-validation-request-b1v2.md).

**Superseded and not to be used:** `e7243e4`, `fe6d4e2`, `a046d5ad`.

### 9.7 Status, unchanged by any of this

**B1 still has NO host validation.** `bs3v1`'s B3/B5 remain **staging-only, qualified**
findings, not re-scored and not back-filled. Nothing here is a production validation.
`18283` stays **SPENT**, `18281` stays **RETIRED-NEVER-BOUND**, the `bs3v1` daemon and tree
are **retained and untouched**, and cleanup remains its own separate request.
