# C1 `c1i` — execution request (BLOCKED on a read-only account)

**Status: REQUESTED, NOT GRANTED, and NOT YET RUNNABLE.** No VM24 contact, no port bound,
no arm started. **This document is not an authorisation.**

**It cannot be executed by any account this campaign currently uses.** §3 states what is
needed and who has to provide it. Submitting it now so the requirement is reviewable while
that is arranged.

Supersedes [C1-C2-execution-request-015-016.md](C1-C2-execution-request-015-016.md) for the
C1 half. **C2 is not requested here** and does not follow from C1.

---

## 1. Why `c1h` did not run, in one paragraph

`c1h` stopped at the production-store pre-flight. The authorisation required a write probe
that must be **refused**; it was **not** refused, because
`/home/odbadmin/python/woa23/data` is owned by `odbadmin`, mode `775`, and C1 runs as
`odbadmin`. The owner can always write. **Read-only was intended but not enforceable**, and
the probe that established this **moved the store directory's mtime** — production
metadata, changed by a pre-flight whose purpose was to prove nothing would be. Full record:
[C1-result-c1h.md](C1-result-c1h.md). Classification `INCOMPLETE_VALIDATION`; the candidate
was neither validated nor invalidated.

## 2. What changed in the procedure

### 2.1 The write probe is gone

**No pre-flight will ever again create or delete anything under the production store.**
`scripts/store_readonly_preflight.sh` replaces it and determines writability from
**`stat` and the kernel's access check only**:

```sh
WRITABLE=no
if [ -w "$STORE" ]; then WRITABLE=yes; fi
```

`test -w` asks whether this account *could* write. It does not attempt one, creates
nothing, removes nothing, and leaves no trace. **The script contains no `touch`, `rm`,
`mkdir`, `mv`, `chmod` or `chown` on any path through it, including its failure paths.**

### 2.2 Identity is captured FIRST

The `c1h` pre-flight took identity in step 4 and probed in step 3, then exited at step 3 —
so the probe destroyed the chance to baseline the thing it might have changed. **There is
no pre-probe file count or byte total for the production store and there never can be for
that moment.**

The replacement records, **before any other step and before any decision**:

| # | recorded |
|---|---|
| 1 | **resolved path** (and refuses if it resolves anywhere but the declared path) |
| 2 | **permissions / ownership** — mode, `owner:group`, `uid:gid` |
| 3 | **directory mtime** — epoch and human |
| 4 | **hard-link count** and directory size |
| 5 | **top-level entry list and count** |
| 6 | **total file count** |
| 7 | **total bytes** |
| 8 | **file-list digest** — `path\tsize\tmtime` over the whole tree, `LC_ALL=C` sorted, SHA-256 |

**The file-list digest is a METADATA FINGERPRINT, not a content baseline**, and the script
now says so in the output line itself rather than only in a comment. It hashes names, sizes
and mtimes; **a write that preserves size and mtime is invisible to it, and it says nothing
about the bytes**. Hashing 35 GB on every pre-flight would cost more than it tells — that
trade is deliberate, and naming it stops the digest being read later as proof of content
integrity.

**It is also not a recovery of what `c1h` lost.** `c1h` had no baseline of any kind, so no
pre-probe file count, byte total or digest for the production store exists and none can be
reconstructed. The first value this script records is the first that has ever existed — a
starting point from here, not a repair. The `c1h` mtime change and the missing baseline
stay on the record in [C1-result-c1h.md](C1-result-c1h.md) §3.

### 2.3 It is testable offline

The `c1h` pre-flight was never exercised anywhere but against the production store, and its
one mistake landed there. The replacement runs on **GNU and BSD** (`stat`, `find -printf`
and `du -sb` all have fallbacks) and **all three exit paths are exercised on the
development machine**: exit 5 on an owned directory, exit 0 on one this account cannot
write, exit 4 on a missing path. **A pre-flight that cannot be rehearsed is the defect, not
the platform difference.**

## 3. THE BLOCKER: an account that cannot write the store

**C1 requires read-only access that is ENFORCED. That needs an account which does not own
`/home/odbadmin/python/woa23/data` and has no write permission to it.**

`odbadmin` cannot satisfy this. It owns the store, so it can always write it, and no care
taken inside the arms makes the guarantee checkable — which is the entire point of
checking it.

### 3.1 What is needed

**Either** an existing account on `odb24` that:

- can read `/home/odbadmin/python/woa23/data` in full (the arms must read the real store);
- **cannot** write, create, delete, rename, `chmod` or `chown` anything under it;
- can read the package clone at `/home/odbadmin/woa23-s2-package-clone/`;
- can execute `/home/odbadmin/.pyenv/versions/py311/bin/python3.11`;
- can create its own workdir and bind loopback ports;

**or** a read-only ACL granting an existing campaign account exactly that.

### 3.2 What this campaign will NOT do

**None of the following will be attempted, and none is a workaround to be reached for:**

| forbidden | why |
|---|---|
| `chmod` the store | modifies production; explicitly forbidden by the C1 authorisation |
| `chown` the store | same |
| create a bind mount | needs privilege this campaign does not hold, and changes the host |
| create or modify any account | host administration, not campaign work |
| set or alter an ACL | same |
| run C1 as-is and rely on discipline | **rejected by the PI.** The guarantee would be unassertable |

**This is a REQUEST TO WHOEVER ADMINISTERS `odb24`. It is not an action for this campaign
to take on VM24, and no attempt will be made to arrange it from inside a run.**

### 3.3 Until then

**C1 does not run.** No further VM24 contact for C1 of any kind — not a pre-flight, not a
dry run, not a permissions check. The next VM24 contact happens only after both an account
exists and a new explicit authorisation is given.

## 4. Execution identity — `c1h` is consumed

**Two states, kept apart — the ledger now distinguishes them and so does this:**

| | `c1h` |
|---|---|
| **identity** (label, staging, workdir) | **CONSUMED** — authorised and started, never reused |
| **run classification** | **`INCOMPLETE_VALIDATION`** — stopped at the store pre-flight |
| **ports 18301 / 18302 / 18949** | **RETIRED-NEVER-BOUND** — no socket ever listened on any of them |

**"Consumed" and "bound" are different facts.** `run_controlled.sh` was never invoked, no
arm started, and nothing listened; the ports are retired by campaign policy, **not because
they were used**. Calling them "spent" would tell a later reader a listener existed, which
is the one thing this ledger exists to answer.

| | `c1i` |
|---|---|
| label | **`c1i`** |
| staging | **`~/woa23-c1i/`** |
| workdir | **`~/woa23-c1i-work/`** |
| candidate arm | **`18321`** |
| reference arm | **`18322`** |
| isolated dask scheduler | **`18969`** |
| grant | **`WOA23_S2_C1_GRANTED=yes`** |

**All three ports are first-use**, and — **verified against the subject itself** — absent
from `scripts/ports_used.tsv` as it exists at `75d0f97`, so `run_controlled.sh` will accept
them.

**This was got wrong once and is recorded rather than quietly fixed.** The three ports were
first added to the ledger at proposal time, in a commit that became an ancestor of this
subject. `run_controlled.sh` refuses any port the ledger names and reads that ledger **from
the export of the subject it runs** — so all three would have been rejected by the run they
were chosen for. Exactly the trap written down for `pm2F`, walked into two commits later.
They are removed, with a note at the head of the ledger saying why they are absent and that
they are added **after** `c1i` runs, per the convention that file already states.

**Nothing from `c1h`, `c1f`, `c2g`, `s2pB` or any `pm2*` run is reused.**

## 5. Subject — NEW, re-cut and re-verified

`ad3f428` **cannot** be the subject: `scripts/store_readonly_preflight.sh` post-dates it,
and a run whose pre-flight is not in its own subject is not running what was authorised.
The subject is re-cut, and every digest re-derived rather than carried forward:

| item | value |
|---|---|
| **commit** | `75d0f977774d22454650c0bf555fa7134ca55ebd` |
| **archive SHA-256** | `7368651303ea8dfdf2f08466c210f1c0a669c905d1ae1420f071a33d7c09ae3e` |
| **file count** | `174` |
| **file-list SHA-256** | `bc1ba0aa41cd60ceacffe92e42c62c619ac04f45c903d177c8de24a2f63cdf6d` |
| **verifier** | `scripts/verify_clean_archive.sh 75d0f97` → **16/16, all passed** |

**Supersedes `ad3f428`** (169 files, archive `308482b9…`, file-list `c58f34a6…`), which
remains the subject of the C1/C2 execution request and of `c1h`.

### 5.1 Why 169 → 174

**Exactly five added files; the one `M` changes content, not count.**

| status | path | kind |
|---|---|---|
| **A** | `scripts/store_readonly_preflight.sh` | **the new pre-flight — the reason for the re-cut** |
| **A** | `specs/C1-result-c1h.md` | the `c1h` result |
| **A** | `specs/C1-execution-request-c1i.md` | this document |
| **A** | `specs/C1-C2-execution-request-015-016.md` | prior request (documentation) |
| **A** | `specs/C1-C2-rerun-request-015-016.md` | prior request (documentation) |
| **M** | `scripts/ports_used.tsv` | ledger semantics + `c1h`/`c1i` rows |

**One of the five is executable. Four are documentation.**

### 5.2 No unauthorised change to `api/` or candidate logic

`git diff ad3f428..8b3ab65 -- dev2026/api/ dev2026/bench/` is **empty**. All four
candidate files — `api/query.py`, `api/app.py`, `api/config.py`, `api/store_paths.py` —
are **byte-identical** to `ad3f428`, individually digest-checked. **The candidate under
test is unchanged; only the pre-flight, the ledger and documentation moved.**

### 5.3 Offline evidence for this subject

**Three strictly serial batches** on `75d0f97` (`WOA23_SUITE_REPEAT=3`,
`concurrency: serial`), with the working tree clean at that commit:

| | |
|---|---|
| total runs | **129** (43 suites × 3) |
| **non-zero exits** | **0** — all 129 exited `0` |
| evidence root | `/var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T//woa23-suites-9MTSuv/` (retained, nothing deleted) |
| per-suite artefacts | stdout, stderr, exit code, timings, `env.txt`, `procs.before`, `procs.after`, `procs.leaked`, `failures.txt` |

**Three things in that evidence that look like problems. All three were checked, not
assumed:**

**1. `fail_lines = 3`** — one per batch, all `test_symmetric_warmup.py`, which exits `0`.
The collector greps `^\s*FAIL`, and that suite prints a **section heading**:
`FAIL CLOSED: an arm that did not answer 200 was not warmed`. The three assertions beneath
it all pass. **A false positive in the evidence collector, not a failing assertion.**

**2. A large leak diff.** It is `ps` before vs after, and on a workstation that is mostly
the workstation: macOS system processes (`CryptoTokenKit`, `mdworker_shared`, `CommCenter`,
`spotlightknowledged`), **processes from the unrelated `ghrsst` project running
concurrently on the same machine**, and in-flight `arm.py` transients caught mid-suite.

**3. Sixteen `arm.py` processes ARE alive afterwards — and none is from this run.** All
sixteen started between **11 and 20 August**; the batch window was
**2026-08-25 12:01:29 → 13:12:20**. **Zero started during it.** They are the known
pre-existing development-machine strays, which are **not VM24 evidence and are not to be
cleared**. They hold ephemeral ports (51232–65220) and **none is in the campaign's `18xxx`
or `39xxx` ranges**, so none collides with `c1i`'s `18321`/`18322`/`18969`.

**Scope of all of the above, stated plainly:** it covers **this development machine only**.
VM24's state has not been re-read, because **C1 has not contacted VM24 since the `c1h`
post-stop verification** and will not until authorised.

**This document, and any commit after `8b3ab65`, are protocol references and must never be
back-filled as the execution subject.**

## 6. Scope — unchanged, and still real-store read-only

**Allowed:**

- **reading** the production store `/home/odbadmin/python/woa23/data`, in full, through the
  arms' symlinks — this is what makes C1 real-store contract evidence;
- contract validation only: canonical values and column sequence, the candidate's canonical
  **column-order** contract, the `(time_period, depth, lat, lon)` **row-order** contract,
  JSON/CSV fields, values, row order and status, and the pre-defined reconstruction rules
  for expected ordering differences.

**Forbidden:**

- **any write, `chmod`, `chown`, delete or rename** under the production store;
- **any HTTP request** to production — 8050 / 8786 / 8787 stay at **zero**;
- **any change** to production PM2, processes, listeners or `conf/`;
- **latency, warm-up, noise pilot, startup, deployment, PM2 or production-API testing**;
- **any conclusion about spec 016's venv runtime behaviour**;
- **touching `pm2G`** — see §7;
- back-filling `c1f`, `c2g`, `s2pB`, `pm2G` or `c1h` into this run's evidence.

**Classification rules:** this run may **never** be reported as a `5.2A raw byte-exact
PASS`. If reconstruction or any required verification does not complete, the classification
is **`INCOMPLETE_VALIDATION`** and **no PASS is reported**.

## 7. pm2G and c1h evidence — both preserved

**`pm2G` stays exactly as it is:** service running (gunicorn 1456369, workers
1456373/1456374), port `18265` **still bound**, PM2 app online under `~/woa23-pm2g-pm2/`,
tree, workdir, store, logs and uv cache all retained. **No `pm2` command, no signal, no
port release, no cleanup**, and `pm2G` is **NOT A PASS** and may not be cited as one.

**`c1h`'s evidence is retained**, including the record of the store directory mtime change
it caused. It is not re-run and not cleaned.

## 8. Failure and cleanup boundaries

**On any failing step: STOP, retain, report, wait.** Retained: both arms' processes, bound
ports, workdir, export, logs, request log and every diagnostic.

**Forbidden after a mid-flight failure:** any `pm2` command; manual process termination
(`kill`, `pkill`, `pgrep|kill`); **SIGKILL**; releasing a port; any `rm`, `find -delete` or
`chmod` on the retained tree; **self-rerun**.

**On success**, the runner's own cleanup runs and the three ports must be **confirmed free
and connection-refused afterwards** — recorded, not assumed. A survivor after cleanup is
**`CLEANUP_FAIL`**: left alive for inspection, **no SIGKILL**, **not repeated**.

**Cleanup touches only this run's own arms.** Nothing belonging to `c1h`, `pm2A`, `pm2B`,
`pm2E`, `pm2F`, `pm2G` or any earlier C-run is touched, on success or failure.

## 9. What a PASS will and will not mean

**Will:** the `015` candidate satisfies the C1 contract gates against the **real production
store** — canonical values and column sequence, the canonical column-order contract, the
row-order contract, JSON/CSV fields and values, and the ordering differences handled by the
pre-defined reconstruction rules.

**Will not:** **not a `5.2A raw byte-exact PASS`. Not a staging PASS. Not a production
cutover PASS. Not a latency result. Not a deployment result.** **B1–B5 remain open.**
**B7 remains open.** **`pm2G` remains NOT A PASS.** **C2 is not authorised by any C1
outcome.**

## 10. Submission

1. **A read-only account or ACL is required before C1 can run** (§3). That is a request to
   whoever administers `odb24`; this campaign will not arrange it on VM24.
2. **No VM24 contact** until such an account exists **and** a new explicit authorisation is
   given.
3. **The subject must be re-cut and re-authorised** when C1 becomes runnable, because the
   new pre-flight script post-dates `ad3f428` (§5).
4. **C2 is not requested** and does not follow from C1.
