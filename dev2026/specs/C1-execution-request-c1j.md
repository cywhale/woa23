# C1 `c1j` — execution request (read-only account, run-as UID 994)

**Status: REQUESTED, NOT GRANTED. Nothing has been run.** No VM24 contact since the
read-only verification, no port bound, no arm started. **This document is not an
authorisation.**

Supersedes [C1-execution-request-c1i.md](C1-execution-request-c1i.md), which was blocked
on an account that now exists. `c1i` never ran and is retired.

---

## 1. What changed since `c1i`

| | |
|---|---|
| **the account** | `woa23c1ro`, uid 994, private group only, dedicated home `/home/woa23c1ro`, key-only SSH, no privilege escalation, no supplementary groups, no TTY, no port forwarding — **created by the PI as a pre-existing host condition; this campaign does not create or alter it** |
| **the mechanism** | **Option 1** — direct SSH as `woa23c1ro`. The *whole* orchestration runs as 994, so every arm and every worker inherits it |
| **the runner** | production paths made explicit; whole-process-tree UID assertions added |
| **the pre-flight** | full-tree readability/non-writability and symlink-escape checks, still without ever writing |
| **the runtime question** | answered and **documented as residual risk**, not silently accepted — §5 |

## 2. Subject

| item | value |
|---|---|
| **commit** | `832e767dfccae5fad07329590eeb3314e540ca22` |
| **archive SHA-256** | `05d36054102be1fcf841a51db88f8d7c6e4241a08a74a1ceb1cf59ccbdb1a041` |
| **file count** | `176` |
| **file-list SHA-256** | `c819b924c87ba5e8bc850987f78152b113416f626d00e0846ef96ac23e286dee` |
| **verifier** | `scripts/verify_clean_archive.sh 832e767` → **16/16** |

**174 → 176**: two added (`scripts/test_c1_readonly_account.sh`,
`specs/017-c1-run-as-readonly-account.md`), four modified (`lib_procs.sh`,
`run_controlled.sh`, `store_readonly_preflight.sh`, the `c1i` request). Supersedes
`75d0f97`.

**`api/` and `bench/` are byte-identical to `ad3f428`** — `git diff` across the whole span
is empty. **The candidate under test has not moved**; only the harness has.

**Offline evidence:** three strictly serial batches, **132 runs, 0 non-zero**, evidence
root `/var/folders/z6/.../woa23-suites-9Xr3Ay/` (retained). `fail_lines = 3` is the
collector matching a section *heading* in `test_symmetric_warmup.py` whose assertions all
pass — a false positive in the evidence collector, not a failing assertion.
`test_c1_readonly_account.sh`: **58 assertions**. **No new `arm.py` strays** — 16 remain,
all from 11–20 August, none from the batch window.

**This document and any later commit are protocol references and must never be back-filled
as the execution subject.**

## 3. Execution identity — first-use throughout

| | `c1j` |
|---|---|
| grant | **`WOA23_S2_C1_GRANTED=yes`** |
| run-as account | **`woa23c1ro`** (uid 994), via direct SSH |
| label | **`c1j`** |
| workdir | **`/home/woa23c1ro/woa23-c1j-work/`** |
| `HOME` | **`/home/woa23c1ro`** |
| `TMPDIR` | **`/home/woa23c1ro/tmp-c1j/`** |
| candidate arm | **`18341`** |
| reference arm | **`18342`** |
| isolated dask scheduler | **`18979`** |
| `WOA23_EXPECT_UID` | **`994`** |

**All three ports are first-use** — absent from `scripts/ports_used.tsv` and named in no
other file. **They are deliberately NOT pre-recorded in the ledger**: `run_controlled.sh`
refuses a port the ledger names, reading it from the export of the subject it runs, so a
port recorded before its run is a port that run cannot use. They are added *after* `c1j`
runs. This is the trap documented for `pm2F` and walked into once at `c1i`.

**Nothing from `c1h`, `c1i`, `c1f`, `c2g`, `s2pB` or any `pm2*` run is reused.**
`c1h`'s and `c1i`'s ports remain **RETIRED-NEVER-BOUND**.

## 4. The command

```
ssh -o BatchMode=yes woa23c1ro@odb24
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c1j \
  WOA23_S2_C1_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_controlled.sh --c1 \
    --workdir /home/woa23c1ro/woa23-c1j-work \
    --prod-dir /home/odbadmin/python/woa23 \
    --store    /home/odbadmin/python/woa23/data \
    --python-binary  /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone  /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --candidate-port 18341 --reference-port 18342 --scheduler-port 18979 \
    --label c1j
```

**`--prod-dir` and `--store` are mandatory here, not optional.** With
`WOA23_EXPECT_UID` set the runner **refuses** to fall back to a `$HOME`-derived default,
because under `HOME=/home/woa23c1ro` that would name a path under the *running* account's
home rather than production's. The refusal names both flags.

## 5. The runtime — shared, and said so plainly

**C1 runs on production's own interpreter and a read-only clone of production's packages.
That is the point of C1, and it is NOT runtime isolation. No claim of isolation is made.**

The arm's complete load path under `-S`, with every mode verified read-only on VM24:

| `sys.path` entry | mode |
|---|---|
| `woa23-s2-package-clone/dist` | **555** |
| `3.11.4/lib/python3.11` (stdlib) | **755** |
| `3.11.4/lib/python3.11/lib-dynload` | **755** |
| interpreter `3.11.4/bin/python3.11` | **755** |

- **World-writable entries on the load path: `0`.**
- `woa23c1ro` is in **no shared group**, so the pyenv tree's group-writable bits (group
  `odbadmin`) do not reach it.
- `py311` and `py311/bin/python3.11` are **symlinks**; their `lrwxrwxrwx` bits are never
  enforced by the kernel and must not be read as world-writable. **An earlier report of
  mine did exactly that and was wrong** — recorded in spec 017 §1.

**ACCEPTED RESIDUAL RISK, by PI decision:** one zero-byte world-writable pyenv lock file,
`3.11.4/envs/py311/.lock`, **not on the arm's `sys.path`**. A process as 994 could truncate
it; it cannot be executed or imported, and nothing in an arm reads it. **It is not
modified by this campaign** — no `chmod`, `chown`, delete or truncate.

## 6. Pre-flight — recorded, and every step non-writing

`scripts/store_readonly_preflight.sh`, run **as `woa23c1ro`**:

1. **identity FIRST**, before any decision — resolved path, mode, `owner:group`, `uid:gid`,
   directory mtime, hard links, size, top-level listing, file count, total bytes, and a
   **metadata fingerprint** (`path`/`size`/`mtime`), which is **not** a content baseline and
   is never reported as one;
2. **writability by `stat` and `test -w`** — the kernel's access check, which attempts
   nothing and leaves no trace;
3. **the complete tree** — every directory traversable and readable, every file readable,
   **nothing writable**. The GNU `find` predicates are **probed before their answers are
   believed**, with a portable per-entry `[ -r ]`/`[ -w ]`/`[ -x ]` fallback, because both
   use `access(2)` and therefore respect the ACL;
4. **symlink escapes** — no symlink inside the store may resolve outside it.

**No `touch`, `rm`, `mkdir`, `mv`, `chmod`, `chown`, `setfacl` or write probe on any path,
including every failure path.** Asserted by the offline suite, which distinguishes a call
from a mention so the refusal message may keep naming what is forbidden.

**This is the `c1h` correction.** `c1h` wrote into the store to learn whether it could,
was not refused, and moved the store directory's mtime doing it — and because it probed
before capturing identity, no pre-probe baseline exists and none can be reconstructed.
That record stands in [C1-result-c1h.md](C1-result-c1h.md) §3 and is not revised.

## 7. Identity verification during the run

**Every tracked process must be uid 994 — masters and every worker.**

`assert_tree_uid` reads `/proc/<pid>/status` for each pid in the recorded tree and compares
**all four** uids: real, effective, saved-set and filesystem. The filesystem uid is the one
that decides whether a write to the store would succeed, and a process can differ from its
parent in any of them. **"The SSH shell was 994" is not the claim the read-only ACL rests
on.**

- checked for `dask_scheduler`, `dask_worker`, `reference` and `candidate`;
- run **after** the trees are recorded and their process counts verified, so the set is
  known-complete;
- **zero processes checked is a failure**, not a pass;
- a mismatch is **fatal** — no warn-and-continue.

## 8. Scope

**Allowed:** reading the production store; C1 contract validation only — canonical values
and column sequence, the candidate's canonical **column-order** contract, the
`(time_period, depth, lat, lon)` **row-order** contract, JSON/CSV fields, values, row order
and status, and the pre-defined reconstruction rules for expected ordering differences.

**Forbidden:** any write, `chmod`, `chown`, delete or rename under the production store;
any HTTP request to production (**8050 / 8786 / 8787 stay at zero**); any change to
production PM2, processes, listeners, `conf/`, ACLs or runtime files; **latency, warm-up,
noise pilot, startup, deployment, PM2 or production-API testing**; any conclusion about
spec 016's venv runtime behaviour; **touching `pm2G`** (§9); back-filling `c1f`, `c2g`,
`s2pB`, `pm2G`, `c1h` or `c1i`.

**Classification:** never reportable as a `5.2A raw byte-exact PASS`. If reconstruction or
any required verification does not complete, the classification is
**`INCOMPLETE_VALIDATION`** and **no PASS is reported**.

## 9. pm2G and prior evidence — untouched

**`pm2G` stays exactly as it is:** port `18265` **still bound**, gunicorn 1456369 and
workers 1456373/1456374 **still running**, PM2 app online under `~/woa23-pm2g-pm2/`, tree,
workdir, store, logs and uv cache retained. **No `pm2` command, no signal, no port release,
no cleanup**, and it remains **NOT A PASS**.

**`c1h`'s evidence is retained**, including the store-directory mtime change it caused
(`1787622836`, unchanged since). `c1i` left nothing to retain.

## 10. Failure and cleanup boundaries

**On any failing step: STOP, retain, report, wait.** Retained: both arms' processes, bound
ports, workdir, export, logs, request log, every diagnostic.

**Forbidden after a mid-flight failure:** any `pm2` command; manual process termination;
**SIGKILL**; releasing a port; any `rm`, `find -delete` or `chmod` on the retained tree;
**self-rerun**.

**On success**, the runner's cleanup runs and the three ports must be **confirmed free and
connection-refused afterwards** — recorded, not assumed. A survivor is **`CLEANUP_FAIL`**:
left alive for inspection, **no SIGKILL**, **not repeated**. Cleanup touches only this
run's own arms.

## 11. What a PASS will and will not mean

**Will:** the `015` candidate satisfies the C1 contract gates against the **real production
store**, read through an enforced read-only ACL by an account that cannot write it, with
every arm process proven to be uid 994.

**Will not:** **not a `5.2A raw byte-exact PASS`; not a staging PASS; not a production
cutover PASS; not a latency result; not a deployment result; not a claim of runtime
isolation.** **B1–B5 remain open. B7 remains open. `pm2G` remains NOT A PASS. C2 remains
blocked** until C1 completes and is reviewed.

## 12. Submission

- **Subject `832e767`.** This document and any later commit are protocol references.
- **Awaiting explicit authorisation.** No VM24 contact until then.
- **C2 is not requested** and does not follow from any C1 outcome.
