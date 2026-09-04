# C1 `c1r` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. C1 has not been run.**
No VM24 contact has been made for `c1r`. `c1q` has not been rerun and remains
`INCOMPLETE_VALIDATION`.

---

## 1. Provenance — the four items asked for, verified

### 1.1 Complete archive SHA-256 (64 characters)

```
a4650395569abdd9f9038cbef3a429c58e92772fa18f8e3aadd02a494077e7b9
```

Recomputed from the commit itself, not copied forward:

```
git archive --format=tar 13d6b74d1372a834e534bc91e91d027ab758a90d dev2026 | sha256sum
```

Length verified: **64 characters**. Archive size: **4 167 680 bytes**.

### 1.2 Complete file-list SHA-256 (64 characters)

```
301668db5c22b2cb3292f3147fdfff2e05ebd34724fcf9b5d98d78b7993404ca
```

Produced by `verify_clean_archive.sh`: per-file SHA-256 over the exported tree, sorted
under `LC_ALL=C` (the locale pin exists because a digest that changes with the reader's
locale cannot be a provenance check), then hashed. Length verified: **64 characters**.

### 1.3 File count and clean-archive verifier result

**193 files**, agreeing across two independent counts:

| method | count |
|---|---|
| `git ls-tree -r --name-only 13d6b74 dev2026` | 193 |
| tar file members (directories excluded) | 193 |

`verify_clean_archive.sh 13d6b74` — **all checks pass**:

```
ok   no .git in the exported tree
ok   git commands cannot reach the source repo from here
ok   bench/package_digests.py IS in the archive
ok   bench/dist_digests.py is NOT in the archive
ok   no file in the archive is named dist_*
ok   nothing in the archive imports dist_digests
ok   imports were found
ok   every imported bench module is in the archive   (21 distinct modules)
ok   compileall over bench and api
ok   every imported bench module loads from inside the archive
ok   test_tracked.sh runs inside the archive, without a repository — exits 0
```

### 1.4 Are the three spec documents inside the subject, or later commits?

**All three are INSIDE subject `13d6b74`.** None is a later protocol-reference commit.
Verified with `git cat-file -e 13d6b74:<path>` for each:

| file | in `13d6b74`? | first committed by |
|---|---|---|
| `specs/018-conformance-comparator-is-pure.md` | **yes** | `13d6b74` itself (13:31:58) |
| `specs/C1-execution-request-c1q.md` | **yes** | `2060c40` (11:17:33) |
| `specs/C1-result-c1q.md` | **yes** | `4cb5eb7` (11:43:41) |

**Correction to my previous report.** I wrote that the two c1q documents "appear here
because they were committed after the c1q subject was cut". That is true of the *c1q*
subject `c62b081` and is why the file count went 188 → 193 — but stated without that
qualifier it invites the reading that they are outside `13d6b74`. They are not. All
five added files are inside `13d6b74`, and the 193-file count and both digests include
them.

The only commit *after* `13d6b74` is `9eed194`, which adds one file —
`specs/C1-offline-fix-result-post-c1q.md` — and is **not** part of the subject.

---

## 2. Proof the three batches ran against exactly `13d6b74`

The batches ran against the **working tree**, at a time when that tree's tracked
`dev2026` content was exactly `13d6b74`. Four independent lines of evidence, then the
limitation stated plainly.

### 2.1 The commit window brackets all three batches

```
13d6b74  committed  13:31:58        <-- the subject
  batch 1   13:42:07 -> 14:05:42
  batch 2   14:05:42 -> 14:29:15
  batch 3   14:29:15 -> 14:52:46
9eed194  committed  14:55:52        <-- the next commit, after every batch ended
```

Timestamps are from each batch's own `manifest.tsv` (first suite start, last suite end),
not from my narration.

### 2.2 No commit exists inside that window

`git log 13d6b74..HEAD` returns exactly one commit, `9eed194` at 14:55:52 — after batch
3 ended at 14:52:46. **HEAD was `13d6b74` for the entire duration of all three batches**,
and could not have been anything else.

### 2.3 `test_tracked.sh` passed in all three batches

Exit 0 in batches 1, 2 and 3. That suite fails on **any** untracked or modified harness
source under `dev2026` — it is precisely what failed in the pre-check run before these
files were committed. Its passing three times is positive evidence that the tree carried
no uncommitted modification and no untracked source while the batches ran.

### 2.4 No tracked file has drifted since

`git diff 13d6b74 HEAD -- dev2026` shows one added file (`9eed194`'s report) and **no
modification to any other tracked file**. `git status --porcelain dev2026` is empty.

### 2.5 The limitation, stated

Points 2.1–2.4 establish that the working tree's tracked `dev2026` content equalled
`13d6b74` throughout. They do **not** make the three batches literally a run of the
commit's extracted archive — they were runs of a working tree that matched it.

A **fourth run, against the commit's own extracted archive**, is reported in §3 to close
that gap directly.

---

## 3. Fourth run: the subject's own extracted archive

The **whole repository** at `13d6b74` extracted via `git archive` to a directory outside
the repository, with **no `.git`**, and the full suite run there. 225 files, of which 193
are `dev2026`. The project `.venv` was symlinked in as the *runtime*; it is not part of
the subject.

**Result: 44 of 46 suites exit 0. The two that do not both fail for one reason:
`fatal: not a git repository`.**

```
root: /var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T/woa23-suites-5qRhls
suites 46   non-zero 2
  test_clean_archive.sh        exit=1   "not a git repository"
  test_production_launcher.sh  exit=1   "not a git repository"
```

Both suites verify **git tracking status** — `deploy/production_app.sh is tracked by
git`, and `verify_clean_archive.sh` itself shells out to git. Neither can run where
there is deliberately no repository. That is a property of the extraction, **not a
defect in the subject**: both exit 0 in all three working-tree batches.

The suites this fix actually changes all pass **inside the archive**:

| suite | exit | result |
|---|--:|---|
| `test_column_contract.py` | 0 | 46 tests, **146 assertions**, 0 failed |
| `test_c1_decided_differences.py` | 0 | all passed (**49 assertions**) |
| `test_contract_row_order.py` | 0 | all passed (69 assertions) |
| `test_docs_only_diff.py` | 0 | all passed (27 assertions) |
| `test_column_order.py` | 0 | all passed (42 assertions) |
| `test_api_surface_ordering.py` | 0 | all passed (34 assertions) |
| `test_tracked.sh` | 0 | all passed (9 assertions, tree-only mode) |

So the fixed comparator is exercised, and passes, from the subject's own archive with no
repository and no working tree behind it.

### 3.0 A first attempt that was invalid, recorded rather than discarded

My first extraction exported only `dev2026`. `test_api_surface_ordering.py` reads the
repository-root `README.md`, which that export did not contain, so the suite failed with
`FileNotFoundError: …/README.md`. **That was my extraction's defect, not the subject's**
— the same suite exits 0 in all three working-tree batches. That attempt was also cut
short at 18 suites. It is reported here because a failed run that is quietly re-attempted
is exactly the evidence-handling this campaign has repeatedly had to correct.

The re-run exports the **whole repository** at `13d6b74`, so root-level files the suites
legitimately read are present.

---

## 4. Execution identity — fresh label, first-use ports

| | value |
|---|---|
| **grant** | `WOA23_S2_C1_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993**, private group only |
| **connection** | direct SSH, campaign Ed25519 key, `BatchMode=yes`, `IdentitiesOnly=yes`, `RequestTTY=no` |
| **label** | **`c1r`** |
| **staging** | `/home/woa23c1ro/woa23-c1r/` |
| **workdir** | `/home/woa23c1ro/woa23-c1r-work/` |
| **HOME** | `/home/woa23c1ro` |
| **TMPDIR** | `/home/woa23c1ro/tmp-c1r/` |
| **candidate arm port** | **`19111`** |
| **reference arm port** | **`19112`** |
| **isolated dask scheduler port** | **`19114`** |
| **`WOA23_EXPECT_UID`** | `994` |

All three staging/workdir/TMPDIR paths are **new** and will be asserted absent in
pre-flight before anything is created, as `c1q`'s were.

### 4.1 Port screening — and one rejection

Each candidate was checked two ways against the **subject's own tree**: zero rows in
`ports_used.tsv`, and zero occurrences *anywhere* in `dev2026`.

| port | mentions in tree | ledger rows | verdict |
|---|--:|--:|---|
| **19111** | 0 | 0 | **CLEAN — candidate arm** |
| **19112** | 0 | 0 | **CLEAN — reference arm** |
| 19113 | **2** | 0 | **REJECTED** |
| 19114 | 0 | 0 | **CLEAN — dask scheduler** |
| 19115 | 0 | 0 | clean, unused |
| 19116 | **3** | 0 | rejected |
| 19117 | 0 | 0 | clean, unused |
| 19118 | **1** | 0 | rejected |

**`19113` was in my previous message and is withdrawn.** It occurs as a coincidental
substring inside the float `107.81911388039589` in `results/paired_s1.json` and
`results/paired_s1_rung21.json` — the identical defect that disqualified `19109` before
`c1q`. Screening only the ledger would have missed it. **`19114` replaces it.**

### 4.2 Deliberately NOT pre-recorded in the ledger

`run_controlled.sh` refuses a port that the subject's own ledger copy names, so
recording `19111`/`19112`/`19114` now would make the run impossible. They are entered as
**BOUND → SPENT** after the run, exactly as `c1p` and `c1q` were.

### 4.3 Identities not reused

Consumed and never to be reused: `c1d`, `c1f`, `c1h`, `c1i`, `c1j`, `c1k`, `c1m`, `c1n`,
`c1p`, `c1q`. **`c1r` has never been used** — zero occurrences in the tree, zero ledger
rows. `c1q`'s ports `19101`/`19102`/`19103` are SPENT and are not reused.

---

## 5. The command

```
ssh -o BatchMode=yes -o IdentitiesOnly=yes -o RequestTTY=no \
    -i ~/.ssh/id_ed25519_odb woa23c1ro@192.168.2.24
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c1r \
  WOA23_S2_C1_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_controlled.sh --c1 \
    --workdir        /home/woa23c1ro/woa23-c1r-work \
    --prod-dir       /home/odbadmin/python/woa23 \
    --store          /home/odbadmin/python/woa23/data \
    --prod-python    /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --python-binary  /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone  /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --prod-pids      "<discovered fresh in this run's own preflight>" \
    --candidate-port 19111 \
    --reference-port 19112 \
    --scheduler-port 19114 \
    --label          c1r
```

No sudo, no sshpass, no privilege escalation, no `odbadmin` launcher.

**Production pids are discovered fresh in this run's own session.** Discriminator:
`woa23_app:app` bound to `127.0.0.1:8050` — module and port both in the cmdline. `pm2G`'s
`api.app` on 18265, `mhw_app` on 8030, `tide_app` on 8040 and ghrsst's `api.app` on 8035
are excluded on that evidence, with the full exclusion list recorded. **Ambiguity is a
stop.** No historical PID is copied from any document.

`/proc/<pid>/exe` is expected to be unreadable as uid 994 and will again be recorded as
`exe_not_readable`, **never** as exe-verified.

---

## 6. What `c1r` will report

The five findings, kept apart, exactly as the c1q report was required to keep them:

1. **raw reconstruction** — per case, `reconstructed` true/false
2. **candidate row-order conformance** — now *checkable*: verified/conformant, or
   UNVERIFIED **with the cause recorded**
3. **API 1.1.0 documentation comparison** — with both bodies retained, not only digests
4. **unexpected differences** — its own class, never merged with either expected class
5. **execution environment** — interpreter, cwd, `sys.path`, env, and any import failure
   with full traceback and chained cause

Plus: fresh PID/starttime evidence, all pre-flight results, the `exe_not_readable`
limitation, UID evidence for every tracked process, request counts, store identity
before and after, cleanup, and final classification.

### 6.1 What a PASS will and will not mean

A PASS will mean the candidate's responses are canonically correct, its column order
**conforms to the spec 015 rule** (checked, not assumed), and every byte difference is
one of the two decided classes, each proven on its own narrow ground.

It will **not** mean anything about latency, throughput, or behaviour at production's
worker count — that is C2's, and **C2 remains blocked**.

It will not retroactively convert `c1q`. `c1q` stays `INCOMPLETE_VALIDATION`.

---

## 7. Scope and forbidden

- **`c1q` is not rerun.** No prior C1 identity is reused.
- `api/query.py` unchanged at `50907dee…2ca8`; the column-order gate is not weakened and
  expected differences are not broadened.
- **C2, latency, deployment and self-rerun: not run.**
- **pm2G, port 18265, its PM2 entry and retained state: not touched.**
- Production store, ACLs, runtime, `.lock` file and permissions: **not modified**.
- Production API requests: **zero**.
- All arms and workers must run as **uid 994**; any identity, path, port, hash, runtime,
  store or UID check failing is an **abort before arms start**.
- All failure evidence preserved.

**Failure handling:** a class-3 unexpected difference is FAIL. Unprovable class-1
conformance is INCOMPLETE_VALIDATION. Neither is reported as a PASS.

---

## 8. Submission

`c1r` is submitted for **explicit authorisation**. It has not been executed. Awaiting
your decision.
