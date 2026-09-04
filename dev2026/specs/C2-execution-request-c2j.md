# C2 `c2j` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. C2 has not been run.**
No VM24 contact has been made for `c2j`. `c1r` and `c1q` evidence is untouched and is
not back-filled.

---

## 1. A blocker found offline, and fixed, before this request

**`run_c2_cycles.sh` could not pass what `run_controlled.sh` requires.**

`run_controlled.sh` makes `--prod-dir`, `--store` and `--prod-python` mandatory under
`WOA23_EXPECT_UID` — falling back to `$HOME` would name a path under the *running*
account's home rather than production's — and `--prod-pids` mandatory for a different
reason: `ss -p` shows a socket's owner only to that owner or root, so a non-owner cannot
discover production's pids and must be told them. All four were added for C1 across
`c1k`/`c1m`/`c1n`/`c1p`.

**`run_c2_cycles.sh` accepted none of them.** A C2 run as `woa23c1ro` would have been
refused by the runner on **cycle 1** — after the wrapper had created a workdir and burned
a label. Found offline while preparing this request, not during a run.

Fixed in `82ecd44`: the four flags are accepted and passed through to every cycle, and
the **wrapper** now refuses, so nothing starts at all. The gate
(`c2_prod_identity_problem`) is pure and sourceable via `WOA23_C2_LIB_ONLY=1`.

### 1.1 A stale comment removed, not left to rot

The wrapper's header said order stability is *"deliberately not part of the verdict:
with no pinned seed a row-order difference is a property of the process, not a defect."*
True before spec 008 §7b gave the candidate a row-order contract; false after.
`c2_summary.py:order_stability` already implemented the split — only the header still
disclaimed it, which is exactly the failure that function's own docstring warns about:
*a function whose comment disclaims a verdict while producing one is how a gate gets
ignored.* The header now states the asymmetry.

### 1.2 Verified: the worker measurement works as a non-owner

C2 **measures** production's worker count from its argv at run time. Under expect-uid,
`PROD_PIDS_BEFORE` comes from the **validated supplied pids** (the `pids_on_port`
fallback is the non-expect-uid branch only), and the count is read from
`/proc/<pid>/cmdline`, which is world-readable. So the measurement is **not**
ownership-dependent. Checked before submitting, not assumed.

---

## 2. Execution subject

The **executable subject is unchanged from the C1r-validated tree**, but the archive is
not, so fresh digests are supplied and a new subject is cut.

```
commit           c7312130d0957ccf9163c6c4af3c187c2ac3a983
subject line     ledger: retire c2h as RETIRED-NEVER-BOUND, superseded by c2j
archive sha256   b96678757d2663627e5f4bc760676dfa7b0f0934f677e1a4b2b096bb0f646c02
archive bytes    4218880
files            196
file-list sha256 0695d3bf2de7b46ed62c4ea8172f08fc7deaa655115d85684f78b3262bc9dbe1
```

`verify_clean_archive.sh` passes **16/16 checks**, no FAIL lines: no `.git` in the
export, no `dist_*`, every imported bench module present, `compileall` clean over
`bench` and `api`, and `test_tracked.sh` exits 0 **inside** the archive with no
repository.

### 2.1 What changed since the C1r subject `13d6b74`

**`api/` and `bench/` diff is EMPTY** — the code C1r validated is byte-identical here.
`api/query.py` = `50907dee…2ca8`, unchanged.

```
scripts/ports_used.tsv     |  9 ++--     ledger: c1r SPENT, c2h retired
scripts/run_c2_cycles.sh   | 90 +++++--  the blocker fix (§1)
scripts/test_c2_driver.sh  | 84 ++++++   its tests
```

Files 193 → 196, the three being the c1r/offline-fix spec documents. **All three are
inside this subject.**

### 2.2 Why a new subject rather than reusing `13d6b74`

Two reasons, both about safety rather than bookkeeping:

1. `13d6b74`'s ledger does **not** record c1r's ports `19111`/`19112`/`19114` as SPENT.
   A run against that subject could not refuse them. This subject's ledger records all
   three (verified: 3 rows), so the runner's own freshness guard is accurate.
2. `13d6b74`'s `run_c2_cycles.sh` carries the §1 blocker. C2 cannot run from it.

### 2.3 Source hashes at the subject, to be verified in pre-flight

```
9a73c25c7e7b5263de256147f7258be91f4d31698ad41b0cd0681fb56779e80a  scripts/run_c2_cycles.sh
6629f2b4664772ce95b812b8436eb0211005ec99b8ade974959c033d0b542d33  scripts/run_controlled.sh
5d1ff7c4cabc6b289933beb8d31f8b06e5b7d14cb3e21e1cada131f8fb441bce  scripts/lib_procs.sh
16f641621e5f058ee68f07e6be26f3bca9417dc39c0380dc213320e8223c3d75  scripts/store_readonly_preflight.sh
4d8825e8c67389a31554bc8ac5aaf2e9cf77bcc69c04448649b1e12ff4186248  scripts/ports_used.tsv
c2b5c6a236e1f28d577ea63d423b2b2a47014cdc817217ce3136bfb39594e2eb  bench/c2_summary.py
862ac7c77f60493db929cb9d82b920dd7ed98fd5e6999f440b713f736c6df099  bench/contract_diff.py
1eff2dc91b55d149b5d1fb317efd99bde635d6c807b931fe8fe49446e3fac4a4  bench/column_contract.py
cbe799426cddabd7437839ef13e1319658f2fda0c14470409ab94e34336d6c8b  bench/contract_cases.py
50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8  api/query.py
15f26444af9fbffbdef8a240833c1ff7f8bc99fdf84277317eb15f859d467ce2  api/app.py
b806641dc7478acaca375380b8e0f8575c1fa48f093362f7f917921a3adc94ca  api/config.py
00cb80c2b1c4ef74f984f42026dcdc5736bfb841e34471bd882c3fd42e35b928  api/store_paths.py
```

### 2.4 Offline evidence for this subject

#### CORRECTION — the first set of batches did NOT run against this subject

Revision 1 of this document claimed "three serial offline batches **on this subject**".
**That was wrong**, and the timeline shows it:

```
82ecd44   16:39:41   the wrapper fix
  batch1  16:39:42 -> 17:03:11
  batch2  17:03:11 -> 17:26:46
  batch3  17:26:46 -> 17:50:43
c731213   17:51:39   THIS SUBJECT — 56 seconds AFTER batch 3 ended
```

Those three batches ran against `82ecd44`, the **parent**. They were launched in the
same step that committed the wrapper fix, and the ledger edit that produced this subject
came afterwards. `scripts/ports_used.tsv` is read by the harness and by the suites, so a
batch run against a different ledger is **not** evidence about this subject. The claim
is withdrawn, not reworded.

#### The batches that DID run against this subject

Re-run in a `git worktree` detached at `c7312130d0957ccf9163c6c4af3c187c2ac3a983` — a
worktree rather than an extraction so the git-dependent suites (`test_tracked.sh`,
`test_clean_archive.sh`, `test_production_launcher.sh`) still have a repository and can
actually run.

**46 suites each, 3899 assertions, ZERO non-zero exits, 0 differences** on all three
pairwise per-suite comparisons.

```
batch 1  /var/folders/z6/…/T/woa23-suites-hbXabH   18:20:02 -> 18:43:51
batch 2  /var/folders/z6/…/T/woa23-suites-SYP8gk   18:43:51 -> 19:07:52
batch 3  /var/folders/z6/…/T/woa23-suites-BCzRDF   19:07:52 -> 19:31:58
```

Each batch recorded `git rev-parse HEAD` **itself**, at the moment it finished, rather
than leaving it to be asserted afterwards:

```
batch1 exit=0 head=c7312130d0957ccf9163c6c4af3c187c2ac3a983 dirty=2
batch2 exit=0 head=c7312130d0957ccf9163c6c4af3c187c2ac3a983 dirty=2
batch3 exit=0 head=c7312130d0957ccf9163c6c4af3c187c2ac3a983 dirty=2
```

`dirty=2` is stated rather than hidden: the two entries are `dev2026/.venv` (the runtime
symlink, which is not part of the subject) and `run3.sh` (the batch launcher, at the
worktree root, outside `dev2026`). **`git diff HEAD` is empty — no tracked file was
modified** — and `test_tracked.sh` exits 0 in all three batches, which is the check that
fails on any untracked or modified harness source.

`test_c2_driver.sh` 72 → **90 assertions**; `test_c2_summary.py` 106, unchanged.

#### The C2 request document is a later protocol reference only

`specs/C2-execution-request-c2j.md` is **NOT inside the subject** — verified with
`git cat-file -e c7312130:…`, which reports it absent. It was added by `0931724` at
17:53:41, after the subject. It is a protocol reference and is not part of the tree C2
executes.

---

## 3. Execution identity — new label, first-use ports

| | value |
|---|---|
| **grant** | `WOA23_S2_C2_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993** |
| **connection** | direct SSH, campaign Ed25519 key, `BatchMode=yes`, `IdentitiesOnly=yes`, `RequestTTY=no` |
| **label prefix** | **`c2j`** (cycles `c2j_cycle1`, `c2j_cycle2`, `c2j_cycle3`) |
| **staging** | `/home/woa23c1ro/woa23-c2j/` |
| **workdir base** | `/home/woa23c1ro/woa23-c2j-work/` (per cycle: `…-work-cycle1/2/3`) |
| **HOME** | `/home/woa23c1ro` |
| **TMPDIR** | `/home/woa23c1ro/tmp-c2j/` |
| **candidate arm port** | **`19131`** |
| **reference arm port** | **`19132`** |
| **isolated dask scheduler port** | **`19133`** |
| **`WOA23_EXPECT_UID`** | `994` |
| **`--expected-workers`** | `2` (asserted; the arms take production's *measured* count) |

### 3.1 Port screening — both ways

Screened against the **subject's own ledger** *and* every occurrence anywhere in
`dev2026`, the two-way screen that caught 19113 before c1r and 19109 before c1q:

| port | tree mentions | ledger rows | verdict |
|---|--:|--:|---|
| **19131** | 0 | 0 | **CLEAN — candidate** |
| **19132** | 0 | 0 | **CLEAN — reference** |
| **19133** | 0 | 0 | **CLEAN — scheduler** |
| 19136 | 2 | 0 | rejected (substring of a float in results JSON) |

**Not pre-recorded**: `run_controlled.sh` refuses a port the subject's ledger names.
Entered BOUND → SPENT after the run.

### 3.2 Labels not reused

C2 labels consumed: `c2c`, `c2e`, `c2f`, `c2g`. **`c2h` is now
`RETIRED-NEVER-BOUND`** — allocated for a re-run request that was never submitted, its
ports `18311`/`18312`/`18959` never carried a listener; superseded by `c2j`.
**`c2j` has never been used.** No C1 identity is reused, and `c1r`/`c1q` are untouched.

---

## 4. The command

```
ssh -o BatchMode=yes -o IdentitiesOnly=yes -o RequestTTY=no \
    -i ~/.ssh/id_ed25519_odb woa23c1ro@192.168.2.24
  cd /home/woa23c1ro/woa23-c2j/dev2026 &&
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c2j \
  PATH=/home/woa23c1ro/.local/bin:$PATH \
  WOA23_S2_C2_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_c2_cycles.sh \
    --python-binary   /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone   /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest  /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --workdir-base    /home/woa23c1ro/woa23-c2j-work \
    --prod-dir        /home/odbadmin/python/woa23 \
    --store           /home/odbadmin/python/woa23/data \
    --prod-python     /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --prod-pids       "<discovered fresh in this run's own preflight>" \
    --candidate-port  19131 \
    --reference-port  19132 \
    --scheduler-port  19133 \
    --expected-workers 2 \
    --label-prefix    c2j
```

No sudo, sshpass, privilege escalation or odbadmin launcher. **No
`--allow-reused-ports`.**

**Three cycles, and only three.** `CYCLES=3` is fixed in the wrapper and there is no
`--cycles` flag; `--cycles` is explicitly refused. **Seeds are unpinned** —
`--c2-cycle` sets no `PYTHONHASHSEED`, which is the whole question C2 asks.

---

## 5. Pre-flight — before any arm starts

Run as a separate step and recorded in full, exactly as `c1r`'s was:

1. connection identity is `uid=994(woa23c1ro) gid=993`, not `odbadmin`;
2. `uv` at **`/home/woa23c1ro/.local/bin/uv`** — path, executability, version, sha256;
3. **production PIDs discovered fresh in this run**, discriminator `woa23_app:app`
   bound to `127.0.0.1:8050`, with **every excluded gunicorn listed**;
4. every PID validated against `/proc/<pid>/stat` — **pid *and* starttime** — and
   `/proc/<pid>/cmdline`;
5. **`ss -ltn` only** for listener presence, never `ss -ltnp`;
6. `/proc/<pid>/exe` recorded as **`exe_not_readable`** if unreadable, never as verified;
7. **store identity captured BEFORE any decision** — mode, owner, mtime, file count,
   bytes, metadata fingerprint;
8. **complete store read-only scan as uid 994** — `store_readonly_preflight.sh`, which
   uses `stat`/`test -w` only and writes nothing;
9. archive, file count, file-list and every per-file hash in §2.3 verified;
10. staging/workdir/TMPDIR absent; all three ports unbound **and** absent from the
    subject's ledger;
11. production baseline (boot id, pids, starttimes, listeners) and **pm2G baseline**
    (18265 bound, its three pids running) recorded.

**Abort before starting arms** on: any hash mismatch, ambiguous production PID, PID
reuse (starttime changed), writable store, wrong UID, occupied path or port, or
uncertain identity. Ambiguity is a stop, not a judgement call.

---

## 6. What C2 gates on, and what it merely observes

| | treatment |
|---|---|
| **5.2B semantic gate** | **VERDICT — unchanged.** PASS only if **every** cycle passed. |
| **candidate row-order conformance** | **VERDICT.** Per-response, on every applicable response. |
| **candidate row-order stability across all 3 cycles** | **VERDICT.** Variation is `ROW_ORDER_CONTRACT_FAILURE`. |
| **reference-side variation** | **OBSERVATION.** Recorded, never gated. |
| **seed diversity** | **OBSERVATION.** Reported, never an escalation; three distinct seeds expected, and a fourth cycle is never run. |
| **missing conformance evidence** | **`INDETERMINATE`** — not a pass. |

**`ROW_ORDER_CONTRACT_FAILURE` is kept separate from semantic divergence** — its own
outcome with its own exit code (6) in `OUTCOME_EXIT`, never folded into the 5.2B verdict.

The asymmetry is deliberate: the **reference** does not implement spec 008's row order,
so with no pinned seed its order is a property of its process and variation is expected.
The **candidate** does implement it, and three independent starts with three different
seeds are precisely the circumstance under which the old order varied — so candidate
variation is a defect, not noise.

`INDETERMINATE` fails closed: a cycle whose contract artefact carries no per-case
conformance record cannot show the contract held, and a run that cannot show it is not a
run that passed.

---

## 7. Scope and forbidden

- **Production API requests: zero.** Nothing in C2 addresses `127.0.0.1:8050`; the arms
  talk only to 19131/19132. Verified in the run's own request budget and re-checked
  after.
- **pm2G, port 18265, its PM2 entry and retained state: not touched.**
- **No latency, startup, deployment or PM2 validation.** C2 is the semantic and
  row-order question only; this request authorises no timing claim of any kind.
- **No back-filling** of `c1r` or `c1q` evidence. Both stand as recorded — `c1r` PASS,
  `c1q` INCOMPLETE_VALIDATION.
- Production store, ACLs, runtime, `.lock` file, permissions: **not modified**.
- `api/query.py` unchanged; no gate weakened, no expected difference broadened.
- All arms and workers must run as **uid 994** in every cycle.
- No self-rerun. No fourth cycle. All failure evidence preserved.

### 7.1 Failure handling

| condition | outcome |
|---|---|
| any cycle fails 5.2B | **FAIL** — the wrapper stops; two cycles plus a failure is not two thirds of an answer |
| candidate order varies across cycles | **`ROW_ORDER_CONTRACT_FAILURE`**, exit 6, separate from semantic divergence |
| conformance record missing | **`INDETERMINATE`** — not a pass |
| fewer than three distinct seeds | **`INSUFFICIENT`** observation; the run stops and **does not** add a cycle |
| a cycle's cleanup unconfirmed | stop; no further cycle starts, state preserved for inspection |
| preflight mismatch of any kind | **abort before any arm starts** |

---

## 8. Submission

`c2j` is submitted for **explicit authorisation**. It has not been executed and no VM24
contact has been made. Awaiting your decision.
