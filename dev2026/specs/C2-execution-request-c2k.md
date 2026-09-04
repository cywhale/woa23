# C2 `c2k` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. C2 has not been run.**
No VM24 contact was made during this fix. `c2j` was not rerun and remains
**NO C2 RESULT**; `c1r` and `c1q` evidence is untouched and not back-filled.

---

## 1. What was fixed, and what was deliberately NOT touched

`c2j` failed because the 5.2B gate had no way to say *"this difference was decided"*.
`compare_semantic` falls back to raw bytes for a non-row payload and never classifies at
all, so `C20a` — the OpenAPI 1.0.0 → 1.1.0 publication `c1r` had already proved
documentation-only — failed cycle 1 and stopped the run.

### 1.1 The 5.2B semantic comparator is UNCHANGED

Verified byte-for-byte against the `c2j` subject: `compare_semantic` hashes
`554d0133ce51ce6d2d3f33289062ffcafce326cd3691ba2ef749fa67be5307a9` at both `c7312130`
and here. Teaching the comparator to forgive bytes would have blunted it for **every**
case. It keeps saying exactly what it said; a **classification layer above it** decides,
separately, whether what it said is the one known change.

### 1.2 The permission is narrower than C1's

| | C1 (`c1r`) | C2 (this layer) |
|---|---|---|
| version pair | any pair, if docs-only | **exactly `1.0.0` → `1.1.0`** |
| structural test | strip `version`/`summary`/`description`, compare | **the same, reused** |

`C2_EXPECTED_DOC_VERSIONS = ("1.0.0", "1.1.0")` is pinned. `1.1.0 → 1.2.0`,
`1.0.0 → 2.0.0`, a downgrade and `1.0.0 → 1.1.0-rc1` are all **refused**.

### 1.3 Three classes, kept apart

An expected documentation difference is **not a semantic MATCH** — the case's verdict
stays `DIFFER` and its notes are kept verbatim, so the record still shows what differed
— and **not a regression**: it does not enter `failed`. It is printed on its own line
and stored in its own artefact field, `expected_documentation_diffs`.

**Status still gates.** A documentation change cannot excuse a status change.

### 1.4 Nothing is broadly forgiven

Refused, each with its own test: a case carrying a **second note** (a second problem is
not this one); a non-200 pair; a non-JSON payload; a JSON object that is not an OpenAPI
document; a **row payload whose values changed**; and any new route, changed route, new
response code, new parameter, dropped parameter or changed schema.

**Exact bodies are retained**, so the classification can be **re-audited from the
artefact** rather than re-derived from a digest — a test replays the retained bytes and
reproduces the verdict.

---

## 2. Execution subject

```
commit           a5ca913c58520f59d3f078b7f1b38813e4d2d900
subject line     fix(c2): classify the decided 1.1.0 docs change ABOVE an unchanged
                 5.2B gate
archive sha256   6dc127cebf1c56e2ff7f5c373b628a1fbe1df1524fceac4087f0a4a05218e318
archive bytes    4270080
files            199
file-list sha256 51698bddbef08c058f1c061a55b398a0bddb47168c64782eb1cd9cfa0972f331
```

`verify_clean_archive.sh` — **all passed (16 assertions)**, no FAIL.

**`api/query.py` unchanged** at `50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8`.

### 2.1 Source hashes at the subject, to be verified in pre-flight

```
663e60b0576273cf489bcc07c3c6eb3ad9d9f00063cf518c64927d10848be48a  bench/contract_diff.py
c2b5c6a236e1f28d577ea63d423b2b2a47014cdc817217ce3136bfb39594e2eb  bench/c2_summary.py
ffaadb9b5e915678755b854c9dcc8c37729377c9b7b1c0c940ad1685f625a4f0  bench/test_c2_expected_docs.py
9a73c25c7e7b5263de256147f7258be91f4d31698ad41b0cd0681fb56779e80a  scripts/run_c2_cycles.sh
6629f2b4664772ce95b812b8436eb0211005ec99b8ade974959c033d0b542d33  scripts/run_controlled.sh
35d15ca7c40cd88455c618cd395919225083d9f31a0cfe6a21ed2db90be892d6  scripts/ports_used.tsv
50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8  api/query.py
15f26444af9fbffbdef8a240833c1ff7f8bc99fdf84277317eb15f859d467ce2  api/app.py
b806641dc7478acaca375380b8e0f8575c1fa48f093362f7f917921a3adc94ca  api/config.py
```

`bench/c2_summary.py`, `scripts/run_c2_cycles.sh` and `scripts/run_controlled.sh` are
**byte-identical to the c2j subject** — the fix is confined to `contract_diff.py` and
its new test.

### 2.2 Offline evidence — run against THIS subject

The `c2j` request got this wrong: its batches ran against the parent. This time the
batches ran in the main working tree **at `a5ca913` with nothing uncommitted**, and each
recorded `git rev-parse HEAD` itself on finishing:

```
batch1 exit=0 head=a5ca913c58520f59d3f078b7f1b38813e4d2d900 dirty=0
batch2 exit=0 head=a5ca913c58520f59d3f078b7f1b38813e4d2d900 dirty=0
batch3 exit=0 head=a5ca913c58520f59d3f078b7f1b38813e4d2d900 dirty=0
```

**`dirty=0` on all three** — no untracked or modified file of any kind, so there are no
worktree helpers to declare this time.

**47 suites each, 3962 assertions, ZERO non-zero exits, 0 differences** on all three
pairwise per-suite comparisons.

```
batch 1  /var/folders/z6/…/T/woa23-suites-oTG55I   20:29:38 -> 20:53:26
batch 2  /var/folders/z6/…/T/woa23-suites-3npl6C   20:53:26 -> 21:17:20
batch 3  /var/folders/z6/…/T/woa23-suites-pB3J9R   21:17:20 -> 21:41:18
```

| suite | assertions |
|---|--:|
| `test_c2_expected_docs.py` | **63** (new) |
| `test_c2_driver.sh` | 90 |
| `test_c2_summary.py` | 106 |
| `test_docs_only_diff.py` | 27 |

Suite count 46 → 47; assertions 3899 → **3962**.

**This request document is a later commit and is NOT part of the subject.**

---

## 3. Execution identity — new label, first-use ports

| | value |
|---|---|
| **grant** | `WOA23_S2_C2_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993** |
| **connection** | direct SSH, campaign Ed25519 key, `BatchMode=yes`, `IdentitiesOnly=yes`, `RequestTTY=no` |
| **label prefix** | **`c2k`** (cycles `c2k_cycle1`, `c2k_cycle2`, `c2k_cycle3`) |
| **staging** | `/home/woa23c1ro/woa23-c2k/` |
| **workdir base** | `/home/woa23c1ro/woa23-c2k-work/` (per cycle `…-work-cycle1/2/3`) |
| **HOME** | `/home/woa23c1ro` |
| **TMPDIR** | `/home/woa23c1ro/tmp-c2k/` |
| **candidate arm port** | **`19134`** |
| **reference arm port** | **`19135`** |
| **isolated dask scheduler port** | **`19137`** |
| **`WOA23_EXPECT_UID`** | `994` |
| **`--expected-workers`** | `2` (asserted; the arms take production's *measured* count) |

### 3.1 Port screening — both ways, at the subject

| port | tree mentions | ledger rows | verdict |
|---|--:|--:|---|
| **19134** | 0 | 0 | **CLEAN — candidate** |
| **19135** | 0 | 0 | **CLEAN — reference** |
| **19137** | 0 | 0 | **CLEAN — scheduler** |

`19136` was rejected earlier by the same two-way screen (it occurs in the tree), which is
why the scheduler port is `19137` and not the next number. `c2j`'s
`19131`/`19132`/`19133` are recorded **SPENT** in this subject's ledger (3 rows), so the
runner's own freshness guard would refuse them.

**Not pre-recorded** — `run_controlled.sh` refuses a port its subject's ledger names.
Entered BOUND → SPENT after the run.

### 3.2 Labels not reused

Consumed: `c2c`, `c2e`, `c2f`, `c2g`, `c2j`; `c2h` is `RETIRED-NEVER-BOUND`.
**`c2k` has never been used.** No C1 identity is reused.

---

## 4. The command

```
ssh -o BatchMode=yes -o IdentitiesOnly=yes -o RequestTTY=no \
    -i ~/.ssh/id_ed25519_odb woa23c1ro@192.168.2.24
  cd /home/woa23c1ro/woa23-c2k/dev2026 &&
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c2k \
  PATH=/home/woa23c1ro/.local/bin:$PATH \
  WOA23_S2_C2_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_c2_cycles.sh \
    --python-binary    /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone    /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest   /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --workdir-base     /home/woa23c1ro/woa23-c2k-work \
    --prod-dir         /home/odbadmin/python/woa23 \
    --store            /home/odbadmin/python/woa23/data \
    --prod-python      /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --prod-pids        "<discovered fresh in this run's own preflight>" \
    --candidate-port   19134 \
    --reference-port   19135 \
    --scheduler-port   19137 \
    --expected-workers 2 \
    --label-prefix     c2k
```

No sudo, sshpass, privilege escalation or odbadmin launcher. **No
`--allow-reused-ports`.** Three cycles and only three — `CYCLES=3` is fixed and
`--cycles` is refused. **Seeds unpinned.**

---

## 5. Pre-flight — before any arm starts

Identical in kind to `c2j`'s, which passed in full: identity `uid=994`, not `odbadmin`;
`uv` at `/home/woa23c1ro/.local/bin/uv` with path, version, sha256 and mode; production
python; **fresh production PID discovery** with every excluded gunicorn listed; **pid and
starttime** validated via `/proc`; `ss -ltn` only for listener presence;
`/proc/<pid>/exe` recorded as `exe_not_readable` if unreadable and never as verified;
**store identity captured before any decision**; **complete store read-only scan as
uid 994**; archive, file count, file-list and every per-file hash in §2.1; staging,
TMPDIR and all three per-cycle workdirs absent; all three ports **live-unbound** and
absent from the subject's ledger; production and **pm2G** baselines recorded.

**Abort before starting arms** on any hash mismatch, ambiguous production PID, PID reuse,
writable store, wrong UID, occupied path or port, or uncertain identity.

---

## 6. What C2 gates on, and what it merely observes — unchanged

| | treatment |
|---|---|
| **5.2B semantic gate** | **VERDICT — comparator unchanged.** PASS only if every cycle passed. |
| **candidate row-order conformance** | **VERDICT**, per applicable response. |
| **candidate row-order stability across all 3 cycles** | **VERDICT.** Variation is `ROW_ORDER_CONTRACT_FAILURE`. |
| **reference-side variation** | **OBSERVATION.** Recorded, never gated. |
| **seed diversity** | **OBSERVATION.** Never an escalation; **no fourth cycle**, even if insufficient. |
| **missing conformance evidence** | **`INDETERMINATE`** — counted, named, never a pass. |
| **the decided 1.0.0 → 1.1.0 documentation change** | **EXPECTED** — its own class, apart from MATCH and from regressions. |

`ROW_ORDER_CONTRACT_FAILURE` keeps its own exit code (6) and stays separate from
semantic divergence.

---

## 7. Scope and forbidden

- **Production API requests: zero.**
- **pm2G, port 18265, its PM2 entry and retained state: not touched.**
- **No latency, startup, deployment or PM2 validation.** No timing claim of any kind.
- **No back-filling** of `c2j`, `c1r` or `c1q`. `c2j` stays NO C2 RESULT.
- Production store, ACLs, runtime, `.lock` file, permissions: **not modified**.
- `api/query.py` unchanged; no gate weakened; raw-byte differences not broadly ignored.
- All arms and workers as **uid 994** in every cycle. No self-rerun. No fourth cycle.

### 7.1 Failure handling

| condition | outcome |
|---|---|
| any cycle fails 5.2B | **FAIL** — the wrapper stops; no further cycle |
| candidate order varies across cycles | **`ROW_ORDER_CONTRACT_FAILURE`**, exit 6 |
| conformance record missing | **`INDETERMINATE`** — not a pass |
| a documentation difference that is not the decided pair | **regression** |
| fewer than three distinct seeds | **`INSUFFICIENT`** observation; run stops, no extra cycle |
| a cycle's cleanup unconfirmed | stop; state preserved |
| preflight mismatch | **abort before any arm starts** |

---

## 8. Submission

`c2k` is submitted for **explicit authorisation**. It has not been executed and no VM24
contact has been made. Awaiting your decision.
