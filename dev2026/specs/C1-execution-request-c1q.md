# C1 `c1q` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. C1 has not been run.** No VM24 contact
during this fix. **This document is not an authorisation.**

Supersedes [C1-execution-request-c1p.md](C1-execution-request-c1p.md). `c1p` is **CONSUMED**
— [C1-result-c1p.md](C1-result-c1p.md), `INCOMPLETE_VALIDATION`. **Its ports were BOUND and
are SPENT.** Neither its label, its ports nor its staging tree are reused, and it is not
back-filled as a clean C1 result.

---

## 1. The `C20a` investigation — resolved, and it is not a candidate defect

**Required before any classification. Evidence chain, entirely offline:**

| step | finding |
|---|---|
| [spec 001](001-remove-dask-read-path.md) | records `C20a` as **`8597/8597`** — **matching**. `8597` is the reference's historical, unchanged size |
| candidate's OpenAPI, generated locally | **exactly `9625` bytes** — the figure `c1p` observed — carrying **`info.version: 1.1.0`** and the row-order statement |
| `api/app.py:43,47` | hold that version and description |
| [spec 008](008-s2b-deterministic-row-order.md) **revision 6** (2026-08-19) | **published API 1.1.0**, and states that applying it *"changes `api/` and would require a new C1/C2"* |
| spec 008 **revision 5** | records **`c1f` and `c2g` passing BEFORE** that publication |

**Classification: `C20a` is spec 008's own decided documentation change, anticipated by
spec 008 itself.** It is **not** spec 015, **not** a regression, and **not** a candidate
defect.

**`api/query.py` is therefore unchanged**, per the instruction to alter nothing unless the
investigation proved a defect. It did not. The decided column-order implementation is
preserved exactly.

**It is not called "expected because it is documentation-sized."** The harness proves the
claim structurally — §2.2.

## 2. What changed in the harness

`c1p` reached the gate and returned FAIL on 5 of 64, producing no verdict because nothing
could say *"this difference was decided"*. **Five findings are now reported separately:**

`expected_column_order_diffs` · `column_order_conformance` ·
`expected_documentation_diffs` · `column_reconstruction` · `regressions`

### 2.1 Column order — accepted only when PROVEN

**The reference's own rows, permuted into the candidate's column sequence, must reproduce
the candidate's bytes EXACTLY.**

**Still refused, and tested:** a value change hiding behind a permutation
(reconstruction fails → **REGRESSION**); a column **SET** difference (a real defect); a
row-count difference. **If the spec 015 rule cannot be imported, conformance is
`UNVERIFIED` and the gate is `INCOMPLETE_VALIDATION` — never a silent pass.**

### 2.2 Documentation — accepted only when structurally confined

**The two OpenAPI documents must be identical once `version`, `summary` and `description`
are stripped.** A **new route**, **new response code** or **new parameter** survives that
normalisation and **still fails**. A non-OpenAPI JSON object does not qualify at all.

**This is what stops "it is the documentation surface, so it is fine" from being the
check.**

### 2.3 The post-run production check — and a third site

Now uses the same non-owner-safe mechanism as the pre-run check: `port_is_listening` for
presence, per-pid `/proc` re-validation for identity, and **PID reuse caught by starttime,
not by the number**.

**Grepping for every remaining call then found a THIRD ownership-dependent site** — a
mid-run recheck — also converted. The three that remain are all in non-expect-uid
branches. **The lesson is that the fix was never one line**, and it is recorded in the code
as such.

### 2.4 One superseded assertion updated, not deleted

`test_contract_row_order.py` asserted *"a moved COLUMN is caught — column order is out of
scope and must not move"*. That was spec 008's decision and was correct until 015 made the
candidate's order a contract. It now asserts a permuted sequence is **no longer** a
canonical mismatch **and** that a column **SET** difference **still is** — keeping the
sensitivity the original protected.

### 2.5 The final report will separate THREE classes, and never merge them

**Required, and stated here so the report cannot quietly collapse them:**

| # | class | source |
|---|---|---|
| **1** | **spec 015 parameter-major column order** | `expected_column_order_diffs` + `column_order_conformance` + `column_reconstruction` |
| **2** | **spec 008 API/OpenAPI version 1.1.0** | `expected_documentation_diffs` |
| **3** | **unexpected differences, if any** | `regressions` |

**Class 1 and class 2 are separate findings with separate causes**, and each is proven by
its own mechanism — reconstruction for the first, structural normalisation for the second.
Reporting them as one bucket of "expected differences" would lose which proof was actually
made.

**Neither is a blanket exemption.** A byte difference is admitted to class 1 only if the
permutation reconstructs exactly, and to class 2 only if the documents are identical once
version, summary and description are stripped. **Everything else lands in class 3 and
fails the gate** — including a value change hiding behind a column permutation, a column
SET difference, and a new route, response code or parameter in the OpenAPI document.

**If class 3 is non-empty the gate is FAIL. If any class-1 reconstruction cannot be proven
the gate is `INCOMPLETE_VALIDATION`.** Neither may be reported as a PASS.

## 3. Execution subject

| item | value |
|---|---|
| **commit** | `c62b0811e02c984a31bdcfc0c042ff5f94b76014` |
| **archive SHA-256** | `fd9fc11ba33c8792a43940e227dd33dc288e7ad1ef9db983615abd6330bb0214` |
| **file count** | `188` |
| **file-list SHA-256** | `4e7df01771bb131d417931bf65f0a3e0781fb0732e7e084f498bfb576314059c` |
| **verifier** | `scripts/verify_clean_archive.sh c62b0811e02c984a31bdcfc0c042ff5f94b76014` → **16/16** |

**Supersedes `2d6f812`** (185 files). **185 → 188**: five modified
(`contract_diff.py`, `test_contract_row_order.py`, `run_controlled.sh`,
`test_c1_readonly_account.sh`, `ports_used.tsv`) and three added —
`bench/test_c1_decided_differences.py` plus the `c1p` request and result.

### 3.1 Full source hashes at the subject

| SHA-256 | file |
|---|---|
| `6629f2b4664772ce95b812b8436eb0211005ec99b8ade974959c033d0b542d33` | `scripts/run_controlled.sh` |
| `5d1ff7c4cabc6b289933beb8d31f8b06e5b7d14cb3e21e1cada131f8fb441bce` | `scripts/lib_procs.sh` |
| `16f641621e5f058ee68f07e6be26f3bca9417dc39c0380dc213320e8223c3d75` | `scripts/store_readonly_preflight.sh` |
| `3f2a6f80a9b8ac8ed1aa5676cbd234e7f907e71fc14b081c521daa98dacc9ade` | `scripts/test_c1_readonly_account.sh` |
| `f8991a002d07af3c8894c0f5ed2eb53b3d2ea5de9665184a0a7100e5e9cba956` | `scripts/ports_used.tsv` |
| `f9e174b0ae6351bd4abb99370a50b864e168172fef72f6c4a1b5679d1743bb89` | `scripts/verify_clean_archive.sh` |
| `c13d73b1835b51d61e69f3035b978327ccb4ec99c02cca2ec6ad25571560093d` | `bench/contract_diff.py` |
| `cbe799426cddabd7437839ef13e1319658f2fda0c14470409ab94e34336d6c8b` | `bench/contract_cases.py` |
| `c2b5c6a236e1f28d577ea63d423b2b2a47014cdc817217ce3136bfb39594e2eb` | `bench/c2_summary.py` |
| `7977e18263a88bca882ea7f46fb815a758899eb06d6227aef7763f7d393dd9e3` | `bench/clone_integrity.py` |
| `7de557cbdcf9159d4401fc9ad41473811c313d68de8a52abfa11a898d1248c04` | `bench/provenance.py` |
| `97f50f6ce20f3e13ddb2ea607407137cdb9c07a927dd339e5a55920744b7e083` | `bench/test_c1_decided_differences.py` |
| **`50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8`** | **`api/query.py` — unchanged** |
| `15f26444af9fbffbdef8a240833c1ff7f8bc99fdf84277317eb15f859d467ce2` | `api/app.py` |
| `b806641dc7478acaca375380b8e0f8575c1fa48f093362f7f917921a3adc94ca` | `api/config.py` |
| `00cb80c2b1c4ef74f984f42026dcdc5736bfb841e34471bd882c3fd42e35b928` | `api/store_paths.py` |
| `aa846b8be70b0b5d466d0e2a0bbb1f4dfe6ccac5d28f795a57c1c6bbea7e378e` | `pyproject.toml` |
| `0d2980a5928d4d0964d6cb3b78bffae14aa11a70d3b51ca00f4cf39073dccc69` | `uv.lock` |

**`git diff ad3f428..c62b081 -- dev2026/api/` is EMPTY.** The candidate under test has not
moved across `c1k`, `c1m`, `c1n`, `c1p` or `c1q` — **only the harness has**.

### 3.2 Offline evidence

| | |
|---|---|
| batches | **three, strictly serial** |
| total runs | **135** (45 suites × 3) |
| **non-zero exits** | **0** |
| evidence root | `/var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T//woa23-suites-vuviSM/` (retained) |
| `test_c1_decided_differences.py` | **38 assertions** (new) |
| `test_c1_readonly_account.sh` | **145 assertions** (was 135) |
| `test_contract_row_order.py` | **69 assertions** |
| `fail_lines` | **3** — the collector matching a section *heading* in `test_symmetric_warmup.py`, which exits `0` |

**One batch was discarded before this one**, and the reason is worth recording:
`test_tracked.sh` failed 3/3 because `test_c1_decided_differences.py` was **staged but not
committed** — the export had 54 bench modules against the tree's 55. **That guard did
exactly its job**; a file that passes every check in the working tree and is absent from
the commit is the defect `verify_clean_archive.sh` exists for. Committing first and
re-running gave the clean 135.

**Retained roots, none cleaned:** `woa23-suites-pwOSWF`, `uUJ4FC`, `nKv4kX`, `9Xr3Ay`, the
discarded `zeIaFJ`, and on VM24 the `c1k`, `c1m`, `c1n` and `c1p` trees with
`results/c1p_*.json`.

## 4. Execution identity — fresh label, first-use ports

| | value |
|---|---|
| **grant** | `WOA23_S2_C1_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993**, private group only |
| **connection** | direct SSH, `mac-claude` Ed25519 key, `BatchMode=yes`, `RequestTTY=no` |
| **label** | **`c1q`** |
| **staging** | `/home/woa23c1ro/woa23-c1q/` |
| **workdir** | `/home/woa23c1ro/woa23-c1q-work/` |
| **HOME** | `/home/woa23c1ro` |
| **TMPDIR** | `/home/woa23c1ro/tmp-c1q/` |
| **candidate arm port** | **`19101`** |
| **reference arm port** | **`19102`** |
| **isolated dask scheduler port** | **`19103`** |
| **`WOA23_EXPECT_UID`** | `994` |

**All three are first-use** — zero ledger rows, zero mentions anywhere in the tree, outside
`18241–18999`. `19109` was considered and rejected: it appears as a coincidental substring
inside floats in `results/paired_s1*.json`. **Deliberately NOT pre-recorded in the ledger.**

**Retired and never reused:** `c1h` `18301`/`18302`/`18949`, `c1i` `18321`/`18322`/`18969`,
`c1j` `18341`/`18342`/`18979`, `c1k` `18361`/`18362`/`18989`, `c1m` `19051`/`19052`/`19059`,
`c1n` `19071`/`19072`/`19079` — all **RETIRED-NEVER-BOUND**; and **`c1p`
`19091`/`19092`/`19099` — BOUND and SPENT**, the only C1 ports that ever carried a
listener.

## 5. The command

```
ssh -o BatchMode=yes -o RequestTTY=no -i ~/.ssh/id_ed25519_odb woa23c1ro@192.168.2.24
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c1q \
  WOA23_S2_C1_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_controlled.sh --c1 \
    --workdir        /home/woa23c1ro/woa23-c1q-work \
    --prod-dir       /home/odbadmin/python/woa23 \
    --store          /home/odbadmin/python/woa23/data \
    --prod-python    /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --python-binary  /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone  /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --prod-pids      "<discovered fresh in this run's preflight>" \
    --candidate-port 19101 \
    --reference-port 19102 \
    --scheduler-port 19103 \
    --label          c1q
```

**Production pids are discovered fresh in this run's own session**, with the precise
discriminator `c1p` established: **`woa23_app:app` bound to `127.0.0.1:8050`** — module and
port both in the cmdline. `pm2G`'s `api.app` on 18265, `mhw_app` on 8030, `tide_app` on
8040 and ghrsst's `api.app` on 8035 are excluded on that evidence. **Ambiguity is a stop.**

**`/proc/<pid>/exe` is expected to be unreadable** and will again be recorded as
`exe_not_readable`, never as exe-verified.

## 6. Execution sequence

Unchanged from `c1p`, which executed every step through the gate, plus the corrected
post-run check: connection identity → **store identity before any decision** → complete
read-only scan as 994 → subject and per-file hashes → ports (ledger + `ss`) → identity
absence → production baseline → **fresh pid discovery, ambiguity is a stop** → pm2G
recorded → required tools → shared environment → **production identity via supplied pids**
→ arms started → **every process asserted uid 994** → **C1 contract validation with the
five findings** → cleanup, ports confirmed free → **production re-read via the corrected
non-owner-safe check** and **store identity re-captured**.

## 7. Scope, forbidden, failure handling

**Scope: C1 contract validation only.** No C2, latency, warm-up, noise pilot, startup,
deployment or PM2 validation.

**Forbidden:** privilege escalation; launching from `odbadmin`; any write, `chmod`,
`chown`, delete or rename under the production store; any modification of the store, ACLs,
runtime files, the `.lock`, permissions, any account, or production's `uv`; any HTTP
request to production (**8050 / 8786 / 8787 stay at zero**); any change to production PM2,
processes, listeners or `conf/`; **touching `pm2G`** — not port `18265`, its PM2 entry or
retained state; re-running or back-filling `c1h`, `c1i`, `c1j`, `c1k`, `c1m`, `c1n` or
`c1p`; back-filling `c1f`, `c2g`, `s2pB` or `pm2G`; **SIGKILL**; **self-rerun**; cleanup
beyond the authorised C1 path.

**On any failing step: STOP, retain, report, wait.** If reconstruction or any required
verification cannot be proven, the classification is **`INCOMPLETE_VALIDATION`** and **no
PASS is reported**. **Never reportable as a `5.2A raw byte-exact PASS`** — the bytes differ
by design.

## 8. What a PASS will and will not mean

**Will:** the `015` candidate satisfies the C1 contract gates against the **real production
store**, with every arm master and worker proven uid 994, production's identity proven
unchanged across the run, and **every byte difference either byte-identical, or a decided
change PROVEN by reconstruction** — column order permuting exactly, documentation confined
to version and description.

**Will not:** **not a `5.2A raw byte-exact PASS`; not a staging PASS; not a production
cutover PASS; not a latency result; not a deployment result; not a claim of runtime
isolation.** **B1–B5 remain open. B7 remains open. `pm2G` remains NOT A PASS. C2 remains
blocked** until a clean C1 validation completes.

## 9. Submission

- **Subject: `c62b0811e02c984a31bdcfc0c042ff5f94b76014`.** This document and any later
  commit are **protocol references** and must never be back-filled as the execution subject.
- **Awaiting explicit authorisation. C1 will not be run until it is given.**
