# Offline fix after `c1q` — result, and the new subject and C1 identity

**No VM24 contact. `c1q` was not rerun.**
**`c1q` remains `INCOMPLETE_VALIDATION` and is not back-filled as a C1 PASS.**

---

## 1. What was fixed

### 1.1 The comparator no longer imports `api.query`

`c1q` could not check candidate column-order conformance at all — not for four cases,
but **in any run, by construction**. The comparator reached the spec 015 rule through
`api.query`, which imports `api.config`, whose line 33 is an unguarded
`os.environ["WOA23_ZARR_STORE"]` **at module import time**. The arms set that variable;
the comparator runs in the harness process, which never does.

[`bench/column_contract.py`](../bench/column_contract.py) is the fix: the rule as a pure
function of explicit inputs. No `api.*` import, no environment variable at import time
or ever, no filesystem, no network, no zarr store — stdlib only.

It **restates** the contract spec 015 §4 decided; `api/query.py` **implements** it. Two
separate things on purpose: a comparator that imported the implementation could only
ever prove the implementation agrees with itself, which is not a check. Drift is caught
by test rather than hoped away — the mirror is cross-checked against `api.query`
whenever `api.query` *is* importable, and against `api/query.py`'s source literal for
the per-grid parameter list (which is a literal, not a name).

### 1.2 Conformance is separate from raw reconstruction

| finding | question | proves | satisfied by |
|---|---|---|---|
| **reconstruction** | did anything other than column order move? | no **value** changed | **any** permutation |
| **conformance** | is the candidate's order the mandated one? | the **order** is right | only the canonical order |

`EXPECTED_COLUMN_ORDER_CHANGE` requires **both**. Reconstruction holding while the order
is non-conformant is **still a regression**. The gate is not weakened and expected
differences are not broadened.

### 1.3 Fail-closed, with the cause preserved

Decoupling removes the *reason* c1q failed, not the guard.

- comparator cannot be loaded → **UNVERIFIED** → `INCOMPLETE_VALIDATION`
- comparator raises → **UNVERIFIED**
- rule inapplicable to a case → **UNVERIFIED**, `conformant: None` — not `False`, not `True`
- **no bare `except`**: every handler binds its exception and records
  `describe_exception(exc)` — type, module, message, repr, full traceback, and the
  chained `__cause__`/`__context__`

An exception is never converted into a pass.

**A defect the new tests found, not me:** with no request supplied, the comparator was
ruling against **invented defaults** and reporting a *conformant* candidate as a
regression. `params=None` (request unknown) now fails closed to UNVERIFIED, kept
distinct from `params={}` (a request that omits them and takes `api/query.py`'s
documented defaults).

### 1.4 The summariser no longer double-counts

c1q's dispatch bucketed `C20a` as `EXPECTED_DOCUMENTATION_CHANGE`; then the notes
fallback ran **unconditionally afterwards** and re-added it to `regressions` because its
note — `non-row payload differs (8597 vs 9625 bytes); compared as raw bytes because
there is no row structure` — is *descriptive* and matched neither excluded prefix.
`C20a` was in two buckets at once and the gate printed FAIL.

The classification is now **authoritative**. `CLASSIFIED_NOT_A_REGRESSION` names the
placed classifications and the fallback never re-buckets them. `UNRECONSTRUCTED` is
among them: "could not check" is not "candidate failed". The fallback keeps its
sensitivity for cases the dispatch did **not** classify — those still reach
`regressions` and still fail the gate. **Unexpected differences remain their own class.**

### 1.5 Evidence is re-auditable, not merely re-derivable

c1q kept C20a's verdict and both body digests but **not the bodies**, so the structural
claim could not be checked from the artefact. Exact bodies are now retained for every
case that is not byte-identical or whose comparison could not be completed — never
truncated, base64 when undecodable, with lengths, digests and the exact classification
alongside. Byte-identical cases are not duplicated. A test proves an independent
re-audit from the artefact alone reproduces the classification.

### 1.6 `api/query.py` unchanged

`50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8`, as since `ad3f428`.
c1q produced no evidence of a candidate defect and none is assumed.

---

## 2. Offline suites — three serial batches

**46 suites per batch, 3881 assertions, exit 0 everywhere.**

| | batch 1 | batch 2 | batch 3 |
|---|---|---|---|
| suites | 46 | 46 | 46 |
| non-zero exits | **0** | **0** | **0** |

### 2.1 Evidence roots

```
batch 1  /var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T/woa23-suites-dCiZa6
batch 2  /var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T/woa23-suites-W784yp
batch 3  /var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T/woa23-suites-hs1roh
```

Manifests copied to `scratchpad/batches/manifest{1,2,3}.tsv`; full stdout/stderr,
per-suite exit codes, timings, failing-assertion text, environment fingerprints and
before/after process snapshots are retained under each root. Nothing was deleted.

### 2.2 Inter-batch consistency

Per-suite final line compared across all three batches, 46 suites each:

```
batch1 vs batch2 differences : 0
batch1 vs batch3 differences : 0
batch2 vs batch3 differences : 0
```

Compared **per suite**, not as a set of distinct lines — suites share assertion-count
strings, and counting distinct lines once produced a misleading 40-for-45.

### 2.3 All 46 suites and assertion counts

| suite | assertions | | suite | assertions |
|---|--:|---|---|--:|
| `test_api_surface_ordering.py` | 34 | | `test_provenance.py` | 297 |
| `test_c1_decided_differences.py` | **49** | | `test_request_log.py` | 61 |
| `test_c1_readonly_account.sh` | 145 | | `test_requests.sh` | 67 |
| `test_c2_driver.sh` | 72 | | `test_row_order.py` | 26 |
| `test_c2_summary.py` | 106 | | `test_run_suites.sh` | 48 |
| `test_clean_archive.sh` | 21 | | `test_s2_integration.py` | 72 |
| `test_cleanup_semantics.sh` | 76 | | `test_s2_provenance.py` | 92 |
| `test_cli.sh` | 305 | | `test_s2perf_driver.sh` | 221 |
| `test_clone_integrity.py` | 76 | | `test_s2perf_integration.py` | 138 |
| `test_column_contract.py` | **146** | | `test_staging_entry.sh` | 125 |
| `test_column_order.py` | 42 | | `test_staging_launcher.sh` | 101 |
| `test_compare_arms.py` | 79 | | `test_staging_override.sh` | 77 |
| `test_contract.py` | 61 | | `test_staging_store.py` | 24 |
| `test_contract_row_order.py` | 69 | | `test_stop_multiworker.sh` | 37 |
| `test_d1_cases.py` | 98 | | `test_store_survey.py` | 165 |
| `test_d1_evidence.py` | 59 | | `test_symmetric_warmup.py` | 50 |
| `test_d1_finalize.sh` | 56 | | `test_tracked.sh` | 19 |
| `test_d1_integration.py` | 64 | | `test_docs_only_diff.py` | 27 |
| `test_d1_store_validation.py` | 55 | | `test_environment.py` | 79 |
| `test_labels.sh` | 30 | | `test_manifest.sh` | 41 |
| `test_paired_stats.py` | 29 | | `test_perf_counts.py` | 84 |
| `test_ports.sh` | 33 | | `test_procs.sh` | 181 |
| `test_production_launcher.sh` | 111 | | `test_production_stop.sh` | 33 |
| | | | **TOTAL** | **3881** |

**Bold** are the two suites this fix changed. `test_column_contract.py` is new
(46 tests / 146 assertions); `test_c1_decided_differences.py` went 38 → 49.

### 2.4 A pre-check run that failed, recorded rather than discarded

A single suite run *before* the new files were committed failed 2 of 46:
`test_tracked.sh` and `test_production_launcher.sh`, both on the untracked-source
guard — the known failure mode where new files exist on disk but not in git. The files
were committed and all three official batches then passed. The guard behaved correctly;
nothing about it was changed to accommodate the fix.

---

## 3. The new subject

```
commit           13d6b74d1372a834e534bc91e91d027ab758a90d
subject line     fix(c1): the conformance comparator is pure; the summariser stops
                 double-counting
archive sha256   a4650395569abdd9f9038cbef3a429c58e92772fa18f8e3aadd02a494077e7b9
archive bytes    4167680
files            193
file-list sha256 301668db5c22b2cb3292f3147fdfff2e05ebd34724fcf9b5d98d78b7993404ca
```

`verify_clean_archive.sh` passes: no `.git` in the export, no `dist_*`, every imported
bench module present, `compileall` clean, and `test_tracked.sh` exits 0 **inside** the
archive with no repository.

### 3.1 `api/` diff versus the c1q subject `c62b081`

**Empty. `api/` is unchanged.** `api/query.py` = `50907dee…2ca8`.

### 3.2 `bench/` diff versus the c1q subject

```
 bench/column_contract.py             | 212 ++++++++          (new)
 bench/contract_diff.py               | 246 ++++++++-
 bench/test_c1_decided_differences.py |  63 ++-
 bench/test_column_contract.py        | 748 +++++++++++++++++  (new)
 4 files changed, 1234 insertions(+), 35 deletions(-)
```

### 3.3 File count 188 → 193

```
+ bench/column_contract.py
+ bench/test_column_contract.py
+ specs/018-conformance-comparator-is-pure.md
+ specs/C1-execution-request-c1q.md
+ specs/C1-result-c1q.md
```

The two c1q spec documents appear here because they were committed *after* the c1q
subject was cut.

---

## 4. The new C1 identity and first-use ports

```
identity   c1r
ports      19111  candidate arm
           19112  reference arm
           19113  isolated dask scheduler
```

**`c1r` has never been used.** Identities consumed so far: `c1d`, `c1f`, `c1h`, `c1i`,
`c1j`, `c1k`, `c1m`, `c1n`, `c1p`, `c1q`. `c1q` and every prior identity are not reused.

**All three ports are absent from the subject's own ledger copy**, verified against
`git show 13d6b74:dev2026/scripts/ports_used.tsv`. They are deliberately **not**
recorded now: `run_controlled.sh` refuses a port the subject's ledger already names, so
recording them before the run would make the run impossible. They are entered as
BOUND → SPENT after the run, as `c1p` and `c1q` were.

Highest port previously recorded: 19103.

---

## 5. Standing limits observed

- No VM24 contact of any kind during this work.
- `c1q` not rerun; `c1q` remains `INCOMPLETE_VALIDATION` and is not back-filled.
- `api/query.py` unchanged; the column-order gate not weakened; expected differences
  not broadened.
- **C2 remains blocked.** No C2 work was done.
- pm2G, port 18265 and all retained state untouched — not contacted.
- No new C1 run performed. `c1r` is proposed, not executed; it awaits authorization.

## 6. Evidence

| Path | Contents |
|---|---|
| `scratchpad/batches/batch{1,2,3}.txt` | full batch output |
| `scratchpad/batches/manifest{1,2,3}.tsv` | per-suite exit codes and timings |
| `scratchpad/c1q/05-import-failure.txt` | the exact `KeyError` this fix addresses |
| the three evidence roots in §2.1 | per-suite stdout/stderr, env, process snapshots |
