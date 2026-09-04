# C1 `c1q` — result: `INCOMPLETE_VALIDATION`

- **Identity:** `c1q` — CONSUMED. Never to be reused.
- **Subject:** commit `c62b0811e02c984a31bdcfc0c042ff5f94b76014`
- **Archive:** `fd9fc11ba33c8792a43940e227dd33dc288e7ad1ef9db983615abd6330bb0214`, 188 files
- **Ports:** 19101 / 19102 / 19103 — BOUND → **SPENT**
- **Ran:** 2026-08-26 03:35–03:38 UTC, as `woa23c1ro` (uid 994) over direct SSH
- **Evidence collection and cleanup verification:** 04:39–04:41 UTC, read-only, no arms started
- **Harness exit:** 1. **Reported gate: `FAIL`** — that reported gate is wrong (§6).
- **Correct classification: `INCOMPLETE_VALIDATION`.**

**This is not a candidate verdict.** Candidate row-order conformance was
**UNVERIFIED**, not proven, and UNVERIFIED is not PASS. Reconstruction evidence
(§2) is real and useful but does **not** convert the result: it shows no value
moved, not that the resulting order is the one spec 015 mandates. Conformance
could not be proven independently within this run, so the result stays
`INCOMPLETE_VALIDATION`.

`api/` and `bench/` product code are unchanged from `ad3f428`; `api/query.py` on
the run host was verified byte-identical at
`50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8`.

---

## 1. Run identity, environment and pre-flight

### 1.1 Fresh production PID evidence

Discovered in-session at 03:35:54 UTC. Nothing copied from any document.
Listener presence from `ss -ltn` **only** — never `ss -ltnp`, whose `pid=` field
is visible only to the socket owner or root.

```
LISTEN 0 2048 127.0.0.1:8050 0.0.0.0:*      8050 listening: yes
```

| pid | starttime | ppid | comm | cmdline |
|---|---|---|---|---|
| 4296 | 14214 | 4295 | `(gunicorn)` | `…/py311/bin/python3.11 …/py311/bin/gunicorn woa23_app:app` |
| 5040 | 15825 | 4296 | `(gunicorn)` | same |
| 5041 | 15829 | 4296 | `(gunicorn)` | same |

Discovery was **unambiguous**. Discriminator: `woa23_app:app` bound to
`127.0.0.1:8050`. Twelve other gunicorns were on the host and **every one was
excluded on recorded evidence** — `mhw_app:app` ×3, `tide_app:app` ×3,
`api.app:app` ×6 (three being pm2G's retained 1456369/1456373/1456374).
`starttime` is carried because a pid alone is not an identity.

**`exe_not_readable`:** `/proc/<pid>/exe` is readable only by the owner or root.
As uid 994 the harness could not read it for any of the three pids. Recorded as
`exe_not_readable`, **never** as exe-verified.

### 1.2 Pre-flight — every condition met

| Check | Result |
|---|---|
| Connection identity | `uid=994(woa23c1ro) gid=993`, `HOME=/home/woa23c1ro` |
| Host guard | `odb24` |
| `uv` | `/home/woa23c1ro/.local/bin/uv`, 0.9.22, sha256 `1f95b3af…a0036`, mode 755 |
| Subject archive / files / file-list | `fd9fc11b…0214` / 188 / `4e7df017…059c` — all match |
| Per-file hashes | **17/17 exact** |
| Identity dirs absent before start | `woa23-c1q`, `woa23-c1q-work`, `tmp-c1q` |
| Ports unbound | 19101, 19102, 19103 |
| Ports absent from the **subject's own** ledger | all three |
| Package clone manifest | `f3b66c49…71f4` — matches |
| pm2G retained | 18265 bound, all three pids running |

Store read-only pre-flight (identity captured *first*; `stat`/`test -w` only —
no write probe exists since c1h):

```
owner odbadmin:odbadmin (1000:1000) mode 775, my uid 994 -> not the owner
kernel access check (test -w) : no
dirs not traversable / not readable : 0 / 0
files not readable                  : 0
paths WRITABLE                      : 0
symlinks / escaping symlinks        : 0 / 0
```

### 1.3 UID evidence and request budget

```
dask_scheduler : 1 process(es), all uid 994 (real, effective, saved, fs)
dask_worker    : 1 process(es), all uid 994 (real, effective, saved, fs)
reference      : 2 process(es), all uid 994 (real, effective, saved, fs)
candidate      : 2 process(es), all uid 994 (real, effective, saved, fs)
```

All four uids compared, not just the real one; zero processes is treated as a
failure, not a vacuous pass. **Requests to production 127.0.0.1:8050 — 0.**

### 1.4 Gate totals

```
canonical values/columns match : 63/64        (c1p: 59/64)
candidate row-order contract   : 44/44 applicable (20 carry no row order)
byte-identical, as required    : 59
```

The canonical improvement is the `compare_canonical` fix landing — it now flags
only a column **SET** difference, not a column **SEQUENCE** difference.
`C16-csv` moved to MATCH.

---

## 2. Raw reconstruction result

**Cases:** `C1`, `C16`, `C1-csv`, `C16-csv`. **All four: `reconstructed = true`.**

The reference's own rows, with their columns permuted into the candidate's
sequence, reproduced the candidate's bytes **exactly**, on real payloads — real
orjson JSON output and real CSV, not synthetic fixtures. This was the part most
at risk of not surviving contact with production-shaped output, and it held.

**What this proves:** no value changed. The difference between reference and
candidate for these four cases is a permutation of columns and nothing else.
A changed number, a dropped row or an altered column *set* would have broken
byte-exactness and been reported as a regression.

**What this does NOT prove:** that the candidate's resulting order is the
ordering spec 015 mandates. Byte-exact reconstruction is satisfied by *any*
permutation. Accepting on reconstruction alone would accept an arbitrary column
order. Reconstruction is therefore **necessary but not sufficient**, and is
reported here as evidence, not as a verdict.

---

## 3. Candidate row-order conformance result

**Result: `UNVERIFIED`. Not PASS, not FAIL.**

```
the column-order difference could not be checked against the spec 015 rule:
api.query is not importable here, so conformance is UNVERIFIED rather than
assumed
```

`canonical_column_rule()` guards its import of `api.query.canonical_column_order`
and returns `None` when the module will not import. The caller then reports the
conformance finding UNVERIFIED and the gate fails **closed** to
`UNRECONSTRUCTED` for all four cases.

The guard behaved exactly as designed, and per the authorization —
*"unprovable class-1 reconstruction is INCOMPLETE_VALIDATION"* — this is the
defined outcome. **The candidate did not fail the rule; the rule was never
applied.** No conformance claim is made in either direction.

Note the separate row-order (not column-order) contract passed cleanly:
44/44 applicable cases, 20 carrying no row order.

---

## 4. API 1.1.0 documentation comparison

**Case:** `C20a`, `GET /api/swagger/woa23/openapi.json`, intent
*documentation surface*.

| | reference | candidate |
|---|---|---|
| status | 200 | 200 |
| bytes | 8597 | 9625 |
| body sha256 | `18649e2d8fe9e90a045c77c6b8c1a9e55ec64606852fccd55fcbfeecee15bc4d` | `bb74dd5e76fa51fdef5b182c63feb62e4a688d960f997d4d5e1b86f0a9e9b15e` |
| `info.version` | 1.0.0 | 1.1.0 |

```
classification : EXPECTED_DOCUMENTATION_CHANGE
docs_only      : true
why            : identical once version, summary and description strings are
                 removed: no route, parameter, schema or response moved
```

The structural normalization admitted it on exactly the narrow ground spec 008
rev 6 established. A new route, response code or parameter would have survived
the stripping and still failed.

### 4.1 Evidence limitation — recorded, not glossed

The harness persisted the normalization **verdict** but **not the per-key diff**,
and it does **not retain the two raw bodies**. The `results/` directory holds no
copy of either OpenAPI document. Consequently the structural claim in §4 cannot
be independently re-audited from the retained artifact alone — it can only be
re-derived offline by regenerating both documents and checking their sha256
against `18649e2d…bc4d` and `bb74dd5e…b15e` above.

The two body digests **are** retained, so the comparison is pinned to two
specific documents and is reproducible. But retaining the digests without the
bodies is an evidence-retention gap in my harness, and it is listed in §8.

---

## 5. Unexpected differences

**None.** No difference in this run is unattributed.

`regressions = ['C20a']` in the emitted JSON is **not** an unexpected difference.
It is defect A (§6) double-counting an already-classified documentation case.
`C20a` appears simultaneously in `expected_documentation_diffs` and in
`regressions`; the two entries are the same case, not two findings.

Neither expected class was used as a blanket exemption: the column-order class
was granted only against byte-exact reconstruction and was still withheld for
want of the rule; the documentation class was granted only against structural
identity after stripping.

---

## 6. Exact import failure and execution environment

Captured 2026-08-26 04:39 UTC by reproducing the guarded import in the same
tree, with the same interpreter and working directory the harness used.

### 6.1 Execution environment

| | |
|---|---|
| interpreter | `/home/woa23c1ro/woa23-c1q/dev2026/.venv/bin/python` |
| version | Python 3.11.4 |
| working directory | `/home/woa23c1ro/woa23-c1q/dev2026` |
| `sys.path[0]` | `''` (the cwd — so `api/` **is** on the path) |
| `WOA23_ZARR_STORE` | **unset** |
| `WOA23_STORE` / `WOA23_DATA` / `PYTHONPATH` | unset |
| `api/query.py` present | yes, `50907dee…2ca8` — byte-identical to the subject |
| `api/config.py` present | yes |

### 6.2 The exact failure

```
Traceback (most recent call last):
  File "<stdin>", line 9, in <module>
  File "/home/woa23c1ro/woa23-c1q/dev2026/api/query.py", line 65, in <module>
    from api.config import (
  File "/home/woa23c1ro/woa23-c1q/dev2026/api/config.py", line 33, in <module>
    zarr_store_path = os.environ["WOA23_ZARR_STORE"]
                      ~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^
  File "<frozen os>", line 679, in __getitem__
KeyError: 'WOA23_ZARR_STORE'
```

**The module is present, correct and on `sys.path`.** It is not missing, not
shadowed, and not broken. The import fails on an **environment-variable
precondition**: `api/config.py:33` does an unguarded `os.environ[...]` lookup at
**module import time**, and `api.query` imports `api.config` at its line 65.

The arms set `WOA23_ZARR_STORE` in their own launch environment. The comparator
(`contract_diff.py`) runs in the **harness** process, which never sets it. So the
comparator can never import the rule it needs, in any run, by construction —
this was not a transient condition and would have recurred on any re-run.

### 6.3 A second defect the failure exposed

[`canonical_column_rule()`](dev2026/bench/contract_diff.py:450) catches with a
bare `except Exception: return None`, **discarding the reason**. The run
therefore recorded only "not importable" and no cause. The traceback above had
to be recovered afterwards by reproducing the import by hand. A guard that fails
closed must still say *why* it closed.

---

## 7. Why the reported gate says FAIL — defect A, in my summariser

The emitted summary is self-contradictory:

```
expected_documentation_diffs = ['C20a']
regressions                  = ['C20a']
gate                         = FAIL
```

Cause is in [`summarise_5_2C`](dev2026/bench/contract_diff.py:719). The
classification dispatch correctly buckets `C20a` as
`EXPECTED_DOCUMENTATION_CHANGE` at line 712. Lines 719–724 then run
**unconditionally afterwards**, re-adding any case carrying a leftover note:

```python
if not [x for x in (r.get("notes") or [])
        if not x.startswith("ROW_ORDER_CONTRACT_FAILURE")
        and not x.startswith("bytes differ where they must not")]:
    canonical_match += 1
elif cid not in regressions:
    regressions.append(cid)
```

`C20a`'s note is **descriptive, not a failure**:

> `non-row payload differs (8597 vs 9625 bytes); compared as raw bytes because there is no row structure`

It matches neither excluded prefix, so the fallback re-adds `C20a` to
`regressions` and drives the gate to FAIL. The fallback predates the two new
expected classes and was never taught about them.

**Correcting defect A does not make this run a PASS.** With `C20a` bucketed once,
`regressions` and `order_failures` are both empty, but four cases remain
unreconstructed — so the gate becomes `INCOMPLETE_VALIDATION`. That is this
document's classification, and the reason is §3, not §4. No claim is made that
the run would otherwise have passed.

---

## 8. Cleanup — prescribed cleanup performed and verified

At run end:

```
candidate      stopped; whole tree exited, port 19101 confirmed free
reference      stopped; whole tree exited, port 19102 confirmed free
dask_worker    stopped; whole tree exited, port n/a
dask_scheduler stopped; whole tree exited, port 19103 confirmed free
production on 8050 unchanged (master 4296, listeners [4296 5040 5041], boot id matches)
```

Re-verified independently at 04:40:49 UTC, after evidence collection:

- **Arms:** none. `ps -u woa23c1ro | grep -E 'gunicorn|dask|api\.app|woa23_app'`
  returns exit 1 (no match) once grep's own command line is excluded.
- **Ports:** 19101, 19102, 19103 all free.
- **Production:** 4296 / 5040 / 5041 ALIVE, starttimes **14214 / 15825 / 15829 —
  unchanged from pre-flight**, `127.0.0.1:8050` LISTEN, boot id
  `0b513a75-…-1c7cbc51a085` unchanged.
- **pm2G:** 18265 bound; 1456369, 1456373, 1456374 running. **Untouched.**
- **Evidence retained:** the `woa23-c1q` tree (528 MB) and all 12 `c1q_*.json`
  artifacts are preserved, not deleted.

**Store identity, before and after — identical on all four measures:**

| | pre-flight | post-run |
|---|---|---|
| dir mtime | 1787622836 | 1787622836 |
| files | 123005 | 123005 |
| bytes | 35101630061 | 35101630061 |
| metadata fingerprint | `1c89be47…b208f` | `1c89be47…b208f` |

The production store, its ACLs, runtime, `.lock` file and permissions were not
modified. Nothing was patched; no re-run occurred.

### 8.1 The c1p fix that worked

c1p ended with a spurious "production down" warning. It did not recur. The
post-run check now uses the same non-owner-safe mechanism as the pre-run one —
`port_is_listening` for presence, per-pid `/proc` re-validation for identity, and
PID reuse caught by `starttime`. When that fix was made, grepping for every
remaining call found a **third** ownership-dependent site (a mid-run recheck),
also converted; the three `pids_on_port` calls that remain are all in
non-expect-uid legacy branches.

---

## 9. Offline work required before the next C1

To be done **offline**, after this run. No patching happened during it.

1. **Break the comparator's dependence on an unavailable `api.query` import.**
   Move `canonical_column_order` and `INDEX_COLUMNS` into a dependency-free
   module that `contract_diff.py` can import without pulling `api.config`'s
   import-time `os.environ["WOA23_ZARR_STORE"]` lookup — **or** make the import
   path explicit and *verified before the gate runs*, so an unavailable rule is
   detected at start rather than discovered per-case at classification time.
   The fail-closed guard must **stay**: if the rule is ever genuinely
   unavailable, the gate must still refuse to pass.
2. **Record why the guard closed.** Replace the bare `except Exception: return
   None` with one that captures and reports the exception, so a future run's
   artifact states the cause without needing manual reproduction.
3. **Teach the notes fallback about the expected classes**, so a case already
   bucketed as `EXPECTED_COLUMN_ORDER_CHANGE` or `EXPECTED_DOCUMENTATION_CHANGE`
   cannot be re-added to `regressions` by a descriptive note — preserving its
   sensitivity for genuinely unclassified notes.
4. **Retain the compared bodies** for non-row payloads (or a normalized form of
   them), so a documentation verdict can be re-audited from the artifact rather
   than only re-derived (§4.1).

**Regression tests required, covering both directions:**

- conformance comparator with the rule **importable** → conformance actually
  checked, conformant order PASSes and a non-conformant permutation FAILs;
- conformance comparator with the rule **unimportable** → UNVERIFIED,
  `INCOMPLETE_VALIDATION`, never a silent pass, **and the cause recorded**;
- an expected-class case carrying a descriptive note → bucketed once, not
  re-added to `regressions`;
- a genuinely unclassified note → still reaches `regressions`.

Both defects had offline fixtures that passed while the real run did not. The
recurring lesson is that the fixtures did not represent the real execution
environment — specifically, they never modelled a comparator process without
`WOA23_ZARR_STORE`. The new tests must reproduce the real condition, not a
synthetic stand-in.

**Then:** run new offline batches, create a new subject, and request a new C1
identity. `c1q` is not to be reused. pm2G is not to be touched.

---

## 10. Standing limits observed

- No self-rerun; no arms started during evidence collection (read-only only).
- No patching of the tree.
- UNVERIFIED was not reinterpreted as PASS.
- `c1q` CONSUMED; ports 19101/19102/19103 BOUND → SPENT.
- All failure evidence preserved on VM24 and in `scratchpad/c1q/`.
- pm2G, 18265, its PM2 entry and retained state untouched.
- No C2, latency or deployment run.
- No sudo, sshpass, privilege escalation or odbadmin launcher.
- Production store not modified; production requests: 0.

## 11. Evidence

| File | Contents |
|---|---|
| `scratchpad/c1q/01-pid-discovery.txt` | fresh PID discovery, full exclusion list, per-pid `/proc` identity |
| `scratchpad/c1q/02-preflight.txt` | all pre-flight checks, store read-only scan |
| `scratchpad/c1q/03-run.txt` | full run, UID evidence, request budget, gate output |
| `scratchpad/c1q/04-poststop.txt` | post-run state, store comparison, classifier diagnosis |
| `scratchpad/c1q/05-import-failure.txt` | exact traceback, interpreter, cwd, `sys.path`, env |
| `scratchpad/c1q/06-c20a-evidence.txt` | C20a request, statuses, byte counts, body digests, retention gap |
| `scratchpad/c1q/07-final-cleanup.txt` | final cleanup verification |
| `results/c1q_*.json` (VM24, retained) | 12 artifacts including `c1q_contract.json` |
