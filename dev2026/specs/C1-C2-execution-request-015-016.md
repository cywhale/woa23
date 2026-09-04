# C1 / C2 execution request — the 015+016 candidate (`c1h` / `c2h`)

**Status: REQUESTED, NOT GRANTED. Nothing here has been run.** No VM24 contact, no port
bound, no arm started. **This document is not an authorisation.**

Supersedes the preliminary [C1-C2-rerun-request-015-016.md](C1-C2-rerun-request-015-016.md),
which is retained; this document is the complete execution request with the provenance
confirmation the PI required before authorising.

---

## 1. Provenance — the complete diff, not a summary

**`git diff --name-status 06661fd..ad3f428`, in full — 13 files, 5 added and 8 modified:**

| status | path | why it is in this subject |
|---|---|---|
| **M** | `dev2026/api/query.py` | **spec 015.** The canonical column order. **The only `api/` change, and the only file C1/C2 actually gate.** |
| **A** | `dev2026/bench/test_column_order.py` | **spec 015's suite** — 42 assertions, subprocess/hash-seed based. **Required by C2's column-order gate.** |
| **M** | `dev2026/deploy/make_staging_override.js` | **spec 016.** `--python` as the sixth permitted override. |
| **M** | `dev2026/deploy/production_app.sh` | **spec 016.** `WOA23_PYTHON` fail-closed, no shared-pyenv default. |
| **M** | `dev2026/deploy/staging_execute.sh` | **spec 016.** Exact-value check + venv-is-runtime proof. |
| **M** | `dev2026/scripts/test_production_launcher.sh` | spec 016 fail-closed tests (111 assertions). |
| **M** | `dev2026/scripts/test_staging_entry.sh` | spec 016 tests, the pm2 jlist regression fixture, and the stale "5 permitted items" fix (125 assertions). |
| **M** | `dev2026/scripts/test_staging_override.sh` | spec 016 `--python` refusal tests (77 assertions). |
| **A** | `dev2026/specs/015-deterministic-column-order.md` | the column-order spec. |
| **A** | `dev2026/specs/016-isolated-venv-is-the-runtime.md` | the venv-is-runtime spec. |
| **A** | `dev2026/specs/PM2-staging-request-pm2G.md` | the pm2G request (documentation). |
| **M** | `dev2026/specs/PM2-staging-result-pm2F.md` | the pm2F cleanup-provenance reconciliation (documentation). |
| **A** | `dev2026/specs/PM2-staging-result-pm2G.md` | the pm2G result (documentation). |

**Line stats: 13 files changed, 2329 insertions, 64 deletions.**

**A correction to an earlier summary of mine.** I previously described this diff as
"4 files, `api/query.py` + three `deploy/` files". **That was the `api/`+`deploy/` subset,
not the diff**, and it omitted the new test suite, three modified harness suites and five
specs. The table above is the complete list.

### 1.1 Why 164 → 169 files

**Exactly the five `A` rows above. `164 + 5 = 169`; the eight `M` rows change content,
not count.**

| # | added file | kind |
|---|---|---|
| 1 | `bench/test_column_order.py` | **runtime/test — required by C2's column-order gate** |
| 2 | `specs/015-deterministic-column-order.md` | spec |
| 3 | `specs/016-isolated-venv-is-the-runtime.md` | spec |
| 4 | `specs/PM2-staging-request-pm2G.md` | spec |
| 5 | `specs/PM2-staging-result-pm2G.md` | spec |

**Four of the five are documentation. One is executable and it is the new gate's suite.**

### 1.2 The ten commits in the range

```
ac3c7c4  spec(pm2G): request on the fixed tree (06661fd), new identity, 18265
cbb7979  test(staging_entry): regression fixture for pm2 jlist parser, 7 modes
1281525  spec(pm2F): reconcile cleanup provenance — record the boundary crossing
7cc909f  spec(pm2G): v2 after PI review — jlist regression, cleanup discipline
5eeecbd  spec(pm2G): record 126/126 offline result on the fixture commit (cbb7979)
456aeef  spec(pm2G): result — NOT a PASS; venv-not-runtime and column-order findings
3d93f7b  spec(015): deterministic JSON field order and CSV header order
7b8fd65  api: canonical column order, parameter-major, by explicit ordered select
ad23890  deploy: WOA23_PYTHON is required, and the venv is proven to be the runtime
ad3f428  test(staging_entry): the generator now reports 6 permitted items, not 5
```

## 2. The execution subject, and that the harness is inside it

| item | value |
|---|---|
| **commit** | `ad3f4280309aab148faa997a8681dcd2cb6120fd` |
| **archive SHA-256** | `308482b9ccb93ed542a187b00d39c05931ac80b7c27609e91c7e9e24f708a648` |
| **file count** | `169` — re-counted from an extracted archive, not inferred |
| **file-list SHA-256** | `c58f34a65901293e1534700e4b74c468ddfb2e5f648125aac985cde67b5289ed` |
| **verifier** | `scripts/verify_clean_archive.sh ad3f428` → **16/16, all passed** |
| **offline evidence** | **three serial batches, 43 suites each, 129 runs, 0 non-zero, 0 failing assertions** |

### 2.1 Every module the C1/C2 path needs, confirmed present in the archive

Extracted from `git archive ad3f428` and checked file by file:

| module | in subject |
|---|---|
| `scripts/run_controlled.sh` | **ok** |
| `scripts/ports_used.tsv` | **ok** |
| `scripts/verify_clean_archive.sh` | **ok** |
| `bench/clone_integrity.py` | **ok** |
| `bench/collect_backend_meta.py` | **ok** |
| `bench/contract_diff.py` | **ok** |
| `bench/contract_cases.py` | **ok** |
| `bench/c2_summary.py` | **ok** |
| `bench/package_digests.py` | **ok** |
| `bench/perf_counts.py` | **ok** |
| `bench/provenance.py` | **ok** |
| `bench/store_survey.py` | **ok** |
| `bench/symmetric_warmup.py` | **ok** |
| `bench/noise_pilot.py` | **ok** |
| `bench/request_log.py` | **ok** |
| `bench/test_contract.py` | **ok** |
| `bench/test_contract_row_order.py` | **ok** |
| **`bench/test_column_order.py`** | **ok — the new gate's suite** |
| `api/query.py`, `api/app.py`, `api/config.py`, `api/store_paths.py` | **ok** |
| `deploy/make_staging_store.py` | **ok** |
| `pyproject.toml`, `uv.lock` | **ok** |

**The 16/16 verifier independently asserts full import closure** — "every bench module
imported anywhere in the archive exists in the archive" and "every imported bench module
loads from **inside** the archive" — so this is proven twice, once by name and once by
import.

**One correction, recorded rather than quietly dropped.** A first pass of this check
listed `bench/compare_arms.py` as MISSING. **No such module exists or is referenced**; the
name was mine, not the tree's. The real file is `bench/test_compare_arms.py`, which is
present and imports `bench.provenance`, also present. **No module is actually missing.**

## 3. The port ledger is campaign metadata, and the live host is the authority

**`scripts/ports_used.tsv` is CAMPAIGN METADATA — a hand-maintained record of what this
campaign has bound. It is not a reading of the host, and it must never be used as one.**

| rule | |
|---|---|
| **the ledger is metadata** | it records what is KNOWN and was written after the fact; absence from it is not proof a port is free |
| **re-read the LIVE ledger on VM24 before executing** | from the export of the subject actually being run, not from this document and not from a developer machine |
| **re-confirm every port unbound with read-only `ss`** | `ss -ltn` immediately before the run, for all six ports |
| **the request's ledger values may NOT replace the live preflight** | a value in this document is a proposal; the host's answer is the fact |
| **a bound port at that moment is a STOP** | not a warning, not a reallocation-in-flight |

### 3.1 The ledger commit is a protocol reference, never the subject

**The ports below are recorded in `3e2c518`, which the subject `ad3f428` does NOT
contain — deliberately, and it is load-bearing.**

`run_controlled.sh` **refuses a port listed in `ports_used.tsv`**, and it reads that ledger
**from the export of the subject it runs**. Confirmed by extraction: all six `c1h`/`c2h`
ports, and `18265`, are **absent** from `ad3f428`'s copy of the ledger, so the runner will
accept them.

**Re-cutting the subject to include `3e2c518` would make the runner reject the very ports
this request allocates.** That is the trap `pm2F`'s ledger note recorded. **The subject
stays `ad3f428`. `3e2c518`, `b942a8e` and this document are protocol references and must
never be back-filled as the execution subject.**

## 4. C1 — `c1h`

| | |
|---|---|
| **grant** | **`WOA23_S2_C1_GRANTED=yes`** — C1's own grant. No other grant substitutes, and any other `WOA23_*_GRANTED` present alongside is a refusal |
| **commit** | `ad3f4280309aab148faa997a8681dcd2cb6120fd` |
| **archive SHA-256** | `308482b9ccb93ed542a187b00d39c05931ac80b7c27609e91c7e9e24f708a648` |
| **file count / file-list** | `169` / `c58f34a65901293e1534700e4b74c468ddfb2e5f648125aac985cde67b5289ed` |
| **label** | **`c1h`** |
| **workdir** | **`~/woa23-c1h-work/`** |
| **staging/export root** | created by the runner under the workdir; **must not exist beforehand** |
| **candidate arm port** | **`18301`** |
| **reference arm port** | **`18302`** |
| **isolated dask scheduler** | **`18949`** |
| **mode** | `run_controlled.sh --c1` (variant 5.2C, contract validation) |
| **request ceiling** | **the C1 contract case set only** — the cases `bench/contract_cases.py` defines for 5.2C, one pass. **No additional queries, no ad-hoc requests, no exploratory calls** |
| **production requests** | **ZERO.** Ports 8050/8786/8787 are never contacted |

## 5. C2 — `c2h`

| | |
|---|---|
| **grant** | **`WOA23_S2_C2_GRANTED=yes`** — C2's own grant, distinct from C1's. C1's grant does not authorise C2 |
| **commit / archive / file-list** | **identical to §4** — the same subject |
| **label** | **`c2h`** |
| **workdir** | **`~/woa23-c2h-work/`** |
| **candidate arm port** | **`18311`** |
| **reference arm port** | **`18312`** |
| **isolated dask scheduler** | **`18959`** |
| **mode** | `run_controlled.sh --c2-cycle`, **three cycles** |
| **request ceiling** | **the C2 case set × 3 cycles**, plus the three-seed ordering checks. **Nothing beyond that** |
| **production requests** | **ZERO** |

**C1 and C2 are separate authorisations.** Granting one does not grant the other, and they
do not share a workdir, a label or a port.

## 6. What C1/C2 verify — and what they explicitly do not

**In scope — the new API candidate's contract, and only that:**

| checked | |
|---|---|
| **canonical column order** | the spec 015 order, and **identical across three seeds** (the C2 gate; the property `pm2G` found violated) |
| **row order** | the spec 008 contract, ascending by `(time_period numeric, depth, lat, lon)` — **unchanged, and must remain so** |
| **JSON fields and CSV headers** | present, correctly named, and **identical to each other** |
| **values** | cell for cell against the reference arm |
| **semantic correctness** | statuses, error bodies, empty-result behaviour, parameter meaning — all **unchanged** |
| **the decided difference** | the column-order change recorded as a **decided change** with byte-level reconstruction proving nothing else moved — the same discipline 008's row-order change already gets |

**Explicitly OUT of scope — not measured, not claimed, not attempted:**

- **latency** — no timing gate, no performance figure. **`s2pB`'s latency does not apply to
  this tree and may not be cited for it.** Measuring the ordered `select` needs its own
  authorisation, identity and ports;
- **deployment** of any kind;
- **PM2** — no daemon, no app, no staging launcher, no `production_app.sh`;
- **production API** — not contacted, not read, not measured;
- **spec 016's runtime behaviour** — the venv/`WOA23_PYTHON` work is staging/cutover and
  **is not exercised by contract gates**, even though its files are in the subject.

**No back-filling.** `c1f`, `c2g`, `s2pB` and `pm2G` were produced by different trees or
different questions. **None of them may stand in for any part of this run, and this run
may not be described as confirming any of them.**

## 7. pm2G is untouched by this run

**`pm2G` stays exactly as it is, and C1/C2 must not go near it:**

| | |
|---|---|
| service | **running** — gunicorn master pid 1456369, workers 1456373/1456374 |
| port `18265` | **still BOUND** |
| PM2 app entry | **present** under `~/woa23-pm2g-pm2/` |
| tree / workdir / store / logs / uv cache | **all retained** |
| classification | **NOT A PASS.** Identity and `18265` **consumed** |

**Forbidden for this run:** `pm2 stop`, `pm2 delete`, releasing `18265`, `kill`/`pkill`,
**SIGKILL**, or any cleanup of `pm2G`'s tree, workdir, store, logs or PM2 home. **pm2G
cleanup is a separate matter requiring its own authorisation** and must not happen as a
side effect of a run that shares the host.

**`pm2G` may not be cited as a PASS anywhere in this run's evidence.**

## 8. Failure and cleanup boundaries

**On any failing step: STOP, retain, report, wait.**

Retained on failure: both arms' processes, the bound ports, the workdir, the export, all
logs, the request log and every diagnostic.

**Forbidden after a mid-flight failure** — the discipline `pm2F` and `pm2G` established:

- no `pm2 *` of any kind;
- no manual process termination — no `kill`, `pkill`, `pgrep|kill`;
- no port release by any means;
- no `rm`, `find -delete` or `chmod` on the retained tree;
- **no self-rerun.** The run is not reissued in-session for any reason.

**On success**, the runner's own cleanup path runs, and **the ports must be confirmed free
and connection-refused afterwards** — recorded, not assumed. **A survivor after cleanup is
`CLEANUP_FAIL`**: the surviving process is left alive for inspection, **no SIGKILL**, and
the run is **not repeated**.

**Cleanup applies only to this run's own arms.** Nothing belonging to `pm2A`, `pm2B`,
`pm2E`, `pm2F`, `pm2G` or any earlier C-run is touched, on success or on failure.

## 9. Pre-execution checklist, to be performed on VM24 and recorded

1. **subject** — archive re-derived on VM24 and compared to §2 byte for byte; every named
   file's digest re-derived; a mismatch is a **stop**;
2. **live ledger** — read from the export of `ad3f428` on VM24, and confirmed not to list
   any of the six ports;
3. **live ports** — `ss -ltn`, read-only, for **all six**, immediately before the run; any
   bound port is a **stop**;
4. **identity absence** — `~/woa23-c1h-work/` and `~/woa23-c2h-work/` must not exist;
5. **production** — boot id, PIDs 4296/5040/5041/4357/4358 with starttimes, listeners on
   8050/8786/8787, production PM2 list: recorded **before and after** and compared;
6. **pm2G** — recorded as still running with `18265` still bound, **and not touched**;
7. **grants** — the run's own grant present, every other `WOA23_*_GRANTED` absent.

## 10. What a PASS will and will not mean

**Will:** the 015 candidate satisfies the contract gates — canonical column order, stable
across three seeds; row order unchanged; JSON and CSV agreeing; values identical to the
reference; the column-order change recorded as a decided change.

**Will not:** **not a staging PASS. Not a production cutover PASS. Not a latency result.
Not a deployment result.** **B1–B5 remain open** — contract gates do not touch them.
**B7 remains open** — spec 016 makes an isolated venv enforceable, but no deployment has
used one. **`pm2G` remains NOT A PASS** and is not re-classified by this or any later run.

## 11. Submission

- **Execution subject:** `ad3f4280309aab148faa997a8681dcd2cb6120fd`. **This document,
  `3e2c518` and `b942a8e` are protocol references and must never be back-filled as the
  subject.**
- **Two separate authorisations requested:** C1 `c1h` and C2 `c2h`.
- **Nothing on VM24 has been contacted by this document.** No `pm2H` is proposed.
- **Awaiting explicit authorisation before any execution.**
