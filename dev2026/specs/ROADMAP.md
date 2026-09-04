# WOA23 refactor roadmap (2026)

Two phases, matching the two consumers of the dataset. Phase 1 is broken into
steps; each step gets its own spec in this directory. Phase 2 is sketched only —
it will be broken down once phase 1 lands.

## How a step moves

```
spec (Claude)  →  review (Codex)  →  implement (Claude)  →  review (Codex)  →  merge
```

A step is not started until its spec has passed review. Every step that claims a
performance change ships a before/after benchmark JSON in `dev2026/results/`,
produced by the shared harness so the two runs are comparable.

## Standing constraints

- Nothing existing is overwritten. `woa23_app.py`, `src/`, `dev/`, `conf/`,
  `requirements.txt`, `Pipfile` are untouched. New code lives under `dev2026/`.
- The remote production directory on VM24 (`~/python/woa23`) is read-only to us.
- No load generation against production beyond light sequential probing.
- Disk and data cleanup is the PI's decision and the PI's action.
- The API is published with a DOI and has external users. Any change to returned
  values, column names, ordering, null handling, or the JSON/CSV contract requires
  an explicit versioning decision — it is not a side effect anyone may take.

## Reference

- [`D2a-request.md`](D2a-request.md) — granted and executed, rung 21
- [`PM2-staging-result-pm2B.md`](PM2-staging-result-pm2B.md) — **granted and executed
  2026-08-20: PASS**, bounded — *PM2 alternate-port staging against a small synthetic
  store*. Subject `716fcc6c…`, label `pm2B`, port **18241** (now spent and in the
  ledger). The environment was read back from the running process's `/proc/<pid>/environ`
  rather than the starting shell, Python **3.11.4** matched production's own binary, the
  candidate served **OpenAPI 1.1.0** and returned **144 rows** in contract order in both
  JSON and CSV across three period groups, survived a restart byte-identically, and
  released every process and the port on stop. Production was read before and after and
  is **identical**; **0 requests** were sent to it. **Not a production deployment PASS,
  not real-data correctness, not a performance result.**
- [`011-production-launcher-cutover.md`](011-production-launcher-cutover.md) — the
  proposed production launcher and PM2 config, **offline, under `dev2026/deploy/`, not
  installed**; **121 assertions** across `test_production_launcher.sh` (88) and
  `test_production_stop.sh` (33). Resolves **B1–B5** offline — `pre_stop` is **deleted**,
  and `deploy/production_stop.sh` stops by **(pid, starttime) identity**, refuses `all`
  and a defaulted `PM2_HOME`, and never sends SIGKILL. Raises **B7** (introduced as B6,
  renumbered when the PI assigned B6 to AVX2): nothing has shown production's interpreter
  carries the candidate's dependencies. **Installing into `conf/` and the cutover each
  need their own authorisation.**
- [`012-b6-avx2-polars-decision-memo.md`](012-b6-avx2-polars-decision-memo.md) — **B6,
  DECIDED 2026-08-20: mainline polars `1.27.1` retained**; `polars-lts-cpu` **not installed
  and not evaluated** in this campaign. Because nothing changes, **no C1/C2 re-run
  follows** and `c1f`, `c2g`, `s2pB`, `pm2B` and `bash5A` all stand. VM24's AVX2 masking is
  carried as **accepted residual risk** — the warning is not silenced, the SIGILL risk is
  unquantified, and B7 showed it applies to production's `py311` too. **No performance,
  SLA or production-runtime equivalence claim follows.** Reopens on: hardware/hypervisor
  change, a polars upgrade, an observed SIGILL, an incorrect result, or a significant
  performance regression.
- [`B7-dependency-check-result.md`](B7-dependency-check-result.md) — **granted and executed
  2026-08-20.** All eight modules present in `/home/odbadmin/.pyenv/versions/py311` at
  versions **identical** to the validated venv. **B7 answered, NOT closed**: eight matching
  packages are not runtime equivalence (S1 was caught by exactly that), `py311` is shared
  with `mhwapi`/`ghrsst_mcp`/`tide`, and the candidate has never been *run* from it.
  Recommendation stands: **deploy an isolated venv**. Production identical before/after,
  0 requests, nothing created.
- [`013-production-launcher-source-audit.md`](013-production-launcher-source-audit.md) —
  what production actually starts, from the files. `conf/start_app.sh` does **not** `exec`,
  so PM2 tracks the shell rather than gunicorn — likely why a `pre_stop` was written at
  all. `conf/simu.sh` is a copy-paste runbook with three `grep | kill -9` pipelines, one of
  which kills **`tide_app`, another project**. Dask is two separate PM2 apps, shared with
  `tide_app` and `mhw_app`, and stopping them is **not** part of this cutover.
- [`Bash5-verification-result-bash5A.md`](Bash5-verification-result-bash5A.md) — **granted
  and executed 2026-08-20: PASS.** `production_app.sh` parses under VM24's **bash 5.2.21**;
  twelve refusal cases fail closed with the stub never invoked and no port bound; two argv
  cases confirm the guarded empty-array expansion. **Compatibility evidence only** — no
  gunicorn started, no PM2, no successful-start test, not a deployment validation.
- [`PM2-staging-request-pm2B.md`](PM2-staging-request-pm2B.md) — the granted request.
  Note its §4 required `/proc/<pid>/exe` to point inside the staging venv; **that check
  is unsatisfiable for any correct venv** and is superseded by spec 010 §5b.
- [`PM2-staging-request-pm2A.md`](PM2-staging-request-pm2A.md) — **granted and executed
  2026-08-20: FAILED AT START.** PM2's `env:` block overrode the shell environment, so
  the process got an empty store and the spent port 18221; the store guard stopped it
  before anything bound. A staging-configuration failure, not a candidate failure and
  not a staging PASS. Record in spec 002 under `pm2A`.
- [`010-pm2-staging-validation.md`](010-pm2-staging-validation.md) — the audit of
  `conf/` (production's `pre_stop` is a `grep | kill -9` that would reach production's
  own workers; its launcher binds 8050, starts the OLD app and passes `--reload`), an
  **isolated** staging launcher and PM2 config under `dev2026/deploy/`, and the
  alternate-port validation checklist. **Designed, not authorised**; port 18221
  allocated and never bound.
- [`009-sort-cost-validation.md`](009-sort-cost-validation.md) — source audit against
  the eight implementation principles (**all hold, no `api/` change required**) and an
  offline sort-only benchmark. **Closed for now**: the PI decided no VM24 sort-cost
  measurement is needed.
- [`C2-request-c2g.md`](C2-request-c2g.md) — **granted and executed 2026-08-19**: three
  unpinned cycles at production's measured worker count (`-w 2`, measured from pid
  4296's argv), label prefix `c2g`, ports 18211 / 18212 / 18939, on the subject
  `1439194`. **C2 OUTCOME PASS** — 5.2B semantic PASS x3, seed diversity OBSERVED (3/3),
  **row-order contract PASS (132 applicable responses, 0 violations, candidate order
  identical across all three cycles)**, shutdown budget CONSISTENT. **The reference
  varied on `C16` and `C16-csv` and the candidate did not** — the difference `c1f` could
  not show under a pinned seed. Record in spec 002 under `c2g`.
- [`C1-5.2C-request.md`](C1-5.2C-request.md) — **granted and executed 2026-08-19**:
  validation of the S2b candidate (`1439194`) under variant 5.2C, label `c1f`, ports
  18201 / 18202 / 18929. **PASS** — canonical values/columns 64/64, candidate row-order
  contract 44/44 applicable, required byte checks pass, no unexpected differences;
  **expected raw-order differences 0, raw-order reconstruction N/A (not exercised)**
  because under a pinned seed the reference also emitted contract order. Record in spec
  002 under `c1f`; the execution subject remains `1439194`. **C2 needs its own request
  and a new execution identity.**
- [`D2b-request.md`](D2b-request.md) — **requested, not granted**: the controlled
  two-arm run that removes the hash-seed and environment variables 5.2B could not
- [`../CODEX_REVIEWER.md`](../CODEX_REVIEWER.md) — **canonical** reviewer briefing.
  It stays at `dev2026/CODEX_REVIEWER.md`; `BASELINE.md` supplements its
  measurements but never overrides its rules.
- [`docs/BASELINE.md`](docs/BASELINE.md) — the measurement record all specs cite
- `../README.md` — the benchmark harness
- `../results/` — raw measurement JSON

**Work happens on `perf/2026-s1-remove-dask`; `main` stays clean.** A spec is
committed once it passes review; implementation and benchmarks are separate
commits after that.

---

## Phase 1 — Web API / Zarr (VM24)

Ordered by measured value, not by convenience. Steps S1–S3 are independent of each
other in principle, but running them in order keeps benchmark attribution clean:
each step's before/after is measured against the previous step's end state.

### S1 — Remove Dask from the read path

**Spec:** [`001-remove-dask-read-path.md`](001-remove-dask-read-path.md) · **Status:**
**measured, with a caveat.** Latency gate **PASS** at rung 21 — every case
established both no-regression and improvement, 1.39×–5.99×, nothing inconclusive.
Contract gate **62/64 semantic match**; the two exceptions were a harness defect
since fixed, and C20's byte equality rests on local in-process evidence, not an HTTP
result. **The A/B is not a clean isolation of the Dask change**: the twelve pinned
packages matched on both arms but 23 shared transitive dependencies did not,
including `fsspec` and `anyio`. Quotable as directional evidence; not as a measured
Dask-only speedup.

The app installs a distributed Dask client as the *default* scheduler, so every
xarray compute round-trips through a scheduler backed by a single worker shared
with `tide_app` and `mhw_app`. Removing it is 1.35×–8.4× faster across every query
shape measured, from 102 rows to 64,800. Largest win, smallest change, no
data-format consequences.

**Gate as run:** variant 5.2B — *semantic* equality over the full contract case
list, because D2a alone cannot pin production's hash seed. Byte-identical output is
what variant 5.2A would establish, and that needs D2b.

### S2 — Production environment correctness

**Spec:** `dev2026/specs/002-production-correctness-deploy-hardening.md` (revision 17)
· **Status:** **C1/C2 CORRECTNESS VALIDATION COMPLETE** for the spec 008 + spec 015
candidate — `c1r` PASS 2026-08-26 and `c2k` PASS 2026-08-26. D1 real-store depth
characterization, PM2 / deployment validation, B1–B5, B7, cutover and **all**
performance validation remain **open and separate**.

> #### C1/C2 correctness validation — COMPLETE (2026-08-26)
>
> **`c1r` PASS** — 59 byte-identical, 4 spec-015 expected column-order differences with
> **both reconstruction and conformance proven**, 1 expected API 1.1.0 documentation
> difference, **0 unexpected regressions**, 64/64 accounted for.
> **`c2k` PASS** — 5.2B semantic gate PASS in all three cycles; candidate row-order
> conformance PASS 132/192 applicable responses; candidate row-order stability PASS
> across all three cycles; seed diversity OBSERVED only; reference-side stability
> OBSERVED only; `C20a` classified separately as the expected 1.0.0 → 1.1.0
> documentation change; no regressions, no missing conformance records, no fourth cycle.
>
> **Together these establish the candidate's current contract correctness under the
> tested conditions, for values, parameter-major column order and deterministic row
> order — and nothing else.**
>
> **NOT established, and NOT to be inferred from them:** latency, throughput, startup,
> deployment, PM2, or production-runtime equivalence. Neither run produced timing of any
> kind. These remain the separate tracks listed below.
>
> Two caveats travel with `c2k` and must not be dropped when it is cited:
> **seed diversity is measured from the launch environment, not directly from the
> workers**; and **reference stability is an observation, not evidence that the
> reference implements the contract**.
>
> Superseded-but-not-deleted: `c1q` remains INCOMPLETE_VALIDATION and `c2j` remains
> NO C2 RESULT. Neither is back-filled; both stand as recorded.
> Results: [`C1-result-c1r.md`](C1-result-c1r.md), [`C2-result-c2k.md`](C2-result-c2k.md).

| step | what it establishes | status |
|---|---|---|
| **C1** | isolated package-tree contract correctness; since spec 008/015, 5.2C canonical values + column order + row-order contract | **COMPLETE — PASS 2026-08-26 (`c1r`)** on the spec 008 + spec 015 candidate. Supersedes the 2026-08-09 `c1e` PASS, which predates both specs. 59 byte-identical, 4 expected column-order (reconstruction **and** conformance proven), 1 expected 1.1.0 docs, **0 regressions**, 64/64 accounted for. |
| **C2** | multi-worker, unpinned seed, 5.2B semantic, seed diversity, row-order conformance and stability | **COMPLETE — PASS 2026-08-26 (`c2k`)**, three cycles. Supersedes `c2c` (2026-08-09) and `c2f` (2026-08-10), both of which predate spec 015. Gate PASS ×3; conformance 132/192; stability PASS; seed diversity and reference stability **observations only**; `C20a` its own class; no regressions, no missing records, no fourth cycle. |
| **D1 — store startup validation** | startup failure modes | **partly closed**, spec `004` revision 10 §54 and spec `005` §16. The patch is applied (`919095e8`) and carried unchanged through C1 (`c1e`) and C2 (`c2f`): an invalid or non-Zarr store fails before the service can be treated as ready, and the **real** anchor group was opened in the C1 rerun. **Depth behaviour is now characterized against the real store** by `d1b` — annual nitrate 0-800 m and winter nitrate (`time_period=13`) at 3000-4000 m — **recorded, not a D1 PASS.** Still open: **real-store missing-group behaviour is uncharacterized** because all twelve query-reachable groups exist; every variable and climatology outside those two; deployment under PM2 with site enabled. |
| **Row-order contract** | whether row order is part of the API contract | **open — and now a product decision, not a conformance fix.** Decision memo `003-row-order-contract-decision.md` rev 2: its §6 **question 1 is answered** — the published surface (OpenAPI title, description, both summaries, all parameter descriptions, `README.md`, and the absence of any response schema or example) **states no row or column order in any form**, audited offline and pinned by `bench/test_api_surface_ordering.py`. **Questions 2-4 remain the PI's**, and one human step remains: the hosted Swagger hub page is served by another system and no test here can read it. The observation is unchanged: `c2c` saw order varying per process on **both** arms; `c2f` saw the same two cases varying on the **candidate only**. Same cases, not the same behaviour, and neither run explains the difference. |
| **PM2 / formal deployment validation** | production's real launcher, site/`.pth` semantics, readiness, nginx, TLS | **alternate-port staging CLOSED 2026-08-20 (`pm2B`): PASS against a small synthetic store** — [`010`](010-pm2-staging-validation.md) rev 4, result in [`PM2-staging-result-pm2B.md`](PM2-staging-result-pm2B.md). The deployment machinery works: isolated `PM2_HOME`, environment intact in the process, 3.11.4 matching production's binary, OpenAPI 1.1.0, 144 rows in contract order, clean restart and release. **Production cutover remains open and separate** — no TLS or proxy was exercised, the store was 72 synthetic files, and production still runs `conf/start_app.sh`. Blockers **B1–B5** have an offline design and implementation in [`011`](011-production-launcher-cutover.md) with 121 offline assertions — **but none is closed, because none has been installed or validated on the host**. **B6** (AVX2, [`012`](012-b6-avx2-polars-decision-memo.md)) and **B7** (the deployment's dependencies) are both **OPEN**. `bash5A` added bash 5.x compatibility evidence for the proposed launcher and nothing more. Nothing is installed and no cutover is authorised. |
| **S2 performance validation** | latency, throughput, resource use for S2 | **rung 21 CLOSED 2026-08-19 (`s2pB`): S2 rung 21 PASS under the approved 0.05 engineering threshold** — spec `007` §12, record in spec `002`. Scope is **single-worker, steady-state, warm-cache request-path latency only**; 8 cases, judged individually, never pooled. It is **not** a production SLA and carries **no** throughput, multi-worker, startup, deployment or PM2/TLS conclusion — so the rest of this row's subject (throughput, resource use) is still **unmeasured**. **Rung 60 is NOT scheduled**: it needs its own authorisation and is not started because a pilot suggested it. |

**The remaining tracks, each separate from C1/C2 correctness and from one another.**
C1/C2 correctness validation is **complete**; **nothing below is closed by it**, and no
result below may be inferred from `c1r` or `c2k`:

| track | status |
|---|---|
| **D1 — real-store startup / depth characterization** | **OPEN.** Missing-group behaviour on the real store is uncharacterized; every variable and climatology outside the two `d1b` covered; deployment under PM2 with site enabled. |
| **Deployment validation (PM2, launcher, site/`.pth`, readiness, nginx, TLS)** | **OPEN.** Alternate-port staging closed at `pm2B`/`pm2G` against a synthetic store. No TLS, no proxy, production still runs `conf/start_app.sh`. |
| **B1–B5 — production launcher cutover blockers** | **OPEN.** Offline design and implementation exist in [`011`](011-production-launcher-cutover.md); **none is closed, because none has been installed or validated on the host**. Two things HAVE happened on the host and neither closes anything: `b1s1` is a **qualified staging-only stop-path PASS** on PM2 5.4.2 under uid 994 ([result](B1-staging-stop-path-result-b1s1.md)) — staging evidence only, **not** back-filled to production; and **Stage B** removed the dead `pre_stop` line from production's config ([result](B1-stageB-remove-prestop-result.md)) — a **configuration cleanup with no PM2 lifecycle operation**, explicitly **not** a B1 validation. |
| **B1 — production stop path** | **CLOSED — PASS on production**, 2026-08-29 ([result](B1-stageC-production-result.md)). PM2 5.4.2, owner `odbadmin`, `PM2_HOME=/home/odbadmin/.pm2`, exact app `woa23`, depth-2 tree, 4/4 recorded processes GONE, exit 0, port released, non-target apps unchanged; the app was recovered on the first attempt and answered one operator check with 200. **Closes the B1 STOP PATH and nothing else.** |
| **B2 / B3 / B4 / B5** | **OPEN.** Untouched by Stage C, and the live production argv confirms all four are still present: `woa23_app:app` (B2), hard-coded `-b 127.0.0.1:8050` (B3), no `WOA23_ZARR_STORE` (B4), `--reload` running in production (B5). Offline design exists in [`011`](011-production-launcher-cutover.md); **nothing is installed or validated on the host**. Roadmap: [`022`](022-b2-b5-cutover-roadmap.md) |
| **Real-store contract evidence (C1/C2)** | **EXISTS, and is limited.** `c1r`/`c2k` ran `woa23_app:app` (reference) against `api.app:app` (candidate) on the **REAL production store**, read-only via symlink — 64/64 cases accounted for, `regressions: []`, three-cycle stability. **What is missing is the production DEPLOYMENT's data path**, not the data: those arms ran under the benchmark harness as uid 994 on staging ports from a package clone. See [`022` §4](022-b2-b5-cutover-roadmap.md) |
| **A11 — API request count** | **QUALIFIED PROXY ONLY, and now demonstrably able to UNDERCOUNT.** In D-1 it read **delta 0 while nine queries were served and answered** ([memo](A11-marker-diagnostic-memo.md), [D-1](D1-production-datapath-characterisation-result.md)). Cause **UNRESOLVED** — five hypotheses, none selected. Markers also disagree by 17, are cumulative since 2024, and have no rotation policy. **May never be reported as an exact request count, and a delta of 0 may not be read as "no traffic"** |
| **c1r / c2k — real-store CONTRACT evidence** | **LIMITED, and it exists.** `woa23_app:app` (reference) vs `api.app:app` (candidate) on the **REAL production store**, read-only via symlink under uid 994 with **filesystem-enforced** read-only. 64/64 cases accounted for, `regressions: []`, three-cycle stability. **Produced by the benchmark harness**, not by a deployment |
| **D-1 — production deployment RAW OBSERVATION** | **COMPLETE.** 10/10 fixed requests to the live deployment, statuses as defined, bodies retained. **A11 UNATTRIBUTED** (delta 0 vs expected 9). **Records only — no adjudication, no PASS** ([result](D1-production-datapath-characterisation-result.md)) |
| **D-2 — offline response ADJUDICATION** | **LIMITED D-2 OFFLINE ADJUDICATION — NO UNEXPECTED REGRESSION IDENTIFIED** ([result](D2-offline-adjudication-result.md)). C1/C16 + CSV = `EXPECTED_COLUMN_ORDER_CHANGE` (same set, pre-change order); C20a = `EXPECTED_DOCUMENTATION_CHANGE` (1.0.0). **NOT a data-path PASS, NOT a deployment PASS, NOT runtime equivalence.** `c1r`'s byte reconstruction was **not** re-executed; C2/C3/C4 byte equality **not** re-confirmed |
| **STILL NOT DONE** | **candidate production-deployment equivalence** (the candidate has never run as a deployment) · **TLS** (never exercised; D-1 used `--insecure` to a loopback address) · **full store identity** (metadata only, no content digest, no content-integrity proof) · **B2–B5** |
| **Not validated by B1** | **production-deployment data-path serving** (the operator check never reaches the query handler; the Zarr store was never opened by it — note C1/C2 DID exercise the real store, but under the harness, not the deployment) · **TLS correctness** (`--insecure`; the certificate was not validated) · **store content identity** (metadata-level only; a same-size same-mtime change is undetectable) · **latency / throughput / deployment / full runtime equivalence** — none claimed |
| **Operator requests against production** | **the only production HTTP request explicitly recorded by this campaign was the operator OpenAPI check** — `GET https://127.0.0.1:8050/api/swagger/woa23/openapi.json`, 2026-08-29T10:06:14Z, status 200, labelled **OPERATOR CHECK**, not organic traffic. **This is a statement about what is recorded, not a claim that no other request was ever issued** — no access-log evidence exists to support the stronger claim |
| **Retained state — NOT cleaned** | `b1s1` daemon `1761143` + tree · `bs3v1` daemon `1709473` + tree/bootstrap/workdir · `~/woa23-b35a1/` · `pm2G`/18265 still bound. **Cleanup remains a separate authorisation** ([`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md)) |
| **`conf/simu.sh`** | **OPEN, separate request.** Still greps `tide_app` directly and kills another project by command-line match. Unlike the config's former `pre_stop` (inert — PM2 5.4.2 has no such hook) it is a **shell script that runs if invoked** |
| **Production B1 — Stage C (superseded row)** | **COMPLETE.** Formerly: Stage A (read-only inventory) and Stage B (dead-config cleanup) are complete; **Stage C is the only stage that would validate B1 on production and it has not been requested.** Remaining blockers: **A11** — the API request count has **no reliable exact source**, and the log-marker method is a **QUALIFIED PROXY ONLY**; store identity is **metadata-level only** (33G, 123,005 files); downtime/restart/recovery policy **undecided**; `conf/simu.sh` still carries the same kill-by-grep technique, including a direct `tide_app` line, and is a **separate issue**. |
| **B6 — AVX2 / polars build** | **OPEN.** [`012`](012-b6-avx2-polars-decision-memo.md); a host-configuration defect in the API's hot path. |
| **B7 — the deployment's dependencies** | **OPEN.** |
| **PM2 / production cutover** | **OPEN and not authorised.** Nothing is installed. |
| **S2 performance validation** | **rung 21 CLOSED** (`s2pB`, single-worker warm-cache request-path latency, 8 cases). Throughput, resource use, multi-worker and startup were never in scope and remain **unmeasured**. Rung 60 is **not** scheduled and needs its own authorisation. |
| **Row-order contract — consumer risk** | Decision MADE (`008`); spec 003 §6 **Q2 remains UNKNOWN** — whether any consumer depends on today's order — and is recorded as consumer risk, not assumed away. The hosted Swagger hub page is served by another system and no test here can read it: one human step. |

**C1 and C2 closed the correctness question they were scoped to and nothing else.**

C1's and C2's results are recorded below and in `specs/docs/BASELINE.md` under
*non-performance contract evidence*. **Neither carries any performance meaning and
neither means the deployment is validated.**

The three host-configuration defects below are separate from C1 and remain open:

Three defects found on the host that are configuration, not code:

- Polars is running a build that requires AVX2/BMI1/BMI2/LZCNT on a VM where
  VMware masks those features. Polars itself warns it "will likely result in a
  crash". Both `polars` and `polars-lts-cpu` 1.27.1 are installed and the AVX2
  build is the active one. This sits in the API's hot path.
- `--reload` is live in production gunicorn (`conf/start_app.sh`), on WOA23 and on
  the neighbouring services.
- The CSV endpoint creates `NamedTemporaryFile(delete=False)` and never removes it
  (`woa23_app.py:440`).

Each needs its own before/after evidence — in particular, whether switching to the
LTS polars build changes throughput at all, and whether it changes any returned
value.

### S2b — Deterministic output ordering

**Spec:** [`008-s2b-deterministic-row-order.md`](008-s2b-deterministic-row-order.md)
· **Status:** **contract decision MADE 2026-08-19; implementation planned, not
implemented** · **This is an API contract change, not a refactor**

**The decision (PI, 2026-08-19), settling spec 003 Q3 and Q4:** rows are ordered
ascending by **`(time_period, depth, lat, lon)`**, with `time_period` sorted
**numerically, never as a string**, and the contract binds **JSON and CSV alike**.
Spec 003 §6 Q1 is answered — the published surface states no order — and **Q2, whether
any consumer depends on today's order, remains UNKNOWN and is recorded as consumer
risk rather than assumed away**.

**Scope is row order only**: JSON field order and CSV header order are unchanged, spec
006 option B is not implemented, and no value, column name, status or query semantic
moves. **It changes `api/`**, so C1 and C2 must be re-run under a new execution
identity, `c1e`/`c2f`/`s2pB` may not be back-filled onto the new candidate, and no
deployment or performance claim may be made before the new C1/C2 pass.

**IMPLEMENTED AND VALIDATED, not deployed.** `693f00f` is the candidate change
(`api/query.py`, one sort); `1439194` is the harness — contract variant **5.2C**, the
C2 candidate-only order gate and their tests. Both controlled gates have passed on
`1439194`: **C1 `c1f`** (5.2C — canonical values/columns 64/64, candidate row-order
contract 44/44 applicable) and **C2 `c2g`** (5.2B semantic PASS ×3, seed diversity
OBSERVED 3/3, **row-order contract PASS**, candidate order identical across three
unpinned starts at production's measured worker count while the reference varied on
`C16`/`C16-csv`). Records in spec 002 under `c1f` and `c2g`; evidence summary in spec
008 §10.

**Published as API 1.1.0.** The PI set the version on 2026-08-19 and framed it as a
**row-order contract change, not an endpoint migration** — routes, request parameters,
response schema and the Swagger URLs are unchanged, so **no consumer moves to a new
URL**. The OpenAPI document, both endpoint descriptions and `README.md` now carry the
statement (spec 008 §9).

That `api/` edit does **not** cost a C1/C2 re-run, and the exemption is **decided by
machine**: `bench/docs_only_diff.py` compares the two revisions' ASTs with docstrings
and the two allowlisted `get_openapi` strings removed, and requires the rest to be
identical; `bench/test_docs_only_diff.py` exercises it rejecting every forbidden
category. **A diff that fails the check may not be re-classified as docs-only** — it
needs a new execution identity and a fresh C1/C2.

**The sort's cost is closed for now**: audited and benchmarked offline
([`009`](009-sort-cost-validation.md)), and per the PI **no VM24 measurement is
required**. It is an informative estimate, never a production-overhead figure.

**What remains before deployment**, each separately authorised:

- ~~**PM2 alternate-port staging validation**~~ — **DONE 2026-08-20 (`pm2B`), PASS**
  against a synthetic store ([`010`](010-pm2-staging-validation.md) rev 4);
- **the polars CPU baseline decision** ([`012`](012-polars-cpu-baseline-decision.md)),
  **open** — if it changes the runtime, the contract and performance evidence must be
  re-established **first**;
- **B6** — show that the interpreter which will serve production carries the candidate's
  dependencies ([`011`](011-production-launcher-cutover.md) §4). A read-only check,
  needing its own authorisation;
- **the versioning and announcement decision** (spec 008 §9.3), still open;
- **installing the proposed launcher into `conf/`** — reviewable at rest, and a separate
  act from the cutover;
- then a **production cutover**, which staging does not substitute for.

`woa23_app.py:160,171,190` build `list(set(...))` over strings. Python randomises
string hashing per interpreter, so `variables` (which drives **column order** after
the pivot) and `pars` (which drives **row order**) come out in an order that is
fixed for the life of a gunicorn master but **changes on every restart**. Verified
across seeds 0–7: `variables` alternates between `['an','mn']` and `['mn','an']`,
and `pars` takes three different orders.

Twenty-four live production requests all returned the same column order, because
gunicorn's workers are forked from one master and inherit its hash seed — so the
instability is invisible until a restart.

Whether any consumer depends on the current ordering is **unmeasured**, and this
roadmap does not assume an answer. Column and row order are user-visible output, the
API is published with a DOI, and we have no telemetry on how clients parse it.
Making the ordering deterministic is therefore a contract change and the PI's call.
If it is wanted, the honest first move is to find out who is affected — nginx access
logs would at least show the shape of real traffic — rather than reasoning about
what a consumer ought to be doing.

S1 deliberately preserves the existing behaviour verbatim so its before/after stays
about Dask. **S2b is where that preserved defect is fixed** — for rows. The
`list(set(...))` calls that drive **column** order stay exactly as they are; spec 008
§3 puts them out of scope, because changing them would be a second contract question
in the same commit.

### S2c — Dependency modernisation

**Spec:** not yet written · **Status:** blocked on S1

Production pins are a year or more old: FastAPI 0.115.12, Uvicorn 0.34.1,
Starlette 0.46.2, Gunicorn 23.0.0, ORJSON 3.11.4, Pydantic 2.11.3, polars 1.27.1,
xarray 2025.3.1, zarr 2.18.6, dask 2025.3.0. Newer releases may carry real
throughput gains — polars in particular, given S2's CPU-baseline problem, and
`polars-runtime-32 1.35.2` is *already installed* on VM24 alongside 1.27.1.
Zarr v3 is a larger question with data-format consequences.

Each upgrade needs its own paired benchmark and a contract re-verification; a
version bump that changes a returned value is a regression, not an upgrade. Kept
out of S1 so that S1's A/B isolates the Dask change alone.

### S3 — Cache opened Zarr datasets

**Spec:** not yet written · **Status:** blocked on S1

Worth ~10% measured. Deferred behind S1 because it is small, and because it
interacts with the gunicorn multi-worker model, pm2's `max_memory_restart: '4G'`,
and the OS page cache over a 32 GB store. The obvious implementation has
non-obvious failure modes.

**Possible sub-item, if its cost turns out to be low: the empty-CSV change of spec
006.** A valid query with no matching data would return **200 with a header-only
CSV** instead of today's **400**.

**Flagged, not folded in.** This is an **intentional API behaviour change**, and a
candidate change that carries it must:

- state it as a behaviour change in its own right — a client that today branches on
  the 400 will stop seeing it;
- add **JSON/CSV contract acceptance** for the empty-result pair, including that the
  empty CSV header is byte-identical to the non-empty header for the same query, and
  that the non-empty responses are unchanged;
- keep the error statuses that are **not** in scope — malformed requests, unsupported
  grid/variable combinations, an absent `time_period`, a missing `mn`, an unopenable
  group (spec 006 §2.2);
- **re-do C1 and C2 validation.** The current `c1e` and `c2f` results describe
  `919095e8` and **may not be backfilled as post-patch evidence**.

If that turns out not to be cheap, it stays where it is — a decision memo waiting for
a change to attach to.

### S4 — Concurrency: unblock the event loop

**Spec:** not yet written · **Status:** blocked on S1

`process_woa23_data` is `async def` but does blocking CPU and I/O work, so it
occupies the event loop; with `-w 2` that bounds real concurrency hard. **No
concurrency benchmark exists yet** — every number in `docs/BASELINE.md` is from
sequential probing. This step starts by building that measurement, against a
non-production target.

### S5 — Cold-cache measurement and the re-chunking decision

**Spec:** not yet written · **Status:** blocked on S4

Chunks are `{time_periods:1, parameters:1, depth:8, lat:90, lon:360}` for both
grids, so a single-cell profile decompresses 12.6 MiB to return 102 values
(×32,400 amplification). This is nearly free today only because the whole 31.9 GiB
store fits in VM24's page cache — cold, the same query takes ~2,140 ms against
19.6 ms warm.

**This step may legitimately conclude "do nothing".** Re-chunking costs a full
rebuild plus storage and optimises one access pattern at another's expense. The
decision needs a cold-cache benchmark that does not yet exist. Do not pre-commit.

### S6 — Cutover

**Spec:** not yet written · **Status:** blocked on S1–S5

Deploy the candidate, re-verify the contract against production, review the nginx
proxy cache policy for a dataset that never changes, and retire the old process.
The nginx cache is currently masking real performance from anyone measuring
casually — that is a measurement hazard, but for a static dataset it may also be
an under-used opportunity.

---

## Phase 2 — PostGIS / GeoServer (VM34)

Sketch only. To be broken into steps after phase 1. Evidence so far is in
`docs/BASELINE.md`; none of it is confirmed by `EXPLAIN ANALYZE`.

1. **Confirm or kill the `MIN/MAX(value) OVER ()` hypothesis.** All ten SQL views
   compute a global min/max inside the virtual table, which GeoServer's tile BBOX
   filter cannot be pushed past. If the hypothesis holds, every WMS tile scans a
   full global slice — 64,800 rows at 1-degree, ~1,036,800 at 0.25-degree.
2. **Precomputed statistics table** for the colour ramps, if (1) confirms.
3. **Storage model review.** `woa23` is 80 GB holding less data than the 32 GB
   Zarr store, because every cell stores a Polygon per depth per time period.
4. **Decide the fate of 54 GB of unreferenced tables.** `grd025_monthly_ts` and
   `grd025_seasonal_ts` (236 M rows) have no GeoServer layer and no mention in
   GeoServer's query logs. PI's decision.
5. **Rewrite the zarr→PostGIS ingest.** `dev/zarr2postgis.ipynb` inserts row by row
   through four nested Python loops, building a Shapely polygon per cell.
6. **GeoServer / GWC tile-cache review.**

## D2b controlled run, 2026-08-08 — the first result that may be quoted

**Commit `48ca10f`**, VM24, workdir `~/woa23-s1-controlled-r6`. Both gates PASS and
cleanup confirmed itself. Three earlier attempts (r2, r3, r4) are recorded as FAIL
and **none of their numbers are carried into this table** — they were produced by a
harness whose cleanup could not verify itself, and two of them by one that let the
benchmark introduce a difference of its own.

### Contract gate — PASS, 64/64 byte-exact

Variant 5.2A, all 64 verdicts `MATCH`, no `DIFFER`, no `ERROR`. Request order
counterbalanced **RC 32 / CR 32**, recorded per case in the artefact.

`C16` and `C16-csv` — the two multi-group cases that differed on 2026-08-08's first
attempt — match byte for byte here. The candidate's ordering logic was never
changed: `api/query.py` is `93b64fc0…`, the same blob as at `ee30084`. What changed
is that the benchmark stopped introducing a difference: both arms now interpolate the
identical store literal.

### Latency gate — PASS, 8/8

rung 21, 21 warm samples per arm (+1 discarded), ±5% practical-significance margin,
bootstrap 5,000 rounds, seed 20260805. `ratio` is candidate ÷ reference, so lower is
faster.

| case | ratio (median) | 95% CI | ≈ speedup | verdict |
|---|---|---|---|---|
| `point_profile_multiparam` | 0.0651 | [0.0641, 0.0660] | ~15.36× | NO_REGRESSION · IMPROVED |
| `point_profile` | 0.1045 | [0.1026, 0.1069] | ~9.57× | NO_REGRESSION · IMPROVED |
| `point_profile_025` | 0.1162 | [0.1120, 0.1209] | ~8.61× | NO_REGRESSION · IMPROVED |
| `readme_example` | 0.1524 | [0.1477, 0.1549] | ~6.56× | NO_REGRESSION · IMPROVED |
| `small_bbox_full_depth` | 0.2115 | [0.2094, 0.2147] | ~4.73× | NO_REGRESSION · IMPROVED |
| `regional_bbox` | 0.4357 | [0.4284, 0.4471] | ~2.30× | NO_REGRESSION · IMPROVED |
| `regional_bbox_025` | 0.4931 | [0.4414, 0.5077] | ~2.03× | NO_REGRESSION · IMPROVED |
| `surface_global` | 0.7795 | [0.7632, 0.7918] | ~1.28× | NO_REGRESSION · IMPROVED |

Every case establishes **both** `NO_REGRESSION` and `IMPROVED`, and every interval
lies wholly below 1.0.

### What made this run controlled

| variable | D2a (2026-08-07) | here |
|---|---|---|
| package environment | 23 shared distributions differed | **one venv**, `distributions_sha256 cca6aa8460ab175a` on both arms |
| interpreter | production's, unverified | **Python 3.11.4**, 58 distributions, both arms |
| hash seed | production's could not be pinned | **`PYTHONHASHSEED=0`** on both arms |
| store path string | absolute vs `data/` | **`'data/'` on both**, canonical store `/home/odbadmin/python/woa23/data` on both |
| comparison | semantic | **byte-exact 5.2A** |
| request order | reference always first | **RC 32 / CR 32** |
| boot | — | `7c674929…` on both arms |

`post_run_drift: []`, `metadata_complete: true`, `metadata_problems: []`.

### Traffic and isolation

- **production `8050`: 0 requests.** The harness was invoked against
  `http://127.0.0.1:8051` and `http://127.0.0.1:8052` only.
- production's shared Dask scheduler on `8786` was never contacted; this run used an
  isolated scheduler on `127.0.0.1:18787`.
- 4 services / **6 OS processes**, verified against the authorised set before the
  gates ran.
- `~/python/woa23` was never written; the reference read the store through a
  read-only symlink.

### Cleanup — PASS

All four services stopped, every process in every recorded tree confirmed exited,
all three ports confirmed free, state files removed, and production on `8050`
unchanged across the run — full listener set `[3960 4334 4366]`, master PID, start
time and boot id all matching what was recorded at preflight.

### What this result is, and what it is not

It is an **8-case, warm-cache, loopback, single-worker controlled comparison** of the
same code with and without Dask on the read path, at rung 21.

It is **not**:

- a public SLA or any statement about what users experience;
- a cold-cache measurement — the 31.9 GiB store was resident in VM24's page cache,
  and these ratios say nothing about a cold store;
- a concurrency result — both arms ran `-w 1` and requests were issued one at a
  time, so nothing here describes behaviour under load;
- a measurement over TLS, nginx or the public path — it is plain HTTP on loopback;
- generalisable beyond these eight queries.

`surface_global` at ~1.28× is the smallest gain and the closest to the margin, which
is expected: it is the largest query, so decompression and serialisation dominate and
Dask's scheduling overhead is proportionally smallest. It is the case to re-examine
first if the rung or the case set changes.

Rung 60 and rung 150 were **not** run. `next_rung: 60` in the artefact is the
ladder's suggestion, not an authorisation.

---

## C1 controlled run, 2026-08-09 — isolated package-tree contract correctness

**Result: C1 PASS — isolated package-tree contract correctness.**

Read the name in full. It is not "C1 PASS", and §"What this is not" below is part of
the result rather than a caveat attached to it.

| | |
|---|---|
| commit | **`c1166bfa7110ad7687360cab06e74fa35a2ac119`** |
| archive verified before shipping | `cc960eb760eef6927f8ddabaf3c6a75f3f70727e785179ac64222d4c8ad21490`, 75 files, file-list `4ee0d631…baac5b74` |
| staging | `~/woa23-s2-c1d/`, workdir `~/woa23-s2-c1d-work/`, both new |
| gate | **5.2A byte-exact**, 64 cases per arm |
| verdict | **64/64 MATCH, 0 DIFFER** |
| request order | **RC 32 / CR 32**, counterbalanced |
| bytes compared | reference 24,440,431 / candidate **24,440,431** |
| error-status cases | **15** (400 and 404) — **byte-exact MATCH as well**, not excluded |
| captured | 2026-08-09T19:01:40+0800 on odb24 |

### The environment both arms ran in

Production's own interpreter, against the read-only clone of production's package
tree. Identical on both arms, field for field:

| | |
|---|---|
| interpreter | `/home/odbadmin/.pyenv/versions/py311/bin/python3.11`, named explicitly, not derived |
| `PYTHONHASHSEED` | **0**, both arms |
| `store_path_literal` | `'data/'`, both arms |
| `clone_manifest_sha256` | `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4` |
| `package_tree_digest` | `b8754d32c8aaec6d2049de5d67d3f81aeff4f19effd1525d76b62955447c9b4b` (240 dist-info directories) |
| `runtime_distribution_digest` | `a26ca6c3cfe20ea643c30075d910bb03dbbc01eb3f9d2b4fd224b5d76701447b` (236 with `METADATA`) |
| `name_version_set_sha256` | `60236d7210c8c3647a32e7da55714e246ecc878d1d6296d10e9caf966d2b0b2a` — the **deprecated** name-keyed canonicalization (spec 002 §7a.3f). **It is not the runtime-distribution digest.** |

The **harness bootstrap is a separate record**: `dev2026/.venv`, 58 distributions,
lockfile `0d2980a5…`. It runs the comparator and is on no arm's import path. `uv`
was authorised for that directory and nothing else; the clone and production
site-packages were never written.

### Clone integrity — three full verifications, all MATCH

| stage | problems | entries / files | bytes | seconds |
|---|---|---|---|---|
| preflight | 0 | 33,565 / 33,565 | 1,690,025,002 | 29.59 |
| before-reference | 0 | 33,565 / 33,565 | 1,690,025,002 | 6.89 |
| before-candidate | 0 | 33,565 / 33,565 | 1,690,025,002 | 6.85 |

Clone parent mode **555**; ancestor chain checked; every file re-hashed each time,
not sampled.

### Import isolation

Both arms: no problems, `no_site=1`, `ignore_environment=0`. Two tracked processes
each; 342 and 343 mapped files respectively, **0 from production**.

### The two cases that differed in the D2b run of 2026-08-08

C16 (37,083 bytes, 204 rows) and C16-csv (10,843 bytes) — **both MATCH**, with
identical row-order digests (`56ccfd33…`) and identical column sequences on the two
arms. That difference was the benchmark handing the arms different store strings; here
both build group paths from `'data/'`.

### Traffic, processes and cleanup

- **6 OS processes**, derived from the measured worker count and verified against it.
- **Requests: 64 contract + 2 data probe + 1–30 readiness per arm = 67–96 per arm,
  134–192 total.** Ceilings were 96 and 192.
- **Production 8050: 0 requests.** 8786 and 8787 were never connected to.
- **Cleanup PASS.** All four services stopped, every process in every recorded tree
  exited, 18061/18062/18798 confirmed free, boot id matched, production unchanged
  (master 3960, start time 1874, listeners 3960/4334/4366). No `.pid`, `.tree`,
  `.uncertain` or `.diag` left behind.
- Afterwards: clone 33,565 files, **0 writable, 0 new `.pyc`**, manifest digest
  unchanged; production site-packages 33,567 files at mtime 2026-02-12;
  `~/python/woa23` at mtime 2026-08-05.

### What this result is

The candidate and the unmodified reference return **byte-identical responses across
all 64 contract cases** — including every error-status case — when both run on
production's interpreter and a read-only copy of production's package tree, with a
pinned hash seed, one worker each, in isolated staging.

### What this result is **not**

Each of these is a limitation of the run, not a hedge about it.

- **Not deployment validated, and not ready to deploy.** A contract gate compares two
  processes this harness started, in a staging directory, under `-S`, with a store
  symlink, launched by a shell script.
- **`-S` means `site.py` never ran**, so no `.pth` in the clone was processed —
  `distutils-precedence.pth` and the basemap nspkg `.pth` are present and did not
  execute. Production's site/`.pth` startup semantics were not exercised.
- **The launcher is not production's PM2 path.**
- **No worker-level Python import provenance exists.** The probe is a sibling
  interpreter launched by the same procedure; `/proc/<pid>/maps` can *refute*
  isolation but its silence proves nothing, because it lists mapped files and not
  imports. "The workers imported from the clone" is an inference.
- **`/home/odbadmin` is writable**, so this account can still re-point the clone's
  path. The three manifest verifications are **bounded detection, not immutability**.
- **D1 is not fixed.** A missing `WOA23_ZARR_STORE` fails at import and startup; an
  invalid or non-Zarr store still starts cleanly and fails on the first data request.
  No candidate change is proposed or made.
- **C2 has not been run.** Multi-worker behaviour and unpinned-seed ordering are
  unexamined.
- **No latency, throughput or resource conclusion of any kind.** None was measured;
  the latency gate and the noise pilot did not run.

---

## C2 controlled run, 2026-08-09 — semantic correctness at production's worker count

**Result: C2 PASS — isolated package-tree semantic correctness at production worker
count, with observed sibling seed diversity.**

The name is the result. §"What this is not" is part of it, not a caveat appended
to it.

| | |
|---|---|
| commit | **`5cfbf0aa70883af28ffd2e03ce21b4497ac30891`** |
| archive verified before shipping | `3f642dd0…ace0ba`, 78 files, file-list `3be3d046…7fe5c909` |
| staging | `~/woa23-s2-c2c/`, workdirs `~/woa23-s2-c2c-work-cycle{1,2,3}`, all new |
| cycles | **three independent start/stop cycles** |
| gate | **5.2B semantic**, 64 cases per arm per cycle |
| seed policy | **`both-unpinned`** — `PYTHONHASHSEED` **unset on both arms**, stated explicitly rather than inherited from the variant |
| verdict | **PASS (exit 0)** |

### Production's worker count, measured not assumed

Read from production's own argv at run time in **every** cycle: **actual = 2**.
`--expected-workers 2` was an assertion only; the arms take the measured number and
a disagreement would have aborted before any arm started. Each cycle also re-read
production's listener set, master PID, start time and boot id after the measurement
and confirmed them unchanged.

Process count is derived from that measurement — `2 Dask + 2 × (1 arbiter + 2
workers)` = **8 per cycle**, verified against the tracked trees:

| cycle | the eight processes |
|---|---|
| 1 | 3631498 3631556 3631617 3631619 3631639 3631715 3631717 3631718 |
| 2 | 3633166 3633229 3633282 3633284 3633285 3633387 3633389 3633390 |
| 3 | 3634816 3634879 3634934 3634936 3634956 3635039 3635041 3635042 |

### 1. Contract — 5.2B semantic, three of three

| | |
|---|---|
| gate | **PASS** |
| per cycle | `PASS`, `PASS`, `PASS` |
| cases | **64/64 per cycle** |
| problems | none |

### 2. Seed diversity — `OBSERVED`

| cycle | candidate seed digest | reference seed digest | `PYTHONHASHSEED` | `hash_randomization` |
|---|---|---|---|---|
| 1 | `d327c5f70cf4c105…` | `93eda1b784141c14…` | unset | 1 |
| 2 | `a529e1a76865da1b…` | `14f58361cfd4d38a…` | unset | 1 |
| 3 | `9bccce43aaec4040…` | `e253f07863f7e86d…` | unset | 1 |

**3 distinct digests across 3 cycles**, with no precondition problems — the seed was
genuinely unset, hash randomisation was genuinely on, and the fixed 11-string probe
was complete in every cycle. The `PASS_WITH_INSUFFICIENT_SEED_DIVERSITY` branch was
**not** taken.

Measured per cycle by hashing a fixed eleven-string tuple with the same binary, the
same `-S`, `PYTHONPATH`, cwd and environment each arm was launched with.

**This is sibling / launch-environment seed diversity.** The interpreter measured is
a sibling launched by the same procedure — **not** the gunicorn master and not any
worker that served a request.

### 3. Order stability — recorded, deliberately outside the verdict

| | |
|---|---|
| cases | 64 |
| comparable (case, arm) pairs across cycles | 94 |
| **varied** | **4** |
| responses with no row structure | 102 (counted, not called stable) |

The four are `C16/candidate`, `C16/reference`, `C16-csv/candidate`,
`C16-csv/reference` — and they are a separate finding, below.

### Traffic, cleanup and host state

- **Requests:** 64 contract + 2 data probe + 1–30 readiness per arm per cycle =
  **67–96 per arm per cycle**, **201–288 per arm** and **402–576 total** across three
  cycles. Ceilings were 288 and 576.
- **Production 8050, 8786, 8787: 0 requests, never connected to.** 8050 was read from
  `/proc` and `ss` only.
- **Clone integrity: nine full verifications, 9/9 MATCH** — three per cycle
  (preflight, before reference, before candidate), 33,565 entries against 33,565
  files, 1,690,025,002 bytes each time.
- **Cleanup: PASS in all three cycles.** Every service stopped, every process in every
  recorded tree exited, 18091/18092/18819 confirmed free each time, production
  unchanged (master 3960, start time 1874, listeners 3960/4334/4366). **Zero blocking
  state files across the whole `run/` tree afterwards.**
- **Per-cycle evidence is isolated and complete:** 10 result artefacts and 4 service
  logs under each of `c2_cycle1`, `c2_cycle2`, `c2_cycle3`; no cycle overwrote
  another.
- **Afterwards:** clone 555/555, manifest digest unchanged, 33,565 files, **0
  writable, 0 new `.pyc`**; production site-packages 33,567 files at mtime
  2026-02-12; `~/python/woa23` at mtime 2026-08-05.

### Finding — C16 and C16-csv: row order varies across cycles, semantics hold

Of the 64 cases, **exactly `C16` and `C16-csv` showed row-order variation across
cycles on both arms.** The other 90 comparable (case, arm) pairs were stable.

**Effect directly observed; source-level mechanism strongly supported.**

What was observed at runtime: the row-order fingerprints of those two cases, and only
those two, differ between cycles on both arms, under an unpinned hash seed, while the
5.2B semantic comparison passed for them in every cycle.

What is strongly supported but **not** established step by step at runtime: that this
arises because `zarr_group_paths` is a `set` of path strings whose iteration order
depends on the process's hash seed, so a query spanning more than one Zarr group
concatenates its groups in a per-process order. The supporting evidence is that C16
and C16-csv are precisely the two cases in the suite that span more than one group,
that the same two cases were the only ones to differ in the 2026-08-08 D2b run when
the arms were given different store strings, and that `bench/repro_c16.py`
demonstrates the mechanism offline on synthetic rows. **No runtime instrumentation
observed the set iteration inside a worker**, and none was authorised; the internal
causal chain is inferred from the shape of the effect and from offline reproduction,
not proven in the running process.

#### Traceability of the two observations

Both are reproducible from the run's own artefacts, under
`~/woa23-s2-c2c/dev2026/results/` on odb24.

**Seed digests** — `c2_summary.json → seed_diversity.per_cycle` reproduces exactly
the `seed_digest` field of each `c2_cycle{1,2,3}_interp_{candidate,reference}.json`;
all six match. Each of those records also carries the probe itself — 11 strings and
11 hashes — so the digests can be recomputed rather than taken on trust. The raw
values differ per cycle, not merely their digests: `hash("1_degree")` was
`-4854500566350811133`, `4447478972511239299`, `-4704986505660421793` on the
candidate and `9026776351318122430`, `3388466108566623068`, `-714907535411415113`
on the reference.

**Order fingerprints** — each `c2_cycle{1,2,3}_contract.json` records, per case and
per arm, `reference_order` and `candidate_order` with `body_sha256`,
`row_order_sha256`, `columns` and `n_rows`. There are **128 distinct (case, arm)
pairs** — 64 cases on two arms — and **each pair was observed three times, once per
cycle**, so the artefacts hold 384 observations in total. Of the 128 pairs, **94
carry a row-order digest** and 34 are responses with no row structure (those 34
account for the 102 orderless responses `c2_summary.json` counts: 34 pairs x 3
cycles). Recomputing the varied set from those artefacts gives exactly the four
`c2_summary.json` reports.

Case by case, the row-order digest per cycle:

| case | arm | cycle 1 | cycle 2 | cycle 3 |
|---|---|---|---|---|
| `C16` | reference | `56ccfd332e27…` | `56ccfd332e27…` | `a35ae14d930a…` |
| `C16` | candidate | `56ccfd332e27…` | `a35ae14d930a…` | `56ccfd332e27…` |
| `C16-csv` | reference | `56ccfd332e27…` | `56ccfd332e27…` | `a35ae14d930a…` |
| `C16-csv` | candidate | `56ccfd332e27…` | `a35ae14d930a…` | `56ccfd332e27…` |
| `C1` (stable, for contrast) | both | `c994a1ff7849…` | `c994a1ff7849…` | `c994a1ff7849…` |

The two cases take exactly **two** distinct orderings and no more, which is what a
two-element set admits. That is consistent with the proposed mechanism and is not on
its own proof of it — the count would look the same for any two-valued cause.

**It is not a defect against this gate.** 5.2B compares the row multiset and the
column set; row order is not part of the criterion, and under an unpinned seed a
per-process ordering is the expected consequence of the arrangement C2 exists to
observe. It matters because **any consumer that depends on row order would see it**,
and because a byte-exact comparison of these two cases across unpinned processes
would fail for a reason that is not a correctness defect. Whether the candidate
should impose a deterministic order is a separate question and a separate decision.

### What this result is

The candidate and the unmodified reference return **semantically equivalent
responses across all 64 contract cases, in each of three independent start/stop
cycles**, when both run on production's interpreter and a read-only copy of
production's package tree, **at production's measured worker count of two**, with no
pinned hash seed — and three independent starts were observed to choose different
hash seeds.

### What this result is **not**

- **Not deployment validated, and not ready to deploy.**
- **No latency, throughput or resource conclusion of any kind.** None was measured;
  the latency gate, the noise pilot and every rung above 21 did not run.
- **`-S` means `site.py` never ran** in any cycle, so no `.pth` in the clone was
  processed. Production's site/`.pth` startup semantics were not exercised.
- **The launcher is not production's PM2 path.**
- **No worker-level Python import provenance exists.** The interpreter probe is a
  sibling process; `/proc/<pid>/maps` can refute isolation but its silence proves
  nothing, because it lists mapped files and not imports.
- **The seed diversity is sibling / launch-environment level, not worker level.** It
  says three starts of that launch procedure chose different seeds. It does not
  measure the seed of any gunicorn master or worker that served a request.
- **D1 is still open.** A missing `WOA23_ZARR_STORE` fails at import and startup; an
  invalid or non-Zarr store still starts cleanly and fails on the first data request.
  No candidate change is proposed or made.
- **Formal deployment validation is still open** — PM2, site/`.pth` semantics,
  readiness, nginx and TLS.
- **Performance validation is still open** in its entirety for S2.

## C2 controlled run, 2026-08-10 (`c2f`) — semantic correctness **after the D1 patch**

**C2 PASS — isolated package-tree semantic correctness after D1 patch at production
worker count, with observed sibling seed diversity.**

**The first complete, valid C2 result on the patched candidate.** The C2 of
2026-08-09 (`c2c`) ran the *unpatched* candidate. Two attempts on the patched one did
not produce a result and are recorded as such, not quietly dropped:

| attempt | outcome | why |
|---|---|---|
| `c2d` | **cleanup FAIL**, cycle 1 | the arms inherited gunicorn's 30 s `graceful_timeout` while `STOP_WAIT_SECS` was 20, and a logging reentrancy race left one worker unreaped. The contract had passed. |
| `c2e` | **INVALID_PRE_START** | `--clone-manifest` was pointed at the clone root's `SHA256SUMS`, which is a digest list, not a manifest. Stopped at clone-integrity preflight; **no service started, no request issued**. |
| `c2f` | **PASS** | below |

`c2d` and `c2e` keep their evidence. Neither was re-run in place, and neither was
backfilled into this result.

### What ran

| | |
|---|---|
| commit | `249aa2749bc14d13bf82b9af9cf5385ac2cb2334`, archive `d77b275f79ae3ce036ab3d3d0a6ea0eac77c7339ff37439426284d4a9835caba`, 87 files |
| candidate `api/` | byte-identical to the C1-tested `919095e8` — `app.py d0d8c781…`, `config.py b8066414…`, `query.py 4e28bdd9…`, `store_paths.py 00cb80c2…` |
| staging / workdir / label | `~/woa23-s2-c2f/`, `~/woa23-s2-c2f-work-cycle{1,2,3}`, `c2f` — all new |
| ports | 18131 / 18132 / 18859 — **first use**, checked against `scripts/ports_used.tsv` before starting, and free on the host |
| clone | the existing read-only clone, `clone.manifest` (four columns, 33,565 entries) |
| seed policy | `both-unpinned` — `PYTHONHASHSEED` unset on **both** arms |
| processes | **8 per cycle**: two arms of arbiter + 2 workers, plus an isolated Dask scheduler and worker |

**Prerequisites were read from the host before any service started**, not taken from
the request: `STOP_WAIT_SECS=20` (source `default`; unset in the environment),
`ARM_GRACEFUL_TIMEOUT=10`, `assert_shutdown_budget` PASS, and production's worker
count **measured as 2** from pid 3960's own argv.

### 1. Contract — 5.2B semantic, three of three

| cycle | gate | cases | verdicts | request order |
|---|---|---|---|---|
| `c2f_cycle1` | **PASS** | 64 | 64 MATCH | RC 32, CR 32 |
| `c2f_cycle2` | **PASS** | 64 | 64 MATCH | RC 32, CR 32 |
| `c2f_cycle3` | **PASS** | 64 | 64 MATCH | RC 32, CR 32 |

### 2. Seed diversity — `OBSERVED`

Six digests from three independent starts, **all distinct**:

| cycle | candidate | reference |
|---|---|---|
| 1 | `b43233d7642d2319` | `dafdbaa61a9bb6da` |
| 2 | `2e04ce4cb4b0348a` | `14f537d43115d1f9` |
| 3 | `e744e91b3df4bc59` | `f346808f0ad1fe09` |

`PYTHONHASHSEED` unset and `hash_randomization=1` are **preconditions, not
evidence** — they say the interpreter was permitted to choose a seed, and are equally
true of three starts that chose the same one. Only the measured digests distinguish
those cases.

### 3. Order stability — recorded, deliberately outside the verdict

| | `c2f` (2026-08-10, patched) | `c2c` (2026-08-09, unpatched) |
|---|---|---|
| comparable (case, arm) pairs | 94 | 94 |
| **varied** | **2** | **4** |
| which | `C16/candidate`, `C16-csv/candidate` | `C16/candidate`, `C16/reference`, `C16-csv/candidate`, `C16-csv/reference` |
| responses with no row structure | 102 | 102 |

**The two runs involve the same two cases; they did not behave identically.** In
`c2c` both arms varied; in `c2f` only the candidate pair did, and the reference
returned the same row order in all three cycles.

**That difference is not itself a finding, and must not be read as one.** With no
pinned seed, three cycles landing on one order is compatible with coincidence — the
reference has three samples, not a demonstrated property. Nothing in this run
attributes the difference to the D1 patch, to the arms' code, or to anything else,
and no mechanism for it was investigated. What is established is what the table
says: which pairs varied, in which run.

Semantics held for these cases in every cycle: they are 5.2B MATCH throughout.

### 4. Shutdown budget — read back from the evidence, not assumed

| cycle | `STOP_WAIT_SECS` | source | arms' `--graceful-timeout`, from each arm's own `/proc/<pid>/cmdline` |
|---|---|---|---|
| 1 | 20 | default | recorded 10 · candidate 10 · reference 10 |
| 2 | 20 | default | recorded 10 · candidate 10 · reference 10 |
| 3 | 20 | default | recorded 10 · candidate 10 · reference 10 |

Status **CONSISTENT**. This is the relationship whose absence failed `c2d`: the
harness's wait must exceed what the arms are entitled to take, and both numbers are
now asserted before a cycle starts and read back afterwards from the processes
themselves.

### Traffic, cleanup and host state

**Requests — actual where recorded, and bounded where not:**

| component | per arm per cycle | recorded? |
|---|---|---|
| contract gate | **64** | yes — `request_order_counts` RC 32 + CR 32 in each `c2f_cycle{1,2,3}_contract.json` |
| store-readiness probe | **2** | yes — two probes per arm, in both orders, no retry path |
| process-readiness probe | 1–30 | **no** — the loop does not count its attempts |

So **396 requests are recorded as actually issued** (66 per arm per cycle × 2 arms ×
3 cycles), and process readiness adds an unrecorded 1–30 per arm per cycle. **Total
actual: between 402 and 576.** The ceiling declared before the run was 576 per run,
96 per arm per cycle. *The unrecorded component is a gap in the harness, not in this
report: `process_ready` should return its attempt count so the actual total is exact.
No change has been made and no rerun is proposed for it.*

- **Production 8050, 8786, 8787: 0 requests, never connected to.** Read from `/proc`
  and `ss` only. Production **unchanged** at every check in all three cycles: master
  3960, start time 1874, listeners 3960/4334/4366, boot id matched.
- **Clone integrity: nine full verifications, 9/9 OK** — three per cycle (preflight,
  before reference, before candidate). Each: 33,565 manifest entries against 33,565
  files on disk, 1,690,025,002 bytes re-hashed in ~7.0 s, **0 missing, 0 extra, 0
  digest mismatch, 0 size mismatch, 0 mtime mismatch, 0 unreadable**; manifest
  `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4`.
- **Clone integrity is detection, not immutability.** `/home/odbadmin` is writable by
  this account — recorded as residual exposure at every check. The window between a
  verification and a worker opening a file is narrowed, not closed.
- **Cleanup: PASS in all three cycles.** Every service stopped, every process in every
  recorded tree exited, 18131/18132/18859 confirmed free each time. **Zero blocking
  state files across the whole `run/` tree afterwards** — no `.pid`, `.starttime`,
  `.tree`, `.diag` or `.uncertain`. Each `run/c2f_cycle{1,2,3}/` holds its four
  service logs, which is what a clean stop leaves behind.
- **The store was not written.** `~/python/woa23/data` mtime remains 2025-04-18
  14:31:47 — *no modification observed*; the basis for the claim is that the runner
  performs no write to it.
- **Earlier evidence untouched:** `c2c`, `c2d` and `c2e` show zero file changes.

### Archive

`~/woa23-s2-archive/2026-08-10-c2f-PASS/` — outside any deploy directory, **50 evidence
files** (37 result artefacts, 12 service logs, the run log) **plus a separate
`SHA256SUMS`**, which is the manifest of those 50 and is not one of them. Each
evidence file was hashed at the source, copied, re-hashed at the destination and
compared; `SHA256SUMS` is
`ffa68f58a6f8831ca4487ee787aef6cb4e8ffd5d8bd79716fe2cb8912e87e101`. Directories 555,
files 444, verified unwritable. **The originals under `~/woa23-s2-c2f/` were copied,
never moved, and are unchanged.**

### What this result is

The candidate — **with the D1 store-startup patch applied** — and the unmodified
reference return **semantically equivalent responses across all 64 contract cases, in
each of three independent start/stop cycles**, when both run on production's
interpreter and a read-only copy of production's package tree, **at production's
measured worker count of two**, with no pinned hash seed. Three independent starts
were observed to choose different hash seeds, and all three cycles stopped cleanly.

### What this result is **not**

- **Not deployment validated, and not ready to deploy.**
- **No latency, throughput or resource conclusion of any kind.** None was measured;
  the latency gate, the noise pilot and every rung above 21 did not run.
- **`-S` means `site.py` never ran** in any cycle, so no `.pth` in the clone was
  processed. Production's site/`.pth` startup semantics were not exercised.
- **The launcher is not production's PM2 path.**
- **No worker-level Python import provenance exists.** The interpreter probe is a
  sibling process; `/proc/<pid>/maps` can refute isolation but its silence proves
  nothing, because it lists mapped files and not imports.
- **The seed diversity is sibling / launch-environment level, not worker level.** It
  says three starts of that launch procedure chose different seeds. It does not
  measure the seed of any gunicorn master or worker that served a request.
- **The candidate's startup anchor validation reading no data or coordinate chunk is
  offline-audited and implementation-supported, not observed on this host.** This run
  installed no audit hook and recorded no file-open events.
- **D1 is only partly closed.** The patch makes an invalid or non-Zarr store fail
  before the service is ready; **real-store depth characterization is still
  CHARACTERIZATION PENDING** and was not run.
- **The row-order contract decision is still open** — no sorting, no pinned seed.
- **Formal deployment validation is still open** — PM2, site/`.pth` semantics,
  readiness, nginx and TLS.
- **Performance validation is still open** in its entirety for S2.

## D1 characterization run, 2026-08-11 (`d1b`) — real-store nitrate depth behaviour

> **Evidence availability, recorded 2026-08-13.** The observations below were
> recorded when the run happened. **The primary artefacts — `d1b_d1.json`,
> `d1b_requests.json`, `d1b_workers.json`, the per-arm provenance records and the
> store survey, under `~/woa23-d1b/results/` on VM24 — are no longer obtainable**,
> the staging and workdir having gone with the 2026-08-11 snapshot rollback. What
> survives is **secondary**: this document and its counterparts in `ROADMAP.md` and
> `specs/docs/BASELINE.md`, and the session console transcript
> (`c5f5a13a-b21d-4abd-bd3c-71c34e797b02.jsonl`, SHA-256
> `e22c9dc0ed5b4f41b8967cd8cd66173df9df67d960ca8a609009da27f5424bfc`), which holds
> the run's output but no artefact bodies.
>
> **No artefact-shaped evidence has been or may be reconstructed from these
> records, and no result here has been back-filled.** A file rebuilt from a
> transcription would be indistinguishable in shape from one a run wrote, and that
> is precisely the distinction this note exists to keep. The figures below stand as
> what was recorded on the day, at secondary-record strength, and are not
> re-derivable.
>
> This does not block S2: no S2 gate depends on `d1b`.

**D1 CHARACTERIZATION RECORDED — real-store nitrate depth behavior under the
declared one-worker scope.**

**Not a D1 PASS, and D1 is not complete.** This records what the real store returns
for two nitrate depth queries. It closes neither D1 as a whole nor any of the parts
listed under *What is still open* below.

Commit `0780566e55da3ff7ba2ec874cd1996a492ed2794`, archive
`3089d8d91f9462bf5577b241fe1034670b8cc915f755ac796b2b55e2f3cae142`, 103 files,
file-list `7e377e5e65d314928291b833c540a79641e6269537a1594171b790062fe93736` —
all three verified on the host before anything started. Staging `~/woa23-d1b/`,
workdir `~/woa23-d1b-work`, label `d1b`, first-use ports 18151/18152/18879.
`api/` byte-identical to the C1-tested `919095e8`.

### The observations

**Recorded, not judged.** No status or body was compared with an expectation; what
follows is what the store and the API returned.

| case | JSON | CSV |
|---|---|---|
| **annual nitrate, 0–800 m** (`D1-DEPTH-SUP`) | **200**, 3075 B, 43 rows | **200**, 872 B, `text/csv; charset=utf-8` |
| **winter nitrate** (`time_period=13`), **3000–4000 m** (`D1-DEPTH-OOR-tp13`) | **200** with **`[]`**, 2 B | **400** with **`No data available for the given parameters.`**, 56 B |

- **Both arms returned identical bytes** for all four cases — the candidate and the
  unmodified reference agree, so none of this is a candidate defect.
- **8 of 8 case observations are byte-identical to the `d1a` run** of the same day
  (status, `Content-Type` and body digest), and the anchor probe's body matched too.
  The behaviour reproduced across two independent runs.
- **An anchor recovery probe followed each case and returned 200** in every instance,
  with every process in each arm's tree still alive. The failures observed are
  confined to their own request.

### Preconditions — P1–P6, before any service or HTTP

`PRECONDITIONS_MET`. The survey runs after the store symlink and before either arm
starts, so it precedes every HTTP request rather than only the data-path ones.

> **P4 Table 4 count/extent compatibility verified; full Table 3 level-list equality
> not verified.**

| group | levels | extent | dtype | units | `levels_sha256` | Table 4 row |
|---|---|---|---|---|---|---|
| `1_degree/seasonal/Nutrients` | **43** | 0.0–800.0 | `float32` | **none declared** | `fd44cd93df8b…` | Seasonal / Nitrate |
| `1_degree/annual/Nutrients` | **102** | 0.0–5500.0 | `float32` | **none declared** | `fc5db2e1330e…` | Annual / Nitrate |

**No depth level is selectable in 3000–4000 m** on the seasonal group — asked with
the API's own selection semantics, not by comparing maxima. The annual group has
**43 levels selectable in 0–800 m**. Both digests are identical to `d1a`'s, so the
store's axes did not move between the runs.

Each group was judged against **its own** Table 4 row. Seasonal nitrate's 43 levels
over 0–800 m is not applied to the annual group, to the other variables or to the
store.

### What was observed about the store but NOT characterized

- **Missing-group behaviour on the real store is not characterized.** All **12 of 12**
  query-reachable groups exist and open, so no real missing group was available to
  request. The synthetic D1-D3 evidence remains the only evidence for that path.
  `query_reachable` means *query-path reachable by API logic; no HTTP request was
  sent* — the survey runs before any service exists.
- **`mn` and the three nutrient variables are a schema observation only.** `mn` — the
  statistical-mean data field of WOA23 Table 2, not a coordinate and not an
  oceanographic variable — is present in both groups, and `parameters` holds
  `nitrate`, `phosphate` and `silicate`. **This run characterizes nitrate.** No
  request was issued for phosphate or silicate, so nothing is known about what the
  API returns for them.

### Traffic, isolation and cleanup

- **Requests: measured, not bounded.** 11 per arm — 1 readiness, 2 store probe, 4
  characterization, 4 anchor recovery — **22 in total**, counted as *attempts* and
  not successes. Recorded in `d1b_requests.json`; the 22–80 range is the ceiling and
  this is the number.
- **One worker per arm**, `-w 1`, asserted against each arm's own
  `/proc/<pid>/cmdline` and recorded with both full launch argv.
  **D1 uses one worker per arm by design. This is not a production-worker-count
  validation and makes no claim about multi-worker D1 behavior.**
- **Clone integrity 3/3**: 33,565 manifest entries, 0 mismatched, 0 missing, 0 extra.
  Detection with a bounded window, not immutability — `/home/odbadmin` is writable by
  the account and that is recorded at every check.
- **Production 8050 / 8786 / 8787: 0 requests.** Master 3960, listeners
  3960/4334/4366, boot id unchanged throughout.
- **Cleanup PASS.** Every service stopped, every process in every recorded tree
  exited, all three ports confirmed free, **zero blocking state left**.
- **All post-run artefacts finalised** — the step that failed in `d1a` and left it
  classified `INVALID_POST_MEASUREMENT_HARNESS`.

### What this result is

For the two queries it issued, against the real store: the API's current behaviour,
measured, with the candidate and the reference in agreement and the observations
reproduced across two runs.

### What this result is **not**

- **Not a D1 PASS and not D1 complete.**
- **Not a validation of the JSON/CSV divergence it observed.** That the CSV path
  answers 400 where JSON answers `200 []` is recorded as **current behaviour**. The
  adopted policy (spec 006) is that both should be 200 — **not implemented, not
  tested, and not what this run measured**. The recorded 400 stands as measured and
  is not to be rewritten.
- **Not a characterization of anything but winter nitrate and annual nitrate.** Not
  `time_period` 14, 15 or 16; not monthly nitrate; not phosphate or silicate; not TS
  or oxygen. Each has its own Table 4 row and would need its own case and evidence.
- **Not a real-store missing-group characterization** — no group was missing.
- **Not a production-worker-count validation** — one worker per arm is fixed by the
  mode.
- **No latency, throughput or resource conclusion.** None was measured.
- **`-S` means `site.py` never ran**, so no `.pth` was processed, and the launcher is
  not production's PM2 path.
- **No worker-level import provenance.** The interpreter evidence is a sibling
  process and `/proc/<pid>/maps`.
- **The zero-chunk property of the startup anchor validation is unchanged**:
  offline-audited and implementation-supported, **not observed on this host**. The
  P1–P6 survey **does** read coordinate chunks, which is a separate operation.

### What is still open in D1

Real-store missing-group behaviour (unavailable while every group exists), every
variable and climatology outside the two characterized, and deployment validation
under PM2 with `site.py` enabled.

### Follow-up noted, not a D1 item

The JSON/CSV divergence this run observed prompted a small API contract decision,
recorded in **`specs/006-json-csv-empty-result-consistency.md`**: for a valid query
with no matching data, both formats should return **200**, with CSV carrying the
same query's **header-only** schema (option B, adopted). **Not implemented.**

**It is not a D1 blocker and not a track of its own.** D1 measured behaviour; the
memo decides what the behaviour should become, and nothing in D1 waits on it. When
it is implemented it belongs to whichever candidate change carries it — see S3 —
and it is an **intentional API behaviour change**, not a refactor.
