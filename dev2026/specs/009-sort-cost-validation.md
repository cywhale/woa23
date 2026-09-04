# 009 — Sort-cost validation for the S2b row-order contract

**Status: CLOSED for now. Offline audit and offline benchmark DONE; the PI decided on
2026-08-19 that no VM24 sort-cost measurement is required (§3a).** No VM24 process has been started for this, no HTTP request sent,
nothing deployed, and **no `api/` change is made or proposed**.

| rev | date | change |
|---|---|---|
| 1 | 2026-08-19 | First draft. Source audit against the PI's eight implementation principles, an offline sort-only benchmark, and the design of the VM24 measurement that would answer the question this one cannot. |

---

## 1. What the sort's cost is, and is not, in this campaign

The PI settled its standing on 2026-08-19, and this section is that decision written
down so nothing drifts:

- it is an **informative measurement**;
- it is an **optimisation input**;
- it is **not** a C1/C2 correctness gate;
- it is **not** a blocker on the S2b contract PASS;
- **being unmeasured does not license calling it negligible**;
- if the measurement shows the cost is high, the result is **reported and an
  optimisation proposed** — the **row-order contract itself is not changed** in
  response.

**`s2pB` may not be used to infer it.** That run measured
`c3b9398b4991539c7701feb0bb3bb5d6ddd564d7`, whose `api/` contains no sort. A tree
without the work cannot bound the cost of the work.

## 2. Source audit — the implementation against the eight principles

Audited at `api/query.py` in
`1439194a091a5c00ac9414ddd46a3898e51dad51`
(`api/query.py` sha256
`8e980e5b60a004902e66e6cb86ed2352a5ec641a6ad4cd3173a5a2efc56cebce`), the tree C1
(`c1f`) and C2 (`c2g`) validated.

| # | principle | result | evidence |
|---|---|---|---|
| 1 | Polars-native, vectorised sort | **holds** | `pl.DataFrame.sort` with expression keys; no Python-level comparison |
| 2 | sorted **once**, after all groups are merged and the pivot is done | **holds** | the single `sort` is the last statement before `return`, after `pl.concat`, `pivot` and both renames |
| 3 | no per-group, per-variable or per-intermediate sort | **holds** | one `.sort(` on a dataframe in the whole file; the only other is `periods.sort()`, which orders the **query's period list** for selection and touches no row |
| 4 | no return to pandas, no Python `list` sort, no manual record movement | **holds** | no `to_pandas`, no `sorted()` over records. The one `pl.from_pandas` is the **pre-existing** conversion of `xarray.to_dataframe()` output and predates this work |
| 5 | `time_period` cast to numeric **only in the sort key**; the output column keeps its string semantics | **holds** | the cast lives inside the sort expression; the column's dtype and values are untouched |
| 6 | no surplus temporary output column | **holds** | expression form, no `with_columns` for the sort, nothing to drop afterwards |
| 7 | JSON and CSV use the **same** sorted frame | **holds** | both handlers call `process_woa23_data` and serialise its return directly |
| 8 | values, columns, column order, status, empty-result and other query semantics unchanged | **holds** | a `sort` cannot change them, and `bench/test_row_order.py` asserts column sequence, row count and the row multiset are identical |

**All eight hold. No `api/` change is required, and none is proposed.** The PI's
instruction was explicit that a compliant one-line sort should not be rewritten for a
theoretical micro-optimisation, and there is no source evidence or measurement here that
would justify one.

## 3. Offline benchmark — what it measured, and what it cannot say

`bench/sort_cost.py`, offline: no store, no host, no HTTP. It times **the exact
expression the read path runs**, on frames shaped like the pipeline's output, at row
counts taken from real contract cases — including **214,812 rows**, the largest
candidate response `c1f` recorded.

Measured on the development machine (**not VM24**), polars 1.27.1, Python 3.11.14, 15
repeats per shape, median:

| shape | rows | cols | cyclic input | descending input |
|---|---|---|---|---|
| point profile | 102 | 5 | 0.20 ms | 0.23 ms |
| C16 multi-group | 204 | 7 | 0.29 ms | 0.17 ms |
| small bbox | 2,914 | 5 | 0.42 ms | 0.36 ms |
| regional bbox | 32,400 | 6 | 1.99 ms | 0.99 ms |
| **large result** | **214,812** | 7 | **6.19 ms** | **4.12 ms** |

At the largest shape that is **≈0.02–0.03 µs per row**.

**Two input orders are reported, and neither is a demonstrated worst case.** The first
draft of the benchmark called the descending frame the worst case; it is not — it
measured **faster** than the cyclic one, which is what a sort on already-monotonic input
tends to do. No adversarial input has been constructed, and none is claimed.

**What this does NOT establish, stated plainly:**

- **It is not the request's cost.** This is the numerator only. 6 ms inside a 200 ms
  request is a 3% effect; the same 6 ms inside a 12 ms request is not. The denominator
  needs the real store.
- **It is not VM24's number.** Different CPU, memory bandwidth and polars build. The
  order of magnitude should carry; the figures should not be quoted as VM24's.
- **It is not a two-tree comparison.** The old tree does no sort, so its sort cost is
  zero by construction and a ratio would be meaningless. The comparison that matters is
  end-to-end.
- **It is not a production SLA and not a performance conclusion**, and nothing here may
  be reported as one.

**What it does give:** "the sort's cost is unmeasured" is no longer the only available
statement. On this machine, for the largest response the contract suite produces, the
sort is single-digit milliseconds. That is an optimisation input and a sanity check —
nothing more.

## 3a. The PI's decision, 2026-08-19: the local benchmark is sufficient

**No VM24 sort-cost measurement is required.** The PI accepted the local Polars
benchmark as the engineering reference and asked instead that the **implementation** be
confirmed correct. Confirmed, each point checked against the source and recorded in §2:

| asked | confirmed |
|---|---|
| Polars **native** sort | yes — `pl.DataFrame.sort` with expression keys |
| sorted **once**, after `process_woa23_data` completes and columns are renamed | yes — the last statement before `return`, after the `time_periods → time_period` rename |
| **no Python-side row materialisation** | yes — no `to_pandas`, no `to_dicts` before serialisation, no `sorted()` over records |
| **no repeated sorting** at intermediate stages | yes — one dataframe sort in the file; `periods.sort()` orders the query's period list, not rows |
| local benchmark is a **non-blocking estimate** | recorded as such throughout |
| local results are **not** a production-overhead measurement | stated in the tool, in §3, and here |

**The point was never the number.** It was that the implementation uses the right
Polars primitive, at the right point, without duplicate sorting or unnecessary data
movement. It does.

**§4 below is therefore a design that is NOT being requested.** It is kept because a
future question about production overhead would start from it, not because anything
here asks for it.

## 4. The VM24 measurement — designed, and NOT requested

Only a paired end-to-end measurement can answer the question §3 leaves open. This is
its design; it becomes a request when the PI wants it, and **this document does not ask
for authorisation**.

### 4.1 The two trees

| arm | tree | property |
|---|---|---|
| **old** | `919095e8f3ae7af0dc8808c6015df255610f5d8f` | no deterministic sort |
| **new** | `1439194a091a5c00ac9414ddd46a3898e51dad51` | the row-order contract |

Both under the **same** query set, the **same** store and the **same** output scope, so
the sort is the only difference in the read path being compared.

**Note the asymmetry honestly:** these two trees differ by `api/query.py` alone (+54/−3,
one executable statement), so the comparison isolates the sort more cleanly than S1's
ever isolated Dask. That is a property of this change, not a general claim.

### 4.2 Cases

Chosen to span the range, not to flatter it:

| case | approximate rows | why |
|---|---|---|
| point profile | ~102 | the smallest real response; sort cost should be invisible |
| small bbox | ~2,900 | the common shape |
| regional bbox | ~32,000 | where a sort starts to be measurable |
| **large result** | **~214,812** | the largest the contract suite produces — the case that decides whether this matters |

### 4.3 What is compared

Per case, per arm: **wall time**, **total request time**, **row count**, **values** and
**column set/sequence**. The last three are not performance figures; they are there so
a timing difference produced by returning *different data* cannot be mistaken for a
timing difference produced by sorting.

### 4.4 Confounders that must be excluded, and how

- **network** — both arms on loopback, as every controlled run in this campaign has
  been;
- **startup** — measured only in steady state; no startup figure is produced and none
  may be quoted;
- **cache warm-up** — a symmetric warm-up pass before any sample, discarded, as spec
  007 §4.1 requires. The candidate reaches its first request having read the anchor
  group's metadata and the reference has not, and that asymmetry precedes the sequence;
- **arm order** — interleaved and counterbalanced per iteration;
- **hash seed** — pinned on both arms, so column order is reproducible and cannot move
  between samples.

### 4.5 What the result may and may not be called

It is a **sort-cost observation**. It is **not** a production SLA, **not** a throughput
figure, **not** a multi-worker result, and **not** a statement about production's
behaviour. **`s2pB`'s latency result may not be back-filled onto the new candidate** at
any point, before or after this measurement exists.

### 4.6 What it would need

A **new execution identity** — new label, staging, workdir and first-use ports — its own
request with full commit, archive and file-list digests, a stated request ceiling, and
its own explicit authorisation. **None of that is asked for here.**

## 5. Boundaries

**Done in this document:** an offline source audit, an offline benchmark, and a design.

**Not done, not proposed, and not authorised by it:** any VM24 action, any HTTP request,
any deployment or PM2 work, any change to `api/`, any change to the row-order contract,
any re-run of C1 or C2, any rung-60 work, spec 006 Option B, and any push.

**C1 (`c1f`) and C2 (`c2g`) stand.** Nothing here changes the candidate, so neither
needs re-running; if a future change does touch `api/`, both do.
