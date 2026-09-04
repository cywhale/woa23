# 008 — S2b: deterministic row ordering

**Status: IMPLEMENTED, VALIDATED and DOCUMENTED as API 1.1.0, revision 6. NOT deployed.**

The candidate `1439194a091a5c00ac9414ddd46a3898e51dad51` carries the sort and has
**passed both controlled gates** (§10):

- **C1 `c1f`, 5.2C** — canonical values/columns 64/64, candidate row-order contract
  44/44 applicable, required byte checks pass, no unexpected differences;
- **C2 `c2g`, 5.2B semantic** — PASS in all three cycles, seed diversity OBSERVED
  (3/3), **row-order contract PASS** (132 applicable responses, 0 violations), and the
  candidate's order **identical across three independent unpinned starts at production's
  measured worker count** while the reference varied.

**Version `1.1.0`, decided by the PI on 2026-08-19**, and the public documentation is
**applied** (§9): the OpenAPI `info.version`, its description, and both endpoint
descriptions carry the row-order statement, and `README.md` records it. It is a
**row-order contract change, not an endpoint migration** — routes, request parameters,
response schema and the Swagger URLs are unchanged, so **no consumer moves to a new
URL**.

That documentation edit is an `api/` diff, and it does **not** cost a C1/C2 re-run: it
is inside a machine-checked **docs-only allowlist** (§9.1), verified by
`bench/docs_only_diff.py`, which compares the two revisions' ASTs with docstrings and
the two allowlisted OpenAPI strings removed. `c1f` and `c2g` remain the execution
evidence and describe the data path, which this diff does not touch.

**What is still open before deployment:** **PM2 alternate-port staging validation**
(spec 010, designed and not authorised) and then a **production cutover**, separately
authorised.

**Nothing is deployed**, no production file has been changed, and no performance claim
about this candidate exists. The sort's cost is audited and benchmarked offline in spec
009; per the PI, **no VM24 sort-cost measurement is required**.

| rev | date | change |
|---|---|---|
| 6 | 2026-08-19 | **API 1.1.0: the row-order contract is published** — §9 rewritten from "proposed" to "applied", §9.1 replaced by the machine-checked docs-only allowlist, §9.3 decided. The PI set the version at **1.1.0** and framed it as a **row-order contract change, not an endpoint migration**: no route, parameter, schema or Swagger URL moves, so no consumer switches URL. The `api/` edit is exempt from a C1/C2 re-run **only because a checker says so**: `bench/docs_only_diff.py` compares ASTs with docstrings and the two allowlisted `get_openapi` strings stripped, and `bench/test_docs_only_diff.py` (27 assertions) exercises it **rejecting** every forbidden category. A failing check may not be re-classified as docs-only. The Swagger paths were **audited from source**, not assumed, and are unchanged. `bench/test_api_surface_ordering.py` now asserts the contract statement is PRESENT — the exact inversion of what it first asserted, which is what its own instruction required. `apiverse` is out of scope throughout. |
| 5 | 2026-08-19 | **Validated: C1 `c1f` and C2 `c2g` both PASS** — new §10 (the evidence) and §9 (the proposed public-documentation text and the versioning decision item). C2 produced what C1 could not: under unpinned seeds the **reference** varied on `C16`/`C16-csv` while the **candidate** held one order across three independent starts, which is the contract doing what it was decided to do. The sort's cost is audited and benchmarked offline in **spec 009** and remains **unmeasured end to end** — informative, not a gate, and never a reason to change the contract. §9 carries the exact wording proposed for the OpenAPI description, both endpoints, `README.md` and the CSV documentation, and records that **applying it changes `api/` and would require a new C1/C2** — so it is proposed, not applied. |
| 4 | 2026-08-19 | **5.2C is now DEFINED here, not deferred to the implementation** — new §7c, and §2a on the public-documentation gap. Rev 2 §7a.5 said the choice between "a third variant (5.2C)" and "a flag on 5.2A" was *"an implementation detail … settled in the implementation commit, not here"*. It was settled — as **5.2C** — and a protocol a C1 request is written against may not live only in code. §7c specifies the variant end to end: how it differs from 5.2A, the canonical value and column-**sequence** comparison, the candidate-only order gate, how an expected raw-order difference is recorded, the JSON and CSV reconstruction rules including their self-check, `INCOMPLETE_VALIDATION`, the C2 division of labour, the `N/A` classification for responses with no row order, and the forbidden report wording. Also records that **the runner now uses 5.2C for the C1 *and* s2perf modes**, which rev 3 did not anticipate. §2a states plainly that the candidate implements a contract the **published documentation does not yet state**. |
| 3 | 2026-08-19 | **C2 must verify the candidate's row-order contract too, and the C1 result may no longer be reported as "5.2A byte-exact PASS"** — new §7b, §7a.4 amended. PI review: C1 alone proves the contract only under a **pinned seed and one worker**. The new order is an API contract, so it has to hold in C2's environment as well — **unpinned seed, production's worker count, three independent cycles**. §7b specifies a **candidate-only** order gate that reuses the responses each cycle already fetches (**no extra HTTP, no budget change**), leaves 5.2B's semantic verdict untouched, and classifies a failure as `ROW_ORDER_CONTRACT_FAILURE` rather than semantic divergence. It also splits `c2_summary.order_stability` by arm: reference variation across cycles stays an observation, **candidate variation becomes a contract failure**. And the C1 report is renamed: a run that carries an intentional ordering change may not be written as "5.2A 64/64 raw byte-exact PASS", and a run whose byte reconstruction did not complete may not be called PASS at all. |
| 2 | 2026-08-19 | **The C1 byte-exact gate conflicts with this change, and the conflict is now specified** — new §7a, and §4.4 narrowed. PI review: rev 1 said only "C1/C2 must be re-run" and never said **how** the new C1 handles a difference the contract now mandates. Under 5.2A the verdict is `ref["body"] == cand["body"]`, so a deliberate row-order change reports `DIFFER` on every multi-row 200 case — a **contract change judged as a regression**. §7a specifies a canonical-order comparison, a separate row-order contract gate on the candidate alone, and an acceptance rule that says which byte differences are expected and which are still regressions. Also, the duplicate-key claim is narrowed: it is what **polars 1.27.1 on the current pivot path** does, protected by a test — **not** a structural guarantee across future polars versions. |
| 1 | 2026-08-19 | First draft, from the PI's spec 003 Q3/Q4 decision of 2026-08-19 and the source audit below. |

---

## 1. The decision, as given

The PI decided spec 003's open questions on 2026-08-19. **This is an API contract
change, not a refactor**, and it is written here in the terms it was decided in.

**Q3 — adopt an explicit deterministic row order.** Not "leave it unspecified", not
"pin `PYTHONHASHSEED`", and not "rely on Zarr group iteration order".

**Q4 — the contract binds JSON and CSV alike.**

**The order:**

> Response records / data rows are ordered ascending by
> `(time_period, depth, lat, lon)`.

with these conditions, each of which is a requirement and not a gloss:

- `time_period` sorts **numerically, never as a string** — `0, 1, 2, …, 13, …`;
- at fixed `time_period` and `depth`, **longitude increases first**;
- when longitude reaches the end of its range, **latitude advances**;
- **at most one row per `(time_period, depth, lat, lon)`**;
- if an implementation ever finds a duplicate key, it **must fail explicitly**. It
  may **not** quietly depend on an unstable first-wins order. **§4.4** records what
  the current code does about this on the polars version production runs, and how far
  that observation may be taken.

**The purpose, in the PI's words:** a consumer can take a fixed `time_period` at a
fixed `depth` and get a bbox block, laid out stably by latitude and longitude inside
that block.

### 1.1 Where the four questions now stand

| spec 003 §6 | status |
|---|---|
| **Q1** — does the published surface state or imply an order? | **ANSWERED: no, in any form.** Audited offline and pinned by `bench/test_api_surface_ordering.py` (15 assertions) over the OpenAPI title and description, both endpoint summaries, all `Query(...)` descriptions, `README.md`, and the absence of any `response_model`, `responses=` schema or example payload. One human step remains outside this repository: the hosted Swagger hub page is served by another system and no test here can read it. |
| **Q2** — is any known consumer reading rows positionally? | **UNKNOWN, and it stays written down as unknown.** See §2. |
| **Q3** — which option? | **DECIDED: specify the order** (§1). |
| **Q4** — JSON and CSV together? | **DECIDED: yes, both** (§1). |

## 2. Consumer risk — unknown, and not assumed away

**Nobody has established that no consumer depends on today's order.** Q2 is not
answerable from this repository: there is no telemetry, no client inventory and no
access-log analysis here, and none is proposed by this spec.

Three things follow, and the third is the one that is easy to get wrong.

1. **"No documented order" and "no consumer depends on order" are different claims.**
   Q1 established the first. It says nothing about the second.
2. **Today's order is not a contract**, and this change does not treat it as one. It
   is per-process and hash-seed dependent; C2 observed it varying across cycles on
   two multi-group cases. A consumer relying on it is relying on something that
   already changes at every restart — but that does not mean no consumer does.
3. **The change is still user-visible.** A client that reads rows positionally will
   see different rows at the same index after deployment. That is a real consequence
   of a decided contract change, recorded here so the deployment decision is made
   with it in view rather than after it.

**This spec does not decide the versioning or announcement question.** The API is
published with a DOI and the roadmap's standing constraint says a contract change
needs an explicit versioning decision. That decision is the PI's and is **not** made
here.

## 2a. The candidate implements a contract the public documentation does not state

**Both halves of this are true right now, and neither may be quoted without the other.**

- **The candidate implements the future contract.** `api/query.py` sorts by
  `(time_period, depth, lat, lon)` as of `693f00f`. Any deployment of that tree would
  put the behaviour in front of users.
- **The published documentation says nothing about row order.** The OpenAPI title and
  description, both endpoint summaries, every `Query(...)` description and `README.md`
  are unchanged, and `bench/test_api_surface_ordering.py` still asserts that they state
  no ordering, in any form.

**That gap is permitted for now and must not be mistaken for a decision.** It is
acceptable *because nothing is deployed*: an unshipped behaviour needs no published
promise. It stops being acceptable at deployment.

**What the current documentation state is NOT.** The audit answering spec 003 §6 Q1 —
*"the published surface states no order"* — is a finding about **today's documentation**,
gathered to decide whether the row-order question was a product decision or a
conformance fix. It is **not** the final public contract, it is **not** a statement that
row order will remain undocumented, and it may **not** be cited as though the API had
deliberately promised "no order". Writing it that way would convert an observation into
a commitment nobody made.

**Versioning and announcement are preconditions of deployment, not follow-ups.** The
roadmap's standing constraint is explicit that a change to returned values, column
names, ordering, null handling or the JSON/CSV contract on a DOI-published API needs an
explicit versioning decision. That decision is the PI's, it is **not made in this
document**, and until it is made:

- the candidate may not be deployed;
- the published surface stays as it is — updating it before the decision would announce
  a contract that has not been versioned;
- when it is made, `bench/test_api_surface_ordering.py` changes **with** it, and its own
  comments say so, so the audit cannot silently drift out of agreement with the docs.

## 3. Scope

**In scope — and this is the whole of it:**

- the order of objects in the JSON array;
- the order of data rows in the CSV body.

**Out of scope, explicitly:**

- **JSON field order and CSV header order.** Not changed by this work. The `list(set(...))`
  over `variables` still drives column order, and it stays exactly as it is.
- **Spec 006 option B** (empty result as 200 with a header-only CSV). Not implemented.
- **Values, column names, HTTP statuses, empty-result behaviour, and every other
  query semantic.** Unchanged.
- **Request parameter meaning.** Unchanged.
- Anything that would make today's observed order into a contract retroactively.

## 4. Source audit

Read from the candidate at `e6683c5`: `api/query.py`, `api/app.py`, `api/config.py`.

### 4.1 Both endpoints already share one serialisation input

| endpoint | line | what it serialises |
|---|---|---|
| `GET /api/woa23` | `api/app.py:150-151` | `df = await process_woa23_data(...)` then `df.to_dicts()` |
| `GET /api/woa23/csv` | `api/app.py:183-188` | the same call, then `df.write_csv(temp_file.name)` |

Neither endpoint reorders, filters or re-frames anything between the call and the
serialiser. **So a single sort at the end of `process_woa23_data` governs both**, and
no rule has to be written twice. This is the shape §1's Q4 asks for, and it is
already there.

### 4.2 What determines row order today

Nothing in the pipeline sorts rows. The order that reaches the serialiser is the
accumulated consequence of four unordered or incidental things:

| source | file | why it is not stable |
|---|---|---|
| `zarr_group_paths` | `api/query.py:135-140` | a **`set` of path strings**; iteration order depends on the process's hash seed. This is the mechanism spec 003 identified. |
| `variables` | `api/query.py:106` | `list(set(...))` — the loop at `api/query.py:203` appends per variable in that order |
| `pars` | `api/query.py:117` | `list(set(...))`, feeds the selection |
| `to_dataframe()` → `pl.concat` → `pivot` | `api/query.py:205-231` | the pivot's index rows come out in first-appearance order, so they inherit whatever the three above produced |

`periods.sort()` at `api/query.py:130` is **not** a row-order sort: it sorts the
*query's* period list, as strings, and is used for selection only. It is also a live
illustration of §1's numeric-sort requirement — `['0','1','10','13','2']` is what
that line produces for `time_period=0,1,2,10,13`.

### 4.3 `time_period` is a STRING column, and this is the trap

`api/config.py:78-96` keys `time_periods` as the strings `'0'` … `'16'`. `api/query.py:125`
builds `periods` from those keys, and `api/query.py:189` intersects them with
`ds.coords['time_periods'].values` — an intersection that is only ever non-empty
because the Zarr coordinate carries the same string values. The column survives the
pivot as the index column `time_periods` and is renamed to `time_period` at
`api/query.py:245`.

**Verified on the production polars version, 1.27.1:**

```
string sort : ['0', '1', '10', '13', '2']      <- wrong, and silently so
numeric sort: ['0', '1', '2', '10', '13']      <- the contract
```

A plain `sort("time_period")` would satisfy a careless reading of §1 and violate it.

### 4.4 Duplicate keys — what is observed, and how far the claim goes

`api/query.py:232-236` pivots with `index=["lon", "lat", "depth", "time_periods"]`.
**On polars 1.27.1 — the version production runs — this pivot emits one row per
distinct index tuple, and a duplicated `(index, on)` pair does not silently
first-wins.** Checked directly:

```
ComputeError: found multiple elements in the same group, please specify an
aggregation function
```

**So on the pivot path as it exists today, the code already fails explicitly**, which
is what §1 requires of an implementation that meets a duplicate.

**The claim goes exactly that far and no further.** This is **observed behaviour of
polars 1.27.1 on the current pivot path**, verified and then pinned by a test — it is
**not** a structural guarantee, and **nothing here claims it holds for future polars
versions**. A later version could add an implicit aggregation, change the error into a
warning, or reorder groups; that is precisely why the property is a **test** and not
an assumption. §6 test 9 asserts no duplicate key reaches a response, and the
duplicate-raises behaviour is pinned alongside it, so a polars upgrade that weakens it
**fails the suite** instead of silently changing what a response means.

**No new runtime check is proposed**, and the reason should be read as a scope
statement rather than confidence: adding one would create a new failure path in a
request handler whose behaviour on this point is already correct, and §3 puts new API
behaviour out of scope. If a future polars version does weaken the guarantee, the test
fires and **the decision is reopened then**, with evidence, rather than pre-empted now.

### 4.5 What a sort cannot change

`sort` reorders rows. It does not touch values, column names, column order, dtypes or
row count. Confirmed on 1.27.1: `out.columns == d.columns` and the row multiset
unchanged. §6 tests 8 and 9 assert this rather than relying on it.

## 5. Design

### 5.1 The sort site — one line's worth, at the one place both endpoints share

**`api/query.py`, at the end of `process_woa23_data`: after the
`rename({"time_periods": "time_period"})` at line 245, before `return result_df`.**

That point is chosen because it is **after** the column has its contract name, and
**after** every source of unordered input has already done its work — so the sort is
the last word on row order and nothing downstream can reintroduce the instability.

### 5.2 The expression

```python
result_df = result_df.sort(
    [pl.col("time_period").cast(pl.Int32), "depth", "lat", "lon"],
    nulls_last=True,
)
```

Four deliberate choices:

- **`cast(pl.Int32)` inside the sort key** — §4.3's requirement. `time_period` stays a
  string in the output; only the sort key is numeric, so no value and no dtype changes.
- **An expression, not a temporary column.** The obvious alternative — add
  `_time_period_order`, sort, drop it — introduces a name that could collide with a
  data column (`{param}_{var}`), and a colliding `with_columns` alias would *overwrite*
  real data. The expression form has no such failure mode.
- **Strict cast.** Polars 1.27.1 raises `InvalidOperationError` on an uncastable value
  rather than producing a null. Today every value is `'0'`–`'16'` so it cannot fire;
  if the coordinate ever carried something else, failing is correct and silently
  sorting nulls to one end is not.
- **`nulls_last=True`** — the index columns come from Zarr coordinates and are not
  null, so this decides a case that does not currently arise. It is specified anyway:
  an unspecified case is how an order stops being deterministic later.

**Key order `(time_period, depth, lat, lon)` produces exactly the layout §1 asks for**:
at fixed `time_period` and `depth`, `lat` is the outer key and `lon` the inner, so
longitude runs to the end of its range before latitude advances.

### 5.3 What is NOT changed

`list(set(...))` at `api/query.py:106`, `:117` and `:125`, and the `zarr_group_paths`
set at `:135` — all stay. They still make **column** order hash-dependent, which is
§3's out-of-scope. Removing them would be a second change in the same commit and a
second contract question in the same breath.

## 6. Offline tests to be added

They must exercise **the real query path** — `process_woa23_data` and both endpoint
handlers through the serialisers — not an isolated sort helper, or the endpoint wiring
could regress unnoticed. A fixture Zarr store built in a temp directory keeps this
offline: no VM24, no production store, no HTTP over a socket.

| # | assertion |
|---|---|
| 1 | JSON response rows are strictly ascending by `(time_period, depth, lat, lon)`, numerically |
| 2 | CSV data rows carry the same key sequence |
| 3 | JSON and CSV row order are identical to each other |
| 4 | changing only the `parameter` order in the query changes no row order |
| 5 | changing only the `time_period` input order changes no row order |
| 6 | a multi-group query's row order is unchanged across hash seeds and group-set iteration orders |
| 7 | a query spanning `0, 1, 2, 13` orders them `0, 1, 2, 13` — the string-sort trap, tested directly |
| 8 | for a non-empty result, the values and the column set are unchanged by sorting |
| 9 | no duplicate `(time_period, depth, lat, lon)` key appears in any response — **and**, pinned beside it, that a duplicated `(index, on)` pair still makes the pivot **raise** rather than silently pick a winner, so a polars upgrade that weakens §4.4's observed behaviour fails the suite |
| 10 | **negative control**: an unsorted and a wrongly-sorted fixture must FAIL the checker |

Test 6 needs the seed varied in a **subprocess**, since `PYTHONHASHSEED` is read at
interpreter start.

## 7. Consequences for existing evidence

**This changes `api/`. It is a candidate source change, and it invalidates the
evidence that describes the tree without it.**

- **C1 and C2 must be re-run under a NEW execution identity.** `c1e` and `c2f`
  describe `919095e8` and **may not be back-filled** as evidence for the new candidate.
  **C1 cannot simply be re-run as it stands** — its 5.2A gate compares raw bytes and
  would report the decided change as `DIFFER`. **§7a** specifies what the new C1 must
  do instead. C2 needs no gate change, only a re-run.
- **`s2pB`'s latency result does not apply to the new candidate.** It measured
  `c3b9398b`, whose `api/` is byte-identical to `919095e8`. The rung-21 PASS stands as
  a result **about that tree** and may not be quoted for a tree containing this sort.
  Whether a re-measurement is wanted is a separate decision and is **not** requested
  here; **rung 60 remains unauthorised and unscheduled**.
- **The sort's own cost is unmeasured, and no existing number covers it.** A sort over
  the result frame is not free, and its cost grows with row count — the largest
  contract case returns 64,800 rows. **`s2pB`'s figures may not be reused to claim it
  is negligible**: they measured a tree without it. Establishing the cost needs its own
  authorised measurement, and **no performance claim about the new candidate may be
  made until then**.
- **`d1b` stays as it is.** It is historical observation and is not rewritten. If the
  new row order changes D1 response bytes, whether that needs a fresh D1
  characterization is **a separate judgement**, to be made then.
- **No deployment and no performance claim** before the new candidate's C1/C2 pass.

## 7a. The C1 byte-exact gate conflicts with this change

**This is the gap rev 1 left.** Saying "C1 must be re-run" is not enough, because
**re-running the existing gate would fail it** — and fail it for the change working as
decided.

### 7a.1 The conflict, exactly

`bench/contract_diff.py:304-305` decides variant 5.2A with

```python
same = ref["status"] == cand["status"] and ref["body"] == cand["body"]
```

**Raw bytes.** After this change the candidate emits rows in contract order while the
reference — production's unmodified `woa23_app.py` — emits them in whatever order its
`zarr_group_paths` set and pivot produced. Same rows, same values, same columns,
different sequence, **different bytes**. Every multi-row 200 case would report
`DIFFER`, and a gate whose whole purpose is to catch unintended change would be
reporting the one change that was intended.

**A `DIFFER` under the current 5.2A is therefore not evidence of a defect after this
change, and must not be read as one.** That is the sentence the new C1 has to make
unnecessary, by not producing a misleading verdict in the first place.

### 7a.2 C2's SEMANTIC verdict is not affected — but C2 is not finished with (see §7b)

C2 runs variant 5.2B, whose comparison is `compare_semantic`
(`bench/contract_diff.py:104-137`): it pairs rows by sorting **both** sides on the
index key before comparing values, and compares column **sets**, not sequences. Row
order is already outside its verdict by construction — deliberately, because under an
unpinned seed a row-order difference is a property of the process.

**So 5.2B's semantic verdict needs no change**, and this spec does not change it.

**That is not the same as "C2 is unaffected", and rev 2 was wrong to imply it.** C1
establishes the new contract only under a **pinned seed and a single worker**. The
contract is an API contract, so it must also hold where C2 looks — **unpinned seed,
production's worker count, three independent starts**. **§7b** specifies that check.
C2 must in any case be **re-run** under a new execution identity, because it is
evidence about a tree and the tree changes.

### 7a.3 What the new C1 must do — three separate things

The harness already separates order from verdict: `order_fingerprint`
(`bench/contract_diff.py:68-100`) records `row_order_sha256` — the ordered sequence of
row keys, values excluded — for **every** variant and is **used as a verdict by none**.
That is the hook this design builds on; it is not new machinery.

**1. Canonical-order value comparison — the correctness verdict.**
Normalise **both** arms to `(time_period, depth, lat, lon)` before comparing, then
compare values exactly and columns as a sequence. This is what establishes that the
change moved rows and nothing else.

**2. A row-order contract gate on the candidate alone — the new-contract verdict.**
Verify that the candidate's **raw** response already satisfies §1's order, per case,
for JSON and CSV alike. This is not a comparison with the reference: the reference does
not implement the contract and cannot be the standard for it. **Without this check the
canonical comparison would pass even if the candidate emitted rows in a random order**,
because canonicalisation would hide it — so the two checks are not redundant and
neither substitutes for the other.

**3. Raw row-order difference recorded as an EXPECTED CONTRACT CHANGE.**
Where the raw orders differ, that is logged as the decided change — with both
`row_order_sha256` values, so the difference is evidenced rather than asserted — and
**not** counted as a regression.

### 7a.4 The acceptance rule, and where a regression still lives

A blanket "byte differences are expected now" would discard exactly the sensitivity C1
exists for. The rule is narrower, and each clause is falsifiable:

| case shape | expected under the new C1 |
|---|---|
| non-200 (400 / 404) bodies | **byte-identical**, as today. Row order cannot apply, so a byte difference here is a **regression** |
| non-row payloads (OpenAPI document, Swagger HTML) | **byte-identical**, as today. A difference is a **regression** |
| 200 with a single data row | **byte-identical**. One row has only one order, so a difference is a **regression** |
| 200 with multiple rows, reference already in contract order | **byte-identical**. A difference is a **regression** |
| 200 with multiple rows, reference not in contract order | **bytes differ, and this is the expected contract change** — provided the two checks below both hold |

For every case in that last row, both must hold:

- the **canonical-order comparison MATCHES** — same row count, same column sequence,
  every value equal; and
- **reconstruction**: permuting the *reference's own data rows* into contract order
  reproduces the **candidate's bytes exactly**.

Reconstruction is what makes "only the order changed" a demonstrated claim instead of
an inference. For CSV it is a permutation of data lines under an unchanged header. For
JSON it is a permutation of the top-level array's elements, spliced at element
boundaries so each element's serialised bytes are carried across untouched rather than
re-serialised — re-serialising would prove the harness's formatting, not the API's.

**If byte-level reconstruction proves infeasible for the JSON payload**, the fallback
is checks 1 and 2 alone. That outcome is **not a PASS**. It must be recorded in the
run's own output as **`INCOMPLETE_VALIDATION`** — weaker evidence, named as such — and
**it may not be carried into any deployment claim**. A fallback that is not written
down is a silent downgrade; a fallback written down and then quoted as a pass is worse.

### 7a.4a How the C1 result must be reported

**A run carrying an intentional ordering change may not be reported as
"5.2A 64/64 raw byte-exact PASS".** That sentence would be false in its plain reading
— the raw bytes are *not* all equal, by design — and it would describe the decided
contract change as if it had not happened.

The result is reported as three findings that a reader can tell apart, in this shape:

> **C1 contract validation — canonical values/columns match, candidate row-order
> contract PASS, expected raw-order differences recorded.**

with the counts attached to each part: how many cases matched canonically, how many
satisfied the candidate order gate, how many raw-order differences were expected and
reconstructed, and how many byte-identical cases stayed byte-identical (§7a.4's first
four rows, where a difference is still a regression).

**PASS requires all three.** If any one is not established the run is not a PASS, and
if reconstruction did not complete it is `INCOMPLETE_VALIDATION` per the paragraph
above. The `variant` field in the artefact must name what actually ran, so no reader
can mistake it for the historical 5.2A byte-exact gate.

### 7a.5 What this requires, and what it does not

**It requires a harness change**: `bench/contract_diff.py` gains the canonical
comparison, the candidate-side order gate and the expected-difference accounting, with
its own offline tests including fixtures where the candidate order is **wrong** — a
gate that cannot fail proves nothing.

**This shipped as a third variant, `5.2C`, and §7c is its definition.** Rev 2 called the
choice "an implementation detail … settled in the implementation commit, not here" —
which was wrong in one respect worth naming: a **protocol a C1 request is written
against may not live only in code**. The decision was an implementation detail; the
resulting protocol is not.

**One trap, recorded because it is invisible in review.** `_sort_key`
(`bench/contract_diff.py:64-65`) is `tuple(str(row.get(k)) for k in INDEX)` — a
**string** tuple, over `INDEX = ("lon", "lat", "depth", "time_period")`. As a *pairing*
key for `compare_semantic` that is sound: both sides are sorted the same way and the
keys are unique, so it is a bijection and the comparison is order-insensitive either
way. **It must not be reused to verify §1's contract order.** String comparison puts
`'13'` before `'2'` and `'-10'` before `'-9'`, and `INDEX` is not even in the contract's
key order. The contract check needs its own **numeric** comparator over
`(time_period, depth, lat, lon)`.

**It does not change 5.2B, C2, or any existing recorded result.** `c1e`, `c2f`,
`s2pB` and `d1b` describe the trees they ran against and are not reinterpreted by a
harness that did not exist when they ran.

### 7a.6 Order of work

The C1 harness change is **offline** and comes **before** any C1 re-run is requested —
re-running the old gate and then explaining away 60-odd `DIFFER` verdicts is not a
result, it is a story about one. Sequence: implement the candidate sort → implement the
new C1 comparison and its tests → run the full offline suite → **then** request
authorisation for the C1 and C2 re-runs, under new execution identities.

## 7b. C2 must verify the candidate's row-order contract as well

**C1 proves the contract under a pinned seed and one worker. That is not the
environment the API runs in.** Production runs with `PYTHONHASHSEED` unset and more
than one worker, and C2 is the mode that looks there. A row order that holds only
under C1's arrangement is not an API contract; it is a property of C1's arrangement.

### 7b.1 What C2 must additionally check

**1. Per-response conformance.** Every **candidate** response in every cycle satisfies
`(time_period, depth, lat, lon)` ascending, numerically, for JSON and CSV alike.

**2. Stability across cycles.** The candidate's row order for a given case is
**identical in all three cycles**, i.e. one distinct `row_order_sha256` per (case,
candidate) across the three independent starts, each with its own seed.

Check 2 is the one C2 exists to make possible: three independent processes with three
different seeds are exactly the circumstance under which the old order varied, so
three-way agreement is what shows the sort — not the seed — is deciding the order.

### 7b.2 It costs no extra requests

Both checks are computed **from the responses each cycle already fetches**. Check 1 is
evaluated when the body is in hand, inside `contract_diff.py`, and recorded per case.
Check 2 is an aggregation over records that already exist.

**No new HTTP request, no change to any request budget or ceiling, and no change to
what C2 asks of either arm.** A validation that needed more traffic than the run was
authorised for would need a new authorisation; this one does not.

### 7b.3 It does not touch the 5.2B semantic verdict

The row-order gate is **candidate-only** and sits **beside** the semantic comparison,
never inside it:

- the **reference** is not held to the contract. It does not implement it, and under
  an unpinned seed its order is genuinely a property of its process. Reference
  variation across cycles stays exactly what it is today — **recorded, not gated**;
- `compare_semantic` is unchanged, so a row-order difference between the arms still
  does not make 5.2B fail;
- the candidate order gate produces its **own** verdict, alongside.

`bench/c2_summary.py:306` (`order_stability`) already compares `row_order_sha256` per
(case, arm) across cycles and is explicitly *"Recorded, not gated"*. **That note
remains true for the reference and becomes false for the candidate**, so the function
splits by arm: reference variation is reported as before; **candidate variation is a
failure**. Its docstring must say so, because a function whose comment says "not a
verdict" while it produces one is how a gate gets ignored.

### 7b.4 A failure here is named for what it is

A candidate response that violates the order, or a candidate order that differs
between cycles, is reported as **`ROW_ORDER_CONTRACT_FAILURE`** — **not** as semantic
divergence, and **not** folded into the 5.2B verdict.

The distinction is not cosmetic. "The arms disagree semantically" and "the candidate
does not implement the contract we decided" have different causes, different fixes and
different consequences, and a report that blurs them sends the reader to the wrong
question. `ROW_ORDER_CONTRACT_FAILURE` also blocks a deployment claim on its own,
without waiting for the semantic gate to have an opinion.

### 7b.5 What the tests must cover

Offline, with fixtures, in the same commit as the harness change:

- a candidate response in contract order → gate PASS;
- a candidate response one row out of order → gate FAIL, and the failure names the
  case and the first offending pair;
- a candidate response ordered by **string** `time_period` (`0, 1, 10, 13, 2`) → gate
  FAIL. This is the exact defect §4.3 is about, and a gate that misses it is worthless;
- three cycles with identical candidate order → stable;
- three cycles where the candidate's order differs in one → `ROW_ORDER_CONTRACT_FAILURE`;
- three cycles where the **reference** order differs → still an observation, **not** a
  failure, and the 5.2B verdict unchanged;
- an order gate failure does **not** silently become a semantic failure, and a
  semantic failure does not mask an order failure — both are reported.

## 7c. Variant 5.2C, as implemented

Rev 2 left the name to the implementation commit. It was settled as **5.2C**, and this
section is the protocol — a C1 request cites this, not the source.

### 7c.1 How 5.2C differs from 5.2A

| | 5.2A | 5.2C |
|---|---|---|
| arms | both ours, both pinned | **the same** |
| seed policy | `both-pinned` | **the same** |
| group-path agreement precondition | required | **the same** |
| the verdict | `ref["body"] == cand["body"]`, raw bytes | **three findings, all three required** |
| a row-order difference | `DIFFER` | **the decided contract change, recorded** |
| what a byte difference means | always a defect | **defect except where §7c.5 permits it** |

**5.2A is not deleted and not deprecated.** It remains correct for a candidate that
does not reorder rows, and the historical `c1e` result stands as what it was. 5.2C
exists because *this* candidate reorders rows by contract, and a byte gate would report
the intended change as a defect.

### 7c.2 Finding 1 — canonical values and column sequence

Both arms are normalised to `(time_period, depth, lat, lon)` — **numerically** — and
then compared: row count, then **column sequence**, then every value exactly.

**Column SEQUENCE, not the column set.** 5.2B compares column sets because under an
unpinned seed the order is a property of the process. Here both arms are pinned, column
order is **out of this spec's scope** (§3), and therefore it must not have moved.
Comparing sets would let a column-order regression through the one gate positioned to
catch it.

A non-200 pair is compared as error bodies, as before. A payload with no row structure
— the OpenAPI document, the Swagger page — is compared as **raw bytes**, because there
is nothing to canonicalise and the honest comparison is the strict one.

### 7c.3 Finding 2 — the candidate-only row-order contract gate

The candidate's **raw** response must already satisfy §1's order, per case, JSON and CSV
alike, with **at most one row per key**.

**Candidate only.** The reference does not implement the contract; holding it to one it
never adopted would manufacture failures.

**Not redundant with finding 1.** Canonicalisation sorts both sides before comparing, so
it would pass a candidate emitting rows in a *random* order. Finding 2 is the only check
that looks at the sequence the candidate actually sent.

**`applies` separates "conforms" from "there was nothing to conform to."** A non-200
body, a non-row payload and a zero-row result are recorded **`N/A`** — `applies: false`,
`ok: null` — and counted apart. They are neither passes nor failures: scoring them as
passes would let a run where every case errored report a perfect order gate, and scoring
them as failures would invent a defect out of an empty result.

### 7c.4 Finding 3 — the expected raw-order difference, recorded

Where the raw bytes differ and §7c.5 permits it, the case is recorded as
**`EXPECTED_ROW_ORDER_CHANGE`**, carrying both arms' `row_order_sha256` so the
difference is evidenced rather than asserted, and it is **not** counted as a regression.

### 7c.5 Which byte differences are permitted, and which remain regressions

A blanket "bytes may differ now" would discard the sensitivity C1 exists for. The
permission is narrow and every clause is falsifiable:

| case shape | required | if it differs |
|---|---|---|
| non-200 (400 / 404) body | byte-identical | **REGRESSION** — no row order to change |
| non-row payload (OpenAPI document, Swagger HTML) | byte-identical | **REGRESSION** |
| 200 with **one** data row | byte-identical | **REGRESSION** — one row has one order |
| 200, multi-row, reference **already** in contract order | byte-identical | **REGRESSION** — the sort cannot explain it |
| 200, multi-row, reference **not** in contract order | may differ | **EXPECTED**, only if §7c.6 reconstructs |

### 7c.6 Reconstruction — the proof that only the order changed

For a permitted difference, the **reference's own data rows** are permuted into contract
order and the result must equal the **candidate's bytes exactly**. Every byte of every
row is then the reference's; only the sequence is the candidate's. That is what makes
"only the order changed" a demonstration instead of an inference.

**CSV.** The header is held fixed and the data lines permuted. The line terminator is
detected and preserved (`\r\n` or `\n`), and a trailing terminator is preserved.
Refused — not guessed — when the parsed row count does not equal the data-line count,
which is what an embedded newline or an unmodelled quoting rule would produce.

**JSON.** The top-level array is split at **element boundaries** and the elements'
**raw byte slices** are carried across untouched. They are never re-serialised:
re-serialising would prove the harness's formatting rather than the API's.

**The self-check, and it is the important part.** Before any conclusion is drawn, the
pieces are reassembled in the reference's **original** order and compared with the
reference's original body. If that does not reproduce it byte for byte, the splitting is
not faithful, no conclusion may be drawn from a rearranged version of it, and
reconstruction reports **unavailable**.

### 7c.7 `INCOMPLETE_VALIDATION` is not a pass

A case whose permitted difference could **not** be reconstructed is `UNRECONSTRUCTED`.
It is neither a pass nor a regression: the evidence for "only the order changed" could
not be produced.

A run containing any such case is **`INCOMPLETE_VALIDATION`**. It **exits non-zero**, it
**may not be called PASS**, and it **may not be carried into any deployment claim**.
There is no flag that downgrades it quietly.

### 7c.8 The gate outcome

**PASS requires all three findings**: every case matches canonically, every applicable
case satisfies the candidate order gate, and every permitted byte difference was
reconstructed. Otherwise:

| outcome | when |
|---|---|
| `FAIL` | any regression under §7c.5, or any `ROW_ORDER_CONTRACT_FAILURE` |
| `INCOMPLETE_VALIDATION` | no regression, but some permitted difference was not reconstructed |
| `PASS` | all three findings established |

### 7c.9 How the result is reported

Per §7a.4a, and repeated here because it is the sentence most likely to be written
wrongly:

> **C1 contract validation — canonical values/columns match, candidate row-order
> contract PASS, expected raw-order differences recorded.**

with counts on each part. **"5.2A 64/64 raw byte-exact PASS" is forbidden** — false in
its plain reading once the bytes deliberately differ. The artefact's `variant` field
records `5.2C`, so no reader can mistake it for the historical gate, and the artefact
carries its own `reporting_rule` saying so.

### 7c.10 C2 keeps 5.2B, and gains the same gate

**The 5.2B semantic verdict is unchanged.** `compare_semantic` pairs rows by sorting
both sides, so row order is outside its verdict by construction, and this spec does not
touch it.

**Beside it, the candidate-only gate**, from the responses each cycle already fetches —
**no extra HTTP, no budget change**:

1. **conformance** — every candidate response with data rows satisfies the numeric
   order; error, empty and non-row responses are **`N/A`**, never failures;
2. **stability** — one distinct candidate `row_order_sha256` per case across all three
   cycles. Three independent starts with three seeds are exactly the circumstance under
   which the old order varied, so three-way agreement is what shows the **sort** and not
   the **seed** decides the order.

**The reference is not held to the contract**, and its variation across cycles stays
recorded-not-gated.

**A failure is `ROW_ORDER_CONTRACT_FAILURE`**, with its own outcome name and its own
exit code — never folded into the semantic verdict, and it blocks a deployment claim on
its own. A cycle carrying **no** conformance record is `INDETERMINATE`, not a pass: "we
did not check" and "we checked and it held" must not look alike.

### 7c.11 Where the runner uses it

`run_controlled.sh` sets `VARIANT=5.2C` for the modes whose arms are **both ours** —
`--c1` and `--s2-perf`. Rev 3 did not anticipate the second: the s2perf contract gate
reads the same variable, so leaving it at 5.2A would have failed any future performance
run's contract gate for the same reason C1's would have failed. **C2 keeps 5.2B.**
`paired_bench` accepts `5.2C` for the `--gate-variant` it records, so a latency artefact
names the contract variant that actually ran.

## 8. Authorisation boundaries

**Offline only, and that is the whole of what is proposed here:** write this spec,
implement the change in `api/`, **implement the new C1 comparison of §7a in
`bench/contract_diff.py` with its own tests**, add the row-order tests of §6, and run
the offline suite, compile checks, shell syntax checks and the clean-archive
verification. The candidate change and the harness change are **separate commits**.

**Not proposed, not authorised, and not to be started as a consequence of this spec:**
starting anything on VM24, sending any HTTP request, touching production, deploying,
running any performance measurement, implementing spec 006 option B, or pushing.

**Needs its own authorisation, later, each separately:** the C1 re-run — **after**
§7a's harness change exists, not before — the C2 re-run — likewise after §7b's
candidate order gate exists — **a measurement of the sort's own performance cost**,
any deployment, any re-measurement of latency on the new candidate, and the versioning
or announcement decision §2 leaves open — which is a **precondition of deployment**,
not a follow-up to it.

**Nothing may be claimed about the new candidate's performance from `s2pB`.** The sort
is new work on the read path and its cost is unmeasured (§7).

## 9. The public documentation — APPLIED as version 1.1.0

**The PI decided this on 2026-08-19: API version `1.1.0`, a row-order contract change,
not an endpoint migration.** The text below is applied in the candidate's OpenAPI
document and both endpoint descriptions.

**What did NOT change, and this is the half that makes it a contract change rather than
a migration:** every route and path, every request parameter, the response schema, the
Swagger and OpenAPI URLs, and the handler, query, serialisation and runtime logic.
Existing consumers call the same URLs with the same parameters and get the same fields.

### 9.1 It is an `api/` edit, and why it does NOT cost a C1/C2 re-run

The OpenAPI description and both endpoint docstrings live in `api/app.py`, so this is a
diff under `api/`. The campaign's rule is that an `api/` change invalidates the evidence
describing the tree without it — **and that rule is about behaviour, not about bytes**.

So the exemption is **not** taken on the strength of anyone's judgement that "it's only
docs". It is **decided by machine**: `bench/docs_only_diff.py` parses both revisions to
an AST, removes every docstring and the two allowlisted `get_openapi` keyword strings,
and requires the remaining trees to be **identical**. Everything that executes is still
in the tree and still compared. `bench/test_docs_only_diff.py` (27 assertions) exercises
the checker **rejecting** each forbidden category — route, path, request parameter and
its `description`, parameter default, handler logic, serialisation, runtime
configuration, OpenAPI `title`, `routes=`, and an added import — and proves the
normaliser is not vacuous.

**What the allowlist permits, and nothing else:**

1. the OpenAPI `info.version`;
2. the OpenAPI `description`;
3. endpoint operation docstrings (and any docstring — a docstring does not execute);
4. comments and blank lines;
5. tests and documents that describe the above.

**Every other `api/` file must be byte-identical**, and the checker verifies that
separately: prose lives in `app.py`, so a "docs-only" change to `query.py` is a
contradiction in terms.

**What a PASS does and does not license:**

- it licenses **skipping a C1/C2 re-run for that diff**;
- it does **not** make the commit **new C1/C2 execution evidence**. `c1f` and `c2g`
  remain the evidence, and what they describe — the **row-order data path** — is exactly
  what a docs-only diff does not touch;
- **if any Swagger change ever touches API behaviour, the exemption does not apply**: it
  needs a **new execution identity** and a fresh C1 and C2. The checker fails such a
  diff, and a failing check may **not** be re-classified as docs-only. Stop and report
  it.

### 9.2 The applied text, and where it went

The PI's wording, verbatim:

> For successful responses containing multiple rows, rows are ordered by
> (time_period numeric ascending, depth ascending, lat ascending, lon ascending).
> Within a fixed time_period and depth block, latitude is the outer dimension and
> longitude varies fastest. JSON field order and CSV header order are unchanged.

| surface | location | applied |
|---|---|---|
| OpenAPI `info.version` | `api/app.py`, `get_openapi(version=...)` | **`1.0.0` → `1.1.0`** |
| OpenAPI description | `api/app.py`, `get_openapi(description=...)` | a new `* Row order (since 1.1.0)` bullet |
| JSON endpoint | `api/app.py`, `get_woa23`'s docstring | a `#### Row order (since 1.1.0)` section |
| CSV endpoint | `api/app.py`, `get_woa23_csv`'s docstring | the same section — the contract binds both |
| `README.md` | repository root | recorded under *Usage*; not an `api/` change |

**The Swagger URLs are unchanged**, and were audited rather than assumed — from
`woa23_app.py` and `dev2026/api/app.py` directly:

| | path | note |
|---|---|---|
| OpenAPI JSON | `/api/swagger/woa23/openapi.json` | identical in both trees |
| Swagger UI | `/api/swagger/woa23` | identical in both trees |
| FastAPI default `/docs` | **disabled** (`docs_url=None`) | in both trees |
| data endpoints | `/api/woa23`, `/api/woa23/csv` | unchanged |

**The last sentence of the text is not decoration.** "JSON field order and CSV header
order are unchanged" keeps the promise to exactly what was decided: column order is
still hash-dependent (§3), and a statement that promised row order without excluding
column order would over-promise on the very axis this work deliberately left alone.

**`apiverse` is out of scope.** That project and its hosted URL are a different system;
nothing here inspects, modifies, validates or cites it.

### 9.3 Versioning and announcement — DECIDED

**`1.1.0`**, decided by the PI on 2026-08-19. A **row-order contract change, not an
endpoint migration**: the endpoint URLs do not change, so **no consumer needs to switch
to a new URL**, and the announcement's subject is "API 1.1.0 adds a row-order contract".

**Announcement scope is the WOA23 API's users and this project's documents.** It does
**not** include `apiverse`.

At a production release, at minimum: the embedded OpenAPI/Swagger document (**done**),
`README.md` or a release note (**done** for the README), and the ROADMAP, spec and
BASELINE records (**done**).

**Q2 remains UNKNOWN** (§2) — nobody has established whether a consumer reads rows
positionally — so the announcement cannot be targeted from evidence, only broadcast.

#### 9.3a The previous open items, for the record

**This is now decided, and the following was the open list before it was.**

The roadmap's standing constraint: *"Any change to returned values, column names,
ordering, null handling, or the JSON/CSV contract requires an explicit versioning
decision — it is not a side effect anyone may take."* The API is published with a DOI,
`api/app.py` declares `version="1.0.0"`, and the row order is a change of exactly that
kind.

**Open, each needing the PI's answer:**

1. **Does the version string change?** `1.0.0` today. A bump, a new path, or no change
   at all are all defensible; **no number is assumed here** and none has been written
   into any file.
2. **Is there an announcement, and to whom?** Q2 — whether any consumer reads rows
   positionally — is still **UNKNOWN** (§2), so the audience cannot be derived from
   evidence.
3. **Does the documentation change land before, with, or after deployment?**
4. **Is the C1/C2 re-run that §9.1 implies acceptable**, or should the documentation
   wait so that one re-run covers both the text and anything else pending?

**Until these are answered:** the text above is not applied, `api/` is not touched, and
the published surface keeps saying nothing about row order — which spec 008 §2a already
records as acceptable *only because nothing is deployed*.

## 10. Validation — the evidence this candidate has

| gate | run | result |
|---|---|---|
| **C1**, variant 5.2C | `c1f`, 2026-08-19 | **PASS** — canonical values/columns 64/64; candidate row-order contract 44/44 applicable (20 `N/A`); required byte checks pass; no unexpected differences. **Expected raw-order differences 0; raw-order reconstruction N/A, not exercised** — under a pinned seed the reference also emitted contract order |
| **C2**, variant 5.2B semantic + candidate order gate | `c2g`, 2026-08-19 | **PASS** — semantic gate PASS ×3; seed diversity OBSERVED 3/3; **row-order contract PASS**, 132 applicable responses, 0 violations, 0 missing records, 60 `N/A`; candidate order **stable across all three cycles**; shutdown budget CONSISTENT |

**C2 is where the contract was actually demonstrated.** On `C16` and `C16-csv` — the two
cases spec 003 was written about — the **reference took two different orders** across
three independent unpinned starts while the **candidate took one**, at production's
**measured** worker count (`-w 2`, read from pid 4296's argv). Three processes with
three measurably different hash seeds, one candidate order: that is what shows the
**sort**, not the seed, decides it.

**The reference's variation is recorded and is not a failure.** It does not implement
the contract, and under an unpinned seed its order is a property of its process.

Full records: `specs/002-production-correctness-deploy-hardening.md` under `c1f` and
`c2g`. **`c1e`, `c2f` and `s2pB` describe the tree without the sort and are not cited as
evidence for this one.**

### 10.1 What the validation does not cover

- **Q2 — does any consumer depend on the old order? Still UNKNOWN** (§2). Neither gate
  observes a client, and no evidence here changes that.
- **Performance.** Neither gate measures it. The sort's cost is spec 009's subject:
  audited and benchmarked offline, **not** measured end to end. **`s2pB` may not be
  quoted for this candidate.**
- **Deployment readiness.** PM2 / formal deployment validation is a separate open track,
  and §9.3's decision is a precondition.
