# 006 — JSON/CSV empty-result response consistency

**Status: decision memo. Option B adopted; NOT implemented, not executed.** No
change to `api/` is made or authorised by this document, and no run has been
performed against it.

**This is a small API contract follow-up, not a work track.** It came out of the D1
`d1b` measurement and is recorded here so the decision is not lost; it blocks
nothing, and it has no status line of its own in the roadmap. When it is
implemented it will belong to whichever candidate change carries it — see the
ROADMAP's S3 sub-item — and it is an **intentional API behaviour change**, not a
refactor.

| rev | date | change |
|---|---|---|
| 5 | 2026-09-02 | **Option B implemented and verified on the real path** — §5.4. Removing the 400 was not sufficient: `pivot` derives value columns from the VALUES of `parameter_variable`, so a zero-row frame pivoted to the index columns alone and the empty CSV header was `lon,lat,depth,time_period` while the same query with data returned `lon,lat,depth,time_period,temperature`. `process_woa23_data` now records the value columns each `(parameter, variable)` pair would produce, at the point the pair is found to have a data array, and restores the missing ones **only when the frame has zero rows**. Nothing is invented, renamed or dropped. Verified end to end against the subject's own deterministic synthetic store: the empty header is byte-equal to the canonical header of the same query with data. Still a **behavioural response fix, not a performance change**. |
| 4 | 2026-09-02 | **Implemented in the candidate, minimally** — the CSV route's `if df.is_empty(): raise HTTPException(400, ...)` branch is REMOVED, so a valid query matching zero rows falls through to `write_csv` and returns **200** with a header-only CSV and `text/csv`. **This is a behavioural response fix, NOT a performance change**: nothing in the query path, data ordering, TLS, PM2, configuration or the legacy production app is touched, and no timing characteristic is altered. The branch was removed, not widened — `process_woa23_data` raises its own `HTTPException` for unusable parameters and for a query matching no data arrays at all (404), `ValueError` becomes 400 and anything else 500, none of which reaches the removed line. `bench/test_csv_empty_result.py` (40 assertions, offline, `process_woa23_data` replaced per case so no store is opened) asserts the empty pair, an unchanged non-empty pair, five error classes that must NOT become 200, and JSON/CSV status consistency across all of them. **§5.3's option-B header is not yet verified against the real pipeline** — see §5.4. |
| 3 | 2026-08-11 | **The column question is decided: option B** — §5.3. The empty CSV carries **the columns the same query would have produced with data**, so one query has one schema whether or not it returns rows. For the nitrate case that header is `lon,lat,depth,time_period,nitrate`. This is the target for a future candidate implementation and **may not be implemented during a D1 rerun**, which measures current behaviour only. |
| 2 | 2026-08-11 | **The CSV empty representation is specified, and the acceptance cases listed** — §5, §6. The existing CSV header, `Content-Type` and `Content-Disposition` are taken from the D1 `d1a` run's own supported response rather than from a reading of the code. **One question the "header-only" instruction does not settle is surfaced rather than decided quietly**: for an empty frame the current code path would emit the INDEX columns only, so the header of an empty response would differ from the header of a non-empty response to the same query. Three options are set out with their costs and one is recommended; the choice is the PI's. Future JSON/CSV contract cases are enumerated with what each must assert. |
| 1 | 2026-08-11 | Initial. **Option A adopted**: a valid query with no matching data returns HTTP 200 with an empty representation in **both** formats. JSON's current `200 []` is the canonical reference. Records the D1 `d1a` observation of the **current** behaviour — JSON 200 `[]` against CSV 400 — as an observation and not as a result of this policy. Implementation, candidate change, C1/C2 rerun and rollout are each separately unauthorised. |

---

## 1. The policy

> For a syntactically and semantically valid query whose selected data range
> contains no available levels or records, JSON and CSV responses use the same HTTP
> success semantics: HTTP 200 with an empty representation.

| format | status | body |
|---|---|---|
| JSON | **200** | `[]` — **unchanged**, and the canonical reference |
| CSV | **200** | a defined empty CSV representation — **specified in §5**. No new column, name or format is invented |

The `Content-Type` may differ between the two — it already does. What must agree is
the **status** and the **machine semantics of "empty result"**.

**CSV no longer returns 400 for this class**, and no longer returns
`No data available for the given parameters.` as an error for it.

**JSON's existing semantics are not changed by this document.** If evidence later
shows `200 []` is itself wrong for the product contract, that is a separate decision
and needs its own memo.

## 2. What this does and does not cover

This is a consistency decision about **one class of response**. It does not say that
4xx becomes 200.

### 2.1 Returns 200 with an empty representation

All of:

- the request is syntactically well formed;
- grid, variable, climatology and `time_period` are legal and the API can process
  them;
- the group exists and is readable;
- **but** the depth selection matches no level, or the query result is otherwise
  empty.

### 2.2 Keeps an error status

- malformed request syntax;
- an unsupported grid/variable combination — e.g. a quarter-degree request for a
  one-degree-only variable, which `api/query.py` refuses because WOA23 does not
  publish it;
- a `time_period` that does not exist or is not permitted;
- a **missing required API data field**, such as the statistical-mean field `mn`
  (spec 005 §5.2) — the query path cannot complete, which is not the same as
  completing and finding nothing;
- the store or group missing or unopenable;
- any other genuine request-validation or server failure.

**"A legal query with no result" and "an illegal request or a store/schema failure"
are different things and must not be merged.** The first is an answer; the second is
a refusal.

## 3. Standing

This is an **API consistency decision, not an HTTP requirement.** Nothing in the
HTTP specifications compels either choice — a 200 with an empty list and a 4xx for
"nothing matched" are both expressible. The reasons for choosing it are internal
consistency across the two representations of the same endpoint, and not making a
client's error handling depend on which format it asked for.

- RFC 9110, *HTTP Semantics* — https://www.rfc-editor.org/rfc/rfc9110.html
- RFC 9457, *Problem Details for HTTP APIs* — https://www.rfc-editor.org/rfc/rfc9457.html

## 4. What was observed, and what is desired

**These are two different things and this document keeps them apart.** The D1 `d1a`
run of 2026-08-11 measured the behaviour that exists today. It did not test this
policy, and nothing in its artefacts may be rewritten to look as though it had.

### 4.1 Observed current behaviour — D1 `d1a`, 2026-08-11

Winter nitrate (`time_period=13`), `1_degree/seasonal/Nutrients`, depth 3000–4000 m,
a range in which the group's depth axis has **no selectable level**:

| case | status | Content-Type | bytes | body |
|---|---|---|---|---|
| `D1-DEPTH-OOR-tp13` | **200** | `application/json` | 2 | `[]` |
| `D1-DEPTH-OOR-tp13-csv` | **400** | `application/json` | 56 | `No data available for the given parameters.` |

Both arms returned **identical bytes**, and the anchor probe after each case
returned 200. This is **the current production/reference behaviour as measured** —
the candidate and the unmodified reference agree, so it is not a candidate defect
and not a consequence of any patch.

### 4.2 Desired behaviour under this policy

| case | status | body |
|---|---|---|
| the JSON case | **200** | `[]` — unchanged |
| the CSV case | **200** | an empty CSV representation — **specified in §5**, with one column question still open (§5.3) |

**Not yet implemented, not yet tested, and not the subject of any run.** The
existing D1 artefacts record 4.1 and must not be edited to record 4.2.

## 5. The CSV empty representation

### 5.1 What the existing CSV response is, measured

Taken from the D1 `d1a` run's **supported** case, `D1-DEPTH-SUP-csv` — the same
endpoint answering the same shape of query with data in it. Not a reading of the
code: these are the recorded bytes.

| | |
|---|---|
| status | `200` |
| `Content-Type` | **`text/csv; charset=utf-8`** |
| `Content-Disposition` | **`attachment; filename="woa23_from_ODB_<YYYY-MM-DD>.csv"`** |
| header row | **`lon,lat,depth,time_period,nitrate`** |
| first data row | `135.5,15.5,0.0,0,` — a land/no-value cell is an **empty field**, not `NaN` |
| line ending | `\n`, with a trailing newline after the last row |

The header is the pivot's index columns — `lon`, `lat`, `depth`, `time_period` —
followed by one column per requested parameter/variable pair, named `{parameter}`
when `mn` is among the requested variables and `{parameter}_{variable}` otherwise.

### 5.2 The empty representation

**Unchanged from the above:** status `200`, `Content-Type`
`text/csv; charset=utf-8`, `Content-Disposition` as today, `\n` line endings, and a
trailing newline. **Body: a single header row and no data rows.**

### 5.3 The column question — decided: option B

**Which columns does the header carry when there are no rows?**

For an empty frame the pivot has nothing to pivot on, so **the current code path
would produce the index columns alone** — `lon,lat,depth,time_period` — and *not*
the `nitrate` column that the same query produces when it has data. That is a
reading of `api/query.py`, **not a measurement**: the CSV 400 meant no empty CSV
body was ever produced, and none has been observed.

| option | body | cost |
|---|---|---|
| **A — index columns only** | `lon,lat,depth,time_period\n` | what falls out of the current code once the 400 branch is removed; **but the header differs between an empty and a non-empty response to the same query**, so a client cannot rely on the column set |
| **B — the columns the query would have produced** — **ADOPTED** | `lon,lat,depth,time_period,nitrate\n` | **one query's schema is one schema, empty or not**, which is the property that makes an empty CSV usable. Needs the column set derived from the requested parameters and variables without data — a small amount of new logic in the candidate |
| C — a zero-byte body | *(nothing)* | rejected: it is not a CSV representation of anything, and a client cannot tell it from a truncated transfer |

**Adopted: B.** The point of returning 200 is that the client can parse the answer
with its ordinary path; a header that changes shape when the result is empty puts
the special case straight back in.

> HTTP 200, header-only body in the existing CSV schema. For the nitrate case the
> header is `lon,lat,depth,time_period,nitrate`.

**What B costs, stated so it is not discovered later.** The current code path
produces the index columns alone, because the pivot has no rows to derive value
columns from. B therefore needs the column set built from the requested parameters
and variables **without data** — the same `{parameter}` / `{parameter}_{variable}`
naming and the same `mn` rename the non-empty path applies. That is new logic in
`api/`, and it is the reason B is a **target** rather than a tidy-up.

**Not to be implemented during a D1 rerun.** A rerun measures the behaviour that
exists; implementing B would change what it measures. The candidate change is a
separate step with its own authorisation, and it invalidates `c1e` and `c2f` as
post-patch evidence (§8).

## 6. Future acceptance — JSON/CSV contract cases

To be added **with the candidate change**, not before, and to a case list that keeps
the existing 64 contract cases untouched.

| case | asserts |
|---|---|
| **valid query, no matching data — JSON** | status **200**; body exactly `[]`; `Content-Type` `application/json`. **Unchanged from today** — this is the control that the policy did not disturb JSON |
| **valid query, no matching data — CSV** | status **200**; `Content-Type` `text/csv; charset=utf-8`; `Content-Disposition` as today; body exactly `lon,lat,depth,time_period,nitrate\n` for the nitrate case — **the columns the same query produces with data (option B)** — and **no data rows** |
| **the empty and non-empty CSV headers agree** | the header of the empty response is **byte-identical** to the header of the non-empty response to the same query. This is what option B buys, and the only assertion that checks it |
| **the same query in both formats** | the two statuses are **equal**, and both are 200 |
| **valid query WITH data — JSON and CSV** | byte-identical to today. The supported annual case is the control: **the non-empty response must not change** |
| **unsupported grid/variable** | still an error status — 0.25° with a one-degree-only parameter |
| **invalid `time_period`** | still an error status |
| **missing `mn`** | still an error status; the query path cannot complete (§2.2) |
| **unopenable group** | still an error status |

The last four exist so that a later reader can see the policy was **bounded**: a
test suite that only asserted the 200s would be equally consistent with having
turned every error into a success.

## 7. Open work

| item | status |
|---|---|
| the empty CSV representation | **settled** — §5.2 and §5.3, option B |
| deriving the column set without data | **open** — the candidate work option B requires |
| candidate change to `api/app.py` | **open, unauthorised.** `api/` is not modified by this memo |
| contract cases for the JSON and CSV out-of-range pair | **open** — both must assert 200; the JSON body must keep `[]` |
| the non-empty annual supported response | **must not change** — it is the control |
| C1 and C2 rerun | **required** once the candidate changes (§6) |
| rollout | **open, separately authorised** |

## 8. If the candidate is changed

A change to `api/` makes every existing correctness result describe a different
candidate. So:

- the candidate source provenance is updated;
- **C1 and C2 are re-run**;
- **the current C1 (`c1e`) and C2 (`c2f`) results may not be cited as post-patch
  evidence.** They describe `919095e8`, which is the candidate as it is now.

This memo does not authorise any of that.

## 9. What this is not

- **not a finding of the D1 run.** D1 observed the current behaviour; the policy is
  a decision taken afterwards, and the two are recorded separately (§4);
- **not related to P4's depth-schema check.** P4 verifies Table 4's count and extent
  for one variable and climatology. Whether an empty result is a 200 or a 400 is an
  API contract question and has nothing to do with it;
- **not a general relaxation of error statuses** (§2.2);
- **not implemented.** Nothing here has been executed, and `api/` is unchanged.


---

## 5.4 Option B — the concern was real, and it is now fixed and verified

§5.3 chose **option B**: the empty CSV carries the columns the same query would have
produced with data. The implementation satisfies that **by construction rather than by
special-casing** — the result frame is passed to `write_csv` unchanged, so the header is
whatever schema the pipeline produced for that query, empty or not. No column is invented,
renamed or dropped for the empty case.

**The concern changelog row 2 raised was real, and removing the 400 alone did not fix it.**
Driven against a local synthetic store, the route returned 200 `text/csv` with:

```
non-empty header : lon,lat,depth,time_period,temperature
empty header     : lon,lat,depth,time_period            <-- the parameter column MISSING
```

`pivot` derives its value columns from the VALUES of `parameter_variable`, so a zero-row
frame pivots to the index columns alone. The status was right and the header was still
wrong: a consumer parsing by header position would read a different table shape depending
on whether anything matched.

**The fix records the value columns where they are already known.** `process_woa23_data`
accumulates `expected_value_columns` at the point each `(parameter, variable)` pair is
found to have a data array — the same `{param}_{var}` the pivot builds and the same rule
`canonical_column_order` uses to place them. When, and only when, the result frame has zero
rows, the missing ones are added as null columns. Nothing is invented: a parameter with no
data array contributes nothing, so an empty result never gains a column a full one would
not have had. Nothing is renamed and nothing is dropped, and the caller's permutation
assertion is untouched.

Verified on the **real** path — real zarr open, filtering, pivot, rename, ordering and
`write_csv`:

| | |
|---|---|
| status for a valid empty result | **settled** — 200, `text/csv`, both routes agree |
| **option-B header for an empty frame** | **SETTLED** — byte-equal to the header the same query returns with data |
| zero data rows | **settled** |
| error classes unchanged | **settled** — 404, malformed request, store failure and 500, on the real path and mocked |

`bench/test_csv_empty_result.py` section 5 builds the subject's own deterministic synthetic
store into a temporary directory and asserts the empty header **equals the canonical header
taken from the non-empty response of the same query**, rather than against a header written
down here. The store is offline and proves deployment machinery and response shape only —
never WOA23 data correctness, which is `c1f`/`c2g`'s.
