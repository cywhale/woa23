# D-2 — offline adjudication of D-1's production responses: **RESULT**

## Classification, fixed

> # LIMITED D-2 OFFLINE ADJUDICATION — NO UNEXPECTED REGRESSION IDENTIFIED
>
> **This wording is fixed and may not be upgraded** — not to a data-path PASS, not to a
> deployment PASS, not to runtime equivalence, in this document or in anything citing it.

### The eight limits that travel with it

| # | limit |
|---|---|
| 1 | **`c1r`'s retained bodies are not held locally**, so its **byte-level reconstruction was NOT re-executed** |
| 2 | **C2 / C3 / C4 byte equality was NOT re-confirmed.** Only *structure matches case intent / no unexpected regression identified* |
| 3 | C1/C16 JSON+CSV **column-set, column-order and spec-015 conformance are recorded** — **not** an upgrade to having redone `c1r`'s full byte-level proof |
| 4 | C20a remains an **expected documentation change** |
| 5 | C5a/C5b's identical generic 404 is an **error-surface observation only**, **not** a regression |
| 6 | **A11 `delta=0` does NOT mean no query ran.** The marker-logging cause remains **UNDETERMINED** |
| 7 | store evidence is **application-level read-only + metadata only** — **no content-integrity proof** |
| 8 | **D-1/D-2 do not back-fill `c1r`/`c2k`** and do not replace the C1/C2 track's own scope |

**Offline only. No VM24 query, no diagnostic contact, no production request.** D-1's ten
retained bodies were transferred once as files and re-hashed locally; **all ten digests
match D-1's record exactly**.

**D-1 is not re-run and its raw observations stand unchanged.**

---

## 1. What could and could not be adjudicated — stated first

| | |
|---|---|
| **available** | D-1's ten bodies, URLs, parameters, statuses, digests; `c1r`'s **classifications** and its recorded **expected (spec-015) column orders**; the subject's own `bench/column_contract.py` |
| **NOT available locally** | **`c1r`'s retained reference/candidate BODIES.** They were retained in the `c1r` run on VM24 and are not in this repository |

**Consequence, and it bounds this result:** D-2 can re-apply the **rules** to D-1's
production bodies and place each case in its class. It **cannot** perform the byte-level
*reference-permuted-reproduces-candidate* reconstruction that `c1r` performed, because that
needs `c1r`'s candidate bytes. **What is verified here is the necessary condition (identical
column SET) and the conformance question (ORDER), not `c1r`'s full byte reproduction.**

That limit is not worked around, and nothing below is stated as though the byte
reconstruction had been repeated.

---

## 2. spec-015 column order — reconstruction precondition and conformance

Production serves the **legacy** module `woa23_app:app`, so the expected finding is that it
sits on the **reference** side of the decided column-order change. It does.

| case | rows | cols | same **SET** as spec-015 | same **ORDER** (conformance) |
|---|---|---|---|---|
| **C1** | 102 | 13 | **YES** | **NO** |
| **C16** | 204 | 8 | **YES** | **NO** |
| **C1-csv** | 102 | 13 | **YES** | **NO** |
| **C16-csv** | 204 | 8 | **YES** | **NO** |

**C1 / C1-csv** — production order:

```
lon lat depth time_period temperature temperature_an temperature_sd temperature_se
temperature_oa temperature_sdo temperature_gp temperature_sea temperature_dd
```

spec-015 order:

```
lon lat depth time_period temperature_an temperature temperature_dd temperature_sd
temperature_se temperature_oa temperature_gp temperature_sdo temperature_sea
```

Permutation (spec-015 ← production, by index):
`[0,1,2,3,5,4,12,6,7,8,10,9,11]`

**C16 / C16-csv** — production `… temperature_an, salinity_an, temperature, salinity`
against spec-015 `… temperature_an, temperature, salinity_an, salinity`; permutation
`[0,1,2,3,4,6,5,7]`.

### 2.1 Classification

**`EXPECTED_COLUMN_ORDER_CHANGE`** for all four — the same class `c1r` assigned.

- The **column set is identical**, so no value or column is added, missing or renamed: the
  reconstruction **precondition holds**.
- The **order differs**, exactly as the decided spec-015 change predicts, with production on
  the pre-change side.
- **JSON and CSV agree** on the same permutation, which is what a single ordering rule
  should produce.

**Not a regression.** This is the decided change, observed from the side that has not
adopted it.

---

## 3. OpenAPI documentation class — C20a

```
openapi      : "3.1.0"
info.title   : "ODB WOA23 API"
info.version : "1.0.0"
paths        : 2   ->  ["/api/woa23", "/api/woa23/csv"]
```

**Classification: `EXPECTED_DOCUMENTATION_CHANGE`**, and **C20a remains a documentation
case — it is not treated as a data query.**

Production serves **`info.version` 1.0.0**, the pre-change side of `c1r`'s recorded
`1.0.0 → 1.1.0` documentation difference. Both routes are present.

**Not adjudicated here:** whether production's 1.0.0 document is byte-identical to `c1r`'s
reference document — that needs `c1r`'s retained body (§1).

---

## 4. The remaining data cases — structure recorded

| case | rows | cols | columns |
|---|---|---|---|
| **C2** | 102 | 5 | `lon, lat, depth, time_period, temperature_an` |
| **C3** | 102 | 5 | `lon, lat, depth, time_period, temperature` |
| **C4** | 30 | 5 | `lon, lat, depth, time_period, temperature` |

Consistent with the case intents in the subject: **C2** ("the `mn` rename is NOT applied")
returns `temperature_an`; **C3** ("the rename IS applied") returns `temperature`; **C4**
(all-land Sahara bbox) returns **30 rows rather than an empty result**, which is the
behaviour that case exists to pin.

**No classification beyond structure.** `c1r` had these among its byte-identical cases;
without its bodies, byte equality **cannot** be re-checked here, and is not claimed.

---

## 5. C5a / C5b — recorded, NOT called a regression

```
C5a  404  {"detail":"No data found for the specified query parameters"}
C5b  404  {"detail":"No data found for the specified query parameters"}
byte-identical: TRUE
```

Both statuses **match the case definitions** (`expect_status=404` for each).

**No existing rule classifies this as a regression, so it is not called one.** The two cases
describe different underlying conditions — C5a a variable absent from an annual group, C5b
seasonal oxygen having no `sdo` — and the API returns the **same generic 404** for both.
That is **recorded as an observation about the error surface's granularity**, and left for
whoever decides whether the contract should distinguish them. **It is not a D-2 finding
against the deployment.**

---

## 6. A11 was NOT used as evidence

**The A11 delta of 0 is not used anywhere in this adjudication**, and specifically **not as
evidence that the queries did not execute.**

The responses adjudicated above are the queries' own output: 102, 204 and 30 rows of real
WOA23 data, correct column sets, the expected 404 bodies. **The queries demonstrably
executed and were served.** The marker shortfall is a **logging observation**, addressed
separately in the [A11 diagnostic memo](A11-marker-diagnostic-memo.md), and it bears on the
log — not on whether the deployment answered.

---

## 7. Verdicts

| case | class | verdict |
|---|---|---|
| C1, C16, C1-csv, C16-csv | `EXPECTED_COLUMN_ORDER_CHANGE` | set identical, order pre-change — **as decided, not a regression** |
| C20a | `EXPECTED_DOCUMENTATION_CHANGE` | `info.version` 1.0.0, pre-change side — **documentation class, not a data query** |
| C2, C3, C4 | structure consistent with case intent | **no regression identified**; byte equality **not re-checked** (§1) |
| C5a, C5b | expected 404, identical bodies | **recorded, not classified as a regression** |
| **unexpected regressions** | | **NONE IDENTIFIED** |

### 7.1 What this result does NOT claim

- **No production data-path correctness PASS.** D-2 adjudicated **classification**, not
  value correctness, and it could not repeat `c1r`'s byte reconstruction (§1).
- **`c1r`/`c2k` are not extended or back-filled.** Their limited real-store contract
  evidence stands exactly as recorded.
- **Nothing about B2–B5, TLS, deployment equivalence or store content identity.**
- Row-order rules were **not applicable** to these ten cases as adjudicated here: `c1r`'s
  row-order contract was evaluated against candidate responses, and D-1 captured only the
  production/legacy side.

---

## 8. Status

**D-2 offline adjudication: COMPLETE.** No unexpected regression identified in D-1's ten
production responses; every difference from spec-015 falls in a **previously decided class**,
with production on the pre-change side — which is what a legacy deployment should look like.

**Standing state unchanged:** B1 production stop path **CLOSED** · B2–B5 **OPEN** · A11
**QUALIFIED PROXY ONLY**, D-1 **UNATTRIBUTED** · TLS, store content identity and deployment
equivalence **not established** · `conf/simu.sh`, `b1s1`, `bs3v1`, `pm2G` **untouched**.
