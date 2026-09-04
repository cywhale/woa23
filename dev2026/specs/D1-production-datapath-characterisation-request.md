# D-1 — production **data-path characterisation**: request draft

**Status: DRAFT. NOT AUTHORISED, NOT EXECUTED. No VM24 contact was made to write it.**

First stage of the B2–B5 roadmap ([`022`](022-b2-b5-cutover-roadmap.md)). Each stage needs
its own authorisation; **nothing beyond D-1 should be requested until D-1 and D-2 are
reviewed.**

> **Scope was reduced by the reconciliation in [`022` §4](022-b2-b5-cutover-roadmap.md).**
> `c1r`/`c2k` already compared `woa23_app:app` against `api.app:app` **on the real
> production store**, read-only, with 64/64 cases accounted for and `regressions: []`.
> **D-1 does not re-ask that question.** It asks a narrower one: **does the production
> DEPLOYMENT behave as the harness runs showed?**

---

## 1. The question, precisely

`c1r`/`c2k` ran the two modules under the **benchmark harness** — uid 994, staging ports
19111/19112, a package **clone** on `PYTHONPATH`, 1–2 workers, no TLS.

Production runs `woa23_app:app` as **uid 1000**, on **8050**, under **PM2**, with **TLS**,
**2 workers**, `--reload`, and the **installed** package rather than a clone.

**D-1 asks whether those differences change the response.** Nothing more.

---

## 2. Case set — fixed here, not chosen at the keyboard

**The case set is `c1r`'s**, so D-1's responses are comparable to bodies that already exist.
`c1r` retained the exact reference bodies for its non-byte-identical cases, and its case
definitions are in the subject.

| | |
|---|---|
| source | `c1r`'s **64-case** set, from the subject's own case definitions |
| **D-1 subset** | see the fixed table in **§2.1** — **10 requests**, resolved verbatim from `bench/contract_cases.py` |
| why a subset | every case is a real production query. **10 is enough to detect a deployment-level difference and is a bounded, stated cost**; 64 would quadruple the marker impact for no additional class of evidence |
| selection rule | **written here, not decided during the run.** No case may be added, dropped or substituted at execution time |

### 2.1 ADDENDUM — the ten requests, fixed and verbatim

**Authorised 2026-08-29.** Resolved from `bench/contract_cases.py` in the subject. Executed
**sequentially in this order**, each **at most once**, **no retry**, and **no request outside
this table**.

| # | case | endpoint | parameters | expect | emits A11 marker |
|---|---|---|---|---|---|
| 1 | **C1** | `/api/woa23` | `lon0=135&lat0=15&append=<the ten append variables, verbatim from case C1>` | 200 | **yes** |
| 2 | **C16** | `/api/woa23` | `lon0=135&lat0=15&parameter=salinity,temperature&time_period=13,0&append=mn,an` | 200 | **yes** |
| 3 | **C20a** | `/api/swagger/woa23/openapi.json` | *(none)* | 200 | **NO** |
| 4 | **C1-csv** | `/api/woa23/csv` | as C1 | 200 | **yes** |
| 5 | **C16-csv** | `/api/woa23/csv` | as C16 | 200 | **yes** |
| 6 | **C2** | `/api/woa23` | `lon0=135&lat0=15&append=an` | 200 | **yes** |
| 7 | **C3** | `/api/woa23` | `lon0=135&lat0=15&append=mn` | 200 | **yes** |
| 8 | **C4** | `/api/woa23` | `lon0=15&lat0=22&lon1=20&lat1=26&dep1=0&parameter=temperature` | 200 | **yes** |
| 9 | **C5a** | `/api/woa23` | `lon0=135&lat0=15&time_period=0&append=ma&parameter=temperature` | **404** | **yes** |
| 10 | **C5b** | `/api/woa23` | `lon0=135&lat0=15&time_period=13&append=sdo&parameter=oxygen` | **404** | **yes** |

**Parameters are taken from `bench/contract_cases.py` at execution time, not retyped here.**
Case C1's `append` list is the ten-variable string defined there; reproducing it by hand in
this table would create a second source that could drift from the first. The runner reads
the case definitions from the subject and emits the exact query string it used into the
per-case evidence, which is what the record is based on.

**A flaw in my own selection rule, corrected here.** The rule in §2 — *"the 5 retained-body
cases plus the first 5 in canonical order"* — **overlaps on C1** and yields nine distinct
requests, not ten. The retained-body set is C1, C16, C20a (JSON) plus C1-csv, C16-csv (CSV
replays); the fixed resolution is **the next 5 in canonical order excluding those already
selected**: C2, C3, C4, C5a, C5b. **No case is substituted to make a number come out.**

### 2.2 ADDENDUM — expected A11 delta is **9**, not 10

**Authorised 2026-08-29**, correcting §4.

The marker is emitted in `process_woa23_data` (`woa23_app.py:185`), which **both**
`/api/woa23` and `/api/woa23/csv` call. **C20a is the Swagger route** (`custom_openapi`) and
**never reaches it**. The only 404 is raised at line **294**, *after* the marker — so
**C5a and C5b do emit**, despite returning 404.

```
10 requests issued   ->   9 marker-emitting   ->   EXPECTED A11 DELTA = 9
```

| outcome | classification |
|---|---|
| **delta = 9** | *consistent with the nine marker-emitting cases* |
| **delta != 9** | **UNATTRIBUTED.** No adjustment of the expectation, no explaining away, no retry |

**A11 remains QUALIFIED PROXY ONLY.** No delta, matching or not, may be reported as the
exact request count, unchanged or otherwise.

### 2.3 ADDENDUM — store read-only is APPLICATION-LEVEL ONLY under D-1

**Authorised 2026-08-29.** A weaker guarantee than `c1r` had, and recorded as such.

| | `c1r` | **D-1** |
|---|---|---|
| executing uid | **994**, *not* the store's owner | **1000 (`odbadmin`)**, **the store's owner** |
| kernel write check on the store | **no** — enforced by permissions | **yes** — mode 775, owned by the executing account |
| guarantee | **filesystem-enforced** read-only | **application-level only** |

**What D-1 may claim:** an **application-level read-only operation** — only fixed GET
requests were issued, the production app opened the store through its own existing handler,
and the before/after metadata identity is unchanged.

**What D-1 may NOT claim:** any filesystem- or ACL-enforced read-only guarantee of the kind
`c1r` had, or any **store content-integrity proof**. The OS could not have prevented a write
by this account; the evidence that none occurred is the fixed request set, the handler's own
behaviour, and the metadata fingerprint — **not permissions**.


---

## 3. Read-only scope

**Permitted:** issuing the fixed HTTPS GET requests to `127.0.0.1:8050`; reading responses;
reading `/proc`, `pm2 jlist`, `ss -ltn`, log marker counts, conf and store metadata.

**Forbidden:**

- any `pm2` lifecycle command — `stop`, `start`, `restart`, `reload`, `delete`, `kill`,
  `save`, `resurrect`;
- any change to `conf/`, `start_app.sh`, the PM2 definition, the environment, or the store;
- any **write** to the production store, and any operation that opens it for writing;
- any request outside the fixed case set;
- any deployment, artifact, venv or config change;
- cleanup of `b1s1`, `bs3v1`, `pm2G`, `pm2A`, `pm2B` or any retained state;
- touching `conf/simu.sh`.

**The production deployment is not modified in any way.** D-1 only asks it questions.

---

## 4. A11 — this stage WILL move the marker, and by a known amount

> **SUPERSEDED IN PART BY §2.2.** The figure below says **+10**; the correct expectation is
> **+9**, because C20a is the Swagger route and does not reach the query handler. §2.2 is
> authoritative. The original text is kept unaltered as the record of what was corrected.

**This is the one place D-1 differs from every read-only stage so far.**

Every case is a real data query, so **each request emits the
`Handling parameters and time_periods` marker**. With 10 cases:

| | |
|---|---|
| expected marker delta | **exactly +10**, all attributable to D-1 |
| recorded as | **10 OPERATOR CHECKS**, itemised with timestamp, URL, status and size — never organic traffic |
| **if the delta ≠ 10** | the difference is **UNATTRIBUTED and reported as such.** It is **not** explained away as organic traffic, and it is **not** used to revise the expected figure |
| A11 status | **QUALIFIED PROXY ONLY, unchanged.** The delta is still not an exact request count — markers disagree by 17, counts are cumulative since 2024, no rotation policy |

**This must be accepted explicitly before authorisation.** A11 has been protected from
perturbation in every prior stage; D-1 knowingly perturbs it, by a stated amount, for a
stated reason. **If that is not acceptable, D-1 cannot run in this form** and needs either a
real counter first or a smaller case set.

---

## 5. Evidence to record

| # | item | note |
|---|---|---|
| 1 | boot id | before and after |
| 2 | production PM2 state | `jlist`, all 9 apps, before and after |
| 3 | target `(pid, starttime)` + depth-2 descendants | **no restart may occur during D-1**; a changed pid is a finding |
| 4 | listener inventory | `ss -ltn` |
| 5 | **per case**: URL, UTC timestamp, HTTP status, `time_total`, size, **full response body** | bodies retained as evidence |
| 6 | **per case**: row count, column list and column order | the comparable structure |
| 7 | A11 marker | before, after, delta, and the expected +10 |
| 8 | production `conf/` digest | `ed5dec6c…2159`, before and after |
| 9 | production store **metadata** identity | files, bytes, `path+size+mtime` fingerprint — **metadata-level only** |
| 10 | external conf digest re-verified | before and after |

**Comparison itself is D-2's job, offline.** D-1 records; it does not adjudicate.

---

## 6. Store protection

The store is opened **read-only by the production app itself**, exactly as it is for organic
traffic — D-1 adds no new access path. Its metadata identity is captured before and after
and **must be unchanged**; a change is a finding, not a tolerance.

**No content digest** — 123,005 files / ~35 GB. Store identity remains **metadata-level**,
with the standing limit that a same-size, same-mtime change is undetectable.

---

## 7. Abort conditions

Stop immediately, preserve evidence, report:

- any non-200 that the case definition does not expect;
- `woa23`'s `(pid, starttime)` changes during the run — the app restarted under us;
- `restart_time` or `unstable_restarts` increases;
- boot id changes;
- the conf digest changes;
- the store metadata fingerprint changes;
- any non-target app changes;
- **any case that cannot be issued exactly as written.**

**No retry of a failed case. No substitution. No additional request.**

---

## 8. What D-1 will and will not establish

| | |
|---|---|
| **will** | how the **production deployment** responds to a fixed 10-case set, with bodies retained for offline comparison |
| **will not** | close B2–B5; validate the candidate module; prove TLS correctness (requests go to `127.0.0.1` with `--insecure`); characterise the full query space; establish latency or throughput; or authorise any cutover |

**No back-fill.** B1's stop-path PASS, `b1s1`, Stage A and Stage B contribute nothing to
D-1, and D-1 contributes nothing to them.

---

## 9. Status

**DRAFT, unauthorised.** Requires: your decision on §4 (the deliberate A11 perturbation),
and the verbatim case list of §2 completed from the subject before authorisation.

**Standing state, unchanged:** B1 production stop path **CLOSED** · B2–B5 **OPEN** · A11
**QUALIFIED PROXY ONLY** · TLS, store content identity, deployment and runtime equivalence
**not established** · `conf/simu.sh` **separate** · `b1s1` and `bs3v1` retained state **not
cleaned**.
