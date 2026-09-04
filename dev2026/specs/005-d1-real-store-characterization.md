# 005 — D1 real-store characterization

**Status: two runs executed, 2026-08-11.** `d1a` is classified
`INVALID_POST_MEASUREMENT_HARNESS` (§15); `d1b` is
**D1 CHARACTERIZATION RECORDED — real-store nitrate depth behavior under the
declared one-worker scope** (§16). **Neither is a D1 PASS and D1 is not complete.**

The plan below is unchanged and remains the plan. What follows it in §15 is a record
of the one run performed against it, and that run is **not** a D1 result: its
measurements completed, its post-run artefacts did not.

Nothing else in this document has been run. No VM24 process has been started for it, no
HTTP request has been sent, and **no change to `api/` is proposed by it**. It plans
one controlled run whose purpose is to *observe* what the real store does for two
depth queries and, if such a case exists, for one missing non-anchor group. It does
not propose to change what the API returns.

| rev | date | change |
|---|---|---|
| 11 | 2026-08-11 | **`d1b` executed and recorded — `D1 CHARACTERIZATION RECORDED — real-store nitrate depth behavior under the declared one-worker scope`** — §16. Same commit family, new execution identity: staging `~/woa23-d1b/`, label `d1b`, first-use ports 18151/18152/18879, `d1a` untouched. **P1-P6 met**; **P4 Table 4 count/extent compatibility verified, full Table 3 level-list equality not verified**; both depth digests identical to `d1a`'s. **Annual nitrate 0-800 m: JSON 200, CSV 200. Winter nitrate `time_period=13` at 3000-4000 m: JSON 200 `[]`, CSV 400 `No data available for the given parameters.`** Both arms byte-identical; **8 of 8 observations reproduce `d1a`**; every recovery probe 200. **22 request attempts, 11 per arm, measured**. Clone integrity 3/3, production 0 requests and unchanged, cleanup PASS, and **all post-run artefacts finalised** — the step that failed in `d1a`. **Not a D1 PASS and not D1 complete**: real-store missing-group behaviour is uncharacterized because all twelve groups exist, `mn` and the three nutrients are a schema observation, and only nitrate was requested. The CSV 400 is recorded as observed and is **not** rewritten toward spec 006's target. |
| 10 | 2026-08-11 | **One run executed and classified `INVALID_POST_MEASUREMENT_HARNESS`** — §15. The `d1a` run completed every measurement — P1-P6 met, four cases issued, both arms byte-identical, every recovery probe 200, cleanup PASS — and then failed writing its post-run artefacts: a heredoc read `os.environ["LABEL"]`, which was never exported, leaving `d1a_workers.json` at zero bytes and `d1a_requests.json` unwritten. **Not a D1 PASS, not a characterization failure, and D1 is not complete.** The status line at the top of this document is corrected accordingly. The JSON/CSV divergence the run observed is carried into **spec 006** as *observed current behaviour*, not as something this run resolved. No artefact was edited and no re-run has been performed. |
| 9 | 2026-08-10 | **`--expect-worker-count` is an assertion and sets nothing** — §6.4, §9. The **D1 mode** sets the worker count, through the `-w 1` on each launch line; the flag only refuses to record provenance for an arm whose `/proc/<pid>/cmdline` disagrees. This is the distinction `--expected-workers` already carries for C2, where confusing the two would let the harness pick a number and report it as production's. §9 also states plainly what the runner re-confirms **at run time** rather than from this document: that the staging directory, workdir and label do not already exist, that the three ports are actually free on the host, and that the clone manifest is the four-column file. A parameter written here is a proposal; the runner checks the world. |
| 8 | 2026-08-10 | **One worker per arm is a design choice, and the record says so** — §6.4. D1 launches `-w 1` for the same reason C1 does, and "workers: 1" reads identically whether it was chosen or measured. **C2 is the mode that measures production's worker count and runs the arms at it; D1 does neither.** The runner now asserts the count against each arm's own `/proc/<pid>/cmdline` (`--expect-worker-count`), writes `results/<label>_workers.json` carrying `arm_workers: 1`, `derived_from_production_measurement: false`, both arms' **full launch argv** and the per-arm count parsed from it, and prints the sentence in full. The configuration banner no longer says "read from production at run time" over a C1 or D1 run — that line was C2's and was printed for every S2 mode. |
| 7 | 2026-08-10 | **Four wording corrections, one of which was a contradiction** — §5, §5.1, §6.1. **`mn` is the statistical-mean data field** (WOA23 Table 2, p. 9 — *available objectively analyzed and statistical fields*), **not a coordinate and not an oceanographic variable**; §5 called it an "append variable" alongside the coordinates and now states its role and the narrow meaning of its absence: the current API query path cannot complete the request, so the characterization's precondition is unmet, the resulting 404 must not be read as a depth-out-of-range outcome, and it is **not by itself evidence that the store violates WOA23**. Checking it is an **API request-path requirement, not a depth-schema requirement**. **The depth-verification claim is narrowed**: P4 verifies Table 4's **count and extent**, records the full observed list and digest, and **does not assert equality with the complete Table 3 standard-depth list** — Table 3 is not transcribed into a machine-comparable form, so a matching count and extent does not establish that the levels are the standard depths. **Case ids name their period explicitly** — `D1-DEPTH-OOR-tp13`, `D1-DEPTH-OOR-tp13-csv` — because "no suffix means 13" is the same implicit default that allowed the silent season substitution; every record also carries `variable`, `climatology`, `season` and `time_period`. **A report of revision 6 misdescribed the digest test**: the canonicalization puts dtype into the hash, so `float32` and `float64` **do** differ, and the test in question compared two values differing only in float `repr` **under one dtype**. The code was correct; the description was not. The suite now asserts the four properties as four: same dtype/units/order/values → same digest; different dtype → different; different units → different; different order → different. |
| 6 | 2026-08-10 | **Scope narrowed to what is actually requested: winter nitrate, one variable of three, one Table 4 row per group** — sections 1a, 3.2, 5, 6.1. `1_degree/seasonal/Nutrients` is renamed **`case_target_group`** and is stated to be the target of *these nitrate cases only* — not a universal invariant, and **not the startup anchor**, which is `1_degree/annual/TS` and belongs to spec 004. **`Nutrients` is a group of three**: nitrate, phosphate and silicate; this run characterizes **nitrate**, and a nitrate result may not be generalized to the other two — the survey records all three as a *store schema observation*, which is not a request characterization. **Table 4 is recorded in full for TS, Oxy and Nutrients across all three climatologies**, with the restriction stated: 43 levels over 0-800 m applies **only** to nitrate, phosphate and silicate under seasonal or monthly climatology. The out-of-range case is named **winter nitrate (`time_period=13`)**, never "seasonal nitrate": `13` is the only season requested, each case carries `variable`, `climatology`, `time_period` and a `scope` string, and 14/15/16 remain separate cases with their own ids. **P4** now records the expected row, expected count and extent, the actual count, extent, ordered levels, dtype, units, monotonicity and a canonical digest whose canonicalization is spelled out; the expected *level list* is **not** transcribed from the PDF and says so rather than being guessed. **P2** additionally checks every coordinate `query.py` selects on — `lon`, `lat`, `depth`, `parameters`, `time_periods` — and the default `append` variable `mn`, for **both** case groups: a group missing `mn` answers 404 before depth is ever consulted. Monthly nitrate is **out of scope** and no result may be extended to it. |
| 5 | 2026-08-10 | **Depth conditions are per variable AND per climatology, and `time_period=13` is no longer substitutable** — sections 3.2, 5 and 6.1. Revision 1-4 let "43 levels / 0-800 m" read as a shared condition; it is **seasonal nitrate's row of WOA23 p12 Table 4 and nothing else's**. **P4** now records the target group's **full depth schema** — level count, min, max, the levels themselves and a digest over them — asks whether any level is *selectable* in 3000-4000 m (not whether the maximum falls short, which an axis with a gap would answer wrongly), and compares the measured axis with **seasonal nitrate's row only**. A disagreement is **`STORE_SCHEMA_MISMATCH`**, the preconditions are unmet, and a result from that group **may not be reported as a standard WOA23 seasonal-nitrate depth characterization**. **P5** does the same for `1_degree/annual/Nutrients` against **annual nitrate's own row** (102 levels, 0-5500 m) — an annual axis that happened to match 43/0-800 is a P5 failure, not a pass. The **twelve-group graph records existence, API-reachability and openability only**, with `depth_checked: false` on every entry: Table 4 states no store-wide invariant and none is invented. **`time_period=13` is required, not preferred** — the survey no longer picks the first seasonal code it finds. If 13 is absent, `PRECONDITION_UNMET` and `D1-depth-out-of-range` stays CHARACTERIZATION PENDING; requesting 14, 15 or 16 produces **different case ids** (`D1-DEPTH-OOR-tp14`), because a run that asked for spring is not the run that asked for winter. |
| 4 | 2026-08-10 | **F2 implemented: the request counter, and section 8.2's range is superseded by measurement** — F2 in section 13 was optional and is now done, in its own harness commit. `scripts/lib_requests.sh` counts **attempts, not successes**: the counter is incremented BEFORE each request is issued, so a readiness probe that timed out, a connection that was refused and a retry each count as one. `process_ready` and `probe` moved to `scripts/lib_http.sh` so the retry loop that decides how many requests a run sends can be driven by a test — it had none while it was inline. Counts are per arm and per stage (`readiness`, `store_probe`, `contract`, `characterization`, `recovery`), with per-arm and grand totals, written to `results/<label>_requests.json`. **The run now reports what it actually issued**, so the 22-80 range stands only as the ceiling and the plan's requirement to report a range is met by reporting the measured number instead. The ceilings in section 8 are unchanged. `scripts/test_requests.sh` drives the real loops against a real server that answers late, hangs and refuses: early success (1 attempt), retry (3 attempts for 1 success), timeout (every attempt counted, no successes) and connection refused (every attempt counted, nothing ever listening). No change to `api/`, to the 64 contract cases, or to any D1 characterization semantics. |
| 3 | 2026-08-10 | **The budget is pinned to named constants, after the implementation stated it two different ways** — section 8. Section 8's arithmetic was already right: **ten countable requests per arm** (2 store probes + 4 cases + 4 recovery probes), twenty for both arms, **22-80** in total. The first harness commit named its constant `BUDGET_D1=8` — the characterization subtotal — and that is the figure a reader, and the test suite, reached for as the per-arm total. The number was correct in one line of the banner and misleading in the name, which is the same defect as being wrong. The two are now separate and separately named, in the runner (`BUDGET_D1_CHARACTERIZATION` = 8, `D1_COUNTABLE_PER_ARM` = 10) and in `bench/d1_cases.py` (`characterization_requests_per_arm()` = 8, `countable_requests_per_arm()` = 10, `request_total_range()` = `(22, 80)`, returned as a tuple so a report cannot flatten it into a total). **The recovery probes are unchanged: one after each case, four per arm** — the mismatch was in what was counted, never in what is issued, and none was removed to make a number smaller. The end-to-end test now counts the sequence the probe actually issues and derives the budget from it rather than asserting a constant. |
| 2 | 2026-08-10 | **The startup-validation window is classified** — §10.1. Between the arms starting and the first HTTP request, D1's implemented part runs and can fail; the reviewed draft had no classification for it. A store validation error in an arm's log is **`STARTUP_VALIDATION_FAILURE`**, an arm that never became ready without one is **`INVALID_PRE_START`**, and an unreadable log is `INVALID_PRE_START` with *classification indeterminate* — fail closed. **Neither is a D1 characterization FAIL**: the cases were never issued, so nothing was characterized and spec 004 is not updated. The candidate failing there while the unmodified reference starts normally is the patch working as designed and is **not** a divergence. |
| 1 | 2026-08-10 | Initial plan. Written against branch HEAD `9749355c`, candidate `api/` byte-identical to the C1-tested `919095e8`. Two contradictions in the reviewed draft corrected: **anchor recovery is one probe per case, so four per arm, not two** (§6.2), and **the P1–P6 real-group preflight runs before the arms start, therefore before any HTTP of any kind** (§5). |

---

## 1. What this plans, in one paragraph

D1's anchor startup validation is implemented and verified against the real store.
Three parts of D1 are not: what the API returns for a depth request inside a
climatology's range, what it returns for one outside that range but inside WOA23's
global maximum, and how a missing non-anchor group behaves on the real store rather
than on a synthetic fixture. This plans a single controlled run to **characterize**
those, under the isolation C1 and C2 already established, measuring nothing about
time and asserting nothing about what the answers ought to be.

**Characterization is observation.** The only pass/fail conditions in this plan are
that the candidate and the reference return the same bytes, that the process is still
serving afterwards, and that cleanup completes. Whether a particular status code is
*right* is a contract question and belongs to spec 003 or to S2b, not here.

---

## 1a. Scope — what is characterized, and what is not

Three names that are easy to blur and mean different things:

| name | group | what it is |
|---|---|---|
| **startup anchor** | `1_degree/annual/TS` | what the **candidate itself** validates at import and lifespan, for every request, on every start. Spec 004. **Not surveyed by this run and not re-validated by it** |
| **case target group** | `1_degree/seasonal/Nutrients` | the group the **winter-nitrate out-of-range case** reaches |
| **annual control group** | `1_degree/annual/Nutrients` | the group the **supported case** reaches |

> `1_degree/seasonal/Nutrients` is the target group for the current nitrate
> characterization cases only. It is not a universal startup invariant and does not
> represent every WOA23 group or every nutrient variable.

### 1a.1 `Nutrients` is a group of three

> The `Nutrients` group contains nitrate, phosphate, and silicate. This run
> characterizes nitrate only. Results for nitrate must not be generalized to
> phosphate or silicate without separate request-level evidence.

P2 confirms `nitrate` is in the group's `parameters` coordinate and records which of
the three are present. **That recording is a store schema observation, not a request
characterization of the other two**: no request is issued for phosphate or silicate,
so nothing is known about what the API returns for them.

### 1a.2 What this run does and does not cover

| | in scope | judged against |
|---|---|---|
| **annual nitrate**, 0–800 m | **yes** — `D1-DEPTH-SUP` | **annual** nitrate's row: 102 levels, 0–5500 m |
| **winter nitrate**, `time_period=13`, 3000–4000 m | **yes** — `D1-DEPTH-OOR-tp13` | **seasonal** nitrate's row: 43 levels, 0–800 m |
| spring / summer / autumn nitrate (`14`/`15`/`16`) | **no** | — would be `D1-DEPTH-OOR-tp14` etc., with their own evidence and budget |
| **monthly** nitrate | **no** | — has its own Table 4 row (43 levels, 0–800 m) and would need its own case. **No result here may be extended to it** |
| phosphate, silicate | **no** | — same group, different variables, no request issued |
| TS, oxygen | **no** | — different Table 4 rows entirely |

The out-of-range case is **winter nitrate (`time_period=13`) depth-out-of-range
characterization**, and its id says so: **`D1-DEPTH-OOR-tp13`**. It is not "seasonal
nitrate characterization": one of four seasons is requested, and calling it by the
climatology would claim the other three. The period is in the id rather than implied
by its absence — "no suffix means 13" is the same implicit default that let the
survey substitute a season silently in revision 4.

---

## 2. D1 closed / open matrix

| item | status | evidence |
|---|---|---|
| **anchor startup validation** — the store resolves, is a directory, and the anchor `1_degree/annual/TS` opens as a readable Zarr v2 group | **closed; verified against the real store** | synthetic: `bench/test_d1_store_validation.py`, fixtures N1–N6 and P1. Real: the C1 rerun (`c1e`) opened production's own `1_degree/annual/TS` with `zarr.open_group(mode="r")` in the lifespan — this is what moved **D1-8** off synthetic fixtures. Carried unchanged through C1 `c1e` and C2 `c2f`; `api/` byte-identical in both |
| **zero-chunk claim** — that the anchor open reads no data or coordinate chunk | **offline-audited / implementation-supported. NOT a VM24 observation** | the audit hook runs only in `bench/test_d1_store_validation.py`, against synthetic fixtures. **No audit hook is installed during a VM24 run and the runner records no file-open events.** What is observed on the host is the anchor *opening*; the zero-chunk property carries over only because the same code path executes (spec 004 §52, §53) |
| **`D1-depth-supported`** — 1_degree / nitrate / annual / 0–800 m | **not established** | no synthetic and no real evidence. The combination appears in none of the 64 contract cases |
| **`D1-depth-out-of-range`** — 1_degree / nitrate / seasonal / 3000–4000 m | **not established — CHARACTERIZATION PENDING** | as above. `C18` (6000–7000 m) tests depth beyond WOA23's **global** 5500 m maximum, which is a different thing from inside the global maximum but outside this climatology's range |
| **non-anchor missing-group request-level behaviour** | **synthetic / offline only** | spec 004 §43 demonstrates all six requirements on a synthetic fixture: the request reaches the group open (raises `FileNotFoundError`, not `HTTPException`), the failure is request-level, a subsequent anchor request succeeds, lifespan does not fail. **Never established against the real store** |
| **D1-11** — `gunicorn --check-config` | **offline only, and an acceptance condition of the split option alone** | N2 and N4 fail; N3, N5, N6 and P1 succeed. A passing `--check-config` says the configuration is well-formed and **says nothing about metadata validity** |
| **deployment / PM2** | **out of D1 scope** | C1 and C2 both ran under `-S` from a shell script: `site.py` never ran, no `.pth` was processed, and the launcher is not production's PM2 path |
| **row-order contract** | **out of D1 scope** | spec 003 undecided. The `c2c` / `c2f` order observations do not bear on D1 |
| **performance** | **out of D1 scope, and prohibited here** | C1 and C2 measured no latency, throughput or resource use, and neither does this |

**Exactly one row reads "verified against the real store".**

---

## 3. Upstream basis — WOA23 p11 / p12

Source: *WOA23 Product Documentation*, NOAA NCEI,
`https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DOCUMENTATION/WOA23_Product_Documentation.pdf`
— sha256 `140aa25f…`, fetched and text-extracted 2026-08-10, 20 pages, as recorded in
spec 004 §23 and §51. Quotations are from printed pages 11 and 12.

### 3.1 Availability (p11)

> Nitrate, Phosphate, and Silicate fields are available ONLY for one-degree grid and
> for the 'all' time span.
>
> The 'all' time span for oxygen and inorganic nutrients is the time span from
> 1965-2022.

### 3.2 Depth ranges vary by variable **and** by climatology (p12, Table 4)

| variable / API group | annual | seasonal | monthly |
|---|---|---|---|
| Temperature, Salinity — `TS` | 0–5500 m / 102 levels | 0–5500 m / 102 levels | 0–1500 m / 57 levels |
| Oxygen and related — `Oxy` | 0–5500 m / 102 levels | 0–1500 m / 57 levels | 0–1500 m / 57 levels |
| Nitrate, Phosphate, Silicate — `Nutrients` | 0–5500 m / 102 levels | **0–800 m / 43 levels** | 0–800 m / 43 levels |

> `43 levels / 0–800 m` applies only to nitrate, phosphate, and silicate under
> seasonal or monthly climatology. It must not be applied to TS, Oxygen, annual
> nitrate, or every group in the store.

This is the entire basis of the depth pair: **the same variable on the same grid has
a 5500 m ceiling annually and an 800 m ceiling seasonally.**

**Every cell above is its own condition, and none of them is a store-wide rule.**
The pair this run requests uses exactly two of them:

| group | applicable Table 4 row | used for |
|---|---|---|
| `1_degree/seasonal/Nutrients` | **Seasonal Climatology / Nitrate: 43 levels, 0–800 m** | P4, and only P4 |
| `1_degree/annual/Nutrients` | **Annual Climatology / Nitrate: 102 levels, 0–5500 m** | P5, and only P5 |

The twelve-group existence graph (§6.3) therefore checks **existence,
API-reachability and openability only** and applies no depth condition to any group;
every entry carries `depth_checked: false`.

`bench/store_survey.WOA23_P12_DEPTH` holds all nine cells, so the shape of the rule
is visible rather than implied by the two the run uses. **P4 and P5 each compare
against one cell only** — the one for their own variable and climatology. A
combination the table does not cover (the quarter-degree grid, a variable not in it)
is **not judged at all**, because inventing an expectation for it would be exactly
the store-wide invariant Table 4 is not.

### 3.3 `source_time_span` is not a request parameter

The 1965–2022 span above is **dataset provenance**. It is not selectable through the
API and appears in no endpoint. What a user selects is the **climatology** —
`annual`, `seasonal`, `monthly` — carried by the `time_period` parameter.
`store_paths.group_path` builds `{store}/{grid}/{climatology}/{param_group}`; **no
source_time_span appears in any path**. Nothing in this plan may use it as a path
component or a request selector.

---

## 4. Three facts read out of the candidate that shape this plan

Stated as readings of the code, **not as predictions and not as expected values**.
Nothing below is written into any assertion.

**4.1 `time_period` is a numeric code.** `config.time_periods` is keyed `'0'`–`'16'`,
and `query.determine_subgroup` maps `'0'` to `annual`, `'1'`–`'12'` to `monthly`, and
**everything else (`'13'`–`'16'`) to `seasonal`**. A seasonal nitrate request is
therefore `time_period=13` (winter), not `time_period="seasonal"`.

**4.2 A depth selection outside a group's range does not raise.**
`ds.sel(depth=slice(3000, 4000))` on a group whose depth axis ends at 800 m yields an
**empty selection**, not an exception. `var in filtered_data` still holds, an empty
frame is still appended, and `result_list` is therefore non-empty — so the 404 branch
at `query.py:196` is not reached. **An out-of-range depth does not announce itself**,
which is why §5's preflight exists.

**4.3 JSON and CSV cannot be assumed to agree.** `app.get_woa23_csv` contains
`if df.is_empty(): raise HTTPException(400, ...)`; `app.get_woa23` has no such
branch. `C5d`, `C5e` and `C18` already record JSON 200 `[]` against CSV 400. This is
why §7's evidence schema is **per endpoint** and why no case in §6 carries a single
expected status.

---

## 5. P1–P6 — the real-group preflight, before any HTTP

**Placement: after clone integrity and after the staging symlink exists, and BEFORE
either arm is started.** It therefore precedes process readiness, precedes the store
probe, and precedes every HTTP request of any kind — not merely every data-path one.
Running it after readiness would mean the store probe had already exercised the read
path before anything had established what the groups contain.

Executed as a **sibling interpreter** launched the way the arms are launched
(production binary, `-S`, `PYTHONPATH` = the read-only clone, cwd = the arm's staging
directory), reading through the staging `data/` symlink with
`zarr.open_group(mode="r")` and the group's coordinates. **No HTTP. No service
running.**

| # | check | what its failure means |
|---|---|---|
| **P1** | `data//1_degree/seasonal/Nutrients` exists and opens as a Zarr group | **depth behaviour cannot be isolated on the real store** |
| **P2** | `nitrate` is in that group's `parameters` coordinate; **every coordinate `query.py` selects on** is present — `lon`, `lat`, `depth`, `parameters`, `time_periods`; and **`mn`**, the statistical-mean data field the current query path reads, exists (§5.2). Which of nitrate/phosphate/silicate are present is recorded as a store schema observation | the request would be skipped at `query.py:169` and end as 404, or `mn`'s absence would leave `result_list` empty and end as 404, or `ds.sel` would raise on a missing coordinate — **none of these is a depth outcome** |
| **P3** | **`time_period=13` specifically** is in that group's `time_periods` coordinate | skipped at `query.py:175`, same 404 — **not a depth outcome**. **No substitution**: if 13 is absent the run stops, whatever other seasons are present |
| **P4** | the `case_target_group`'s **full depth schema** and the expected row beside it — see §5.1 — then two separate questions: **(a)** is any level *selectable* by the API's own depth semantics in 3000–4000 m, and **(b)** does the measured axis match **seasonal nitrate's** Table 4 row? | **(a)** if a level is selectable there, the case is not out-of-range at all. **(b)** if the axis is not seasonal nitrate's, the result **may not be reported as a standard WOA23 seasonal-nitrate depth characterization** — recorded as `STORE_SCHEMA_MISMATCH` |
| **P5** | the `annual_control_group`: nitrate and `time_period=0` present, the same coordinate and `mn` coverage as P2, **at least one level selectable in 0–800 m**, and its schema matched against **annual nitrate's own row** (102 levels, 0–5500 m) | the supported case has no valid control. Seasonal nitrate's 43/0–800 is **not** applied here — an annual axis matching it would be a P5 **failure** |
| **P6** | `store_paths.group_path(store, "1_degree", "seasonal/Nutrients")` and the annual equivalent are **byte-identical** to the paths checked above, double slash included | the preflight checked a different location from the one the read path uses |

### 5.2 `mn` — a data field, not a coordinate

`mn` is the **statistical-mean field** of WOA23 Table 2 (p. 9), *available objectively
analyzed and statistical fields*, listed there as computable on the quarter-, one- and
five-degree grids.

> `mn` is the required statistical-mean data field used by the current API query
> path.

**It is not an oceanographic variable and it is not a coordinate.** `api.query` reads
it when a request does not name another field with `append`, which is why P2 and P5
check it — and that check is an **API request-path requirement, not a depth-schema
requirement**. Its presence or absence says nothing about the group's depth axis.

If `mn` is absent from a case group:

- the **current API query path cannot complete the request** — `result_list` stays
  empty and the response is a 404;
- the **D1 depth characterization's precondition is unmet**, and the run stops;
- **the 404 must not be recorded as a depth-out-of-range outcome**, which is exactly
  what it would look like from outside;
- and it is **not, by itself, evidence that the store violates WOA23**. That would
  need it separately established that this group was meant to carry `mn`.

### 5.1 What P4 and P5 record, and how the digest is formed

Per group, expected and actual side by side:

| | |
|---|---|
| **expected** | the Table 4 row for **that** variable and climatology: level count, min, max, and the citation. **The expected ordered level list is NOT transcribed from the PDF** — Table 3's standard depths are not copied here, because 102 numbers transcribed by hand is a comparison nobody could check without redoing it. The record says so (`levels_list_transcribed: false`) rather than leaving a reader to assume the list was checked |
| **actual** | level count, min, max, the **full ordered level list**, the coordinate **dtype**, the declared **units**, whether the axis is monotonically increasing, and a canonical digest |

**The digest's canonicalization, stated so it is reproducible:**

```
sha256("depth-levels/v1|dtype=<dtype>|units=<units>|n=<count>|"
       + ",".join(format(float(v), ".6f") for v in levels_in_stored_order))
```

Every clause answers a way two correct readers would otherwise disagree:

- **stored order, never sorted** — `ds.sel(depth=slice(...))` depends on the stored
  order, so sorting before hashing would erase the property the selection uses;
- **fixed-point, six decimals** — `repr()` of a float32-widened `0.1` and a float64
  `0.1` differ; `format(v, ".6f")` makes them the same, and six decimals is far below
  metre-scale depths and far above their precision;
- **dtype and units are inputs** — a metre axis and a decibar axis with identical
  numbers must not collide, and **neither must float32 and float64**. The four
  properties this gives, asserted as four in `bench/test_store_survey.py`: same
  dtype, units, order and values → **same** digest; a different dtype → **different**;
  different units → **different**; a different order → **different**. The fixed-point
  clause is about *values*: two numbers differing only in float `repr` **under one
  dtype** are one level and hash alike, which is not a statement about dtype;
- **the count is included** — a truncation cannot coincide with a reformatting;
- separators `|` and `,` appear in no field, and the string is UTF-8 encoded.

**What P4 verifies, stated no wider than it is:**

> P4 verifies the Table 4 count and extent, records the full observed depth list and
> digest, but does not yet assert equality with the complete Table 3 standard-depth
> list.

Table 4 gives the count and the range; the complete standard depths are **Table 3's**,
and they are not transcribed into a machine-comparable list here. So a matching count
and extent means the axis has **the right number of levels spanning the right
range** — **not** that those levels are the standard depths. Transcribing Table 3
would widen this, and would be its own piece of work with its own review.

**Question (a) uses the API's own depth semantics.** `api.query` calls
`ds.sel(depth=slice(dep0, dep1))`, which selects *levels*, so the check is "which
levels lie in [3000, 4000]" and **not** `max_depth < 3000`: an axis with a gap can
exceed the interval and hold nothing inside it, and the cruder test would call that
in-range.

**This is a precondition, not a prediction.** Establishing that no level is
selectable says nothing about whether the API answers 200, 400, 404 or an empty
body — that is measured by the characterization requests on VM24 and by nothing
else.

**If any of P1–P6 fails: stop before starting the arms. Record
`PRECONDITION_UNMET`, or `STORE_SCHEMA_MISMATCH` where P4 or P5 found an axis that is
not the one Table 4 describes for that variable and climatology.** `D1-depth-out-of-range` stays CHARACTERIZATION PENDING, the
result is recorded as *depth behaviour cannot be isolated on the real store*, and it
**must not be reported as depth characterization**. The production store is not
modified and no substitute data is created — neither is authorised, and neither would
answer the question asked.

**Observer scope, stated so it is not over-read.** P2, P3 and P4 read the
`parameters`, `time_periods` and `depth` **coordinate arrays**, and coordinate arrays
are stored as chunks — the measurement that made spec 004 revision 6 abandon
`xr.open_zarr(chunks=None)`. **This preflight therefore reads coordinate chunks.**
That is a different operation from the lifespan anchor validation and says nothing
for or against its zero-chunk property, which remains offline-audited exactly as in
§2.

---

## 6. Exact request matrix

`P = {"lon0": 135, "lat0": 15}` — the same reference point as the 64 contract cases.

### 6.1 Cases, per arm

| id | endpoint | parameters | basis |
|---|---|---|---|
| `D1-DEPTH-SUP` | `/api/woa23` | `{**P, grid:"1", parameter:"nitrate", time_period:"0", dep0:0, dep1:800}` | **annual nitrate**, judged against **annual** nitrate's row, 102 levels / 0–5500 m. Seasonal nitrate's 43 / 0–800 m does **not** apply to it |
| `D1-DEPTH-SUP-csv` | `/api/woa23/csv` | as above | recorded separately (§4.3) |
| `D1-DEPTH-OOR-tp13` | `/api/woa23` | `{**P, grid:"1", parameter:"nitrate", time_period:"13", dep0:3000, dep1:4000}` | **winter nitrate (`time_period=13`)**, judged against **seasonal** nitrate's row, 43 levels / 0–800 m; 3000–4000 m is inside WOA23's 5500 m global maximum and outside that row. **Winter only — not the four seasons** |
| `D1-DEPTH-OOR-tp13-csv` | `/api/woa23/csv` | as above | recorded separately |

`time_period=13` is **winter, and it is required rather than preferred**. If P3 finds
13 absent from the group's coordinate, the run stops with `PRECONDITION_UNMET` and
`D1-depth-out-of-range` stays CHARACTERIZATION PENDING — **it is not replaced by
whichever season happens to be present.** Substituting silently would make the run a
different experiment while it was still being reported as the `time_period=13` case.

Requesting 14, 15 or 16 is a legitimate thing to want and is **a different case**:
`bench/d1_cases.depth_cases("14")` yields `D1-DEPTH-OOR-tp14` and
`D1-DEPTH-OOR-tp14-csv`, records the request as issued, and needs its own
authorisation. No `source_time_span` appears in any parameter (§3.3).

### 6.2 Anchor recovery — one probe per case, four per arm

| id | endpoint | parameters | when |
|---|---|---|---|
| `D1-ANCHOR-RECOVER` | `/api/woa23` | `{**P}` — default temperature / annual / TS | **immediately after each of the four cases above, before the next case begins** |

**Four cases, therefore four recovery probes per arm.** Placing one after each case
is what makes "the failure was request-level" attributable to a specific case;
batching them at the end would only show that the process survived all four
together. The reviewed draft said two, which contradicted its own per-case rule; four
is the number.

### 6.4 One worker per arm — a design choice, not a measurement

> **D1 uses one worker per arm by design. This is not a production-worker-count
> validation and makes no claim about multi-worker D1 behavior.**

D1 launches `-w 1` for the reason C1 does: the candidate and the reference are
compared byte for byte, and one process per side keeps the comparison about the code
rather than about which worker answered.

**C2 is the mode that measures production's worker count and runs the arms at it.**
It reads the number from production's own argv at run time and asserts it. D1 reads
nothing from production and asserts nothing about it. `workers: 1` reads the same in
both records, so the difference is recorded rather than inferred:

| recorded per run, in `results/<label>_workers.json` | |
|---|---|
| `arm_workers` | `1` |
| `source` | `fixed at 1 by the D1 mode` |
| `derived_from_production_measurement` | **`false`** |
| `note` | the sentence above, verbatim |
| `contrast` | that C2 measures and runs at it, and D1 does neither |
| `per_arm.<arm>.worker_count` | parsed from that arm's own launch argv |
| `per_arm.<arm>.launch_argv` | the arm's **full** argv, as `/proc/<pid>/cmdline` gave it |
| `per_arm.<arm>.launch_command` | the same, joined, for reading |

**What sets the count, and what merely checks it, are different things:**

| | |
|---|---|
| **sets it** | the **D1 mode** — the `-w 1` on each arm's launch line |
| **asserts it** | `--expect-worker-count 1`, which reads each arm's own `/proc/<pid>/cmdline` and **refuses to write provenance** for an arm that disagrees. **It sets nothing** |

A run whose arms do not carry the count it recorded is not the run that was
authorised — which is what the assertion is for. This is the same distinction
`--expected-workers` already carries for C2: confusing a setting with an assertion is
how a harness picks a number and then reports it as production's.

### 6.3 Non-anchor missing group — how one is looked for

`query.py` can reach **twelve** group paths. `query.py:115` restricts the
quarter-degree grid to temperature and salinity, so six of the eighteen combinations
are refused with a 400 before any group is opened:

```
1_degree/{annual,seasonal,monthly}/{TS,Oxy,Nutrients}      9
025_degree/{annual,seasonal,monthly}/TS                    3
```

Each of the twelve is checked for existence **read-only, without HTTP**, in the same
preflight as P1–P6. A usable real case must satisfy all four of:

1. the API criteria are legal — it passes the three 400 checks at `query.py:110`,
   `:120` and `:126`;
2. upstream WOA23 availability permits the combination (§3.1);
3. the group is genuinely **absent** from the real store;
4. the failure would occur at the **group open** — `xr.open_zarr` raising
   `FileNotFoundError`, surfacing through `app.py`'s `except Exception` — and **not**
   at `query.py`'s 400 validation.

**If all twelve exist**, which is the likely outcome for a store built for this API:

- the synthetic D1-D3 evidence is **retained** and remains the only evidence;
- the result is recorded as **real-store characterization unavailable**, together
  with the positive fact that all twelve reachable groups are present;
- **no group is guessed at, and none is created or removed.** The production store is
  not modified.

An HTTP case for a missing group is only added if a real one is found. **This plan
does not authorise inventing one.**

---

## 7. Evidence schema — JSON and CSV recorded separately

Per case, per arm. **No expected status or body is written anywhere in advance, and
JSON and CSV are never assumed to agree** (§4.3).

| field | note |
|---|---|
| `case_id`, `arm`, `endpoint`, `params` | as issued. **The id names the period**: `D1-DEPTH-OOR-tp13`, never a bare `D1-DEPTH-OOR` whose meaning depends on a convention |
| `variable`, `climatology`, `season`, `time_period`, `group`, `scope` | carried per record, so a reader never infers the scope from an id. `season` is `winter`; `scope` spells out "winter nitrate (time_period=13)" and the Table 4 row it was judged against |
| `request_order` | `RC` or `CR` — see §8.2 |
| `http_status` | as returned |
| `content_type` | header, verbatim |
| `content_disposition` | present on the CSV path; recorded verbatim or null |
| `body_bytes` | length in bytes |
| `body_sha256` | over the raw bytes |
| `body_head` | first 512 raw bytes, unmodified |
| `error_text` | the response's `detail` when present, verbatim; otherwise null |
| `row_count` | only when the body parses as a JSON array; otherwise null |
| `request_level` | did the anchor recovery probe that followed **this** case return 200 |
| `process_alive` | every PID in that arm's recorded tree, checked after the case |
| `depth_isolated` | true only if P1–P6 all passed; false makes the record a non-depth observation |

The store's own state is recorded once per run: the group paths checked, their
coordinate ranges as measured in P1–P6, and the existence map of the twelve reachable
groups.

---

## 8. Request budget

### 8.1 Per arm

| stage | ceiling | countable? |
|---|---|---|
| import-time and lifespan validation | **0 HTTP** | not applicable — it happens before any socket is served |
| P1–P6 preflight | **0 HTTP** | not applicable — it runs before the arms start |
| process readiness (OpenAPI) | ≤ 30 | **no.** `process_ready` does not count its attempts — a known harness gap (§10, F2) |
| store-readiness probe (data path) | **2** | yes, fixed by construction |
| characterization cases | **4** | yes |
| anchor recovery probes | **4** | yes |
| **characterization subtotal** | **8 per arm** | the four cases and their four recovery probes — `characterization_requests_per_arm()`. **Not the per-arm total** |
| **countable total** | **10 per arm** | the subtotal above **plus the two store probes** — `countable_requests_per_arm()`. This is the figure a report uses |

### 8.2 Both arms

**20 countable requests. Process readiness adds an uncounted 1–30 per arm, so the
run's total is between 22 and 80.** The ceiling is 80.

**Superseded by measurement as of revision 4.** F2 is implemented, so the run counts
what it actually issues — attempts and not successes, readiness included — and writes
it to `results/<label>_requests.json`. **The report gives the measured number.** The
figures above remain the *ceilings*, and 22-80 remains what the total may be, not
what it was.

Until F2 landed this had to be reported as a range, because `process_ready` swallowed
its failures with `|| true` and a run that took nineteen attempts to become ready was
indistinguishable from one that took one.

`bench/d1_cases.request_total_range()` returns it as a **tuple**, not a number, so a
caller cannot turn the range into a total by accident. The runner derives the same
two figures from its constants and prints both, rather than printing a literal that
could drift from them.

Each case is issued once per arm in **counterbalanced order** (`RC`, then `CR`), so
any ordering effect falls symmetrically on both arms.

**No cache-busting is used, and none is needed.** Nothing here measures time, so page
cache state cannot affect the recorded bytes; `C19a`/`C19b` already established that
the ignored `_cb` parameter leaves the body unchanged, and the application has no
caching middleware.

### 8.3 The three boundaries, kept apart

| boundary | what it establishes |
|---|---|
| **startup validation** | import-time existence and type, plus the lifespan anchor open. **Zero HTTP.** Failure means the worker exits and never becomes ready |
| **process readiness** | a 200 on the OpenAPI document. **The process is serving.** By this point the candidate has read the anchor group's metadata and the reference has validated nothing — the arms are asymmetric in what they have read, which cannot change any byte either returns |
| **store / data requests** | the probe and the characterization cases. **Only this layer establishes that data can be served** |

---

## 9. Execution order

**Every parameter in this document is a proposal. The runner checks the world.**
Before anything starts it re-confirms, at run time and from the host rather than
from this text:

- the **label** owns nothing yet — no `results/<label>_*` and no `run/<label>/`
  (§10, `INVALID_PRE_START`);
- the **staging directory and workdir** do not already exist, so nothing is written
  over;
- the three **ports are actually free**, by a read-only `ss` — the ledger says only
  that this campaign has not bound them before, which is a different question;
- the **clone manifest** is the four-column file, checked by shape at the moment the
  argument is read;
- the **shutdown budget** holds for the values actually in force.

A disagreement between any of these and this document stops the run. The document
does not get the last word on the state of the host.


Corrected so that P1–P6 precedes every HTTP request, resolving the contradiction in
the reviewed draft between "before any HTTP" and a placement after readiness.

1. label-collision guard; port ledger check (first use) and read-only `ss` free check
2. `assert_shutdown_budget` — `STOP_WAIT_SECS` must exceed `--graceful-timeout`;
   both values and the source of `STOP_WAIT_SECS` recorded
3. clone integrity — **preflight**: ancestor chain, mode bits, full manifest
4. staging built; `data/` symlink created for both arms
5. **P1–P6 real-group preflight, plus the twelve-group existence map — read-only, no
   HTTP, no service running.** `PRECONDITION_UNMET` here stops the run **before any
   arm is started**
6. clone integrity — **before-reference**, start the reference; clone integrity —
   **before-candidate**, start the candidate
   · **the startup-validation window opens here**: from the moment an arm is
   started until its first HTTP request, the candidate's import-time and lifespan
   checks run. A failure inside this window is classified by §10.1 and is **never**
   a characterization result
7. process readiness (OpenAPI only — not the data path)
8. store-readiness probe, 2 per arm, counterbalanced
9. the four characterization cases, each followed immediately by its anchor recovery
   probe
10. cleanup — every tree drained and verified, fail-closed

---

## 10. Failure classification and stop conditions

| classification | trigger | disposition |
|---|---|---|
| `INVALID_PRE_START` | clone integrity failure, label or port collision, shutdown-budget assertion failure, unreadable or wrong-shaped manifest — **or** an arm that never becomes ready for a reason that is *not* store validation (§10.1) | nothing was characterized; evidence kept; re-request |
| `PRECONDITION_UNMET` | any of P1–P6 fails — including `time_period=13` being absent from the target group | **not a FAIL.** Recorded as *depth behaviour cannot be isolated on the real store*; PENDING stands; arms are never started |
| `STORE_SCHEMA_MISMATCH` | P4 or P5 measured a depth axis that is not the Table 4 row for **that** variable and climatology | **not a FAIL**, and a stricter statement than `PRECONDITION_UNMET`: the arms are never started, and any result from that group **may not be reported as a standard WOA23 depth characterization for it**. The store is not modified |
| `STARTUP_VALIDATION_FAILURE` | an arm exits during import or lifespan because the store failed the candidate's own validation, after the arms are started and **before any HTTP** (§10.1) | **not a D1 characterization FAIL.** The characterization never began. Evidence kept; the run is re-requested after the store configuration is understood |
| `DIVERGENCE` | candidate and reference return different bytes for any case | **FAIL, stop immediately**, all evidence preserved |
| `REQUEST_LEVEL_VIOLATION` | an anchor recovery probe is not 200, or any PID in an arm's tree has died | **FAIL** — the failure was not request-level, which is the property under test |
| `CLEANUP_FAIL` | any recorded tree does not drain inside the stop window | **whole run FAIL**, state preserved, **no SIGKILL**, **no self-rerun** |
| `CHARACTERIZATION_RECORDED` | none of the above | the success state. **Characterization itself has no pass/fail**: no status or body is judged right or wrong |

### 10.1 The startup-validation window, and why it is not a characterization result

There is a window between step 6 and step 7 of §9 — the arms are running, and no
HTTP request has been sent. **D1's implemented part executes entirely inside it**:
`api.config` checks existence and type at import, and `api.app`'s lifespan opens the
anchor group. Either can raise, and the worker then exits without ever serving.

**A failure there says something about the store or the staging, and nothing about
depth or about missing non-anchor groups.** The characterization cases were never
issued. Recording it as a D1 characterization FAIL would report a result that was
never measured, and would attribute a configuration problem to the API's behaviour.

**It is also not a defect on its face.** Refusing to become ready on an unusable
store is exactly what the D1 patch was written to do; if it fires, the correct
reading is that the validation worked and the *store or the staging* is wrong.

| what happened | classification |
|---|---|
| an arm's log carries the candidate's own store-validation error — `WOA23_ZARR_STORE does not exist`, `... is not a directory`, or `the WOA23 store's required anchor group could not be opened` — and the arm exited | **`STARTUP_VALIDATION_FAILURE`** |
| an arm never became ready with no such error in its log: the port never bound, the worker crashed for an unrelated reason, readiness timed out | **`INVALID_PRE_START`** |
| an arm never became ready and its log is missing, truncated or unreadable, so the two cannot be told apart | **`INVALID_PRE_START`**, with *classification indeterminate* recorded explicitly. Fail closed: the weaker claim is the one that may be made |

**Under either classification: `D1-depth-supported` and `D1-depth-out-of-range` stay
exactly as they were, `D1-depth-out-of-range` stays CHARACTERIZATION PENDING, spec 004
is not updated, and the run is re-requested rather than re-run in place.**

**The asymmetry is expected and is not a divergence.** The reference is unmodified
`woa23_app.py` and validates nothing at startup, so a store problem makes the
candidate fail here while the reference starts normally. That is the D1 patch
behaving as designed — it is **not** a candidate-versus-reference divergence and must
not be recorded as one. `DIVERGENCE` applies only to response bytes from cases that
both arms actually served.

---

## 11. What may update spec 004, and what may only be a finding

| may update spec 004 | may only be a characterization finding |
|---|---|
| `D1-depth-supported` — if it returns data, it may be marked **verified against the real store** | the concrete status and body for seasonal nitrate at 3000–4000 m. It is a measurement, not a contract being asserted |
| `D1-depth-out-of-range` — **only if P1–P6 all pass**, it may move from CHARACTERIZATION PENDING to characterized, carrying the JSON and CSV values separately | any difference between JSON and CSV. Whether they *should* agree is a contract question for S2b or spec 003 |
| the existence map of the twelve reachable groups, as a positive fact | a missing non-anchor group, if none is found: **real-store characterization unavailable** |

**May not be updated by this run under any outcome:** the zero-chunk evidence level
(unchanged, §2), deployment and PM2, the row-order decision, anything about
performance, and whether D1 as a whole is closed.

---

## 12. Authorization boundaries

**Each of the following is a separate authorisation. None implies another.**

1. **A harness commit** — the changes in §13, all of them outside `api/`, with their
   offline tests, clean-archive verification and the full offline suite.
2. **One VM24 run** — new staging, workdir, label and first-use ports, proposed with
   the request and verified against the ledger and against the host before starting.
3. **A documentation commit** — recording whatever the run establishes.

Kept apart in the record, as C1 (`919095e8`), C2 (`249aa274`) and the documentation
commit (`9749355c`) already are.

### 12.1 Not touched by this run

**This is not a production-worker-count validation.** One worker per arm is fixed
by the mode; nothing here measures production's worker count or runs at it, and no
claim is made about multi-worker D1 behaviour (section 6.4).

Production API `8050`; Dask `8786` and `8787`; PM2, systemd, nginx and TLS; any
latency, throughput or resource measurement of any kind; the production store, which
is read through the staging symlink and never written.

**On the symlink, stated precisely:** it provides a *read path*. It does not itself
make anything read-only. The basis for "the production store was not written" is that
**the runner performs no write operation against it**; an unchanged mtime corroborates
that and can only be reported as *no modification observed*.

### 12.2 Isolation reused, not reinvented

The read-only package clone is reused, with its **full manifest verification, ancestor
chain inspection and mode-bit check performed three times in the run** — preflight,
before the reference starts, and before the candidate starts. As in C1 and C2, this
is **detection with a bounded window, not immutability**: `/home/odbadmin` is writable
by the account, and the window between a verification and a worker opening a file is
narrowed, not closed.

---

## 13. Code changes this would need — listed, not implemented

| # | change | needed? | scope |
|---|---|---|---|
| F1 | a `--d1` runner mode | **yes** | harness. The characterization cases are not among the 64, and the per-case recovery probe and evidence fields are new |
| F2 | a request counter in `process_ready` | **done** — separate harness commit | harness. `lib_requests.sh` counts attempts per arm and per stage; `lib_http.sh` holds the two request loops so they can be tested. Section 8's total is now measured rather than bounded |
| F3 | a separate case list, `bench/d1_cases.py` | **yes** | harness. **These cases must never be added to `CASES`** — changing the 64 would break comparability with C1 and C2 |
| F4 | the real-group preflight tool implementing P1–P6 and the twelve-group map | **yes** | harness, read-only. Must record that it reads coordinate chunks (§5) |
| F5 | the evidence schema of §7, with its offline tests | **yes** | harness |
| F6 | spec, test and documentation updates | **yes** | spec 004 revision 11, and the non-performance sections of ROADMAP and BASELINE |

**F1–F6 are entirely harness and documentation. `api/` is not modified by any of
them, so C1 and C2 do not need re-running.**

**If anything here later turns out to require a candidate change** — for example
making an out-of-range depth return an explicit error, or reconciling the JSON and
CSV empty-result behaviour — **that is a contract change. It needs its own
implementation plan and it requires C1 and C2 to be re-run.** It must not be folded
into this characterization, whose entire purpose is to observe the current behaviour
rather than to alter it.

---

## 15. Execution record — `d1a`, 2026-08-11

**Classification: `INVALID_POST_MEASUREMENT_HARNESS`.**

> characterization observations recorded, but required post-run artefacts were not
> finalized.

**This is not a D1 PASS and D1 is not complete.** It is also not a characterization
failure: every case was issued, every response recorded, both arms agreed byte for
byte, every recovery probe returned 200, and cleanup passed. What failed came after
the last request.

### 15.1 What failed

`run_controlled.sh` finalised the run through a heredoc that read
`os.environ["LABEL"]`; the shell variable was never exported. The run died there,
after writing `d1a_d1.json`:

- **`d1a_workers.json` — zero bytes**;
- **`d1a_requests.json` — never written**;
- runner exit **1**, with no statement of what had failed.

**A harness defect, not a candidate finding.** Fixed offline in a separate commit:
the label is passed as an argument, the finalisation is a sourced function that
`scripts/test_d1_finalize.sh` executes for real, and a finalisation failure is now
**classified** — `INVALID_POST_MEASUREMENT_HARNESS`, written to a file, with a
distinct exit status — instead of being a bare non-zero exit.

### 15.2 What the run did establish, and how it may be used

The four observations may be cited **as observations**. They may not be cited as a
completed D1 result, and D1's status in spec 004 is unchanged until the harness fix
is reviewed and a decision is taken on whether to re-run.

- **P1–P6: `PRECONDITIONS_MET`**, before any service or HTTP.
  **P4 Table 4 count/extent compatibility verified; full Table 3 level-list equality
  not verified.**
- **Twelve reachable groups: all present**, so the non-anchor missing-group
  characterization is **unavailable on the real store** and the synthetic D1-D3
  evidence stands as the only evidence for it.
- The four case observations, recorded in `results/d1a_d1.json`; the JSON/CSV
  divergence they show is carried into **spec 006** as *observed current behaviour*,
  not as a finding this run resolved.
- **Cleanup PASS**, production untouched, ports free afterwards.

### 15.3 Not done

The result is **not** written into spec 004, ROADMAP or BASELINE as a D1 outcome,
and no artefact was edited. No re-run has been performed or proposed.

## 16. Execution record — `d1b`, 2026-08-11

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

#### The observations

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

#### Preconditions — P1–P6, before any service or HTTP

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

#### What was observed about the store but NOT characterized

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

#### Traffic, isolation and cleanup

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

#### What this result is

For the two queries it issued, against the real store: the API's current behaviour,
measured, with the candidate and the reference in agreement and the observations
reproduced across two runs.

#### What this result is **not**

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

#### What is still open in D1

Real-store missing-group behaviour (unavailable while every group exists), every
variable and climatology outside the two characterized, and deployment validation
under PM2 with `site.py` enabled.

### Follow-up noted, not a D1 item

The JSON/CSV divergence this run observed prompted a small API contract decision,
recorded in **`specs/006-json-csv-empty-result-consistency.md`**: for a valid query
with no matching data, both formats should return **200**, with CSV carrying the
same query's **header-only** schema (option B, adopted). **Not implemented.**

**It is not a D1 blocker and not a track of its own.** D1 measured behaviour; the
memo decides what the behaviour should become, and nothing in D1 waits on it.

---

## 14. This round's limits

No file under `api/` was modified. Nothing was committed, nothing was pushed, no VM24
process was started, no HTTP request was sent, and no measurement of any kind was
taken. This document is a plan; executing it needs the authorisations in §12.
