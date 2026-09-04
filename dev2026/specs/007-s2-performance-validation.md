# 007 — S2 single-worker steady-state request-path performance validation

**Status: rung 21 EXECUTED once — `s2pB`, 2026-08-19: S2 rung-21 PASS under the approved
0.05 engineering threshold (§12). Everything else in this document remains plan only.**

One authorised execution has taken place: the rung-21 latency gate, run `s2pB`. Its
result is §12 and its full record is spec 002. The margin it was decided against,
`0.05`, is an **approved engineering threshold** (PI, 2026-08-19 — §5.2). **No other part
of this document has been run** — no rung above 21, no startup measurement, no
throughput, no multi-worker, no cold-cache — and **rung 60 is not entered automatically**:
approving the margin authorises no escalation. **No change to `api/` is made or
proposed**, and `api/` was not modified by the run or by the approval. Executing any
further part needs its own authorisation (§10).

| rev | date | change |
|---|---|---|
| 14 | 2026-08-19 | **`IMPROVED` is a two-case gate, not an eight-case result** — §1, §5.2, §12. The `s2pB` record read `NO_REGRESSION / IMPROVED` for all eight cases. `IMPROVED` gates only the **improvement-required** cases `readme_example` and `point_profile_multiparam` (`bench/paired_bench.py`: `IMPROVEMENT_REQUIRED`), which are the only ones `FAIL_NO_ESTABLISHED_IMPROVEMENT` consults; both passed. The other six have intervals below 1.0, which is an **observation about their ratios**, not a gate they passed, and is no longer written as one. **No measured number changes and no rerun is required** — the artefacts already carried `improvement_required` per case; the specs were reading it as though every case were gated. §5.2 now says which cases the improvement verdict gates, and the §1 D2b sentence is made precise the same way. |
| 13 | 2026-08-19 | **The 0.05 margin is APPROVED** — §5.2, §10, §12. The PI has formally approved `margin = 0.05` as an **engineering decision threshold**, applied exactly as already defined: `NO_REGRESSION` iff `ci_high <= 1.05`, `REGRESSION` iff `ci_low > 1.05`, `INCONCLUSIVE` otherwise, and `IMPROVED` (for improvement-required cases) iff `ci_high < 1.0`. It was declared and used in these terms for `s2pB`, so **no rerun is required** and that run's verdicts stand unchanged. What the approval does **not** change: it is still **not** a statistical property of the data, **not** a confidence guarantee, and **not** a production SLA — and approving the threshold authorises **no escalation**, so rung 60 remains unauthorised. Documentation only: `api/`, `bench/` and `scripts/*.sh` are untouched, and the runner's own banner still reads "PROPOSED ENGINEERING THRESHOLD, pending approval" (§12a). |
| 12 | 2026-08-19 | **Rung 21 executed once — `s2pB`, gate PASS** — §12. First and only execution of any part of this document. 467 requests per arm / 934 total against the authorised 496 / 992, count **exact** (`counts_exact`, `host_attempt_count_exact`, `measurement_complete` all true; `failed_stages` and `problems` empty; every stage's evidence `stage_artefact`), and the pre-request journal agrees with the stage artefacts record-for-record with 0 truncated and 0 unattributed. Contract 64/64 byte-exact; latency gate PASS on all eight cases, every ratio and its whole interval below 1 (0.062–0.776). Production 8050 received **0 requests**; 8786 and 8787 were never contacted; cleanup was clean with no retained state and no `SIGKILL`. The margin 0.05 remains a **proposed engineering threshold, pending approval**. |
| 11 | 2026-08-13 | **An inexact count is never a ceiling breach, and a non-zero exit is not a hard kill** — §5.4.6, §5.4.7. The ceiling was applied before exactness was consulted, so a journal count that bounds nothing could have produced `CEILING_EXCEEDED`. **The ceiling is now only applied to an EXACT attempt count**; an inexact run records its journaled attempts, `host_attempt_count_exact: false` and the authorised ceiling, and classifies `INCOMPLETE_STAGE_FAILURE` — no breach and no host-traffic claim. Separately, `--stage-failed` was being read as "killed": `pilot_counts` skipped a valid partial artefact and every failed stage was labelled `journal_after_abrupt_termination`, so an HTTP 500 that wrote a proper partial artefact was reported as a hard kill. **The artefact is now authoritative** — it exists only if a process was alive to write it, and it carries its own `attempt_evidence`. And **measurement completeness is separated from attempt exactness**: `measurement_complete`, `attempt_count_exact`/`host_attempt_count_exact`, `attempt_evidence` and the failure classification are four answers, not one flag. A caught failure is now `measurement_complete: false` with an EXACT count that IS ceiling-checked; only a hard kill is inexact. The clean-archive verifier is fixed too: pristine identity is computed before anything compiles, imports or runs in the tree, the project interpreter is named rather than inherited from PATH, bytecode is written outside the export, and the tree is re-checked against its own identity afterwards. |
| 10 | 2026-08-13 | **A pre-request journal is not a floor under abrupt termination** — §5.4.6, §5.4.7. The line is flushed BEFORE the call, so a process killed in between leaves a record for a request that was never sent (one too high), and a truncated final record cannot be attributed to an arm and is not counted (one too low). The count may therefore not be called a floor, "at least N", "requests actually issued" or observed host traffic. It is `journaled_attempts` with `host_attempt_count_exact: false`, reported beside the authorised ceiling; a real bound would need a derivation over the request loop, the flush point and the truncated-record case, and none is claimed. **`attempt_evidence` separates the two strengths**: a stage that caught its own failure closed its own journal and its attempt count IS exact, which is not the same evidence as an asynchronous kill. Also: `<label>_perf_classification.json` now carries `failed_stages`, so the run-level classification names the stage without deferring to another file, and `s2perf_finish`’s precedence comment includes `REQUEST_ACCOUNTING_INCONSISTENT` (9) to match the implementation. |
| 9 | 2026-08-13 | **The failed-stage information now reaches every consumer, and the run-level classification keeps the cause** — §5.4.6, §5.4.6a, §5.4.7. `--stage-failed` reached only the final artefact, so a **hard-killed pilot** was recorded as 0 by the shell counter, reconciled 0 against a freshly computed 0, and reported as no traffic while its journal held the attempts it had issued. `s2perf_stage_record` computes the record **once** and the counter, the reconciliation and the artefact all read that one computation. The run-level classification becomes neutral `INCOMPLETE_STAGE_FAILURE` with `stage_failure_classification` read back from the stage artefacts, so an HTTP 500 is no longer reported at run level as a transport failure. **The noise pilot now stops after a failing arm** (§5.4.6a): the noise floor is a property of the pair, so a candidate-only figure plans nothing and 208 more requests would buy nothing. Once-only markers **fail closed** when they cannot be written. |
| 8 | 2026-08-13 | **The rev 7 account of the duplicate-owner defect was wrong, and two failure modes were conflated** — §5.4.1, §5.4.6, §5.4.7. A duplicate count owner **issues no HTTP**; it double-records requests one gate already made. The formal path issues **496 per arm / 992 total**; a double-recorded contract stage would have made the COUNTER read 560 / 1,120, and the 528 rev 7 quoted was the offline driver figure, which contains no readiness and no store probe. **Rev 7’s "1,056 requests issued" is withdrawn.** `REQUEST_ACCOUNTING_INCONSISTENT` (exit 9) is now separate from `CEILING_EXCEEDED` (exit 8): the first is about bookkeeping and makes no claim about the host, the second is reserved for attempts the evidence demonstrates. `s2perf_reconcile` checks every counter against its evidence before the ceiling is applied, and `record_contract_count` refuses a second call. Separately, the pilot’s unexpected-status path raised `SystemExit` and so skipped the partial artefact, the journal close and `complete: false` — it is an ordinary exception now, classified `STAGE_ABORTED_UNEXPECTED_STATUS`, and the runner passes `--stage-failed` so the record and the report cannot disagree. |
| 7 | 2026-08-11 | **[SUPERSEDED BY REV 8 — the figures in this row are wrong. A duplicate count owner issues no HTTP; the run would not have issued 1,056 requests. Kept as written because a revision history is a record, not a draft.]** ~~The real runner path was 528 per arm, not 496~~ — §5.4.1, §5.4.3a, §5.4.7, §9.1. The contract stage had **two count owners**: `run_controlled.sh` recorded it before the gate and `s2perf_record_counts` recorded it again, so a successful s2perf run would have issued 1,056 requests under a 992 authorisation. The driver test missed it because it called the library directly and never passed through the runner’s own accounting. Fixed with **one owner** (`record_contract_count`), a **hard ceiling assertion** that fails any run over its authorised total and gives it **no quotable result** (`CEILING_EXCEEDED`, exit 8, ahead of every other status), and a driver regression that accumulates the real chain and asserts **496/992 stage by stage**. Also: the derived contract count **may not stand in for measured attempts when the gate did not complete** (§5.4.3a); the shared finaliser takes a `--mode`, so an S2 run no longer writes D1-specific worker provenance; and the noise pilot’s **non-zero exit is carried into the tail** rather than dropped, because an absent pilot artefact is legitimate at rung 150 and only the status tells "did not run" from "ran and died". |
| 6 | 2026-08-11 | **Two verification gaps closed before any authorisation is proposed** — §5.4.6, §5.4.7, §9. The stage chain moved into `scripts/lib_s2perf.sh` as sourced functions and **`scripts/test_s2perf_driver.sh` executes it**, so the order, the stop-on-failure clauses and the exit statuses are now tested at driver level rather than read out of a script that cannot run off VM24. That test found two defects: a `REGRESSION` verdict **never reached finalisation** because `set -e` ended the run at the gate, and a `set -e` inside the library turned errexit on for a caller that had it off. Separately, **a stage killed by a transport failure now keeps its evidence**: every attempt is journalled before it is issued, the stage writes a partial artefact with no invented verdict, and `counts_exact: false` **forbids reporting an exact request total** — observed count plus ceiling only, and the run is not a latency result. Three exit statuses are named and ordered: 1 a verdict, 6 artefacts not finalised, 7 stages not finished. The noise pilot is skipped when the gate did not finish. |
| 5 | 2026-08-11 | **The rung-21 ceiling is corrected to 496 per arm / 992 total** — §5.4.1, §5.4.2, §5.4.3, §5.4.4. The noise pilot samples **every case**, so its cost is `8 × 26 = 208` per arm, not 26; revisions 3 and 4 carried the per-case figure in the budget table while the prose beside it already said 208. **An authorisation granted against 314 would have been exceeded by the run it authorised.** The split-execution alternative moves with it: the latency-only run becomes 432 per arm / 864 total. Found by the offline integration test (`bench/test_s2perf_integration.py`) before any request was issued; the runner now records **measured** stage counts from each stage's own artefact (`bench/perf_counts.py`) instead of the budget constants, so a stage that issues more or fewer than planned is reported as what it issued. |
| 4 | 2026-08-11 | **Four corrections before the harness commit** — §5.4.5, §5.2, §2.1, §10. The startup budget is **expanded arithmetically**: 32 per arm per cycle, 64 per cycle, **192 over three cycles and two arms**, containing no characterization, contract or latency request. The statistic is restated **per case**: eight cases, eight ratios, eight bootstraps, eight verdicts — **samples are never pooled across cases into one ratio**. The startup protocol records **launch order per cycle** and counterbalances it where three cycles allow; **any cycle whose cleanup fails makes the whole startup execution `CLEANUP_FAIL` / `INCOMPLETE`**, and partial timings may not produce min/median/max. `margin = 0.05` must be **restated in the execution authorisation itself** as a proposed engineering threshold, not a statistical property and not an SLA. |
| 3 | 2026-08-11 | **One authorisable ceiling, named warm-ups, an exact bootstrap statement, and the startup protocol** — §5.4, §4.1, §5.2, §2.1. **314 per arm / 628 total** is the single formal ceiling, for a run that carries the contract gate; the split alternative is spelled out as a *different execution identity with its own label, ports and budget*. **The startup measurement becomes its own execution** with its own identity and budget, so it does not sit inside the latency run's number. The two warm-ups are **named and separated** — `symmetric_warmup_pass` and `per_case_leading_sample` — and neither enters any statistic. **The noise pilot's trigger, count and rung dependence are fixed.** §5.2 now states that the interval is a **percentile interval of the raw bootstrap ratios**, that the point estimate comes from the original samples so `r` is not multiplied in twice, and that **nominal 95% coverage is not claimed** without an i.i.d. or autocorrelation argument. Startup gains its clock, endpoints, error classification and per-cycle cleanup evidence. Decisions recorded: the eight D2b cases are retained, production worker count moves to S4, and margin 0.05 stays **pending approval**. |
| 2 | 2026-08-11 | **Four additions before any harness work** — §1a, §5.2, §5.4, §2.1. The **decision equations are written out** rather than cited, including the gate's precedence order and the escalation estimator. The **D2b figures are relabelled a historical protocol/reference result**: §6.1a compares the two runs condition by condition and shows the interpreter and package environment are known to differ, so they are not this campaign's baseline. The **rung-21 budget is itemised as arithmetic** per stage, with the scope of "8 cases × two orders" stated and **bootstrap resampling excluded from the request count** — it issues no HTTP. The **startup measurement is decided: three independent start/stop cycles, reported as a descriptive distribution and entering no gate.** Scope renamed *single-worker steady-state request-path performance validation*. |
| 1 | 2026-08-11 | Initial plan, built from the D2b latency gate that already ran (BASELINE, spec 002), the sampling and statistics in `bench/paired_stats.py`, the ladder in spec 001, and the startup asymmetry recorded in spec 004 §48–51. Every parameter below is cited to where it already exists rather than restated from memory. |

---

## 1a. Scope, in its own words

> **Single-worker steady-state request-path performance validation.**

Every word is doing work. **Single-worker**: one gunicorn worker per arm, by choice
(§6.2). **Steady-state**: after warm-up, excluding the startup cost, which is
measured separately and gates nothing (§2.1). **Request-path**: the latency of
individual requests over loopback, not the behaviour of a system under load.
**Validation**: against a pre-specified margin, not a search for a number.

**It is not, and no result from it may be quoted as:**

- a **production SLA** or any statement about what users experience;
- **throughput** or capacity;
- **multi-worker** behaviour — production runs two, this runs one;
- **cold-cache** behaviour — S5;
- **PM2, TLS, nginx or deployment** validation — the launcher is this harness and
  `site.py` never runs;
- a **production API** measurement — production receives no request.

## 1. What is being asked

S1 removed Dask from the read path. **That change has been measured once**, under
D2b, and the result may be quoted: rung 21, eight cases, every one `NO_REGRESSION`,
every interval wholly below 1.0, and the two **improvement-required** cases
(`readme_example`, `point_profile_multiparam`) `IMPROVED` (BASELINE, *Latency gate —
PASS, 8/8*). The other six were not gated on improvement; their intervals are an
observation, not an `IMPROVED` gate result.

**What has never been measured is the same change in the S2 environment** —
production's own interpreter against the read-only package clone, which is what C1
and C2 established correctness for. Every S2 result says so in terms: *"this
invocation produced no timing of any kind and nothing may be quoted from it as
performance"*, printed by the runner itself for `--c1` and `--c2-cycle`.

So the question this spec plans is narrow, and its name says how narrow —
**single-worker steady-state request-path performance validation** (§1a):

> **Does the S1 read-path change hold its latency behaviour when both arms run on
> production's interpreter and package clone, rather than on `dev2026/.venv`?**

It is **not** "is the candidate fast enough", and it is not a throughput,
concurrency or resource study. Those are S4 and S5.

## 2. Two measurements that must not be pooled

### 2.1 Startup validation and anchor metadata read

Under the patched candidate, `api.config` validates the store at **import** and
`api.app`'s lifespan opens the anchor group's Zarr metadata **before the worker
serves anything** (spec 004). That work happens once per worker start.

**It is a startup cost, and it must be measured as one** — as time from process
launch to first successful readiness response, per arm, not folded into any
per-request statistic. Pooling it with request latency would let a one-off cost be
amortised into a per-request number, which is exactly the shape of claim that cannot
be undone later.

**Decided: three independent start/stop cycles per arm, reported as a descriptive
distribution, entering no gate.**

| | |
|---|---|
| **repetitions** | **3**, each a full independent start and stop — not three timings of one process |
| **what is reported** | **all three values**, plus min, median and max, per arm |
| **what is not reported** | no confidence interval, no ratio, no verdict. Three points do not support one |
| **gate participation** | **none.** The startup number cannot make a run pass or fail, and no ladder or margin applies to it |
| **cost** | three start/stop cycles, so **three cleanup verifications**, each fail-closed on the same terms as any other (§9.2) |
| **what travels with it** | the §3 asymmetry, in the record itself: the candidate validates the store and reads the anchor metadata; the reference does neither, so a difference **includes that work by construction** |
| execution | **its own execution identity and budget** — §5.4.5 |

**The protocol, defined so two implementations would agree.**

| | |
|---|---|
| **clock** | **`time.monotonic()`**, one process taking both readings. **Never wall clock**: an NTP step during a launch would silently add or remove time, and `CLOCK_REALTIME` is not monotonic |
| **start `t0`** | taken **immediately before** the arm's launch command is spawned — after its argv is built, before `exec`. What is measured is therefore process creation plus import plus lifespan plus first-serve, which is what a restart costs |
| **end `t1`** | the return of the **first readiness response satisfying the existing predicate** — HTTP **200** *and* a non-empty body, the same test `process_ready` applies. Not the first TCP accept, not the first response of any status |
| **value** | `t1 − t0`, seconds, plus the **number of readiness attempts** that cycle took — a 12-attempt start and a 1-attempt start of the same duration are different facts |
| **per cycle also recorded** | the arm's pid, its full launch argv, the cycle's cleanup verdict, and the ports |

**Error and timeout classification — a cycle that does not reach ready yields no
timing value:**

| what happened | classification | effect on the statistics |
|---|---|---|
| readiness satisfied within `READY_ATTEMPTS` | `OK` | contributes one value |
| `READY_ATTEMPTS` exhausted, process still alive | **`STARTUP_TIMEOUT`** | **no value.** Recorded with its attempt count |
| the arm exited with the candidate's own store-validation error | **`STARTUP_VALIDATION_FAILURE`** | no value; the run stops (§9.2) |
| the arm exited without such an error, or its log is unreadable | **`INVALID_PRE_START`**, indeterminate noted | no value; the run stops |

**The distribution is reported only over `OK` cycles, and the count of non-`OK`
cycles is reported beside it.** **If fewer than three cycles are `OK`, the startup
measurement is `INCOMPLETE`** — min/median/max are not reported at all, because three
was already the minimum for showing spread and two points do not show it.

**Launch order is recorded, and counterbalanced as far as three cycles allow.**
Whichever arm starts first in a cycle may benefit from — or pay for — whatever the
other's start left on the host. Three cycles cannot balance two orders evenly, so:

| cycle | first | second |
|---|---|---|
| 1 | reference | candidate |
| 2 | candidate | reference |
| 3 | reference | candidate |

**The order is written into every cycle's record** (`launch_order: "RC"` or `"CR"`),
and the report states that the balance is **2:1 and not even**. A difference between
the arms that is the size of the order effect cannot be separated from it by three
cycles, and no attempt is made to claim otherwise.

**A cleanup failure in any cycle ends the whole execution.**

| | |
|---|---|
| classification | **`CLEANUP_FAIL`**, and the startup measurement is **`INCOMPLETE`** |
| remaining cycles | **not attempted** |
| statistics | **none.** No min, median or max is computed, and the timings already collected **may not be quoted as a distribution** — they are recorded as individual cycle values with the failure beside them |
| evidence | preserved: state files, logs and per-cycle records all kept; no SIGKILL; no self-rerun |

**Partial timings are not a distribution.** Two `OK` cycles and one `CLEANUP_FAIL` is
not "the startup time with one cycle missing" — it is a run whose host state after
the failure is unknown, and a median of the survivors would be a number computed
across that.

**Execution identity of the three launches.** One authorised run, three sequential
cycles, each fully stopped and verified before the next begins — the arrangement C2
uses. Per-cycle run-state directories `<label>_cycle{1,2,3}` so no cycle can overwrite
another's logs or preserved state; ports are reused **across cycles within that run**,
which is sound only because each cycle's cleanup confirms them free before the next
starts. A cycle whose cleanup does not confirm **stops the sequence** — the remaining
cycles are not attempted, and the measurement is `INCOMPLETE`.

**A single observation would not do.** One launch per arm gives one number each and
no way to tell a real difference from the variation between launches — and a single
number invites exactly the claim that must not be made, that startup latency is
*stable*. **Three is enough to show spread and not enough to characterise a
distribution**; that is why the output is descriptive and why it gates nothing.
Anything stronger needs its own design and its own authorisation.

### 2.2 Steady-state request latency

Warm per-request latency, which is what the existing gate measures and what §5
defines. **Separate artefacts, separate verdicts, and no arithmetic that combines
them.**

## 3. The asymmetry the arms already have, and what it costs

**By the time either arm is ready, they have not done the same work.** Spec 004
§48–51 records it, and the runner prints it at readiness:

| | candidate | reference |
|---|---|---|
| import-time store check | yes | no |
| lifespan anchor metadata read | **yes — `1_degree/annual/TS` metadata** | **no** — `woa23_app.py` validates nothing at startup |

**For a byte comparison this is harmless**, which is why C1 and C2 could ignore it.
**For a latency comparison it is not**: the candidate arrives at its first request
with the anchor group's metadata already read and whatever the OS cached along with
it, and the reference does not.

Three consequences, and they are requirements rather than caveats:

1. **The startup measurement (§2.1) must report the asymmetry as part of the
   result**, not as a footnote — the candidate is doing work the reference does not,
   and any startup difference includes it by construction.
2. **The steady-state measurement must warm both arms to the same state before
   sampling** (§4.1), so the difference is not the leftover of that head start.
3. **No claim may be made that the two arms began from the same cache state.** They
   did not. What can be claimed is that they were warmed to a common state
   afterwards, which is a weaker and true statement.

## 4. Warm-up, cache state, counterbalancing

### 4.1 Two warm-ups, named apart

They are different things at different scopes, and calling both "warm-up" is how one
gets counted as the other.

| name | when | per arm | purpose | enters statistics? |
|---|---|---|---|---|
| **`symmetric_warmup_pass`** | **once per run**, before any sampling begins | **16** = 8 cases × 2 orders | to erase §3's head start: the candidate reaches its first request having read the anchor metadata and the reference has not | **No.** Responses are discarded entirely and never recorded as samples |
| **`per_case_leading_sample`** | **once per case per arm**, inside the latency sequence | **8** = 1 per case | `paired_stats.WARMUP_REQUESTS = 1` — pays for connection setup, lazy imports and a cold page cache for *that case* | **No.** `paired_stats.warm()` drops it before any statistic sees it |
| measured samples | after the leading sample | **168** = 8 × 21 | the measurement | **Yes** — these and only these |

**Per case per arm the latency sequence is therefore `1 + 21 = 22` requests**, of
which **21 are measured**. `warm()` raises rather than returning an empty list when
given too few samples, so a truncated sequence cannot silently become a statistic
over nothing.

**Neither warm-up enters any statistic**, and neither appears in `n_a` / `n_b`. Both
are HTTP requests and both are counted in the budget (§5.4) — discarded from the
statistics is not the same as free.

### 4.2 Cache state

**Warm only.** The OS page cache over a 32 GB store is not controlled here, and
nothing in this plan drops or manipulates it: doing so on a production host is a
write-adjacent action and is not in scope. **Cold-cache measurement is S5** and is
explicitly excluded (§8).

The state is therefore: *warmed by the harness, on a host whose cache the harness
does not control, with production live on the same machine.* That is a limitation
of every number this produces and travels with them.

### 4.3 Counterbalancing

The gate interleaves the arms per case and alternates which is asked first — the
same `RC`/`CR` scheme the contract gate uses, which is what `bootstrap_ratio`'s
docstring means by pairing "in *time*". **Interleaving defends against host drift
across the run; it is not per-observation pairing, and the bootstrap does not claim
to be paired.** That distinction is already stated in `paired_stats` and is carried
into the report.

## 5. Metric, statistics, ladder and ceilings

### 5.1 Metric

Per-request wall-clock latency over loopback, per case, **median** as the summary,
with `ratio = median(candidate) / median(reference)` so **below 1.0 means the
candidate is faster** (`paired_stats.bootstrap_ratio`).

**Percentiles beyond the median are recorded but do not decide anything.** The
existing statistic is a bootstrap on the median; a tail percentile would need its
own interval and its own sample-size argument, and inventing one here would produce
a verdict the machinery does not support. Raw per-request samples are kept in the
artefacts (as `noise_pilot` already does), so a tail analysis is possible later
without re-running.

### 5.2 The decision, written out

The implementation is `bench/paired_stats.py`; these are the equations it applies,
stated here so the decision can be checked without reading it.

**Everything below is PER CASE.** There are eight cases, and they produce **eight
ratios, eight bootstrap distributions, eight intervals and eight verdicts**.

> **Samples are never pooled across cases.** The eight cases span three orders of
> magnitude of work — `point_profile` against `surface_global` — so a ratio of
> pooled medians would be a ratio of whatever mix of case sizes happened to be
> sampled, and a regression in a small case could be hidden by a large one. There is
> **no total ratio**, and none may be computed from these artefacts.

**Inputs.** For case `k`: warm sample vectors `C_k` (candidate) and `R_k`
(reference), each of length `n` after that case's leading sample is discarded
(§4.1).

**Point estimate, per case.**

```
r_k = median(C_k) / median(R_k)               r_k < 1  ⟹  candidate faster on case k
```

**Interval — a percentile interval of the raw bootstrap ratios.** `B = 5000`
rounds, seed `20260805`. Each round resamples **each arm independently, with
replacement, at its own size** — an interleaved two-arm bootstrap, **not** an
observation-paired one, and no per-observation pairing is claimed:

```
for each case k:
    for b in 1..B:
        C*_k = sample_with_replacement(C_k, |C_k|)
        R*_k = sample_with_replacement(R_k, |R_k|)
        r*_kb = median(C*_k) / median(R*_k)

    sort r*_k
    ci_low_k  = r*_k[ floor(0.025 × B) ]      = r*_k[125]
    ci_high_k = r*_k[ floor(0.975 × B) ]      = r*_k[4875]
```

Each case carries its own `n_a`, `n_b`, seed and rounds in the artefact. **The gate's
single verdict (below) is a function of the eight per-case verdicts, not of a pooled
statistic.**

**Exactly what those quantiles are taken over, because it decides whether `r` is
counted twice.**

- the quantiles are of the **raw bootstrap ratio distribution `r*`** — the ratios
  themselves, in the same units as `r`. They are **not** normalised by `r`, **not**
  pivoted (`2r − r*`), and **not** studentised;
- the point estimate `r` is computed **from the original samples**, not from `r*`.
  It is not the median of the bootstrap distribution and is not recombined with it;
- so **`r` enters the interval once, through the data, and is never multiplied in a
  second time.** Verified against `bench/paired_stats.bootstrap_ratio`, which
  reports `median_ratio` from the originals and `ci95_low`/`ci95_high` as
  `sorted(ratios)[125]` and `[4875]`;
- there is **no bias correction and no acceleration** — this is the plain percentile
  method, not BCa.

**What "95%" does and does not claim.** The interval is a **bootstrap percentile
interval at the 2.5th and 97.5th quantiles**. Percentile-method coverage is nominal,
and it is *asymptotic and conditional on the resampling assumption* — that the
samples within an arm are exchangeable draws from one distribution.

**That assumption is not established here, and this spec does not claim it.** The
samples are collected interleaved over minutes on a host running production, so they
may be **autocorrelated** (page cache, co-tenant load, drift), and no independence or
stationarity test is run. Autocorrelation would make the interval **too narrow**.

So the correct statement of a result is:

> *the 2.5th–97.5th percentile range of 5,000 bootstrap ratios*

and **not** *"a 95% confidence interval for the true ratio"*. The verdicts of §5.2
are decisions taken with respect to that range, under a margin chosen for
engineering reasons; they are not statements about a population parameter's
coverage. Establishing coverage would need an independence or autocorrelation
argument that no run has produced.

**Threshold.**

```
margin T_m = 0.05          (a PI / engineering judgement, not a statistic)
threshold  = 1 + T_m = 1.05
```

**APPROVED by the PI on 2026-08-19** as an **engineering decision threshold**. The
approval settles which number the verdicts below are taken against; it settles nothing
about the data. `0.05` is still **not** a statistical property of the measurements,
**not** a confidence guarantee, and **not** a production SLA, and a different margin
would still give different verdicts from the same measurements.

**Regression verdict, per case — one of exactly three:**

```
REGRESSION_k      ⟺  ci_low_k  >  1.05      the whole interval is worse
NO_REGRESSION_k   ⟺  ci_high_k ≤  1.05      the whole interval is acceptable
INCONCLUSIVE_k    ⟺  otherwise              the interval straddles the threshold
```

`INCONCLUSIVE` means **collect more samples**. It never means "close enough".

**Improvement verdict — computed for every case, but a GATE for only two.** It is
asked of every case because the number is informative; it decides the gate **only** for
the **improvement-required** cases `readme_example` and `point_profile_multiparam`
(`bench/paired_bench.py`: `IMPROVEMENT_REQUIRED`), via
`FAIL_NO_ESTABLISHED_IMPROVEMENT` in §5.3. A case that is not improvement-required and
whose interval sits below 1.0 has **not** passed an `IMPROVED` gate, and may not be
reported as though it had:

```
IMPROVED        ⟺  ci_high <  1.0
NOT_IMPROVED    ⟺  ci_low  ≥  1.0
INCONCLUSIVE    ⟺  otherwise
```

Improvement is **required** only for the cases named in
`paired_bench.IMPROVEMENT_REQUIRED` — today `readme_example` and
`point_profile_multiparam`. For every other case it is recorded and does not decide
anything.

**The run's single verdict, in strict precedence** (`paired_bench.decide_gate`).
Validity questions come first: a run whose provenance or runtime cannot be trusted
has no performance result to report at all.

```
1. INVALID_METADATA                 provenance problems on either arm
2. INVALID_RUNTIME_DRIFT            the runtime moved during the run
3. INVALID                          an unexpected HTTP status in any sample
4. FAIL                             any case REGRESSION
5. FAIL_NO_ESTABLISHED_IMPROVEMENT  an improvement-required case not IMPROVED
6. INCONCLUSIVE                     any case undecided
7. PASS                             none of the above
```

**`PASS` therefore means:** every case has `ci_high ≤ 1.05`, the improvement-required
cases have `ci_high < 1.0`, both arms' provenance agreed, nothing drifted, and every
response carried its expected status. It does **not** mean the candidate is faster
everywhere, and it does not mean anything about production (§1a).

**Escalation estimator**, used only to choose the next rung and never to decide a
verdict:

```
gap     = |1.05 − r|
binding = (ci_high − r)   if r < 1.05
          (r − ci_low)    if r > 1.05
n_required = n                        if binding ≤ gap
             ⌈ n × (binding / gap)² ⌉  otherwise      (bootstrap width ~ 1/√n)
n_required = undefined                if gap ≈ 0
```

**The binding side is one side, not the half-width.** Bootstrap intervals are not
symmetric, and using the symmetric half-width once produced "21 samples are enough"
for a case whose verdict was `INCONCLUSIVE`. Where `gap ≈ 0` no sample size resolves
it: the effect is exactly the size declared not worth acting on, and the case goes
to the PI.

### 5.3 Ladder and stopping rule

The pre-specified ladder from spec 001: **21 → 60 → 150 samples per arm**, each rung
an **independent run** rather than an accumulation, verdict from the largest rung
run. `provenance.verify_prior_rung()` requires an escalation to inherit the earlier
rung's conditions — same gate variant, same warm sample count, complete provenance,
no runtime drift, identical source digests, store path and `.zmetadata` fingerprints
on both arms — and **refuses to escalate past an established `FAIL` or regression**,
because more samples cannot overturn a confirmed failure.

**Stopping rule:**

| state | action |
|---|---|
| every case resolved at a rung | stop; that rung is the result |
| some case `INCONCLUSIVE` | escalate **only those cases**, one rung, one invocation — **each rung needs its own authorisation** |
| still `INCONCLUSIVE` at 150 | **stop and refer to the PI.** No rung above 150 exists, and the runner refuses one |
| any `REGRESSION` | stop. Escalation is not attempted |

**One rung per invocation**, enforced by the script. The noise pilot runs **after**
the gate and against **both** arms — sampling one arm 208 times before a paired
measurement would warm one side ahead of the other — and its output is
**sample-size planning for the next rung only**, never the gate's threshold.

### 5.4 Request budget — one authorisable ceiling

Let `K = 8` cases, `n = 21` measured samples per arm per case,
`W = WARMUP_REQUESTS = 1`.

**Every line is an HTTP request and is counted by F2** (`lib_requests.sh`), which
increments **before** each attempt — a timeout, a refused connection and a retry each
count as one. The measured totals come from `results/<label>_requests.json`; the
figures here are the ceiling.

#### 5.4.1 The formal ceiling: the latency run

**One number is authorisable, and it is the combined one.** The contract gate runs in
the same invocation, because a latency measurement of an arm that has not been shown
to return the right answers is not worth having (§7).

| stage | per arm | arithmetic |
|---|---|---|
| readiness | ≤ 30 | `READY_ATTEMPTS`; every attempt counted, failures included |
| store probe | 2 | each arm probed twice, both orders |
| **contract gate** | **64** | `len(all_cases())` |
| `symmetric_warmup_pass` | **16** | `K × 2` |
| latency gate | **176** | `K × (W + n) = 8 × (1 + 21)` |
| noise pilot | **208** | `K × (25 + W) = 8 × 26`, conditional — §5.4.3 |
| retries | 0 by design | no stage retries; readiness re-attempts are already counted above |

```
per arm  =  30 + 2 + 64 + 16 + 176 + 208  =  496
both arms =  496 × 2                      =  992
```

> **FORMAL CEILING, rung 21: 496 requests per arm, 992 total.**
> Production 8050 / 8786 / 8787: **0**.

**The ceiling is a limit, not a label, and the harness now enforces it.**
`assert_request_ceiling` compares the run's own recorded counts against this number
before anything is reported; a run over its ceiling exits `8` (`CEILING_EXCEEDED`)
and **has no quotable result of any kind** — not the gate's verdict, not a latency
figure, not a request total, because the authorisation it ran under did not cover
what it did.

**Every stage has exactly one count owner.** The contract stage was recorded twice —
once by `run_controlled.sh` before the gate and once by the finaliser afterwards.

**What that defect did, stated exactly.** A duplicate count owner **issues no HTTP**.
The contract gate makes one request per case per arm whether the shell records that
number once or twice; the second `request_add` doubles the *bookkeeping* for requests
one gate already made. The arithmetic:

| | per arm | both arms |
|---|---|---|
| requests the formal path actually issues | **496** | **992** |
| what the counter would have reported, contract double-recorded | **560** | **1,120** |
| the figure the offline driver test showed | 528 | — |

`560 = 30 + 2 + (64 × 2) + 16 + 176 + 208`. The 528 quoted in rev 7 was the **offline
driver's** number, which contains no readiness and no store probe — it is
`(64 × 2) + 16 + 176 + 208`, and it is not the runner's total. **Nothing here says the
run would have issued 1,056 requests, because it would not have issued one extra
request at all.** Rev 7 said so and was wrong.

Two findings follow, and they are **not** the same finding:

- **`CEILING_EXCEEDED` (exit 8)** — reserved for attempts the *evidence* demonstrates:
  journals and artefacts that account for more requests than the authorisation
  covered. It is a statement about the host.
- **`REQUEST_ACCOUNTING_INCONSISTENT` (exit 9)** — the counter disagrees with the
  evidence. It is a statement about the bookkeeping, and it makes **no claim about how
  many requests reached the host**. A miscount classified as a ceiling breach would
  report traffic that never happened.

`s2perf_reconcile` checks every stage's recorded count against the artefact or
journal it came from and runs **before** the ceiling, because a counter that
disagrees with its evidence cannot be compared against anything.
`record_contract_count` and `s2perf_record_counts` each refuse a second call within
one run: the function being the owner stops a second *call site* being added, and the
refusal stops the same site being *reached* twice. **The refusal is enforced by a
marker file, and a marker that cannot be written fails the run** — the guard's whole
contract is that a second call is refused, and an unenforceable guard reported as
enforced is worse than none.

**Revisions 3 and 4 of this document gave the pilot 26 and the ceiling 314 / 628.
Those numbers were wrong.** `bench/noise_pilot.py` loops over the whole selected
case set and takes `--warm 25` plus one discarded leading request **for each case**;
26 is its per-*case* count, not its per-*arm* count. The pilot is the largest stage
in this mode, not the smallest, and the real ceiling is 182 requests per arm higher
than the figure previously reviewed. Recorded here rather than corrected silently:
**an authorisation granted against 314 would have been exceeded by the run it
authorised.** The error was found while building the offline integration test
(`bench/test_s2perf_integration.py`), before any request was issued.

Higher rungs change one line — the latency gate becomes `8 × (1 + 60) = 488` or
`8 × (1 + 150) = 1,208` per arm — and each rung is a **separate authorisation** with
its own ceiling computed the same way.

#### 5.4.2 If the contract gate is run separately

Permitted, and then it is **not the same run**:

- a **different execution identity** — its own staging, workdir, label and first-use
  ports;
- its own budget: `30 + 2 + 64 = 96` per arm, `192` total;
- the latency run's ceiling drops to `30 + 2 + 16 + 176 + 208 = 432` per arm /
  `864` total;
- the latency run must **cite the contract result by commit and label**, and the two
  must be the identical commit.

**Two runs, two budgets, two labels. There is no arrangement in which one
authorisation covers both at 496.**

#### 5.4.3 The noise pilot — trigger, count, rungs

Fixed here rather than left to the runner's current habit of always running it:

| rung | runs the pilot? | why |
|---|---|---|
| **21** | **yes, always** | its output is the noise floor of *this pair in this environment*, which has never been measured under the package clone. It also plans rung 60 if one is needed |
| **60** | **only if a case is `INCONCLUSIVE` and rung 150 is still available** | its only use is planning the next rung |
| **150** | **no** | there is no rung above it. A pilot would plan nothing |

**26 requests per case** (`--warm 25`, plus the one leading sample the same
`WARMUP_REQUESTS` rule discards), and the pilot visits **every case in the selected
set**: `8 × 26 = 208 per arm per invocation`. **Both arms, and after the gate** —
sampling one arm 208 times before a paired measurement would warm one side ahead of
the other. Its output is **sample-size planning only** and never the gate's
threshold.

*Note on the correction:* the sentence above already said 208 in rev 3, describing
the invocation, while the budget table beside it said 26. The prose was right and the
budget was wrong. Two numbers for one stage in one document is how a ceiling gets
approved that the run cannot honour.

#### 5.4.3a The contract stage's count is DERIVED, and may not always stand in

It is `len(all_cases())` per arm, recorded before the gate rather than measured
inside it. That equals what reached the host only while the transport does not retry
and no response is a 3xx — both pinned behaviourally in `bench/test_contract.py`.

**And only while the gate completes.** `contract_diff` records a case whose request
raised as `verdict: ERROR` and moves on; under RC order a case that failed on the
reference never issued the candidate's request at all. A gate with any ERROR case
therefore issued **fewer** requests than the derived number claims, and an
over-count is not the harmless direction: it would report traffic that was never
sent, and — since the ceiling is checked against the recorded total — could fail a
run for requests it did not issue.

`bench/perf_counts.py` reads those verdicts back and marks the whole record
`counts_exact: false` when it finds any. **A derived number may not pose as measured
attempts.**

#### 5.4.4 What is not a request

**Bootstrap resampling issues no HTTP.** The 5,000 rounds of §5.2 resample
already-collected numbers in memory; they contact no backend and **must never appear
in a request count**. The same holds for the noise pilot's own bootstrap — only its
208 sampling requests are traffic.

#### 5.4.5 The startup measurement is a separate execution

**It is not inside the ceiling above.** Three start/stop cycles have their own
readiness traffic and their own cleanup evidence, and folding them into the latency
run would put three extra cleanup verifications inside a measurement that needs one.

| | |
|---|---|
| execution identity | **its own** staging, workdir, label and first-use ports |
| case requests | **none** — no characterization, no contract, no latency |
| cleanup | **three verifications**, one per cycle, each fail-closed |

**The arithmetic, in full:**

| | per arm | both arms |
|---|---|---|
| readiness | ≤ **30** | ≤ 60 |
| store probe | **2** | 4 |
| **per cycle** | **≤ 32** | **≤ 64** |
| **× 3 cycles** | ≤ 96 | **≤ 192** |

```
per arm per cycle   =  30 + 2            =  32
both arms per cycle =  32 × 2            =  64
three cycles        =  64 × 3            = 192
```

> **FORMAL CEILING, startup measurement: ≤ 192 requests in total** — `3 cycles ×
> 2 arms × (30 readiness + 2 store probe)`. **No contract, characterization or
> latency request is issued by this execution.** Production 8050 / 8786 / 8787: **0**.

#### 5.4.6 When a stage does not finish

A refused connection or a timeout ends a sampling stage part-way. Until 2026-08-11
that meant **no artefact at all**: `paired_bench` and `noise_pilot` did not catch
transport errors, so the requests they had already put on the host became
uncountable, and a run could not say what it had sent.

Three things now happen instead, and all three are conditions on reporting.

**1. Every attempt is journalled before it is issued.** `bench/request_log.py`
appends one line per attempt — stage, arm, case, sequence — and flushes it *before*
the request leaves, on the same rule `lib_requests.sh` states for the shell counter:
record the attempt before issuing it, never after, and never conditionally on the
outcome. A process killed mid-stage leaves the journal behind.

**The run-level classification is neutral, and the cause travels with it.** It was
`INCOMPLETE_TRANSPORT_FAILURE`, hardcoded — so a backend answering HTTP 500 was
reported at run level as a transport failure while its own artefact said otherwise,
and it is the run-level headline that gets quoted. The classification is now
`INCOMPLETE_STAGE_FAILURE` with `stage_failure_classification` **read back from the
stage artefacts** (`bench/stage_cause.py`), so it cannot drift from what the stage
recorded. A stage that left no artefact cannot name its own reason, and that is said
rather than guessed: `NO_STAGE_ARTEFACT_RECORDS_A_CAUSE`.

**A stage can also fail while perfectly reachable.** The noise pilot refuses to
report timings from a case that answered a status the run did not expect — a live
backend answering 500 is not a transport failure and does not share its
classification (`STAGE_ABORTED_UNEXPECTED_STATUS`). That path raised `SystemExit`,
which derives from `BaseException` and so passed straight through the handler that
writes the partial artefact: a pilot that had already issued hundreds of requests
exited with **no artefact at all**, and since an absent pilot artefact is the
legitimate "no pilot at this rung" state (§5.4.3), the run counted zero pilot traffic
and called itself exact. It is an ordinary exception now, and the journal is closed
in a `finally`.

**The caller's exit status is part of the evidence.** Absence of a pilot artefact
cannot be distinguished from a pilot that died before writing one, so the runner
passes `--stage-failed noise_pilot` to `perf_counts` when it watched the stage exit
non-zero, and the counts come from the journal.

**One record, three consumers.** That flag reached only the final artefact at first,
and the shell counter and the reconciliation each asked `perf_counts` again without
it — so a hard-killed pilot was recorded as **0** by the counter, reconciled 0
against a freshly computed 0, and reported as no traffic, while the journal held
every attempt it had made. `s2perf_stage_record` now computes the record **once**,
with the caller's failed-stage information, writing `<label>_perf_counts.json` and
emitting the same numbers as shell assignments from the same call. The counter and
the reconciliation read that computation; neither recomputes. Three questions with
three flag sets cannot give three answers if there is only one question.

#### 5.4.6a The noise pilot stops after a failing arm

**Decided: fail-fast.** If the reference arm's pilot exits non-zero, the candidate
arm is **not** sampled.

The pilot's output is a noise floor **of the pair** — the escalation estimate it
feeds is a property of the comparison, so a figure from one arm plans nothing on its
own. Sampling the second arm would spend a further **208 requests** to produce a
number that cannot be used, which is the opposite of what a budget is for.

What is kept: the journal (every attempt, both arms), the first arm's partial
artefact if it wrote one, and its journaled attempt counts. What is not produced:
any noise floor, for either arm. The run is `INCOMPLETE_STAGE_FAILURE` and reports
those counts with `host_attempt_count_exact` and the ceiling, as §5.4.6 requires.

**2. The stage writes a partial artefact.** `complete: false`,
`gate: INVALID_TRANSPORT_FAILURE`, `classification:
STAGE_ABORTED_TRANSPORT_FAILURE`, the failing case, and the cases that did finish
with their raw samples. **No verdict is computed from the cases that finished** —
`decide_gate` would happily produce one, and it would be a gate verdict for a
measurement that never happened.

**3. The request total stops being exact — and what may be said depends on HOW the
stage ended.** `bench/perf_counts.py` reads the journal when an artefact is missing
or partial and sets `counts_exact: false`. There are **two evidence strengths**, and
they are not interchangeable:

| `attempt_evidence` | when | what the number is |
|---|---|---|
| `stage_artefact` | the stage finished | attempts, exactly |
| `journal_writer_exited_normally` | the stage caught its own failure and wrote a partial artefact | **attempts, exactly.** Every `attempt()` that returned was followed by a call that completed or raised, and the process closed its own journal |
| `journal_after_abrupt_termination` | the caller watched it die and no artefact exists | **`journaled_attempts`: the number of records the journal contains.** Nothing more |

**Under abrupt termination the journal count is not a bound in either direction.**
The line is written and flushed *before* the call, so a process killed in between
leaves a record for a request that was never sent — the count can be **one too
high**. A final record truncated mid-write cannot be attributed to an arm and is not
counted — the count can be **one too low**. So it may **not** be described as:

- a floor, or a lower bound;
- "at least N requests";
- "requests actually issued";
- observed host traffic.

A genuine bound would require a derivation over the single-threaded request loop, the
flush point and the truncated-record case, formally stated and tested. Until such a
derivation exists, the record count and `host_attempt_count_exact: false` are what is
reported, together with the authorised ceiling.

> **When `counts_exact` is false, the run reports the journaled attempt counts, the
> exactness flag and the authorised ceiling — and may NOT report a single exact
> total. The run is not a complete latency result and no case in it may be quoted as
> performance.**

The chain classifies such a run `INCOMPLETE_STAGE_FAILURE`, writes
`<label>_perf_classification.json` carrying the observed figures and the ceiling,
and exits **7**. The noise pilot is **skipped** when the latency gate did not
finish: 416 requests to plan an escalation of a measurement that never completed is
the opposite of a budget.

#### 5.4.7 Exit statuses, and why they are distinct

Three different states that a single non-zero exit cannot tell apart:

| status | state | what may be reported |
|---|---|---|
| `1` | the gate returned a verdict other than PASS | **everything.** The artefacts are complete; `FAIL`, `INCONCLUSIVE` and `FAIL_NO_ESTABLISHED_IMPROVEMENT` are results |
| `6` | `INVALID_POST_MEASUREMENT_HARNESS` | the measurements are complete and the artefacts are not. Not a gate verdict |
| `7` | `INCOMPLETE_STAGE_FAILURE` | journaled attempt counts, `host_attempt_count_exact`, `measurement_complete` and the ceiling. Not a latency result. `stage_failure_classification` and `failed_stages` travel with it, so the classification names the stage on its own |
| `8` | `CEILING_EXCEEDED` | the counts and the ceiling, and the fact of the breach. **Nothing else.** Reachable **only when the attempt count is exact** — an inexact count cannot demonstrate a breach and is never compared against the ceiling |
| `9` | `REQUEST_ACCOUNTING_INCONSISTENT` | the counters, the evidence, and that they disagree. **No claim about what reached the host** |

Precedence: artefacts-not-written, then **counters-disagree-with-evidence**, then
**over-ceiling**, then total-unknown, then the gate's own verdict. Reconciliation
comes before the ceiling because an unreconciled counter is not a measurement of
anything, so comparing it against an authorisation would be comparing the wrong
number. "We issued more than we were authorised to" then outranks "we are unsure
exactly how many", and both outrank the verdict.
A run whose artefacts were never written cannot be reported at all; a run whose
request total is unknown cannot be reported as a latency result; only then does the
verdict apply. A cleanup failure raises a zero status to 1 and **never lowers** 6 or
7 — an EXIT trap's `return` does not set the exit status, so raising it takes an
`exit`, which is what c2d proved by exiting 0 while saying the run had failed.


## 6. Which environment, and why

### 6.1 The choice

| | D2b environment | **S2 environment** |
|---|---|---|
| packages | `dev2026/.venv` | **production's interpreter + read-only clone** |
| already measured | **yes** — BASELINE, rung 21, 8/8 | **no** |
| correctness established | D2b | **C1 `c1e`, C2 `c2f`** |

**This plan measures the S2 environment.** Measuring `dev2026/.venv` again would
re-answer a question that has an answer.

### 6.1a The D2b numbers are a historical protocol result, not this baseline

The D2b latency gate is quoted in this document as **evidence that the protocol
works and produced a decidable result** — the ladder, the margin, the bootstrap and
the eight cases all ran and resolved. **It is a historical protocol/reference
result. It is not the baseline this campaign compares against**, and no S2 number
may be placed beside it as though the two measured the same thing.

The bar for calling it a baseline is that **every** condition below is shown
identical. It is not met:

| condition | D2b, 2026-08-08 | planned here | same? |
|---|---|---|---|
| candidate source | the pre-D1 candidate | **`919095e8`** — includes the D1 store-validation patch | **NO** |
| package environment | **`dev2026/.venv`**, 58 distributions, `distributions_sha256 cca6aa8460ab175a` | **production's read-only clone**, 236 distributions | **NO** |
| interpreter | the venv's Python 3.11.4 | **production's** `python3.11` binary, started with `-S` | **NO** |
| worker count | 1 per arm | 1 per arm | yes |
| cwd | staging arm directories | staging arm directories | expected yes, **to be verified per run** |
| store | `'data/'` → `/home/odbadmin/python/woa23/data` on both arms | same literal, same canonical store | expected yes, **to be verified per run** |
| warm-up | `WARMUP_REQUESTS = 1` | same, **plus a symmetric warm-up pass** (§4.1) | **NO — this plan adds one** |
| measurement protocol | rung 21, margin 0.05, bootstrap 5,000, seed 20260805, RC/CR | identical | yes |
| ports / host | 8051 / 8052, odb24 | new first-use ports, odb24 | different ports, same host |

**Four conditions are known to differ and one is added by this plan.** Any of the
first three alone is enough: a different interpreter and a different package set are
precisely what S2 exists to vary.

**Therefore:**

- D2b's ratios are cited as **"the protocol produced these on that environment"**,
  never as **"the candidate is 15× faster"** in an S2 context;
- **the S2 baseline is the S2 reference arm**, measured in the same run as the
  candidate — which is what the paired design is for. There is no cross-run baseline
  and none is needed;
- if a cross-campaign comparison is ever wanted, it needs the table above to be
  filled in from **both** runs' provenance artefacts, and the differences reported
  with it.

### 6.2 Worker count — one, and why

**One worker per arm**, as C1 and D1 use.

The reason is not that one worker is realistic — production runs two (measured from
its own argv in C2). It is that **a two-worker arm makes the measurement ambiguous
without answering a different question**: requests would be distributed between two
processes whose page-cache and interpreter state differ, and a median over that
mixture is a median over an allocation policy as much as over the code.

**What that costs, stated plainly:** this measures the read path, **not production's
serving configuration**. It supports no claim about behaviour at production's worker
count, about concurrency, or about queuing — those are S4's questions.

> **A production-worker-count latency mode is a separate study**, needs its own
> spec and its own authorisation, and its result would not be comparable with this
> one.

### 6.3 Boundaries, inherited unchanged from C1/C2

- **loopback and staging only**; both arms are copies, verified byte-identical to
  their sources before starting;
- **production's interpreter** and the **read-only package clone**, with full
  manifest, ancestor-chain and mode-bit verification three times per run;
- **`-S`**: `site.py` does not run and no `.pth` in the clone is processed. **This
  is a limitation of the measurement, not only of the correctness runs**, and it is
  carried into every performance number: the timing describes an interpreter started
  the way this harness starts it;
- **the store is read through a staging symlink and never written.** The basis for
  that is that the runner performs no write to it — an unchanged mtime corroborates
  and does not prove it;
- production is read from `/proc` and `ss` only, and its identity is re-verified.

## 7. Sequencing against the correctness gates

**Correctness first, and it is already done for this candidate.**

| | status |
|---|---|
| C1 `c1e` — 5.2A byte-exact, 64/64 | **PASS**, `919095e8` |
| C2 `c2f` — 5.2B, three cycles | **PASS**, same `api/` |
| this performance work | planned, on the **same** `api/` |

**The rule that makes those citable here:** the candidate source must be
byte-identical to what C1 and C2 measured. If `api/` changes — including for the
spec 006 empty-CSV item — **C1 and C2 must be re-run first**, and the existing
results may not be backfilled as post-patch evidence. A performance number for a
candidate whose correctness was established on different bytes is a number about
nothing.

**Order within a rung:** contract gate before latency, in the same invocation, or
the contract result cited from a run on the identical commit. A latency measurement
of an arm that does not return the right answers is not a measurement worth having.

## 8. What this run does NOT do

- **Not Option B, and not any candidate API change.** `api/` is unchanged. The
  empty-CSV change of spec 006 **may not be carried in the same run**: it alters
  what one of the eight cases returns, so a timing comparison spanning it would be
  comparing two different APIs. If Option B lands, this measurement is re-planned
  against the new candidate **after** C1 and C2 are re-run.
- **Not cold-cache** — S5.
- **Not concurrency or throughput** — S4.
- **Not production's worker count** (§6.2).
- **Not deployment or PM2** — the launcher is this harness.
- **Not a production API measurement**: production receives no request.

## 9. Artefacts, provenance and failure classification

### 9.1 Artefacts

Per rung, per label — the names the harness already writes:

| file | content |
|---|---|
| `<label>_paired.json` | the gate: per case, both arms' warm samples, median ratio, CI, verdict |
| `<label>_noise_pilot_{candidate,reference}.json` | sample-size planning for the next rung, both arms, raw samples kept |
| `<label>_startup.json` | **new** — §2.1: per arm, **three** launch-to-ready times with min/median/max, the readiness attempts each took, the §3 asymmetry stated in the record, and `gates: none` |
| `<label>_meta_{candidate,reference}.json` | provenance: argv, worker count, interpreter, dependencies, store fingerprints |
| `<label>_interp_{candidate,reference}.json` | import isolation |
| `<label>_environment.json`, `<label>_clone_integrity_*.json` | environment and clone |
| `<label>_requests.json` | measured request attempts per arm per stage |
| `<label>_perf_counts.json` | **new** — §5.4.6: the per-stage counts as derived from each stage's own artefact, with `counts_exact`, `host_attempt_count_exact` and `attempt_evidence` saying what kind of number each one is |
| `run/journals/<stage>.jsonl` | **new** — §5.4.6: one line per attempt, appended before the request is issued. The only record that survives a stage killed mid-request |
| `<label>_perf_classification.json` | **new** — §5.4.6: written only when a stage did not finish. `failed_stages`, the stage's own cause, the journaled attempt counts, `host_attempt_count_exact`, the authorised ceiling, and the sentence that the run is not a latency result |
| `<label>_shutdown_budget.json`, `<label>_ports.json` | the run's own configuration, as measured |
| `<label>_workers.json` | one worker per arm, recorded with **this mode's** wording. The finaliser is shared with D1 and takes a `--mode`: an S2 artefact carrying "D1 uses one worker per arm by design" would describe neither run, which is what it did before rev 7 |

**Provenance requirements**, unchanged: both arms must agree on interpreter,
dependency digests, store path literal and `.zmetadata` fingerprints, checked by
`provenance.compare_arms(..., s2=True)`; a mismatch is `INVALID_ENVIRONMENT` and no
timing may be quoted from it.

### 9.2 Failure classification

Exit statuses and their precedence are §5.4.7. `INCOMPLETE_STAGE_FAILURE` (7) is
neither a gate verdict nor a harness-artefact failure, and it carries the stage's own
reason in `stage_failure_classification` rather than restating one at run level.

| classification | meaning |
|---|---|
| `INVALID_PRE_START` | clone integrity, label, ports, budget or manifest wrong; nothing started |
| `STARTUP_VALIDATION_FAILURE` | an arm failed the candidate's own store validation before serving |
| `INVALID_ENVIRONMENT` | the arms are not comparable (provenance mismatch, runtime drift) — **no timing may be quoted** |
| `CONTRACT_FAIL` | the contract gate did not pass; the latency gate does not run |
| `REGRESSION` | established: an interval wholly above the margin. **Stop; no escalation** |
| `INCONCLUSIVE` | at least one case unresolved; escalation is a **separate authorised run** |
| `NO_REGRESSION` | every case resolved and acceptable at this rung |
| `INVALID_POST_MEASUREMENT_HARNESS` | measurements complete, artefacts not finalised (spec 005 §15) |
| `CLEANUP_FAIL` | any tree not drained; **whole run fails**, state preserved, no SIGKILL, no self-rerun |

## 10. What still needs authorisation, and what is undecided

**Needs its own authorisation, in this order:**

1. **A harness change.** The latency gate and the noise pilot currently run **only in
   the D2b environment**: `run_controlled.sh` stops the S2 modes immediately after
   the contract gate, printing that no timing was produced. Measuring the S2
   environment therefore needs either a new mode or an extension of `--c1`, plus the
   startup measurement of §2.1, which does not exist at all. **Offline work, its own
   commit, its own tests.**
2. **One VM24 run at rung 21** — new staging, workdir, label and first-use ports,
   at the **496 per arm / 992 total** ceiling of §5.4.1.

   **The authorisation for that run must itself restate the margin**, in these
   terms and not by reference:

   > `margin = 0.05` is an **approved engineering threshold** (PI, 2026-08-19). It
   > is **not** a statistical property of the data, **not** a confidence guarantee,
   > and **not** a production SLA. Every `REGRESSION` / `NO_REGRESSION` /
   > `INCONCLUSIVE` verdict in the run is relative to it, and a different margin
   > yields different verdicts from the same measurements.

   Approval changed the first clause and nothing else: the sentence is still
   **required verbatim in every authorisation**, because a run authorised without it
   is authorised without the thing that decides its verdicts.

   A run authorised without that sentence is authorised without the thing that
   decides its verdicts.
3. **Each escalation rung** — 60, then 150 — separately, one rung per invocation.
4. **Any change to the 0.05 practical margin** (§5.2). `paired_stats` says it *"needs
   PI sign-off rather than reviewer agreement"*. That sign-off has now been **given
   for 0.05** (PI, 2026-08-19); it is sign-off for **this value**, and any other value
   needs its own.

**Decided:**

- **the eight D2b cases are retained unchanged** for the first S2 rung. They were
  chosen for S1's read path, and keeping them is what makes the two campaigns
  comparable at all. A different set would measure S2 better and nothing else;
- **production's worker count does not block this**, which runs one worker per arm by
  design (§6.2). It becomes **S4 or a separate concurrency study**, with its own
  spec, and its result would not be comparable with this one;
- **margin `0.05` is APPROVED** (PI, 2026-08-19), as an **engineering decision
  threshold**, applied exactly as §5.2 already defines it. It was inherited from S1,
  declared in these terms for `s2pB` and used there, so **no rerun is required**. The
  approval does not upgrade what the number is: it is **not a statistical property of
  the data**, **not a confidence guarantee**, and **not a production SLA**. Every
  verdict in §5.2 is relative to it, so a different margin gives different verdicts
  from the same measurements.

**Undecided, and listed rather than settled:**

- nothing outstanding in the measurement design. What remains is the authorisation
  sequence above.

## 11. This round's limits

No file under `api/` was modified. Nothing was executed, no VM24 process started, no
HTTP request sent, no timing taken, and no result quoted. This is a plan.


## 12. Result — S2 rung-21 PASS under the approved 0.05 engineering threshold (`s2pB`, 2026-08-19)

**The one execution this document has had.** Commit
`c3b9398b4991539c7701feb0bb3bb5d6ddd564d7`, label `s2pB`, ports 18191 / 18192 / 18919,
exit status 0. The complete record — preflight, digests, provenance, cleanup — is in
spec 002 under `s2pB`; this section states only what the measurement found.

**The formal record of this result, to be quoted in this form:**

> **S2 rung 21: PASS under the approved 0.05 engineering threshold.** Scope is limited to
> **single-worker, steady-state, warm-cache request-path latency**. It is **not** a
> production SLA, and it supports **no** multi-worker, startup, deployment or throughput
> conclusion.

**S2 rung-21 PASS — under the approved 0.05 engineering threshold.** Eight cases, 21 warm samples per arm per case (+1
discarded), interleaved and counterbalanced AB/BA per iteration, bootstrap 5,000 rounds,
seed 20260805, **per-case ratios, never pooled, and no total ratio**.

| case | candidate | reference | ratio | 95% percentile interval | regression verdict | improvement gate |
|---|---|---|---|---|---|---|
| point_profile | 20.2 ms | 185.6 ms | 0.109 | [0.105, 0.112] | NO_REGRESSION | not required |
| **point_profile_multiparam** | 194.5 ms | 3121.5 ms | 0.062 | [0.061, 0.064] | NO_REGRESSION | **required — IMPROVED** |
| **readme_example** | 180.9 ms | 1401.6 ms | 0.129 | [0.124, 0.136] | NO_REGRESSION | **required — IMPROVED** |
| small_bbox_full_depth | 41.2 ms | 200.5 ms | 0.206 | [0.203, 0.208] | NO_REGRESSION | not required |
| regional_bbox | 65.4 ms | 146.0 ms | 0.448 | [0.433, 0.451] | NO_REGRESSION | not required |
| surface_global | 149.6 ms | 192.8 ms | 0.776 | [0.770, 0.787] | NO_REGRESSION | not required |
| point_profile_025 | 25.6 ms | 222.2 ms | 0.116 | [0.113, 0.117] | NO_REGRESSION | not required |
| regional_bbox_025 | 145.1 ms | 289.9 ms | 0.500 | [0.487, 0.510] | NO_REGRESSION | not required |

**All eight cases are `NO_REGRESSION`** — every `ci_high` at or below 1.05, in fact none
above 0.787.

**The `IMPROVED` gate is not an eight-case result.** It applies to the two
**improvement-required** cases named in `bench/paired_bench.py`
(`IMPROVEMENT_REQUIRED = ("readme_example", "point_profile_multiparam")`), and only those
two enter the gate's `FAIL_NO_ESTABLISHED_IMPROVEMENT` check (§5.3). **Both passed it.**

The remaining six also have intervals wholly below 1.0. That is an **observation about
their measured ratios and nothing more**: no improvement was required of them, none was
gated, and **this document does not claim they passed an `IMPROVED` gate**. The harness
computes an `improvement_verdict` for every case because the number is useful; only the
two named cases make it a gate.

Every response on both arms matched its expected status. The contract gate passed
64 / 64 byte-exact beforehand, so these are timings for arms that were both shown to
answer correctly.

**Requests: 467 per arm, 934 total, under the authorised 496 / 992, and exact.**
readiness 1, store probe 2, contract 64, symmetric warm-up 16, latency 176, noise pilot
208; characterization, recovery and startup 0. `counts_exact: true`,
`host_attempt_count_exact: true`, `measurement_complete: true`, `failed_stages: []`,
`problems: []`, each stage's `attempt_evidence` `stage_artefact`. The pre-request journal
holds 32 / 352 / 416 attributable records for warm-up / latency / pilot across both arms
— exactly the artefact figures — with **0 truncated and 0 unattributed**.

**What this does NOT say.**

- The margin **0.05 is an approved engineering threshold** (PI, 2026-08-19) — approved
  as a **decision threshold**, which is not a claim about the data. It is **not** a
  statistical property of the measurements, **not** a confidence guarantee, and **not** a
  production SLA. Every verdict above is relative to it. It was declared and used in
  exactly these terms for this run, so the approval required **no rerun** and changes no
  number above.
- The interval is the 2.5th–97.5th percentile range of 5,000 bootstrap ratios. It is
  **not a proven 95% coverage interval**: the samples may be autocorrelated and no
  independence argument has been made.
- The measurement is **single-worker, steady-state, warm-cache, request-path latency on
  two local arms**. It is **not** throughput, not multi-worker, not cold-cache, not
  PM2/TLS, not a production API measurement, and **contains no startup measurement**.
  Production's own worker count is 2; this run fixed **1 per arm by design** and makes no
  claim about behaviour at production's count — that is S4's.
- It authorises **no deployment**. It is one rung of one gate. Approving the margin
  authorises **no escalation** either: **rung 60 remains unauthorised** and is not
  entered automatically.

### 12a. The runner still prints the pre-approval wording

`scripts/lib_s2perf.sh` and `scripts/run_controlled.sh` print
`margin 0.05 is a PROPOSED ENGINEERING THRESHOLD, pending approval`, and
`scripts/test_cli.sh` and `scripts/test_s2perf_driver.sh` assert that string. **Those
files are deliberately not changed here**: this is a documentation-only commit, and
editing them would change the harness and its tests outside the scope of the approval.

The consequence, stated rather than hidden: **any future run will print the superseded
wording** until that is corrected under its own authorisation. The `s2pB` banner and log
already on VM24 say `pending approval` and are evidence — they are **not edited**, and
they record what the run declared at the time it ran, which is exactly what makes the
approval applicable to it without a rerun.

**The correction, when it is authorised** (PI direction, 2026-08-19): a **separate, small
harness/documentation commit** before any future execution, replacing

```
PROPOSED ENGINEERING THRESHOLD, pending approval
```

with

```
APPROVED ENGINEERING THRESHOLD: 0.05
```

in the two banners and in the two tests that assert the string. **That text change alone
requires no rerun of `s2pB`** — it changes what a future run prints, not what any run
measured. **But if a future change touches measurement logic, the measurement must be
re-run under a NEW execution identity**, not attributed to `s2pB`.

**What the noise pilot adds.** Rung 21 buys a per-case tolerance of roughly ±1.0% to
±3.4%, measured after the fact on the reference arm. The suggested next rung is 60. That
is a planning figure for any escalation, not a result.
