# `f66ddd8` — three CLEAN validation batches, subject-bound sentinel written

**Subject: `f66ddd8cd18840213b086a03dba4545b0da8ad44`** — current HEAD, and **identical to
`f66ddd8` in `dev2026` with an empty diff**, so no drift between what was validated and what
is committed.

---

## 1. `test_s2perf_driver.sh` is HARNESS VALIDATION, not performance evidence

**Recorded because I previously mis-framed it, twice: first as a possible performance
exclusion, then as a suite whose cost justified reclassification. Both were wrong.**

It is a **harness / regression suite and a REQUIRED_PASS for D4.** It validates:

- the **order** contract -> warm-up -> latency -> noise pilot -> finalization -> cleanup;
- failure handling, **partial artefacts**, request accounting, ceilings, cleanup and
  **fail-closed** behaviour;
- `run_controlled.sh`'s **pre-flight guards**.

**What it does NOT do:** it drives **local stand-in HTTP servers** with deliberate synthetic
delays and statuses. It starts **no real WOA23 API**, reads **no real store**, exercises **no
Polars behaviour**, and issues **no production request**.

**Its latency and statistics figures measure the harness's own control flow and are NOT
production or API performance evidence.** They must never be cited as such.

**The ~18 minutes are expected.** They come from repeated synthetic scenarios plus per-case
pauses (latency ~0.2 s, pilot ~0.3 s) and per-case bootstrap work. Measured here: **1081 s,
1080 s, 1081 s** across the three batches — **all three passing**. It was also 1080 s in the
batches where it passed under earlier subjects. **The duration is not a failure, not an API
stall, not a Polars fault and not a performance regression, and it is not grounds for
NOT_APPLICABLE.**

It is deliberately **absent from `d4_profile.tsv`**, which means REQUIRED_PASS by default.

## 2. The two fixes confirmed in place before the run

| fix | evidence |
|---|---|
| the suite **builds its own collision fixture** and depends on no previous suite's leftovers | `test_s2perf_driver.sh:1169` `FIX="$WORK/labelfix"`; `:1188` counts fixture entries with `find … -mindepth 1`; `:1197` asserts exit 4 via `fixture_rc` |
| **diagnostics before the summary; summary last** | `:64` `_keep_noticed=no`, `:77` trap speaks only if the notice did not, `:1315` `keep_notice` immediately before `:1316` `suite_summary` |

## 3. Focused tests, before the batches

| suite | result |
|---|---|
| `test_summary_order.sh` | **`ASSERTIONS=12 FAILED=0`** |
| `test_ports.sh` | **`ASSERTIONS=41 FAILED=0`** |
| `test_production_stop.sh` | **`ASSERTIONS=66 FAILED=0`** |
| `test_summary_contract.sh` | **`ASSERTIONS=92 FAILED=0`** |

## 4. Three batches — all clean, and consistent

Worktree `/home/odbadmin/python/woa23-f66ddd8-val`, venv built from the **real** standalone
interpreter (`…/uv-pythons/cpython-3.11.14-linux-x86_64-gnu/bin`), lock `0d2980a5…dccc69`,
`"polars==1.27.1"`.

| batch | HEAD pre | HEAD post | tracked dirty | untracked pre/post | exit | verdict |
|---|---|---|---|---|---|---|
| 1 | `f66ddd8` OK | `f66ddd8` OK | **0 / 0** | 0 / 2 | **0** | **`D4_VALIDATION: PASS`** |
| 2 | `f66ddd8` OK | `f66ddd8` OK | **0 / 0** | 2 / 2 | **0** | **`D4_VALIDATION: PASS`** |
| 3 | `f66ddd8` OK | `f66ddd8` OK | **0 / 0** | 2 / 2 | **0** | **`D4_VALIDATION: PASS`** |

**Classification totals — byte-identical across all three batches:**

```
REQUIRED_PASS passed   : 50
REQUIRED_PASS FAILED   : 0
REQUIRED_FAIL held     : 0
REQUIRED_FAIL BROKEN   : 0
NOT_APPLICABLE         : 4    (skipped before launch, never a PASS)
ENVIRONMENT_BLOCKED    : 6    (skipped before launch, never a PASS)
UNRESOLVED             : 0
skipped before launch  : 10
=== TOTAL: 50 | NON-ZERO: 0 ===
```

**`test_s2perf_driver.sh`, per batch:** `exit=0  1081s / 1080s / 1081s — all passed
(226 assertions)`.

**`test_s2_provenance.py`, the previously flaky one:** `exit=0`, 98 assertions, **3/3**.

**Judgement was not made on process absence.** Each batch was judged on its own
self-record: HEAD verified before **and** after, tracked/untracked state, every suite's exit
line, the classification totals, the summary contract, and agreement across the three.

## 5. The sentinel

```
D4_VALIDATION_SENTINEL
subject=f66ddd8cd18840213b086a03dba4545b0da8ad44
batches=3
clean=yes
```

Written only because **all three batches completed with exit 0, every REQUIRED_PASS suite
passed, and UNRESOLVED was 0.** The driver refuses to write it otherwise, as it did for
`c3cf4da`, `be3b7b8` and `d42c44e`.

**Nothing was skipped, reclassified or relaxed to obtain this.** The 10 skipped suites are the
same 10 as in the failing runs, with the same reasons, and are counted separately.

## 6. A note on this record's own placement

**The harness-vs-performance clarification in §1 is recorded HERE, outside the subject, on
purpose.** I first added it as a comment block to `dev2026/scripts/d4_profile.tsv` and then
reverted it: editing anything under `dev2026/` would have produced a new subject whose batches
had not run, and the sentinel above is bound to `f66ddd8`. **`dev2026` is byte-identical to
`f66ddd8` — `git diff` against it is empty.** The clarification can be folded into the profile
whenever a successor subject is created for some other reason.

## 7. Scope held

Standard **polars 1.27.1**; `pyproject.toml` and `uv.lock` unchanged; **no** `polars-lts-cpu`;
**no** `POLARS_SKIP_CPU_CHECK`; the three B6 suites untouched; no application or API code
changed; **no C1/C2 rerun; no real production performance test**; no production cutover, PM2,
Nginx, TLS, store access, production API request, or cleanup of retained state.
`pkill -f`/`pgrep -f` were not used — the driver's identity was the recorded
`(pid, starttime) = 3295152 lin-175827724`, and the ~18-minute suite was allowed to complete
normally in every batch.
