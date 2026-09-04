# `test_s2perf_driver.sh` — root cause found: a test ORDER/STATE dependency

**Classification stays REQUIRED_PASS. The earlier suggestion to mark it NOT_APPLICABLE is
WITHDRAWN** — it was reclassification driven by a blocked batch, not by scope evidence. The
evidence now shows it is **not** a performance-scope question at all.

**No full performance test was run.** One targeted invocation of the single failing assertion,
seconds not minutes. No production API, PM2, Nginx, TLS, C1/C2, or `POLARS_SKIP_CPU_CHECK`.

## 1. Batches — no abort was needed

The driver had already **completed on its own** before any stop was considered, so nothing was
interrupted, rerun, patched or cleaned. Logs, temp roots and kept state are intact.

| batch | exit | `test_s2perf_driver.sh` |
|---|---|---|
| 1 | 1 | **exit=1, 1081s**, 1 FAILED of 221 |
| 2 | 0 | exit=0, **1080s**, 221 passed |
| 3 | 0 | exit=0, **1080s**, 221 passed |

`batches completed 3 of 3 · batches failed 1 · aborted no · NO SENTINEL`. **INCOMPLETE. No
sentinel exists and none is claimed.**

## 2. The 1081 seconds are NOT a symptom

**1081s failing, 1080s and 1080s passing.** The ~18 minutes is this suite's normal cost in
every batch, so the duration carries no information about the failure. It is not startup,
store read, timeout or retry — it is the suite's ordinary runtime, and it is the same when it
passes.

## 3. The failing assertion, separated from the summary violation

**The assertion** (`failures.txt`, preserved):

```
FAIL with its own grant it proceeds to the host check — expected [4], got [1]
```

`test_s2perf_driver.sh:1124` invokes `scripts/run_controlled.sh --s2-perf` with
`WOA23_S2PERF_GRANTED=yes` and a stub `--python-binary`, expecting the run to stop at a host
check with **exit 4**.

In `run_controlled.sh` the exit-4 host guards are at lines **998–1084**; **exit 1** first
appears at **1097/1100** — the production-interpreter existence and version check. **Getting 1
instead of 4 means the run got PAST every host guard**, which is the opposite of what the
assertion is testing.

**The summary contract violation is a SEPARATE, second defect.** On the failure path the
suite prints `state kept for inspection: …` **after** its `ASSERTIONS=` line, breaking the
last-line contract. It appears **only** in batch 1 — batches 2 and 3 end on the summary — so
it is triggered by failing, not by running. It is a **harness output-contract defect**, and it
made things worse: it **suppressed the batch log's "failing assertions, in full" block**, which
is why the first look showed no detail. **It is not the root cause and is not treated as one.**

## 4. Root cause — the assertion passes only on a DIRTY tree

Targeted reproduction of that one invocation returned **exit 4**, the expected value, and named
the guard that produced it:

```
Refusing to start: this run would write over it.
  …/dev2026/results/s2perf_shutdown_budget.json
  …/dev2026/run/s2perf
```

**Exit 4 here is a "results already exist for this label" refusal — a guard on leftover
state.** Confirmed against the driver's own per-batch record:

| batch | precheck untracked | guard state | result |
|---|---|---|---|
| **1** | **0 — clean tree** | artifacts absent, guard does **not** fire | run continues to the interpreter check -> **exit 1 -> FAIL** |
| 2 | **2** | `results/`, `run/s2perf` present from batch 1 | guard fires -> **exit 4 -> pass** |
| 3 | **2** | same | **exit 4 -> pass** |

**The assertion depends on artifacts a previous run left behind. On a genuinely clean tree —
which is what a first batch is — it fails.** That is a test-isolation defect: the suite asserts
a state-dependent exit code without establishing the state it needs.

### 4.1 Why it surfaced now, and why that is not a regression

In the `c3cf4da` batches this suite passed. There,
`test_c1_readonly_account.sh` still **ran** and itself drives `run_controlled.sh`, priming
`results/` and `run/s2perf` before `test_s2perf_driver.sh` reached them. Skipping it
**before launch** removed that accidental priming.

**The skip-before-launch change did not introduce a defect; it removed the state-priming that
was masking one.** The latent order dependency was always there.

## 5. Environment facts — all as required, none implicated

| | |
|---|---|
| subject | `be3b7b8de88cadbe5afc9ce59d503beaf98ac39e` |
| account | uid **1000**, `odbadmin` |
| venv home | `…/uv-pythons/cpython-3.11.14-linux-x86_64-gnu/bin` — the **real** standalone root, not the alias |
| **polars distribution** | **`['polars']` — standard 1.27.1, NOT `polars-lts-cpu`** |
| polars import path | `…/woa23-be3b7b8-val/dev2026/.venv/lib/python3.11/site-packages/polars/__init__.py` |
| **AVX2 warning** | **emitted: YES** — as B6 accepts. `POLARS_SKIP_CPU_CHECK` was **not** set |
| CPU | `avx2` **ABSENT** from the guest |
| `UV_CACHE_DIR` | `/home/odbadmin/.cache/uv` — `odbadmin`'s own |
| venv rebuilt by another suite? | **no** — `test_c1_readonly_account.sh` is now skipped before launch; the venv home was unchanged after the targeted run |
| stderr | **0 bytes** |

**Nothing here is implicated in the failure.** polars, the AVX2 warning, the interpreter, the
cache and the store are identical in the batches that passed and the one that failed.

## 6. Does it touch `run_controlled.sh` / rebuild the venv / read the production interpreter?

**Yes to the first and third, no to the second in this path.** It invokes `run_controlled.sh`
directly (line 1124), and the failing path reaches the **production interpreter** check at
1097/1100. It exits there — **before** the `uv sync --locked --python $PROD_PY` at line 1125 —
so on this path it does **not** rebuild the shared venv. Verified after the targeted run: the
venv home was unchanged.

**That margin is thin and worth stating: the failing path stops one guard short of a command
that would rebuild the shared venv.**

## 7. Status and the decision

| | |
|---|---|
| classification | **REQUIRED_PASS, currently failing. NOT_APPLICABLE is withdrawn and not applied** |
| root cause | **known** — order/state dependency on artifacts from a previous run |
| second defect | **summary contract violation on the failure path**, separate, and it suppressed the failure detail |
| performance scope | **not the issue** — 1080s is the normal cost, identical when passing |
| batches | **INCOMPLETE**, no sentinel |

**This is a functional test-isolation defect, not a performance benchmark exclusion**, so on
the evidence it does **not** qualify for NOT_APPLICABLE under the scope rule.

**Two candidate fixes, neither applied, both needing a successor subject and your decision:**

1. **Make the assertion establish its own precondition** — create the `results/`/`run/s2perf`
   state it expects before asserting exit 4, so it is order-independent. Assertion preserved.
2. **Assert the guard it actually reaches on a clean tree** — but that changes what is being
   tested, so it needs your judgement rather than mine.

Separately, the **summary contract violation** should be fixed so a failing run cannot hide its
own failing assertions.
