# Status recorded, and a HALT: the successor reverses decided spec 012 (B6)

## 1. Status, as instructed

| item | status |
|---|---|
| `0d96f7a` polars-lts-cpu **focused validation** | **PASS** — 43 assertions, 0 failures, on VM24's CPU, `POLARS_SKIP_CPU_CHECK` unset |
| `0d96f7a` **successor batch validation** | **INCOMPLETE** |
| clean batches for `0d96f7a` | **NONE. Not claimed.** Batch 1 exited 1 with 15 non-zero suites; batches 2–3 did not complete |
| subject-bound sentinel for `0d96f7a` | **NOT produced. Not claimed.** |

---

## 2. HALT — the requested change would overturn a decided memo

**I have not modified `test_environment.py`, `test_s2_provenance.py` or `test_manifest.sh`,
and I have not run batches for a further successor.** Here is why, before anything else is
done.

`dev2026/specs/012-b6-avx2-polars-decision-memo.md` reads:

> **Status: DECIDED, 2026-08-20 by the PI. Mainline polars is retained at the validated
> version 1.27.1.** `polars-lts-cpu` is **not installed and not evaluated in this campaign**.

and §7:

> **The PI's decision, 2026-08-20: adopt the current mainline polars. Do not install or
> evaluate `polars-lts-cpu` in this campaign.**

**The three suites are not stale. They are the enforcement of that decision.**
`test_manifest.sh:97-100` says so in its own words:

```sh
check "polars is mainline 1.27.1, per the B6 decision" ...
check "polars-lts-cpu is NOT installed" "no" ...
```

**Changing those expectations to `polars-lts-cpu` would delete the assertions that guard a
recorded PI decision, and would leave spec 012 saying the opposite of what the suite
enforces.** That is not a minimal test update; it is the implementation of the option the
memo ruled out.

### 2.1 Successor `0d96f7a` implements the ruled-out option

`0d96f7a` installs `polars-lts-cpu` and removes mainline polars — **exactly what §7 rules
out and §8's boundary line ("`polars-lts-cpu` is not installed and not evaluated") forbids
for this campaign.** Its three "failures" in batch 1 are the campaign's own guards firing
correctly.

### 2.2 B6's reopening conditions — none is met

Spec 012 §10 lists what reopens it. Against what this campaign has actually observed:

| # | reopening condition | met? |
|---|---|---|
| 1 | hardware/hypervisor change on the host (e.g. AVX2 unmasked) | **no** — masking unchanged |
| 2 | a polars upgrade to any version other than 1.27.1 | **no** — 1.27.1 throughout |
| 3 | an observed SIGILL / illegal-instruction crash in a woa23 process | **no** — none observed |
| 4 | an incorrect result attributable to the library | **no** — none observed |
| 5 | a significant performance regression traceable to the CPU baseline | **no** — not measured, not claimed |

**What I reported as a new blocker was the import warning. Spec 012 §9 accepts precisely
that warning, by name, and says it is "not silenced, not suppressed, and not argued away —
it is accepted."** On the present evidence **B6 stands and was never reopened.**

## 3. A correction to my own earlier report

**I stated that production was unaffected because its shared pyenv runs `polars-lts-cpu`.
That was wrong.** Measured read-only just now, on production's interpreter:

```
polars.__version__               : 1.27.1
packages_distributions[polars]   : ['polars', 'polars-lts-cpu']    <-- BOTH dist-infos
AVX2 warning emitted             : YES
```

**Production emits the same AVX2 warning.** The presence of a `polars_lts_cpu` dist-info does
not mean the LTS build is what loads; the warning proves the mainline build is active. This
matches spec 012 §9, which already recorded that the risk "applies to production's
interpreter too".

**So the comparison that motivated calling this a new blocker — "production is protected, the
new venv is not" — does not hold. Both are in the same accepted-risk state.** I did not check
`dev2026/specs/` for a prior decision before proposing the successor; had I done so, spec 012
would have been the starting point rather than a late discovery.

## 4. What I did NOT do

- did **not** edit `test_environment.py`, `test_s2_provenance.py`, `test_manifest.sh`;
- did **not** create a further successor subject;
- did **not** run batches for one;
- did **not** modify `0d96f7a`, its lock, its dependencies, or the focused LTS suite;
- did **not** set `POLARS_SKIP_CPU_CHECK`, rerun C1/C2, or run a performance test;
- did **not** touch production, PM2, Nginx, TLS, the store, or the API.

**The runner process-control fix and the Class B baseline are also not started**, because
both are work in service of validating a subject whose premise is now in question. Neither is
blocked technically; they are held pending the decision below.

## 5. The decision needed

**This is yours, and it is a spec decision rather than a test-maintenance one:**

| option | what it means |
|---|---|
| **A — B6 stands** | `0d96f7a` is **abandoned**, not merged and not batched. The three suites stay as they are, because they are correct. The AVX2 warning remains accepted residual risk. The cutover artifact returns to **`143bf8c`**, whose provisioned runtime is already in place and verified |
| **B — reopen B6** | spec 012 is **revised** to record the reopening and the new decision, *then* `0d96f7a`'s successor updates the three suites to match. The memo's own §10 says B6 "does not survive its preconditions" — so reopening should be written down, not implied by changing tests |

**If B, the reopening reason should be stated explicitly**, since none of §10's five
conditions is currently met — the honest reason would be a change of judgement about the
accepted risk, not new evidence.

**Under either option the three suites and spec 012 must agree.** Right now `0d96f7a` makes
them disagree, which is the state that should not be carried forward.
