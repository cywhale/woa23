# Class B baseline on `143bf8c`, same odbadmin/VM24 environment — batches INCOMPLETE

**Three batches ran to completion, HEAD pinned to `143bf8c` throughout. They are NOT clean,
and no sentinel exists.** The runner refused one, correctly.

```
subject : 143bf8caae4aaa4cd4d4ef9ec0ddcab9ada1174d      label: base1
token   : base1-2203669-lin-171899282
batch 1 precheck/postcheck HEAD == subject  OK    exit 1   non-zero suites 13   (TOTAL 59)
batch 2 precheck/postcheck HEAD == subject  OK    exit 1   non-zero suites 12   (TOTAL 59)
batch 3 precheck/postcheck HEAD == subject  OK    exit 1   non-zero suites 12   (TOTAL 59)
tracked dirty : 0        untracked : 16
batches completed : 3 of 3      non-zero total : 40      postconditions : yes
sentinel_write: REFUSING -- 40 non-zero suite result(s) in the run.
NO SUCCESS SENTINEL — this run is not complete.
```

**Environment:** uid **1000** `odbadmin`, groups include **`root`**, `sudo`, `docker`;
worktree `/home/odbadmin/python/woa23-143bf8c-val` (`odbadmin:odbadmin 775`); `TMPDIR` unset;
**`/tmp` is `nginx:root 1777`, uid 121** — not root-owned; `/var/tmp` is `root:root 1777`.
Standard **polars 1.27.1**, no `polars-lts-cpu`, lock `0d2980a5…dccc69`, 58 distributions.

## 1. The attribution question is now settled

| | `0d96f7a` | `143bf8c` baseline |
|---|---|---|
| suites run | 60 | 59 |
| non-zero | **15** | **13 / 12 / 12** |

**The difference is exactly two suites: `test_environment.py` and `test_manifest.sh`** — the
two that enforce B6. They fail **only** on `0d96f7a`, and they are correct to.

**`test_s2_provenance.py` was in my earlier Class A list. That was wrong.** It fails on
`143bf8c` too, with the same 4 assertions — in **1 of 3** batches. It is environmental **and
flaky**, not caused by any dependency change. My earlier attribution was made by grepping the
suites for the string `polars`; those hits are fixture path strings, not distribution names.

## 2. Per-suite causes — 12 reproducible in all three batches

| suite | failed | cause, as measured | environment dependency |
|---|---|---|---|
| `test_clone_integrity.py` | 4 | checker reports `'/tmp is group-writable (drwxrwxrwt) and owned by uid 121, not 1000 — an ancestor N levels up'`; fixtures live under `TMPDIR` -> `/tmp` | **`/tmp` owner uid 121 (`nginx`), not 0** |
| `test_compare_arms.py` | 1 | `and that is exactly two problems, one per arm — expected 2, got 0` — same checker, opposite direction | same `/tmp` ownership |
| `test_s2perf_integration.py` | contract | latency gate returns **`FAIL_NO_ESTABLISHED_IMPROVEMENT`**; on that path the suite emits no `ASSERTIONS=` line, so the runner reports `SUMMARY CONTRACT VIOLATION` | **performance-gated**, host-speed dependent |
| `test_c1_readonly_account.sh` | 2 | `a readable, non-writable tree passes — expected 0, got 5` | **`odbadmin` is in the `root` group**, so a tree meant to be non-writable is writable |
| `test_cli.sh` | 3–6 | `no backticks survive inside the embedded python — got 190`; `no candidate/reference/scheduler process was left behind — got 9`; `no staging workdir was created — yes` | leftover processes / staging workdir; **cross-suite interference and account layout** |
| `test_d1_finalize.sh` | 14 | `it returns success — got 6`; `workers.json exists and is NOT empty — no`; counts read `[]` | **D1 run artifacts absent** under this account |
| `test_ports.sh` | 1 | `grep -vE '^[0-9]{2,5}\t'` — **GNU grep `-E` treats `\t` as literal `t`**, so no row can match and all **88** rows are counted bad. Verified: `grep -cP '^[0-9]+\t'` = **88**, i.e. every row *is* correct | **toolchain: GNU grep ERE.** A real defect, not a profile issue |
| `test_production_launcher.sh` | 1 | `this host's bash really is the strict one (3.2) — expected 3, got 5` | **asserts bash 3.2 (macOS)**; VM24 runs bash 5 |
| `test_production_stop.sh` | 1 | reproduces standalone (`ASSERTIONS=66 FAILED=1`) | account/path layout |
| `test_staging_bootstrap.sh` | 1 | `and it is refused as a symlinked PARENT, explicitly — got no` | staging tree/symlink layout |
| `test_staging_entry.sh` | 1 | `no /home/odbadmin path survived into it at all — expected 0, got 1` | **the suite asserts no `/home/odbadmin` path appears — we are running as `odbadmin`, under `/home/odbadmin`** |
| `test_staging_launcher.sh` | 7 | validation fails before reaching the interpreter (`gets as far as the interpreter — got no`, ×7) | staging identity / port ledger / store preconditions absent |

**Flaky, 1 of 3 batches:** `test_s2_provenance.py` (4 assertions). **Not reproducible in all
three, so it is NOT classified. It stays INCOMPLETE.**

### 2.1 Two findings that are defects, not profile mismatches

1. **`test_ports.sh` — `\t` inside an ERE.** `grep -E` does not interpret `\t`; the ledger is
   correct (88/88 rows start with digits + a real tab). The check can never pass on GNU grep.
2. **`test_d1_finalize.sh` is mode `100644` in git**, while every neighbouring suite is
   `100755`. It runs in batches only because `run_suites.sh` invokes shell suites as
   `bash "$suite"`. Invoking it as `./scripts/test_d1_finalize.sh` exits **126** — which is
   how I briefly misread it as passing standalone. Recorded so the next reader does not.

**Neither was fixed.** Both need a successor subject and review.

## 3. Proposed batch policy / test profile — FOR REVIEW, not applied

**No assertion was modified, relaxed, deleted or ignored, and none should be.** The suites are
not wrong about the environment they were written for; they are being run outside it. The
campaign's earlier batches ran under **`woa23c1ro`**, in its own tree, with `/tmp` root-owned
and a bash-3.2 reference host.

| # | proposal | covers |
|---|---|---|
| **P1** | Declare an explicit **test profile** in the runner (e.g. `staging-woa23c1ro` vs `odbadmin-vm24`) and have `run_suites.sh` record out-of-profile suites as **`SKIPPED (out of profile)`**, counted separately and never as passes | `test_c1_readonly_account.sh`, `test_staging_*.sh`, `test_production_launcher.sh`, `test_d1_finalize.sh`, `test_cli.sh` |
| **P2** | Make the environment preconditions **explicit and asserted up front** — `/tmp` ownership, bash version, D1 artifacts present — so a suite reports *"precondition not met"* rather than a failed behavioural assertion | `test_clone_integrity.py`, `test_compare_arms.py` |
| **P3** | Move **performance-gated** suites out of the correctness batch into their own gate | `test_s2perf_integration.py` |
| **P4** | Fix the two real defects (§2.1) in a successor subject | `test_ports.sh`, `test_d1_finalize.sh` mode |
| **P5** | Investigate the flaky suite before classifying it | `test_s2_provenance.py` |

**Each of P1–P4 changes files inside the subject, so each needs a successor subject and your
review. None is applied here.**

## 4. Status

| | |
|---|---|
| `143bf8c` clean batches | **NOT ACHIEVED — INCOMPLETE.** 3 batches completed, 40 non-zero results, **no sentinel** |
| Class B | **12 of 13 reproduce on `143bf8c` in this environment** with the causes above; **1 (`test_s2_provenance.py`) is flaky and stays INCOMPLETE** |
| Class A | **exactly 2** — `test_environment.py`, `test_manifest.sh` — fail only on `0d96f7a`, and correctly |
| `0d96f7a` | **abandoned**; its batches and LTS evidence are **not usable** and are not cited |
| assertions | **none modified, relaxed or ignored** |
| runner process control | **fixed outside the subject**; `pkill -f` banned, snapshot-first (pid, starttime), self-tested |
| production | **untouched** — no PM2, Nginx, TLS, config, store access, API request, or cleanup |
| C1/C2, performance | **not run** |

**Clean batches for `143bf8c` are not achievable in this environment without P1–P4.** That is
a policy decision for you, and the campaign record stays **INCOMPLETE** until it is taken —
not absorbed as success.
