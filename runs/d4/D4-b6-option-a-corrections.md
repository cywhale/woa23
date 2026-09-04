# Option A adopted: B6 stands. `0d96f7a` ABANDONED. Artifact returns to `143bf8c`.

## 1. Subject status

| subject | status |
|---|---|
| **`143bf8c`** | **the D-4 runtime artifact.** Standard **polars 1.27.1** retained · `uv.lock` **unchanged** (`0d2980a5…dccc69`) · **no** `polars-lts-cpu` · **no** `POLARS_SKIP_CPU_CHECK` |
| **`0d96f7a`** | **ABANDONED / SUPERSEDED.** Not merged, not deployed, not batched to completion. It implements the option spec 012 §7 rules out |

**`test_environment.py`, `test_s2_provenance.py` and `test_manifest.sh` are NOT modified.**
They enforce the B6 decision and are correct as they stand. On `143bf8c` they have nothing to
fail against — the tree and the lock are the ones they were written for.

**`0d96f7a`'s 43-assertion LTS focused validation is an INDEPENDENT EXPLORATORY RECORD only.**
It is not evidence for `143bf8c`, is not back-filled, and no batch, sentinel or CPU claim from
it may be cited anywhere in the D-4 record.

## 2. Corrections to the record

**1. Production loads the STANDARD polars.** Measured read-only on production's interpreter,
`/home/odbadmin/.pyenv/versions/py311/bin/python3.11`:

```
polars.__version__             : 1.27.1
polars.__file__                : …/py311/lib/python3.11/site-packages/polars/__init__.py
packages_distributions[polars] : ['polars', 'polars-lts-cpu']
```

**2. Production also emits the AVX2 warning.** The same read-only import reported
**`AVX2 warning emitted: YES`**. Production is in the same state as any other environment on
this masked host.

**3. A `polars_lts_cpu` dist-info being present does NOT mean the LTS build is in use.**
Production carries **both** dist-infos over a single `polars/` module directory. The warning
proves the **mainline** build is what loads. My earlier inference — that the dist-info implied
production ran the LTS build, and was therefore protected — **was wrong, and is withdrawn.**

**4. B6's "mainline polars retained" remains the current decision.**
`dev2026/specs/012-b6-avx2-polars-decision-memo.md` is **DECIDED, 2026-08-20**, and stands
unchanged. `polars-lts-cpu` is not installed and not evaluated in this campaign.

**5. The AVX2 warning is a KNOWN, ACCEPTED, UNRESOLVED runtime risk. It must NOT be written
as a CPU-safety PASS.** Spec 012 §9 accepts it explicitly: the risk is **unquantified**, the
failure mode is a **dead worker (SIGILL), not a wrong answer**, it applies to **production's
interpreter too**, and no absolute performance figure from this campaign is
production-representative while the masking stands. Nothing in this campaign — including
`0d96f7a`'s passing suite — converts that into a safety result.

**6. `polars-lts-cpu` may be re-evaluated only after B6 is explicitly reopened.** Spec 012
§10 lists the five reopening conditions; **none is currently met.** Until one is met and the
reopening is recorded in the spec, the LTS variant stays out of scope.

## 3. What was corrected about my own process

I proposed `0d96f7a` without first checking `dev2026/specs/` for a prior decision on the same
question. Spec 012 had already decided it, five days before, and the three suites I read as
stale were its enforcement. The check that would have caught this is reading the specs for the
subject area **before** proposing a dependency change, not after a batch failure surfaces the
guards.

## 4. Runner process control — FIXED, outside the subject

The previous stop used `pkill -f "run_suites.sh"`, whose pattern matched the argv of the shell
executing it. The observer entered its own matcher and the session was killed — **the 8th
occurrence of that class in this campaign.**

**The fix lives in `procctl.sh`, outside the subject**, so no successor subject was needed and
**`143bf8c` is not modified**.

| rule | implementation |
|---|---|
| **`pkill -f` and `pgrep -f` are BANNED** | no pattern is matched against any command line, anywhere in the driver path |
| **snapshot first** | the driver's `(pid, starttime)` is recorded from `/proc` at launch, before any waiting begins |
| **stop by verified identity only** | `stop` re-reads `/proc/<pid>`, and signals **only** if the starttime still matches the recorded one |
| **pid reuse cannot be hit** | a changed starttime is a refusal, not a signal |
| **no observer argv in any matcher** | descendants are found via `/proc/<pid>/stat` ppid, not by matching text |

`starttime` is parsed **after the final `)`** of `/proc/<pid>/stat`, because `comm` may contain
spaces and parentheses and field-splitting the whole line is wrong.

**Self-tested before use:**

```
recorded identity: 2203646 lin-171899281
alive check: OK (same process)
REFUSING: pid 2203646 is now lin-171899281, recorded lin-999999999
          -- pid reuse, nothing signalled          (exit 3, correct)
```

## 5. Scope held

No production cutover · no PM2 start/stop · no `PM2_HOME` · no store symlink or store access ·
no Nginx/TLS/config change · no production API request · no C1/C2 rerun · no performance test ·
no cleanup of retained state · no `POLARS_SKIP_CPU_CHECK` · **no assertion was modified,
relaxed, deleted or ignored** to absorb any failure.
