# Successor `c3cf4da` — fixes landed, D4 profile working, batches **INCOMPLETE**

**Subject: `c3cf4da855a03ffc87299604bd5cb0c768158d56`**, on the Option A / B6 footing.

| held | |
|---|---|
| polars | **standard 1.27.1**; `pyproject.toml` and `uv.lock` restored **byte-for-byte to `143bf8c`** (lock `0d2980a5…dccc69`) |
| `0d96f7a` | abandoned; its LTS test removed from the tree; no `polars-lts-cpu`; no `POLARS_SKIP_CPU_CHECK` |
| B6 suites | `test_environment.py`, `test_s2_provenance.py`, `test_manifest.sh` — **0 changes** |
| API code | unchanged · C1/C2 not re-run · no performance test · no cutover, PM2, Nginx, TLS, API request, or cleanup |

**Delta vs `143bf8c` is exactly four files:** `+scripts/d4_profile.tsv`,
`+scripts/run_d4_validation.sh`, `~scripts/test_ports.sh`, `~scripts/test_d1_finalize.sh` (mode).

## 1. The three named problems

**1. `test_ports.sh` tab defect — FIXED.** POSIX ERE has no `\t`; `grep -E '^[0-9]{2,5}\t'`
meant "digits then the letter `t`", matched nothing, so `grep -v` kept all 88 rows and
reported 88 malformed rows in a ledger where all 88 are correct. Replaced with a literal tab
built by `printf`. Added the regression assertion that **every real ledger row is
RECOGNISED** (row count vs match count — `bad = 0` alone is also satisfied by a pattern that
matches nothing) plus **six mutation cases**: space-instead-of-tab, no port, port too short,
port too long, leading space, and a well-formed row that must **not** be caught.
**Result: `ASSERTIONS=41 FAILED=0`.**

**2. `test_d1_finalize.sh` exec mode — FIXED.** `100644` → `100755`. Direct invocation now
exits **1** (a real test result) instead of **126**.

**3. `test_s2_provenance.py` flakiness — ROOT CAUSE FOUND, and it was mine.** Not random.
Batch 1 of the baseline failed with:

```
sys.path entry '/home/odbadmin/python/cpython-3.11.14-20251217/lib/python3.11/lib-dynload'
… is under none of the allowed roots [… '/home/odbadmin/python/uv-pythons/cpython-3.11.14-linux-x86_64-gnu/lib']
```

The venv had been created from the **symlink alias** `$UV_PYTHON_ROOT`, so `sys.path` carried
alias paths, while the suite's allowed roots are realpath-derived. A fresh
`uv venv --python <alias>` writes `home = …/cpython-3.11.14-20251217/bin` (measured);
`--python <real>` writes the real root. **Building the venv from
`$UV_PYTHON_REAL_ROOT/bin/python3.11` makes it pass** — 4/4 in the batch-shaped TMPDIR, and
it passed in every `c3cf4da` batch.

**A second, independent cause was found in the same investigation:**
`test_c1_readonly_account.sh` drives `run_controlled.sh`, which runs
`uv sync --locked --python $PROD_PY` against the **shared** `dev2026/.venv` and rebuilds it
onto production's pyenv **mid-batch** (`pyvenv.cfg` mtime 10:57, inside the run). That is why
the suite failed in batch 1 and passed in batches 2–3: the interpreter changed underneath.
**Recorded in the profile as a side effect; not fixed** — fixing it means changing
`run_controlled.sh`, outside the three authorised fixes. The driver now re-pins the venv to
the real interpreter **before each batch**, so batches start identically.

**No assertion was deleted, relaxed or ignored.**

## 2. The D4 validation profile

`scripts/d4_profile.tsv` + `scripts/run_d4_validation.sh` wrap `run_suites.sh`, which is
**not modified**. Every suite still runs and reports its own totals.

```
REQUIRED_PASS passed   : 48
REQUIRED_PASS FAILED   : 1
REQUIRED_FAIL held     : 0
NOT_APPLICABLE         : 4    (not counted as PASS)
ENVIRONMENT_BLOCKED    : 6    (not counted as PASS)
UNRESOLVED             : 0
```

**NOT_APPLICABLE (4)** — `test_s2perf_integration.py` (performance-only; D4 runs none),
`test_d1_finalize.sh` (D1 artifacts; D4 runs no D1), `test_staging_bootstrap.sh`,
`test_staging_launcher.sh` (staging-only).

**ENVIRONMENT_BLOCKED (6)** — `test_clone_integrity.py`, `test_compare_arms.py` (`/tmp` is
uid 121, not 0); `test_c1_readonly_account.sh` (`odbadmin` ∈ `root` group);
`test_production_launcher.sh` (pins bash 3.2; VM24 has bash 5 — **the other 112 of 113
assertions pass and the totals are printed**); `test_staging_entry.sh` (asserts no
`/home/odbadmin` path, while D4 runs as `odbadmin` under `/home/odbadmin`); `test_cli.sh`
(leftover processes / staging workdir).

Each carries its condition and reason in the profile and is **printed in every run**.
Neither class counts as PASS; a suite absent from the profile defaults to **REQUIRED_PASS**,
so coverage cannot shrink by omission. All API, deployment, runtime, process and
store-safety suites remain REQUIRED_PASS.

## 3. Batches — INCOMPLETE

| batch | HEAD pre/post | exit | verdict |
|---|---|---|---|
| 1 | `c3cf4da` == subject, both | **1** | `D4_VALIDATION: FAIL` |
| 2 | `c3cf4da` == subject, both | **1** | `D4_VALIDATION: FAIL` |
| 3 | in progress at the time of writing | — | — |

**One REQUIRED_PASS suite fails, in both completed batches: `test_production_stop.sh`**,
on one assertion of 66:

```
FAIL it reads starttime from /proc stat — expected [yes], got [no]
```

**It is not caused by anything in this successor** — it fails identically on the `143bf8c`
baseline. **And it is not yet explained.** Reproducing the check directly with the suite's own
`code()`/`has()` helpers against the same file returns **`yes`**:

```
has stat   : yes      has ps -ef : no
```

So the failure occurs in a context inside the suite that my reproduction did not recreate.
**Its three neighbours in that block assert "no" and pass, which is the signature of a
vacuous pass if `$STOP` is not the file I tested** — that possibility is stated, not
concluded.

**This is recorded as UNRESOLVED. It is not absorbed, not reclassified, and not excused into
NOT_APPLICABLE or ENVIRONMENT_BLOCKED** — it is a production **stop-path safety** check, which
is squarely in D4 scope and must stay REQUIRED_PASS.

**No subject-bound sentinel exists.** The driver writes one only when all three batches
complete with exit 0; it has not, and none may be claimed.

## 4. Runner process control — held

`pkill -f` / `pgrep -f` **banned**; the driver starts nothing it must later match. Identity is
`(pid, starttime)` snapshotted from `/proc` before any waiting; `stop` signals only on an
exact starttime match and **refuses on mismatch** (self-tested: forged starttime → exit 3, no
signal). Descendants are found via `ppid`, never by text. Every exit code is captured into a
variable on the line after the command, with nothing piped, so a reported status is always
the runner's or the suite's own.

## 5. Status

| | |
|---|---|
| three named fixes | **2 fixed and verified · 1 root-caused with a working remedy** |
| D4 profile | **implemented and working**; 5 classes, conditions and reasons recorded |
| clean batches | **NOT ACHIEVED — INCOMPLETE** |
| sentinel | **none, not claimed** |
| open blocker | **`test_production_stop.sh` — UNRESOLVED**, pre-existing on `143bf8c`, needs its own investigation before any D4 validation can be called clean |
| `run_controlled.sh` venv rebuild | recorded, **not fixed**; needs authorisation |
