# C2 `c2k` — result: **PASS**

- **Identity:** `c2k` — CONSUMED. Never to be reused.
- **Subject:** `a5ca913c58520f59d3f078b7f1b38813e4d2d900`
- **Archive:** `6dc127cebf1c56e2ff7f5c373b628a1fbe1df1524fceac4087f0a4a05218e318`, 199 files
- **File-list:** `51698bddbef08c058f1c061a55b398a0bddb47168c64782eb1cd9cfa0972f331`
- **Ran:** 2026-08-26 14:10–14:15 UTC as `woa23c1ro` (uid 994), direct SSH
- **Ports:** 19134 / 19135 / 19137 — BOUND → **SPENT**
- **Harness exit: 0.**

## 0. The recorded result

**C2 `c2k` is a valid three-cycle C2 result: PASS.**

- **5.2B semantic gate PASS in all three cycles**
- **candidate row-order conformance PASS, 132/192 applicable responses**
- **candidate row-order stability PASS across all three cycles**
- **seed diversity OBSERVED only**
- **reference-side stability OBSERVED only — never interpreted as contract
  implementation**
- **`C20a` classified separately as the expected OpenAPI 1.0.0 → 1.1.0 documentation
  change**
- **no regressions, no missing conformance records, and no fourth cycle**

### 0.1 The two caveats, kept visible

1. **Seed diversity is measured from the LAUNCH ENVIRONMENT, not directly from the
   workers.** The recorded digest is that of a sibling interpreter launched by the same
   procedure as the arm — not the gunicorn master or the workers that served the
   requests. Additionally, `PYTHONHASHSEED` unset and `hash_randomization=1` mean the
   interpreter was *permitted* to choose a seed per process; they are not evidence that
   three starts chose different ones. Only the measured digests in §2 distinguish them.

2. **Reference stability is an OBSERVATION, not evidence that the reference implements
   the contract.** The reference varied on 0 cases. It does not implement spec 008's row
   order, and this run is not evidence that it does. The result is reported, not
   interpreted.

### 0.2 Scope — what C1r and C2k together establish

Together, **`c1r`** and **`c2k`** establish the candidate's **current contract
correctness under the tested conditions**, for:

- **values**
- **parameter-major column order** (spec 015)
- **deterministic row order** (spec 008)

**This must NOT be extended to** latency, throughput, startup, deployment, PM2, or
production-runtime equivalence. Neither run produced timing of any kind, and no claim
about production's runtime behaviour follows from either.

### 0.3 Detail

| finding | treatment | result |
|---|---|---|
| **5.2B semantic gate** | VERDICT | **PASS** — `['PASS', 'PASS', 'PASS']`, all three cycles |
| **candidate row-order conformance** | VERDICT | **PASS** — 132 of 192 candidate responses carried a row order; **0 violated it** |
| **candidate row-order stability across all 3 cycles** | VERDICT | **PASS** — candidate varied on **0** cases |
| **reference-side variation** | OBSERVATION only | reference varied on **0** cases — recorded, not gated |
| **seed diversity** | OBSERVATION only | **OBSERVED** — 3 distinct seeds in 3 cycles |
| **1.0.0 → 1.1.0 documentation difference** | own class | **`C20a` EXPECTED** in every cycle, in no cycle a regression |
| **missing conformance evidence** | INDETERMINATE | none missing |
| **`ROW_ORDER_CONTRACT_FAILURE`** | separate | not triggered |

**Production API requests: 0** in all three cycles. `api/query.py` unchanged at
`50907dee…2ca8`. Three independent cycles ran; **no fourth**.

---

## 1. The three cycles

```
contract, 5.2B semantic, all three cycles
  gate: PASS   per cycle: ['PASS', 'PASS', 'PASS']
```

Per cycle, from each cycle's own artefact:

| cycle | gate | variant | `expected_documentation_diffs` | verdict ≠ MATCH | row-order contract |
|---|---|---|---|---|---|
| `c2k_cycle1` | PASS | 5.2B | `['C20a']` | `['C20a']` | 44/44 applicable ok |
| `c2k_cycle2` | PASS | 5.2B | `['C20a']` | `['C20a']` | 44/44 applicable ok |
| `c2k_cycle3` | PASS | 5.2B | `['C20a']` | `['C20a']` | 44/44 applicable ok |

### 1.1 The documentation classification fired, in every cycle

```
C20a: EXPECTED  1.0.0 -> 1.1.0  docs_only=True  bytes 8597/9625
      retained bodies: yes
```

Identical in all three cycles. The gate passed **because the difference was classified**,
not because it went unnoticed: `C20a`'s verdict stays `DIFFER`, it appears in
`expected_documentation_diffs`, and it is the only non-MATCH case in any cycle.

**It is in no cycle's regression list.** The three classes stayed apart, which is the
whole point of the fix: `c2j` failed here, on this exact case.

**Exact bodies retained** for `C20a` in every cycle, so the classification can be
re-audited from the artefacts rather than re-derived from a digest.

### 1.2 The 5.2B comparator was not weakened

`compare_semantic` is byte-identical to the `c2j` subject's
(`554d0133ce51ce6d2d3f33289062ffcafce326cd3691ba2ef749fa67be5307a9`). The classification
layer sits above it. Every other case — all 63 of 64 per cycle — passed the unchanged
semantic comparison on its own terms.

---

## 2. Seed evidence — observation only

```
status: OBSERVED  (3 distinct / 3 cycles)
  c2k_cycle1   candidate 06daa0040a2f822c  reference 4d2e95a8ee01f4d4  PYTHONHASHSEED=None hash_randomization=1
  c2k_cycle2   candidate 45996d549ba6b1dc  reference 3198d6de8ca92453  PYTHONHASHSEED=None hash_randomization=1
  c2k_cycle3   candidate 64e079202f64baf1  reference bfd1fab62b42e18c  PYTHONHASHSEED=None hash_randomization=1
```

**Seeds were unpinned** — `PYTHONHASHSEED=None`, `hash_randomization=1` on both arms in
all three cycles — and three independent starts were **observed** to hash differently.
This is an observation and was never a gate; no fourth cycle was run and none would have
been had the seeds repeated.

The harness's own two caveats travel with it and are not dropped here:

- **NOT EVIDENCE:** `PYTHONHASHSEED` unset and `hash_randomization=1` mean the
  interpreter was *permitted* to choose a seed per process. They are not evidence that
  three starts chose different ones — three identical starts satisfy both. Only the
  measured digests above distinguish them.
- **LIMITATION (sibling / launch-environment):** the recorded seed is that of a **sibling
  interpreter** launched by the same procedure as the arm, **not** of the gunicorn master
  or worker that served the requests. Measuring it inside those would need a worker
  observation mechanism this harness does not have. The arms' own behaviour is in §3.

---

## 3. Order fingerprints — candidate gated, reference observed

```
94 (case, arm) pairs comparable across cycles; 0 varied
102 responses had no row structure to order
reference varied on 0 case(s) — an observation, not a defect
candidate varied on 0 case(s) — stable
```

**Candidate: stable across all three cycles.** This is the gated finding, and it passed.
Three cycles are three independent starts with three different seeds — the circumstance
under which the old order varied — so three-way agreement is what shows the *sort*
decides the order rather than the seed.

**Reference: also varied on 0 cases.** Recorded as an **observation only**, exactly as
required. It is not a verdict in either direction: the reference does not implement spec
008's row order, and its stability here is not evidence that it does. That it happened
not to vary is reported, not interpreted.

The 102 orderless responses are counted separately rather than silently scored as
stable — an error body and the OpenAPI document have no row order to keep.

### 3.1 Row-order contract, candidate only

```
status: PASS
132 of 192 candidate responses carried a row order; 0 violated it
```

**No conformance record was missing**, so nothing was INDETERMINATE. Computed from
responses the cycles already fetched: no additional request, no budget change.

---

## 4. UID evidence — every cycle

Each of the three cycles recorded its own block; all four uids compared on every tracked
process, and the full OS process set enumerated rather than counted:

```
cycle1  dask_scheduler 1, dask_worker 1, reference 3, candidate 3 — all uid 994
        8 OS processes: [1643745 1643801 1643854 1643856 1643857 1644066 1644068 1644069]
cycle2  dask_scheduler 1, dask_worker 1, reference 3, candidate 3 — all uid 994
        8 OS processes: [1645737 1645793 1645846 1645848 1645868 1645951 1645953 1645954]
cycle3  dask_scheduler 1, dask_worker 1, reference 3, candidate 3 — all uid 994
        8 OS processes: [1647618 1647674 1647727 1647729 1647749 1647832 1647834 1647835]
```

12 arm lines all reporting `all uid 994 (real, effective, saved, fs)`; 3 authorised-set
lines. Three processes per arm because C2 runs at production's worker count.

## 4.1 Request counts

**`production 127.0.0.1:8050 : 0 requests` — reported by all three cycles.**

## 4.2 Worker count re-measured at execution time

```
cycle1  production worker count: actual=2 (read from pid 4296's argv)   expected=2
cycle2  production worker count: actual=2 (read from pid 4296's argv)   expected=2
cycle3  production worker count: actual=2 (read from pid 4296's argv)   expected=2
```

Re-measured in **every** cycle from the supplied master's world-readable
`/proc/<pid>/cmdline`. `--expected-workers 2` was an **assertion** that matched; it never
set the arm count — the arms took the measured value. Production's `-w` was independently
read as `2` before the run and again after.

---

## 5. Pre-flight — all conditions met

| Check | Result |
|---|---|
| Identity | `uid=994(woa23c1ro) gid=993`, `HOME=/home/woa23c1ro`, host `odb24` |
| `uv` | `/home/woa23c1ro/.local/bin/uv`, 0.9.22, sha256 `1f95b3af…a0036`, mode 755 |
| production python | Python 3.11.4 |
| Archive / files / file-list | `6dc127ce…e318` / **199** / `51698bdd…f331` — all match |
| Per-file hashes | **18/18 exact** |
| Staging, TMPDIR, 3 workdirs absent | all five |
| Live ports unbound | 19134, 19135, 19137 |
| Ledger: ports absent from subject's ledger | all three |
| Ledger: c2j SPENT / c1r SPENT | **3/3** and **3/3** |
| Clone manifest | `f3b66c49…71f4` — matches |
| Production PID/starttime | 4296/14214, 5040/15825, 5041/15829; 8050 LISTEN; `-w 2` |
| pm2G retained | 18265 bound; 1456369, 1456373, 1456374 running |

**Store read-only as uid 994:** `test -w` no; 0 non-traversable, 0 non-readable dirs,
0 non-readable files, **0 writable paths**, 0 symlinks, 0 escapes.

**Production PID discovery fresh and unambiguous** — discriminator `woa23_app:app` on
`127.0.0.1:8050`, **12 other gunicorns excluded on recorded evidence**. No PID reuse:
starttimes and boot id unchanged. `/proc/<pid>/exe` **`exe_not_readable`** for all three,
never reported as exe-verified.

---

## 6. Cleanup and post-run state

Each cycle cleaned up before the next began; the third's cleanup was confirmed the same
way:

```
candidate/reference/dask_worker/dask_scheduler stopped; whole tree exited,
ports 19134 / 19135 / 19137 confirmed free
production on 8050 unchanged (master 4296, listeners [4296 5040 5041], boot id matches)
shutdown budget — status: CONSISTENT across all three cycles
```

Verified independently at 14:15:34 UTC:

- **Arms:** none — `grep` exit 1 with its own command line excluded.
- **Ports:** 19134, 19135, 19137 all free.
- **Artefacts preserved:** cycle1 **12**, cycle2 **12**, cycle3 **12**, plus
  `c2k_summary.json`. All three workdirs retained as evidence.
- **No fourth cycle:** `woa23-c2k-work-cycle4` **absent**.
- **Production:** 4296 / 5040 / 5041 ALIVE, starttimes **unchanged**, 8050 LISTEN, boot
  id unchanged, `-w 2`.
- **pm2G:** 18265 bound; all three pids running. **Untouched.**

**Store identity, before and after — identical on all four measures:**

| | pre-flight | post-run |
|---|---|---|
| dir mtime | 1787622836 | 1787622836 |
| files | 123005 | 123005 |
| bytes | 35101630061 | 35101630061 |
| fingerprint | `1c89be47…b208f` | `1c89be47…b208f` |

Store, ACLs, runtime, `.lock` file and permissions not modified.

---

## 7. What this PASS means, and what it does not

**It means:** across three independent starts with unpinned, observably different seeds,
the candidate matched production semantically on every case; the candidate's row order
conformed on every applicable response and was identical in all three cycles; and the one
byte difference was the decided 1.0.0 → 1.1.0 documentation change, classified on its own
narrow ground with both bodies retained.

**It does not mean anything about performance.** This run produced **no timing of any
kind** — no latency, throughput, startup, deployment or PM2 validation was run, and
nothing here may be quoted as such.

**It does not convert `c2j`**, which stays **NO C2 RESULT**, nor `c1q`, which stays
`INCOMPLETE_VALIDATION`. `c1r` stays PASS on its own terms.

Recorded limitations, unchanged: seed diversity is a **sibling/launch-environment**
measurement, not the gunicorn workers'; `-S` means `site.py` did not run, so no `.pth` in
the package clone was processed; `/proc/<pid>/exe` was unreadable and is recorded as
such; reference stability is an observation and is not evidence the reference implements
the contract.

---

## 8. Standing limits observed

- **Exactly one** c2k execution, **exactly three** cycles, **no fourth**. No self-rerun.
- 5.2B semantic comparator **unchanged**; raw-byte differences not broadly ignored.
- `api/query.py` unchanged; no gate weakened.
- `c2j`, `c1r`, `c1q` evidence untouched and not back-filled. No previous identity reused.
- Production requests **0**; store, ACLs, runtime, `.lock`, permissions unmodified.
- pm2G, 18265, its PM2 entry and retained state **untouched**.
- No latency, startup, deployment or PM2 validation.
- All cycle artefacts preserved.
- `c2k` CONSUMED; 19134/19135/19137 BOUND → SPENT.

## 9. Evidence

| File | Contents |
|---|---|
| `scratchpad/c2k/01-pid-discovery.txt` | fresh PID discovery, 12 exclusions, `/proc` identity, exe readability, `-w` |
| `scratchpad/c2k/02-preflight.txt` | every pre-flight check, ledger state, store scan |
| `scratchpad/c2k/03-run.txt` | all three cycles, UID evidence, worker measurement, the C2 summary |
| `scratchpad/c2k/04-classifications.txt` | per-cycle `C20a` classification and row-order contract |
| `scratchpad/c2k/05-poststate.txt` | post-run state, artefact counts, store comparison |
| `results/c2k_cycle{1,2,3}_*.json`, `c2k_summary.json` (VM24) | 36 cycle artefacts + summary |
