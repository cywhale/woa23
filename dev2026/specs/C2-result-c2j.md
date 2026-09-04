# C2 `c2j` — result: **NO C2 RESULT** (cycle 1 stopped the run)

- **Identity:** `c2j` — CONSUMED. The single authorised execution is spent.
- **Subject:** `c7312130d0957ccf9163c6c4af3c187c2ac3a983`
- **Archive:** `b96678757d2663627e5f4bc760676dfa7b0f0934f677e1a4b2b096bb0f646c02`, 196 files
- **File-list:** `0695d3bf2de7b46ed62c4ea8172f08fc7deaa655115d85684f78b3262bc9dbe1`
- **Ran:** 2026-08-26 12:04–12:08 UTC as `woa23c1ro` (uid 994), direct SSH
- **Ports:** 19131 / 19132 / 19133 — BOUND by cycle 1 → **SPENT**
- **Harness exit: 1.**

## 0. The recorded result

**There is no C2 result.** A C2 result is three cycles; **cycle 1 failed the 5.2B gate
and the wrapper stopped**, exactly as designed — *"two cycles plus a failure is not two
thirds of an answer."* Cycles 2 and 3 **never ran**.

Therefore **none** of the following exist for `c2j`, and none may be quoted:

- no 5.2B verdict
- no candidate row-order **stability across three cycles** (one cycle cannot show it)
- no seed-diversity observation
- no reference-side variation observation

**This is NOT a candidate failure.** The single differing case is `C20a`, the decided
API/OpenAPI 1.0.0 → 1.1.0 documentation change that `c1r` proved is documentation-only.
The failure is that **the 5.2B semantic gate has no way to say "this difference was
decided"** — a gap in my harness, in the 5.2B path.

**`api/query.py` is unchanged** at `50907dee…2ca8`.

---

## 1. Exactly what happened

Cycle 1 ran all 64 cases. **63 of 64 MATCH.** One differed:

```
variant: 5.2B   gate: FAIL
C20a   verdict=DIFFER   200/200   8597/9625 bytes
   note: non-row payload differs (8597 vs 9625 bytes); compared as raw bytes
         because there is no row structure to compare semantically
   raw_difference: None   <-- 5.2B does not classify at all
```

```
contract gate: FAIL
  differing: C20a
cycle 1 exited 1. Stopping: a C2 result is three cycles, and two
  cycles plus a failure is not two thirds of an answer.
```

### 1.1 Why — and why it is not the candidate

`C20a` is `GET /api/swagger/woa23/openapi.json`. Reference emits `info.version 1.0.0`
(8597 bytes); candidate emits `1.1.0` (9625 bytes). This is spec 008 rev 6's **own
decided publication**, and rev 6 said in terms that applying it *"changes api/ and would
require a new C1/C2"*.

`c1r` classified exactly this case as `EXPECTED_DOCUMENTATION_CHANGE`, `docs_only: true`
— *identical once version, summary and description are stripped: no route, parameter,
schema or response moved* — and accepted it on that narrow, proven ground.

**But that classification lives in the 5.2C path**, `classify_raw_difference`. C2 runs
the **5.2B** path, `compare_semantic`, which for a non-row payload falls back to raw-byte
comparison and carries **no notion of a decided change** — `raw_difference` is `None`
because 5.2B never classifies.

So this is the c1p situation again, in the other gate. c1r taught 5.2C about the decided
documentation change; **5.2B was never taught**, and I did not check that before
submitting the request. The request asserted C2's gates were ready; on this point it was
wrong.

### 1.2 What I have NOT done about it

The authorisation says **"keep the 5.2B semantic gate unchanged."** I have not modified
it, and I will not without an explicit decision. The options are the PI's to choose, not
mine to take.

---

## 2. What cycle 1 did establish

Recorded because it is real evidence, and clearly labelled as **one cycle only**.

### 2.1 Cycle 1 results

| | |
|---|---|
| cases | 64 |
| MATCH | **63** |
| DIFFER | **1** (`C20a`, §1.1) |
| candidate row-order contract | **44/44 applicable, all ok** |

**Candidate row-order conformance passed on every applicable response in cycle 1.**
Stability across three cycles is a different question and **was not answered**.

### 2.2 Seed evidence — one cycle, no distribution

```
candidate  seed_digest=4552c6321b5b9347ae3ebb9b…   hashseed_env=None
reference  seed_digest=bd73134020b85926ad368bff…   hashseed_env=None
```

**Seeds were unpinned**, as C2 requires (`hashseed_env: None` on both arms). The two
digests differ, which is expected between two processes. **One cycle is one seed, and
one seed is not a distribution** — no seed-diversity observation exists.

### 2.3 Order fingerprints

Per-case `reference_order` and `candidate_order` fingerprints were recorded for all 64
cases in `c2j_cycle1_contract.json`. **They cannot be compared across cycles**, because
there are no other cycles. No stability statement, for either arm, is possible.

### 2.4 UID evidence

```
dask_scheduler : 1 process(es), all uid 994 (real, effective, saved, fs)
dask_worker    : 1 process(es), all uid 994 (real, effective, saved, fs)
reference      : 3 process(es), all uid 994 (real, effective, saved, fs)
candidate      : 3 process(es), all uid 994 (real, effective, saved, fs)
8 OS processes, matching the authorised set:
  [1633076 1633132 1633185 1633187 1633207 1633283 1633285 1633286]
```

All four uids on every tracked process; the full set enumerated, not counted. Three
processes per arm here, against two in C1, because C2 runs production's worker count.

### 2.5 Request counts

**Requests to production `127.0.0.1:8050`: 0.**

### 2.6 The wrapper fix worked

```
production worker count: actual=2 (read from pid 4296's argv)
                        expected=2
```

The `82ecd44` fix did its job: `--prod-dir`, `--store`, `--prod-python` and
`--prod-pids` reached the cycle, the run was **not** refused, and the worker count was
measured from the supplied master's argv as a non-owner — the thing §1.2 of the request
predicted would work.

---

## 3. Pre-flight — all conditions met

| Check | Result |
|---|---|
| Identity | `uid=994(woa23c1ro) gid=993`, `HOME=/home/woa23c1ro`, host `odb24` |
| `uv` | `/home/woa23c1ro/.local/bin/uv`, 0.9.22, sha256 `1f95b3af…a0036`, mode 755 |
| production python | `…/py311/bin/python3.11` → Python 3.11.4 |
| Archive | `b9667875…6c02` — **matches authorization** |
| File count | **196** — matches |
| File-list | `0695d3bf…dbe1` — **matches** |
| Per-file hashes | **13/13 exact** |
| Staging/TMPDIR/3 workdirs absent | all five |
| **Live** ports unbound | 19131, 19132, 19133 |
| **Live ledger**: ports absent from subject's ledger | all three |
| ledger records c1r ports SPENT | **3/3** |
| ledger records c2h retired | **3/3** |
| Clone manifest | `f3b66c49…71f4` — matches |
| Production listeners + PID/starttime | 4296/14214, 5040/15825, 5041/15829; 8050 LISTEN |
| pm2G retained | 18265 bound; 1456369, 1456373, 1456374 running |

**Store read-only as uid 994:** owner `odbadmin` 1000:1000 mode 775, `test -w` no,
0 non-traversable, 0 non-readable dirs, 0 non-readable files, **0 writable paths**,
0 symlinks, 0 escapes. 123005 files, 35101630061 bytes.

**Production PID discovery was fresh and unambiguous** — discriminator
`woa23_app:app` on `127.0.0.1:8050`, **twelve other gunicorns excluded on recorded
evidence**. No PID reuse: starttimes and boot id unchanged. `/proc/<pid>/exe` was
**`exe_not_readable`** for all three and is never reported as exe-verified.

### 3.1 The two untracked worktree helpers, as required

Recorded before execution. **Neither is part of the subject, and neither travels to
VM24** — the run extracts the authorised archive fresh.

| | `dev2026/.venv` | `run3.sh` |
|---|---|---|
| path | `<worktree>/dev2026/.venv` | `<worktree>/run3.sh` |
| kind | symlink → `/Users/cywhale/proj/woa23/dev2026/.venv` | file, 331 bytes |
| digest | link text sha256 `ff9367c79bb26d511022b3158d53053c17ebe702fd7c258d8815c7489062aaba` | sha256 `50656bf3a6f20f769e92250c4ab730eb1cfb1e3f9b6e1a7f3f34b7a8d3d3c23d` |
| in the subject's 196 files | **no** | **no** |
| git-ignored | **yes** | n/a (outside `dev2026`) |
| inside a subject source path | inside `dev2026/`, but not a subject file | **no** — worktree root |

Neither shadows or replaces any `api`, `bench`, `scripts` or `deploy` file: the
subject's `api` 10, `bench` 86, `scripts` 35, `deploy` 12 files are untouched,
`git diff HEAD` is empty, and no subject path is named `.venv` or `run3.sh`.
`.venv` is the interpreter `run_suites.sh` is designed to use. **No abort condition.**

---

## 4. Cleanup and post-run state

Cycle 1's own cleanup ran and was confirmed:

```
candidate      stopped; whole tree exited, port 19131 confirmed free
reference      stopped; whole tree exited, port 19132 confirmed free
dask_worker    stopped; whole tree exited, port n/a
dask_scheduler stopped; whole tree exited, port 19133 confirmed free
production on 8050 unchanged (master 4296, listeners [4296 5040 5041], boot id matches)
```

Verified independently at 12:08:16 UTC:

- **Arms:** none — `grep` exit 1 with its own command line excluded.
- **Ports:** 19131, 19132, 19133 all free.
- **Cycle artefacts:** cycle 1 → **12**; cycle 2 → **0**; cycle 3 → **0**. Workdir
  `-cycle1` present (preserved as evidence), `-cycle2` and `-cycle3` **absent** — they
  were never created, confirming no fourth-or-later cycle ran.
- **Production:** 4296 / 5040 / 5041 ALIVE, starttimes **unchanged**, 8050 LISTEN, boot
  id unchanged.
- **pm2G:** 18265 bound; all three pids running. **Untouched.**

**Store identity, before and after — identical on all four measures:**

| | pre-flight | post-run |
|---|---|---|
| dir mtime | 1787622836 | 1787622836 |
| files | 123005 | 123005 |
| bytes | 35101630061 | 35101630061 |
| fingerprint | `1c89be47…b208f` | `1c89be47…b208f` |

Store, ACLs, runtime, `.lock` file and permissions not modified. No latency, startup,
deployment or self-rerun.

---

## 5. What must be decided before any further C2

**This is the PI's decision, not mine.** I have changed nothing.

The 5.2B semantic gate cannot express "this difference was decided". C2 cannot complete
while the candidate publishes OpenAPI 1.1.0 and the reference publishes 1.0.0, because
cycle 1 will fail on `C20a` every time. Three options, stated with their costs:

1. **Teach 5.2B the same narrow documentation classification 5.2C already has** — accept
   `C20a` only when the two documents are identical once `version`, `summary` and
   `description` are stripped, so a new route, response, parameter or schema still
   fails. This *changes the 5.2B gate*, which the authorisation forbade, so it needs an
   explicit decision.
2. **Exclude `C20a` from the C2 case set**, on the ground that C2's question is semantic
   equivalence of *data* responses across unpinned seeds and the documentation surface
   carries no row order. This changes the case set rather than the gate, and it means C2
   stops covering the documentation surface at all.
3. **Leave both unchanged** and accept that C2 cannot pass against this candidate.

I recommend **option 1**: it keeps C2's coverage, reuses a classification already proven
in `c1r`, and its permission is narrow and tested both ways. But it is a change to a gate
you told me to leave alone, so I am not making it.

Whichever is chosen, it needs offline work, new offline batches, a **new subject**, and a
**new C2 identity with first-use ports**. `c2j` is consumed and is not to be reused.

---

## 6. Standing limits observed

- **Exactly one** c2j execution. No self-rerun. No fourth cycle — and, in fact, no
  second or third.
- **5.2B gate unchanged.** Nothing was modified to make the run pass.
- `api/query.py` unchanged; no gate weakened, no expected difference broadened.
- `c1r` and `c1q` evidence untouched and not back-filled.
- Production requests **0**; store unmodified; pm2G, 18265 and retained state untouched.
- No latency, startup, deployment or PM2 validation. No timing of any kind produced.
- All failure evidence preserved: cycle 1's 12 artefacts and its workdir are retained.
- `c2j` CONSUMED; 19131/19132/19133 BOUND → SPENT.

## 7. Evidence

| File | Contents |
|---|---|
| `scratchpad/c2j/00-untracked-helpers.txt` | the two helpers, paths, digests, vetting |
| `scratchpad/c2j/01-pid-discovery.txt` | fresh PID discovery, exclusions, `/proc` identity, exe readability |
| `scratchpad/c2j/02-preflight.txt` | every pre-flight check, live ledger, store scan |
| `scratchpad/c2j/03-run.txt` | the full run, UID evidence, worker measurement, gate |
| `scratchpad/c2j/04-cycle1-failure.txt` | the C20a failure, exactly |
| `scratchpad/c2j/05-poststate.txt` | post-run state, artefact counts, store comparison |
| `results/c2j_cycle1_*.json` (VM24) | 12 retained artefacts |
