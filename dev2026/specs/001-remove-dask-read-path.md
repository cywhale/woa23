# Spec 001 — Remove Dask from the API read path

| | |
|---|---|
| **Phase / step** | Phase 1, S1 |
| **Status** | Revision 40 — historical latency evidence recorded with its caveats; **D2b requested** to obtain a controlled measurement. |
| **Author** | Claude |
| **Reviewer** | Codex — revision 25 approved the spec; every revision since has been `REQUEST CHANGES`, including the two that followed the campaign |
| **Evidence** | [`docs/BASELINE.md`](docs/BASELINE.md), `../results/zarr_vm24_all.json`, `../results/zarr_vm24_heavy.json`, `../results/http_prod_baseline_heavy.json` |
| **Blocks** | S2, S2b, S2c, S3, S4 |

## 0. PI decisions

**D1 — Where does the candidate API live? → DECIDED: `dev2026/api/`.**
PI decision, 2026-08-05. `app2026/` is not used in this phase. The reviewer
recommended `app2026/`; the PI chose otherwise, which is the PI's call. The
briefing has been updated to record it as a PI decision rather than an open item.
The PI was explicit that this settles **repository layout only** and grants no
authorisation to run anything anywhere.

**D2 — process authorisation on VM24. → NOT GRANTED. Split into two asks.**

Revision 3 asked about the reference instance only. That was incomplete: the
candidate has to run somewhere too, and the 33 GB store is on VM24, so **no part of
S1 can be measured without at least one new process on a production host.**

| | ask | needed for | status |
|---|---|---|---|
| **D2a** | candidate on `127.0.0.1:8051` | **any** S1 measurement at all — contract or latency | not granted |
| **D2b** | unmodified reference on `127.0.0.1:8052`, pinned hash seed | upgrading the contract gate from semantic to byte-exact | not granted |

D2a is unavoidable. D2b is optional and buys strictness. Both procedures — start,
health check, PID capture, port-collision check, stop, cleanup on failure — are in
§5.3.1 (reference), §5.3.2 (candidate) and §5.3.3 (rules for both). Neither runs
until the PI says so; disclosure is not authorisation and a reviewer cannot grant it
on the PI's behalf.

### On the reviewer's fallback suggestion

The review says: if D2 is refused, do S2b first, then accept semantic comparison.
I want to flag one thing before that is adopted, because I think it rests on a
misreading and I would rather say so than quietly comply.

**S2b does not enable byte comparison for S1.** S2b makes the *candidate*
deterministic. The thing it would be compared against — the live production process
— stays nondeterministic, because S2b cannot be applied to a running process we are
not allowed to restart. So after S2b the gate is still semantic. Sequencing S2b
first therefore buys nothing for S1's gate; it only means the riskier change (a
user-visible reordering of output columns) ships before the safer one.

The real choice is D2b or no D2b:

- **With D2b:** byte-exact gate (§5.2A). Strongest, costs one extra short-lived
  process.
- **Without D2b:** semantic gate (§5.2B) — row multiset keyed on
  `(lon, lat, depth, time_period)`, column *set* not order, values compared exactly.
  Called semantic comparison, because that is what it is. I am not normalising both
  sides and calling the result a byte comparison; the review is right that that
  would be a costume.

Whether S2b should jump the queue is a separate question and a PI contract decision
either way.

## The campaign artefacts are historical, and are labelled so

`results/paired_s1.json` and `results/contract_s1.json` were produced by the harness
**as it was on 2026-08-07, before** the provenance fixes below. They therefore have
**not** passed revision 37's two-arm validation, and this document does not claim
they have: `contract_s1.json` carries `reference_meta: null`, and both files carry
the dependency record that named the wrong interpreter.

**Nothing has been back-filled into them.** An artefact that has been edited after
the fact to satisfy a check it did not pass is worse than one that plainly failed —
it looks verified. The post-hoc verification lives in its own file,
`results/runtime_environment_check_2026-08-07.json`, which states what it is and when
it was taken.

### What the environments actually are — and a caveat I got wrong twice

The **twelve packages pinned in `pyproject.toml` match exactly** on both arms:
fastapi 0.115.12, starlette 0.46.2, uvicorn 0.34.1, gunicorn 23.0.0, orjson 3.11.4,
pydantic 2.11.3, numcodecs 0.15.1, numpy 2.2.4, pandas 2.2.3, polars 1.27.1,
xarray 2025.3.1, zarr 2.18.6. The pyenv base install that was wrongly recorded has
fastapi 0.115.2 / polars 1.10.0 / xarray 2024.9.0 — neither arm.

**But the arms are not otherwise identical, and revision 38 said they were.** The
candidate venv has 58 distributions and production's environment has 236 — that much
is expected, since production's hosts other ODB services. What is not expected:
**23 of the 58 packages present in both carry different versions.** Among them:

| package | candidate | production | where it sits |
|---|---|---|---|
| `fsspec` | 2026.7.0 | 2025.10.0 | zarr/xarray storage access |
| `anyio` | 4.14.2 | 4.11.0 | starlette's async path |
| `pyarrow` | 25.0.0 | 22.0.0 | pandas/polars interchange |
| `tornado`, `msgpack`, `tblib`, `psutil` | newer | older | distributed's transport |
| 16 others | newer | older | packaging, tz data, TLS roots, CLI |

`fsspec` and `anyio` are in the storage and request paths respectively; `pyarrow`
backs the pandas interchange the query pipeline uses. **I do not know that any of
them affected the measurement, and that is the problem** — an uncontrolled variable
is uncontrolled whether or not it turned out to matter.

**So the honest statement of what the 2026-08-07 campaign establishes:** a paired
latency comparison in which the twelve benchmark-relevant pinned packages were
identical and 23 transitive dependencies were not. It is evidence, and it points the
same way as the Zarr-level measurement did. It is **not** a clean isolation of the
Dask change, and the speedup figures should not be quoted as one.

Two things would fix it, and both are the PI's call: pin the full transitive set on
both sides (which means changing production's environment — out of scope for S1), or
run variant 5.2A against a controlled reference built from the same lock, where both
arms come from one venv and the question disappears.

The second is now specified and ready: [`D2b-request.md`](D2b-request.md) and
`scripts/run_controlled.sh`. Both arms are processes we start, from a single venv,
with the seed pinned — so the contract gate becomes byte-exact and **production
receives zero requests**. It is requested, not granted, and it is not implied by D2a.

The record itself also did not meet the current provenance standard, and
re-establishing that would need a fresh campaign under the fixed harness.

## Provenance defects found in the first record — 2026-08-07

Both were in the record, not the runtime. The A/B itself is sound, and the latency
figures stand.

**1. The dependency record described neither backend.** `/proc/<pid>/exe` follows a
venv's symlink to the base interpreter, so `dependencies()` ran `pip freeze` against
`~/.pyenv/versions/3.11.4` — an unrelated older install — and recorded **fastapi
0.115.2 / polars 1.10.0 / xarray 2024.9.0** for *both* arms. Checked directly:

| environment | fastapi | polars | xarray |
|---|---|---|---|
| candidate venv | 0.115.12 | 1.27.1 | 2025.3.1 |
| production (`versions/py311`) | 0.115.12 | 1.27.1 | 2025.3.1 |
| base install (`versions/3.11.4`) — what was recorded | 0.115.2 | 1.10.0 | 2024.9.0 |

Both arms ran the same **pinned** versions. They did **not** run identical
environments — 23 shared transitive dependencies differ, which is the caveat above
and which revision 38 wrongly waved away as "the comparison was clean". Fixed: the
package environment is now resolved from `VIRTUAL_ENV`,
falling back to the launcher's sibling `python`, never from `/proc/<pid>/exe`; both
values are recorded so the difference is visible; and distributions are listed via
`importlib.metadata` rather than `pip freeze`, since a uv venv has no pip and the
call was silently answering for a different interpreter. `env_python`,
`env_python_source` and a distributions digest are now required fields, and an
unresolved environment fails the gate.

The first fix reproduced the bug in its own test — calling `.resolve()` on
`.venv/bin/python` returned 2 distributions instead of 58.

**2. The carried-over contract result had no reference provenance.**
`contract_s1.json` stored `reference_meta: null`, and `verify_prior_contract()`
compared only the candidate — so §4.6's claim of "same code, store, data and
dependencies" covered one arm of a two-arm comparison. Fixed: `contract_diff` now
requires reference provenance under both variants (waiving only the hash-seed
requirement under 5.2B), and the reuse check compares **both** arms, requires each
permitted failure to carry the *known comparator note* — a permitted case id is not a
permit for a different failure on that case — and requires the **exact set of case
identities**, not a count. Revision 37 checked only the count and uniqueness, so
**sixty-four fabricated `FAKE-*` ids passed**: the reuse would have been justified by
a result about nothing. Reproduced before fixing, and pinned by a test.

## Latency gate — observed paired latency, 2026-08-07

**This is a historical artefact.** It was produced by the harness as it stood on
2026-08-07, before the provenance fixes above. It did **not** pass the current
carried-over validation, and this section does not claim it did.

What the reuse check of the day actually did: it compared the candidate's source
digests, store path, store fingerprints and dependency record against the contract
run, and confirmed that no case had failed except the two known comparator defects.
What it did **not** do — because the code did not yet do any of it — was validate the
reference arm at all, check the gate state, check verdict values, or check the case
identities. The reference record was `null`, and the dependency comparison it did
make was between two copies of the same wrong value. Preflight did assert, before any
production request, that the candidate interpreter was **Python 3.11.4** matching
production's and that the candidate had **`PYTHONHASHSEED=0`**; those two hold.

**176 production requests — exactly the remaining authorisation, none over.**

The figures below are **observed paired latency evidence** and **apparent speedup**
against live production. They are directional, they agree with the Zarr-level
measurement, and they are **not a measured Dask-only speedup** — see the caveats that
follow the table.

| case | ratio | apparent speedup | 95% CI | regression | improvement |
|---|---|---|---|---|---|
| `readme_example` | 0.167 | **5.99×** | [0.162, 0.169] | NO_REGRESSION | IMPROVED |
| `point_profile_025` | 0.196 | **5.11×** | [0.193, 0.207] | NO_REGRESSION | IMPROVED |
| `point_profile_multiparam` | 0.222 | **4.51×** | [0.218, 0.230] | NO_REGRESSION | IMPROVED |
| `point_profile` | 0.293 | **3.41×** | [0.285, 0.298] | NO_REGRESSION | IMPROVED |
| `small_bbox_full_depth` | 0.354 | **2.82×** | [0.346, 0.361] | NO_REGRESSION | IMPROVED |
| `regional_bbox` | 0.531 | **1.88×** | [0.525, 0.538] | NO_REGRESSION | IMPROVED |
| `regional_bbox_025` | 0.603 | **1.66×** | [0.588, 0.612] | NO_REGRESSION | IMPROVED |
| `surface_global` | 0.717 | **1.39×** | [0.695, 0.740] | NO_REGRESSION | IMPROVED |

`gate: PASS`. Every case established both no-regression and improvement; nothing was
`INCONCLUSIVE`, so the ladder was never needed. `metadata_complete: true`,
`post_run_drift: none`, all responses 200, one `unpinned_seed_note` for production as
expected under 5.2B.

**The §6.1.2 improvement gate required `readme_example` and
`point_profile_multiparam` to show an established speedup; both did, at 5.99× and
4.51×, clearing the ≥3× target as well.**

### Two confounders, and which way they push

Stated in §6.1 before the run and unchanged by it. The reference arm is live
production, which **serves TLS** where the candidate serves plain HTTP, and runs
**`-w 2`** where the candidate runs `-w 1`. Sequential probing makes the worker count
nearly irrelevant; the TLS handshake cost is real and sits entirely on the reference
side.

**The direction, stated correctly.** `ratio = candidate ÷ reference`, so extra cost
on the reference **depresses the ratio** — and since `speedup = 1 ÷ ratio`, it
**inflates the apparent speedup**. An earlier draft called these "lower bounds on the
ratio", which is true of the ratio and the opposite of what a reader takes from the
speedup column. The figures above are therefore **upper-leaning estimates of the
speedup, not a clean measurement of the Dask change alone**. A byte-exact comparison
against a controlled reference on plain HTTP with matched workers (D2b, variant
5.2A) is what would remove them.

### How the prediction held

The Zarr-level measurement predicted 1.35×–8.4×; the HTTP layer delivered
1.39×–5.99×. The ordering is preserved and the direction is right, but the multiples
are consistently **lower** at HTTP than at the Zarr layer — except `surface_global`,
which matched. That is what §6.1.2 predicted when it demoted `≥ 3×` from a gate to a
target: the two layers are not related by a constant, and per-case HTTP ÷ Zarr ranged
0.60–2.27 in the baseline.

## Campaign result — contract gate, D2a P1, 2026-08-06

The candidate ran on VM24 `127.0.0.1:8051`, variant 5.2B, and was stopped cleanly:
`port 8051 released, pidfile removed`, confirmed by an independent check afterwards.

**Contract gate: 62 of 64 cases were a semantic match**, with response byte lengths
equal on both arms — every data case, all 31 CSV replays, every error path, and the
5.2 MB antimeridian case. The CSV-versus-JSON status divergence held exactly as
recorded.

The precision matters and an earlier draft of this paragraph did not have it. Variant
5.2B compares **semantically**: rows as a multiset, columns as a set, values exactly.
Equal byte lengths are corroboration, not proof of byte equality — a float formatted
differently at the same width would satisfy both and still be a difference. Calling
this "62/64 byte-identical" would claim the guarantee of 5.2A while running 5.2B.

**The two failures were a defect in the harness, not evidence about the candidate.**
C20a and C20b returned `200/200` with `8597/8597` and `868/868` bytes and were
reported as `unparseable body`: `compare_semantic()` assumed every 200 response is a
list of rows, but `openapi.json` is a JSON *object* and the Swagger page is HTML.
Non-row payloads are now compared as raw bytes — there is no ordering to be
insensitive to, so the strict comparison is the honest one — with five tests.

**C20's status, stated exactly.** Over HTTP the only evidence is that the two bodies
have the **same length**; the comparator never got as far as comparing them. Byte
equality for these two routes rests on `bench/smoke_local.py`, which calls the route
handlers **in-process** on the development machine and compares
`JSONResponse.body` — 8,597 and 868 bytes identical. That is local, in-process
evidence. It is not an HTTP-level result, and **the campaign has not established a
64/64 HTTP contract pass.** Re-testing those two cases over HTTP costs 2 production
requests and is not covered by the current authorisation, whose 176 remaining
requests are exactly consumed by the latency gate.

**The latency gate never started**, because the contract gate failed first. Of the
240 production requests the PI authorised, **64 were spent and 176 were not**.

## Changes since revision 34 — what the first campaign found

Two things surfaced during D2a P1 that no amount of offline review had reached,
because both are properties of the deployed environment rather than of the code.

**1. `requires-python = ">=3.11"` let `uv sync` choose Python 3.14 on VM24.**
Production runs 3.11.4. The sync failed only because no wheels exist for 3.14 yet,
so it tried to build numpy, pandas and pydantic-core from source and went looking
for a Rust toolchain. **Had those wheels existed it would have succeeded silently**
and benchmarked the candidate on an interpreter production does not run — which
would have invalidated every number in the campaign with nothing appearing wrong.
Pinned to `>=3.11,<3.12`, and the candidate's venv is now built against production's
own interpreter (`~/.pyenv/versions/py311/bin/python3.11`, 3.11.4). The local
development venv was on 3.13.11 and is now 3.11.14 — another mismatch that had gone
unnoticed.

**2. `validate_meta` required a pinned `PYTHONHASHSEED` of every arm, which makes
variant 5.2B unrunnable.** Under 5.2B the reference *is* live production, which we
are not permitted to restart; its seed reported `<unset — randomised>`, exactly as
expected. An unpinned seed there is the condition that **makes 5.2B necessary**, not
a defect in the record — §5.1 says so, and the validator contradicted it. The
requirement is now per-arm and stated by the caller: never waived for a backend we
start ourselves, waived for the 5.2B reference and recorded in
`unpinned_seed_notes` rather than passing silently.

The second is a spec-level inconsistency that survived thirty-four revisions of
review. It could only appear once something actually ran, and the latency gate has
since **exercised it end to end**: the candidate reported `PYTHONHASHSEED=0`,
production reported `<unset — randomised>`, the 5.2B rule waived the requirement for
the reference arm only, and the waiver was recorded in `unpinned_seed_notes` rather
than passing silently.

**A phase selector has been added to `run_candidate.sh`** (`latency-only`) so that
completing the campaign need not re-charge production for the 64 contract-gate
requests whose answer is already recorded and unaffected by the fix. It refuses to
run unless the earlier run's contract and pilot results are present, so it cannot be
used to skip a phase that never happened. **Nothing will be started on VM24 without
a fresh decision.**

## Changes since revision 33

| # | review point | resolution |
|---|---|---|
| 1 | `verify_prior_rung()` checked the case-id set but not each row's `regression_verdict` | added, and the failure mode is worth naming: rows missing a verdict would satisfy the case-set check, the escalation would then find no `INCONCLUSIVE` case, and the script would report **"no escalation needed"** — a malformed file reading as a clean bill of health. Rows must now be objects with a non-empty `id` and a verdict from the known set; `results` and `post_run_drift` must be lists; `metadata_complete` must be **exactly `True`**, since a truthy `1` or `"yes"` sailed through the old `if not ...` test. Eleven new assertions. |

**D2a P1 granted by the PI**: candidate on VM24 `127.0.0.1:8051`, variant 5.2B, rung
21 — candidate-only noise pilot, 64-case contract gate, 8-case latency gate, ~240
loopback requests and ~301 MiB to production 8050. Not granted: rung 60, rung 150,
D2b, any restart or reconfiguration of 8050, any new Dask client, any public cutover.

## Changes since revision 32

| # | review point | resolution |
|---|---|---|
| 1 | a stale pidfile could be overwritten when the port happened to be free | **real gap, and it closed a loop I had left open**: `stop_candidate` deliberately leaves the pidfile when it refuses to kill, and preflight only looked at the port — so the next run would overwrite that record and orphan whatever it pointed at. Preflight now refuses to start while either the pidfile or the starttime file exists, and reports whether the recorded PID is still running and what its command line is. Leftover state has to be cleared by a person. |
| 2 | `verify_prior_rung()` could be satisfied by a truncated file | presence is now checked before meaning — eleven required fields, and it returns early if any are absent, because `.get()` yielding `None` is not a value that passed. Added: the **full eight-case set**, unique ids, matching **candidate and reference URLs**, schema validation of **both embedded metadata records** on their own terms, and a refusal to escalate past an **established `FAIL` or regression** — more samples cannot overturn a confirmed failure, and re-running as if they might is looking for a better answer. Twenty-one new assertions. |
| 3 | `noise_pilot.py` still documented `noise_pilot_reference.json`, and "no loss of rigour" was too strong | filename and example URL corrected. The claim is downgraded in the tool, the spec and the request: a candidate-only pilot does not affect the gate's **validity**, but its **planning evidence** covers one arm only, so a case sized against a quiet candidate can still return `INCONCLUSIVE` against a noisier production. |

## Changes since revision 31

| # | review point | resolution |
|---|---|---|
| 1 | a dead PID let cleanup remove the pidfile and report success without checking the port | **real hole.** A dead master is not a released socket — a surviving worker, or something else that grabbed the port, can still hold it. The dead-PID branch now polls for release for up to 20 s and only then removes the pidfile; if the port is still held it prints the holder, leaves the pidfile, and fails. |
| 2 | rung 60 checked only that the rung-21 file existed | **`provenance.verify_prior_rung()`** — escalation inherits everything the earlier rung established, so it now requires: same gate variant, `warm_samples_per_arm == 21`, complete provenance, no recorded runtime drift, a non-`INVALID` gate, non-empty results, and — on **both** arms — identical source digests, store path and `.zmetadata` fingerprints. Code or data changing between rungs stops the escalation. Eight branches verified. |
| 3 | spec still named `noise_pilot_reference.json` and 8052 for the pilot | corrected to `noise_pilot_candidate.json` on 8051, with the reason stated: the pilot only sizes the sample, so it need not touch the arm it is compared against. The remaining 8052 references are §5.2A and §5.3.1, which are genuinely about D2b. |

## Changes since revision 30

| # | review point | resolution |
|---|---|---|
| 1 | traffic undercounted: rung 60 re-ran the pilot and contract gate, and escalation re-ran all 8 cases | **one rung per invocation**, enforced by the script. Rung 60 is escalation only: it reads the rung-21 result, runs the latency gate for *just* the `INCONCLUSIVE` cases, and skips the pilot and contract gate because neither improves with more latency samples. Costed per invocation and cumulatively: rung 21 alone is **240 production requests / ~301 MiB**; 21 + 60 worst case is **728 / ~1,128 MiB**. The request now asks for **rung 21 only**. |
| 2 | `stop_candidate` killed on the pidfile alone, and the script would run anywhere | before killing, the trap requires the process's **start time** (`/proc/<pid>/stat` field 22) still to match what was recorded at launch **and** the PID still to hold port 8051; either mismatch refuses and leaves the pidfile. The script also refuses to run unless `hostname -s` is `odb24` and the store path exists — without that, `WOA23_D2A_GRANTED=yes` would start a process anywhere it had been copied. The bash starttime parser is cross-checked against the Python one on a live VM24 process: both give 1874. |
| 3 | `compare_semantic()` false-passed on error bodies | **real bug, reproduced from the review's two examples.** Comparing `detail` alone scored two unparseable bodies, and two objects with no `detail` at all, as matches. Now both bodies must parse as JSON objects carrying `detail`, and the whole object is compared. New **`bench/test_contract.py`** — 25 offline assertions covering error bodies, row/column order insensitivity, value and null changes, the CSV empty-field-versus-`NaN` distinction, and the case list's own invariants. |
| 4 | artefact names diverged between spec and script | canonical set fixed in §4.8 of the request: `results/paired_s1.json` is **the** latency gate result, per-rung files kept alongside as the record. The pilot's file follows its target, which is now the candidate. |
| 5 | C20 was claimed settled yet still in the 64-case list | the claim was too broad and is narrowed rather than the case dropped. The local check calls the handlers **in-process**; the gate calls them **over HTTP against the deployed candidate**, through gunicorn and the real request path. Different checks, the second stronger — so C20 stays and 64 per arm stands. Preflight on the deploy target now **exits 1** when the directory exists instead of printing a warning. |

## Changes since revision 29

| # | review point | resolution |
|---|---|---|
| 1 | **`bench/contract_diff.py` did not exist** — the spec had cited it since revision 1 | written, with **`bench/contract_cases.py`** holding §5.4 as executable definitions: 33 data and documentation cases, 31 CSV replays, **64 requests per arm**, each carrying the status it was actually observed to return. Both gate variants implemented; under 5.2A parsing only localises a failure, never decides it. |
| 2 | the request claimed a `trap` its start command did not have, and "auto-cleanup" contradicted "never auto-kill" | the campaign is now **`scripts/run_candidate.sh`**, a file in the repo, so the trap lives in the shell that starts the process and the whole sequence is diffable and syntax-checkable. The contradiction is stated in the script: the trap cleans up **this** run and never kills a process it did not start. |
| 3 | the health check was a single `curl` | polls to a 30 s ceiling requiring **200 and a non-empty body**; a timeout is a failure, not something to wait through. |
| 4 | stop succeeded whether or not the port was released | polls 20 s and removes the pidfile **only after** the port is confirmed released, otherwise reports and leaves everything in place. |
| 5 | `rsync --delete` against an unverified target | `--delete` removed entirely; the target's absence is checked mechanically before anything is created. |
| 6 | the traffic figure counted only the latency gate | recomputed for the whole campaign from measured payloads: one pass over the eight cases is **13.55 MiB**. At the rung-60 cap production sees **552 requests / ~830 MiB**; at rung 150 it would be **1,272 / ~2.0 GiB** — which is why the request stops at 60, and why **the script refuses rung 150 outright** rather than relying on this document to discourage it. |
| 7 | `--pause` is per AB/BA pair, not per request | documented precisely in the flag's help: `2*(N+1)` requests and `N+1` pauses for N warm samples. |

**A reduction offered rather than assumed:** the noise pilot now targets the
candidate. Since revision 8 it only does sample-size planning — the gate's own
interval carries the noise — so it need not sample the arm it will be compared
against. That removes 208 requests and 352 MiB from production.

**The project's own safety guard shaped this round twice.** It refused a command
carrying a forced recursive delete inside a heredoc, which pushed the start sequence
out of a markdown block and into a reviewable file — where its three refusal paths
could actually be executed and verified. It then refused a second command because
the *spec text describing that refusal* quoted the same token. Both were the guard
working correctly on a pattern match it cannot contextualise, and neither was worth
weakening it over.

## Changes since revision 28

| # | review point | resolution |
|---|---|---|
| 1 | the smoke test compared `json.dumps(dict)`, not the route's actual `JSONResponse.body` | now calls both route handlers and compares `.body`. `openapi.json` 8,597 bytes identical — matching the reviewer's independent measurement — and the Swagger UI route, which was not covered before, 868 bytes identical. |
| — | guard scope | `.claude/` is git-ignored, so nothing about it enters the repo. The `pyproject.toml` narrowing stays until `dev2026` lands, then reverts. The PI's authorisation extends to `requirements.txt` / `Pipfile` / `setup.py`, but **the guard is not being widened for those** — the rules of engagement say those production files stay untouched, and this work has no reason to open them. |

**D2a requested:** [`D2a-request.md`](D2a-request.md) — one process on
`127.0.0.1:8051`, exact commands, sample volume per ladder rung, cleanup and
verification. It flags one thing for the PI rather than deciding it: under gate
variant 5.2B the "before" arm is live production, so the ladder top would send 1,208
sequential requests to the process serving real users — more than "light sequential
probing" naturally covers. Two ways to reduce it are offered.

## Changes since revision 27

| # | review point | resolution |
|---|---|---|
| 1 | "5 differences" and the tool's "total: 7" could be read as the same count | §4.6 now states both explicitly — **7 AST changed lines, 5 conceptual changes** — and the tool's final line says the same. An edited call is one change and two lines; the removals have no partner line. |
| 2 | the candidate added `from __future__ import annotations`, which changes annotation semantics FastAPI depends on | **removed from all three modules.** It was an undeclared difference with a plausible route to changing the very document C20 compares, so it is gone rather than defended. `port_diff.py` now counts a candidate-only `__future__` import instead of filing it under informational imports. |
| — | dependencies | added at the §4.3 pins with `uv lock` / `uv sync`. Dask stays, annotated as harness-only — removing it from the candidate is the point of S1. |

**C20 is now settled locally.** `bench/smoke_local.py` generates both OpenAPI
documents in-process and byte-compares them: **identical**. That case never needed
the Zarr store, so it did not need D2a either.

## Changes since revision 26

| # | review point | resolution |
|---|---|---|
| 1 | dependencies missing — the candidate cannot actually start | `pyproject.toml` edit proposed with the §4.3 pins; it is a guard-protected file so it needs the PI's approval. Local import / OpenAPI / route smoke tests follow, no VM and no benchmark. |
| 2 | `port_diff.py` covered 6 of 10 functions and no module level | now covers **all ten**, cross-checked against the functions actually defined in the original so an unported one is a finding rather than a silent omission, plus a module-level comparison of assignments and import-time statements. That is what found difference 5 and would have found the `RuntimeError` deviation. |
| 3 | three dead assignments, and the undeclared `KeyError` → `RuntimeError` | all four reverted. The port now differs only where §4.2, §4.4 and §4.5 authorise it. |

The `RuntimeError` deviation is worth naming plainly: it was introduced in the same
revision that added a generated inventory, and the inventory did not catch it
because the tool only looked at function bodies. A verification tool that does not
cover the whole claim is a claim that has not been verified.

## Changes since revision 25 — the port

Spec approved at revision 25; `dev2026/api/` implemented. No process has been
started on VM24 and no benchmark has been run: D2a is still ungranted, so nothing in
§6 has been executed.

| file | contents |
|---|---|
| `dev2026/api/config.py` | tables + the mandatory `WOA23_ZARR_STORE` resolution |
| `dev2026/api/query.py` | the read pipeline |
| `dev2026/api/app.py` | all four routes |
| `dev2026/bench/port_diff.py` | **new** — generates §4.6's inventory from the AST |

New in this revision: **§4.6**, a generated inventory of every executable difference
between original and port. It exists because the "verbatim apart from two lines"
claim turned out to be wrong when checked — three dead assignments had also been
dropped — and a claim of that shape should never have been left to prose in the first
place.

## Changes since revision 24

| # | review point | resolution |
|---|---|---|
| 1 | §6.1.4's normative paragraph still described the post-run check as needing only store path, master PID and argv, and listed only store/process/port drift | rewritten as a table of all four classes — process, port, sources, store — each with what it re-reads and **which provenance fields it consumes**, which is the part that had silently grown from three to nine. The failure list now also names the host-reboot and edited-source cases, and states that a check which cannot be performed fails identically. |
| 2 | the "nothing has looked at them since" comment was vague | replaced with the precise claim: the pre-run verification was the last observation before this one. |

## Changes since revision 23

| # | review point | resolution |
|---|---|---|
| 1 | the new docstring said all four are verified before sampling "and none of them again" — inside the function that checks them again | corrected to "verified before sampling and checked again here", and "what can move" widened to "what can change, or become unverifiable" |
| 2 | the spec's sampling paragraph still listed only store and process; "moved" was too narrow | now a table of all four — process, port, sources, store — each with what would go unnoticed without it. `INVALID_RUNTIME_DRIFT` is described as **changed or became unverifiable**, since an unreadable `/proc` entry or an ambiguous listener set fails the same way as a change. |
| 3 | *(suggested)* the drift tests asserted helper strings, not the final verdict | taken. `decide_gate()` extracted from `main()` and pinned by `test_gate_precedence`: each finding's verdict in isolation, plus metadata above everything, drift above every performance verdict, and regression above unproven improvement. A status-mapping edit can no longer slip past the tests. |

## Changes since revision 22

| # | review point | resolution |
|---|---|---|
| 1 | `INVALID_STORE_DRIFT` covered source drift too but the spec described only store and process | renamed **`INVALID_RUNTIME_DRIFT`**, and the description now names all four things it covers: process, port, sources, store |
| 2 | `post_run_store_check()` and its docstring said store/process while the code checked sources | renamed **`post_run_runtime_check()`**; the docstring enumerates what can move during sampling and why each matters |
| 3 | a test docstring still said "nothing else looks at them after", which revision 22 disproved | corrected, and it now records what revision 21 missed |

All three were the same defect in different places: an implementation that grew past
its own naming. Renamed rather than re-described — a name that needs prose to
explain it away will mislead whoever reads the artefact without the prose.

## Changes since revision 21

| # | review point | resolution |
|---|---|---|
| 1 | the no-counts paragraph still quoted nine / thirteen / sixteen | gone. Citing a past figure to illustrate why figures go stale is still a figure, and it is the second time this paragraph has contradicted itself. |
| 2 | the unreachable-cwd fixture fallback still used `"a" * 64` | replaced with a named `STUB_DIGEST` and a comment saying why it is never compared: those fixtures have no real cwd, so validation stops at "cwd is not reachable" before any digest is examined. |
| 3 | re-hashing covered only up to gate start, not the sampling window | source verification now runs again inside `post_run_runtime_check()`, so a file edited mid-run fails as `INVALID_RUNTIME_DRIFT` in the same artefact. This keeps `--against` an out-of-band convenience rather than an acceptance dependency. |

## Changes since revision 20

| # | review point | resolution |
|---|---|---|
| 1 | `_verify_source_set()` compared filenames but never the digests | **reproduced from the suite's own fixtures** — they wrote `x` and recorded `"a" * 64`, and `validate_meta` returned `[]`. Files are now re-hashed during validation and compared. Fixtures carry real digests; three assertions cover a matching record, a well-formed digest of the wrong content, and a source edited after collection. |
| 2 | the paragraph declaring counts would not be written down wrote two down | removed. The suites print their totals; this document does not repeat them, not even illustratively — which is how the contradiction arose. *(Revision 22: the replacement paragraph still quoted the historical figures, so those are gone too.)* |

## Changes since revision 19

| # | review point | resolution |
|---|---|---|
| 1 | `--against` validated only the baseline record | both are now validated, with problems labelled by side. A current record with a bad hash seed, an incomplete manifest, an unreadable store or `port_verified: false` blocks the comparison instead of yielding "store unchanged". Five assertions added. |
| 2 | the assertion count was still wrong | it was — and "thirteen" was wrong too; the real figure was **sixteen**, because `test_master_selection` grew when revision 19 added the fail-closed cases. Rather than correct it a third time, **both suites now print their own totals** and the enumeration in §6.1.4 lists the cases instead of counting them. |

## Changes since revision 18

| # | review point | resolution |
|---|---|---|
| 1 | `master_of([pid], ppid=lambda _: None)` returned the PID, contradicting fail-closed | reproduced, then fixed: any unreadable parent makes the whole set ambiguous. The revision-18 test that appeared to cover this passed by arity — three unreadable PIDs give three roots — not by logic. Three assertions added, including the single-PID case. |
| 2 | `listener_pids` was only checked for non-emptiness | contents validated: positive integers (booleans rejected), no duplicates, and `master_pid` must be a member. `["not-a-pid"]` with `port_verified: true` used to pass. |
| 3 | stale counts, and revision 17's `port_verified: false` / exit-3 description no longer matched the code | 24 → **26**, "nine assertions" → **thirteen**, and the dead exit-3 branch removed with the paragraph rewritten to describe the flag as defence against hand-edited or older records — which is all it can now be. |

**Counts are no longer written into this document.** Both suites print their own
totals on every run, and that output is the only place to read them. Every
hand-maintained figure here went stale within a revision or two, was corrected, and
went stale again — including inside the paragraph that announced it would stop. Even
citing a past figure to illustrate the problem re-creates it, so none appear here at
all.

## Changes since revision 17

| # | review point | resolution |
|---|---|---|
| 1 | `--pid` only had to be *in* the listener set, so a worker could be recorded as master | now required to equal `master_of(listeners)`; a worker is refused by name, with the reason |
| 2 | an unidentifiable master should fail closed | it does, in both the sidecar and the post-run check. `master_of()` returns `None` for zero or several roots and every caller treats that as failure. |
| 3 | post-run used `master_of(...) not in (None, pid)`, letting ambiguity pass | changed to strict equality — `None` now fails |
| 4 | `pids_on_port()` returned at the first matching `ss` row | accumulates across all rows. Parsing split into the pure `parse_ss_listeners()` so it is tested against captured output rather than a live host. |

Covered by `test_ss_parsing_accumulates_rows`, `test_master_selection` and
`test_post_run_listener_change`: both address families collected, every PID sharing
one socket, an unused port, a suffix port not matched, master selected from a
parent-child tree, a worker not selected, two unrelated roots ambiguous, no
resolvable parent ambiguous, an unreadable parent ambiguous, an empty set, a lone
listener its own master, and four post-run listener transitions — unchanged,
vanished, taken over, gone ambiguous.

## Changes since revision 16

| # | review point | resolution |
|---|---|---|
| 1 | an unconfirmed port left only a printed warning | **`port_verified` and `listener_pids` are now persisted and required**, and `validate_meta` fails a record with `port_verified: false`. The sidecar also exits 3 in that case, so neither the artefact nor the exit status is silent. |
| 2 | `post_run_runtime_check()` did not re-check the port's listener | added. A port changing hands while the original process lives is invisible to PID, start-time and argv checks alike, so the listener set is re-read and the master must still hold it. |
| 3 | the "programmatic reconciliation" claim was not backed by anything in the repo | **the claim was overstated and is now made true**: `build_meta()` split out of `main()`, and `test_schema_matches_sidecar_output` asserts both directions between it and `REQUIRED_META_FIELDS`. |

**A worse bug found while answering (2).** `pid_on_port()` returned the *first* PID
`ss` prints for a port. For gunicorn that is a **worker**: on the live production
port it gave 4366 while the master is 3960. Every identity field — start time,
worker list, argv — would have described a worker and then failed its own post-run
comparison for the wrong reason. Replaced by `pids_on_port()` + `master_of()`, which
picks the process whose parent is not in the listener set; verified against
production on VM24.

## Changes since revision 15

| # | review point | resolution |
|---|---|---|
| 1 | `compare_identity()` did not compare `port` | reproduced — 8051 → 8052 returned no problems — then added, with a regression test |
| 2 | `--pid` was not checked against the port's actual listener | the sidecar now compares `--pid` with what `ss` reports for `--port` and aborts on a mismatch; when `ss` cannot see a listener it records the PID but says so |
| 3 | spec said the stub yields 18, and described `port` as nullable | measured: **24**. `port` documented as a required 1–65535 integer. |
| 4 | `env_whitelist` / `collector_python` were emitted but unchecked | added to the required set. The set is now **reconciled programmatically against what the sidecar emits** — the mismatch is what let `expect_argv_contains` through, so the fix is the reconciliation, not the two fields. |
| 5 | "start time 1874, consistent with 54-day uptime" is nonsense in clock ticks | **the review is right and the claim was wrong.** Recomputed on VM24: `CLK_TCK=100`, so 1874 ticks = 18.7 s *after boot*, not an uptime. Host uptime 55.76 days − 18.7 s = process age 55.76 days; derived start 2026-06-11 16:45:36 against `ps -o lstart` 16:45:35. The paragraph now shows the arithmetic instead of asserting a coincidence. |

## Changes since revision 14

| # | review point | resolution |
|---|---|---|
| 1 | **Blocker:** `expect_argv_contains` was mandatory in the spec but absent from `REQUIRED_META_FIELDS`, so a record without it validated and `post_run_runtime_check()` skipped the argv check | reproduced first — a stripped record returned `[]` from `validate_meta` — then required as a non-empty `list[str]`, with a regression test named for the gap. `port` likewise promoted to a required integer in 1–65535. |
| 2 | PID + argv cannot detect a PID recycled into an identically-launched process | sidecar now records **`proc_starttime`** (`/proc/<pid>/stat` field 22) and **`boot_id`**; the post-run check compares those before argv, and `compare_identity()` includes them. Parse verified on VM24 against the real production master. |
| 3 | `collect_backend_meta.py` docstring still described a `.zmetadata` difference as a data difference | corrected in place: it is consolidated metadata — shapes, chunk grids, compressors, attributes — and agreement shows two collections saw the same store *configuration*, not the same bytes. |
| — | `--port` was optional | now required even alongside `--pid`: the port is part of what identifies which arm a record describes. |

## Changes since revision 13

| # | review point | resolution |
|---|---|---|
| 1 | `--against` ran after the gate file was written, so `gate: PASS` could coexist with a drift exit 2 | the check moved **inside the gate run**: `post_run_runtime_check()` re-reads each arm's store and process after the last request and folds the result in as **`INVALID_RUNTIME_DRIFT`**, ranked just below `INVALID_METADATA`. One artefact, one verdict. It also catches a backend that restarted or died mid-run, and a reused PID. |
| 2 | `--against` trusted the earlier file and could compare two different backends | it now runs the earlier record through the same `load_meta()` / `validate_meta()` as the gate and refuses an invalid baseline; **`compare_identity()`** requires arm, manifest, cwd, PID, argv and source digests to match; and `--out` equal to `--against` is refused outright. |
| — | stale spec text | "14 problems" → **18**, measured rather than guessed — my first draft of this row said 17 and the count disagreed. `api/*.py` → the §6.1.5 manifests. Changelog rows keep their original text; they are a record, not documentation. |

**Refactor this forced, worth noting:** the sidecar now needs the same validation
the gate uses, and the gate now needs the sidecar's store reader — importing either
from the other would be circular. The shared rules moved to **`bench/provenance.py`**
so there is one definition of what a valid record is.

## Changes since revision 12

| # | review point | resolution |
|---|---|---|
| 1 | a `.zmetadata` digest was described as if it covered the data | reworded throughout: it is a **metadata fingerprint**, and agreement shows both arms were *configured against the same store*, not that they read identical bytes — chunk contents are outside it. Mismatches now say "consolidated metadata". Added the **read-only / no-refresh precondition** as an explicit operational commitment the harness cannot enforce. |
| 2 | `store_facts()` still read revision-11 field names and emitted `None` | **real bug**, confirmed: it returned `{'store': …, 'zmetadata_mtimes': None}` on a revision-12 record. The whole duplicate summary is **removed** — both sidecar files are already embedded verbatim, so a second copy could only go stale again. |
| 3 | `resolve_store()` must key off the backend label | **real bug.** The reference hard-codes `data/` and never reads the environment, so branching on the variable would have recorded a stray value as the store in use. Now label-driven; a candidate without `WOA23_ZARR_STORE` is refused rather than guessed; `validate_meta` rejects a `store_source` that does not match its arm. |
| 4 | claiming detection of changes *during* the benchmark | the in-gate check brackets the two provenance collections and now says so. Covering the sampling window is a **new `--against` mode** on the sidecar, run after the gate, exiting non-zero on drift — and it is a required step in §5.3.3, not an optional extra. |
| 5 | stale "14 fields" and `api/*.py` | corrected to **18** and `api/**/*.py` / `src/**/*.py` |

Two regression tests failed on this round's rewording and were updated — which is
what they are for; the old strings no longer described the check.

## Changes since revision 11

| # | review point | resolution |
|---|---|---|
| 1 | is cross-arm store agreement required? | **Yes, and it is now enforced.** New `validate_store_agreement()` compares resolved `store_path`, the group set, and each group's digest and `mtime_ns`; any mismatch is `INVALID_METADATA`. It separates "different data" (digests differ) from "store touched mid-campaign" (same digest, different mtime). Six branches tested. |
| 2 | `zmetadata_mtimes()` scanned only three levels and used second-resolution mtime | scan is now **recursive** (`**/.zmetadata`) and each group records **SHA-256 + `mtime_ns` + size**. The digest is authoritative; the timestamp corroborates. Renamed `zmetadata_fingerprints` since it is no longer just mtimes. |

**A wrinkle worth flagging:** under gate variant 5.2B the reference *is* live
production, which has no `WOA23_ZARR_STORE` — `woa23_app.py:63` hard-codes a
relative `data/`. An unset variable means "cwd/data", not "unknown", so the sidecar
resolves it and records `store_source: cwd_fallback`. Without that, the store
agreement check would have compared a real path against nothing and passed.

## Changes since revision 10

| # | review point | resolution |
|---|---|---|
| 1 | `kernel` / `zmetadata_mtimes` not required; a fake or subset hash set could still pass | both now required, with mtimes type-checked. **`_verify_source_set()` re-expands the manifest against the backend's `cwd` and requires the recorded filenames to equal it exactly** — a fabricated entry and an omitted real file are each reported by name. An unreachable `cwd` is itself a problem, not a skipped check. |
| 2 | `http_reference_s1.json` / `http_candidate_s1.json` still in §6.1.3 and §8 | removed. **One gate artefact: `results/paired_s1.json`.** The rev-2 changelog row keeps its original wording with a note that the names were superseded, rather than rewriting the record. |
| 3 | identity check should be mandatory | `--expect-argv-contains` is now a **required** sidecar argument; an optional identity check is one nobody runs |
| 4 | invalid metadata should stop before HTTP requests | the harness now aborts **before the first request**, writes a `gate: INVALID_METADATA` record stating that none were issued, and exits 1 — verified against a dead port, where no connection was attempted |

Also deduplicated: a provenance file that fails to load reported its problem twice,
once from `load_meta` and again from the schema check running on `None`.

## Changes since revision 9

| # | review point | resolution |
|---|---|---|
| 1 | `validate_meta()` was a spot check a stub could pass | full **schema validation** — 14 required fields with types and non-emptiness, `kind`/`label`/`manifest_patterns` cross-checks, digests matched against `^[0-9a-f]{64}$`, dependencies must carry a digest. New `load_meta()` turns unreadable / malformed / non-object JSON into `INVALID_METADATA` instead of a traceback after the samples are already spent. The review's stub now yields 14 problems. |
| 2 | spec's required metadata ≠ harness output; artefact naming muddled | harness now emits `sys.executable`, full `sys.argv`, `--gate-variant`, `cpu_count`/`meminfo`/`loadavg`, and `WOA23_ZARR_STORE` + `.zmetadata` mtimes taken from the sidecar. **Naming reconciled: one gate artefact, `results/paired_s1.json` (kind `paired_latency`).** The revision-9 names `http_reference_s1.json` / `http_candidate_s1.json` implied two separate single-backend runs and are retired. |
| 3 | Rev9 logic had no regression tests | **new `bench/test_provenance.py`** — 38 assertions over missing metadata, malformed JSON, non-object JSON, unreadable file, wrong label, stale manifest, wrong kind, empty hash set, unreadable hash, short/uppercase/non-string digest, bad seed (four spellings), missing dependency digest, wrong field types, recursive glob reaching a nested module, and an empty pattern aborting. |
| 4 | `api/*.py` claimed more coverage than it had | manifests are now **recursive** (`api/**/*.py`, `src/**/*.py`), so the claim and the behaviour agree without depending on the layout staying flat |
| 5 | launch command lossy; port could belong to any process | sidecar records **`launch_argv`** as an array alongside the display string, and takes `--expect-argv-contains`, aborting rather than describing a process that merely holds the port |

## Changes since revision 8

| # | review point | resolution |
|---|---|---|
| 1 | incomplete provenance should be a formal gate | **`INVALID_METADATA`**, §6.1.4 — fires on missing sidecar output, an empty hash set, any unreadable source, or `PYTHONHASHSEED != "0"` on either backend. It outranks every other verdict including `PASS`. All five branches verified. |
| 2 | fix the source manifest so `--source` cannot miss files | **`bench/manifests.py`**, §6.1.5 — manifests are glob patterns held in code and shared with this spec; `--source` is gone. A pattern matching nothing aborts, verified against the not-yet-built `api/`. |
| — | ±5% margin | recorded as a **PI decision of 2026-08-06**: an engineering no-regression threshold for S1, not a statistical estimate, not a public SLA, and carrying no process authorisation |
| — | 150 still inconclusive | unchanged — referred to the PI, never auto-passed |

## Changes since revision 7

| # | review point | resolution |
|---|---|---|
| 1 | `required_n` estimate presented as if it were an executable step | `plan_rung()` added; the tool now reports `samples_needed_estimate` **and** `next_ladder_rung` separately, and says "refer to PI" when the estimate exceeds the ladder's top |
| 2 | multiplicity claim overstated; wanted a full ladder null simulation | downgraded to **operational evidence**, with "no family-wise error rate is being controlled" stated outright. Full 21→60→150 null simulation added: 0 false `REGRESSION`, 3 false `IMPROVED`, 41/60 `INCONCLUSIVE` at σ=0.25 — and the 41 is reported as the honest headline, not buried |
| 3 | backend source hashes, `PYTHONHASHSEED`, dependency metadata missing | **new `bench/collect_backend_meta.py`** sidecar, run on the backend's host, reading `/proc/<pid>/` for launch command, cwd, worker PIDs, full source SHA-256, lockfile and `pip freeze` digests, and whitelisted env. Embedded verbatim by the harness; `metadata_complete` flags an incomplete record. |
| — | statistics documentation | the comparison is now stated as an **interleaved two-arm bootstrap, not observation-paired**, in the code and the spec; **±5% recorded as a PI/engineering threshold**, not a derived statistic |

**Justification for the ladder's top rung, produced while answering (2).** Scaling
each case's measured k=21 noise floor, the noisiest case (`regional_bbox`, ±11.7%)
needs ~115 samples to reach a ±5% half-width. 150 covers it; five of the eight cases
should resolve at 21. The ladder is sized to the data rather than picked.

**A privacy point I added unprompted:** the sidecar reads `/proc/<pid>/environ`,
which routinely contains credentials, and its output is committed to the repo. It
therefore reads a **whitelist** of six variables rather than dumping the
environment.

## Changes since revision 6

| # | review point | resolution |
|---|---|---|
| 1 | spec described `paired_selftest.json` as establishing `NO_REGRESSION`; the file says `INCONCLUSIVE` | corrected. The claim came from a run made before the `required_n` fix, citing a file written after it. The live self-test is retired in favour of `bench/test_paired_stats.py`. |
| 2 | `required_n()` still contradicted the verdict | **real bug, root cause found:** it used the symmetric half-width, but bootstrap intervals are asymmetric and only the binding side matters. Fixed, and pinned by a regression test using the exact failing numbers. |
| 3 | A/B order fixed, giving one arm the first slot every time | counterbalanced AB/BA alternation; the realised order is recorded per case |
| 4 | no bound on re-running until resolved | pre-specified ladder **21 → 60 → 150**, independent runs, verdict from the largest rung; still inconclusive at 150 goes to the PI. The p-hacking rationale is stated in the code and the spec. |
| 5 | pilot's heavy-case default, underpowered enforcement, unexpected statuses | pilot now covers heavy cases **by default** (`--skip-heavy` to opt out); `--warm` below k=21 is a hard error, not a warning; any unexpected status aborts rather than being timed |
| 6 | truncated hashes, no backend launch metadata | full SHA-256 for harness *and* backend sources; `--candidate-cmd` / `--reference-cmd` recorded verbatim |

**Found while fixing these, not raised in review:** `paired_bench.py` collected
status codes but never acted on them. A candidate returning fast `500`s would have
been timed as an enormous speedup. Status now voids the case (`INVALID_STATUS`), and
`INVALID` takes precedence over every other gate verdict.

## Changes since revision 5

All six points were code gaps, not wording. The harness now exists.

| # | review point | resolution |
|---|---|---|
| 1 | §6.1 still said `--repeat 3` while §6.1.1 said 21 | unified on `--warm 21` throughout |
| 2 | `http_bench.py` cannot interleave or bootstrap; the spec promised a protocol no tool implemented | **new `bench/paired_bench.py`** — interleaved A,B,A,B within each case, bootstrap CI, three-state verdict, raw samples persisted. **New `bench/paired_stats.py`** holds the sampling and statistics rules all tools share. `http_bench.py` is unchanged and keeps its role as the single-backend baseline capture. |
| 3 | pilot covered 3 cases, gate has 8 | pilot now defaults to the **full gate case list**; all 8 measured (§6.1.1) |
| 4 | cold/warm definitions diverged; `--repeat 21` yields 20 warm | one rule in `paired_stats.WARMUP_REQUESTS`; `--warm N` means N warm samples from N+1 requests. **This mattered:** the old pilot pooled the cold sample and reported ±10.2% for `point_profile`; excluding it gives **±3.9%**. |
| 5 | "true ratio is 1.0" overstated; raw samples not kept | reworded to **null hypothesis** with the drift caveat stated in the tool, the docstring and the result JSON; raw per-request latencies now written to every result file |
| 6 | no `INCONCLUSIVE` state; improvement and no-regression gates conflated | §6.1.2 — three states, separate gates, and the tool reports the sample count that would resolve an undecided case |

**A finding from building it, not from the review.** A self-test of the paired
harness with both arms on the *same* backend returned a CI of `[0.833, 1.211]` for
`point_profile` — five times wider than the ±3.9% the pilot had just measured for
that case. So the revision-5 design of "pilot supplies the tolerance" was itself
unsound: the tolerance and the interval estimate the same quantity from different
windows. The gate's own CI already carries the noise, so the threshold is now a
**practical-significance margin (±5%)** — a judgement about what is worth acting on
— and the pilot's role is sample-size planning. Details in §6.1.1.

Two smaller self-corrections: `noise_pilot.py` justified a hand-rolled LCG by citing
a rule that applies to JS workflow scripts, not Python — replaced with
`random.Random(seed)`, equally deterministic. And a pilot pool smaller than `k` is
now refused as `underpowered`, after a 4-sample run reported ±1.3% for a case whose
real floor is ±3.9%.

## Changes since revision 4

| # | review point | resolution |
|---|---|---|
| 1 | "no case slower than the public baseline" is not a sound pass/fail input | removed. The public path carries nginx, TLS and network while both gate arms are loopback. `http_prod_baseline_heavy.json` is retained as context only. |
| 2 | 5% and ≥3× unsupported | **Measured instead of argued.** New `bench/noise_pilot.py` + `results/noise_pilot_prod.json`: at `--repeat 3` the noise floor is **±84%** for `point_profile` and **±77%** for `small_bbox_full_depth`. A 5% gate at k=3 would have fired on noise almost every run. `≥3×` demoted to a **target**, with the Zarr→HTTP derivation shown to be unsound: the per-case `HTTP ÷ zarr-dist` ratio spans **0.60–2.27**, so no constant relates the layers. |
| 3 | needs noise pilot, more repeats, defined statistics | §6.1.1/§6.1.2 — `--repeat 21`; **per-case** tolerances taken from a reference-vs-reference pilot run on 8052, not from the public path; interleaved A,B,A,B sampling; verdicts from bootstrap 95% CIs on the paired median ratio rather than from bare median comparison. |

Correction to my own earlier reporting, surfaced by the pilot: the small-case
medians in `results/http_prod_baseline*.json` were taken at `--repeat 3` or `5` and
therefore carry **tens of percent** of uncertainty. `docs/BASELINE.md` has been
annotated. The multi-group and Zarr-level figures are unaffected — they are stable
to a few percent — so the headline finding does not move, but the small-case HTTP
numbers should not be quoted to three significant figures.

Also, the reviewer's summary attributed 1.35×/2.4× to the HTTP layer. Those are
**Zarr-level** figures for `surface_global` and `regional_bbox_025`
(`results/zarr_vm24_heavy.json`). There has never been an HTTP-layer measurement of
the candidate, which is precisely why §6.1.2 no longer asserts an HTTP-level
multiple as a gate.

## Changes since revision 3

| # | review point | resolution |
|---|---|---|
| 1 | amendment log implied the candidate-API path was already a PI decision while D1 was open | briefing log row split: the *briefing file's* location vs the *candidate API's* location are now distinct entries; D1 recorded as a PI decision of 2026-08-05 |
| 2 | no performance pass/fail threshold | §6.1.1/6.1.2 — thresholds fixed before the numbers exist (superseded in rev 5: the 5% band and the ≥3× gate were both unsupportable — see "Changes since revision 4") |
| 3 | RSS presented as gating; shared Dask `VmHWM` delta unsound | §6.2 rewritten as **informational, not a gate**. The dask delta is dropped entirely — `VmHWM` is monotonic and the process is shared, so a delta attributes nothing. Only the candidate's absolute figure vs the 4 GB ceiling is used, and no before/after memory claim is made. §8.4 restated accordingly. |
| 4 | candidate lifecycle missing; D2 covered only the reference | §0 splits the ask into **D2a** (candidate — required for *any* S1 measurement) and **D2b** (reference — upgrades the gate). §5.3.2 and §5.3.3 give preflight, PID capture, collision handling, trap-based cleanup, and end-of-run port verification for both. |
| 5 | C4/C5 lacked concrete inputs; C14 scope; benchmark metadata | §5.4 rewritten with full query strings, **all executed against production** — two of my assumptions were wrong and are corrected. C14 excludes C20. §6.1.3 lists required metadata including per-file source hashes. |
| 6 | OpenAPI `servers` might vary with listen port | §4.1 — checked: `woa23_app.py:30-34` hard-codes the URL, so `servers` is port-independent and the byte gate is sound |
| 7 | ROADMAP asserted "no sane consumer depends on ordering" | removed; replaced with an explicit statement that consumer dependence is **unmeasured** and must not be predicted |

Also corrected from my own re-testing, not raised in review: **§5.0** — revision 2
claimed CSV emits the literal `NaN` for land cells. Production emits an **empty
field**. `pl.from_pandas` defaults to `nan_to_null=True`, so NaN becomes null before
serialisation. The distinction is invisible in JSON and is a live trap for S2c.

## Changes since revision 2

| # | review point | resolution |
|---|---|---|
| Blocker | briefing rewritten to close a finding; BASELINE authority self-contradictory; stale VM34 disk figure | briefing reverted; `app2026` reopened as `OPEN DECISION`; authority split measurements-vs-rules; disk corrected to 407 GB with the old figure marked historical; an amendment log and a change policy added so author edits are auditable |
| High | latency before/after compares a public-nginx baseline against a loopback candidate | §6.1 — a controlled reference on 8052; the public baseline demoted to context and no longer a gate. *(The two per-arm filenames named at the time were superseded in rev 11 by the single `results/paired_s1.json`.)* |
| High | reference instance needs explicit authorisation | §0 D2 and §5.3.1 — start/health-check/stop/port-release procedure; spec is blocked until granted |
| Medium | error bodies escaped the byte rule | §5.2 — error bodies are byte-compared too; no semantic exception |
| Medium | "restart the worker" is not executable at `-w 1` | §6.2 — full server restart per case, PID capture, exact request sequence, sampling point, and Dask worker memory recorded separately |

## Changes since revision 1

| # | review point | resolution |
|---|---|---|
| Blocker | relative `data/` store path breaks when run from `dev2026/` | §4.2 — explicit absolute store path, no relative default |
| Blocker | `dev2026/api` vs `app2026/` inconsistency | **not resolved by me** — reopened as PI decision D1 |
| High | "byte-identical" vs parsed-JSON comparator | §5.2 — raw bytes are the gate; parsing is only used to *localise* a failure |
| High | `set` ordering varies with `PYTHONHASHSEED` | §5.1 — confirmed experimentally; comparison now runs against a **controlled reference instance**, not the live process. New roadmap step S2b owns the underlying fix. |
| High | Swagger endpoints unmentioned | §4.1 — all four routes enumerated and in the contract case list |
| High | heavy cases had no paired HTTP before/after | §6 — `results/http_prod_baseline_heavy.json` captured; both runs use `--include-heavy` |
| Medium | HTTPS to `127.0.0.1:8050` fails hostname verification | §5.3 — reference instance runs plain HTTP; the live-production check is explicitly `verify=False` and runs on VM24 |
| Medium | "heaviest case" ambiguous for RSS | §6.2 — two named cases, named metric, named method |
| Medium | dependency pins | §4.3 — exact versions read off VM24 |
| — | extra contract cases | §5.4 — C15–C19 added |

---

## 1. Problem

`woa23_app.py:17` installs a Dask **distributed** client as the process-wide
default scheduler, so every `.compute()` — triggered inside `to_dataframe()` —
round-trips through `tcp://localhost:8786`, a scheduler backed by a **single**
`dask-worker` (`--memory-limit 8GB`) shared with `tide_app` and `mhw_app`.

Measured on VM24 under the production interpreter, warm median (ms):

| query | `numpy` (no dask) | `threads` | `distributed` (production) | rows | speedup |
|---|---|---|---|---|---|
| `point_profile_025` | **29.1** | 114.7 | 244.0 | 102 | **8.4×** |
| `point_profile_multiparam` | **258.6** | 906.1 | 2,097.2 | 318 | **8.1×** |
| `readme_example` | **186.8** | 701.0 | 1,220.0 | 4,992 | **6.5×** |
| `point_profile` | **19.6** | 48.6 | 108.5 | 102 | **5.5×** |
| `regional_bbox` | **56.7** | 87.5 | 197.2 | 16,275 | **3.5×** |
| `small_bbox_full_depth` | **37.1** | 68.4 | 117.2 | 9,792 | **3.2×** |
| `regional_bbox_025` | **123.7** | 218.4 | 298.3 | 42,025 | **2.4×** |
| `surface_global` | **122.8** | 128.7 | 165.9 | 64,800 | **1.35×** |

The two largest cases were measured specifically because they are where Dask
should win — more work per task amortises scheduling. It does not win there
either. Across 102 → 64,800 rows there is **no query shape for which Dask pays for
itself**.

## 2. Goals

- Remove Dask from the WOA23 read path.
- Preserve the JSON and CSV response contract, verified by raw-byte comparison
  against a controlled reference (§5).
- Produce paired before/after benchmark JSON over the **same** case set, heavy
  cases included.

## 3. Non-goals

Excluded so the before/after attributes cleanly to the Dask change. Each is
already scheduled:

- Caching opened Zarr datasets (S3)
- polars build / AVX2, `--reload`, CSV temp-file leak (S2)
- **Making the output ordering deterministic (S2b)** — S1 keeps `list(set(...))`
  verbatim, bugs and all
- Dependency upgrades (S2c)
- Non-blocking endpoints, concurrency (S4)
- Re-chunking (S5)
- Replacing `.to_dataframe()` → `pl.from_pandas()` with a direct numpy path
- nginx cache policy (S6)

**Also out of scope: stopping or reconfiguring the Dask cluster on VM24.**
`dask-scheduler` and `dask-worker` keep running — `tide_app` and `mhw_app` depend
on them. `src/dask_client_manager.py` is shared infrastructure; not modified, not
deleted.

## 4. Design

### 4.1 Layout and routes

```
dev2026/
├── api/
│   ├── __init__.py
│   ├── app.py          # FastAPI app + all four routes
│   ├── config.py       # parameter/period/variable tables, grid dirs, store path
│   └── query.py        # the read pipeline (port of process_woa23_data)
└── pyproject.toml      # gains the web stack, pinned per §4.3
```

Module path `api.app:app`, run from `dev2026/` with its own uv-managed `.venv`,
mirroring the layout the PI already runs for the GHRSST refactor on this host
(`ghrsst-dev2026-phase2/dev2026/.venv/bin/gunicorn api.app:app`).

**Settled by the PI on 2026-08-05 (§0 D1): `dev2026/api/`.** `app2026/` is not used
in this phase.

**All four production routes are in scope** — dropping the Swagger pair at cutover
would silently break the published documentation link:

| route | source |
|---|---|
| `GET /api/woa23` | `woa23_app.py:374` |
| `GET /api/woa23/csv` | `woa23_app.py:415` |
| `GET /api/swagger/woa23` | `woa23_app.py:55` |
| `GET /api/swagger/woa23/openapi.json` | `woa23_app.py:50` |

The OpenAPI document is generated from route signatures and descriptions, so it is
contract surface too: the `servers` block, title, version, and every parameter
description must come out identical.

**The listen port cannot leak into `openapi.json`.** The review raised that two
instances on 8051 and 8052 would necessarily produce different `servers` blocks,
which would make a raw-byte gate on that route fail by construction. Checked:
`woa23_app.py:30-34` hard-codes

```python
openapi_schema["servers"] = [{"url": "https://eco.odb.ntu.edu.tw"}]
```

so `servers` is a constant, independent of bind address. The candidate must keep
that literal — copying it verbatim rather than deriving it from the request — and
the byte gate on `/api/swagger/woa23/openapi.json` is therefore sound. Any change
to that literal is a contract change and belongs in S6, not here.

### 4.2 Store path — no relative default

`woa23_app.py:63` hard-codes `zarr_store_path = "data/"`. That works only because
production gunicorn runs with cwd `~/python/woa23`. The candidate runs from
`dev2026/`, where `data/` would resolve to a non-existent `dev2026/data`.

The candidate therefore takes the store path from the environment, with **no
relative fallback**:

```python
WOA23_ZARR_STORE = os.environ["WOA23_ZARR_STORE"]   # KeyError at import if unset
```

On VM24 that is `/home/odbadmin/python/woa23/data`, **opened read-only**. Failing
loudly at import is deliberate: a silent fallback that half-works is how you ship a
benchmark that measured the wrong thing.

Resolution is otherwise unchanged — `f"{store}/{grid_path}/{subgroup}"`, same
subgroup logic.

### 4.3 Dependencies — pinned to production

Read off the production interpreter on VM24 (`~/.pyenv/versions/py311/bin/python3.11`):

| package | version | | package | version |
|---|---|---|---|---|
| fastapi | 0.115.12 | | xarray | 2025.3.1 |
| starlette | 0.46.2 | | zarr | 2.18.6 |
| uvicorn | 0.34.1 | | numcodecs | 0.15.1 |
| gunicorn | 23.0.0 | | numpy | 2.2.4 |
| orjson | 3.11.4 | | pandas | 2.2.3 |
| pydantic | 2.11.3 | | polars | 1.27.1 |

`dask` and `distributed` are **not** dependencies of the candidate — removing them
is the point. They stay in `dev2026/pyproject.toml` only for the benchmark harness,
which still needs to drive the `distributed` mode.

Upgrading any of these is S2c, with its own paired benchmark. S1 pins to
production so its A/B isolates the Dask change alone.

### 4.4 The change

```python
# removed:  client = get_dask_client("woa23api")   and close_dask_client in lifespan
ds = xr.open_zarr(zarr_group_path, chunks=None)    # was: xr.open_zarr(zarr_group_path)
```

`chunks=None` disables dask; xarray returns arrays backed by lazy Zarr indexing and
the selection materialises at `.to_dataframe()` — where it materialised before too.

Splitting into three modules is presentation, not behaviour: `config.py` and
`query.py` are lifted from `woa23_app.py` unchanged apart from the above and §4.5.

### 4.5 One deliberate, verified-equivalent difference

`woa23_app.py:353` calls `result_df.pivot(..., columns="parameter_variable", ...)`.
In polars 1.27.1 `columns=` is deprecated in favour of `on=`; the current code emits
a `DeprecationWarning` on every request. The candidate uses `on=`. Verified on
polars 1.27.1 — identical frame contents *and* identical column order.

This is the only intentional textual divergence. Anything else the harness finds is
a bug, not a decision.

### 4.6 Complete inventory of differences, generated not asserted

"Lifted verbatim apart from X" is a claim a reviewer cannot check by reading, and a
textual diff cannot check either: `woa23_app.py` carries dead code inside
triple-quoted string literals — a pandas implementation and a duplicate-check block,
both inert — which a text diff reports as removals it never made.

`bench/port_diff.py` generates this section. It parses both sides, drops bare string
statements at every nesting level, unparses and diffs; it covers **every function
defined in the original** and reports any it does not cover as a finding; and it
compares module level separately, which is where the Dask client lived.

**Functions: 8 of 10 identical.**
`to_lowest_grid_point` (5 statements), `determine_subgroup` (13),
`custom_json_serializer` (5), `generate_custom_openapi` (7), `custom_openapi` (3),
`custom_swagger_ui_html` (3), `get_woa23` (12), `get_woa23_csv` (16) — byte-identical
after unparse, decorators included.

**Two figures, and they are not the same figure.** `port_diff.py` reports **7 AST
changed lines**; they group into **5 conceptual changes**. A `-`/`+` pair for one
edited call is two lines and one change, and the removals have no partner. Both are
stated so neither can be read as the other.

**Every difference, and its authority:**

| # | where | difference | authority |
|---|---|---|---|
| 1 | `process_woa23_data` | `xr.open_zarr(path)` → `xr.open_zarr(path, chunks=None)` | §4.4 — the change this step exists for |
| 2 | `process_woa23_data` | `pivot(columns=…)` → `pivot(on=…)` | §4.5 |
| 3 | `lifespan` | `close_dask_client("woa23api")` removed | §4.4 |
| 4 | module | `client = get_dask_client('woa23api')` removed | §4.4 |
| 5 | module | `zarr_store_path`: `'data/'` → `os.environ['WOA23_ZARR_STORE']` | §4.2 |

**No unauthorised differences remain.** Three did, and were reverted after review:

- `all_columns`, `start_time` and `appending_start_time` — assignments never read,
  leftovers from commented-out timing prints — had been dropped as dead code. That
  was a judgement the spec did not authorise, and "it is obviously harmless" is how
  a verbatim port stops being one. Restored.
- `config.py` had wrapped the store lookup to raise a friendlier `RuntimeError`
  instead of the spec's bare `os.environ[...]` subscript. A small improvement, an
  undeclared difference, and — since it lives at module level — one the first
  version of `port_diff.py` could not have seen, because that version only compared
  function bodies. Reverted to the bare subscript.

The constant also keeps its original name, `zarr_store_path`, so difference 5 lives
entirely in `config.py` and `process_woa23_data` needs no edit for it. An earlier
draft renamed it to `ZARR_STORE_PATH`, which put an extra line in the function's
diff for no gain.

Import lines are reported but not counted: the candidate is three modules where the
original was one, so they cannot match and a difference there is not evidence by
itself. **`__future__` imports are the exception and are counted.** An earlier draft
of the port added `from __future__ import annotations` to all three modules out of
habit. Under it, annotations exist at runtime as strings — and FastAPI builds the
OpenAPI document by introspecting exactly those annotations, which C20 byte-compares.
It was removed rather than argued to be harmless, and `port_diff.py` now counts a
candidate-only `__future__` import as a difference so the same reflex cannot slip
through again.

### 4.7 What has been verified locally, without D2a

Dependencies are installed (`uv lock` / `uv sync`, §4.3 pins, `uv.lock` committed)
and `bench/smoke_local.py` runs entirely on the development machine — no VM24
process, no Zarr store, no requests. It confirms the candidate imports, exposes
exactly the four expected routes as `GET`, and produces an OpenAPI document whose
`servers`, title and version are the fixed production values.

It also settles **contract case C20 outright**, and does it the way the gate does:
it calls both Swagger route handlers and byte-compares the `JSONResponse.body` each
returns — `openapi.json` **8,597 bytes identical**, Swagger UI **868 bytes
identical**. An earlier version compared `json.dumps()` of the two schema dicts,
which is the right answer by a route the gate never takes; serialisation is where a
byte difference would appear, so evidence that skips it is evidence about something
else. Importing `woa23_app.py`
locally is safe — its Dask client fails to connect to a scheduler that is not there,
which `DaskClientManager` already swallows; a one-second connect timeout just stops
it waiting to find out. Nothing is started and nothing is contacted beyond a refused
localhost connection.

That removes the one contract case that never needed the data from the list waiting
on D2a.

## 5. Contract preservation

The API is published under DOI 10.5281/zenodo.13739802 and has external users.

### 5.0 The contract as measured, not assumed

Every statement here was verified against live production while writing this spec.
One of them contradicts what an earlier revision asserted from reading the code.

- **Missing data reaches the serialiser as `null`, not NaN — and that is an
  accident of a default.** `woa23_app.py:278` calls `pl.from_pandas(data)`, whose
  `nan_to_null` parameter defaults to **`True`**, so every NaN out of the Zarr store
  becomes a polars null at that line. Verified locally: default gives
  `'lon,t\n1.5,\n2.5,12.25\n'`; `nan_to_null=False` gives
  `'lon,t\n1.5,NaN\n2.5,12.25\n'`.
- **JSON:** land cells surface as `null`. Verified against production —
  `{"lon":15.5,"lat":22.5,"depth":0.0,"time_period":"0","temperature":null}`.
- **CSV:** land cells surface as an **empty field**, not the string `NaN`. Verified
  against production — `15.5,22.5,0.0,0,`.

  > **Trap for later steps.** Revision 2 of this spec asserted that CSV emits the
  > literal `NaN`. That was reasoning from `write_csv` in isolation, and testing
  > production disproved it. The distinction is *invisible in JSON*, because orjson
  > renders NaN and null identically as `null` — only CSV shows which is in the
  > frame. So any future change that builds the frame without `pl.from_pandas`
  > (S2c, or the direct-numpy path listed in the non-goals) will emit `NaN` in CSV
  > unless it converts explicitly, **and a JSON-only gate will not catch it**. S1
  > keeps the pandas path, so the behaviour is preserved here by construction.

- `/api/woa23` and `/api/woa23/csv` **disagree on status for an empty result**, and
  both spellings are contract: out-of-range inputs give JSON `200 []` but CSV
  `400 {"detail":"No data available for the given parameters."}`
  (`woa23_app.py:437`). Verified for both `lon0=200` and `dep0=6000`.
- A requested `append` variable that does not exist in the target group gives
  **404**, not 400 and not a silent omission — *unless* another requested variable
  does exist, in which case the response is 200 and the missing one silently
  vanishes. Verified: `append=ma&time_period=0` → 404; `append=ma,mn&time_period=0`
  → 200 without any `ma` column.
- Column order comes out of the pivot; row order out of the selection. Both are
  unstable across processes — see §5.1.
- The `mn` → bare-parameter rename applies only when `mn` is among the requested
  `append` variables.
- `time_periods` is renamed to `time_period` in the output.

### 5.1 The ordering problem, and why we do not compare against the live process

`woa23_app.py:160,171,190` build `list(set(...))` over strings. Python randomises
string hashing per interpreter, so:

- `variables` drives the order `result_list` is built, hence **column order** after
  the pivot;
- `pars` drives the `parameters` selection order, hence **row order**;
- `zarr_group_paths` is itself a `set`, so group iteration order varies too.

Measured across `PYTHONHASHSEED` 0–7: `variables` alternates between `['an','mn']`
and `['mn','an']`, and `pars` takes three distinct orders.

Twenty-four consecutive cache-busted production requests returned one single column
order — because gunicorn forks its workers from one master and they inherit its
hash seed. The instability is therefore **invisible until a restart**, and a
freshly started candidate is overwhelmingly likely to differ from a process that
has been up for 54 days, for reasons that have nothing to do with Dask.

**Consequence for this spec:** comparing a freshly started candidate against the
54-day-old live process would fail for reasons unrelated to Dask, so the gate takes
one of two shapes depending on §0 D2b. Fixing the nondeterminism is real work with a
user-visible effect, so it is its own step (S2b) and its own PI decision; S1 keeps
the behaviour verbatim either way.

### 5.2 What "identical" means — two variants, decided by D2b

#### 5.2A — With D2b: byte-exact (preferred)

Candidate on 8051 vs an unmodified reference on 8052, both `PYTHONHASHSEED=0`,
`-w 1`. Both sides serialise through the same orjson 3.11.4, so identical data must
produce identical bytes; anything weaker would let a float-formatting or whitespace
change through unnoticed.

- **JSON:** `resp.content` compared byte for byte.
- **CSV:** `resp.content` compared byte for byte.
- **Errors:** status code **and `resp.content` byte for byte**. There is no
  semantic exception for error bodies. FastAPI renders `HTTPException` through the
  same JSON encoder, so a whitespace or key-order difference in `{"detail": ...}`
  is exactly the kind of drift this gate exists to catch, and exempting it would
  let it through.
- Parsing is used **only to localise a failure** — reporting "row 412, key
  `salinity_an`" instead of "byte 91,244". It is never the pass criterion, for
  success or error responses.
- Headers are excluded except `content-type`. `Content-Disposition` on the CSV
  endpoint embeds today's date and is expected to match only because both runs
  happen on the same day; the comparator asserts the filename *pattern*, not the
  literal string.

#### 5.2B — Without D2b: semantic (fallback)

Candidate on 8051 vs the **live** production backend on `https://127.0.0.1:8050`.
Hash seeds differ and cannot be aligned, so ordering is out of scope for the
comparison:

- rows compared as a **multiset keyed on `(lon, lat, depth, time_period)`**;
- columns compared as a **set**, not a sequence;
- values compared with **exact equality**, including the null/NaN spelling in each
  format;
- error responses: status code plus parsed `detail`, since byte equality of the
  body is not available under differing seeds.

**This is semantic comparison and the spec calls it that.** Normalising both sides
and then declaring a byte match would overstate the guarantee, which the review
correctly objected to. The cost of 5.2B is real: a float-formatting or key-ordering
regression would pass. That is the price of not running a reference process, and it
should be weighed when answering D2b rather than papered over.

Under 5.2B the loopback connection to 8050 uses `verify=False` — deliberate, and
narrow: production's gunicorn presents the `eco.odb.ntu.edu.tw` certificate, so
hostname verification fails on `127.0.0.1`. It is a loopback connection to a process
we can see in `ps`, not a trust decision about a remote peer.

### 5.3 Verification harness — `bench/contract_diff.py` (new)

Runs **on VM24**; every backend involved is loopback-only.

| | 5.2A (with D2b) | 5.2B (without) |
|---|---|---|
| **A side** | reference: unmodified `woa23_app.py`, cwd `~/python/woa23`, `PYTHONHASHSEED=0`, HTTP `127.0.0.1:8052`, `-w 1`, no `--reload` | live production, HTTPS `127.0.0.1:8050`, `verify=False`, `-w 2`, untouched |
| **B side** | candidate, `PYTHONHASHSEED=0`, HTTP `127.0.0.1:8051`, `-w 1` | candidate, HTTP `127.0.0.1:8051`, `-w 1` |
| **rule** | raw bytes | semantic |

Under 5.2A a **separate, non-gating** check also runs candidate vs live production
semantically, to confirm the reference is faithful to what users actually get —
row multiset keyed on `(lon, lat, depth, time_period)`, column
*set* rather than order. This confirms the reference instance is faithful to what
users actually get today, without inheriting the hash-seed problem.

#### 5.3.1 Reference instance — the procedure being asked for (PI decision D2)

Nothing below runs until the PI authorises it.

**Before starting** — confirm 8051 and 8052 are free, so nothing else is displaced:

```bash
ss -lntp | grep -E ':(8051|8052)\b' || echo "both ports free"
```

**Start** (one line, from a directory the PI names; cwd must be `~/python/woa23` so
the app's relative `data/` resolves):

```bash
cd ~/python/woa23 && PYTHONHASHSEED=0 nohup ~/.pyenv/versions/py311/bin/gunicorn \
  woa23_app:app -w 1 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8052 \
  --timeout 120 > ~/woa23_bench2026/reference_8052.log 2>&1 & echo $! > ~/woa23_bench2026/reference_8052.pid
```

No `--reload` (it would watch files under `PRJ_DIR`), no TLS, `-w 1`.

**Health check** before any comparison — a known-good query must return 200 and a
non-empty body:

```bash
curl -sf -o /dev/null -w '%{http_code} %{size_download}\n' \
  'http://127.0.0.1:8052/api/woa23?lon0=135&lat0=15&parameter=temperature'
```

**Stop, and confirm the port is released:**

```bash
kill "$(cat ~/woa23_bench2026/reference_8052.pid)" && sleep 2
ss -lntp | grep -E ':8052\b' && echo "STILL BOUND" || echo "8052 released"
```

**What this touches:** it reads `~/python/woa23` and the Zarr store; it writes only
its own log and pidfile under `~/woa23_bench2026/`. It does open a second client to
the shared Dask scheduler — that is the one genuine side effect on other services,
and it lasts only as long as the comparison run. Production on 8050 is not
restarted, reconfigured, or touched.

#### 5.3.2 Candidate instance — same discipline (PI decision D2a)

The candidate is also a process on a production host and needs the same
authorisation and the same lifecycle. It does **not** connect to Dask.

```bash
# preflight: refuse to start if the port is taken by anything
ss -lntp | grep -qE ':8051\b' && { echo "8051 IN USE — abort"; exit 1; }

cd ~/woa23-dev2026/dev2026 && PYTHONHASHSEED=0 nohup .venv/bin/gunicorn \
  api.app:app -w 1 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8051 \
  --timeout 120 > run/candidate_8051.log 2>&1 & echo $! > run/candidate_8051.pid
```

`WOA23_ZARR_STORE=/home/odbadmin/python/woa23/data` must be exported (§4.2); the
process aborts at import if it is unset, which is the intended behaviour.

#### 5.3.3 Lifecycle rules for both instances

- **Directories:** `run/` (pidfiles, logs) and `results/` are created with
  `mkdir -p` before anything starts. A run that cannot create them fails rather
  than writing elsewhere.
- **Port collision:** preflight on both 8051 and 8052. If either is occupied,
  abort — never kill whatever holds it. `8050` is production and is never a target.
- **PID handling:** the pidfile holds the gunicorn **master**; the worker is
  `pgrep -P "$(cat <pidfile>)"`. A stale pidfile whose PID is gone, or whose PID is
  not a gunicorn master, is treated as an error, not silently overwritten.
- **Cleanup on failure or interrupt:** every start is paired with a `trap` that
  stops the instance and verifies port release. If a run is interrupted, the next
  run's preflight will find the port bound and abort, which is the desired
  failure — a stranded instance must be noticed, not stepped over.
- **End of run:** the gate has already re-checked both stores and processes and
  recorded the outcome in `post_run_drift`; then stop both instances, verify both
  ports released, remove pidfiles, and record that verification in the result JSON.
  Criterion §8.6 is not met on assertion alone.
- **Never:** `--reload` (it would watch files under `PRJ_DIR`), `-w` greater than 1,
  TLS on the candidate, or any write under `PRJ_DIR`.

### 5.4 Case list

The eight cases in `bench/queries.py` (`--include-heavy`), plus:

| # | case | what it pins down |
|---|---|---|
Query strings are given in full. Every one below was executed against live
production while writing this spec, and the "observed today" column is what it
actually returned — not what I expected it to return. Two of my earlier assumptions
were wrong and are corrected here.

| # | query string (appended to `/api/woa23`) | pins down | observed today |
|---|---|---|---|
| C1 | `lon0=135&lat0=15&append=an,mn,dd,ma,sd,se,oa,gp,sdo,sea` | every statistical field; **`ma` is absent from annual groups and is silently dropped** | 200 |
| C2 | `lon0=135&lat0=15&append=an` | the `mn` rename is *not* applied | 200 |
| C3 | `lon0=135&lat0=15&append=mn` | the rename *is* applied | 200 |
| C4 | `lon0=15&lat0=22&lon1=20&lat1=26&dep1=0&parameter=temperature` | all-land bbox (Sahara) → rows with `null`, **not** an empty result | 200, 30 rows, `"temperature":null` |
| C5a | `lon0=135&lat0=15&time_period=0&append=ma&parameter=temperature` | the **real** 404 path: the requested variable does not exist in that group | **404** `{"detail":"No data found for the specified query parameters"}` |
| C5b | `lon0=135&lat0=15&time_period=13&append=sdo&parameter=oxygen` | same, seasonal Oxy has no `sdo` | **404** same body |
| C5c | `lon0=135&lat0=15&time_period=0&append=ma,mn&parameter=temperature` | **asymmetry:** the same missing `ma`, but paired with a present `mn`, yields 200 and `ma` vanishes silently | 200 |
| C5d | `lon0=200&lat0=15&parameter=temperature` | out-of-range longitude → **200 with `[]`**, not 404 | 200, `[]` |
| C5e | `lon0=135&lat0=95&parameter=temperature` | out-of-range latitude | 200, `[]` |
| C6 | `lon0=150&lat0=20&lon1=135&lat1=15&parameter=temperature` | the lon/lat swap logic | 200 |
| C7 | `lon0=135&lat0=15&dep0=200&dep1=0&parameter=temperature` | the depth swap logic | 200 |
| C8 | `lon0=135&lat0=15&grid=0.25&parameter=oxygen` | 400 and `detail` text | **400** `{"detail":"Invalid parameters. Allowed parameters are temperature, salinity for grid size = 0.25"}` |
| C9 | `…&append=zzz` / `…&parameter=zzz` / `…&time_period=99` | all three 400 paths | 400 |
| C10 | `lon0=135&lat0=15&grid=1` / `grid=0.25` / `grid=25` / omitted | grid-resolution parsing | 200 |
| C11 | `lon0=135&lat0=15&time_period=0` / `time_period=16` | annual and seasonal boundaries | 200 |
| C12 | `lon0=135&lat0=15` / `lon0=135&lat0=15&lon1=135&lat1=15` | both single-point branches | 200 |
| C13 | `lon0=175&lat0=15&lon1=-175&lat1=20&parameter=temperature` | antimeridian — whatever today's behaviour is, preserved | to be recorded |
| C15 | `lon0=135&lat0=15&lon1=150` / `lon0=135&lat0=15&lat1=20` | the `lon1 is None or lat1 is None` branch | 200 |
| C16 | `lon0=135&lat0=15&parameter=salinity,temperature&time_period=13,0&append=mn,an` | reversed input order — **expected to expose §5.1** | 200 |
| C17 | `lon0=135&lat0=15&parameter=temperature,temperature&append=mn,mn` | the `set` dedup | 200 |
| C18 | `lon0=135&lat0=15&dep0=6000&dep1=7000` | depth beyond the 5,500 m maximum | 200, `[]` |
| C19 | any case above, with and without `_cb=<uuid>` | the ignored-parameter behaviour the benchmark harness depends on | identical bodies |
| C20 | `GET /api/swagger/woa23/openapi.json`, `GET /api/swagger/woa23` | the documentation surface; `servers` is a constant (§4.1) | 200 |

Plus the eight performance cases in `bench/queries.py` (`--include-heavy`).

**C14 — CSV replay.** Every case above **except C20** is re-run against
`/api/woa23/csv`. C20 is excluded because the Swagger routes are not data endpoints
and have no CSV form. Note that `/api/woa23/csv` raises 400 rather than 404 on an
empty frame (`woa23_app.py:437`), so the CSV replay of C5a/C5b/C5d/C5e pins a
*different* status code than the JSON original — that difference is part of today's
contract and must be preserved, not normalised away.

C5c and C13 are not claims that today's behaviour is correct. C5c in particular
looks like a bug — the same missing variable either 404s or is silently dropped
depending on what else was requested — but fixing it is a contract change and
belongs in its own step, not here. This spec pins it so it cannot change by
accident.

## 6. Benchmarks required

### 6.1 Latency

The paired gate is **reference vs candidate, both on VM24 loopback, same harness,
same invocation**. Anything else mixes nginx, TLS and network into a number that is
supposed to isolate one change.

| file | produced by | kind | role |
|---|---|---|---|
| `results/paired_s1.json` | `bench/paired_bench.py`, one run against **both** arms | `paired_latency` | **the gate** — the only artefact a verdict is read from |
| `results/noise_pilot_candidate.json` | `bench/noise_pilot.py` against the candidate on 8051 | `noise_pilot` | sample-size planning |
| `results/meta_candidate.json`, `results/meta_reference.json` | `bench/collect_backend_meta.py` on VM24 | `backend_meta` | provenance; embedded verbatim into the gate file |
| `results/http_prod_baseline_heavy.json` | `bench/http_bench.py` against the public URL | `http` | **context only, not a gate** |

Revision 9 named two files `http_reference_s1.json` and `http_candidate_s1.json`, as
though the gate were two separate single-backend runs compared afterwards. It is
not — the whole point of `paired_bench.py` is that both arms are sampled inside one
interleaved run, so there is **one** gate artefact and those two names are retired.
`http_bench.py` keeps its single-backend role for context captures only.

Both arms are driven by one `bench/paired_bench.py` invocation — same 8 cases,
`--include-heavy`, `--warm 21`, interleaved and counterbalanced. Cache-busting stays on even though
loopback has no nginx cache, so the two runs remain comparable with the public
baseline in shape.

`http_prod_baseline_heavy.json` is retained because it is the only measurement of
what users actually experience (nginx + TLS + network on top). Its medians —
158.2 / 1,986.5 / 1,454.0 / 202.3 / 245.3 / 376.1 / 146.6 / 434.0 ms — bound how
much of the win reaches the outside world, but the reference-vs-candidate pair is
what the speedup claim rests on.

Under 5.2B (no D2b) the "before" side is live production on `https://127.0.0.1:8050`
instead, and two confounders must be stated in the result file rather than glossed:
it serves **TLS** where the candidate serves plain HTTP, and it runs **`-w 2`** where
the candidate runs `-w 1`. Both inflate the apparent speedup slightly. Sequential
probing makes the worker count nearly irrelevant, but the claim must be written as
"≥ X× including a TLS-on-loopback confounder", not as a clean number.

#### 6.1.1 The noise floor, and why the pilot no longer sets the threshold

Revision 4 proposed "more than 5% slower fails" at `--repeat 3`. Revision 5 replaced
that with per-case tolerances measured by a pilot. **Both were wrong, and building
the harness is what showed it.**

`bench/noise_pilot.py` samples one backend, then bootstraps the ratio of medians
between two arms of `k` draws from that pool. Both arms are the same backend, so the
**null hypothesis** is a ratio of 1.0 — not a guarantee of it: drift, GC, page cache
and co-tenant load are inside those samples rather than controlled away. Over all
eight gate cases against public production
(`results/noise_pilot_prod.json`, 25 warm samples each):

| case | median | spread | k=3 | k=21 |
|---|---|---|---|---|
| `point_profile` | 113.0 ms | 141% | ±72.8% | ±3.9% |
| `point_profile_multiparam` | 1,923.7 ms | 151% | ±13.1% | ±3.7% |
| `readme_example` | 1,217.3 ms | 25% | ±11.7% | ±2.3% |
| `small_bbox_full_depth` | 155.5 ms | 140% | ±22.6% | ±4.8% |
| `regional_bbox` | 168.2 ms | 84% | ±27.5% | ±11.7% |
| `surface_global` | 277.0 ms | 10% | ±4.5% | ±1.6% |
| `point_profile_025` | 153.0 ms | 230% | ±88.1% | ±10.0% |
| `regional_bbox_025` | 357.6 ms | 76% | ±42.2% | ±7.3% |

`--repeat 3` is unusable — up to ±88% on a small case. That much of revision 5
stands.

**What does not stand is feeding these tolerances into the gate.** A self-test of
the new paired harness with *both arms pointed at the same backend* produced, for
`point_profile`, an interval of `[0.833, 1.211]` — five times wider than the ±3.9%
the pilot had just reported for the same case. The two were estimating the same
quantity from different windows and disagreeing, so a tolerance imported from one
run would convert ordinary noise in another into a verdict.

The fix is to stop double-counting: **the gate's own confidence interval already
carries the noise.** So the threshold's only job is to say what size of slowdown is
worth acting on. That is a practical-significance judgement — fixed at **±5%** — not
a noise estimate, and the pilot's job becomes sample-size planning rather than
threshold-setting.

Sampling rules, now shared by all three tools through `bench/paired_stats.py`:

- `WARMUP_REQUESTS = 1`. `--warm 21` means **21 warm samples**, from 22 requests.
  Revision 5 said `--repeat 21` and would have yielded 20; `http_bench.py` already
  reported its median over `samples[1:]`, and the pilot had been pooling everything
  including the cold sample. That mismatch was real and it mattered: correcting it
  moved `point_profile`'s measured floor from ±10.2% to ±3.9%, because the cold
  outlier had been inflating it.
- The comparison is an **interleaved two-arm bootstrap, not an observation-paired
  one**. Each arm is resampled independently at its own size. Interleaving pairs the
  arms in *time*, which is what defends against drift over a run, but two adjacent
  requests to different backends are not two measurements of one underlying
  quantity, so no per-observation pairing is exploited and none is claimed. A truly
  paired design would difference matched observations and resample the differences;
  it would give narrower intervals, and it is not what this does.
- The **±5% margin is a PI / engineering threshold**, not a statistic. Nothing in
  the data derives it — it encodes a judgement that a WOA23 query getting up to 5%
  slower is not worth blocking a change over, and it needs PI sign-off rather than
  reviewer agreement.
- Requests are **interleaved and counterbalanced**: within each case the pair order
  alternates AB, BA, AB, … so neither arm systematically occupies the first slot of
  a pair, where connection and cache state differ. The realised order is written to
  the result file rather than assumed from the flag.
- **Every response's status is checked** against the expected code, per arm. A
  mismatch voids the case (`INVALID_STATUS`) instead of contributing timings.
- **Raw per-request latencies are written to the result file**, so every statistic
  can be recomputed without re-running anything.
- The pilot covers the **full gate case list**, not a subset; the floor varies by
  7× across cases, so a per-case number is the only useful kind.
- A pilot whose pool is smaller than `k` is flagged `underpowered` and refused,
  because resampling 21 draws from 4 observations reported ±1.3% for a case whose
  real floor was an order of magnitude worse.

#### 6.1.2 Pass / fail — three states

`bench/paired_bench.py` implements this; it is not a description of intent.

| verdict | condition | meaning |
|---|---|---|
| `REGRESSION` | CI low > 1.05 | established: the whole interval is worse than we accept |
| `NO_REGRESSION` | CI high ≤ 1.05 | established: the whole interval is acceptable |
| `INCONCLUSIVE` | interval straddles 1.05 | **not a pass.** The tool reports how many samples per arm would resolve it. |
| `INVALID_STATUS` | any request returned an unexpected status | the case is void, not fast. A backend erroring quickly would otherwise read as a large speedup. |
| `INVALID_METADATA` | run-level: provenance missing or unusable | see §6.1.4 |
| `IMPROVED` | CI high < 1.0 | established speedup |
| `NOT_IMPROVED` | CI low ≥ 1.0 | established absence of speedup |

The two gates are separate, as the review asked:

- **Correctness/no-regression gate:** no case may come back `REGRESSION`. Any
  `INCONCLUSIVE` case must be re-run at the suggested sample count until it
  resolves; an unresolved case blocks the step rather than passing it.
- **Improvement gate:** `readme_example` and `point_profile_multiparam` must each
  come back `IMPROVED`. These are the work-dominated, low-noise cases; if the
  change cannot demonstrate a speedup there, the premise did not survive the HTTP
  layer.

Overall verdicts, in precedence order: **`INVALID_METADATA`** (§6.1.4) >
**`INVALID_RUNTIME_DRIFT`** (a process, port, source file or store **changed or
became unverifiable** during sampling) >
`INVALID` (any unexpected HTTP status) > `FAIL` (any established regression) >
`FAIL_NO_ESTABLISHED_IMPROVEMENT` > `INCONCLUSIVE` > `PASS`.

The **±5% margin is a PI decision, taken 2026-08-06**: an engineering
no-regression acceptance threshold for S1. It is explicitly *not* a statistical
estimate and *not* a public SLA, and it carries no process authorisation.

**Sample-size escalation is pre-specified, and bounded.** Re-running until a case
resolves is p-hacking — each extra peek at an accumulating sample raises the chance
of crossing a threshold by luck. So the ladder is fixed in advance at
**21 → 60 → 150 samples per arm**, each rung an independent run rather than an
accumulation, with the verdict taken from the largest rung run. A case still
`INCONCLUSIVE` at 150 is **reported as inconclusive and referred to the PI**; it is
not promoted to a pass, and the ladder is not extended to chase a result.

The rungs are not arbitrary. Scaling each case's measured k=21 floor as 1/√n, the
samples needed to bring the half-width to 5% are:

| case | floor at k=21 | n for ±5% | rung |
|---|---|---|---|
| `surface_global` | ±1.6% | 2 | 21 |
| `readme_example` | ±2.3% | 4 | 21 |
| `point_profile_multiparam` | ±3.7% | 11 | 21 |
| `point_profile` | ±3.9% | 13 | 21 |
| `small_bbox_full_depth` | ±4.8% | 19 | 21 |
| `regional_bbox_025` | ±7.3% | 45 | 60 |
| `point_profile_025` | ±10.0% | 84 | 150 |
| `regional_bbox` | ±11.7% | 115 | 150 |

150 is chosen to cover the noisiest measured case with margin; five of eight should
resolve at the first rung. These are public-path figures and loopback should be
quieter, so this is a conservative plan.

**Estimate and instruction are reported separately.** `required_n()` answers "how
many would resolve this" and its answer is usually off-ladder — 23 is a valid
estimate and an invalid instruction, because choosing 23 after seeing a result is
exactly the unplanned sample size the ladder exists to prevent. The tool reports
`samples_needed_estimate` alongside `next_ladder_rung`, the smallest pre-specified
rung that meets it, and says "refer to PI" when the estimate exceeds the ladder.

`required_n()` estimates which rung to jump to. It had a bug worth recording: it
used the symmetric half-width, but bootstrap intervals are asymmetric and only one
side binds. For ratio 1.022 with CI [1.003, 1.051] it reported "21 is enough" while
the verdict was `INCONCLUSIVE` — symmetric half-width 0.0239 was below the 0.0276
gap to the threshold, but the binding upper half-width was 0.0286, above it. Fixed
to use the binding side, and pinned by a test.

**Harness self-test — local, not against production.** Revision 6 pointed both arms
at public production and described the outcome as "no regression established on the
stable case". The stored artefact does not say that: `results/paired_selftest.json`
records **`INCONCLUSIVE` on both cases**, gate
`FAIL_NO_ESTABLISHED_IMPROVEMENT`. The claim came from an earlier run than the file
it cited. Corrected, and the live self-test is retired — checking arithmetic should
not generate production traffic.

The null case is a property of the statistics, so it is now
`bench/test_paired_stats.py`, which runs offline in about a second:

- two arms drawn from one distribution must not yield `REGRESSION` or `IMPROVED`
  **more often than nominal**. A first draft asserted "never" and failed on trial 2
  of 20 — correctly. A 95% interval is wrong ~5% of the time by construction, ~2.5%
  per direction. Observed: 1/100 each.
- a synthetic 40% slowdown must be called `REGRESSION`, and its mirror `IMPROVED`,
  or the gate is decorative;
- plus the `required_n` case below, pinned as a regression test.

**On multiplicity — this is operational evidence, not a controlled error rate.**
Across eight cases at ~2.5% each, a spurious `REGRESSION` should turn up in roughly
one run in five, so a single flag escalates rather than condemning the change. But
the 1/100 figure above is one observation of a procedure, not a proof about it, and
**no family-wise error rate is being controlled** — there is no Bonferroni or FDR
adjustment here, and none is claimed.

What *is* measured is the escalation procedure end to end, since taking three looks
at the same question does not inherit the single-comparison rate. Simulating the
full 21 → 60 → 150 ladder against synthetic null data (σ = 0.25 lognormal, 60
trials): **0 false `REGRESSION`, 3 false `IMPROVED`, 19 `NO_REGRESSION`, 41
`INCONCLUSIVE`**, mean final rung 136. Against a real +20% slowdown, 18 of 20 trials
end in `REGRESSION`.

The 41 inconclusive results are the honest headline: **at that noise level the
procedure often cannot establish "no regression" even at 150 samples**, because the
interval half-width lands right at the 5% margin. That simulation is deliberately
pessimistic — σ = 0.25 corresponds to a ±10% floor at k=21, worse than six of the
eight real cases — but it is why the ladder's top rung is 150 and not smaller.

**`≥ 3×` is a target, not a gate.** Revision 4 made it a hard criterion; that was
not supportable. The 6.5× and 8.1× figures are **Zarr-level**
(`results/zarr_vm24_all.json`, `numpy` vs `distributed`) and do not translate to
HTTP by any constant. Measured per case, `prod HTTP ÷ zarr-distributed` ranges from
**0.60** (`point_profile_025`) to **2.27** (`surface_global`). A ratio below 1.0
means production HTTP beat my on-host Zarr benchmark, which is impossible if the
Zarr work were a strict subset — so the harness carries overhead production does
not, and no fixed factor relates the layers. Missing the target prompts
investigation, not automatic failure.

**The public baseline is not a pass/fail input.** Revision 4 included "no case
slower than the public baseline"; removed. That path carries nginx, TLS and network
while both gate arms are loopback. `http_prod_baseline_heavy.json` stays as context
— it bounds how much of a win reaches real users — and nothing more.

Reference and candidate runs execute **back to back in the same session**, and the
pilot is re-run immediately beforehand, because the floor is not stable across
sessions. **It samples the candidate on 8051, not the reference.** Since the
threshold became a practical-significance margin rather than a measured tolerance
(§6.1.1), the pilot only sizes the sample, and pointing it at the candidate keeps 208
requests and 352 MiB off the production backend.

That is a narrowing, not a free lunch. A candidate-only pilot measures the
candidate's noise floor and says nothing about production's. The gate's **validity**
is unaffected — its interval comes from both arms' actual samples — but the
**planning evidence** now covers one arm, so a case sized against a quiet candidate
can still return `INCONCLUSIVE` against a noisier production. The ladder absorbs
that.

#### 6.1.4 `INVALID_METADATA` — provenance is a gate, not a footnote

**The check runs before the first request.** Sampling a run already known to be
unpublishable wastes the operator's time, puts avoidable load on a production host,
and produces numbers that will get quoted anyway. On failure the harness writes a
`gate: INVALID_METADATA` record noting that no requests were issued, and exits 1.

Validation is a **schema check**, not a spot check. Revision 9 verified only that
source hashes were non-empty and readable and that the seed was pinned, which a
two-field stub would have satisfied — `{"source_sha256": {"fake.py": "000…"},
"env": {"PYTHONHASHSEED": "0"}}` passed. It now produces **26**.

`provenance.validate_meta()` requires all **26** fields the sidecar emits, with
their types, non-empty: `kind`, `label`, `manifest_patterns`, `collected_at`,
`host`, `kernel`, `cwd`, `executable`, `launch_argv`, `launch_command`,
`master_pid`, `port`, `listener_pids`, `port_verified`, `proc_starttime`,
`boot_id`, `expect_argv_contains`, `worker_pids`, `env`, `env_whitelist`,
`collector_python`, `source_sha256`, `store_path`, `store_source`,
`zmetadata_fingerprints`, `dependencies`.

The set is **reconciled by a test, not by assertion.** Revision 16 claimed the
reconciliation was programmatic when it had only been done once by hand in a shell —
the repository still held two independently maintained lists. The record is now
assembled by `collect_backend_meta.build_meta()`, split out of `main()` precisely so
that `test_schema_matches_sidecar_output` can build one without a live process and
assert both directions: nothing emitted that the validator ignores, nothing required
that the sidecar omits. That test fails the moment either list grows a field the
other does not know about, which is what `expect_argv_contains` needed and did not
have.

`port_verified` is a required boolean and **must be true**. Since revision 18 the
sidecar cannot emit `false`: every path that fails to identify the master now raises
before the record is built, so the flag is a belt-and-braces check against a record
that was hand-edited or produced by an older collector rather than something the
current collector can produce. Revision 17's description of a `port_verified: false`
record and an exit-3 path described code that revision 18 made unreachable; the dead
branch is removed and this paragraph now matches what runs.

`listener_pids` is validated for **contents, not just presence**: every entry a
positive integer (booleans rejected), no duplicates, and `master_pid` must be among
them — otherwise the record's own fields disagree about which process held the port.
Revision 18 checked only that the list was non-empty, so `["not-a-pid"]` passed.

Three of those were added in revision 15 after the review found the gap that
matters most here: **`expect_argv_contains` was produced by the sidecar and called
mandatory in this document, but was not in the required set** — so a record with it
stripped validated cleanly, and `post_run_runtime_check()` then had nothing to assert
and skipped the argv identity check without saying so. Confirmed by test before
fixing. A validator that passes a record which silently disables a downstream check
is worse than no validator, so it is now required as a non-empty list of non-empty
strings, and `port` is required as an integer in 1–65535 rather than optional. Beyond presence it checks that `kind` is
`backend_meta`; that `label` matches the arm being described; that
`manifest_patterns` equals the manifest **currently in force**, so a stale
provenance file cannot vouch for a different file set; that `master_pid` is a
positive integer and `port` an integer in 1–65535 — **not nullable**, since the port
is part of what identifies which arm a record describes; that `launch_argv` holds only
strings; that every digest matches `^[0-9a-f]{64}$`; and that `dependencies`
carries at least a lockfile or pip-freeze digest.

**The recorded sources must be the manifest's files, with the right hashes.** Two
checks, and the second is the one with teeth:

1. **File set.** The manifest is re-expanded against the backend's own `cwd` and the
   key sets compared, catching a fabricated entry and a subset that quietly omits
   the file that changed.
2. **Digests, recomputed.** Each file is re-hashed during validation and compared
   against the record. Checking that a digest *looks* like a digest is not enough —
   a well-formed hash of the wrong content passes every syntactic rule, and until
   revision 21 it passed this check too. The proof was sitting in the test suite:
   the fixtures wrote `x` to each file while recording `"a" * 64`, and nothing
   objected. Fixtures now carry real digests.

A digest that disagrees is not necessarily tampering — a source edited between
provenance collection and the gate looks identical to it. Either way the record no
longer describes what is on disk, and the run cannot be published.

**The same check runs again after sampling.** Re-hashing at validation time covers
provenance-collection through gate-start and nothing after it: a file edited while
the benchmark was running would leave the pre-run digests describing code that
stopped being what served requests partway through. `post_run_runtime_check()`
therefore re-runs the source verification alongside the store and process checks, so
source drift lands in `INVALID_RUNTIME_DRIFT` in the same artefact rather than
depending on anyone remembering to run a separate command afterwards.

The verdict was called `INVALID_STORE_DRIFT` until revision 23, by which point it
covered four different things and the name described one of them. Renamed rather
than re-described: a name that has to be explained away in prose is a name that will
mislead whoever reads the artefact without the prose.

Both need the gate to run on the backend's host, which it does by construction since
both arms are loopback; an unreachable `cwd` is **reported, not skipped**, because an
unverifiable source set is not a verified one.

`kernel`, `store_path`, `store_source` and `zmetadata_fingerprints` are required
alongside the rest. Each group's fingerprint must carry a well-formed SHA-256 and an
integer `mtime_ns`; an `<unreadable: …>` entry or a store-scan `error` fails the run.

**This is a metadata fingerprint, not a data checksum.** `.zmetadata` holds array
shapes, chunk grids, compressors and attributes — it says nothing about the bytes
inside the chunks, and two stores with identical `.zmetadata` can hold different
values. Agreement is therefore evidence that both arms were **configured against the
same store**, not proof that they read identical data; that proof comes from the
contract gate in §5, which compares the responses themselves. A digest mismatch is
reported as a *metadata* mismatch and worded accordingly.

**Precondition, and it is a real one:** the store is treated as **read-only and
frozen for the duration of the campaign** — no ingest, no re-consolidation, no
`.zmetadata` refresh between the pilot, the gate and the post-run check. Nothing in
the harness can enforce that; it is an operational commitment, and the fingerprints
exist to catch it being broken rather than to prevent it.

**Digest over timestamp.** Second-resolution mtimes lose same-second edits and any
`touch` moves them without a byte changing, so each group's `.zmetadata` is hashed
and the nanosecond mtime kept as corroboration. The
scan is **recursive** (`**/.zmetadata`): revision 11 globbed `*/*/*/.zmetadata`,
hard-coding the grid/period/parameter-group depth, so a store reorganised to any
other shape would have produced an empty and confidently wrong fingerprint. The
three-level layout is what exists today; the scan no longer depends on it.

**The store path is resolved by *which backend it is*, not by what is in the
environment.** The two are not symmetric:

| backend | store | `store_source` |
|---|---|---|
| `reference` | `cwd/data` — `woa23_app.py:63` hard-codes it and never reads the environment | `hardcoded_relative` |
| `candidate` | `WOA23_ZARR_STORE`, mandatory with no fallback (§4.2) | `env` |

Revision 12 branched on whether the variable happened to be set, which for the
reference would have recorded a stray environment value as the store in use while
the process was reading somewhere else entirely. The sidecar now decides from the
manifest label, refuses to guess when a candidate has no `WOA23_ZARR_STORE`, and
`validate_meta` rejects a record whose `store_source` does not match its arm.

**Both arms must be reading the same store, and disagreement is fatal.** A latency
comparison across two different stores measures the stores, not the change.
`validate_store_agreement()` compares the resolved `store_path`, the set of groups
found, and each group's digest and `mtime_ns`, distinguishing the two failures it
can see: differing digests mean the arms were pointed at stores whose **consolidated
metadata differs**; identical digests with differing mtimes mean the store was
**touched between the two provenance collections**. Either fails the run.

**The sampling window is covered inside the same run, not by a separate command.**
The two pre-run collections bracket each other, not the sampling. So after the last
request `paired_bench.post_run_runtime_check()` re-checks **all four** things the run
depends on staying still, **in the same invocation**, and folds the outcome into this
run's verdict as `INVALID_RUNTIME_DRIFT`:

| what | what would go unnoticed without it |
|---|---|
| **process** | a backend that restarted or died, or a PID recycled into another process — the timings would not all come from one process |
| **port** | the socket handed to a different process while the original still lives, so PID, start time and argv all still match while the requests went elsewhere |
| **sources** | a file edited after its digest was recorded, leaving the record describing code that stopped serving requests partway through |
| **store** | `.zmetadata` rewritten underneath the reads |

"Changed" is not the only failure: a check that can no longer be *performed* — an
unreadable `/proc` entry, an ambiguous listener set, a store that has become
unreachable — fails the same way. Not knowing and knowing nothing moved are
different answers, and only one of them clears the gate.

**The precedence itself is pinned by a test.** Every check feeding the verdict had
one; the mapping from findings to verdict had none, so an edit routing drift to a
passing status would have been caught only by prose. `decide_gate()` is split out of
`main()` for that reason, and `test_gate_precedence` asserts each finding's verdict
in isolation plus the orderings that matter — metadata invalidity above everything,
drift above every performance result, a regression above an unproven improvement.
Validity questions come first because a run whose provenance or runtime cannot be
trusted has no performance result to report at all.

Revision 13 left that check outside, as `collect_backend_meta.py --against` run
afterwards. That produced **contradictory artefacts**: the gate file could say
`gate: PASS` while a separate command exited 2 because the store had moved, and
anyone reading the artefact later sees only the `PASS`. A verdict that depends on a
check must contain it.

Everything the post-run check needs is already in the pre-run record, so it asks the
operator for nothing. It re-checks **four** classes of runtime state, each against
the fields the sidecar captured before sampling:

| class | what it re-reads | provenance fields it needs |
|---|---|---|
| **process** | `/proc/<pid>/` for the master | `master_pid`, `proc_starttime`, `boot_id`, `launch_argv`, `expect_argv_contains` |
| **port** | the listener set from `ss` | `port`, `master_pid` |
| **sources** | re-hashes every manifest file | `cwd`, `label`, `source_sha256` |
| **store** | re-fingerprints every `.zmetadata` | `store_path`, `zmetadata_fingerprints` |

Concretely it catches a backend that **restarted or died during sampling** (the PID
is gone, so the timings do not all come from one process), a **recycled PID** (same
PID, different start time), a **host reboot** (different boot id), a **port that
changed hands**, a **source file edited mid-run**, and a **store rewritten
underneath the reads** — plus, in every case, the possibility that the check simply
could not be performed, which fails the same way.

The port case is not covered by any of the others: another process can bind the port
after the first releases it while the original process is still alive, so PID, start
time and argv all still match and the HTTP traffic went somewhere else entirely. The
post-run check therefore re-reads the listener set directly and requires the master
still to hold the port.

**Identifying the master needs more than reading the first PID `ss` reports.** A
forking server has several: gunicorn's master creates the listening socket and each
worker inherits it, so `ss -lntp` lists all of them in no meaningful order. Revision
16 took the first match — on the live production port that is **worker 4366 while
the master is 3960**, so every identity field in the record would have described the
wrong process. `pids_on_port()` now returns the whole set and `master_of()` picks the
one whose parent is not also in it. Verified against all three live services on
VM24: 8050 `[3960, 4334, 4366] → 3960`, 8040 `[1766639, 1766641, 1766642] →
1766639`, 8030 `[2834272, 2834335, 2834336] → 2834272`.

Two rules follow, and both are strict:

- **`--pid` must equal the derived master, not merely appear among the listeners.**
  Every worker holds the inherited socket, so membership proves nothing; a worker
  recorded as `master_pid` would poison the start time, the worker list and every
  post-run comparison.
- **An ambiguous listener set fails closed.** `master_of()` returns `None` when no
  single process in the set is the parent of the rest — two unrelated servers on one
  port, a parent that already exited, **or any PID whose `/proc/<pid>/stat` cannot be
  read**. That last case was wrong until revision 19: a `None` parent is trivially
  "not in the set", so a single unreadable PID looked like a root and was returned as
  the master. An unreadable parent is not evidence of being a root; it is evidence of
  knowing nothing. Guessing would produce a record that looks
  authoritative and names the wrong process, so both the sidecar and the post-run
  check treat `None` as a failure. "We could not tell" and "it is still the same
  process" are different answers and only one of them clears the gate.

`ss` output is also **accumulated across every matching row**, not read from the
first. A server appears once per address family — VM34's Postgres is two rows,
`0.0.0.0:5433` and `[::]:5433` — and stopping at the first would silently drop
whichever came second.

Detecting the recycled PID needs more than PID and argv. A backend restarted by the
same launch command has *identical* argv, so those two together cannot tell the
replacement from the original. The sidecar therefore records **`proc_starttime`**
(field 22 of `/proc/<pid>/stat`, clock ticks since boot) and **`boot_id`**, and the
post-run check compares those first — a start time that moved means a different
process wearing the same PID, and a changed boot id means the host rebooted mid-run.
Verified arithmetically against the live production gunicorn master on VM24, because
the first version of this paragraph got the units wrong — it read "start time 1874,
consistent with its 54-day uptime", but 1874 does not encode uptime at all. With
`CLK_TCK = 100`, field 22 of `1874` means the process started **18.7 s after boot**.
The consistency check is `uptime − start` : host uptime 55.76 days minus 18.7 s
gives a process age of 55.76 days, and the derived start of 2026-06-11 16:45:36
matches `ps -o lstart` at 16:45:35. The `comm` field can contain spaces and
parentheses, so the parse anchors on the last `)` rather than splitting the line.

`collect_backend_meta.py --against` remains available for an out-of-band check, and
is now hardened: it validates the earlier record with the same `load_meta()` /
`validate_meta()` the gate uses and refuses to compare against an invalid one;
**Both records are validated, not just the baseline.** Revision 19 ran only the
earlier file through `validate_meta()`, so a freshly collected record with an
unpinned hash seed, a truncated source manifest or an unreadable store could still
be reported as showing no drift — a reassurance about something not fit to be
compared at all. Now each side is validated and its problems are labelled `earlier
record:` or `current record:`.

`compare_identity()` confirms the two records describe the *same* backend — arm,
port, manifest, cwd, PID, start time, boot id, argv and source digests must all be
unchanged, or it says so
rather than reporting a reassuring "store unchanged" about two unrelated things; and
`--out` naming the same file as `--against` is refused, since the comparison would
overwrite its own baseline and then find nothing changed.

`load_meta()` handles the file itself: unreadable, not JSON, or JSON that is not an
object all become `INVALID_METADATA` problems. **A broken provenance file must not
raise** — a traceback would tell the operator nothing the verdict could not have
said, and under the old ordering it would have discarded samples already collected.

A number whose backend cannot be reconstructed cannot be re-checked by anyone later,
and an unpinned hash seed means the ordering the contract gate compared is not the
ordering a rerun would produce. Both invalidate a run rather than annotating it, so
`paired_bench.validate_meta()` fails the whole run when any of these holds:

| condition | why |
|---|---|
| `metadata_complete == false` — either sidecar file absent | the record cannot be reproduced |
| the manifest produced **no** source hashes | a stale manifest silently hashing nothing |
| any source recorded as `<unreadable: …>` | a missing hash is indistinguishable from a file that never existed |
| `PYTHONHASHSEED != "0"` on either backend | §5.1 — output ordering is not reproducible, so the byte gate is meaningless |

`INVALID_METADATA` outranks every other verdict, including `PASS`. The problems are
listed in the console output and stored in `metadata_problems`.

#### 6.1.5 The source manifest is fixed in code, not typed at the command line

Revision 8 had the sidecar take `--source a --source b`. That puts completeness in
the operator's hands, and a file left off is invisible: the record looks complete
and simply omits the thing that changed.

`bench/manifests.py` now holds the manifests, shared by the sidecar and this spec,
as **globs over directories** so a newly added module is covered without anyone
remembering a flag:

| backend | patterns | resolves to |
|---|---|---|
| `reference` | `woa23_app.py`, `src/**/*.py` | `woa23_app.py`, `src/__init__.py`, `src/config.py`, `src/dask_client_manager.py`, `src/woa23_utils.py` |
| `candidate` | `api/**/*.py` | `api/__init__.py`, `api/app.py`, `api/config.py`, `api/query.py` |

The globs are **recursive** (`**`). Revision 9 used `src/*.py` and `api/*.py`, which
cover one directory level only: a module moved into a subpackage would have dropped
silently out of the hash set while the record still looked complete. The layouts are
expected to stay flat, but a manifest should not depend on that holding.

**A pattern that matches nothing is an error**, which is what catches a manifest
gone stale against a moved directory — verified: with `api/` not yet built, the
candidate manifest aborts rather than reporting an empty hash set.

The reference manifest is deliberately a **superset**. `woa23_app.py` imports only
`src.dask_client_manager`; `src/config.py` (all `None`) and `src/woa23_utils.py` (a
density function) are unused by the API path. Hashing them costs nothing and guards
against that changing unnoticed. Hashing a superset is safe; hashing a subset is not.

#### 6.1.3 Metadata recorded in every result file

A benchmark JSON that cannot be reproduced is an anecdote. Each of
`results/paired_s1.json` records:

- `sys.version`, `sys.executable`, and the **full harness invocation** (`sys.argv`);
- the **gate variant in force** (`--gate-variant 5.2A|5.2B`), because the number
  means different things under each and inferring it later is guesswork;
- `PYTHONHASHSEED` as set, from the sidecar rather than assumed;
- `cpu_count`, `/proc/meminfo` and `loadavg` at the start of the run;
- the resolved `store_path` with its `store_source`, and a per-group `.zmetadata`
  fingerprint (SHA-256 + `mtime_ns` + size), taken from the backend's own
  environment and filesystem rather than a harness flag;
- the **full launch command** of the backend under test, verbatim;
- `git rev-parse HEAD` plus the **full, untruncated SHA-256 of every source file**
  under test (per the §6.1.5 manifests: `woa23_app.py` + `src/**/*.py` for the
  reference, `api/**/*.py` for the candidate) *and*
  of every harness file in `bench/` — a commit hash does not cover uncommitted
  edits, this spec is being written on a dirty working tree, and a truncated digest
  is not verifiable evidence;
- **backend provenance from a sidecar, not from harness flags.** The harness only
  speaks HTTP; it cannot see the source files a backend loaded, its hash seed, or
  its resolved dependencies, and a flag the operator types is an assertion rather
  than an observation. `bench/collect_backend_meta.py` runs on the backend's own
  host, finds the process by port, and reads from `/proc/<pid>/`: the verbatim
  launch command, cwd, executable, worker PIDs, full SHA-256 of each named source
  file, the `uv.lock` digest and a digest of the full distribution list read through
  the environment's own interpreter via `importlib.metadata`, and a **whitelisted** set of
  environment variables — `PYTHONHASHSEED`, `WOA23_ZARR_STORE`,
  `DASK_SCHEDULER_ADDRESS` and a few others. Environments are whitelisted, never
  dumped, because they routinely hold credentials and these artefacts are committed.
  An unset `PYTHONHASHSEED` is recorded as `<unset — randomised>` and warned about,
  since it means that backend's output ordering is not reproducible (§5.1).
  The sidecar also records the **argv array**, not only a space-joined string — a
  joined command line cannot be reversed when an argument contains a space — and it
  **refuses to describe a process that is merely occupying the port**:
  `--expect-argv-contains` is a **required** argument — `api.app:app` for the
  candidate, `woa23_app:app` for the reference — and the sidecar aborts unless it
  matches. It also **verifies the PID actually holds the port**: when `--pid` is
  given it is checked against the listener `ss` reports, and a mismatch aborts,
  because otherwise `--pid` quietly describes one process while the benchmark talks
  to another and every hash and identity field in the record is about the wrong
  thing. It is mandatory rather than optional because an optional identity check
  is one nobody runs. It additionally stamps the mtime of
  every group's `.zmetadata`, so two runs that read a store edited in between are
  distinguishable.
  `paired_bench.py` embeds both files verbatim and sets `metadata_complete: false`
  if either is absent or invalid;
- the resolved dependency set: the `uv.lock` digest plus a digest of the full
  distribution list, read through **the environment's own interpreter** via
  `importlib.metadata`. Not `pip freeze`, and not `/proc/<pid>/exe`: a uv venv has no
  pip, and the exe link resolves to the base interpreter, which is how the first
  campaign recorded a third environment belonging to neither arm;
- `WOA23_ZARR_STORE`, and the store's `.zmetadata` mtime;
- host, kernel, core count, and `free -m` at start;
- the harness invocation and `--repeat`;
- which gate variant (5.2A or 5.2B) was in force.

### 6.2 Memory — informational, **not a gate**

Removing Dask moves memory that lived in the shared 8 GB dask worker into the API
worker, where pm2's `max_memory_restart: '4G'` applies. That is worth knowing, but
it **cannot be measured cleanly enough to gate on**, so it does not appear in §8's
acceptance criteria. Two reasons, both fatal to a before/after claim:

1. `VmHWM` is monotonic — it never falls. The shared `dask-worker` has been up for
   54 days serving `tide_app` and `mhw_app`, so its high-water mark reflects the
   largest thing *any* of those services has ever done. Revision 3 proposed
   sampling it before and after each case and reporting the delta; that was wrong,
   and the review is right to reject it. A delta on a monotonic counter shared with
   two other services attributes nothing.
2. Even a clean number would be incomparable: the reference's true footprint is
   split across two processes and the candidate's is in one.

**What is therefore recorded, and how it may be used:**

- the **candidate's own** `VmHWM` per case, as an *absolute* number, checked
  against pm2's 4 GB ceiling. This is the question that actually matters — "does
  the candidate fit?" — and it is answerable.
- the reference's API-worker `VmHWM`, labelled explicitly as **understating** its
  real cost because data lived in the shared dask worker.
- **No claim of memory improvement or regression is made from these numbers**, in
  either direction. If the candidate's absolute figure approaches 4 GB that is a
  finding in its own right and blocks S6, but it is not a comparison.

Two cases, because they are heavy in different ways:

| case | why |
|---|---|
| `surface_global` | largest output — 64,800 rows, 5.2 MiB JSON |
| `point_profile_multiparam` | largest decompressed read — 242 chunks, 224.7 MiB |

Method, applied identically to reference and candidate. `VmHWM` is a high-water
mark that only resets when the process does, and at `-w 1` with no `--reload` there
is no way to recycle a worker in place — so **the whole server is restarted for
every case**:

1. Stop the server; confirm the port is released (`ss -lntp | grep :<port>`).
2. Start it with `PYTHONHASHSEED=0`, `-w 1`. Capture the **worker** PID, not the
   master: `pgrep -P "$(cat <pidfile>)"` returns the single child.
3. Wait for ready: poll the health-check URL from §5.3.1 until it returns 200, max
   30 s. Do not proceed on a timeout — record a failure.
4. **Cold sample:** issue the case's request exactly once, wait for the full
   response body, then read `VmHWM` from `/proc/<worker-pid>/status`.
5. **Warm sample:** issue the same request three more times, then read `VmHWM`
   again. (It can only rise; a warm value equal to cold means the first request
   already reached the peak, which is itself the finding.)
6. Restart before the next case, so no high-water mark carries over.

Each case is therefore one full start → cold read → 3 requests → warm read →
stop cycle, run for both `surface_global` and `point_profile_multiparam`, on both
the reference and the candidate: eight cycles.

**The shared `dask-worker`'s `VmHWM` is not sampled at all.** Revision 3 proposed a
before/after delta on it; that is meaningless on a monotonic counter in a process
shared with two other live services, and it is dropped. The reference's API-worker
figure is reported with the explicit caveat that it understates the true cost, and
nothing is subtracted, compared, or concluded from it.

The whole of §6.2 is informational. It does not gate §8.

## 7. Risks

| risk | assessment |
|---|---|
| Memory moves into the API worker | Real, and the reference's RSS understates its true cost because dask held data elsewhere. §6.2 measures it; the pm2 4 GB ceiling is the thing to watch. |
| Loss of parallelism on large queries | Measured and rejected: `numpy` still wins at 64,800 rows (1.35×). Margin narrows, does not invert. |
| Other services lose the Dask cluster | No. Only WOA23's client goes; cluster and `dask_client_manager.py` stay. |
| Ordering differs from today's live process | Expected, and not caused by this change — see §5.1. The gate controls for it; S2b owns the fix. |
| `chunks=None` changes dtype or fill handling | Not expected — same codecs, same `_FillValue`. C4 is the probe. |
| The reference instance disturbs production | It adds a client to the shared Dask scheduler. Brief, light, and it needs PI sign-off before it is started. |

## 8. Acceptance criteria

0. D2a is granted (and D2b answered) before anything below begins. D1 is settled.
1. **Contract.** Every case in §5.4 passes the gate in force:
   under 5.2A, byte-identical between reference and candidate — success bodies, CSV
   bodies **and error bodies** — at `PYTHONHASHSEED=0` on both sides; under 5.2B,
   semantically identical against live production, with the weaker guarantee stated
   plainly in the report.
2. Under 5.2A only: the non-gating semantic check against live production passes,
   confirming the reference is faithful to what users get.
3. **Latency.** `results/paired_s1.json` exists — the single gate artefact, kind
   `paired_latency`, produced by one interleaved `bench/paired_bench.py` run over
   both arms — carrying the §6.1.3 metadata and **meeting every §6.1.2 threshold**.
   Its `gate` field is `PASS`. The speedup is claimed from that file, never from
   the public baseline. Alongside it: `results/noise_pilot_candidate.json`,
   `results/contract_s1.json`, `results/meta_candidate.json`,
   `results/meta_reference.json`.
4. **Memory (informational).** The candidate's absolute `VmHWM` is recorded for
   both §6.2 cases, cold and warm, and is below pm2's 4 GB ceiling with margin.
   No before/after memory claim is made. This criterion fails only if the
   candidate's own absolute figure is unsafe — never on a comparison.
5. `git status` shows no modification to `woa23_app.py`, `src/`, `dev/`, `conf/`,
   `requirements.txt`, `Pipfile`, `setup.py`.
6. **Host left clean, and verified so.** Production on 8050 still running and
   untouched; the Dask cluster still running; candidate and reference stopped;
   ports 8051 and 8052 confirmed released by `ss`; no stray pidfiles. The
   verification output is pasted into the result record — an assertion that it was
   cleaned up is not the same as showing it.
7. Candidate runs from a uv-managed venv with `uv.lock` committed.
8. `.claude/` is absent from every commit.

## 9. Open questions for the reviewer

1. **Do you accept the §0 argument that S2b-first buys nothing for S1's gate?**
   This is the one place I am pushing back rather than complying. If I am wrong —
   if there is a way S2b makes the S1 comparison byte-exact that I have not seen —
   then the sequencing recommendation stands and I will take it. If I am right, the
   only real question is D2b, and S2b's position in the queue is an independent
   decision.
   A third option exists and I do not recommend it: run the reference on a
   non-production host, which means copying enough of the 33 GB store to serve the
   case list. It removes the VM24 process entirely at the cost of the disk the PI
   specifically did not want to spend.
2. **Is `chunks=None` the right lever**, or should the app open with an explicit
   chunk spec and use the synchronous scheduler? Both were measured and `numpy` won,
   but `chunks=None` forecloses chunked streaming for very large requests later.
3. **Is exact byte equality achievable**, given that both sides use the same orjson
   3.11.4 and the same float values? I believe this path contains no reduction at
   all — only selection and reshaping — so no last-bit divergence is possible. Worth
   a second opinion.
4. **Is the case list still missing a shape?** C15–C19 came from your review; I
   chose the rest, so the remaining blind spot is mine.
