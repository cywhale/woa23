# D2a — request to run the candidate API on VM24

**Status:** revision 3, requested, not granted. Nothing below has been run.
**Asking for:** one process on `127.0.0.1:8051`, **variant 5.2B, rung 21 only**.
**Not asking for:** rung 60, rung 150, or D2b. Each is a separate signature.

**One authorisation covers one invocation of one rung.** Revision 2 asked for "up to
rung 60" and then costed a single rung-60 run — but the escalation rule says run 21
first, so the real worst case was the sum of both, and it was understated. The script
now enforces one rung per invocation, and rung 60 skips the pilot and the contract
gate entirely because neither improves with more latency samples.

Earlier revisions were rejected for citing a contract harness that did not exist,
claiming a `trap` the start command did not have, a health check that was a single
`curl`, a stop that succeeded whether or not the port was released, `rsync --delete`
against an unverified target, and counting only the latency gate's traffic. All fixed
and verified below.

---

## 1. What runs

| | |
|---|---|
| **host** | VM24, `192.168.2.24` |
| **port** | `127.0.0.1:8051`, loopback only, plain HTTP |
| **user** | `odbadmin` |
| **directory** | `~/woa23-dev2026/` — new. `~/python/woa23` is read-only to this work. |
| **workers** | `-w 1`, no `--reload`, no TLS |
| **lifetime** | one campaign; stopped at the end, not left running |

## 2. Traffic — per invocation, and cumulative

Under variant 5.2B the "before" arm is **live production on `127.0.0.1:8050`**.
Sizes come from the measured payloads in `results/http_prod_baseline_heavy.json`:
one pass over the eight gate cases moves **13.55 MiB**, dominated by
`surface_global` (5.1 MiB) and `regional_bbox_025` (4.7 MiB).

**What this request covers — the rung-21 invocation:**

| phase | → production 8050 | → candidate 8051 | bytes from production |
|---|---|---|---|
| provenance (reads `/proc`) | 0 | 0 | 0 |
| sample-size pilot (§3) | 0 | 208 | 0 |
| contract gate, 64 cases per arm | 64 | 64 | ~3 MiB |
| latency gate, rung 21 | 176 | 176 | 298 MiB |
| **total** | **240** | **448** | **~301 MiB** |

**If rung 60 is granted later**, it is a separate invocation that runs *only* the
cases rung 21 left `INCONCLUSIVE`, and skips the pilot and contract gate:

| | → production | → candidate | bytes from production |
|---|---|---|---|
| rung 60, worst case (all 8 cases undecided) | 488 | 488 | 827 MiB |
| rung 60, expected (3 undecided — five should resolve at 21) | 183 | 183 | 310 MiB |
| **cumulative worst case, 21 + 60** | **728** | **936** | **~1,128 MiB** |

The review computed 792 / 1,208 on the assumption that rung 60 re-runs the pilot and
contract gate. It did, in revision 2. It no longer does, which is where the
difference comes from.

Rung 150 would add a further 1,208 production requests and ~2.0 GiB. It is refused by
the script, not merely discouraged here.

Everything is strictly sequential. `paired_bench` pauses once per AB/BA pair, so the
two arms are never in flight simultaneously and production sees at most one request
at a time from us.

## 3. One reduction I am proposing

**Run the sample-size pilot against the candidate, not production.** Since revision 8
of the spec the pilot only does sample-size planning — the gate's own confidence
interval is computed from both arms' actual samples and carries the noise — so
pointing it at 8051 removes **208 requests and 352 MiB** from production. The tables
above assume this.

**It is not free of consequence, and revision 3 overstated it as "no loss of
rigour".** A candidate-only pilot measures the candidate's noise floor, which says
nothing about production's. That does not affect the **validity** of the gate; it
narrows the **planning evidence** to one of the two arms. Practically: a case sized
from a quiet candidate may still come back `INCONCLUSIVE` against a noisier
production, and the ladder is what absorbs that.

If you would rather it sampled production, say so and add those requests back.

## 4. Commands, in order

### 4.1 Deploy — with the target verified before anything is deleted

`rsync --delete` removes whatever is in the destination and not in the source. The
directory is supposed to be new, but "supposed to be" is not a check:

The check must **fail**, not print a warning someone can scroll past:

```bash
ssh vm24 'test -e ~/woa23-dev2026 && { echo "EXISTS — inspect before proceeding" >&2; exit 1; }'
```

Only once that exits 0:

```bash
ssh vm24 'mkdir -p ~/woa23-dev2026/dev2026'
rsync -a --exclude '.venv' --exclude '__pycache__' --exclude 'results' \
  ~/proj/woa23/dev2026/ vm24:~/woa23-dev2026/dev2026/
ssh vm24 'cd ~/woa23-dev2026/dev2026 && uv sync --frozen'
```

`--delete` is **not used**. On a directory we just created there is nothing to
delete, and on any other directory it is the wrong tool.

### 4.2 The campaign is a script, not a paste

Everything from §4.2 to §4.7 lives in **`dev2026/scripts/run_candidate.sh`**, which
is in the repo and reviewable as a diff. A start sequence that only ever exists as a
block quoted in a document cannot be syntax-checked, diffed, or re-run identically —
and revision 1 of this request proved the point by describing a `trap` that its own
commands did not contain.

```bash
ssh vm24 'cd ~/woa23-dev2026/dev2026 && WOA23_D2A_GRANTED=yes ./scripts/run_candidate.sh 21'
```

Four refusal paths, each executed to verify it:

```
$ ./scripts/run_candidate.sh 21
D2a authorisation not stated. This starts a process on a production host.   exit 3

$ ./scripts/run_candidate.sh 150
rung 150 needs a separate signature (D2a-request.md section 2)              exit 2

$ WOA23_D2A_GRANTED=yes ./scripts/run_candidate.sh 99
usage: ./scripts/run_candidate.sh <21|60>                                   exit 2

$ WOA23_D2A_GRANTED=yes ./scripts/run_candidate.sh 21     # on the wrong machine
this runs on odb24 only; hostname is ODBAI-M1                               exit 4
```

The host check matters: without it, `WOA23_D2A_GRANTED=yes` would let the script
start a process anywhere it had been copied. It also requires the store path to
exist, so a renamed or moved store stops it too.

What it does, in order — preflight (abort if 8051 is held by anything), install a
trap **in the shell that starts the process**, start gunicorn with
`PYTHONHASHSEED=0` and the explicit store, poll for readiness requiring **200 and a
non-empty body** to a 30 s ceiling, then run the phases in §4.3–§4.6, then stop.

### 4.3 Provenance — both arms, before anything samples

```bash
uv run python -m bench.collect_backend_meta --port 8051 --manifest candidate \
  --expect-argv-contains api.app:app --lockfile uv.lock \
  --out results/meta_candidate.json

uv run python -m bench.collect_backend_meta --port 8050 --manifest reference \
  --expect-argv-contains woa23_app:app \
  --out results/meta_reference.json
```

The second reads production's process — `/proc` only, nothing is written or
signalled. If it cannot confirm the master on 8050 it aborts, and the gates then
refuse to sample.

### 4.4 Sample-size planning

```bash
uv run python -m bench.noise_pilot --base-url http://127.0.0.1:8051 \
  --warm 25 --out results/noise_pilot_candidate.json
```

### 4.5 Contract gate — variant 5.2B, 64 cases per arm

```bash
uv run python -m bench.contract_diff \
  --candidate http://127.0.0.1:8051 \
  --reference https://127.0.0.1:8050 --variant 5.2B --insecure \
  --candidate-meta results/meta_candidate.json \
  --out results/contract_s1.json
```

`--insecure` is deliberate and narrow: production's gunicorn presents the
`eco.odb.ntu.edu.tw` certificate, so hostname verification fails on `127.0.0.1`. It
is a loopback connection to a process we can see in `ps`.

### 4.6 Latency gate — rung 21 first

```bash
uv run python -m bench.paired_bench \
  --candidate http://127.0.0.1:8051 \
  --reference https://127.0.0.1:8050 --insecure \
  --gate-variant 5.2B --warm 21 --include-heavy --margin 0.05 \
  --candidate-meta results/meta_candidate.json \
  --reference-meta results/meta_reference.json \
  --out results/paired_s1_rung21.json
```

Escalate to `--warm 60` **only** for cases the tool reports `INCONCLUSIVE`, writing
`results/paired_s1_rung60.json`. **Stop there.** A case still undecided at 60 is
reported as inconclusive and comes back to you; the ladder is not extended without a
new signature.

### 4.7 Stop, fail closed — and prove the PID is still ours before killing it

A pidfile is a claim, not proof. PIDs are recycled, so revision 2's `kill $(cat
pidfile)` could have signalled an unrelated process if the candidate had died. Before
killing anything the trap now requires **both**:

- the process's **start time** (field 22 of `/proc/<pid>/stat`) still matches what was
  recorded at launch — this is what distinguishes a recycled PID from the original;
- the PID still **holds port 8051**.

If either fails it refuses and leaves the pidfile in place:

```
REFUSING TO KILL: PID 12345 has a different start time than the process we
started — the PID was recycled. Pidfile left in place for inspection.
```

Otherwise it kills, polls for up to 20 s, and **removes the pidfile only after the
port is confirmed released**; if the socket is still held it says so and leaves
everything for inspection. The trap cleans up **this** run and never kills anything
else — a later preflight that finds 8051 bound aborts and leaves it alone.

### 4.8 Canonical artefacts

`results/paired_s1.json` is the latency gate result, written by whichever rung ran
last; the per-rung files are kept alongside it as the record.

| file | what |
|---|---|
| `results/paired_s1.json` | **the** latency gate result |
| `results/paired_s1_rung21.json`, `..._rung60.json` | per-rung record |
| `results/contract_s1.json` | contract gate (rung 21 only) |
| `results/noise_pilot_candidate.json` | sample-size planning |
| `results/meta_candidate.json`, `results/meta_reference.json` | provenance |

The spec's §6.1 table previously named `noise_pilot_reference.json`; the pilot now
targets the candidate (§3) and the name follows the target.

## 5. What it touches

- **Reads:** `~/python/woa23/data`, and `/proc` for the production master.
- **Writes:** only under `~/woa23-dev2026/`.
- **Does not touch:** production on 8050 beyond HTTP requests (not restarted, not
  reconfigured, not signalled); the Dask scheduler and worker (the candidate has no
  Dask client at all); `~/python/woa23`; pm2; nginx.
- **Network:** loopback only.
- **Store:** must stay frozen for the campaign — no ingest, no re-consolidation, no
  `.zmetadata` refresh. The harness detects a breach and voids the run; it cannot
  prevent one.

## 6. Already done, without this authorisation

- 135 + 29 offline assertions across the statistics and provenance suites
- `port_diff.py` — every executable difference between original and port; five, all
  authorised
- `smoke_local.py` — four routes present, and the Swagger route **bodies** are
  byte-identical to the original's: `openapi.json` 8,597 bytes, Swagger UI 868 bytes.

  That claim needs narrowing, which revision 2 did not do. The local check calls the
  route handlers **in-process**; the contract gate calls them **over HTTP against the
  deployed candidate**, through gunicorn, uvicorn and the real request path. Those
  are different checks and the second is stronger, so C20a/C20b stay in the case list
  and the 64-per-arm figure stands. What the local result establishes is that no
  divergence exists in the generation itself — it does not remove a case from the
  gate.

  **After the first campaign:** the HTTP run of C20a/C20b produced only equal byte
  *lengths* before failing on a comparator defect, so byte equality over HTTP is
  still unestablished. The local in-process result stands on its own terms and is
  labelled as such. Re-testing the two routes over HTTP would cost 2 production
  requests, which the current authorisation does not include.
- `contract_diff.py` + `contract_cases.py` — the harness revision 1 wrongly implied
  already existed: 33 data and documentation cases, 31 CSV replays, 64 requests per
  arm, with the CSV-vs-JSON status divergence encoded per case.
- `scripts/run_candidate.sh` — syntax-checked, and its three refusal paths
  (no authorisation, rung 150, unknown rung) verified by running them.
