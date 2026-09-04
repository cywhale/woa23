# Bash 5.x read-only verification (`bash5A`) — request and grant definition

**Purpose: to close one named gap and nothing else.** `deploy/production_app.sh` has never
executed under bash 5.x. Everything asserted about it so far was asserted on bash 3.2.

**This is not a deployment, not PM2 validation, and not a test of the launcher's
successful start.** No gunicorn, no PM2, no Dask, no port bound, no HTTP request, no
production file read or written.

---

## 1. The dedicated grant — why a new one exists

```
WOA23_BASH5_VERIFY_GRANTED=yes
```

Six grants already exist — `WOA23_S2PERF_GRANTED`, `WOA23_S2_C1_GRANTED`,
`WOA23_S2_C2_GRANTED`, `WOA23_D1_GRANTED`, `WOA23_D2A_GRANTED`, `WOA23_D2B_GRANTED` — and
**none may be reused.** A grant is what makes an authorisation specific to one kind of
run; accepting another run's grant would let an authorisation for a benchmark start a
launcher verification.

The runner enforces this in **both** directions:

1. it refuses unless its own grant is exactly `yes`;
2. it **refuses if any of the other six is set at all**, so a Bash 5 verification cannot
   be run in a shell that is still carrying another run's authorisation.

It also refuses any target under `conf/`, and any target that does not name
`api.app:app` — so it cannot be pointed at production's launcher.

## 2. Execution subject

| item | value |
|---|---|
| commit | `343309b70dec3b37378bb824bd4b283bd3d5ed86` |
| archive SHA-256 | `c856421543e48ddf7f7019faa05b385761e5172f7b468e7076bf9339da5b7cea` |
| file count | `143` |
| file-list SHA-256 | `5a61af7471fb3f4a0d5a79af99162ae97a5dde2695c66c80e8627be33c948b7c` |
| script under test | `dev2026/deploy/production_app.sh` |
| its SHA-256 in the subject | `3c6737a817cc12b7a37a2e9954698d0ce285e98c64e6b7a0b5a6e89a64d38ef3` |

**The runner is NOT part of the subject.** `scripts/verify_bash5_refusals.sh` is written
after `343309b` and is therefore a **verification harness shipped alongside**, with its
own digest recorded in the result. The subject stays exactly what was authorised; the tool
that drives it is identified separately rather than silently folded in.

## 3. Identity — new, and not reused

| | |
|---|---|
| label | **`bash5A`** |
| staging | **`~/woa23-bash5a/`** |
| workdir | **`~/woa23-bash5a-work/`** |
| PM2_HOME | **none — PM2 is not involved** |
| port | **none bound.** `18251` is passed as a *value* only, and every case asserts it never became a listener |

`18251` is **not** entered in `scripts/ports_used.tsv`: the ledger records ports a run has
**bound**, and this run binds nothing. Recording it would wrongly retire a first-use port.

## 4. The cases

Each records case name, bash version, exit code, stdout, stderr, the expected refusal,
whether the stub was invoked, and whether any process or listener appeared.

**Twelve refusal cases**, each of which must exit **2** with the stub **not** invoked:

| case | expected refusal |
|---|---|
| `port-missing`, `port-empty` | `WOA23_PORT is missing or empty` |
| `port-not-a-number` | `is not a number` |
| `port-out-of-range` | `out of range` |
| `store-missing`, `store-empty` | `WOA23_ZARR_STORE is missing or empty` |
| `store-not-a-dir`, `store-absent` | `is not a directory` |
| `store-no-anchor` | `no readable anchor group metadata` |
| `tls-key-unreadable` | `TLS key not readable` |
| `tls-cert-unreadable` | `TLS certificate not readable` |
| `interpreter-absent` | `no interpreter at` |

**Two argv cases**, `argv-tls-off` and `argv-tls-on`, which do reach the `exec`.

**These are the reason the exercise exists.** `${TLS_ARGS[@]+"${TLS_ARGS[@]}"}` on an
**empty** array is the construct that differs between bash 3.2 and 5.x, and it is
unreachable from any refusal. It is neutralised twice: the interpreter is a **stub** that
appends its argv to a file and exits — it cannot bind, serve or fork — and the store is a
**synthetic temporary directory**, never production's. No gunicorn binary is involved.

**That is argv verification, not a launch.** The distinction is what keeps this inside the
authorisation, and the evidence for it is the stub's own invocation log plus a listener
count taken after every single case.

## 5. Success conditions

- `bash -n` passes under bash 5.x;
- all twelve refusals fail closed, at the expected stage, with the stub never invoked;
- the two argv cases show `python -m gunicorn api.app:app`, **no `--reload`**, no
  `woa23_app`, `--keyfile` present with TLS on and **absent** with TLS off — the empty
  array expanding to nothing, which is the whole point;
- the probe port has the same listener count before and after every case;
- production's boot id, master and worker PIDs and starttimes, and the listeners on
  8050 / 8786 / 8787 are identical before and after;
- **0 requests** to production;
- no process leak;
- evidence retained.

## 6. Forbidden

The launcher's real successful start; gunicorn; PM2; Dask; binding any port; touching the
production API, store, PM2 state or `conf/`; SIGKILL; `pm2 * all`; global `save`/`resurrect`;
clearing `pm2A`/`pm2B` or any other failure evidence; self-rerun.

**Out of scope by the PI's explicit instruction:** the 16 local `arm.py` fixture strays on
the development machine. They are not touched by this authorisation.

## 7. What a PASS will and will not mean

**Will:** `production_app.sh` parses under VM24's bash 5.x, and its refusal logic and argv
construction behave there exactly as they do on bash 3.2 — including the guarded
empty-array expansion.

**Will not:** it says nothing about whether the launcher *works* in production. It does not
start the app, does not touch the real store, does not involve PM2 or TLS termination, and
is not a production deployment validation. **B1–B5 remain open cutover blockers**, and B6
— whether the deployment's interpreter carries the candidate's dependencies — is untouched
by this run.
