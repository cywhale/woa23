# Bash 5.x read-only verification (`bash5A`) — result

**Outcome: PASS**, in the bounded sense §7 defines. Executed 2026-08-20 on VM24 (`odb24`).

**Not a deployment. Not PM2 validation. Not a test of the launcher's successful start.**
No gunicorn, PM2 or Dask was started, no port was bound, no HTTP request was sent, and no
production file was read or written. **B1–B5 remain open cutover blockers**, and B6 is
untouched.

---

## 1. Subject, and the runner that is not part of it

| item | value | verified on VM24 |
|---|---|---|
| commit | `343309b70dec3b37378bb824bd4b283bd3d5ed86` | — |
| archive SHA-256 | `c856421543e48ddf7f7019faa05b385761e5172f7b468e7076bf9339da5b7cea` | **exact after transfer** |
| file count | `143` | **143** |
| file-list SHA-256 | `5a61af7471fb3f4a0d5a79af99162ae97a5dde2695c66c80e8627be33c948b7c` | **exact** |
| script under test | `dev2026/deploy/production_app.sh` | found at `~/woa23-bash5a/dev2026/deploy/production_app.sh` |
| its SHA-256 | `3c6737a817cc12b7a37a2e9954698d0ce285e98c64e6b7a0b5a6e89a64d38ef3` | **exact** |

The file-list digest matching means **all 143 files** are byte-identical to the subject,
not merely the one under test.

**The runner is not part of the subject and is not claimed to be.**
`scripts/verify_bash5_refusals.sh`, SHA-256
`02e97553f6a26a34c2598cc2b5ba15b1437b945631dae4c66c1e714c57e7f341`, was written after
`343309b` and shipped alongside. Confirmed absent from the subject tree on the host.

## 2. The grant

`WOA23_BASH5_VERIFY_GRANTED=yes` — **new, and used for the first time here.** The runner
refuses without it, and **also refuses if any of the six existing grants is set**, so a
Bash 5 verification cannot run in a shell still carrying a benchmark's authorisation. It
further refuses any target under `conf/` and any target not naming `api.app:app`. All
three refusals were exercised offline before execution.

## 3. Bash version

```
GNU bash, version 5.2.21(1)-release (x86_64-pc-linux-gnu)
BASH_VERSION = 5.2.21(1)-release        /usr/bin/bash
```

`bash -n` on the subject's `production_app.sh` under 5.2.21: **no syntax error**.

## 4. The fourteen cases

Every case recorded name, bash version, exit code, stdout, stderr, expected refusal,
whether the stub was invoked, the listener count on the probe port, and the host process
count before and after.

### Twelve refusals — all exit 2, stub never invoked, listeners unchanged

| case | expected refusal | exit | stub | listeners |
|---|---|---|---|---|
| `port-missing` | `WOA23_PORT is missing or empty` | 2 | no | 0 |
| `port-empty` | `WOA23_PORT is missing or empty` | 2 | no | 0 |
| `port-not-a-number` | `is not a number` | 2 | no | 0 |
| `port-out-of-range` | `out of range` | 2 | no | 0 |
| `store-missing` | `WOA23_ZARR_STORE is missing or empty` | 2 | no | 0 |
| `store-empty` | `WOA23_ZARR_STORE is missing or empty` | 2 | no | 0 |
| `store-not-a-dir` | `is not a directory` | 2 | no | 0 |
| `store-absent` | `is not a directory` | 2 | no | 0 |
| `store-no-anchor` | `no readable anchor group metadata` | 2 | no | 0 |
| `tls-key-unreadable` | `TLS key not readable` | 2 | no | 0 |
| `tls-cert-unreadable` | `TLS certificate not readable` | 2 | no | 0 |
| `interpreter-absent` | `no interpreter at` | 2 | no | 0 |

`proc_count` was **identical before and after every one** (482 → 482).

**The refusals happen in order, and the order is evidence.** `port-not-a-number` reports
only `WOA23_PORT is not a number: 'eighty-fifty'` — it never reaches the store, TLS or
interpreter checks. Every refusal precedes any gunicorn, PM2, Dask or bind.

### Two argv cases — the reason this run exists

`argv-tls-off` and `argv-tls-on`, both exit 0 with the stub invoked, no listener.

```
INVOKED: -m gunicorn api.app:app -w 2 -k uvicorn.workers.UvicornWorker \
         -b 127.0.0.1:18251 --timeout 120 --graceful-timeout 10
INVOKED: -m gunicorn api.app:app -w 2 -k uvicorn.workers.UvicornWorker \
         -b 127.0.0.1:18251 --keyfile …/privkey.pem --certfile …/fullchain.pem \
         --timeout 120 --graceful-timeout 10
```

- names `python -m gunicorn api.app:app`;
- **no `--reload`** (B5);
- **no `woa23_app`** (B2);
- `--keyfile`/`--certfile` present with TLS on;
- **absent with TLS off — the empty array expanded to nothing.**

That last line is the whole point. `${TLS_ARGS[@]+"${TLS_ARGS[@]}"}` on an empty array
under `set -u` is the construct that differs between bash 3.2 and 5.x, it is unreachable
from any refusal, and it now has execution evidence on **both**.

**Exactly 2 stub invocations were recorded**, matching the 2 argv cases and no others —
so the twelve refusals demonstrably ran nothing.

## 5. Production — before and after, read-only

**0 requests** were sent to 8050, 8786 or 8787. Everything below is from `/proc` and `ss`.

| | before | after |
|---|---|---|
| boot id | `0b513a75-213b-40bf-8219-1c7cbc51a085` | **identical** |
| God Daemon | 3459, started 2026-08-14 13:25:09 | **identical** |
| gunicorn 4296 / 5040 / 5041 starttime | 14214 / 15825 / 15829 | **identical** |
| dask 4357 / 4358 starttime | 14323 / 14330 | **identical** |
| listeners 8050 / 8786 / 8787 | present, same PIDs | **identical** |
| exe of every production pid | `.pyenv/versions/3.11.4/bin/python3.11` | **identical** |

`conf/`, production's store and production's PM2 state were never written to. `pm2A` and
`pm2B` evidence is intact — 19,664 files across their four directories.

## 6. Processes and ports — and two self-matches I had to rule out

- **listeners on 18251: 0**, before, after and following every individual case.
- **no process referencing `woa23-bash5a` survives.**
- **no `python-stub` process exists.**
- **no gunicorn started during this run** — the youngest on the host is 81,398s old
  (~22.6 h) and belongs to another project; the next are ~2.3 days old.

Two intermediate readings looked alarming and were **wrong, both for the same reason**: my
own `ps | grep` command line contained the very string being searched for, so it matched
itself. "3 processes referencing woa23-bash5a" and "2 python-stub processes" were both
artefacts of that. Re-checked with the pattern split so it cannot self-match, both are
zero. The runner's own `gunicorn/pm2/dask processes matching now: 19` line is likewise
**informational only** — those 19 are production's, another project's, and the two
retained staging daemons, all pre-existing.

## 7. What this PASS means, and what it does not

**Means:** `production_app.sh` parses under VM24's bash 5.2.21, and its refusal logic and
argv construction behave there exactly as on bash 3.2 — including the guarded empty-array
expansion that motivated the exercise. The bash-5 gap recorded in spec 011 §2.5 is closed.

**Does not mean:** anything about whether the launcher *works* in production. It never
started the app, never touched the real store, never involved PM2 or real TLS termination.
**Not a production deployment validation.** B1–B5 remain open; **B6 — whether the
deployment's interpreter carries the candidate's dependencies — is untouched and still
unverified.**

## 8. Evidence

| | |
|---|---|
| VM24 workdir | `/home/odbadmin/woa23-bash5a-work` — 49 files, 14 case directories, each with `record.txt`, `stdout.txt`, `stderr.txt`, plus `stub-invocations.txt` and `parse.err` |
| VM24 staging | `/home/odbadmin/woa23-bash5a` — the 143-file verified subject tree |
| local | `scratchpad/bash5A/` — preflight, archive verification, run log, after-snapshot, leak checks, sample records |

**Cosmetic note, recorded rather than tidied:** the runner creates an unused
`fixture/unreadable` directory at mode 000, so `find` reports "Permission denied" on it and
the 49-file count excludes its (empty) contents. No case uses it. It is left exactly as the
run produced it.

**Ledger:** `18251` is **not** entered in `scripts/ports_used.tsv`. The ledger records
ports a run has **bound**; this run bound nothing, and recording it would wrongly retire a
first-use port.
