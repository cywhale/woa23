# C1 / C2 re-run for the 015+016 candidate — authorisation request

**Status: REQUESTED, NOT GRANTED. Nothing here has been run.** No VM24 contact, no port
bound, no arm started. **This document is not an authorisation.**

**Why a re-run at all:** [spec 015](015-deterministic-column-order.md) modifies `api/`.
Every existing C1/C2 result was produced by a different `api/` tree, so none of them
speaks for this one.

---

## 1. What may NOT be reused, stated first

| evidence | status for this candidate |
|---|---|
| `c1f` (C1 5.2C contract validation, PASS) | **does not apply.** Different `api/query.py` |
| `c2g` (C2 three cycles + row-order gate, PASS) | **does not apply.** Same reason |
| `s2pB` (S2 rung-21 latency, gate PASS) | **does not apply, and its latency figure may not be cited.** See §6 |
| `pm2G` | **NOT A PASS**, and unrelated to this: it ran `06661fd`, before either change |

**Nothing above may be back-filled.** A new candidate needs new evidence under new
execution identities, which is what this request is for.

## 2. The candidate

| item | value |
|---|---|
| commit | `ad3f4280309aab148faa997a8681dcd2cb6120fd` |
| archive SHA-256 | `308482b9ccb93ed542a187b00d39c05931ac80b7c27609e91c7e9e24f708a648` |
| file count | `169` |
| file-list SHA-256 | `c58f34a65901293e1534700e4b74c468ddfb2e5f648125aac985cde67b5289ed` |
| verifier | `scripts/verify_clean_archive.sh ad3f428`, **16/16** |
| offline evidence | **three serial batches, 43 suites each, 129 runs, 0 non-zero, 0 failing assertions** |

Superseded: `06661fd` (169 ← 164 files) — the `pm2G` subject, which predates both changes
and remains the subject of `pm2G`'s own result.

### 2.1 The files that changed, and the ones that did not

| file | SHA-256 at `ad3f428` | changed vs `06661fd`? |
|---|---|---|
| `api/query.py` | `50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8` | **YES — spec 015** |
| `api/app.py` | `15f26444af9fbffbdef8a240833c1ff7f8bc99fdf84277317eb15f859d467ce2` | no |
| `api/config.py` | `b806641dc7478acaca375380b8e0f8575c1fa48f093362f7f917921a3adc94ca` | no |
| `deploy/production_app.sh` | `07bc37642d9c0f82ed709c91f973170b85b7df7ab98ef6058eb49eea3e71295a` | **YES — spec 016** |
| `deploy/make_staging_override.js` | `143ffa6381750789e33cdea1004c680bd1e8dc83b49267da2d77d903317ccd57` | **YES — spec 016** |
| `deploy/staging_execute.sh` | `bd505a4d23a4e434f2733f10da970d111d400cb4326105390921251348f154f3` | **YES — spec 016** |

Total diff vs `06661fd`: **4 files, +232 / −20.**

**Only `api/query.py` is in scope for C1/C2.** The three `deploy/` files are spec 016 and
affect staging/cutover, not the contract gates — but they are listed because they are in
the same subject tree and the archive digest covers them.

## 3. What changed in `api/query.py`

**One behaviour change: column order becomes deterministic.** Nothing else.

| | before | after |
|---|---|---|
| JSON field order | whatever the hash seed produced, stable per process | **canonical, identical across processes** |
| CSV header order | likewise | **canonical, identical to the JSON field order** |
| values | — | **unchanged** |
| row order | 008 contract | **unchanged** |
| column names | — | **unchanged**, including `{param}_mn` → `{param}` |
| HTTP statuses, error bodies | — | **unchanged** |
| empty-result behaviour | — | **unchanged** (spec 006 option B still unimplemented) |

**Canonical order:** `lon, lat, depth, time_period`, then data columns **parameter-major**
— `available_pars` outer, `available_vars` inner — with `mn` renamed to the bare parameter
but keeping `mn`'s slot.

```
parameter=temperature,salinity,oxygen&append=mn,an
→ lon,lat,depth,time_period,temperature_an,temperature,salinity_an,salinity,oxygen_an,oxygen
```

**This is a deliberate, PI-decided change, not a bug fix that restores prior behaviour.**
The old order was never a contract; it was hash-seed output. **A C1 comparison against a
reference built from the old tree will show column order differing, and that is the
decided change** — the same shape of finding that 008's row-order change produced for
C1 5.2C, and it must be recorded as such rather than canonicalised away.

## 4. Execution identities — all first-use

| | C1 | C2 |
|---|---|---|
| label | **`c1h`** | **`c2h`** |
| candidate arm | **`18301`** | **`18311`** |
| reference arm | **`18302`** | **`18312`** |
| isolated dask scheduler | **`18949`** | **`18959`** |
| workdir | **`~/woa23-c1h-work/`** | **`~/woa23-c2h-work/`** |

**All six ports are first-use**: absent from `scripts/ports_used.tsv` before this request
and named in no other file. They are recorded in the ledger as *allocated, not yet bound*,
per the ledger's own convention for ports named in a request.

**`pm2G`'s `18265` is now recorded in the ledger as SPENT**, and — unlike every earlier
entry — **still bound**, because `pm2G`'s process was deliberately left running under the
retention discipline.

**No identity from `c1f`, `c2g`, `s2pB`, `c2f`, `d1b` or any `pm2*` run is reused.**

### 4.1 The subject is `ad3f428`, and this document is NOT part of it

**This document and the ledger edit that accompanies it live in a LATER commit
(`3e2c518`). That commit is a protocol reference and must never be back-filled as the
execution subject.** The subject is `ad3f428`, whose digests are in §2, and the copy of
this file found in an export of that subject will be **absent** — it did not exist yet.
That is expected; the digests identify the tree, not the prose about it.

**This is load-bearing, not bookkeeping.** `run_controlled.sh` refuses a port that appears
in `scripts/ports_used.tsv`, and it reads that ledger **from the export of the subject it
runs**. The six ports in §4 are recorded in the ledger *in `3e2c518`*, which the subject
`ad3f428` does not contain — so the runner will not refuse them. **Re-cutting the subject
to include `3e2c518` would make the runner reject the very ports this request allocates**,
which is the trap `pm2F`'s ledger note recorded and the reason ports are recorded around
a run rather than inside its subject.

## 5. The gates

**C1 (`c1h`)** — variant 5.2C, contract validation:

1. canonical values and columns compared between arms;
2. the candidate's own row-order conformance (008);
3. the raw-order difference recorded as the decided change, with byte-level
   reconstruction proving nothing else moved;
4. **and now: the column-order difference recorded the same way** — as a decided change
   under spec 015, with the same reconstruction discipline.

**C2 (`c2h`)** — three cycles, plus:

- the row-order gate as in `c2g`;
- **the column-order gate: the candidate must order columns identically under three
  different seeds**, which is the same shape as the existing three-seed row-order
  requirement and is the property `pm2G` found violated.

## 6. Latency is explicitly NOT claimed

**`s2pB`'s latency result does not apply to this tree and may not be cited for it.**

The ordered `select` is one projection over an already-materialised frame, so the expected
cost is small — **but "expected" is not "measured", and this request does not measure it.**
If the cost of the canonical projection needs a number, that is a **separate measurement
with its own authorisation, its own first-use identity and its own ports.** No figure from
`s2pB` may stand in for it.

## 7. Scope and forbidden

**In scope:** C1 `c1h` and C2 `c2h` against the candidate `ad3f428`, on the ports in §4.

**Forbidden:** production API, production store, `conf/`, production's app name and ports
8050/8786/8787; reusing any `c1f`/`c2g`/`s2pB`/`pm2*` identity, port or workdir;
**touching `pm2G`'s retained state in any way** — no `pm2 stop`, no `pm2 delete`, no
release of `18265`, no cleanup of its tree, workdir, store, logs or PM2 home;
`polars-lts-cpu`; SIGKILL; self-rerun after any failing step.

**`pm2G` cleanup is a separate matter needing its own authorisation, and this run must not
perform it as a side effect of being in the same neighbourhood.**

## 8. What a PASS will and will not mean

**Will:** the 015 candidate satisfies the contract gates — values and row order unchanged
against the reference, the column-order change recorded as decided, and column order
identical across three seeds.

**Will not:** **not a staging PASS, not a production cutover PASS, not a latency result.**
**B1–B5 remain open** and are untouched by contract gates. **B7 remains open** — spec 016
makes an isolated venv enforceable but no deployment has used one. **`pm2G` remains NOT A
PASS** and is not re-classified.

## 9. Prerequisite

**The PI has asked to review the new specs, the `api/` diff, the test results and the
execution boundaries before anything runs.** This request is submitted for that review.
**No VM24 contact until it is granted**, and **no `pm2H` is proposed** — that needs the
decisions in this document settled first, and its own separate authorisation.
