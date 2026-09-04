# PM2 alternate-port staging of the PRODUCTION launcher (`pm2D`) — authorisation request

**Status: SUPERSEDED — AUTHORISED, AND STOPPED BY A PRE-START AUDIT. NEVER EXECUTED.**

An authorisation was issued for this request on 2026-08-24 and **execution never began.**
Walking the authorised sequence offline, before any VM24 contact, showed it could not
complete: **step 3 extracts the archive into `~/woa23-pm2d/`, and step 7 then runs the
entry with `--root ~/woa23-pm2d`, whose freshness guard refuses a root that exists.** The
run would have stopped at its own guard having created a staging tree and done nothing
else. Reproduced end to end against subject `4f9fdfb`: `REFUSING: …/woa23-pm2d already
exists`, exit 2.

The guard was right in principle and applied to the wrong path at the wrong moment. The
offline suite had asserted "an existing staging root is refused" as correct without ever
exercising it in the order a real run uses.

**Nothing on VM24 was touched.** No SSH session, no staging tree, no venv, no PM2 daemon,
no store, and **`18262` was never bound and is NOT spent** — it does not appear in
`scripts/ports_used.tsv` and must not be added. **This is not a VM24 execution** and must
never be counted as one; there is no `pm2D` result document because there was no run.

**Superseded by `PM2-staging-request-pm2E.md`**, which splits the freshness rule into two
phases, verifies subject provenance over the whole staged tree, and carries a wholly new
execution identity.

**Retained as the record of a request that was stopped before it ran.**

---

*(Original request text follows, unchanged.)*

**Status when written: REQUESTED, NOT GRANTED.** No SSH session, no PM2
process, no port bound, no staging tree, no venv, no VM24 action of any kind.
**This document is not an authorisation.**

**It supersedes `PM2-staging-request-pm2C.md`**, which was authorised and then **stopped by
a pre-start audit before execution began** — the production config it named could not have
worked. That request is retained as the record of a run that never happened; **`pm2C` is
not a VM24 execution and `18261` was never bound and is not spent.**

`pm2D` validates the three files actually proposed for production, as a set, on an
alternate port:

| file | role |
|---|---|
| `deploy/production_app.sh` | the proposed replacement for `conf/start_app.sh` |
| `deploy/ecosystem.production.config.js` | the proposed replacement for `conf/ecosystem.config.js` |
| `deploy/production_stop.sh` | the identity-based stop that replaces the `pre_stop` line |

---

## 0. What changed since `pm2C`, and why this request exists at all

### 0.1 The defect that stopped `pm2C`

`ecosystem.production.config.js` had **no `cwd`**, and two requirements pulled in opposite
directions:

```
script: './dev2026/deploy/production_app.sh'   resolves only from the REPOSITORY ROOT
python -m gunicorn api.app:app                 puts cwd on sys.path, and api/ lives under
                                               dev2026/ — from the repo root 'api' is NOT
                                               importable (verified offline)
```

PM2 would have reported `online`; gunicorn would have died at import. **The B4 failure mode
— a fault that surfaces after PM2 says started — arriving through a door B4's own checks do
not cover.**

**Fixed** the way the staging config already did it, which `pm2B` proved end to end: an
explicit `cwd` from `__dirname`, and `script` relative to that cwd. The TLS certificate
paths became **absolute** as a consequence — `conf/privkey.pem` would otherwise now resolve
to `dev2026/conf/privkey.pem`.

**The install location in those TLS paths is a PLACEHOLDER and is not confirmed.** `pm2D`
runs with `WOA23_TLS=off` and never reads them; **confirming them is cutover work.**

### 0.2 Two things `pm2C` assumed and did not have

| gap | now |
|---|---|
| no dedicated grant | **`WOA23_PM2C_GRANTED=yes`**, enforced both ways (§2) |
| no override generator | `deploy/make_staging_override.js`, 65 offline assertions (§6) |

## 1. Execution subject

| item | value |
|---|---|
| commit | `4f9fdfb8baef7ca31e7a3a80ea8dbc2e6ad66e5f` |
| archive SHA-256 | `072f919ced1b1439a536bc921c29c689c9e37b148a5ff42d62dc98ff78f66bf3` |
| file count | `160` |
| file-list SHA-256 | `06d8b598ed845b8f88df8c0f72b8e68400942018daf07a8258df1238328361a2` |
| verifier | `verify_clean_archive.sh` 16/16 |
| offline evidence | **three independent serial batches, 42/42 each, 0 non-zero, 0 failing assertions** |

**These values supersede TWO earlier sets**, both void:

| void subject | why |
|---|---|
| `ee2c88a` | the execution-entry work (§2) changed the tree |
| `30a4232` | the full-environment and config-provenance work (§6.2, §6.3) changed it again |
| `eca5671` | the environment-count correction (§6.3) changed it again |
| `7e1d6db` | the fail-closed allowlist decision (§6.3.2) changed it again |
| `c44e96d` | consuming the grant at the entry (§6.3.3) changed it again |

**Only the commit named above is the execution subject.** The values are from the tree that
passed the three batches reported with this request.

The files this run exercises. **Every digest is re-derived on VM24; a mismatch is a stop.**

| file | SHA-256 |
|---|---|
| `deploy/pm2d_execute.sh` | `c3947ab0f04fb67369654c4dd6df64e4fef5232b06857e18d962487b4ca50e2f` |
| `deploy/make_staging_override.js` | `59495cd4a91700e2414de597080a52f699dd3630b941cb9fe0954c641b76e1d9` |
| `deploy/production_app.sh` | `3c6737a817cc12b7a37a2e9954698d0ce285e98c64e6b7a0b5a6e89a64d38ef3` |
| `deploy/production_stop.sh` | `e86f07f18b38ee4b470e7049d15c5363cfe9c3f8847e2602aa0096c1e6b7803f` |
| `deploy/ecosystem.production.config.js` | `a7e4cb7fcb47e83b8192b225093e5dbe20aad2c673e5fed511b2328ee22410c4` |
| `deploy/record_manifest.py` | `8e7a0aaba21ea7634033318297584bf9e756b765603a8626d934cafd7a91cd34` |
| `deploy/make_staging_store.py` | `cf121f7f41e15cd9a381d461772e2bc4a8b58281f7ba341baef14e9d3d5f69f1` |
| `api/query.py` | `8e980e5b60a004902e66e6cb86ed2352a5ec641a6ad4cd3173a5a2efc56cebce` |
| `api/app.py` | `15f26444af9fbffbdef8a240833c1ff7f8bc99fdf84277317eb15f859d467ce2` |

**`api/` is unchanged since the C1/C2 subject** — all five digests identical. No `api/`,
`conf/`, query-logic, response-behaviour or dependency-version change has occurred, so
`c1f`/`c2g`/`s2pB` are undisturbed.

**On this document's own place in the subject — stated because it is easy to get wrong.**
The subject `4f9fdfb` **contains this document with its §1 subject fields still blank**;
that is the tree the three batches ran against and the tree the digests above describe.
Filling those fields in produced a **later** commit, which is a **protocol reference and is
NOT the subject**. So:

- **the execution subject is `4f9fdfb`**, and the archive/file-list digests above are of
  that tree;
- the commit that fills in §1, and every commit after it, may **never** be back-filled as
  the execution subject;
- on VM24 the archive is verified against `4f9fdfb`'s digests, so the copy of this document
  found in the staged tree will have blank §1 fields. **That is expected and is not a
  mismatch** — the digests are what identify the tree, not the prose inside it.

## 2. The grant, and where it is enforced

```
WOA23_PM2C_GRANTED=yes
```

**Enforced by `deploy/pm2d_execute.sh`, the execution entry — the thing that actually
starts the run.** This is the correction that mattered most in review: the grant was
previously checked only by `make_staging_override.js`, and **that is not a boundary**.
Anyone running `pm2 start` directly, or reusing a config an earlier run generated, would
bypass it entirely and the grant would mean nothing.

| enforced where | refuses on |
|---|---|
| **`deploy/pm2d_execute.sh`** — *the* entry, checked **first**, before arguments or paths | grant absent, empty, or any value other than exactly `yes`; **any** of the seven other grants present |
| `deploy/make_staging_override.js` — defence in depth | the same two conditions |

**Both directions of bypass are closed.** If the entry were skipped, the generator still
refuses; if the generator were skipped by reusing an old config file, **the entry refuses**,
because it regenerates rather than reuses and stops if the target config already exists.

The seven refused alongside: `WOA23_S2PERF_GRANTED`, `WOA23_S2_C1_GRANTED`,
`WOA23_S2_C2_GRANTED`, `WOA23_D1_GRANTED`, `WOA23_D2A_GRANTED`, `WOA23_D2B_GRANTED`,
`WOA23_BASH5_VERIFY_GRANTED`. **No existing grant substitutes.**

`scripts/test_pm2d_entry.sh` exercises every refusal with a **fake `pm2` on `PATH` that
logs any invocation**, so "PM2 was never called" is evidence rather than an assertion.

### 2.1 Three tokens that must not be confused

| token | what it is |
|---|---|
| **`PM2C`** | the **validation mode** and the **grant name** (`WOA23_PM2C_GRANTED`). Not a run |
| **`pm2D`** | **this run's execution identity** — label, staging tree, workdir, `PM2_HOME`, port, app name |
| **`pm2C`** | an **earlier request**, authorised and then stopped by a pre-start audit. It **never executed**, bound nothing and left no state. Not a run either |

The grant keeps the `PM2C` name at the PI's explicit instruction: **the grant names the
validation, the label names the run.**

## 3. Execution identity — entirely new

| | value | not reused |
|---|---|---|
| label | **`pm2D`** | `pm2A`, `pm2B`, `pm2C` |
| staging | **`~/woa23-pm2d/`** | — |
| workdir | **`~/woa23-pm2d-work/`** | — |
| PM2 home | **`~/woa23-pm2d-pm2/`** | — |
| store | **`/home/odbadmin/woa23-pm2d/store`** | — |
| API port | **`18262`** | — |
| PM2 app name | **`woa23-pm2d-candidate`** | **never `woa23`** |

**`18262` is first-use in the strict sense:** absent from `scripts/ports_used.tsv` **and**
named in no file in the tree, derived by scanning both at request time. 18221/18231/18241
are spent; 18251 and 18261 and 18271 are unspent but *named* in earlier documents or tests,
so none of them is used.

**`18262` is deliberately NOT pre-recorded in the ledger.** The launcher refuses any
ledgered port, so entering one before its run makes the guard reject the run it was chosen
for. It enters the ledger only after this run binds it.

**`pm2A` and `pm2B` are untouched** — trees, stores, PM2 homes, logs and the retained
daemons **1242814** and **1248938**. Their cleanup is a **separate item** and is not part of
this run.

### 3.1 Restated for the record, because each has been a source of confusion

1. **`pm2D` is THIS run's execution identity** — label, staging tree, workdir, `PM2_HOME`,
   port and app name, all new.
2. **`PM2C` is the validation mode and the grant name** (`WOA23_PM2C_GRANTED`). It is not a
   run and never was.
3. **The old `pm2C` request NEVER EXECUTED.** It was authorised and stopped by a pre-start
   audit; no SSH session, no tree, no venv, no daemon, no bound port, no state. **It is not
   a VM24 execution and must never be counted as one**, and `18261` is not spent.
4. **The store is synthetic** — 72 files built on the host by the subject's own builder.
   **No production data is copied and no symlink to the production store is created.** The
   production store is never read.
5. **`18262` is re-verified at execution time**, immediately before use: absent from
   `scripts/ports_used.tsv` **and** unbound on the host. A ledger entry or a live listener
   at that moment is a stop.
6. **Before/after evidence is recorded for all of it** — PM2 state under both the isolated
   and production daemons, production's listeners on 8050/8786/8787, the synthetic store's
   digest, and the cleanup outcome. §7 steps 1, 5, 15 and 16.

## 4. The store — synthetic only

Built on VM24 by the archive's own `deploy/make_staging_store.py`:

| | |
|---|---|
| path | `/home/odbadmin/woa23-pm2d/store` |
| files / bytes | **72 / 25,191** |
| file-list SHA-256 | `06d8b598ed845b8f88df8c0f72b8e68400942018daf07a8258df1238328361a2` |
| groups | `1_degree/annual/TS` (period 0), `monthly/TS` (1, 2), `seasonal/TS` (13) |

**No production data is copied. No symlink to the production store. The production store is
never read.** Made read-only after the build, with a write probe required to fail and the
digest unchanged afterwards.

## 5. The venv — isolated, pinned, fully recorded

```
cd ~/woa23-pm2d/dev2026
uv sync --python /home/odbadmin/.pyenv/versions/py311/bin/python3.11
```

| | |
|---|---|
| location | `~/woa23-pm2d/dev2026/.venv` — inside this run's own tree |
| interpreter | **Python 3.11.4**, production's, named not resolved |
| shared `py311` | **not the runtime** — only the interpreter `uv sync` builds *from* |
| polars | **mainline 1.27.1**, per the B6 decision. **`polars-lts-cpu` is not installed** |

`deploy/record_manifest.py` records the **complete** manifest, not B7's eight packages.
**CORE (ten packages) must match `uv.lock` exactly — a difference is a stop.** FULL is
recorded with its digest, and **every difference from the development manifest must be
NAMED with its reason**; an unexplained one is a stop. The recorder imports nothing.

## 6. The override config — five items, and a sixth is a stop

Generated on the host by `deploy/make_staging_override.js` **from the production config
itself**, which it evaluates with node:

```
WOA23_PM2C_GRANTED=yes node deploy/make_staging_override.js \
  --name woa23-pm2d-candidate --port 18262 \
  --store /home/odbadmin/woa23-pm2d/store \
  --logdir tmp-pm2d --out deploy/ecosystem.pm2d.config.js
```

| permitted item | keys |
|---|---|
| app name | `name` |
| port | `env.WOA23_PORT` |
| store | `env.WOA23_ZARR_STORE` |
| TLS | `env.WOA23_TLS` = `off` |
| log paths | `log_file`, `out_file`, `error_file` |

**Five items, seven keys.** Anything else — `cwd`, `script`, `args`, `autorestart`,
`kill_timeout`, `max_memory_restart`, `append_env_to_name`, `WOA23_WORKERS`, the TLS
certificate paths — **must be identical, and the generator refuses if any of them differs.**
A resurrected `pre_stop` is refused outright. **A sixth difference is a stop, and the run
does not proceed.**

**`cwd` and `script` are carried through unchanged, and that is the point of the run:**
staging exercises production's own cwd/script relationship, which is what `pm2C` would have
proved cannot work.

**TLS is off — a stated GAP, not a claim.** Staging has no certificates and fabricates
none. It does exercise `production_app.sh`'s explicit `WOA23_TLS=off` path, which warns.
**TLS validation belongs to the cutover.**

### 6.1 The generated config carries an `env` block — deliberately, and it does not
### reintroduce the `pm2A` failure

**It does carry one**, because it is generated from the production config and production
values are fixed properties of the deployment (spec 011 §2.3). PM2 therefore applies that
block **over** whatever environment `pm2 start` is given — which is exactly the mechanism
that broke `pm2A`.

**What made `pm2A` a failure was not the mechanism. It was PLACEHOLDERS in the block**: an
empty store and a stale port silently replacing correct command-line values.

Three things keep that from recurring, and none of them is "be careful":

1. **the values are generated, never typed** — port, store and TLS come from the entry's
   arguments, which are themselves guarded (production port refused, production store
   refused, `..` refused);
2. **the generator refuses an empty or missing value** in the source config, so a
   placeholder cannot survive into the output;
3. **the entry verifies from the RUNNING PROCESS.** After `pm2 start` it reads
   `/proc/<pid>/environ` and compares `WOA23_PORT`, `WOA23_ZARR_STORE` and `WOA23_TLS`
   against the intended values, and **refuses to report an unverified environment** if
   `/proc` cannot be read. It reads argv from `/proc/<pid>/cmdline` too.

**That third point is the whole lesson of `pm2A`: every value in the starting shell was
right and every value in the process was wrong.** Only reading the process would have
caught it, so that is what the entry does — and the run's report quotes those readings, not
the config.

## 6.2 Config provenance — by digest, not by history

**"This config passed through the generator" is not provenance.** It says nothing about
whether the bytes PM2 reads are the bytes the generator produced. A file can be edited,
replaced, or left over from an earlier attempt between generation and start.

So the entry pins it:

| moment | what is recorded or checked |
|---|---|
| before generating | the **generator's own** SHA-256, and the **source production config's** SHA-256 |
| the instant generation returns | the **generated config's** SHA-256 (`CONFIG_SHA`) |
| immediately before `pm2 start` | the config is re-digested and **must equal `CONFIG_SHA`** |
| if it differs | **stop** — "PM2 would read bytes this run did not produce" |

Three further guards close the surrounding paths: the entry **refuses if the target config
already exists** (so a stale file is never adopted), it **regenerates rather than reuses**,
and the generator refuses without the grant. All four digests appear in the run's evidence,
so the config PM2 read is identifiable after the fact rather than asserted.

**Subject provenance** is separate and also checked: the staged tree's archive and
file-list digests are re-derived on VM24 against §1, and each named file's digest is
compared individually. A config generated from a tree that is not the subject cannot arise,
because the generator reads the subject tree's own production config — whose digest is
recorded above and re-derived on the host.

## 6.3 Every environment variable, verified from the process

**Ten checks: six exact values, four required ABSENT.** Eight of the ten are the variables
`production_app.sh` actually reads; the other two it does not read and must not carry.

**A correction, recorded rather than quietly fixed.** An earlier revision of this request
and of the entry itself said "ten … four ABSENT". Both numbers were wrong — they were
copied from this table's ROW count (nine variables plus a catch-all row) and reported as a
VARIABLE count. **The counts are now derived from the entry's own source by
`scripts/test_pm2d_entry.sh` and asserted**, including that the parts sum to the whole, so
the same drift fails a test rather than reaching a report.

| # | variable | read by `production_app.sh`? | expected in the `pm2D` process | observed in `/proc/<pid>/environ` | result |
|---|---|---|---|---|---|
| 1 | `WOA23_PORT` | **yes** | `18262` | *(filled at execution)* | *(filled)* |
| 2 | `WOA23_ZARR_STORE` | **yes** | `/home/odbadmin/woa23-pm2d/store` | *(filled at execution)* | *(filled)* |
| 3 | `WOA23_TLS` | **yes** | `off` | *(filled at execution)* | *(filled)* |
| 4 | `WOA23_WORKERS` | **yes** | production's value, unchanged (`2`) | *(filled at execution)* | *(filled)* |
| 5 | `WOA23_TLS_KEYFILE` | **yes** | production's value, unchanged — never read, TLS is off | *(filled at execution)* | *(filled)* |
| 6 | `WOA23_TLS_CERTFILE` | **yes** | production's value, unchanged — never read | *(filled at execution)* | *(filled)* |
| 7 | `WOA23_ANCHOR_REL` | **yes** | **ABSENT**, so the launcher's default `1_degree/annual/TS` applies | *(filled at execution)* | *(filled)* |
| 8 | `WOA23_PYTHON` | **yes** | **ABSENT** — a stray value would silently change which interpreter serves | *(filled at execution)* | *(filled)* |
| 9 | `WOA23_PRODUCTION_STORE` | **NO** | **ABSENT** — see §6.3.1 | *(filled at execution)* | *(filled)* |
| 10 | `WOA23_PM2C_GRANTED` | **NO** | **ABSENT** — consumed at the entry, see §6.3.3 | *(filled at execution)* | *(filled)* |

**Rows 1–8 are exactly the eight variables the launcher reads: six valued, two absent.
Rows 9 and 10 are two it does not read and must not carry.** A mismatch on any row is a
**stop**, not a warning.

### 6.3.2 Anything outside the ten — the decision, and it is fail-closed

**The ten are an ALLOWLIST. Any `WOA23_*` in the process that is not one of them makes
the environment INVALID: the run stops, and NO staging PASS is produced.**

An earlier draft printed unexpected variables and continued. **That is not a check** — it
is a note a reader may or may not act on, in a report that says PASS at the bottom.

The failure it would miss is precise and likely: **a variable is added to
`production_app.sh`, this entry is not updated, and the run reports a verified environment
while the new variable's effect goes entirely unexamined.** Fail-closed converts that
silence into a stop.

| condition | classification | outcome |
|---|---|---|
| every `WOA23_*` is one of the ten | environment valid | the run continues |
| **any** `WOA23_*` outside the ten | **`INVALID ENVIRONMENT`** | **stop; no staging PASS**; the offending names are printed |

**Exceptions today: none.** The allowlist is exactly the ten of §6.3.

**If an exception is ever needed it must be added here by name**, with (a) its purpose,
(b) why it cannot affect the launcher's behaviour, and (c) the same addition made to the
entry's allowlist. **Never waved through at run time**, and never by widening a pattern.

### 6.3.3 The grant is consumed at the entry and must not reach the service

**`WOA23_PM2C_GRANTED` is an authorisation token for the entry, not runtime configuration
for the service.** PM2 spawns its daemon with the entry's environment and the daemon passes
that to the app, so without action the grant would land in the running service. Two things
would go wrong:

1. **a serving process would carry an authorisation token** — visible to anything that
   reads that process's environment;
2. **the strict allowlist of §6.3.2 would reject the run's own legitimate grant** as an
   unexpected variable. A safety rule that fails correctly-authorised runs is a rule
   whoever hits it next will weaken, which is how strict rules die.

**So the entry `unset`s it immediately before `pm2 start`**, after the generator has already
received it inline. **Its absence from the child is then row 10 of §6.3 — verified from
`/proc`, not assumed.** The unset is the intent; `/proc` is the evidence.

`WOA23_PM2C_GRANTED` is in the allowlist as a *known* name so that a leak is reported **by
name** as a process mismatch, rather than as a generic "unlisted variable" — which would
send the next reader hunting an unknown variable instead of a leaked token. The offline
suite injects a leaked grant and requires exactly that message.

**This is exercised, not asserted.** `scripts/test_pm2d_entry.sh` runs the entry's real
post-start path against a synthetic procfs — a clean environment passes; an injected
`WOA23_SOMETHING_NEW` is classified `INVALID ENVIRONMENT` with the variable named; and the
"launcher gained a variable" scenario fails closed. The full `WOA23_*` set is printed
before any assertion, so the evidence shows what was there as well as what was checked.

### 6.3.1 Why `WOA23_PRODUCTION_STORE` is checked ABSENT rather than verified

**`production_app.sh` has no production-store guard, and that is correct — production is
supposed to point AT production's store.** A "refuse a store inside production's" rule
belongs to:

- `start_staging.sh`, which has it (staging must never read production's data); and
- `make_staging_override.js`, which refuses a `--store` that is or is inside
  `/home/odbadmin/python/woa23/data`.

So there is **no such variable to verify in this process**, and its **presence** would mean
the staging launcher's guard had been mistaken for the production one. It is required
absent for that reason. `scripts/test_pm2d_entry.sh` asserts the asymmetry from the files
themselves: **zero** occurrences in `production_app.sh`, **non-zero** in `start_staging.sh`.

**The variable list is derived from the launcher's own source by the offline suite**, so a
variable added to `production_app.sh` and forgotten here fails a test rather than going
unverified on the host.

## 7. The sequence — each step a stop

1. **identity absent** — `~/woa23-pm2d/`, `~/woa23-pm2d-work/`, `~/woa23-pm2d-pm2/`; label
   `pm2D` has 0 artefacts. `pm2A`/`pm2B` paths and daemons confirmed present and untouched.
2. **port** — `18262` absent from the ledger **and** unbound on the host, re-checked now.
3. **archive** — digests and every named file hash re-derived on VM24 and compared file by
   file.
4. **venv** — `uv sync` pinned; `.venv/bin/python --version` is **3.11.4**; complete
   manifest recorded; CORE compared to `uv.lock`; FULL digest recorded, differences named.
5. **store** — built by the archive's builder; 72 / 25,191 / digest verified; anchor
   present; physical path not under production's; read-only with a failing write probe.
6. **config** — generated by §6; **the diff must show exactly the seven permitted keys**.
7. **start** — isolated `PM2_HOME`, one named app:
   `pm2 start deploy/ecosystem.pm2d.config.js --only woa23-pm2d-candidate`.
8. **environment in the process** — `/proc/<pid>/environ`: `WOA23_PORT=18262`, the staging
   store, `WOA23_TLS=off`.
9. **argv from `/proc/<pid>/cmdline`, not inferred from the config** — `api.app:app`, the
   staging venv's python, **no `--reload`**, **no `woa23_app`**, **no `--keyfile`**. Scoped
   by path, since another project also runs `api.app:app` on this host.
10. **provenance** — `/proc/<pid>/exe` resolves to `.pyenv/versions/3.11.4/bin/python3.11`,
    equal to production's PID 4296; `/proc/<worker>/maps` shows libraries from the `pm2D`
    venv and **none** from production or `py311`.
11. **readiness** — PM2 `online`, lifespan complete, **OpenAPI 1.1.0** on 18262 with the
    row-order statement in the description and both endpoints.
12. **contract** — JSON and CSV both 200, **144 rows each**, identical counts, strictly
    ascending by **numeric** `(time_period, depth, lat, lon)` across the three groups,
    with `13` after `2`.
13. **restart** — named app only; re-check 8–12; response byte-identical.
14. **stop via `production_stop.sh`** — the B1 path and the point of the run:
    `WOA23_PM2_HOME=~/woa23-pm2d-pm2 ./deploy/production_stop.sh woa23-pm2d-candidate`.
    It resolves the pid from PM2, records `(pid, starttime)` for master and workers, stops
    gracefully, and **verifies by identity** that the tree is gone. **No SIGKILL. No process
    of any other project may be signalled or named.**
15. **release** — 18262 free, `curl` refused, `pm2 delete` the named app, 0 apps left.
16. **production unchanged** — boot id, master/worker PIDs and starttimes, listeners on
    8050/8786/8787, production's PM2 list, read **before and after** and compared.
    **0 requests.** The staging app must not appear in production's PM2.

## 8. Failure handling

**Any step failing is a stop.** Staging tree, PM2 state, logs, store and diagnostics are
**retained**; nothing is cleared and nothing is re-run.

**If `production_stop.sh` exits 7 with a survivor, the run is classified `CLEANUP_FAIL`.**
The surviving process is left alive for inspection, **no SIGKILL is sent**, and the run is
**not repeated**.

## 9. Forbidden

Production API, production store, production PM2 state, `conf/`, production's app name
`woa23`, production's ports 8050/8786/8787; `pm2 * all`, `pm2 kill`, global
`save`/`resurrect`; copying production data; symlinking the production store; **SIGKILL**;
**self-rerun**; installing or evaluating **`polars-lts-cpu`**; changing any dependency
version, `api/`, or `conf/`; clearing or reusing `pm2A`/`pm2B`/`bash5A` evidence; touching
retained daemons **1242814**/**1248938**; touching the 16 local `arm.py` strays on the
development machine.

## 10. What a PASS will and will not mean

**Will:** the three proposed production files work **together** — the launcher starts the
candidate under PM2 with production's own `cwd`/`script` relationship and a validated
environment, the config carries real values with **no `pre_stop`**, the identity-based stop
terminates the recorded tree and proves it, and the port is released. The runtime is an
isolated venv on production's 3.11.4 with a fully recorded manifest.

**Will not:** **NOT a production cutover PASS.** Alternate port, **72-file synthetic
store**, **no TLS**, no reverse proxy, isolated PM2 daemon, `conf/` unmodified, production
untouched. **Not real-store correctness** — `c1f`/`c2g` are that. **Not a performance
result** — and under B6's accepted AVX2 masking no absolute figure is
production-representative. **B1–B5 are not closed by this run**: they would be *validated in
staging*, and closing them requires installation and a cutover, each separately authorised.
**B7 remains open**; spec 014 settles the runtime as an isolated venv, but B7 closes only
when a deployment actually uses one.

## 11. Submission values

Filled from the tree that passed three independent serial offline batches; see the covering
report. **The subject is `4f9fdfb`, filled in §1 above. This document's own commit is a protocol
reference and must never be back-filled as the subject.**
