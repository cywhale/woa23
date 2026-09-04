# PM2 alternate-port staging of the PRODUCTION launcher (`pm2G`) — authorisation request

**Status: REQUESTED, NOT GRANTED. Nothing here has been run.** No SSH session, no PM2
process, no port bound, no staging tree, no venv, no VM24 action of any kind.
**This document is not an authorisation.**

**It supersedes FOUR requests.** Two were stopped before any VM24 contact; two ran on VM24
and did not produce a staging PASS.

| identity | authorised | what the audit or run found | VM24 state created | port |
|---|---|---|---|---|
| `pm2C` | yes | `ecosystem.production.config.js` had no `cwd`; `script` and `api.app` needed different working directories, so gunicorn would have died at import | **none** | `18261` never bound, **not spent** |
| `pm2D` | yes | the entry's single freshness rule refused the staging root that step 3 had just created — the run would have stopped at its own guard | **none** | `18262` never bound, **not spent** |
| `pm2E` | yes | `INVALID_PRE_START` on VM24 — the venv step had put `UV_CACHE_DIR` inside the workdir; the run phase's workdir-lifecycle guard refused a workdir it did not create | **tree, venv, workdir — retained** | `18263` never bound, **SPENT** |
| `pm2F` | yes | **`INVALID_ENVIRONMENT` on VM24** — the service started; `/proc/<pid>/environ` carried `WOA23_PM2_BIN`, an eleventh `WOA23_*` outside the ten-variable allowlist | **tree, venv, workdir, store, generated config, PM2 daemon, running gunicorn — retained** | `18264` bound then released; **SPENT** |

**`pm2C` and `pm2D` are not VM24 executions** — no SSH session, no tree, no state — and
their identities were not consumed. **`pm2E` and `pm2F` ARE**: each created a staging tree
on VM24, so **their identities ARE consumed and their state is retained, not cleaned.** The
results are `PM2-staging-result-pm2E.md` (`INVALID_PRE_START`) and
`PM2-staging-result-pm2F.md` (`INVALID_ENVIRONMENT`).

**No part of `pm2E` or `pm2F` is reused here** — not the labels, not `~/woa23-pm2e*`, not
`~/woa23-pm2f*`, not `18263` or `18264`, and not their staged trees, venvs, workdirs, stores
or generated configs. `pm2G` is entirely new, as §3 sets out. **Nothing from either earlier
run may be back-filled as a staging PASS.**

**Revised after PI review of pm2G v1.** Two items were required and are now in:
(a) the pm2 jlist parser has a real regression fixture — a parameterised fake pm2
that emits `{"pid":…,"name":…}` (pid before name — the pm2F case), plus six other
scenarios — driven through the entry's actual post-start line, not a helper (§6.4,
`scripts/test_staging_entry.sh` +17 assertions);
(b) the pm2F cleanup provenance is reconciled honestly against the on-VM24 evidence
— `pm2 stop && pm2 delete` DID happen after INVALID_ENVIRONMENT and DID cross the
failure-state retention boundary. `PM2-staging-result-pm2F.md` §7.1 records this
explicitly; §0.3 below states it plainly; §8/§9 add the discipline that pm2G will
not repeat the pattern.

`pm2G` validates the three files actually proposed for production, as a set, on an
alternate port, on the fixed tree:

| file | role |
|---|---|
| `deploy/production_app.sh` | the proposed replacement for `conf/start_app.sh` |
| `deploy/ecosystem.production.config.js` | the proposed replacement for `conf/ecosystem.config.js` |
| `deploy/production_stop.sh` | the identity-based stop that replaces the `pre_stop` line |

---

## 0. What changed since `pm2F`, and what did not

### 0.1 The two defects `pm2F` surfaced, both mine, both in the entry

**Defect 1 — `WOA23_PM2_BIN` leaked to the service process.** The entry uses
`WOA23_PM2_BIN` to locate `pm2` (`PM2="${WOA23_PM2_BIN:-pm2}"`) and never `unset`s it before
`pm2 start`. PM2 spawns its God Daemon with the entry's environment and the daemon passes
that to the app, so the running service carried `WOA23_PM2_BIN` in `/proc/<pid>/environ`.
The strict allowlist of §6.3.2 refused this as an eleventh `WOA23_*` outside the ten and
classified the run `INVALID_ENVIRONMENT`. **The allowlist behaved correctly.** A variable
that does not affect the launcher, but whose presence in a "verified environment" report
would mean the verification was incomplete, is what fail-closed is for.

This is the same class of defect as the earlier `WOA23_PM2C_GRANTED` leak — an entry-only
authorisation token that PM2 would have inherited into the service — and the fix is the
same shape: `unset WOA23_PM2_BIN` beside `unset WOA23_PM2C_GRANTED`, immediately before
`pm2 start`.

**Defect 2 — pid extraction from `pm2 jlist` depended on JSON field order.** The awk
splits jlist on commas and waits for `"name"` before setting a flag that lets it read
`"pid"`. PM2 5.4.2 emits `"pid":<n>` **before** `"name":"<app>"`, so the flag was never
set when pid was on the line and pid was returned empty. **The service was running fine;
only the extraction failed**, and the entry died on `pm2 reports no pid`. The offline
fixture's fake `pm2` emits fields in the awk's expected order, which masked the bug there.

**Both fixed on `perf/2026-s1-remove-dask`, commit `81c2180`:**

```
unset WOA23_PM2_BIN                          # beside unset WOA23_PM2C_GRANTED
PID="$("$PM2" jlist | node -e '              # replaces the order-dependent awk
  const j = JSON.parse(require("fs").readFileSync(0, "utf8"));
  const p = j.find(a => a.name === process.argv[1]);
  if (p && p.pid) process.stdout.write(String(p.pid));
' "$APP")"
```

The pid parser now reads jlist as JSON and matches by `name`, so no field order can
mislead it. Missing pid, wrong app, multiple matches or malformed output all leave the
subsequent `[ -n "$PID" ] && [ "$PID" != "0" ]` check refusing.

### 0.2 What the port ledger and the test file needed after that

- `07f6143` records `pm2D`/`pm2E`/`pm2F` in `scripts/ports_used.tsv` — three identities
  the ledger had not caught up with. `pm2F` is marked SPENT and `INVALID_ENVIRONMENT`.
- `61168eb` moves the offline staging-entry test's fake ports from `18263`/`18264` (now
  claimed by pm2E/pm2F in the ledger) to `39263`/`39264`, a test-reserved range that will
  never appear in the real ledger. Nothing in the entry changed for this — the test's own
  fake ports needed to move out of the way of the real ones.

### 0.3 The complete lineage `pm2F` established, restated so the record cannot be softened

**`pm2F` is `INVALID_ENVIRONMENT`. Not a candidate failure — the candidate started and
ran correctly. Not a staging PASS.** The `pm2F` identity is CONSUMED. Port `18264` is
SPENT. The staging tree, workdir, PM2 home, uv cache and PM2 logs remain retained on
VM24 and must not be cleaned. **B1–B5 are NOT closed by this run** — the environment
was invalid and no contract check was reached.

Between `pm2 start` and the environment gate:

| question | answer |
|---|---|
| was the service listening on `18264`? | **yes** — gunicorn master pid 1438258, `online`, workers spawned |
| was any staging HTTP request issued from the entry? | **no** — the entry died at the environment-allowlist check (§6.3.2), which is BEFORE the readiness/OpenAPI step (§7 step 11) that would have been the first HTTP call |
| was any production request touched? | **no** — production listeners on 8050/8786/8787 unchanged, boot id identical, production PM2 `woa23` pid/starttime identical |
| how was the run stopped? | a **discretionary** `pm2 stop woa23-pm2f-candidate && pm2 delete woa23-pm2f-candidate` under the isolated `PM2_HOME` — **not `production_stop.sh`**, which was never invoked. **This crossed the pm2F failure-state retention boundary** as recorded in `PM2-staging-result-pm2F.md` §7.1. No SIGKILL was used; no filesystem evidence was cleared |
| what was released? | port `18264` — `ss` confirms unbound afterwards; the PM2 app entry `woa23-pm2f-candidate` was deleted; gunicorn pid 1438258 exited. **The `~/woa23-pm2f-pm2/` directory itself was NOT removed** — its `module_conf.json` and `logs/` remain and the isolated God Daemon still runs |
| what is retained on the filesystem? | `~/woa23-pm2f/` (9518 files), `~/woa23-pm2f-work/` (1 file), `~/woa23-pm2f-pm2/` (4 files — the daemon home directory), `~/woa23-pm2f-uvcache/` (8899 files), `tmp-pm2F/` under the tree, and the generated `ecosystem.pm2F.config.js`. **Nothing was deleted from any of these paths** — see `PM2-staging-result-pm2F.md` §7 |

### 0.4 What did NOT change

- No dependency version, no `api/`, no `conf/` — `c1f`/`c2g`/`s2pB` are undisturbed.
- No production launcher, no `ecosystem.production.config.js`, no `production_stop.sh`
  behaviour change — the three files this run validates are byte-identical to the ones
  `pm2F` was pointed at.
- No change to the allowlist itself, and no exception added — the fix is that the entry
  no longer leaks an entry-only variable into the child, not that the allowlist grew.
- No change to the ten-variable environment check.
- No change to the two-phase lifecycle, workdir stamping, store builder, override
  generator, `WOA23_PM2C_GRANTED` grant or the ports-ledger guard.

## 1. Execution subject

| item | value |
|---|---|
| commit | `06661fd9361b2fb48052e1e61825a41483fc1c9f` |
| archive SHA-256 | `597078731597e33554c4b30bc48ea573fb843c8d6154ad2ff3f97feee6259282` |
| file count | `164` |
| file-list SHA-256 | `00f866a9bc9a9c9c6d04f4a035f69f41bb9c05c82da5548533b626277332f6bd` |
| verifier | `scripts/verify_clean_archive.sh 06661fd`, **16/16** |
| offline evidence | **three independent serial batches on the fixed tree, 42/42 each, 0 non-zero, 0 failing assertions** |

**These values supersede pm2F's subject** (`a208b5c`, 163 files, archive `8ae507c4…`,
file-list `25835e39…`), which is void because the entry, port ledger and test-port fix
each changed the tree. Only the commit named above is the execution subject.

The files this run exercises. **Every digest is re-derived on VM24; a mismatch is a
stop.** Only two digests differ from pm2F: the entry (`staging_execute.sh`, fixed) and
the file count (`+1`, from adding `PM2-staging-result-pm2F.md`).

| file | SHA-256 |
|---|---|
| `deploy/staging_execute.sh` | `7098fd6b1cef91d9b7c3ce60d60c0171a1419ec8e6ba2880495feeb92d7593e5` |
| `deploy/make_staging_override.js` | `59495cd4a91700e2414de597080a52f699dd3630b941cb9fe0954c641b76e1d9` |
| `deploy/production_app.sh` | `3c6737a817cc12b7a37a2e9954698d0ce285e98c64e6b7a0b5a6e89a64d38ef3` |
| `deploy/production_stop.sh` | `e86f07f18b38ee4b470e7049d15c5363cfe9c3f8847e2602aa0096c1e6b7803f` |
| `deploy/ecosystem.production.config.js` | `a7e4cb7fcb47e83b8192b225093e5dbe20aad2c673e5fed511b2328ee22410c4` |
| `deploy/record_manifest.py` | `8e7a0aaba21ea7634033318297584bf9e756b765603a8626d934cafd7a91cd34` |
| `deploy/make_staging_store.py` | `cf121f7f41e15cd9a381d461772e2bc4a8b58281f7ba341baef14e9d3d5f69f1` |
| `api/query.py` | `8e980e5b60a004902e66e6cb86ed2352a5ec641a6ad4cd3173a5a2efc56cebce` |
| `api/app.py` | `15f26444af9fbffbdef8a240833c1ff7f8bc99fdf84277317eb15f859d467ce2` |

**`api/` is unchanged since the C1/C2 subject** — all five digests identical to `pm2F`
and back to `c1f`/`c2g`. No `api/`, `conf/`, query-logic, response-behaviour or
dependency-version change has occurred.

**On this document's own place in the subject — stated because it is easy to get wrong.**
The subject `06661fd` is FROZEN by explicit instruction; it is the tree the fixed entry
and the port/test fixes together belong to. **This document is a protocol reference and
its own commit MUST NEVER be back-filled as the execution subject.** On VM24 the archive
is verified against `06661fd`'s digests, so the copy of this document found in the
staged tree is **absent** — it was not written yet at that commit. That is expected and
is not a mismatch — the digests identify the tree, not the prose inside it.

## 2. The grant, and where it is enforced

```
WOA23_PM2C_GRANTED=yes
```

**Enforced by `deploy/staging_execute.sh`, the execution entry — the thing that actually
starts the run.** Unchanged from `pm2F`.

| enforced where | refuses on |
|---|---|
| **`deploy/staging_execute.sh`** — *the* entry, checked **first**, before arguments or paths | grant absent, empty, or any value other than exactly `yes`; **any** of the seven other grants present |
| `deploy/make_staging_override.js` — defence in depth | the same two conditions |

The grant is **consumed** at the entry (see §6.3.3), joined now by `unset WOA23_PM2_BIN`
immediately after — closing the leak `pm2F` exposed.

**Both directions of bypass are closed.** If the entry were skipped, the generator still
refuses; if the generator were skipped by reusing an old config file, **the entry
refuses**, because it regenerates rather than reuses and stops if the target config
already exists.

The seven refused alongside: `WOA23_S2PERF_GRANTED`, `WOA23_S2_C1_GRANTED`,
`WOA23_S2_C2_GRANTED`, `WOA23_D1_GRANTED`, `WOA23_D2A_GRANTED`, `WOA23_D2B_GRANTED`,
`WOA23_BASH5_VERIFY_GRANTED`. **No existing grant substitutes.**

### 2.1 Three tokens that must not be confused

| token | what it is |
|---|---|
| **`PM2C`** | the **validation mode** and the **grant name** (`WOA23_PM2C_GRANTED`). Not a run |
| **`pm2G`** | **this run's execution identity** — label, staging tree, workdir, `PM2_HOME`, port, app name |
| **`pm2C`** | an **earlier request**, authorised and then stopped by a pre-start audit. It **never executed**, bound nothing and left no state. Not a run either |

## 3. Execution identity — entirely new, and none of pm2F is reused

| | value | not reused |
|---|---|---|
| label | **`pm2G`** | `pm2A`, `pm2B`, `pm2C`, `pm2D`, `pm2E`, **`pm2F`** |
| staging | **`~/woa23-pm2g/`** | — |
| workdir | **`~/woa23-pm2g-work/`** | — |
| PM2 home | **`~/woa23-pm2g-pm2/`** | — |
| store | **`/home/odbadmin/woa23-pm2g/store`** | — |
| uv cache | **`~/woa23-pm2g-uvcache`** | — |
| API port | **`18265`** | **never `18262`, `18263`, `18264`** |
| PM2 app name | **`woa23-pm2g-candidate`** | **never `woa23`** |
| generated config | **`deploy/ecosystem.pm2G.config.js`** (inside the staged tree) | — |
| log dir | **`tmp-pm2G`** (inside the staged tree) | — |

**`18265` is first-use in the strict sense:** absent from `scripts/ports_used.tsv` **and**
named in no other file in the tree. Ports `18221`/`18231`/`18241`/`18262`/`18263`/`18264`
are spent — 18262 and 18263 by pm2D/pm2E (allocated, never bound), 18264 by pm2F (bound,
released, SPENT). Ports `18251`, `18261`, `18271` are unspent but named in earlier
documents or tests, so none of them is used.

**`18265` is deliberately NOT pre-recorded in the ledger.** The launcher refuses any
ledgered port, so entering one before its run makes the guard reject the run it was
chosen for. It enters the ledger only after this run binds it — and only through a
deliberate `07f6143`-style commit after the result is written.

**`pm2A`, `pm2B`, `pm2E` and `pm2F` are untouched** — their trees, stores, PM2 homes,
uv caches, logs and retained daemons (**1242814** for pm2A, **1248938** for pm2B, and
the pm2F daemon under `~/woa23-pm2f-pm2/`) stay as they are. **Their cleanup is a
separate item and is not part of this run.**

### 3.1 Restated for the record, because each has been a source of confusion

1. **`pm2G` is THIS run's execution identity** — label, staging tree, workdir,
   `PM2_HOME`, port and app name, all new.
2. **`PM2C` is the validation mode and the grant name** (`WOA23_PM2C_GRANTED`). It is
   not a run and never was.
3. **The old `pm2C` request NEVER EXECUTED.** It was authorised and stopped by a
   pre-start audit; no SSH session, no tree, no venv, no daemon, no bound port, no
   state. **It is not a VM24 execution and must never be counted as one**, and `18261`
   is not spent.
4. **`pm2E` and `pm2F` ARE VM24 executions and their identities are consumed.** Their
   state is retained and must not be cleaned. Their ports are SPENT. Neither is a
   staging PASS and nothing they established may be back-filled as one.
5. **The store is synthetic** — 72 files built on the host by the subject's own
   builder. **No production data is copied and no symlink to the production store is
   created.** The production store is never read.
6. **`18265` is re-verified at execution time**, immediately before use: absent from
   `scripts/ports_used.tsv` **and** unbound on the host (via `ss -ltn`). A ledger
   entry or a live listener at that moment is a stop.
7. **Before/after evidence is recorded for all of it** — PM2 state under both the
   isolated and production daemons, production's listeners on 8050/8786/8787, the
   synthetic store's digest, and the cleanup outcome. §7 steps 1, 5, 15 and 16.

## 4. The store — synthetic only

Built on VM24 by the archive's own `deploy/make_staging_store.py`:

| | |
|---|---|
| path | `/home/odbadmin/woa23-pm2g/store` |
| files / bytes | **72 / 25,191** (identical to `pm2F` — same builder) |
| file-list SHA-256 | *(derived at execution time on VM24; expected to equal `pm2F`'s `8fb70f2c64d7a3ee3d7fa451de08218c7a1740b19451cf015dd83394fd328624` because `make_staging_store.py` is byte-identical between the two subjects)* |
| groups | `1_degree/annual/TS` (period 0), `monthly/TS` (1, 2), `seasonal/TS` (13) |

**No production data is copied. No symlink to the production store. The production
store is never read.** Made read-only after the build, with a write probe required to
fail and the digest unchanged afterwards.

## 5. The venv — isolated, pinned, fully recorded

```
cd ~/woa23-pm2g/dev2026
UV_CACHE_DIR=~/woa23-pm2g-uvcache \
  uv sync --python /home/odbadmin/.pyenv/versions/py311/bin/python3.11
```

**`UV_CACHE_DIR` is a task-specific path OUTSIDE the workdir, and that is the `pm2E`
fix, upheld here.** The workdir must not exist when `--phase run` starts — the run phase
creates and stamps it with `run-identity-v1` markers.

| | |
|---|---|
| location | `~/woa23-pm2g/dev2026/.venv` — inside this run's own tree |
| interpreter | **Python 3.11.4**, production's, named not resolved |
| shared `py311` | **not the runtime** — only the interpreter `uv sync` builds *from* |
| polars | **mainline 1.27.1**, per the B6 decision. **`polars-lts-cpu` is not installed** |
| manifest | expected to match `pm2E`/`pm2F` — 58 distributions, `manifest_sha256 = 3835c975859181f377de085c70f36a2c43a33d5b81bfc1c690be0ee59739d0d6`, because no dependency changed between subjects |

`deploy/record_manifest.py` records the **complete** manifest. CORE (ten packages) must
match `uv.lock` exactly — a difference is a stop. FULL is recorded with its digest, and
**every difference from the development manifest must be NAMED with its reason**; an
unexplained one is a stop. The recorder imports nothing.

## 6. The override config — five items, and a sixth is a stop

Generated on the host by `deploy/make_staging_override.js` **from the production config
itself**, which it evaluates with node:

```
WOA23_PM2C_GRANTED=yes node deploy/make_staging_override.js \
  --name woa23-pm2g-candidate --port 18265 \
  --store /home/odbadmin/woa23-pm2g/store \
  --logdir tmp-pm2G --out deploy/ecosystem.pm2G.config.js
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
certificate paths — **must be identical, and the generator refuses if any of them
differs.** A resurrected `pre_stop` is refused outright. **A sixth difference is a
stop, and the run does not proceed.**

**`cwd` and `script` are carried through unchanged, and that is the point of the run**
— staging exercises production's own cwd/script relationship.

**TLS is off — a stated GAP, not a claim.** Staging has no certificates and fabricates
none. It does exercise `production_app.sh`'s explicit `WOA23_TLS=off` path, which
warns. **TLS validation belongs to the cutover.**

### 6.1, 6.2 unchanged from `pm2F`

The `env` block behaviour (§6.1), the digest-based config provenance (§6.2), and the
subject provenance guarding around the generator all carry through unchanged. Every
digest is re-verified on VM24 immediately before `pm2 start` and a mismatch is a stop.

## 6.3 Every environment variable, verified from the process

**Ten checks: six exact values, four required ABSENT.** Eight of the ten are the
variables `production_app.sh` actually reads; the other two it does not read and must
not carry. Unchanged from `pm2F`.

| # | variable | read by `production_app.sh`? | expected in the `pm2G` process | observed in `/proc/<pid>/environ` | result |
|---|---|---|---|---|---|
| 1 | `WOA23_PORT` | **yes** | `18265` | *(filled at execution)* | *(filled)* |
| 2 | `WOA23_ZARR_STORE` | **yes** | `/home/odbadmin/woa23-pm2g/store` | *(filled at execution)* | *(filled)* |
| 3 | `WOA23_TLS` | **yes** | `off` | *(filled at execution)* | *(filled)* |
| 4 | `WOA23_WORKERS` | **yes** | production's value, unchanged (`2`) | *(filled at execution)* | *(filled)* |
| 5 | `WOA23_TLS_KEYFILE` | **yes** | production's value, unchanged — never read, TLS is off | *(filled at execution)* | *(filled)* |
| 6 | `WOA23_TLS_CERTFILE` | **yes** | production's value, unchanged — never read | *(filled at execution)* | *(filled)* |
| 7 | `WOA23_ANCHOR_REL` | **yes** | **ABSENT**, so the launcher's default `1_degree/annual/TS` applies | *(filled at execution)* | *(filled)* |
| 8 | `WOA23_PYTHON` | **yes** | **ABSENT** — a stray value would silently change which interpreter serves | *(filled at execution)* | *(filled)* |
| 9 | `WOA23_PRODUCTION_STORE` | **NO** | **ABSENT** — see §6.3.1 | *(filled at execution)* | *(filled)* |
| 10 | `WOA23_PM2C_GRANTED` | **NO** | **ABSENT** — consumed at the entry, see §6.3.3 | *(filled at execution)* | *(filled)* |

**Rows 1–8 are exactly the eight variables the launcher reads: six valued, two absent.
Rows 9 and 10 are two it does not read and must not carry.** A mismatch on any row is
a **stop**, not a warning.

### 6.3.2 Anything outside the ten — the decision, and it is fail-closed

**The ten are an ALLOWLIST. Any `WOA23_*` in the process that is not one of them makes
the environment INVALID: the run stops, and NO staging PASS is produced.**

**This is what caught `pm2F`.** The eleventh `WOA23_*` was `WOA23_PM2_BIN`, an
entry-only variable that leaked to the service. The fix is not to widen the
allowlist — it is to stop the leak, which `81c2180` does with an `unset WOA23_PM2_BIN`
beside `unset WOA23_PM2C_GRANTED`.

| condition | classification | outcome |
|---|---|---|
| every `WOA23_*` is one of the ten | environment valid | the run continues |
| **any** `WOA23_*` outside the ten | **`INVALID ENVIRONMENT`** | **stop; no staging PASS**; the offending names are printed |

**Exceptions today: none.** The allowlist is exactly the ten of §6.3. **`WOA23_PM2_BIN`
is NOT added to the allowlist** — it does not belong in the service's environment at
all, and no entry-only variable should. Any future entry-only variable added inside the
`WOA23_*` namespace must be `unset` before `pm2 start` alongside the other two.

**If a runtime exception is ever needed it must be added here by name**, with (a) its
purpose, (b) why it cannot affect the launcher's behaviour, and (c) the same addition
made to the entry's allowlist. **Never waved through at run time**, and never by
widening a pattern.

### 6.3.3 The two entry-only variables that must not reach the service

**Both are authorisation- or lookup-tokens for the entry, not runtime configuration for
the service.** PM2 spawns its daemon with the entry's environment and the daemon passes
that to the app, so without action either would land in the running service:

| variable | purpose in the entry | if leaked to the service |
|---|---|---|
| `WOA23_PM2C_GRANTED` | the run's grant — the authorisation token the entry checks first | a serving process would carry an authorisation token; the allowlist would reject the run's own grant |
| `WOA23_PM2_BIN` | overrides which `pm2` binary the entry invokes (used because `pm2` is not on the default SSH `PATH` on VM24) | an eleventh `WOA23_*` outside the allowlist — INVALID ENVIRONMENT — as `pm2F` demonstrated |

**Both `unset` immediately before `pm2 start`:**

```
unset WOA23_PM2C_GRANTED
unset WOA23_PM2_BIN
```

**Absence in the child is then rows 10 and the allowlist check of §6.3.2, respectively
— verified from `/proc`, not assumed.** The unset is the intent; `/proc` is the
evidence.

`WOA23_PM2C_GRANTED` remains in the allowlist as a *known* name so that a leak is
reported by name as a process mismatch rather than as a generic "unlisted variable".
`WOA23_PM2_BIN` is *not* added to the allowlist — the correct outcome for
`WOA23_PM2_BIN` in the child is INVALID_ENVIRONMENT, exactly as `pm2F` produced.

**This is exercised offline in `scripts/test_staging_entry.sh`**: the entry's real
post-start path against a synthetic procfs — a clean environment passes; an injected
`WOA23_SOMETHING_NEW` is classified `INVALID ENVIRONMENT` with the variable named; and
a leaked `WOA23_PM2C_GRANTED` is caught as a leak of that token by name. **The offline
suite does not currently exercise a live PM2 daemon with `WOA23_PM2_BIN` in its
environment** — that would require a real PM2 install and would rebuild every scenario
the fake `pm2` already covers, so the assurance for `WOA23_PM2_BIN` specifically
comes from the source: the entry `unset`s it, and the allowlist rejects any leak by
its own logic. On VM24 the reading from `/proc` is the evidence.

### 6.3.1 unchanged from `pm2F`

`WOA23_PRODUCTION_STORE` is checked absent because `production_app.sh` has no
production-store guard — production is supposed to point AT production's store. The
staging guard lives in `start_staging.sh` and `make_staging_override.js`.

## 6.4 pid extraction — JSON, not field order

`pm2F` also revealed a defect in how the entry extracts the process's pid from `pm2
jlist`. The old awk assumed `"name"` appeared before `"pid"` in each app object; PM2
5.4.2 emits them the other way round, so the awk returned an empty pid and the entry
died on `pm2 reports no pid` while the service was actually running fine.

**Fix (`81c2180`):**

```
PID="$("$PM2" jlist 2>/dev/null | node -e "
  const j = JSON.parse(require('fs').readFileSync(0, 'utf8'));
  const p = j.find(a => a.name === process.argv[1]);
  if (p && p.pid) process.stdout.write(String(p.pid));
" "$APP")"
[ -n "$PID" ] && [ "$PID" != "0" ] || die "pm2 reports no pid for $APP after start."
```

- **Parses the full JSON**, not commas-and-fields, so the order PM2 emits things in is
  irrelevant.
- **Matches by `name`** rather than by textual proximity, so a partial substring in
  another field cannot be mistaken for the app.
- **Emits nothing if there is no match, an empty name, or a missing pid**, and the
  subsequent check dies. No pid, no continuation.
- Multiple matches would be a genuine surprise (this run's isolated PM2_HOME hosts one
  app) and the entry then trusts the FIRST — but the check that follows is against
  `/proc/<PID>/environ`, so a wrong pid would fail the environment allowlist
  immediately. A hardening to refuse `>1` matches is out of scope for this fix.
- Malformed jlist output (a non-JSON payload) would make `JSON.parse` throw and node
  exit non-zero; the outer command substitution yields empty and the `[ -n "$PID" ]`
  check refuses.

**Regression evidence — the tests that drive the ENTRY'S OWN parser through every
case PM2 could produce, added after PI review of pm2G v1.** The fake `pm2` in
`scripts/test_staging_entry.sh` is now parameterised by `FAKE_JLIST_MODE`, and a new
section drives the entry past `--preflight-only` so the real post-start pid-extraction
line runs. Each mode targets a specific behaviour, and the assertion is on what the
entry printed or refused with — **not on a helper, and not a fake success path.**

| mode | jlist body | expected entry behaviour | asserted by |
|---|---|---|---|
| `pid_first` | `[{"pid":4242,"name":"c-JPID","pm2_env":{...}}]` — **the pm2F case** | prints `pid: 4242`, proceeds to /proc | `grep -q '^pid: 4242$'` in the entry's output |
| `name_first` | `[{"name":"c-JNM","pid":4242,"pm2_env":{...}}]` | prints `pid: 4242`, proceeds to /proc | `grep -q '^pid: 4242$'` |
| `missing_pid` | `[{"name":"c-JMS","pm2_env":{...}}]` — no `pid` key | dies "pm2 reports no pid" | `rc = 2` and grep |
| `empty` | `[]` | dies "pm2 reports no pid" | `rc = 2` and grep |
| `multiple` | two entries, both named `c-JML`, pids 4242 then 9999 | takes the FIRST (`pid: 4242`), NOT the second | positive and negative grep |
| `malformed` | `not valid json` | node throws, pid empty, dies "pm2 reports no pid" | `rc = 2` and grep |
| `wrong_app` | `[{"pid":4242,"name":"some-other-app",...}]` | dies "pm2 reports no pid" (no name match) | `rc = 2` and grep |

The tests go PAST `--preflight-only`, so the entry actually runs `pm2 start` and then
the pid-extraction line at `deploy/staging_execute.sh:407`. The fake pm2's returned
pid (4242) does not correspond to any real process on the test host, so after
extraction the entry dies at `/proc/4242/environ` unreadable — that is stable across
every case and does not skew the pass/fail signal. Two extra assertions confirm this
was the entry, not a helper: `pm2 start` and `pm2 jlist` are both recorded in the
fake pm2's invocation log.

Two source-level hygiene checks close the door on the old parser:

```
check "the entry parses jlist with 'node -e' (JSON), not by field order" "yes"
check "  and it must not use the old awk-based field-order parser" "no"
```

The first fails if `node -e` on `jlist` disappears; the second fails if the awk
`/"name"/ { inapp = … }` shape returns. Renaming the parser is fine, silently
replacing it with an order-dependent one is not.

**Total: seventeen new assertions in `scripts/test_staging_entry.sh` — 92 → 109.**
The suite now proves the new parser is what the entry runs, and the pm2F case is now
a positive test rather than a hole covered by the VM24 run alone. Full offline
evidence in the corresponding suite result cited in §11.

## 7. The sequence — each step a stop

1. **identity absent** — `~/woa23-pm2g/`, `~/woa23-pm2g-work/`, `~/woa23-pm2g-pm2/`,
   `~/woa23-pm2g-uvcache`; label `pm2G` has 0 artefacts. `pm2A`/`pm2B`/`pm2E`/`pm2F`
   paths and daemons confirmed present and untouched.
2. **port** — `18265` absent from the ledger **and** unbound on the host, re-checked
   now via `ss -ltn`.
3. **archive** — digests and every named file hash re-derived on VM24 and compared
   file by file against §1 (164 files, `00f866a9…`, entry `7098fd6b…`).
4. **venv** — `uv sync` pinned; `.venv/bin/python --version` is **3.11.4**; complete
   manifest recorded; CORE compared to `uv.lock`; FULL digest expected to match
   `pm2E`/`pm2F`'s `3835c975…`, any difference NAMED with its reason.
5. **store** — built by the archive's builder; 72 / 25,191 / digest verified (expected
   `8fb70f2c…`); anchor present; physical path not under production's; read-only with
   a failing write probe.
6. **config** — generated by §6; **the diff must show exactly the seven permitted
   keys**.
7. **start** — isolated `PM2_HOME`, one named app:
   `pm2 start deploy/ecosystem.pm2G.config.js --only woa23-pm2g-candidate`.
   **`WOA23_PM2C_GRANTED` and `WOA23_PM2_BIN` are both `unset` in the entry
   immediately before this step; §6.3.3.**
8. **pid extraction** — via the JSON-parsed `pm2 jlist`, matched by `name`. Empty pid
   is a stop.
9. **environment in the process** — `/proc/<pid>/environ`: ten checks per §6.3,
   allowlist per §6.3.2. Any `WOA23_*` outside the ten is `INVALID ENVIRONMENT`, a
   stop, no staging PASS. This is the gate `pm2F` failed and this request has to
   pass.
10. **argv from `/proc/<pid>/cmdline`, not inferred from the config** — `api.app:app`,
    the staging venv's python, **no `--reload`**, **no `woa23_app`**, **no
    `--keyfile`**. Scoped by path.
11. **provenance** — `/proc/<pid>/exe` resolves to
    `.pyenv/versions/3.11.4/bin/python3.11`, equal to production's PID 4296;
    `/proc/<worker>/maps` shows libraries from the `pm2G` venv and **none** from
    production or `py311`.
12. **readiness** — PM2 `online`, lifespan complete, **OpenAPI 1.1.0** on 18265 with
    the row-order statement in the description and both endpoints.
13. **contract** — JSON and CSV both 200, **144 rows each**, identical counts,
    strictly ascending by **numeric** `(time_period, depth, lat, lon)` across the
    three groups, with `13` after `2`.
14. **restart** — named app only; re-check 9–13; response byte-identical.
15. **stop via `production_stop.sh`** — the B1 path and the point of the run:
    `WOA23_PM2_HOME=~/woa23-pm2g-pm2 ./deploy/production_stop.sh
    woa23-pm2g-candidate`. It resolves the pid from PM2, records `(pid, starttime)`
    for master and workers, stops gracefully, and **verifies by identity** that the
    tree is gone. **No SIGKILL. No process of any other project may be signalled or
    named.**
16. **release** — 18265 free, `curl` refused, `pm2 delete` the named app, 0 apps left
    under this run's isolated `PM2_HOME`.
17. **production unchanged** — boot id, master/worker PIDs and starttimes, listeners
    on 8050/8786/8787, production's PM2 list, read **before and after** and compared.
    **0 requests.** The staging app must not appear in production's PM2.

## 8. Failure handling

**Any step failing is a stop.** Staging tree, PM2 state, logs, store, generated
config, uv cache, running processes and every diagnostic are **retained**; nothing is
cleared and nothing is re-run. **The `pm2A`/`pm2B`/`pm2E`/`pm2F` evidence trees are
also untouched by this run.**

**After a mid-flight failure — including `INVALID_ENVIRONMENT` at step 9 — the
following are FORBIDDEN, tightening the boundary pm2F crossed:**

- **No `pm2 stop`** of the failing app under any PM2_HOME.
- **No `pm2 delete`** of the failing app under any PM2_HOME.
- **No port release** by any other means — the port stays bound as long as the
  process is alive.
- **No manual process termination** — no `kill`, `pkill`, `pgrep|kill`, `pm2 kill`,
  `pm2 flush`, `pm2 reload`, `pm2 restart`, `pm2 save` or `pm2 resurrect`.
- **No filesystem cleanup** — no `rm`, no `find -delete`, no `chmod` on the
  retained tree.

**pm2F crossed this boundary by performing a discretionary `pm2 stop && pm2 delete`
after INVALID_ENVIRONMENT** (`PM2-staging-result-pm2F.md` §7.1). The rationale then
was to release the bound port and stop the running gunicorn. **That is not a
rationale that survives review** — the request document said "retain, do not clear"
and the port/process are what "retain" was about. pm2G forbids repeating that
choice.

**The correct action on a mid-flight failure is:** stop reading the code, write the
result, submit it, and wait for the PI's explicit cleanup authorisation. A separate
cleanup step, if authorised, will use `production_stop.sh` — the same identity-based
stop the successful path exercises — under its own logged evidence.

**If step 15 (`production_stop.sh`) is reached and exits 7 with a survivor, the run
is classified `CLEANUP_FAIL`.** The surviving process is left alive for inspection,
**no SIGKILL is sent**, and the run is **not repeated**.

**If step 9 refuses with `INVALID ENVIRONMENT`, the classification is
`INVALID_ENVIRONMENT`, the identity is CONSUMED, the port becomes SPENT, and the
running process is LEFT ALIVE** — the same failure classification `pm2F` received,
with the retention discipline pm2F did not observe.

## 9. Forbidden

Production API, production store, production PM2 state, `conf/`, production's app name
`woa23`, production's ports 8050/8786/8787; `pm2 * all`, `pm2 kill`, global
`save`/`resurrect`; copying production data; symlinking the production store;
**SIGKILL**; **self-rerun** (never reissue this run in-session on any failure);
**clearing or reusing** `pm2A`/`pm2B`/`pm2E`/`pm2F` evidence; **reusing** `pm2F`'s
label, staging tree, workdir, PM2 home, uv cache, generated config, app name or port
`18264`; touching retained daemons **1242814** (pm2A), **1248938** (pm2B) or the
`pm2F` daemon under `~/woa23-pm2f-pm2/`; installing or evaluating
**`polars-lts-cpu`**; changing any dependency version, `api/`, or `conf/`; touching
the 16 local `arm.py` strays on the development machine; touching `apiverse`.

**Also forbidden after any mid-flight failure of THIS run (the boundary pm2F
crossed):** `pm2 stop`, `pm2 delete`, `pm2 flush`, `pm2 reload`, `pm2 restart`,
`pm2 save`, `pm2 resurrect` on this run's app under any PM2_HOME; manual process
termination by `kill`, `pkill`, `pgrep|kill`; port release by any means; any `rm`,
`find -delete` or `chmod` on this run's retained tree. **The running process and the
PM2 app entry stay as they are until the PI explicitly authorises a separate
cleanup.** See §8.

## 10. What a PASS will and will not mean

**Will:** the three proposed production files work **together** — the launcher starts
the candidate under PM2 with production's own `cwd`/`script` relationship and a
validated environment, the config carries real values with **no `pre_stop`**, the
identity-based stop terminates the recorded tree and proves it, and the port is
released. The runtime is an isolated venv on production's 3.11.4 with a fully
recorded manifest. **The `WOA23_PM2_BIN` and pid-extraction fixes both exercise on
VM24, in that they are on the path a PASS must traverse.**

**Will not:** **NOT a production cutover PASS.** Alternate port, **72-file synthetic
store**, **no TLS**, no reverse proxy, isolated PM2 daemon, `conf/` unmodified,
production untouched. **Not real-store correctness** — `c1f`/`c2g` are that. **Not a
performance result** — and under B6's accepted AVX2 masking no absolute figure is
production-representative. **B1–B5 are not closed by this run**: they would be
*validated in staging*, and closing them requires installation and a cutover, each
separately authorised. **B7 remains open**; spec 014 settles the runtime as an
isolated venv, but B7 closes only when a deployment actually uses one. **pm2F's
`INVALID_ENVIRONMENT` remains the pm2F result**; a pm2G PASS does not retroactively
re-classify it.

## 11. Submission values

- **Subject:** `06661fd9361b2fb48052e1e61825a41483fc1c9f`, filled in §1 above.
  **FROZEN by explicit PI instruction.** This document's own commit is a protocol
  reference and must never be back-filled as the subject.
- **Identity:** `pm2G` / `~/woa23-pm2g/` / `~/woa23-pm2g-work/` /
  `~/woa23-pm2g-pm2/` / `~/woa23-pm2g-uvcache` / port `18265` / app
  `woa23-pm2g-candidate` / config `deploy/ecosystem.pm2G.config.js` / logs
  `tmp-pm2G` — all first-use.
- **Grant:** `WOA23_PM2C_GRANTED=yes`, enforced by the entry and by the generator.
- **`WOA23_PM2_BIN`:** entry-only, `unset` before `pm2 start`, absence verified
  from `/proc/<pid>/environ`, allowlist fail-closes if it leaks.
- **pid extraction:** JSON-parsed, no field-order dependency, seventeen new
  regression assertions (see §6.4) exercise pid-before-name, name-before-pid,
  missing pid, empty jlist, multiple matches, malformed JSON and wrong app name —
  through the entry itself, past `--preflight-only`, not through a helper.
- **Failure-state discipline:** §8 and §9 forbid the discretionary
  `pm2 stop`/`pm2 delete` pattern pm2F used; a mid-flight failure leaves the
  process and PM2 app entry alive until the PI authorises a separate cleanup.
- **Offline evidence on the FIXED SUBJECT `06661fd`:** three serial batches,
  42/42 each, 0 non-zero, 0 failing assertions — reported in
  [`PM2-staging-result-pm2F.md`](PM2-staging-result-pm2F.md) §11.
- **Offline evidence on the LATER PROTOCOL-REFERENCE COMMIT that adds the
  regression tests (test-only change, entry byte-identical to `06661fd`):**
  three serial batches, **42/42 each, 0 non-zero, 0 failing assertions; total
  126 runs**, on `cbb7979` (the commit that adds the fixture) with the
  reconciliation commits `1281525` (pm2F cleanup) and `7cc909f` (pm2G v2) also
  in the tree. `test_staging_entry.sh` reported **109 assertions passed** in
  each of the three iterations (92 pre-existing + 17 new). Batch root:
  `/var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T//woa23-suites-KMukym/`
  (local, retained). Because the entry file's SHA-256 is byte-identical between
  `06661fd` and the commits above (both `7098fd6b…`), the seventeen new tests
  exercise the same parser bytes that `06661fd` will run on VM24.
- **Nothing on VM24 is contacted by this document.** A separate explicit
  authorisation is required to execute.
