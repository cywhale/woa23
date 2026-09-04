# 011 — The production launcher, and the cutover it makes possible

**Status: OFFLINE DESIGN AND IMPLEMENTATION DONE. Nothing is installed, nothing is
deployed, and no production file has been changed.** `conf/start_app.sh` and
`conf/ecosystem.config.js` are byte-identical to what production runs today; this spec's
own test suite asserts that they still carry every defect it describes.

**Installing these files into `conf/`, and the cutover itself, each need their own
explicit authorisation.** This document is a proposal and a plan, not a request.

| rev | date | change |
|---|---|---|
| 1 | 2026-08-20 | First draft, after `pm2B` PASSED. Resolves B1–B5 offline; raises **B7** (introduced as B6, renumbered — see §4). |

---

## 1. Why this comes before the authorisation request, not after

`pm2B` proved the candidate runs under PM2 with a correct environment, serves the 1.1.0
contract, holds row order and stops cleanly. **It proved that about
`dev2026/deploy/start_staging.sh` — a file production does not use.** Production still
runs a launcher that starts the wrong application on a hard-coded port with no store and
`--reload` live, behind a `pre_stop` that `kill -9`s by grep.

**The staging configuration must not be copied to production.** It has no TLS, it refuses
production's ports by design, and it reads a ledger of spent staging ports. It was built
to be unlike production; that is the whole reason it was safe to run.

So the cutover needs a *production* launcher, designed as such, tested offline, and
reviewed before anyone is asked to authorise running it.

## 2. What is proposed

Two new files, both under `dev2026/deploy/`, neither installed:

| file | replaces | status |
|---|---|---|
| `deploy/production_app.sh` | `conf/start_app.sh` | proposed |
| `deploy/ecosystem.production.config.js` | `conf/ecosystem.config.js` | proposed |
| `deploy/production_stop.sh` | the `pre_stop` line, which is **deleted** | proposed |

Checked by `scripts/test_production_launcher.sh` (**88 assertions**) and
`scripts/test_production_stop.sh` (**33 assertions**), all passing — including behavioural
runs against a stub interpreter, so the argv that would reach gunicorn is observed rather
than inferred, and a synthetic `/proc` with a decoy process from another project, so the
stop path is proven not to touch it.

`production_stop.sh` is **not wired in as a `pre_stop` hook** — reintroducing a `pre_stop`
is the thing being removed. PM2's own signal stops the app, because the launcher `exec`s;
the script is the operator's stop-and-verify path, and it is what makes "the tree exited"
a checked claim rather than an assumption.

### 2.1 How each blocker is resolved

| # | blocker | resolution |
|---|---|---|
| **B2** | launches the old `woa23_app:app` | `APP="api.app:app"`, fixed in the launcher, asserted in the argv |
| **B3** | hard-coded `-b 127.0.0.1:8050` | the port is `WOA23_PORT`, **required, no default**; no port literal remains in the file |
| **B4** | no `WOA23_ZARR_STORE` | **required and validated** — directory, readable, and the anchor group's `.zgroup` present, so a store fault reads as a store fault instead of a restart loop |
| **B5** | `--reload` live in production | absent, and asserted absent from the argv |
| **B1** | `pre_stop` greps `woa23_app` and `kill -9`s the matches | **removed, not rewritten** — see §2.2 |

### 2.2 B1 deserves its own explanation, because it is worse than recorded

The production `pre_stop` is:

```
ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9
```

Spec 010 recorded two faults — it matches a command-line string, and it uses `kill -9`.
**There is a third, and it is that the pipeline is saved from killing itself by
accident.**

`ps -ef` lists the shell running this very command, and that shell's argv contains
`woa23_app`, so it *does* match `grep -w 'woa23_app'`. It is then removed — because that
same command string also contains the word `grep`, and `grep -v grep` filters any line
containing it. **The pipeline survives because the text being filtered happens to contain
the filter's own word.**

That is not a designed safety property, and it is load-bearing. Rewrite the `pre_stop` to
use `pgrep` — the obvious modernisation, and one that removes the word `grep` from the
matched portion of the command line — and the self-match becomes live: the pipeline puts
its own PID in its own `kill -9` list, and what it kills before killing itself depends on
scheduling.

**So the safest-looking repair of this line is the one that breaks it.** That is the
argument for deleting it rather than improving it.

And after a cutover it would match a name the service no longer has — so it would stop
doing anything at all, which is a defect that stops looking like one.

**It is deleted rather than repaired.** It is unnecessary: `production_app.sh` `exec`s
gunicorn, so PM2 tracks the master directly and PM2's own stop signal reaches it. `pm2B`
demonstrated exactly this — SIGINT to the master, both workers drained and gone, no
`pre_stop` present anywhere.

### 2.3 The `env` block: why staging removed one and production has one

`pm2A` failed because a config `env` block overrode the environment `pm2 start` was given.
The lesson is **not** "never use `env`" — it is "the config wins, so what is in it must be
the truth".

- **Staging values are per-run** — a new port, tree and store each time — so they come
  from the operator, and the staging config carries no `env` block at all.
- **Production values are fixed properties of the deployment**, so they belong in the
  config, where they are visible, reviewable, and identical on every restart.

What made `pm2A` a failure was **placeholders** in that block: an empty store and a stale
port. Every value in the proposed production block is real and explicit, the test suite
asserts that none is an empty string, and the launcher refuses an empty value rather than
defaulting it.

### 2.4 What is deliberately NOT changed

TLS stays in gunicorn (`--keyfile`/`--certfile`), because that is where production
terminates it today; dropping it would turn an HTTPS endpoint into an HTTP one at cutover.
Certificate paths are configurable and are checked for readability **before** the port is
claimed, and a bad path is a refusal — never a silent downgrade to plain HTTP. `autorestart`,
the 4G ceiling, two workers and the 120s timeout are all unchanged: a cutover changes the
application, not the restart policy.

One change beyond the blockers: **`append_env_to_name` goes from `true` to `false`.** With
it true, a `pm2 start --env production` creates `woa23-production` *alongside* `woa23` —
two apps, one port, and the original's PM2 state orphaned. At a cutover, where an operator
is more likely than usual to type `--env`, that is a live hazard.

### 2.5 A defect the offline suite caught

The first version of the launcher expanded `"${TLS_ARGS[@]}"` unguarded. Under `set -u`,
expanding an **empty** array is an unbound-variable error on bash 3.2 — still what macOS
ships. VM24 runs bash 5, where it is fine. **So the bug would have been invisible until
the first time someone set `WOA23_TLS=off`, and would then have looked like a TLS problem
rather than a shell-portability one.** Fixed with `${TLS_ARGS[@]+"${TLS_ARGS[@]}"}`, which
is correct on both.

**Both halves now have execution evidence, closed 2026-08-20 by `bash5A`**
(`Bash5-verification-result-bash5A.md`). `production_app.sh` had never run under bash 5,
so the fix was reasoned rather than observed there. It now parses under VM24's **bash
5.2.21**, its twelve refusal cases all fail closed with nothing started and no port bound,
and the two argv cases show the guarded expansion working in both directions — `--keyfile`
present with TLS on, **absent** with TLS off. Nothing was deployed and no cutover
conclusion follows; this closed one portability gap and nothing else.

## 3. Fail-closed, and the trade it makes

Every required value is refused by name when missing or empty. **This means a
misconfigured production launcher does not start.**

That is deliberate, and it is the weaker-looking half of a good trade: a launcher that
refuses is visible in `pm2 list` within seconds and is fixed by correcting the config; a
launcher that starts on a silently-defaulted value serves the wrong store or the wrong
port **and looks healthy doing it**. The campaign has already been bitten by the second
kind twice — `pm2A`'s placeholder `env`, and production's own missing store, which would
surface as an application error rather than a configuration one.

The rollback path (§7) is what makes a refusal recoverable, and it must be rehearsed
before the cutover rather than improvised during it.

## 4. B7 — a NEW blocker: the deployment has not been shown to have the dependencies

**Renumbered from B6 to B7**, 2026-08-20. This document introduced it as B6; the PI's
instruction of the same day assigns **B6** to the AVX2 masking risk (spec 012). The PI's
labelling wins, and the change is recorded rather than made silently — a blocker that
quietly changes number is a blocker that gets lost.

**This is raised here for the first time and is not yet verified.**

Production runs `/home/odbadmin/.pyenv/versions/py311/bin/gunicorn` against
`woa23_app:app`, whose dependencies are `dask`, `xarray` and `zarr`. **The candidate needs
`polars`, `orjson`, `fastapi` and `uvicorn`.** Whether pyenv's `py311` environment
contains them is **unknown** — this campaign has never run the candidate from it. Every
candidate run so far, `c1f`, `c2g`, `s2pB` and `pm2B`, used a `dev2026/.venv` built by
`uv sync`.

If `py311` lacks them, `production_app.sh` starts, gunicorn imports `api.app`, and the
service fails at import — after PM2 has reported it started.

**Two ways to resolve it, and the choice is the PI's:**

| option | what it means | cost |
|---|---|---|
| **install into `py311`** | production keeps one interpreter, shared with whatever else uses it | mutates a shared environment; a dependency conflict with the old app's `dask` stack is possible and would affect the running service |
| **deploy a venv, and point `WOA23_PYTHON` at it** | production runs from its own `.venv`, exactly as every validated run did | one more directory to deploy; **the runtime then matches what was validated**, which no other option can claim |

**I recommend the venv.** It is the configuration `pm2B` actually validated, it isolates
the cutover from the old app's dependency set so a rollback does not have to undo package
installs, and `WOA23_PYTHON` already exists to point at it.

**Executed 2026-08-20 (`B7-dependency-check-result.md`): all eight modules are present in
`py311`, at versions identical to the validated venv's** — `polars` 1.27.1, `orjson`
3.11.4, `fastapi` 0.115.12, `uvicorn` 0.34.1, plus `gunicorn`, `dask`, `xarray`, `zarr`.
**B7 is answered but NOT closed**: eight matching packages are not runtime equivalence (S1
was caught by exactly that — 12 pinned matched while 23 transitive did not), `py311` is
shared with three other projects, and the candidate has never actually been *run* from it.
The deployment-runtime decision is still open and still the PI's; my recommendation remains
the venv, as this section said it would be regardless of the result. It also makes B6
concrete: `polars` in `py311` is the mainline build, so the AVX2 warning would appear in
production too.

**The check that settled the factual half was a read-only one:**

```
/home/odbadmin/.pyenv/versions/py311/bin/python3.11 -c \
  "import importlib.util as u; print({m: bool(u.find_spec(m)) for m in ('polars','orjson','fastapi','uvicorn','gunicorn')})"
```

**B7 is a hard prerequisite, exactly like B2, B3 and B4.** No cutover can be authorised
before it is resolved.

## 5. Preconditions for a cutover request

Not one of these is satisfied by `pm2B`, and all must hold before a cutover is proposed:

1. **B7 resolved** — the runtime that will serve production is shown to have the
   dependencies (§4).
2. **B6, the polars CPU baseline — DECIDED 2026-08-20**, spec 012 rev 3. Mainline polars
   **1.27.1 retained**; `polars-lts-cpu` ruled out of this campaign. **This is no longer a
   precondition**: nothing changes, so no contract or performance evidence is invalidated
   and **no C1/C2 re-run follows**. The AVX2 masking is carried as **accepted residual
   risk** — which still means no absolute performance figure may be presented as
   production-representative, and B6 reopens on a hardware change, a polars upgrade, a
   SIGILL, an incorrect result, or a significant performance regression.
3. **The versioning and announcement decision** — spec 008 §9.3. 1.1.0 is published in the
   candidate's OpenAPI; consumers have not been told.
4. **A rehearsed rollback** (§7), tested where testing it is free.
5. **`conf/` installation authorised separately** from the cutover itself — writing the
   files and restarting the service are two acts, and the first is reviewable at rest.

## 6. The cutover, as currently designed

Not authorised, not scheduled, and written down so it can be reviewed rather than
improvised.

| # | step | stop condition |
|---|---|---|
| 1 | record production's full state — PIDs, starttimes, listeners, `pm2 jlist`, boot id | any later step's comparison needs this baseline |
| 2 | verify the deployed tree's identity against the authorised subject, file by file | any mismatch |
| 3 | resolve B7 — confirm the interpreter has the dependencies | anything missing |
| 4 | install the two files into `conf/`, **keeping the originals** under their own names | — |
| 5 | `pm2 restart woa23` under **production's** `PM2_HOME`, named app only | never `all`; never `pm2 kill` |
| 6 | health: process online, listener on 8050, `openapi.json` reports **1.1.0** | any failure → §7 |
| 7 | contract: a known query returns rows in `(time_period, depth, lat, lon)` order, JSON and CSV | any failure → §7 |
| 8 | compare against the step-1 baseline; confirm the old `woa23_app` is gone | an old worker still serving |
| 9 | watch for a defined period before declaring the cutover complete | any restart, any 5xx |

**Health checks are HTTPS**, since gunicorn terminates TLS: `https://127.0.0.1:8050/...`.
This is the first time production's own port is contacted in this campaign, and it may
only happen under an authorisation that says so explicitly.

## 7. Rollback

**Restoring two files and restarting is the whole of it**, which is the main argument for
this shape of change:

```
cp conf/start_app.sh.pre-cutover      conf/start_app.sh
cp conf/ecosystem.config.js.pre-cutover conf/ecosystem.config.js
PM2_HOME=/home/odbadmin/.pm2 pm2 restart woa23
```

Then the step-6/7 health checks again, expecting the **old** app: no `1.1.0`, no row-order
guarantee.

**Two properties make this real rather than aspirational.** The originals are kept as
files rather than recovered from git, so a rollback needs no network and no repository.
And nothing about the data is changed by the cutover — the store is read-only to the
application, so there is no migration to reverse.

**One caveat, stated rather than buried:** if B7 is resolved by installing packages into
`py311`, the rollback does **not** undo that. The old app would then be running against a
mutated dependency set. This is the second reason §4 recommends the venv.

## 8. Boundaries

**Done here:** the audit of production's two files, the proposed launcher and PM2 config,
59 offline assertions, the B1 third fault, the new blocker B7, the cutover sequence, and
the rollback.

**Not done, not proposed, not authorised:** no file in `conf/` changed; nothing installed;
no PM2 command run anywhere; no VM24 action; no request to production's ports; no
dependency installed; no cutover. **Spec 012 is an open decision and nothing here presumes
its outcome.**

**Needs its own authorisation, each separately:** the read-only B7 check (§4), installing
the files into `conf/` (§6 step 4), and the cutover (§6 steps 5–9).
