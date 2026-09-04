# 010 — PM2 alternate-port staging validation, and the road to a production cutover

**Status: revision 4. The alternate-port staging run this spec designs has been
authorised, executed and PASSED — `pm2B`, 2026-08-20, recorded in
`PM2-staging-result-pm2B.md`.** The PASS is bounded exactly as §4 states: the deployment
machinery works against a 72-file synthetic store. It is **not** a production deployment
PASS, **not** real-data correctness, and **not** a performance result. **No production
file has been changed and no cutover is authorised**; blockers B1–B5 (§5a) remain open and
are addressed offline by spec 011.

| rev | date | change |
|---|---|---|
| 4 | 2026-08-20 | **`pm2B` PASSED; two rules written in from its review.** New §5b: a venv runtime claim rests on four facts (argv, `exe`, an equality comparison against production's own `exe`, and mapped libraries), because the check `pm2B` was written with — `/proc/<pid>/exe` inside the venv — **is unsatisfiable for any correct venv**, `.venv/bin/python` being a symlink that `exe` resolves. New §5c: staging daemons are **retained by design** (`pm2 kill` is forbidden), so they are now registered, may never be reused, and the scoped single-`PM2_HOME` cleanup that would stop one is **designed here and left unexecuted** pending its own authorisation. Result: `PM2-staging-result-pm2B.md`. |
| 3 | 2026-08-20 | **The `pm2A` staging-configuration failure, and its fix** — new §2.4a, §2.2 rewritten. `pm2A` failed at start: PM2 merges a config's `env` block over the environment `pm2 start` is given and **the config wins**, so the block's placeholders replaced the values the command supplied — the process received an empty store and port **18221** instead of 18231. The store guard refused the empty store and stopped it **before a spent port could be bound**. Fixed by removing the `env` block entirely, making all three variables **required with no defaults**, refusing spent ports **from `ports_used.tsv` itself** rather than a hard-coded list, naming the interpreter instead of resolving `gunicorn` through `PATH`, and adding `deploy/verify_staging_env.sh`, which reads the values back from the **running process's `/proc/<pid>/environ`** — because in `pm2A` every value in the starting shell was right and every value in the process was wrong. §2.4a pins Python **3.11.4**, production's, after `pm2A` silently resolved 3.11.14. |
| 2 | 2026-08-19 | **Five corrections from PI review, and one correction of my own.** (1) The empty `WOA23_STAGING_STORE` placeholder may **not** be used at run time; the launcher now refuses empty, unreadable, anchorless and production-resident stores, each checked. (2) **`PM2_HOME` isolation is mandatory** and `pm2 delete all` / `restart all` / `stop all` / `kill` / global `save` are named forbidden (§2.5). (3) **18221 is already ALLOCATED, NEVER BOUND and is no longer first-use** — the execution request must carry a **new** identity (§2.4). (4) The candidate-app requirement and the ten checks are restated as the request's acceptance criteria (§3). (5) Production's `conf/` defects are promoted to **named cutover blockers** and explicitly **must not be fixed inside the staging run** (§5a). My own: an earlier comment claimed `${VAR:?}` does not catch an empty value — **it does**, the colon form fires on null too; the launcher uses an explicit check for a better message, not because `:?` would have missed it. |
| 1 | 2026-08-19 | First draft. Audit of `conf/`, the isolated staging launcher and PM2 config, the alternate-port validation checklist, and the separation of staging from production cutover. |

---

## 1. Why production's deployment files cannot be reused

Audited: `conf/ecosystem.config.js`, `conf/start_app.sh`, `conf/simu.sh`. **None can be
copied to staging with the numbers changed.** Each defect below is a reason the staging
launcher is a new file rather than an adaptation.

### 1.1 `conf/ecosystem.config.js` — the stop hook is a production-outage command

```
pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
```

It matches on a **command-line string**, not on a process this config started. Run beside
production it would find **production's own master and workers** — their argv contains
`woa23_app` — and `kill -9` them. It is also `kill -9`, which this campaign's cleanup
policy forbids outright, and it would defeat any graceful-shutdown budget.

Also hard-coded: `name: 'woa23'`, the log paths `tmp/woa23*.log`, and
`append_env_to_name: true`.

### 1.2 `conf/start_app.sh` — four independent blockers

```
gunicorn woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8050 \
  --keyfile conf/privkey.pem --certfile conf/fullchain.pem --timeout 120 --reload
```

| | why staging cannot use it |
|---|---|
| `woa23_app:app` | **the OLD app.** Staging the candidate and starting this instead would validate nothing and would look like a pass |
| `-b 127.0.0.1:8050` | **production's port** |
| `--reload` | live in production, a known defect, and no part of a validation |
| no `WOA23_ZARR_STORE` | `api.config` **requires** it at import; the candidate would fail to start, and the failure would be about the store rather than about the launcher |

### 1.3 `conf/simu.sh` — a developer scratchpad, not a deployment path

It starts Dask on 8786, binds 8050 with `--reload`, and carries three `grep | kill -9`
lines — one of which kills **`tide_app`**, a neighbouring service that has nothing to do
with WOA23. Nothing here is reusable.

### 1.4 The candidate needs no Dask at all

Checked, not assumed: **`dev2026/api/` imports no `dask` and no `distributed`** — the
only occurrences are comments explaining their removal. Production's `woa23_app.py:17`
installs a client at import; the candidate does not.

**So staging allocates no Dask scheduler or worker ports.** Reserving them would stage
something the candidate does not use. If a *reference* arm is ever staged beside the
candidate, that arm needs Dask and its own alternate ports, and that is a different
request.

## 2. The isolated staging design

Two new files under `dev2026/`, and **no change to anything under `conf/`**.

### 2.1 `dev2026/deploy/start_staging.sh`

- launches **`api.app:app`** — the candidate, named explicitly;
- **refuses 8050, 8786 and 8787 in the launcher itself**, with a non-zero exit. Not in a
  runbook: a runbook is a thing someone reads, this is a thing that stops;
- requires `WOA23_STAGING_PORT` and `WOA23_STAGING_STORE`, and **refuses to default
  either** — a guessed store would silently stage the wrong data;
- **requires all three variables** — `WOA23_STAGING_PORT`, `WOA23_STAGING_STORE`,
  `WOA23_PRODUCTION_STORE` — and **refuses a missing or empty value by name**, with no
  default for any of them. `WOA23_PRODUCTION_STORE` is required rather than optional
  because it is what arms the production-store guard, and a guard that silently does
  not run is worse than none;
- **refuses any port already in `scripts/ports_used.tsv`**, reading the ledger itself
  rather than a list kept in the launcher — so a port cannot be reused by forgetting to
  update a hard-coded exclusion. `18221` and `18231`, both spent by the `pm2A` failure,
  are refused by this rule without being named in the code;
- **refuses a non-numeric or out-of-range port**;
- **refuses a store that is not a directory, or not readable**;
- **refuses a store with no readable anchor group** (`<store>/1_degree/annual/TS/.zgroup`
  by default, overridable). `api.app`'s lifespan opens the anchor before the worker
  serves anything, so this turns "the app died at startup" into "the store you named has
  no anchor";
- **refuses a staging store that resolves inside `WOA23_PRODUCTION_STORE`** when that is
  set, comparing resolved physical paths so a symlink cannot slip past. A read-only
  *intent* is not a read-only *guarantee*, and staging must not be pointed at the tree
  production serves from;
- exports `WOA23_ZARR_STORE` from the staging store, which `api.config` requires;
- **no `--reload`, no TLS, no Dask**;
- **names the interpreter** — `exec "$PY" -m gunicorn`, `$PY` defaulting to the staging
  venv and overridable with `WOA23_STAGING_PYTHON`. `exec gunicorn` resolved through
  `PATH` and would run whichever gunicorn came first, which is not necessarily the
  pinned one in the verified tree;
- `exec`s, so PM2 tracks the gunicorn master rather than a wrapper shell;
- `--graceful-timeout 10`, matching the campaign's cleanup budget.

### 2.2 `dev2026/deploy/ecosystem.staging.config.js`

- `name: 'woa23-staging-candidate'` — **not** `woa23`, so no `pm2 stop|restart|delete`
  can reach production's app by mistake;
- **no `pre_stop` hook at all.** PM2 signals the process it started, by PID, and
  gunicorn's graceful timeout does the rest. **Nothing greps and nothing `kill -9`s**;
- `kill_timeout: 20000` — longer than the arm's 10 s graceful timeout, so PM2's SIGKILL
  fallback is not what normally stops it;
- own logs under `tmp-staging/`, sharing no path with production's `tmp/woa23*.log`;
- **`autorestart: false`**, unlike production: a staging crash must stay visible, and an
  autorestart would mask exactly the startup and lifespan failures this validation
  exists to observe;
- **NO `env` block at all**, and its absence is the fix `pm2A` forced. PM2 merges a
  config's `env` over the environment `pm2 start` was given and **the config wins**, so
  the block's placeholders — an empty store and port `18221` — replaced the correct
  values the command supplied. With no block, the starting environment passes through
  unchanged, and the three required variables must be supplied at `pm2 start`:
  `WOA23_STAGING_PORT`, `WOA23_STAGING_STORE`, `WOA23_PRODUCTION_STORE`. **Nothing is
  defaulted**: a default is what caused the failure.

### 2.2a Verifying the environment in the PROCESS, not the shell

`deploy/verify_staging_env.sh <pid> <port> <store> <production-store>` reads
`/proc/<pid>/environ` and checks the four values the running service actually has —
the three inputs plus the `WOA23_ZARR_STORE` the launcher exported — and fails if the
port is `18221` or `18231`.

**It exists because of how `pm2A` failed.** Every value in the starting shell was
correct and every value in the process was wrong. A shell variable is an intention;
`/proc/<pid>/environ` is what happened, and only the second is evidence.

`PROC_ROOT` is overridable so the logic is exercised offline, including against a fake
`/proc` reproducing the `pm2A` shape exactly — port `18221`, empty store — which the
checker must catch, name as a spent port, and answer with "stop and report rather than
restarting".

### 2.3 Offline verification of both

`scripts/test_staging_launcher.sh` — **57 assertions**, nothing started. It checks the
candidate app is launched and `woa23_app:app` never is; that the launcher **actually
exits non-zero** when given 8050, 8786 or 8787 (run, not read); that an absent store or
port is refused; that `--reload`, TLS and Dask are absent; that the PM2 config has no
`pre_stop`, no `kill -9` and no `ps`/`grep` matching; and that logs, name and restart
policy are its own. It runs the launcher against an **empty**, a **whitespace-only**, an
**anchorless** and a **production-resident** store and requires a non-zero exit and a
message naming the cause for each. It asserts the PM2 config documents `PM2_HOME`
isolation and names `pm2 delete all`, `restart all`, `stop all` and `kill` as
forbidden. And it asserts that **production's files really do** carry the defects
being designed out, so the comparison cannot rot into a claim about files that changed
underneath it.

*(The checks strip comments before matching. Both files explain at length why they do
not do what production does — naming `--reload`, `pre_stop` and `kill -9` in prose — and
the first version of the test failed five of its own checks on those comments.)*

### 2.4 Ports — and why 18221 is NOT the execution port

**18221 is already recorded in `scripts/ports_used.tsv` as `ALLOCATED, NEVER BOUND`.**
It was written into the ledger when this design was committed. That entry means the
number is **spent**: this campaign's rule is that a run gets ports no earlier run
allocated, and "allocated but never bound" is still allocated.

**So 18221 may no longer be described as first-use, and the execution request must
carry a NEW identity** — a new staging directory, a new workdir, a new label, and
**first-use ports** confirmed absent from the ledger at the time the request is written.
The `18221` in `ecosystem.staging.config.js` is a **default for the offline design**,
and the authorised run overrides it via `WOA23_STAGING_PORT`.

| | value |
|---|---|
| Dask scheduler / worker | **none allocated** (§1.4) — the candidate imports no Dask |
| production 8050 / 8786 / 8787 | **never touched**, and refused by the launcher itself |

### 2.4a Python runtime — production's 3.11.4, pinned

**Decision, 2026-08-20: staging uses production's interpreter,
`/home/odbadmin/.pyenv/versions/py311/bin/python3.11` (Python 3.11.4), named
explicitly.** The environment is created with

```
uv sync --python /home/odbadmin/.pyenv/versions/py311/bin/python3.11
```

and the resulting `python --version` is recorded in the run's evidence.

**Why it is pinned.** The `pm2A` attempt let `uv` choose, and it resolved **3.11.14**
while production runs **3.11.4**. Nothing failed because of it, and nothing here claims
it would have — but a deployment validation whose interpreter differs from production's
is validating a configuration production does not have, and the difference was
discovered by accident rather than declared.

**If a future run cannot use 3.11.4** — the pyenv build is gone, or `uv` refuses it —
the run may proceed on another interpreter **only if the report states, in the result
itself**:

> This run did **not** use production's Python 3.11.4. It is **not** a
> production-runtime validation, and no runtime-equivalence claim follows from it.

That sentence is the price of proceeding, and it is not omitted for brevity. A staging
pass on a different interpreter is a pass about **PM2, startup, readiness, ordering and
cleanup** — never about production's runtime.

`start_staging.sh` names the interpreter rather than inheriting it from `PATH`
(`exec "$PY" -m gunicorn`, `$PY` defaulting to the staging venv and overridable with
`WOA23_STAGING_PYTHON`), so whatever is pinned is what actually runs.

### 2.5 PM2 state isolation — mandatory, not advisory

**Every PM2 command in this validation sets `PM2_HOME` to a staging directory**, so the
staging app lives in its **own PM2 daemon and process list** and cannot appear in — or
be reached from — production's:

```
export PM2_HOME=~/woa23-staging-pm2
pm2 start   dev2026/deploy/ecosystem.staging.config.js
pm2 logs    woa23-staging-candidate
pm2 restart woa23-staging-candidate
pm2 stop    woa23-staging-candidate
pm2 delete  woa23-staging-candidate
```

**Only the named staging app is ever addressed.**

**FORBIDDEN, with or without `PM2_HOME` set:**

| command | why |
|---|---|
| `pm2 delete all` | operates on **every** app in whichever daemon is addressed |
| `pm2 restart all` | same |
| `pm2 stop all` | same |
| `pm2 kill` | kills the daemon itself |
| global `pm2 save` / `pm2 resurrect` | rewrites a saved process list that production may depend on |

The hazard is not that these are obviously wrong — it is that **a single missing
`export` makes the daemon they address production's.** Naming the app explicitly is
what removes the dependence on an environment variable being right.

## 3. What alternate-port staging must validate

Nothing here has been run. Each item is a check the authorised run would make.

| # | check |
|---|---|
| 1 | PM2 reports the app **online** under `woa23-staging-candidate` |
| 1a | the running process is the **CANDIDATE `api.app`** — verified from the process's own `/proc/<pid>/cmdline`, not from the config that was supposed to start it. **`woa23_app` must appear nowhere** in the staging tree's argv |
| 2 | **startup / import / lifespan** validation — the candidate's anchor-group check runs and the worker becomes ready, or fails visibly (`autorestart: false` keeps a crash a crash) |
| 3 | **readiness** on the alternate port |
| 4 | **OpenAPI JSON `info.version` = `1.1.0`** at `/api/swagger/woa23/openapi.json` |
| 5 | the **row-order statement is present** in the OpenAPI description and both endpoint descriptions |
| 6 | basic **JSON and CSV** requests succeed |
| 7 | the candidate's rows conform to **`(time_period, depth, lat, lon)`**, numerically — the same check `contract_diff.row_order_contract` applies |
| 8 | **`pm2 restart`** — the app returns to online and still passes 3–7 |
| 9 | **stop** — process gone, port 18221 free, logs and PM2 state cleanly finalised, **no `kill -9` used** |
| 10 | **production unchanged** — master PID, starttime, listeners on 8050/8786/8787, and boot id all identical before and after, read from `/proc` and `ss` only |

| 11 | **PM2 state isolation held** — the staging app appears in the staging `PM2_HOME`'s list and **not** in production's, and no `all`-scoped or daemon-scoped command was issued |
| 12 | the **store** used was the one named, was readable, carried the anchor, and **was not inside the production store** — the launcher's own refusals, evidenced by the run reaching the point of serving |

**Item 10 is checked before and after**, not only after: "unchanged" needs both readings
from the same run, and no value from an earlier run substitutes for either.

**Items 1a and 11 exist because the two most damaging failures here would both look
like success.** Starting `woa23_app` instead of the candidate would produce a healthy
service that validates nothing; touching production's PM2 daemon would produce a
staging app that appears to work while having moved something it must not.

## 4. What alternate-port staging does NOT establish

- **not TLS**, and not the reverse proxy. Staging is plain loopback; production
  terminates TLS with real certificates behind nginx. **This is a stated gap**, and it
  belongs to §5;
- **not production's launcher.** Staging runs the new launcher deliberately, so it says
  nothing about `conf/start_app.sh`, whose `--reload` remains a live production defect;
- **not performance.** No latency, no throughput, no startup timing;
- **not a cutover rehearsal.** No production file, port, process or PM2 app is touched.

## 5. Production cutover — separate, and later

**Not requested here, and not authorisable by this document.** It may only be proposed
**after** an alternate-port staging validation has passed, and it must record:

- **backup** — of the deployed tree, `conf/`, and the PM2 process list, taken before any
  change;
- **rollback** — the exact steps and their preconditions, tested in the plan rather than
  improvised on the day;
- **health checks** — what is polled, from where, and what result aborts the cutover;
- **TLS and reverse proxy** — certificates, the nginx path, and the cache policy for a
  dataset that never changes;
- **production listener checks** — before, during and after;
- the **versioning and announcement** decision (spec 008 §9.3), which is a precondition;
- **blockers B1–B5 of §5a**, each resolved by an authorised production deployment
  change. B2, B3 and B4 are hard prerequisites: production's launcher must name
  `api.app`, take a port, and set `WOA23_ZARR_STORE` before it can serve the candidate
  at all.

**An alternate-port staging PASS does not substitute for cutover validation.** Different
launcher, different port, no TLS, no proxy, no production process: it establishes that
the candidate runs under PM2 in isolation, and that is all.

## 5a. Production cutover BLOCKERS found in `conf/` — not fixed here

**These are defects in production's own deployment configuration.** They are recorded as
**named cutover blockers**, and they must be resolved by a **separate production
deployment change with its own authorisation**.

| # | blocker | file |
|---|---|---|
| **B1** | `pre_stop` is `ps -ef \| grep -w 'woa23_app' \| … \| xargs -r kill -9` — it matches a **command-line string**, so beside production it finds **production's own master and workers** and `kill -9`s them. It is also `kill -9`, which this campaign's cleanup policy forbids | `conf/ecosystem.config.js` |
| **B2** | launches the **old `woa23_app:app`** — after cutover this would keep serving the old app whatever else changed | `conf/start_app.sh` |
| **B3** | **hard-coded `-b 127.0.0.1:8050`**, so the port cannot be varied without editing production's launcher | `conf/start_app.sh` |
| **B4** | **no `WOA23_ZARR_STORE`** — `api.config` requires it at import, so the candidate cannot start from this launcher at all | `conf/start_app.sh` |
| **B5** | **`--reload` live in production** — a long-standing defect, recorded in the roadmap before this spec | `conf/start_app.sh` |

**None of these is fixed in the staging run, and none may be.** The staging validation
runs `dev2026/deploy/start_staging.sh`, which is a **new file**; `conf/` is not touched.
Fixing a production launcher **inside** a staging execution would be an unauthorised
production change wearing a validation's clothes — and it would also destroy the thing
being validated, since the run would no longer be testing the configuration production
actually has.

**Consequence for cutover:** B2, B3 and B4 are **hard prerequisites** — production
cannot serve the candidate until its launcher names `api.app`, takes a port, and sets
the store. B1 and B5 are defects that should be fixed in the same change, and B1 in
particular must be fixed **before** any PM2-driven cutover, because a cutover that
invokes production's stop path invokes `kill -9` against a grep.

## 5b. How a venv runtime is verified — the rule, and why the obvious check is wrong

**`readlink /proc/<pid>/exe` pointing at the base interpreter is NOT a venv failure.**

The `pm2B` request required `/proc/<pid>/exe` to resolve inside
`~/woa23-pm2b/dev2026/.venv`. **No correct venv can satisfy that.** `.venv/bin/python` is
a symlink to the interpreter the venv was created from, and `/proc/<pid>/exe` always
resolves to the real inode — so a healthy venv necessarily reports the base path. A check
written that way fails on every correct configuration and would only "pass" if someone
copied a real binary into the venv, which is the rarer and stranger arrangement.

**The rule from now on.** A venv runtime claim is carried by four facts, none sufficient
alone:

| # | fact | read from | what it establishes |
|---|---|---|---|
| 1 | `argv[0]` is `<staging>/.venv/bin/python` | `/proc/<pid>/cmdline` | the venv's launcher was the thing invoked |
| 2 | `/proc/<pid>/exe` resolves to the expected base interpreter | `/proc/<pid>/exe` | which interpreter is actually executing |
| 3 | that path **equals production's** `/proc/<prod-pid>/exe` | both, compared | runtime equivalence with production, as a comparison rather than an assertion about a version string |
| 4 | mapped libraries come from the venv and **none** from production | `/proc/<worker>/maps` | the venv supplies `site-packages`, so the code being run is the staged code |

Fact 4 is the one that would actually catch a wrong environment — a process can be started
by a venv's python and still import from elsewhere if `PYTHONPATH` or a `.pth` interfered.
In `pm2B` it read **416 mapped regions** under the staging venv and **0** under any
production path.

A version string on its own (`python --version` in the starting shell) establishes none of
these: it describes the shell's interpreter, not the running process's — the same
distinction that made `pm2A` look correct from outside while every value inside the
process was wrong.

## 5c. Retained staging daemons — a registry, and why they are not left to drift

A staging run's PM2 daemon **outlives the run by design**: `pm2 kill` is forbidden, so
after `stop` and `delete` the God Daemon for that `PM2_HOME` stays up with 0 apps. It is
part of the run's retained state — its `pm2.log` records the start, the restart and the
stop — and deleting it would destroy evidence.

**But retained is not forgotten.** Each one is recorded here, and none may be reused by a
later run, restarted, or left indefinitely.

| daemon PID | `PM2_HOME` | run | apps | status |
|---|---|---|---|---|
| 1242814 | `~/woa23-pm2a-pm2` | `pm2A` (failed at start) | 0 | **retained** — failure evidence |
| 1248938 | `~/woa23-pm2b-pm2` | `pm2B` (PASS) | 0 | **retained** — run evidence |
| 3459 | `/home/odbadmin/.pm2` | **production** | 9 | not ours; never touched |

**Reuse is forbidden.** A later staging run takes a new `PM2_HOME`, as it takes a new
port, staging tree and label. Starting an app under a retained daemon would mix two runs'
state in one `dump.pm2` and one log.

### The cleanup action, designed here and NOT executed

Stopping a retained daemon needs **its own authorisation** and must be scoped to one
`PM2_HOME`. The safe shape:

```
PM2_HOME=~/woa23-pm2b-pm2 pm2 ping          # confirm which daemon answers
PM2_HOME=~/woa23-pm2b-pm2 pm2 jlist         # MUST be 0 apps before anything
PM2_HOME=~/woa23-pm2b-pm2 pm2 kill          # scoped: kills THAT daemon only
```

Four properties make it safe, and all four are required:

1. **`PM2_HOME` is set on every command**, so `pm2` never falls back to `~/.pm2` —
   production's. A single command without it is a production action.
2. **The daemon is identified before it is stopped** — `pm2 ping` and the PID from
   `pm2.pid` are checked against the expected PID, so a recycled PID cannot be hit.
3. **0 apps is a precondition**, not an expectation: a daemon still holding an app means
   a run did not clean up, and that is a `CLEANUP_FAIL` to investigate rather than kill.
4. **Logs and `pm2.log` are preserved** — the daemon is stopped, its directory is not
   deleted. Evidence survives the cleanup.

**`pm2 kill` here is not the forbidden `pm2 kill`.** The prohibition is against issuing it
in a context that reaches production's daemon; with `PM2_HOME` naming a staging home and 0
apps confirmed, it stops exactly that daemon. This distinction is the reason the action
needs authorisation rather than being folded into a run's cleanup.

## 6. Boundaries

**Done offline:** the audit, the two staging files, the 42-assertion check, and this
document.

**Not done, not proposed, not authorised:** starting PM2 anywhere, any VM24 action, any
HTTP request, any production change, any change to `conf/`, any deployment, any
`apiverse` work, any C1/C2 re-run, and any push. **No `conf/` file is modified by this
spec or by the staging run it designs** (§5a).

**Needs its own authorisation, each separately:** the alternate-port staging run (§3),
and — only after it passes — the production cutover (§5).
