# 013 — B1–B5 host validation: matrix, ownership audit, and the evidence boundary

**Status: OFFLINE AUDIT. Nothing here is authorised, requested or started.**
No VM24 contact. Every host action below is a *proposal* and would need its own request,
review and explicit authorisation.

**C1/C2 correctness is complete** (`c1r`, `c2k`) and **is not reopened**: no C1/C2 rerun,
no latency work, no rung-60 performance work. **B6 is decided** — mainline
`polars` 1.27.1 retained, AVX2 masking carried as accepted residual risk.

---

## 0. Two corrections before anything else

**The "121 offline assertions" figure is stale.** It was `test_production_launcher.sh`
88 + `test_production_stop.sh` 33. The launcher suite has since grown:

| suite | spec 011 records | actual now |
|---|--:|--:|
| `scripts/test_production_launcher.sh` | 88 | **111** |
| `scripts/test_production_stop.sh` | 33 | **33** |
| **total** | 121 | **144** |

**And the number is beside the point.** 144 offline assertions are **not host
validation**, and this document does not treat them as such. They establish that the
scripts behave as designed *against stubs and a synthetic `/proc`*. Not one of them has
observed the production host. That distinction is the subject of §3.

---

## 1. The B1–B5 matrix

### Source files and current hashes

| file | sha256 |
|---|---|
| `deploy/production_app.sh` | `07bc37642d9c0f82ed709c91f973170b85b7df7ab98ef6058eb49eea3e71295a` |
| `deploy/production_stop.sh` | `e86f07f18b38ee4b470e7049d15c5363cfe9c3f8847e2602aa0096c1e6b7803f` |
| `deploy/ecosystem.production.config.js` | `a7e4cb7fcb47e83b8192b225093e5dbe20aad2c673e5fed511b2328ee22410c4` |
| `deploy/ecosystem.staging.config.js` | `47cf48cfc8125ef0cec9355906493493a53700e6a058b175303406301d71457a` |
| `deploy/start_staging.sh` | `ab256716c1a919b6322425db3ddba36131d0756bb3ac196e541ab25bd9c4ccd6` |
| `deploy/staging_execute.sh` | `bd505a4d23a4e434f2733f10da970d111d400cb4326105390921251348f154f3` |
| `deploy/verify_staging_env.sh` | `e42c341509fc5d22c634d7947a505bec4df66cf2de18f1e3cddc1e9d31aac5cf` |
| `deploy/make_staging_store.py` | `cf121f7f41e15cd9a381d461772e2bc4a8b58281f7ba341baef14e9d3d5f69f1` |
| `deploy/make_staging_override.js` | `143ffa6381750789e33cdea1004c680bd1e8dc83b49267da2d77d903317ccd57` |
| `deploy/record_manifest.py` | `8e7a0aaba21ea7634033318297584bf9e756b765603a8626d934cafd7a91cd34` |
| `scripts/test_production_launcher.sh` | `7397094324e2fc9ddbb000972b37ccadcfb990fe5cd8f044df7d73a5ca23aa08` |
| `scripts/test_production_stop.sh` | `12ccbb11df7c2f9062c0d6a43f77a4177e924bb93d6a550b029357f9b7c331dd` |

Production's current files, by hash recorded in the `c1r`/`c2j`/`c2k` pre-flights
(read-only, never modified):

```
8db9a6ba1888821c833452ee9602871045e4017500800ae69b00aba9fdde4340  conf/ecosystem.config.js
4aaed5b7c75e624f82cc30cc7e540428547103830d6231e537ff756973376d77  conf/start_app.sh
```

### The matrix

| | **B1** stop identity | **B2** `api.app:app` | **B3** port/config separation | **B4** store config | **B5** no `--reload` |
|---|---|---|---|---|---|
| **What it fixes** | `pre_stop` greps `woa23_app` and `kill -9`s matches | launcher serves `woa23_app:app` | hard-coded `-b 127.0.0.1:8050` | no `WOA23_ZARR_STORE`; store fault reads as a restart loop | `--reload` live in production |
| **Resolution** | `pre_stop` **deleted**, not rewritten; launcher `exec`s so PM2 tracks the master; `production_stop.sh` is the operator's stop-and-verify path | `APP="api.app:app"` fixed in the launcher, asserted in argv | `WOA23_PORT` **required, no default**; no port literal in the file | required **and validated**: directory, readable, anchor `.zgroup` present | absent, and asserted absent from argv |
| **Validatable WITHOUT touching production** | **Partly.** Behaviour against a **staging** PM2_HOME owned by `woa23c1ro`: identity capture, graceful stop, survivor fail-closed, no SIGKILL, decoy untouched. | **Partly.** Staging launcher argv shows `api.app:app`. | **Yes.** Staging run on a first-use port proves no 8050 literal is reachable. | **Yes.** Staging store, plus fail-closed refusals on a bad path. | **Yes.** Staging argv shows no `--reload`. |
| **Required host state** | a **new** staging `PM2_HOME` owned by uid 994; first-use port; synthetic store | same | same | same, plus a deliberately invalid store path for the refusal case | same |
| **Expected evidence** | recorded `(pid, starttime)` tree; `pm2 stop` only, named app, never `all`; tree gone within grace; survivors → `CLEANUP_FAIL` exit non-zero; decoy process alive | argv containing `api.app:app`, and **not** `woa23_app` | argv containing the first-use port; `grep -c 8050` = 0 in the file | startup refusal messages naming the store fault; successful start with a valid store | `grep -c reload` = 0 in argv |
| **Possible production side effects** | **None if and only if** `WOA23_PM2_HOME` is the staging one. Pointing it at `/home/odbadmin/.pm2` would stop the real service. | none | none | none | none |
| **Stop conditions** | any `PM2_HOME` other than the authorised staging one; `APP` = `all`; any SIGKILL in the path; decoy touched; production pid/starttime changed | production pid/starttime changed | port ≠ authorised first-use port | store path outside the authorised synthetic tree | any `--reload` present |
| **Read-only / staging / cutover** | **staging-only** for behaviour; **CUTOVER** for "production stops correctly" | **staging-only**; **CUTOVER** for "production serves the candidate" | **staging-only**; **CUTOVER** for production's real port | **staging-only**; **CUTOVER** for the real store | **staging-only**; **CUTOVER** for production's own argv |

**Every one of B1–B5 is staging-only offline, and every one has a cutover remainder.**
That is the honest reading and §3 states it precisely.

---

## 2. Ownership and identity audit

Production is owned by **`odbadmin` (uid 1000)**. The validation account is
**`woa23c1ro` (uid 994, gid 993)**, which owns nothing of production's.

### 2.1 The finding that decides B1's scope

`deploy/production_stop.sh` resolves the target PID from **PM2's own record**:

```
JLIST="$("$PM2" jlist 2>/dev/null)" || die "cannot read pm2 jlist under $PM2_HOME"
```

`WOA23_PM2_HOME` is **required and never defaulted** — good — but production's is
`/home/odbadmin/.pm2`, and `pm2 jlist` needs to reach the PM2 **daemon** owned by
`odbadmin`. **uid 994 cannot.**

**Therefore: B1's behaviour against production's PM2 is not validatable by `woa23c1ro`
at all.** It is validatable only against a staging `PM2_HOME` that `woa23c1ro` owns.
Proving "the real service stops correctly" requires a **cutover**, by definition, and no
staging run can substitute for it. This is a boundary, not a gap to be worked around.

This is the campaign's recurring defect class — a harness assuming it runs as the account
owning what it inspects — caught **before** a request rather than by a refused run, which
is what the C1 sequence (`c1k`, `c1m`, `c1n`, `c1p`) cost us.

### 2.2 `ss -p` — not relied on anywhere

Audited across `deploy/` and both launcher suites. The **only** `ss` use is:

```
deploy/staging_execute.sh:331:  [ "$(ss -ltn 2>/dev/null | grep -c ":$PORT ")" -eq 0 ]
```

`ss -ltn` — **listener presence only**, no `-p`, no ownership attribution. Correct for
uid 994: `ss -p` shows a socket's owner only to that owner or root. **No check in B1–B5
depends on `ss -p` visibility.** This must stay true; any future check that needs
socket→PID attribution must take explicit `--prod-pids` and validate via `/proc`, as
`run_controlled.sh` does.

### 2.3 Production PID/starttime identity — handled explicitly

`production_stop.sh` defines identity as **`(pid, starttime)`**, starttime being field 22
of `/proc/<pid>/stat`, and refuses to act on a pid it cannot read:

```
MASTER_START="$(starttime_of "$PID")" \
  || die "cannot read $PROC/$PID/stat — refusing to act on a pid I cannot identify."
```

`/proc/<pid>/stat` and `/cmdline` are world-readable, so this works as uid 994.
`/proc/<pid>/exe` is **not** and must continue to be recorded as `exe_not_readable`,
never as verified. Children come from `/proc`, never from `ps` — correct, since `ps`
output is a text match, which is precisely B1's original defect.

**Every proposed run must additionally record production's own `(pid, starttime)` and
boot id before and after**, exactly as `c1r`/`c2k` did, so a production restart during a
staging run is detected rather than assumed away.

### 2.4 pm2G — running, and not to be touched

`pm2G` remains running on **18265** (pids 1456369, 1456373, 1456374), confirmed in the
`c2k` pre-flight and post-state. For every run below:

- **not stopped, not deleted, not cleaned**;
- **not inspected destructively** — no `pm2 delete`, no `pm2 flush`, no writes to its
  `PM2_HOME`;
- its `PM2_HOME`, port, app entry, retained state and identity are **never reused**;
- presence is checked with `ss -ltn` and world-readable `/proc` only.

A new staging identity means a **new** `PM2_HOME` path, a **new** app name, and
**first-use** ports screened both ways — the two-way screen that has already rejected
19109, 19113, 19116, 19118 and 19136.

---

## 3. What staging can establish, and what it cannot

**A synthetic-store, alternate-port staging run does NOT close a production blocker.**
It closes a *behavioural* question about the script. Stated per blocker:

| blocker | staging CAN establish | CUTOVER evidence still REQUIRED |
|---|---|---|
| **B1** | the stop path records `(pid, starttime)`, sends no SIGKILL, waits the grace, fails closed on survivors, never touches a decoy, and refuses `all` | that **production's** service stops correctly under it; that PM2's signal reaches the real master; that the deleted `pre_stop` is genuinely unnecessary for the real tree |
| **B2** | the launcher's argv names `api.app:app` | that **production** serves the candidate — and that the candidate behaves correctly under production's real store, worker count and traffic |
| **B3** | no `8050` literal is reachable; the port comes from `WOA23_PORT` | that production's **real** port and config are separated in the installed files, and that nginx/TLS in front still resolve |
| **B4** | the store is required and validated; a bad path is a refusal, not a restart loop | that the **real** 123 005-file store passes that validation, and that a real store fault behaves as designed |
| **B5** | `--reload` is absent from the argv | that production's **installed** launcher has no `--reload` — the defect is in `conf/start_app.sh`, which staging never touches |

**The common remainder:** staging uses a **72-file synthetic store**, an alternate port,
no TLS and no proxy. Production has a 35 GB store, TLS terminated in gunicorn, nginx in
front, and live callers. **None of those is exercised by any staging run**, and no number
of staging passes changes that.

### 3.1 Naming the trap

The tempting sentence is *"B5 is closed: the launcher has no `--reload`."* It is false.
The blocker is that **production is running `--reload` right now**, in
`conf/start_app.sh` (`4aaed5b7…`). A file that is not installed cannot close it. Every
blocker here has that shape, and this document refuses that framing for all five.

---

## 4. B7 and the runtime decision — in the dependency map

**B7 is ANSWERED but NOT CLOSED, and this run does not close it.**

`B7-dependency-check-result.md` (2026-08-20, read-only) found all eight modules present
in `py311` at versions identical to the validated venv's — `polars` 1.27.1, `orjson`
3.11.4, `fastapi` 0.115.12, `uvicorn` 0.34.1, plus `gunicorn`, `dask`, `xarray`, `zarr`.

**Eight matching packages are not runtime equivalence.** S1 was caught by exactly that:
12 pinned matched while 23 transitive did not.

### 4.1 The runtime gap this creates, stated plainly

| | interpreter |
|---|---|
| **What C1/C2 actually validated** (`c1r`, `c2k`) | the **shared production Python**, `/home/odbadmin/.pyenv/versions/py311/bin/python3.11`, passed as `--python-binary`/`--prod-python` |
| **What the deployment plan may require** | an **isolated venv**, with `WOA23_PYTHON` pointed at it — spec 011 §4's recommendation |

**These are not the same runtime.** If the deployment ships a venv, then the runtime that
serves production is one **neither `c1r` nor `c2k` exercised**, and the correctness
evidence does not transfer to it unaltered. If the deployment uses `py311`, the runtime
matches what was validated, but `py311` is **shared with three other projects** and the
candidate has never actually been *run* from it under PM2.

**Either way B7 stays open**, and it must not be claimed closed until **the actual
deployment path uses and verifies the selected runtime**. The decision is the PI's; spec
011's recommendation was the venv and is unchanged, but the venv is the option that
*costs* a runtime-equivalence argument, and that cost belongs in the decision.

### 4.2 Dependency map

```
B6 (polars 1.27.1, DECIDED)  ──> no C1/C2 invalidation; residual risk accepted
                                  └─ makes the AVX2 warning production-visible

B7 (runtime)  ──> gates every cutover
   ├─ option py311 : matches what c1r/c2k validated; shared with 3 projects; never run under PM2
   └─ option venv  : matches what pm2B validated; NOT the runtime c1r/c2k used
                     └─ would need its own runtime-equivalence evidence

B1..B5 (launcher) ──> staging-validatable in behaviour only
                      └─ every one has a cutover remainder (§3)

cutover ──> requires B7 resolved, versioning/announcement decision (spec 008 §9.3),
            a rehearsed rollback, and conf/ installation authorised SEPARATELY
```

---

## 5. Proposed sequence — one blocker per authorised run

Narrow, and in this order, because each later one benefits from the earlier's evidence:

1. **B5 + B3** *(proposed first)* — the two that are pure argv assertions on a staging
   run, cheapest and lowest risk, and they exercise the staging harness end-to-end before
   anything harder depends on it.
2. **B2** — argv plus a real candidate import under PM2 on the staging store.
3. **B4** — needs a deliberately invalid store path, so it is the first that tests a
   refusal rather than a success.
4. **B1** — last, because it is the only one that stops a service, and because its
   production half is unreachable by uid 994 (§2.1). Staging-only, against a staging
   `PM2_HOME`.

**One blocker per authorised VM24 run. No run bundles two.**

The first request will cover **B5 + B3 only** — they share one staging start and
separating them would mean two identical runs for two `grep`s on the same argv. If you
prefer strictly one blocker per run, say so and I will split them.

---

## 6. What I will NOT do

- Treat 144 offline assertions as host validation.
- Claim any staging run closes a production blocker.
- Reuse `pm2G`, its `PM2_HOME`, port 18265, its app entry, retained state, or any
  previous staging identity.
- Clean, stop, delete or destructively inspect `pm2G`.
- Rely on `ss -p` ownership visibility as uid 994.
- Touch `conf/start_app.sh`, `conf/ecosystem.config.js`, or production's PM2 entry.
- Rerun C1/C2, run latency work, or start rung-60 performance work.
- Contact VM24 or execute anything before a reviewed, explicitly authorised request.

---

## 7. Next step

Submit the **first narrowly scoped host validation request (B5 + B3, staging-only)**,
with a new subject, exact digests, a fresh identity and first-use ports, an explicit
grant, a production-impact statement and failure handling — for review and explicit
authorisation. Nothing runs before that.
