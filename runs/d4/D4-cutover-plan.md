# D-4 — production cutover plan for subject `143bf8c`

**OFFLINE PLAN ONLY. Nothing here has been executed.** VM24 was not contacted while writing
it, production was not modified, production PM2 was not started, stopped or reloaded, C1/C2
were not re-run, no performance test was run, and no retained state was cleaned.

**No claim is made of production equivalence, cutover success, TLS validation or
new-runtime validation.** This document is a proposal awaiting review and authorization.

**`143bf8c` is the FIRST deployment of the response fix.** The CSV empty-result behaviour it
changes has never run in production, and the offline gate G1 (§17) is the only evidence that
it behaves as specified — offline, on 3.11.14, against a synthetic store.

**The read-only preflight has now been performed — see
[`D4-preflight-result.md`](D4-preflight-result.md).** PM2 measured and PASSED; **P-TLS was
BLOCKED** on two independent grounds (§8.4). The preflight also invalidated three premises
this plan was written on: `pre_stop` is not present in production today, PM2 tracks a shell
wrapper rather than the gunicorn master, and production runs with `--reload`. Those are
recorded in §§3, 6 and 8.4 rather than left standing.

**The read-only TLS topology audit has been performed — see
[`D4-tls-topology-audit.md`](D4-tls-topology-audit.md).** It found a hybrid architecture:
nginx terminates public TLS with a valid certificate and then re-encrypts to the application
over an unverified loopback TLS hop that uses an expired certificate.

**THE OWNER DECISIONS ARE NOW CONFIRMED, and this plan is updated to them (§8.10):**

| decision | value |
|---|---|
| official production hostname | **`eco.odb.ntu.edu.tw`** |
| architecture | **A-move** — nginx remains the public TLS terminator; the application runs **TLS-off**; nginx proxies over **loopback HTTP** |
| the active upstream change | `proxy_pass https://woa23api;` -> `proxy_pass http://woa23api;` |

**This authorises an OFFLINE PLAN UPDATE ONLY.** VM24 was not contacted while writing this
revision, no nginx or TLS file was modified, production was not stopped, started or reloaded,
no API request was issued, and no cutover was performed.

**The proposed cutover artifact remains `143bf8c`.**

---

## 0A. What this cutover actually changes — more than the CSV response

**Stated plainly, because "a response fix" understates it.** Measured against production:

| # | change | today | proposed |
|---|---|---|---|
| 1 | **app module** | `woa23_app:app` | `api.app:app` |
| 2 | **launcher / process shape** | PM2 tracks `bash conf/start_app.sh`; gunicorn master is its **child** | `production_app.sh` **`exec`s**, so PM2 tracks the master directly |
| 3 | **runtime environment** | **no `WOA23_*` variables at all** on the running master | an explicit `env` block: port, store, TLS paths, workers, `WOA23_PYTHON` |
| 4 | **PM2 definition** | `pm_exec_path=conf/start_app.sh`, `fork_mode`, `bash`, `append_env_to_name=true`, `kill_timeout=null` | new config, `append_env_to_name=false`, `kill_timeout=20000` |
| 5 | **Python / venv** | shared pyenv **3.11.4** interpreter, **no venv** | **standalone uv-managed CPython 3.11.14** at `$UV_PYTHON_ROOT` plus its own venv at `$VENV`, with `/home/odbadmin/.pyenv` excluded from the serving process entirely (§4.0). **The interpreter patch level changes, 3.11.4 -> 3.11.14** |
| 6 | **`--reload`** | **present** | **absent** |
| 7 | **application TLS** | the app terminates TLS on 8050 with an **expired** certificate and a **644** key | **`WOA23_TLS=off`** — the app terminates no TLS and loads **neither** certificate nor key |
| 8 | **the nginx->app hop** | `proxy_pass https://woa23api` — encrypted, **unverified** | `proxy_pass http://woa23api` — **plaintext on loopback** |
| 9 | CSV empty-result response | 400 with a JSON error body | 200 with a canonical header-only CSV |

**Only item 9 is the response fix.** Items 1–8 are deployment-shape and topology changes that
ride along with it, and each is a way the cutover can fail independently of the CSV
behaviour. They must be authorised as part of the change, not treated as incidental.

**Item 8 is a change to `/etc/nginx`** — a different file tree, a different privilege level
and a different operator from every other item here. It is therefore carried as a
**separately authorised, separately gated operation** with its own verification and its own
rollback: **§10A**.

**The prior D-3 deployment evidence belongs to `a361f70` and does not validate `143bf8c`** —
see §0.

---

## 0. The evidence boundary — stated first because it is the easiest thing to get wrong

**The D-3 deployment observation for subject `a361f70` is NOT deployment evidence for
`143bf8c`, and must not be back-filled as such.**

| | |
|---|---|
| what D-3 observed | subject **`a361f70`**, archive `92e93bd3…`, 261 files |
| what is proposed here | subject **`143bf8c`**, archive `0873a970…`, 262 files |
| relationship | `143bf8c` is `a361f70` **plus two API behaviour changes** — the CSV empty-result status and the empty-frame value columns |

D-3 ran a *different artefact*. It showed that the deployment machinery starts a candidate
of that subject against the real store, and it recorded the JSON/CSV divergence that
`143bf8c` exists to fix. **It did not run `143bf8c`, so it says nothing about how `143bf8c`
behaves under PM2, against the real store, or at all outside the offline suite.** Nothing in
§13's smoke checks may be replaced by citing D-3.

**What `143bf8c` has behind it today is offline evidence only:** `bench/test_csv_empty_result.py`
— 60 assertions, three clean repeated runs on a frozen HEAD — plus the unchanged contract,
row-order, column-order and clone-integrity suites. No PM2 run, no real store, no TLS.

---

## 1. Artifact provenance and transfer verification

**SETTLED: the cutover artifact is `f66ddd8`** (R1 adopted). It is provisioned on VM24 at
`$APP_ROOT=/home/odbadmin/python/woa23-f66ddd8` — archive, member/file counts, file-list,
delivery-member digests, modes and a fresh venv all verified; see
[`D4-f66ddd8-provisioning.md`](D4-f66ddd8-provisioning.md).
**The `143bf8c` provisioning is SUPERSEDED and is not deployment evidence for `f66ddd8`.**
Both provenance blocks are kept below because the distinction is the point.

```
subject   143bf8caae4aaa4cd4d4ef9ec0ddcab9ada1174d          PROVISIONED on VM24
archive   0873a9708a992aaad373ec4ec4999b631deae30a5b0787d72d11dfbbf5c58d17
members   273 tar members
files     262 regular files
file-list c436362ae4918d67e59c61b1a4d3328160d877d5da41f770bbb99aba78d74497

subject   f66ddd8cd18840213b086a03dba4545b0da8ad44          VALIDATED, 3 clean batches
archive   2bea7db91bcf00270abe0c73beff33ab66692ee6749e57483eb165ef6373de90
members   276 tar members
files     265 regular files
file-list ca461166312a61a3d338edf6211fbf6604facac3009a2c546ff54bee6e8ea169
```

**`f66ddd8` differs from `143bf8c` in EIGHT files, all under `dev2026/scripts/` — harness,
test and validation tooling only.** `api/`, `deploy/`, `bench/`, `uv.lock` and
`pyproject.toml` are **byte-identical**, proven by identical git object ids. The served
application, the delivery files and the dependency set do not change between them.

**But the identities differ, and evidence follows identity.** `f66ddd8` carries the three
clean batches and the sentinel; the tree extracted into `$APP_ROOT` on VM24 is `143bf8c`'s.
**Citing one's validation for the other would mix runtime evidence across subjects.** The
choice, and what each choice costs, is §3 of the reconciliation. **It is not made here.**

**Members and files are different counts and both are recorded.** Comparing one against the
other produced a spurious "wrong archive" finding during D-3; it will not be available to do
so again.

| step | check | expected |
|---|---|---|
| 1 | `git archive <subject> dev2026 \| sha256sum`, locally | `0873a970…` |
| 2 | transfer to a **fresh** path, outside every identity and install path | — |
| 3 | `sha256sum` **on the host** | the same digest |
| 4 | `tar -tf \| wc -l` **on the host** | **273 members** |
| 5 | `tar -tvf \| grep -c '^-'` **on the host** | **262 files** |
| 6 | extract, then re-derive the file-list with the driver's own `tree_filelist` | `c436362a…` |

**Abort if any of the six disagrees.** No repair, no re-transfer over the same path.

---

## 2. Production preflight, as `odbadmin`

Read-only. Every item is recorded before anything is changed; any mismatch aborts.

1. `id` → uid **1000**, the production owner.
2. The live PM2 app list under production's `PM2_HOME` — **the app name is read, never
   assumed** (§3).
3. The current live PM2 definition, in full, saved to the recovery bundle (§11).
4. The current `conf/ecosystem.config.js`, byte-for-byte, with its sha256.
5. The current process tree serving 8050: master pid + starttime, worker pids + starttimes,
   argv, and the module it serves (expected: `woa23_app:app`).
6. Port 8050 listener identity.
7. The production store's owner, mode, ACL and metadata fingerprint (§9).
8. The TLS certificate and key: existence, real path, owner, mode, readability by the
   production account, and expiry (§8).
9. Free disk for the new tree and venv.

---

## 3. Production owner, app name, `PM2_HOME`, PM2 version

| | value | status |
|---|---|---|
| owner | uid **1000** `odbadmin` | recorded |
| app name | **read from the live PM2 list at preflight** | the config proposes `woa23`; the live name governs |
| `PM2_HOME` | production's own, read at preflight | **never** the staging `PM2_HOME`, never a default |
| PM2 version | **5.4.2 — MEASURED** | closed |

**MEASURED at preflight, and the open item is closed.** Production PM2 is **5.4.2**, from two
independent sources — the running daemon's own cmdline and `package.json` read as JSON.
`pm2 -v` was never invoked. The binary at `/home/odbadmin/.npm-global/bin/pm2` resolves to
`…/lib/node_modules/pm2/bin/pm2` with sha256
**`bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d`**, matching the recorded
reference. The daemon is pid **3459**, starttime `lin-13189`, uid 1000, `PM2_HOME`
`/home/odbadmin/.pm2`.

**This is the same version every PM2 behaviour in this campaign was proven on.**

**NEW RISK, measured: production's `PM2_HOME` holds NINE apps** — `gateway`, `odbbathy`,
`mhwapi`, **`woa23`**, `ghrsst`, `ghrsst_mcp`, `dask-scheduler`, `dask-worker`, `tide`; seven
are online. **A wildcard, `all`, `save`, `resurrect` or `pm2 kill` would hit eight other live
services.** Every operation in this plan names `woa23` exactly, and that is now a
production-safety requirement rather than a stylistic one.

**`append_env_to_name` is confirmed `true` in the LIVE definition**, so setting it false and
never passing `--env` is load-bearing.

**`append_env_to_name: false` is mandatory.** Production sets it true; with `--env` given at
a cutover, PM2 would create `woa23-production` *alongside* `woa23` — two apps, one port, and
the original's PM2 state orphaned.

---

## 4. Production venv, interpreter and locked packages

### 4.0 DECIDED — the runtime path set

**These are owner decisions, now settled. They are no longer open items.**

```sh
# uv's own knob — the PARENT directory uv installs interpreters into
UV_PYTHON_INSTALL_DIR=/home/odbadmin/python/uv-pythons

# what uv ACTUALLY creates inside it; the exact name is READ from `uv python list`,
# never guessed
UV_PYTHON_REAL_ROOT=$UV_PYTHON_INSTALL_DIR/cpython-3.11.14-linux-x86_64-gnu

# a SYMLINK ALIAS to the above — this plan's stable name, not a uv variable
UV_PYTHON_ROOT=/home/odbadmin/python/cpython-3.11.14-20251217

APP_ROOT=/home/odbadmin/python/woa23-143bf8c
VENV=$APP_ROOT/.venv
WOA23_PYTHON=$VENV/bin/python3.11

# the production account's OWN package cache — never another account's
UV_CACHE_DIR=/home/odbadmin/.cache/uv
```

| name | what it IS | what it is NOT |
|---|---|---|
| **`UV_PYTHON_ROOT`** | the **uv-managed standalone CPython installation root** — a complete CPython 3.11.14 (python-build-standalone) with its own `bin/`, `lib/python3.11/` stdlib and `lib-dynload/` | **NOT a venv.** It has no project packages and is never the value of `WOA23_PYTHON` |
| **`APP_ROOT`** | the **new WOA23 deployment / release tree** — where subject `143bf8c` is extracted | **NOT the serving venv**, and **not** the live tree `/home/odbadmin/python/woa23` |
| **`VENV`** | **`$APP_ROOT/.venv`** — the **actual serving venv**, created from `$UV_PYTHON_ROOT` and populated by `uv sync --locked` | not a copy of the interpreter; its `python3.11` is a symlink into `$UV_PYTHON_ROOT` |
| **`WOA23_PYTHON`** | **`$VENV/bin/python3.11`** — the interpreter the service is launched with | never `$UV_PYTHON_ROOT/bin/python3.11`, and never anything under `/home/odbadmin/.pyenv` |

#### Five names, and which of them uv actually reads

| name | read by uv? | what it is |
|---|---|---|
| **`UV_PYTHON_INSTALL_DIR`** | **YES — uv's real knob** (`uv python install -i/--install-dir`) | the **PARENT** directory uv installs interpreters *into*. **Not** an installation root |
| **`UV_PYTHON_REAL_ROOT`** | no — plan name | **the directory uv actually manages**: `$UV_PYTHON_INSTALL_DIR/cpython-<version>-<platform>-<libc>`. **This is the real installation** — what `uv python list` knows about, what `sys.base_prefix` reports, and what `/proc/<pid>/exe` resolves to |
| **`UV_PYTHON_ROOT`** | **NO — a SYMLINK ALIAS, and a name used by THIS PLAN only.** Exporting it changes nothing in uv | a symlink pointing at `$UV_PYTHON_REAL_ROOT`, so the decided stable path `…/cpython-3.11.14-20251217/bin/python3.11` resolves. **uv does not manage, know about, or read this path** |
| **`APP_ROOT`** | no — plan name | the candidate **deployment / release tree**; the subject is extracted here |
| **`VENV`** | no — plan name (but see `UV_PROJECT_ENVIRONMENT` below) | **`$APP_ROOT/.venv`**, the **actual serving venv** |
| **`WOA23_PYTHON`** | no — read by `production_app.sh` | **`$VENV/bin/python3.11`**, the interpreter **PM2 and the app actually execute** |
| **`UV_CACHE_DIR`** | **YES — uv's package cache** (`--cache-dir`, `uv cache dir`) | **`odbadmin`'s own** cache. §4.4a |

**The alias and the real directory are not interchangeable, and the plan says which is which
at every check.** `UV_PYTHON_ROOT` is what the operator types and what `$UV_PYTHON_ROOT/bin/python3.11`
resolves through; `UV_PYTHON_REAL_ROOT` is what uv manages and what the interpreter reports
about itself. **Every identity check compares realpaths for exactly this reason** (§4.2).

#### MEASURED CONFLICT — `--install-dir` is a PARENT, and uv names the subdirectory

**Verified against uv's own behaviour, not assumed:**

```
uv python dir                                     -> <install-dir>
UV_PYTHON_INSTALL_DIR=/tmp/probe uv python dir    -> /tmp/probe        (the variable IS honoured)
ls <install-dir>                                  -> cpython-3.11.14-macos-aarch64-none
                                                     cpython-3.12.12-macos-aarch64-none
interpreter actually lands at
      <install-dir>/cpython-<version>-<platform>-<libc>/bin/python3.11
```

**`uv python install` has NO option that installs into an exact directory of your choosing** —
`--install-dir` names the parent, and **uv chooses the subdirectory name itself**.

**Consequence: a plain `uv python install` will NOT produce
`/home/odbadmin/python/cpython-3.11.14-20251217/bin/python3.11`.** On VM24 uv would create
`<install-dir>/cpython-3.11.14-linux-x86_64-gnu/`. **The decided name is kept; what has to be
specified is how uv is invoked to make it real.**

| # | way to reconcile | consequence |
|---|---|---|
| **V1 — ADOPTED** | install with `UV_PYTHON_INSTALL_DIR`, then **symlink `$UV_PYTHON_ROOT` -> `$UV_PYTHON_REAL_ROOT`** | the **decided name is preserved** and `$UV_PYTHON_ROOT/bin/python3.11` resolves. **uv still recognises `$UV_PYTHON_REAL_ROOT` as uv-managed** (§4.2 field 2a). `sys.base_prefix` and `/proc/<pid>/exe` report **`$UV_PYTHON_REAL_ROOT`**, so those checks compare **realpaths** — §4.2 |
| V2 | adopt uv's own name as `UV_PYTHON_ROOT`, i.e. `/home/odbadmin/python/uv-pythons/cpython-3.11.14-linux-x86_64-gnu` | simplest, no symlink, and `sys.base_prefix` matches literally — but the decided directory name is not used |
| **V3 — DO NOT** | install, then **move/rename** the directory to the decided name | **breaks `uv python list` recognition**, so §4.2 field 2a can no longer confirm the installation is uv-managed. **Rejected** |

**V1 is the recommendation and the provisioning commands in §4.4 are written for it.** The
exact `linux-x86_64-gnu` suffix is **not guessed** — it is read from `uv python list` on VM24
at provisioning time (§4.4 step 3).

**Access requirement — `odbadmin` must be able to read and execute all of it.** The service
runs as uid 1000, so:

| # | path | required for `odbadmin` |
|---|---|---|
| 1 | `$UV_PYTHON_ROOT` and every parent | **traversable** (`x`) |
| 2 | `$UV_PYTHON_ROOT/bin/python3.11` | **readable and executable** (`r`, `x`) |
| 3 | `$UV_PYTHON_ROOT/lib/python3.11/` and `lib-dynload/` | **readable** |
| 4 | `$APP_ROOT`, `$VENV` and `$VENV/lib/python3.11/site-packages/` | **readable**, and `$VENV/bin/python3.11` **executable** |

**These must be verified as `odbadmin` itself**, not as whoever performed the provisioning.
An installation created by another account, or under a mode that only its owner can traverse,
is exactly the failure this requirement exists to prevent — and it is why the campaign's
`woa23c1ro` interpreter is not reused (§4.3).

### 4.1 The serving interpreter, named exactly

**The architecture requirement is a standalone uv-managed CPython plus its own venv. Nothing
under `/home/odbadmin/.pyenv` may take part in serving — not the interpreter, not the standard
library, not `site-packages`.** The earlier proposal to build the venv on pyenv is
**REJECTED**; see §4.3.

| | |
|---|---|
| **production account** | **`odbadmin`, uid 1000** — the account PM2 and the application run as |
| **standalone interpreter** | **`$UV_PYTHON_ROOT/bin/python3.11`** — a **uv-managed standalone CPython 3.11.14**, provisioned **for `odbadmin`** |
| **venv** | **`$APP_ROOT/.venv`**, created from that standalone interpreter — `uv venv --python $UV_PYTHON_ROOT/bin/python3.11` |
| **the SERVING interpreter** | **`$APP_ROOT/.venv/bin/python3.11`**, and nothing else |
| `WOA23_PYTHON` | **that path, absolute** |
| **pyenv** | **excluded entirely.** Not the base, not a fallback, not on `PATH` for the service |
| packages | `uv sync --locked` from the subject's `uv.lock` |
| lock digest | `uv.lock` sha256 **`0d2980a5928d4d0964d6cb3b78bffae14aa11a70d3b51ca00f4cf39073dccc69`** — recorded before and after, and **must not change** |
| `pyproject.toml` digest | `aa846b8be70b0b5d466d0e2a0bbb1f4dfe6ccac5d28f795a57c1c6bbea7e378e` |
| `requires-python` | `>=3.11,<3.12` — the standalone interpreter must satisfy this |

**Under this design the check becomes absolute rather than nuanced.** With a pyenv-based venv,
the base installation legitimately supplies the interpreter binary and the standard library,
so a `maps` check had to distinguish stdlib from third-party. **With a standalone uv-managed
CPython there is no such carve-out: NOTHING under `/home/odbadmin/.pyenv` may appear in the
serving process at all.**

**This is not a theoretical rule — it already failed once.** `pm2G` built a per-run isolated
venv, verified its interpreter, and recorded a 58-package manifest for it; the service it
started then mapped 1153 and 451 libraries from the **shared** environment and **zero** from
that venv. `polars` came from the shared environment too. **The manifest described something
that was not serving, which makes that evidence worse than absent — it reads as proof.**

`production_app.sh` now **fails closed**: `WOA23_PYTHON` is required, has **no default**, and
must name an **executable** interpreter. The old fallback to the shared pyenv environment was
removed for exactly this reason.

**CORRECTION to an earlier draft of this section.** It required `/proc/<pid>/exe` to resolve
**inside the venv**. **That is wrong and would fail on a correct deployment.** A venv's
`bin/python3.11` is a *symlink* to its base interpreter and Linux resolves `/proc/<pid>/exe`
to the real binary, so it points into the **base installation** — under P2, into `$UV_PYTHON_ROOT`.
Requiring otherwise would have made a correct cutover look broken and might have been "fixed"
by copying an interpreter around. The corrected checks are §4.2.

### 4.2 Runtime identity — the exact fields to record, and the check that actually discriminates

**All values below are recorded AT CUTOVER. None is measured today** — the venv does not
exist yet, and this update contacted no host. They are written as the fields to capture and
the relations that must hold, not as findings.

| # | field | how | expected |
|---|---|---|---|
| 1 | **production account** | `id` | **uid 1000, `odbadmin`** — the account PM2 and the application run as |
| 2 | **standalone Python path** | the alias | **`$UV_PYTHON_ROOT/bin/python3.11`** exists and is executable |
| 2a | **uv's own identification** | `uv python list` / `uv python find`, as **`odbadmin`** | **`$UV_PYTHON_REAL_ROOT`** is listed **as a uv-managed CPython**. **uv knows the real directory, not the alias** — §4.0. If uv does not recognise it, it is not the adopted runtime: stop |
| 2b | **BUILD / version string** | `$UV_PYTHON_ROOT/bin/python3.11 -VV` | reports **3.11.14**, build identifier recorded **verbatim** — the interpreter identity G1 and every candidate run used |
| 2c | **the alias resolves to the real root** | `readlink "$UV_PYTHON_ROOT"` | equals **`$UV_PYTHON_REAL_ROOT`** (§4.4e check 4) |
| 3 | **standalone Python realpath** | `readlink -f $UV_PYTHON_ROOT/bin/python3.11` | resolves inside the **real** uv installation directory — under V1 that is the symlink target, which is expected. **Must NOT resolve under `/home/odbadmin/.pyenv`** |
| 4 | **venv path** | the directory `uv venv` created | **`$VENV` = `$APP_ROOT/.venv`** |
| 5 | **`WOA23_PYTHON`** | `/proc/<master>/environ` | **`$APP_ROOT/.venv/bin/python3.11`**, absolute |
| 6 | **venv Python realpath** | `readlink -f $APP_ROOT/.venv/bin/python3.11` | resolves to **`$UV_PYTHON_ROOT/bin/python3.11`** (or inside `$UV_PYTHON_ROOT`). **Must NOT resolve under `/home/odbadmin/.pyenv`** |
| 7 | **`sys.prefix`** | `"$WOA23_PYTHON" -c 'import sys;print(sys.prefix)'` | **`$APP_ROOT/.venv`** |
| 8 | **`sys.base_prefix`** | same, `sys.base_prefix` | reports **`$UV_PYTHON_REAL_ROOT`** — compare by **realpath**: `realpath(sys.base_prefix) == realpath($UV_PYTHON_ROOT) == $UV_PYTHON_REAL_ROOT`. It reports the symlink **target**, which is correct and **must not be read as a mismatch**. `sys.prefix != sys.base_prefix` proves a venv is active. **Neither may be under `/home/odbadmin/.pyenv` or `/home/woa23c1ro`** |
| 9 | **`sys.executable`** of the running master | the launcher's own echo, in the PM2 log | **`$APP_ROOT/.venv/bin/python3.11`** |
| 10 | **`/proc/<pid>/exe`** | master **and** every worker | resolves to **`$UV_PYTHON_REAL_ROOT/bin/python3.11`** — the real standalone install, since `/proc` resolves symlinks. **This is correct, not a mismatch.** **NOT into `/home/odbadmin/.pyenv` or `/home/woa23c1ro`** |
| 11 | **`/proc/<pid>/maps`** | master **and** every worker | **see below — the discriminating check** |
| 12 | **the venv did not leak into the project directory** | `test ! -e $APP_ROOT/dev2026/.venv` | **absent.** This is the version-independent proof that the `UV_PROJECT_ENVIRONMENT` override took effect — §4.4 |

**Field 11, and under P2 it is absolute:**

| must NOT appear in `maps` — for the master or ANY worker | must appear |
|---|---|
| **any path under `/home/odbadmin/.pyenv`** — interpreter, `libpython*`, stdlib, `lib-dynload`, or `site-packages` — **and any path under `/home/woa23c1ro`. Zero occurrences of either.** | third-party extension modules under **`$APP_ROOT/.venv/lib/python3.11/site-packages/`** — `numpy`, `polars`, `pyarrow`, `pydantic_core`, `orjson`, `zarr`/`numcodecs` |
| **any production `site-packages` outside the venv** | the standard library and `lib-dynload` under **`$UV_PYTHON_REAL_ROOT`** — the standalone install, which is correct |

**Why P2 makes this check clean.** A pyenv-based venv legitimately reads its stdlib and C
extension modules from the pyenv installation, so a `maps` check would have had to permit
pyenv paths for stdlib while forbidding them for `site-packages` — a distinction easy to get
wrong in both directions. **Under P2 the rule is simply: `/home/odbadmin/.pyenv` must not
appear at all**, which is checkable by a single `grep -c` returning **0**.

**Record field 11 for the master AND every worker**, not the master alone. `pm2G` passed the
environ and prefix checks while serving 1153 and 451 libraries from the shared environment
and **zero** from its own venv; `polars` came from the shared environment too. **The manifest
described something that was not serving, which makes that evidence worse than absent: it
reads as proof.** Fields 5, 7, 8 and 9 are all things `pm2G` would have passed. **Field 11 is
the one it would have failed.**

**Why this matters more than the config says it does.** `pm2G` built an isolated venv,
verified its interpreter, and recorded a 58-package manifest — then served with **1153 and
451 libraries mapped from the shared environment and zero from that venv**. `polars` came
from the shared environment too. **The manifest described something that was not serving,
which makes that evidence worse than absent: it reads as proof.** Fields 3, 5, 6 and 7 are
all things `pm2G` would have passed. **Field 9 is the one it would have failed.**

**Recording rule:** capture field 9 for the **master and every worker**, not the master
alone — workers are forks, but the record must show it rather than assume it.

### 4.3 P1 is REJECTED. P2 is ADOPTED — and provisioning it is a BLOCKER

#### P1 — venv built on `/home/odbadmin/.pyenv/versions/py311` — **REJECTED**

**Not an option, and no longer listed as one.** A venv built on pyenv **cannot** exclude
pyenv from the serving process: the venv interpreter is a symlink to the pyenv binary, and the
standard library and its C extension modules are read from the pyenv installation. A venv
isolates the third-party dependency set and **nothing else**.

The WOA23 refactor requires a **standalone uv-managed Python plus its own venv**. **P1 does
not meet that requirement, so it is REJECTED** — not weighed, not offered as the cheaper
alternative, and not to be reinstated because provisioning P2 turns out to be inconvenient.

#### P2 — standalone uv-managed CPython, then an independent venv — **ADOPTED**

```
uv python install <version>          ->  $UV_PYTHON_ROOT/bin/python3.11      standalone CPython
uv venv --python $UV_PYTHON_ROOT/bin/python3.11 $APP_ROOT/.venv
uv sync --locked                     ->  $APP_ROOT/.venv/lib/python3.11/site-packages
WOA23_PYTHON=$APP_ROOT/.venv/bin/python3.11
```

**`/home/odbadmin/.pyenv` takes no part.** §4.2 field 11 is then a single check: zero
occurrences of that path in the master's or any worker's `maps`.

#### BLOCKER — `RUNTIME-PROVISIONING-INCOMPLETE`

**The production-side runtime does not exist yet, and provisioning it is a REQUIRED STEP of
this cutover — not a precondition someone else has already met.**

| # | why it is not satisfied today | |
|---|---|---|
| 1 | **the interpreter this campaign used was provisioned under `woa23c1ro`, not `odbadmin`** | Every candidate run, and G1, ran on **3.11.14** under the staging account. **That installation must NOT be assumed readable, executable or usable by `odbadmin`**, and this plan does not assume it. Reusing another account's runtime is also how a serving process ends up depending on a tree nobody owns |
| 2 | **the installation does not exist on VM24 yet** | the paths are now decided (§4.0); **provisioning them is still an unperformed step** |
| 3 | **whether `uv` is available to `odbadmin` on VM24 is UNMEASURED** | this update contacted no host |
| 4 | **`uv python install` needs a download** | today's plan sets `UV_OFFLINE=1` and `UV_PYTHON_DOWNLOADS=never`. Provisioning must either relax those **for the provisioning step only**, or stage the python-build-standalone archive out of band and install from it. **`uv sync --locked` for the packages stays offline** |

**Until production-side provisioning is completed AND authorised, this is a BLOCKER and the
plan is not execution-ready.** Provisioning is itself a production-side change and needs its
own authorization; it is **not** covered by the cutover authorization.

#### The version is DECIDED: 3.11.14

**Chosen by the owner, and it is the option that matches the evidence.** Every candidate run
in this campaign and **G1** were executed on **3.11.14**, so the serving runtime will be the
one all offline evidence was produced on.

**It differs from today's production interpreter (3.11.4), and that difference is real and
recorded** — §4.6. There was no option that matched both, because production and this
campaign already differed before this decision.

**§4.2 field 11 is required regardless.**

### 4.4 Provisioning the runtime — a REQUIRED subsequent step, NOT performed

**Nothing below has been executed. This round updated the document only** — VM24 was not
contacted, no interpreter was installed, no venv was created, no package was fetched.

**Provisioning is a production-side change under `odbadmin` and needs its own authorization.
It is not covered by the cutover authorization, and it must complete and be verified BEFORE a
cutover window is scheduled** — not inside one. A window is not the place to discover that a
download is blocked or that a path is unreadable.

#### The layout hazard this must avoid — MEASURED, not hypothetical

**`pyproject.toml` is at `$APP_ROOT/dev2026`, but the serving venv must be at
`$APP_ROOT/.venv`. uv's default puts it in the wrong place.** Reproduced on that exact
layout:

```
$ cd $APP_ROOT/dev2026 && uv sync
Using CPython 3.11.14
Creating virtual environment at: .venv        <-- $APP_ROOT/dev2026/.venv
  APP_ROOT/.venv          : absent
  APP_ROOT/dev2026/.venv  : EXISTS            <-- WRONG
```

**The fix, and it was tested on the same layout rather than assumed:**

```
$ cd $APP_ROOT/dev2026 && UV_PROJECT_ENVIRONMENT=$APP_ROOT/.venv uv sync
Creating virtual environment at: /…/APP_ROOT/.venv
  APP_ROOT/.venv          : EXISTS            <-- correct
  APP_ROOT/dev2026/.venv  : absent
```

**And the full sequence — pre-created venv, then `uv sync --locked` — was verified end to end:**

```
uv venv --python $UV_PYTHON_ROOT/bin/python3.11 $VENV
UV_PROJECT_ENVIRONMENT=$VENV uv sync --locked
  -> "Audited" — the existing venv is REUSED, not recreated
  sys.prefix       = $VENV
  sys.base_prefix  = the standalone installation root
  venv active      = True
  realpath($VENV/bin/python3.11) = <installation root>/bin/python3.11
  APP_ROOT/dev2026/.venv : absent
```

**`UV_PROJECT_ENVIRONMENT` is an environment variable, NOT a `uv sync` flag** — it does not
appear in `uv sync --help`. `--project <dir>` (env `UV_PROJECT`) also works when running from
`$APP_ROOT`, and was verified; the form below is preferred because the path is absolute and
the working directory is the one PM2 will use.

**VERSION CAVEAT, stated rather than glossed.** The behaviour above was verified with **uv
0.9.27 on the local macOS workstation**, not with **0.9.22** on VM24 — and **VM24's uv version
is UNMEASURED** (this round contacted no host). **Step 2 below re-confirms the option on the
host's own uv before anything depends on it, and step 8 verifies the OUTCOME by path**, which
is version-independent and does not require trusting the variable at all.

#### The provisioning steps

| # | step | done when |
|---|---|---|
| 1 | confirm **`uv` is available to `odbadmin`**; record `uv --version` | succeeds **as `odbadmin`**; the version is recorded, not assumed to be 0.9.22 |
| 2 | **re-confirm the override on the host's uv**: `uv sync --help`, and `uv python install --help` for `--install-dir` | `UV_PROJECT_ENVIRONMENT` and `-i/--install-dir` behave as §4.0 and above describe. **If they differ on that version, STOP** — do not improvise a substitute |
| 3 | **install the interpreter — by the PRIMARY method of §4.4c** (staged archive + local file mirror, offline), invoking uv by **absolute path `/home/odbadmin/.local/bin/uv`** (it is **not** on the non-login `PATH` — measured), then `uv python list` and **read** the real directory name | `$UV_PYTHON_REAL_ROOT` exists and **uv lists it as uv-managed**. **The `linux-x86_64-gnu` suffix is READ, never guessed** |
| 4 | **create the alias**:<br>`ln -s "$UV_PYTHON_REAL_ROOT" /home/odbadmin/python/cpython-3.11.14-20251217` | `$UV_PYTHON_ROOT` is a **symlink** whose target is `$UV_PYTHON_REAL_ROOT`, confirmed by `readlink`. `$UV_PYTHON_ROOT/bin/python3.11` exists and is executable. **Do not move or rename the real directory** — that breaks uv's recognition (§4.0 V3) |
| 5 | **verify the interpreter, as `odbadmin`** — the five checks of §4.4e | all five pass |
| 6 | verify **§4.0 access requirements 1–3 AS `odbadmin`** | traversable, readable, executable — checked by the account that will serve, **not** by the installer |
| 7 | extract subject `143bf8c` under **`$APP_ROOT`**, verifying the six checks of §1 | file-list `c436362a…` |
| 8 | **the package-cache gate — §4.4a** | `uv sync --locked --offline` **succeeds** |
| 9 | verify **§4.2 fields 4–9 and 12**, plus **§4.0 access requirement 4** | `$VENV/bin/python3.11` exists and is **executable by `odbadmin`**; `sys.prefix` = `$VENV`; `realpath(sys.base_prefix)` = `realpath($UV_PYTHON_REAL_ROOT)`; neither under `/home/odbadmin/.pyenv`; `$APP_ROOT/dev2026/.venv` **absent** |
| 10 | record the whole set as the provisioning evidence | — |

#### 4.4a The package-cache gate — `odbadmin`'s own cache, and an offline sync that must SUCCEED

**`UV_CACHE_DIR=/home/odbadmin/.cache/uv`, named explicitly and recorded.**

**No other account's cache may be assumed usable.** The campaign's package cache was
`/home/woa23c1ro/.cache/uv`, under the **staging** account. It is **not** assumed readable by
`odbadmin`, **not** assumed complete for this lock, and **not** to be pointed at. Record
`uv cache dir` as `odbadmin` and confirm it is the intended path — the value uv *reports*, not
the value someone meant to set.

**The gate, run from `$APP_ROOT/dev2026`:**

```sh
uv venv --python "$UV_PYTHON_ROOT/bin/python3.11" "$VENV"

UV_CACHE_DIR=/home/odbadmin/.cache/uv \
UV_PROJECT_ENVIRONMENT="$VENV" \
UV_PYTHON_DOWNLOADS=never \
  uv sync --locked --offline
```

| # | success condition | required |
|---|---|---|
| 1 | **exit status** | **0** |
| 2 | **`uv.lock` sha256** | **`0d2980a5…dccc69`, unchanged** — `--locked` asserts this |
| 3 | **the result is in the designated `$VENV`** | packages land in **`$VENV/lib/python3.11/site-packages`**, and **`$APP_ROOT/dev2026/.venv` does NOT exist** (§4.2 field 12) |

**The counts, stated exactly — 60 and 58 are different numbers and mean different things:**

| number | what it counts |
|---|---|
| **60** | **distributions RESOLVED** — every entry in `uv.lock`. This is what uv reports as *"Resolved 60 packages"* |
| **58** | **distributions APPLICABLE AND REQUIRED on Linux / cp311** — what is actually installed into `$VENV` |
| **2** | **not installed**, and correctly so |

The two that are resolved but not installed, read from `uv.lock`:

| excluded | why | lock evidence |
|---|---|---|
| **`woa23-bench2026`** | the **virtual project itself** — it is the root, not a dependency to install | `source = { virtual = "." }` |
| **`colorama`** | **win32-only**, so not applicable on Linux | `{ name = "colorama", marker = "sys_platform == 'win32'" }` |

**So "all 60 installed" would be WRONG as a success condition, and is not used as one.** The
check is condition 3 above — the packages are present in the **designated** `$VENV` — together
with exit 0 and an unchanged lock. `60 - 1 virtual - 1 win32-only = 58` is arithmetic that
agrees with what uv reports, and it is recorded so that "58" is never read as two missing
packages.

**`--dry-run` is INDICATIVE INFORMATION ONLY and must NEVER be used as the completeness
gate.** Measured, against a deliberately empty cache with `--offline`:

**MEASURED — `--dry-run` is NOT a completeness gate. Do not use it as one.** Against a
deliberately empty cache, with `--offline`:

```
$ uv sync --locked --offline --dry-run
Resolved 60 packages
Would download 58 packages          <-- it PLANS the downloads
Would install 58 packages
exit=0                              <-- and exits ZERO
```

```
$ uv sync --locked --offline        (the same empty cache, for real)
  × Failed to download `pyarrow==25.0.0`
  ╰─▶ Network connectivity is disabled, but the requested data wasn't found in the cache
exit=1
```

**Only the real `uv sync --locked --offline` proves the artifacts are actually present.** A
green `--dry-run` says what uv *would* do, not what the cache *holds*.

**It also names only the FIRST missing artifact, not all of them.** Populating the cache and
retrying can therefore iterate; each retry is the same gate with the same three conditions.

#### 4.4b The partial-venv rule

**A failed `uv sync` can leave `$VENV` behind, partially populated.** Measured: in the failure
above, `$APP_ROOT/.venv` had already been created when the download failed.

| rule | |
|---|---|
| 1 | **The existence of `$VENV` is NOT evidence of success.** Only **exit 0**, with conditions 2 and 3, is |
| 2 | **A partial venv must NOT be carried into the cutover.** Not as "mostly there", not with a manual `pip install` to finish it, not at all |
| 3 | **A retry must either use a FRESH `$APP_ROOT`/`$VENV`, or re-run the same offline sync for the same lock to a clean exit 0.** A retry that merely appends to a half-built environment is not a passing gate |
| 4 | **`$APP_ROOT/dev2026/.venv` must not exist** at the end, in every case (§4.2 field 12) |
| 5 | **No fallback of any kind** — no re-lock, no dropping `--locked` or `--offline`, no other account's cache, no pyenv, no `woa23c1ro` installation |

**If a fresh `$APP_ROOT` is used for the retry, the subject must be re-extracted and §1's six
checks re-run against it.** A new tree is a new tree, and its provenance is verified like the
first one.

**ON FAILURE — every one of these is forbidden:**

| forbidden | |
|---|---|
| downloading packages | dropping `--offline`, or relaxing `UV_OFFLINE`, to "just get it working" |
| editing `uv.lock` or `pyproject.toml` | re-locking changes the subject and needs a **successor subject** |
| dropping `--locked` | it is what asserts the lock is unchanged |
| pointing `UV_CACHE_DIR` at `woa23c1ro`'s cache | another account's cache, assumed neither readable nor complete |
| using the partially-populated `$VENV` | see consequence 2 |
| **continuing to the cutover** | **the runtime is not provisioned; no window is scheduled** |

**Populating `odbadmin`'s cache from an approved source is a separate, separately authorised
step.** It is not part of this gate, and this gate does not authorise a download.

#### 4.4c How the interpreter is obtained — PRIMARY: staged archive + local file mirror

**This is the preferred method, and it is the one the steps above are written for.** It keeps
the download outside VM24, makes the artifact digest checkable **before** anything is
installed, and lets uv install **fully offline**.

| # | step | who | done when |
|---|---|---|---|
| 1 | **transfer the APPROVED CPython 3.11.14 archive to VM24** — obtained and approved out of band, not fetched by VM24 | operator | the file is present at a staging path |
| 2 | **verify its sha256 ON VM24 against the approved value** | `odbadmin` | **digests match exactly.** A mismatch stops provisioning — no reuse, no re-download |
| 3 | **place it in the local file-mirror layout** | `odbadmin` | see the layout below |
| 4 | **install offline through the mirror**:<br>`UV_PYTHON_INSTALL_DIR=/home/odbadmin/python/uv-pythons \`<br>`UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=manual \`<br>`  uv python install 3.11.14 --mirror "file:///home/odbadmin/python/py-mirror"` | `odbadmin` | exit 0, and `$UV_PYTHON_REAL_ROOT` created |
| 5 | **uv recognises it** — `uv python list` | `odbadmin` | listed as uv-managed (§4.4e check 1) |

**The mirror layout — measured, and it does not have to be guessed.** `--mirror` accepts a
`file://` URL and works with `--offline` (verified). When the file is absent, **uv's own error
names the exact path it expects**:

```
$ uv python install 3.11.14 --mirror "file:///…/py-mirror" --offline
error: Failed to install cpython-3.11.14-…
  Caused by: failed to query metadata of file
  `/…/py-mirror/<YYYYMMDD>/cpython-3.11.14+<YYYYMMDD>-<triple>-install_only_stripped.tar.gz`
exit=1     (nothing installed — only uv's own .lock/.temp bookkeeping)
```

**Use that error to learn the exact expected filename on VM24, then stage the approved archive
at that path.** The `<YYYYMMDD>` release stamp and the `<triple>` are **read from uv's message
on VM24**, never assumed from this document — the triple is a Linux one there, and the stamp
belongs to the python-build-standalone release **that VM24's uv version asks for**.

**`--mirror` is a FLAG.** This uv version's `--help` shows **no environment variable** for it,
unlike `--install-dir`. Pass it explicitly; do not export a guessed variable name.

**MEASURED — `UV_PYTHON_DOWNLOADS=never` BLOCKS this install, and `manual` is required.** uv
0.9.22 refuses an **explicit** `uv python install` under `never`, even from a local `file://`
mirror with `--offline`:

```
Python downloads are not allowed (`python-downloads = "never"`).
Change to `python-downloads = "manual"` to allow explicit installs.
```

In uv, `python-downloads` governs **whether uv may install a Python at all**, not where from.
**There is no combination that keeps `never` and still installs.** `manual` permits an
**explicit** install while still forbidding **automatic** ones, and with `--offline` plus the
`file://` mirror **no network access remains possible** — the intent of `never` is preserved.

**This applies to the interpreter install ONLY. `UV_PYTHON_DOWNLOADS=never` stays in force for
`uv sync` (§4.4a), where nothing should ever fetch a Python.**

**MEASURED ON VM24's OWN uv 0.9.22 — the exact path is known and does NOT have to be
guessed:**

```
<mirror>/20251217/cpython-3.11.14+20251217-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz
```

**The release stamp uv 0.9.22 requests is `20251217`, which matches the approved build and the
decided alias name `cpython-3.11.14-20251217`.** An earlier revision of this plan warned that
the alias date might not match the stamp; that warning came from a probe on a **different uv
version** and **does not apply to VM24** — corrected here. The authoritative identity is still
§4.4e (uv's listing, `-VV`, and the digest); the name agreeing is a convenience, not the proof.

#### 4.4d ALTERNATIVE — direct `uv python install` download

**Kept as a fallback only, and it is a WEAKER evidence path. It must never be described, or
written up, as if it were the staged-archive verification of §4.4c.**

```sh
UV_PYTHON_INSTALL_DIR=/home/odbadmin/python/uv-pythons uv python install 3.11.14
```

| | |
|---|---|
| network | VM24 downloads directly — `UV_OFFLINE`/`UV_PYTHON_DOWNLOADS` must be relaxed **for this step only** |
| **the archive is NOT retained** | uv does not leave the downloaded archive as a file on disk to digest afterwards |
| **therefore there is no pre-install artifact digest** | the approved-archive comparison of §4.4c step 2 **cannot be performed at all** |
| what CAN be recorded | only an **installed-binary FIRST CAPTURE**: sha256 of `$UV_PYTHON_REAL_ROOT/bin/python3.11`, plus uv's listing and `-VV`, taken immediately after install |
| what that first capture IS | a **baseline for detecting later change** to the installed tree |
| what it is **NOT** | **it is NOT verification against the approved artifact.** Nothing was compared to an approved value; the first capture only records what arrived |

**Writing rule, and it is not stylistic.** If this alternative is used, the provisioning
evidence must say **"installed-binary first capture, no approved-archive verification
performed"**. It must **not** be recorded under the same heading, in the same sentence, or in
the same digest field as a §4.4c staged-archive check. **Two different claims with two
different strengths must not be merged into one line that reads as the stronger of them** —
that is precisely how `pm2G`'s manifest came to read as proof of something it did not show.

#### 4.4e Interpreter verification — five checks, all as `odbadmin`

| # | check | passes when |
|---|---|---|
| 1 | **uv recognises it** — `uv python list` | `$UV_PYTHON_REAL_ROOT` is listed as a **uv-managed** CPython. **If uv does not recognise it, it is not the adopted runtime — stop** |
| 2 | **realpath and BUILD/version** — `readlink -f "$UV_PYTHON_ROOT/bin/python3.11"` and `-VV` | realpath is inside **`$UV_PYTHON_REAL_ROOT`**; `-VV` reports **CPython 3.11.14** with its build string recorded **verbatim** |
| 3 | **artifact digest** | **PRIMARY (§4.4c): the staged archive's sha256 equals the APPROVED value, verified on VM24 BEFORE installation.** **ALTERNATIVE (§4.4d): no approved-archive verification is possible** — record an installed-binary **first capture** only, and label it as such. **The evidence must state which path was used, and the two must never be written as one** |
| 4 | **the alias points at the real directory** — `readlink "$UV_PYTHON_ROOT"` | equals **`$UV_PYTHON_REAL_ROOT`**, and `realpath($UV_PYTHON_ROOT) == realpath($UV_PYTHON_REAL_ROOT)` |
| 5 | **neither pyenv nor `woa23c1ro`** | no path under **`/home/odbadmin/.pyenv`** and none under **`/home/woa23c1ro`** appears in the realpath, in `sys.base_prefix`, or in the uv listing for this installation |

**`--locked` and `--frozen` are mutually exclusive** in this uv line — use `--locked`, which
asserts the lock will not change. Do not pass both.

**Steps 3 and 8 have different offline postures on purpose.** The interpreter must come from
somewhere and may need the network; **the packages must not** — `uv sync --locked` stays
offline so the dependency set is the locked one and nothing is silently resolved.

**Step 8's second condition is the real gate.** `$APP_ROOT/dev2026/.venv` being absent proves
the override took effect on *that* uv version, whatever its help text says. **If it exists,
the sync went to the wrong place: STOP, and do not "fix" it by pointing `WOA23_PYTHON` at it.**

**Field 11 (`/proc/<pid>/maps`) cannot be checked here** — it needs a running service, so it
belongs to the cutover window (§5.2 check 6, §10 step 5).

**NO FALLBACK — at any step, for any reason.** If a step fails, the runtime is **not
provisioned** and the window is not scheduled.

| forbidden fallback | why |
|---|---|
| `/home/odbadmin/.pyenv/versions/py311` | the **rejected P1** design (§4.3) arriving by a side door |
| the **`woa23c1ro`** installation | another account's runtime, **not assumed usable by `odbadmin`**, and not owned by the production account |
| `$APP_ROOT/dev2026/.venv`, if it was created by mistake | it is the **wrong venv** — the evidence of a failed step, not a substitute for it |
| any interpreter found on `PATH` | `PATH` under PM2 is not a login shell's, which is why `WOA23_PYTHON` is named explicitly |
| **`woa23c1ro`'s package cache** | a different account's cache — **not assumed readable, not assumed complete for this lock**, and never a substitute for `odbadmin`'s own (§4.4a) |
| **downloading packages to get past the cache gate** | §4.4a — dropping `--offline` or `--locked`, or re-locking, is forbidden. **Populating the cache from an approved source is a separate authorisation** |

`production_app.sh` **fails closed**: `WOA23_PYTHON` has **no default**, so an unprovisioned
runtime refuses to start within seconds of `pm2 start` rather than serving from somewhere
unintended. **That refusal is the designed behaviour, not a problem to work around.**

### 4.5 The package set — 60 locked distributions

Built from the subject's `uv.lock` (digest above). The full locked set:

```
annotated-types anyio asciitree bokeh certifi click cloudpickle colorama contourpy dask
deprecated distributed fastapi fasteners fsspec gunicorn h11 httpcore httpx idna
importlib-metadata jinja2 locket lz4 markupsafe msgpack narwhals numcodecs numpy orjson
packaging pandas partd pillow polars psutil pyarrow pydantic pydantic-core python-dateutil
pytz pyyaml six sortedcontainers starlette tblib toolz tornado typing-extensions
typing-inspection tzdata urllib3 uvicorn woa23-bench2026 wrapt xarray xyzservices zarr zict
zipp
```

The versions the application actually depends on are pinned to production's:
`numcodecs==0.15.1`, `numpy==2.2.4`, `pandas[pyarrow]==2.2.3`, `polars==1.27.1`,
`xarray==2025.3.1`, `zarr==2.18.6`, `fastapi==0.115.12`, `starlette==0.46.2`,
`uvicorn==0.34.1`, `gunicorn==23.0.0`, `orjson==3.11.4`, `pydantic==2.11.3`.

**`dask` and `distributed` REMAIN in `pyproject.toml` and in `uv.lock`, and they WILL be
installed into the production venv. They are NOT removed from the package set, and this plan
must not be read as saying they are.** What is true, and all that is true, is narrower: **the
serving path does not import them.** They are benchmark-harness dependencies
(`bench/zarr_bench.py` drives the `distributed` mode for the *before* side of the S1
comparison). Verified: nothing under `dev2026/api/` imports them; the only occurrences are
comments recording that Dask was removed. **Removing them from the production venv would
require editing `pyproject.toml` and re-locking — a change to the subject, hence a successor
subject. It is deliberately NOT bundled here**, and it is recorded so their presence in the
venv is not later mistaken for the application still using Dask.

### 4.6 Runtime divergence and the attribution limitation

**Recorded, not resolved, and NOT a reason to re-run anything.**

| | |
|---|---|
| today's production | **3.11.4**, the shared pyenv interpreter, no venv |
| every candidate run in this campaign, and **G1** | **3.11.14**, under the **`woa23c1ro`** staging account |
| after cutover | **3.11.14**, a standalone uv-managed CPython at `$UV_PYTHON_ROOT` under **`odbadmin`** (§4.0) |

**The chosen version matches the evidence and differs from today's production.** The serving
patch level will be the one G1 and every candidate measurement ran on; it will **not** be
production's current 3.11.4. **That is a real change to the interpreter, and it is recorded as
one** — it is item 5 of §0A, not a detail.

**ATTRIBUTION LIMITATION — stated so no later reading overclaims.** Matching the version is
**not** the same as having tested the deployment. G1's 48 assertions and every candidate
measurement were produced under **`woa23c1ro`**, from a **differently-provisioned**
installation, against a **synthetic** store. They establish the **behaviour of the code on
3.11.14**; they do **not** establish that this deployment — a different installation, a
different account, the real store, production's PM2 — behaves the same. **§13's smoke checks
are the first execution of `143bf8c` on the production-side runtime**, and the plan says so
rather than treating the offline evidence as covering it.

**This does NOT require re-running C1 or C2, and no performance test is implied.** C1/C2
answer a different question, and re-running them would add a second variable to a cutover
that has one. **The divergence is recorded and carried; it is not converted into more work.**

---

## 5. Configuration and live-definition adoption

The proposed config is `dev2026/deploy/ecosystem.production.config.js`, already in the
subject and **not installed**.

Adoption sequence: the live definition is **captured first** (§11), the new config is placed,
and PM2 is made to adopt it by a **delete-then-start of the single named app** — not
`reload`, not `restart`, and never `all` or a wildcard.

**Configuration values, under the confirmed A-move decision:**

- **`WOA23_TLS=off`.** Under A-move the application terminates no TLS. **`WOA23_TLS_KEYFILE`
  and `WOA23_TLS_CERTFILE` must not be set to the expired application certificate and key,
  and the running master must be verified to have loaded neither** — the same
  ABSENT-from-`/proc` check every candidate run in this campaign already used. The
  placeholder values in `dev2026/deploy/ecosystem.production.config.js` are therefore **not
  promoted to real values; they are removed from the effective configuration.** §8.10.
- `WOA23_ZARR_STORE=/home/odbadmin/python/woa23/data` — confirmed by D-3 preflight against
  the real store, and re-confirmed at §2.7.

### 5.1 THE COMMITTED CONFIG CANNOT BE PLACED AS-IS — three required deltas

**Read from `dev2026/deploy/ecosystem.production.config.js` at `143bf8c`. Two of these are
defects that would stop or misconfigure the cutover, and they are stated here rather than
discovered in the window.**

| # | delta | why it is REQUIRED |
|---|---|---|
| **1** | **ADD `WOA23_PYTHON: '/home/odbadmin/python/woa23-143bf8c/.venv/bin/python3.11'`** | **The committed `env` block does not set it at all.** `production_app.sh` requires it with **no default** and dies with *"WOA23_PYTHON is not set."* **The application would not start.** Fail-closed, so it is safe — but it is a hard stop, not a warning |
| **2** | **ADD `WOA23_TLS: 'off'`** | **The committed `env` block does not set it.** The launcher reads `${WOA23_TLS:-on}`, so **unset means TLS ON** — it would find the old key and certificate readable and start **with TLS**, i.e. **B-keep**. That directly contradicts the confirmed **A-move** decision, and nginx would then be speaking plaintext to a TLS socket |
| **3** | **REMOVE `WOA23_TLS_KEYFILE` and `WOA23_TLS_CERTFILE`** | Under `WOA23_TLS=off` the launcher resolves, reads and stats **neither** path. Leaving them set would be inert but misleading — the config would name TLS material the service does not use, which is precisely the kind of "evidence that reads as proof" this campaign has been bitten by |

**Delta 2 is the more dangerous of the two, because it fails SILENTLY in the safe
direction and then breaks the route.** Delta 1 refuses loudly within seconds of
`pm2 start`; delta 2 would produce an application that looks healthy and a `/api/woa23` that
does not work.

**The configuration placed at cutover therefore differs from the file committed at
`143bf8c` in three respects.** That difference **must be visible in the diff the operator
reviews**, and the placed file's sha256 must be recorded in the §11 bundle. **Amending the
committed file instead would require a successor subject; this plan does not propose one, and
the artifact remains `143bf8c`.**

### 5.2 The exact config diff, its backup, its verification and its rollback

**The candidate PM2 config is placed as a NEW file; the old one is never edited in place.**
`conf/ecosystem.config.js` (sha256 `ed5dec6ca064bd54cfe4a45f33fd416871859e959671164292b21549b96f2159`,
384 bytes) stays byte-for-byte where it is — §11 records it and §12 restores from that record.

**The diff below is against `dev2026/deploy/ecosystem.production.config.js` as committed at
`143bf8c`, and it is applied to the COPY that is placed, never to the committed file.**

```diff
       env: {
         WOA23_PORT: '8050',
         WOA23_ZARR_STORE: '/home/odbadmin/python/woa23/data',
-        WOA23_TLS_KEYFILE: '/home/odbadmin/python/woa23/conf/privkey.pem',
-        WOA23_TLS_CERTFILE: '/home/odbadmin/python/woa23/conf/fullchain.pem',
+        WOA23_TLS: 'off',
+        WOA23_PYTHON: '/home/odbadmin/python/woa23-143bf8c/.venv/bin/python3.11',
         WOA23_WORKERS: '2',
       },
```

**Three changes, and nothing else in the file changes.**

| # | change | if it is omitted |
|---|---|---|
| 1 | **`+ WOA23_TLS: 'off'`** | the launcher reads `${WOA23_TLS:-on}` -> **TLS ON**, i.e. B-keep. nginx would then speak plaintext to a TLS socket and `/api/woa23` breaks. **Fails silently in the safe direction, then breaks the route** |
| 2 | **`+ WOA23_PYTHON: '…'`** | `production_app.sh` dies with *"WOA23_PYTHON is not set."* — **the app does not start**. Loud, immediate, fail-closed |
| 3 | **`- WOA23_TLS_KEYFILE` / `- WOA23_TLS_CERTFILE`** | inert under TLS-off (the launcher never resolves, stats or opens them), but the config would name TLS material the service does not use |

**CHANGE 3 IS A CONFIGURATION EDIT ONLY. IT DELETES NOTHING ON DISK.**
`/home/odbadmin/python/woa23/conf/privkey.pem` and `conf/fullchain.pem` are **not deleted, not
moved, not renamed, not chmod-ed**. Removing the two variables removes them from *this
candidate configuration*; the files stay exactly where they are because **rollback restarts
the old application, which cannot start without them** (§8.11, §11 item 9).

**Backup and hashes — recorded in the §11 bundle before the placement:**

| what | recorded |
|---|---|
| `conf/ecosystem.config.js` (the live one) | byte copy + sha256 `ed5dec6c…2159` |
| the live PM2 definition (`pm2 jlist`) | verbatim |
| `dev2026/deploy/ecosystem.production.config.js` **as committed** | sha256, from the extracted archive |
| **the placed config, after the three edits** | **sha256 of the exact bytes placed** — this is the value every later check compares against |
| old app certificate and key | paths, owner, mode, size, **sha256 only — the key's content is NOT copied** |

**Verification, after `pm2 start` and before anything is called good:**

| # | check | expected |
|---|---|---|
| 1 | sha256 of the placed config file on disk | equals the recorded placed-config digest |
| 2 | `/proc/<master>/environ` | `WOA23_TLS=off`, `WOA23_PYTHON=/home/odbadmin/python/woa23-143bf8c/.venv/bin/python3.11`, `WOA23_PORT=8050`, `WOA23_ZARR_STORE=/home/odbadmin/python/woa23/data`, `WOA23_WORKERS=2` |
| 3 | `/proc/<master>/environ` **and every worker's** | **`WOA23_TLS_KEYFILE` and `WOA23_TLS_CERTFILE` are ABSENT** — not empty, absent |
| 4 | master argv | **no `--certfile`, no `--keyfile`**, no `--reload`, bound to `127.0.0.1:8050` |
| 5 | launcher stdout in the PM2 log | the line `TLS : OFF (explicitly)` |
| 6 | §4.2 fields 1–12 | as specified there — including field 11, **zero** occurrences of `/home/odbadmin/.pyenv` in the master's **or any worker's** `maps`, and field 12, `$APP_ROOT/dev2026/.venv` **absent** |
| 7 | `$VENV/bin/python3.11` | **exists and is executable by `odbadmin`** — the account the service runs as |

**Check 3 is the one that proves change 3 took effect**, and checks 3–5 together are what
distinguish a genuine TLS-off start from a config that merely *looks* right.

**Rollback for the configuration, specifically:**

| # | step | operator |
|---|---|---|
| 1 | `pm2 delete woa23` — named app only | [app] |
| 2 | restore `conf/ecosystem.config.js` from the §11 bundle; **verify sha256 `ed5dec6c…2159`** | [app] |
| 3 | `pm2 start conf/ecosystem.config.js --only woa23` | [app] |
| 4 | confirm master argv shows `woa23_app:app` **with `--keyfile`/`--certfile`** — the old app is TLS-on and its certificate and key are still in place | [app] |

**The placed candidate config is left on disk during rollback, not deleted** — it is evidence
of what was attempted. Removing it is a later, separate cleanup.

**This configuration rollback is only half a rollback.** It must be paired with the nginx
restore (§10A.6), in the order §12 gives.

### 5.3 The install path is DECIDED

`APP_ROOT` = **`/home/odbadmin/python/woa23-143bf8c`** — see §4.0 for the full path set.

It sits **beside** the live tree `/home/odbadmin/python/woa23` (§10 step 2), names the
subject, and **touches nothing that exists**. `WOA23_PYTHON`, `cwd` and `script` are therefore
all resolved:

| PM2 field | value |
|---|---|
| `cwd` | `__dirname + '/..'`, which resolves to **`$APP_ROOT/dev2026`** once the archive is extracted under `$APP_ROOT` |
| `script` | `./deploy/production_app.sh`, relative to that `cwd` |
| `WOA23_PYTHON` | **`$VENV/bin/python3.11`** |

**`cwd` must be `$APP_ROOT/dev2026`, not `$APP_ROOT`** — `python -m gunicorn api.app:app` puts
`cwd` on `sys.path`, and `api/` lives under `dev2026/`. From one level up `api` is not
importable, and PM2 would report `online` while gunicorn died at import. That is the B4
failure mode arriving through a different door (§5).

`cwd: __dirname + '/..'` and `script: './deploy/production_app.sh'` are load-bearing: from
the repository root `api` is not importable, so PM2 would report `online` while gunicorn died
at import.

---

## 6. Migration from `woa23_app:app` to `api.app:app`

This is the substantive change.

**PREFLIGHT CORRECTION — this section was written against the pre-change configuration.**
Measured at preflight: `pre_stop` is **`None`** in the live PM2 definition and appears
**0 times** in `/home/odbadmin/python/woa23/conf/ecosystem.config.js` (sha256 `ed5dec6c…2159`,
384 bytes). **There is no `pre_stop` to remove.**

**`B1-stageA` recorded the pre-change configuration correctly; Stage B later removed the dead
`pre_stop` line.** Both records are accurate for their moment — it is this plan's §6 that was
written as though Stage A were still current. The paragraphs below are retained as the
rationale for why the proposed config carries none, not as a description of production today.

**What the preflight found instead is a different and still-live concern (§14):** PM2 tracks
`bash conf/start_app.sh` (pid 1828351), and the gunicorn master (1828352) is its **child**.
The proposed launcher `exec`s, so PM2 would track the master directly — a real change to the
stop path, which is why B1 validation for the new tree is still required.

**A THIRD change, measured and not previously stated: production runs with `--reload`.** The
live argv is `… gunicorn woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker -b
127.0.0.1:8050 --keyfile conf/privkey.pem --certfile conf/fullchain.pem --timeout 120
--reload`. The proposed configuration carries no `--reload`, and §10 step 5 checks for its
absence. **That is a behaviour change beyond the response fix** and must be authorised as
one, not discovered at cutover. Production also runs the **shared pyenv 3.11.4 interpreter
directly, with no venv and no `WOA23_*` environment variables at all** — so §4's move to a
dedicated venv with explicit `WOA23_*` values is a larger change than "same runtime, new
code".

The historical rationale, retained:

```
pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
```

**It is removed, not replaced.** Three independent faults: it matches a *command-line string*
rather than processes this app owns (so it can kill a second copy's master and workers);
`kill -9` cannot be caught, so a worker mid-response is destroyed rather than drained; and it
**matches itself** — `ps -ef` lists the very shell running it and `grep -v grep` does not
exclude it. After cutover it would also match a name the service no longer has, and would
quietly stop doing anything.

Removal is safe *because* `production_app.sh` `exec`s gunicorn, so PM2 tracks the master
directly and its own stop signal reaches it — demonstrated end to end by `pm2B`.

**The process tree therefore changes shape at cutover**, which is why §14 exists.

---

## 7. Port 8050

Production keeps **8050**, and the new application binds **`127.0.0.1:8050`** — loopback
only, exactly as today.

**Under A-move the socket becomes plaintext HTTP.** The launcher's
certificate-readability-before-binding check does not apply when `WOA23_TLS=off`; there is no
certificate to read. The new app must be confirmed to be the **sole** listener after start,
by pid and starttime.

**Sequencing consequence, and it is the single most important ordering constraint in this
plan:** while the OLD application is running it speaks **TLS** on 8050, and while the NEW
application is running it speaks **plaintext**. nginx can only be correct for one of them.
**The nginx scheme change and the application swap must therefore happen inside the same
outage, in the order given in §10** — neither may be applied on its own.

---

## 8. TLS — audited, decided, and now an A-move architecture

| | |
|---|---|
| production today | serves TLS on 8050 |
| every candidate run in this campaign | ran **`WOA23_TLS=off`**, with key and certificate verified **ABSENT** from master and workers |
| therefore | **no TLS behaviour of `143bf8c` has ever been exercised, anywhere** |

### 8.1 The paths were NOT known when this section was written — they are now MEASURED

`dev2026/deploy/ecosystem.production.config.js` carries
`/home/odbadmin/python/woa23/conf/{privkey,fullchain}.pem`, and says of them: *"a
PLACEHOLDER for the install location … must be confirmed against the host before any
cutover; it is not confirmed today."*

**PREFLIGHT UPDATE.** The paths production actually uses were read from the **live process
argv** resolved against the live `pm_cwd` (§8.4) — they coincide with the placeholder values,
but they are recorded because the running process uses them, **not** because the placeholder
said so. **The placeholder is confirmed as a matter of fact, not promoted as a matter of
assumption.**

**Confirming the paths did not unblock TLS**: the certificate at those paths is expired
(§8.4).

**SUPERSEDED BY THE A-MOVE DECISION (§8.10).** These paths are no longer inputs to the
cutover — the new application loads **neither** of them. They are retained here because the
**old** application still uses them and **rollback depends on them** (§8.11), so they must be
preserved rather than forgotten.

### 8.2 PREREQUISITE P-TLS — a separate, read-only VM24 preflight

**Not part of the cutover window and not authorised by this plan.** It reads; it changes
nothing.

| # | read | recorded |
|---|---|---|
| 1 | the live PM2 definition's TLS env values — the paths production *actually* uses | exact paths |
| 2 | for each: existence, **realpath**, regular-file, owner, group, mode | verbatim |
| 3 | readability **by the production account** (`test -r`), never by widening permissions | yes/no |
| 4 | certificate subject, issuer, **notBefore/notAfter** | verbatim |
| 5 | **hostname / SAN** covering the name clients use | the SAN list |
| 6 | **key/certificate pair match** — modulus or SPKI digest of both, compared | equal / not |
| 7 | key file mode is not world- or group-readable | mode |

**Deterministic outcomes.** P-TLS PASSES only if both files exist, are regular, are readable
by the production account, the pair matches, the SAN covers the served name, and the
certificate is inside its validity window. **Any other outcome blocks the cutover** — no
`chmod`, no `chown`, no ACL change, no re-issue under this plan.

The confirmed paths from P-TLS replace the placeholders in the config **before** the config
is placed. **The cutover may not be authorised until P-TLS has passed and its values are
written into this plan.**

**SUPERSEDED BY §8.10.** P-TLS was specified for a branch-B world in which the application's
own certificate is what matters. Under A-move it is not: the placeholders are **removed**
rather than replaced, and the checks that gate the cutover are §8.9's four gates against the
**public** certificate. **This subsection is retained as the record of what was asked and
why, not as a live requirement.**

### 8.4 P-TLS RESULT — **BLOCKED**, and how each blocker later closed

Performed read-only. Paths taken from the **live process argv** resolved against the live
`pm_cwd`, not from any repository or placeholder:

```
certificate /home/odbadmin/python/woa23/conf/fullchain.pem   odbadmin:odbadmin 644  5603 B
private key /home/odbadmin/python/woa23/conf/privkey.pem     odbadmin:odbadmin 644  1704 B
both are regular files, neither is a symlink, both readable by the production account

subject   CN = eco.odb.ntu.edu.tw
issuer    C = US, O = Let's Encrypt, CN = R3
notBefore May 27 23:22:04 2023 GMT
notAfter  Aug 25 23:22:03 2023 GMT      <-- EXPIRED
SAN       DNS:eco.odb.ntu.edu.tw
pair match YES (public-key sha256 373aeb23…82a1 on both)
```

| blocker | |
|---|---|
| **B-TLS-1** | **the certificate is EXPIRED** — `notAfter` is 2023-08-25, over three years ago; `checkend 0` returns NO. "Currently valid" fails outright |
| **B-TLS-2** | **the official production hostname is not explicitly known.** The SAN covers exactly `eco.odb.ntu.edu.tw`; the repository also mentions `api.odb.ntu.edu.tw` and `www.odb.ntu.edu.tw`, and no document states which is official. SAN coverage is therefore **unestablished, not failed** — and it is not decided by guessing |

**Observation, offered as a question:** the service binds **loopback only** (`127.0.0.1:8050`)
while presenting a three-year-expired certificate there. A separate public listener
terminating TLS in front would explain both facts — but **I did not look for one and do not
assert it exists.** If it does, the certificate that matters is *its* certificate and P-TLS
is examining the wrong file, which is itself worth settling before cutover.

**Recorded, not changed:** the private key is mode **644** — world-readable. No `chmod`,
`chown`, ACL change, renewal or modification was performed.

**To unblock:** (1) the official public hostname, stated explicitly; (2) a decision on the
expired certificate — whether TLS is terminated elsewhere and, if so, which certificate and
key the cutover must verify.

**BOTH BLOCKERS ARE NOW CLOSED — and the record above is kept as written, because how they
closed matters.**

| blocker | how it closed |
|---|---|
| **B-TLS-2** hostname | **stated by the owner: `eco.odb.ntu.edu.tw`.** Not inferred, not chosen by frequency or SAN order |
| **B-TLS-1** expired certificate | **closed by REMOVAL, not by renewal.** Under A-move the application terminates no TLS, so this certificate stops being a dependency of the cutover. **It was not renewed, replaced or fixed** |

**This distinction is load-bearing and must not be compressed into "TLS resolved".** The
expired certificate still exists on disk, is still mode 644, and is still required for
rollback (§8.11). What changed is that the cutover no longer depends on it.

**The certificate the cutover now verifies is the PUBLIC one** for `eco.odb.ntu.edu.tw`
(§8.9 gate 3) — which the audit measured as currently valid. **P-TLS as originally specified
examined the wrong file**, exactly as §5.2 of the preflight suspected it might.

### 8.5 TLS TOPOLOGY — audited read-only; the live shape is a HYBRID

Full audit: [`D4-tls-topology-audit.md`](D4-tls-topology-audit.md). Measured, not inferred.

```
client ──TLS──▶ nginx 0.0.0.0:443            public terminator, VALID certificate
                  │  location /api/woa23 { proxy_pass https://woa23api; }
                  ▼  upstream woa23api { server 127.0.0.1:8050; }
              gunicorn 127.0.0.1:8050        SECOND TLS, EXPIRED certificate, UNVERIFIED
```

| | |
|---|---|
| public terminator | **nginx** (pid 1088337, root), active site `sites-enabled/vm124.conf` → `sites-available/vm124-final.conf` |
| `server_name` | `eco.odb.ntu.edu.tw` |
| public certificate | `/etc/letsencrypt/live/eco.odb.ntu.edu.tw/fullchain.pem` → `archive/…/fullchain40.pem` — **VALID** `Jul 13 2026 → Oct 11 2026`, issuer Let's Encrypt YR2, SAN `DNS:eco.odb.ntu.edu.tw` |
| app certificate | `/home/odbadmin/python/woa23/conf/fullchain.pem` — **EXPIRED 2023-08-25**, pair match YES |
| upstream verification | **none** — no `proxy_ssl_verify` in scope for the WOA23 locations, so nginx's default `off` applies |
| apache2 | running, but **no reference to 8050** — does not serve this API |

**The app's expired certificate is ACTIVE, not stale** — it serves the nginx→app hop. It
works only because nginx does not verify it.

### 8.6 The two architecture branches — **A-move DECIDED**

| branch | description | evidenced? |
|---|---|---|
| **A** | reverse proxy terminates TLS; app runs loopback HTTP, TLS off | **NO** — `proxy_pass https://` would fail against a plaintext socket |
| **B** | the app terminates TLS and needs a valid, correctly protected cert/key | **PARTLY** — the app does terminate TLS, but with an expired certificate and a `644` key |

**Neither alone describes the live system. The evidenced architecture is a hybrid:** public
TLS at nginx (valid) plus a re-encrypted, unverified internal hop (expired).

**Constraint this places on the cutover:** every candidate run in this campaign used
`WOA23_TLS=off`. **The new app cannot simply run TLS-off** — `/api/woa23` would break.

| option | change | consequence |
|---|---|---|
| **B-keep** | new app terminates TLS on 8050, as today | smallest change; **perpetuates the expired certificate and the `644` key** |
| **A-move** | new app runs plaintext **and** nginx changes to `proxy_pass http://woa23api` | removes the second TLS layer and both findings; **changes nginx**, which is outside this plan's scope and needs its own authorization and rollback |

**DECIDED BY THE OWNER: A-move.** See §8.10 for the target topology and §10A for the nginx
operation it requires.

**Consequence for the proposed config:** it was written for **B-keep** (it carries TLS
key/certificate placeholders). Under A-move those values are **removed from the effective
configuration** and `WOA23_TLS=off` is set — §5. **The configuration placed at cutover
therefore differs from the file committed at `143bf8c`, and that difference must be visible in
the diff the operator reviews.**

### 8.7 Security findings — separate authorizations, kept OUT of the cutover

| # | finding | classification |
|---|---|---|
| **S1** | the **app** private key `conf/privkey.pem` is mode **644** — world-readable — and is **ACTIVE** on the internal hop | active finding; **separate permission-remediation authorization**. Not the public terminator's key, so it fits neither category the review anticipated, and is stated exactly rather than forced into one |
| **S2** | the **public** TLS private key is **readable by the production account** — `/etc/letsencrypt/{live,archive}` are `root:root` `710` but `odbadmin` is in the **`root` group**; the `live/` symlinks are `777` | **more serious than S1**; separate authorization, and remediation must consider what else depends on that group membership |

Neither is fixed here. Neither is bundled into the cutover.

**Both remain open after the A-move decision, and their status has been worked out
explicitly:** **S1 -> §8.11** (becomes retirable at cutover, but is *preserved* for rollback
and retired later under separate authorization); **S2 -> §8.12** (unchanged by this cutover;
**requires explicit risk acceptance**, not remediation-before-cutover).

### 8.8 The hostname — **`eco.odb.ntu.edu.tw`, stated by the owner**

| candidate | evidence |
|---|---|
| **`eco.odb.ntu.edu.tw`** | `server_name` of the **live active** block carrying `location /api/woa23`; SAN of the valid public certificate |
| `api.odb.ntu.edu.tw` | 5 repository mentions; **not** a `server_name` in the active config |
| `www.odb.ntu.edu.tw` | 1 repository mention; **not** a `server_name` in the active config |

**SELECTED BY THE OWNER: `eco.odb.ntu.edu.tw`.** Stated explicitly, not chosen by me and not
derived from frequency, SAN order or repository mentions.

**SAN coverage is therefore now ESTABLISHED**, and it is a match rather than an assumption:
the valid public certificate's SAN is exactly `DNS:eco.odb.ntu.edu.tw`, which is the stated
official hostname and the `server_name` of the live active block carrying
`location /api/woa23`. `api.odb.ntu.edu.tw` and `www.odb.ntu.edu.tw` are **not** the official
hostname and are not used by any check in this plan.

**All external verification in this plan addresses `eco.odb.ntu.edu.tw` by name**, with normal
certificate validation — §8.9 gate 4, §13.

### 8.9 Execution-time gate and post-cutover verification — REVISED FOR A-MOVE

**Superseded in part.** This section originally required a TLS request to 8050 presenting the
application certificate. **Under A-move there is no TLS on 8050 and no application
certificate**, so that check is not merely unnecessary — it would be wrong, and passing it
would mean the cutover had failed.

What replaces it:

| # | gate | expected |
|---|---|---|
| 1 | the application master's `/proc` argv and environ | **no** `--certfile`, **no** `--keyfile`, `WOA23_TLS=off`; certificate and key paths **ABSENT** from master and every worker |
| 2 | the 8050 socket | serves **plaintext HTTP** on `127.0.0.1:8050`; a TLS handshake attempt to it **fails**, which is the correct outcome |
| 3 | **public TLS at `https://eco.odb.ntu.edu.tw`** | the certificate presented is the nginx certificate recorded at §8.5 — matched by **public-key sha256 `eb7dc653...7c19`** — and is **currently valid** |
| 4 | external verification method | the **real hostname**, **normal certificate validation**. **`--insecure` / `-k` / disabled verification is forbidden** — it would make the check vacuous, since an unvalidated fetch cannot distinguish a correct certificate from a wrong one |

**The certificate to verify after cutover is the PUBLIC certificate for
`eco.odb.ntu.edu.tw`**, not any application certificate.

**Until gates 1-4 pass, nothing here may be described as TLS validated.**

### 8.10 DECISION — A-move, adopted; the exact target topology

**Confirmed by the owner.** Recorded as a decision, with the measured configuration it acts
on.

```
TODAY      client --HTTPS--> nginx :443 --https://woa23api--> 127.0.0.1:8050  (app TLS, EXPIRED)
PROPOSED   client --HTTPS--> nginx :443 --http://woa23api---> 127.0.0.1:8050  (app TLS-off)
```

| | |
|---|---|
| official production hostname | **`eco.odb.ntu.edu.tw`** |
| public TLS terminator | **nginx** — unchanged, stays the terminator |
| public certificate | `/etc/letsencrypt/live/eco.odb.ntu.edu.tw/fullchain.pem` — unchanged, **not touched by this plan** |
| the active site | `sites-enabled/vm124.conf` -> `sites-available/vm124-final.conf`, `server_name eco.odb.ntu.edu.tw` |
| the routes file to change | `/etc/nginx/conf2.d/routes-vm124.conf` (`root:root` `644`, 5914 bytes at audit) |
| the upstream | `upstream woa23api { server 127.0.0.1:8050; }` in `conf2.d/upstreams-vm124.conf:59` — **unchanged; the upstream block itself is not edited** |
| the application | `WOA23_TLS=off`, plaintext on `127.0.0.1:8050` |

**Explicitly required, and each is independently checkable:**

1. **The new app runs `WOA23_TLS=off` and must load neither the expired application
   certificate nor the application private key.** Verified from `/proc` argv and environ of
   the master and of every worker — ABSENT, not merely "not configured".
2. **The old application certificate and key are PRESERVED.** `conf/fullchain.pem` and
   `conf/privkey.pem` are **not deleted, not moved, not renamed and not chmod-ed** by this
   plan or during the window. They are required for **rollback**: recovery restarts the old
   application, and the old application terminates TLS with them. **They may be retired only
   when rollback is no longer possible**, and that retirement is a separate, later
   authorisation (§8.11).
3. **S1 remains a security finding** — see §8.11.
4. **S2 remains a separate security finding, and nothing in this plan touches it** — no
   `chmod`, no `chown`, no ACL change, no group-membership change. See §8.12.

**What A-move does NOT change:** the public certificate, the public key, `/etc/letsencrypt`,
any other server block, route, upstream, site, or TLS file.

### 8.11 S1 after A-move — from ACTIVE to retirable, but not retired

| | |
|---|---|
| finding | `/home/odbadmin/python/woa23/conf/privkey.pem` is mode **644** — world-readable — and its certificate expired 2023-08-25 |
| today | **ACTIVE** — the running application serves the nginx->app hop with it |
| after cutover | **no longer loaded by any process** — but **still present on disk at mode 644** |
| this plan | **preserves it** (required for rollback, above) and **does not fix it** |
| classification | **post-cutover retirement / remediation item**, separately authorised |

**Retirement has a precondition, and it is a real one:** the files must remain until rollback
to the old application is no longer possible. Deleting or chmod-ing them during the window
would remove the rollback path. The retirement step is therefore *after* the window, after
the deployment is accepted, and under its own authorization.

**A note offered as reasoning, not as measurement:** A-move replaces an *encrypted but
unauthenticated* loopback hop with a *plaintext* loopback hop. That is not a reduction in the
effective protection of that hop — nginx never verified the certificate, and the key
authenticating it was readable by **any local account**, whereas observing loopback traffic
requires far higher privilege. **This is an argument, not a measurement, and it is not offered
as a security claim.**

### 8.12 S2 — must it be remediated before cutover, or explicitly risk-accepted?

**Answer: explicit risk acceptance. Remediation before cutover is NOT technically required by
this change — but proceeding is a decision that must be made, not defaulted into.**

The reasoning, stated so it can be disagreed with:

| | |
|---|---|
| the finding | the **public** TLS private key `/etc/letsencrypt/live/eco.odb.ntu.edu.tw/privkey.pem` is readable by the production account: the directories are `root:root` `710`, but `odbadmin` is in the **`root` group**, and the `live/` symlinks are `777` |
| does this cutover create it? | **No.** It predates the change |
| does this cutover worsen it? | **No.** Neither exposure nor consequence changes: the same account reads the same key before and after, and that key is the public TLS key in both topologies |
| does this cutover mitigate it? | **No.** A-move touches the application's TLS, not the terminator's |
| does the cutover depend on it? | **No.** Nothing in §10 or §10A reads, writes or requires that key |

**Therefore there is no technical coupling between S2 and this cutover, and a
remediate-before-cutover requirement cannot be justified from this change.** What remains is a
governance question: **the window would proceed with a known, unremediated exposure of the
production public TLS key.** That requires **explicit, recorded risk acceptance by the owner
before the window opens** — not a silent proceed.

**Why it must not be bundled into the window even though it looks like a one-line fix.**
The production account belongs to eleven supplementary groups, `root` among them, including
`docker` and `shiny-apps`. Removing `root`-group membership, or tightening the `777` `live/`
symlinks, could affect **other services on this host** — production's `PM2_HOME` alone carries
nine applications (§3). **A remediation whose blast radius has not been measured must not ride
along inside a cutover window.** It needs its own authorization, its own preflight and its own
rollback.

**Recommendation: risk-accept S2 explicitly, proceed, and remediate as a separately
authorised follow-up. This is a recommendation; the decision is the owner's.**

---

## 9. Store access, and the integrity limitation

The new app reads `/home/odbadmin/python/woa23/data` directly (production owns it), not
through a staging symlink.

Preflight and post-cutover record the store's **metadata fingerprint** — path, size, mtime,
`LC_ALL=C` sorted, sha256. D-3's value was `abe6c212…c61806`.

**This is metadata-only and is NOT a content digest.** A same-size, same-mtime change is
invisible to it. It detects gross change and accidental writes; it does **not** establish
that store contents are unchanged, and no such claim may be made from it. **Computing a
content digest of 123005 files / 35 GB is explicitly out of scope (§16).**

---

## 10. The maintenance-window sequence

**No arbitrary minute cap.** Each step has a deterministic condition, and the window is as
long as the conditions take.

**TWO OPERATORS, TWO PRIVILEGE LEVELS.** Steps marked **[app]** are performed by the
production account (uid 1000, `odbadmin`). Step 4 is marked **[root]** and is performed by an
operator with root / nginx administration authority. **They are not the same authorisation**
— see §10A.

| # | step | operator | proceed when | abort if |
|---|---|---|---|---|
| 1 | **read-only final preflight** §2, and the recovery bundle §11 written and verified readable — **including the byte copy and sha256 of the active nginx configuration** | [app] + [root] | every item recorded, no mismatch | any mismatch |
| 2 | **prepare and verify the new runtime and config** — new tree and venv installed **beside** the live one; nothing serving yet | [app] | file-list `c436362a…`, lock digest unchanged, venv interpreter is the deployment venv, `WOA23_TLS=off` present in the config to be placed | any digest mismatch |
| 3a | **stop the old app by exact name** — `pm2 stop woa23` | [app] | master **and** every worker gone, verified by **pid + starttime**; **8050 free** | any recorded process still present after `kill_timeout` |
| 3b | `pm2 delete woa23` (named app only) | [app] | the app is absent from the live list | it is not |
| **4** | **change the nginx upstream scheme — §10A**: edit, `nginx -t`, verify active config and upstream, then **reload** | **[root]** | `nginx -t` OK, the active dump shows `http://woa23api` for the WOA23 locations and **nothing else changed**, reload completes | **any** `nginx -t` failure, **any** unexpected diff -> §10A rollback |
| 5 | **start the new app** from its new live PM2 definition — `pm2 start <new config> --only woa23` | [app] | PM2 `online` **and** the master's argv shows `api.app:app` on `127.0.0.1:8050`, **no `--reload`**, **no `--certfile`/`--keyfile`** | `online` without the expected argv — the B4 failure mode |
| 6 | **verify** — process identities, 8050 plaintext, **external HTTPS at `https://eco.odb.ntu.edu.tw`**, and the focused JSON/CSV smoke cases §13 + §8.9 | [app] | all pass | any failure -> §12 |
| 7 | **validate the new exec-based B1 stop path** — §14.1 step 3 | [app] | master and every worker gone by **pid + starttime**; 8050 free | -> §12 |
| 8 | **restart the same new application** with the predeclared command — §12 | [app] | PM2 `online`, argv as step 5, **new** (pid, starttime) identities | -> §12 recovery to the OLD application |
| 9 | **verify service and public TLS again** — §13 + §8.9, second pass | [app] | all pass | -> §12 |
| 10 | record the final state | [app] | process tree, argv, env read back from `/proc`, store fingerprint, the post-change nginx dump | — |

**If any gate fails, follow the predeclared recovery/rollback path of §12 and §10A. No
improvised retries, no re-running a step "to see", no composing a new command inside the
window.**

**Steps 3a–6 are the outage.** `stop` and `delete` are separate and both name the single app:
no `all`, no wildcard, no `save`, no `resurrect`, no manual signal, no `kill -9`.

**Between step 3a and step 5, `/api/woa23` returns 502.** That is expected and bounded: the
old app is stopped and the new one is not yet started. It is stated here so an operator does
not mistake a correct intermediate state for a failure.

**Why step 4 sits between the stop and the start, and may not be moved:** the old application
speaks **TLS** on 8050 and the new one speaks **plaintext**. Applying the nginx change while
the old app is still serving would break the live service (§10A.1); applying it after the new
app has started would leave nginx speaking TLS to a plaintext socket. **The only interval in
which the change is correct is the one in which nothing is listening on 8050.**

---

## 10A. The nginx upstream change — a SEPARATELY AUTHORISED operation

**This section describes a change to `/etc/nginx`. It is not authorised by this document, it
has not been performed, and it must not be performed now.**

### 10A.1 It must NOT be applied ahead of the window

**Applying `proxy_pass http://woa23api` before the cutover would break the live service.** The
old production application is listening on 8050 with **TLS** (measured: `--certfile
conf/fullchain.pem --keyfile conf/privkey.pem` in the live argv). nginx speaking plaintext to
a TLS socket fails; `/api/woa23` and `/api/swagger/woa23` would return errors to real users.

**The change is only correct once the old application has stopped and before the new one
starts — §10 step 4. It has no correct standalone moment.**

### 10A.2 Who performs it, and with what authority

| | |
|---|---|
| files | `/etc/nginx/conf2.d/routes-vm124.conf` — **`root:root` `644`** at audit |
| the reload | `nginx -t` and a **reload** of the master process (pid 1088337, running as **root**) |
| required authority | **root / nginx administration on VM24** |
| performed by | an **operator holding that authority**. This is a different person-or-role from the [app] steps |

**Claude does not have this authority, must not assume it, and must not attempt privilege
escalation.** The campaign's staging identity (uid 994) has no relationship to `/etc/nginx`
whatsoever. **This plan does not authorise any privilege-escalation command, and none appears
in it.**

**Measured, and recorded so it is not mistaken for an authorisation:** the production account
holds eleven supplementary groups including one that grants administrative escalation on this
host. **That is a fact about the host, not a permission granted by this plan.** The nginx
change is to be performed by the designated operator under its own explicit authorization.

### 10A.3 The exact diff — TWO lines, two locations, CONFIRMED by the owner

**The instruction says "exact one-location diff". The measured configuration has TWO
locations**, both in the same file, both pointing at the same upstream:

```
/etc/nginx/conf2.d/routes-vm124.conf

151: location /api/woa23 {
152:     proxy_pass https://woa23api;          <-- change
153:     include /etc/nginx/conf2.d/aio_cache_proxy.conf;
154: }
155:
156: location /api/swagger/woa23 {
157:     proxy_pass https://woa23api;          <-- change
158:     include /etc/nginx/conf2.d/aio_cache_proxy.conf;
159: }
```

**RESOLVED — the owner has confirmed that BOTH locations change.** The earlier
specification said "exact one-location diff"; the measured configuration has two, and the
owner has now stated that `/api/woa23` **and** `/api/swagger/woa23` both move from
`https://woa23api` to `http://woa23api`. **This open item is closed.**

It is recorded rather than deleted because the mismatch was real, and because the resolution
was a decision, not a discovery.

**Changing only `/api/woa23` would leave `/api/swagger/woa23` speaking HTTPS to a plaintext
socket — the Swagger UI route would break at cutover.** The two locations serve the same
application on the same upstream, so under A-move they must move together.

**The minimal correct change is therefore: ONE file, TWO lines, TWO location blocks,
`https` -> `http`, nothing else.**

```diff
--- routes-vm124.conf
+++ routes-vm124.conf
@@ location /api/woa23 {
-    proxy_pass https://woa23api;
+    proxy_pass http://woa23api;
@@ location /api/swagger/woa23 {
-    proxy_pass https://woa23api;
+    proxy_pass http://woa23api;
```

**The two-line scope is CONFIRMED by the owner.** `/api/swagger/woa23` moves with
`/api/woa23`; it is not excluded.

**And it must not be applied now.** §10A.1 — the old application is still listening with TLS
on 8050, so an early change breaks the live service. The only correct moment is §10 step 4,
after the old app has stopped, performed by the `[root]` operator.

**Explicitly forbidden in this change:** any edit to another `location`, another `server`
block, another site, another upstream, `upstreams-vm124.conf`, `ssl_snippet.conf`,
`nginx.conf`, any TLS certificate or key, any file permission, ownership or ACL. **Only the
scheme token on those two `proxy_pass` lines changes.**

### 10A.4 Where the backup goes — and one place it must NOT go

The pre-change byte copy of `routes-vm124.conf` goes into the **§11 recovery bundle, OUTSIDE
`/etc/nginx`**.

**Reason, measured:** `nginx.conf:138` contains `include /etc/nginx/conf.d/*.conf`. **A backup
file left in a glob-included directory with a `.conf` suffix would be loaded by nginx**, with
unpredictable effect. `conf2.d/` is included by explicit filename only, so it is not glob-
exposed — but the safe rule is simply not to leave backups anywhere under `/etc/nginx`.

**Not yet captured:** the read-only audit recorded `routes-vm124.conf`'s owner, mode and size
(`root:root` `644`, 5914 bytes) but **not its sha256**. Capturing that digest is **step 1 of
the window**, before any edit.

### 10A.5 Verification — six checks, in order, all before the change is accepted

| # | check | passes when |
|---|---|---|
| 1 | **exact diff** — `diff` the pre-change byte copy against the edited file | **exactly two changed lines**, each `https://woa23api` -> `http://woa23api`; **no other line differs**, no whitespace-only change, no reordering |
| 2 | **`nginx -t`** — syntax and configuration test, **before any reload** | `syntax is ok` **and** `test is successful`. **Any** failure aborts and nothing is reloaded |
| 3 | **active configuration verification** — dump the running configuration (`nginx -T`) after reload and compare against the pre-change dump | the **only** differences are the two `proxy_pass` scheme tokens. The include chain, `server_name eco.odb.ntu.edu.tw`, the `ssl_certificate` lines and every other route are **byte-identical** |
| 4 | **upstream protocol and target verification** | `upstream woa23api { server 127.0.0.1:8050; }` is **unchanged**, and both WOA23 locations now read `proxy_pass http://woa23api` |
| 5 | **post-change public HTTPS verification** — §8.9 gates 3 and 4 against `https://eco.odb.ntu.edu.tw` | 200 from the WOA23 route, and the presented certificate matches public-key sha256 `eb7dc653...7c19`, **with normal certificate validation — never `--insecure`** |
| 6 | **byte-for-byte rollback diff** — restore the bundle copy over the edited file in a dry comparison | the restored content is **byte-identical** to the pre-change copy and its **sha256 matches** the value captured at step 1 of the window |

**Reload, never restart.** The nginx master serves every other site on this host. A reload
keeps the listening sockets and drains workers; a restart would drop connections for
unrelated services. **`nginx -s reload` / `systemctl reload nginx` only.**

### 10A.6 Rollback for this operation

| # | step | operator |
|---|---|---|
| 1 | restore `routes-vm124.conf` **byte-for-byte** from the §11 bundle | [root] |
| 2 | confirm **sha256 equals** the value captured before the change | [root] |
| 3 | `nginx -t` | [root] |
| 4 | **reload** nginx | [root] |
| 5 | confirm the active dump shows `proxy_pass https://woa23api` for both WOA23 locations again | [root] |

**nginx rollback is coupled to application rollback** — restoring `https://` is only correct
once the **old**, TLS-speaking application is running again. §12 gives the combined order.

### 10A.7 The response cache — **AUDITED**; WOA23 IS cached and 400 IS cacheable

Full audit: [`D4-cache-audit.md`](D4-cache-audit.md). Read-only; no cache entry was opened,
purged or modified, and no API request was issued.

| | |
|---|---|
| caching enabled for WOA23? | **YES, both locations** — `proxy_cache api_proxy`, reached via the include at `routes-vm124.conf:153` and `:158` |
| `proxy_cache off` at line 170 | belongs to `location /mcp/metocean` (162–172). **Does NOT apply to WOA23** — scope was read, not assumed |
| zone / storage | `api_proxy` (8m) · `/tmp/nginx-api-cache`, `levels=1:2`, `max_size=1000m`, `inactive=600m` |
| **cache key** | **`"$http_host$request_uri"`** — host **YES**, URI **YES**, query string **YES**, **scheme NO**, upstream scheme **NO**, body **NO** |
| **is 400 cacheable?** | **YES — `proxy_cache_valid 400 404 1m`**, explicit, 1-minute freshness |
| is a 400 in the cache right now? | **UNMEASURED** — `/tmp/nginx-api-cache` is `nginx:root` mode **700**; `odbadmin` cannot traverse it. **Not inferred** |
| resolved runtime config (`nginx -T`) | **UNMEASURED** — needs root; §10A.5 check 3 already requires it from the `[root]` operator |

**THE CONSEQUENCE FOR THIS CUTOVER, stated exactly.** The cache key contains **nothing about
the upstream**, so changing `proxy_pass https://` to `http://` **does not alter a single cache
key**. Every pre-cutover entry — including any cached 400 — remains addressable by exactly the
same key afterwards. **Nothing in the window clears this cache**: not the app stop/start, not
the scheme change, not an nginx reload. `/tmp` is emptied only at boot, and host uptime is
2 weeks 5 days.

**Two paths by which a stale 400 can still be served**, from
`proxy_cache_use_stale error timeout updating …` and `proxy_cache_background_update on`:

1. **during the outage** (§10 steps 3a–5) nginx may serve a **stale cached response instead
   of 502** — including a stale 400;
2. **on the first request after cutover** for a previously-cached URI, the stale entry is
   served **to that client** while nginx refreshes in the background. The **second** request
   gets the new 200.

**Path 2 is exactly how a post-cutover smoke check could see the old 400 and read as a
deployment failure when the deployment is correct.**

### 10A.8 The intended bypass — and why it is a BLOCKER, not a solution

**No purge. No cache-entry edit. No cache-directive change.** The intended way to defeat a
stale entry uses only what the active configuration already provides.

**The mechanism, read from the active files:**

```
conf.d/api_proxy_cache.conf     map $request_method     $not_post          { default 1; POST 0; }
                                map $http_cache_control $api_cache_bypass  { default $not_post; "" 0; }
conf2.d/api_cache_proxy.conf:10 proxy_cache_bypass $request_body_file $api_cache_bypass;
conf2.d/api_cache_proxy.conf:7  proxy_no_cache     $request_body_file;
conf2.d/api_cache_proxy.conf:15 add_header X-api-cache $upstream_cache_status;
```

Read literally: a **GET** with a **non-empty `Cache-Control` request header** takes the map's
`default`, which is `$not_post` = 1, so `proxy_cache_bypass` is true and nginx **does not
serve from the cache**. `$request_body_file` is empty for a GET, so `proxy_no_cache` is false
and the fresh response **is stored**, replacing the stale entry.

**THAT READING IS NOT PROOF, AND IT IS NOT TREATED AS PROOF.**

| # | why it is unproven | what would close it |
|---|---|---|
| 1 | the **resolved running configuration is UNMEASURED** — `nginx -T` needs root. Everything above is read from static files. What the running master holds has not been seen | the `[root]` operator's `nginx -T` dump, §10A.5 check 3 — which the window already requires |
| 2 | **the behaviour has never been observed.** No API request may be issued, so no response has ever been seen carrying `X-api-cache: BYPASS`. The chain map -> variable -> directive is a reading of nginx semantics, i.e. **an inference** | one observed request inside the window, §13 |
| 3 | **`add_header` here has no `always`.** nginx adds the field only for 200/201/204/206/301/302/303/304/307/308. **A 400 response therefore carries NO `X-api-cache` header at all** | nothing to fix — but the smoke procedure must not depend on reading the header off a 400 |

**Finding 3 corrects an earlier statement in this plan.** A previous revision proposed reading
`X-api-cache: HIT` or `STALE` off a returned 400 to identify a cache result. **That header
will not be present on a 400**, so that discriminator does not exist. §13 uses one that does.

**STATUS: `CACHE-BYPASS-UNPROVEN` — a BLOCKER.**

**The plan may not be marked execution-ready while this is open, and the bypass must not be
described as established.** It closes on two measurements, both inside the window and neither
available now: the `[root]` `nginx -T` dump (§10A.5 check 3), and the first observed
`X-api-cache: BYPASS` response (§13.1 **R2**).

**If, at the window, R2 does NOT report `BYPASS`,** the reading above is wrong: stop, do not
improvise a purge, do not edit cache directives, and treat it as a gate failure under §10
step 6.

**And a `BYPASS` on R2 does not license the converse inference.** If R3 then returns 400, that
is **`CACHE_OR_ROUTING_FAILURE`** (§13.2) — **not** "confirmed to be the cache". The bypass
being real says the cache was skipped for R2; it says nothing about **why** R3 differs.

**No cache entry is purged, edited or hand-touched by this plan.**

---

## 11. Preserving the old configuration and recovery information

Written **before** step 3, to a path outside both the old and new trees, and verified
readable before the stop:

1. `conf/ecosystem.config.js` verbatim, plus its sha256;
2. the live PM2 definition (`pm2 jlist`) verbatim;
3. the live process tree: master and worker pids **with starttimes**, full argv, and the
   environment read from `/proc/<pid>/environ`;
4. the port-8050 listener identity;
5. the old application's location and entry point (`woa23_app:app`);
6. the store metadata fingerprint;
7. the PM2 version read from `package.json`;
8. **the active nginx configuration** — and this is new, required by A-move:
   - a **byte copy** of `/etc/nginx/conf2.d/routes-vm124.conf` **and its sha256**;
   - a byte copy of `/etc/nginx/conf2.d/upstreams-vm124.conf` and its sha256 (**not edited** —
     kept so "unchanged" can be proven, not asserted);
   - the full running-configuration dump (`nginx -T`), which is the baseline for §10A.5
     check 3;
   - the `sites-enabled/vm124.conf` -> `sites-available/vm124-final.conf` symlink target;
   - the nginx master pid and starttime.
   **Stored OUTSIDE `/etc/nginx`** — see §10A.4;
9. **the old application certificate and key: paths, owner, mode, size and sha256** —
   **recorded, and the files left exactly where they are**.

**Nothing in the old tree is deleted by this plan.** The old application and its config stay
in place; recovery is a restart of what is already there.

**The old TLS certificate and key are PRESERVED for the whole window and beyond** —
`conf/fullchain.pem` and `conf/privkey.pem` are not deleted, moved, renamed or chmod-ed.
**Rollback restarts the old application, and the old application cannot start without them.**
They may be retired only when rollback is no longer possible, under a separate authorization
(§8.11).

**The private key is NOT copied into the recovery bundle.** It is world-readable at mode 644
already; copying it would create a second exposed copy. **Its sha256 is recorded; its content
is not.**

---

## 12. Failure recovery vs deployment rollback

These are different operations and the plan keeps them apart.

**Failure recovery** — the new app fails to start, fails a smoke check, or 8050 does not
serve. Restore service on the *old* application.

**Under A-move this is a TWO-OPERATOR rollback, and the nginx step comes FIRST.**

| # | step | operator |
|---|---|---|
| 1 | `pm2 delete woa23` — named app only, if the new definition was started | [app] |
| 2 | **restore `routes-vm124.conf` byte-for-byte from the §11 bundle; verify sha256; `nginx -t`; reload** — §10A.6 | **[root]** |
| 3 | confirm the active dump shows `proxy_pass https://woa23api` for **both** WOA23 locations | [root] |
| 4 | restore `conf/ecosystem.config.js` from the §11 bundle, digest-verified | [app] |
| 5 | `pm2 start conf/ecosystem.config.js --only woa23` | [app] |
| 6 | confirm 8050 serves **TLS** again and the master's argv shows `woa23_app:app` | [app] |
| 7 | confirm `https://eco.odb.ntu.edu.tw` serves the WOA23 route again, normal certificate validation | [app] |
| 8 | record the recovered tree: pids, starttimes, argv | [app] |

**Why nginx first.** Restoring `https://` before the old application starts means service is
correct the instant step 5 completes, with no second cross-operator handoff. Between steps 1
and 5 the route returns 502 — expected, and the same bounded interval as §10 steps 3a–5.

**Both halves are required.** Restoring only nginx, or only the application, leaves a
scheme mismatch and a broken route. **A partial rollback is not a rollback.**

The old tree, old venv, and the old certificate and key are untouched throughout, so recovery
does not depend on reinstalling or re-issuing anything.

**New-application restart** — used by §14.1 step 4, after the stop path has been validated.
This restores the **new** application, not the old one, and is a different command from
failure recovery. It is predeclared here so that step 4 runs something written down before
the window, not composed inside it:

| owner | step |
|---|---|
| operator (uid 1000) | `pm2 start <new config> --only woa23` — named app only, under production's `PM2_HOME` `/home/odbadmin/.pm2` |
| operator | confirm PM2 `online` **and** master argv shows `api.app:app` on `127.0.0.1:8050`, **no `--reload`**, **no `--certfile`/`--keyfile`** |
| operator | record master and worker **(pid, starttime)** pairs — they must be NEW identities, not the pre-stop ones |
| operator | §13 smoke checks and §8.9 gates, second pass |

**This restart needs NO nginx action.** The nginx configuration is already `http://` and the
application it restarts is the plaintext one — the scheme still matches. **Only a rollback to
the OLD application requires touching nginx.**

If this restart fails, the route is failure recovery to the **old** application, above — the
old definition is still preserved and untouched.

**Deployment rollback** — the new app runs but is judged unacceptable later. Same mechanism
as failure recovery, but **not an emergency**, and it needs its own authorization: it is a
second production change, and the reason for rolling back must be recorded before it
happens.

**Neither path uses SIGKILL, `pm2 kill`, `all`, a wildcard, `save`, or `resurrect`.**

---

## 13. Post-cutover smoke checks — focused only

Focused only. No C1/C2, no performance test, no case-set expansion.

**Each check is run at BOTH layers**, because A-move introduces a second thing that can be
wrong: the application, and the scheme nginx uses to reach it.

| layer | address | method |
|---|---|---|
| **loopback** | `http://127.0.0.1:8050` — **plaintext**, from the production account | proves the application |
| **public** | `https://eco.odb.ntu.edu.tw/api/woa23...` | proves the whole path, including nginx's scheme |

| # | check | expected |
|---|---|---|
| 1 | OpenAPI document served | 200, and the two `/api/woa23` routes present |
| 2 | one **non-empty JSON** query | 200, rows present |
| 3 | one **non-empty CSV** query | 200, `text/csv`, header + rows |
| 4 | the **valid empty-result pair**, same query both ways | JSON **200 `[]`**; CSV **200**, `text/csv`, **header identical to check 3's for the same query shape**, **zero data rows** |

Check 4 is the reason for this cutover, so it is checked directly rather than inferred.

**Plus the §8.9 gates:** the application master and workers carry **no** certificate or key;
8050 is **plaintext** (a TLS handshake to it fails, which is correct); and
`https://eco.odb.ntu.edu.tw` presents the nginx certificate `eb7dc653...7c19`, currently
valid.

**External verification uses the real hostname and normal certificate validation.
`--insecure` / `-k` / any disabled-verification flag is FORBIDDEN** — it cannot distinguish a
correct certificate from a wrong one, so a check that used it would be vacuous. Requests are
made **to `eco.odb.ntu.edu.tw`**, not to an IP with an overridden Host header, because the
certificate check is part of what is being verified.

### 13.1 The public layer IS cached — so each public check is issued twice

**Measured (§10A.7): WOA23 responses are cached, 400 is cacheable for 1 minute, and stale
entries can be served after the cutover.** The loopback layer is not cached at all — it
bypasses nginx entirely — so it stays authoritative for the application's behaviour.

**No purge, at any point.** The sequence below defeats a stale entry using only ordinary
cache behaviour; nothing is deleted, edited or hand-touched, and no cache directive changes.

**THREE requests, in this exact order, and the URL must be IDENTICAL across R2 and R3:**

| # | layer | request | records |
|---|---|---|---|
| **R1** | **loopback** | `http://127.0.0.1:8050/...` — plaintext, from the production account, **bypasses nginx entirely** | status · **body sha256** · response headers |
| **R2** | **public** | `https://eco.odb.ntu.edu.tw/...` — the **same URL** as R3, **with `Cache-Control: no-cache`** | status · **body sha256** · **request URL, verbatim** · response headers · **`X-api-cache`** |
| **R3** | **public** | the **byte-identical URL** of R2, **without** the header | status · **body sha256** · **request URL, verbatim** · response headers · **`X-api-cache`** |

**R1 first.** It is the only observation that reaches the application without nginx, so it is
authoritative for application behaviour and it is recorded before any public request can
confuse the picture.

**R2 and R3 must use the same URL, byte for byte** — the cache key is
`"$http_host$request_uri"`, so any difference in path, parameter order or encoding is a
**different cache entry** and the comparison would be meaningless. **Record both URLs
verbatim and compare them**; do not assume they matched because they were meant to.

**Compare five things across R1/R2/R3, not just status:** status · body **sha256** · the
request URL · the response headers · `X-api-cache`.

**`X-api-cache` on R2 is a gate.** It is the first observation that can confirm the bypass
mechanism §10A.8 marks `CACHE-BYPASS-UNPROVEN`. **If R2 does not report `BYPASS`, stop —
§10 step 6 fails.** Do not purge, do not edit cache directives, do not retry with a different
header.

**A 400 may carry NO `X-api-cache` header at all.** `add_header X-api-cache` has **no
`always`**, so nginx omits it on a 400. **Cache status therefore cannot be read off any 400
response**, and no classification below depends on doing so.

### 13.2 Classification — and what may NOT be concluded

| observed | classification | action |
|---|---|---|
| R1 200 · R2 200 · R3 200, body digests equal | **PASS** | continue |
| R1 **200** · R2 **200** · R3 **400** | **`CACHE_OR_ROUTING_FAILURE`** | **STOP.** §10 step 6 fails |
| R1 200 · R2 **400** | **`CACHE_OR_ROUTING_FAILURE`** | **STOP** |
| R1 **400** | **application failure** — the loopback path does not traverse nginx or any cache | **STOP** |
| digests differ where statuses agree | **unresolved** — do not rationalise it | **STOP** |

**`CACHE_OR_ROUTING_FAILURE` is a single, deliberately undivided classification. It must NOT
be recorded as "a cache result".**

An earlier revision of this plan concluded that R2 = 200 with R3 = 400 *is* a caching
artefact and not an application failure. **That conclusion is withdrawn — it does not
follow.** The same observation is equally consistent with a routing or upstream fault that
differs between the two requests, and `X-api-cache` cannot arbitrate because **the 400 may
carry no such header**. With the evidence available at that moment the two are **not
distinguishable**, so the plan stops rather than picking the more comfortable of them.

**Deciding between them requires evidence this step does not have** — the `[root]` operator's
`nginx -T` dump, and the `X-api-cache` values from the responses that *do* carry the header.
**Gathering that is a separate diagnostic step, not something to infer inside a gate.**

**The loopback layer stays authoritative for application behaviour**, and R1 is why it is
issued first.

Each smoke query is issued **once**, to the two addresses above and to nothing else.

---

## 14. B1 — the recommended route is validation INSIDE this window

**Production B1 is currently unvalidated and BLOCKED.** `b1s1` was a *qualified
staging-only* stop-path PASS on PM2 5.4.2 under uid 994, and the ledger records that it
**"closes NO production blocker: production B1 remains unvalidated."**

**The old B1 closure does not cover the new tree, and this plan does not treat it as if it
did.** The cutover changes exactly what B1 is about: it removes `pre_stop`, and it moves to
an `exec`-based tree where PM2 tracks the gunicorn master directly rather than a shell.

### 14.1 RECOMMENDED — B1 validated inside the same planned window

This is a *planned* sequence, declared in advance. It is **not** an improvised extra retry:
every step, its identities and its abort condition are written down before the window opens.

**This is now integrated into §10 as steps 7–9.** The table below is the same sequence stated
in B1 terms; §10 is the operational order.

| # | step | done when | abort to |
|---|---|---|---|
| 1 | **cut over** — §10 steps 1–5, **including the [root] nginx step 4** | PM2 `online` **and** master argv shows `api.app:app` on 8050 with no TLS flags | §12 recovery |
| 2 | **verify the new application** — §13 smoke checks + §8.9 gates | all pass | §12 recovery |
| 3 | **validate the new stop path** — `pm2 stop woa23` under production's `PM2_HOME` | master **and** every worker gone, confirmed by **pid + starttime**; 8050 free | §12 recovery |
| 4 | **recover the same new application** — the predeclared command from §12, run verbatim | PM2 `online`, argv shows `api.app:app` on 8050 | §12 recovery to the OLD application |
| 5 | **verify again** — §13 smoke checks + §8.9 gates, second pass | all pass | §12 recovery |

**Steps 3–5 require no nginx action**, because they stop and restart the *plaintext*
application while nginx is already configured for `http://`. **Only an abort to the OLD
application brings the [root] operator back in** — which is why §12 keeps them ordered and why
the [root] operator must remain reachable for the whole window, not only for step 4.

**Identity rules, throughout:** the exact app name read from the live list; master and worker
identity as **(pid, starttime)** pairs recorded before and compared after; **no wildcard, no
`all`, no `kill`, no SIGKILL, no `save`, no `resurrect`, no manual signal**. The old
production definition stays preserved in the §11 bundle for rollback for the whole window.

**Why inside the window.** Step 3 is the first time the new stop path is exercised anywhere,
and step 4 is the same command §12 relies on for recovery. Validating them while the
operator is present, the old definition is preserved and rollback is one command away is
strictly safer than discovering a stop-path defect later during an unplanned incident.

**What it costs:** a second short outage inside the same window, at step 3 — planned,
bounded by the same deterministic conditions, and immediately followed by step 4.

**What it still does not do:** it validates the stop path for the **new tree on production's
PM2**. If production's PM2 version differs from 5.4.2 (§3) that is *why* this validation is
worth doing, not a reason to skip it.

### 14.2 ALTERNATIVE — defer B1 to a separately authorised follow-up

Retained as an explicit option, with its cost stated rather than implied:

**Accepting this alternative means accepting that the new production tree runs while its
stop path is unvalidated.** §12's recovery uses `pm2 stop` on the named app — the same
mechanism step 3 would have exercised — so if the stop path is defective, *recovery is
affected too*, and it would be discovered during an incident rather than during a planned
window with an operator present.

This alternative requires that risk to be accepted explicitly. It is not the recommendation,
and the old B1 closure may not be cited in support of it.

---

## 15. A11 — not a gate, and not a request counter

A11 marker deltas are **observation only** and are **not** a gate for this cutover.

**Operator checks are not an exact request counter.** The smoke checks issue a known number
of requests, but production serves real traffic during and after the window, so any marker
delta reflects both. No count derived from operator activity may be reported as the number of
requests the deployment received.

---

## 16. Scope exclusions — explicit

Out of scope for this cutover, and not to be performed under its authorization:

| excluded | why |
|---|---|
| `conf/simu.sh` | **remains separate** — not modified, not read as part of the cutover |
| full **C1/C2** rerun | the change is a response-shape fix; C1/C2 measure a different question and would add a second variable |
| **performance testing** | this is a behavioural response fix, and **no performance claim is made or implied** |
| **store content digest** | 123005 files / 35 GB; §9's fingerprint is metadata-only and says so |
| unrelated retained state | `dep3m`, `dep3h`, `bs3v1`, `b1s1`, `pm2G` and all D-3 evidence are untouched |
| production B1 validation | §14 — now integrated as §10 steps 7–9 under the recommended route |
| **S1 remediation** — the app key's `644` mode, and retiring the expired app certificate/key | §8.11 — **post-cutover**, and blocked until rollback is no longer possible |
| **S2 remediation** — the public TLS key readable by the production account | §8.12 — **separate authorization**; no `chmod`, `chown`, ACL or group change in this plan |
| **any nginx change beyond the two `proxy_pass` scheme tokens** | §10A.3 — no other site, server block, route, upstream, TLS file, permission or ACL |
| **`/etc/letsencrypt`** | not read, not written, not renewed, not copied by this cutover |
| **any nginx cache purge or cache-config change** | §10A.7 — the stale-entry problem is solved by a `Cache-Control` request header, **not** by purging, editing entries, or changing cache directives |
| removing `dask`/`distributed` from the venv | §4.5 — they **remain** in `pyproject.toml` and `uv.lock` and **will be installed**; only the serving path does not import them. Removing them means re-locking, hence a **successor subject** |
| **re-running C1/C2 because of the runtime change** | §4.6 — the runtime divergence and its attribution limitation are **recorded, not converted into more work**. No performance test is implied |
| **reusing the `woa23c1ro` interpreter** | §4.3 — another account's runtime is **not assumed usable by `odbadmin`**; production-side provisioning is a required, separately authorised step |
| interpreter upgrade to 3.11.14 | §4 — deliberately not bundled |

---

## 17. The coverage gap — gate G1, RUN and PASSED offline

**The gap:** the committed focused suite exercises the empty-CSV header for one parameter and
one statistic. Multi-parameter and multi-statistic empty results follow the same code path
and the same naming rule, but are not committed as assertions.

**G1 has now been run** against the proposed artifact's own worktree at `143bf8c`, using the
subject's own deterministic synthetic store. **48 assertions, 0 failures.**

| shape | canonical header (from the non-empty response) | empty header |
|---|---|---|
| S1 1 param / 1 stat | `lon,lat,depth,time_period,temperature` | **byte-identical** |
| S2 2 params / 1 stat | `lon,lat,depth,time_period,temperature,salinity` | **byte-identical** |
| S3 1 param / 2 stats | `lon,lat,depth,time_period,temperature_an,temperature` | **byte-identical** |
| S4 2 params / 2 stats | `lon,lat,depth,time_period,temperature_an,temperature,salinity_an,salinity` | **byte-identical** |

For every shape: JSON **200** with the existing empty shape; CSV **200**; content type
`text/csv`; header byte-identical to the canonical header; **zero data rows**; **no
parameter or statistic column missing** (1, 2, 2 and 4 value columns respectively); and the
body is not a JSON error object.

**And errors are still errors:** a parameter with no data array, an unsupported
`time_period`, a malformed request (missing `lon0`) and a store failure are each **not 200**,
with JSON and CSV agreeing in every case, and the store failure not returning CSV.

**The canonical header is taken from the non-empty response at run time, never written into
the gate** — a header typed into a check is a second place for it to be wrong.

**Method, and why it needed no successor subject.** The runner lives outside `dev2026/` and
is streamed to the interpreter, so **no executable test code was added to the subject**: the
archive, file-list and subject are unchanged by running G1. It contacted no VM24, opened no
production store, issued no request to 8050, measured no performance and re-ran no C1/C2.

Evidence: [`G1-run.log`](G1-run.log) and [`G1-evidence.json`](G1-evidence.json).

**This confirms a test-coverage gap, not a behaviour gap.** Committing G1's assertions into
the suite would be a change to a test file and would require a **successor subject** and its
own focused batches; the verification itself does not, and has now been done.

---

## 18. Status

| | |
|---|---|
| this plan | **offline proposal, not executed, awaiting review and authorization** |
| VM24 | **not contacted** while writing it |
| production | **not modified**; PM2 not started, stopped or reloaded |
| C1/C2 | **not re-run** |
| performance tests | **not run** |
| retained state | **not cleaned** |
| D-3 `a361f70` observation | **not back-filled** as deployment evidence for `143bf8c` (§0) |
| claims made | **none** — not production equivalence, not cutover success, not TLS validation, not new-runtime validation |
| G1 | **RUN and PASSED offline** — 48 assertions, 0 failures, all four shapes byte-identical (§17) |
| **offline validation, `f66ddd8`** | **THREE CLEAN BATCHES.** HEAD verified before and after each, tracked dirty 0 throughout, identical totals: **REQUIRED_PASS 50 passed / 0 failed · NOT_APPLICABLE 4 · ENVIRONMENT_BLOCKED 6 (both skipped BEFORE launch, never counted as passes) · UNRESOLVED 0**. Sentinel `subject=f66ddd8… batches=3 clean=yes` |
| **`test_s2perf_driver.sh`** | **REQUIRED_PASS harness/regression validation**, 226 assertions, passed in all three batches. Local stand-in HTTP servers, synthetic delays and statuses; **no real API, no real store, no Polars behaviour, no production request.** Its ~18 minutes are the test's own cost and its timings are **NOT** production or API performance evidence |
| **production performance evidence** | **NONE, and none is claimed.** No C1/C2 rerun, no real performance test |
| **artifact identity** | **SETTLED — `f66ddd8`**, provisioned at `$APP_ROOT=/home/odbadmin/python/woa23-f66ddd8`. `143bf8c` provisioning **superseded**, preserved as historical evidence only |
| **`WOA23_PYTHON`** | `/home/odbadmin/python/woa23-f66ddd8/.venv/bin/python3.11` — fresh venv, 58 distributions, lock unchanged, `sys.base_prefix` = the standalone root |
| deployed? | **NO.** The tree, interpreter and venv are provisioned; **nothing is deployed** — no PM2 app, no `PM2_HOME`, no store symlink, no config placed, no nginx change. The sentinel proves **validation**, not deployment |
| PM2 preflight | **PASSED and MEASURED** — 5.4.2 confirmed twice, binary digest matches, daemon pid 3459 uid 1000 (§3) |
| **P-TLS** | **CLOSED** — B-TLS-2 by the owner stating the hostname; B-TLS-1 **by removal, not renewal** (§8.4) |
| TLS topology | **AUDITED** — hybrid: nginx public TLS (valid) + unverified internal TLS to the app (expired) (§8.5) |
| **architecture** | **A-move, DECIDED** — nginx stays the public terminator; the app runs `WOA23_TLS=off`; nginx proxies over loopback HTTP (§8.10) |
| **official hostname** | **`eco.odb.ntu.edu.tw`, STATED BY THE OWNER** — SAN coverage now established (§8.8) |
| **nginx change** | **PLANNED, NOT PERFORMED** — `/etc/nginx` untouched. A separately authorised [root] operation with its own six checks and byte-for-byte rollback (§10A) |
| **response cache** | **AUDITED read-only** (§10A.7, [`D4-cache-audit.md`](D4-cache-audit.md)). WOA23 **is** cached; **400 is cacheable** for 1m; the key omits the upstream, so entries carry across the cutover unchanged; whether a 400 is cached **right now is UNMEASURED** (dir is `nginx:root` 700). **No entry opened, purged or modified; no API request issued.** **No purge is planned** |
| **`CACHE-BYPASS-UNPROVEN`** | **BLOCKER (§10A.8).** The `Cache-Control` bypass is read from static files only — `nginx -T` is root-only and no request has ever been observed. **Not asserted as established.** Closes on the `[root]` operator's `nginx -T` dump plus the first observed `X-api-cache: BYPASS` (§13.1 **R2**) — **both remain cutover-window gates** |
| **`add_header X-api-cache`** | has **no `always`**, so a **400 carries no `X-api-cache` header**. **No classification may depend on reading cache status off a 400** |
| **smoke sequence** | **R1 loopback -> R2 public with `Cache-Control: no-cache` -> R3 public plain, R2 and R3 on a byte-identical URL.** Compare status, **body sha256**, request URL, response headers and `X-api-cache` (§13.1) |
| **`CACHE_OR_ROUTING_FAILURE`** | R2 200 with R3 400 is classified as **one undivided outcome and STOPS the window** (§13.2). **The earlier conclusion that it "is a cache result" is WITHDRAWN** — it does not follow, and the header cannot arbitrate on a 400 |
| cache purge | **none, at any point** |
| **nginx two-line scope** | **CONFIRMED by the owner** — `/api/woa23` **and** `/api/swagger/woa23` both move (§10A.3). Must **not** be applied before §10 step 4 |
| **runtime architecture** | **DECIDED — P2.** Standalone uv-managed **CPython 3.11.14** plus its own venv (§4.0). **P1 (venv on the pyenv base) is REJECTED** and is not an option. `/home/odbadmin/.pyenv` takes **no part** in serving — not interpreter, not stdlib, not `site-packages`, not the venv base |
| **runtime paths** | **DECIDED, no longer open** (§4.0) — `UV_PYTHON_INSTALL_DIR=/home/odbadmin/python/uv-pythons` (**uv's real knob**, a PARENT) · `UV_PYTHON_REAL_ROOT=$UV_PYTHON_INSTALL_DIR/cpython-3.11.14-linux-x86_64-gnu` (**what uv actually manages**; the suffix is **read from `uv python list`**, never guessed) · `UV_PYTHON_ROOT=/home/odbadmin/python/cpython-3.11.14-20251217` (**a SYMLINK ALIAS**, this plan's stable name, **not** a uv variable) · `APP_ROOT=/home/odbadmin/python/woa23-143bf8c` (the **deployment/release tree**) · `VENV=$APP_ROOT/.venv` (the **actual serving venv**) · `WOA23_PYTHON=$VENV/bin/python3.11` (**what PM2 and the app execute**) · `UV_CACHE_DIR=/home/odbadmin/.cache/uv` (**`odbadmin`'s own** package cache) |
| **alias vs real root** | `uv` knows **`$UV_PYTHON_REAL_ROOT`**; `sys.base_prefix` and `/proc/<pid>/exe` report it, **not** the alias. **That is correct and must not be read as a mismatch** — every identity check compares **realpaths** (§4.2 fields 2a, 2c, 8, 10) |
| **`PACKAGE-CACHE-GATE`** | **§4.4a.** `UV_CACHE_DIR` named and recorded as `odbadmin`'s own; **`woa23c1ro`'s cache is NOT assumed readable or complete**. Success is **exit 0 · lock digest unchanged · the result in the designated `$VENV`, with `$APP_ROOT/dev2026/.venv` absent**. On failure: **no download, no lock edit, no fallback, no cutover** |
| **`--dry-run` is INDICATIVE ONLY** | **MEASURED.** Against an empty cache with `--offline`, `uv sync --locked --offline --dry-run` printed *"Would download 58 packages"* and **exited 0**; the real `uv sync --locked --offline` **exited 1**, naming the first missing artifact. **Only the real sync proves the artifacts are present** (§4.4a) |
| failed-gate residue | a failed offline sync **can leave a partially-populated `$VENV`** — measured. **Its existence is not evidence of success; only exit 0 is** (§4.4a) |
| runtime verification | `$VENV/bin/python3.11` **exists and is executable by `odbadmin`** · `WOA23_PYTHON` = that path · uv's own identification · `-VV` **BUILD/version** verbatim · realpath · `sys.prefix` = `$VENV` · **`realpath(sys.base_prefix)` = `realpath($UV_PYTHON_ROOT)`** · **`/proc/<pid>/maps` for the master AND every worker with ZERO occurrences of `/home/odbadmin/.pyenv`** · **`$APP_ROOT/dev2026/.venv` absent** (§4.2 fields 1–12). **No fallback to pyenv, to `woa23c1ro`, or to a stray project venv** (§4.4) |
| **venv-location hazard** | **MEASURED and fixed in the plan.** `pyproject.toml` is at `$APP_ROOT/dev2026`, so a plain `uv sync` creates **`$APP_ROOT/dev2026/.venv`** — the wrong venv. `UV_PROJECT_ENVIRONMENT=$VENV` was **tested on that exact layout** and puts it at `$APP_ROOT/.venv`, reusing a pre-created venv rather than recreating it (§4.4) |
| **uv install-dir conflict** | **MEASURED.** `uv python install --install-dir` (env **`UV_PYTHON_INSTALL_DIR`**) names a **PARENT**; uv chooses the subdirectory name (`cpython-<version>-<platform>-<libc>`). A plain install does **not** produce `$UV_PYTHON_ROOT/bin/python3.11`. Reconciled by **V1 — symlink the decided name to uv's real directory**; moving/renaming it is rejected because it breaks uv's own recognition (§4.0) |
| uv version | **verified on uv 0.9.27 locally (macOS), NOT on 0.9.22 or on VM24** — VM24's uv version is **UNMEASURED**. §4.4 step 2 re-confirms on the host's uv, and step 8 verifies the **outcome by path**, which is version-independent |
| access requirement | **`odbadmin` must be able to traverse, read and execute** `$UV_PYTHON_ROOT`, its stdlib, `$APP_ROOT` and `$VENV` — **verified as `odbadmin`**, not as the installer (§4.0) |
| **`RUNTIME-PROVISIONING-INCOMPLETE`** | **BLOCKER (§4.3, §4.4).** The paths are decided but **the installation does not exist on VM24 yet**, and **nothing was provisioned this round**. The campaign's 3.11.14 **and its package cache** were provisioned under **`woa23c1ro`** and **neither is assumed usable by `odbadmin`**. Whether `uv` is available to `odbadmin`, and whether `odbadmin`'s cache holds the locked artifacts, are both **UNMEASURED**. Provisioning is a **required, separately authorised** production-side step that must **complete and be verified before a window is scheduled** |
| interpreter verification | five checks, **all as `odbadmin`** (§4.4e): uv recognises it · realpath + **BUILD/version** = CPython 3.11.14 · **artifact digest** · **alias resolves to the real root** · **no `/home/odbadmin/.pyenv`, no `/home/woa23c1ro`** |
| **how the interpreter is obtained** | **PRIMARY (§4.4c): operator transfers the APPROVED archive -> sha256 verified ON VM24 before install -> staged into a `file://` mirror -> `uv python install --mirror … --offline`.** `--mirror` accepts a `file://` URL offline (**measured**), and uv's own error **names the exact filename to stage**, so the release stamp and triple are **read, not guessed** |
| **ALTERNATIVE, weaker (§4.4d)** | direct `uv python install` download. **The archive is NOT retained, so no approved-archive verification is possible at all** — only an **installed-binary first capture**, which is a change-detection baseline, **not** verification against an approved artifact. **The two must never be written as one line**; the evidence must say which path was used |
| package counts | **60 resolved · 58 applicable and required on Linux/cp311**. Not installed, correctly: **`woa23-bench2026`** (`source = { virtual = "." }`, the virtual project) and **`colorama`** (`marker = "sys_platform == 'win32'"`). **"All 60 installed" is NOT a success condition** (§4.4a) |
| partial-venv rule | **§4.4b.** `$VENV` existing is **not** evidence of success; a partial venv **must not** enter the cutover; a retry uses a **fresh `$APP_ROOT`/`$VENV`** (re-extracting the subject and re-running §1's six checks) **or** re-runs the same offline sync for the same lock to a clean **exit 0** |
| runtime divergence | **recorded, not resolved** (§4.6). Serving becomes **3.11.14**, matching G1 and every candidate run; today's production is **3.11.4**, so the **interpreter patch level changes**. **Attribution limitation stated: matching the version is not the same as having tested this deployment. C1/C2 are NOT re-run and no performance test is implied** |
| **serving interpreter** | **`$VENV/bin/python3.11`**, from a **standalone uv-managed CPython 3.11.14** (§4.0, §4.1). **`/home/odbadmin/.pyenv` takes NO part in serving** — not interpreter, not stdlib, not `site-packages`, and it is **not** the venv base |
| **package set** | 60 locked distributions from `uv.lock` sha256 `0d2980a5…dccc69`; `dask`/`distributed` are installed but **never imported** by the served application (§4.5) |
| old app certificate/key | **PRESERVED** — not deleted, moved, renamed or chmod-ed; required for rollback (§8.11) |
| security findings | **S1** -> post-cutover retirement, blocked until rollback is impossible (§8.11) · **S2** -> **explicit risk acceptance recommended, remediation separately authorised** (§8.12). Neither bundled into the cutover |
| first deployment | **`143bf8c` is the FIRST deployment of the response fix.** The behaviour it changes has never run in production |
| A11 | **qualified only** — unchanged by this audit |
| store content integrity | **unproven** — the recorded fingerprint is metadata-only, unchanged by this audit |
| B1 for the new tree | **still required**, and §14.1 remains the recommendation |
| `conf/simu.sh` | **remains separate** — untouched |
| unrelated retained state | **not cleaned** |
| **closed since the last revision** | official hostname **`eco.odb.ntu.edu.tw`** adopted (§8.8) · **A-move** adopted (§8.10) · the **two-location** nginx scope confirmed (§10A.3) · the **three config deltas** specified with diff, hashes, verification and rollback (§5.2) |
| **closed this round** | the **runtime architecture** (P2), the **version** (3.11.14) and **all four runtime paths** are DECIDED and are **no longer owner decisions** (§4.0, §4.3, §5.3) |
| open before authorization | **(a)** **authorise production-side runtime provisioning** as a separate step — §4.4's ten steps plus the §4.4a cache gate and §4.4e interpreter checks, to be completed and verified **before** a window is scheduled; **approve the interpreter artifact** its digest is checked against (§4.4e check 3), and **authorise populating `odbadmin`'s package cache** from an approved source if the gate fails (§4.4a) · **(b)** **S2 explicit risk acceptance** (§8.12) · **(c)** a **[root] operator** must be named and available for the whole window, not only for step 4 (§10A.2) · **(d)** authorise the **three config deltas** (§5.2) · **(e)** **eight deployment-shape/topology changes beyond the CSV fix**, which now includes the **interpreter patch-level change** (§0A) · **(f)** production B1 unvalidated for the new tree — §10 steps 7–9 recommended · **(g)** nine apps share production's `PM2_HOME` (§3) |
| deferred to the window, by design | **`CACHE-BYPASS-UNPROVEN`** (§10A.8) and the resolved `nginx -T` view — neither is measurable without root or an API request, and both are already gates inside the window |
| **EXECUTION-READY?** | **NO.** Two blockers stand — **`RUNTIME-PROVISIONING-INCOMPLETE`** (§4.3, §4.4) and **`CACHE-BYPASS-UNPROVEN`** (§10A.8) — and items (a)–(d) are unresolved. **This plan must not be marked execution-ready, and no executable cutover command is prepared** |
| this round | **document update only.** VM24 not contacted · **no provisioning performed on VM24** · no production, nginx, TLS or PM2 change · no reload, no purge, no API request. The uv behaviour above was reproduced **locally, in the session scratchpad**, on a throwaway `pyproject.toml` with **no dependencies** — nothing from this repository, this subject or VM24 was involved |
| executable cutover commands | **NOT prepared** — this remains an offline plan |
