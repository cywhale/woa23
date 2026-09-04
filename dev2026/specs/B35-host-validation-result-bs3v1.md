# `bs3v1` — B3 PASS, B5 PASS (staging only), and a **defect found in `production_stop.sh`**

- **Ran:** 2026-08-27 12:42–12:48 UTC as `woa23c1ro` (uid 994) on `odb24`
- **Subject:** `043b7e958e0b37f5d36d505ef7d3e13da646af69`
- **Archive:** `ac2d51fc5e3db9e2f05bc4ea467af159a4c66ee42404b3cf7c4ff02b088ae1ae`, 219 files
- **File-list:** `0565338622d7ad03ac3703fc3184d0fe36df35ba5854591f87a360884525c0ee`
- **Identity:** `bs3v1`, port **18283** — BOUND, then released. **SPENT.**

> **This is a STAGING result only. It does NOT close B3, B5, or any B1–B5 blocker on
> production, and it is not a cutover PASS.** Production's `conf/start_app.sh`
> (`4aaed5b7…`) was never touched and still carries the defects.

## 0. The recorded result — TWO valid staging findings, and this run is NOT a clean PASS

**Accepted as valid, staging-only evidence:**

- **B3 — PASS (staging).** 18283 used correctly; **no `8050` in any argv.**
- **B5 — PASS (staging).** No `--reload` in master's or either worker's argv.

**`bs3v1` as a whole must NOT be described as an unconditional or clean PASS.** These
qualifications are part of the result, not footnotes to it:

| # | qualification |
|---|---|
| 1 | **Environment isolation deviation** — the app environment carried **production TLS key/cert paths**: `WOA23_TLS_CERTFILE` and `WOA23_TLS_KEYFILE` under `/home/odbadmin/python/woa23/conf`. |
| 2 | **`TLS=off` and zero open fds prove only that they were NOT USED IN THIS RUN.** They do **not** show that production paths were kept out of the environment — they were carried in. Absence of use is not absence of exposure. |
| 3 | **Production's PM2 version is UNVERIFIED.** |
| 4 | **Staging ran PM2 `5.4.2`.** |
| 5 | **B3 and B5 speak only for staging.** They close **no** production B1–B5 blocker. |

Additionally, and separately from the B3/B5 findings: **`deploy/production_stop.sh`
reported success while the service kept running** (§5). That is a B1 defect surfaced by
this run, not a B3/B5 result.

**Port `18283`: BOUND by the staging app, then released — recorded SPENT** (not
RETIRED-NEVER-BOUND; it genuinely carried a listener).

**The staging PM2 daemon remains running** under the staging `PM2_HOME` and is
**retained** — not killed, not cleaned. Its removal requires its own separate request.

**A separate, unplanned finding: `deploy/production_stop.sh` silently failed to stop the
service and reported success.** §5. That is B1's territory, not B3/B5's, and it is
reported here because this run is where it surfaced.

---

## 1. Provenance, re-confirmed before execution

| check | result |
|---|---|
| archive sha256 (local **and** on host) | `ac2d51fc…e1ae` — **matches authorisation** |
| file count | **219** — matches |
| file-list digest | `0565338622…c0ee` — verified by the driver against the staged tree |
| per-file verification | `STAGED and VERIFIED: the tree is the authorised subject, file for file` |
| `bs3v1` in the subject | **0 files** |
| `18283` in the subject / ledger | **0 files / 0 rows** |
| request doc `b9a7b95` in the subject | **ABSENT** — protocol reference only |
| archive transfer path fresh | `/home/woa23c1ro/bs3v1-archive.tar` — absent beforehand |
| bootstrap path fresh | `/home/woa23c1ro/bs3v1-bootstrap` — absent beforehand |
| bootstrap / parent / realpath vs identity | all outside; parent not a symlink |
| driver from the verified archive | unique regular-file member; **sha256 in archive == on disk** = `d798bf37a74a08c213626da8df71da69008c295b4b1881255509864f856106d7` |
| 18283 live-unbound | confirmed |
| PM2 binary vs `probeC` | realpath, sha256 `bbb58671…9256d`, version **5.4.2**, `odbadmin:odbadmin 775`, `r-x` on binary/parent/package — **all match** |

**No bootstrap-of-the-bootstrap was needed:** `staging_bootstrap.sh` has no shell
`BASH_SOURCE`/`$0` dependency (line 7 is a comment, line 152 is awk's own `$0`), so it was
**piped over SSH stdin**. The only file it wrote was the driver, into the bootstrap path.

---

## 2. B3 — port/config isolation · **PASS (staging only)**

**argv, read from `/proc/<pid>/cmdline` for master and both workers:**

```
/home/woa23c1ro/woa23-bs3v1/dev2026/.venv/bin/python -m gunicorn api.app:app \
  -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:18283 \
  --timeout 120 --graceful-timeout 10
```

| | |
|---|---|
| bind address | **`127.0.0.1:18283`** on all three processes — the port came from `WOA23_PORT` |
| `WOA23_PORT` in process env | `18283` |
| any `8050` literal in any argv | **0** |

**What this does NOT establish:** that production's **installed** files separate port from
config. `conf/start_app.sh` and `conf/ecosystem.config.js` were read for their digests
only and are unchanged.

## 3. B5 — no `--reload` · **PASS (staging only)**

**`--reload` occurrences across master and both workers' argv: 0.**

**What this does NOT establish:** that production is not running `--reload`. **It still
is** — the defect is live in `conf/start_app.sh` (`4aaed5b7…`), which this run never
touched. A file that is not installed cannot close a blocker about the file that is.

---

## 4. The required `/proc` verifications

| requirement | result |
|---|---|
| argv is `production_app.sh` / `api.app:app` | **`api.app:app`** present; no `woa23_app`; launched via the staged venv interpreter |
| `PM2_HOME` in the app process | **`/home/woa23c1ro/woa23-bs3v1-pm2`** — the staging one |
| PM2 **daemon**'s `PM2_HOME` | same staging path (`PM2 v5.4.2: God Daemon (/home/woa23c1ro/woa23-bs3v1-pm2)`) |
| `WOA23_PM2_BIN` leaked into app process | **0 occurrences** |
| production PM2 variables carried in | **none** |
| master uid | `1709484` — **994,994,994,994** (real, effective, saved, fs) |
| worker uids | `1709493`, `1709494` — **994,994,994,994** each |
| port 18283 held by this staging process | `users:(("python",pid=1709494),("python",pid=1709493),("python",pid=1709484))` |
| production app name / production PM2_HOME | **not used** |

The launcher's own environment allowlist also passed: 10 checks, 7 exact values, 3
required ABSENT, and **no `WOA23_*` outside the ten**.

### 4.1 Two production paths ARE present in the app environment — reported, not glossed

```
WOA23_TLS_CERTFILE=/home/odbadmin/python/woa23/conf/fullchain.pem
WOA23_TLS_KEYFILE =/home/odbadmin/python/woa23/conf/privkey.pem
```

These are **production paths in the staging process's environment**. `WOA23_TLS=off`, and
I verified they are **not used**:

- argv contains **no** `--keyfile` and **no** `--certfile`;
- **open file descriptors referencing `/home/odbadmin`: 0** on all three processes.

They come from the staging config's TLS defaults. They are inert here, but they are
present, and the authorisation asked that no production path be carried in — so this is a
**deviation from that requirement, recorded rather than waved through.**

### 4.2 Mapped `/home/odbadmin` files — reconciled, not alarming

My raw scan found **198** mapped files under `/home/odbadmin` while the driver reported
`production=0`. Both are right, and the reconciliation matters:

```
all 198 are under /home/odbadmin/.pyenv/versions/3.11.4
  (the base CPython binary, libpython3.11.so, and stdlib lib-dynload extensions)
production tree /home/odbadmin/python/woa23 : 0
production conf                             : 0
shared py311 site-packages                  : 0
THIS RUN'S venv                             : 416
```

The venv was built with `--python /home/odbadmin/.pyenv/…/python3.11`, so it **must** map
that base interpreter. No production code, config or shared site-package is loaded.

---

## 5. **DEFECT: `production_stop.sh` reported success while the service kept running**

The authorised stop was issued:

```
WOA23_PM2_HOME=/home/woa23c1ro/woa23-bs3v1-pm2 \
  .../deploy/production_stop.sh woa23-bs3v1-candidate
→ "pm2 reports no running pid for 'woa23-bs3v1-candidate' — nothing to stop."
→ exit 0
```

**It was not stopped.** Immediately afterwards: pids 1709484/1709493/1709494 all **ALIVE**,
and **18283 still bound by them**.

### 5.1 The cause, pinned

`production_stop.sh` parses `pm2 jlist` with awk, setting `inapp` on `"name"` and then
taking the next `"pid"`. In pm2 **5.4.2** the fields arrive in the **opposite order**:

```
1:[{"pid":1709484
2:"name":"woa23-bs3v1-candidate"
```

The only `"pid"` passes **before** `inapp` is ever set, so the parser extracts **empty** —
and empty is treated as "nothing to stop". A `node` JSON parse of the same bytes returns
`name=woa23-bs3v1-candidate pid=1709484 status=online`.

**This is the pm2F defect.** It was found there, and fixed there by replacing the awk
parser with `node -e` in `staging_execute.sh`. **`production_stop.sh` was never fixed and
still carries it.**

### 5.2 Why this matters beyond this run

`production_stop.sh` is B1's proposed replacement for production's `pre_stop`. As it
stands it **fails open in the worst direction**: it tells an operator the service stopped
when it did not, and exits 0. Its careful `(pid, starttime)` identity work and its
fail-closed survivor check are all downstream of a pid it never obtains.

**This is a B1 finding, not a B3/B5 one**, and B1 is not in this run's scope. It needs its
own offline fix and its own authorisation. **I have not changed it.**

### 5.3 What I did instead, and why

The run's B3/B5 objective had already succeeded; what failed was the cleanup wrapper. I
used the **named-app stop the request authorises**, directly:

```
PM2_HOME=/home/woa23c1ro/woa23-bs3v1-pm2  pm2 stop woa23-bs3v1-candidate
```

Named app only — **not** `all`, **not** `kill`, no `save`, no `resurrect`, **no SIGKILL**.
All three processes exited **within 1 second**, and **18283 released**.

I judged this within "clean up only the staging scope per the request" rather than a
mid-run failure requiring frozen state: the evidence was already fully captured, and
leaving a staging service holding a port is worse than stopping it by the authorised
means. **If you would rather I had frozen it, say so and I will treat a failed cleanup
wrapper as a stop-and-preserve condition in future.**

---

## 6. Cleanup and production before/after

**Staging:** no arm process remains; **18283 listeners: 0**; total listeners **45 → 45**.

**The PM2 daemon (pid 1709473) remains running** under the staging `PM2_HOME`. `pm2 kill`
is **forbidden**, so it was not killed — my process count is 17 vs a baseline of 16, and
that daemon is the difference. **Left for your decision.**

**Production — every measure identical:**

| | before | after |
|---|---|---|
| pids : starttimes | 4296:14214, 5040:15825, 5041:15829 | identical |
| 8050 | LISTEN | LISTEN |
| `conf/start_app.sh` | `4aaed5b7…3776d77` | identical |
| `conf/ecosystem.config.js` | `8db9a6ba…de4340` | identical |
| `/home/odbadmin/.pm2` mtime | 1786685045 | identical |
| store mtime / files / bytes | 1787622836 / 123005 / 35101630061 | identical |
| boot id | `0b513a75-…-1c7cbc51a085` | identical |
| pm2 binary sha256 | `bbb58671…9256d` | identical |

**pm2G untouched:** 18265 bound; 1456369, 1456373, 1456374 running.
**`b35a1` failure evidence intact.** No production API request was made at any point.

**Retained:** staging tree (9 939 files incl. the venv), bootstrap (**1 file** — the
driver only), workdir, and this run's logs.

---

## 7. Standing limitations

- **staging ran PM2 `5.4.2`**; **production's PM2 version is UNVERIFIED** — establishing it
  needs executing pm2 or reading its daemon state, both forbidden. If they differ, this
  exercised a different PM2 than production runs. Note the defect in §5 is
  **version-sensitive**: it is pm2 5.4.2's field order that exposes it.
- the PM2 binary lives under **another account's home**; the pre-start digest check is a
  **mitigation, not an immutable guarantee**.
- **B1, B2, B4, B7 and the cutover are untouched.** B3 and B5 remain **open on
  production** whatever this staging run shows.

## 8. Evidence

| file | contents |
|---|---|
| `scratchpad/bs3v1/01-preflight.txt` | pre-flight incl. the pm2 gate |
| `scratchpad/bs3v1/02-stage.txt` | bootstrap + stage, digests, verified tree |
| `scratchpad/bs3v1/03-setup.txt` | venv build; bootstrap holds one file |
| `scratchpad/bs3v1/04-run.txt` | the run, argv and env readback |
| `scratchpad/bs3v1/05-proc-verify.txt` | independent `/proc` verification |
| `scratchpad/bs3v1/06-tls-finding.txt` | TLS paths present but unopened |
| `scratchpad/bs3v1/07-maps.txt` | the 198 mapped files, reconciled |
| `scratchpad/bs3v1/08-stop.txt` | `production_stop.sh` false "nothing to stop" |
| `scratchpad/bs3v1/09-state.txt` | proof the service was still running |
| `scratchpad/bs3v1/10-stop-defect.txt`, `11-awk-defect.txt` | the parser defect, pinned |
| `scratchpad/bs3v1/12-cleanup.txt` | named-app stop, release verified |
| `scratchpad/bs3v1/13-final.txt` | final state, production before/after |
