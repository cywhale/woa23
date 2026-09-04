# D-4 read-only preflight — PM2 measured, **P-TLS BLOCKED**

**Read-only. Nothing was modified.** No production stop, start, restart, reload, config
write, API request, cleanup or C1/C2 rerun. No file permission, ownership or ACL was
changed. No private-key material was printed or copied.

**Outcome: PM2 preflight PASSES with findings. P-TLS is BLOCKED on two independent
grounds.** Cutover is not requested and must not proceed.

---

## 1. PM2 daemon precondition — PASSED, established before any `pm2` was invoked

| check | result |
|---|---|
| `PM2_HOME` is exactly `/home/odbadmin/.pm2` | **yes** — exists, `odbadmin:odbadmin` `775`, **not a symlink** |
| pid file `…/pm2.pid` | present, mtime `2026-08-14 13:23:59 +0800` |
| recorded pid | **3459** |
| `/proc/3459` | exists |
| uid of that process | **1000** |
| starttime | `lin-13189` |
| cmdline | `PM2 v5.4.2: God Daemon (/home/odbadmin/.pm2)` |
| its own `PM2_HOME` environ | `/home/odbadmin/.pm2` |
| exe | `/usr/bin/node` |

**`pm2` was not invoked until every line above was established from the pid file and
`/proc`.** The daemon is alive, identified and owned by uid 1000, so no `pm2` call could
create one.

## 2. PM2 version and binary — measured, `pm2 -v` never invoked

| | |
|---|---|
| version, source 1 | **5.4.2** — from the running daemon's own cmdline |
| version, source 2 | **5.4.2** — `package.json`, read as JSON |
| binary | `/home/odbadmin/.npm-global/bin/pm2` — symlink, `odbadmin:odbadmin` `777` |
| realpath | `/home/odbadmin/.npm-global/lib/node_modules/pm2/bin/pm2` |
| realpath owner/mode/size | `odbadmin:odbadmin` `775`, 56 bytes |
| **realpath sha256** | **`bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d`** |
| `package.json` sha256 | `7a4dc970b9cf4e7a227ba4e5e0254870e8bed1ee08d94703ead9123badcd0a16` |
| node | `/usr/bin/node`, `root:root` `755`, 120177224 bytes, sha256 `1abce2374a485bddae3c27b17a3e3143e2780232026e627c4fe74ddde3f380a1` |

**Production PM2 is 5.4.2 — the same version every PM2 behaviour in this campaign was
proven on**, and the binary digest matches the recorded reference exactly. The D-4 plan's
"production PM2 version UNVERIFIED" open item is **closed**.

## 3. The exact app name — `woa23`, and it shares its `PM2_HOME` with eight others

| app | status | pm_id |
|---|---|---|
| gateway | stopped | 0 |
| odbbathy | stopped | 1 |
| mhwapi | online | 2 |
| **woa23** | **online** | **3** |
| ghrsst | online | 4 |
| ghrsst_mcp | online | 5 |
| dask-scheduler | online | 6 |
| dask-worker | online | 7 |
| tide | online | 10 |

**This is a material cutover risk that the plan must carry: a wildcard, `all`, `save`,
`resurrect` or `pm2 kill` in production's `PM2_HOME` would hit eight other live services.**
Every operation must name `woa23` exactly.

---

## 4. Findings that change the plan's premises

### 4.1 `pre_stop` is NOT present — Stage A recorded the pre-change configuration

| where | `pre_stop` |
|---|---|
| live PM2 definition for `woa23` | **`None`** |
| `/home/odbadmin/python/woa23/conf/ecosystem.config.js` on disk (sha256 `ed5dec6c…2159`, 384 bytes) | **0 occurrences** |

The D-4 plan's §6 states production's config carries the `grep`/`kill -9` `pre_stop` hook.
**Today it does not** — neither in the live definition nor in the config file.

**`B1-stageA` is not stale or mistaken: it correctly recorded the PRE-CHANGE configuration,
and Stage B subsequently removed the dead `pre_stop` line.** The Stage A record and this
measurement describe two different, correctly-recorded points in time. What is out of date is
the D-4 plan's §6, which was written as though the Stage A state were still current.

**The B1 concern is not thereby resolved — it changes shape.** See 4.2.

### 4.2 PM2 does NOT track the gunicorn master today

| | pid | starttime | what it is |
|---|---|---|---|
| PM2-tracked | **1828351** | `lin-131297234` | `bash /home/odbadmin/python/woa23/conf/start_app.sh` |
| gunicorn master | **1828352** | `lin-131297235` | the actual server |
| worker | 1828389 | `lin-131297318` | |
| worker | 1828409 | `lin-131297327` | |

`pm_exec_path` is `conf/start_app.sh`, `exec_mode` `fork_mode`, `exec_interpreter` `bash`.
**PM2 tracks a shell wrapper; the master is its child.** That is precisely the condition the
proposed `exec`-based launcher removes, and it means **B1 validation for the new tree remains
necessary** — the stop path changes from "signal a wrapper" to "signal the master directly".

### 4.3 Production currently runs with `--reload`

Live argv of the master:

```
/home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
  /home/odbadmin/.pyenv/versions/py311/bin/gunicorn woa23_app:app \
  -w 2 -k uvicorn.workers.UvicornWorker -b 127.0.0.1:8050 \
  --keyfile conf/privkey.pem --certfile conf/fullchain.pem \
  --timeout 120 --reload
```

**`--reload` is present in production today.** The proposed configuration does not carry it,
and the plan asserts its absence as a post-start check. That is a **behaviour change** beyond
the response fix and must be stated as such rather than discovered at cutover.

Also: production runs the **shared pyenv 3.11.4 interpreter directly**, with **no venv** and
**no `WOA23_*` environment variables at all** on the running master (its cwd is
`/home/odbadmin/python/woa23`). The legacy app does not read `WOA23_ZARR_STORE`.

### 4.4 A stray `api.app:app` on 127.0.0.1:18265

pids 1456369 / 1456373 / 1456374 under uid 1000, serving `api.app:app` on port 18265 from
the pyenv interpreter. **Not touched, not cleaned** — recorded because an operator at a
cutover should know it exists before wondering whether they started it.

---

## 5. P-TLS — **BLOCKED**

Paths taken from the **live process argv** (`--keyfile conf/privkey.pem --certfile
conf/fullchain.pem`) resolved against the **live `pm_cwd`** `/home/odbadmin/python/woa23`.
**Not inferred from the repository, the placeholders, staging, or the proposed D-4 config** —
they coincide with the placeholder values, but they are recorded here because the running
process uses them.

| | certificate | private key |
|---|---|---|
| path | `/home/odbadmin/python/woa23/conf/fullchain.pem` | `/home/odbadmin/python/woa23/conf/privkey.pem` |
| realpath | same (not a symlink) | same (not a symlink) |
| regular file | yes | yes |
| owner / mode | `odbadmin:odbadmin` **644** | `odbadmin:odbadmin` **644** |
| readable by production account | yes | yes |
| size | 5603 | 1704 |

**Certificate metadata:**

```
subject   CN = eco.odb.ntu.edu.tw
issuer    C = US, O = Let's Encrypt, CN = R3
notBefore May 27 23:22:04 2023 GMT
notAfter  Aug 25 23:22:03 2023 GMT
SAN       DNS:eco.odb.ntu.edu.tw
sha256    AC:D0:1F:DF:14:8F:0F:59:F0:92:E5:77:60:A4:68:BF:28:44:06:85:D9:EA:6B:65:C0:D9:A8:D8:6D:A1:8C:02
```

**Key/certificate pair match: YES** — public-key SHA-256 of both is
`373aeb238f8c398c7ff81b7ce62415fa5b55d9fc9de14f2510365f29f7f782a1`. No key material was
printed or copied.

### 5.1 Why it is BLOCKED — two independent reasons

**B-TLS-1 — the certificate is EXPIRED.** `notAfter = Aug 25 23:22:03 2023 GMT`.
`openssl -checkend 0` returns **NO**: it expired **over three years ago**. The pass condition
"the certificate is currently valid" fails outright.

**B-TLS-2 — the official production hostname is not explicitly known to me.** The SAN covers
exactly one name, `eco.odb.ntu.edu.tw`. The repository mentions `eco.odb.ntu.edu.tw` (21×),
`api.odb.ntu.edu.tw` (5×) and `www.odb.ntu.edu.tw` (1×), and **no document states which is
the official public hostname for the WOA23 API.** I will not decide it by frequency. Without
it, "SAN covers the exact official production hostname" **cannot be established**, only
guessed — so it is reported unestablished.

### 5.2 An observation, offered as a question and not as an answer

The service binds **`127.0.0.1:8050`** — loopback only — while presenting an expired
certificate on that socket. A public listener terminating TLS in front of it would explain
both how the site can serve valid TLS today and how this certificate can be three years
stale without anyone noticing. **I have not looked for such a component and am not
asserting one exists.** If it does, the certificate that matters for public TLS is *its*
certificate, not this one, and P-TLS is asking about the wrong file — which would itself be a
finding worth settling before cutover.

### 5.3 What was NOT done

No `chmod`, `chown`, ACL change, re-issue, renewal, copy or modification of any TLS file.
Mode `644` on the private key is **recorded as an observation** — it is world-readable — and
is **not** changed by this preflight.

---

## 6. Rollback baseline — recorded, unmodified

| item | value |
|---|---|
| PM2 daemon | pid **3459**, starttime `lin-13189`, uid 1000, v5.4.2 |
| PM2-tracked app pid | **1828351** `lin-131297234` — `bash conf/start_app.sh` |
| gunicorn master | **1828352** `lin-131297235` |
| workers | **1828389** `lin-131297318`, **1828409** `lin-131297327` |
| listener | `127.0.0.1:8050`, held by 1828352 / 1828389 / 1828409 |
| live definition | `pm_exec_path=conf/start_app.sh`, `pm_cwd=/home/odbadmin/python/woa23`, `fork_mode`, `bash`, `pre_stop=None`, `append_env_to_name=True`, `autorestart=True`, `max_memory_restart=4294967296`, `kill_timeout=None`, **no `WOA23_*`** |
| config file | `conf/ecosystem.config.js`, sha256 `ed5dec6ca064bd54cfe4a45f33fd416871859e959671164292b21549b96f2159`, 384 bytes, `664` |
| production store | `1000:1000` `775`, **123005** files, metadata fingerprint `abe6c21221b5081eb352a1a549c9d1fd6399c74b1f06b8c2ce09952828c61806` |

`append_env_to_name = True` is confirmed **live**, so the plan's requirement to set it false
— and never to pass `--env` — is load-bearing, not theoretical.

---

## 7. Status

| | |
|---|---|
| PM2 preflight | **PASSED** — version 5.4.2 confirmed twice, binary digest matches, daemon alive under uid 1000, app name `woa23` |
| **P-TLS** | **BLOCKED** — B-TLS-1 certificate expired 2023-08-25; B-TLS-2 official hostname not explicitly known |
| cutover | **NOT requested, NOT performed** |
| production | **unmodified** — no stop/start/restart/reload/config write/API request/cleanup |
| proposed artifact | still subject **`143bf8c`**, archive `0873a970…`, 262 files, file-list `c436362a…` |
| D-3 evidence | belongs to **`a361f70`**; **not** back-filled |
| C1/C2 · performance | **not re-run · not run** |
| A11 | qualified only |
| store content integrity | **unproven** — §6's fingerprint is metadata-only |
| `conf/simu.sh` | separate, untouched |
| B1 for the new tree | still a **planned in-window validation, not executed** — and 4.2 shows why it is still needed |

**To unblock P-TLS, two things are needed from you, and neither is mine to decide:**

1. the **official public hostname** for the WOA23 API, stated explicitly;
2. a decision on the **expired certificate** — whether TLS is in fact terminated elsewhere
   (§5.2), and if so which certificate and key the cutover should verify instead.
