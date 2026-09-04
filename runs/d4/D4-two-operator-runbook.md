# D-4 cutover runbook — TWO-OPERATOR model

**Nothing here has been executed.** This document supersedes §5 of
[`D4-runbook-and-readiness.md`](D4-runbook-and-readiness.md) as the operational sequence.

**Artifact — fixed, and the only one:**

| | |
|---|---|
| subject | `f66ddd8cd18840213b086a03dba4545b0da8ad44` |
| archive | `2bea7db91bcf00270abe0c73beff33ab66692ee6749e57483eb165ef6373de90` |
| file-list | `ca461166312a61a3d338edf6211fbf6604facac3009a2c546ff54bee6e8ea169` |
| `APP_ROOT` | `/home/odbadmin/python/woa23-f66ddd8` |
| `WOA23_PYTHON` | `/home/odbadmin/python/woa23-f66ddd8/.venv/bin/python3.11` |
| lock | `0d2980a5928d4d0964d6cb3b78bffae14aa11a70d3b51ca00f4cf39073dccc69` |

**`/home/odbadmin/python/woa23-143bf8c` and its venv must never be named by any command here.**

---

## 0. The two operators

| role | who | scope |
|---|---|---|
| **[APP]** | `odbadmin` (uid 1000), driven by Claude | PM2 on the named app `woa23`, the f66 `APP_ROOT`, process / port / venv / environment verification, smoke and cache checks, evidence capture |
| **[ROOT]** | **a human with real Unix root (euid 0) or demonstrable privileged nginx rights — to be named** | nginx config backup, the two-location edit, `nginx -t`, reload, and the nginx half of rollback |

**Claude performs no [ROOT] step.** No privilege escalation, no permission change, no
`sudoers` change, no `.env` read, no plaintext password.

### 0.1 THE BLOCKING GATE — G0

> **Claude MUST NOT run `pm2 stop woa23` until the [ROOT] operator is present, has confirmed
> they can perform steps R1–R4 immediately, and has demonstrated privilege.**

**G0 evidence required, captured before anything is stopped:**

| # | evidence | acceptance |
|---|---|---|
| 1 | [ROOT] operator **named**, and present for the whole window | a name recorded, not "someone will be around" |
| 2 | privilege demonstrated **by them**, e.g. `id -u` returning `0` in the shell they will use, or a successful no-op write test **they** perform on a scratch file under `/etc/nginx/conf2.d/` **which they then remove** | recorded output. **Claude does not run this and does not evaluate a claim in place of evidence** |
| 3 | they confirm they can run `nginx -t` and reload nginx **now** | explicit confirmation |
| 4 | they confirm they will remain reachable **through rollback**, not only through step R4 | explicit confirmation |

**If any of 1–4 is missing: the window does not open. Status stays NOT EXECUTION-READY, and
production is not touched.** A [ROOT] operator who leaves after step R4 is **not** sufficient —
rollback needs them again (§4).

## 1. Handoff points

There are exactly **three** handoffs. Each is a stop-and-confirm, not a hint.

```
  [APP] steps A1-A4   ──H1──▶   [ROOT] steps R1-R4   ──H2──▶   [APP] steps A5-A11
                                                                      │
                                                        (on any gate failure)
                                                                      ▼
                                                   ──H3──▶   [ROOT] rollback RB1-RB3
                                                              then [APP] RB4-RB6
```

| handoff | from -> to | precondition, stated by the sender | the receiver must not start before |
|---|---|---|---|
| **H1** | [APP] -> [ROOT] | "the old app is stopped and deleted; **8050 is free**; here are the pids and starttimes that are gone" | seeing 8050 free **themselves** |
| **H2** | [ROOT] -> [APP] | "both locations now read `http://woa23api`; `nginx -t` passed; reload done; here is the `nginx -T` diff" | seeing the reload confirmation and the diff |
| **H3** | [APP] -> [ROOT] | "gate X failed; rollback now; nginx first" | — rollback begins immediately |

---

## 2. The window, step by step

### Phase A — [APP], before any privileged action

| step | action | input evidence | output evidence | abort -> |
|---|---|---|---|---|
| **A1** | **Preflight**: confirm artifact identity | `APP_ROOT` exists; `uv.lock` sha256; `WOA23_PYTHON` executable; venv `sys.prefix` / `realpath(sys.base_prefix)`; 58 distributions | all recorded and matching §0's table | abort, nothing changed |
| **A2** | **Capture the rollback baseline** | live `pm2 jlist` for `woa23`; `conf/ecosystem.config.js` byte copy + sha256 (`ed5dec6c…2159`); master/worker **(pid, starttime)**; 8050 listener; old cert/key **digests only** | a bundle **outside** both trees, verified readable | abort |
| **A3** | **Place the config** — placement **3a** | write `$APP_ROOT/dev2026/deploy/ecosystem.production.config.js` with the three deltas: `WOA23_TLS: 'off'`; `WOA23_PYTHON` = the f66 venv; TLS key/cert vars **removed**; `cwd: __dirname + '/..'` unchanged | sha256 of the **placed** file, recorded; a diff against the committed file showing **exactly** those three changes | abort |
| **A4** | **`pm2 stop woa23`** then **`pm2 delete woa23`** — exact name only | the recorded (pid, starttime) set | master **and every worker** absent by (pid, starttime); **8050 free**; `woa23` absent from `pm2 jlist` | **RB (app-only)**: restart old app from the A2 bundle |

> **A4 may not begin until G0 is satisfied.**

**H1 -> [ROOT].**

### Phase R — [ROOT], privileged

| step | action | input evidence | output evidence | abort -> |
|---|---|---|---|---|
| **R1** | **Back up** `/etc/nginx/conf2.d/routes-vm124.conf` | 8050 confirmed free (seen by [ROOT]) | byte copy **outside `/etc/nginx`** + its **sha256**, and a full `nginx -T` dump as the pre-change baseline | abort; nothing edited |
| **R2** | **Edit exactly two lines**, in the two WOA23 locations only:<br>`proxy_pass https://woa23api;` -> `proxy_pass http://woa23api;` | the backup from R1 | `diff` old vs new showing **exactly two changed lines**, no whitespace or ordering change | restore from R1; nothing reloaded |
| **R3** | **`nginx -t`** | the edited file | `syntax is ok` **and** `test is successful` | **restore from R1 and STOP.** Do not reload |
| **R4** | **Reload** nginx (`nginx -s reload` / `systemctl reload nginx`) — **never restart** | R3 passed | reload completes; post-change `nginx -T` differs from R1's baseline **only** in the two `proxy_pass` scheme tokens; `upstream woa23api { server 127.0.0.1:8050; }` **unchanged** | **RB1–RB3** |

**Not permitted in Phase R:** any other location, server block, site, upstream, TLS file,
permission, ownership or ACL; any restart; any `sudoers` change.

**H2 -> [APP].**

### Phase A' — [APP], bring up and verify

| step | action | output evidence | abort -> |
|---|---|---|---|
| **A5** | `pm2 start $APP_ROOT/dev2026/deploy/ecosystem.production.config.js --only woa23` | PM2 `online`; master argv shows `api.app:app` on `127.0.0.1:8050`, **no `--reload`**, **no `--certfile`/`--keyfile`** | **RB (full)** |
| **A6** | **Process & port** | master + worker **(pid, starttime)** recorded — **new** identities; 8050's **sole** listener | RB |
| **A7** | **Venv & environment** from `/proc` | `sys.prefix` = `$APP_ROOT/.venv`; `realpath(sys.base_prefix)` = the uv interpreter root; **`/proc/<pid>/maps` has ZERO `/home/odbadmin/.pyenv`** for master **and every worker**; environ has `WOA23_TLS=off`, `WOA23_PYTHON` = f66 venv, and **`WOA23_TLS_KEYFILE`/`WOA23_TLS_CERTFILE` ABSENT** | RB |
| **A8** | **Store** | store path readable; metadata fingerprint recorded. **No content digest is claimed** | RB |
| **A9** | **Smoke + cache**, in order: **R1** loopback `http://127.0.0.1:8050` -> **R2** public `https://eco.odb.ntu.edu.tw` with `Cache-Control: no-cache` -> **R3** public plain, **R2/R3 on a byte-identical URL** | status, **body sha256**, request URL verbatim, headers, `X-api-cache` for each. **R2 must report `BYPASS`.** Public TLS verified with **normal certificate validation — never `--insecure`** | **R2 not `BYPASS`**, or `CACHE_OR_ROUTING_FAILURE` (R2 200 with R3 400) -> **STOP, RB** |
| **A10** | **B1 — stop the NEW app**: `pm2 stop woa23` | master and every worker gone by (pid, starttime); 8050 free | RB |
| **A11** | **B1 — restart the SAME new app** with the predeclared command, then repeat A6–A9 | `online`; argv as A5; **new** (pid, starttime); all A6–A9 gates pass again | RB |

**B1 is validation of the NEW app's stop/restart path. A10 is NOT a return to the old
production app**, and the old app must not be started at A11.

**Final record:** process tree, argv, environ from `/proc`, store fingerprint, post-change
`nginx -T`, and every digest above.

---

## 3. Abort conditions — any one halts the window

- G0 not satisfied, or the [ROOT] operator becomes unavailable at any point;
- artifact identity mismatch at A1, or the placed-config diff at A3 is not exactly the three deltas;
- at A4: any recorded pid still present after `kill_timeout`, or 8050 not free;
- at R2: any changed line beyond the two `proxy_pass` scheme tokens;
- at R3: `nginx -t` not fully successful;
- at R4: the post-change `nginx -T` differs anywhere else, or the upstream block changed;
- at A5: PM2 `online` without the expected argv (the B4 failure mode);
- at A7: any `/home/odbadmin/.pyenv` path in `maps`, or a TLS env var present;
- at A9: **R2 not `BYPASS`**, or `CACHE_OR_ROUTING_FAILURE`, or a body-digest mismatch where statuses agree;
- at A10/A11: a survivor by (pid, starttime), or the restart not producing new identities.

## 4. Rollback — nginx FIRST, and it is not optional

**H3 -> [ROOT] immediately.**

| step | operator | action | acceptance |
|---|---|---|---|
| **RB1** | [APP] | `pm2 delete woa23` — named app only, if the new definition was started | absent from `pm2 jlist` |
| **RB2** | **[ROOT]** | restore `routes-vm124.conf` **byte-for-byte** from R1; verify **sha256 equals R1's**; `nginx -t` | digest match; `test is successful` |
| **RB3** | **[ROOT]** | **reload** nginx; confirm both locations read `proxy_pass https://woa23api` again | post-restore `nginx -T` matches R1's baseline |
| **RB4** | [APP] | restore `conf/ecosystem.config.js` from the A2 bundle; verify sha256 `ed5dec6c…2159` | digest match |
| **RB5** | [APP] | `pm2 start conf/ecosystem.config.js --only woa23` | `online`; master argv shows `woa23_app:app`; 8050 serves **TLS** again |
| **RB6** | [APP] | confirm `https://eco.odb.ntu.edu.tw` serves the WOA23 route, normal certificate validation | 200 |

**nginx goes first so service is correct the instant the old app returns.** Between RB1 and
RB5 the route returns 502 — expected, and the same bounded interval as A4–A5.

**A partial rollback is not a rollback.** Restoring only nginx, or only the app, leaves a
scheme mismatch and a broken route. If the [ROOT] operator is unreachable at RB2, the window
is in a **degraded state that [APP] alone cannot exit** — which is precisely why G0 item 4
exists.

**The placed f66 config is left on disk during rollback**, as evidence of what was attempted.
Removing it is a separate, later cleanup.

## 5. Constraints carried unchanged

| | |
|---|---|
| PM2 | **no wildcard, no `all`, no `pm2 kill`, no `save`, no `resurrect`, no SIGKILL**, no manual signal. Every command names **`woa23`** exactly. Nine apps share this `PM2_HOME` |
| permissions | **no permission change, no ownership change, no ACL change, no `sudoers` change** |
| cache | **no purge, at any point.** A9's R2 refreshes one entry through ordinary cache behaviour |
| old TLS certificate and key | **not modified, deleted, moved, renamed or chmod-ed.** Rollback depends on them |
| artifact | **only `f66ddd8`.** `woa23-143bf8c` and its venv are never named |
| B1 | **in the same window**, steps A10–A11 |
| credentials | **no `.env` read, no plaintext password.** [APP] access is SSH key-based |
| evidence | **the D-3 observation, the synthetic harness timings and the batch sentinel are NOT production equivalence and NOT performance evidence.** No production performance evidence exists or is claimed |
| S2 | accepted as a **known, unresolved** risk. **Not** claimed resolved, and nothing about it is changed |
| AVX2 | accepted, unresolved residual risk under B6. **Never a CPU-safety PASS.** `POLARS_SKIP_CPU_CHECK` never set |
| store content integrity | **unproven** — the fingerprint is metadata-only |

## 6. Status

**NOT EXECUTION-READY.** One gate is outstanding: **G0 — the [ROOT] operator is not yet named,
present, and privilege-demonstrated.**

Everything on the [APP] side is ready: the artifact is provisioned and verified, the three
config deltas are specified literally, and the sequence, gates and rollback are fixed above.

**Production is untouched and will stay so until G0 is satisfied.**
