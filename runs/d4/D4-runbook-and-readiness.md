# D-4 final readiness reconciliation and cutover runbook

**Document only. No cutover was executed.** No PM2 operation, no nginx reload, no API
request, no cache purge, no cleanup. Nothing under `dev2026/` was modified.

# VERDICT: **NOT EXECUTION-READY.** Five owner approvals are outstanding (§7).

Everything the campaign can settle without them is settled and verified.

---

## 1. `f66ddd8` — the single provenance

| | |
|---|---|
| **subject** | `f66ddd8cd18840213b086a03dba4545b0da8ad44` |
| **archive sha256** | `2bea7db91bcf00270abe0c73beff33ab66692ee6749e57483eb165ef6373de90` |
| members / files | 276 / 265 |
| **file-list** | `ca461166312a61a3d338edf6211fbf6604facac3009a2c546ff54bee6e8ea169` |
| **`APP_ROOT`** | **`/home/odbadmin/python/woa23-f66ddd8`** |
| **`VENV`** | `/home/odbadmin/python/woa23-f66ddd8/.venv` |
| **`WOA23_PYTHON`** | `/home/odbadmin/python/woa23-f66ddd8/.venv/bin/python3.11` |
| **interpreter root** | `/home/odbadmin/python/uv-pythons/cpython-3.11.14-linux-x86_64-gnu` (alias `…/cpython-3.11.14-20251217`; binary sha256 `96d1b01675f2492922ec6f6ed8445791d2d3231ccae727cda521db30494b751e`) |
| **`uv.lock` digest** | `0d2980a5928d4d0964d6cb3b78bffae14aa11a70d3b51ca00f4cf39073dccc69` |
| **sentinel** | `D4_VALIDATION_SENTINEL subject=f66ddd8cd18840213b086a03dba4545b0da8ad44 batches=3 clean=yes` |
| production account | `odbadmin`, uid 1000 · `PM2_HOME=/home/odbadmin/.pm2` · PM2 5.4.2 |

## 2. The deployment tree

**The deployment tree is `/home/odbadmin/python/woa23-f66ddd8`.**

**`/home/odbadmin/python/woa23-143bf8c` MUST NOT be used.** It is retained as historical
evidence only; it is not the artifact, and its provisioning is superseded. No cutover command
may name it.

## 3. Done, and not done

**DONE and verified:**

| | |
|---|---|
| artifact validation | three clean batches on `f66ddd8`; REQUIRED_PASS 50/0, NOT_APPLICABLE 4, ENVIRONMENT_BLOCKED 6 (both skipped **before launch**), UNRESOLVED 0; subject-bound sentinel |
| fresh extraction | into a new `APP_ROOT`; archive, member/file counts and file-list all matched |
| mode / digest verification | four delivery members archive-digest = disk-digest; `test_d1_finalize.sh` `-rwxrwxr-x`, exits 1 not 126; 48/48 executable members |
| standalone CPython 3.11.14 | uv-recognised (with `UV_PYTHON_INSTALL_DIR` set), `-VV` build 2025-12-17, binary digest matched, `odbadmin` r+x, no pyenv, no `woa23c1ro` |
| fresh venv | built from the **real** interpreter path; nothing reused from the 143 tree |
| offline locked sync | `uv sync --locked --offline` exit 0, lock unchanged, cache unchanged (nothing downloaded) |
| 58 distributions | present; `dev2026/.venv` absent; `sys.prefix` = `$VENV`; `realpath(sys.base_prefix)` = interpreter root |

**NOT DONE — all five are owner approvals or in-window gates:**

| | |
|---|---|
| `[root]` operator | **not named**, and must be available for the whole window |
| S2 risk acceptance | **not given** |
| three config deltas | **not authorised** (§4) |
| in-window cache-bypass verification | **`CACHE-BYPASS-UNPROVEN`** — cannot be closed offline |
| B1 stop/restart validation | **not performed** — the `exec`-based stop path has never run in production |

## 4. The three config deltas — literal

Against `dev2026/deploy/ecosystem.production.config.js` as committed at `f66ddd8`. Applied to
the **copy that is placed**; the committed file is not edited.

### Delta 1 — app TLS off, and the app-side TLS environment removed

```diff
       env: {
         WOA23_PORT: '8050',
         WOA23_ZARR_STORE: '/home/odbadmin/python/woa23/data',
-        WOA23_TLS_KEYFILE: '/home/odbadmin/python/woa23/conf/privkey.pem',
-        WOA23_TLS_CERTFILE: '/home/odbadmin/python/woa23/conf/fullchain.pem',
+        WOA23_TLS: 'off',
         WOA23_WORKERS: '2',
       },
```

**Without `WOA23_TLS: 'off'` the launcher reads `${WOA23_TLS:-on}` and starts WITH TLS**,
which contradicts A-move and breaks `/api/woa23` once nginx speaks plaintext. Removing the two
path variables is a **configuration edit only — it deletes nothing on disk** (§6).

### Delta 2 — `WOA23_PYTHON` points at the f66 venv

```diff
       env: {
         WOA23_PORT: '8050',
         WOA23_ZARR_STORE: '/home/odbadmin/python/woa23/data',
         WOA23_TLS: 'off',
+        WOA23_PYTHON: '/home/odbadmin/python/woa23-f66ddd8/.venv/bin/python3.11',
         WOA23_WORKERS: '2',
       },
```

**`production_app.sh` requires it and has no default**; without it the app refuses to start
(fail-closed). The value is the **f66** venv — not the 143 tree's.

### Delta 3 — the PM2 definition points at the f66 deployment tree

The committed file uses `cwd: __dirname + '/..'`, which resolves relative to **where the
config is placed**. Two ways to satisfy it, and the choice must be recorded:

```
3a  place the config INSIDE the f66 tree, at
      /home/odbadmin/python/woa23-f66ddd8/dev2026/deploy/ecosystem.production.config.js
    then __dirname + '/..'  ->  /home/odbadmin/python/woa23-f66ddd8/dev2026     (unchanged line)
```

```diff
3b  place it elsewhere, and name the tree absolutely:
-      cwd: __dirname + '/..',
+      cwd: '/home/odbadmin/python/woa23-f66ddd8/dev2026',
       script: './deploy/production_app.sh',
```

**Either way `cwd` must resolve to `/home/odbadmin/python/woa23-f66ddd8/dev2026`** — the
`dev2026` level, not `APP_ROOT`, because `python -m gunicorn api.app:app` puts `cwd` on
`sys.path` and `api/` lives under `dev2026/`. From one level up PM2 reports `online` while
gunicorn dies at import — the B4 failure mode.

**Unchanged and load-bearing:** `name: 'woa23'` · `append_env_to_name: false` · never pass
`--env` · `kill_timeout: 20000` · `autorestart: true` · `max_memory_restart: '4G'`.

### The nginx change — both locations, root operator, after the old app stops

In `/etc/nginx/conf2.d/routes-vm124.conf`, **two lines, two location blocks, nothing else:**

```diff
 location /api/woa23 {
-    proxy_pass https://woa23api;
+    proxy_pass http://woa23api;
 location /api/swagger/woa23 {
-    proxy_pass https://woa23api;
+    proxy_pass http://woa23api;
```

**Both must change together** — leaving `/api/swagger/woa23` on `https://` would break the
Swagger route against a plaintext socket. `upstream woa23api { server 127.0.0.1:8050; }` is
**not edited**. Performed by the **`[root]` operator**, **only inside the window, only after
the old app has stopped** — applying it earlier breaks the live service, because the old app
still speaks TLS on 8050.

## 5. The cutover window — ordered, with rollback triggers

> **SUPERSEDED by [`D4-two-operator-runbook.md`](D4-two-operator-runbook.md).** After the
> privilege preflight established that `odbadmin` is not root and cannot write
> `/etc/nginx/conf2.d/` or signal the nginx master, the window was re-specified as a
> **two-operator** sequence with explicit handoffs, per-step input/output evidence, abort
> conditions and a rollback handoff. **Gate G0 there blocks `pm2 stop woa23` until a named
> [ROOT] operator is present and has demonstrated privilege.** The table below is retained as
> the single-operator draft it was.

| # | step | operator | gate | on failure |
|---|---|---|---|---|
| 1 | **preflight**: HEAD/artifact identity, `$APP_ROOT` digests, venv fields, PM2 daemon 3459 alive, live `woa23` definition captured, nginx config byte-copied + sha256 **outside `/etc/nginx`**, old cert/key digests recorded | [app]+[root] | every item recorded, no mismatch | abort before any change |
| 2 | **`pm2 stop woa23`** — exact name | [app] | master **and every worker** gone by **(pid, starttime)**; **8050 free** | §12 recovery |
| 3 | **verify the old listener and processes are gone** | [app] | nothing holds 8050; recorded pids absent | §12 recovery |
| 4 | **`pm2 delete woa23`** — exact name | [app] | absent from the live list | §12 recovery |
| 5 | **edit the two nginx locations; `nginx -t`** | **[root]** | `syntax is ok` **and** `test is successful`; diff is exactly two changed lines | revert the byte-copy; **do not reload** |
| 6 | **reload nginx** (never restart) | **[root]** | reload completes; `nginx -T` shows `http://woa23api` for both locations and **nothing else changed**; upstream unchanged | §10A.6 rollback |
| 7 | **start the f66 app**: `pm2 start <placed config> --only woa23` | [app] | PM2 `online` **and** master argv shows `api.app:app` on `127.0.0.1:8050`, **no `--reload`**, **no `--certfile`/`--keyfile`** | §12 recovery |
| 8 | **verify** PM2 state, master/workers by **(pid, starttime)**, port 8050 sole listener, venv (`sys.prefix`, `realpath(sys.base_prefix)`, `/proc/<pid>/maps` with **zero** `/home/odbadmin/.pyenv`), environ (`WOA23_TLS=off`, `WOA23_PYTHON` = f66 venv; TLS key/cert vars **ABSENT**) | [app] | all pass | §12 recovery |
| 9 | **smoke + cache checks**: **R1** loopback `http://127.0.0.1:8050`, then **R2** public with `Cache-Control: no-cache`, then **R3** public plain — R2/R3 on a **byte-identical URL**; compare status, **body sha256**, URL, headers, `X-api-cache`; public TLS verified at `https://eco.odb.ntu.edu.tw` with **normal certificate validation, never `--insecure`** | [app] | R2 reports **`BYPASS`**; all three show the new behaviour | R2 not `BYPASS`, or `CACHE_OR_ROUTING_FAILURE` -> **STOP**, §12 |
| 10 | **validate the new exec-based stop path**: `pm2 stop woa23` | [app] | master and every worker gone by (pid, starttime); 8050 free | §12 recovery |
| 11 | **restart the same new app** with the predeclared command | [app] | `online`, argv as step 7, **NEW** (pid, starttime) identities | §12 recovery to the OLD app |
| 12 | **verify again** — steps 8 and 9, second pass | [app] | all pass | §12 recovery |
| 13 | record the final state | [app] | process tree, argv, environ from `/proc`, store fingerprint, post-change `nginx -T` | — |

**Rollback triggers:** any gate above failing; `nginx -t` failing; an unexpected line in the
nginx diff; `CACHE_OR_ROUTING_FAILURE`; PM2 `online` without the expected argv.

**Rollback is TWO-OPERATOR and nginx goes FIRST**, so service is correct the moment the old
app returns: `pm2 delete woa23` -> **[root]** restore `routes-vm124.conf` byte-for-byte,
verify sha256, `nginx -t`, reload, confirm both locations read `https://` again -> restore
`conf/ecosystem.config.js` (sha256 `ed5dec6c…2159`) -> `pm2 start conf/ecosystem.config.js
--only woa23` -> confirm 8050 serves **TLS** and argv shows `woa23_app:app` -> confirm
`https://eco.odb.ntu.edu.tw` serves the route. **A partial rollback is not a rollback.**

`/api/woa23` returns 502 between steps 2 and 7 — expected and bounded, not a failure.

## 6. Reconfirmed constraints

| | |
|---|---|
| PM2 | **no wildcard, no `all`, no `pm2 kill`, no `save`, no `resurrect`**, no manual signal, no `kill -9`. Every operation names **`woa23`** exactly. Nine apps share this `PM2_HOME` |
| cache | **no purge, ever.** R2's `Cache-Control` request refreshes one entry through ordinary cache behaviour; no entry is deleted, edited or hand-touched |
| old certificate and key | **not modified, not deleted, not moved, not renamed, not chmod-ed.** Rollback restarts the old app, which cannot start without them. Retiring them is a separate, later authorization |
| evidence discipline | **D-3's `a361f70` observation, the synthetic harness timings and the batch sentinel are NOT production equivalence and NOT performance evidence.** `test_s2perf_driver.sh` uses local stand-in servers with synthetic delays: no real API, no real store, no Polars behaviour, no production request. **There is no production performance evidence and none is claimed** |
| **S2** | public TLS private key readable by `odbadmin` (root-group membership; `/etc/letsencrypt/{live,archive}` 710, `live/` symlinks 777). **Open, unresolved, requires explicit recorded risk acceptance.** Not remediated here |
| **AVX2** | the guest masks `avx2`/`bmi1`/`bmi2`/`lzcnt`; mainline polars warns at every import, in production too. **Accepted, unresolved residual risk under B6 (spec 012). Never a CPU-safety PASS.** `POLARS_SKIP_CPU_CHECK` is never set |
| **store content integrity** | **unproven.** The recorded fingerprint is metadata-only; a same-size, same-mtime change is invisible to it. No content digest is in scope |
| **TLS scope** | nginx remains the public terminator with the valid `eco.odb.ntu.edu.tw` certificate; the app moves to plaintext loopback. **The internal hop becomes unencrypted** — accepted as part of A-move. No TLS file is touched |
| A11 · `conf/simu.sh` · retained state | qualified only · separate, untouched · not cleaned |

## 7. The five outstanding owner approvals

1. **Name the `[root]` operator**, available for the whole window — not only step 5–6, since rollback needs them again.
2. **S2 risk acceptance**, explicit and recorded.
3. **Authorise the three config deltas** (§4), including the **3a / 3b** choice for where the config is placed.
4. **Authorise the two-line nginx change** and its in-window timing.
5. **Authorise the window itself**, including steps 10–12 (B1 stop/restart validation inside the same window).

**Until all five are given, D-4 is NOT execution-ready. No cutover command has been prepared
for execution, and none was run.**
