# Post-`pm2 save` verification, and the application-path benchmark

## 0. The accurate record — how these results must be stated

**This section governs. Where anything below reads more strongly, this wording wins.**

| # | statement |
|---|---|
| 1 | **The PM2 dump was updated by a `pm2 save` run by the human operator**, deliberately, and **post-save verification is complete and passed** (§1). I did not run it |
| 2 | **Reboot recovery has NOT been tested.** The saved definition now names the f66 tree, which is what a reboot would resurrect — but **no reboot, and no `pm2 resurrect`, was performed.** The improvement is in the saved configuration, not in a demonstrated recovery |
| 3 | **The 60 `BYPASS` requests are the application/store baseline.** They are the only measurements that reached the application |
| 4 | **The 120-request ladder is a BOUNDED LOAD PROBE — not a full stress test and not a capacity test.** It was capped by design, no level reached saturation, and **no capacity or throughput ceiling was established** |
| 5 | **The CPU figures are point-in-time snapshots and are NOT a capacity conclusion.** They were sampled around levels, not integrated over load, and must not be read as headroom |
| 6 | **Warm-cache and BYPASS results are separate throughout.** Warm-cache figures describe nginx serving from cache and are **not** application evidence |
| 7 | **There is no like-for-like old-version baseline, so no significant improvement is claimed** — and none may be inferred from these numbers |
| 8 | **What changed on production, and by whom:** the human intentionally updated the **PM2 persistence dump**. **Runtime, configuration and nginx were NOT changed.** The HTTP requests I issued may have **read from or populated the nginx cache**, which is ordinary request behaviour — no cache was purged, edited or hand-touched |


**Read-only verification, then benchmarking through the public endpoint only.** No `pm2 save`
was run by me, no backup restored, no app stopped/restarted/reloaded/killed, no wildcard, no
cache purge, no C1/C2, no production modification. `test_s2perf_driver.sh` was **not** used
and is not performance evidence.

---

## 1. Post-save verification — **PASSED**

### 1.1 Files

| | sha256 | |
|---|---|---|
| `/home/odbadmin/backup/woa23/dump.pm2` (human backup) | `cbfd7d87…d4d3c` | **preserved, unmodified** |
| `/home/odbadmin/.pm2/dump.pm2.bak` (PM2's own pre-save copy) | `cbfd7d87…d4d3c` | **byte-identical to the backup** |
| `/home/odbadmin/.pm2/dump.pm2` (new) | `7a850585…9e1cd` | written 2026-09-04 11:13:33 |

The independent backup and PM2's own `.bak` having the **same digest** is a useful
cross-check: both are the pre-save state, so the "before" side of the comparison is
corroborated by two sources.

### 1.2 All nine apps — saved definitions

| name | cwd | script | interp | rst |
|---|---|---|---|---|
| dask-scheduler | `/home/odbadmin/python` | `/bin/bash` | none | 0 |
| dask-worker | `/home/odbadmin/python` | `/bin/bash` | none | 0 |
| gateway | `/home/odbadmin/proj/apiverse/server` | node binary | node | 0 |
| ghrsst | `/home/odbadmin/python/ghrsst` | `ghrsst/conf/start_app.sh` | bash | 25 |
| ghrsst_mcp | `/home/odbadmin/python/ghrsst` | `ghrsst/mcp/metocean_mcp…` | (py) | 0 |
| mhwapi | `/home/odbadmin/proj/marineheatwave/API` | `API/conf/start_app.sh` | bash | 1 |
| odbbathy | `/home/odbadmin/python/odbbathy` | `odbbathy/conf/start_app.sh` | bash | 0 |
| tide | `/home/odbadmin/python/tide_tpxo10_atlas_v2` | `…/conf/…` | bash | 0 |
| **woa23** | **`/home/odbadmin/python/woa23-f66ddd8/dev2026`** | **`…/dev2026/deploy/production_app.sh`** | bash | **0** |

**9 apps before, 9 after. None added, none removed.**

### 1.3 `woa23` — every required check

| check | result |
|---|---|
| cwd is the f66 tree | **OK** — `/home/odbadmin/python/woa23-f66ddd8/dev2026` |
| script is the f66 launcher | **OK** — `…/dev2026/deploy/production_app.sh` |
| `WOA23_PYTHON` is the f66 venv | **OK** — `/home/odbadmin/python/woa23-f66ddd8/.venv/bin/python3.11` |
| `WOA23_TLS` | **OK** — `off` |
| old `/home/odbadmin/python/woa23` no longer the saved definition | **OK** — absent from cwd and script |
| TLS key/cert env vars | **ABSENT** in both the live definition and the dump |
| status | **`online`** |

`WOA23_ZARR_STORE` remains `/home/odbadmin/python/woa23/data`. **That is correct and
intended** — the production store was never moved; only the application tree changed.

### 1.4 Differences from the backup, and why each is expected

**Only safe fields were compared and printed.** A PM2 dump carries every app's full
environment, so a naive diff would have spilled nine services' secrets into a log; the
comparison was restricted to an allow-list (`pm_cwd`, `pm_exec_path`, `exec_interpreter`,
`exec_mode`, `restart_time`, `append_env_to_name`, `kill_timeout`, `autorestart`, `instances`
and five `WOA23_*` keys).

| app | difference | assessment |
|---|---|---|
| **woa23** | `pm_cwd`, `pm_exec_path` -> the f66 tree; `kill_timeout` `None -> 20000`; `WOA23_PYTHON`, `WOA23_TLS`, `WOA23_PORT`, `WOA23_WORKERS`, `WOA23_ZARR_STORE` now present; `append_env_to_name` `True -> None` | **the expected update** — exactly the cutover |
| **ghrsst** | `restart_time` `0 -> 25` | **runtime counter, not configuration.** Live `pm2 jlist` also reports 25 — the dump recorded the current count. The backup was written 2026-08-14; `ghrsst` has restarted 25 times since |
| **mhwapi** | `restart_time` `0 -> 1` | same — live value is 1 |
| the other six | **no change in any safe field** | |

**No unexpected difference.** No app's cwd, script, interpreter or exec mode changed except
`woa23`'s.

### 1.5 One check of mine was wrong, and I am correcting it rather than the evidence

My verifier asserted `append_env_to_name is False` and reported **FAIL** on `None`.

**The save is correct; my assertion was too strict.** The live `pm2 jlist` also shows
`append_env_to_name: None`, so the dump faithfully persisted live state. PM2 normalises an
explicit `false` in the config to an absent value in `pm2_env`, and absent is false. What
mattered was that the **old** value — `True`, the hazard the plan flagged, where
`pm2 start --env` would create a second `woa23-production` app — is **gone**. It is.

**Verdict: `woa23`'s saved definition is correct and the other eight are unchanged in
configuration.** The backup is retained.

**What this does NOT establish:** that a reboot recovers the service. **No reboot and no
`pm2 resurrect` was performed.** The saved definition is now the one a reboot would
resurrect, which removes the specific defect found in Stage 2 — but **reboot recovery is
untested**, and "reboot-durable" may not be claimed until it is exercised (§0 item 2).

### 1.6 `pm2 save` did not disturb production

| | |
|---|---|
| `woa23` | `online`, **restarts 0**, pid **3505552** — unchanged |
| 8050 | same master + two workers, master start 2026-09-04 09:46:53 |
| nginx | master 1088337 (root); `routes-vm124.conf` mtime **09:39:53**, still **2** `proxy_pass http://woa23api` |
| public API | **200**, TLS verify **0** |

---

## 2. Application-path benchmark

**Only `X-api-cache: BYPASS` responses count as application/store evidence.** A `HIT` is nginx
answering from cache and says nothing about the app. Warm-cache figures are measured but kept
**strictly separate**.

**Eligibility: 60 BYPASS-mode requests issued, 60 confirmed `BYPASS`, 0 excluded.**

Timing is microsecond-resolution from curl (`time_total`, `time_starttransfer`), reported in
ms to 2 decimals; wall clocks are nanosecond (`date +%s%N`) so spans are measurable — the
earlier ladder used 1-second stamps and its throughput was unusable.

### 2.1 Application path — BYPASS only (10 reps/case)

| case | n | ok | p50 | p95 | p99 | min | max | bytes |
|---|---|---|---|---|---|---|---|---|
| `point_profile_json` | 10 | 10 | **52.54** | 62.75 | 64.84 | 46.89 | 65.36 | 9 033 |
| `point_profile_csv` | 10 | 10 | **49.89** | 59.16 | 59.57 | 48.48 | 59.67 | 2 914 |
| `D1_DEPTH_OOR_tp13_json` | 10 | 10 | **37.35** | 37.94 | 38.05 | 36.33 | 38.08 | 2 |
| `D1_DEPTH_OOR_tp13_csv` | 10 | 10 | **38.05** | 38.84 | 39.00 | 37.73 | 39.03 | 34 |
| `D1_DEPTH_SUP_json` | 10 | 10 | **44.50** | 48.87 | 48.93 | 43.12 | 48.94 | 3 075 |
| `small_bbox_full_depth_json` | 10 | 10 | **70.47** | 83.57 | 85.40 | 69.91 | 85.86 | 852 106 |

**TTFB vs total** — transfer is negligible except for the 852 kB case, so these are
server-time dominated:

| case | ttfb p50 | total p50 | transfer |
|---|---|---|---|
| `point_profile_json` | 52.46 | 52.54 | 0.08 |
| `D1_DEPTH_OOR_tp13_json` | 37.28 | 37.35 | 0.07 |
| `small_bbox_full_depth_json` | 68.39 | 70.47 | **2.08** |

### 2.2 Warm cache — separate, and NOT application evidence

All `HIT=10` per case: p50 **26.6 – 28.5 ms** across all six cases, p99 ≤ 29.5 ms.

**The warm path is ~26–28 ms regardless of case, including the 852 kB one — which is the
signature of nginx serving from cache rather than the application doing work.**

**120/120 status 200 · 0 curl errors · success rate 100.0 %.**
**Body digests agree between warm and BYPASS for all six cases** — the cache serves exactly
what the origin produces.

### 2.3 Application-path concurrency ladder — BYPASS only

30 requests per level, **120 total**, light representative query (`point_profile`, 9 kB).
**Every one of the 120 responses was `BYPASS`; 0 excluded.**

| level | n | ok | p50 | p95 | p99 | min | max | throughput |
|---|---|---|---|---|---|---|---|---|
| 1 | 30 | 30 | **52.31** | 64.34 | 70.84 | 46.51 | 73.38 | **13.81 rps** |
| 2 | 30 | 30 | **56.89** | 73.69 | 85.96 | 47.15 | 86.04 | **24.91 rps** |
| 4 | 30 | 30 | **58.41** | 103.31 | 117.51 | 47.49 | 118.18 | **34.22 rps** |
| 8 | 30 | 30 | **87.69** | 125.14 | 126.75 | 47.78 | 127.11 | **52.49 rps** |

**120/120 → 200 · 0 errors · error rate 0.00 %.**

**Process state at all 9 sample points:** `online`; **restarts 0**; master **3505552** with
starttime **180143568** — one distinct value; worker pids one distinct set; listener always
present; **RSS 469 232 – 470 516 kB (delta 1 284 kB)**; CPU 0.2 – 0.3 %.

**Reading it honestly:** minimum latency stays ~47 ms at every level, while p50 rises 52 → 88
ms and throughput rises 13.8 → 52.5 rps. With **2 workers**, that is queueing beginning around
concurrency 4–8 — expected, not a fault. **No level was run to saturation, and no throughput
ceiling was established;** the ladder was bounded by design.

## 3. Comparability — unchanged limits

**No like-for-like older baseline exists. No improvement is claimed.** The request path itself
changed (app-terminated TLS -> plaintext behind nginx), the interpreter changed (pyenv 3.11.4
-> standalone 3.11.14), older figures were not gathered under a declared cache state, and
C1/C2 were not re-run. These are a **first application-path baseline** of the new production
API, and nothing more.

## 4. Production state changes

**None from this session.**

| | |
|---|---|
| `woa23` | `online`, restarts **0**, pid 3505552 — unchanged throughout |
| 8050 | one listener, same master and workers |
| `dump.pm2` | mtime 11:13:33, digest `7a850585…` — **written by the human's `pm2 save`, not by me** |
| backup | `cbfd7d87…` — **intact** |
| nginx | `routes-vm124.conf` mtime 09:39:53, 2 `http://woa23api` — untouched |
| cache | **not purged** |
| store | read-only; not modified |
| public API | 200, TLS verified |

## 5. Risks carried unchanged

| | |
|---|---|
| **PM2 persistence** | **the Stage-2 defect is corrected** — the dump now names the f66 tree. **Reboot recovery itself remains UNTESTED**; no reboot or `pm2 resurrect` was performed |
| **AVX2 / polars** | standard polars **1.27.1**; guest masks `avx2`; warning at every import. **Accepted, unresolved residual risk under B6 — never a CPU-safety PASS.** `POLARS_SKIP_CPU_CHECK` never set |
| **S2** | public TLS private key readable by `odbadmin` — accepted, **unresolved**, not claimed fixed |
| **store content integrity** | **unproven**; the fingerprint is metadata-only |
| **internal hop** | nginx -> app is plaintext on loopback, accepted as part of A-move |
