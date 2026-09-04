# Post-cutover controlled observation — `f66ddd8` in production

> **See §0 of [`D4-post-save-and-apppath-benchmark.md`](D4-post-save-and-apppath-benchmark.md)
> for the governing wording.** In particular: the 120-request ladder here is a **bounded load
> probe, not a stress or capacity test**; the CPU figures are **snapshots, not a capacity
> conclusion**; warm-cache and BYPASS are separate; and **no improvement over the old version
> is claimed**. Stage 2's persistence gap was subsequently closed by a **human-run `pm2 save`**,
> and **reboot recovery has still not been tested**.


**All four stages completed. No anomaly.** Nothing was modified: no application, dependency,
PM2, nginx, TLS, store or configuration change; **no `pm2 save`**, no restart/stop/reload/
resurrect/kill, no wildcard, no cache purge, no cleanup. Every request went through the
**public HTTPS endpoint** `https://eco.odb.ntu.edu.tw`; **8050 was never addressed directly**.

## 0. Deployed state, confirmed

| | |
|---|---|
| PM2 app | **`woa23`**, `online`, pm_id 28, pid **3505552**, **restart_time 0**, unstable 0 |
| `pm_cwd` | `/home/odbadmin/python/woa23-f66ddd8/dev2026` |
| `pm_exec_path` | `/home/odbadmin/python/woa23-f66ddd8/dev2026/deploy/production_app.sh` |
| env | `WOA23_TLS=off` · `WOA23_PYTHON=/home/odbadmin/python/woa23-f66ddd8/.venv/bin/python3.11` · `WOA23_PORT=8050` · `WOA23_WORKERS=2` · **`WOA23_TLS_KEYFILE`/`WOA23_TLS_CERTFILE` ABSENT** |
| serving processes | master **3505552** + workers **3505555, 3505556**, all from the **f66 venv** |
| nginx | both `/api/woa23` and `/api/swagger/woa23` on **`proxy_pass http://woa23api`** |
| started | 2026-09-04 09:46:53 |

**The `143bf8c` tree and venv are not referenced by any running process.**

---

## 1. Stage 1 — stability observation (~20 min, read-only)

**21 samples at 60 s intervals. Completed.**

| | |
|---|---|
| status | `online` in **every** sample |
| restarts / unstable restarts | **0 / 0** throughout |
| master identity | pid **3505552**, starttime **180143568** — **one distinct value**, so no silent restart-and-reuse |
| workers | **2** in every sample; pids **3505555, 3505556** — one distinct set |
| 8050 listener | present in every sample |
| RSS (master+workers) | **436 380 – 436 952 kB** — flat, no growth trend |
| CPU | **0.2 – 0.3 %** |

**Application log scan** (`tmp/woa23.log`, `woa23.outerr.log`, `woa23_err.log`): **no
`Traceback`, no `ImportError`/`ModuleNotFound`, no `Illegal instruction`/`SIGILL`, no OOM, no
`MemoryError`, no `CRITICAL`, no worker death or timeout.** The only lifecycle lines are from
the cutover itself:

```
09:41:46  Booting worker 3505294, 3505295      <- an earlier start
09:46:42  Shutting down; Shutting down: Master <- that instance stopped
09:46:54  Booting worker 3505555, 3505556      <- the current instance
```

**Recorded rather than interpreted:** there were two app starts on 2026-09-04, at 09:41 and
09:46. `restart_time` is 0 for the current PM2 entry, so the 09:41 instance was a separate
start/stop during the cutover, not a crash-restart of the current one. **I did not perform
either and cannot attest to what happened between them** — that belongs to the session that
executed the cutover.

**Verdict: stable. No restart, no worker loss, no port loss, no 5xx, no error storm.**

## 2. Stage 2 — PM2 persistence audit (read-only) — **A REAL RISK**

**No `pm2 save` was run. The dump was not modified. Nothing was restarted.**

| | |
|---|---|
| boot unit | **`pm2-odbadmin.service` — `enabled` and `active`** |
| `ExecStart` | **`pm2 resurrect`** |
| `PM2_HOME` | `/home/odbadmin/.pm2` |
| dump file | `/home/odbadmin/.pm2/dump.pm2`, mtime **2026-08-14 14:16:39** |
| apps in dump | 9 |

**The dump still describes the OLD woa23:**

```
woa23 in dump ->  cwd        : /home/odbadmin/python/woa23
                  script     : /home/odbadmin/python/woa23/conf/start_app.sh
                  interpreter: bash
                  WOA23_PYTHON: <absent>
                  WOA23_TLS   : <absent>
```

### The risk, stated concretely

**On reboot, `pm2 resurrect` would start the OLD app, not `f66ddd8`.** The old app serves
**TLS** on 8050 — and nginx now proxies **plaintext** to it. So a reboot today would produce:

1. the **f66 app would not start at all** — it is not in the dump;
2. the **old app would start** from the pre-cutover tree, with the pyenv interpreter and the
   expired app certificate;
3. **`/api/woa23` and `/api/swagger/woa23` would break** — nginx speaking `http://` to a
   TLS socket.

**The cutover is therefore not reboot-durable.** This is not a latent theoretical gap: the
unit is enabled, the dump is three weeks older than the cutover, and the mismatch is exact.

**No remediation was performed.** Running `pm2 save` would fix it in one command, and it is
explicitly outside this session's authority. **It needs its own authorization**, and should be
weighed with the fact that `pm2 save` rewrites the dump for **all nine** apps in this
`PM2_HOME`, not only `woa23`.

## 3. Stage 3 — small production benchmark

**Cases were taken from existing definitions, not invented:** `dev2026/bench/queries.py`
(`point_profile`, `small_bbox_full_depth`) and `runs/d3/D3-execution-request-a361f70.md`
(`D1-DEPTH-SUP`, `D1-DEPTH-OOR-tp13`). `append=mn` is `queries.py`'s documented default.

**60 requests · 5 repetitions per case per mode · no cache purge.** BYPASS was obtained with a
`Cache-Control: no-cache` request header, which the active config's map turns into
`proxy_cache_bypass`.

| case | mode | n | ok | status | `X-api-cache` | bytes | p50 | p95 | p99 | min | max |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `point_profile_json` | plain | 5 | 5 | 200 | HIT | 9 033 | **27.2** | 27.6 | 27.6 | 26.7 | 27.7 |
| `point_profile_json` | bypass | 5 | 5 | 200 | BYPASS | 9 033 | **57.7** | 58.8 | 58.9 | 48.5 | 58.9 |
| `point_profile_csv` | plain | 5 | 5 | 200 | HIT,MISS | 2 914 | 26.8 | 54.8 | 58.3 | 26.8 | 59.2 |
| `point_profile_csv` | bypass | 5 | 5 | 200 | BYPASS | 2 914 | 59.2 | 71.0 | 73.4 | 48.8 | 73.9 |
| `D1_DEPTH_OOR_tp13_json` | plain | 5 | 5 | **200** | HIT,MISS | **2** | 26.7 | 39.2 | 41.7 | 26.7 | 42.3 |
| `D1_DEPTH_OOR_tp13_json` | bypass | 5 | 5 | **200** | BYPASS | **2** | 37.7 | 38.6 | 38.7 | 37.4 | 38.8 |
| `D1_DEPTH_OOR_tp13_csv` | plain | 5 | 5 | **200** | HIT,MISS | **34** | 27.1 | 37.2 | 39.2 | 26.7 | 39.7 |
| `D1_DEPTH_OOR_tp13_csv` | bypass | 5 | 5 | **200** | BYPASS | **34** | 39.0 | 39.0 | 39.0 | 38.5 | 39.0 |
| `D1_DEPTH_SUP_json` | plain | 5 | 5 | 200 | HIT,MISS | 3 075 | 26.9 | 45.9 | 49.7 | 26.6 | 50.6 |
| `D1_DEPTH_SUP_json` | bypass | 5 | 5 | 200 | BYPASS | 3 075 | 44.8 | 48.4 | 48.4 | 44.4 | 48.5 |
| `small_bbox_full_depth_json` | plain | 5 | 5 | 200 | HIT,MISS | 852 106 | 29.1 | 65.5 | 72.8 | 28.7 | 74.6 |
| `small_bbox_full_depth_json` | bypass | 5 | 5 | 200 | BYPASS | 852 106 | 72.7 | 128.6 | 138.0 | 71.9 | 140.4 |

*(latency in ms)*

**By cache state, separated as required:**

| state | n | p50 | p95 | p99 |
|---|---|---|---|---|
| **HIT** | 25 | **26.9 ms** | 29.2 | 35.1 |
| **MISS** | 5 | 50.6 ms | 71.5 | 74.0 |
| **BYPASS** | 30 | **48.5 ms** | 78.0 | 123.3 |

**Success rate 100.0 % — 60/60 status 200, zero curl errors or timeouts.**

**Two findings beyond timing:**

1. **The empty-result fix is live.** `D1-DEPTH-OOR-tp13` — the exact case that returned
   `400` before this campaign — now returns **200** on both routes: **2 bytes** of JSON
   (`[]`) and **34 bytes** of header-only CSV.
2. **Cache and origin agree byte-for-byte.** Body sha256 is stable within every
   case+mode, and **identical between `plain` and `bypass` for all six cases** — the cache is
   serving exactly what a fresh origin fetch produces.

## 4. Stage 4 — bounded stress

**Concurrency ladder 1 → 2 → 4 → 8. 30 requests per level, 120 total — within the 300 cap.**
Light representative query only (`point_profile`, ~9 kB); the 852 kB box case was deliberately
**not** used under load.

| level | n | ok | p50 | p95 | p99 | max | `X-api-cache` |
|---|---|---|---|---|---|---|---|
| 1 | 30 | 30 | 26.5 | 26.9 | 27.0 | 27.1 | HIT ×30 |
| 2 | 30 | 30 | 26.7 | 27.1 | 27.9 | 28.2 | HIT ×30 |
| 4 | 30 | 30 | 26.8 | 27.2 | 27.4 | 27.5 | HIT ×30 |
| 8 | 30 | 30 | **27.2** | 29.5 | 29.6 | 29.7 | HIT ×30 |

**120/120 status 200 · 0 errors · 0 timeouts · error rate 0.00 %.**

**Process state, sampled before and after every level:**

| | |
|---|---|
| status | `online` at all 9 sample points |
| **restarts** | **0** throughout |
| master pid / starttime | **3505552 / 180143568** — one distinct value across the whole ladder |
| worker pids | **3505555, 3505556** — one distinct set; count 2 always |
| listener | present at every sample |
| **RSS** | **463 272 kB at every sample — delta 0 kB** |
| CPU | 0.2 % |

**No abort condition fired:** no 5xx, no timeout, no restart, no worker loss, no port loss, no
CPU/memory approach to any threshold, and nothing in the log resembling a crash, OOM, illegal
instruction or repeated traceback.

### 4.1 Two measurement limits, stated rather than papered over

1. **Throughput at levels 2 and 8 is NOT reportable.** Timestamps were recorded at
   **1-second resolution**, and those levels completed within a single second, so the computed
   rate is an artefact (it printed as ~3×10¹⁰ rps). **Levels 1 and 4 measured 15 and 30 rps**,
   which is consistent with the request count over the observed span, but the ladder was
   **latency-bounded and cap-bounded, not throughput-saturating** — no level was run long
   enough to find a throughput ceiling, and none was intended to.
2. **Stage 4 exercised the CACHED path, not the application.** Every one of the 120 responses
   was `HIT`: the ladder repeats one URL, so nginx served it from cache after the first fetch.
   **These numbers characterise nginx + cache under concurrency, not the WOA23 application or
   the store.** The application path under load was not measured, and no claim is made about
   it.

## 5. Comparability with older data — limits

**No like-for-like older baseline exists, so no improvement is claimed.**

| limit | |
|---|---|
| no matched prior baseline | there is no earlier run of these same queries, through the same public endpoint, in the same cache state, against the old app. Comparison would be **directional at best** |
| cache state differs | these figures separate HIT / MISS / BYPASS; older campaign figures were not gathered under a declared cache state |
| **path changed** | the old app terminated **TLS** on 8050; the new one is **plaintext** behind nginx. The request path itself is different, so latency is not attributable to the application alone |
| interpreter changed | production moved from shared pyenv **3.11.4** to standalone **3.11.14** |
| C1/C2 not re-run | and not authorised here |
| harness timings excluded | `test_s2perf_driver.sh` is a synthetic harness suite and **is not** production performance evidence; it was not used |

**Nothing here may be described as a significant improvement, a performance claim, an SLA, or
production equivalence.** It is a **first measured baseline of the new production API**, and
that is all.

## 6. Production state changes

**None.** Verified after all four stages:

| | |
|---|---|
| PM2 `woa23` | `online`, **restarts 0**, pid **3505552** — unchanged from before Stage 1 |
| 8050 listener | same master + two workers |
| `dump.pm2` mtime | **2026-08-14 14:16:39 — unchanged.** No `pm2 save` was run |
| nginx / TLS / store / config | untouched |
| cache | **not purged.** Stage 3's bypass requests refreshed entries through ordinary cache behaviour |

The only artefacts created are read-only observation logs under
`/home/odbadmin/python/woa23-f66ddd8-observe/`, mirrored into `runs/d4/observe/`.

## 7. Risks carried unchanged

| | |
|---|---|
| **PM2 persistence** | **the one open risk from this session** — see §2. Not reboot-durable |
| **AVX2 / polars** | standard polars **1.27.1**; the guest masks `avx2`. Warning at every import. **Accepted, unresolved residual risk under B6 — never a CPU-safety PASS.** `POLARS_SKIP_CPU_CHECK` never set |
| **S2** | public TLS private key readable by `odbadmin` — accepted, **unresolved**, not claimed fixed |
| **store content integrity** | **unproven**; the fingerprint is metadata-only. Store remained read-only |
| **internal hop** | nginx→app is now **plaintext** on loopback, accepted as part of A-move |
