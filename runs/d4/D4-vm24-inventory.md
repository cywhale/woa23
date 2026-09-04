# VM24 deployment-root inventory — read-only

**Nothing was deleted, moved, archived or stopped.** This is an inventory and a proposal;
every classification below awaits separate authorization. **No cleanup runs before the PR
merges.**

**Disk: `/` 393 G, 285 G used, 92 G free (76 %).** No space pressure.

---

## 1. Roots, size, mtime, and what each one is

| path | size | mtime | processes | PM2 | nginx ref | classification |
|---|---|---|---|---|---|---|
| **`/home/odbadmin/python/woa23-f66ddd8`** | 533 M | 09-04 00:24 | **3 (live service)** | **live + dump** | — | **PRODUCTION — KEEP** |
| **`/home/odbadmin/python/woa23`** | **33 G** | 08-05 13:54 | none | **pre-cutover dump** | 5 files* | **KEEP — live store + the rollback path** |
| `/home/odbadmin/python/uv-pythons` | 97 M | 09-03 08:35 | (interpreter) | — | — | **KEEP — the serving interpreter** |
| `cpython-3.11.14-20251217` | symlink | — | — | — | — | **KEEP — the alias `$UV_PYTHON_ROOT`** |
| `/home/odbadmin/python/woa23-143bf8c` | 515 M | 09-03 08:35 | none | **none, ever** | — | **historical candidate — never deployed** |
| `/home/odbadmin/python/woa23-f66ddd8-val` | 552 M | 09-03 21:47 | **15 leftover** | — | — | validation worktree — **archive then remove** |
| `woa23-d42c44e-val` | 552 M | 09-03 17:21 | **15 leftover** | — | — | superseded subject — **remove** |
| `woa23-be3b7b8-val` | 552 M | 09-03 15:37 | **15 leftover** | — | — | superseded subject — **remove** |
| `woa23-c3cf4da-val` | 552 M | 09-03 13:10 | **15 leftover** | — | — | superseded subject — **remove** |
| `woa23-143bf8c-val` | 524 M | 09-03 10:52 | **15 leftover** | — | — | baseline worktree — **archive then remove** |
| `woa23-0d96f7a-val` | 551 M | 09-03 10:01 | **5 leftover** | — | — | **ABANDONED** (rejected LTS subject) — **remove** |
| `py-mirror` | 30 M | 09-02 22:30 | — | — | — | approved CPython archive — **keep until reprovisioning is ruled out** |
| `wheelhouse` | 34 M | 09-03 10:00 | — | — | — | LTS wheel from the **abandoned** subject — **remove** |
| `/home/odbadmin/backup/woa23` | 112 K | 09-04 11:12 | — | — | — | **KEEP — the human's pre-save PM2 backup** |

\* the five nginx references are to the string `woa23` (routes, upstream), **not** to any
deployment root. **No nginx file references any `woa23-*` tree.** The upstream is
`server 127.0.0.1:8050` — port-based, path-independent.

**Evidence directories** (small, all unarchived until this commit):

| path | size | files |
|---|---|---|
| `woa23-f66ddd8-observe` | 96 K | 10 |
| `woa23-f66ddd8-val-runs` | 92 K | 13 |
| `woa23-d42c44e-val-runs` | 88 K | 12 |
| `woa23-be3b7b8-val-runs` | 88 K | 12 |
| `woa23-c3cf4da-val-runs` | 136 K | 12 |
| `woa23-143bf8c-val-runs` | 124 K | 10 |
| `woa23-0d96f7a-val-runs` | 64 K | — |

## 2. Rollback classification — corrected, with the evidence

**A correction to the previous revision of this document.** It said `woa23-143bf8c` "is the only
on-disk rollback target that is not the live one." **That was wrong, and it inverted the two
trees.** `143bf8c` was never deployed and is not a rollback target at all; the rollback path is
`/home/odbadmin/python/woa23`. The corrected reading and the read-only evidence for each claim:

### 2.1 `/home/odbadmin/python/woa23-f66ddd8` — **current production**

| evidence | value |
|---|---|
| live PM2 `pm_cwd` | `/home/odbadmin/python/woa23-f66ddd8/dev2026` |
| live PM2 `pm_exec_path` | `…/dev2026/deploy/production_app.sh` |
| saved dump | **agrees** with the live definition (post-`pm2 save`) |
| processes | master + 2 workers, from `$APP_ROOT/.venv` |
| listener | the single listener on 127.0.0.1:8050 |

### 2.2 `/home/odbadmin/python/woa23` — **old-production tree, the rollback path, AND the host of the live store**

This tree is **not merely historical**. It is load-bearing for the service running right now.

| evidence | value |
|---|---|
| **live store** | `WOA23_ZARR_STORE=/home/odbadmin/python/woa23/data` — in the **current** saved dump; **33 G**, mtime 08-25 |
| pre-cutover PM2 dump (backup) | `woa23` → cwd `/home/odbadmin/python/woa23`, script `…/woa23/conf/start_app.sh`, interpreter `bash` |
| rollback artifact | `conf/ecosystem.config.js`, 384 B, `sha256 ed5dec6ca064bd54cfe4a45f33fd416871859e959671164292b21549b96f2159`, defining `name: 'woa23'`, `script: './conf/start_app.sh'` — **relative**, so its cwd *is* this tree |
| old app entrypoint | `conf/start_app.sh` (172 B) and `woa23_app.py` (19 108 B), both present |
| old TLS material | `conf/privkey.pem` (1 704 B) + `conf/fullchain.pem` (5 603 B), mode 644, mtime 2024-06-18 — the pre-A-move in-app TLS pair |
| old interpreter | `/home/odbadmin/.pyenv/versions/py311/bin/python3.11` → `3.11.4`, still present |
| last write | `tmp/woa23.outerr.log`, **09-04 09:37 — the old master's own `Shutting down: Master` at cutover**; **no process holds it open now** |

**So:** runbook steps RB4/RB5 restore *this* config and start *this* tree. The rollback path,
its entrypoint, its interpreter and its TLS material are all still on disk and coherent.

**Two independent reasons this tree is untouchable**, and either alone is sufficient:

1. **the live store lives inside it** — removing or pruning it would take down current
   production, not a superseded one;
2. **it is the rollback target** — the only tree that can serve the pre-cutover application.

It also contains the symlink `tmp_data -> /home/odbadmin/Data/woa23/netcdf`.

### 2.3 `/home/odbadmin/python/woa23-143bf8c` — **historical candidate / provisioning tree, never deployed**

| check | result |
|---|---|
| `conf/start_app.sh` | **absent** |
| `conf/ecosystem.config.js` | **absent** |
| `woa23_app.py` | **absent** |
| top-level contents | `dev2026` **only** (plus its own `.venv`) |
| PM2 — live jlist | **no entry** |
| PM2 — current saved dump | **0 occurrences** of `143bf8c` |
| PM2 — pre-cutover dump backup | **0 occurrences** of `143bf8c` |
| nginx (`/etc/nginx`, recursive) | **0 files** reference it |
| rollback `ecosystem.config.js` | **0 occurrences** of `143bf8c` |
| running processes | **none** |
| PM2 log directory | **does not exist** — it was never started under PM2 |

**It has no old-application entrypoint at all**, so it cannot serve the pre-cutover app; and it
was superseded by `f66ddd8` before deployment, so it never served the new one either. It is a
**provisioning artifact of a candidate that was never cut over** — worth keeping as campaign
history, but **it is not a rollback target and this document does not describe it as one.**

### 2.4 The corrected summary sentence

> **Production is `woa23-f66ddd8`. The rollback path is `/home/odbadmin/python/woa23`, which
> also hosts the live 33 GB store and must not be touched under any cleanup.
> `woa23-143bf8c` is a never-deployed historical candidate tree, not a rollback target.**

## 3. A finding I have to own: 80 leftover processes, and they are mine

**80 `python3` processes are still running with cwd inside the validation worktrees**, holding
**~81 ephemeral loopback ports** and **≈1 547 MB RSS** in total.

| tree | leftover processes |
|---|---|
| `woa23-143bf8c-val` | 15 |
| `woa23-c3cf4da-val` | 15 |
| `woa23-be3b7b8-val` | 15 |
| `woa23-d42c44e-val` | 15 |
| `woa23-f66ddd8-val` | 15 |
| `woa23-0d96f7a-val` | 5 |

**These are the stand-in HTTP servers from `test_s2perf_driver.sh`, left behind by the batch
runs I executed** — the batch logs' own `NOTE: this suite left processes behind` lines
recorded them at the time and I did not follow up. **5 per batch × 3 batches × 5 subjects,
plus 5 from the abandoned one.** That is residue I created and did not clean.

**Impact, measured rather than assumed:**

- they are **loopback-only** on high ephemeral ports; nothing external can reach them;
- **production is unaffected** — 8050 has exactly one listener, the live service;
- ~1.5 GB RSS and 80 process slots on a host with 92 GB free disk — **clutter, not danger**;
- they will **not** survive a reboot.

**They were not stopped.** No authorization, and nothing is to be cleaned before the merge.

**Separately, and NOT mine:** pids 1456369/73/74 serve `api.app:app` on **127.0.0.1:18265**
from `/home/odbadmin/woa23-pm2g/dev2026`, started 2026-08-24, and pids 1242814 / 1248938 sit in
`woa23-pm2a` / `woa23-pm2b`. These predate this session and were recorded as pre-existing in
the D-4 preflight. **Out of scope, and explicitly excluded from the cleanup below.**

## 4. Pre-merge scope

**Before the PR merges, the only deliverables are code and evidence review material.** No
tree, process, evidence directory or PM2 state is removed, stopped or modified. No
old-version performance comparison is run — the pre-cutover application is no longer running,
and re-starting it to benchmark it would mean a rollback.

The pre-merge material is: [`D4-pr-review-material.md`](D4-pr-review-material.md) (artifact
identity and the PR split), the D-4 observation and benchmark records, and this inventory.

---

## 5. POST-MERGE cleanup checklist — prepared, **not executed**

**Precondition: the PR is merged.** Every step below is read-and-verify before act, and the
whole checklist is subject to separate authorization at the time it is run.

### C0. Archive and hash the evidence FIRST

| | action | gate |
|---|---|---|
| C0.1 | For each evidence dir (`woa23-f66ddd8-observe`, `woa23-{f66ddd8,143bf8c,c3cf4da,be3b7b8,d42c44e,0d96f7a}-val-runs`): produce a per-file `sha256` manifest **on VM24**, `LC_ALL=C`, `sort -u` | manifest file count == `find -type f \| wc -l` |
| C0.2 | Compare each manifest against the mirror already committed under `runs/d4/` | **every** file either matches by digest or is explicitly listed as not-mirrored |
| C0.3 | Anything not mirrored: transfer it in and commit it **before** anything is removed | `git status` clean afterwards |
| C0.4 | Record the manifest digests in the cleanup record | — |

**If C0.2 shows any unmirrored file, the cleanup stops there.** Evidence is archived first, or
not at all.

### C1. Per-tree pre-removal inventory

For **each** candidate tree, record and verify all five before it is touched:

| field | how it is established |
|---|---|
| **full absolute path** | literal, no globs, no `woa23-*` in any removal command |
| **purpose** | which subject/batch it belongs to, and why it is superseded |
| **size** | `du -sh <abs path>` at the moment of the check |
| **no process** | scan `/proc/*/cwd` and `/proc/*/exe` for the path — **zero** matches |
| **no reference** | zero hits in live PM2 jlist, `/home/odbadmin/.pm2/dump.pm2`, the dump backup, `/etc/nginx` (recursive), any `ecosystem.config.js`, and no symlink under `/home/odbadmin/python` resolving into it |

**Candidates (all under `/home/odbadmin/python/`):**

| # | absolute path | purpose | size |
|---|---|---|---|
| 1 | `/home/odbadmin/python/woa23-0d96f7a-val` | abandoned LTS subject, B6-rejected | 551 M |
| 2 | `/home/odbadmin/python/wheelhouse` | `polars-lts-cpu` wheel, used only by #1 | 34 M |
| 3 | `/home/odbadmin/python/woa23-c3cf4da-val` | superseded subject worktree | 552 M |
| 4 | `/home/odbadmin/python/woa23-be3b7b8-val` | superseded subject worktree | 552 M |
| 5 | `/home/odbadmin/python/woa23-d42c44e-val` | superseded subject worktree | 552 M |
| 6 | `/home/odbadmin/python/woa23-143bf8c-val` | baseline validation worktree | 524 M |
| 7 | `/home/odbadmin/python/woa23-f66ddd8-val` | validation worktree for the deployed subject | 552 M |

**≈3.3 GB total.** Free disk is 92 GB — **there is no urgency, and no step here may be
justified by disk pressure.**

`/home/odbadmin/python/woa23-143bf8c` (515 M) is **deliberately not a candidate.** It is the
never-deployed historical candidate tree of §2.3; retiring it is a separate decision with its
own record, not part of a cleanup.

### C2. Stop the 80 leftover processes — by verified identity only

| | rule |
|---|---|
| C2.1 | **`pkill -f` and `pgrep -f` are forbidden.** The observer's own argv enters the matcher; that has cost this campaign eight incidents, including killing the operating ssh session |
| C2.2 | Snapshot first: for every candidate pid, record `(pid, starttime)` — `starttime` parsed **after the final `)`** of `/proc/<pid>/stat` — together with `cwd`, `exe` and argv |
| C2.3 | Confirm each pid's `cwd` is inside one of the seven candidate trees, and that it is **not** the production master or a worker (pids under `woa23-f66ddd8`), **not** the 18265 `api.app` group, **not** `woa23-pm2a`/`b`/`g` |
| C2.4 | Signal **individual pids only**, re-verifying `(pid, starttime)` immediately before each signal — a recycled pid must abort the step |
| C2.5 | `TERM` first; re-check; only then `KILL` for stragglers, with the same identity re-verification |
| C2.6 | Expected reclaim: ≈1 547 MB RSS and ~81 loopback ports. Verify 8050 still has **exactly one** listener afterwards |

**C2 must precede C3** — removing a tree while a process still has its cwd there orphans that
process.

### C3. Removal order

`0d96f7a-val` → `wheelhouse` → `c3cf4da-val` → `be3b7b8-val` → `d42c44e-val` →
`143bf8c-val` → `f66ddd8-val`. Each one re-runs its C1 checks immediately before removal;
each removal names its **full absolute path** literally.

### C4. NEVER TOUCH — the exclusion list

Every item here is excluded from every step above, without exception:

| path / object | why |
|---|---|
| `/home/odbadmin/python/woa23-f66ddd8` | **the running production tree** |
| `/home/odbadmin/python/woa23` | **the live 33 GB store *and* the rollback path** (§2.2) — including `data/`, `conf/`, `tmp/` and `tmp_data` |
| `/home/odbadmin/backup/woa23` | the human's pre-save PM2 dump backup |
| `/home/odbadmin/python/uv-pythons` + the `cpython-3.11.14-20251217` alias | the standalone uv runtime that **serves production** |
| `/home/odbadmin/python/py-mirror` | approved CPython archive; re-obtaining it needs another approved transfer |
| `/home/odbadmin/woa23-pm2a`, `-pm2b`, `-pm2g` and the 18265 listener (pids 1456369/73/74, 1242814, 1248938) | **pre-existing, not from this session** |
| `/home/odbadmin/.pm2/dump.pm2`, and `pm2 save` / `pm2 restart` / `pm2 stop` on `woa23` | cleanup must not rewrite deployment state |
| `/home/odbadmin/.pyenv/versions/py311` | the **old** interpreter — required by the rollback path |
| nginx configuration and TLS material | untouched by cleanup |

### C5. Post-cleanup re-verification — all must pass

| # | check | expected |
|---|---|---|
| V1 | PM2 `woa23` | `online`, `restart_time` unchanged, `pm_cwd` still `…/woa23-f66ddd8/dev2026` |
| V2 | processes | master + 2 workers, from `$APP_ROOT/.venv`; `/proc/<pid>/maps` shows **zero** `.pyenv` for master and every worker |
| V3 | 8050 | **exactly one** listener, on 127.0.0.1 |
| V4 | nginx | `nginx -t` clean; both `/api/woa23` and `/api/swagger/woa23` still `proxy_pass http://woa23api` |
| V5 | PM2 fleet | the same **9** apps as before cleanup; no app changed status |
| V6 | store | `/home/odbadmin/python/woa23/data` present, **33 G**, and `WOA23_ZARR_STORE` in the live env still points at it |
| V7 | rollback path | `conf/ecosystem.config.js` still `sha256 ed5dec6c…f2159`; `conf/start_app.sh`, `woa23_app.py` and the pyenv interpreter all still present |
| V8 | **public API** | a real request over **public HTTPS** to `eco.odb.ntu.edu.tw` returns 200 with the expected payload — never a direct hit on 8050 |
| V9 | dump | `/home/odbadmin/.pm2/dump.pm2` byte-identical to before cleanup |

**If any of V1–V9 fails, stop and preserve the scene.** Do not attempt a repair inside the
cleanup step.

## 6. Guardrails for whoever executes this

- **`pkill -f` / `pgrep -f` are banned** — a pattern matched against command lines matches the
  shell running it. Stop by verified `(pid, starttime)`.
- **Never touch `/home/odbadmin/python/woa23`** — live store *and* rollback path.
- Verify the `runs/d4/` mirror **before** removing any originals (C0).
- **Do not run `pm2 save`, or stop/restart the `woa23` app**, as part of cleanup.
- Removing a tree while a process still has its cwd there orphans that process — hence C2
  before C3.
- No removal command may use a glob. Full absolute paths, one at a time.
