# Stage B — dead `pre_stop` cleanup: **RESULT**

## Classification, fixed

> **Stage B configuration cleanup only · dead config cleanup · NO PM2 lifecycle operation ·
> NO production B1 validation · NO Stage C execution.**

**Not** a B1 result. **Not** production evidence. **Does not** close production B1, which
remains unvalidated and unauthorised.

**What was done:** one dead line deleted from one production file. **What was not done:** no
`pm2 stop/start/restart/reload/delete/kill/save/resurrect`, no signal of any kind, no API
call, no store change, no `simu.sh` change, no other conf/program/PM2/environment change, no
cleanup of `b1s1`, `bs3v1` or any retained state.

---

## 1. File — before and after

```
/home/odbadmin/python/woa23/conf/ecosystem.config.js
  BEFORE  sha256 8db9a6ba1888821c833452ee9602871045e4017500800ae69b00aba9fdde4340   482 bytes, 18 lines
  AFTER   sha256 ed5dec6ca064bd54cfe4a45f33fd416871859e959671164292b21549b96f2159   384 bytes, 17 lines
```

The before-digest was verified **twice** — at preflight and again immediately before the
write — and matched the authorised `8db9a6ba…4340` both times.

### 1.1 The exact diff

```
16d15
<     pre_stop: "ps -ef | grep -w 'woa23_app' | grep -v grep | awk '{print $2}' | xargs -r kill -9"
```

**One line, deleted. Nothing added, nothing else altered.** Guards that ran **before** the
write, each of which would have stopped it:

- line 16 confirmed to be the `pre_stop` line, and the **only** `pre_stop` in the file;
- the diff confirmed to be **exactly one changed line** and a **pure deletion** (no `>` line);
- the candidate **parsed as JS** and compared key-by-key against the original: **13 keys → 12**,
  removed `["pre_stop"]`, added `[]`, and **zero surviving keys whose value changed**.

`pre_stop` occurrences in the file now: **0**.

### 1.2 Metadata preserved

| | before | after |
|---|---|---|
| mode | `664` | **`664`** |
| owner / group | `odbadmin` / `odbadmin` | **`odbadmin` / `odbadmin`** |
| inode | `15732726` | **`15732726`** |

Written **in place** (`cat candidate > file`) precisely so inode, mode and ownership could
not change. Only size (482→384) and mtime moved.

### 1.3 `conf/` file-list — exactly one file changed

```
before 6840b8fc0cc6b106e6abd3314d7e5451ada1b4814f798892b99dee6e47856083
after  4b34f6e82a4a5fa1b04525a554925e39ea15f7e2f00ae209260d351d8febce4c
changed: ecosystem.config.js   (1 of 5)
```

**`simu.sh` digest unchanged** — `3bc46819…eef9` before and after, as required.

---

## 2. Preflight — every gate passed before anything was written

| | |
|---|---|
| target file | `/home/odbadmin/python/woa23/conf/ecosystem.config.js` ✓ |
| digest | `8db9a6ba…4340` ✓ (re-checked immediately before the write) |
| byte-for-byte copy | `stageB-evidence/ecosystem.config.js.ORIGINAL`, digest verified, `cmp` identical, mode/owner preserved |
| daemon | pid **3459**, starttime **13189**, `Name: PM2 v5.4.2: God`, uid 1000 — identical to Stage A |
| `PM2_HOME` | `/home/odbadmin/.pm2`, from the daemon's own environ |
| boot id | `0b513a75-…-1c7cbc51a085` — same as Stage A |
| app name | `woa23`, exact |
| target tree | 4295 / 4296 / 5040 / 5041 |
| `watch` | **false** on `woa23` (and every other app) |
| `dump.pm2` | `cbfd7d87…4d3c` |

---

## 3. After — PM2 and production unchanged

| check | result |
|---|---|
| all 9 apps: name, pid, status, `restart_time`, `pre_stop` | **identical** |
| `dump.pm2` digest | **unchanged** `cbfd7d87…4d3c` |
| target tree `(pid, starttime)` ×4 | **identical** — `4295:14198 4296:14214 5040:15825 5041:15829` |
| PM2 daemon `(3459, 13189)` | **unchanged** |
| listener inventory | **identical** (45 listeners, same address set) |
| **`8050` still listening** | **yes** |
| `18265` (`pm2G`) still bound | **yes**, untouched |
| boot id | **unchanged** |
| `woa23` `restart_time` / `unstable_restarts` / `pm_uptime` | **0 / 0 / unchanged** |

---

## 4. External watcher or unexpected reload? — **none**

| evidence | |
|---|---|
| `watch` on `woa23` | **false** — PM2 was not watching this file |
| `pm2.log` last write | `epoch 1787920286` |
| config edit | `epoch 1787970681` |
| gap | **the daemon log predates the edit by ~14 hours** |
| `woa23` mentions in the last 200 `pm2.log` lines | **0** |
| `woa23.outerr.log` mtime | **2026-08-14**, untouched by the edit |

**No daemon activity followed the edit.**

**One thing in the log that is NOT mine, stated so it is not mistaken for a side effect.**
`pm2.log`'s tail shows `ghrsst` restart lines. They are **~14 hours older than the edit**,
concern a **different app**, and `ghrsst`'s pid is **1751875 — unchanged from Stage A**. Its
`restart_time` is 18, and it was 18 in both my before- and after-captures. **Not caused by
this change.**

---

## 5. Rollback — **not needed**

Every verification passed; nothing was restored. The byte-for-byte original is **retained**
at `stageB-evidence/ecosystem.config.js.ORIGINAL` with digest `8db9a6ba…4340`, and the
`conf/` list would return to `6840b8fc…6083`. Restoring is a file copy — **no `pm2`
command** — and was not required.

---

## 6. A11 — **QUALIFIED PROXY ONLY**

```
marker "Handling parameters and time_periods"   before 10950   after 10950   delta 0
```

**No API was called.** This is a **proxy**, and per
[Stage A §6.1](B1-stageA-production-inventory-result.md) it is **not** an exact request
count: the two markers disagree by 17, counts are cumulative since 2024 with no rotation
policy found, and log freshness is inconsistent.

**A delta of 0 is not a claim that the exact API request count is unchanged**, and it is not
written as one. It says only that no query marker was appended during the window.

---

## 7. A divergence created by this change — recorded, not resolved

**The production file and this repository's copy now differ.**

```
repo   conf/ecosystem.config.js   8db9a6ba…4340   (still HAS pre_stop)
prod   conf/ecosystem.config.js   ed5dec6c…2159   (pre_stop removed)
```

They were **identical** before this change. Also: `test_production_stop.sh:99` asserts
*"production's config still HAS the grep pre_stop (the defect is real)"* — an assertion
about the **repo** copy, which still passes, and which would need rewriting if the repo copy
were ever brought into line.

**No repo change is made here.** The authorisation covered the production file only, and
editing the repo copy would change the subject and break an assertion that is currently
truthful about the file it inspects. **This divergence needs its own decision.**

---

## 8. Remaining blockers for production B1 — unchanged by this stage

| | |
|---|---|
| **production B1** | **UNVALIDATED and UNAUTHORISED.** Stage C not requested, not prepared, not executed |
| **A11 exactness** | if production B1 must prove the **exact** request count unchanged, A11 is a **BLOCKER** — the needed counter was not found |
| **store identity** | **metadata-level only** (33G, 123,005 files); no content digest exists |
| **downtime / restart / recovery policy** | **undecided.** Stage C stops the production API and `autorestart` does not restart a deliberately stopped app |
| **`conf/simu.sh`** | still contains the same pattern, including a line that greps `tide_app` **directly**. Deliberately untouched; needs its own decision |
| **repo/production divergence** | §7 |
| resolver order-independence on production PM2 | production is **5.4.2**, the version the resolver was proven against — one difference removed, nothing transferred |

---

## 9. What this result does NOT do

- **Does not** start Stage C, and Stage C is not automatically unblocked by it.
- **Does not** claim production B1 PASS.
- **Does not** back-fill `b1s1`'s **qualified staging-only stop-path PASS** into production
  evidence, nor Stage A's inventory, which validates nothing.
- **Does not** clean `b1s1` (daemon `1761143`) or `bs3v1` (daemon `1709473`) or any retained
  state. [`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md) remains separate.
- `b1v2` remains **WITHDRAWN**; the staging config digest remains **INCOMPLETE**.
