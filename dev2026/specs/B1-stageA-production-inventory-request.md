# Stage A — production **read-only inventory** request

**Status: DRAFT, OFFLINE. Not authorised, not executed. No VM24 contact was made to write
it.** First of three stages ([B](B1-stageB-remove-prestop-change-request.md),
[C](B1-stageC-production-b1-validation-request.md)); each needs its own authorisation.

**Nothing is stopped, restarted, reloaded, deleted or modified. No file is written.** This
stage exists only to replace guesses with facts, because Stage C cannot be written safely
without them.

---

## 1. Scope

**Permitted:** reading production's PM2 state, reading `/proc` for the app's processes,
reading the production config file, reading listener state, reading the PM2 version.

**Forbidden, without exception:** `pm2 stop`, `restart`, `reload`, `delete`, `save`,
`resurrect`, `kill`; any write to any production file; any change to `conf/`, the store,
ACLs or permissions; touching `pm2G`/18265, `pm2A`/`pm2B`, or the retained `bs3v1` and
`b1s1` daemons and trees.

**Account:** the actual owner of production's PM2 state — **expected `odbadmin` (uid
1000)**, to be confirmed. **uid 994 cannot substitute**: `pm2 jlist` as uid 994 returns a
staging daemon, not production's.

---

## 2. PRECONDITION — a read can create state, and this one can

> **`pm2` SPAWNS a God Daemon if none is running.** Every `pm2` subcommand, including
> `jlist`, will start a daemon for that `PM2_HOME` when one is absent.

So `pm2 jlist` is read-only **only if production's daemon is already running**. If it is
not, the "read" creates a daemon — a state change, under production's `PM2_HOME`.

**Required first, before any `pm2` invocation:**

```
ls -l /home/odbadmin/.pm2/pm2.pid          # does a daemon pid file exist?
# and confirm that pid is alive and is a PM2 God Daemon, from /proc
```

**If production's PM2 daemon is NOT already running, STOP.** Report it and take no `pm2`
action: a production API that is up without its PM2 daemon is a situation to understand,
not to alter with a read command.

---

## 3. What to collect

| # | item | method | if unobtainable |
|---|---|---|---|
| **A1** | **exact registered app name** | `pm2 jlist`, exact string, recorded verbatim | **BLOCKER.** No guessing, no wildcard, no prefix match |
| **A2** | **production PM2 version** | `pm2 --version` (does not touch the daemon) | **BLOCKER** |
| **A3** | **production `PM2_HOME`** | the value actually in use, confirmed against `/proc/<daemon-pid>/environ` — not assumed from a header comment | **BLOCKER** |
| **A4** | **target PID + starttime** | `/proc/<pid>/status` (`PPid:`) and `stat` field 22, read after the comm | **BLOCKER** |
| **A5** | **descendants** | every descendant with `comm` **as it actually appears**, `PPid:`, `starttime`, tree depth | **BLOCKER** |
| **A6** | **the live `pre_stop`** | from `pm2 jlist` (the daemon's own copy) **and** from `conf/ecosystem.config.js` on disk | **BLOCKER** |
| **A7** | **boot id** | `/proc/sys/kernel/random/boot_id` | **BLOCKER** |
| **A8** | **listener inventory** | `ss -ltn` — never `-ltnp` | **BLOCKER** |
| **A9** | **non-target PM2 apps** | name, pid, status for every other registered app | **BLOCKER** |
| **A10** | **production `conf/` + store identity** | file list + SHA-256 | **BLOCKER** |
| **A11** | **API request count method** | see §4 | **BLOCKER** |

### 3.1 A6 matters more than it looks

`pre_stop` exists in **two places**: the file on disk, and the **daemon's in-memory app
definition** (persisted in `dump.pm2`). **They can differ.** The file may have been edited
without the change ever reaching the running app.

**The daemon's copy is the one that executes on stop.** Stage B is planned against the file;
if the two disagree, Stage B's plan is wrong and must be rewritten. So both are captured,
and any difference is a **blocker**.

---

## 4. A11 — the API request count, and why it is not obvious

**Measuring it by calling the API increments it.** Any method that issues a request perturbs
the quantity being measured, so an HTTP probe is **not** an acceptable read.

Candidate sources, none yet confirmed:

| | source | concern |
|---|---|---|
| a | an app metrics/health endpoint | **calling it is itself a request** — usable only if it is excluded from the count, which must be shown, not assumed |
| b | PM2 out/error logs (`tmp/woa23.log`) | only if the app logs per-request; needs a counting rule that survives rotation |
| c | a reverse proxy / access log in front of `8050` | outside production's PM2 scope; needs its own read authorisation |
| d | an in-process counter | needs a non-HTTP way to read it |

**Stage A's job is to determine which, if any, exists and is reliably readable.** If none is,
**A11 is a blocker for Stage C** — and Stage C then cannot claim the request count is
unchanged, which is a limitation to record rather than an obstacle to route around.

---

## 5. Protected — untouched and observed only

`pm2G`/18265 · `pm2A`, `pm2B` and every other historical `PM2_HOME` · `bs3v1` retained
daemon `1709473`, tree, bootstrap, workdir · `b1s1` retained daemon `1761143`, tree,
workdir, bootstrap paths · all non-target PM2 apps · ports 18281, 18283, 19157.

Their cleanup remains separate ([`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md)) and is
**not** part of any stage here.

---

## 6. Output

A result document recording every item verbatim, each marked **OBTAINED** or **BLOCKER**,
with no inference and no "no change observed" phrasing anywhere. **No conclusion about B1
is drawn from Stage A** — it collects facts; it validates nothing.

---

## 7. Access disclosure

This session can reach VM24 as `odbadmin` — production's owner — over an existing key.
**That access has not been used for any production PM2 read and will not be until you
authorise Stage A explicitly.** Availability is not authorisation.
