# Read-only host capability probe `probeA` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. Nothing has been run.**
No VM24 contact.

**This is not a B1–B5 validation and closes no blocker.** It answers one question:
**can uid 994 reach `node` and `pm2` at all**, and does it fall back to production's
`PM2_HOME`? It exists so that `b35a1` does not abort after spending an identity, a port
and a staging tree on a capability check.

---

## 1. Why it is separate, and why it must not execute `pm2`

`pm2G`, `pm2B` and `pm2F` all ran as **`odbadmin`** — every path under
`/home/odbadmin/woa23-pm2g/`. The staging harness has **never** run as `woa23c1ro`.
Two prerequisites cannot be checked offline:

| tool | required by | status for uid 994 |
|---|---|---|
| `node` | `staging_execute.sh:367` | **UNKNOWN** |
| `pm2` | `staging_execute.sh:387` | **UNKNOWN** |

Either may live under `odbadmin`'s home or an `nvm` tree uid 994 cannot read.

### 1.1 The design point: `pm2` is never executed

**A `pm2` subcommand — including, on some builds, `pm2 -v` — connects to or SPAWNS the
PM2 daemon, creating `$PM2_HOME` as a side effect.** The review forbids creating a
daemon, so the probe **never runs `pm2` at all**:

- `pm2` is resolved with `command -v`, `readlink -f`'d, and `stat`'d;
- its **version is read from its package's own `package.json` as text**;
- `node --version` **is** run — node is not a daemon and starting it creates nothing.

That distinction is asserted by test, not merely intended.

---

## 2. What the probe reports

| § | reports |
|---|---|
| 1 | `id`, uid, gid, user, `HOME` — and refuses if not uid 994 / not `/home/woa23c1ro` |
| 2 | `PATH` as the account resolves it, **entry by entry**, each marked `dir` or `ABSENT` |
| 3 | `node`: resolved path, realpath, owner/mode/size, **version** |
| 4 | `pm2`: resolved path, realpath, owner/mode/size, version **from `package.json`** |
| 5 | **the PM2_HOME proof** — see §3 |
| 6 | the would-be staging paths, reported **absent and NOT created** |
| 7 | `uv` path, version, sha256 — confirmed rather than assumed |
| 8 | boot id; `ss -ltn` listener **presence** for 8050 and 18265; production pid/starttime; pm2G's three pids observed and untouched |

Every filesystem call is `stat`, `test`, `readlink`, `ls` or a read of a regular file.

---

## 3. The PM2_HOME proof, and the refusal

The probe shows, rather than asserts:

1. `PM2_HOME` **as set in the environment** (expected: unset);
2. the **effective** value pm2 *would* use — `$HOME/.pm2` when unset;
3. that value checked against `/home/odbadmin/.pm2`, anything **inside** it, and
   `/root/.pm2`;
4. that value checked against **any** foreign home, not just production's;
5. for each production PM2 path: whether it exists, is readable, is **writable** — and
   it **refuses if this account can write one**.

**It refuses (exit 3) if the effective `PM2_HOME` is a production PM2 path or is under
another account's home.** `PM2_HOME` is never exported and no PM2 directory is created.

The guards are pure functions, tested both ways: `/home/odbadmin/.pm2`, `.pm2/`,
`.pm2/pids` and a deep path inside it are all **refused**, while `/home/odbadmin/.pm2backup`
and `.pm2-old` must **not** false-positive, and `/home/woa23c1ro-other` **is** foreign.

---

## 4. What the probe will NOT do

There is **no code path** for any of these — asserted against the source, because a probe
that wrongly started a daemon would already have started it:

- `pm2 start`, `stop`, `delete`, `kill`, `save`, `resurrect`, `jlist`, `list`, `ping` —
  or executing `$PM2` as a command at all
- creating a PM2 daemon or any `PM2_HOME`
- creating a staging tree, workdir or store
- binding any port
- contacting the production API
- modifying production PM2, files, ACLs or permissions

Also asserted absent: `mkdir`, `touch`, `chmod`, `chown`, `rm`, `curl`, `wget`, `nc`,
any redirect targeting a file (every redirect is `/dev/null` or a descriptor), any
`ss -p`, and any `export PM2_HOME`.

---

## 5. Offline verification already done

**`scripts/test_probe_host_capability.sh` — 53 assertions, passing.** Guards as pure
functions; prohibitions as source properties.

**And the probe was actually run locally**, where it must refuse:

```
exit=3
  REFUSE: expected uid 994, got 501
  REFUSE: unexpected HOME: /Users/cywhale
  REFUSE: pm2 is NOT on PATH for this account.
$HOME entries before=97 after=97  UNCHANGED
~/.pm2 created? no
pm2 daemons started: 0
```

That run also exercised the **"pm2 is NOT on PATH"** branch — the exact failure mode
this exists to catch on the host.

**Two of my own assertions were wrong and were fixed rather than loosened:** the redirect
check counted nine `<unset>`/`<unreadable>` placeholders as file redirects, and the
exit-code check required `^exit 3$` and missed the indented line.

---

## 6. Execution subject

```
commit           fca22cd9d05cd1b6ce4a8da80b6a27f1aaaf7228
subject line     probe: read-only uid-994 capability probe that never executes pm2
archive sha256   ec2dbb201a91e61b30d0fe1cfa4134854c7076e3889ac9ab4d57b6ea2c8b6655
files            206
file-list sha256 83fc7c8a1858d437bb52e094ae40c18a74d998bf5a9bf344f3d0e30d48bdbe9f
```

`verify_clean_archive.sh` — **all passed (16 assertions)**.

```
sha256 of the probe : (verified in pre-flight against the file-list digest above)
scripts/probe_host_capability.sh
scripts/test_probe_host_capability.sh
```

**Offline evidence:** three serial batches at `fca22cd`, each recording HEAD itself —
all attest `head=fca22cd… dirty=0`. **48 suites, 4015 assertions, 0 non-zero exits,
0 differences** across all three pairings. Roots `6WxHnW`, `UJovaS`, `l6Yjif`.
`test_probe_host_capability.sh` 53.

`api/query.py` unchanged at `50907dee…2ca8`.

---

## 7. Execution identity

| | value |
|---|---|
| **label** | **`probeA`** |
| **grant** | *(none required — the probe has no grant guard, because it creates nothing)* |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993** |
| **connection** | direct SSH, campaign Ed25519 key, `BatchMode=yes`, `IdentitiesOnly=yes`, `RequestTTY=no` |
| **ports** | **none.** No port is bound, allocated or reserved. |
| **staging tree / workdir / store / PM2_HOME** | **none created** |
| **files written on the host** | **the probe script only**, copied to `/home/woa23c1ro/probe_host_capability.sh` |

The single byte written to the host is the probe script itself, under the validation
account's own home. If you prefer it not be written at all, it can be piped over SSH
stdin instead — say so and I will do that.

---

## 8. Production-impact statement

**Intended impact: NONE.**

| | |
|---|---|
| production files | **not read except** `/proc/<pid>/stat` for pid/starttime; nothing written |
| production PM2 | **not touched.** `/home/odbadmin/.pm2` is checked for existence and writability and **never entered, read into, or set as `PM2_HOME`** |
| production service | **not contacted.** Zero API requests. Not stopped, not restarted |
| pm2G / 18265 | **untouched.** Presence via `ss -ltn` and world-readable `/proc` only |
| ports | **none bound** |
| store, ACLs, runtime, `.lock`, permissions | **not touched** |
| PM2 daemon | **none started**, by construction |

---

## 9. Failure handling

The probe **runs every section and then reports**, rather than exiting at the first
refusal — so one run gives the whole capability picture instead of one problem at a
time. Exit **3** if any guard failed; **0** only if all passed.

**A refusal is a result, not an error to retry.** If `node` or `pm2` is unreachable, the
answer is that the staging harness cannot run as uid 994 as it stands, and the next step
is a decision — not another run.

**No identity or port is consumed either way**, which is the point of separating this
from `b35a1`.

---

## 10. If the probe passes

`b35a1` proceeds as the **combined B3 + B5 staging request**, unchanged: its own
isolated `PM2_HOME` (`/home/woa23c1ro/woa23-b35a1-pm2/`), alternate port **18281**, and a
**synthetic** store — nothing of pm2G reused.

**Its final result will distinguish four things, and will not merge them:**

1. **tool capability probe** — what `probeA` established;
2. **staging-only B3 result** — port from `WOA23_PORT`, no `8050` literal;
3. **staging-only B5 result** — no `--reload` in the argv;
4. **remaining production / cutover validation** — everything only a cutover can show.

**No staging PASS will be described as closing B1–B5 on production.** B3 and B5 will be
reported as **two separate findings**, and neither will be said to close its production
blocker.

### 10.1 One naming note

You wrote **`b35A`**. I am using **`b35a1`** because `b35A` occurs as a substring inside
`uv.lock` — the same coincidental-substring class that disqualified ports 19109, 19113,
19116, 19118 and 19136, and the labels `pm2H`. If you would rather keep `b35A` as the
name in discussion, that is fine; the on-disk label needs to be `b35a1`.

---

## 11. Submission

`probeA` is submitted for **explicit authorisation**. It has not been executed and no
VM24 contact has been made. `b35a1` remains submitted and unauthorised, and will not run
before this probe reports.
