# `b35a1` — result: **`INVALID_PRE_START`**

**No B3 result. No B5 result. No PM2 was started.**

- **Ran:** 2026-08-27 05:14–05:17 UTC as `woa23c1ro` (uid 994) on `odb24`
- **Subject:** `823a6edef5bb33c1e724131dafbf288f5af5e6f5`, archive
  `85d032e8c1358ef766fe26a0cbbf4a435643fcb4628d7f166e0e17fe08cf2fb3`, 203 files
- **Outcome:** the staging driver **refused at its own freshness guard**, before any
  PM2 invocation, before any port was bound, before `PM2_HOME` existed.
- **Cause: my error, not the driver's.** Detailed in §2.

Per the authorisation, state is **preserved**: nothing stopped, deleted, killed, cleaned
or re-run, and no new identity chosen.

---

## 1. What completed before the refusal

### 1.1 Pre-start pm2 gate — **PASS**, every value matching `probeC`

```
realpath      /home/odbadmin/.npm-global/lib/node_modules/pm2/bin/pm2   MATCH
sha256        bbb586713050b21d86aa41bde704a3a4776aa300ddcbd9710e81fa7c0089256d   MATCH
pkg version   5.4.2                                                     MATCH
owner/mode    odbadmin:odbadmin / 775                                    MATCH
binary  rwx   r-x     parent rwx  r-x     package rwx  r-x
package.json  r=yes w=no
production state: not under any production path
```

**pm2 was not executed.** The gate reads and hashes only.

### 1.2 Live pre-flight — **PASS**

identity `uid=994(woa23c1ro) gid=993`; all five staging identity paths **absent**;
**18281 live-unbound**; `node` v22.14.0; staging `PM2_HOME` target under this account's
own home and **not** production's; `/home/odbadmin/.pm2` **not writable** by uid 994;
production baseline recorded (pids 4296/5040/5041, starttimes 14214/15825/15829, 8050
listening, `conf/start_app.sh` `4aaed5b7…`, `conf/ecosystem.config.js` `8db9a6ba…`);
pm2G bound on 18265 with its three pids running; 45 listeners, 16 processes.

### 1.3 Archive verified

Local archive digest matched the request exactly (`85d032e8…f2fb3`), 203 files. The
`deploy/` tree and the staging scripts are **byte-identical** between the request's
subject `823a6ed` and current HEAD, so the tree validated is the reviewed one.

---

## 2. Why it stopped — my error

`staging_execute.sh` refused:

```
REFUSING: /home/woa23c1ro/woa23-b35a1 already exists.
  Every element of this run's identity must be absent before staging begins:
    staging root / workdir / PM2_HOME / store / generated config
  It is NOT deleted, emptied or reused. Choose a new identity.
```

**I created that root myself, one step earlier.** `staging_execute.sh` lives *inside* the
archive, and `$HERE` is derived from `BASH_SOURCE`, so it cannot be piped over stdin —
it has to exist on disk before it can run. To get it there I extracted the archive into
`/home/woa23c1ro/woa23-b35a1`, which is **the staging root the driver requires to be
absent**. So my bootstrap consumed the identity the driver was about to check.

**The guard is correct and I would not weaken it.** Its whole purpose is that a
half-finished previous attempt must not be silently reused, and it cannot distinguish
my deliberate bootstrap from exactly that. The `--phase stage` step is *designed* to be
the thing that creates the root, and I pre-empted it.

**What I should have done:** obtain `deploy/staging_execute.sh` at a path **outside**
every element of the run's identity — a bootstrap directory such as
`/home/woa23c1ro/b35a1-bootstrap/` — and invoke `--phase stage` from there, letting the
driver create `woa23-b35a1` itself.

Two further flags were also missing on my first two attempts (`--label`, then
`--pm2-home`); the driver named each in turn and refused cleanly. Those cost nothing —
the identity-collision refusal is the one that matters.

---

## 3. B3 and B5 — reported separately, and both are **NOT RUN**

| | result |
|---|---|
| **B3** — port/config isolation and the real startup argv | **NOT ESTABLISHED.** No PM2 start occurred, so no argv exists to inspect. |
| **B5** — no `--reload` in the running process's argv | **NOT ESTABLISHED.** Same reason. |

**Neither is a pass, a fail, or partial evidence.** Nothing was started, so there is
nothing to report about either.

None of the required `/proc` verifications were performed, because there was no process:
argv, `PM2_HOME` in `environ`, `WOA23_PM2_BIN` non-leakage, absence of production
variables and paths, uid 994 on master and workers, and 18281 ownership — **all
outstanding.**

---

## 4. State, preserved

| | |
|---|---|
| `/home/woa23c1ro/woa23-b35a1` | **PRESENT** — the 203-file staged tree, left in place |
| `/home/woa23c1ro/b35a1-archive.tar` | **PRESENT** |
| `woa23-b35a1-work`, `woa23-b35a1-pm2`, `tmp-b35a1`, `woa23-b35a1/store`, `ecosystem.b35a1.config.js` | **absent — never created** |
| PM2 daemons (this account) | **0** |
| `~/.pm2` | absent |
| staging `PM2_HOME` | **absent — never created** |
| app / pm2 processes | **none** |
| **port 18281** | **free — never bound** |
| listeners | 45, unchanged from baseline |

**Production untouched:** pids 4296/5040/5041 with starttimes 14214/15825/15829,
`conf/start_app.sh` `4aaed5b7…`, `conf/ecosystem.config.js` `8db9a6ba…`,
`/home/odbadmin/.pm2` mtime `1786685045`, boot id unchanged.

**pm2G untouched:** 18265 bound; 1456369, 1456373, 1456374 running.

**The pm2 binary unchanged:** sha256 `bbb58671…9256d`, mtime `1728136844` — the gate's
read did not touch it.

**No SIGKILL was sent. No cleanup was performed.** The staged tree stays for inspection.

---

## 5. What this run does and does not mean

**Does not mean** B3, B5 or any B1–B5 blocker is closed, or moved. **No production
cutover is implied or approached.** The defect remains live in `conf/start_app.sh`
(`4aaed5b7…`), which this run never touched.

**Does establish**, incidentally and worth keeping: the pre-start pm2 gate works —
realpath, digest, version, ownership and the three `r-x` permission triples all matched
`probeC` at the moment of use, and the staging driver's identity-freshness guard is
effective enough to catch its operator.

### 5.1 The standing limitations, unchanged

- **staging would use PM2 `5.4.2`**; **production's PM2 version remains UNVERIFIED**.
- the PM2 binary is under **another account's home**, and the pre-start digest check is a
  **mitigation, not an immutable guarantee** — `odbadmin` can replace it at any time; the
  gate narrows the window to the moment of use, it does not close it.

---

## 6. What is needed next — for your decision, not my action

`b35a1`'s identity is **consumed**: its staging root exists, and the driver will refuse it
by design. I have **not** chosen a replacement.

The correct next attempt would need a **new label and a new first-use port**, plus the
bootstrap fix in §2 — obtaining the driver outside every identity path so `--phase stage`
creates the root itself.

**Whether the existing `/home/woa23c1ro/woa23-b35a1` tree and `b35a1-archive.tar` should
be removed, and by whom, is your call.** They are evidence of this refusal; I have left
them exactly as they are.

## 7. Evidence

| file | contents |
|---|---|
| `scratchpad/b35a1/01-pm2-gate.txt` | the pre-start gate, all values matching probeC |
| `scratchpad/b35a1/02-preflight.txt` | live preflight |
| `scratchpad/b35a1/03-stage.txt`, `03b-`, `03c-` | the three driver invocations and their refusals |
| `scratchpad/b35a1/04-state.txt` | preserved state after the refusal |
