# `b1s1` — **QUALIFIED STAGING-ONLY STOP-PATH PASS**

> ## Final classification: **qualified staging-only stop-path PASS**
>
> This wording is fixed. It is **not** to be upgraded — not to "B1 PASS", not to
> "stop-path validated", not to "PASS" unqualified — in this document or in anything
> citing it.

**The qualification is part of the result, not a caveat attached to it.** This is a
**staging stop-path validation on PM2 5.4.2 under uid 994**. It does **not** validate the
production stop path, does **not** close **B1**, and does **not** establish that production
is unchanged — only that this run never reached it.

### The six limitations that travel with this result

| # | limitation | status |
|---|---|---|
| 1 | **production B1** | **NOT closed.** Unvalidated and unauthorised |
| 2 | PM2 version | staging ran **5.4.2**; **production's PM2 version is UNVERIFIED** |
| 3 | generated `ecosystem.b1s1.config.js` digest | **INCOMPLETE** — not retained (§9.5). **Must not be written up as verified** |
| 4 | `comm` special characters, truncated `stat`, non-numeric starttime, unreadable `/proc`, PID reuse, `UNKNOWN`/`INDETERMINATE` (exit 8) | **offline evidence only** (§2.1). **Not host-verified** |
| 5 | production PM2, production PIDs, API request count, `conf`/store, non-target apps | **all UNVERIFIED** (§7) |
| 6 | `bs3v1` and `b1s1` retained daemons/trees | **not cleaned, not back-filled** by this result (§6) |

```
subject   787a72d0062ef581a31b752971cfdb5ecb8edc91
archive   5d909f012b4efa3ddc3fefd4786c3751c9832b1d15d8e801a40d73817312134d
files     229    file-list 84f4096447eefb0d067be03fcbdeb48cef830b99a1ae5015faa33708a06ebba9
request   3214360   identity b1s1 / port 19157 / app woa23-b1s1-candidate
account   uid 994 (woa23c1ro)      PM2_HOME /home/woa23c1ro/woa23-b1s1-pm2
boot id   0b513a75-213b-40bf-8219-1c7cbc51a085   (unchanged across the run)
```

---

## 1. Preflight

| # | check | result |
|---|---|---|
| 1 | archive digest rebuilt locally, transferred, re-hashed on host | **all three identical** to the subject |
| 1 | file count / file-list, verified by the driver at stage | **229** / `84f40964…bba9` — match |
| 2 | every identity path absent on the live host | **all absent** |
| 3 | `19157` live-unbound | **0 listeners** |
| 3 | live ledger conflict | **none** |
| 4 | staging `PM2_HOME`, never production's | daemon spawned at `woa23-b1s1-pm2` |
| 5 | protected set | untouched — §5 |
| 7 | B1 grant gate | **7/7 refusals** — §3 |

### 1.1 Two pre-start refusals — TWO INDEPENDENT EVENTS

They are recorded separately and are **not** to be collapsed into one. They have different
causes, different owners, and different consequences.

#### Event 1 — `INVALID_PRE_START` / bootstrap path already exists

| | |
|---|---|
| cause | **mine.** My wrapper's `mkdir -p` created the `--bootstrap` directory one line before invoking the guard that forbids a pre-existing one |
| owner | the wrapper, not the subject |
| exit | **2**, at guard `1b` |
| identity impact | **none** — root, workdir, `PM2_HOME`, tmpdir, store all absent; nothing started; nothing bound |

Full command line, message and state snapshot: **§9.1**; how the directory came to exist:
**§9.2**.

#### Event 2 — `INVALID_PRE_START` / missing `--pm2-home` passthrough

| | |
|---|---|
| cause | **a PROTOCOL / DOCUMENTATION DEFECT IN THE SUBJECT.** `staging_bootstrap.sh`'s usage example passes `--pm2-home` as a *bootstrap* flag but omits it from the passthrough after `--`, while `staging_execute.sh:131` requires it in the stage phase. **Following the documented example verbatim cannot succeed** |
| owner | **the subject**, surfaced by my use of it. The bootstrap's *executable behaviour* is correct against its own stated contract (§9.4) |
| exit | **2**, during the driver's argument validation, before any extraction |
| identity impact | **none** — all identity paths absent, `19157` unbound |

Full command line, the verbatim handover line showing the missing flag, and the
usage-error-vs-defect analysis: **§9.3–§9.4**.

#### The successful attempt used the subject's original bytes

**No subject file was modified, replaced, patched or regenerated on VM24.** The driver came
straight out of the archive and was hashed before and after extraction; the bootstrap
script and `staging_execute.sh` are byte-identical to the subject; and the tree's identity
recomputed **after** the run is still `229` files / `84f40964…bba9`. Digests and proof:
**§9.5–§9.6**.

The only change between Event 2 and success was **in my invocation** — `--pm2-home` added
to the passthrough, and a fresh bootstrap path. **Nothing in the subject was touched**, and
the defect in Event 2 is left in place: fixing it would change the subject.

**Consumed bootstrap paths:** `b1s1-boot`, `b1s1-boot2`, `b1s1-boot3`, plus `b1s1-launch`.
None was deleted.

---

## 2. Gate results

| gate | evidence | verdict |
|---|---|---|
| **jlist field order — the pm2F ground truth** | the real daemon emits `"pid"` **before** `"name"` | **confirmed on 5.4.2**; resolver returned `OK 1761154` from those exact bytes |
| **exact app-name matching** | `woa23-b1s1-candidate` → `OK 1761154`; `woa23-b1s1`, `candidate`, `woa23-b1s1-candidat`, `woa23-b1s1-candidatex` → all `NOTFOUND` | **PASS** — no prefix, suffix or near-miss match |
| **`ppid` from `status`** | `PPid:\t1761143`, rc 0 | **PASS** |
| **`starttime` past the comm** | master `124827423`; workers `124827447`, `124827452` | **PASS** |
| **descendant tree** | 2 descendants, both direct children, depth 1 | **PASS** |
| **unresolved scan** | **0 entries** across a live `/proc` holding other accounts' processes | **PASS** |
| **`proc_state`** | all three `ALIVE <starttime>`; pid 999999 → `GONE` | **PASS** |
| **stop target = exact app NAME** | the only stop issued was `pm2 stop woa23-b1s1-candidate` | **PASS** — see §10 |
| **`(pid, starttime)` verification** | master + both workers recorded **before** the stop, each re-checked after; all three gone | **PASS**, exit 0, waited 0s of 30s |
| **port released** | `19157` 1 → 0 | **PASS** |
| **idempotency** | daemon reports `pid 0`, `status "stopped"` → resolver `STOPPED` → exit 0 | **PASS** |
| **named app only** | `pm2 stop woa23-b1s1-candidate`; no `all`, no wildcard, no `kill`, no SIGKILL, no `save`, no `resurrect` | **PASS** |

### 2.1 What was NOT exercised on the host, and is offline evidence only

**`comm` on this host is plain `python`** — no space, no parenthesis. So the real `/proc`
did **not** exercise the parsing defect that motivated the fix. The space/parenthesis,
truncated-`stat`, non-numeric-`starttime`, unreadable-entry and PID-reuse cases are covered
**offline only** (`test_stop_proc_parsing.sh`, 100 assertions; `test_production_stop.sh`,
59). What the host confirmed is that the **new readers work correctly on real data**; it did
not confirm they handle malformed data, because the host produced none.

`UNKNOWN`/`INDETERMINATE` was likewise **not** triggered here: every process was cleanly
`ALIVE` then cleanly `GONE`. Exit 8 has **never fired on a host**.

---

## 3. The grant gate — 7/7, against the live daemon

Each refused **before any `pm2` invocation**, and the app was verified still `online` on
`19157` afterwards.

```
no grant at all          rc=2  WOA23_B1_GRANTED is not 'yes' (got '<unset>')
empty grant              rc=2  ... (got '<unset>')
wrong value (1)          rc=2  ... (got '1')
wrong case (YES)         rc=2  ... (got 'YES')
staging grant instead    rc=2  ... (got '<unset>')
B1 + PM2C grant          rc=2  WOA23_PM2C_GRANTED is set alongside WOA23_B1_GRANTED
B1 + D1 grant            rc=2  WOA23_D1_GRANTED is set alongside WOA23_B1_GRANTED
```

**Grant did not reach the service.** The running app's environment carried exactly the ten
allowlisted `WOA23_*` variables and no grant of any kind. **Precisely:** the app was started
*before* any B1 grant existed in the session, so this run shows the service ran without it —
it does **not** exercise "grant set, then withheld from a newly started service". That
stronger claim rests on the `unset` in the source and its offline tests.

---

## 4. TLS environment isolation — separate finding, staging only

`WOA23_TLS_KEYFILE` and `WOA23_TLS_CERTFILE` were **ABSENT from the master and both
workers**, verified from `/proc/<pid>/environ`. No `/home/odbadmin/.../conf` path appeared
anywhere in the environment, and argv carried no `--keyfile`/`--certfile`.

**This is not a B1 result and is not merged into one.** It says nothing about production
TLS, which remains ON and untouched.

---

## 5. Before / after — and what is untouched

Boot id **unchanged**, so every starttime comparison is valid rather than merely equal.

| | before | after |
|---|---|---|
| listeners | 45 | **45**, byte-identical set — no address present in one and not the other |
| `19157` | 0 | **0** (1 while running) |
| `18265` — pm2G, retained | 1 | **1** |
| `18283` — bs3v1 | 0 | **0** |
| `8050` — production API | 1 | **1** |
| `8786`/`8787` — dask | 3 | **3** |
| bs3v1 daemon `1709473` | present, starttime `114971541` | **present, starttime `114971541`** — same process |
| bs3v1 tree / `PM2_HOME` / `b35a1` tree | present | **present** |
| subject tree | 229 / `84f40964…bba9` | **229 / `84f40964…bba9`** |

**These are non-contact claims.** They show this run did not reach those things. **They are
not verification that production is unchanged** — see §7.

---

## 6. Retained state — nothing cleaned

| item | state |
|---|---|
| **`b1s1` PM2 daemon, pid `1761143`, starttime `124827409`** | **RUNNING, retained.** Daemon cleanup is its own authorisation and is never implied by stopping an app |
| `b1s1` staging tree, workdir, uv cache | retained |
| bootstrap paths `b1s1-boot`, `-boot2`, `-boot3`, `b1s1-launch` | retained |
| `b1s1-archive.tar`, `b1s1-evidence/` | retained |
| **`bs3v1` daemon / tree, `b35a1` tree** | **untouched — out of scope.** [`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md) remains separate |

**No survivor and no indeterminate process.** The stop reported every recorded process gone,
and all three were independently confirmed absent.

---

## 7. Production — explicitly NOT validated

| | status |
|---|---|
| **production B1** | **OPEN.** Not validated, not attempted, not authorised |
| production stop path | **not exercised** |
| production PM2 state | **NOT VERIFIED** (blocker B-1) |
| production PID/starttime | **NOT VERIFIED** (B-2) |
| API request count | **NOT VERIFIED** (B-3) |
| production store / `conf/` identity | **NOT VERIFIED** (B-4) |
| non-target PM2 apps | **NOT VERIFIED** (B-5) |
| **production PM2 version** | **NOT VERIFIED** (B-6) |

**This result may not be back-filled as a production B1 PASS**, cited as production
evidence, or read as "production unchanged". The listener inventory is the only
cross-cutting check, and it shows no listener changed — that is a statement about *sockets*,
not about production's PM2 state, store, conf or request counts.

**B3 and B5 are untouched.** `bs3v1`'s staging-only evidence stands as recorded with its
five qualifications, neither extended nor re-scored.

---

## 8. One-line form, for anything that cites this run

> `b1s1` is a **staging-only stop-path PASS on PM2 5.4.2 under uid 994**. Production B1
> remains **unvalidated and unauthorised**, and production PM2 state, store, conf and API
> request counts remain **unverified**.

---

## 9. Provenance clarification

Added after review, before any further action. **No VM24 contact was made to produce this
section** — every digest below is computed locally from the subject archive and compared
against what the host printed during the run.

### 9.1 Refusal 1 — `INVALID_PRE_START / bootstrap already exists`

**Command** (wrapper `b1s1-stage.sh`, piped to `bash -s` as uid 994):

```
mkdir -p /home/woa23c1ro/b1s1-boot                       # <-- THE CAUSE
tar -xO -f /home/woa23c1ro/b1s1-archive.tar \
    dev2026/deploy/staging_bootstrap.sh > /home/woa23c1ro/b1s1-boot/staging_bootstrap.sh

WOA23_PM2C_GRANTED=yes bash /home/woa23c1ro/b1s1-boot/staging_bootstrap.sh \
  --archive   /home/woa23c1ro/b1s1-archive.tar \
  --archive-sha256 5d909f01…134d \
  --bootstrap /home/woa23c1ro/b1s1-boot \
  --root      /home/woa23c1ro/woa23-b1s1 \
  --workdir   /home/woa23c1ro/woa23-b1s1-work \
  --tmpdir    /home/woa23c1ro/tmp-b1s1 \
  --pm2-home  /home/woa23c1ro/woa23-b1s1-pm2 \
  -- --phase stage --label b1s1 --port 19157 --app woa23-b1s1-candidate \
     --files 229 --filelist 84f40964…bba9
```

**Refusal**, at guard `1b`, after guard `1` had already passed:

```
REFUSING: the bootstrap path already exists: /home/woa23c1ro/b1s1-boot
  A pre-existing bootstrap may hold a driver from another archive, or from a
  previous attempt. It is NOT emptied or reused. Choose a fresh bootstrap path.
```

**Exit code: 2.**

**State snapshot immediately after:**

```
absent: /home/woa23c1ro/woa23-b1s1            absent: /home/woa23c1ro/woa23-b1s1-work
absent: /home/woa23c1ro/woa23-b1s1-pm2        absent: /home/woa23c1ro/tmp-b1s1
absent: /home/woa23c1ro/woa23-b1s1/store
19157 listeners: 0        b1s1 pm2 daemon: absent
b1s1-boot/ : one file, staging_bootstrap.sh (15376 bytes), placed by the wrapper
```

Nothing of the identity existed; nothing was started; nothing was bound.

### 9.2 How the bootstrap path came to exist — item 2, answered directly

**My wrapper created it, one line before invoking the guard that forbids it.** The bootstrap
*script* has to be read from somewhere, and I put it in the very directory I then passed as
`--bootstrap`. The tool's contract is that it **creates** that directory itself, so any
pre-existing one is refused.

This is the same shape as `b35a1`, where my extraction created the staging root the guard
required absent — a wrapper preparing the thing a guard exists to find absent. The
difference, and the reason this run continued while `b35a1` did not: **`b35a1` created an
IDENTITY path; this created only the BOOTSTRAP path**, which spec 013's BOOTSTRAP-vs-IDENTITY
distinction explicitly holds separate, and which the refusal itself tells you to replace.

### 9.3 Refusal 2 — `INVALID_PRE_START / missing pm2-home forwarding`

**Command** (wrapper `b1s1-stage2.sh`; bootstrap script now in a *separate* directory,
`--bootstrap` left for the tool to create):

```
WOA23_PM2C_GRANTED=yes bash /home/woa23c1ro/b1s1-launch/staging_bootstrap.sh \
  --archive /home/woa23c1ro/b1s1-archive.tar --archive-sha256 5d909f01…134d \
  --bootstrap /home/woa23c1ro/b1s1-boot2 \
  --root /home/woa23c1ro/woa23-b1s1 --workdir /home/woa23c1ro/woa23-b1s1-work \
  --tmpdir /home/woa23c1ro/tmp-b1s1 --pm2-home /home/woa23c1ro/woa23-b1s1-pm2 \
  -- --phase stage --label b1s1 --port 19157 --app woa23-b1s1-candidate \
     --files 229 --filelist 84f40964…bba9
```

All bootstrap guards passed, including the archive digest and the driver's
before/after-extraction digest match. **Handover line, as printed:**

```
/home/woa23c1ro/b1s1-boot2/deploy/staging_execute.sh --root /home/woa23c1ro/woa23-b1s1 \
  --archive /home/woa23c1ro/b1s1-archive.tar --phase stage --label b1s1 --port 19157 \
  --app woa23-b1s1-candidate --files 229 --filelist 84f40964…bba9
```

**No `--pm2-home` in it.** The driver then refused:

```
--pm2-home is required in the stage phase too.
```

**Exit code: 2.** State snapshot after: all four identity paths **absent**, `19157`
listeners **0**. The refusal happened during the driver's argument validation, before any
extraction.

### 9.4 Item 3 — usage error, or executable defect? **Both, and precisely which.**

| | finding |
|---|---|
| **executable behaviour** | **CORRECT.** `staging_bootstrap.sh:332` is `exec "$BOOT/deploy/staging_execute.sh" --root "$ROOT" --archive "$ARCHIVE" "$@"`, and line 42 states: *"Everything after `--` is passed to the driver unchanged, with `--root` and `--archive` supplied automatically."* The code does exactly that. **This is not a code defect against its own stated contract.** |
| **the usage example** | **DEFECTIVE.** The example at lines 33–40 passes `--pm2-home` as a *bootstrap* flag but **omits it from the passthrough after `--`**, while `staging_execute.sh:131` makes it **required in the stage phase**. **Following the documented example verbatim cannot succeed.** |
| **my command** | reproduced that example, substituting this run's values. So the immediate cause was my command, and the reason my command was wrong was the example it was taken from. |

**Classification: a documentation defect in the subject, surfaced by my use of it.** Not a
behavioural defect, and not solely my invention. There is also a design smell worth
recording: `--pm2-home` must be supplied **twice** — once for the bootstrap's own guards and
again in the passthrough — which is precisely the shape that invites this mistake.

**No fix is applied here.** Changing `staging_bootstrap.sh` would change the subject, and
this section exists to describe `787a72d` as it is. The correction for **future** requests
is [spec 019](019-bootstrap-invocation-protocol-note.md), which changes no code and does
**not** require `b1s1` to be re-run.

### 9.5 Item 4 — what the successful run actually executed

| question | answer |
|---|---|
| driver taken directly from the subject archive? | **Yes.** `tar -xO` from `b1s1-archive.tar`, hashed **before** extraction and again **on disk**, compared by the tool |
| bootstrap script = subject bytes? | **Yes** |
| `staging_execute.sh` = subject bytes? | **Yes** |
| any subject file modified, replaced, patched or regenerated on VM24? | **No** — see §9.6 |

**Digests — computed locally from the subject archive, and identical to what the host
printed during the run:**

```
8c023ab6f0820a384ed6ed304b517bf0df57ac50e812e990935a11d391a54bc4  deploy/staging_bootstrap.sh
b6eb2e5540259222bb5a3ee446749d50ad7030c1728ac2701527c4642bad2630  deploy/staging_execute.sh
d1e5630143fee2bc62fbdfc4df68b8dfeb57eb8dce6d529d11e5197013dd25ed  deploy/production_stop.sh
0b2e719aecb0fae13ec0271cf03dde9ff8fdd33dea2342bc1cf70481d9d04f60  deploy/make_staging_override.js
archive 5d909f012b4efa3ddc3fefd4786c3751c9832b1d15d8e801a40d73817312134d
```

The host reported `staging_bootstrap.sh` as `8c023ab6…4bc4` both **streamed from the
archive** and **on disk**, and `staging_execute.sh` as `b6eb2e55…2630` both **in the archive**
and **on disk after extraction**. `production_stop.sh` is the blob the run stopped with, and
is the same digest recorded in [`b1v3` §9](B1-staging-stop-path-request-b1v3.md).

**One gap, recorded rather than filled:** the **generated `ecosystem.b1s1.config.js`
digest was not captured** into the retained transcript. The driver did compute and
re-verify it (`config provenance re-verified: <sha>`) immediately before `pm2 start`, and
that line scrolled out of the captured tail. It can be read from the retained tree by a
read-only check, which is **not** performed now because no further VM24 contact is
authorised. **This is an incomplete provenance item, not a verified one.**

### 9.6 Was anything in the subject tree changed on VM24? — No, and here is the proof

After the run, the tree's identity was recomputed **on the host, by the same procedure that
defines it**:

```
files    : 229    (authorised 229)
file-list: 84f4096447eefb0d067be03fcbdeb48cef830b99a1ae5015faa33708a06ebba9
expected : 84f4096447eefb0d067be03fcbdeb48cef830b99a1ae5015faa33708a06ebba9
```

A modified, replaced, patched or regenerated subject file would change that digest. It did
not change.

**What the run did create inside the tree, all excluded from the file-list by name and by
design:** `.venv/` (from `uv sync`), `__pycache__/`, `tmp-b1s1/` (PM2 logs), and the
generated `ecosystem.b1s1.config.js`. The synthetic store lives at `$ROOT/store`, outside
`dev2026/` entirely. **My wrapper scripts were never written into the subject tree** — they
were piped to `bash -s` over ssh and existed only as the shell's stdin.

### 9.7 Item 6 — the two refusals, classified

| | classification |
|---|---|
| refusal 1 | **`INVALID_PRE_START` / bootstrap already exists** |
| refusal 2 | **`INVALID_PRE_START` / missing `pm2-home` forwarding** |
| final attempt | **succeeded using the subject's original bytes**, unmodified, with `--pm2-home` added to the passthrough and a fresh bootstrap path |

Item 5 does not apply: **no executable bytes from outside the subject were used.** The only
non-subject code involved was my own wrapper plumbing — `mkdir`, `tar -xO`, `sha256sum`,
`ss`, and the ssh transport — none of which is part of the validated stop path, and none of
which wrote into the tree.

---

## 10. Correction — what "stop by identity" does and does not mean

The earlier wording in §2 said *"stop by `(pid, starttime)`"*. That is **wrong as
written** and is corrected here, because it could be read as PM2 targeting processes by
pid — which is not what happened and not what the command evidence shows.

| step | what actually happens | evidence |
|---|---|---|
| **1. resolve** | the **exact app name** is matched against `pm2 jlist` (`p.name === app`) to obtain the master pid | `jlist_resolve → OK 1761154` |
| **2. record** | the master and every descendant are recorded as `(pid, starttime)` pairs read from `/proc`, **before** the stop | `1761154:124827423`, `1761163:124827447`, `1761164:124827452` |
| **3. STOP TARGET** | **PM2 is given the app NAME and nothing else** | `pm2 stop woa23-b1s1-candidate` — the only stop command issued |
| **4. verify** | each recorded pair is re-checked afterwards; a pid whose starttime differs is a *different* process and is not ours | all three `GONE` |

**PM2 does not stop by pid or by starttime**, and this run supplies no evidence that it
could. `(pid, starttime)` is the **verification identity before and after**; the **stop
target is the exact app name**. The two are separate mechanisms and are no longer described
as one.

---

## 11. Scope — restated, unchanged by anything above

- **`b1s1` validates the STAGING stop path only.**
- It does **not** close **production B1**, which remains **unvalidated and unauthorised**.
- It does **not** verify production PM2 state, production PIDs/starttimes, the API request
  count, production `conf/` or store identity, non-target PM2 apps, or **production's PM2
  version**.
- The **offline-only** evidence — `comm` containing spaces or parentheses, truncated
  `stat`, non-numeric starttime, unreadable `/proc` entries, PID reuse, and
  `UNKNOWN`/`INDETERMINATE` (exit 8) — **must not be upgraded to host evidence.** The host
  produced none of those conditions; `comm` was plain `python` throughout and every process
  was cleanly `ALIVE` then cleanly `GONE`.
- **`bs3v1`'s retained daemon and tree, and their cleanup, remain a separate
  authorisation** ([`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md)) and were not touched.
