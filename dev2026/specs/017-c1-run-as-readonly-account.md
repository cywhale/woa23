# 017 — Running C1 as a read-only account, and the shared-runtime question

**Status: DESIGN, for review. Nothing implemented, nothing run, no VM24 mutation.**

Answers three questions the PI asked before any C1 work resumes: how the arms come to run
as UID 994, whether the shared Python runtime is a real exposure, and what — if anything —
must be done host-side.

---

## 1. A correction I have to make first

**I reported that the shared runtime is world-writable and that UID 994 could modify the
interpreter. That was wrong, and it was wrong because I read symlink permission bits as
directory permission bits.**

```
/home/odbadmin/.pyenv/versions/py311
    lrwxrwxrwx  -> /home/odbadmin/.pyenv/versions/3.11.4/envs/py311
/home/odbadmin/.pyenv/versions/py311/bin/python3.11
    lrwxrwxrwx  -> /home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11
```

**Both are symlinks.** On Linux a symlink's mode is always `lrwxrwxrwx` and **the kernel
never enforces it** — permission is decided by the target. `stat -c '%a'` reported `777`
for the *link*; `stat -Lc '%a'` reports `775` for the *target*. I quoted the first and
called it world-writable.

**What the arm's real load path actually is**, with `-S` and `PYTHONPATH=$PKG_CLONE`:

| # | `sys.path` entry | mode | owner |
|---|---|---|---|
| 1 | `woa23-s2-package-clone/dist` | **555** | `odbadmin:odbadmin` |
| 2 | `3.11.4/lib/python311.zip` | *(absent)* | — |
| 3 | `3.11.4/lib/python3.11` | **755** | `odbadmin:odbadmin` |
| 4 | `3.11.4/lib/python3.11/lib-dynload` | **755** | `odbadmin:odbadmin` |
| — | the interpreter `3.11.4/bin/python3.11` | **755** | `odbadmin:odbadmin` |

**World-writable entries across the entire real load path: `0`.**

## 2. Shared-runtime risk assessment

### 2.1 What UID 994 can and cannot write

| surface | mode | can 994 write? |
|---|---|---|
| the interpreter binary (resolved) | `755` | **no** |
| the stdlib tree `3.11.4/lib/python3.11` | `755` | **no** |
| `lib-dynload` | `755` | **no** |
| the package clone `dist/` | `555` | **no** — read-only to everyone, owner included |
| `envs/py311` (the resolved env dir) | `775` | **no** — group is `odbadmin`, and 994 is not in it |
| `envs/py311/.../site-packages` | `775` | **no** — same, and **not on the arm's `sys.path`** under `-S` |

`id woa23c1ro` → `uid=994 gid=993 groups=993`. **It is in no shared group**, so the
group-writable bits throughout the pyenv tree (7,598 dirs, 67,456 files, all group
`odbadmin`) **do not apply to it**.

### 2.2 The one real finding

```
-rw-rw-rw- 1 odbadmin odbadmin 0  /home/odbadmin/.pyenv/versions/3.11.4/envs/py311/.lock
```

**One world-writable file: a zero-byte pyenv lock file.** It is **not on the arm's
`sys.path`** — `-S` excludes `envs/py311`, and the four entries above are the whole path.

**Residual risk: a process running as 994 could truncate or write that lock file.** It
cannot execute it, cannot get it imported, and nothing in an arm reads it. The realistic
consequence is confusing a future `pyenv` operation, not affecting C1 or production.

### 2.3 The recommendation

**Documented residual-risk acceptance, with no change to the runtime.**

| option | assessment |
|---|---|
| **A. Accept and document (RECOMMENDED)** | The real load path has zero world-writable entries. The only exposure is one 0-byte lock file off the import path. Changing nothing means **touching no production runtime**, which is worth more than closing a hole this narrow |
| B. Read-only ACL on the runtime tree | Solves almost nothing — 994 already cannot write the load path — and modifies the runtime production itself uses. Cost far exceeds benefit |
| C. Isolated runtime for C1 | Would answer a different question than C1 asks. C1's premise is production's own interpreter and a clone of its packages; substituting a runtime invalidates the comparison |

**If the `.lock` exposure is nevertheless to be closed, `chmod 664` on that single file is
the whole fix** — a host-side action, needing its own review, and **not performed here**.

**What may NOT be claimed either way:** that C1 runs on an isolated runtime. It does not,
by design. It runs on production's interpreter and a read-only clone of production's
packages, and §2.1 is the precise statement of what that means for a 994 process.

## 3. The run-as mechanism

### 3.1 The constraint that decides it

**An unprivileged user cannot change UID.** `odbadmin` is uid 1000, not root. `setpriv`,
`runuser` and any setuid path all require privilege it does not have, and `sudo -n -u
woa23c1ro` is refused without a password.

**So there is no pure-offline solution. Every design needs one host-side action.** The
question is only which action has the narrowest boundary.

### 3.2 The options

| option | mechanism | boundary | verdict |
|---|---|---|---|
| **1. SSH directly as `woa23c1ro`** | give 994 a home, a shell and an `authorized_keys`; run the **whole** orchestration as 994 | **narrowest.** No sudo, no setuid, no root path. One unprivileged account with `r-x` on the store | **RECOMMENDED** |
| 2. NOPASSWD sudoers for one wrapper | `odbadmin ALL=(woa23c1ro) NOPASSWD: /exact/wrapper` | wider — `odbadmin` gains the ability to run as 994. Still no root. Needs sudoers editing and its own review. **Also unusable from this session, which blocks `sudo` outright** | not recommended |
| 3. root-owned setuid wrapper | a compiled/owned wrapper that drops to 994 | widest; new root-owned executable to audit forever | rejected |

**Option 1 is the PI's stated preference and is also the narrowest.** It gives the writable
`HOME`/`TMPDIR`/workdir for free — 994 owns its own home — and makes the UID question
trivially true rather than something to verify per-process: **if the session is 994, every
child is 994.**

### 3.3 What Option 1 needs host-side

**All of it is host administration and none of it is performed by this campaign:**

1. a home directory owned by `woa23c1ro`, e.g. `/home/woa23c1ro` or `/srv/woa23c1ro`,
   mode `700`, **outside** `/home/odbadmin` and **outside** the production store;
2. a usable login shell (currently `/usr/sbin/nologin`);
3. `~/.ssh/authorized_keys` for the campaign's existing key, so `ssh vm24-c1ro` works
   under `BatchMode=yes`;
4. **nothing else.** No sudo rule, no ACL change, no `chmod`, no group membership, no
   change to production, the runtime, or the store's existing `user:woa23c1ro:r-x`.

**Security boundary, stated exactly:** one unprivileged account that can read the
production store, read the package clone, execute the shared interpreter, and write only
inside its own home. **It cannot escalate, cannot write the store, cannot write the arm's
load path, and is in no shared group.**

### 3.4 If Option 1 is refused

Then C1 runs under Option 2 with a **single, exact-path** sudoers rule — and even then
**this session cannot drive it**, because `sudo` is blocked here regardless of host
configuration. That would mean the PI runs the arms. The design is presentable on request;
it is not developed further here because Option 1 is both narrower and preferred.

## 4. The offline code change Option 1 forces — and it is not small

**`run_controlled.sh` derives production's location from `$HOME`:**

```sh
PROD_DIR=$HOME/python/woa23
PROD_PY=$HOME/.pyenv/versions/py311/bin/python3.11
STORE=$PROD_DIR/data
```

Under `HOME=/home/woa23c1ro` those become `/home/woa23c1ro/python/woa23/data` and a
non-existent interpreter. **The runner would not find the store, the interpreter or
production's tree, and would fail at its own preflight** — for a reason that looks nothing
like the one it actually had.

**Required change:** `--prod-dir`, `--store` and `--python-binary` become explicit, with
today's `$HOME`-derived values kept as defaults so every existing invocation is unaffected.
`--python-binary` already exists; the other two do not.

**Also required, and not optional:**

- **`/proc`-tree UID verification.** Even under Option 1, the run must *prove* what it
  claims: every arm master and **every worker** read from `/proc/<pid>/status` and asserted
  `Uid: 994 994 994 994`. Asserting the launching shell's UID is not the same claim, and
  it is the claim that would quietly pass if a future change reintroduced a privilege path.
- **A writable-path assertion.** The workdir, `HOME` and `TMPDIR` must be writable by 994
  and must **not** be inside, or resolve into, the production store.
- **A store-writability refusal.** If any path under the store is writable by the running
  UID, the run refuses — the `c1h` condition, checked by `stat`/`access`, never by writing.

## 5. What this design does NOT do

- **No VM24 mutation of any kind in this turn.** Everything above is read-only inspection
  plus offline reasoning.
- **No sudoers, ACL, `chmod`, `chown`, account or bind-mount change**, proposed as
  self-service or otherwise.
- **No modification of the production runtime**, including the `.lock` file.
- **No C1 or C2 execution**, and no reuse of `c1h`.
- **No claim of runtime isolation.** §2 is the honest statement instead.

## 6. What happens next, in order

1. **PI decides Option 1 vs Option 2**, and whether the `.lock` residual is accepted.
2. **If Option 1: the host-side items in §3.3 are performed by whoever administers `odb24`
   — not by this campaign.**
3. **Offline:** implement `--prod-dir`/`--store`, the `/proc`-tree UID assertions, the
   writable-path assertions and the store-writability refusal, **with tests** (§3 of the
   PI's list), then three strictly serial batches.
4. **New subject**, fresh archive/file-list digests, **new execution identity and
   first-use ports** — `c1i`'s `18321`/`18322`/`18969` are retired unrun if the runner
   changes, which it will.
5. **New C1 request**, submitted for explicit authorisation. **No VM24 contact before
   that.**

**`pm2G` remains untouched throughout** — 18265 bound, pid 1456369 and its workers running,
all state retained, still **NOT A PASS**.
