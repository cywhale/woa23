# PR review material — the code branch, the evidence branch, and what each one claims

**`f66ddd8` is NOT amended, rewritten or moved.** It stays on `perf/2026-s1-remove-dask`.
The code branch **copies its tree** onto `origin/main`; nothing rewrites the artifact that
production was built from.

**Nothing has been pushed. No GitHub PR exists.** Both branches below are local only.

---

## 0. A correction to the previous revision of this document

The previous revision proposed:

> *"PR 1 — code. Ends at **`f66ddd8`**, `dev2026/**` only."*

**That is withdrawn. It is not achievable as a commit range, and it is no longer accurate as a
description of the content either.** Two separate reasons:

| | |
|---|---|
| **the range is not `dev2026`-only** | `origin/main..f66ddd8` is **368 commits** containing **75 `runs/` files**, plus `conf/ecosystem.config.js` and `README.md`. Cutting the history at `f66ddd8` carries the campaign evidence along with it. Of those commits, 327 are `dev2026`-only, 38 are `runs`-only, **2 are mixed** and 1 is neither — so no clean commit boundary exists |
| **the branch does not end at `f66ddd8`** | the deployed tree diverged from `f66ddd8` **at the cutover**, in one file. §3 carries that divergence as an explicit second commit. **The code branch is `f66ddd8` plus one reconciliation commit, and this document does not describe it as pure `f66ddd8`** |

The branch is therefore built **by content**, not by history.

## 1. Target base

| | value |
|---|---|
| **base branch** | **`origin/main`** — the repo default (`ls-remote --symref origin HEAD` → `refs/heads/main`) |
| **base sha** | `35624922d58aa062ae08a0afbab063692bb2d738` |
| rebase needed | **no** — `merge-base(origin/main, f66ddd8)` **equals** `origin/main` |

**Do not use the local `main` ref**: it is `cedb5e4`, **30 commits behind** `origin/main`.

## 2. The code branch

**`review/woa23-dev2026-f66ddd8`** — verified free locally and on `origin` before creation.

| | commit | |
|---|---|---|
| **1** | `7c7ed1f0c407e7585f8c661160eedce1a6c567f0` | `dev2026`: the WOA23 API rewrite as deployed (artifact `f66ddd8`) |
| **2** | `06bfdcc89e0e08b66cbf95e478c4b5706158958a` | `deploy`: reconcile the production PM2 config with what VM24 actually runs |

```
range: 35624922d58aa062ae08a0afbab063692bb2d738..06bfdcc89e0e08b66cbf95e478c4b5706158958a
```

**Scope: 231 files, +70 311 / −312.** `runs/` files in the branch tree: **0**. `scratchpad/`:
**0** (it is untracked and is not in `.gitignore` — adding it there would be prudent).

| area | files | |
|---|---|---|
| `dev2026/specs` | 99 | |
| `dev2026/scripts` | 57 | |
| `dev2026/bench` | 57 | |
| `dev2026/deploy` | 12 | |
| `dev2026/api` | 4 | the application |
| `conf/ecosystem.config.js` | 1 | −1 line: removes the `kill -9` `pre_stop` |
| `README.md` | 1 | +13: the 1.1.0 row-order contract |

**Commit 1 identity, verified rather than asserted:**

```
git rev-parse f66ddd8:dev2026  = 30ea7e0e89b3e36c5d39ca4861bc6c735093e59b
git rev-parse 7c7ed1f:dev2026  = 30ea7e0e89b3e36c5d39ca4861bc6c735093e59b   MATCH
git diff --name-only f66ddd8 7c7ed1f -- dev2026 conf README.md  ->  0 files
```

Re-running the deploy driver's own `tree_filelist` over commit 1's 265 files reproduces
**`ca461166312a61a3d338edf6211fbf6604facac3009a2c546ff54bee6e8ea169`** — the digest recorded
on VM24 after extraction.

**`conf/ecosystem.config.js`** at commit 1 is `sha256 ed5dec6ca064bd54cfe4a45f33fd416871859e959671164292b21549b96f2159`,
384 bytes — **byte-identical to the copy on VM24**, so it reconciles the repository to the host
rather than proposing a change to it. The host copy carries **no** `pre_stop`.

## 3. Commit 2 — the real divergence, carried openly

A file-by-file comparison of the **live** tree at `/home/odbadmin/python/woa23-f66ddd8/dev2026`
against `f66ddd8` found **264 of 265 files identical, 0 missing, 0 extra** (only `tmp/*.log`
runtime artifacts, correctly outside the manifest). **One file differs**, because it was edited
in place on VM24 during the cutover:

`dev2026/deploy/ecosystem.production.config.js` — repo `a7e4cb7f…` (6 472 B) vs live
`52343e7e…` (6 427 B).

**Executable changes in commit 2:**

| variable | change |
|---|---|
| `WOA23_TLS` | **added**, `'off'` — nginx terminates public TLS (A-move) |
| `WOA23_TLS_KEYFILE` | **removed** |
| `WOA23_TLS_CERTFILE` | **removed** |
| `WOA23_PYTHON` | **added**, `/home/odbadmin/python/woa23-f66ddd8/.venv/bin/python3.11` |
| `WOA23_ZARR_STORE` | **unchanged** — `/home/odbadmin/python/woa23/data` |
| `WOA23_PORT`, `WOA23_WORKERS` | unchanged |

Everything else is untouched: `name`, `cwd`, `script`, `args`, the three log paths,
`autorestart`, `max_memory_restart`, `watch`, `merge_logs`, `log_date_format`,
`append_env_to_name`, `kill_timeout`. **No secret, no PM2 dump, no sensitive environment
value** is introduced — the `env` block holds five keys: two paths, a port, a worker count and
`'off'`.

**`conf/ecosystem.config.js` — the pre-cutover config and the rollback artifact — is NOT
modified by commit 2.**

Comments are corrected in the same commit because they had become false: the header said
*"PROPOSED … NOT INSTALLED, NOT IN USE"* (in use since the cutover); a block described a TLS
path as *"A PLACEHOLDER … not confirmed today"* directly above the lines being removed; and the
`pre_stop` discussion used the present tense for something since removed from the host copy too.

### 3.1 Focused verification performed on commit 2

| check | result |
|---|---|
| `node --check` | **OK** (node v24.13.0) |
| loads as CommonJS, app count | **1 app**, `name: 'woa23'` |
| focused assertions (TLS / interpreter / store / shape) | **`ASSERTIONS=12 FAILED=0`** |
| **safe-field diff vs the live VM24 copy** | **14 fields compared, 0 differ**; `cwd` is textually `__dirname + '/..'` in both |
| diff restricted to non-comment, non-blank lines | **IDENTICAL** — every remaining byte difference is comment text (91 → 112 comment lines, 6 427 → 7 918 bytes) |
| secret-shaped content | **none** (`password`/`secret`/`token`/`api_key`/`BEGIN … PRIVATE KEY`) |

The 12 assertions: `WOA23_TLS === 'off'`; `WOA23_TLS_KEYFILE` absent; `WOA23_TLS_CERTFILE`
absent; no `privkey`/`fullchain`/`.pem` value anywhere in `env`; `WOA23_PYTHON` is the
`f66ddd8` venv; `WOA23_PYTHON` is absolute; `WOA23_ZARR_STORE` preserved; `WOA23_PORT === '8050'`;
`WOA23_WORKERS === '2'`; `env` has exactly 5 keys; no `pre_stop`; `append_env_to_name === false`.

### 3.2 New tree identity

| | file-list sha256 | entries |
|---|---|---|
| `f66ddd8` / commit 1 | `ca461166312a61a3d338edf6211fbf6604facac3009a2c546ff54bee6e8ea169` | 265 |
| **commit 2** | **`ffa1804996ce05d3aff9e87dc9ada42b7219d6ccf99a04bcdfc3c76d83bd47fc`** | 265 |

**Exactly one entry changed** — `deploy/ecosystem.production.config.js`, `a7e4cb7f…` →
`cdec67af6aec1d39dd04ebeef4f71e7ce4c4329196e6669dd6b5a2bac8a98d67`.

### 3.3 Commit 2 is NOT installed on VM24

Because the comments are corrected, the committed file is **not byte-identical** to the host
copy (`52343e7e…`). The **executable content is identical**, proven above. Copying it to VM24
would be a production change and needs its own authorisation; **it has not been done, and
nothing on VM24 was modified.**

## 4. Evidence scope — which evidence covers which commit

**This is the distinction the reader must not blur.**

| | **commit 1 — `f66ddd8`** | **commit 2 — config reconciliation** |
|---|---|---|
| identity | file-list `ca461166…` | file-list `ffa18049…` |
| **batch validation / sentinel evidence** | **YES** — three clean batches, produced against `ca461166…` | **NO. It does not cover this commit, and no claim is made that it does** |
| **production cutover evidence** | the extraction and start of `f66ddd8` | the **live env**: `WOA23_TLS=off`, TLS vars absent, `WOA23_PYTHON` = the `f66ddd8` venv, `WOA23_ZARR_STORE` unchanged — observed on the running service and in the saved PM2 dump |
| what runs today | this tree | **PM2 reads this file at `pm2 start`, not during service. The running process is unaffected by it either way** |

**The f66ddd8 sentinel does not extend to commit 2.** What supports commit 2 is (a) the
focused verification in §3.1, and (b) the post-cutover observation that the live PM2 env carries
exactly these values. Those are different kinds of evidence and are labelled as such.

## 5. The evidence branch

**`review/woa23-d4-evidence`**, based on the code branch. **97 files, +29 578** — `runs/d4` 50,
`runs/d3` 47. Nothing in it is deployed or executed, and merging it changes nothing that runs.

```
content range: 35624922d58aa062ae08a0afbab063692bb2d738..842a3ca  -- runs
```

**Why the two are separate.** The evidence describes the code **by digest**. Rewriting the code
commit would change `f66ddd8`, and every digest in `runs/` — archive, file-list, sentinel —
would then name a subject that no longer exists, while `$APP_ROOT` on VM24 still holds the
original. **Amending is the one action that would break the chain this campaign spent its
effort building** — which is precisely why the divergence in §3 is a new commit rather than an
amendment.

## 6. What the reviewer should know is NOT in the code PR

| | |
|---|---|
| production performance evidence | **none exists, and none is claimed** |
| old-version performance comparison | **not run, and not runnable** — the pre-cutover app is stopped; benchmarking it would require a rollback |
| C1/C2 | **not re-run** |
| `test_s2perf_driver.sh` | a **harness/regression** suite — **not** production performance evidence; **not re-run** |
| reboot recovery | **untested** |
| AVX2 masking | **accepted, unresolved** residual risk under B6 (spec 012) — never a CPU-safety PASS |
| S2 | public TLS key readable by `odbadmin` — accepted, **unresolved**; commit 2 removes the variables but **does not resolve the finding** |
| store content integrity | **unproven**; the recorded fingerprint is metadata-only |
| internal hop | nginx → app is **plaintext** on loopback, by design under A-move |
