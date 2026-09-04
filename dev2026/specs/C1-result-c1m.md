# C1 `c1m` — result: `INCOMPLETE_VALIDATION` (uv unreachable by uid 994)

**Classification: `INCOMPLETE_VALIDATION`. NOT a PASS. NOT a candidate failure. NOT
quotable as any C1 result.**

The full pre-flight passed, **including everything `c1k` died on**. The runner then aborted
at `uv sync`, because `uv` exists only inside a directory uid 994 cannot traverse.

Executed 2026-08-25 under the C1 `c1m` authorisation of the same date.

---

## 1. What this is

| | |
|---|---|
| **classification** | **`INCOMPLETE_VALIDATION`** |
| **quotable?** | **NO** |
| **a candidate failure?** | **NO.** The candidate was never exercised |
| **host-guard integration** | **COMPLETED and unchanged** — see §3 |
| **the `c1k` defect** | **FIXED and proven on VM24** — see §2 |
| **cause** | a **host access gap**: `uv` is unreachable by uid 994 |
| **any arm started?** | **NO** |
| **any port bound?** | **NO** — 19051, 19052, 19059 unbound throughout |
| **workdir / TMPDIR created?** | **NO** |
| **production API requests** | **ZERO** |
| **production store modified?** | **NO** — identical on all four measures, §5 |
| **pm2G** | **untouched** |

## 2. The pre-flight — everything passed, including the `c1k` fix

**Identity:** `uid=994(woa23c1ro) gid=993 groups=993`, not `odbadmin`, **no privilege
escalation at any point**.

**Explicit production paths, and the resolution `c1k` lacked:**

| | |
|---|---|
| `--prod-dir` | `/home/odbadmin/python/woa23` — exists |
| `--store` | `/home/odbadmin/python/woa23/data` — exists |
| `--prod-python` | `/home/odbadmin/.pyenv/versions/py311/bin/python3.11` — **executable, Python 3.11.4** |
| `--python-binary` | the same file; the flags stay separate |
| **`PROD_SITE` derived** | **`/home/odbadmin/.pyenv/versions/py311/lib/python3.11/site-packages`** — exists, and **under production's home, not the running account's** |
| mandatory-flag check, all three given | **empty** (ok) |
| mandatory-flag check, `--prod-python` missing | **refuses**, naming the flag |
| `uv sync` line in the subject | `985:uv sync --locked --python "$PROD_PY"` |

**The `c1k` defect is fixed and the fix is proven on the host, not just offline.** The run
passed the interpreter existence/version check that stopped `c1k`, and `PROD_SITE` resolved
under `/home/odbadmin/` rather than `/home/woa23c1ro/` — the silent-guard-weakening the
audit caught.

**Store identity, captured BEFORE any decision:**

| | |
|---|---|
| resolved path | `/home/odbadmin/python/woa23/data` |
| mode / owner | `775` / `odbadmin:odbadmin` (`1000:1000`) |
| directory mtime | `1787622836` |
| **total files** | **123005** |
| **total bytes** | **35101630061** |
| **metadata fingerprint** | **`1c89be47344074c87c0d45e1de3e7c6eeca769f74924a3a2fa1c7219205b208f`** |

**Complete read-only scan as uid 994** (GNU predicates, capability probed first): store uid
1000 vs my 994 — **does not own**; `test -w` **no**; directories not traversable **0**; not
readable **0**; files not readable **0**; **paths writable 0**; symlinks **0**; **escaping
symlinks 0**.

**Subject:** archive `bf237e75a50f8bde3b0b2f8a7ec3f4bad230e2ae32206ad8e6ac10b9cc443943`,
**181 files**, file-list `d48438498ce6a45695d69d9f0acfc5b334536f4ceb1759b518fa382f7bebd936`
— all exact. **All 17 per-file hashes: ok.**

**Ports:** 19051, 19052, 19059 unbound by `ss` **and** absent from the ledger read from the
subject's own export. **Identity absence:** staging, workdir, TMPDIR all absent beforehand.

**Production baseline:** boot `0b513a75-213b-40bf-8219-1c7cbc51a085`; PIDs
4296/5040/5041/4357/4358 at 14214/15825/15829/14323/14330; listeners on 8050/8786/8787;
`conf/` digests recorded. **pm2G:** `18265` bound, 1456369/1456373/1456374 running.

**Clone manifest** `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4`,
exact.

## 3. Host-guard integration — completed, unchanged, fail-closed

**Verified on VM24, from the subject's own copy:**

```
28:EXPECT_HOST=odb24
881:  echo "this runs on $EXPECT_HOST only; hostname is $(hostname -s)" >&2
this host: odb24
```

**The guard is unmodified and no bypass exists or was used.** It passed here because this
genuinely is `odb24`. **No test hook was added to it** — the offline suite still cannot
reach the shared-environment stage, and that limitation stands as recorded rather than
being engineered away.

## 4. Where it stopped

```
== preparing the shared environment ==
== harness bootstrap: dev2026/.venv — NOT the environment under test ==
./scripts/run_controlled.sh: line 985: uv: command not found
=== run_controlled.sh exit=127 ===
```

**`uv` exists. Uid 994 cannot reach it.**

| path | mode | ACL for `woa23c1ro` | |
|---|---|---|---|
| `/home/odbadmin` | `755` | **`--x`** | traverse ok |
| **`/home/odbadmin/.local`** | **`700`** | **none** | **BLOCKED — cannot traverse** |
| `/home/odbadmin/.local/bin` | `775` | none | unreachable behind the above |
| `/home/odbadmin/.local/bin/uv` | `755`, `odbadmin:odbadmin` | — | **exists**, unreachable |

`uv` is on no system path — not `/usr/local/bin`, not `/usr/bin`, and not on `odbadmin`'s
own `PATH` either. The runner's `export PATH="$HOME/.local/bin:$PATH"` resolves, under
`HOME=/home/woa23c1ro`, to a directory that **does not exist** for the run account.

**A correction to my own first reading.** The post-abort probe run *as 994* reported
`/home/odbadmin/.local/bin/uv` **absent**. That was unreachability, not absence — 994
cannot traverse `.local`, so `[ -e ]` is false for a file that is really there. Checked
again from `odbadmin`, it exists. **"Not visible to this account" and "not present" are
different findings and must not be reported as the same one.**

### 4.1 This is the same class as `c1k`, and my audit did not cover it

`c1k` was `PROD_PY`; the audit then caught `PROD_SITE`. **Both were `$HOME`-derived
*paths*. This is a `$HOME`-derived *tool lookup*, and I did not audit `PATH` at all.**

Line 26's `export PATH="$HOME/.local/bin:$PATH"` is not obviously wrong — it adds the
running account's own local bin, which is reasonable. The gap is that **nothing checks the
tools the run needs are actually reachable before it commits to needing them**, and `uv` is
discovered only at the moment it is invoked.

## 5. Store identity, before and after — identical

| | pre-flight | post-abort |
|---|---|---|
| directory mtime | `1787622836` | **`1787622836`** |
| total files | `123005` | **`123005`** |
| total bytes | `35101630061` | **`35101630061`** |
| metadata fingerprint | `1c89be47…` | **identical** |

**No write, `chmod`, `chown`, ACL, `.lock` or permission change.** The mtime is still the
value `c1h`'s probe left, untouched since.

**Production baseline after:** boot id and all five PIDs/starttimes **identical**.
**pm2G after:** `18265` **still bound**; 1456369, 1456373, 1456374 **still running**. No
`pm2` command, no signal, no port release, no cleanup.

## 6. Gate results

**None ran.**

| | |
|---|---|
| canonical values and column sequence | **not run** |
| candidate column-order contract | **not run** |
| row-order contract | **not run** |
| JSON / CSV fields, values, status | **not run** |
| reconstruction rules | **not run** |
| **UID evidence** | **none** — no arm process existed to assert against |
| **requests to the arms** | **0** |
| **requests to production** | **0** |
| **cleanup** | **not applicable** — nothing started; none performed |

## 7. What the run left

| path | state |
|---|---|
| `/home/woa23c1ro/woa23-c1m/` | **present**, 183 files — **retained as failure evidence** |
| `/home/woa23c1ro/woa23-c1m-work/` | **absent** — never created |
| `/home/woa23c1ro/tmp-c1m/` | **absent** — never created |
| ports 19051 / 19052 / 19059 | **unbound**, never bound |
| `/home/woa23c1ro/c1m-archive.tar` | present |

**Nothing cleaned, nothing re-run.** `c1k`'s retained tree is also untouched.

## 8. The `c1m` identity

**CONSUMED.** Authorised, started, created a staging tree. **Ports `19051`, `19052`,
`19059` are RETIRED-NEVER-BOUND** — no socket ever listened.

## 9. What is needed — a decision, and it is not mine

**`uv` must be reachable and executable by uid 994.** Three ways:

| option | |
|---|---|
| **A. give the run account its own `uv`** at `/home/woa23c1ro/.local/bin/uv` | **Recommended.** The runner's existing `PATH` line finds it with **no code change**, touches nothing of `odbadmin`'s, and keeps the run account self-contained |
| B. grant `woa23c1ro` traverse on `/home/odbadmin/.local` and `.local/bin` | Opens a path into `odbadmin`'s home that is currently closed. Narrow, but it widens the account's reach for one tool |
| C. add an explicit `--uv` flag | Consistent with `--prod-python`, but still needs a reachable binary, so it does not remove the provisioning step |

**This campaign will not install software, change an ACL, `chmod`, or alter any account.**
That is host administration.

**Whatever is chosen, the harness should also fail early and by name** — check the tools it
requires during pre-flight rather than discovering `uv` missing at the moment of use. That
is offline work I can do once the provisioning route is decided, and it needs a new subject,
fresh digests, a new label and new first-use ports.

## 10. Standing limits, unchanged

**B1–B5 remain open. B7 remains open.** **`pm2G` remains NOT A PASS.** **C2 remains
blocked** and was not run. **`c1f`, `c2g`, `s2pB`, `pm2G`, `c1h`, `c1i`, `c1j` and `c1k` are
not back-filled.**

**The candidate `77bf4fa` is neither validated nor invalidated.**

## 11. Evidence

`scratchpad/c1m/01-preflight.txt` (full pre-flight, all conditions met),
`02-run.txt` (**the abort**), `03-poststop.txt` (store identical, production unchanged,
pm2G untouched), `04-uv-diag.txt` (**the traversal chain**).

On VM24, retained: `/home/woa23c1ro/woa23-c1m/`, `/home/woa23c1ro/c1m-archive.tar`, and
`c1k`'s tree from before.
