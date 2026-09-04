# C1 `c1k` — result: `INCOMPLETE_VALIDATION` (harness defect, aborted before any arm)

**Classification: `INCOMPLETE_VALIDATION`. NOT a PASS. NOT a candidate failure. NOT
quotable as any C1 result.**

The full pre-flight passed. The runner then aborted in *"preparing the shared
environment"*, **before any arm, port or workdir existed**, on a `$HOME`-derived path I
failed to make explicit in spec 017.

Executed 2026-08-25 under the C1 `c1k` authorisation of the same date.

---

## 1. What this is

| | |
|---|---|
| **classification** | **`INCOMPLETE_VALIDATION`** |
| **quotable?** | **NO** — not as PASS, not as FAIL, not as evidence about the candidate |
| **a candidate failure?** | **NO.** The candidate was never exercised |
| **cause** | **a defect in my own harness change**, not in the candidate, the account, the ACL or the host |
| **any arm started?** | **NO** |
| **any port bound?** | **NO** — 18361, 18362, 18989 all unbound throughout |
| **workdir / TMPDIR created?** | **NO** — both absent |
| **staging created?** | **yes** — `/home/woa23c1ro/woa23-c1k/`, 178 files |
| **production API requests** | **ZERO** |
| **production store modified?** | **NO** — verified identical, §5 |
| **pm2G** | **untouched** |

## 2. The pre-flight — every condition passed

**Step 1 — connection identity.** `uid=994(woa23c1ro) gid=993(woa23c1ro)
groups=993(woa23c1ro)`, `whoami=woa23c1ro`, `HOME=/home/woa23c1ro`. Not `odbadmin`. **No
privilege escalation was used at any point.**

**Step 2 — store identity, captured BEFORE any decision:**

| | |
|---|---|
| resolved path | `/home/odbadmin/python/woa23/data` (no indirection) |
| mode / owner | `775` / `odbadmin:odbadmin` (uid:gid `1000:1000`) |
| directory mtime | `1787622836` (`2026-08-25 09:53:56 +0800`) |
| hard links / size | `5` / `4096` |
| top-level | 3 entries — `025_degree`, `1_degree`, `test` |
| **total files** | **123005** |
| **total bytes** | **35101630061** |
| **metadata fingerprint** | **`1c89be47344074c87c0d45e1de3e7c6eeca769f74924a3a2fa1c7219205b208f`** |

**Step 3 — complete read-only scan as uid 994**, GNU `find` predicates (capability probed
first):

| check | result |
|---|---|
| store uid 1000 vs my uid 994 | **does not own the store** |
| `test -w` on the root | **no** |
| directories NOT traversable | **0** |
| directories NOT readable | **0** |
| files NOT readable | **0** |
| **paths WRITABLE** | **0** |
| symlinks under the store | **0** |
| **escaping symlinks** | **0** |

**Step 4 — subject:** archive `05d36054102be1fcf841a51db88f8d7c6e4241a08a74a1ceb1cf59ccbdb1a041`,
**176 files**, file-list `c819b924c87ba5e8bc850987f78152b113416f626d00e0846ef96ac23e286dee`
— all three exact. **All 17 per-file source hashes: ok.**

**Step 5 — ports:** 18361, 18362, 18989 unbound by `ss`, **and** absent from the ledger
read from the subject's own export.

**Step 6 — identity absence:** staging, workdir and TMPDIR all absent beforehand.

**Step 7 — production baseline:** boot `0b513a75-213b-40bf-8219-1c7cbc51a085`; PIDs
4296/5040/5041/4357/4358 at starttimes 14214/15825/15829/14323/14330; listeners on
8050/8786/8787; `conf/` digests `8db9a6ba…` and `4aaed5b7…`.

**Step 8 — pm2G:** `18265` still bound; 1456369, 1456373, 1456374 all running.

**Clone manifest** `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4`,
exact. **Interpreter** `Python 3.11.4`.

## 3. Where it stopped, and why

```
== preparing the shared environment ==
production interpreter /home/woa23c1ro/.pyenv/versions/py311/bin/python3.11 not found
=== run_controlled.sh exit=1 ===
```

**`run_controlled.sh` uses TWO different interpreter variables, and spec 017 only fixed
one of them.**

| variable | set by | used for |
|---|---|---|
| `PY_BINARY` | **`--python-binary`** — passed correctly | the **arms** |
| **`PROD_PY`** | **`$HOME`-derived, no flag exists** | `[ -x "$PROD_PY" ]` at line 858, and **`uv sync --locked --python "$PROD_PY"`** at line 886, which builds the harness venv |

Under `HOME=/home/woa23c1ro` the second resolves to
`/home/woa23c1ro/.pyenv/versions/py311/bin/python3.11`, which does not exist. The runner
refused — **correctly, and before starting anything**.

### 3.1 This is my defect, and the tests should have caught it

Spec 017 §4 claimed production paths were made explicit. **They were not — `PROD_PY` was
left `$HOME`-derived with no override flag.** I introduced `PROD_PY_DEFAULT` as a named
variable and then never gave it a way to be set.

**Worse: my own test suite had an assertion that would have caught this and I removed it.**
An early version checked `no PROD_PY=$HOME/... remains`; when I reworked the block after
the `test_cli.sh` regression, I replaced it with checks about `PROD_DIR` only and **never
added an equivalent for `PROD_PY`, nor any test that `PROD_PY` is overridable at all**. The
guard that makes `--prod-dir` and `--store` mandatory under `WOA23_EXPECT_UID` does not
mention `PROD_PY`, so a run asserting a foreign UID sails past it.

**The 132-run offline suite could not have caught it**: every offline run has
`HOME=/Users/cywhale`, where the derived path is equally absent, and no offline test runs
the runner far enough to reach line 858.

## 4. Gate results

**None. No gate ran.**

| | |
|---|---|
| canonical values and column sequence | **not run** |
| candidate column-order contract | **not run** |
| row-order contract | **not run** |
| JSON / CSV fields, values, status | **not run** |
| reconstruction rules | **not run** |
| **UID evidence** | **none** — no arm process existed to assert against |
| **request count to the arms** | **0** |
| **request count to production** | **0** |
| **cleanup** | **not applicable** — nothing started. None performed |

## 5. Store identity, before and after — identical

| | pre-flight | post-abort |
|---|---|---|
| directory mtime | `1787622836` | **`1787622836`** |
| total files | `123005` | **`123005`** |
| total bytes | `35101630061` | **`35101630061`** |
| metadata fingerprint | `1c89be47344074c87c0d45e1de3e7c6eeca769f74924a3a2fa1c7219205b208f` | **identical** |

**No write, no `chmod`, no `chown`, no ACL change, no `.lock` change, no permission
change.** Unlike `c1h`, **this run did not perturb the store in any way** — the mtime is
still the value `c1h`'s probe left, untouched since.

**Production baseline, after:** boot id, all five PIDs and starttimes, and the three
listeners — **all identical** to the pre-flight record.

**pm2G, after:** `18265` **still bound**; 1456369, 1456373, 1456374 **still running**. No
`pm2` command, no signal, no port release, no cleanup.

## 6. What the run left

| path | state |
|---|---|
| `/home/woa23c1ro/woa23-c1k/` | **present**, 178 files — the extracted subject plus the pre-flight's own file list. **Retained as failure evidence** |
| `/home/woa23c1ro/woa23-c1k-work/` | **absent** — never created |
| `/home/woa23c1ro/tmp-c1k/` | **absent** — never created |
| ports 18361 / 18362 / 18989 | **unbound**, never bound |
| `/home/woa23c1ro/c1k-archive.tar` | present — the transferred subject archive |

**Nothing cleaned, nothing re-run.**

## 7. The `c1k` identity

**CONSUMED.** The run was authorised, started, and created a staging tree on VM24. Its
label and paths are never reused.

**Ports `18361`, `18362`, `18989` are RETIRED-NEVER-BOUND** — no socket ever listened on
any of them.

## 8. The fix — offline, and it needs one decision

`PROD_PY` must be explicit and mandatory under `WOA23_EXPECT_UID`, exactly as `--prod-dir`
and `--store` now are. Two ways, and **the choice is the PI's**:

| option | |
|---|---|
| **A. add `--prod-python`** | A third mandatory flag under `WOA23_EXPECT_UID`. Keeps the harness interpreter and the arm interpreter separately nameable, which is what they are |
| **B. default `PROD_PY` to `PY_BINARY`** when `--python-binary` is given | Fewer flags, and in every real invocation they are the same file — but it merges two things the runner deliberately keeps apart, and a future run wanting them different could not say so |

**A is the safer reading and matches the existing pattern.** Either way the test suite gains
what it should have had: that `PROD_PY` is overridable, that it is mandatory under
`WOA23_EXPECT_UID`, and that the refusal names it.

**A new subject, new archive/file-list digests, a fresh label and new first-use ports will
be required**, since the runner changes.

## 9. Standing limits, unchanged

**B1–B5 remain open. B7 remains open.** **`pm2G` remains NOT A PASS.** **C2 remains
blocked** and was not run. **`c1f`, `c2g`, `s2pB`, `pm2G`, `c1h`, `c1i` and `c1j` are not
back-filled**, and nothing here confirms any of them.

**The candidate `832e767` is neither validated nor invalidated.** Its contract remains
untested on VM24.

## 10. Evidence

`scratchpad/c1k/01-preflight.txt` (the full pre-flight, all conditions met),
`02-run.txt` (**the abort**), `03-poststop.txt` (store identical, production unchanged,
pm2G untouched, ports unbound).

On VM24, retained: `/home/woa23c1ro/woa23-c1k/` and `/home/woa23c1ro/c1k-archive.tar`.
