# C1 `c1n` — result: `INCOMPLETE_VALIDATION` (uid 994 cannot attribute production's socket)

**Classification: `INCOMPLETE_VALIDATION`. NOT a PASS. NOT a candidate failure. NOT
quotable as any C1 result.**

The furthest any C1 attempt has reached. The harness venv **built**, clone integrity
**matched**, ports confirmed **first-use** — and the run then stopped because
`run_controlled.sh` could not identify production's process, **not because production was
down**.

Executed 2026-08-25 under the C1 `c1n` authorisation of the same date.

---

## 1. What this is

| | |
|---|---|
| **classification** | **`INCOMPLETE_VALIDATION`** |
| **quotable?** | **NO** |
| **a candidate failure?** | **NO.** The candidate was never exercised |
| **was production down?** | **NO.** It was listening throughout — §4 |
| **cause** | uid 994 **cannot attribute another user's socket to a PID** |
| **uv preflight** | **WORKED** — §2 |
| **harness venv** | **BUILT** — 58 distributions |
| **clone integrity** | **MATCH** — 33,565 entries, 1,690,025,002 bytes |
| **any arm started?** | **NO** |
| **any port bound?** | **NO** — 19071, 19072, 19079 unbound throughout |
| **workdir / TMPDIR created?** | **NO** |
| **production API requests** | **ZERO** |
| **production store modified?** | **NO** — identical on all four measures |
| **pm2G** | **untouched** |

## 2. The pre-flight — everything passed, including the `c1m` fix

**Identity:** `uid=994(woa23c1ro) gid=993 groups=993`, `HOME=/home/woa23c1ro`, not
`odbadmin`, **no privilege escalation**.

**uv, verified before anything was created — the `c1m` fix, working:**

| | |
|---|---|
| resolved | **`/home/woa23c1ro/.local/bin/uv`** |
| executable | **yes** |
| **version** | **`uv 0.9.22`** |
| **SHA-256** | **`1f95b3affb7fd478f068f62b80e374b84bf46764c37e9f35d2a648e5b9aa0036`** |
| owner / mode | `woa23c1ro:woa23c1ro` / `755` |

**A distinction worth recording, because I nearly misreported it again.** Under a *bare*
`ssh host cmd` invocation, `command -v uv` **fails** — sshd's default `PATH` does not
include `~/.local/bin`. The file is present and executable the whole time. That is a
**third** state, distinct from both of `c1m`'s: not absent, not unreachable, **not on
PATH**. The runner's own `export PATH="$HOME/.local/bin:$PATH"` resolves it, and
`require_tool` runs after that line.

**Explicit production paths:** `--prod-dir`, `--store`, `--prod-python` (Python 3.11.4) and
the clone all reachable; clone manifest `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4`
exact.

**Store identity, captured BEFORE any decision:** mtime `1787622836`, **123005 files**,
**35101630061 bytes**, fingerprint
`1c89be47344074c87c0d45e1de3e7c6eeca769f74924a3a2fa1c7219205b208f`.

**Complete read-only scan as uid 994:** does not own the store; `test -w` **no**; not
traversable **0**; not readable **0**; files not readable **0**; **writable 0**; symlinks
**0**; escapes **0**.

**Subject:** archive `07a5cd302feaa83b01bac324e2f58a272413065210b53e5e887e776d3fea90e2`,
**183 files**, file-list `57bc01678fa733b381f722fec085ead95f2300e3b0bc9bd794152f2cf18b91c0`
— exact. **All 17 per-file hashes: ok.**

**Ports:** unbound by `ss` **and** absent from the subject's own ledger.

## 3. How far it got — further than any previous attempt

| stage | result |
|---|---|
| required tools | **passed** — uv resolved, version and hash recorded |
| interpreter existence + 3.11.4 version | **passed** |
| **`uv sync --locked --python "$PROD_PY"`** | **SUCCEEDED** — harness venv, python 3.11.4, **58 distributions**, lock `0d2980a5928d4d09` |
| environment under test recorded | clone python 3.11.4, **236 distributions**; `package_tree_digest b8754d32…`; `runtime_distribution_digest a26ca6c3…` |
| **clone integrity** | **MATCH** — 33,565 entries vs 33,565 files, 1,690,025,002 bytes, 68.09s |
| ports | **19071, 19072, 19079 all first-use** |
| **production identity** | **STOP** |

**`c1m`'s defect is fixed and proven on the host.** `uv` resolved, the venv built, and the
run reached a stage no earlier attempt had.

## 4. Where it stopped — and production was never down

```
production is not listening on 8050
=== run_controlled.sh exit=1 ===
```

**That message is wrong about the world.** Production was listening before, during and
after. The runner cannot *attribute* the socket.

`run_controlled.sh:1353` requires `pids_on_port 8050` to be non-empty, and
`lib_ports.sh:70` implements it as:

```sh
rows="$(ss_rows_on_port "$1")" || return 2
out="$(printf '%s\n' "$rows" | grep -oE 'pid=[0-9]+' | ...)"
```

**It greps `ss` output for `pid=`, which only `ss -p` emits — and `ss -p` shows PIDs only
for sockets the caller owns, or to root.** Demonstrated as uid 994:

```
ss -ltn   :8050  →  LISTEN 0 2048 127.0.0.1:8050 0.0.0.0:*      ← the listener IS visible
ss -ltnp  :8050  →  LISTEN 0 2048 127.0.0.1:8050 0.0.0.0:*      ← identical; no pid=
```

Same line, both ways. **The port is visible; the owner is not.** So `pids_on_port` returns
empty and the caller reads that as "nothing is listening".

**The check itself is sound and its purpose is real.** Its comment says so:

> "Someone is still listening on 8050" is not the same as "production is the process it
> was". A restart between the two checks would leave the port occupied and every comparison
> in this run describing a different backend.

**It is right to want production's identity. It cannot get it as a non-owner**, and this is
the fourth instance of one class: *the harness assumes it runs as the account that owns
what it is looking at.* `c1k` was `PROD_PY`, the audit caught `PROD_SITE`, `c1m` was `uv`
on `PATH`, and this is socket→PID attribution — the first that **cannot be fixed by naming
a path**, because the information is privileged.

## 5. Store identity, before and after — identical

| | pre-flight | post-abort |
|---|---|---|
| directory mtime | `1787622836` | **`1787622836`** |
| total files | `123005` | **`123005`** |
| total bytes | `35101630061` | **`35101630061`** |
| metadata fingerprint | `1c89be47…` | **identical** |

**No write, `chmod`, `chown`, ACL, `.lock` or permission change.**

**Production after:** listeners on 8050/8786/8787 present; PIDs 4296/5040/5041/4357/4358 at
starttimes 14214/15825/15829/14323/14330 — **all identical to the pre-flight**. Production
never restarted and was never touched.

**pm2G after:** `18265` **still bound**; 1456369, 1456373, 1456374 **still running**. No
`pm2` command, no signal, no port release, no cleanup.

## 6. Gate results

**None ran.** The stop is before any arm starts.

| | |
|---|---|
| canonical values and column sequence | **not run** |
| candidate column-order contract | **not run** |
| row-order contract | **not run** |
| JSON / CSV fields, values, status | **not run** |
| reconstruction rules | **not run** |
| **UID evidence** | **none** — no arm process existed. The orchestration itself ran as uid 994 throughout |
| **requests to the arms** | **0** |
| **requests to production** | **0** |
| **cleanup** | **not applicable** — no service started; none performed |

## 7. What the run left

| path | state |
|---|---|
| `/home/woa23c1ro/woa23-c1n/` | **present**, 8806 files — subject **plus the built harness venv**. **Retained as failure evidence** |
| `/home/woa23c1ro/woa23-c1n-work/` | **absent** — never created |
| `/home/woa23c1ro/tmp-c1n/` | **absent** — never created |
| ports 19071 / 19072 / 19079 | **unbound**, never bound |
| `/home/woa23c1ro/c1n-archive.tar` | present |

**Nothing cleaned, nothing re-run.** `c1k`'s and `c1m`'s trees are also untouched.

## 8. The `c1n` identity

**CONSUMED.** **Ports `19071`, `19072`, `19079` are RETIRED-NEVER-BOUND.**

## 9. What is needed — a design decision, and it is the PI's

**Socket→PID attribution requires root or socket ownership. There is no way for uid 994 to
obtain it**, so this cannot be solved by naming another path.

| option | |
|---|---|
| **A. supply production's PIDs explicitly** (e.g. `--prod-pids "4296 5040 5041"`), and verify their `(pid, starttime)` from `/proc/<pid>/stat` — **world-readable, so 994 can read it** | **Recommended.** It *preserves the actual guarantee*: if production restarts mid-run those PIDs die or their starttimes change, and the run detects exactly what the current check exists to detect. It also makes the expectation explicit and reviewable rather than discovered |
| B. accept "the port is listening" without attribution | **Weakens a real check** to silence a symptom. It is precisely the "someone is listening ≠ production is the process it was" case the comment warns against. Not recommended |
| C. have `odbadmin` record production's identity and pass it in | Mixes accounts inside one run and needs a second session; A gets the same evidence without that |
| D. grant 994 a capability to read others' socket info | Host privilege change; disproportionate for this |

**Under A the run still needs the PIDs to be correct at the moment it starts**, so they
would be taken from the pre-flight — which already reads and records them — and re-verified
inside the run.

**This campaign will not change any privilege, capability or account.**

## 10. Standing limits, unchanged

**B1–B5 remain open. B7 remains open.** **`pm2G` remains NOT A PASS.** **C2 remains
blocked** and was not run. **`c1f`, `c2g`, `s2pB`, `pm2G`, `c1h`, `c1i`, `c1j`, `c1k` and
`c1m` are not back-filled.**

**The candidate `7c29585` is neither validated nor invalidated.**

## 11. Evidence

`scratchpad/c1n/01-preflight.txt` (full pre-flight incl. uv evidence),
`02-run.txt` (**venv built, clone MATCH, then the stop**),
`03-poststop.txt` (store identical, production up and unchanged, pm2G untouched, and the
`ss` with/without `-p` demonstration).

On VM24, retained: `/home/woa23c1ro/woa23-c1n/`, `/home/woa23c1ro/c1n-archive.tar`, and the
`c1k` and `c1m` trees from before.
