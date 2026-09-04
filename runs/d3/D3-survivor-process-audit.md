# D-3 blocker — survivor processes from `test_requests.sh`: **offline audit**

**Offline only. No VM24 contact, no D-3 execution, nothing killed.**

Raised by review after the three clean batches at subject `6ce915e` were found to have left
**15 survivor processes** — five per batch.

## Verdict, stated first

> ### The leak is REAL, its cause is a shell-scope defect in `scripts/test_requests.sh`, and **the D-3 execution path does not use any of it.**
>
> **Route taken: §5 of the review** — evidence that D-3 is unaffected, plus a
> **process inventory** and an **unexpected-survivor fail-closed gate** added to the request.
> **No new subject is cut and the batches are not re-run**, because no file D-3 executes is
> changed.

**The package is NOT described as fully clean**, and **no VM24 execution authorisation is
submitted**. `test_requests.sh` is still defective; fixing it is separate work (§7).

---

## 1. The five processes, one by one

`start_server` is called **exactly five times**, and every survivor maps to one call site.
Mapping is by each process's own environment, read from the live process table — not inferred
from order.

| # | source | call | survivor (batch 1) | `MODE` | `FAIL_TIMES` | port |
|---|---|---|---|---|---|---|
| 1 | `test_requests.sh:121` | `start_server 0 ok` | `61767` | `ok` | `0` | 61712 |
| 2 | `test_requests.sh:133` | `start_server 2 ok` | `61809` | `ok` | `2` | 61716 |
| 3 | `test_requests.sh:144` | `start_server 0 hang` | `61864` | `hang` | `0` | 61722 |
| 4 | `test_requests.sh:165` | `start_server 0 ok` | `62543` | `ok` | `0` | 61734 |
| 5 | `test_requests.sh:176` | `start_server 5 ok` | `62625` | `ok` | `5` | 61740 |

**One-to-one, with no leftover and no unexplained process.** Batches 2 and 3 produced the
same five with the same environments.

### 1.1 Launch command

```bash
FAIL_TIMES="$fail_times" MODE="$mode" PORT="$port" python3 - >/dev/null 2>&1 <<'PY' &
…ThreadingHTTPServer(("127.0.0.1", int(os.environ["PORT"])), H).serve_forever()
```

A `ThreadingHTTPServer` on loopback, **`serve_forever()` — no timeout, no self-exit**. It
runs until something kills it. `python3` here is the **system** Python 3.9, not the project
venv's 3.11.

`>/dev/null 2>&1` is deliberate and correct: the server is started inside
`port="$(start_server …)"`, and a background child inheriting the substitution's stdout would
hold that pipe open so the substitution never returns. **I confirmed that experimentally** —
a reproduction without the redirect hangs exactly as the source comment predicts.

### 1.2 Parent / child and process group

| | |
|---|---|
| parent at creation | the **command-substitution subshell** of `start_server` |
| parent now | **`1` (launchd)** — reparented when the subshell and script exited |
| process group | **all five share pgid `21059`** — the batch's group |
| pgid leader | **dead**; the group outlives its leader |
| session / tty | session `0`, **no controlling terminal** |
| state | `SN` — sleeping, still **LISTEN**ing on their loopback ports |

**A group-directed `kill -- -21059` would have reaped all five in one call.** The suite never
does that; it relies on a PID list.

### 1.3 Expected cleanup, and why it does nothing

```bash
SERVERS=""                                              # line 45, parent
cleanup() { for p in $SERVERS; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT                                       # line 49, parent
…
SERVERS="$SERVERS $!"                                   # line 101, INSIDE start_server
```

**`SERVERS` is written in exactly one place — line 101 — and `start_server` is invoked only
as `port="$(start_server …)"`, at all five call sites.** Command substitution runs in a
**subshell**, so line 101 mutates a copy that is destroyed when the substitution returns.
**In the parent, `SERVERS` is never anything but `""`.**

`cleanup` therefore iterates an empty list, executes `kill` **zero** times, and returns
success.

**Demonstrated, not merely argued:**

```
inside start_server (subshell): SERVERS='[ 73236]'
parent after the call:          SERVERS='[]'
cleanup sees SERVERS='[]'
cleanup killed 0 process(es)
→ pid 73236 survived
```

**And the EXIT trap does not fire inside the substitution** — verified separately on
**bash 3.2.57**, the version these suites target: the trap fires once, in the parent, after
the substitution returns. That is *why the tests still work* — the server must stay alive
while the case runs — and it is also why nothing ever reaps it.

**This is the same defect class already fixed once in this campaign:** `production_stop.sh`'s
`SCAN_UNRESOLVED` had to become a **file** rather than a variable for exactly this reason — a
subshell's write is invisible to the parent's trap.

---

## 2. Why the suite still exits 0 — and it is NOT "just background noise"

**Four independent reasons, none of which is "the processes are harmless".**

| # | reason |
|---|---|
| 1 | **No assertion covers it.** The suite's 67 assertions are about the request *counter* — attempts vs successes under retry, timeout and refusal. **Not one asserts that a server was reaped.** Its exit status reflects its `fail` counter, and that counter was never asked the question |
| 2 | **`cleanup` cannot fail.** Zero loop iterations; and each `kill` is `2>/dev/null \|\| true` regardless. There is no path from "nothing was cleaned up" to a non-zero status |
| 3 | **It is a VACUOUS PASS** — the shape this campaign has already named: *a check whose success is indistinguishable from its input being absent*. An empty `SERVERS` because everything was already reaped, and an empty `SERVERS` because the writes went to a dead subshell, produce **identical** observable behaviour |
| 4 | **`run_suites.sh` sees them but does not judge.** Its process snapshot correctly reported the survivors as a `NOTE: this suite left processes behind`. That is **diagnostic context by design**, deliberately not an assertion — so it informs the log and changes no exit code |

**So the batch was right on both counts simultaneously:** every assertion genuinely passed,
**and** five processes genuinely survived. Those are not in tension — nothing in the system
was ever wired to turn the second into a failure. **That gap is the finding**, and §6 closes
it for D-3 by adding the assertion that does not exist here.

---

## 3. Does the D-3 execution path use this code? **No — and here is the evidence**

Checked over every file D-3 executes, at subject `6ce915e`.

### 3.1 Source evidence

| check, across all 11 `deploy/` files | result |
|---|---|
| reference `test_requests.sh` | **0** |
| source `lib_requests.sh` or `lib_http.sh` | **0** |
| invoke `run_suites.sh` or any `scripts/test_*.sh` | **0** |
| contain **any** background-launch operator (`&` at end of line, `nohup`, `setsid`, `disown`, `Popen`, `subprocess.`) | **0** |

**The last row is the strongest and it is unconditional: no file in `deploy/` launches a
background process at all.** Whatever D-3 runs, it cannot leak this way, because it never
detaches anything.

| check | result |
|---|---|
| `start_server` defined or called anywhere outside `scripts/test_requests.sh` | **0** — all 7 occurrences are in that one file |
| background launches in `lib_requests.sh`, `lib_http.sh`, `run_controlled.sh`, `lib_d1_finalize.sh` | **0, 0, 0, 0** |
| invokers of `test_requests.sh` anywhere in the subject | **only** `run_suites.sh`, via its glob `ls scripts/test_*.sh`. Every other mention is prose in a comment or spec |

**`start_server` is test scaffolding, private to one file.** It exists to give the request
counter a server that answers late, hangs or refuses. D-3 talks to a **real staging service
started by PM2**; it has no use for a fake one.

### 3.2 The honest caveat

**`scripts/test_requests.sh` IS present in the subject archive** and therefore reaches VM24,
because the archive is `git archive 6ce915e dev2026` and that includes `scripts/`.
**Presence is not invocation**, and D-3 never runs `run_suites.sh` — the only thing that
would invoke it. §6's gate is written so that this distinction does not have to be taken on
trust.

### 3.3 What D-3 *does* leave running, by design

PM2's God Daemon, the gunicorn master, and **2** workers. These are **intended, tracked and
retained by decision** (§8.1 of the request) — started by `pm2 start`, stoppable by
`production_stop.sh`, and enumerated in the inventory of §6 below. **They are not survivors
in the sense of this audit**, and the gate must not confuse the two.

---

## 4. Route decision

**Review §4 does not apply** — no file on the D-3 execution path is changed, so **no new
subject is cut and the three batches at `6ce915e` are not re-run.** Re-cutting on the
strength of a defect in a file D-3 never executes would burn `dep3a`/`19161` and buy nothing.

**Review §5 applies**, and §5 and §6 below deliver it.

---

## 5. Evidence preserved, nothing killed

| | |
|---|---|
| the **15** survivors from these batches | **alive, untouched**, PIDs and start times recorded above and in the request's A.1a-1 |
| the **~1065** pre-existing survivors (from **Aug 10** onward) | **alive, untouched** |
| cleanup | **not performed, and requires separate authorisation** |

**One process was killed, and it was mine, not evidence:** pid `73236`, a `sleep` created by
my own reproduction in §1.3 minutes earlier. It is named here so the count of preserved
evidence is unambiguous.

**Port-collision check on this development machine:** nothing listens on **`19161`**, and
**no listener at all** occupies `18000–18999` or `19000–19999`. The 1065 leaked servers hold
**ephemeral** ports only. **This machine is not VM24** — `19161`'s availability there is
established at execution by `ss -ltn`, and this check does not substitute for it.

---

## 6. What is added to the D-3 request

**A process inventory before and after, and a fail-closed gate on unexpected survivors.**

### 6.1 Inventory

Taken **immediately before the run starts** and **again after the ten cases and after stop**,
by the same method both times, recording **PID, PPID, PGID, start time, uid and full argv**
for every process owned by uid 994 — not a count, and not a command-name summary.

### 6.2 The gate

**After-set minus before-set, minus the processes D-3 is expected to have created.**

| expected, and enumerated in advance | |
|---|---|
| PM2 God Daemon for **this run's** `PM2_HOME` | 1 |
| gunicorn master | 1 |
| gunicorn workers | **2** (B.4) |

**Any process in the after-set that is not in the before-set and not on that list is an
UNEXPECTED SURVIVOR.**

| | |
|---|---|
| **on detection** | **FAIL CLOSED** — the run is reported as **INCOMPLETE**, no result is claimed from it |
| **the survivor** | **left running and recorded** — PID, PPID, PGID, start time, argv. **Not killed**, because that would destroy the evidence needed to explain it |
| **zero unexpected survivors** | recorded as a **positive assertion**, not as silence. An absent finding and an unrun check must not look the same — that is the vacuous-pass shape this whole audit is about |

**The gate is deliberately stricter than the defect requires.** §3 argues D-3 cannot leak this
way; the gate means that argument does **not** have to be trusted.

---

## 7. Still open — separate work, not part of D-3

**`scripts/test_requests.sh` remains defective.** Every offline batch run leaks five
processes, and ~1065 have accumulated since Aug 10.

**The fix is known** and is the one already applied to `production_stop.sh`: record the PIDs
where the parent can see them — a **file**, not a variable — since command substitution puts
the writer in a subshell. A **survivor regression test** should accompany it, asserting the
count of listening test servers returns to its pre-suite value.

**Deliberately NOT done here**, because touching `scripts/` changes the subject tree and would
force a new subject, fresh archive and three new batches — for a defect on a path D-3 does not
execute. **It is offered as its own change request whenever the PI wants it.**

---

## 8. Status

| | |
|---|---|
| leak | **REAL, root-caused, reproduced** |
| D-3 execution path | **does not use `test_requests.sh`, its helper, or any background launch** |
| subject | **`6ce915e` unchanged**; batches **not** re-run |
| D-3 package | **NOT fully clean** — this defect is open in the harness |
| VM24 | **no contact. No execution authorisation is submitted.** |

**D-3's four decisions are unchanged:** real production store · TLS off · the fixed ten
cases · daemon, tree and artefacts retained.
