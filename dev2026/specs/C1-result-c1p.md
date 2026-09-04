# C1 `c1p` — result: `INCOMPLETE_VALIDATION` (gate FAIL, but not a candidate verdict)

**Classification: `INCOMPLETE_VALIDATION`. NOT a PASS. NOT a `5.2A raw byte-exact PASS`.
NOT a candidate failure — and equally NOT a candidate acquittal.**

**The first C1 attempt to reach the contract gate and run it.** Every arm started, every
process was uid 994, production's identity was established without privilege, and 64 cases
were compared. The gate returned **FAIL** on 5 cases — **4 of which are spec 015's decided
column-order change, which the harness has no rule to classify**, and **1 of which
(`C20a`) is unexplained**.

Executed 2026-08-26 under the C1 `c1p` authorisation.

---

## 1. Fresh production PID evidence

**Discovered in this run's own session. Nothing copied from any document.**

The first discovery pass was **ambiguous and I did not proceed on it.** A coarse
`cmdline` filter returned **6** candidates, three of which were `mhw_app:app` on 8030 — my
exclusion list matched the *service name* `mhwapi`, not the *module* `mhw_app`. A second
pass used a precise discriminator:

| pid | starttime | ppid | role |
|---|---|---|---|
| **4296** | `14214` | 4295 (PM2 God) | master |
| **5040** | `15825` | 4296 | worker |
| **5041** | `15829` | 4296 | worker |

**Discriminator: `woa23_app:app` bound to `127.0.0.1:8050`** — production's module *and*
port, both present in the cmdline. Everything else was excluded on evidence, not
assumption: `mhw_app` on 8030, `tide_app` on 8040 (three processes), ghrsst `api.app` on
8035, and **pm2G's `api.app` on 18265**.

**Validated in-run:**

```
== production identity, from the supplied pids (a non-owner cannot read ss -p) ==
  ok  4296 14214  (pid starttime)
  ok  5040 15825  (pid starttime)
  ok  5041 15829  (pid starttime)
production master 4296 (start 14214), listeners: 4296 5040 5041
```

**Re-validated after the run: all three alive, starttimes unchanged.** Production never
restarted.

### 1.1 `exe_not_readable` — the limitation, recorded as required

**`/proc/<pid>/exe` was NOT readable for any production pid.** It is a symlink only the
owner or root may read through, and uid 994 is neither.

**This is recorded as `exe_not_readable` and is NOT reported as exe-verified.** Identity
rests on the checks that *were* possible: `/proc/<pid>/stat` readable, `(pid, starttime)`
captured and re-verified after the run, `cmdline` matching `woa23_app:app` on
`127.0.0.1:8050`, and the listener present by `ss -ltn`.

## 2. Pre-flight — all conditions met

| | |
|---|---|
| identity | `uid=994(woa23c1ro) gid=993`, private group only, not `odbadmin` |
| **uv** | `/home/woa23c1ro/.local/bin/uv`, **0.9.22**, sha256 `1f95b3affb7fd478f068f62b80e374b84bf46764c37e9f35d2a648e5b9aa0036` |
| paths | `--prod-dir`, `--store`, `--prod-python` (3.11.4), clone all reachable |
| clone manifest | `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4` |
| subject | archive `baa445ed…`, **185 files**, file-list `005a2b4f…` — exact; **all 17 hashes ok** |
| ports | 19091/19092/19099 unbound **and** absent from the subject's ledger |
| **store scan as 994** | not traversable **0**, not readable **0**, files not readable **0**, **writable 0**, symlinks **0**, escapes **0** |
| clone integrity | **MATCH** — 33,565 entries, 1,690,025,002 bytes |

## 3. UID evidence — every process, all four uids

```
== process identity: every tracked process must run as uid 994 ==
  dask_scheduler: 1 process(es), all uid 994 (real, effective, saved, fs)
  dask_worker:    1 process(es), all uid 994 (real, effective, saved, fs)
  reference:      2 process(es), all uid 994 (real, effective, saved, fs)
  candidate:      2 process(es), all uid 994 (real, effective, saved, fs)
  6 OS processes, matching the authorised set:
    [1558150 1558206 1558260 1558262 1558331 1558333]
```

**Masters and workers alike.** The orchestration, both arms and every worker ran as 994.

## 4. C1 gate results

| finding | value |
|---|---|
| **gate** | **FAIL** |
| variant | 5.2C |
| cases | **64** |
| canonical values/columns match | **59 / 64** |
| byte-identical | **59** |
| **candidate row-order contract** | **44 / 44 applicable** (20 carry no row order) |
| order failures | **none** |
| expected raw-order differences | **0** |
| reconstruction | **`N/A_NOT_EXERCISED`** — no raw-order difference occurred, so **this run is no evidence that the reconstruction path works** |
| **regressions** | **`C1`, `C1-csv`, `C16`, `C16-csv`, `C20a`** |

**Request counts:** `{"RC": 32, "CR": 32}` — 64 total, both orders, within the ≤192
ceiling. **Production 8050/8786/8787: ZERO requests.**

### 4.1 Four of the five are spec 015's decided change

`C16-csv`:

```
reference : lon, lat, depth, time_period, temperature_an, salinity_an, temperature, salinity
candidate : lon, lat, depth, time_period, temperature_an, temperature, salinity_an, salinity
```

`C1-csv`, more tellingly:

```
reference : …, temperature_an, temperature_oa, temperature_gp, temperature_sd,
              temperature_sdo, temperature_sea, temperature_se, temperature, temperature_dd
candidate : …, temperature_an, temperature, temperature_dd, temperature_sd,
              temperature_se, temperature_oa, temperature_gp, temperature_sdo, temperature_sea
```

**The candidate's order is exactly `available_vars`: `an, mn, dd, ma, sd, se, oa, gp, sdo,
sea`** — with `mn` rendered as the bare `temperature` and holding `mn`'s slot. **That is
spec 015 working precisely as specified.** The reference is unmodified `woa23_app`, still
producing hash-seeded order.

**So the harness is right that the bytes differ and wrong that it is a regression.** Its
message says so itself: *"column order is out of spec 008's scope and must not have
moved"* — a rule written before 015 decided that it should.

**This is my gap, not the candidate's.** The C1/C2 request stated C1 would gain *"a fourth
finding — the column-order difference recorded as a decided change, with byte-level
reconstruction"*. **I never implemented it in `bench/contract_diff.py`.** The request
promised a capability the code does not have.

### 4.2 The fifth, `C20a`, is NOT column order and is unexplained

```
C20a  DIFFER 200/200  8597/9625  <-- non-row payload differs; compared as raw bytes
```

`C20a` is the **documentation surface** (the OpenAPI document). The payloads differ by
~1 KB. **This is not a column-order difference and spec 015 does not explain it.**

**I am not going to guess at it.** It needs investigation against `c1f`'s record — whether
`C20a` differed there too and was classified some other way, or whether something changed.
Until that is understood, **no verdict about the candidate can be drawn from this run.**

## 5. The cleanup warning was a FALSE ALARM — production was never down

```
WARNING: nothing is listening on 8050 any more — production's
  listener disappeared while this run was using the host
  before: [4296 5040 5041]
CLEANUP DID NOT COMPLETE — this run is a failure regardless of its gates
```

**Production was listening throughout.** Verified immediately afterwards: `127.0.0.1:8050`
present, pids 4296/5040/5041 alive at unchanged starttimes 14214/15825/15829, PM2 God 4295
alive, and every other service (8030, 8035, 8040, 18265) up.

**The cause is my incomplete fix.** `run_controlled.sh:1566` still calls:

```sh
after="$(pids_on_port "$PROD_PORT")"
```

I replaced the **pre-start** production check with the `port_is_listening` + supplied-pids
mechanism and **left the post-run comparison on the old ownership-dependent path**. As uid
994 it returns empty, line 1574's `[ -z "$after" ]` fires, and the run declares production
gone.

**Both halves of the same check had to move together and I only moved one.**

## 6. Cleanup — the arms themselves stopped correctly

```
candidate stopped; whole tree exited, port 19091 confirmed free
reference stopped; whole tree exited, port 19092 confirmed free
dask_worker stopped; whole tree exited, port n/a
dask_scheduler stopped; whole tree exited, port 19099 confirmed free
```

**All four services stopped, whole trees exited, all three ports confirmed free** —
re-confirmed afterwards. **No survivor. No SIGKILL. `CLEANUP_FAIL` was declared solely by
the false production assertion in §5**, not by anything left running.

## 7. Store identity, before and after — identical

| | pre-flight | post-run |
|---|---|---|
| directory mtime | `1787622836` | **`1787622836`** |
| total files | `123005` | **`123005`** |
| total bytes | `35101630061` | **`35101630061`** |
| metadata fingerprint | `1c89be47…` | **identical** |

**No write, `chmod`, `chown`, ACL, `.lock` or permission change.** The store was read by
both arms throughout and is byte-for-byte as it was.

**pm2G:** `18265` **still bound**; 1456369, 1456373, 1456374 **still running**. No `pm2`
command, no signal, no cleanup.

## 8. What the run left

`/home/woa23c1ro/woa23-c1p/` — subject, harness venv and **`results/c1p_*.json`**
(contract, environment, clone integrity ×3, interp ×2, meta ×2, ports, shutdown budget).
`/home/woa23c1ro/woa23-c1p-work/`, `tmp-c1p/`, and the archive. **All retained. Nothing
cleaned, nothing re-run.** `c1k`, `c1m` and `c1n` trees also untouched.

**`c1p` is CONSUMED.** Ports `19091`, `19092`, `19099` were **BOUND** by this run and
released — **SPENT**, not retired-never-bound. **This is the first C1 identity whose ports
actually carried a listener.**

## 9. What is needed

**Two defects, both mine, neither in the candidate:**

1. **Implement the column-order finding in `contract_diff.py`** — the decided 015 change
   must be classified as a *decided difference with reconstruction*, exactly as the C1/C2
   request said it would be, instead of counting as a regression. Until then C1 cannot
   return a verdict on this candidate.
2. **Move the post-run production check** to `port_is_listening` + re-validation of the
   supplied pids' `(pid, starttime)`, matching the pre-start check. Both halves must use
   the same mechanism.

**And one open question that is not a defect until understood:**

3. **`C20a`'s ~1 KB documentation-surface difference** must be explained before any C1
   verdict. It is not column order.

## 10. Standing limits

**B1–B5 remain open. B7 remains open. `pm2G` remains NOT A PASS. C2 remains blocked.**
**`c1f`, `c2g`, `s2pB`, `pm2G` and every earlier C1 attempt are not back-filled.**

**The candidate `2d6f812` is neither validated nor invalidated.** The 59/64 canonical
matches and 44/44 row-order conformance are real and encouraging, **but a gate that
returned FAIL is not a pass, and an unexplained case is not a clean one.**

## 11. Evidence

`scratchpad/c1p/01-pid-discovery.txt` (the ambiguous first pass),
`02-pid-discovery-precise.txt` (**the unambiguous set**), `03-preflight.txt`,
`04-run.txt` (**the full run and gate**), `05-production-check.txt` (**production proven
up**), `06-poststop.txt`.

On VM24: `/home/woa23c1ro/woa23-c1p/` with `results/c1p_*.json`, plus the retained `c1k`,
`c1m`, `c1n` trees.
