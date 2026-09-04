# C1 `c1r` — result: **PASS**

- **Identity:** `c1r` — CONSUMED. Never to be reused.
- **Subject:** `13d6b74d1372a834e534bc91e91d027ab758a90d`
- **Archive:** `a4650395569abdd9f9038cbef3a429c58e92772fa18f8e3aadd02a494077e7b9`, 193 files
- **File-list:** `301668db5c22b2cb3292f3147fdfff2e05ebd34724fcf9b5d98d78b7993404ca`
- **Ran:** 2026-08-26 08:04–08:08 UTC as `woa23c1ro` (uid 994), direct SSH
- **Ports:** 19111 / 19112 / 19114 — BOUND → **SPENT**
- **Harness exit: 0. Gate: `PASS`.**

**This is a real PASS, not an assumed one.** The difference from `c1q` is that the
spec 015 rule was **applied**: all four column-order cases came back
`conformance_verified: true, conformant: true` with `setup_error: null`. c1q could not
apply the rule at all.

---

## 0. The recorded result

**C1 `c1r` is a valid C1 contract-validation result: PASS.**

- **59** byte-identical cases
- **4** spec-015 expected column-order differences, **both reconstruction and
  conformance proven**
- **1** expected API/OpenAPI 1.1.0 documentation difference
- **0** unexpected regressions
- **64/64** cases accounted for

`59 + 4 + 1 = 64`. No case is unclassified and no case is counted twice.

### 0.1 The row-order limitation, explicit

**No real raw row-order difference occurred during `c1r`.** The row-order reconstruction
path was therefore **never exercised**, and `c1r` is **not evidence that row-order
reconstruction works**. The harness reports this as `N/A — NOT EXERCISED`, which is a
non-finding, not a success. This limitation is stated here so the PASS cannot be read as
covering it.

### 0.2 What this PASS may NOT be extended to

This result is confined to C1 contract validation. It carries **no** claim about:

- **latency** — this invocation produced no timing of any kind
- **throughput**
- **deployment**
- **PM2**
- **production-runtime behaviour** — including behaviour at production's worker count

Nothing from `c1r` may be quoted in support of any of the above.

### 0.3 Scope boundaries preserved

- **`c1q` is not modified and not back-filled.** It remains `INCOMPLETE_VALIDATION`.
- `c1r`'s production, store, UID and cleanup evidence is retained intact (§3, §6, §9).
- **C2 is the next separate validation.** It requires its own request, a **new execution
  identity** and **first-use ports**, and must not be executed until that request is
  reviewed and explicitly authorised. No C2 work has been done.

---

## 1. Fresh production PID evidence

Discovered in this session at 08:04:33 UTC. Nothing copied from any document.
Listener presence from `ss -ltn` **only**.

```
LISTEN 0 2048 127.0.0.1:8050 0.0.0.0:*      8050 listening: yes
```

| pid | starttime | ppid | comm | cmdline |
|---|---|---|---|---|
| 4296 | 14214 | 4295 | `(gunicorn)` | `…/py311/bin/python3.11 …/py311/bin/gunicorn woa23_app…` |
| 5040 | 15825 | 4296 | `(gunicorn)` | same |
| 5041 | 15829 | 4296 | `(gunicorn)` | same |

**Unambiguous.** Discriminator: `woa23_app:app` bound to `127.0.0.1:8050`. Twelve other
gunicorns were present and **every one excluded on recorded evidence** — `mhw_app:app`
×3, `tide_app:app` ×3, `api.app:app` ×6 (three of them pm2G's retained
1456369/1456373/1456374).

**No PID reuse:** starttimes 14214 / 15825 / 15829 are identical to c1q's and the boot id
`0b513a75-…-1c7cbc51a085` is unchanged, so these are the same processes, not recycled
numbers.

**`exe_not_readable`** — `/proc/<pid>/exe` is owner/root-only. Recorded as a limitation
for all three pids and **never** reported as exe-verified.

---

## 2. Pre-flight — every condition met, before any arm started

| Check | Result |
|---|---|
| Identity | `uid=994(woa23c1ro) gid=993`, `HOME=/home/woa23c1ro` |
| Host | `odb24` |
| `uv` | `/home/woa23c1ro/.local/bin/uv`, 0.9.22, sha256 `1f95b3af…a0036`, mode 755 |
| Archive | `a4650395…e7b9` — **matches authorization** |
| File count | **193** — matches |
| File-list | `301668db…04ca` — **matches** |
| Per-file hashes | **18/18 exact**, including `bench/column_contract.py` `1eff2dc9…c4a4` |
| Staging/workdir/TMPDIR absent | `woa23-c1r`, `woa23-c1r-work`, `tmp-c1r` |
| Ports unbound | 19111, 19112, 19114 |
| Ports absent from the **subject's own** ledger | all three |
| Clone manifest | `f3b66c49…71f4` — matches |
| Production baseline | boot id + 5 pids + listeners recorded |
| pm2G baseline | 18265 bound, all three pids running |

**Store read-only pre-flight**, identity captured *first*, `stat`/`test -w` only:

```
owner odbadmin:odbadmin (1000:1000) mode 775, my uid 994 -> not the owner
kernel access check (test -w) : no
dirs not traversable / not readable : 0 / 0
files not readable                  : 0
paths WRITABLE                      : 0
symlinks / escaping symlinks        : 0 / 0
123005 files, 35101630061 bytes
```

Enforceable read-only confirmed across the whole tree. Nothing aborted.

---

## 3. UID evidence and request budget

```
dask_scheduler : 1 process(es), all uid 994 (real, effective, saved, fs)
dask_worker    : 1 process(es), all uid 994 (real, effective, saved, fs)
reference      : 2 process(es), all uid 994 (real, effective, saved, fs)
candidate      : 2 process(es), all uid 994 (real, effective, saved, fs)
6 OS processes, matching the authorised set: [1617793 1617849 1617909 1617911 1617973 1617975]
```

All four uids compared on every tracked process; the full set is enumerated rather than
counted. **Requests to production `127.0.0.1:8050`: 0.**

---

## 4. Gate results

```
gate                         : PASS
n_cases                      : 64
canonical values/columns     : 63/64
candidate row-order contract : 44/44 applicable (20 carry no row order)
byte-identical, as required  : 59
regressions                  : []
unreconstructed              : []
order_failures               : []
```

### 4.1 Class 1 — spec 015 parameter-major column order

`expected_column_order_diffs: ['C1', 'C1-csv', 'C16', 'C16-csv']`
`column_reconstruction: RECONSTRUCTED`

**Both halves proven, separately, for every case:**

| case | reconstructed | conformance verified | conformant | setup_error |
|---|:--:|:--:|:--:|---|
| C1 | **true** | **true** | **true** | null |
| C16 | **true** | **true** | **true** | null |
| C1-csv | **true** | **true** | **true** | null |
| C16-csv | **true** | **true** | **true** | null |

The expected orders the rule computed and the candidate matched:

```
C1  / C1-csv  : lon, lat, depth, time_period, temperature_an, temperature,
                temperature_dd, temperature_sd, temperature_se, temperature_oa,
                temperature_gp, temperature_sdo, temperature_sea
C16 / C16-csv : lon, lat, depth, time_period, temperature_an, temperature,
                salinity_an, salinity
```

Both show the decided contract holding: **parameter-major**, index columns leading in
their fixed order, and **`mn` renamed to the bare parameter name while keeping mn's
slot** — `temperature_an` before `temperature`, because `an` precedes `mn` in
`available_vars`. C16's request named `salinity` and `mn` *first*; the output does not
follow the request's order, which is spec 015 §3.

### 4.2 Class 2 — spec 008 API/OpenAPI 1.1.0

`expected_documentation_diffs: ['C20a']`

```
GET /api/swagger/woa23/openapi.json   200/200
classification : EXPECTED_DOCUMENTATION_CHANGE
docs_only      : true
1.0.0 -> 1.1.0 : identical once version, summary and description are removed:
                 no route, parameter, schema or response moved
```

**C20a appears in the documentation class and nowhere else.** It is *not* in
`regressions` — the c1q double-count is gone. It is the one case counted against
`canonical_match` (63/64), correctly, because it is a non-row payload whose bytes differ.

### 4.3 Class 3 — unexpected differences

**None. `regressions: []`.** Every one of the 64 cases is accounted for: 59
byte-identical, 4 class 1, 1 class 2.

Neither expected class was used as a blanket exemption — class 1 required reconstruction
**and** conformance; class 2 required structural identity after stripping.

### 4.4 What was NOT exercised

`expected_diffs: 0` and `column_reconstruction`'s sibling field report the **row**-order
path as **`N/A — NOT EXERCISED`**: no raw row-order difference occurred, so this run is
**no evidence that the row-order reconstruction path works**. That is reported as a
non-finding, not as a success.

---

## 5. Evidence retention

`retained_bodies` present for all five non-byte-identical cases — C1, C16, C1-csv,
C16-csv and C20a — carrying the **exact reference and candidate bodies**, their lengths,
their sha256 digests and the exact classification. C20a's structural claim can now be
**re-audited from the artefact**, not merely re-derived from a digest, which is what c1q
could not offer.

---

## 6. Cleanup and post-run state

```
candidate      stopped; whole tree exited, port 19111 confirmed free
reference      stopped; whole tree exited, port 19112 confirmed free
dask_worker    stopped; whole tree exited, port n/a
dask_scheduler stopped; whole tree exited, port 19114 confirmed free
production on 8050 unchanged (master 4296, listeners [4296 5040 5041], boot id matches)
```

Independently verified at 08:08 UTC:

- **Arms:** none. `grep` exit 1 with its own command line excluded; no arm process under
  `woa23c1ro`.
- **Ports:** 19111, 19112, 19114 all free.
- **Production:** 4296 / 5040 / 5041 ALIVE, starttimes **unchanged**, `127.0.0.1:8050`
  LISTEN, boot id unchanged.
- **pm2G:** 18265 bound; 1456369, 1456373, 1456374 running. **Untouched.**
- **Evidence retained:** 507 MB tree and all 12 `c1r_*.json` artefacts preserved.

**Store identity, before and after — identical on all four measures:**

| | pre-flight | post-run |
|---|---|---|
| dir mtime | 1787622836 | 1787622836 |
| files | 123005 | 123005 |
| bytes | 35101630061 | 35101630061 |
| metadata fingerprint | `1c89be47…b208f` | `1c89be47…b208f` |

The production store, ACLs, runtime, `.lock` file and permissions were not modified.

---

## 7. What this PASS means, and what it does not

**It means:** the candidate's responses are canonically correct (63/64, the 64th being
the decided documentation change); the candidate row-order contract holds on all 44
applicable cases; the candidate's column order **conforms to the spec 015 rule, checked
rather than assumed**; and every byte difference is one of the two decided classes,
each proven on its own narrow ground, with no unexpected difference anywhere.

**It does not mean anything about performance.** This invocation produced **no timing of
any kind** and nothing may be quoted from it as latency or throughput. **C2 remains
blocked.**

It does not retroactively convert `c1q`. **`c1q` stays `INCOMPLETE_VALIDATION`.**

Recorded limitations, unchanged: `-S` means `site.py` did not run, so no `.pth` in the
package clone was processed — this is isolated package-tree import correctness, and the
launcher is `run_controlled.sh` rather than production's PM2 path. `/proc/<pid>/exe` was
unreadable and is recorded as such. The row-order reconstruction path was not exercised.

---

## 8. Standing limits observed

- Exactly **one** c1r execution. No self-rerun.
- `c1q` not rerun; no prior identity reused. `c1r` CONSUMED; ports BOUND → SPENT.
- `api/query.py` unchanged at `50907dee…2ca8`.
- No C2, latency or deployment run.
- pm2G, 18265, its PM2 entry and retained state untouched.
- No sudo, sshpass, privilege escalation or odbadmin launcher.
- Production store not modified; production requests 0.
- All evidence preserved.

## 9. Evidence

| File | Contents |
|---|---|
| `scratchpad/c1r/01-pid-discovery.txt` | fresh PID discovery, full exclusion list, `/proc` identity |
| `scratchpad/c1r/02-preflight.txt` | every pre-flight check, store read-only scan |
| `scratchpad/c1r/03-run.txt` | full run, UID evidence, request budget, gate |
| `scratchpad/c1r/04-classifications.txt` | per-case classifications and conformance |
| `scratchpad/c1r/05-poststate.txt` | post-run state, store comparison |
| `results/c1r_*.json` (VM24, retained) | 12 artefacts including `c1r_contract.json` |
