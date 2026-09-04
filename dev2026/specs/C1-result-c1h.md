# C1 `c1h` — result: `INCOMPLETE_VALIDATION` (blocked at pre-flight)

**Classification: `INCOMPLETE_VALIDATION`. NOT a PASS. NOT a candidate failure.**

The run stopped at the production-store pre-flight, **before any service started**. The
store is **writable by the account C1 runs as**, and the authorisation's own stop condition
is "stop immediately if the store is writable".

Executed 2026-08-25 on VM24 under the C1 `c1h` authorisation of the same date.

---

## 1. What this is

| | |
|---|---|
| **C1 classification** | **`INCOMPLETE_VALIDATION`** — blocked at pre-flight |
| **exit code** | **5** (store pre-flight; `run_controlled.sh` was never invoked) |
| **a candidate failure?** | **NO.** The candidate was never exercised |
| **any arm started?** | **NO** |
| **any port bound?** | **NO** — 18301, 18302, 18949 all still unbound |
| **staging / workdir created?** | **NO** — `~/woa23-c1h/` and `~/woa23-c1h-work/` never created |
| **production API requests** | **ZERO** on 8050 / 8786 / 8787 |
| **pm2G** | **completely untouched** |
| **5.2A raw byte-exact PASS** | **NOT claimed, and not applicable** |

## 2. Where it stopped, and why

The authorisation narrowed "do not touch the production store" to "do not **modify** the
production store", authorised read-only access, and required this before starting services:

> verify the resolved store path is exactly the production store; verify both arms use the
> same read-only store; **perform a harmless write probe and require refusal**; record
> permissions, resolved path and pre-run store identity; **stop immediately if the store is
> writable**, resolves elsewhere, or any write is possible.

**The probe was not refused.**

```
== 2. permissions and ownership ==
  drwxrwxr-x 5 odbadmin odbadmin 4096  4月 18  2025 /home/odbadmin/python/woa23/data
  owner: odbadmin:odbadmin mode=775
  current user: odbadmin

== 3. WRITE PROBE -- refusal is REQUIRED ==
  STORE IS WRITABLE -- removing the probe and STOPPING
  probe removed
  STOP: the production store must not be writable by this run.
```

**The store is owned by `odbadmin`, mode `775`, and C1 runs as `odbadmin`.** The owner has
write permission, so a write cannot be refused by the filesystem. This is a **pre-existing
property of the host**, not something this run created or changed.

**What that means, stated plainly:** for this account, "read-only access to the production
store" **cannot be enforced by permissions**. It can only be a discipline in the code that
reads it. The authorisation asked for a guarantee the filesystem is not in a position to
give.

## 3. The probe changed production metadata — the exact record

**This run DID modify production metadata. It must not be described as a zero-modification
run.** One empty file was created inside the production store and immediately removed, and
that moved the store directory's mtime.

### 3.1 What was measured BEFORE the probe, and what was not

**The pre-flight script exited at step 3. Step 4 — the identity block — never ran.** So the
pre-probe record is exactly what steps 1 and 2 captured, and no more:

| pre-probe, actually measured | value |
|---|---|
| resolved path | `/home/odbadmin/python/woa23/data` (no indirection) |
| `ls -ld` | `drwxrwxr-x 5 odbadmin odbadmin 4096  4月 18  2025` |
| mode / owner | `775` / `odbadmin:odbadmin` |
| **directory mtime** | **`4月 18 2025`** |

| pre-probe, NOT measured | consequence |
|---|---|
| file count | no baseline exists |
| total bytes | no baseline exists |
| file-list / content digest of the store | **never computed, before or after** |
| per-file digests below the anchor | never computed |

**This is a defect in my pre-flight script** — identity was ordered *after* the probe
instead of before it, so the probe destroyed the chance to baseline the very thing it
might have changed. `verify_clean_archive.sh` gets this right by computing its file list
before anything compiles; the store pre-flight did not. **Fixed in the new request (§6).**

### 3.2 Before / after, for the attributes that DO have both

`ls -ld` was captured on both sides, so these are genuine comparisons:

| attribute | before | after | |
|---|---|---|---|
| **directory mtime** | **`4月 18 2025`** | **`8月 25 09:53` (`1787622836`)** | **CHANGED — the probe's own timestamp** |
| mode | `drwxrwxr-x` (775) | `drwxrwxr-x` (775) | unchanged |
| owner : group | `odbadmin:odbadmin` | `odbadmin:odbadmin` | unchanged |
| **hard-link count** | **5** | **5** | unchanged — **no subdirectory added or removed** |
| directory size | `4096` | `4096` | unchanged |

### 3.3 Post-probe state, recorded without a baseline to compare against

| | value | status |
|---|---|---|
| probe artifacts remaining | **0** | **verified** |
| hidden entries at top level | only `.` and `..` | **verified** |
| top-level entries | 3 | post-probe only |
| total files | 123005 | post-probe only |
| apparent size | 35101630061 | post-probe only |
| `1_degree/annual/TS/.zgroup` | `2383746e67b4bcc2762b3f100f06c3fa2d5f149ab5a8e5da5d33521464a01959` | post-probe only |
| `1_degree/annual/TS/.zmetadata` | `517f2946b7ac195aaaa6b6c39bab1e0b6799fff3c048435a69ce80207a959a5a` | post-probe only |

### 3.4 What may and may not be claimed

**May be claimed, from evidence:**

- the probe left **no file** — verified twice, by name-glob and by listing hidden entries;
- **no subdirectory was added or removed** — the hard-link count is 5 on both sides;
- mode, ownership and directory size are **unchanged**;
- **production API, PM2, processes, listeners and `conf/` are entirely unaffected** (§5),
  with **zero HTTP requests** to 8050 / 8786 / 8787.

**May NOT be claimed:**

- **not** "file count and byte total are unchanged" — there is no pre-probe measurement of
  either. That a single empty top-level file was created and removed makes a net change of
  zero *by construction*, but that is **inference from what `touch` and `rm` do, not a
  measurement**, and it is recorded as inference;
- **not** "the file-list digest is unchanged" — no such digest was ever computed for the
  production store, before or after;
- **not** "this run modified nothing." **The store directory's mtime is production
  metadata and this run changed it.**

## 4. What was verified before the stop — all of it passing

**None of this is a staging or contract PASS.** It is the pre-flight that ran before the
stop.

| check | result |
|---|---|
| local archive vs authorised | `308482b9ccb93ed542a187b00d39c05931ac80b7c27609e91c7e9e24f708a648` — **exact** |
| local verifier | `verify_clean_archive.sh ad3f428` → **16/16** |
| archive on VM24 | **same digest**, 3,758,080 bytes |
| files in the VM24 export | **169** — authorised 169 |
| file-list digest on VM24 | `c58f34a65901293e1534700e4b74c468ddfb2e5f648125aac985cde67b5289ed` — **exact** |
| **live ledger, read from the subject's own export on VM24** | 18301, 18302, 18949 **all absent** — the runner would accept them |
| **live `ss -ltn`, read-only** | 18301, 18302, 18949 **all unbound** |
| `~/woa23-c1h/`, `~/woa23-c1h-work/` | **absent** |
| package clone | present, 33,565 files; manifest `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4` |
| interpreter | `/home/odbadmin/.pyenv/versions/py311/bin/python3.11` — **Python 3.11.4** |
| store resolved path | `/home/odbadmin/python/woa23/data` — **exactly the production store, no indirection** |
| **store writability** | **WRITABLE — the stop** |

## 5. Production and pm2G — both untouched

**Production, before and after, identical:**

| | value |
|---|---|
| boot id | `0b513a75-213b-40bf-8219-1c7cbc51a085` — unchanged |
| PIDs 4296 / 5040 / 5041 | starttimes 14214 / 15825 / 15829 — unchanged |
| PIDs 4357 / 4358 | starttimes 14323 / 14330 — unchanged |
| listeners 8050 / 8786 / 8787 | present, unchanged |
| production PM2 | unchanged |
| **HTTP requests to production** | **ZERO** |
| `conf/` digests | unchanged (`8db9a6ba…`, `4aaed5b7…`) |

**pm2G, untouched exactly as required:**

| | |
|---|---|
| port `18265` | **still BOUND** |
| gunicorn 1456369 + workers 1456373 / 1456374 | **still RUNNING** |
| PM2 app `woa23-pm2g-candidate` | **online** under `~/woa23-pm2g-pm2/` |
| `~/woa23-pm2g{,-work,-pm2,-uvcache}` | **all retained** (9669 / 1 / 5 / 8898 files) |

**No `pm2` command was issued. No process signalled. No port released. No cleanup.**

## 6. The decision: option B — a non-owner account

**PI decision, 2026-08-25: C1 must run as an account with no write permission to the
production store.**

| option | outcome |
|---|---|
| A. accept writable, rely on discipline | **rejected** — the guarantee would be a code property, unassertable by pre-flight |
| **B. run C1 as an account without store write permission** | **ADOPTED** |
| C. read-only bind mount | not taken — needs privilege this campaign does not hold |
| D. `chmod` / `chown` the store | **forbidden**, and recorded only as considered-and-refused |

**Two things follow, and both are offline work:**

1. **The write probe is removed entirely.** No future pre-flight creates or deletes
   anything under the production store. Writability is determined from **`stat` and
   `access` evidence only** — ownership, mode, and the effective permissions of the running
   account. See the new request's pre-flight order.
2. **Identity is captured FIRST**, before any other step: resolved path,
   permissions/ownership, directory mtime, file count, total bytes, and a file-list digest.

**`c1h` is a CONSUMED failed-preflight identity.** Its label, staging path, workdir and
ports `18301`/`18302`/`18949` are spent and must never be reused, even though no port was
ever bound.

**What this run may not do, and did not do:** `chmod`, `chown`, create a bind mount, or
change any account or ACL. **Obtaining a suitable account or ACL is a request to IT, not an
action for this campaign to take on VM24.**

## 7. Standing limits, unchanged

**B1–B5 remain open.** **B6** remains decided. **B7** remains open. **`pm2G` remains NOT A
PASS** and is not re-classified. **`c1f`, `c2g`, `s2pB` and `pm2G` are NOT back-filled into
this run's evidence**, and nothing here may be reported as confirming them. **C2 `c2h` was
not authorised and has not run.**

**The candidate `ad3f428` is neither validated nor invalidated by this run.** Its contract
remains untested on VM24; the 129-run offline evidence stands on its own and is not a
substitute.

## 8. Evidence

`scratchpad/c1h/01-preflight.txt` (VM24 pre-flight), `02-archive-ledger.txt` (archive,
file-list, live ledger), `03-store-preflight.txt` (**the stop**), `04-store-poststop.txt`
(store intact, nothing started).

On VM24: `/home/odbadmin/c1h-archive.tar` — the transferred subject archive, retained. No
other file was created by this run.
