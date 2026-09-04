# New subject and reserved identity after `b35a1`

**Status: OFFLINE RECORD. No staging request is submitted, and none will be until you
review this subject and identity.** No VM24 contact.

`b35a1` remains **`INVALID_PRE_START`** — not a candidate, B3 or B5 failure, and **not
back-fillable as any staging PASS**.

---

## 1. VM24 state, left exactly as it was

| | |
|---|---|
| `~/woa23-b35a1/` | **retained as failure evidence** — not cleaned, stopped, deleted or reused |
| `~/b35a1-archive.tar` | retained |
| `b35a1` identity | **CONSUMED** |
| **18281** | **`RETIRED-NEVER-BOUND`** — never bound, carried no listener. **Not SPENT.** Recorded in the ledger in those words |
| production, PM2, store, ACLs, permissions, port state | **unmodified** |

Nothing on VM24 was touched during this fix.

---

## 2. The fix — bootstrap path separated from execution identity

`deploy/staging_bootstrap.sh` (new). The freshness guard is **not relaxed**; the
bootstrap simply stops standing on the identity's toes.

| kind | meaning |
|---|---|
| **BOOTSTRAP PATH** | where the driver is placed so it can be run. Scratch, one file, no part of the run's identity |
| **IDENTITY PATHS** | staging root, workdir, TMPDIR, PM2_HOME, store, generated config — **all must be absent** at stage time |

`bootstrap_path_problem` refuses a bootstrap that **is**, is **inside**, or **contains**
any identity path — containment counts both ways — plus any production path, a relative
path, and an empty one.

**The driver comes from the archive, never a checkout:** streamed out with `tar -xO` and
hashed **first**, extracted, hashed again on disk, compared. A mismatch is a refusal.
**The identity-absence check runs before extraction**, so a doomed run creates nothing —
not even the bootstrap.

`staging_execute.sh` gains a comment block naming the two path kinds **at the exact guard
that refused `b35a1`**, so the trap is documented where it was fallen into.

**Tested as the real sequence** — transfer → extract driver to an external bootstrap →
`--phase stage` creates the staging root → hand over — not the shape that failed.
`test_staging_bootstrap.sh`, **65 assertions**.

One of my own assertions was wrong and corrected rather than loosened: a bootstrap inside
the store is attributed to the **staging root**, because the default store is nested
inside it. The test now checks both the root-nested store and an external one.

---

## 3. New execution subject

```
commit           58950321910eab835f730b6bacb4c028e8bbd230
subject line     fix(staging): separate the bootstrap path from the execution identity
archive sha256   a172a260b315e112c889e77a5d07d5b7af15069ce17a8e02acffe19c27a4d19b
files            218
file-list sha256 39a87bf0f2b6176c6049b73c1f683f76592ef201d30a7a3327a8963e455c2815
```

`verify_clean_archive.sh` — **all passed (16 assertions)**.

**Offline batches, re-run because the harness changed.** Three serial batches at
`5895032`, each recording HEAD itself — all attest `head=5895032… dirty=0`.
**50 suites, 4186 assertions, 0 non-zero exits, 0 differences** across all three
pairings. Roots `g2nywW`, `VwGWEm`, `iMlble`.
`test_staging_bootstrap.sh` **65** (new), `test_staging_entry.sh` 125,
`test_production_launcher.sh` 111.

`api/query.py` unchanged at `50907dee…2ca8`.

---

## 4. Reserved identity — new label, new first-use port

| | value |
|---|---|
| **label** | **`b35c1`** |
| **bootstrap path** | `/home/woa23c1ro/b35c1-bootstrap/` — **outside every identity path** |
| **staging root** | `/home/woa23c1ro/woa23-b35c1/` |
| **workdir** | `/home/woa23c1ro/woa23-b35c1-work/` |
| **TMPDIR** | `/home/woa23c1ro/tmp-b35c1/` |
| **PM2_HOME** | `/home/woa23c1ro/woa23-b35c1-pm2/` |
| **store** | `/home/woa23c1ro/woa23-b35c1/store` (synthetic) |
| **app name** | `woa23-b35c1-candidate` |
| **port** | **`18282`** |

**Nothing of `b35a1` is reused** — not its label, root, workdir, PM2_HOME, store, app
name, archive or port. **18281 is not reused.**

### 4.1 Screening — and two rejections from my own documentation

Screened two ways at the subject: zero ledger rows **and** zero occurrences anywhere in
`dev2026`.

| candidate | tree | ledger | verdict |
|---|--:|--:|---|
| label `b35c1` | 0 | — | **CLEAN** |
| label `b35b1` | **1** | — | **rejected** |
| **18282** | 0 | 0 | **CLEAN** |
| 18291 | **1** | 0 | **rejected** |
| 18297 | **6** | 0 | rejected |

`b35b1` and `18291` were rejected because **my own `staging_bootstrap.sh` usage example
names them**. Writing an example burned both. That is the two-way screen working on its
author — the same class that rejected 19109, 19113, 19116, 19118, 19136, 18271, 18273
and the label `pm2H`.

---

## 5. What has NOT been done

- **No staging request submitted**, and none will be until you review this subject and
  identity.
- **No VM24 contact** during this fix.
- `b35a1` not re-run, not cleaned, not back-filled.
- No guard relaxed; no override, force or skip flag added or passed.
- **B3, B5 and every B1–B5 blocker remain open.** Nothing here closes or moves one, and
  no production cutover is implied.

### 5.1 Limitations that still travel with any future staging run

- staging would use **PM2 5.4.2**; **production's PM2 version remains UNVERIFIED**;
- the PM2 binary sits under **another account's home**, so the pre-start digest check is
  a **mitigation, not an immutable guarantee**.
