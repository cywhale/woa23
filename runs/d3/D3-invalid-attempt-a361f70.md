# D-3 attempt on subject `a361f70` — `INVALID_PRE_START_MISSING_PM2_HOME`

**Offline/read-only reconciliation. No VM24 contact was made while preparing this document.
No retry, no staging, no PM2, no store access, no cleanup. `dep3s` / `19387` not reused and
not replaced.**

---

## 1. `52fe5806…` identified

```
52fe5806e421f693e0387be53cddff7d177a032e249884d00e2f32d6a373794f
```

is the archive of

| | |
|---|---|
| subject | `19aabf02860955b0e228918e451258341c6b49fd` |
| short | `19aabf0` |
| dated | Mon 31 Aug 2026 08:18:28 +0800 |
| title | `fix(sentinel): canonical starttime; test the driver's exit code on refusal` |
| tar members | **264** |
| regular files | **253** |

It is a **superseded** subject from earlier in this campaign — five subjects before
`a361f70` — and it carries a request of its own, `D3-execution-request-19aabf0.md`.

**So the digest is real and the "253" is real: 253 is `19aabf02`'s FILE count** (its member
count is 264).

---

## 2. Classification: `INVALID_PRE_START_MISSING_PM2_HOME`

The attempt used and verified the **authorised** archive. It halted at the driver's argument
validation because `--pm2-home` was not passed through the `--` separator (§4.1).

```
subject a361f70668f28eaec49fabf078ca4c9c05d5d4ed
archive 92e93bd378af5835ab3d917e6a90ffbfbc40b2632dffbcabee99c70ac6da6ba3
        272 tar members, 261 regular files
```

Three checks, all computed **on VM24**, agreed on that archive:

| check | what ran | result |
|---|---|---|
| A | `sha256sum` on the transferred file | `92e93bd3…` |
| B | the bootstrap's own stale-archive guard (`== 3.`), reading `~/d3xfer-a361f70.tar` | `92e93bd3…` — *"MATCH — this is the authorised archive, not a leftover"* |
| C | both delivery members, hashed in-archive and again on disk | `a69b1265…c52d2b`, `dc82e80f…c23d23d` |

**An earlier reading of this attempt as a wrong-archive transfer has been withdrawn on
review and is NOT a VM24 finding.** `52fe5806…` was never on VM24 as far as any evidence
shows; it is recorded in §1 only as the archive digest of the superseded subject `19aabf02`.

**Why the number shapes looked like a mismatch, recorded so it cannot recur.** `tar` members
and regular files are different counts:

| subject | tar members | regular files |
|---|---|---|
| `a361f70` (authorised) | **272** | **261** |
| `19aabf02` (superseded, never transferred) | 264 | **253** |

The attempt log printed `members : 272` while the request stated `files 261` — the same
archive described two ways. From this point the member count is verified and recorded
alongside the digest at both ends of every transfer (§6), so the two counts are never again
available to be compared against each other.

## 3. The authorised subject, re-confirmed locally

```
subject   a361f70668f28eaec49fabf078ca4c9c05d5d4ed
archive   92e93bd378af5835ab3d917e6a90ffbfbc40b2632dffbcabee99c70ac6da6ba3
files     261
file-list 699d0f55175db02fe0aa6083c7223685c4082bfe2e9f450968a182e47f7c43db
```

Re-derived from git at the time of writing; all four values match the authorisation. The
archive additionally has **272 tar members**, and that number is recorded here so the
members/files distinction is never again available to be mistaken for a mismatch.

---

## 4. How far the attempt got — nothing was reached

From the retained attempt log and the post-halt state capture:

| | |
|---|---|
| staging root `~/woa23-dep3s` | **never created** |
| workdir `~/woa23-dep3s-work` | **never created** |
| `PM2_HOME` `~/woa23-dep3s-pm2` | **never created** |
| tmpdir `~/tmp-dep3s` | **never created** |
| store symlink `~/woa23-dep3s/store` | **never created** |
| PM2 | **never started** — no daemon, no app |
| production store | **never opened** by the driver; fingerprint `abe6c212…c61806` unchanged |
| D-3 cases | **none issued** — not one request to port 19387, and none to production 8050 |
| port 19387 | **never bound**; 0 listeners, 0 sockets in any state |
| `dep3s` processes | **0** |

The driver refused during **argument validation**, before its first `mkdir`. The refusal was:

```
--pm2-home is required in the stage phase too.
STAGE_EXIT=2
```

### 4.1 The `--pm2-home` passthrough defect

The bootstrap accepts `--pm2-home` for its own identity-absence checks, but forwards only two
arguments to the driver:

```bash
exec "$DRIVER_LOCAL" --root "$ROOT" --archive "$ARCHIVE" "$@"       # line 421
```

The driver requires it in the stage phase (line 188). It must therefore be supplied **twice**:
once to the bootstrap, and once again after `--`.

**The subject's own documented example omits the second one**, so following it verbatim
produces exactly this refusal:

```
#       --pm2-home  /home/woa23c1ro/woa23-b35b1-pm2 \
#       -- --phase stage --label b35b1 --port 18291 --app woa23-b35b1-candidate \
#          --files 203 --filelist <sha256>
```

This is a defect in a usage example, not in a guard: the driver failed **closed**, refusing
rather than running with an unset `PM2_HOME`. Correcting the example would change
`staging_bootstrap.sh` and therefore require a new subject and three clean batches
(requirement 8); the request-level correction in §5 does not.

---

## 5. The corrected invocation

Both occurrences of `--pm2-home` are required, and are marked:

```bash
export WOA23_PM2C_GRANTED=yes
export UV_OFFLINE=1
export UV_PYTHON_DOWNLOADS=never
export PYTHONDONTWRITEBYTECODE=1          # exported by the SPAWNING shell

bash <fresh-bootstrap-extract>/dev2026/deploy/staging_bootstrap.sh \
  --archive        <FRESH transfer path>/subject-a361f70.tar \
  --archive-sha256 92e93bd378af5835ab3d917e6a90ffbfbc40b2632dffbcabee99c70ac6da6ba3 \
  --bootstrap      <FRESH bootstrap path, outside every identity path> \
  --root           ~/woa23-dep3s \
  --workdir        ~/woa23-dep3s-work \
  --tmpdir         ~/tmp-dep3s \
  --pm2-home       ~/woa23-dep3s-pm2 \            # (1) for the bootstrap's own checks
  -- --phase stage --label dep3s --port 19387 --app woa23-dep3s-candidate \
     --pm2-home  ~/woa23-dep3s-pm2 \              # (2) passthrough: the driver requires it
     --files 261 \
     --filelist 699d0f55175db02fe0aa6083c7223685c4082bfe2e9f450968a182e47f7c43db \
     --store-mode real-readonly \
     --real-store /home/odbadmin/python/woa23/data
```

`--root` and `--archive` are supplied by the bootstrap automatically and must **not** be
repeated after `--`.

---

## 6. What a corrected continuation must use — and must not

**Must not reuse:**

| artefact | reason |
|---|---|
| `~/d3boot-dep3s` | it exists; the bootstrap refuses a pre-existing bootstrap path by design, and it is retained as evidence of this attempt |
| `~/d3xfer-a361f70.tar` and `~/d3xfer-a361f70/` | retained as evidence of the halted attempt, not overwritten and not reused |

**Must use:** a **fresh external transfer path** and a **fresh bootstrap path**, both outside
every identity path, with the **261-file `a361f70` archive**.

**Digest verification, before and after transfer:**

1. derive locally from git and record the digest;
2. transfer;
3. `sha256sum` **on VM24** and compare with `92e93bd3…`;
4. `tar -tf | wc -l` on VM24 → expect **272 members**, and record it as members, not files;
5. verify both delivery members from that archive —
   `staging_execute.sh` `a69b1265…c52d2b`, `lib_store_guard.sh` `dc82e80f…c23d23d` —
   in-archive and again after extraction.

Step 4 is new, and it exists because of §2: recording the member count alongside the digest
removes the ambiguity that a members-versus-files comparison can create.

---

## 7. Evidence retained

Nothing has been deleted, moved or overwritten. On VM24, untouched since the halt:
`~/d3xfer-a361f70.tar`, `~/d3xfer-a361f70/`, `~/d3boot-dep3s/` (containing the two delivered
files, mode `700`), and all `dep3m` / `dep3h` retained state. Locally, the attempt's
`stage.log` holds the bootstrap's complete on-VM24 output including Checks B and C.

---

## 8. `dep3s` / `19387` — the facts, and no assumption of reuse

**I am not treating it as reusable, and I have not selected a replacement.**

| | |
|---|---|
| ever bound | **no** — the port was never bound, in any socket state |
| identity paths created | **none** — root, workdir, `PM2_HOME`, tmpdir and store link were all never created |
| PM2 / cases / store access | none |
| named in an authorised attempt | **yes** — this one |
| bootstrap path created | `~/d3boot-dep3s` exists, but a bootstrap path is **not part of run identity** (spec 019) |

**Assessment.** On the campaign's own criterion — an identity is consumed once a run has been
authorised and *started*, and its evidence is named against it — `dep3s` / `19387` is
**cleaner than `dep3m`**, which is retired: `dep3m`'s staging root and workdir exist on disk,
whereas `dep3s` created none of its identity paths. The only artefact bearing its name is the
bootstrap directory, which the campaign has already ruled is not part of identity.

**On that basis it is technically eligible for explicit reuse in a corrected continuation.**
It is nevertheless **not** reused here, because the attempt is classified invalid and because
reuse must be an explicit decision rather than my inference. If reuse is declined, a fresh
label and first-use port will be selected only after the final subject is fixed, and no
alternatives will be listed.

---

## 9. Status

| | |
|---|---|
| attempt | **halted pre-start** at argument validation |
| classification | **`INVALID_PRE_START_MISSING_PM2_HOME`** — the authorised archive was used and verified; `--pm2-home` was not passed through the `--` separator |
| `52fe5806…` | the archive digest of the superseded subject `19aabf02` (264 members / 253 files). **Not a VM24 finding**; it was never transferred |
| authorised subject | `a361f70…`, archive `92e93bd3…`, 261 files / 272 members, file-list `699d0f55…` — re-confirmed locally |
| reached | **nothing** — no case, no PM2, no store symlink, no identity path |
| `dep3s` / `19387` | never-bound, pre-start invalid; **reuse not assumed** — §8 |
| executable code | **unchanged**; this package is documentation only, so no new subject and no batch rerun |
| VM24 | **not contacted** during this reconciliation |
| evidence | retained, nothing deleted or overwritten |
| D-3 | **stopped, awaiting review** |
