# B3 + B5 staging validation `bs3v1` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. Nothing has been run.**
No VM24 contact since `probeC`. **No execution authorisation is requested for anything
beyond the B3 + B5 staging validation defined here.**

**This closes no production blocker.** A staging PASS would establish the launcher's
argv under a staging PM2 — nothing about production's installed `conf/start_app.sh`.

`b35a1` remains **`INVALID_PRE_START`**; `18281` remains **`RETIRED-NEVER-BOUND`**; the
existing failure evidence on VM24 is untouched.

---

## 1. The ten provenance checks, and where each is enforced

**Six of the ten were not implemented before this review.** They are now, which changed
executable code — so the subject and batches were regenerated, as instructed.

| # | check | enforced by |
|---|---|---|
| 1 | archive transfer location fresh, no reuse of an old archive | `--archive-sha256` **required** and compared to the file on disk; archive must be a regular file, not a symlink, and must not sit inside any identity or bootstrap path |
| 2 | bootstrap path must not exist before execution | `[ -e "$BOOT" ]` → refuse; not emptied, not reused |
| 3 | bootstrap, **parent** and **realpath** outside every identity/production path | `bootstrap_path_problem` (full overlap) for the bootstrap; `ancestor_path_problem` (`path_is_within`) for parent and realpaths |
| 4 | symlinked parent, symlinked bootstrap, path-prefix confusion fail closed | explicit `-L` refusals on bootstrap and parent; extracted driver refused if symlink or non-regular; `paths_overlap` is a **true path relation**, so `/a/bc` never matches `/a/b` |
| 5 | driver member unique, expected path, regular file only | `archive_member_problem` — refuses directory, symlink, hard link, duplicate, missing, and a `.bak`-style lookalike |
| 6 | driver bytes compared before and after extraction | `tar -xO` → sha256 **first**, extract, sha256 **again**, compare |
| 7 | the real stage→run sequence | end-to-end test: transfer → external bootstrap driver → `--phase stage` creates the root → `--phase run` |
| 8 | run phase loads the subject from the **staging root** | end-to-end test asserts `api`, `bench`, `scripts`, `deploy` all read from `$ROOT/dev2026` with staged content, bootstrap holding only the driver; plus source assertions that the driver derives `TREE` from `--root` and never uses `$HERE` for subject files |
| 9 | all existing guards stay fail-closed | asserted present: pre-existing identity, `.git`, file count, stale/foreign/modified tree; and the bootstrap passes **no** override, force or skip flag |
| 10 | mid-run failure → no self-clean | §6 |

**Order matters and is tested:** the identity-absence check runs **before** the driver is
extracted, so a doomed run creates nothing — not even the bootstrap.

### 1.1 Two defects of my own, found by running the tests

**The ancestor rule was wrong.** I first checked the bootstrap's **parent** with the full
overlap rule, which includes containment — but a parent legitimately *contains* the
staging root, since bootstrap and root are normally siblings. That refused every sane
layout. `path_is_within` is now the narrower relation for ancestors; containment still
applies to the bootstrap itself, where it matters.

**The member matcher missed symlinks.** A `tar -tvf` symlink line ends with
`-> target`, not the member name, so matching on a trailing name missed them. They were
reported "no member" — still a refusal, so fail-closed held, but with the wrong reason,
and **a symlink duplicating a real member would have gone unseen.** Both shapes are now
matched.

---

## 2. Execution subject

```
commit           043b7e958e0b37f5d36d505ef7d3e13da646af69
subject line     fix(staging): archive/bootstrap provenance guards — freshness,
                 symlinks, member type
archive sha256   ac2d51fc5e3db9e2f05bc4ea467af159a4c66ee42404b3cf7c4ff02b088ae1ae
files            219
file-list sha256 0565338622d7ad03ac3703fc3184d0fe36df35ba5854591f87a360884525c0ee
```

`verify_clean_archive.sh` — **all passed (16 assertions)**.
**`api/query.py` unchanged** at `50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8`;
the `api/` diff against the C1r/C2k-validated `13d6b74` is **empty**.

**Offline batches, re-run because executable code changed.** Three serial batches at
`043b7e9`, each recording HEAD itself — all attest `head=043b7e9… dirty=0`.
**50 suites, 4223 assertions, 0 non-zero exits, 0 differences** across all three
pairings. Roots `GPMivB`, `69z1HP`, `BbCMoW`. `test_staging_bootstrap.sh` **102**.

*(My commit message for `043b7e9` said 103 assertions; the correct figure is 102.)*

**This request document is a LATER commit and is not part of the subject** — see §3.1.

---

## 3. Execution identity — new label, new first-use port

| | value |
|---|---|
| **grant** | `WOA23_PM2C_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, uid 994, gid 993 |
| **connection** | direct SSH, campaign Ed25519 key, `BatchMode=yes`, `IdentitiesOnly=yes`, `RequestTTY=no` |
| **label** | **`bs3v1`** |
| **bootstrap path** | `/home/woa23c1ro/bs3v1-bootstrap/` — **outside every identity path** |
| **archive transfer path** | `/home/woa23c1ro/bs3v1-archive.tar` — outside identity and bootstrap |
| **staging root** | `/home/woa23c1ro/woa23-bs3v1/` |
| **workdir** | `/home/woa23c1ro/woa23-bs3v1-work/` |
| **TMPDIR** | `/home/woa23c1ro/tmp-bs3v1/` |
| **PM2_HOME** | `/home/woa23c1ro/woa23-bs3v1-pm2/` |
| **store** | `/home/woa23c1ro/woa23-bs3v1/store` (synthetic) |
| **app name** | `woa23-bs3v1-candidate` |
| **port** | **`18283`** |
| **`WOA23_PM2_BIN`** | `/home/odbadmin/.npm-global/bin/pm2` (absolute; no host PATH change) |

Both screened against the subject: `bs3v1` **0** occurrences, `18283` **0** tree
occurrences and **0** ledger rows.

### 3.1 `b35c1`/`18282` withdrawn — and the structural reason

I reserved `b35c1` and `18282` in `B35-subject-and-identity-b35c1.md`, **then committed
that document, and it became part of the subject.** The two-way screen now finds both
named in the tree and rejects them — correctly.

**The rule this establishes:** an identity must be named **only in a request document
committed AFTER the subject is cut**, exactly as every C1/C2 request was. Reserving an
identity inside the subject burns it. That is why this document is a later commit, and
why `b35c1` and `18282` are withdrawn rather than argued for.

Rejections so far by this screen: ports 19109, 19113, 19116, 19118, 19136, 18271,
18273, 18291, 18297, 18282; labels `pm2H`, `b35A`, `b35b1`, `b35c1`.

**Nothing of `b35a1` is reused** — not its label, root, workdir, PM2_HOME, store, app
name, archive or port. **`18281` is not reused.**

---

## 4. The sequence

```
1. transfer   archive -> /home/woa23c1ro/bs3v1-archive.tar   (outside identity+bootstrap)
2. gate       re-verify the pm2 binary against probeC: realpath, sha256, version,
              owner/mode, and r-x on binary / parent / package. Any drift = STOP.
3. preflight  identity; 18283 live-unbound and absent from the subject's ledger; all
              identity paths absent; PM2_HOME not production's; production and pm2G
              baselines; store read-only scan as uid 994
4. bootstrap  deploy/staging_bootstrap.sh --archive ... --archive-sha256 ...
              --bootstrap /home/woa23c1ro/bs3v1-bootstrap --root ... --pm2-home ...
              -- --phase stage --label bs3v1 --port 18283 --app woa23-bs3v1-candidate
              --files 219 --filelist 0565338622d7ad03ac3703fc3184d0fe36df35ba5854591f87a360884525c0ee
5. setup      venv / store, per the driver
6. run        --phase run, with WOA23_PM2_BIN absolute
7. verify     /proc/<pid>/cmdline and /proc/<pid>/environ (§5)
8. stop       named app only; no SIGKILL; verify release
```

The bootstrap script itself must be obtained the same way — it is **in the archive**, at
`dev2026/deploy/staging_bootstrap.sh`, and will be extracted to the bootstrap path by the
same `tar -xO` + double-hash method it applies to the driver.

---

## 5. What will be verified from the running process

From `/proc/<pid>/cmdline` and `/proc/<pid>/environ`:

- the argv is the expected `production_app.sh` / `api.app:app`;
- `PM2_HOME` in the process environment is the **staging** one;
- **`WOA23_PM2_BIN` has not leaked** into the app process;
- no production PM2 variable or production path is carried in;
- **master and every worker are uid 994**;
- **port 18283 is held by this staging process**.

### 5.1 B3 and B5, reported separately

| | staging can show |
|---|---|
| **B3** | port/config isolation: the argv binds the port from `WOA23_PORT`; no `8050` literal reachable |
| **B5** | no `--reload` in the **running process's** argv |

**Neither will be described as closing its production blocker.**

---

## 6. Failure handling

**If any step fails: stop, preserve everything, report.** No `stop`, `delete`, `kill`,
cleanup, port release or re-run on my own initiative — the `b35a1` discipline, unchanged.
Cleanup only within the authorised staging scope, and only on a successful run.

**Never:** `pm2 * all`, `pm2 kill`, global `save`, `resurrect`, any production-scoped
command, any SIGKILL, any touch of production API, store, ACLs, permissions or `conf`,
any use of production's app name or `/home/odbadmin/.pm2`, or any modification of the
host PATH.

---

## 7. Standing limitations

- staging would run **PM2 5.4.2**; **production's PM2 version is UNVERIFIED** — determining
  it needs executing pm2 or reading daemon state, both forbidden. If they differ, this
  exercises a different PM2 than production runs.
- the PM2 binary is under **another account's home**. The pre-start digest check is a
  **mitigation, not an immutable guarantee**: `odbadmin` can replace it, and the gate
  narrows the window to the moment of use rather than closing it.
- **B1, B2, B4, B7 and the cutover are untouched**, and B3/B5 remain open on production
  whatever this run shows.

## 8. Submission

`bs3v1` is submitted for **review**. It has not been executed and no VM24 contact has
been made. Nothing runs until you authorise it explicitly.
