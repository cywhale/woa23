# C1 `c1j` — FINAL execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. Nothing has been run.** No VM24
contact since the read-only verification of 2026-08-25. No port bound, no arm started.
**This document is not an authorisation.**

Final form of [C1-execution-request-c1j.md](C1-execution-request-c1j.md), with every value
written out in full and unabbreviated, per review.

---

## 1. Execution subject — complete values

| item | value |
|---|---|
| **commit** | `832e767dfccae5fad07329590eeb3314e540ca22` |
| **archive SHA-256** | `05d36054102be1fcf841a51db88f8d7c6e4241a08a74a1ceb1cf59ccbdb1a041` |
| **file count** | `176` |
| **file-list SHA-256** | `c819b924c87ba5e8bc850987f78152b113416f626d00e0846ef96ac23e286dee` |
| **verifier** | `scripts/verify_clean_archive.sh 832e767dfccae5fad07329590eeb3314e540ca22` → **16/16, all passed** |

**Supersedes** `75d0f977774d22454650c0bf555fa7134ca55ebd` (174 files). Every digest is
re-derived on VM24 before the run; a mismatch on any of them is a **stop**.

### 1.1 Full source hashes at the subject

Re-derived on VM24 file by file. **Any mismatch is a stop.**

| SHA-256 | file |
|---|---|
| `e06ac433fafbb73f1174667071ee5783e3c9b79fc73e7fad4eb5ef1510d4f694` | `scripts/run_controlled.sh` |
| `15467af7fbd16b64576609ace2ce33ccf522089e7b81bf4da6376c6054b8b019` | `scripts/lib_procs.sh` |
| `16f641621e5f058ee68f07e6be26f3bca9417dc39c0380dc213320e8223c3d75` | `scripts/store_readonly_preflight.sh` |
| `6f1ba3a0fb280811eb9b60b13bd52b8c4ed00d0606f2b8f3135fda71894a2f0d` | `scripts/test_c1_readonly_account.sh` |
| `32f2c4a4dd55561f57b38488d0121199099a6aa4e3cfc28ba0fb48731f74da5e` | `scripts/ports_used.tsv` |
| `f9e174b0ae6351bd4abb99370a50b864e168172fef72f6c4a1b5679d1743bb89` | `scripts/verify_clean_archive.sh` |
| `50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8` | `api/query.py` |
| `15f26444af9fbffbdef8a240833c1ff7f8bc99fdf84277317eb15f859d467ce2` | `api/app.py` |
| `b806641dc7478acaca375380b8e0f8575c1fa48f093362f7f917921a3adc94ca` | `api/config.py` |
| `00cb80c2b1c4ef74f984f42026dcdc5736bfb841e34471bd882c3fd42e35b928` | `api/store_paths.py` |
| `bbe24c893d4c0708ab7d7eeb02baee5051d682559eee76e7512c0c56870bfbee` | `bench/contract_diff.py` |
| `cbe799426cddabd7437839ef13e1319658f2fda0c14470409ab94e34336d6c8b` | `bench/contract_cases.py` |
| `c2b5c6a236e1f28d577ea63d423b2b2a47014cdc817217ce3136bfb39594e2eb` | `bench/c2_summary.py` |
| `7977e18263a88bca882ea7f46fb815a758899eb06d6227aef7763f7d393dd9e3` | `bench/clone_integrity.py` |
| `7de557cbdcf9159d4401fc9ad41473811c313d68de8a52abfa11a898d1248c04` | `bench/provenance.py` |
| `aa846b8be70b0b5d466d0e2a0bbb1f4dfe6ccac5d28f795a57c1c6bbea7e378e` | `pyproject.toml` |
| `0d2980a5928d4d0964d6cb3b78bffae14aa11a70d3b51ca00f4cf39073dccc69` | `uv.lock` |

**`api/` and `bench/` are byte-identical to `ad3f428`** —
`git diff ad3f428..832e767 -- dev2026/api/ dev2026/bench/` is **empty**. **The candidate
under test has not moved since the 015 work; only the harness has.**

### 1.2 Offline evidence for this subject

| | |
|---|---|
| batches | **three, strictly serial** (`WOA23_SUITE_REPEAT=3`, `concurrency: serial`) |
| total runs | **132** (44 suites × 3) |
| **non-zero exits** | **0** |
| evidence root | `/var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T//woa23-suites-9Xr3Ay/` (retained) |
| `test_c1_readonly_account.sh` | **58 assertions**, all passed |
| `test_cli.sh` | **305 assertions**, all passed |
| `fail_lines` | **3** — the collector matching a section *heading* (`FAIL CLOSED:`) in `test_symmetric_warmup.py`, whose assertions all pass and which exits `0`. A false positive in the evidence collector, not a failing assertion |
| stray processes | **none created.** 16 pre-existing `arm.py` strays remain, all 11–20 August, **zero from the batch window** |

## 2. Execution identity — complete, all first-use

| | value |
|---|---|
| **grant** | `WOA23_S2_C1_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993**, private group only |
| **connection** | direct SSH, campaign Ed25519 key, `BatchMode=yes` |
| **label** | `c1j` |
| **staging / export root** | `/home/woa23c1ro/woa23-c1j/` |
| **workdir** | `/home/woa23c1ro/woa23-c1j-work/` |
| **HOME** | `/home/woa23c1ro` |
| **TMPDIR** | `/home/woa23c1ro/tmp-c1j/` |
| **candidate arm port** | **`18341`** |
| **reference arm port** | **`18342`** |
| **isolated dask scheduler port** | **`18979`** |
| **`WOA23_EXPECT_UID`** | `994` |

**All three ports are first-use** — absent from `scripts/ports_used.tsv` and named in no
other file. **Deliberately NOT pre-recorded in the ledger**: `run_controlled.sh` refuses a
port the ledger names, reading it from the export of the subject it runs, so a port
recorded before its run is a port that run cannot use. Verified against the subject: none
of `18341`, `18342`, `18979` matches a port row in `832e767`'s copy of the ledger. They
are added **after** `c1j` runs.

**Nothing from `c1h`, `c1i`, `c1f`, `c2g`, `s2pB` or any `pm2*` run is reused.** `c1h`
(`18301`/`18302`/`18949`) and `c1i` (`18321`/`18322`/`18969`) remain
**RETIRED-NEVER-BOUND**.

## 3. Production paths — explicit and absolute

```
--prod-dir       /home/odbadmin/python/woa23
--store          /home/odbadmin/python/woa23/data
--python-binary  /home/odbadmin/.pyenv/versions/py311/bin/python3.11
--package-clone  /home/odbadmin/woa23-s2-package-clone/dist
--clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest
```

**All three of `--prod-dir`, `--store` and `--python-binary` are MANDATORY**, enforced by
the runner: with `WOA23_EXPECT_UID` set it refuses any `$HOME`-derived fallback, because
under `HOME=/home/woa23c1ro` that would name a path inside the *running* account's home
rather than production's. The refusal names the flags it needs.

Clone manifest SHA-256, as verified 2026-08-25:
`f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4`.

## 4. The command

```
ssh -o BatchMode=yes woa23c1ro@odb24
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c1j \
  WOA23_S2_C1_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_controlled.sh --c1 \
    --workdir        /home/woa23c1ro/woa23-c1j-work \
    --prod-dir       /home/odbadmin/python/woa23 \
    --store          /home/odbadmin/python/woa23/data \
    --python-binary  /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone  /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --candidate-port 18341 \
    --reference-port 18342 \
    --scheduler-port 18979 \
    --label          c1j
```

**No privilege escalation of any kind.** Not `sudo`, not `sshpass`, not `su`, not
`setpriv`, and **not launched from `odbadmin`**. The orchestration is the SSH session, so
every arm and every worker inherits uid 994 by descent rather than by a drop.

## 5. Execution sequence — each step a stop

1. **connection identity** — `id`, `id -u`, `id -g`, `whoami`, `groups` re-confirmed as
   `uid=994(woa23c1ro) gid=993(woa23c1ro) groups=993(woa23c1ro)`; the SSH key fingerprint
   and that the session is **not** `odbadmin`. A mismatch is a stop;
2. **store identity, captured BEFORE any decision** — resolved path, mode, `owner:group`,
   `uid:gid`, directory mtime, hard links, size, top-level listing, file count, total
   bytes, and the **metadata fingerprint** (`path`/`size`/`mtime`), which is **not** a
   content baseline and is not reported as one;
3. **complete store read-only scan as uid 994** — every directory traversable and
   readable, every file readable, **nothing writable**, no symlink resolving outside the
   store. GNU `find` predicates probed before their answers are believed, with a portable
   `[ -r ]`/`[ -w ]`/`[ -x ]` fallback; both use `access(2)` and so respect the ACL.
   **No `touch`, `rm`, `mkdir`, `mv`, `chmod`, `chown`, `setfacl` or write probe on any
   path, including every failure path;**
4. **subject** — archive, file count, file-list digest and every hash in §1.1 re-derived
   on VM24 and compared;
5. **ports** — `18341`, `18342`, `18979` confirmed absent from the live ledger read from
   the subject's own export, **and** unbound by read-only `ss`;
6. **identity absence** — `/home/woa23c1ro/woa23-c1j/` and `woa23-c1j-work/` must not
   exist;
7. **production baseline** — boot id, PIDs 4296/5040/5041/4357/4358 with starttimes,
   listeners on 8050/8786/8787, production PM2 list, `conf/` digests;
8. **pm2G recorded untouched** — 18265 still bound, 1456369 and its workers still running;
9. **arms started**, then **every tracked process asserted uid 994** — masters and every
   worker, all four uids (real, effective, saved-set, filesystem), across
   `dask_scheduler`, `dask_worker`, `reference`, `candidate`. Zero checked is a failure;
10. **C1 contract validation** — canonical values and column sequence, the candidate's
    canonical **column-order** contract, the `(time_period, depth, lat, lon)` **row-order**
    contract, JSON/CSV fields, values, row order and status, with the pre-defined
    reconstruction rules for expected ordering differences;
11. **cleanup** — the runner's own path; the three ports confirmed free and
    connection-refused afterwards, recorded not assumed;
12. **production re-read and compared**, and the **store identity re-captured** and
    compared against step 2.

## 6. The runtime — shared, and stated as such

**C1 runs on production's own interpreter and a read-only clone of production's packages.
That is the point of C1. It is NOT runtime isolation and no such claim is made.**

| `sys.path` entry under `-S` | mode |
|---|---|
| `/home/odbadmin/woa23-s2-package-clone/dist` | **555** |
| `/home/odbadmin/.pyenv/versions/3.11.4/lib/python3.11` | **755** |
| `/home/odbadmin/.pyenv/versions/3.11.4/lib/python3.11/lib-dynload` | **755** |
| interpreter `/home/odbadmin/.pyenv/versions/3.11.4/bin/python3.11` | **755** |

- **World-writable entries on the load path: `0`.**
- `woa23c1ro` is in **no shared group**, so the pyenv tree's group-writable bits (group
  `odbadmin`) do not reach it.
- `py311` and `py311/bin/python3.11` are **symlinks**; `lrwxrwxrwx` is never enforced by
  the kernel. **An earlier report of mine read those bits as directory permissions and was
  wrong** — corrected in [spec 017](017-c1-run-as-readonly-account.md) §1.

**ACCEPTED RESIDUAL RISK, by PI decision:** the zero-byte world-writable
`/home/odbadmin/.pyenv/versions/3.11.4/envs/py311/.lock`, **outside the arm's `sys.path`**.
**It is not modified by this campaign** — no `chmod`, `chown`, delete or truncate.

## 7. Forbidden

- **No privilege escalation** — no `sudo`, `sshpass`, `su`, `setpriv`; **the orchestration
  is never launched from `odbadmin`**;
- **no write, `chmod`, `chown`, delete or rename** under the production store;
- **no modification** of the store, ACLs, runtime files, the `.lock`, permissions or any
  account;
- **no HTTP request to production** — 8050 / 8786 / 8787 stay at **zero**;
- **no change** to production PM2, processes, listeners or `conf/`;
- **no touching `pm2G`** — not port `18265`, not its PM2 entry, not its tree, workdir,
  store, logs or uv cache; no `pm2` command, no signal, no port release, no cleanup;
- **no `c1h` re-run**, and its evidence is retained including the store-directory mtime
  change it caused (`1787622836`, unchanged since);
- **no latency, warm-up, noise pilot, startup, deployment, PM2 or production-API testing**;
- **no conclusion about spec 016's venv runtime behaviour**;
- **no back-filling** of `c1f`, `c2g`, `s2pB`, `pm2G`, `c1h` or `c1i`;
- **no C2**, under any C1 outcome;
- **no SIGKILL**, and **no self-rerun**.

## 8. Failure handling

**Any failing step is a stop.** Retained: both arms' processes, the bound ports, the
workdir, the export, all logs, the request log and every diagnostic.

**Forbidden after a mid-flight failure:** any `pm2` command; manual process termination
(`kill`, `pkill`, `pgrep|kill`); **SIGKILL**; releasing a port; any `rm`, `find -delete` or
`chmod` on the retained tree; **self-rerun**.

A survivor after cleanup is **`CLEANUP_FAIL`** — left alive for inspection, **no SIGKILL**,
**not repeated**. Cleanup touches only this run's own arms; nothing belonging to `c1h`,
`pm2A`, `pm2B`, `pm2E`, `pm2F`, `pm2G` or any earlier C-run.

**Classification:** never reportable as a `5.2A raw byte-exact PASS`. If reconstruction or
any required verification does not complete, the classification is
**`INCOMPLETE_VALIDATION`** and **no PASS is reported**.

## 9. What a PASS will and will not mean

**Will:** the `015` candidate satisfies the C1 contract gates against the **real production
store**, read through an enforced read-only ACL by an account that cannot write it, with
every arm master and worker proven to be uid 994.

**Will not:** **not a `5.2A raw byte-exact PASS`; not a staging PASS; not a production
cutover PASS; not a latency result; not a deployment result; not a claim of runtime
isolation.** **B1–B5 remain open. B7 remains open. `pm2G` remains NOT A PASS. C2 remains
blocked** until C1 completes and is reviewed.

## 10. Submission

- **Subject: `832e767dfccae5fad07329590eeb3314e540ca22`.** This document and any later
  commit are **protocol references** and must never be back-filled as the execution
  subject.
- **Awaiting explicit authorisation. No VM24 contact until then.**
