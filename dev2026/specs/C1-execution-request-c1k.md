# C1 `c1k` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. C1 has not been run.** No arm started,
no port bound, no staging or workdir created. **This document is not an authorisation.**

Supersedes [C1-execution-request-c1j-FINAL.md](C1-execution-request-c1j-FINAL.md). `c1j` is
**CONSUMED** — [C1-result-c1j.md](C1-result-c1j.md), `INCOMPLETE_VALIDATION`, aborted at
connection identity — and neither its label nor its ports are reused.

---

## 1. What changed since `c1j`

**Only the SSH key, and it is fixed.** `authorized_keys` for `woa23c1ro` now holds the
`mac-claude` public key with restricted options, installed by the PI.

**No code changed. The subject is unchanged.**

### 1.1 The connection premise, verified rather than assumed

`c1j` died at step 1 on an unverified assumption. It is now verified — **read-only, nothing
created, and not C1 execution**:

| check, as `woa23c1ro` | result |
|---|---|
| `id` | **`uid=994(woa23c1ro) gid=993(woa23c1ro) groups=993(woa23c1ro)`** |
| `whoami` | `woa23c1ro` |
| `HOME` | `/home/woa23c1ro` |
| shell | `/bin/bash` |
| arbitrary command permitted (no forced `command=`) | **yes** |
| env-prefixed multi-argument invocation — the run's shape | **yes** — `uid=994`, `TMPDIR` and `WOA23_EXPECT_UID` both propagate |
| traverse + read the production store root | **yes** |
| **store writable by 994** | **NO** — correct |
| read a real data file | **yes** — `…/1_degree/seasonal/Nutrients/.zgroup` |
| package clone readable | **yes** |
| interpreter executable | **yes** |
| write its own `HOME` | **yes** |
| `c1k` paths | **absent** |

**The restricted SSH options do not block the run's command shape**, which was the one new
risk this check existed to retire.

## 2. Execution subject — unchanged, and re-verified

**No executable code has changed since `832e767`.** `git diff 832e767..HEAD` over
`api/`, `bench/`, `scripts/` and `deploy/` touches only `scripts/ports_used.tsv`, and that
edit is **comment-only** (a note recording that `c1j`'s ports must not be pre-listed). The
subject therefore stands with its verified digests.

| item | value |
|---|---|
| **commit** | `832e767dfccae5fad07329590eeb3314e540ca22` |
| **archive SHA-256** | `05d36054102be1fcf841a51db88f8d7c6e4241a08a74a1ceb1cf59ccbdb1a041` |
| **file count** | `176` |
| **file-list SHA-256** | `c819b924c87ba5e8bc850987f78152b113416f626d00e0846ef96ac23e286dee` |
| **verifier** | `scripts/verify_clean_archive.sh 832e767dfccae5fad07329590eeb3314e540ca22` → **16/16** |

### 2.1 Full source hashes at the subject

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

**`api/` and `bench/` remain byte-identical to `ad3f428`.** The candidate under test has
not moved since the 015 work.

### 2.2 Offline evidence

Three strictly serial batches on this subject: **132 runs, 0 non-zero**, evidence root
`/var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T//woa23-suites-9Xr3Ay/` (retained).
`test_c1_readonly_account.sh` **58 assertions**; `test_cli.sh` **305**. `fail_lines = 3` is
the collector matching a section *heading* in `test_symmetric_warmup.py`, which exits `0` —
a false positive in the evidence collector. **No strays created.**

## 3. Execution identity — fresh label, first-use ports

| | value |
|---|---|
| **grant** | `WOA23_S2_C1_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993**, private group only |
| **connection** | direct SSH, `mac-claude` Ed25519 key, `BatchMode=yes`, `RequestTTY=no` |
| **label** | **`c1k`** |
| **staging / export root** | `/home/woa23c1ro/woa23-c1k/` |
| **workdir** | `/home/woa23c1ro/woa23-c1k-work/` |
| **HOME** | `/home/woa23c1ro` |
| **TMPDIR** | `/home/woa23c1ro/tmp-c1k/` |
| **candidate arm port** | **`18361`** |
| **reference arm port** | **`18362`** |
| **isolated dask scheduler port** | **`18989`** |
| **`WOA23_EXPECT_UID`** | `994` |

**All three ports are first-use** — absent from `scripts/ports_used.tsv` and named in no
other file. **Deliberately NOT pre-recorded in the ledger**: `run_controlled.sh` refuses a
port the ledger names, reading it from the export of the subject it runs, so a port
recorded before its run is a port that run cannot use. They are added **after** `c1k` runs.

**Nothing from `c1j`, `c1i`, `c1h`, `c1f`, `c2g`, `s2pB` or any `pm2*` run is reused.**
Retired, all **RETIRED-NEVER-BOUND**: `c1h` `18301`/`18302`/`18949`, `c1i`
`18321`/`18322`/`18969`, `c1j` `18341`/`18342`/`18979`.

## 4. The command

```
ssh -o BatchMode=yes -o RequestTTY=no -i ~/.ssh/id_ed25519_odb woa23c1ro@192.168.2.24
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c1k \
  WOA23_S2_C1_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_controlled.sh --c1 \
    --workdir        /home/woa23c1ro/woa23-c1k-work \
    --prod-dir       /home/odbadmin/python/woa23 \
    --store          /home/odbadmin/python/woa23/data \
    --python-binary  /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone  /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --candidate-port 18361 \
    --reference-port 18362 \
    --scheduler-port 18989 \
    --label          c1k
```

**No privilege escalation.** Not `sudo`, not `sshpass`, not `su`, not `setpriv`, and
**never launched from `odbadmin`**. The orchestration *is* the SSH session, so every arm
and every worker inherits uid 994 **by descent, not by a drop**.

## 5. Execution sequence — each step a stop

1. **connection identity** — `id`, `whoami`, `groups` re-confirmed as `uid=994 gid=993`,
   private group only, and **not** `odbadmin`;
2. **store identity, captured BEFORE any decision** — resolved path, mode, `owner:group`,
   `uid:gid`, directory mtime, hard links, size, top-level listing, file count, total
   bytes, and the **metadata fingerprint** (`path`/`size`/`mtime`), which is **not** a
   content baseline and is never reported as one;
3. **complete store read-only scan as uid 994** — every directory traversable and
   readable, every file readable, **nothing writable**, no symlink resolving outside the
   store. GNU `find` predicates probed before their answers are believed, with a portable
   `[ -r ]`/`[ -w ]`/`[ -x ]` fallback; both use `access(2)` and so respect the ACL.
   **No `touch`, `rm`, `mkdir`, `mv`, `chmod`, `chown`, `setfacl` or write probe on any
   path, including every failure path;**
4. **subject** — archive, file count, file-list digest and every hash in §2.1 re-derived on
   VM24 and compared;
5. **ports** — `18361`, `18362`, `18989` confirmed absent from the live ledger read from
   the subject's own export, **and** unbound by read-only `ss`;
6. **identity absence** — `/home/woa23c1ro/woa23-c1k/` and `woa23-c1k-work/` must not
   exist;
7. **production baseline** — boot id, PIDs 4296/5040/5041/4357/4358 with starttimes,
   listeners on 8050/8786/8787, production PM2 list, `conf/` digests;
8. **pm2G recorded untouched** — `18265` still bound, 1456369 and its workers still
   running;
9. **arms started**, then **every tracked process asserted uid 994** — masters and every
   worker, all four uids (real, effective, saved-set, filesystem), across
   `dask_scheduler`, `dask_worker`, `reference`, `candidate`. **Zero checked is a failure;**
10. **C1 contract validation** — canonical values and column sequence, the candidate's
    canonical **column-order** contract, the `(time_period, depth, lat, lon)` **row-order**
    contract, JSON/CSV fields, values, row order and status, with the pre-defined
    reconstruction rules for expected ordering differences;
11. **cleanup** — the runner's own authorised C1 path only; the three ports confirmed free
    and connection-refused afterwards, recorded not assumed;
12. **production re-read and compared**, and **store identity re-captured** and compared
    against step 2.

## 6. The runtime — shared, and stated as such

**C1 runs on production's own interpreter and a read-only clone of production's packages.
That is the point of C1. It is NOT runtime isolation and no such claim is made.**

Load path under `-S`: the clone `dist/` (**555**), the 3.11.4 stdlib (**755**),
`lib-dynload` (**755**), the interpreter (**755**). **World-writable entries: `0`.**
`woa23c1ro` is in **no shared group**. `py311` and `py311/bin/python3.11` are **symlinks**
whose `lrwxrwxrwx` bits the kernel never enforces — an earlier report of mine misread those
as directory permissions and was wrong ([spec 017](017-c1-run-as-readonly-account.md) §1).

**ACCEPTED RESIDUAL RISK, by PI decision:** the zero-byte world-writable
`/home/odbadmin/.pyenv/versions/3.11.4/envs/py311/.lock`, **outside the arm's `sys.path`**.
**Not modified by this campaign.**

## 7. Forbidden

- **No privilege escalation** — no `sudo`, `sshpass`, `su`, `setpriv`; **never launched
  from `odbadmin`**;
- **no write, `chmod`, `chown`, delete or rename** under the production store;
- **no modification** of the store, ACLs, runtime files, the `.lock`, permissions or any
  account;
- **no HTTP request to production** — 8050 / 8786 / 8787 stay at **zero**;
- **no change** to production PM2, processes, listeners or `conf/`;
- **no touching `pm2G`** — not port `18265`, not its PM2 entry, tree, workdir, store, logs
  or uv cache; no `pm2` command, no signal, no port release, no cleanup;
- **no re-run of `c1h`, `c1i` or `c1j`**, and their evidence is retained;
- **no latency, warm-up, noise pilot, startup, deployment, PM2 or production-API testing**;
- **no conclusion about spec 016's venv runtime behaviour**;
- **no back-filling** of `c1f`, `c2g`, `s2pB`, `pm2G`, `c1h`, `c1i` or `c1j`;
- **no C2**, under any C1 outcome — it needs its own later authorisation;
- **no SIGKILL**, **no self-rerun**, and **no cleanup beyond the authorised C1 path**.

## 8. Failure handling

**Any failing step is a stop.** Retained: both arms' processes, the bound ports, the
workdir, the export, all logs, the request log and every diagnostic.

**Forbidden after a mid-flight failure:** any `pm2` command; manual process termination;
**SIGKILL**; releasing a port; any `rm`, `find -delete` or `chmod` on the retained tree;
**self-rerun**.

A survivor after cleanup is **`CLEANUP_FAIL`** — left alive for inspection, **no SIGKILL**,
**not repeated**. Cleanup touches only this run's own arms.

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
blocked.**

## 10. Submission

- **Subject: `832e767dfccae5fad07329590eeb3314e540ca22`**, unchanged and re-verified. This
  document and any later commit are **protocol references** and must never be back-filled
  as the execution subject.
- **Awaiting explicit authorisation. C1 will not be run until it is given.**
