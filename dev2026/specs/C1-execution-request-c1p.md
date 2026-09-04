# C1 `c1p` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. C1 has not been run.** No VM24 contact
during this fix. **This document is not an authorisation.**

Supersedes [C1-execution-request-c1n.md](C1-execution-request-c1n.md). `c1n` is **CONSUMED**
— [C1-result-c1n.md](C1-result-c1n.md), `INCOMPLETE_VALIDATION` — and neither its label, its
ports nor its staging tree are reused.

---

## 1. What changed since `c1n`

`c1n` reached further than any attempt: uv resolved, the harness venv **built** (58
distributions), clone integrity **matched**. It then stopped on *"production is not
listening on 8050"* **while production was listening**, because `pids_on_port` greps `ss`
output for `pid=` and **`ss -p` reveals a socket's owner only to that owner or to root**.

**Option A is implemented: production's identity now comes from explicitly supplied pids,
validated against `/proc`.**

| question | how it is answered now |
|---|---|
| is the port listening? | **`port_is_listening`** — `ss -ltn`, no `-p`, **no ownership consulted** |
| is it the expected process? | **the supplied pids**, each validated against `/proc` — world-readable for `stat` and `cmdline` |

This **preserves the property the original check wanted**. Its comment is right that
*"someone is still listening" is not "production is the process it was"*: if production
restarts, the supplied pids die or their starttimes change, and the run refuses.

### 1.1 `verify_prod_pid`, per pid

`/proc/<pid>/stat` readable · `(pid, starttime)` captured · `cmdline` identifies production
· `exe` checked **when readable**.

**`/proc/<pid>/exe` is a symlink only the owner and root may read *through*, so a failure
there is not evidence of anything and is not reported as one.** It is checked when readable
and skipped otherwise — the same absent-vs-unreachable lesson `c1m` taught, applied before
it could produce another false refusal.

**Fail-closed on:** missing `--prod-pids`, an empty list, a non-numeric pid, a missing
`/proc` entry, `cmdline` mismatch, `exe` mismatch, listener absent, and none-validated.
**A pid that fails does not let the others stand in for it.**

**PID reuse is why starttime is carried everywhere** — a pid alone is not an identity;
after a restart the same number belongs to something else.

### 1.2 Where the fresh pids come from — and the one risk in it

**The pids are discovered in this run's own pre-flight, not copied from any earlier report
and not written into any script.** The pre-flight scans `/proc/*/cmdline` — world-readable
— for production's gunicorn signature and reports what it finds; those values are then
passed to `--prod-pids` and re-validated inside the run.

**The risk, stated rather than glossed: `pm2G` is still running a gunicorn on 18265 whose
command line resembles production's.** A naive `cmdline` scan would match both. So the
pre-flight:

- **excludes** the known `pm2G` pids (`1456369`, `1456373`, `1456374`), which are recorded
  in [C1-result-c1n.md](C1-result-c1n.md) as retained-and-running;
- **cross-checks** the surviving set against the count and starttimes seen in the
  production baseline read moments earlier in the same pre-flight;
- **STOPS and reports rather than guessing** if the set is ambiguous, empty, or disagrees
  with the baseline.

**If the pre-flight cannot identify production unambiguously, the run does not start and I
will ask you to name the pids.** Guessing which gunicorn is production is exactly the
mistake this whole mechanism exists to prevent.

## 2. Execution subject

| item | value |
|---|---|
| **commit** | `2d6f812e22f355ed4f5abad21106596f6274623d` |
| **archive SHA-256** | `baa445ed340eff0775d325dedbc8e5e085fd8b25ec2097ef5dac5a6b46a9ca0c` |
| **file count** | `185` |
| **file-list SHA-256** | `005a2b4f029a9c4660db987c19cf5661860ab5c1ff42a3d7bfdffd05c35b0f91` |
| **verifier** | `scripts/verify_clean_archive.sh 2d6f812e22f355ed4f5abad21106596f6274623d` → **16/16** |

**Supersedes `7c29585`** (183 files). **183 → 185**: four modified (`lib_procs.sh`,
`run_controlled.sh`, `test_c1_readonly_account.sh`, `ports_used.tsv`) and two added — the
`c1n` request and result, both documentation.

### 2.1 Full source hashes at the subject

| SHA-256 | file |
|---|---|
| `cbc8731dabd2bb96b9bb99236a34c7db8aee2ed308f59c2e3347d60939ea4002` | `scripts/run_controlled.sh` |
| `5d1ff7c4cabc6b289933beb8d31f8b06e5b7d14cb3e21e1cada131f8fb441bce` | `scripts/lib_procs.sh` |
| `16f641621e5f058ee68f07e6be26f3bca9417dc39c0380dc213320e8223c3d75` | `scripts/store_readonly_preflight.sh` |
| `666fea93ed7a20f49b552ebcd6079e3cd21bd3701c4a6ca8476de0f8ca2c1710` | `scripts/test_c1_readonly_account.sh` |
| `16bf18ab203a86931fea04050e69092f9b60f4da4a9244ad0f8dee14ee7f1f62` | `scripts/ports_used.tsv` |
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

**`api/` and `bench/` remain byte-identical to `ad3f428`.** The candidate under test has not
moved across `c1k`, `c1m`, `c1n` or `c1p` — **only the harness has**.

### 2.2 Offline evidence

| | |
|---|---|
| batches | **three, strictly serial** |
| total runs | **132** (44 suites × 3) |
| **non-zero exits** | **0** |
| evidence root | `/var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T//woa23-suites-pwOSWF/` (retained) |
| `test_c1_readonly_account.sh` | **135 assertions** (was 106) |
| `test_procs.sh` | **181 assertions** |
| `test_cli.sh` | **305 assertions** |
| `fail_lines` | **3** — the collector matching a section *heading* in `test_symmetric_warmup.py`, which exits `0` |

**The unprivileged shape is reproduced, not described:** a fake `ss` prints `LISTEN` with
**no `pid=`** for `:8050` — exactly what `c1n` saw — and `port_is_listening` is proven to
work under it. A **synthetic procfs** (the project's existing `PROC_ROOT` seam) exercises a
valid pid, a `cmdline` mismatch, a missing entry, a non-numeric pid, and **PID reuse via a
changed starttime**.

**Retained roots, none cleaned:** `woa23-suites-uUJ4FC`, `woa23-suites-nKv4kX`,
`woa23-suites-9Xr3Ay`, and on VM24 the `c1k`, `c1m` and `c1n` trees with their archives.

## 3. Execution identity — fresh label, first-use ports

| | value |
|---|---|
| **grant** | `WOA23_S2_C1_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993**, private group only |
| **connection** | direct SSH, `mac-claude` Ed25519 key, `BatchMode=yes`, `RequestTTY=no` |
| **label** | **`c1p`** |
| **staging** | `/home/woa23c1ro/woa23-c1p/` |
| **workdir** | `/home/woa23c1ro/woa23-c1p-work/` |
| **HOME** | `/home/woa23c1ro` |
| **TMPDIR** | `/home/woa23c1ro/tmp-c1p/` |
| **candidate arm port** | **`19091`** |
| **reference arm port** | **`19092`** |
| **isolated dask scheduler port** | **`19099`** |
| **`WOA23_EXPECT_UID`** | `994` |

**All three ports are first-use** — zero ledger rows, **zero mentions anywhere in the
tree**, and outside `18241–18999`. `19081` was considered and rejected: it appears as a
coincidental substring inside a `uv.lock` URL. **Deliberately NOT pre-recorded in the
ledger.**

**Nothing from `c1n`, `c1m`, `c1k`, `c1j`, `c1i`, `c1h`, `c1f`, `c2g`, `s2pB` or any
`pm2*` run is reused.** Retired, all **RETIRED-NEVER-BOUND**: `c1h` `18301`/`18302`/`18949`,
`c1i` `18321`/`18322`/`18969`, `c1j` `18341`/`18342`/`18979`, `c1k` `18361`/`18362`/`18989`,
`c1m` `19051`/`19052`/`19059`, `c1n` `19071`/`19072`/`19079`.

## 4. The command

```
ssh -o BatchMode=yes -o RequestTTY=no -i ~/.ssh/id_ed25519_odb woa23c1ro@192.168.2.24
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c1p \
  WOA23_S2_C1_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_controlled.sh --c1 \
    --workdir        /home/woa23c1ro/woa23-c1p-work \
    --prod-dir       /home/odbadmin/python/woa23 \
    --store          /home/odbadmin/python/woa23/data \
    --prod-python    /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --python-binary  /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone  /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --prod-pids      "<discovered fresh in this run's preflight>" \
    --candidate-port 19091 \
    --reference-port 19092 \
    --scheduler-port 19099 \
    --label          c1p
```

**`--prod-pids` is filled from the pre-flight's own discovery, in the same session, and is
reported verbatim.** It is not written into this document, precisely because a pid copied
from a document is the thing the mechanism refuses.

**No privilege escalation**, and **never launched from `odbadmin`**.

## 5. Execution sequence — each step a stop

1. **connection identity** — `uid=994 gid=993`, not `odbadmin`;
2. **store identity, captured BEFORE any decision** — including the metadata fingerprint,
   which is not a content baseline;
3. **complete store read-only scan as uid 994** — nothing writable, no symlink escaping,
   **no write verb on any path**;
4. **subject** — archive, file count, file-list and every hash in §2.1 re-derived;
5. **ports** — `19091`, `19092`, `19099` absent from the live ledger and unbound by `ss`;
6. **identity absence** — staging, workdir, TMPDIR must not exist;
7. **production baseline** — boot id, listeners on 8050/8786/8787, `conf/` digests;
8. **production pid discovery** — `/proc/*/cmdline` scanned, `pm2G`'s pids excluded, the
   set cross-checked against the baseline. **Ambiguity is a stop, not a guess;**
9. **pm2G recorded untouched** — `18265` bound, its pids running;
10. **required tools** — `uv` resolved, path/version/SHA-256 recorded, before any staging
    or venv preparation;
11. **the shared environment** — interpreter, then `uv sync` with the explicit `PROD_PY`;
12. **production identity** — the port proven listening by `ss -ltn`, and **every supplied
    pid validated against `/proc`**;
13. **arms started**, then **every tracked process asserted uid 994** — masters and every
    worker, all four uids. Zero checked is a failure;
14. **C1 contract validation** — canonical values and column sequence, the candidate's
    canonical column-order contract, the `(time_period, depth, lat, lon)` row-order
    contract, JSON/CSV fields, values, row order and status, with the pre-defined
    reconstruction rules;
15. **cleanup** — the runner's authorised C1 path only; ports confirmed free afterwards;
16. **production re-read** — including **re-validating the same pids and starttimes** — and
    **store identity re-captured** and compared against step 2.

## 6. Scope, forbidden, failure handling

**Scope: C1 contract validation only.** No C2, latency, warm-up, noise pilot, startup,
deployment or PM2 validation.

**Forbidden:** privilege escalation of any kind, and launching from `odbadmin`; granting
`CAP_NET_ADMIN`, sudo, extra groups or any new privilege; any write, `chmod`, `chown`,
delete or rename under the production store; any modification of the store, ACLs, runtime
files, the `.lock`, permissions, any account, or production's `uv`; granting traverse into
`/home/odbadmin/.local`; any HTTP request to production (**8050 / 8786 / 8787 stay at
zero**); any change to production PM2, processes, listeners or `conf/`; **touching `pm2G`**
— not port `18265`, its PM2 entry or retained state; re-running `c1h`, `c1i`, `c1j`, `c1k`,
`c1m` or `c1n`, whose evidence is retained; back-filling `c1f`, `c2g`, `s2pB`, `pm2G` or
any earlier C-run; **SIGKILL**; **self-rerun**; cleanup beyond the authorised C1 path.

**On any failing step: STOP, retain, report, wait.** A survivor after cleanup is
**`CLEANUP_FAIL`** — left alive, no SIGKILL, not repeated. If reconstruction or any required
verification does not complete, the classification is **`INCOMPLETE_VALIDATION`** and **no
PASS is reported**. Never reportable as a `5.2A raw byte-exact PASS`.

## 7. What a PASS will and will not mean

**Will:** the `015` candidate satisfies the C1 contract gates against the **real production
store**, read through an enforced read-only ACL by an account that cannot write it, with
every arm master and worker proven to be uid 994 and production's identity proven unchanged
across the run.

**Will not:** **not a `5.2A raw byte-exact PASS`; not a staging PASS; not a production
cutover PASS; not a latency result; not a deployment result; not a claim of runtime
isolation.** **B1–B5 remain open. B7 remains open. `pm2G` remains NOT A PASS. C2 remains
blocked.**

## 8. Submission

- **Subject: `2d6f812e22f355ed4f5abad21106596f6274623d`.** This document and any later
  commit are **protocol references** and must never be back-filled as the execution subject.
- **Awaiting explicit authorisation. C1 will not be run until it is given.**
