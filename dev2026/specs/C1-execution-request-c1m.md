# C1 `c1m` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. C1 has not been run.** No VM24 contact
during this fix. **This document is not an authorisation.**

Supersedes [C1-execution-request-c1k.md](C1-execution-request-c1k.md). `c1k` is **CONSUMED**
— [C1-result-c1k.md](C1-result-c1k.md), `INCOMPLETE_VALIDATION` — and neither its label, its
ports nor its staging tree are reused.

---

## 1. What changed since `c1k`

`c1k` passed a completely clean pre-flight and then aborted in *"preparing the shared
environment"* because `PROD_PY` was still `$HOME`-derived with no flag. **Option A is
implemented: `--prod-python` is now an explicit flag, mandatory under `WOA23_EXPECT_UID`.**

**The two interpreters are kept separate, because they are two different questions:**

| flag | variable | used for |
|---|---|---|
| `--python-binary` | `PY_BINARY` | the interpreter the **arms** run — what is under test |
| **`--prod-python`** | **`PROD_PY`** | production's interpreter: the existence/version check, and **`uv sync --python`** for the **harness** venv |

They name the same file today. That is exactly why conflating them is tempting and wrong:
a run wanting them different could not say so, and the harness environment is not the
environment under test.

### 1.1 The audit found a second `$HOME` path, and it was the worse one

**`PROD_SITE`** names production's **live site-packages** and exists only so the arms can be
**forbidden** to reach it (`--forbid "$PROD_SITE"`). Left deriving from `$HOME`, a run as
`woa23c1ro` would have forbidden `/home/woa23c1ro/.pyenv/.../site-packages` — **a path that
does not exist** — so the guard would have *passed while protecting nothing*.

**A weakened check that reports success is worse than an absent one.** `PROD_SITE` now
derives from `--prod-python`'s prefix, with an optional `--prod-site` override, and
**refuses rather than guesses** if the interpreter path has no `/bin/` component.

**No `$HOME`-derived interpreter path survives the expected-UID execution path.** The only
remaining `$HOME/.pyenv` text is the usage message describing the ordinary-mode default;
the test excludes it deliberately, because deleting that sentence would make the flag
harder to use without making any run safer.

### 1.2 On testing the shared-environment stage — what I could not do, and why

**You asked for tests that reach the shared-environment preparation stage. I could not do
that, and I did not fake it.**

`run_controlled.sh` refuses to run on any host but `odb24` (`EXPECT_HOST`, a hard `exit 4`
with no override hook). **That stage is unreachable from a developer machine through the
CLI** — which is precisely how `c1k`'s defect survived a 132-run suite: it sat past every
check a laptop could execute.

**I did not add a test bypass.** Weakening a real host guard in order to exercise another
guard is a bad trade, and it is the kind of change that outlives the reason for it.

**Instead the decisions are now pure functions**, defined before the script has any side
effect, and the suite sources the runner with `WOA23_RUNNER_LIB_ONLY=1` and calls them with
real values:

| function | tested with |
|---|---|
| `derive_prod_site` | a real interpreter path → correct site-packages; a path with no `/bin/` → refused (rc 2) |
| `prod_paths_mandatory_problem` | all-three-missing, `--prod-python`-missing, all-present, and ordinary mode |
| `prod_python_absolute_problem` | relative-and-explicit → refused; absolute → accepted; relative-but-default → ignored |

Plus structural proof of propagation: `PROD_PY` is what `uv sync --locked --python`
receives, `PY_BINARY` is what the arms get, and the two are set from different flags.

**This exercises the resolution logic itself rather than an early CLI rejection.** If you
want genuine end-to-end coverage of that stage, it needs a hook in the host guard — a
change I would want authorised explicitly, and my recommendation is against it.

## 2. Execution subject

| item | value |
|---|---|
| **commit** | `77bf4fa15e5e28cc17f8efc0d104f045d799146c` |
| **archive SHA-256** | `bf237e75a50f8bde3b0b2f8a7ec3f4bad230e2ae32206ad8e6ac10b9cc443943` |
| **file count** | `181` |
| **file-list SHA-256** | `d48438498ce6a45695d69d9f0acfc5b334536f4ceb1759b518fa382f7bebd936` |
| **verifier** | `scripts/verify_clean_archive.sh 77bf4fa15e5e28cc17f8efc0d104f045d799146c` → **16/16** |

**Supersedes `832e767`** (176 files). **176 → 181**: three modified
(`run_controlled.sh`, `test_c1_readonly_account.sh`, `ports_used.tsv`) and five added — the
`c1j` request and FINAL request, the `c1k` request, and the `c1j` and `c1k` results, all
documentation.

### 2.1 Full source hashes at the subject

| SHA-256 | file |
|---|---|
| `9cd3510fd4db7babb5a50f36bb95fca5afb648a011d1997a2fec1c8eb51cf905` | `scripts/run_controlled.sh` |
| `15467af7fbd16b64576609ace2ce33ccf522089e7b81bf4da6376c6054b8b019` | `scripts/lib_procs.sh` |
| `16f641621e5f058ee68f07e6be26f3bca9417dc39c0380dc213320e8223c3d75` | `scripts/store_readonly_preflight.sh` |
| `07eaf62a1708c61ba8bea32caed20dc134f5414b3fd332f4b53065a8e030df50` | `scripts/test_c1_readonly_account.sh` |
| `611580ffd83471c194d856eb0f78d2d7203505a5a0816139cdf9297f1baff47b` | `scripts/ports_used.tsv` |
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

**Only `scripts/run_controlled.sh`, `scripts/test_c1_readonly_account.sh` and
`scripts/ports_used.tsv` differ from `832e767`.** **`api/` and `bench/` remain
byte-identical to `ad3f428`** — the candidate under test has not moved since the 015 work.

### 2.2 Offline evidence

| | |
|---|---|
| batches | **three, strictly serial** (`WOA23_SUITE_REPEAT=3`, `concurrency: serial`) |
| total runs | **132** (44 suites × 3) |
| **non-zero exits** | **0** |
| evidence root | `/var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T//woa23-suites-nKv4kX/` (retained) |
| `test_c1_readonly_account.sh` | **80 assertions** (was 58) |
| `test_cli.sh` | **305 assertions** |
| `fail_lines` | **3** — the collector matching a section *heading* in `test_symmetric_warmup.py`, which exits `0`. A false positive in the collector, not a failing assertion |
| strays | **none created** — 16 pre-existing `arm.py` remain, unchanged |

**Retained evidence roots from the preceding runs**, none cleaned:
`woa23-suites-9Xr3Ay` (the `832e767` batches), and on VM24
`/home/woa23c1ro/woa23-c1k/` with `/home/woa23c1ro/c1k-archive.tar`.

## 3. Execution identity — fresh label, first-use ports

| | value |
|---|---|
| **grant** | `WOA23_S2_C1_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993**, private group only |
| **connection** | direct SSH, `mac-claude` Ed25519 key, `BatchMode=yes`, `RequestTTY=no` |
| **label** | **`c1m`** |
| **staging / export root** | `/home/woa23c1ro/woa23-c1m/` |
| **workdir** | `/home/woa23c1ro/woa23-c1m-work/` |
| **HOME** | `/home/woa23c1ro` |
| **TMPDIR** | `/home/woa23c1ro/tmp-c1m/` |
| **candidate arm port** | **`19051`** |
| **reference arm port** | **`19052`** |
| **isolated dask scheduler port** | **`19059`** |
| **`WOA23_EXPECT_UID`** | `994` |

**All three ports are first-use** — zero ledger rows and **zero mentions anywhere else in
the tree**. They sit **outside** `18241–18999`, the range `test_staging_launcher.sh` scans;
`18381`/`18382`/`18999` were considered and rejected for falling inside it. **Deliberately
NOT pre-recorded in the ledger** — the runner refuses a port the ledger names, reading it
from the export of the subject it runs.

**Nothing from `c1k`, `c1j`, `c1i`, `c1h`, `c1f`, `c2g`, `s2pB` or any `pm2*` run is
reused**, including `c1k`'s staging tree. Retired, all **RETIRED-NEVER-BOUND**: `c1h`
`18301`/`18302`/`18949`, `c1i` `18321`/`18322`/`18969`, `c1j` `18341`/`18342`/`18979`,
`c1k` `18361`/`18362`/`18989`.

## 4. The command

```
ssh -o BatchMode=yes -o RequestTTY=no -i ~/.ssh/id_ed25519_odb woa23c1ro@192.168.2.24
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c1m \
  WOA23_S2_C1_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_controlled.sh --c1 \
    --workdir        /home/woa23c1ro/woa23-c1m-work \
    --prod-dir       /home/odbadmin/python/woa23 \
    --store          /home/odbadmin/python/woa23/data \
    --prod-python    /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --python-binary  /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone  /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --candidate-port 19051 \
    --reference-port 19052 \
    --scheduler-port 19059 \
    --label          c1m
```

**`--prod-dir`, `--store` and `--prod-python` are all mandatory** under
`WOA23_EXPECT_UID`; the runner refuses any `$HOME`-derived fallback and names all three.
**No privilege escalation** — not `sudo`, `sshpass`, `su` or `setpriv, and **never launched
from `odbadmin`**. The orchestration *is* the SSH session, so every arm and worker inherits
uid 994 **by descent, not by a drop**.

## 5. Execution sequence — each step a stop

Unchanged from the `c1k` request, which executed steps 1–8 cleanly:

1. **connection identity** — `uid=994 gid=993`, private group only, not `odbadmin`;
2. **store identity, captured BEFORE any decision** — including the **metadata
   fingerprint**, which is not a content baseline and is never reported as one;
3. **complete store read-only scan as uid 994** — every directory traversable and readable,
   every file readable, **nothing writable**, no symlink escaping. **No write verb on any
   path, including every failure path;**
4. **subject** — archive, file count, file-list digest and every hash in §2.1 re-derived on
   VM24;
5. **ports** — `19051`, `19052`, `19059` absent from the live ledger read from the subject's
   own export, **and** unbound by read-only `ss`;
6. **identity absence** — staging, workdir and TMPDIR must not exist;
7. **production baseline** — boot id, PIDs with starttimes, listeners, PM2 list, `conf/`
   digests;
8. **pm2G recorded untouched** — `18265` still bound, 1456369 and its workers running;
9. **arms started**, then **every tracked process asserted uid 994** — masters and every
   worker, all four uids. **Zero checked is a failure;**
10. **C1 contract validation** — canonical values and column sequence, the candidate's
    canonical **column-order** contract, the `(time_period, depth, lat, lon)` **row-order**
    contract, JSON/CSV fields, values, row order and status, with the pre-defined
    reconstruction rules;
11. **cleanup** — the runner's authorised C1 path only; ports confirmed free and
    connection-refused afterwards;
12. **production re-read** and **store identity re-captured** and compared against step 2.

## 6. Scope, forbidden, failure handling

**Scope: C1 contract validation only.** No C2, latency, warm-up, noise pilot, startup,
deployment or PM2 validation. No conclusion about spec 016's venv runtime behaviour.

**Forbidden:** privilege escalation of any kind, and launching from `odbadmin`; any write,
`chmod`, `chown`, delete or rename under the production store; any modification of the
store, ACLs, runtime files, the `.lock`, permissions or any account; any HTTP request to
production (**8050 / 8786 / 8787 stay at zero**); any change to production PM2, processes,
listeners or `conf/`; **touching `pm2G`** — not port `18265`, its PM2 entry or retained
state; re-running `c1h`, `c1i`, `c1j` or `c1k`, whose evidence is retained; back-filling
`c1f`, `c2g`, `s2pB`, `pm2G` or any earlier C-run; **SIGKILL**; **self-rerun**; cleanup
beyond the authorised C1 path.

**On any failing step: STOP, retain, report, wait.** A survivor after cleanup is
**`CLEANUP_FAIL`** — left alive, no SIGKILL, not repeated. If reconstruction or any required
verification does not complete, the classification is **`INCOMPLETE_VALIDATION`** and **no
PASS is reported**. Never reportable as a `5.2A raw byte-exact PASS`.

## 7. What a PASS will and will not mean

**Will:** the `015` candidate satisfies the C1 contract gates against the **real production
store**, read through an enforced read-only ACL by an account that cannot write it, with
every arm master and worker proven to be uid 994.

**Will not:** **not a `5.2A raw byte-exact PASS`; not a staging PASS; not a production
cutover PASS; not a latency result; not a deployment result; not a claim of runtime
isolation.** **B1–B5 remain open. B7 remains open. `pm2G` remains NOT A PASS. C2 remains
blocked** and needs its own authorisation.

## 8. Submission

- **Subject: `77bf4fa15e5e28cc17f8efc0d104f045d799146c`.** This document and any later
  commit are **protocol references** and must never be back-filled as the execution subject.
- **Awaiting explicit authorisation. C1 will not be run until it is given.**
