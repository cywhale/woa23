# C1 `c1n` — execution request

**Status: SUBMITTED FOR AUTHORISATION. NOT GRANTED. C1 has not been run.** No VM24 contact
during this fix. **This document is not an authorisation.**

Supersedes [C1-execution-request-c1m.md](C1-execution-request-c1m.md). `c1m` is **CONSUMED**
— [C1-result-c1m.md](C1-result-c1m.md), `INCOMPLETE_VALIDATION` — and neither its label, its
ports nor its staging tree are reused.

---

## 1. What changed since `c1m`

`c1m` passed a complete pre-flight — including everything `c1k` died on — and then aborted
at `uv sync` because **`uv` was unreachable by uid 994**. Two things follow, one host-side
and one in code.

### 1.1 Host-side, already done by the PI: option A

`uv` is now provided at **`/home/woa23c1ro/.local/bin/uv`** — the run account's own copy.
**`/home/odbadmin/.local` stays inaccessible** and production's `uv` is unmodified. The
non-interactive SSH check confirms uid 994, `command -v uv` resolving to that path, and
`uv --version` succeeding.

**This campaign did not install it, change any ACL, `chmod` anything, or alter any
account.**

### 1.2 In code: tools are verified before any preparation

**`require_tool` runs before the interpreter probe, before `uv sync`, and before any
staging directory or venv exists.** It records `uv`'s resolved absolute path, version and
SHA-256, and on failure exits saying **nothing has been created**.

**And it never calls "unreachable" "absent".** That distinction is the point:

> `c1m`'s first diagnosis said `uv` was **absent**. It is present at
> `/home/odbadmin/.local/bin/uv`, mode 755 — merely behind a directory uid 994 cannot
> traverse. `[ -e ]` is false for a file behind a directory you cannot enter, and I
> reported unreachability as absence. **"Not visible to me" and "not on the host" send a
> reader to entirely different places.**

`path_state` walks a path's ancestors outermost-first and names the **first thing that
actually stops us**, in five distinguishable states:

| state | meaning |
|---|---|
| `present` | exists and this account can reach it |
| `notexec` | reachable and present, but not executable |
| **`absent:<path>`** | an ancestor, or the target, **genuinely does not exist** |
| **`unreachable:<dir>`** | an ancestor exists but **cannot be traversed** — the file may well be there |
| `notabsolute` | not an absolute path |

On failure `require_tool` classifies every known candidate and prints, for a blocked one,
`UNREACHABLE -- cannot traverse <dir>` followed by `(this is NOT the same as absent: the
file may well be there)`.

### 1.3 Two of my own mistakes, caught by the suite rather than by me

- I wrote a **`case` inside a command substitution** — which this project already forbids
  and checks for in `test_production_launcher.sh`. The rule caught me.
- I restored the blocked test-fixture directory's permissions **before** the `require_tool`
  cases instead of after, which quietly turned the UNREACHABLE case into a reachable one
  and made **two assertions pass for the wrong reason**. Moved after, with a comment.

Both are recorded in comments where they happened, not silently rewritten.

## 2. Execution subject

| item | value |
|---|---|
| **commit** | `7c29585673125dbc85c89c43783eb6c497eba211` |
| **archive SHA-256** | `07a5cd302feaa83b01bac324e2f58a272413065210b53e5e887e776d3fea90e2` |
| **file count** | `183` |
| **file-list SHA-256** | `57bc01678fa733b381f722fec085ead95f2300e3b0bc9bd794152f2cf18b91c0` |
| **verifier** | `scripts/verify_clean_archive.sh 7c29585673125dbc85c89c43783eb6c497eba211` → **16/16** |

**Supersedes `77bf4fa`** (181 files). **181 → 183**: three modified
(`run_controlled.sh`, `test_c1_readonly_account.sh`, `ports_used.tsv`) and two added — the
`c1m` request and the `c1m` result, both documentation.

### 2.1 Full source hashes at the subject

| SHA-256 | file |
|---|---|
| `8ead2c10921664015e70495b7aa012659c45a51f419c49cd7f8f235ce761a780` | `scripts/run_controlled.sh` |
| `15467af7fbd16b64576609ace2ce33ccf522089e7b81bf4da6376c6054b8b019` | `scripts/lib_procs.sh` |
| `16f641621e5f058ee68f07e6be26f3bca9417dc39c0380dc213320e8223c3d75` | `scripts/store_readonly_preflight.sh` |
| `e4c986aea8d0f6f962a8f47755d5b5998e0511923399b5bb6dd983fedc1a4245` | `scripts/test_c1_readonly_account.sh` |
| `5ad868c916a3121d4607ee4cc44028784502de8fba5c80dd515877699d2ca840` | `scripts/ports_used.tsv` |
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

**`api/` and `bench/` remain byte-identical to `ad3f428`.** **The candidate under test has
not moved since the 015 work** — only the harness has, across `c1k`, `c1m` and now `c1n`.

### 2.2 Offline evidence

| | |
|---|---|
| batches | **three, strictly serial** |
| total runs | **132** (44 suites × 3) |
| **non-zero exits** | **0** |
| evidence root | `/var/folders/z6/3v58whgn56q6jnvrjdmshlz00000gn/T//woa23-suites-uUJ4FC/` (retained) |
| `test_c1_readonly_account.sh` | **106 assertions** (was 80) |
| `test_production_launcher.sh` | **111 assertions** |
| `test_cli.sh` | **305 assertions** |
| `fail_lines` | **3** — the collector matching a section *heading* in `test_symmetric_warmup.py`, which exits `0` |
| strays | **none created** — 16 pre-existing remain |

**Retained roots, none cleaned:** `woa23-suites-nKv4kX`, `woa23-suites-9Xr3Ay`, and on VM24
`/home/woa23c1ro/woa23-c1m/`, `/home/woa23c1ro/woa23-c1k/` with both archives.

## 3. Execution identity — fresh label, first-use ports

| | value |
|---|---|
| **grant** | `WOA23_S2_C1_GRANTED=yes` |
| **run-as account** | `woa23c1ro`, **uid 994, gid 993**, private group only |
| **connection** | direct SSH, `mac-claude` Ed25519 key, `BatchMode=yes`, `RequestTTY=no` |
| **label** | **`c1n`** |
| **staging / export root** | `/home/woa23c1ro/woa23-c1n/` |
| **workdir** | `/home/woa23c1ro/woa23-c1n-work/` |
| **HOME** | `/home/woa23c1ro` |
| **TMPDIR** | `/home/woa23c1ro/tmp-c1n/` |
| **candidate arm port** | **`19071`** |
| **reference arm port** | **`19072`** |
| **isolated dask scheduler port** | **`19079`** |
| **`WOA23_EXPECT_UID`** | `994` |

**All three ports are first-use** — zero ledger rows, **zero mentions anywhere else in the
tree**, and **outside `18241–18999`** (the range `test_staging_launcher.sh` scans).
`19061` was considered and rejected: it appears as a coincidental substring inside a
`uv.lock` URL. **Deliberately NOT pre-recorded in the ledger.**

**Nothing from `c1m`, `c1k`, `c1j`, `c1i`, `c1h`, `c1f`, `c2g`, `s2pB` or any `pm2*` run is
reused**, including their staging trees. Retired, all **RETIRED-NEVER-BOUND**: `c1h`
`18301`/`18302`/`18949`, `c1i` `18321`/`18322`/`18969`, `c1j` `18341`/`18342`/`18979`,
`c1k` `18361`/`18362`/`18989`, `c1m` `19051`/`19052`/`19059`.

## 4. The command

```
ssh -o BatchMode=yes -o RequestTTY=no -i ~/.ssh/id_ed25519_odb woa23c1ro@192.168.2.24
  HOME=/home/woa23c1ro \
  TMPDIR=/home/woa23c1ro/tmp-c1n \
  WOA23_S2_C1_GRANTED=yes \
  WOA23_EXPECT_UID=994 \
  ./scripts/run_controlled.sh --c1 \
    --workdir        /home/woa23c1ro/woa23-c1n-work \
    --prod-dir       /home/odbadmin/python/woa23 \
    --store          /home/odbadmin/python/woa23/data \
    --prod-python    /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --python-binary  /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone  /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --candidate-port 19071 \
    --reference-port 19072 \
    --scheduler-port 19079 \
    --label          c1n
```

**No new flag is needed for `uv`** — the runner's existing `PATH` line finds the run
account's own copy, which is exactly why option A required no code change to the command.
**No privilege escalation**, and **never launched from `odbadmin`**: the orchestration *is*
the SSH session, so every arm and worker inherits uid 994 **by descent**.

## 5. Execution sequence — each step a stop

1. **connection identity** — `uid=994 gid=993`, private group only, not `odbadmin`;
2. **store identity, captured BEFORE any decision** — including the **metadata
   fingerprint**, which is not a content baseline and is never reported as one;
3. **complete store read-only scan as uid 994** — traversable, readable, **nothing
   writable**, no symlink escaping. **No write verb on any path, including failure paths;**
4. **subject** — archive, file count, file-list digest and every hash in §2.1 re-derived;
5. **ports** — `19071`, `19072`, `19079` absent from the live ledger read from the
   subject's own export, **and** unbound by read-only `ss`;
6. **identity absence** — staging, workdir, TMPDIR must not exist;
7. **production baseline** — boot id, PIDs with starttimes, listeners, PM2 list, `conf/`;
8. **pm2G recorded untouched** — `18265` bound, 1456369 and workers running;
9. **required tools** — **`uv` resolved, with its absolute path, version and SHA-256
   recorded, before any staging or venv preparation.** A failure here creates nothing;
10. **the shared environment** — interpreter existence and 3.11.4 version, then `uv sync`
    with the explicit `PROD_PY`;
11. **arms started**, then **every tracked process asserted uid 994** — masters and every
    worker, all four uids. **Zero checked is a failure;**
12. **C1 contract validation** — canonical values and column sequence, the candidate's
    canonical **column-order** contract, the `(time_period, depth, lat, lon)` **row-order**
    contract, JSON/CSV fields, values, row order and status, with the pre-defined
    reconstruction rules;
13. **cleanup** — the runner's authorised C1 path only; ports confirmed free and
    connection-refused afterwards;
14. **production re-read** and **store identity re-captured**, compared against step 2.

## 6. Scope, forbidden, failure handling

**Scope: C1 contract validation only.** No C2, latency, warm-up, noise pilot, startup,
deployment or PM2 validation. No conclusion about spec 016's venv runtime behaviour.

**Forbidden:** privilege escalation of any kind, and launching from `odbadmin`; any write,
`chmod`, `chown`, delete or rename under the production store; any modification of the
store, ACLs, runtime files, the `.lock`, permissions, any account, or production's `uv`;
**granting traverse into `/home/odbadmin/.local`**; any HTTP request to production
(**8050 / 8786 / 8787 stay at zero**); any change to production PM2, processes, listeners
or `conf/`; **touching `pm2G`** — not port `18265`, its PM2 entry or retained state;
re-running `c1h`, `c1i`, `c1j`, `c1k` or `c1m`, whose evidence is retained; back-filling
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

- **Subject: `7c29585673125dbc85c89c43783eb6c497eba211`.** This document and any later
  commit are **protocol references** and must never be back-filled as the execution subject.
- **Awaiting explicit authorisation. C1 will not be run until it is given.**
