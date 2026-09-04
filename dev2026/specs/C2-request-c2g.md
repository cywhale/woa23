# C2 (`c2g`) — authorisation request: semantic correctness and the row-order contract at production's worker count

**Status: GRANTED and EXECUTED once, 2026-08-19 — `c2g`, C2 OUTCOME PASS.**

> 5.2B semantic gate PASS in all three cycles; seed diversity OBSERVED (3 distinct / 3);
> **row-order contract PASS — 132 of 192 candidate responses carried a row order, 0
> violated it, and the candidate's order was identical across all three cycles**;
> shutdown budget CONSISTENT.
>
> **The reference varied on `C16` and `C16-csv` — recorded, not a failure.** §6.5 said
> in advance that a difference between the arms was expected here, and it appeared.

The full record is in `specs/002-production-correctness-deploy-hardening.md` under
`c2g`. This document stands as the request that was authorised; it is not
re-executable, and a further run needs a **new execution identity**.

**Asks for:** three independent start/stop cycles. Each cycle is four services — **six
OS processes** at one worker per arm, more if production runs more — all loopback.

**Why now:** C1 (`c1f`) passed under a **pinned** seed, and under that seed the
reference already emitted contract order, so **no raw row-order difference occurred and
the reconstruction path was never exercised against real data**. The variation spec 003
documented appears under an **unpinned** seed. **C2 is the environment where a
difference between the arms is expected to appear**, and it is the only place the
contract's stability can be tested against three independent hash seeds.

---

## 0. The three roles, kept apart

### 0.1 Execution subject — the only tree that runs

```
1439194a091a5c00ac9414ddd46a3898e51dad51
```

**Fixed, and the same tree C1 validated.** It does not change if further commits are
made before authorisation, and the HEAD at authorisation time is **not** the subject.

### 0.2 Protocol / documentation references — NOT inside the execution archive

| commit | what it is |
|---|---|
| `a7e3a408a2a92119b4a2cc1db3c10a75262865a7` | spec 008 rev 4 — 5.2C in §7c, the C2 gate in §7c.10 |
| the commits carrying this request and the `c1f` record | cited, not shipped |

All were committed **after** the subject and are **absent from its archive**. No report
of this run may say or imply otherwise. `api/`, `bench/` and `scripts/` are
byte-identical between the subject and every later commit, so the behaviour executed is
the behaviour those documents describe.

### 0.3 Evidence that may NOT be back-filled

| label | tree it describes | standing |
|---|---|---|
| `c1e` | `919095e8f3ae7af0dc8808c6015df255610f5d8f` | not evidence for the subject |
| `c2f` | `919095e8f3ae7af0dc8808c6015df255610f5d8f` | not evidence for the subject |
| `s2pB` | `c3b9398b4991539c7701feb0bb3bb5d6ddd564d7` | not evidence for the subject; **no** performance claim rests on it |
| **`c1f`** | `1439194a091a5c00ac9414ddd46a3898e51dad51` | **the same tree**, but a **different question**: pinned seed, one worker, contract bytes. It does **not** answer what C2 asks, and C2's verdict may not lean on it |

## 1. The execution tree — complete digests, none abbreviated

| item | value |
|---|---|
| commit SHA | `1439194a091a5c00ac9414ddd46a3898e51dad51` |
| archive SHA-256 | `e7a4abad2b04896fbe3b76e91f4d513f4814b226b664b2e8112762a2b09902bb` |
| archive file count | `120` |
| file-list SHA-256 | `18c164b699e579cfee12cfe97ede6139503f370359d6945ded47b3a7ff5a8b6a` |

`api/` source SHA-256, read from the commit itself:

| file | SHA-256 |
|---|---|
| `api/__init__.py` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `api/app.py` | `d0d8c781f6d2170b87ecc43a9b2e0ebedb51d14186bb7a0bf94668485340489a` |
| `api/config.py` | `b806641dc7478acaca375380b8e0f8575c1fa48f093362f7f917921a3adc94ca` |
| `api/query.py` | `8e980e5b60a004902e66e6cb86ed2352a5ec641a6ad4cd3173a5a2efc56cebce` |
| `api/store_paths.py` | `00cb80c2b1c4ef74f984f42026dcdc5736bfb841e34471bd882c3fd42e35b928` |

Archive verification (`scripts/verify_clean_archive.sh 1439194`), 2026-08-19:

```
   commit           1439194a091a5c00ac9414ddd46a3898e51dad51
   archive_sha256   e7a4abad2b04896fbe3b76e91f4d513f4814b226b664b2e8112762a2b09902bb
   file_count       120
   file_list_sha256 18c164b699e579cfee12cfe97ede6139503f370359d6945ded47b3a7ff5a8b6a

all passed (16 assertions)
```

**A fresh staging is required.** `~/woa23-c1f/` is `c1f`'s evidence and is **not
reused**, even though the tree is the same. All four values and the five source hashes
are **re-derived on VM24 after transfer**, and the staged tree is compared **file by
file** against the authorised commit. Any mismatch stops the run before a service
starts.

## 2. Execution identity — every element first-use

| | value |
|---|---|
| label prefix | `c2g` — cycles `c2g_cycle1`, `c2g_cycle2`, `c2g_cycle3` |
| staging | `~/woa23-c2g/` |
| workdir base | `~/woa23-c2g-work/` — **created by the runner; never pre-created** |
| candidate port | `18211` |
| reference port | `18212` |
| scheduler port | `18939` |

`c2b`–`c2f` are the earlier C2 labels; `c2g` is unused. `18211`, `18212` and `18939` are
absent from `scripts/ports_used.tsv` — **first use** for all three. One port triple
serves all three cycles, each cycle binding and releasing them in turn, as `c2f` did.
No earlier identity is reused, reopened, cleaned or overwritten, **including `c1f`**.

**Do not pre-create the workdir base.** `s2pA` was lost to a `mkdir -p` on a workdir
during staging. Stage into `~/woa23-c2g/` **only**.

## 3. Execution parameters

| | value | note |
|---|---|---|
| grant variable | **`WOA23_S2_C2_GRANTED=yes`** | the runner refuses `WOA23_S2_C1_GRANTED` standing in for it, and refuses a C1 run with the C2 grant set |
| cycles | **3**, fixed | not a flag: "three independent cycles" is the authorised design, and a `--cycles` flag is how three becomes five the first time three is inconvenient |
| contract variant | **5.2B semantic** | **unchanged by spec 008** |
| seed policy | **`both-unpinned`** | stated explicitly, not inherited: 5.2B's default is "pinned candidate against live production", and C2 is a third arrangement — both arms ours, both unpinned |
| `PYTHONHASHSEED` | **unset on both arms** | this is the whole point of C2 |
| workers per arm | **production's measured count** | `EXPECT_ARM_WORKERS` comes from `/proc/<master>/cmdline` **at run time**; C2 is the only mode that does this |
| `--expected-workers` | **optional assertion only** | it asserts against the measurement and **sets nothing**; if given it must equal the measured value or the run stops |
| readiness | ≤ **30** per arm per cycle | |
| store probe | **2** per arm per cycle | |
| contract gate | **64** per arm per cycle | |
| **ceiling** | **≤ 96 per arm per cycle; ≤ 288 per arm and ≤ 576 total across three cycles** | |
| production 8050 / 8786 / 8787 | **0 requests** | read from `/proc` and `ss` only |

### 3.1 Clone and interpreter

| | value |
|---|---|
| package clone | `/home/odbadmin/woa23-s2-package-clone/dist` |
| clone manifest | `/home/odbadmin/woa23-s2-package-clone/clone.manifest` |
| manifest SHA-256, **expected** | `f3b66c493b40ed399a08add5742dce2dd0ad5fb51cf76f6fe083128df0e771f4` |
| interpreter | `/home/odbadmin/.pyenv/versions/py311/bin/python3.11` |

**The manifest digest is an EXPECTATION, not evidence.** It must be re-derived in this
run's own preflight and compared; a mismatch is a stop. **No value from `c1f`, `s2pB` or
any earlier run substitutes for this run's preflight.**

## 4. The proposed invocation

Staging creates `~/woa23-c2g/` **only**.

```
cd ~/woa23-c2g/dev2026 && WOA23_S2_C2_GRANTED=yes \
  ./scripts/run_c2_cycles.sh \
    --python-binary /home/odbadmin/.pyenv/versions/py311/bin/python3.11 \
    --package-clone /home/odbadmin/woa23-s2-package-clone/dist \
    --clone-manifest /home/odbadmin/woa23-s2-package-clone/clone.manifest \
    --workdir-base ~/woa23-c2g-work \
    --candidate-port 18211 --reference-port 18212 --scheduler-port 18939 \
    --label-prefix c2g
```

SSH uses `ssh vm24` with the configured key and `BatchMode=yes`. **`sshpass` is
forbidden.**

## 5. Fresh preflight — read-only, this run's own values only

Re-read immediately before launch. **Nothing from `c1f`, `s2pB`, `clnB` or any earlier
run is carried forward or quoted in its place**, including values read only hours
earlier.

1. `~/woa23-c2g/` and `~/woa23-c2g-work/` **absent**; labels `c2g_cycle1..3` have **0**
   artefacts;
2. ports `18211`, `18212`, `18939` **actually free** (`ss`) **and** first-use in the
   ledger — two questions, both asked;
3. **boot id**;
4. production **master PID, worker PIDs, their starttimes, and listeners**;
5. **production's actual worker count**, read from `/proc/<master>/cmdline` — and the
   arms are launched at **that** count (§6.1);
6. clone **permissions**, manifest **shape** and manifest **SHA-256** vs §3.1;
7. archive SHA-256, file count, file-list SHA-256 and the **five `api/` source hashes**,
   re-derived on VM24 vs §1;
8. the staged tree compared **file by file** against the authorised commit;
9. **before each subsequent cycle**: the previous cycle's cleanup confirmed, its ports
   free again, and production unchanged.

## 6. What C2 verifies

### 6.1 Production's worker count — measured, then used

C2 is the **only** mode that runs the arms at production's worker count, and it
**measures** that count rather than assuming it: `/proc/<master>/cmdline` at run time.
The measurement is recorded with its source, and the arms' actual worker count is
asserted afterwards against each arm's own `/proc/<pid>/cmdline`. **A mismatch between
the intended and the actual count stops the run.**

*(The last recorded observation was 2. That number is **not** authority for this run and
is not to be assumed — §5 item 5 requires it re-measured.)*

### 6.2 The semantic gate — 5.2B, unchanged

`compare_semantic` pairs rows by sorting **both** sides on the index key, compares
column **sets** and every value exactly. Row order is outside its verdict by
construction. **Spec 008 does not change this**, and the verdict is `PASS` only if
**all three cycles** pass.

### 6.3 The candidate-only row-order gate — spec 008 §7c.10

Computed from the responses each cycle **already fetches**: **no extra HTTP request and
no change to any budget.**

**Conformance.** Every candidate response **with data rows** — JSON and CSV alike —
ascends by numeric `(time_period, depth, lat, lon)`, with at most one row per key.

**Stability.** The candidate's `row_order_sha256` for a given case is **identical in all
three cycles**. Three independent starts with three different seeds are exactly the
circumstance under which the old order varied, so three-way agreement is what shows the
**sort**, and not the **seed**, decides the order.

**The reference is not held to the contract.** It does not implement it, and under an
unpinned seed its order is a property of its process. **Reference variation across
cycles is RECORDED and is NOT a failure** — it is the observable spec 003 is about, and
this run is expected to see it.

**`N/A`, neither pass nor failure:** error responses, empty results and non-row payloads
are recorded `applies: false`, `ok: null`, and counted apart. They may **not** be
reported as row-order failures, and they may not be counted as passes.

### 6.4 Failure classification — kept separate

| outcome | exit | when |
|---|---|---|
| `PASS` | 0 | all three cycles pass 5.2B; candidate conforms and is stable; shutdown budget consistent |
| `PASS_WITH_INSUFFICIENT_SEED_DIVERSITY` | 5 | as above, but three distinct seeds were not observed — **not a plain PASS**, not a candidate failure, and **not a reason to run a fourth cycle** |
| **`ROW_ORDER_CONTRACT_FAILURE`** | **6** | any candidate response violating the order, **or** the candidate ordering a case differently across cycles |
| `FAIL` | 1 | a failing 5.2B verdict, an unconfirmed shutdown budget, or an `INDETERMINATE` row-order record |

**`ROW_ORDER_CONTRACT_FAILURE` is never folded into semantic divergence.** "The arms
disagree semantically" and "the candidate does not implement the contract we decided"
have different causes and different fixes, and it **blocks a deployment claim on its
own**.

**`INDETERMINATE`** — a cycle whose contract artefact carries no per-response
conformance record — is **not a pass**: "we did not check" and "we checked and it held"
must not look alike.

### 6.5 What this run is expected to show, stated in advance

**A difference between the arms' row order is EXPECTED here**, unlike in `c1f`. If the
reference varies across cycles and the candidate does not, that is the contract working.
**Recording the expectation in advance is deliberate**, so a result matching it is not
mistaken for a result that confirms it by construction, and so a result *not* matching
it is visible as a surprise rather than absorbed.

## 7. Execution boundaries

- **no latency, warm-up, noise pilot or startup measurement** — the C2 cycle stops after
  the contract gate;
- **production 8050 / 8786 / 8787: 0 requests**, never contacted;
- **no deployment**, no PM2, no production modification, no `main`, no push;
- **no spec 006 Option B**, no other API behaviour change;
- **no self-rerun.** Three cycles is the authorised design; a fourth is not run for any
  reason, including an `INSUFFICIENT` seed observation;
- **no back-filling** from `c1e`, `c2f`, `s2pB` or `c1f`;
- **no modification of historical artefacts**, `c1f`'s and `s2pB`'s included;
- **cleanup fail-closed** on **every** cycle: no `SIGKILL`, no clearing of retained
  state, an unconfirmed identity is a FAIL with state kept, and the latch is released
  only by independent human review. Three cycles are three chances to strand a process,
  and cycle 1 of the 2026-08-10 run took one of them;
- **any preflight, identity, digest, gate or cleanup failure preserves all evidence and
  stops.** No rule is relaxed to obtain a verdict, and a later cycle does not start over
  an unresolved earlier one.

## 8. What this run will not establish

- **No performance claim.** The sort's cost stays unmeasured; `s2pB` measured a
  different tree and may not be quoted for this one.
- **It does not validate the public documentation.** The published surface still states
  no row order (spec 008 §2a). Versioning and announcement remain the PI's decision and
  are a **precondition of deployment**.
- **It is not a deployment gate on its own.** After a C2 PASS the outstanding items are
  the versioning/announcement decision, a measurement of the sort's cost if one is
  wanted, and PM2 / formal deployment validation — **each separately authorised**.
