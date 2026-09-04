# 022 — B2–B5: offline roadmap and request plan

**Status: OFFLINE PLANNING. Nothing requested, nothing authorised, nothing executed. No
VM24 contact.** Prepared after B1's stop path closed
([Stage C result](B1-stageC-production-result.md)).

> **B1 closed the STOP PATH and nothing else.** B2–B5 are untouched by it, and none of them
> inherits any evidence from it.

---

## 1. What is still live in production — confirmed by observation, not by design docs

The argv captured during Stage A and unchanged through Stage C:

```
/home/odbadmin/.pyenv/versions/py311/bin/python3.11
  /home/odbadmin/.pyenv/versions/py311/bin/gunicorn
  woa23_app:app -w 2 -k uvicorn.workers.UvicornWorker
  -b 127.0.0.1:8050
  --keyfile conf/privkey.pem --certfile conf/fullchain.pem
  --timeout 120 --reload
```

| | blocker | what is live | why it matters |
|---|---|---|---|
| **B2** | wrong app path | `woa23_app:app` | the proposed launcher serves `api.app:app`. The production app is a **different module**, so B2 is not a rename — it is a change of what gets served |
| **B3** | hard-coded port | `-b 127.0.0.1:8050` | no `WOA23_PORT`; the port cannot be varied without editing the launcher |
| **B4** | store not required or validated | **no `WOA23_ZARR_STORE`** in the app env | the app resolves its store internally; a store fault surfaces as a restart loop rather than a refusal |
| **B5** | `--reload` in production | `--reload` | a file-watching auto-reloader in a production service — extra processes, and a reload triggered by any file change |

**All four are one file:** `conf/start_app.sh` (`4aaed5b7…6d77`), three lines long, no `exec`.

---

## 2. The dependency that shapes everything else

**`start_app.sh` does not `exec`.** PM2 tracks the **bash wrapper**; gunicorn is a child.
That is why B1's tree is depth-2, and it is also the single change that touches all four
blockers at once: the proposed `production_app.sh` **execs**, so PM2 would track gunicorn
directly.

> **This is a runtime-shape change, not a flag change.** After it, PM2's tracked pid *is*
> the gunicorn master, the process tree is depth-1, and `pm2 stop` signals gunicorn instead
> of a shell. **B1's production evidence does not carry over to that shape** — it validated
> the stop path against the tree that exists **today**.

**Consequence, stated now rather than discovered later:** cutting over to the new launcher
**invalidates B1's production evidence** and requires B1 to be **re-validated** against the
new tree. B1 is closed for the current runtime, not for all time.

---

## 3. Why B2–B5 cannot be staged like B1 was

| | B1 | B2–B5 |
|---|---|---|
| what changed on production | **nothing** — the app was stopped and restarted unchanged | **the launcher**, and therefore what runs |
| rollback | not applicable — nothing was installed | **required**, and it is a real deployment rollback: [Stage C §7.3](B1-stageC-production-b1-request-draft.md) lists the seven scopes |
| failure mode | app not running; recovery restarts it | app running **differently**, or serving wrong data, possibly without an obvious error |
| evidence needed | process/socket | **data-path correctness** — see §4 |

**B2–B5 is a deployment.** It should be planned as one, with an artifact, a rollback, and a
verification that the restored version is the intended one.

---

## 4. What C1/C2 actually established — a CORRECTION to my earlier claim

**I previously wrote that C1/C2 ran "against a staging deployment and a synthetic/read-only
store, not production", and that `woa23_app:app` vs `api.app:app` had "NEVER BEEN
COMPARED". Both statements are wrong.** They came from a summary rather than the record.
Read from `run_controlled.sh` and the `c1r`/`c2k` results:

```
ln -s "$STORE" "$REF_DIR/data"        # $STORE = /home/odbadmin/python/woa23/data
ln -s "$STORE" "$CAND_DIR/data"
STORE_LITERAL='data/'                 # byte-for-byte what woa23_app.py:63 sets
```

| | |
|---|---|
| **store the arms opened** | **the REAL production store**, `/home/odbadmin/python/woa23/data`, reached through a per-arm `data` symlink. **Not synthetic** |
| access | **read-only, enforced**: uid 994 is not the owner, `test -w` = no, **0 writable paths**, 0 escaping symlinks, across 123,005 files / 35,101,630,061 bytes |
| **reference arm** | **`woa23_app:app`** — the **legacy/production module** |
| **candidate arm** | **`api.app:app`** — the **candidate module** |
| interpreter | production's own binary, `-S`, package **clone** on `PYTHONPATH` |

**So a legacy-vs-candidate comparison on the real production store DOES exist.**

### 4.1 What IS established

| | evidence |
|---|---|
| candidate correctness on **real store data** | `c1r`: **64/64 cases accounted for** — 59 byte-identical, 4 decided column-order, 1 decided documentation. **`regressions: []`** |
| canonical values/columns | **63/64** (the 64th is the decided doc change) |
| candidate row-order contract | **44/44 applicable** (20 cases carry no row order) |
| repeatability | `c2k`: **5.2B semantic gate PASS in all three cycles**; row-order conformance **132/192 applicable, 0 violations**; candidate varied on **0** cases across 3 cycles |
| store integrity across those runs | production store **not modified**; identity identical before and after |

### 4.2 What is still MISSING — and it is not the store

The gap is the **deployment**, not the data:

| | |
|---|---|
| **production deployment data path** | **NOT validated.** The arms ran under the **benchmark harness** as **uid 994**, on staging ports (19111/19112), from a **package clone** on `PYTHONPATH`, with 1–2 workers — **not** production's PM2-managed `woa23` app on `8050` |
| **case-set coverage** | 64 cases (`c1r`) / 192 responses (`c2k`) is a **defined set, not a characterisation** of production's real query space |
| **production-side legacy behaviour** | the reference arm was `woa23_app:app` **as the harness ran it**, not as production runs it — different uid, worker count, port, interpreter invocation and import path |
| **TLS / proxy** | never exercised in C1/C2 |

**Correct framing:** *limited real-store contract evidence exists for
`api.app:app` versus `woa23_app:app`, produced by the benchmark harness under uid 994. What
is missing is a full data-path characterisation of the **production deployment**, and a
legacy-vs-candidate comparison **as production actually runs them**.*

**This materially reduces D-1's scope** — the question is no longer "does the candidate
return correct data from the real store", which `c1r`/`c2k` answered, but "does the
production deployment behave as the harness runs showed".

## 5. Proposed stage sequence — each its own request and authorisation

| stage | scope | changes production? |
|---|---|---|
| **D-1** | **read-only production data-path characterisation** — the **production deployment's** responses for a fixed case set, compared against `c1r`'s retained reference bodies. Closes §4.2's gap, **not** §4.1's, which is already answered | **no** |
| **D-2** | **offline comparison** — D-1's production responses against `c1r`/`c2k`'s recorded harness responses. Establishes whether the deployment matches the harness | **no** |
| **D-3** | **staging cutover rehearsal** — the new launcher under a staging PM2, full B2–B5 assertions, depth-1 tree, `WOA23_PORT`/`WOA23_ZARR_STORE` honoured | **no** |
| **D-4** | **production cutover** — install `production_app.sh`, switch the app, re-validate B1 against the **new depth-1 tree**, full rollback plan | **YES** |
| **D-5** | **post-cutover B1 re-validation** — B1's evidence does not survive D-4 (§2) | yes, already changed |

**Nothing beyond D-1 should be requested until D-1 and D-2 are reviewed.** Sequencing them
now would be planning past the evidence.

---

## 6. Open questions — for you, not assumed here

1. **Is a cutover wanted at all?** B2–B5 are real defects, but production is serving. The
   campaign has never established that the cutover's benefit exceeds its risk, and that is
   not a technical question.
2. **A11 under D-1.** Real queries emit the marker; a characterisation run would move it by
   a known amount. Acceptable, or does D-1 need a counter first?
3. **Is `api.app:app` the intended target module**, and has anyone confirmed it serves the
   same contract as `woa23_app:app` on production data?
4. **`conf/simu.sh`** — separate request, still open, still greps `tide_app` directly.
5. **Retained state** — `b1s1` (`1761143`), `bs3v1` (`1709473`), `~/woa23-b35a1/`,
   `pm2G`/18265. Cleanup is still separate and still unauthorised.

---

## 6a. Python 3.12+ — the policy, decided in advance

**Recorded now so it is not improvised later.** The campaign currently pins
`requires-python = ">=3.11,<3.12"` (`uv.lock`: `==3.11.*`), production runs **3.11.4**, and
D-3's isolated venv resolves **3.11.14**.

> ### A move to Python 3.12+ is a RUNTIME / DEPENDENCY UPGRADE — **not** an API architecture
> rewrite.
>
> It changes the interpreter and the resolved dependency set. It does **not**, by itself,
> change `api.app:app`, the contract, spec-015 column order, or the store.

### What it does NOT require

**The whole historical validation set is NOT re-run.** C1/C2's contract evidence, the B1
stop-path result, D-1's production observation and D-2's adjudication **stand as recorded**,
each within its own stated limits. Re-running them wholesale would cost more than it
establishes and would not answer the question an upgrade actually raises.

### What it DOES require — none of it optional

| # | required |
|---|---|
| 1 | a **new venv**, isolated and built by the run — never a shared or system interpreter (**spec 016**) |
| 2 | a **new `uv.lock`**, re-resolved for the new interpreter. The 3.11 lock is not carried over or edited |
| 3 | a **compatibility check** over the dependency set — polars, xarray, zarr, numcodecs, numpy, pyarrow, pandas, orjson and gunicorn/uvicorn all participate in reading, ordering and serialising, and each may change behaviour across an interpreter or version bump |
| 4 | a **trimmed staging smoke test** — the service starts, serves, and stops cleanly under the new runtime |
| 5 | a **trimmed data-path validation** — a small, fixed case set re-issued and adjudicated against the decided difference classes |

**"Trimmed" means scoped, not skipped.** The point is to re-establish the data path under the
new runtime cheaply, not to repeat every historical stage.

### The rule that makes this safe

> **Evidence gathered under the old runtime may NEVER be presented as validating the new
> one.** Every result carries the runtime it was obtained under. A 3.11 result is a 3.11
> result; it does not transfer to 3.12 by inheritance, by similarity, or by the upgrade being
> "only" a version bump.

**This is the same attribution limit D-3 already carries** — that a response difference cannot
be attributed to API code when the interpreter and package set also differ — applied forward
rather than backward. An upgrade changes exactly those two things, which is precisely why old
evidence cannot speak for it.

---

## 7. What this document is not

- **Not a request.** No stage here is submitted for authorisation.
- **Not a schedule.**
- **Not a claim that B2–B5 should proceed.** §6.1 is a genuine open question.
- **No B1 evidence is inherited.** B1 closed the stop path for **today's runtime shape**,
  and §2 records that a cutover would invalidate exactly that.
