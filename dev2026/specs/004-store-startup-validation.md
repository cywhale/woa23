# 004 — D1: store startup validation

**Status:** spec, acceptance cases and test plan only. **No candidate change is
proposed for implementation here**, and nothing in this document authorises one.

| rev | date | change |
|---|---|---|
| 1 | 2026-08-09 | First draft. Measured behaviour carried over from spec 002 §7 D1; acceptance cases, test plan and authorisation boundaries added. |

---

## 1. The behaviour, as measured

Measured 2026-08-09 in isolated staging against the package clone, with no socket
bound and no request sent. **Six negative fixtures and one real-store control.**

| # | fixture | what is at the path | import | startup | first data request |
|---|---|---|---|---|---|
| N1 | `WOA23_ZARR_STORE` **unset** | nothing configured | **exit 1** `KeyError` | **exit 1** | not reached |
| N2 | path does not exist | nothing | 0 | 0 | `FileNotFoundError` on the group path |
| N3 | existing but **empty** directory | a directory, no contents | 0 | 0 | `FileNotFoundError` on the group path |
| N4 | a **regular file** | a file, not a directory | 0 | 0 | `FileNotFoundError` on the group path |
| N5 | group path present, `.zgroup` **not JSON** | right shape, unparseable metadata | 0 | 0 | **`JSONDecodeError`**: `Expecting value: line 1 column 1 (char 0)` |
| N6 | group path present, `zarr_format: 99` | right shape, impossible format | 0 | 0 | **`MetadataError`**: `unsupported zarr format: 99` |
| P1 | the real production store | — | 0 | 0 | **returns data** |

Three facts this establishes, each of which constrains what a fix may claim:

1. **Only N1 fails before serving.** Everything else imports, passes
   `gunicorn --check-config`, and fails per request.
2. **N2, N3 and N4 are indistinguishable at every stage.** All three produce the
   identical `FileNotFoundError` naming `<store>/1_degree/annual/TS`. **A missing
   group path is not proof that the target is not a Zarr store** — it reports that
   the expected subpath is absent and says nothing about the store root.
3. **The two genuinely malformed stores produce a different class of error, and
   neither names the store.** `JSONDecodeError` names **no file and no path at all**;
   from a log it does not identify the store, the group, or even that the failure is
   store-related. `MetadataError` names the problem but not the file.

**Readiness is a fourth stage and is not established.** Whether the OpenAPI endpoint
answers 200 while the store is unreadable needs a listening server, which nothing has
authorised. It is a strong inference from N2–N4 — they import and pass
`--check-config` — and an inference is what it remains.

## 2. Scope

**In scope:** whether, and where, the candidate should detect an unusable store
before it starts serving; what a fix would have to establish; how it would be tested.

**Out of scope:** the reference's behaviour beyond §3 (it is not ours to change); the
row-order question (spec 003); PM2 / deployment validation; performance.

## 3. The reference behaves differently, and the difference is structural

| | reference | candidate |
|---|---|---|
| store source | `woa23_app.py:63`, `zarr_store_path = "data/"`, hard-coded | `api/config.py:31`, `os.environ["WOA23_ZARR_STORE"]`, mandatory |
| a missing store at startup | import succeeds, `--check-config` exit 0 | **unset variable**: import fails, exit 1 |
| effect of `WOA23_ZARR_STORE` | **none** — it is ignored | it *is* the store |
| what "not configured" looks like | a relative path resolved against whatever the cwd is | an absent variable, which is loud |

The reference cannot be misconfigured by the environment because it ignores it; it
can be misconfigured by being started in the wrong directory, and nothing detects
that at startup either. **`WOA23_ZARR_STORE` is not in production's environment
today** — production's gunicorn master has no `WOA23_*` variable at all — so the
candidate requires something production does not currently set. **Whether PM2 would
pass it through is unknown and belongs to deployment validation, not here.**

## 4. Options

None is chosen. Each is a **candidate change requiring its own approval**.

### A. Do nothing; document the behaviour

- Cost: none. Risk: a misconfigured deployment serves errors per request rather than
  failing to start, and one of those errors does not mention the store.

### B. Validate at import time, in `api/config.py`

Check that the configured path exists and is a directory when the variable is read.

- Catches N2, N3, N4 at import — the same stage that already catches N1.
- **Does not** catch N5 or N6: a directory that exists is not necessarily a Zarr
  store, and this check does not claim otherwise.
- Cheapest, and the narrowest claim.

### C. Validate the store opens, at startup

Open the store — or one known group — during application startup.

- Catches N2–N6, i.e. every negative fixture.
- Costs a read at startup and makes startup depend on the store being reachable,
  which changes the failure mode of a restart during a storage outage from "serves
  errors" to "will not start". **That is a trade, not an improvement**, and which side
  is preferable is an operational decision.
- **Must not be conflated with readiness.** §1 stage 4 is still unestablished.

### D. Improve the error rather than the timing

Leave startup alone; wrap the per-request failure so the message names the store path
and the group it tried to open.

- Fixes the worst property found — `JSONDecodeError` naming nothing — without
  changing when the process fails.
- Does not stop a misconfigured process from starting.

## 5. Acceptance cases

Whatever is chosen, these are the cases, and the fixtures already exist as built for
the 2026-08-09 measurement.

| case | fixture | required outcome under A | under B | under C | under D |
|---|---|---|---|---|---|
| D1-1 | N1 unset | fail at import, exit 1 | unchanged | unchanged | unchanged |
| D1-2 | N2 nonexistent | error per request | **fail at import** | **fail at startup** | error per request, **naming the path** |
| D1-3 | N3 empty dir | error per request | **fail at import** | **fail at startup** | error per request, **naming the path** |
| D1-4 | N4 regular file | error per request | **fail at import** | **fail at startup** | error per request, **naming the path** |
| D1-5 | N5 `.zgroup` not JSON | error per request | error per request | **fail at startup** | error per request, **naming the path and group** |
| D1-6 | N6 `zarr_format: 99` | error per request | error per request | **fail at startup** | error per request, **naming the path and group** |
| D1-7 | P1 real store | serves data | serves data | serves data | serves data |

**D1-7 is not decoration.** Without it, "every fixture failed" is equally consistent
with the harness being broken, and six identical errors look like six findings rather
than one behaviour plus a harness that cannot read anything.

Two further acceptance conditions for any option that changes behaviour:

- **D1-8, no false positive:** the real store must not be rejected by the new check,
  under the same launch the arms use (`-S`, package clone on `PYTHONPATH`).
- **D1-9, the message is actionable:** every failure message must name the configured
  path. This is the one property the measured behaviour most clearly lacks.

## 6. Test plan

**Offline, no host:** every case above can be exercised with a temporary directory
and the interpreter alone, as the 2026-08-09 measurement was. The three stages are
distinguished by how they are invoked, and the distinction is part of the test:

| stage | how it is exercised |
|---|---|
| import | `python -S -c "import api.app"` |
| startup | `gunicorn api.app:app --check-config` — builds the app, binds nothing |
| first data request | the query coroutine called in-process, no socket, no HTTP |

A regression suite would assert the **full 7×3 matrix**, not just the changed cells:
an option that fixes D1-2 by breaking D1-7 passes any test that only looks at the
fixture it was written for.

**On a host:** only if readiness (stage 4) is to be established, and that needs a
listening server and its own authorisation. It is not part of this plan.

**Fixture handling:** the six negative fixtures are built under a staging directory
and are never placed inside the package clone, production's site-packages or
`~/python/woa23`.

## 7. Authorisation boundaries

- **This document authorises nothing.** It is a spec and a test plan.
- **No candidate change is implemented.** `api/` remains byte-identical to
  `origin/main`, and any of options B, C or D is a change to `api/config.py` or
  `api/app.py` needing its own spec revision, review and explicit approval.
- **No VM24 action is proposed.** The measurement in §1 is already done; nothing here
  requires re-running it.
- **No production contact.** Not 8050, 8786 or 8787; no PM2, no systemd.
- **Readiness stays out.** Stage 4 is named as unestablished and is not smuggled in
  under "startup validation" — they are different stages and conflating them is how
  a health check comes to mean less than it appears to.
- **No performance claim.** Option C adds a read to startup; its cost is unmeasured.

## 8. Open questions for the PI

1. Which option — A, B, C or D — or a combination? B and D compose; C subsumes B.
2. For C: is "will not start during a storage outage" preferable to "starts and
   serves errors"? This is an operational preference and not a technical one.
3. Should the reference be left entirely alone? §3 says it ignores the variable
   altogether, so any alignment between the two arms is a separate question.
4. Is D1-9 — every failure message names the configured path — required regardless of
   which option is chosen? It is the smallest change with the clearest benefit.
