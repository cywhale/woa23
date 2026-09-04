# 014 — Staging TLS paths, and the cleanup policy `bs3v1` changed

**Status: §1 DECIDED AND IMPLEMENTED (offline). §2 is a policy change and IS binding.**

> **Decision, recorded 2026-08-27.** The PI chose omission — the key and certificate are
> not present at all when TLS is off — and authorised offline implementation only.
> Implemented in `e80c6f1`; see §1.5 for what shipped and §1.6 for a limit found while
> testing it.
>
> **Label mismatch, flagged not smoothed over.** The instruction said "Option A"; the
> table below labels **omission as B** and **blanking as A**. The *described* behaviour
> was unambiguous — key and cert absent — so omission is what was built. Blanking would
> have left `production_app.sh` defaulting to the relative production-shaped path
> `conf/privkey.pem`, which is the weaker of the two.
No VM24 contact. `bs3v1` evidence untouched; no new code back-filled into it.

---

## 1. The environment-isolation deviation — design, not yet implemented

### 1.1 What happened, precisely

The `bs3v1` app process carried:

```
WOA23_TLS_CERTFILE=/home/odbadmin/python/woa23/conf/fullchain.pem
WOA23_TLS_KEYFILE =/home/odbadmin/python/woa23/conf/privkey.pem
```

**Production paths, in a staging process's environment.** `WOA23_TLS=off`, and they were
**not used** — no `--keyfile`/`--certfile` in argv, **0 open fds** under `/home/odbadmin`
on any of the three processes.

**That proves non-use in this run. It does not prove non-exposure**, and the two are not
the same claim. They were carried in.

### 1.2 Why they are there — the override model

`deploy/make_staging_override.js` builds the staging config by **overriding only six
things** on `ecosystem.production.config.js`: app name, port, store, `WOA23_TLS=off`,
`WOA23_PYTHON`, and the log paths. Line 247 explicitly **carries over**
`WOA23_WORKERS`, `WOA23_TLS_KEYFILE` and `WOA23_TLS_CERTFILE` from production.

That inheritance is deliberate — it is what makes staging a faithful copy of production's
configuration rather than a separately-invented one. The TLS paths come along with it.

Downstream, `production_app.sh:86-98` reads them into `KEYFILE`/`CERTFILE` but, with
`WOA23_TLS=off`, leaves `TLS_ARGS=()` empty and never opens them.
`staging_execute.sh:460-461` then **requires** both to be present and to match the config,
and its allowlist (line 477) includes them.

So the deviation is **structural, not accidental**: three files agree that these variables
should be present.

### 1.3 It CAN be avoided. Three options, with costs

| | option | cost / risk |
|---|---|---|
| **A** | **Blank them when `WOA23_TLS=off`** — set both to `''` in the staging override, and have `env_must` expect empty | smallest change. But `production_app.sh` falls back to `conf/privkey.pem` when unset/empty, so the *default* would point at a relative production-shaped path. Harmless with TLS off; misleading if anyone reads it. |
| **B** | **Omit them entirely when `WOA23_TLS=off`** — drop the keys from the staging config, and make `env_must` + the allowlist TLS-aware: required when `WOA23_TLS=on`, **required-absent** when `off` | cleanest semantically: when TLS is off the paths are meaningless, so they should not exist. Touches the allowlist and two `env_must` lines, and the tests that assert them. |
| **C** | **Accept the deviation** and record it as a known, bounded exposure | no code change; the deviation stays and must be re-stated in every future staging result. |

**I recommended B**, and at the time had **not implemented it**, because it touches the TLS
configuration path and TLS defaults are security-relevant. The property that must survive
any change is `production_app.sh`'s existing one: **TLS is ON unless an explicit
`WOA23_TLS=off` says otherwise**, and when on, both paths must be present and readable or
the launcher refuses. Option B does not weaken that — it only removes the variables in
the case where they are already inert.

The tests proposed at the time — all now written, and extended in §1.5:

- `WOA23_TLS=off` → both keys **absent** from the generated config and from the running
  process's environment;
- `WOA23_TLS=on` → both keys **present**, and the launcher still refuses an unreadable
  key or certificate;
- the allowlist still rejects any `WOA23_*` outside the permitted set in both modes;
- a staging run with TLS off carries **no** `/home/odbadmin` path in its environment at
  all;
- `production_app.sh` still defaults to TLS **on** when `WOA23_TLS` is unset.

**No production `conf/` file would be touched by any option.**

### 1.4 Superseded — the decision was made

This section read *"Until you decide: the deviation stands."* It no longer does. The
deviation is closed **for future staging runs** by §1.5.

It is **not** closed retroactively: the `bs3v1` result keeps the qualification exactly as
recorded, because `bs3v1` really did run with those paths in its environment and no later
code changes what already happened. **No new code is back-filled into `bs3v1`'s evidence.**

### 1.5 What was implemented — `e80c6f1`

Three files, offline, no VM24 contact:

| file | change |
|---|---|
| `deploy/make_staging_override.js` | deletes both keys when `WOA23_TLS === 'off'`; `EXPECTED` **gains** the two removals so the diff stays exhaustive; the equality guard is now **mode-aware** — equal to production when TLS is on, **required absent** when off |
| `deploy/staging_execute.sh` | `env_must` is TLS-aware: `ABSENT` when the config says off, exact match when on. The `WOA23_*` allowlist is unchanged and still fails closed |
| `deploy/production_app.sh` | `KEYFILE`/`CERTFILE` are resolved **only inside the TLS-on branch**, so with TLS off nothing is resolved, read or opened |

**What did not change, and is asserted in both directions.** `${WOA23_TLS:-on}` is intact,
so **unset still means ON**; TLS on still defaults both paths, still requires both to be
**readable**, still refuses before the port is bound, and still passes `--keyfile` /
`--certfile` through. The production config's own TLS entries are untouched and it does
not acquire `WOA23_TLS=off` from any of this. B2/B3/B4/B5's existing checks are
re-asserted, not relaxed.

`scripts/test_tls_off_omission.sh`, **59 assertions**, covering every item requested:
absent from the generated config; absent from the **actual child process environment**;
no `--keyfile`/`--certfile` in **argv**; still required when TLS is on; **fail-closed** on
a missing or unreadable key or certificate; production's TLS defaults unchanged; B3/B5 not
loosened. `test_staging_override.sh` 78 → **81**.

### 1.6 A limit of omission, found while testing it

**Omission removes the structural source, not every source.** PM2 merges `env` over its
own environment and has **no unset directive**, so a parent process that already exports
`WOA23_TLS_KEYFILE` still passes it to the child. The test spawns a child exactly as PM2
would, with the parent exporting the production paths, and **shows the value arriving**.

What closes that hole is the **harness**, not the config: `staging_execute.sh` reads
`/proc/<pid>/environ` and requires the literal value `ABSENT`, so an inherited path
**fails the run**. Both halves are asserted — the leak is real, and the check catches it —
including that a **present-but-empty** variable is not absence.

This is recorded rather than asserted away because "the config omits it" and "the process
does not have it" are different claims, and only the second is the one that matters.

---

## 2. Cleanup policy — changed, and binding

### 2.1 What I did in `bs3v1`, and why it is now disallowed

`production_stop.sh` reported "nothing to stop" and exited 0 while the service ran. I
judged the B3/B5 objective already met and the failure to be in the cleanup *wrapper*,
so I fell back to the authorised named-app stop directly (`pm2 stop <name>` under the
staging `PM2_HOME`) — no `all`, no `kill`, no SIGKILL — and the port was released.

**That fallback is now forbidden.** Whatever its merits in the moment, it means a broken
cleanup wrapper gets papered over by hand, and the next person cannot tell from the record
whether the wrapper worked.

### 2.2 The policy

> **If a cleanup wrapper fails, or reports success without achieving it, the run STOPS
> and PRESERVES state. There is no manual fallback, no direct `pm2 stop`, no port
> release, no retry.**
>
> The state is left exactly as it is, the discrepancy is reported, and the next action —
> including any cleanup — requires its **own separate authorisation**.

**Any daemon cleanup is its own authorisation.** Stopping an app and removing a PM2
daemon are different acts with different blast radii, and the second is never implied by
the first.

### 2.3 What this means for the outstanding state

The `bs3v1` staging **PM2 daemon (pid 1709473)** is still running under
`/home/woa23c1ro/woa23-bs3v1-pm2`. Under this policy it is **retained**, and it is **not**
mine to remove. A cleanup request covering it — and anything else `bs3v1` left — is
**drafted separately** as [`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md), is not
authorised, and is **not** bundled into any validation run. The B1 host validation that
follows from the stop fix is likewise its own draft,
[`b1v1`](B1-host-validation-request-b1v1.md).

Also outstanding and untouched: `~/woa23-b35a1/` and `b35a1-archive.tar`, the
`bs3v1` staging tree, bootstrap and workdir.

### 2.4 Why the policy is worth the friction

The `bs3v1` stop defect was found **because** the wrapper's claim was checked against the
actual process table. Had I trusted its exit code, the run would have reported a clean
cleanup and the B1 defect would still be undiscovered. A policy that stops on the
discrepancy keeps that check load-bearing; a policy that permits a quiet manual fix does
not.
