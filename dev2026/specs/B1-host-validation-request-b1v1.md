# `b1v1` — execution request: B1 host validation (follow-on)

> # WITHDRAWN — SUPERSEDED BY [`b1v2`](B1-host-validation-request-b1v2.md)
>
> **Do not authorise, execute, or re-point this request.** It is kept, not deleted,
> because it is the record of what was proposed and why it could not stand.
>
> **Why it was withdrawn.** It pins subject `e7243e4`. `production_stop.sh` — the very
> script B1 validates — changed after that commit, when the review found `children_of`
> reading `/proc/<pid>/stat` field 4. Fixing it also uncovered the same defect in
> `starttime_of` (field 22, the PID-reuse identity) and two silent-skip paths.
>
> A host validation must not run against a tree that no longer contains the code under
> test, and re-pointing this document would have left stale provenance looking current.
> The proposed identity in it is **not** to be reused.

**Status: WITHDRAWN. Not authorised, not executed. Independent of every other
request — in particular it does NOT bundle the `bs3v1` cleanup, which is
[`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md) and needs its own authorisation.**

---

## 1. Why this exists, and what it is not

`bs3v1` produced valid staging-only evidence for **B3** and **B5**, with five recorded
qualifications. It did **not** validate B1, and it exposed a real B1 defect on the way:
`production_stop.sh` printed *"nothing to stop"* and exited 0 **while the service was
still running and holding port 18283**.

That defect is now fixed offline and covered by tests — but **a test suite is not host
validation**. The fix has never run against a real PM2 daemon on the real host. This
request is for exactly that, and for nothing more.

**This request does not:**

- claim or imply any production readiness;
- touch production's config, launcher, runtime, ports, PM2 daemon or data;
- close, upgrade or re-open B3/B5, whose evidence stands as recorded with its
  qualifications;
- carry out any cleanup, including of what `bs3v1` left behind.

---

## 2. What B1 asserts

> **B1 — a stop is a stop.** `production_stop.sh` terminates the named app and its
> workers, verifies they are gone, and **fails closed** when it cannot establish that.
> It never reports success on an unverified premise.

The three properties to be exercised **on the host**:

| | property | why it needs a host |
|---|---|---|
| **B1-a** | the pid resolver reads a **real `pm2 jlist`** and returns the right pid | the defect was a real-pm2 field-order dependency; fixtures cannot re-create the daemon's own output |
| **B1-b** | a stop of a **running** app terminates it and its workers, verified against the real process table | survivor detection reads `/proc`, which does not exist in the offline suite |
| **B1-c** | a **second** stop is idempotent — pid 0 with status `stopped` reports STOPPED and exits 0 | the STOPPED branch was derived from pm2's documented shape; it has not been seen from a live daemon |

---

## 3. Scope — staging only, one app, one port

Everything below runs under a **staging PM2 daemon** with its own `PM2_HOME`, as an
unprivileged account, against a synthetic store. **No production identity is touched.**

The run is a **single app started for the sole purpose of stopping it.** It serves no
request, runs no benchmark, and produces no latency, throughput or correctness evidence.
If the app fails to start, the run is `INVALID_PRE_START` and stops there.

### 3.1 Identity — new, first-use, and reusing nothing

**Requested identity:** to be named in a request committed **after** the subject commit,
per the standing rule that a *future* identity named in the document that becomes the
subject is burned. This draft therefore reserves **no label and no port**.

Binding conditions on that identity when it is named:

| | requirement |
|---|---|
| label | **entirely new.** Not `bs3v1`, not `b35a1`, not any earlier label, and not a suffixed variant of one |
| port | **first use.** Allocated fresh from the ledger, never previously bound, allocated or retired |
| specifically excluded | **18281** (RETIRED-NEVER-BOUND) and **18283** (SPENT) — neither may be reissued |
| staging tree, workdir, `PM2_HOME`, store, generated config | **all new and all absent** before the run. **No part of `bs3v1`'s retained state is reused**, mounted, adopted, restarted or written to |
| the retained `bs3v1` PM2 daemon (pid `1709473`) | **not used as this run's daemon.** This run starts its own under its own `PM2_HOME` |

**`18281` stays RETIRED-NEVER-BOUND and `18283` stays SPENT.** They are historical ledger
entries and are retained as such; excluding them from reuse is not a reason to remove or
rewrite them.

**No `bs3v1` evidence is carried into this run's result.** `bs3v1`'s B3/B5 staging
evidence is not back-filled onto the new subject, and this run inherits none of it: a
B1 result here says nothing about B3 or B5, which keep their own recorded qualifications.

---

## 4. Procedure

1. **Bootstrap** via `staging_bootstrap.sh` with `--archive-sha256`, into a path that
   does not already exist. Refusals stand: no symlinked members, no pre-existing root,
   driver hashed before and after extraction.
2. **Pre-start capture** — `ss -ltn` (never `-ltnp`), the staging daemon's app list, and
   the empty state of the target port.
3. **Start** the app under the staging `PM2_HOME`.
4. **B1-a** — capture `pm2 jlist` **verbatim** and record the field order the daemon
   actually emits, then record the pid the resolver returns from that exact bytes.
   *This is the evidence the offline fix cannot produce.*
5. **B1-b** — record the pid and its children from `/proc` and their `starttime`s, run
   `production_stop.sh` for the **named app**, then re-check. Survivors are
   `CLEANUP_FAIL`. `all` remains refused; SIGKILL is not sent.
6. **B1-c** — run `production_stop.sh` **again** on the now-stopped app, and record the
   verdict, the exit code, and the raw `jlist` entry it read.
7. **Post-run capture** — port released, app list, process table.

### 4.1 TLS environment isolation is verified in this run, and reported SEPARATELY

The entry now **unsets** `WOA23_TLS_KEYFILE` and `WOA23_TLS_CERTFILE` before `pm2 start`
when TLS is off, and then verifies their absence from `/proc/<pid>/environ` on the
**master and every worker**. Both happen here as a matter of course, because the app must
start before it can be stopped.

**This is a separate result line from B1, and must not be merged into it.** A clean TLS
environment does not make the stop path validated, and a B1 pass does not vouch for the
environment. They are reported as two findings.

If either path is present with TLS off, the run is **`INVALID_ENVIRONMENT`**: it yields
**no** B1 result, is **not** a clean PASS, and state is **preserved** — the exporting
ancestor must be found before any re-run. Zero workers found is likewise a refusal, not a
pass, since the master alone is not the thing that serves.

---

## 5. What a PASS and a FAIL each mean — reported per objective, never merged

Four findings, kept apart, each with its own status:

| objective | what this run can establish | what it cannot |
|---|---|---|
| **B1** | staging evidence that the stop path resolves a real pid, terminates the app and its workers, and is idempotent | **nothing about production.** The production stop path is NOT validated by this run |
| **TLS environment isolation** | staging evidence that the paths are absent from master and workers | nothing about production TLS, which stays ON and untouched |
| **B3** | **nothing.** Not exercised here | its `bs3v1` staging evidence stands as recorded, with its five qualifications |
| **B5** | **nothing.** Not exercised here | as B3 |

**Every result from this run is STAGING EVIDENCE ONLY.** No production validation of any
kind is performed, claimed or implied, and no `bs3v1` evidence is back-filled onto it.


**PASS** requires all of: the resolver returned the correct pid from real `jlist` output;
the app and every child were gone after the stop, verified by `(pid, starttime)`; the
second stop reported `STOPPED` with exit 0 and did not report `NOTFOUND`; the port was
released.

**A PASS is B1 staging evidence only.** It is not a production stop-path validation and
must not be recorded as one.

**FAIL, or any discrepancy between what a wrapper claims and what the process table
shows, stops the run and preserves state** under spec 014 §2. No manual `pm2 stop`, no
port release by hand, no retry. That policy exists precisely because it was the
process-table cross-check — not the wrapper's exit code — that found the `bs3v1` defect.

---

## 6. Cleanup

`production_stop.sh` **is the thing under test**, so its own success cannot be assumed as
this run's cleanup. If step 5 or 6 leaves anything running, the run **preserves** it and
reports; removal is a separate authorisation.

The staging PM2 **daemon** started by this run is retained on any failure and is never
removed implicitly — daemon cleanup is always its own authorisation.

---

## 7. Provenance — this request references the subject, and postdates it

**This section is committed AFTER the subject it names**, so that the request is a
protocol reference to a fixed tree rather than part of it.

```
subject commit   e7243e4dea8b8a9a267a2d3ca297b02092acb697
archive sha256   bc0cb2ab6515c67a5ba9fbc6075621bb08169fad99723a0a39d261b8c48de5c0
files            227
file-list sha256 baebc9c96a2f32deb05f8dd4c903a4b7820b136d1a80f90bd26ecf73e87167a4
```

Verified by `verify_clean_archive.sh` (16 assertions), and exercised by **three serial
offline batches at that commit: 52 suites, 4243 assertions, 0 non-zero**, each batch
recording its own HEAD and a clean tracked tree.

**Superseded and not to be used:** `fe6d4e2` (226 files) and `a046d5ad`. Only `e7243e4`
is current.

### 7.1 Still to be supplied at authorisation time

- the **new** label and the **first-use** port, named in a commit **later than
  `e7243e4`** — see §3.1 for the conditions they must satisfy;
- confirmation that the port is `allocated-not-yet-run` in the ledger, has **never** been
  bound or retired, and appears **nowhere** in the subject tree;
- confirmation that no element of the identity — tree, workdir, `PM2_HOME`, store,
  generated config — exists on the host before the run.

### 7.2 What is NOT carried into this run

- **No `bs3v1` state**: not its tree, bootstrap, workdir, `PM2_HOME`, or its retained
  daemon (pid `1709473`). This run starts its own daemon under its own `PM2_HOME`.
- **No `bs3v1` evidence**: its staging B3/B5 result is not back-filled onto this subject
  and is not extended by anything this run produces.
- **No retired or spent port**: `18281` stays **RETIRED-NEVER-BOUND**, `18283` stays
  **SPENT**. Both remain in the ledger as history; excluding them from reuse is not a
  reason to remove them.
