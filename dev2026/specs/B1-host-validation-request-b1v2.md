# `b1v2` — execution request: B1 host validation (replaces `b1v1`)

> # WITHDRAWN — SUPERSEDED BY [`b1v3`](B1-staging-stop-path-request-b1v3.md)
>
> **Do not authorise, execute, or re-point.** Kept as the record, not deleted. No result
> is back-filled into it.
>
> **Why.** The pre-authorisation audit found two things it could not stand on. First,
> `starttime_of` returned success with an **empty** value on a truncated `stat`, so a
> **live** process was classified GONE and the script reported a clean stop over it — the
> defect B1 exists to catch, in the code B1 validates. Second, `production_stop.sh`
> required **no grant at all**, so a staging grant reached a `pm2 stop`. Both are fixed in
> `787a72d`, which supersedes this request's subject `cba1329`.
>
> It was also **mis-titled**: it read as "B1 host validation" while proposing a run under
> an unprivileged staging account, which cannot close production B1. `b1v3` is titled
> **staging stop-path validation** and says so in its first paragraph.

**Status: WITHDRAWN. Not authorised, not executed. No VM24 contact.**

**`b1v1` is withdrawn and is NOT to be reused.** It referenced subject `e7243e4`, which
is superseded: `production_stop.sh` — the very script B1 validates — changed after it.
A host validation must not run a request pinned to a tree that no longer contains the
code under test.

Independent of [`CLEAN-bs3v1`](CLEANUP-request-clean-bs3v1.md), which remains its own
request and is not bundled here.

---

## 1. Why `b1v1` could not simply be re-pointed

`b1v1` was correct in shape, and re-labelling it would have been the quick move. It is
withdrawn instead because three of its premises changed:

| | `b1v1` said | now |
|---|---|---|
| subject | `e7243e4` | **superseded** — the stop path changed after it |
| the code under test | the jlist parser fix | **that plus** the `/proc` read path, which was carrying two more defects |
| identity | to be named later, no reuse of `bs3v1` | unchanged, **plus** an explicit ledger reconciliation before any port is taken |

A request is a statement about a specific tree. When the tree changes underneath it, the
honest move is a new request, not an edit that leaves the old provenance looking current.

---

## 2. What B1 asserts, unchanged

> **B1 — a stop is a stop.** `production_stop.sh` terminates the named app and its
> workers, verifies they are gone, and **fails closed** when it cannot establish that.
> It never reports success on an unverified premise.

### 2.1 What has been fixed offline since `b1v1`, and therefore needs host evidence

| | defect | why a fixture cannot settle it |
|---|---|---|
| jlist field order | pm2 5.4.2 emits `pid` before `name`; the awk parser needed the opposite | only the real daemon emits its own field order |
| `ppid` from `stat` field 4 | wrong whenever `comm` contains a space | a real gunicorn tree, with the real `comm` values, is the case |
| `starttime` from `stat` field 22 | same shift — and this field **is** the PID-reuse defence | as above |
| unreadable pid silently skipped | an unknown parent was treated as "not a child" | a real `/proc` with other accounts' processes in it |
| direct children only | a grandchild would never be checked | the real worker tree |

Every one of these is covered offline (**77 + 61 + 33 + 37 assertions**). None has met a
real PM2 daemon or a real `/proc`.

---

## 3. Scope — staging only, one app, started solely to be stopped

Runs under a **staging PM2 daemon** with its own `PM2_HOME`, as an unprivileged account,
against a synthetic store. **No production identity is touched.** The app serves no
request, runs no benchmark, and produces no latency, throughput or correctness evidence.
If it fails to start, the run is `INVALID_PRE_START` and stops there.

### 3.1 Identity — new, first-use, and reconciled before allocation

**No label and no port are reserved by this document.** They are named in a request
committed **after** the new subject.

**Before any port is named, a ledger provenance reconciliation is required** and must be
submitted for review:

1. every port the ledger records, with its state — `BOUND/SPENT`, `RETIRED-NEVER-BOUND`,
   or `allocated-not-yet-run`;
2. for each, the run that consumed it and the commit that named it;
3. any port that appears in the ledger but in no committed request, or in a request but
   not the ledger — **discrepancies listed, not silently reconciled**;
4. the proposed port, shown absent from all three of: the ledger's used set, every
   committed request, and the entire subject tree.

Binding conditions on the identity:

| | requirement |
|---|---|
| label | **entirely new.** Not `b1v1`'s proposed identity, not `bs3v1`, not `b35a1`, not a suffixed variant |
| port | **first use.** Never bound, allocated or retired. **18281** and **18283** are excluded and stay as recorded |
| tree, workdir, `PM2_HOME`, store, generated config | **all new, all absent** before the run |
| daemon | **its own.** Not `bs3v1`'s retained daemon (pid `1709473`) |

---

## 4. Procedure

1. **Bootstrap** via `staging_bootstrap.sh` with `--archive-sha256`, into a path that does
   not already exist. All existing refusals stand.
2. **Pre-start capture** — `ss -ltn` (never `-ltnp`), the staging daemon's app list, the
   target port empty.
3. **Start** the app under the staging `PM2_HOME`.
4. **B1-a — the daemon's own jlist.** Capture `pm2 jlist` **verbatim**, record the field
   order it actually emits, and record the pid the resolver returns from those exact bytes.
5. **B1-b — the real `/proc` read.** Before stopping, record for the master and every
   descendant: the `comm` as it actually appears, the `PPid:` line from
   `/proc/<pid>/status`, the `starttime` from `stat`, and the depth of the tree. *This is
   the evidence the offline fixtures structurally cannot produce.*
6. **B1-c — the stop.** Run `production_stop.sh` for the **named app**, then re-check by
   `(pid, starttime)`. Survivors are `CLEANUP_FAIL`. `all` remains refused; SIGKILL is not
   sent.
7. **B1-d — idempotency.** Run it **again** on the now-stopped app; record the verdict,
   the exit code, and the raw jlist entry it read.
8. **TLS environment isolation** — verified as a matter of course, and **reported
   separately** (§5).
9. **Post-run capture** — port released, app list, process table.

---

## 5. Results are reported per objective, never merged

| objective | what this run can establish | what it cannot |
|---|---|---|
| **B1** | **staging** evidence that the resolver reads a real jlist, the `/proc` read path works on real `comm` values, the tree is fully recorded, the stop terminates it, and a second stop is idempotent | **nothing about production.** The production stop path is NOT validated |
| **TLS environment isolation** | **staging** evidence that the paths are absent from master and every worker | nothing about production TLS, which stays ON and untouched |
| **B3** | **nothing.** Not exercised | `bs3v1`'s staging evidence stands as recorded, with its five qualifications |
| **B5** | **nothing.** Not exercised | as B3 |

**Everything here is STAGING EVIDENCE ONLY.** No production validation is performed,
claimed or implied. No `bs3v1` evidence is back-filled onto this run, and nothing this run
produces is back-filled onto `bs3v1` or any earlier subject.

**A leak of either TLS path with TLS off is `INVALID_ENVIRONMENT`:** no B1 result, not a
clean PASS, state preserved. Zero workers found is likewise a refusal.

**FAIL, or any discrepancy between what a wrapper claims and what the process table
shows, stops the run and preserves state** (spec 014 §2). No manual `pm2 stop`, no port
release by hand, no retry.

---

## 6. Cleanup

`production_stop.sh` **is the thing under test**, so its success cannot be assumed as this
run's cleanup. Anything left running is **preserved** and reported; removal is a separate
authorisation. The staging daemon this run starts is never removed implicitly.

---

## 7. Provenance — this request postdates the subject it names

```
subject commit   cba1329cf444179636f6298e4d98112083c247ab
subject line     fix(B1): read ppid from status, starttime past the comm, and never skip a pid silently
archive sha256   17044c0bd2a19563bb4c4178b8908509ca08d1da06ad1aabb08198e322dfc7b3
files            228
file-list sha256 4724ce1e8688ff07e4d101d087e32300f50b4a447af0ead162e32b2c74d0a844
```

`verify_clean_archive.sh` — **16 assertions**, plus 9 tree-only inside the export.

**Three serial offline batches at that commit: 53 suites, 4320 assertions, 0 non-zero**,
identical across all three, each batch recording its own HEAD with `tracked dirty = 0`.

**Per-file digests — the code under test, and what changed since `e7243e4`:**

```
5606dc15e7cd0d1174baa97b866a22041e4204b6b2ab8e9a803ba113a0c0624a  deploy/production_stop.sh
be16be5c82ccd4534dd4e5d36b44e5556f8f9a80bf09985ce0307b11f2d0d340  scripts/test_production_stop.sh
13a61527913a03341ede88ac315d7292d83c30d10c370f600802a58862ef777d  scripts/test_stop_proc_parsing.sh
```

**Unchanged from `e7243e4`, verified by blob identity rather than asserted:**

```
bb90652f2ed7966360c2b5b1150157405dc30090f2f15c200848615b184c5c56  deploy/staging_execute.sh
7f4da8d76ededc424c748d84e15b05750b3b1cb7fdb1b69fe8e4a47a217f7bea  deploy/production_app.sh
0b2e719aecb0fae13ec0271cf03dde9ff8fdd33dea2342bc1cf70481d9d04f60  deploy/make_staging_override.js
a7e4cb7fcb47e83b8192b225093e5dbe20aad2c673e5fed511b2328ee22410c4  deploy/ecosystem.production.config.js
50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8  api/query.py
```

So **TLS Option A behaviour is untouched by this change**, and so is the production config.

**The jlist parser is untouched, and that is shown rather than argued.** `jlist_resolve`
is **byte-identical** between `e7243e4` and `cba1329` (`fde549be…79d7`), and
`test_stop_jlist_parser.sh` is the same blob (`3336adcb…6ffa`) passing the same
**61 assertions**.

**Superseded, and not to be used for any run:** `e7243e4`, `fe6d4e2`, `a046d5ad`. Only
`cba1329` is current.

### 7.1 Still required before authorisation

The ledger provenance reconciliation of §3.1, then the new label and first-use port named
in a commit **later than `cba1329`**.
