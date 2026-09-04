# Next steps after `c1r` + `c2k` — proposal

**Status: PROPOSAL. Nothing here is authorised, requested or started.**
No VM24 contact. No execution of any kind follows from this document; each item below
would need its own request, its own review and its own explicit authorisation.

---

## 1. Where the campaign actually stands

**Complete:** C1/C2 contract correctness for the spec 008 + spec 015 candidate —
`c1r` PASS and `c2k` PASS, both 2026-08-26. Together they establish **values,
parameter-major column order and deterministic row order under the tested conditions.**

**Not established by them, and not inferable from them:** latency, throughput, startup,
deployment, PM2, production-runtime equivalence.

**Still open, each on its own track:** D1 real-store characterization; deployment
validation; B1–B5; B6; B7; PM2 / cutover; the unmeasured remainder of S2 performance;
and spec 003 §6 Q2, the consumer-risk question.

### 1.1 The honest summary of what is blocking a cutover

Nothing in the correctness work unblocks deployment. **B1–B5 have an offline design and
121 offline assertions, and not one of them has been installed or validated on the
host.** That, not correctness, is the critical path.

---

## 2. Proposed next step — and the one I recommend

I recommend **option A**. The others are listed because they are real alternatives, not
to pad the choice.

### Option A — B1–B5 host validation, one blocker at a time *(recommended)*

**Why:** it is the critical path, the design already exists offline, and each blocker is
independently testable. The correctness work is done and cannot be extended to cover
this, so continuing to add correctness evidence would be motion without progress.

**Shape:** one blocker per authorised run, each with its own identity and first-use
ports, each on the alternate port and **never** touching production's `start_app.sh`,
its PM2 entry, `pm2G`, 18265 or retained state. Offline first: a written per-blocker
plan, its own regression tests, three serial batches, a new subject with fresh digests,
then a request.

**Risk to state clearly:** B1–B5 touch the *launcher*. Every one of them is closer to
production than anything C1/C2 did. The staging discipline that `pm2B`/`pm2G` used —
isolated `PM2_HOME`, alternate port, synthetic store — must hold for all of them, and a
run that would need to modify production's own launcher is **not** in scope for any of
them without a separate, explicit cutover authorisation.

### Option B — D1 real-store characterization

**Why:** the missing-group behaviour is genuinely uncharacterized, and it is a startup
failure mode. **Why not first:** it is characterization, not a blocker; it does not move
the cutover, and it can run in parallel with A later.

### Option C — B6, the AVX2 / polars build

**Why:** polars is running an AVX2 build on a VM where VMware masks AVX2/BMI1/BMI2/LZCNT,
and polars itself warns this "will likely result in a crash" — in the API's hot path.
**Why not first:** the decision memo `012` is open, switching builds needs before/after
evidence on **both** throughput and returned values, and the value half of that is a
correctness question that would want its own C1-shaped run. It is a real risk but a
larger piece of work than it looks.

### Option D — the unmeasured remainder of S2 performance

**Why not now:** rung 60 is not scheduled and needs its own authorisation; throughput,
resource use and multi-worker were never in rung 21's scope. Nothing about `c1r`/`c2k`
makes this more ready than it was.

---

## 3. What I would prepare offline, if option A is chosen

Nothing below happens without your say-so; this is the shape, so the decision is
informed.

1. **Read B1–B5 as currently designed** in [`011`](011-production-launcher-cutover.md)
   and state, per blocker, exactly what installing and validating it would touch on the
   host — and what it would not.
2. **Identify the ownership assumptions**, the way the C1 sequence had to. The recurring
   defect class in this campaign has been a harness assuming it runs as the account that
   owns what it inspects. The launcher work runs closer to `odbadmin`-owned paths than
   anything so far, so that audit belongs *before* a request, not after a refused run.
3. **Per-blocker regression tests**, both ways: the blocker's fix works, and the
   condition it guards against still fails.
4. **Three serial offline batches**, a new subject with fresh archive and file-list
   digests, and each batch recording `git rev-parse HEAD` itself — the c2j
   batch-provenance error is not to be repeated.
5. **A request per blocker**, with exact paths, grant name, new label, first-use ports
   screened both ways (ledger **and** whole tree — the screen that caught 19109, 19113,
   19116, 19118 and 19136), pre-flight, and failure handling.

---

## 4. Standing constraints that carry into whatever comes next

- **Do not rerun** `c2k`, `c2j` or any earlier C2 identity; do not rerun `c1r` or any
  earlier C1 identity. All are consumed.
- **Preserve all `c2k` artefacts** — three cycles × 12, plus `c2k_summary.json`, plus
  all three workdirs, on VM24; and `scratchpad/c2k/` locally.
- **pm2G, port 18265, its PM2 entry and retained state: untouched.**
- Production store, ACLs, runtime, `.lock` file and permissions: **not modified**.
- No latency, startup, deployment or PM2 validation without its own authorisation.
- `api/query.py` unchanged unless new evidence proves a candidate defect.
- Every new run: **new identity, first-use ports, screened both ways, never
  pre-recorded** in the subject's own ledger.

---

## 5. The question for you

**Which track next — A (B1–B5 host validation), B (D1 characterization), C (B6 polars),
or D (performance)?** I recommend **A**, and will do the offline preparation in §3 for
whichever you choose. I will not contact VM24 or start any run until you review that
preparation and authorise it explicitly.
