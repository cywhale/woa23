# 012 — B6: VM24 masks AVX2. The polars CPU baseline decision

**Status: DECIDED, 2026-08-20 by the PI. Mainline polars is retained at the validated
version 1.27.1.** `polars-lts-cpu` is **not installed and not evaluated in this campaign**.
Nothing was changed to implement this decision — the decision *is* to keep what is already
running, so `pyproject.toml`, `uv.lock` and `api/` are all untouched.

**The AVX2 masking on VM24 is recorded as ACCEPTED RESIDUAL RISK** (§9).

| rev | date | change |
|---|---|---|
| 3 | 2026-08-20 | **DECIDED.** The PI adopts mainline polars 1.27.1 and rules `polars-lts-cpu` out of this campaign. Masking accepted as residual risk. **No C1/C2 re-run follows** (§8). Reopening conditions recorded (§10). Options table kept as the record of what was weighed. |
| 2 | 2026-08-20 | Reworked as the **B6** decision memo to the PI's scope. **Blocker renumbering, see §0.** Adds the observed CPU/masking evidence, the package and warning as they actually are, the dependency and runtime impact, the C1/C2 re-run question answered explicitly, and a recommendation with a stated preference for deferral. |
| 1 | 2026-08-20 | First draft, from the `pm2B` finding. |

---

## 0. A blocker-numbering conflict, resolved the PI's way

Spec 011 §4 introduced **B6** for a *different* thing: whether the interpreter that will
serve production carries the candidate's dependencies. The PI's 2026-08-20 instruction
assigns **B6** to the AVX2 masking risk.

**The PI's labelling wins.** From this revision:

| label | subject | status |
|---|---|---|
| **B6** | **AVX2 masking / polars CPU baseline** — this memo | **DECIDED 2026-08-20** (§7) |
| **B7** | **deployment dependency provisioning** — formerly called B6 in spec 011 §4 | **OPEN**; the factual half answered 2026-08-20, the runtime decision still open |

Spec 011 is updated to match. The renumbering is recorded rather than done silently,
because a blocker that quietly changes number is a blocker that gets lost.

## 1. Observed CPU and masking — what was actually seen

From VM24, read-only, during `pm2B` (2026-08-20):

```
model name : Intel(R) Xeon(R) Gold 6326 CPU @ 2.90GHz
grep -o avx2 /proc/cpuinfo   ->  no match
```

| | |
|---|---|
| host CPU | **Xeon Gold 6326** (Ice Lake-SP) |
| does that silicon have AVX2? | **yes** |
| does the guest see it? | **no** — `avx2`, `bmi1`, `bmi2`, `lzcnt` are all absent from `/proc/cpuinfo` |
| therefore | **the hypervisor is masking the feature set from the guest** |

This is a **property of the VM's CPU model**, not of the hardware and not of anything this
campaign changed. It has been true for every run.

## 2. The package, and the warning as it actually reads

Installed: **`polars==1.27.1`** (mainline build) in `dev2026/.venv`. At import, every
process emits:

```
polars/_cpu_check.py:258: RuntimeWarning: Missing required CPU features.
The following required CPU features were not detected:
    avx2, bmi1, bmi2, lzcnt
Continuing to use this version of Polars on this processor will likely result in a crash.
Install the `polars-lts-cpu` package instead of `polars` to run Polars with better compatibility.
```

**It says "likely result in a crash". That is the upstream's own wording and it is not
softened here.** The mechanism it describes is an illegal-instruction fault (SIGILL) when a
routine compiled for an unavailable baseline is reached — a **hard process crash, not a
wrong answer**. That distinction matters: the failure mode is a dead worker, not silent
data corruption.

**And it has not happened.** Across `c1f`, `c2g`, `s2pB` (934 of 992 requests) and `pm2B`
(288 rows over two formats and two process generations), with the warning present
throughout, **no crash has been observed**, and the row-order contract came out exact every
time. A direct sort on the contract key returns `['0','1','2','13']` correctly.

**I will not argue from that to safety.** The paths exercised are the paths exercised; a
build can carry AVX2 code in a routine no query has yet reached. What can be said is that
the risk is **unquantified**, not that it is absent.

## 3. What `polars-lts-cpu` actually is, and what changing to it would touch

`polars-lts-cpu` is the **same library, same version, compiled for an older CPU baseline**.
It is published by the polars project for exactly this situation. It is **not** a
downgrade, a fork, or a different API.

| question | answer |
|---|---|
| does the **API** change? | **no** — same `import polars as pl`, same functions |
| does **`api/` source** change? | **no.** Not one line. The `result_df.sort(...)` in `api/query.py` is untouched |
| does **response behaviour** change? | **not by design** — same library version, same semantics |
| does the **runtime** change? | **YES** — a different compiled binary, different vectorised code paths |
| does **`pyproject.toml` / `uv.lock`** change? | **yes**, both |
| is it a drop-in at the package level? | it **conflicts** with `polars`; the two cannot both be installed, so it is a replacement, not an addition |

**The API does not change; the runtime does.** That single distinction is what the rest of
this memo turns on.

## 4. Would C1/C2 have to be re-run? — yes, and this is the expensive answer

**Yes. Both.** Not as a precaution, but because of what C1 and C2 actually establish.

- **C1 (`c1f`)** established the row-order contract on an **isolated package tree**. The
  package tree is the subject of the test. A different polars build **is** a different
  package tree.
- **C2 (`c2g`)** was decisive precisely because it compared **sort implementations** across
  three unpinned starts — the reference took two different orders while the candidate held
  one. That result is a statement about *the sort in that build*.

**The row-order contract is spec 008's entire subject, and it is implemented by a call into
polars.** Substituting the library that performs the sort and carrying the old evidence
forward would be assuming the conclusion.

Also required, if the change is made:

- **`s2pB` is void for the new build** and rung 21 must be re-satisfied. This is the point
  of the exercise, not a side effect: an LTS build may be measurably slower, and the
  NO_REGRESSION and IMPROVED gates exist to find out;
- the **offline suite** and a **fresh tree identity**, since both dependency files change;
- **a new staging run** — new label, port, `PM2_HOME` and tree — because `pm2B` validated a
  tree whose lockfile would no longer describe what runs.

**Each of those needs its own authorisation. None is implied by a decision on this page.**

## 5. Impact, separated by what it actually touches

| | correctness | performance | production deployment |
|---|---|---|---|
| **today, mainline polars** | contract PASS on real data (`c1f`, `c2g`); no crash observed | measured only on this masked host | blocked by B1–B5, B7 regardless |
| **the risk if left** | **none observed**; a SIGILL would kill a worker, not corrupt a response | absolute figures are **not** production-representative | a crash under production load would be a visible outage |
| **if switched** | **all contract evidence must be re-established** | all figures must be re-measured; may be slower | one more full validation cycle before any cutover |

**The performance consequence is the one to state plainly:** every performance number this
campaign has produced was measured on a host with AVX2 masked. **Relative** results —
candidate versus reference, same host, same masking — remain valid, and that is what the S2
gates were judged on. **Absolute** figures, and any claim about what production hardware
would do, do not follow.

**The decision of §7 does not change this.** Deciding to keep mainline polars settles
*which package runs*; it does not unmask the CPU. **No performance validation of production
may be claimed as complete on the strength of figures measured under masking**, and the
decision explicitly creates no SLA and no production-runtime equivalence claim (§8).

## 6. Options

| # | option | cost | buys |
|---|---|---|---|
| **A** | do nothing; keep `polars==1.27.1` | none now; unquantified crash risk persists | no re-validation |
| **B** | switch to `polars-lts-cpu==1.27.1` **now** | §4 in full — C1, C2, S2, suite, new staging run | removes the SIGILL class |
| **C** | ask the VM administrator to **unmask AVX2** (host CPU model) | outside this repository; not ours to make | keeps the mainline build and its vectorised paths |
| **D** | **record as a performance/cutover risk now; decide before any performance claim or cutover** | none now | keeps the B1–B5 work moving; defers the expensive cycle to when it is actually forced |

## 7. The decision

**The PI's decision, 2026-08-20: adopt the current mainline polars. Do not install or
evaluate `polars-lts-cpu` in this campaign. Use the validated version, `polars 1.27.1`.**

This is **option A** of §6 — retain what is running — taken deliberately rather than by
default. My memo recommended D (defer and revisit); the PI decided A, which is the same
package outcome with the question closed rather than deferred. The distinction matters for
the campaign's record: **B6 is no longer waiting on anything.**

## 8. What follows, and specifically what does NOT

**No C1/C2 re-run is caused by this decision.** §4 established that a re-run would be
required *if the library changed*. **It does not change.** `polars` stays at 1.27.1 —
byte-identically the build that `c1f`, `c2g`, `s2pB` and `pm2B` ran against — so:

| evidence | status under this decision |
|---|---|
| `c1f` (C1 5.2C contract) | **stands.** Same package tree, same sort implementation |
| `c2g` (C2, three unpinned cycles) | **stands** |
| `s2pB` (S2 rung 21) | **stands**, within its stated scope |
| `pm2B`, `bash5A` | **stand** |
| offline suite / tree identity | unaffected; no dependency file is edited |

**Nothing is re-run, and nothing needs re-authorising because of B6.**

**No claim is created by this decision either.** Adopting mainline polars is not a
performance result, not an SLA, and **not a production-runtime equivalence claim**. In
particular it does not convert any figure measured on the masked host into a
production-representative one — §5's separation of *relative* from *absolute* results is
unchanged.

## 9. The accepted residual risk, stated plainly

**VM24 masks `avx2`, `bmi1`, `bmi2` and `lzcnt` from the guest. Mainline polars warns at
every import that this will "likely result in a crash". That warning is not silenced, not
suppressed, and not argued away — it is accepted.**

What is accepted, precisely:

- **the risk is unquantified.** No crash has been observed across `c1f`, `c2g`, `s2pB`
  (934 of 992 requests) and `pm2B`, but the paths exercised are the paths exercised;
- **the failure mode is a dead worker (SIGILL), not a wrong answer.** Under PM2's
  `autorestart` that presents as a restart, not as corrupted output;
- **it applies to production's interpreter too.** B7 found `polars` **1.27.1, mainline** in
  `/home/odbadmin/.pyenv/versions/py311` — so the warning would appear in production if the
  candidate ran from there, on the same masked CPU. The risk is not confined to the
  `dev2026` venv;
- **no absolute performance figure from this campaign is production-representative** while
  the masking stands.

**Accepting a risk means naming it and carrying it, not deciding it is small.**

## 10. What reopens B6

**Any one of these reopens it, and none requires a new argument to do so:**

1. **a hardware or hypervisor change** on the deployment host — including AVX2 being
   unmasked, which would make the mainline build straightforwardly correct rather than
   accepted-with-risk;
2. **a polars upgrade** — any version other than 1.27.1, in the venv or in `py311`;
3. **an observed SIGILL** or illegal-instruction crash in any woa23 process;
4. **an incorrect result** attributable to the library;
5. **a significant performance regression** that could plausibly trace to the CPU baseline.

On any of these, B6 is reopened and this decision is re-taken from §6 — **it does not
survive its preconditions.**

## 8. Boundaries

**Done here:** the observed masking, the package and its warning, what a switch would touch,
the C1/C2 answer, the impact split, four options and a recommendation.

**Not done, and now ruled out for this campaign:** `polars-lts-cpu` is **not installed and
not evaluated**; no dependency file edited; no benchmark; no VM24 action; no hypervisor
request; no production change.

**Nothing in this document alters what runs anywhere.** The decision is to keep the
validated package, so implementing it required no change at all — which is also why it
triggers no re-validation.
