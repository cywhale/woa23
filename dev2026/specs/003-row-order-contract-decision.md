# 003 — Is row order part of the WOA23 API contract?

**Status:** decision memo. Options only — no option is implemented, none is
recommended as settled, and nothing here authorises a candidate change.
**Owner of the decision:** the PI.

| rev | date | change |
|---|---|---|
| 1 | 2026-08-09 | First draft, from the C2 observation of 2026-08-09. |

---

## 1. The observation, and what it is not

C2 ran three independent start/stop cycles with `PYTHONHASHSEED` unset, at
production's measured worker count of two. Of the 64 contract cases, **exactly `C16`
and `C16-csv` showed row-order variation across cycles on both arms**. The other 90
comparable (case, arm) pairs were stable. The 5.2B semantic gate passed in every
cycle: the row multiset and the column set were identical every time.

Two things follow that must not be blurred.

**This is not a candidate regression.** The **reference** — production's unmodified
`woa23_app.py` — varied in exactly the same way, in the same cycles, on the same two
cases. Whatever this is, the candidate did not introduce it and removing the
candidate would not remove it.

**Production is already in this configuration.** `PYTHONHASHSEED` is absent from
production's gunicorn environment (read from `/proc/3960/environ`, 2026-08-09), and
production runs `-w 2`. So production today serves these two cases from two workers
that hash independently. **Whether two production workers actually return different
row orders for the same query has not been measured** — that needs requests to
production, which nothing authorises — but the configuration that produces it in
staging is the configuration production runs.

**Effect directly observed; source-level mechanism strongly supported.** What was
observed is the row-order fingerprints differing on those two cases and no others.
What is supported but not proven step by step in a running process is that
`zarr_group_paths` is a `set` of path strings whose iteration order depends on the
process's hash seed, so a query spanning more than one Zarr group concatenates its
groups in a per-process order. Supporting evidence: those two are the only
multi-group cases in the suite; they were the only two to differ in the 2026-08-08
D2b run for a related reason; `bench/repro_c16.py` reproduces the mechanism offline;
and the two cases take exactly two distinct orderings, which is what a two-element
set admits. No instrumentation observed the iteration inside a worker.

## 2. Why it is a question at all

| who is affected | how |
|---|---|
| a consumer that reads rows positionally | sees a different row at the same index between requests |
| a consumer that diffs or checksums responses | sees a spurious difference |
| **our own 5.2A byte-exact gate** | cannot be used across unpinned processes for these cases — it would fail for a reason that is not a correctness defect |
| a consumer that keys on `(lon, lat, depth, time_period)` | unaffected |

The API's own documentation is the thing to check first and this memo does not
assume the answer: **§6 open question — does the published OpenAPI description, or
any README example, state or imply an ordering?** If it does, the contract already
answers this and the only question is conformance. If it does not, the choice below
is a product decision, not a bug fix.

## 3. Options

Costs are relative; none has been measured, and measuring any of them is out of scope
here.

### A. Do nothing; document that row order is unspecified

State in the OpenAPI description that row order is not guaranteed and that consumers
must key on the index columns.

- **Changes:** documentation only. No candidate change, no deployment change.
- **Risk:** an existing consumer already depends on order and breaks silently — we do
  not know whether one does. **Evidence needed:** whether any known consumer reads
  positionally.
- **Effect on our gates:** 5.2A stays unusable across unpinned processes for the two
  multi-group cases; C1-style byte-exact comparison keeps needing a pinned seed.
- **Honest framing:** this makes the current behaviour the contract. It is a real
  choice, not a non-choice.

### B. Pin `PYTHONHASHSEED` in production's launcher

Set it in `conf/start_app.sh` so every worker hashes identically.

- **Changes:** production deployment configuration. **No candidate code change.**
- **Effect:** removes the per-process variation for these cases, and makes 5.2A
  byte-exact usable against production.
- **Risk:** it is a global interpreter setting affecting every `set` and `dict` of
  strings in the process, not just this one; it is a deployment-wide change made to
  fix an API-surface question; and it silently becomes load-bearing — a future
  deployment that forgets it reintroduces the variation with no signal.
- **Note:** pinning the seed makes the *order* stable across workers. It does not
  make it *specified*: the order would be whatever seed 0 happens to produce.

### C. Make the candidate deterministic at the source

Iterate `zarr_group_paths` in a defined order — e.g. build it as a sorted sequence
rather than relying on `set` iteration.

- **Changes:** **a candidate code change** (`api/query.py`), which needs its own spec,
  review and approval. Not authorised by this memo.
- **Effect:** the candidate's row order becomes reproducible regardless of seed. The
  reference's does not, so the two arms would then differ by construction for these
  two cases and 5.2A would need a rule for that.
- **Risk:** it changes the candidate's output ordering relative to production's
  *current* output for these cases — which is either the fix or a breaking change,
  depending on §6's answer.

### D. Sort the response rows explicitly

Order the final result by `(lon, lat, depth, time_period)` — or another stated key —
before serialising.

- **Changes:** **a candidate code change**, larger than C, and a stated contract.
- **Effect:** row order becomes specified rather than merely stable, which is the only
  option that gives consumers something to rely on.
- **Risk:** a sort over the full result set on every request, on the read path this
  campaign has just spent months making faster. **Cost unmeasured**, and measuring it
  belongs to S2 performance validation.
- **Also:** it changes output ordering for *all* cases, not only the two — a much
  wider blast radius than the problem observed.

### Not an option

Leaving it undecided and relying on 5.2A byte-exact comparisons against unpinned
processes. That combination is already known to fail for a reason that is not a
defect, and a gate that fails for non-defects gets waived.

## 4. What would need to be true to choose

| to choose | establish first |
|---|---|
| A | no known consumer reads rows positionally; the OpenAPI description is updated |
| B | the PI accepts a deployment-wide interpreter setting for an API-surface reason, and it is recorded where a future deployment will see it |
| C | §6's answer says the current ordering is not contractual; a candidate-change spec exists and is approved |
| D | as C, plus a measured cost on the read path |

## 5. Test plan, if any option is taken

No implementation, and no test written, until an option is chosen. What each would
need:

- **A:** no new test. The existing 5.2B gate already encodes "order is not the
  criterion". One documentation assertion that the OpenAPI text says so.
- **B:** a provenance assertion that production's workers report the pinned seed —
  the collector already records `PYTHONHASHSEED` per arm, so this is a check on an
  existing field. Plus a repeat of C2's three cycles expecting `INSUFFICIENT` seed
  diversity, which under B is the *correct* outcome and must be reported as such.
- **C:** an offline test that the group sequence is identical across seeds — runnable
  with no host, in the shape of `bench/repro_c16.py`. Plus a 5.2A rule for the two
  cases where the arms would now legitimately differ.
- **D:** the same, plus an explicit order assertion per case in the contract suite,
  plus a measured before/after on the read path under S2 performance validation.

## 6. Open questions for the PI

1. **Does the published API description, or any README example, state or imply a row
   order?** This decides whether the choice is a product decision or a conformance
   fix, and it is not answered here.
2. Is any known consumer reading rows positionally?
3. If the answer to 1 is "no ordering is stated": is the preference to specify one
   (D), to stabilise without specifying (B or C), or to state that there is none (A)?
4. Does the decision apply to the CSV endpoint and the JSON endpoint alike? They vary
   together today, but nothing requires them to be answered together.

## 7. Boundaries of this memo

- **Nothing is implemented.** No option is chosen, no candidate file is touched, and
  `api/` remains byte-identical to `origin/main`.
- **No VM24 action is proposed or authorised** — in particular, measuring whether two
  live production workers return different orders would require requests to
  production, which is not authorised and is not requested here.
- **No performance claim.** Option D's cost is unmeasured and this memo does not
  estimate it.
- This memo does not depend on, and is not blocked by, D1, PM2 deployment validation
  or S2 performance validation.
