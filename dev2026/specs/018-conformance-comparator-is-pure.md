# Spec 018 — the conformance comparator is pure

**Status:** revision 1, offline. Written after `c1q` returned INCOMPLETE_VALIDATION.
**Supersedes nothing.** Spec 015 still defines the column order; this defines how the
harness is allowed to *check* it.

---

## 1. What c1q proved

`c1q` could not check candidate column-order conformance at all. Not for four cases —
at all, in any run, by construction.

The comparator reached the spec 015 rule by importing `api.query`. `api.query` imports
`api.config` at its line 65. `api/config.py` line 33 is:

```python
zarr_store_path = os.environ["WOA23_ZARR_STORE"]
```

an **unguarded lookup at module import time**. The arms set that variable in their
launch environment. The comparator runs in the *harness* process, which never does. So:

```
File "…/dev2026/api/query.py", line 65, in <module>
    from api.config import (
File "…/dev2026/api/config.py", line 33, in <module>
    zarr_store_path = os.environ["WOA23_ZARR_STORE"]
KeyError: 'WOA23_ZARR_STORE'
```

This was **not transient**. A re-run would have produced exactly the same result. The
module was present, correct, on `sys.path`, and byte-identical to the subject.

Two consequences followed, and both are the point of this spec:

1. The rule could not be applied, so conformance was UNVERIFIED and the run could not
   be a PASS.
2. `canonical_column_rule()` caught with a **bare `except Exception: return None`**,
   discarding the reason. The artefact said only "not importable". The traceback above
   had to be recovered afterwards by reproducing the import by hand.

## 2. The rule

**The conformance comparator MUST be a pure function of explicit inputs.**

It may not import `api.*`. It may not read an environment variable, at import time or
at any other time. It may not touch the filesystem, the network, or the zarr store. It
is stdlib-only.

It lives in [`bench/column_contract.py`](../bench/column_contract.py).

### 2.1 Why a mirror, not an import

`bench/column_contract.py` restates the contract that spec 015 section 4 decided.
`api/query.py` implements it. These are deliberately two separate things.

A comparator that imported the implementation could only ever prove *the implementation
agrees with itself*. That is not a check. The mirror is what makes the C1 gate an
independent statement about the candidate.

The obvious risk of a mirror is drift. That is handled by test, not by hope:
`test_column_contract.py` cross-checks the mirror against `api.query` **whenever
`api.query` is importable**, and against `api/query.py`'s source literal for the
per-grid parameter list, which is a literal rather than a name. If the product's order
ever changes and the mirror does not, the suite fails.

### 2.2 Conformance and reconstruction are separate findings

They answer different questions and must never be reported as one:

| finding | question | proves | satisfied by |
|---|---|---|---|
| **reconstruction** | did anything other than column order move? | no **value** changed | **any** permutation |
| **conformance** | is the candidate's order the one spec 015 mandates? | the **order** is right | only the canonical order |

Reconstruction is **necessary but not sufficient**. c1q had reconstruction and not
conformance, which is exactly why it could not be a PASS, and why a report that ran the
two together would have read as if it could.

A case is `EXPECTED_COLUMN_ORDER_CHANGE` only when **both** hold. If reconstruction
fails it is a regression. If reconstruction holds but the order is not conformant it is
**still a regression** — the gate is not weakened by this spec.

### 2.3 No request means UNVERIFIED, not defaults

`api/query.py` defaults `parameter` to `temperature` and `append` to `mn`. A case whose
request genuinely omits them takes those defaults, and the comparator rules normally.

But a caller that supplies **no request at all** is a different thing, and the
comparator must not invent one. Ruling a candidate against assumed defaults reports a
*conformant* candidate as a regression. `params is None` is therefore UNVERIFIED with
the cause recorded — never `params or {}`.

## 3. Fail-closed, with the cause

Fail-closed behaviour is **unchanged and non-negotiable**. Decoupling the comparator
removes the reason it failed; it does not remove the guard.

- A comparator that cannot be loaded → **UNVERIFIED** → the run is
  `INCOMPLETE_VALIDATION`.
- A comparator that raises → **UNVERIFIED**. Never a pass.
- A rule that cannot be applied to a case → **UNVERIFIED**, `conformant: None` —
  not `False`, and not `True`.
- **No bare `except`.** Every handler binds its exception and records
  `describe_exception(exc)`: type, module, message, repr, full traceback, and the
  chained `__cause__`/`__context__` where there is one.

`UNVERIFIED` is never `PASS`. It is not a soft pass, and it exits non-zero.

## 4. Evidence must be re-auditable, not merely re-derivable

c1q retained C20a's classification and **both body digests**, but not the bodies. The
digests prove *which* two documents were compared. They do not let anyone check the
comparison. The structural claim — "no route, parameter, schema or response moved" —
could be re-derived offline by regenerating documents and matching digests, but not
re-audited from the artefact.

The artefact now retains the **exact bodies** for every case that is not
byte-identical, or whose comparison could not be completed, alongside their lengths,
digests and the exact classification. Byte-identical cases are not duplicated.

Bodies are never truncated. Undecodable bodies are stored base64. The recorded length
sits beside the body so silent loss is detectable.

## 5. The summariser: the buckets partition the cases

c1q's gate said FAIL. It was wrong, and this is why: the classification dispatch
correctly bucketed `C20a` as `EXPECTED_DOCUMENTATION_CHANGE`, and then a **notes
fallback ran unconditionally afterwards** and re-added it to `regressions` on the
strength of a note that was merely descriptive —

> `non-row payload differs (8597 vs 9625 bytes); compared as raw bytes because there is no row structure`

— which matched neither of the fallback's two excluded prefixes. `C20a` appeared in
`expected_documentation_diffs` **and** `regressions` at once.

**The classification is authoritative.** A case the dispatch has placed is never
re-bucketed by the fallback. `CLASSIFIED_NOT_A_REGRESSION` names those classifications,
and `UNRECONSTRUCTED` is among them: "could not check" is not "candidate failed", and
counting it as both turns an incomplete validation into a false accusation.

The fallback keeps its sensitivity for cases the dispatch did **not** classify — those
still reach `regressions` and still fail the gate.

**Unexpected differences remain their own class.** Neither expected class is a blanket
exemption: the column class is granted only against reconstruction *and* conformance,
the documentation class only against structural identity after stripping `version`,
`summary` and `description`. A new route, response code, parameter or schema survives
that stripping and still fails.

## 6. `api/query.py` is unchanged

Nothing in this spec changes the candidate. `api/query.py` remains
`50907deeba50b707b2b85bcb8656f6bf39e32831a1d794dfba6ad396e2e82ca8`, as it has since
`ad3f428`. c1q produced no evidence of a candidate defect, and none is assumed.

The column-order gate is not weakened and expected differences are not broadened.

## 7. Required tests

All in [`bench/test_column_contract.py`](../bench/test_column_contract.py), which
exists because the offline fixtures that passed before c1q **never modelled a
comparator process without `WOA23_ZARR_STORE`**. That is precisely why they passed
while the real run did not.

| # | Test |
|---|---|
| 1 | comparator rules on conformance with `WOA23_ZARR_STORE` unset |
| 2 | the module imports in a **fresh interpreter** with every `WOA23_*` scrubbed |
| 3 | `api.query` really does still fail without the variable — the cause, asserted |
| 4 | comparator with proper explicit inputs: parameter-major, `mn` keeps its slot |
| 5 | request order discarded (`an,mn` ≡ `mn,an`); duplicates collapse |
| 6 | the result is a permutation, never a projection |
| 7 | a non-conformant permutation is caught, with the divergence located |
| 8 | setup/import failure → UNVERIFIED with type, message, traceback and cause |
| 9 | no bare `except` in the rule |
| 10 | **reconstruction succeeds but conformance unverified → UNRECONSTRUCTED** |
| 11 | reconstruction + conformance both hold → EXPECTED |
| 12 | reconstruction holds, order not conformant → REGRESSION |
| 13 | a changed value, and a column SET difference, still REGRESSION |
| 14 | C20a expected; new route / response / parameter / schema still FAIL |
| 15 | expected classes never appear in `regressions`; real ones still do |
| 16 | the buckets partition the cases — no case in two |
| 17 | bodies retained, and an independent re-audit reproduces the classification |
| 18 | the mirror agrees with `api.query` whenever it is importable |

Test 2 cannot be done in-process: `api.config` may already be in `sys.modules` from
another suite. Only a new interpreter proves the module has no import-time environment
dependency of its own.
