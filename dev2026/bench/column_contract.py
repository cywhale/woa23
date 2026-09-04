"""The spec 015 column-order contract, as a pure function of explicit inputs.

WHY THIS MODULE EXISTS
----------------------
c1q could not check candidate column-order conformance at all. The comparator
reached the rule by importing `api.query`, and `api.query` imports `api.config`,
which does an UNGUARDED `os.environ["WOA23_ZARR_STORE"]` lookup at module import
time (api/config.py:33). The arms set that variable in their launch environment;
the comparator runs in the harness process, which never does. So the import
raised `KeyError: 'WOA23_ZARR_STORE'` and conformance came out UNVERIFIED.

That was not a transient failure. It was structural: the comparator could never
have imported the rule, in any run, and a re-run would not have helped.

So the rule lives here instead, as a pure function of explicit inputs:

  * no `api.*` import,
  * no environment variable, at import time or ever,
  * no filesystem, no network, no zarr store,
  * stdlib only.

This module is THE CONTRACT that spec 015 section 4 decided. `api/query.py` is
the IMPLEMENTATION of that contract. They are deliberately two separate things:
a comparator that imported the implementation could only ever prove the
implementation agrees with itself. `test_column_contract.py` cross-checks the
two whenever `api.query` IS importable, so drift between them is caught rather
than assumed away.

SEPARATION OF CONCERNS
----------------------
Conformance and reconstruction answer different questions and must not be
conflated:

  reconstruction  did anything other than column order move?
                  (the reference's rows, permuted into the candidate's sequence,
                  reproduce the candidate's bytes exactly)
                  -> proves no VALUE changed. Satisfied by ANY permutation.

  conformance     is the candidate's column order the one spec 015 mandates?
                  -> proves the ORDER is correct. This module answers only this.

Reconstruction is necessary but not sufficient. Neither substitutes for the
other, and this module deliberately cannot see the reference at all.
"""

from __future__ import annotations

# --------------------------------------------------------------------------
# The canonical declarations. Mirrored from the product deliberately, NOT
# imported from it -- see the module docstring. Kept in step by
# test_column_contract.py, which fails if the product's own values drift.
# --------------------------------------------------------------------------

#: api/query.py:106
INDEX_COLUMNS = ("lon", "lat", "depth", "time_period")

#: api/config.py:100 -- the inner loop's canonical sequence.
AVAILABLE_VARS = ("an", "mn", "dd", "ma", "sd", "se", "oa", "gp", "sdo", "sea")

#: api/query.py:188 -- the outer loop's canonical sequence, per grid.
AVAILABLE_PARS_1_DEGREE = ("temperature", "salinity", "oxygen", "o2sat",
                           "AOU", "silicate", "phosphate", "nitrate")
AVAILABLE_PARS_QUARTER_DEGREE = ("temperature", "salinity")


class ContractInputError(ValueError):
    """The inputs do not describe a query this contract can rule on.

    Raised, never swallowed. A comparator that cannot determine the expected
    order must report UNVERIFIED with this as the recorded cause -- it must not
    fall back to "looks fine".
    """


def grid_size(grid) -> float:
    """The grid size, by api/query.py:166-169's own rule.

    `'04'` and anything containing `25` mean the quarter-degree grid; `None` and
    everything else mean the one-degree grid.
    """
    if grid is None:
        return 1.0
    return 0.25 if "25" in str(grid) else 1.0


def available_pars(grid=None) -> tuple:
    """The canonical parameter sequence for this grid (api/query.py:188)."""
    return (AVAILABLE_PARS_QUARTER_DEGREE if grid_size(grid) == 0.25
            else AVAILABLE_PARS_1_DEGREE)


def requested_vars(append, grid=None) -> list:
    """The requested statistics, deduplicated and in CANONICAL order.

    Mirrors api/query.py:179-180. The request's own order is discarded on
    purpose: `append=an,mn` and `append=mn,an` must produce the same columns,
    and a hash seed must not produce a different one. That is the entire point
    of spec 015 section 3.
    """
    if append is None:
        append = "mn"
    wanted = {v.strip() for v in str(append).split(",") if v.strip() in AVAILABLE_VARS}
    return [v for v in AVAILABLE_VARS if v in wanted]


def requested_pars(parameter, grid=None) -> list:
    """The requested parameters, deduplicated and in CANONICAL order.

    Mirrors api/query.py:191-192, with the same discarding of request order.
    """
    if parameter is None:
        parameter = "temperature"
    canon = available_pars(grid)
    wanted = {c.strip() for c in str(parameter).split(",") if c.strip() in canon}
    return [c for c in canon if c in wanted]


def canonical_column_order(present, pars, variables) -> list:
    """The canonical column order for `present`, per spec 015 section 4.

    A faithful mirror of api/query.py:109-149. `pars` and `variables` must
    ALREADY be in canonical sequence -- `requested_pars`/`requested_vars` put
    them there. Passing a request's raw order here would silently rule on the
    wrong order, so `expected_column_order` is the entry point callers should
    use; this one exists to be compared against the product function directly.

    Returns a PERMUTATION of `present`: never a projection. A rule that quietly
    dropped a column would trade a visible ordering bug for an invisible
    data-loss one.
    """
    present = list(present)
    remaining = set(present)
    order = []

    for col in INDEX_COLUMNS:
        if col in remaining:
            order.append(col)
            remaining.discard(col)

    # Parameter-major: every statistic of one parameter, then the next parameter.
    for param in pars:
        for var in variables:
            # `mn` wears the bare parameter name after the rename, and keeps mn's slot.
            col = param if var == "mn" else f"{param}_{var}"
            if col in remaining:
                order.append(col)
                remaining.discard(col)

    # Anything the rules above did not name is KEPT, sorted for determinism --
    # the frame's own order is exactly what is not trustworthy here.
    order.extend(sorted(remaining))
    return order


def expected_column_order(present, params: dict) -> list:
    """The order spec 015 mandates for `present`, given the REQUEST's parameters.

    This is the entry point: it takes the raw request dict a contract case
    carries (`parameter`, `append`, `grid`) and does the canonical normalisation
    itself, so no caller can accidentally hand in an uncanonicalised sequence.
    """
    if not isinstance(params, dict):
        raise ContractInputError(f"params must be a dict, got {type(params).__name__}")
    present = list(present)
    if not present:
        raise ContractInputError("no columns to rule on")
    grid = params.get("grid")
    pars = requested_pars(params.get("parameter"), grid)
    variables = requested_vars(params.get("append"), grid)
    if not pars:
        raise ContractInputError(
            f"no valid parameter in request {params.get('parameter')!r} for grid {grid!r}")
    if not variables:
        raise ContractInputError(f"no valid statistic in request {params.get('append')!r}")
    return canonical_column_order(present, pars, variables)


def column_order_conformance(columns, params: dict) -> dict:
    """Does `columns` match the order spec 015 mandates for this request?

    The candidate's OWN columns are ruled on directly. The reference is not
    consulted and is not an input: whether the reference agrees is a different
    question, answered by reconstruction, and a conformance check that needed
    the reference could not rule on a single arm at all.

    Returns a finding dict. `verified` is False ONLY when the contract could not
    be applied, and then `error` carries the cause -- never a silent pass.
    """
    out = {"verified": False, "conformant": None, "actual": None,
           "expected": None, "first_divergence": None, "why": None, "error": None}
    try:
        actual = list(columns)
        expected = expected_column_order(actual, params)
    except ContractInputError as exc:
        out["error"] = {"type": type(exc).__name__, "message": str(exc)}
        out["why"] = f"the spec 015 rule could not be applied to this case: {exc}"
        return out

    out["verified"] = True
    out["actual"], out["expected"] = actual, expected
    out["conformant"] = (actual == expected)
    if out["conformant"]:
        out["why"] = ("the candidate's column order is exactly the parameter-major "
                      "order spec 015 section 4 mandates")
        return out
    for i, (a, e) in enumerate(zip(actual, expected)):
        if a != e:
            out["first_divergence"] = {"index": i, "actual": a, "expected": e}
            break
    out["why"] = ("the candidate's column order is NOT the order spec 015 mandates; "
                  f"first divergence at index {(out['first_divergence'] or {}).get('index')}")
    return out
