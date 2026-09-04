"""The spec 015 conformance comparator: works without the environment, fails loudly.

Every one of these exists because c1q could not check conformance at all. The
comparator reached the rule through `api.query`, which imports `api.config`, which
does `os.environ["WOA23_ZARR_STORE"]` at module import time -- unset in the harness
process. Conformance came out UNVERIFIED and the run could not be a PASS.

The offline fixtures that passed before that run never modelled a comparator process
without `WOA23_ZARR_STORE`, which is exactly why they passed while the real run did
not. These tests model it directly.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.column_contract import (                                    # noqa: E402
    AVAILABLE_PARS_1_DEGREE, AVAILABLE_VARS, INDEX_COLUMNS, ContractInputError,
    available_pars, canonical_column_order, column_order_conformance,
    expected_column_order, grid_size, requested_pars, requested_vars,
)
from bench.contract_diff import (                                      # noqa: E402
    CLASSIFIED_NOT_A_REGRESSION, canonical_column_rule,
    column_order_conformance_result, describe_exception, retained_evidence,
    summarise_5_2C,
)
from bench.suite_summary import summary, summary_line   # noqa: E402

CHECKS = 0


def check(cond, label):
    global CHECKS
    CHECKS += 1
    if not cond:
        raise AssertionError(label)


# ---------------------------------------------------------------------------
# 1. The comparator works with NO environment at all.
#    This is the c1q failure, reproduced and then required not to recur.
# ---------------------------------------------------------------------------

def test_comparator_without_zarr_store_env():
    """The whole point: no WOA23_ZARR_STORE, and conformance is still decided."""
    saved = {k: os.environ.pop(k, None)
             for k in ("WOA23_ZARR_STORE", "WOA23_STORE", "WOA23_DATA")}
    try:
        check("WOA23_ZARR_STORE" not in os.environ, "the variable really is unset")
        # canonical: index columns, then an (which precedes mn), then mn's bare name
        cols = ["lon", "lat", "depth", "time_period", "temperature_an", "temperature"]
        got = column_order_conformance(cols, {"parameter": "temperature",
                                              "append": "mn,an"})
        check(got["verified"] is True,
              "conformance is VERIFIED with no environment variable set")
        check(got["conformant"] is True, "and the canonical order is conformant")
        check(got["error"] is None, "with no error recorded")
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


def test_comparator_module_imports_in_a_bare_interpreter():
    """Import it in a FRESH process with a scrubbed environment.

    An in-process test cannot prove this: `api.config` may already be in
    `sys.modules` from another test. Only a new interpreter proves the module has no
    import-time environment dependency of its own.
    """
    env = {k: v for k, v in os.environ.items()
           if not k.startswith("WOA23_")}
    env["PYTHONPATH"] = str(Path(__file__).resolve().parent.parent)
    code = ("import bench.column_contract as m;"
            "print(m.column_order_conformance(['lon','temperature'],"
            "{'parameter':'temperature','append':'mn'})['verified'])")
    p = subprocess.run([sys.executable, "-c", code], capture_output=True,
                       text=True, env=env, timeout=120)
    check(p.returncode == 0,
          f"the module imports in a scrubbed interpreter (stderr: {p.stderr[-400:]})")
    check(p.stdout.strip() == "True", "and rules on conformance there")
    check("WOA23_ZARR_STORE" not in p.stderr,
          "with no complaint about the store variable")


def test_api_query_import_really_does_fail_without_the_env():
    """The c1q cause itself, asserted -- so the reason for this module is not folklore.

    If `api.query` ever becomes importable without the variable this test fails and
    someone must decide deliberately whether the decoupling is still wanted. It is
    skipped, not passed, when api/ is not on the path at all.
    """
    root = Path(__file__).resolve().parent.parent
    if not (root / "api" / "config.py").exists():
        return
    env = {k: v for k, v in os.environ.items() if not k.startswith("WOA23_")}
    env["PYTHONPATH"] = str(root)
    p = subprocess.run([sys.executable, "-c", "import api.query"],
                       capture_output=True, text=True, env=env, timeout=120)
    check(p.returncode != 0,
          "api.query does NOT import without WOA23_ZARR_STORE -- the c1q cause")
    check("WOA23_ZARR_STORE" in p.stderr,
          f"and the reason names the variable: {p.stderr[-300:]}")


# ---------------------------------------------------------------------------
# 2. The comparator with proper explicit inputs.
# ---------------------------------------------------------------------------

def test_explicit_inputs_parameter_major():
    """Parameter-major: every statistic of one parameter, then the next."""
    cols = ["lon", "lat", "depth", "time_period",
            "salinity", "salinity_an", "temperature", "temperature_an"]
    order = expected_column_order(cols, {"parameter": "temperature,salinity",
                                         "append": "an,mn"})
    check(order[:4] == list(INDEX_COLUMNS), "index columns lead, in their fixed order")
    check(order == ["lon", "lat", "depth", "time_period",
                    "temperature_an", "temperature", "salinity_an", "salinity"],
          f"parameter-major with mn holding its slot, got {order}")
    check(order.index("temperature_an") < order.index("salinity_an"),
          "temperature's statistics all precede salinity's")


def test_mn_is_renamed_but_keeps_its_slot():
    """`mn` becomes the bare parameter name and keeps mn's position in available_vars."""
    check(AVAILABLE_VARS.index("an") < AVAILABLE_VARS.index("mn"),
          "an precedes mn in the canonical declaration")
    order = expected_column_order(["temperature", "temperature_an"],
                                  {"parameter": "temperature", "append": "an,mn"})
    check(order == ["temperature_an", "temperature"],
          f"the bare name sits in mn's slot, after an, got {order}")


def test_request_order_is_discarded():
    """`an,mn` and `mn,an` must produce the same order. Spec 015 section 3."""
    cols = ["lon", "temperature", "temperature_an"]
    a = expected_column_order(cols, {"parameter": "temperature", "append": "an,mn"})
    b = expected_column_order(cols, {"parameter": "temperature", "append": "mn,an"})
    check(a == b, f"append order does not matter: {a} vs {b}")
    cols2 = ["lon", "temperature", "salinity"]
    c = expected_column_order(cols2, {"parameter": "temperature,salinity", "append": "mn"})
    d = expected_column_order(cols2, {"parameter": "salinity,temperature", "append": "mn"})
    check(c == d, f"parameter order does not matter: {c} vs {d}")
    check(c == ["lon", "temperature", "salinity"],
          "and the result is the canonical declaration's order, not the request's")


def test_duplicates_are_deduplicated():
    """C17's shape: `temperature,temperature` and `mn,mn`."""
    order = expected_column_order(["lon", "temperature"],
                                  {"parameter": "temperature,temperature",
                                   "append": "mn,mn"})
    check(order == ["lon", "temperature"], f"duplicates collapse, got {order}")
    check(requested_pars("temperature,temperature") == ["temperature"], "pars dedup")
    check(requested_vars("mn,mn") == ["mn"], "vars dedup")


def test_the_result_is_a_permutation_never_a_projection():
    """A rule that dropped a column would hide data loss behind an ordering fix."""
    cols = ["lon", "temperature", "an_unexpected_column", "temperature_an"]
    order = expected_column_order(cols, {"parameter": "temperature", "append": "an,mn"})
    check(sorted(order) == sorted(cols), "every input column survives")
    check(len(order) == len(cols), "and none is duplicated")
    check(order[-1] == "an_unexpected_column",
          "an unrecognised column is kept, at the end, sorted for determinism")


def test_grid_selects_the_parameter_set():
    # api/query.py:166-169 maps the REQUEST value to a grid: only a value CONTAINING
    # "25" selects the quarter-degree grid. A request of "04" therefore means ONE
    # degree, however much the internal grid id for quarter-degree happens to be '04'.
    check(grid_size(None) == 1.0, "no grid means one degree")
    check(grid_size("025") == 0.25, "a request containing 25 means quarter degree")
    check(grid_size("0.25") == 0.25, "and so does 0.25")
    check(grid_size("04") == 1.0,
          "a request of '04' does NOT: only '25' selects it (api/query.py:167)")
    check(grid_size("01") == 1.0, "01 means one degree")
    check(len(available_pars(None)) == 8, "eight parameters on the one-degree grid")
    check(available_pars("025") == ("temperature", "salinity"),
          "only two on the quarter-degree grid")
    check(requested_pars("oxygen", "025") == [],
          "oxygen is not available on the quarter-degree grid")
    check(requested_pars("oxygen", None) == ["oxygen"], "but it is on the one-degree")


def test_conformance_detects_a_wrong_order():
    """A non-conformant permutation FAILS. The gate is not weakened."""
    cols = ["lon", "temperature_an", "temperature"]           # canonical
    wrong = ["lon", "temperature", "temperature_an"]          # mn before an
    params = {"parameter": "temperature", "append": "an,mn"}
    ok = column_order_conformance(cols, params)
    bad = column_order_conformance(wrong, params)
    check(ok["conformant"] is True, "the canonical order conforms")
    check(bad["verified"] is True, "the wrong order is VERIFIED (it was checked)...")
    check(bad["conformant"] is False, "...and found NOT conformant")
    check(bad["first_divergence"]["index"] == 1,
          f"divergence located, got {bad['first_divergence']}")
    check(bad["first_divergence"]["expected"] == "temperature_an", "expected recorded")
    check(bad["first_divergence"]["actual"] == "temperature", "actual recorded")


def test_index_columns_must_lead_in_their_fixed_order():
    scrambled = ["time_period", "depth", "lat", "lon", "temperature"]
    got = column_order_conformance(scrambled,
                                   {"parameter": "temperature", "append": "mn"})
    check(got["verified"] is True, "checked")
    check(got["conformant"] is False, "index columns out of order is NOT conformant")
    check(got["expected"][:4] == list(INDEX_COLUMNS), "expected leads with lon,lat,...")


# ---------------------------------------------------------------------------
# 3. Setup failure preserves its cause. Never a silent pass.
# ---------------------------------------------------------------------------

def test_contract_input_error_is_unverified_not_a_pass():
    got = column_order_conformance(["lon", "temperature"],
                                   {"parameter": "not_a_parameter", "append": "mn"})
    check(got["verified"] is False, "an unrulable request is UNVERIFIED")
    check(got["conformant"] is None, "and conformant is None -- NOT False, NOT True")
    check(got["error"]["type"] == "ContractInputError", "with the cause's type")
    check("not_a_parameter" in got["error"]["message"], "and its message")


def test_empty_columns_is_unverified():
    got = column_order_conformance([], {"parameter": "temperature", "append": "mn"})
    check(got["verified"] is False, "no columns is UNVERIFIED")
    check(got["conformant"] is None, "not a pass")


def test_describe_exception_preserves_type_message_and_traceback():
    try:
        raise KeyError("WOA23_ZARR_STORE")
    except KeyError as exc:
        d = describe_exception(exc)
    check(d["type"] == "KeyError", "type preserved")
    check("WOA23_ZARR_STORE" in d["message"], "message preserved")
    check(isinstance(d["traceback"], list) and d["traceback"], "traceback preserved")
    check(any("WOA23_ZARR_STORE" in line for line in d["traceback"]),
          "and the traceback names the cause")
    check("repr" in d, "repr preserved")


def test_describe_exception_preserves_the_chained_cause():
    """The c1q shape exactly: an ImportError whose real cause is a KeyError."""
    try:
        try:
            raise KeyError("WOA23_ZARR_STORE")
        except KeyError as inner:
            raise ImportError("cannot import api.query") from inner
    except ImportError as exc:
        d = describe_exception(exc)
    check(d["type"] == "ImportError", "outer type")
    check(d["cause"]["type"] == "KeyError", "inner cause type preserved")
    check("WOA23_ZARR_STORE" in d["cause"]["message"],
          "and the inner message -- the detail c1q threw away")


def test_comparator_setup_failure_is_unverified_with_the_cause():
    """A comparator that raises must produce UNVERIFIED carrying its traceback."""
    import bench.contract_diff as cd

    def exploding(_cols, _params):
        raise KeyError("WOA23_ZARR_STORE")

    saved = cd.canonical_column_rule
    cd.canonical_column_rule = lambda: (exploding, None)
    try:
        got = cd.column_order_conformance_result(["lon"], {})
    finally:
        cd.canonical_column_rule = saved
    check(got["verified"] is False, "a raising comparator is UNVERIFIED")
    check(got["conformant"] is None, "never converted into a pass")
    check(got["setup_error"]["type"] == "KeyError", "with the exception type")
    check("WOA23_ZARR_STORE" in got["setup_error"]["message"], "and the message")
    check(got["setup_error"]["traceback"], "and the traceback")


def test_comparator_import_failure_is_unverified_with_the_cause():
    """The import itself failing -- the literal c1q condition."""
    import bench.contract_diff as cd
    try:
        raise ImportError("no module named bench.column_contract")
    except ImportError as exc:
        failure = describe_exception(exc)
    saved = cd.canonical_column_rule
    cd.canonical_column_rule = lambda: (None, failure)
    try:
        got = cd.column_order_conformance_result(["lon"], {})
    finally:
        cd.canonical_column_rule = saved
    check(got["verified"] is False, "an unimportable comparator is UNVERIFIED")
    check(got["conformant"] is None, "not a pass")
    check(got["setup_error"]["type"] == "ImportError", "cause type recorded")
    check("bench.column_contract" in got["setup_error"]["message"], "cause message")


def test_canonical_column_rule_returns_a_pair_and_actually_loads():
    comparator, failure = canonical_column_rule()
    check(comparator is not None, "the comparator loads in the harness process")
    check(failure is None, "with no failure")
    check(callable(comparator), "and is callable")


def test_no_bare_except_discarding_the_reason():
    """The source itself: `except Exception:` with no binding, before a `return None`."""
    src = (Path(__file__).resolve().parent / "contract_diff.py").read_text()
    rule_src = src.split("def canonical_column_rule")[1].split("\ndef ")[0]
    check("except Exception:" not in rule_src,
          "the reason-discarding guard c1q shipped with is gone from the rule")
    check("return None\n" not in rule_src.split("except")[-1],
          "the rule never returns a bare None from its handler")
    check("except Exception as exc" in rule_src,
          "the rule's guard binds the exception")
    check("describe_exception(exc)" in rule_src,
          "and records it rather than discarding it")


# ---------------------------------------------------------------------------
# 4. Reconstruction and conformance are separate findings.
# ---------------------------------------------------------------------------

def _cd():
    import bench.contract_diff as cd
    return cd


def _payload(cols, rows):
    return json.dumps([{c: r[c] for c in cols} for r in rows],
                      separators=(",", ":")).encode()


ROWS = [{"lon": 135.0, "temperature": 28.5, "temperature_an": 28.4},
        {"lon": 136.0, "temperature": 27.5, "temperature_an": 27.4}]
PARAMS = {"parameter": "temperature", "append": "an,mn"}
CANON = ["lon", "temperature_an", "temperature"]
NONCANON = ["lon", "temperature", "temperature_an"]


def test_reconstruction_succeeds_but_conformance_unverified():
    """The exact c1q situation: bytes reconstruct, the rule cannot be applied.

    Must be UNRECONSTRUCTED (-> INCOMPLETE_VALIDATION), never EXPECTED.
    """
    cd = _cd()
    ref = {"status": 200, "body": _payload(NONCANON, ROWS)}
    cand = {"status": 200, "body": _payload(CANON, ROWS)}
    saved = cd.canonical_column_rule
    cd.canonical_column_rule = lambda: (None, {"type": "ImportError",
                                               "message": "unavailable",
                                               "repr": "ImportError('unavailable')"})
    try:
        got = cd.classify_raw_difference(ref, cand, False, {}, PARAMS)
    finally:
        cd.canonical_column_rule = saved
    check(got["classification"] == "UNRECONSTRUCTED",
          f"unverified conformance is INCOMPLETE, got {got['classification']}")
    check(got["columns"]["reconstructed"] is True,
          "even though reconstruction SUCCEEDED -- the two are separate")
    check(got["conformance"]["verified"] is False, "conformance is unverified")
    check(got["conformance"]["setup_error"]["type"] == "ImportError",
          "and the setup cause is carried into the case's own record")


def test_reconstruction_and_conformance_both_pass_is_expected():
    cd = _cd()
    ref = {"status": 200, "body": _payload(NONCANON, ROWS)}
    cand = {"status": 200, "body": _payload(CANON, ROWS)}
    got = cd.classify_raw_difference(ref, cand, False, {}, PARAMS)
    check(got["classification"] == "EXPECTED_COLUMN_ORDER_CHANGE",
          f"both hold -> expected, got {got['classification']}: {got['why']}")
    check(got["columns"]["reconstructed"] is True, "reconstruction proven")
    check(got["conformance"]["verified"] is True, "conformance checked")
    check(got["conformance"]["conformant"] is True, "and conformant")


def test_reconstruction_passes_but_order_is_not_conformant_is_a_regression():
    """A permutation that is not spec 015's order FAILS. The gate is not broadened."""
    cd = _cd()
    ref = {"status": 200, "body": _payload(CANON, ROWS)}
    cand = {"status": 200, "body": _payload(NONCANON, ROWS)}   # candidate is wrong
    got = cd.classify_raw_difference(ref, cand, False, {}, PARAMS)
    check(got["columns"]["reconstructed"] is True, "the bytes still reconstruct...")
    check(got["conformance"]["conformant"] is False, "...but the order is wrong...")
    check(got["classification"] == "REGRESSION_BYTES_MUST_MATCH",
          f"...so it is a REGRESSION, got {got['classification']}")


def test_a_changed_value_hiding_behind_a_permutation_is_still_a_regression():
    cd = _cd()
    tampered = [dict(ROWS[0]), dict(ROWS[1])]
    tampered[0]["temperature"] = 99.9
    ref = {"status": 200, "body": _payload(NONCANON, ROWS)}
    cand = {"status": 200, "body": _payload(CANON, tampered)}
    got = cd.classify_raw_difference(ref, cand, False, {}, PARAMS)
    check(got["classification"] == "REGRESSION_BYTES_MUST_MATCH",
          f"a changed value is caught, got {got['classification']}")
    check(got["columns"]["reconstructed"] is False, "reconstruction fails, as it must")


def test_a_column_set_difference_is_still_a_regression():
    cd = _cd()
    dropped = [{k: v for k, v in r.items() if k != "temperature_an"} for r in ROWS]
    ref = {"status": 200, "body": _payload(NONCANON, ROWS)}
    cand = {"status": 200, "body": _payload(["lon", "temperature"], dropped)}
    got = cd.classify_raw_difference(ref, cand, False, {}, PARAMS)
    check(got["classification"] == "REGRESSION_BYTES_MUST_MATCH",
          f"a dropped column is caught, got {got['classification']}")
    check(got["columns"]["applies"] is False, "and it is not treated as an order change")
    check("column SETS differ" in (got["columns"]["why"] or ""),
          f"with the set difference recorded as the diagnosis: {got['columns']['why']}")
    check("conformance" not in got,
          "and conformance is not consulted for a set difference")


# ---------------------------------------------------------------------------
# 5. The documentation class, and the double-count that broke c1q's gate.
# ---------------------------------------------------------------------------

REF_DOC = {"openapi": "3.1.0", "info": {"title": "WOA23", "version": "1.0.0"},
           "paths": {"/api/woa23": {"get": {"summary": "old",
                                            "responses": {"200": {"description": "ok"}}}}}}


def _doc(**over):
    import copy
    d = copy.deepcopy(REF_DOC)
    d["info"]["version"] = over.get("version", "1.1.0")
    if over.get("new_route"):
        d["paths"]["/api/woa23/new"] = {"get": {"responses": {"200": {}}}}
    if over.get("new_response"):
        d["paths"]["/api/woa23"]["get"]["responses"]["418"] = {"description": "teapot"}
    if over.get("new_parameter"):
        d["paths"]["/api/woa23"]["get"]["parameters"] = [{"name": "extra", "in": "query"}]
    if over.get("changed_schema"):
        d["paths"]["/api/woa23"]["get"]["responses"]["200"]["content"] = {
            "application/json": {"schema": {"type": "array"}}}
    return d


def _as(body):
    return {"status": 200, "body": json.dumps(body, separators=(",", ":")).encode()}


def test_c20a_expected_documentation_difference():
    cd = _cd()
    got = cd.classify_raw_difference(_as(REF_DOC), _as(_doc()), False, {}, {})
    check(got["classification"] == "EXPECTED_DOCUMENTATION_CHANGE",
          f"1.0.0 -> 1.1.0 with nothing else moved, got {got['classification']}")
    check(got["docs"]["docs_only"] is True, "docs_only")
    check(got["docs"]["ref_version"] == "1.0.0", "ref version recorded")
    check(got["docs"]["cand_version"] == "1.1.0", "cand version recorded")


def test_description_and_summary_changes_are_still_documentation_only():
    cd = _cd()
    d = _doc()
    d["paths"]["/api/woa23"]["get"]["summary"] = "rows are returned in contract order"
    d["paths"]["/api/woa23"]["get"]["responses"]["200"]["description"] = "sorted"
    got = cd.classify_raw_difference(_as(REF_DOC), _as(d), False, {}, {})
    check(got["classification"] == "EXPECTED_DOCUMENTATION_CHANGE",
          "summary and description may move")


def test_a_new_route_still_fails():
    cd = _cd()
    got = cd.classify_raw_difference(_as(REF_DOC), _as(_doc(new_route=True)),
                                     False, {}, {})
    check(got["classification"] == "REGRESSION_BYTES_MUST_MATCH",
          f"a new route survives normalisation, got {got['classification']}")
    check(got["docs"]["docs_only"] is False, "and is reported as not docs-only")


def test_a_new_response_code_still_fails():
    cd = _cd()
    got = cd.classify_raw_difference(_as(REF_DOC), _as(_doc(new_response=True)),
                                     False, {}, {})
    check(got["classification"] == "REGRESSION_BYTES_MUST_MATCH", "a new response fails")


def test_a_new_parameter_still_fails():
    cd = _cd()
    got = cd.classify_raw_difference(_as(REF_DOC), _as(_doc(new_parameter=True)),
                                     False, {}, {})
    check(got["classification"] == "REGRESSION_BYTES_MUST_MATCH", "a new parameter fails")


def test_a_changed_schema_still_fails():
    cd = _cd()
    got = cd.classify_raw_difference(_as(REF_DOC), _as(_doc(changed_schema=True)),
                                     False, {}, {})
    check(got["classification"] == "REGRESSION_BYTES_MUST_MATCH", "a schema change fails")


# ---------------------------------------------------------------------------
# 6. The summariser: no double-counting.
# ---------------------------------------------------------------------------

C20A_NOTE = ("non-row payload differs (8597 vs 9625 bytes); compared as raw bytes "
             "because there is no row structure")


def _result(cid, cls, notes=None, columns=None, conformance=None):
    return {"id": cid, "verdict": "DIFFER" if notes else "MATCH",
            "notes": list(notes or []),
            "candidate_row_order_contract": {"applies": False},
            "raw_difference": {"classification": cls, "columns": columns,
                               "conformance": conformance}}


def test_expected_documentation_difference_is_not_also_a_regression():
    """c1q's exact defect: C20a in both buckets, driving the gate to FAIL."""
    s = summarise_5_2C([_result("C20a", "EXPECTED_DOCUMENTATION_CHANGE", [C20A_NOTE])])
    check(s["expected_documentation_diffs"] == ["C20a"],
          "C20a is an expected documentation difference")
    check(s["regressions"] == [],
          f"and is NOT also a regression, got {s['regressions']}")
    check(s["gate"] != "FAIL",
          f"so a descriptive note cannot drive the gate to FAIL, got {s['gate']}")


def test_expected_column_order_difference_is_not_also_a_regression():
    s = summarise_5_2C([_result("C1", "EXPECTED_COLUMN_ORDER_CHANGE",
                                ["some descriptive note"],
                                columns={"reconstructed": True},
                                conformance={"verified": True, "conformant": True})])
    check(s["expected_column_order_diffs"] == ["C1"], "bucketed as expected")
    check(s["regressions"] == [], "and not also a regression")


def test_unreconstructed_is_not_a_regression_but_blocks_the_pass():
    s = summarise_5_2C([_result("C1", "UNRECONSTRUCTED", [C20A_NOTE],
                                columns={"reconstructed": True},
                                conformance={"verified": False})])
    check(s["regressions"] == [],
          "'could not check' is not 'candidate failed'")
    check(s["unreconstructed"] == ["C1"], "it is its own finding")
    check(s["gate"] == "INCOMPLETE_VALIDATION",
          f"and it blocks a PASS, got {s['gate']}")


def test_a_real_regression_still_reaches_the_regression_bucket():
    """The fallback keeps its sensitivity for genuinely unclassified notes."""
    s = summarise_5_2C([_result("CX", "REGRESSION_BYTES_MUST_MATCH",
                                ["bytes differ where they must not: something moved"])])
    check(s["regressions"] == ["CX"], "a real regression is counted")
    check(s["gate"] == "FAIL", "and fails the gate")


def test_an_unclassified_note_still_reaches_the_regression_bucket():
    s = summarise_5_2C([_result("CY", None, ["an unexplained difference"])])
    check(s["regressions"] == ["CY"],
          "a case the dispatch did not classify still falls through to regressions")
    check(s["gate"] == "FAIL", "and fails")


def test_no_case_appears_in_two_buckets():
    """The invariant, stated once: the buckets partition the cases."""
    s = summarise_5_2C([
        _result("C1", "EXPECTED_COLUMN_ORDER_CHANGE", ["note"],
                columns={"reconstructed": True},
                conformance={"verified": True, "conformant": True}),
        _result("C20a", "EXPECTED_DOCUMENTATION_CHANGE", [C20A_NOTE]),
        _result("C9", "UNRECONSTRUCTED", ["note"], columns={"reconstructed": True},
                conformance={"verified": False}),
        _result("CX", "REGRESSION_BYTES_MUST_MATCH", ["bytes differ where they must not"]),
    ])
    f = s
    buckets = {"expected_column_order_diffs": f["expected_column_order_diffs"],
               "expected_documentation_diffs": f["expected_documentation_diffs"],
               "unreconstructed": f["unreconstructed"],
               "regressions": f["regressions"]}
    seen = {}
    for name, ids in buckets.items():
        for cid in ids:
            check(cid not in seen,
                  f"{cid} is in {name} and also in {seen.get(cid)} -- double-counted")
            seen[cid] = name
    check(seen == {"C1": "expected_column_order_diffs",
                   "C20a": "expected_documentation_diffs",
                   "C9": "unreconstructed",
                   "CX": "regressions"},
          f"each case in exactly one bucket, got {seen}")
    check(s["gate"] == "FAIL", "and a real regression still dominates the gate")


def test_unexpected_differences_remain_their_own_class():
    check("EXPECTED_DOCUMENTATION_CHANGE" in CLASSIFIED_NOT_A_REGRESSION, "docs")
    check("EXPECTED_COLUMN_ORDER_CHANGE" in CLASSIFIED_NOT_A_REGRESSION, "columns")
    check("UNRECONSTRUCTED" in CLASSIFIED_NOT_A_REGRESSION, "incomplete")
    check("REGRESSION_BYTES_MUST_MATCH" not in CLASSIFIED_NOT_A_REGRESSION,
          "a regression is NOT exempt -- the unexpected class stays separate")


# ---------------------------------------------------------------------------
# 7. Evidence retention: re-auditable, not merely re-derivable.
# ---------------------------------------------------------------------------

def test_bodies_are_retained_for_a_documentation_difference():
    ref, cand = _as(REF_DOC), _as(_doc())
    rd = _cd().classify_raw_difference(ref, cand, False, {}, {})
    ev = retained_evidence(ref, cand, rd)
    check(ev is not None, "C20a's bodies are retained")
    check(ev["reference"]["text"] == ref["body"].decode(), "the reference body EXACTLY")
    check(ev["candidate"]["text"] == cand["body"].decode(), "the candidate body EXACTLY")
    check(ev["reference"]["bytes"] == len(ref["body"]), "with its length")
    check(ev["candidate"]["sha256"], "and its digest")
    check(ev["classification"] == "EXPECTED_DOCUMENTATION_CHANGE",
          "and the exact classification alongside")


def test_retained_bodies_permit_an_INDEPENDENT_re_audit():
    """The point of retention: re-run the comparison from the artefact alone."""
    ref, cand = _as(REF_DOC), _as(_doc(new_route=True))
    rd = _cd().classify_raw_difference(ref, cand, False, {}, {})
    ev = retained_evidence(ref, cand, rd)
    replay_ref = {"status": 200, "body": ev["reference"]["text"].encode()}
    replay_cand = {"status": 200, "body": ev["candidate"]["text"].encode()}
    again = _cd().classify_raw_difference(replay_ref, replay_cand, False, {}, {})
    check(again["classification"] == rd["classification"],
          "the artefact alone reproduces the classification")
    check(again["docs"]["docs_only"] is False,
          "including the structural finding -- not merely the digest")


def test_bodies_are_retained_when_conformance_is_unverified():
    cd = _cd()
    ref = {"status": 200, "body": _payload(NONCANON, ROWS)}
    cand = {"status": 200, "body": _payload(CANON, ROWS)}
    saved = cd.canonical_column_rule
    cd.canonical_column_rule = lambda: (None, {"type": "ImportError",
                                               "message": "x", "repr": "x"})
    try:
        rd = cd.classify_raw_difference(ref, cand, False, {}, PARAMS)
    finally:
        cd.canonical_column_rule = saved
    ev = retained_evidence(ref, cand, rd)
    check(ev is not None, "an unverified case retains its bodies")
    check(ev["classification"] == "UNRECONSTRUCTED", "with its classification")


def test_byte_identical_cases_do_not_retain_bodies():
    same = {"status": 200, "body": b'[{"lon":1}]'}
    rd = _cd().classify_raw_difference(same, dict(same), False, {}, {})
    check(rd["classification"] == "BYTE_IDENTICAL", "identical")
    check(retained_evidence(same, dict(same), rd) is None,
          "matching payloads are not duplicated into the artefact")


def test_retention_falls_back_to_the_bodies_when_there_is_no_classification():
    a = {"status": 200, "body": b"one"}
    b = {"status": 200, "body": b"two"}
    check(retained_evidence(a, b, None) is not None,
          "differing bodies retained even with no raw_difference (5.2A/5.2B)")
    check(retained_evidence(a, dict(a), None) is None,
          "identical bodies not retained there either")


def test_non_utf8_bodies_survive_as_base64():
    a = {"status": 200, "body": b"\xff\xfe\x00binary"}
    b = {"status": 200, "body": b"\xff\xfe\x01binary"}
    ev = retained_evidence(a, b, None)
    check(ev["reference"]["encoding"] == "base64", "undecodable bodies use base64")
    import base64
    check(base64.b64decode(ev["reference"]["base64"]) == a["body"],
          "and round-trip exactly")


# ---------------------------------------------------------------------------
# 8. The mirror must not drift from the product.
# ---------------------------------------------------------------------------

def test_the_mirror_agrees_with_api_query_when_it_is_importable():
    """Cross-check against the real implementation, if the environment allows it.

    The comparator does not import `api.query` -- that is the whole fix. But when the
    environment DOES permit the import, the two must agree, or the comparator is
    ruling on a contract the product no longer implements.
    """
    try:
        from api.query import INDEX_COLUMNS as PROD_INDEX
        from api.query import canonical_column_order as prod_order
    except Exception:                                          # noqa: BLE001
        return                                                 # skipped, not passed
    check(tuple(PROD_INDEX) == INDEX_COLUMNS,
          f"index columns agree: {PROD_INDEX} vs {INDEX_COLUMNS}")
    cases = [
        (["lon", "lat", "depth", "time_period", "temperature", "temperature_an"],
         ["temperature"], ["an", "mn"]),
        (["lon", "salinity", "temperature", "temperature_an", "salinity_an"],
         ["temperature", "salinity"], ["an", "mn"]),
        (["lon", "temperature", "unexpected"], ["temperature"], ["mn"]),
        (["temperature_sd", "temperature", "lat", "lon"],
         ["temperature"], ["mn", "sd"]),
    ]
    for present, pars, variables in cases:
        mine = canonical_column_order(present, pars, variables)
        theirs = prod_order(present, pars, variables)
        check(mine == theirs,
              f"mirror agrees for {present}/{pars}/{variables}: {mine} vs {theirs}")


def test_the_declarations_match_api_config_when_importable():
    try:
        from api.config import available_vars as prod_vars
    except Exception:                                          # noqa: BLE001
        return
    check(tuple(prod_vars) == AVAILABLE_VARS,
          f"available_vars agree: {prod_vars} vs {list(AVAILABLE_VARS)}")


def test_the_parameter_declaration_matches_the_products_literal():
    """api/query.py:188 is a literal, not a name -- so read it out of the source."""
    src = (Path(__file__).resolve().parent.parent / "api" / "query.py")
    if not src.exists():
        return
    line = [x for x in src.read_text().splitlines()
            if x.strip().startswith("available_pars =")]
    check(len(line) == 1, f"exactly one declaration, found {len(line)}")
    for par in AVAILABLE_PARS_1_DEGREE:
        check(f"'{par}'" in line[0], f"{par} is in the product's declaration")
    check(line[0].count("'temperature'") >= 1 and "'salinity'" in line[0],
          "the quarter-degree pair is there too")


def main() -> int:
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failed = []
    for t in tests:
        try:
            t()
        except AssertionError as exc:
            failed.append(f"{t.__name__}: {exc}")
        except Exception as exc:                               # noqa: BLE001
            failed.append(f"{t.__name__}: unexpected {exc!r}")
    for f in failed:
        print("FAIL", f)
    print(f"test_column_contract.py: {len(tests)} tests, {CHECKS} assertions, "
          f"{len(failed)} failed")
    # This suite counts failed TESTS, not failed assertions, so it keeps its own prose and
    # its own return and emits only the contract line. What the contract requires is that
    # FAILED be non-zero exactly when the exit status is -- which `return 1 if failed` and
    # `len(failed)` satisfy together.
    summary_line(CHECKS, len(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
