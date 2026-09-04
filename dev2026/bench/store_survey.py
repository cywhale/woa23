"""P1-P6: what the real store actually contains, read before anything is started.

Spec 005 section 5. This runs **before either arm is started**, therefore before any
HTTP request of any kind, and it decides whether the depth characterization can mean
what it is supposed to mean.

**The question it exists to answer.** A request for seasonal nitrate at 3000-4000 m
can come back empty for two completely different reasons: the depth is outside that
climatology's range, or the group has no `nitrate` in its `parameters` coordinate and
`api.query` skipped it entirely, ending at the 404 branch. Those look identical from
outside and mean opposite things. Nothing downstream can tell them apart, so this
does — and if it cannot, the run stops rather than reporting a depth result it did
not measure.

**This reads coordinate chunks, and that is stated rather than glossed.** P2, P3 and
P4 read the `parameters`, `time_periods` and `depth` coordinate arrays, and
coordinate arrays are stored as chunks — the measurement that made spec 004 revision
6 abandon `xr.open_zarr(chunks=None)` for the lifespan check. This is a different
operation from that check and says nothing for or against its zero-chunk property,
which remains offline-audited (spec 005 section 2).

**Read-only.** Every store access is `mode="r"`; nothing here creates, writes or
removes anything under the store.

    uv run python -m bench.store_survey --store data/ --cwd /path/to/arm \\
        --out results/<label>_store_survey.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# `api.store_paths` is pure and imports only `os` — importing it has no side effect
# and needs no store. `api.config` is deliberately NOT imported: it performs
# import-time filesystem validation and would make this tool require the very
# configuration it is checking.
from api.store_paths import ANCHOR_GRID, ANCHOR_SUBGROUP, group_path  # noqa: E402

#: The group paths a request can actually reach. `api/query.py:115` restricts the
#: quarter-degree grid to temperature and salinity, so six of the eighteen
#: grid x climatology x parameter-group combinations are refused with a 400 before
#: any group is opened, and asking whether they exist would answer nothing.
#:
#: Derived from the same rules `api.query.determine_subgroup` applies.
#: `bench/test_store_survey.py` checks this list against that function itself, in a
#: subprocess with a synthetic store, rather than against this comment.
REACHABLE_GROUPS = (
    ("1_degree", "annual/TS"), ("1_degree", "annual/Oxy"),
    ("1_degree", "annual/Nutrients"),
    ("1_degree", "seasonal/TS"), ("1_degree", "seasonal/Oxy"),
    ("1_degree", "seasonal/Nutrients"),
    ("1_degree", "monthly/TS"), ("1_degree", "monthly/Oxy"),
    ("1_degree", "monthly/Nutrients"),
    ("025_degree", "annual/TS"), ("025_degree", "seasonal/TS"),
    ("025_degree", "monthly/TS"),
)

#: The group each characterization case reaches. **Neither is a universal
#: invariant**, and neither is the startup anchor:
#:
#:   startup anchor         1_degree/annual/TS — validated at import and lifespan by
#:                          the candidate itself, for every request. Spec 004.
#:   case target group      1_degree/seasonal/Nutrients — reached ONLY by this run's
#:                          winter-nitrate out-of-range case.
#:   annual control group   1_degree/annual/Nutrients — reached by the supported case.
#:
#: `1_degree/seasonal/Nutrients` is the target group for the current nitrate
#: characterization cases only. It is not a startup invariant and does not represent
#: every WOA23 group or every nutrient variable.
CASE_TARGET_GROUP = ("1_degree", "seasonal/Nutrients")
ANNUAL_CONTROL_GROUP = ("1_degree", "annual/Nutrients")

#: The variable under characterization. **`Nutrients` is a group of three**, and this
#: run characterizes one of them.
CASE_VARIABLE = "nitrate"
NUTRIENT_VARIABLES = ("nitrate", "phosphate", "silicate")

#: The coordinates `api.query` selects on. A group missing one of them raises inside
#: `ds.sel` before depth is ever consulted.
REQUIRED_COORDS = ("lon", "lat", "depth", "parameters", "time_periods")

#: `mn` is the **statistical-mean data field** — WOA23 Table 2 (p. 9), *Available
#: objectively analyzed and statistical fields*, where it is listed as a field
#: computable on the quarter-, one- and five-degree grids. **It is not an
#: oceanographic variable and it is not a coordinate.** It is the field this API's
#: query path reads when a request does not name another with `append`, so:
#:
#:   `mn` is the required statistical-mean data field used by the current API
#:   query path.
#:
#: **This is an API request-path requirement, not a depth-schema requirement.** Its
#: absence says nothing about the group's depth axis. What it means is narrower and
#: is stated at the check itself: the current query path cannot complete the request,
#: so the depth characterization's precondition is unmet — and the resulting 404 must
#: not be read as a depth-out-of-range outcome. It is **not** grounds for claiming
#: the store violates WOA23, unless it is separately established that this group was
#: meant to carry `mn`.
DEFAULT_APPEND_VAR = "mn"
APPEND_VAR_ROLE = (
    "mn is the required statistical-mean data field used by the current API query "
    "path (WOA23 Table 2, p. 9 — available objectively analyzed and statistical "
    "fields). It is not an oceanographic variable and not a coordinate. Checking it "
    "is an API request-path requirement, not a depth-schema requirement.")

#: The `time_period` this run requests. **Fixed, not chosen at run time.** An
#: earlier version picked the first seasonal code present in the group, which meant
#: a run could quietly become a different experiment and still be reported as the
#: `time_period=13` case. If 13 is absent the preconditions are unmet; testing 14,
#: 15 or 16 needs its own case id (spec 005 section 6.1).
REQUIRED_SEASONAL_PERIOD = "13"
SEASONAL_PERIODS = ("13", "14", "15", "16")

#: What `query_reachable` claims, and what it does not. The field was called
#: `api_reachable`, which reads as "the HTTP API was tried and answered". This
#: survey runs BEFORE any service exists and sends no request at all.
QUERY_REACHABLE_MEANING = (
    "query-path reachable by API logic; no HTTP request was sent")

#: WOA23 Product Documentation, Table 4 (pp. 11-12) — **per variable AND per
#: climatology**. Recorded in full for the three API parameter groups so the shape of
#: the rule is visible: it is a grid of nine cells, not one number.
#:
#:   group        annual              seasonal             monthly
#:   TS           0-5500 m / 102      0-5500 m / 102       0-1500 m / 57
#:   Oxy          0-5500 m / 102      0-1500 m / 57        0-1500 m / 57
#:   Nutrients    0-5500 m / 102      0-800 m / 43         0-800 m / 43
#:
#: **43 levels over 0-800 m applies only to nitrate, phosphate and silicate under
#: seasonal or monthly climatology.** It is not TS's row, not oxygen's, not annual
#: nitrate's, and not a property of the store. Nothing in this module applies a row
#: to a group other than the one it belongs to.
#:
#: https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DOCUMENTATION/WOA23_Product_Documentation.pdf
#:
#: `Nutrients` is an API and storage GROUP holding three variables. Table 4 gives
#: nitrate, phosphate and silicate the same row, and they are listed separately here
#: anyway, because the group name is not one of them.
_P12_BY_GROUP = {
    ("annual", "TS"): (102, 0.0, 5500.0),
    ("seasonal", "TS"): (102, 0.0, 5500.0),
    ("monthly", "TS"): (57, 0.0, 1500.0),
    ("annual", "Oxy"): (102, 0.0, 5500.0),
    ("seasonal", "Oxy"): (57, 0.0, 1500.0),
    ("monthly", "Oxy"): (57, 0.0, 1500.0),
    ("annual", "Nutrients"): (102, 0.0, 5500.0),
    ("seasonal", "Nutrients"): (43, 0.0, 800.0),
    ("monthly", "Nutrients"): (43, 0.0, 800.0),
}

_GROUP_OF_VARIABLE = {
    "temperature": "TS", "salinity": "TS",
    "oxygen": "Oxy", "o2sat": "Oxy", "AOU": "Oxy",
    "nitrate": "Nutrients", "phosphate": "Nutrients", "silicate": "Nutrients",
}

_CLIMATOLOGY_LABEL = {"annual": "Annual", "seasonal": "Seasonal",
                      "monthly": "Monthly"}

WOA23_P12_DEPTH = {}
for _var, _grp in _GROUP_OF_VARIABLE.items():
    for _clim in ("annual", "seasonal", "monthly"):
        _n, _lo, _hi = _P12_BY_GROUP[(_clim, _grp)]
        WOA23_P12_DEPTH[("1_degree", _clim, _var)] = {
            "levels": _n, "min": _lo, "max": _hi, "group": _grp,
            "cite": (f"WOA23 Table 4 (pp. 11-12), "
                     f"{_CLIMATOLOGY_LABEL[_clim]} Climatology / "
                     f"{_var.capitalize() if _var not in ('o2sat', 'AOU') else _var}: "
                     f"{_n} levels, {_lo:.0f}-{_hi:.0f} m"),
        }

#: The ordered level VALUES are not transcribed here. Table 4 gives the count and the
#: extent; the complete standard-depth list is Table 3's, and copying 102 numbers out
#: of a PDF into a comparison is a transcription nobody could check without redoing
#: it.
#:
#: **So what P4 and P5 verify is narrower than a standard-depth schema match, and
#: must be described as such:**
#:
#:   P4 verifies the Table 4 count and extent, records the full observed depth list
#:   and digest, but does not yet assert equality with the complete Table 3
#:   standard-depth list.
#:
#: Until Table 3 is transcribed into a machine-comparable list, a matching count and
#: extent means the axis has the right number of levels spanning the right range —
#: **not that those levels are the standard depths.**
P12_EXPECTED_LEVELS_TRANSCRIBED = False
DEPTH_VERIFICATION_SCOPE = (
    "Table 4 count and extent are verified; the full observed depth list and its "
    "canonical digest are recorded. Equality with the complete Table 3 "
    "standard-depth list is NOT asserted — Table 3 is not transcribed into a "
    "machine-comparable list, so a matching count and extent does not establish that "
    "the levels are the standard depths.")

#: The depth interval the out-of-range case asks for.
OOR_DEPTH_MIN, OOR_DEPTH_MAX = 3000.0, 4000.0


def _append_state(present: bool) -> str:
    """How the statistical-mean field's absence is described, in one place.

    Narrow on purpose. A missing `mn` means the current API query path cannot
    complete the request — it is a request-path precondition, and it says nothing
    about the group's depth axis and nothing about whether the store conforms to
    WOA23.
    """
    if present:
        return "present"
    return ("ABSENT — the current API query path cannot complete the request; a "
            "request-path precondition, not a depth-schema finding, and not by "
            "itself evidence that the store violates WOA23")


def _resolve(store: str, cwd: str) -> str:
    """The path a process with this cwd would use. Mirrors `store_paths.resolve`."""
    import os
    return store if os.path.isabs(store) else os.path.join(cwd, store)


def open_group(path: str) -> tuple[object | None, str | None]:
    """`zarr.open_group(path, mode="r")`. Returns (group, error); never raises."""
    try:
        import zarr
        return zarr.open_group(path, mode="r"), None
    except Exception as exc:                      # noqa: BLE001 - reported, not raised
        return None, f"{type(exc).__name__}: {exc}"


def read_coord(group, name: str) -> tuple[list | None, str | None]:
    """One coordinate array, as a plain list.

    **This reads a chunk.** A coordinate array is stored as chunks like any other,
    so materialising it opens chunk files. Recorded in the artefact so no reader has
    to infer it.
    """
    try:
        if name not in group:
            return None, f"no {name!r} array in the group"
        return [_plain(v) for v in group[name][:]], None
    except Exception as exc:                      # noqa: BLE001
        return None, f"{type(exc).__name__}: {exc}"


def read_coord_meta(group, name: str) -> dict:
    """The array's dtype and declared units, beside its values.

    Both belong in the record: a depth axis in metres and one in decibars would
    compare identically as numbers and mean different things, and a float32 axis
    cannot represent the same values a float64 one can — so a hash taken without
    them would be reproducible and still not comparable.
    """
    out = {"name": name, "present": False, "dtype": None, "units": None,
           "attrs": None, "error": None}
    try:
        if name not in group:
            out["error"] = f"no {name!r} array in the group"
            return out
        arr = group[name]
        out["present"] = True
        out["dtype"] = str(getattr(arr, "dtype", None))
        attrs = dict(getattr(arr, "attrs", {}) or {})
        out["attrs"] = {k: _plain(v) for k, v in attrs.items()
                        if isinstance(v, (str, int, float, bool))}
        out["units"] = out["attrs"].get("units")
    except Exception as exc:                      # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
    return out


#: How the depth-level digest is formed. Spelled out because a hash whose
#: canonicalization is unstated is a hash nobody can reproduce, and every clause
#: below is a way two correct readers would otherwise disagree:
#:
#:   1. STORED ORDER is preserved. The levels are not sorted. `ds.sel(depth=slice())`
#:      depends on the stored order, so re-ordering before hashing would erase the
#:      very property the selection depends on.
#:   2. Each level is formatted with `format(float(v), ".6f")` — fixed point, six
#:      decimals. `repr()` of a float differs between float32-widened and float64
#:      values (`0.1` vs `0.10000000149011612`) and between interpreters; a fixed
#:      format makes 0.1 hash the same from either. Six decimals is far below WOA23's
#:      metre-scale depths and far above their precision.
#:   3. The dtype and units are part of the input, so a metre axis and a decibar axis
#:      with identical numbers do NOT collide, and neither do float32 and float64.
#:   4. The count is included, so a truncation cannot coincide with a reformat.
#:   5. The pieces are joined with "|" and the levels with "," — both absent from
#:      every field — and the result is UTF-8 encoded before SHA-256.
DEPTH_HASH_CANONICALIZATION = (
    'sha256("depth-levels/v1|dtype=<dtype>|units=<units>|n=<count>|" + '
    '",".join(format(float(v), ".6f") for v in levels_in_stored_order))')


def depth_canonical_string(depths: list, dtype=None, units=None) -> str:
    """The exact string the digest is taken over. Returned so it can be checked."""
    body = ",".join(format(float(v), ".6f") for v in depths)
    return (f"depth-levels/v1|dtype={dtype}|units={units}|n={len(depths)}|{body}")


def depth_schema(depths: list | None, *, dtype=None, units=None) -> dict:
    """The group's depth axis as measured, in full, with a reproducible digest.

    Count, extent, the ordered levels, the dtype, the units, whether the axis
    increases monotonically, and a digest over all of it. The levels are kept as well
    as hashed: a digest tells a later run whether the axis changed, and only the list
    tells anyone what it changed to.
    """
    if depths is None:
        return {"n_levels": None, "min": None, "max": None, "levels": None,
                "dtype": dtype, "units": units, "monotonic_increasing": None,
                "levels_sha256": None,
                "canonicalization": DEPTH_HASH_CANONICALIZATION}
    import hashlib
    levels = [float(d) for d in depths]
    canonical = depth_canonical_string(levels, dtype, units)
    return {
        "n_levels": len(levels),
        "min": min(levels) if levels else None,
        "max": max(levels) if levels else None,
        "levels": levels,
        "dtype": dtype,
        "units": units,
        "monotonic_increasing": all(a < b for a, b in zip(levels, levels[1:])),
        "levels_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "canonicalization": DEPTH_HASH_CANONICALIZATION,
    }


def selectable_levels_in(depths: list | None, lo: float, hi: float) -> list:
    """The levels an `ds.sel(depth=slice(lo, hi))` could actually select.

    This is the question, and it is not the same as "does max exceed lo". An axis
    with a gap could have a maximum above the interval and nothing inside it.
    """
    return [] if depths is None else [float(d) for d in depths if lo <= float(d) <= hi]


def compare_to_p12(schema: dict, key: tuple) -> dict:
    """The measured axis against **this variable and climatology's** Table 4 row.

    Never against another's. Applying seasonal nitrate's 43 levels / 0-800 m to
    annual nitrate, to oxygen or to a monthly group would be reading a per-row
    condition as a store-wide invariant, which Table 4 explicitly is not.
    """
    expected = WOA23_P12_DEPTH.get(key)
    if expected is None:
        return {"checked": False,
                "note": f"no Table 4 entry is recorded for {key}; this run does not "
                        f"request it and must not judge it against another row"}
    mismatches = []
    if schema["n_levels"] != expected["levels"]:
        mismatches.append(f"{schema['n_levels']} levels, expected {expected['levels']}")
    if schema["min"] != expected["min"]:
        mismatches.append(f"min {schema['min']} m, expected {expected['min']} m")
    if schema["max"] != expected["max"]:
        mismatches.append(f"max {schema['max']} m, expected {expected['max']} m")
    return {
        "checked": True,
        "key": {"grid": key[0], "climatology": key[1], "variable": key[2]},
        "expected": {"levels": expected["levels"], "min": expected["min"],
                     "max": expected["max"], "group": expected["group"],
                     # Table 3's standard depths are not transcribed here; see
                     # P12_EXPECTED_LEVELS_TRANSCRIBED. The comparison is over the
                     # count and the extent, and the measured list is recorded in
                     # full beside it rather than being checked against a guess.
                     "levels_list": None,
                     "levels_list_transcribed": P12_EXPECTED_LEVELS_TRANSCRIBED,
                     "source": "WOA23 Table 4 (count and extent only)"},
        "actual": {"levels": schema["n_levels"], "min": schema["min"],
                   "max": schema["max"], "dtype": schema["dtype"],
                   "units": schema["units"],
                   "levels_sha256": schema["levels_sha256"]},
        "matches": not mismatches, "mismatches": mismatches,
        "cite": expected["cite"],
        "verification_scope": DEPTH_VERIFICATION_SCOPE,
        "applies_only_to": (f"{key[1]} {key[2]} on {key[0]} — this row is not "
                            f"applied to any other variable, grid or climatology"),
    }


def _plain(v):
    """numpy scalars and bytes to something json.dumps will accept."""
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace")
    item = getattr(v, "item", None)
    return item() if callable(item) else v


def survey(store: str, cwd: str, *, seasonal_periods=SEASONAL_PERIODS) -> dict:
    """P1-P6 and the reachable-group existence map. Fails closed on anything unclear."""
    resolved_store = _resolve(store, cwd)
    seasonal = group_path(store, *CASE_TARGET_GROUP)
    annual = group_path(store, *ANNUAL_CONTROL_GROUP)

    out: dict = {
        "kind": "d1_store_survey",
        "store_literal": store,
        "cwd": cwd,
        "resolved_store": resolved_store,
        "reads_coordinate_chunks": True,
        "reads_coordinate_chunks_note": (
            "P2, P3 and P4 materialise the parameters, time_periods and depth "
            "coordinate arrays, which are stored as chunks. This is a different "
            "operation from the lifespan anchor validation and neither strengthens "
            "nor weakens its zero-chunk property, which remains offline-audited."),
        "case_target_group": {
            "path": seasonal, "grid": CASE_TARGET_GROUP[0],
            "subgroup": CASE_TARGET_GROUP[1], "climatology": "seasonal",
            "variable_under_characterization": CASE_VARIABLE,
            "time_period": REQUIRED_SEASONAL_PERIOD,
            "note": ("the target group for the current nitrate characterization "
                     "cases only. It is NOT a universal startup invariant and does "
                     "not represent every WOA23 group or every nutrient variable")},
        "annual_control_group": {
            "path": annual, "grid": ANNUAL_CONTROL_GROUP[0],
            "subgroup": ANNUAL_CONTROL_GROUP[1], "climatology": "annual",
            "variable_under_characterization": CASE_VARIABLE,
            "time_period": "0"},
        "startup_anchor_group": {
            "path": group_path(store, ANCHOR_GRID, ANCHOR_SUBGROUP),
            "note": ("the group the CANDIDATE validates at import and lifespan "
                     "(spec 004). A different thing from either group above, and "
                     "not surveyed here — this run does not re-validate it")},
        "nutrients_group_note": (
            "the Nutrients group contains nitrate, phosphate and silicate. This run "
            "characterizes NITRATE only. Results for nitrate must not be generalized "
            "to phosphate or silicate without separate request-level evidence."),
        "checks": {},
        "problems": [],
    }

    def record(name, ok, detail):
        out["checks"][name] = {"ok": bool(ok), "detail": detail}
        if not ok:
            out["problems"].append(f"{name}: {detail}")

    # ------------------------------------------------------------------- P1 ---
    grp, err = _open_at(seasonal, cwd)
    record("P1", grp is not None,
           f"{seasonal} — {'opened as a Zarr group' if grp is not None else err}")

    # P2, P3 and P4 all need P1's group. Without it they are not "failed", they are
    # unrunnable, and saying so is different from saying the coordinate was absent.
    if grp is None:
        for name in ("P2", "P3", "P4"):
            record(name, False, f"not run: {seasonal} did not open")
        chosen = None
    else:
        # P2 — the variable, the coordinates `ds.sel` needs, and the statistical
        # -mean data field the current query path reads. Each can end a request
        # before depth is consulted: a missing coordinate raises inside `ds.sel`, and
        # a group without `mn` leaves `result_list` empty and answers 404. Either
        # would look like a depth outcome and be nothing of the kind.
        params, perr = read_coord(grp, "parameters")
        coords = {c: read_coord_meta(grp, c) for c in REQUIRED_COORDS}
        missing_coords = [c for c, m in coords.items() if not m["present"]]
        has_append = DEFAULT_APPEND_VAR in grp
        nutrients_present = sorted(v for v in NUTRIENT_VARIABLES
                                   if v in (params or []))
        out["case_target_coords"] = coords
        out["case_target_append_var"] = {
            "name": DEFAULT_APPEND_VAR, "present": bool(has_append),
            "kind": "statistical-mean data field",
            "role": APPEND_VAR_ROLE,
            "if_absent": (
                "the current API query path cannot complete the request, so the D1 "
                "depth characterization's precondition is unmet. The resulting 404 "
                "must NOT be recorded as a depth-out-of-range outcome. It is not by "
                "itself evidence that the store violates WOA23: that would need it "
                "separately established that this group was meant to carry mn.")}
        out["nutrient_variables_present"] = nutrients_present
        out["nutrient_variables_characterized"] = [CASE_VARIABLE]
        record("P2",
               params is not None and CASE_VARIABLE in (params or [])
               and not missing_coords and has_append,
               (f"parameters={params!r}; {CASE_VARIABLE!r} "
                f"{'present' if CASE_VARIABLE in (params or []) else 'ABSENT'}; "
                f"required coordinates {'all present' if not missing_coords else 'MISSING ' + str(missing_coords)}; "
                f"statistical-mean data field {DEFAULT_APPEND_VAR!r} "
                f"{_append_state(has_append)}; "
                f"nutrient variables in the store: {nutrients_present} "
                f"(a store schema observation — only {CASE_VARIABLE!r} is "
                f"characterized by this run)")
               if params is not None else perr)

        # P3 — the REQUIRED code, not whichever seasonal code happens to be there.
        # Substituting silently would make the run a different experiment while it
        # was still being reported as the `time_period=13` case.
        periods, tperr = read_coord(grp, "time_periods")
        has_required = REQUIRED_SEASONAL_PERIOD in (periods or [])
        others = [x for x in seasonal_periods
                  if x != REQUIRED_SEASONAL_PERIOD and x in (periods or [])]
        record("P3", has_required,
               (f"time_periods={periods!r}; required "
                f"{REQUIRED_SEASONAL_PERIOD!r} "
                f"{'present' if has_required else 'ABSENT'}"
                + (f"; other seasonal codes present={others} — testing one of those "
                   f"needs its own case id and is NOT this case"
                   if others and not has_required else ""))
               if periods is not None else tperr)
        out["required_seasonal_period"] = REQUIRED_SEASONAL_PERIOD
        out["seasonal_periods_present"] = (
            [x for x in seasonal_periods if x in (periods or [])]
            if periods is not None else None)

        # P4 — the target group's own depth schema, recorded in full, then two
        # separate questions asked of it.
        depth, derr = read_coord(grp, "depth")
        dmeta = read_coord_meta(grp, "depth")
        schema = depth_schema(depth, dtype=dmeta["dtype"], units=dmeta["units"])
        out["seasonal_depth"] = schema
        key = (CASE_TARGET_GROUP[0], "seasonal", CASE_VARIABLE)
        p12 = compare_to_p12(schema, key)
        out["seasonal_depth_vs_p12"] = p12
        if depth is None:
            record("P4", False, derr)
            out["store_schema_mismatch"] = None
        else:
            # (a) Is there anything selectable in the requested interval? Not "does
            #     the maximum fall short" — an axis with a gap could exceed the
            #     interval and still hold nothing inside it, and `ds.sel` selects
            #     levels, not ranges.
            inside = selectable_levels_in(depth, OOR_DEPTH_MIN, OOR_DEPTH_MAX)
            # (b) Does the measured axis match THIS variable and climatology's
            #     Table 4 row? Seasonal nitrate's row, and no other's.
            out["store_schema_mismatch"] = bool(p12.get("checked")
                                                and not p12.get("matches"))
            record("P4", not inside and not out["store_schema_mismatch"],
                   f"{schema['n_levels']} levels, {schema['min']}-{schema['max']} "
                   f"{schema['units'] or '<no units attr>'} "
                   f"(dtype {schema['dtype']}), "
                   f"levels_sha256={str(schema['levels_sha256'])[:12]}; "
                   f"selectable in {OOR_DEPTH_MIN:.0f}-{OOR_DEPTH_MAX:.0f} m: "
                   f"{inside if inside else 'none'}; "
                   + ("matches " + p12["cite"] if p12.get("matches")
                      else "DOES NOT match " + str(p12.get("cite", p12.get("note")))
                           + " — " + "; ".join(p12.get("mismatches", []))))
            out["selectable_levels_in_requested_range"] = inside

    # ------------------------------------------------------------------- P5 ---
    # The annual control, judged against ANNUAL nitrate's Table 4 row — 102 levels
    # over 0-5500 m. Seasonal nitrate's 43 / 0-800 m is a different row and must not
    # be applied here; the supported case asks for 0-800 m, which is inside the
    # annual range, and that is the only relationship between the two numbers.
    agrp, aerr = _open_at(annual, cwd)
    if agrp is None:
        record("P5", False, f"{annual} — {aerr}")
        out["annual_depth"] = depth_schema(None)
        out["annual_depth_vs_p12"] = {"checked": False, "note": "the group did not open"}
    else:
        aparams, _ = read_coord(agrp, "parameters")
        aperiods, _ = read_coord(agrp, "time_periods")
        adepth, aderr = read_coord(agrp, "depth")
        admeta = read_coord_meta(agrp, "depth")
        acoords = {c: read_coord_meta(agrp, c) for c in REQUIRED_COORDS}
        amissing = [c for c, m in acoords.items() if not m["present"]]
        ahas_append = DEFAULT_APPEND_VAR in agrp
        out["annual_control_coords"] = acoords
        out["annual_control_append_var"] = {
            "name": DEFAULT_APPEND_VAR, "present": bool(ahas_append),
            "kind": "statistical-mean data field", "role": APPEND_VAR_ROLE}
        aschema = depth_schema(adepth, dtype=admeta["dtype"], units=admeta["units"])
        out["annual_depth"] = aschema
        ap12 = compare_to_p12(aschema, (ANNUAL_CONTROL_GROUP[0], "annual",
                                        CASE_VARIABLE))
        out["annual_depth_vs_p12"] = ap12
        if adepth is None:
            record("P5", False, aderr)
        else:
            # The supported case asks for 0-800 m: it needs levels selectable there.
            selectable = selectable_levels_in(adepth, 0.0, 800.0)
            ok = (CASE_VARIABLE in (aparams or []) and "0" in (aperiods or [])
                  and bool(selectable) and ap12.get("matches", False)
                  and not amissing and ahas_append)
            out["annual_selectable_levels_0_800"] = selectable
            record("P5", ok,
                   f"{annual}: parameters={aparams!r}, time_periods={aperiods!r}, "
                   f"coordinates {'all present' if not amissing else 'MISSING ' + str(amissing)}, "
                   f"statistical-mean field {DEFAULT_APPEND_VAR!r} "
                   f"{_append_state(ahas_append)}, "
                   f"{aschema['n_levels']} levels, {aschema['min']}-{aschema['max']} "
                   f"{aschema['units'] or '<no units attr>'} "
                   f"(dtype {aschema['dtype']}), "
                   f"levels_sha256={str(aschema['levels_sha256'])[:12]}; "
                   f"{len(selectable)} level(s) selectable in 0-800 m; "
                   + ("matches " + ap12["cite"] if ap12.get("matches")
                      else "DOES NOT match "
                           + str(ap12.get("cite", ap12.get("note")))
                           + " — " + "; ".join(ap12.get("mismatches", []))))

    # ------------------------------------------------------------------- P6 ---
    # The builder is the authority on the path, so the check is that what was opened
    # above IS what the builder produces — including the double slash `data/` gives.
    # Comparing two calls to the same function would prove nothing; the strings
    # actually used are compared instead.
    expected = {"seasonal": group_path(store, "1_degree", "seasonal/Nutrients"),
                "annual": group_path(store, "1_degree", "annual/Nutrients"),
                "anchor": group_path(store, ANCHOR_GRID, ANCHOR_SUBGROUP)}
    same = seasonal == expected["seasonal"] and annual == expected["annual"]
    record("P6", same,
           f"paths surveyed: seasonal={seasonal!r} annual={annual!r}; "
           f"builder: {expected!r}")
    out["group_paths"] = expected

    # ------------------------------------------- the reachable-group existence map ---
    # Three questions and no others: does the group exist, is it reachable through
    # `api.query`'s own criteria, and does it open. **No HTTP request is sent** —
    # this runs before any service exists, which is why the field is
    # `query_reachable` and not `api_reachable`. **No depth invariant is applied here and
    # none exists.** Table 4 gives every variable and climatology its own depth row,
    # so there is nothing a group's mere existence implies about its depth axis —
    # asking that of twelve groups at once would be inventing a store-wide rule the
    # documentation does not state.
    #
    # Whether a group is missing is a fact about the store, not a failure: a missing
    # one is a candidate for the non-anchor characterization (spec 005 section 6.3).
    existence = []
    for grid, sub in REACHABLE_GROUPS:
        p = group_path(store, grid, sub)
        g, e = _open_at(p, cwd)
        existence.append({"grid": grid, "subgroup": sub, "path": p,
                          # `query_reachable`, not `api_reachable`: this survey runs
                          # before any service exists and sends no HTTP request. It
                          # means the group is reachable through `api.query`'s own
                          # logic — it survives the three 400 checks — and nothing
                          # about whether an HTTP request for it would succeed.
                          "query_reachable": True,    # by construction of this list
                          "query_reachable_meaning": QUERY_REACHABLE_MEANING,
                          "exists": g is not None,
                          "opens": g is not None,
                          "error": None if g is not None else e,
                          "depth_checked": False})
    out["reachable_groups"] = existence
    missing = [g for g in existence if not g["exists"]]
    out["missing_reachable_groups"] = [g["path"] for g in missing]
    out["non_anchor_characterization"] = (
        "available" if missing else "unavailable: every reachable group exists")

    out["depth_invariant_note"] = (
        "Depth ranges are per variable AND per climatology (WOA23 Table 4, "
        "pp. 11-12). 43 levels over 0-800 m applies ONLY to nitrate, phosphate and "
        "silicate under seasonal or monthly climatology. It is not TS's row "
        "(0-5500 m / 102 annual and seasonal, 0-1500 m / 57 monthly), not oxygen's "
        "(0-5500 / 102 annual, 0-1500 / 57 seasonal and monthly), not annual "
        "nitrate's (0-5500 m / 102), and not a property of the store. The "
        "twelve-group graph records existence, reachability and openability only "
        "and applies no depth condition to any group.")
    out["scope_note"] = (
        "This run characterizes: annual nitrate 0-800 m (supported), and WINTER "
        "nitrate — time_period=13 — at 3000-4000 m (out of range). It does not "
        "characterize time_period 14, 15 or 16, monthly nitrate, phosphate, "
        "silicate, TS or oxygen. No result here may be generalized to any of them; "
        "each would need its own case, evidence and request budget.")
    out["source_time_span_note"] = (
        "source_time_span (for the nutrients, 'all' = 1965-2022) is dataset "
        "provenance. It is not an API request parameter, appears in no group path, "
        "and is not a climatology. The API's climatology is annual / seasonal / "
        "monthly, carried by time_period.")
    out["preconditions_met"] = not out["problems"]
    out["depth_isolated"] = out["preconditions_met"]
    if not out["preconditions_met"]:
        out["verdict"] = ("STORE_SCHEMA_MISMATCH" if out.get("store_schema_mismatch")
                          else "PRECONDITION_UNMET")
        out["verdict_note"] = (
            ("the target group's depth schema does not match seasonal nitrate's "
             "WOA23 p12 Table 4 row, so a result from it may NOT be reported as a "
             "standard WOA23 seasonal-nitrate depth characterization. "
             if out.get("store_schema_mismatch") else "")
            + "depth behaviour cannot be isolated on the real store. The depth "
            "cases must not be issued and D1-depth-out-of-range stays "
            "CHARACTERIZATION PENDING. The store is not modified and no substitute "
            "data is created.")
    else:
        out["verdict"] = "PRECONDITIONS_MET"
        out["verdict_note"] = (
            f"depth is the isolated variable between the two cases: both groups "
            f"open, both carry nitrate, the seasonal group carries the required "
            f"time_period {REQUIRED_SEASONAL_PERIOD!r}, no depth level is selectable "
            f"in {OOR_DEPTH_MIN:.0f}-{OOR_DEPTH_MAX:.0f} m, and each group's measured "
            f"depth schema matches its own Table 4 row.")
    return out


def _open_at(path: str, cwd: str):
    """Open a store-relative group path from `cwd`, the way an arm would."""
    return open_group(_resolve(path, cwd))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--store", required=True,
                    help="the store literal, exactly as the arms are given it "
                         "(normally 'data/', resolved against --cwd)")
    ap.add_argument("--cwd", required=True,
                    help="the arm's working directory, which is what a relative "
                         "store literal resolves against")
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    result = survey(args.store, args.cwd)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2))

    print(f"== D1 store survey: {result['verdict']} ==")
    for name in sorted(result["checks"]):
        c = result["checks"][name]
        print(f"   {name} {'ok  ' if c['ok'] else 'FAIL'}  {c['detail']}")
    print(f"   reachable groups present: "
          f"{sum(1 for g in result['reachable_groups'] if g['exists'])}"
          f"/{len(result['reachable_groups'])}")
    print(f"   non-anchor characterization: {result['non_anchor_characterization']}")
    print(f"   required time_period: {REQUIRED_SEASONAL_PERIOD!r}; present in the "
          f"target group: {result.get('seasonal_periods_present')!r}")
    print(f"   NOTE {result['depth_invariant_note']}")
    print(f"   NOTE this survey read coordinate chunks; the lifespan anchor check's "
          f"zero-chunk property is unaffected and remains offline-audited")
    print(f"   {result['verdict_note']}")
    print(f"wrote {args.out}")
    # 3, not 1: a precondition that was not met is not a failure of the run, and a
    # caller must be able to tell the two apart without parsing the message.
    return 0 if result["preconditions_met"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
