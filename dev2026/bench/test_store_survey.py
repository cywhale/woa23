"""P1-P6 against synthetic stores that reproduce the confusions it exists to prevent.

The survey's whole job is to tell apart two things that look identical from outside:
a request that came back empty because the depth was outside the climatology's range,
and one that came back empty because `api.query` skipped the group entirely. So the
fixtures here are not "a good store and a broken store" — they are a good store and
**four stores that would each produce a plausible-looking wrong answer**.

Nothing here touches a real store, and no fixture is written outside a temporary
directory.

    uv run python -m bench.test_store_survey
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import xarray as xr

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench.store_survey import (                              # noqa: E402
    ANNUAL_CONTROL_GROUP, CASE_TARGET_GROUP, CASE_VARIABLE, DEFAULT_APPEND_VAR,
    NUTRIENT_VARIABLES, OOR_DEPTH_MAX, OOR_DEPTH_MIN, REACHABLE_GROUPS,
    REQUIRED_COORDS, REQUIRED_SEASONAL_PERIOD, WOA23_P12_DEPTH, compare_to_p12,
    depth_canonical_string, depth_schema, selectable_levels_in, survey,
)

PASS = 0
FAIL = 0
REPO = Path(__file__).resolve().parent.parent


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


def build(path, *, depths, params, periods, data_vars=("an", "mn")):
    """A Zarr v2 group with the coordinates `api.query` actually reads."""
    lon, lat = [135.5, 136.5], [15.5, 16.5]
    shape = (len(lon), len(lat), len(depths), len(params), len(periods))
    xr.Dataset(
        {v: (("lon", "lat", "depth", "parameters", "time_periods"),
             np.zeros(shape, dtype="float32")) for v in data_vars},
        coords={"lon": lon, "lat": lat, "depth": list(depths),
                "parameters": list(params), "time_periods": list(periods)},
    ).to_zarr(str(path), mode="w", consolidated=True)


NUTRIENT_PARAMS = ["nitrate", "phosphate", "silicate"]

# Table 4 is checked per variable AND per climatology, so the fixtures carry the
# real level counts rather than a token axis: annual nitrate is 102 levels over
# 0-5500 m and seasonal nitrate is 43 over 0-800 m. A fixture with three levels
# would now be a STORE_SCHEMA_MISMATCH, which is the behaviour under test.
def _axis(n, lo, hi):
    return [lo + (hi - lo) * i / (n - 1) for i in range(n)]


ANNUAL_DEPTHS = _axis(102, 0.0, 5500.0)                 # Table 4: annual nitrate
SEASONAL_DEPTHS = _axis(43, 0.0, 800.0)                 # Table 4: seasonal nitrate


def make_store(root, *, seasonal=True, seasonal_params=NUTRIENT_PARAMS,
               seasonal_periods=("13", "14", "15", "16"),
               seasonal_depths=SEASONAL_DEPTHS, complete=True,
               data_vars=("an", "mn")):
    """A store shaped like WOA23's, with one thing wrong at a time."""
    store = Path(root)
    build(store / "1_degree" / "annual" / "Nutrients",
          depths=ANNUAL_DEPTHS, params=NUTRIENT_PARAMS, periods=["0"],
          data_vars=data_vars)
    if seasonal:
        build(store / "1_degree" / "seasonal" / "Nutrients",
              depths=seasonal_depths, params=seasonal_params,
              periods=list(seasonal_periods), data_vars=data_vars)
    if complete:
        for grid, sub in REACHABLE_GROUPS:
            p = store / grid / sub
            if p.exists():
                continue
            build(p, depths=[0.0, 100.0], params=["temperature"], periods=["0"])
    return str(store)


print("a store shaped like WOA23 passes, and says depth is the isolated variable")
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "good")
    r = survey(store, td)
    check("verdict", "PRECONDITIONS_MET", r["verdict"])
    check("preconditions met", True, r["preconditions_met"])
    check("depth isolated", True, r["depth_isolated"])
    check("no problems", [], r["problems"])
    check("P1 opened the seasonal group", True, r["checks"]["P1"]["ok"])
    check("P2 found nitrate", True, r["checks"]["P2"]["ok"])
    check("P3 found a seasonal code", True, r["checks"]["P3"]["ok"])
    check("and it is the REQUIRED code, not a discovered one", "13",
          r["required_seasonal_period"])
    check("P4 found nothing selectable in the requested interval", True,
          r["checks"]["P4"]["ok"])
    check("and recorded the full depth schema, not just a range",
          {"n_levels", "min", "max", "levels", "dtype", "units",
           "monotonic_increasing", "levels_sha256", "canonicalization"},
          set(r["seasonal_depth"]))
    check("with the dtype", "float64", r["seasonal_depth"]["dtype"])
    check("and the canonicalization stated, so the hash is reproducible", True,
          "depth-levels/v1" in r["seasonal_depth"]["canonicalization"])
    check("the expected side names the row and says the level list is not "
          "transcribed", (43, False),
          (r["seasonal_depth_vs_p12"]["expected"]["levels"],
           r["seasonal_depth_vs_p12"]["expected"]["levels_list_transcribed"]))
    check("and the comparison says which row it applies to and only that", True,
          "not applied to any other" in r["seasonal_depth_vs_p12"]["applies_only_to"])
    check("with the level count", 43, r["seasonal_depth"]["n_levels"])
    check("the extent", (0.0, 800.0),
          (r["seasonal_depth"]["min"], r["seasonal_depth"]["max"]))
    check("and a digest over the levels themselves", 64,
          len(r["seasonal_depth"]["levels_sha256"]))
    check("no level is selectable in 3000-4000 m", [],
          r["selectable_levels_in_requested_range"])
    check("the schema was compared with SEASONAL NITRATE's Table 4 row", True,
          r["seasonal_depth_vs_p12"]["matches"])
    check("and the citation is the seasonal nitrate one", True,
          "Seasonal Climatology / Nitrate" in r["seasonal_depth_vs_p12"]["cite"])
    check("no schema mismatch", False, r["store_schema_mismatch"])
    check("P5 confirmed the annual control", True, r["checks"]["P5"]["ok"])
    check("with its OWN Table 4 row, 102 levels over 0-5500 m", (102, 5500.0),
          (r["annual_depth"]["n_levels"], r["annual_depth"]["max"]))
    check("compared against the annual citation, not the seasonal one", True,
          "Annual Climatology / Nitrate" in r["annual_depth_vs_p12"]["cite"])
    check("and it matches", True, r["annual_depth_vs_p12"]["matches"])
    check("P6 confirmed the paths came from the builder", True, r["checks"]["P6"]["ok"])
    check("every reachable group was checked", 12, len(r["reachable_groups"]))
    check("all present in this fixture", [], r["missing_reachable_groups"])
    check("so the non-anchor case is unavailable here", True,
          r["non_anchor_characterization"].startswith("unavailable"))
    check("and it says out loud that it read coordinate chunks", True,
          r["reads_coordinate_chunks"])
    check("and that depth ranges are per variable and per climatology", True,
          "per variable AND per climatology" in r["depth_invariant_note"])

print()
print("the four ways an empty response would NOT be about depth")

with tempfile.TemporaryDirectory() as td:
    # 1. The group is not there at all. The request would 500 at the group open.
    store = make_store(Path(td) / "nogroup", seasonal=False, complete=False)
    r = survey(store, td)
    check("P1 fails when the seasonal group is absent", False, r["checks"]["P1"]["ok"])
    check("verdict", "PRECONDITION_UNMET", r["verdict"])
    check("P2-P4 are reported as not run, not as failed coordinates", True,
          all("not run" in r["checks"][k]["detail"] for k in ("P2", "P3", "P4")))
    check("and the note refuses to call it depth characterization", True,
          "CHARACTERIZATION PENDING" in r["verdict_note"])
    check("it also says the store is not modified", True,
          "not modified" in r["verdict_note"])

with tempfile.TemporaryDirectory() as td:
    # 2. The group exists but has no nitrate: query.py skips it and answers 404.
    store = make_store(Path(td) / "nonitrate",
                       seasonal_params=["phosphate", "silicate"])
    r = survey(store, td)
    check("P2 fails when nitrate is absent from the coordinate", False,
          r["checks"]["P2"]["ok"])
    check("and P1 still passed, so the failure is attributed correctly", True,
          r["checks"]["P1"]["ok"])
    check("verdict", "PRECONDITION_UNMET", r["verdict"])

with tempfile.TemporaryDirectory() as td:
    # 3. The required period is not in the coordinate: also a 404, also not depth.
    store = make_store(Path(td) / "noperiod", seasonal_periods=["0"])
    r = survey(store, td)
    check("P3 fails when the required code is absent", False, r["checks"]["P3"]["ok"])
    check("verdict", "PRECONDITION_UNMET", r["verdict"])

with tempfile.TemporaryDirectory() as td:
    # 4. The seasonal axis DOES have a level in 3000-4000 m. Then the case is not
    #    out-of-range at all, and calling it that would be simply false.
    store = make_store(Path(td) / "deep",
                       seasonal_depths=_axis(42, 0.0, 800.0) + [3500.0])
    r = survey(store, td)
    check("P4 fails when a level is selectable in the interval", False,
          r["checks"]["P4"]["ok"])
    check("and the level is named", [3500.0],
          r["selectable_levels_in_requested_range"])
    # An axis reaching into 3000-4000 m cannot also be Table 4's 0-800 m axis, so
    # both signals fire and the more specific verdict is the one reported.
    check("verdict", "STORE_SCHEMA_MISMATCH", r["verdict"])
    check("the preconditions are unmet either way", False, r["preconditions_met"])

print()
print("a missing reachable group is a fact about the store, not a failure")
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "gap")
    # Remove one reachable group that is not part of P1-P6.
    import shutil
    shutil.rmtree(Path(store) / "1_degree" / "monthly" / "Oxy")
    r = survey(store, td)
    check("the preconditions still hold", True, r["preconditions_met"])
    check("the gap is reported", 1, len(r["missing_reachable_groups"]))
    check("and it is the one removed", True,
          r["missing_reachable_groups"][0].endswith("1_degree/monthly/Oxy"))
    check("so the non-anchor characterization becomes available", "available",
          r["non_anchor_characterization"])

print()
print("the reachable set is what query.py can actually reach — checked against it")
PROBE = r"""
import json, os, sys
sys.path.insert(0, os.environ["REPO"])
os.environ["WOA23_ZARR_STORE"] = os.environ["STORE"]
from api.config import grid_dir, time_periods
from api.query import determine_subgroup
# The same restriction query.py:115 applies: 0.25 degree is temperature/salinity only.
one_deg = ['temperature', 'salinity', 'oxygen', 'o2sat', 'AOU',
           'silicate', 'phosphate', 'nitrate']
quarter = ['temperature', 'salinity']
out = set()
for code, path in grid_dir.items():
    pars = quarter if code == '04' else one_deg
    for p in pars:
        for period in time_periods:
            out.add((path, determine_subgroup(p, period)))
print(json.dumps(sorted(out)))
"""
with tempfile.TemporaryDirectory() as td:
    st = Path(td) / "s"
    (st / "1_degree" / "annual" / "TS").mkdir(parents=True)
    env = dict(os.environ, REPO=str(REPO), STORE=str(st))
    r = subprocess.run([sys.executable, "-c", PROBE], capture_output=True, text=True,
                       env=env)
    if r.returncode != 0:
        FAIL += 1
        print(f"  FAIL could not enumerate from query.py: {r.stderr.strip()[-300:]}")
    else:
        reachable = {tuple(x) for x in json.loads(r.stdout)}
        check("the survey's list is exactly what query.py can reach",
              sorted(reachable), sorted(tuple(g) for g in REACHABLE_GROUPS))
        check("twelve of them", 12, len(reachable))

print()
print("an unreadable store is refused, never reported as an empty pass")
with tempfile.TemporaryDirectory() as td:
    r = survey("no_such_store/", td)
    check("verdict", "PRECONDITION_UNMET", r["verdict"])
    check("P1 failed", False, r["checks"]["P1"]["ok"])
    check("every reachable group is reported absent, not skipped", 12,
          len(r["missing_reachable_groups"]))

print()
print("the store literal is resolved the way an arm would resolve it")
with tempfile.TemporaryDirectory() as td:
    arm = Path(td) / "armdir"
    arm.mkdir()
    real = make_store(Path(td) / "realstore")
    os.symlink(real, arm / "data")
    r = survey("data/", str(arm))
    check("a relative literal resolves against the arm's cwd", True,
          r["preconditions_met"])
    check("the double slash the builder produces is preserved", True,
          "data//1_degree/seasonal/Nutrients" == r["group_paths"]["seasonal"])
    check("and the anchor path is recorded too", "data//1_degree/annual/TS",
          r["group_paths"]["anchor"])

print()
print("the CLI writes the artefact and separates 'unmet' from 'broken'")
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "good")
    out = Path(td) / "survey.json"
    r = subprocess.run(
        [sys.executable, "-m", "bench.store_survey", "--store", store,
         "--cwd", td, "--out", str(out)],
        capture_output=True, text=True, cwd=str(REPO),
        env=dict(os.environ, PYTHONPATH=str(REPO)))
    check("exit 0 when the preconditions hold", 0, r.returncode)
    check("the artefact was written", True, out.exists())
    check("and carries the coordinate-chunk note", True,
          json.loads(out.read_text())["reads_coordinate_chunks"])
    check("the output says the chunk note out loud", True,
          "read coordinate chunks" in r.stdout)

    store2 = make_store(Path(td) / "bad", seasonal=False, complete=False)
    out2 = Path(td) / "survey2.json"
    r2 = subprocess.run(
        [sys.executable, "-m", "bench.store_survey", "--store", store2,
         "--cwd", td, "--out", str(out2)],
        capture_output=True, text=True, cwd=str(REPO),
        env=dict(os.environ, PYTHONPATH=str(REPO)))
    # 3, not 1: a precondition that was not met is not a failure of the run, and a
    # caller has to be able to tell them apart without parsing the message.
    check("exit 3 when a precondition is unmet", 3, r2.returncode)
    check("the artefact is still written, so the reason survives", True, out2.exists())

print()
print("the substitution that used to happen silently now stops the run")
# A run that requested winter and a run that requested spring are different
# experiments. The earlier survey picked the first season present and the case kept
# its name, so the second would have been reported as the first.
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "spring", seasonal_periods=["14", "15", "16"])
    r = survey(store, td)
    check("13 absent is PRECONDITION_UNMET, not a substitution",
          "PRECONDITION_UNMET", r["verdict"])
    check("P3 failed", False, r["checks"]["P3"]["ok"])
    check("the other seasons present are recorded", ["14", "15", "16"],
          r["seasonal_periods_present"])
    check("and the detail says another season needs its own case id", True,
          "own case id" in r["checks"]["P3"]["detail"])
    check("nothing named a substituted period", False,
          "chosen_seasonal_period" in r)

print()
print("a schema that is not seasonal nitrate's is STORE_SCHEMA_MISMATCH")
# 3000-4000 m holds no level, so the depth question alone would pass — but the axis
# is not the one Table 4 describes, and a result from it may not be reported as a
# standard WOA23 seasonal-nitrate depth characterization.
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "wrongaxis", seasonal_depths=_axis(20, 0.0, 500.0))
    r = survey(store, td)
    check("nothing is selectable in the requested interval", [],
          r["selectable_levels_in_requested_range"])
    check("but the schema does not match Table 4", False,
          r["seasonal_depth_vs_p12"]["matches"])
    check("so the mismatch is flagged", True, r["store_schema_mismatch"])
    check("and the verdict names it", "STORE_SCHEMA_MISMATCH", r["verdict"])
    check("P4 fails", False, r["checks"]["P4"]["ok"])
    check("the note forbids calling it a standard characterization", True,
          "may NOT be reported as a standard" in r["verdict_note"])
    check("and the mismatch is spelled out", True,
          any("levels" in m for m in r["seasonal_depth_vs_p12"]["mismatches"]))

with tempfile.TemporaryDirectory() as td:
    # The annual control is judged against ITS row. An annual axis that happened to
    # match seasonal nitrate's 43/0-800 would be wrong, not right.
    store = make_store(Path(td) / "annualwrong")
    import shutil
    shutil.rmtree(Path(store) / "1_degree" / "annual" / "Nutrients")
    build(Path(store) / "1_degree" / "annual" / "Nutrients",
          depths=_axis(43, 0.0, 800.0), params=NUTRIENT_PARAMS, periods=["0"])
    r = survey(store, td)
    check("an annual axis matching the SEASONAL row is a P5 failure", False,
          r["checks"]["P5"]["ok"])
    check("and it is compared against the annual citation", True,
          "Annual Climatology / Nitrate" in r["annual_depth_vs_p12"]["cite"])
    check("with the shortfall named", True,
          any("102" in m for m in r["annual_depth_vs_p12"]["mismatches"]))

print()
print("Table 4 is per row, and the table holds only the rows this run requests")
check("seasonal nitrate is 43 levels over 0-800 m",
      (43, 0.0, 800.0),
      (WOA23_P12_DEPTH[("1_degree", "seasonal", "nitrate")]["levels"],
       WOA23_P12_DEPTH[("1_degree", "seasonal", "nitrate")]["min"],
       WOA23_P12_DEPTH[("1_degree", "seasonal", "nitrate")]["max"]))
check("annual nitrate is 102 levels over 0-5500 m, a different row",
      (102, 0.0, 5500.0),
      (WOA23_P12_DEPTH[("1_degree", "annual", "nitrate")]["levels"],
       WOA23_P12_DEPTH[("1_degree", "annual", "nitrate")]["min"],
       WOA23_P12_DEPTH[("1_degree", "annual", "nitrate")]["max"]))
check("the two rows differ, so neither may stand in for the other", True,
      WOA23_P12_DEPTH[("1_degree", "seasonal", "nitrate")]
      != WOA23_P12_DEPTH[("1_degree", "annual", "nitrate")])
# A combination this run does not request is not judged at all — inventing an
# expectation for it would be exactly the store-wide invariant Table 4 is not.
# Table 4 is recorded in full for the three parameter groups, so oxygen and
# monthly nitrate DO have rows and are judged against their own. What has no row is
# a combination the table does not cover — the quarter-degree grid, and a variable
# that is not in it at all.
check("seasonal oxygen has its own row, 57 levels over 0-1500 m",
      (57, 1500.0),
      (WOA23_P12_DEPTH[("1_degree", "seasonal", "oxygen")]["levels"],
       WOA23_P12_DEPTH[("1_degree", "seasonal", "oxygen")]["max"]))
check("monthly nitrate has its own row, and it is not seasonal nitrate's entry",
      True,
      WOA23_P12_DEPTH[("1_degree", "monthly", "nitrate")]
      is not WOA23_P12_DEPTH[("1_degree", "seasonal", "nitrate")])
check("monthly TS is 57 levels over 0-1500 m, unlike its annual row", (57, 1500.0),
      (WOA23_P12_DEPTH[("1_degree", "monthly", "temperature")]["levels"],
       WOA23_P12_DEPTH[("1_degree", "monthly", "temperature")]["max"]))
check("annual TS is 102 over 0-5500 m", (102, 5500.0),
      (WOA23_P12_DEPTH[("1_degree", "annual", "temperature")]["levels"],
       WOA23_P12_DEPTH[("1_degree", "annual", "temperature")]["max"]))
for absent in (("025_degree", "annual", "temperature"),
               ("1_degree", "annual", "chlorophyll"),
               ("1_degree", "decav", "nitrate")):
    got = compare_to_p12(depth_schema([0.0, 1.0]), absent)
    check(f"{absent} is not judged", False, got["checked"])
    check("and it says why", True, "must not judge it against another row" in got["note"])

print()
print("the twelve-group graph answers three questions and no depth question")
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "graph")
    r = survey(store, td)
    keys = set(r["reachable_groups"][0])
    check("existence, reachability, openability — and no depth field",
          {"grid", "subgroup", "path", "query_reachable", "query_reachable_meaning",
           "exists", "opens", "error", "depth_checked"}, keys)
    check("every entry says depth was not checked", True,
          all(g["depth_checked"] is False for g in r["reachable_groups"]))
    check("and every entry is query-reachable by construction", True,
          all(g["query_reachable"] for g in r["reachable_groups"]))
    # The old name read as "the HTTP API was tried and answered". This survey runs
    # before any service exists and sends no request.
    check("no entry claims an HTTP API reachability", False,
          any("api_reachable" in g for g in r["reachable_groups"]))
    check("and each says what the claim actually is",
          "query-path reachable by API logic; no HTTP request was sent",
          r["reachable_groups"][0]["query_reachable_meaning"])

print()
print("selectable_levels_in asks about levels, not about the maximum")
# An axis whose maximum exceeds the interval while holding nothing inside it: the
# earlier `max < 3000` test would have called this in-range and been wrong.
gappy = [0.0, 100.0, 800.0, 4500.0]
check("a gap over the interval yields no selectable level", [],
      selectable_levels_in(gappy, OOR_DEPTH_MIN, OOR_DEPTH_MAX))
check("even though the maximum is above it", True, max(gappy) > OOR_DEPTH_MAX)
check("a level inside the interval is found", [3500.0],
      selectable_levels_in([0.0, 3500.0, 5000.0], OOR_DEPTH_MIN, OOR_DEPTH_MAX))
check("the bounds are inclusive at both ends", [3000.0, 4000.0],
      selectable_levels_in([2999.0, 3000.0, 4000.0, 4001.0],
                           OOR_DEPTH_MIN, OOR_DEPTH_MAX))

print()
print("the depth schema digest changes when the levels do, not only their extent")
a = depth_schema([0.0, 100.0, 800.0])
b = depth_schema([0.0, 200.0, 800.0])
check("same count, same min, same max", (a["n_levels"], a["min"], a["max"]),
      (b["n_levels"], b["min"], b["max"]))
check("different levels, different digest", True,
      a["levels_sha256"] != b["levels_sha256"])
check("the levels themselves are kept too", [0.0, 100.0, 800.0], a["levels"])

print()
print("the two case groups are named for their cases, and neither is the anchor")
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "named")
    r = survey(store, td)
    check("the case target group is the seasonal Nutrients one",
          ("1_degree", "seasonal/Nutrients"), CASE_TARGET_GROUP)
    check("and it is labelled as this case's target, not a universal one", True,
          "NOT a universal startup invariant" in r["case_target_group"]["note"])
    check("it names the variable under characterization", "nitrate",
          r["case_target_group"]["variable_under_characterization"])
    check("and the time period", "13", r["case_target_group"]["time_period"])
    check("the annual control is a separate group",
          ("1_degree", "annual/Nutrients"), ANNUAL_CONTROL_GROUP)
    check("the startup anchor is a third thing, and is recorded as such", True,
          r["startup_anchor_group"]["path"].endswith("1_degree/annual/TS"))
    check("and is explicitly not re-validated here", True,
          "not surveyed here" in r["startup_anchor_group"]["note"])

print()
print("Nutrients is a group of three, and one of them is characterized")
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "three")
    r = survey(store, td)
    check("all three nutrient variables are in the store",
          ["nitrate", "phosphate", "silicate"], r["nutrient_variables_present"])
    check("but only nitrate is characterized", ["nitrate"],
          r["nutrient_variables_characterized"])
    check("and the record says the difference out loud", True,
          "must not be generalized to phosphate or silicate"
          in r["nutrients_group_note"])
    check("the three are named in the module too",
          ("nitrate", "phosphate", "silicate"), NUTRIENT_VARIABLES)
    check("the presence of three is called a store schema observation", True,
          "store schema observation" in r["checks"]["P2"]["detail"])

print()
print("P2 checks everything the read path needs before depth is consulted")
# A group missing `mn` returns an empty result_list and a 404; a group missing a
# coordinate raises inside ds.sel. Either would look like a depth outcome.
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "coords")
    r = survey(store, td)
    check("every coordinate query.py selects on is checked",
          set(REQUIRED_COORDS), set(r["case_target_coords"]))
    check("lon and lat are among them", True,
          {"lon", "lat"} <= set(REQUIRED_COORDS))
    check("all present in a good fixture", True,
          all(m["present"] for m in r["case_target_coords"].values()))
    check("the default append variable is checked too", DEFAULT_APPEND_VAR,
          r["case_target_append_var"]["name"])
    check("and found", True, r["case_target_append_var"]["present"])
    check("the annual control's coordinates are checked as well",
          set(REQUIRED_COORDS), set(r["annual_control_coords"]))
    check("and its append variable", True,
          r["annual_control_append_var"]["present"])

with tempfile.TemporaryDirectory() as td:
    # No `mn`: the request would 404 with result_list empty, not because of depth.
    store = make_store(Path(td) / "nomn", data_vars=("an",))
    r = survey(store, td)
    check("P2 fails when the default append variable is absent", False,
          r["checks"]["P2"]["ok"])
    check("and says which", True, "'mn' ABSENT" in r["checks"]["P2"]["detail"])
    check("verdict", "PRECONDITION_UNMET", r["verdict"])

print()
print("the depth hash canonicalization is stated, and it is what makes it stable")
levels = [0.0, 0.1, 800.0]
canon = depth_canonical_string(levels, "float64", "meters")
check("the version is in the string", True, canon.startswith("depth-levels/v1|"))
check("the dtype is part of it", True, "dtype=float64" in canon)
check("the units too", True, "units=meters" in canon)
check("and the count", True, "n=3|" in canon)
check("levels are fixed-point, not repr", True, "0.100000" in canon)
check("stored order is preserved, not sorted",
      depth_canonical_string([800.0, 0.0], None, None),
      "depth-levels/v1|dtype=None|units=None|n=2|800.000000,0.000000")

# The three ways two correct readers would otherwise disagree.
a = depth_schema([0.0, 0.1], dtype="float64", units="meters")
b = depth_schema([0.0, 0.1], dtype="float32", units="meters")
c = depth_schema([0.0, 0.1], dtype="float64", units="decibars")
d = depth_schema([0.1, 0.0], dtype="float64", units="meters")
check("a different dtype is a different digest", True,
      a["levels_sha256"] != b["levels_sha256"])
check("different units too", True, a["levels_sha256"] != c["levels_sha256"])
check("and a different order, even with the same values", True,
      a["levels_sha256"] != d["levels_sha256"])
check("the same axis twice is the same digest",
      a["levels_sha256"],
      depth_schema([0.0, 0.1], dtype="float64", units="meters")["levels_sha256"])
check("monotonicity is recorded", True, a["monotonic_increasing"])
check("and a descending axis is not called monotonic increasing", False,
      d["monotonic_increasing"])

print()
print("the scope is winter nitrate, and the record says so")
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "scope")
    r = survey(store, td)
    check("the scope note names winter and the time period", True,
          "WINTER" in r["scope_note"] and "time_period=13" in r["scope_note"])
    check("and excludes 14, 15, 16 and monthly", True,
          "does not characterize time_period 14, 15 or 16, monthly nitrate"
          in r["scope_note"])
    check("source_time_span is provenance, not a request parameter", True,
          "not an API request parameter" in r["source_time_span_note"])
    check("and not a climatology", True,
          "is not a climatology" in r["source_time_span_note"])
    check("the depth note forbids applying 43/800 beyond its own row", True,
          "applies ONLY to nitrate, phosphate and silicate under seasonal or "
          "monthly climatology" in r["depth_invariant_note"])
    check("and names TS's and oxygen's rows as different", True,
          "not TS's row" in r["depth_invariant_note"]
          and "not oxygen's" in r["depth_invariant_note"])

print()
print("the four digest properties, stated as the four they are")
# The canonicalization puts dtype and units INTO the hash, so identical numbers
# under different dtypes must not collide. An earlier report of this suite described
# the repr case as "float32 and float64 hash the same", which contradicts that
# clause; these four make the actual rule unmissable.
base = dict(depths=[0.0, 100.0, 800.0], dtype="float64", units="meters")
h = lambda **kw: depth_schema(**{**base, **kw})["levels_sha256"]
check("1. same dtype, units, order and values -> same digest", h(), h())
check("2. a different dtype -> DIFFERENT digest", True, h() != h(dtype="float32"))
check("3. different units -> DIFFERENT digest", True, h() != h(units="decibars"))
check("4. a different order -> DIFFERENT digest", True,
      h() != h(depths=[800.0, 100.0, 0.0]))
# And the case those four leave open: two values that differ only in float repr,
# under the SAME dtype, are the same level and hash the same. That is the
# fixed-point clause, and it is not a statement about dtype.
import numpy as _np2
from bench.suite_summary import summary          # noqa: E402
check("under one dtype, values differing only in repr are one level",
      h(depths=[0.0, float(_np2.float32(0.1))]), h(depths=[0.0, 0.1]))

print()
print("mn is a statistical-mean data field, and its absence is a request-path fact")
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "mnrole")
    r = survey(store, td)
    ap = r["case_target_append_var"]
    check("it is recorded as a data field, not a coordinate",
          "statistical-mean data field", ap["kind"])
    check("and is not among the coordinates checked", False,
          DEFAULT_APPEND_VAR in REQUIRED_COORDS)
    check("its role cites Table 2", True, "Table 2, p. 9" in ap["role"])
    check("and says it is not an oceanographic variable", True,
          "not an oceanographic variable and not a coordinate" in ap["role"])
    check("checking it is a request-path requirement, not a depth-schema one", True,
          "not a depth-schema requirement" in ap["role"])
    check("if absent: the query path cannot complete the request", True,
          "cannot complete the request" in ap["if_absent"])
    check("and the 404 must not be read as depth-out-of-range", True,
          "must NOT be recorded as a depth-out-of-range outcome" in ap["if_absent"])
    check("and it is not by itself evidence the store violates WOA23", True,
          "not by itself evidence that the store violates WOA23" in ap["if_absent"])

with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "nomn2", data_vars=("an",))
    r = survey(store, td)
    check("P2 fails", False, r["checks"]["P2"]["ok"])
    check("and the detail calls it a request-path precondition", True,
          "request-path precondition, not a depth-schema finding"
          in r["checks"]["P2"]["detail"])
    # The phrase appears — in its denial. Asserting the substring's absence would
    # have failed on the disclaimer that exists to prevent the claim.
    check("and explicitly declines to call it a WOA23 violation", True,
          "not by itself evidence that the store violates WOA23"
          in r["checks"]["P2"]["detail"])

print()
print("what P4 verifies, and what it does not")
with tempfile.TemporaryDirectory() as td:
    store = make_store(Path(td) / "claim")
    r = survey(store, td)
    scope = r["seasonal_depth_vs_p12"]["verification_scope"]
    check("the count and extent are verified", True,
          "Table 4 count and extent are verified" in scope)
    check("the observed list and digest are recorded", True,
          "full observed depth list and its canonical digest are recorded" in scope)
    check("equality with Table 3's standard depths is NOT asserted", True,
          "Equality with the complete Table 3 standard-depth list is NOT asserted"
          in scope)
    check("and it says why a matching count is not that", True,
          "does not establish that the levels are the standard depths" in scope)
    check("the expected side names its source as Table 4 only",
          "WOA23 Table 4 (count and extent only)",
          r["seasonal_depth_vs_p12"]["expected"]["source"])
    check("and the annual comparison carries the same scope note", True,
          "NOT asserted" in r["annual_depth_vs_p12"]["verification_scope"])

print()
raise SystemExit(summary(PASS, FAIL))