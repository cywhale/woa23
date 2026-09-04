"""The D1 cases, and the separation from the 64 that must not erode.

The 64 contract cases are what C1 (`c1e`) and C2 (`c2f`) compared. A 65th makes
every future run incomparable with them, and the way that happens is not a decision
— it is someone appending to the wrong list. So the separation is asserted here, not
described in a comment.

    uv run python -m bench.test_d1_cases
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bench import contract_cases                              # noqa: E402
from bench.d1_cases import (                                  # noqa: E402
    ANNUAL_PERIOD, D1Case, DEFAULT_SEASONAL_PERIOD, READINESS_MAX_PER_ARM,
    REQUIRED_SEASONAL_PERIOD,
    READINESS_MIN_PER_ARM, RECOVERY, SEASONAL_PERIODS,
    STORE_PROBE_REQUESTS_PER_ARM, characterization_requests_per_arm,
    countable_requests_per_arm, depth_cases, request_total_range, scheduled,
)

PASS = 0
FAIL = 0


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


print("the 64 contract cases are untouched")
check("contract_cases still has 64 cases in total", 64, len(contract_cases.all_cases()))
check("split 33 JSON and 31 CSV replays", (33, 31),
      (len(contract_cases.CASES), len(contract_cases.csv_cases())))
ids = {c.id for c in contract_cases.all_cases()}
check("no D1 case leaked into the contract list", set(),
      {i for i in ids if i.startswith("D1-")})
check("the contract list has no idea this module exists", False,
      "d1_cases" in Path(contract_cases.__file__).read_text())

print()
print("a characterization case cannot carry an expectation")
# Structural, not conventional: there is no field to put one in. A `Case` has
# `expect_status`; a `D1Case` does not, so "we forgot to remove the expectation"
# cannot happen.
check("D1Case has no expect_status field", False,
      "expect_status" in D1Case.__dataclass_fields__)
check("D1Case has no csv_status field either", False,
      "csv_status" in D1Case.__dataclass_fields__)
check("contract Case does have one, which is the difference", True,
      "expect_status" in contract_cases.Case.__dataclass_fields__)

print()
print("the depth pair differs in exactly one selectable dimension")
cases = depth_cases()
check("four cases", 4, len(cases))
sup = next(c for c in cases if c.id == "D1-DEPTH-SUP")
oor = next(c for c in cases if c.id == "D1-DEPTH-OOR-tp13")
differing = {k for k in set(sup.params) | set(oor.params)
             if sup.params.get(k) != oor.params.get(k)}
check("only the climatology and the depths differ",
      {"time_period", "dep0", "dep1"}, differing)
check("same grid", "1", oor.params["grid"])
check("same variable", "nitrate", oor.params["parameter"])
check("the supported case is annual", ANNUAL_PERIOD, sup.params["time_period"])
check("the out-of-range case is seasonal", True,
      oor.params["time_period"] in SEASONAL_PERIODS)
check("supported depths are inside annual nitrate's 0-5500 m", (0, 800),
      (sup.params["dep0"], sup.params["dep1"]))
check("out-of-range depths are inside WOA23's 5500 m global maximum", True,
      oor.params["dep1"] <= 5500)
check("and outside seasonal nitrate's 0-800 m", True, oor.params["dep0"] > 800)

# C18 already covers depth beyond the global maximum. Repeating it would answer a
# question that has an answer.
c18 = next(c for c in contract_cases.CASES if c.id == "C18")
check("C18 is the beyond-the-global-maximum case, and this is not it", True,
      c18.params["dep0"] > 5500 and oor.params["dep0"] < 5500)

print()
print("no source_time_span anywhere — it is provenance, not a request parameter")
for c in cases + [RECOVERY]:
    for forbidden in ("all", "decav", "decav71A0", "A5B4", "B5C2", "source_time_span"):
        if forbidden in c.params or forbidden in str(c.params.get("time_period", "")):
            FAIL += 1
            print(f"  FAIL {c.id} carries {forbidden!r}")
            break
    else:
        PASS += 1
        print(f"  ok   {c.id} selects a climatology, not a time span")

print()
print("JSON and CSV are separate cases, never one case with two readings")
check("two endpoints appear", {"/api/woa23", "/api/woa23/csv"},
      {c.path for c in cases})
check("the same params are used for both", True,
      sup.params == next(c for c in cases if c.id == "D1-DEPTH-SUP-csv").params)
check("and they are distinct case ids", 4, len({c.id for c in cases}))

print()
print("the seasonal code is a number, and a wrong one is refused")
check("13 is accepted", "13", depth_cases("13")[2].params["time_period"])
check("16 is accepted", "16", depth_cases("16")[2].params["time_period"])
for bad in ("0", "1", "12", "seasonal", "", "99"):
    try:
        depth_cases(bad)
        FAIL += 1
        print(f"  FAIL {bad!r} was accepted as a seasonal code")
    except ValueError as exc:
        PASS += 1
        print(f"  ok   {bad!r} is refused ({str(exc)[:48]}...)")

print()
print("the recovery probe follows each case, and there are four of them")
sched = scheduled()
check("eight requests per arm", 8, len(sched))
check("they alternate case, recovery, case, recovery",
      ["D1-DEPTH-SUP", "D1-ANCHOR-RECOVER", "D1-DEPTH-SUP-csv", "D1-ANCHOR-RECOVER",
       "D1-DEPTH-OOR-tp13", "D1-ANCHOR-RECOVER", "D1-DEPTH-OOR-tp13-csv",
       "D1-ANCHOR-RECOVER"],
      [c.id for c in sched])
check("four recovery probes, not two", 4,
      sum(1 for c in sched if c.id == RECOVERY.id))
check("eight characterization requests per arm", 8,
      characterization_requests_per_arm())
check("the recovery probe targets the anchor group", "1_degree/annual/TS",
      RECOVERY.group)
check("and asks for nothing but the defaults", {"lon0", "lat0"}, set(RECOVERY.params))

print()
print("only the out-of-range cases depend on the survey establishing isolation")
check("the supported case does not", [False, False],
      [c.needs_depth_isolation for c in cases if c.id.startswith("D1-DEPTH-SUP")])
check("both out-of-range cases do", [True, True],
      [c.needs_depth_isolation for c in cases if c.id.startswith("D1-DEPTH-OOR")])

print()
print("the seasonal mapping matches api.query, not this module's comment")
# `determine_subgroup` is the authority. Importing `api.query` pulls in `api.config`,
# which validates a store at import — so it is exercised in a subprocess with a
# synthetic one rather than reimplemented here.
PROBE = r"""
import json, os, sys
sys.path.insert(0, os.environ["REPO"])
os.environ["WOA23_ZARR_STORE"] = os.environ["STORE"]
from api.query import determine_subgroup
out = {p: determine_subgroup("nitrate", p) for p in ["0", "1", "12", "13", "16"]}
out["ts"] = determine_subgroup("temperature", "0")
out["oxy"] = determine_subgroup("oxygen", "13")
print(json.dumps(out))
"""
with tempfile.TemporaryDirectory() as td:
    store = Path(td) / "store"
    (store / "1_degree" / "annual" / "TS").mkdir(parents=True)
    env = dict(os.environ, REPO=str(Path(__file__).resolve().parent.parent),
               STORE=str(store))
    r = subprocess.run([sys.executable, "-c", PROBE], capture_output=True, text=True,
                       env=env)
    if r.returncode != 0:
        FAIL += 1
        print(f"  FAIL could not read determine_subgroup: {r.stderr.strip()[-300:]}")
    else:
        got = json.loads(r.stdout)
        check("'0' is annual", "annual/Nutrients", got["0"])
        check("'1' is monthly", "monthly/Nutrients", got["1"])
        check("'12' is monthly", "monthly/Nutrients", got["12"])
        check("'13' is seasonal — the code this module uses",
              "seasonal/Nutrients", got["13"])
        check("'16' is seasonal too", "seasonal/Nutrients", got["16"])
        check("the default seasonal code really is seasonal", "seasonal/Nutrients",
              got[DEFAULT_SEASONAL_PERIOD])
        check("nitrate lands in Nutrients, not TS", "annual/TS", got["ts"])
        check("and oxygen in Oxy", "seasonal/Oxy", got["oxy"])
        check("the case's declared group matches determine_subgroup",
              "1_degree/" + got["13"], oor.group)
        check("and the supported case's too", "1_degree/" + got["0"], sup.group)

print()
print("the per-arm countable total is TEN, and eight is a different number")
# The mismatch this section exists to prevent: eight is the characterization
# requests, and it was once the constant a reader would reach for as the per-arm
# total. The store-readiness probes are issued too, they are countable, and leaving
# them out understates every figure downstream.
check("two store-readiness probes per arm", 2, STORE_PROBE_REQUESTS_PER_ARM)
check("so ten countable per arm, not eight", 10, countable_requests_per_arm())
check("and the characterization subtotal stays eight", 8,
      characterization_requests_per_arm())
check("the difference is exactly the store probes", STORE_PROBE_REQUESTS_PER_ARM,
      countable_requests_per_arm() - characterization_requests_per_arm())
check("twenty countable across both arms", 20, countable_requests_per_arm() * 2)

print()
print("the total is a range, and it is returned as one so it cannot become a number")
check("readiness is at least one per arm", 1, READINESS_MIN_PER_ARM)
check("and at most thirty", 30, READINESS_MAX_PER_ARM)
check("so the run's total is 22 to 80", (22, 80), request_total_range())
check("the minimum is the countable total plus one readiness probe per arm",
      countable_requests_per_arm() * 2 + 2, request_total_range()[0])
check("the maximum is the countable total plus thirty per arm",
      countable_requests_per_arm() * 2 + 60, request_total_range()[1])
check("it is a tuple, not a total", tuple, type(request_total_range()))

print()
print("the runner states the same numbers, and does not state eight as the total")
RUNNER = Path(__file__).resolve().parent.parent / "scripts" / "run_controlled.sh"
runner = RUNNER.read_text()
check("the characterization constant is named for what it counts", True,
      "BUDGET_D1_CHARACTERIZATION=8" in runner)
check("and the old ambiguous name is gone", False,
      "BUDGET_D1=" in runner)
check("the banner states the countable per-arm figure", True,
      "COUNTABLE per arm : $D1_COUNTABLE_PER_ARM" in runner)
check("it says the recovery probes are one per case", True,
      "ONE AFTER EACH CASE" in runner)
check("it says the total is a range and must be reported as one", True,
      "a RANGE, and must be reported as one" in runner)
check("and it derives the range rather than printing a literal", True,
      "$D1_TOTAL_MIN to $((BUDGET_ARM * 2))" in runner)

print()
print("a different season is a different case, and says so in its id")
# The substitution this replaces: the survey picked whichever season it found and
# the case kept its name, so a spring run would have been reported as the winter one.
check("the required code is winter", "13", REQUIRED_SEASONAL_PERIOD)
check("and it is what the default resolves to", REQUIRED_SEASONAL_PERIOD,
      DEFAULT_SEASONAL_PERIOD)
# Every seasonal case names its period, winter included. "No suffix means 13" was
# the same implicit default that let the survey substitute a season silently.
check("winter's ids name winter's period explicitly",
      ["D1-DEPTH-SUP", "D1-DEPTH-SUP-csv", "D1-DEPTH-OOR-tp13",
       "D1-DEPTH-OOR-tp13-csv"],
      [c.id for c in depth_cases("13")])
check("no case id relies on an absent suffix to mean 13", False,
      any(c.id in ("D1-DEPTH-OOR", "D1-DEPTH-OOR-csv") for c in depth_cases("13")))
check("and each carries its season in metadata", ("winter", "seasonal", "13"),
      tuple(str(getattr(oor, f, "") or oor.params.get(f, ""))
            for f in ("season", "climatology", "time_period")))
for other in ("14", "15", "16"):
    ids = [c.id for c in depth_cases(other)]
    check(f"time_period={other} renames the out-of-range cases",
          [f"D1-DEPTH-OOR-tp{other}", f"D1-DEPTH-OOR-tp{other}-csv"],
          [i for i in ids if "OOR" in i])
    check(f"and none of them is still called the {REQUIRED_SEASONAL_PERIOD} case",
          False, f"D1-DEPTH-OOR-tp{REQUIRED_SEASONAL_PERIOD}" in ids)
check("the annual cases are unaffected by the season", True,
      [c.id for c in depth_cases("13") if "SUP" in c.id]
      == [c.id for c in depth_cases("15") if "SUP" in c.id])
check("and the request really carries the season it is named for", "15",
      next(c for c in depth_cases("15")
           if c.id == "D1-DEPTH-OOR-tp15").params["time_period"])

print()
print("each case says what it is, so no report has to infer it from an id")
cases = depth_cases()
sup = next(c for c in cases if c.id == "D1-DEPTH-SUP")
oor = next(c for c in cases if c.id == "D1-DEPTH-OOR-tp13")
check("the supported case names its variable", "nitrate", sup.variable)
check("and its climatology", "annual", sup.climatology)
check("the out-of-range case is seasonal", "seasonal", oor.climatology)
check("its scope names WINTER, not 'seasonal nitrate'", True,
      oor.scope.startswith("winter nitrate (time_period=13)"))
check("and says so explicitly", True,
      "Not 'seasonal nitrate' as a whole" in oor.scope)
check("the supported case is judged against ANNUAL nitrate's row", True,
      "ANNUAL nitrate's Table 4 row, 102 levels over 0-5500 m" in sup.scope)
check("and the out-of-range one against SEASONAL nitrate's", True,
      "SEASONAL nitrate's Table 4 row, 43 levels over 0-800 m" in oor.scope)
check("the supported case's intent refuses the seasonal row", True,
      "Seasonal nitrate's 43/0-800 does not apply here" in sup.intent)
check("the recovery probe is the startup anchor, and says it is not a target", True,
      "NOT either characterization target" in RECOVERY.scope)
for other, name in (("14", "spring"), ("15", "summer"), ("16", "autumn")):
    c = next(x for x in depth_cases(other) if x.id.endswith(f"-tp{other}"))
    check(f"time_period={other} is named {name}", True, c.scope.startswith(f"{name} nitrate"))

print()
print("the module states what it does NOT characterize")
import bench.d1_cases as _dc
from bench.suite_summary import summary          # noqa: E402
doc = _dc.__doc__
for excluded in ("spring, summer or autumn", "monthly nitrate",
                 "phosphate or silicate", "TS or oxygen"):
    check(f"{excluded} is excluded in writing", True, excluded in doc)
check("and the annual case is judged against its own row", True,
      "Seasonal nitrate's 43 / 0-800 m does not apply to it" in doc)

print()
raise SystemExit(summary(PASS, FAIL))