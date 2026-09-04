"""The D1 characterization cases — a separate list, and separate on purpose.

**These are not contract cases and must never be added to `bench.contract_cases`.**
The 64 cases there are what C1 (`c1e`) and C2 (`c2f`) compared, and a 65th would
make every future run incomparable with them. `bench/test_d1_cases.py` asserts the
separation rather than trusting this paragraph.

The difference is not only bookkeeping. A contract case carries `expect_status`, and
the gate fails when the response does not match it. **A characterization case carries
no expectation at all**, because its whole purpose is to find out what the real store
returns for a query nobody has run against it. Writing an expected status here would
answer the question this is being run to ask.

What *is* asserted about these responses lives elsewhere and is deliberately narrow:
that the candidate and the reference return the same bytes, that the process still
serves afterwards, and that cleanup completes (spec 005 sections 1 and 10).

The pair comes from WOA23 Product Documentation Table 4 (pp. 11-12): nitrate reaches
**5500 m annually** and stops at **800 m seasonally**. So the two requests differ
in exactly one dimension a user can select — the climatology — and the depths are
chosen so that one is inside its range and the other is outside its range while
staying inside WOA23's 5500 m global maximum. `C18` already covers depth beyond the
global maximum; that is a different question and this does not repeat it.

**What this characterizes, stated narrowly enough to be true:**

* **annual nitrate** at 0-800 m, judged against *annual* nitrate's row — 102 levels
  over 0-5500 m. Seasonal nitrate's 43 / 0-800 m does not apply to it.
* **winter nitrate**, `time_period=13`, at 3000-4000 m, judged against *seasonal*
  nitrate's row — 43 levels over 0-800 m.

**Not** spring, summer or autumn: only `13` is requested, and a case for another
season carries its own id. **Not** monthly nitrate, which has its own row and would
need its own case, evidence and request budget. **Not** phosphate or silicate: the
`Nutrients` group holds all three and this characterizes one. **Not** TS or oxygen,
whose Table 4 rows are different again.
"""

from dataclasses import dataclass

# The same reference point as the contract cases, so the geography is not a second
# variable between this run and everything already measured.
P = {"lon0": 135, "lat0": 15}

#: `time_period` is a NUMERIC CODE, not a word. `api.config.time_periods` is keyed
#: '0'..'16' and `api.query.determine_subgroup` maps '0' to annual, '1'-'12' to
#: monthly and everything else to seasonal — so a seasonal request is `13` (winter),
#: never `"seasonal"`. `bench/test_d1_cases.py` checks this against `determine_subgroup`
#: itself rather than against this comment.
ANNUAL_PERIOD = "0"
SEASONAL_PERIODS = ("13", "14", "15", "16")     # winter, spring, summer, autumn

#: The seasonal code this run requests: **winter, and only winter.** Not a default
#: that something may quietly replace. An earlier survey chose the first seasonal
#: code present in the group, which meant a run could become a different experiment
#: and still be reported as the `time_period=13` case.
REQUIRED_SEASONAL_PERIOD = "13"
DEFAULT_SEASONAL_PERIOD = REQUIRED_SEASONAL_PERIOD

#: The season each code names, so a report can say "winter" instead of "seasonal".
#: Only `13` is requested by this run; the others exist so that a case built for one
#: of them is named for it rather than inheriting winter's description.
SEASON_NAMES = {"13": "winter", "14": "spring", "15": "summer", "16": "autumn"}


@dataclass(frozen=True)
class D1Case:
    """One characterization request.

    No `expect_status`, and there is no field to put one in. That is the difference
    from `contract_cases.Case`, and it is structural so it cannot be forgotten.

    The variable, climatology and Table 4 row travel WITH the case rather than being
    implied by its id. A record that says only `D1-DEPTH-OOR` invites "the seasonal
    nitrate result"; a record carrying `climatology=seasonal`, `variable=nitrate`,
    `time_period=13` invites nothing, because it says exactly what was asked.
    """
    id: str
    intent: str
    params: dict
    path: str
    #: the group this request is designed to reach, so the preflight can be checked
    #: against the case rather than against a comment
    group: str
    #: the variable actually requested. `Nutrients` is a group of three; this is the
    #: one of them under characterization, and no result generalises to the others.
    variable: str = ""
    #: annual / seasonal / monthly — the API climatology, never a source_time_span
    climatology: str = ""
    #: the human name, naming the season rather than the group. Used in reports so
    #: "seasonal nitrate" cannot stand in for "winter nitrate".
    scope: str = ""
    #: winter / spring / summer / autumn for a seasonal case, "annual" for the
    #: annual one. Carried so a record never needs a lookup table to be read.
    season: str = ""
    #: `True` for the case whose outcome is only meaningful if P1-P6 established that
    #: depth is the isolated variable
    needs_depth_isolation: bool = False


#: Issued immediately after EACH characterization case, before the next one begins.
#: Four cases therefore mean four of these per arm. Batching them at the end would
#: only show that the process survived all four together, and "the failure was
#: request-level" would not be attributable to any one case (spec 005 section 6.2).
RECOVERY = D1Case(
    "D1-ANCHOR-RECOVER",
    "the anchor still serves after the case that preceded this probe",
    dict(P),                                    # defaults: temperature, annual, TS
    "/api/woa23",
    "1_degree/annual/TS",
    variable="temperature",
    climatology="annual",
    season="annual",
    scope="startup anchor group, annual temperature — the group the candidate "
          "validates at startup, which is NOT either characterization target",
)


def depth_cases(seasonal_period: str = DEFAULT_SEASONAL_PERIOD) -> list[D1Case]:
    """The four depth cases, JSON and CSV recorded separately.

    `seasonal_period` may be something other than `13`, and then **the case ids
    change with it** — `D1-DEPTH-OOR-tp14` rather than `D1-DEPTH-OOR`. That is the
    point of the parameter. A run that requested winter and a run that requested
    spring are different experiments, and the earlier arrangement — the survey
    substituting whichever season it found while the case kept its name — would have
    reported the second as the first.

    If `13` is absent from the target group the preconditions are unmet and no
    characterization request is issued at all. Asking for another season is a
    decision needing its own case id and its own authorisation, not a fallback
    (spec 005 section 6.1).

    JSON and CSV are separate cases and are never assumed to agree:
    `app.get_woa23_csv` raises 400 on an empty frame and `app.get_woa23` has no such
    branch, which `C5d`, `C5e` and `C18` already show in the contract results.
    """
    if seasonal_period not in SEASONAL_PERIODS:
        raise ValueError(
            f"{seasonal_period!r} is not a seasonal time_period code. "
            f"`determine_subgroup` sends {SEASONAL_PERIODS} to the seasonal groups; "
            f"'0' is annual and '1'-'12' are monthly.")

    # EVERY seasonal case names its period in its id, including winter's. "No suffix
    # means 13" is the same implicit default that let the survey substitute a season
    # silently: it puts the reader one convention away from knowing what was asked.
    suffix = f"-tp{seasonal_period}"
    supported = {**P, "grid": "1", "parameter": "nitrate",
                 "time_period": ANNUAL_PERIOD, "dep0": 0, "dep1": 800}
    out_of_range = {**P, "grid": "1", "parameter": "nitrate",
                    "time_period": seasonal_period, "dep0": 3000, "dep1": 4000}
    season = SEASON_NAMES[seasonal_period]
    sup_scope = ("annual nitrate, 1_degree/annual/Nutrients — judged against ANNUAL "
                 "nitrate's Table 4 row, 102 levels over 0-5500 m")
    oor_scope = (f"{season} nitrate (time_period={seasonal_period}), "
                 f"1_degree/seasonal/Nutrients — judged against SEASONAL nitrate's "
                 f"Table 4 row, 43 levels over 0-800 m. Not 'seasonal nitrate' as a "
                 f"whole: only {season} is requested.")
    return [
        D1Case("D1-DEPTH-SUP",
               "annual nitrate, 0-800 m — inside ANNUAL nitrate's own 0-5500 m row "
               "(WOA23 Table 4). Seasonal nitrate's 43/0-800 does not apply here",
               supported, "/api/woa23", "1_degree/annual/Nutrients",
               variable="nitrate", climatology="annual", season="annual",
               scope=sup_scope),
        D1Case("D1-DEPTH-SUP-csv",
               "the same request against the CSV endpoint, recorded separately",
               supported, "/api/woa23/csv", "1_degree/annual/Nutrients",
               variable="nitrate", climatology="annual", season="annual",
               scope=sup_scope),
        D1Case(f"D1-DEPTH-OOR{suffix}",
               f"{season} nitrate (time_period={seasonal_period}), 3000-4000 m — "
               f"inside WOA23's 5500 m global maximum and outside SEASONAL "
               f"nitrate's 0-800 m row (WOA23 Table 4)",
               out_of_range, "/api/woa23", "1_degree/seasonal/Nutrients",
               variable="nitrate", climatology="seasonal", season=season,
               scope=oor_scope, needs_depth_isolation=True),
        D1Case(f"D1-DEPTH-OOR{suffix}-csv",
               "the same request against the CSV endpoint, recorded separately",
               out_of_range, "/api/woa23/csv", "1_degree/seasonal/Nutrients",
               variable="nitrate", climatology="seasonal", season=season,
               scope=oor_scope, needs_depth_isolation=True),
    ]


def scheduled(seasonal_period: str = DEFAULT_SEASONAL_PERIOD) -> list[D1Case]:
    """Every request this run issues, in order: each case followed by its recovery.

    The interleaving is the point, so it is produced here rather than left to the
    caller to remember.
    """
    out: list[D1Case] = []
    for case in depth_cases(seasonal_period):
        out.append(case)
        out.append(RECOVERY)
    return out


#: The store-readiness probes every mode issues before any gate: two per arm, in
#: both orders. They are countable and they are part of the per-arm total, which is
#: why this constant exists rather than living only in the runner — the figure
#: `BUDGET_D1` once carried was the characterization requests alone, and it was read
#: as the per-arm countable total.
STORE_PROBE_REQUESTS_PER_ARM = 2

#: Process readiness. NOT counted: `process_ready` does not count its attempts, so
#: the run's total is a range and must be reported as one (spec 005 section 8.2).
READINESS_MIN_PER_ARM = 1
READINESS_MAX_PER_ARM = 30


def characterization_requests_per_arm(
        seasonal_period: str = DEFAULT_SEASONAL_PERIOD) -> int:
    """The four cases plus their four recovery probes. **Not the per-arm total.**"""
    return len(scheduled(seasonal_period))


def countable_requests_per_arm(seasonal_period: str = DEFAULT_SEASONAL_PERIOD) -> int:
    """Every request this run issues per arm that the harness can actually count.

    The characterization requests **plus the two store-readiness probes** — ten, not
    eight. Readiness is excluded because it is not counted, not because it is not
    issued.
    """
    return (characterization_requests_per_arm(seasonal_period)
            + STORE_PROBE_REQUESTS_PER_ARM)


def request_total_range(seasonal_period: str = DEFAULT_SEASONAL_PERIOD) -> tuple:
    """(minimum, maximum) requests across both arms, readiness included as a range.

    A range and not a number, and it is returned as one so a report cannot quietly
    turn it into a total.
    """
    countable = countable_requests_per_arm(seasonal_period) * 2
    return (countable + READINESS_MIN_PER_ARM * 2,
            countable + READINESS_MAX_PER_ARM * 2)
