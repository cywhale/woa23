"""The contract case list from spec 001 section 5.4, as executable definitions.

Every case here was run against live production while the spec was written, and the
`expect` field records what it actually returned — not what it ought to return.
Three of them pin behaviour that looks wrong (C5c, C5d/e, C13); the point of this
step is that S1 does not change it by accident, not that it is correct.

`csv_replay` marks the cases that C14 re-runs against `/api/woa23/csv`. C20 is
excluded: the Swagger routes are not data endpoints and have no CSV form. Note that
the CSV endpoint returns **400** where the JSON endpoint returns **200 []** for an
empty frame — a divergence that is part of today's contract.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Case:
    id: str
    intent: str
    params: dict
    expect_status: int = 200
    csv_status: int | None = None        # when the CSV replay differs; None = same
    csv_replay: bool = True
    path: str = "/api/woa23"


P = {"lon0": 135, "lat0": 15}            # the reference point used by most cases

CASES: list[Case] = [
    Case("C1", "all ten append variables; `ma` is absent from annual groups and is "
               "silently dropped",
         {**P, "append": "an,mn,dd,ma,sd,se,oa,gp,sdo,sea"}),
    Case("C2", "the mn rename is NOT applied", {**P, "append": "an"}),
    Case("C3", "the mn rename IS applied", {**P, "append": "mn"}),
    Case("C4", "all-land bbox (Sahara) -> rows with null, not an empty result",
         {"lon0": 15, "lat0": 22, "lon1": 20, "lat1": 26, "dep1": 0,
          "parameter": "temperature"}),
    Case("C5a", "the real 404 path: requested variable absent from that group",
         {**P, "time_period": "0", "append": "ma", "parameter": "temperature"},
         expect_status=404),
    Case("C5b", "same, seasonal Oxy has no sdo",
         {**P, "time_period": "13", "append": "sdo", "parameter": "oxygen"},
         expect_status=404),
    Case("C5c", "asymmetry: the same missing `ma` paired with a present `mn` gives "
                "200 and `ma` vanishes silently",
         {**P, "time_period": "0", "append": "ma,mn", "parameter": "temperature"}),
    Case("C5d", "out-of-range longitude -> 200 with [], CSV 400",
         {"lon0": 200, "lat0": 15, "parameter": "temperature"}, csv_status=400),
    Case("C5e", "out-of-range latitude", {"lon0": 135, "lat0": 95,
                                          "parameter": "temperature"}, csv_status=400),
    Case("C6", "the lon/lat swap logic",
         {"lon0": 150, "lat0": 20, "lon1": 135, "lat1": 15, "parameter": "temperature"}),
    Case("C7", "the depth swap logic",
         {**P, "dep0": 200, "dep1": 0, "parameter": "temperature"}),
    Case("C8", "0.25-degree with a 1-degree-only parameter -> 400",
         {**P, "grid": "0.25", "parameter": "oxygen"}, expect_status=400),
    Case("C9a", "invalid append -> 400", {**P, "append": "zzz"}, expect_status=400),
    Case("C9b", "invalid parameter -> 400", {**P, "parameter": "zzz"}, expect_status=400),
    Case("C9c", "invalid time_period -> 400", {**P, "time_period": "99"},
         expect_status=400),
    Case("C10a", "grid=1", {**P, "grid": "1"}),
    Case("C10b", "grid=0.25", {**P, "grid": "0.25"}),
    Case("C10c", "grid=25 parses as 0.25", {**P, "grid": "25"}),
    Case("C10d", "grid omitted", {**P}),
    Case("C11a", "annual boundary", {**P, "time_period": "0"}),
    Case("C11b", "seasonal boundary", {**P, "time_period": "16"}),
    Case("C12a", "single point, lon1/lat1 omitted", {**P}),
    Case("C12b", "single point, lon1 == lon0", {**P, "lon1": 135, "lat1": 15}),
    Case("C13", "antimeridian — whatever today's behaviour is, preserved",
         {"lon0": 175, "lat0": 15, "lon1": -175, "lat1": 20,
          "parameter": "temperature"}),
    Case("C15a", "only lon1 given", {**P, "lon1": 150}),
    Case("C15b", "only lat1 given", {**P, "lat1": 20}),
    Case("C16", "multi-group path-representation ordering",
         {**P, "parameter": "salinity,temperature", "time_period": "13,0",
          "append": "mn,an"}),
    Case("C17", "duplicate values exercise the set dedup",
         {**P, "parameter": "temperature,temperature", "append": "mn,mn"}),
    Case("C18", "depth beyond the 5,500 m maximum", {**P, "dep0": 6000, "dep1": 7000},
         csv_status=400),
    Case("C19a", "the ignored-parameter behaviour the harness depends on",
         {**P, "_cb": "fixed-probe-value"}),
    Case("C19b", "the same query without it", {**P}),
    Case("C20a", "documentation surface", {}, csv_replay=False,
         path="/api/swagger/woa23/openapi.json"),
    Case("C20b", "documentation surface", {}, csv_replay=False,
         path="/api/swagger/woa23"),
]


def csv_cases() -> list[Case]:
    """C14: every data case replayed against /api/woa23/csv."""
    return [
        Case(f"{c.id}-csv", c.intent, c.params,
             expect_status=c.csv_status or c.expect_status,
             csv_replay=False, path="/api/woa23/csv")
        for c in CASES if c.csv_replay
    ]


def all_cases() -> list[Case]:
    return CASES + csv_cases()
