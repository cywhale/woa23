"""Shared query definitions used by both the black-box (HTTP) and white-box (Zarr) benchmarks.

Keeping one list means an HTTP result and a Zarr result for the same `id` are
directly comparable: HTTP total minus Zarr total is the framework/serialisation
overhead.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Query:
    id: str
    # What this case is meant to expose.
    intent: str
    lon0: float
    lat0: float
    lon1: Optional[float] = None
    lat1: Optional[float] = None
    dep0: Optional[float] = None
    dep1: Optional[float] = None
    grid: str = "1"
    parameter: str = "temperature"
    time_period: str = "0"
    append: str = "mn"
    # Cases that are expected to be heavy; skipped unless --include-heavy.
    heavy: bool = False

    def params(self) -> dict:
        """Query-string parameters for the HTTP API."""
        out = {
            "lon0": self.lon0,
            "lat0": self.lat0,
            "grid": self.grid,
            "parameter": self.parameter,
            "time_period": self.time_period,
            "append": self.append,
        }
        for name in ("lon1", "lat1", "dep0", "dep1"):
            value = getattr(self, name)
            if value is not None:
                out[name] = value
        return out


# The grid resolutions in WOA23: 1-degree has all parameters, 0.25-degree only T/S.
QUERIES: list[Query] = [
    Query(
        id="point_profile",
        intent="single grid cell, full depth column -- the cheapest possible query; "
               "any time above a few ms here is pure overhead (open_zarr, dask, framework)",
        lon0=135, lat0=15,
        parameter="temperature",
        time_period="0",
    ),
    Query(
        id="point_profile_multiparam",
        intent="single cell, 3 parameters x 4 time periods -- fans out across several "
               "Zarr groups (TS / Oxy) and so several open_zarr calls",
        lon0=135, lat0=15,
        parameter="temperature,salinity,oxygen",
        time_period="0,1,2,13",
        append="mn,an",
    ),
    Query(
        id="readme_example",
        intent="the documented example URL: 15x5 degree box, shallow, 3 params, 4 periods",
        lon0=135, lat0=15, lon1=150, lat1=20,
        dep1=60,
        parameter="temperature,salinity,oxygen",
        time_period="0,1,2,13",
        append="mn,an",
    ),
    Query(
        id="small_bbox_full_depth",
        intent="same small box but every depth level -- isolates the depth-chunk cost",
        lon0=135, lat0=15, lon1=150, lat1=20,
        parameter="temperature",
        time_period="0",
    ),
    Query(
        id="regional_bbox",
        intent="30x20 degree region, annual T+S, shallow -- a realistic map/analysis request",
        lon0=110, lat0=10, lon1=140, lat1=30,
        dep1=200,
        parameter="temperature,salinity",
        time_period="0",
    ),
    Query(
        id="surface_global",
        intent="whole world at the surface -- the read pattern the current chunking "
               "(lat=90, lon=360) is actually optimised for",
        lon0=-180, lat0=-90, lon1=180, lat1=90,
        dep1=0,
        parameter="temperature",
        time_period="0",
        heavy=True,
    ),
    Query(
        id="point_profile_025",
        intent="0.25-degree grid, single cell full depth -- the finer store, worst "
               "read-amplification case for point queries",
        lon0=135, lat0=15,
        grid="0.25",
        parameter="temperature,salinity",
        time_period="0",
    ),
    Query(
        id="regional_bbox_025",
        intent="0.25-degree grid, 10x10 degree box, upper ocean",
        lon0=118, lat0=18, lon1=128, lat1=28,
        dep1=200,
        grid="0.25",
        parameter="temperature,salinity",
        time_period="0",
        heavy=True,
    ),
]


def select(ids: Optional[list[str]] = None, include_heavy: bool = False) -> list[Query]:
    if ids:
        wanted = set(ids)
        chosen = [q for q in QUERIES if q.id in wanted]
        missing = wanted - {q.id for q in chosen}
        if missing:
            raise SystemExit(f"unknown query id(s): {', '.join(sorted(missing))}")
        return chosen
    return [q for q in QUERIES if include_heavy or not q.heavy]
