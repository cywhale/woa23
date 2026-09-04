"""The WOA23 read pipeline. Port of `woa23_app.process_woa23_data`.

**Three** behavioural changes now, and nothing else. Two are spec 001's; the third is
spec 008's row-order contract, added later and listed as 3 below. The count is stated
because "everything else is verbatim" is only worth reading if the exceptions are
enumerated.

1. **No Dask.** `woa23_app.py:17` installs a distributed client as the process-wide
   default scheduler, so every `.compute()` — triggered inside `to_dataframe()` —
   round-trips through a scheduler backed by one worker shared with `tide_app` and
   `mhw_app`. Measured, that costs 1.35x-8.4x across every query shape from 102 to
   64,800 rows. `chunks=None` disables it; the selection materialises at
   `to_dataframe()`, which is where it materialised before.
2. **`pivot(on=)` instead of `pivot(columns=)`**, deprecated in polars 1.27.1 and
   emitting a warning on every request today. Verified equivalent — identical frame
   contents and identical column order.
3. **The row-order contract** (spec 008, decided by the PI on 2026-08-19): rows ascend
   by `(time_period, depth, lat, lon)`, `time_period` numerically, binding JSON and
   CSV alike. **This is an API contract change, not a refactor** — it changes the
   sequence of rows a client receives, and a client reading rows positionally will see
   different rows at the same index. It changes no value, no column name, no column
   order, no status and no query semantic. Whether any consumer depends on the old
   order is **unknown**; spec 008 §2 carries that as risk rather than assuming it away.

`from __future__ import annotations` is deliberately **absent**. The original does
not have it, and under it FastAPI sees annotations as strings rather than objects —
which is precisely what it introspects to build the OpenAPI document. An earlier
draft added it out of habit; that is an undeclared difference with a plausible route
to changing the very artefact §5.4 C20 byte-compares.

Everything else is deliberately verbatim, bugs included, because S1's before/after
has to attribute to the Dask change alone:

Three assignments that are never read — `all_columns`, `start_time`,
`appending_start_time`, leftovers from commented-out timing prints — are kept. An
earlier draft dropped them as dead code; that was a judgement the spec had not
authorised, and "it is obviously harmless" is how a verbatim port stops being one.

* `list(set(...))` at three points leaves column and row order dependent on
  `PYTHONHASHSEED`. That was a real defect and it belonged to S2b; both arms of the
  contract gate run with the seed pinned so it could not contaminate this comparison.
  **S2b has since fixed the ROW half of it** — see the sort at the end of
  `process_woa23_data`, which is spec 008's contract and the one behavioural change in
  this file that spec 001 did not ask for. The **column** half is untouched and still
  hash-dependent: spec 008 §3 puts it out of scope, because column order is a separate
  contract question and answering two in one commit answers neither cleanly.
* `pl.from_pandas` defaults to `nan_to_null=True`, so NaN becomes null before
  serialisation. Land cells surface as `null` in JSON and as an **empty field** in
  CSV, not the string `NaN`. Any future rewrite that builds the frame without
  pandas must convert explicitly — the JSON gate cannot see the difference.
* A requested `append` variable absent from the target group makes `result_list`
  empty and raises 404, unless another requested variable exists, in which case the
  response is 200 and the missing one silently vanishes. Preserved, not fixed.
"""

import math
from datetime import datetime
from typing import Optional

import numpy as np
import polars as pl
import xarray as xr
from fastapi import HTTPException

from api.config import (
    available_vars, grid_dir, time_periods, zarr_store_path,
)
from api.store_paths import group_path


def to_lowest_grid_point(lon: float, lat: float, grid_size: float) -> tuple:
    # Calculate the grid snapping offset based on grid size
    offset = 0.5 * grid_size

    # Snap longitude and latitude to the nearest grid points
    grid_lon = (math.floor(lon / grid_size) * grid_size) + offset
    grid_lat = (math.floor(lat / grid_size) * grid_size) + offset

    return grid_lon, grid_lat


def determine_subgroup(param, period):
    param_group = 'Nutrients'
    if param in ['temperature', 'salinity']:
        param_group = 'TS'
    elif param in ['oxygen', 'o2sat', 'AOU']:
        param_group = 'Oxy'

    if period == '0':
        subgroup = f'annual/{param_group}'
    elif period in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']:
        subgroup = f'monthly/{param_group}'
    else:
        subgroup = f'seasonal/{param_group}'

    return subgroup


#: The index columns, in their fixed order, ahead of every data column.
#:
#: Pinned by every recorded header in the tree — spec 006's
#: `lon,lat,depth,time_period,nitrate` and `bench/test_contract.py`'s
#: `lon,lat,depth,time_period,t`. Those examples carry a single data column each, so they
#: settle these four and say nothing about what follows; spec 015 section 4.1 records that,
#: and the parameter-major rule below was decided rather than inferred from them.
INDEX_COLUMNS = ("lon", "lat", "depth", "time_period")


def canonical_column_order(present, pars, variables):
    """The canonical column order for `present`, per spec 015 section 4.

    `present` is the frame's own column names. `pars` and `variables` are the requested
    parameters and statistics; only their MEMBERSHIP is read, never their order, which is
    the whole point — the request said `append=mn,an` in one breath and `an,mn` in the
    next and both must come out the same.

    Parameter-major: every statistic of one parameter, then the next parameter.
    `available_pars` orders the outer loop, `available_vars` the inner. A `mn` column has
    been renamed to the bare parameter name by the time this is called, so it is looked up
    under that name — but it keeps the SLOT `mn` occupies in `available_vars`, which is
    why the rename is handled here rather than by appending the bare name somewhere else.

    Returns a list that is a PERMUTATION of `present`: the caller asserts it, because a
    projection that quietly dropped a column would trade a visible ordering bug for an
    invisible data-loss one.
    """
    present = list(present)
    remaining = set(present)
    order = []

    for col in INDEX_COLUMNS:
        if col in remaining:
            order.append(col)
            remaining.discard(col)

    # `available_pars` is per-grid and local to the caller, so it arrives as `pars`'
    # canonical sequence; `available_vars` is module-level and canonical as declared.
    for param in pars:
        for var in variables:
            # `mn` wears the bare parameter name after the rename, and keeps mn's position.
            col = param if var == "mn" else f"{param}_{var}"
            if col in remaining:
                order.append(col)
                remaining.discard(col)

    # Anything the rules above did not name — a column from a shape not anticipated here —
    # is kept, in the frame's own order, rather than dropped. Sorted for determinism: the
    # frame's order is exactly what is not trustworthy.
    order.extend(sorted(remaining))
    return order


def custom_json_serializer(obj):
    # Unreferenced in the original too; carried over so the port is a faithful one.
    if isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
    return obj


async def process_woa23_data(lon0: float, lat0: float, lon1: Optional[float], lat1: Optional[float], dep0: Optional[float], dep1: Optional[float], grid: Optional[str], append: Optional[str], parameter: Optional[str], time_period: Optional[str]):
    init_time = datetime.now()

    if grid is None:
        grid = '01'
    else:
        grid = '04' if '25' in str(grid) else '01'

    gridSz = 0.25 if grid == '04' else 1.0
    grid_path = grid_dir[grid]

    if append is None:
        append = 'mn'

    # `set` to dedup, then CANONICAL ORDER from `available_vars` — not the request's order
    # and not the set's. The bare `list(set(...))` here was one of the five unordered
    # containers that reached column order (spec 015 section 3); `an,mn` and `mn,an` must
    # produce the same output, and a hash seed must not produce a different one.
    requested_vars = {var.strip() for var in append.split(',') if var.strip() in available_vars}
    variables = [var for var in available_vars if var in requested_vars]
    if not variables:
        raise HTTPException(
            status_code=400, detail=f"Invalid variables. Allowed variables are {', '.join(available_vars)}")

    if parameter is None:
        parameter = 'temperature'

    available_pars = ['temperature', 'salinity'] if gridSz == 0.25 else ['temperature', 'salinity', 'oxygen', 'o2sat', 'AOU', 'silicate', 'phosphate', 'nitrate']

    # Same treatment as `variables`: dedup by set, order by the canonical declaration.
    requested_pars = {c.strip() for c in parameter.split(',') if c.strip() in available_pars}
    pars = [c for c in available_pars if c in requested_pars]
    if not pars:
        raise HTTPException(
            status_code=400, detail=f"Invalid parameters. Allowed parameters are {', '.join(available_pars)} for grid size = {gridSz}")

    if time_period is None:
        time_period = '0'

    periods = list(set([p.strip() for p in str(time_period).split(
        ',') if p.strip() in list(time_periods)]))
    if not periods:
        raise HTTPException(
            status_code=400, detail=f"Invalid time_periods. Allowed time_periods are {', '.join(list(time_periods))}")
    periods.sort()  # in-place sort not return anything
    print("Handling parameters and time_periods: ", pars, periods)

    # Load the appropriate Zarr group
    # Note some parameters and time_periods belong to the same subgroups in zarr.
    # Use `set` to prevent duplicated zarr_group_paths being appended.
    zarr_group_paths = set()
    for param in pars:
        for period in periods:
            subgroup = determine_subgroup(param, period)
            zarr_group_paths.add(group_path(zarr_store_path, grid_path, subgroup))
    # Iterated in sorted order, not set order. The set is right for deduplication and wrong
    # for traversal: it decided which group was opened first, hence the order frames were
    # concatenated, hence the order `pivot` discovered columns in (spec 015 section 3).
    zarr_group_paths = sorted(zarr_group_paths)

    if dep0 is None:
        dep0 = 0

    if dep1 is None:
        dep1 = 5501  # max depth in WOA23 is 5500m

    if dep0 <= dep1:
        depth_min, depth_max = dep0, dep1
    else:
        depth_min, depth_max = dep1, dep0

    if lon1 is None or lat1 is None or (lon0 == lon1 and lat0 == lat1):
        # Only one point
        lon0, lat0 = to_lowest_grid_point(lon0, lat0, gridSz)
        lon_min, lon_max = lon0, lon0+0.1
        lat_min, lat_max = lat0, lat0+0.1
    else:
        # Bounding box
        lon0, lat0 = to_lowest_grid_point(lon0, lat0, gridSz)
        lon1, lat1 = to_lowest_grid_point(lon1, lat1, gridSz)

        if lon0 <= lon1:
            lon_min, lon_max = lon0, lon1+0.1
        else:
            lon_min, lon_max = lon1, lon0+0.1

        if lat0 <= lat1:
            lat_min, lat_max = lat0, lat1+0.1
        else:
            lat_min, lat_max = lat1, lat0+0.1

    result_list = []
    # THE VALUE COLUMNS THIS QUERY WOULD PRODUCE WITH DATA, recorded as they are decided.
    #
    # `pivot` derives its value columns from the VALUES of `parameter_variable`, so a
    # zero-row frame pivots to the index columns alone and the parameter columns vanish.
    # The header of an empty CSV then differs from the header of a non-empty one for the
    # same query -- exactly what spec 006 section 5 warned about and what D-3 left open.
    #
    # These names are not invented here: they are the same `{param}_{var}` the pivot
    # builds and the same rule `canonical_column_order` uses to place them, recorded at
    # the point where the (parameter, variable) pair is already known to have a data
    # array. A parameter with no array contributes nothing, so an empty result never
    # gains a column a full one would not have had.
    expected_value_columns = []
    all_columns = set()

    start_time = datetime.now()
    for zarr_group_path in zarr_group_paths:
        # THE CHANGE (spec 001 section 4.4): chunks=None disables Dask entirely.
        ds = xr.open_zarr(zarr_group_path, chunks=None)

        # Ensure the selected parameters exist in the dataset
        existing_params = set(ds.coords['parameters'].values)
        selected_params = existing_params.intersection(pars)

        if not selected_params:
            continue

        # Ensure the selected time periods exist in the dataset
        existing_periods = set(ds.coords['time_periods'].values)
        selected_periods = existing_periods.intersection(periods)
        if not selected_periods:
            continue

        # Select the appropriate data based on the query parameters
        filtered_data = ds.sel(
            lon=slice(lon_min, lon_max),
            lat=slice(lat_min, lat_max),
            depth=slice(depth_min, depth_max),
            # Both come from set intersections, so both are ordered here rather than left
            # to the set. `pars` and `periods` are already canonical, so filtering by them
            # carries that order through instead of discarding it.
            parameters=[p for p in pars if p in selected_params],
            time_periods=[p for p in periods if p in selected_periods]
        )

        appending_start_time = datetime.now()
        group_params = [p for p in pars if p in selected_params]
        for var in variables:
            if var in filtered_data:
                for _p in group_params:
                    _col = _p if var == "mn" else f"{_p}_{var}"
                    if _col not in expected_value_columns:
                        expected_value_columns.append(_col)
                data = filtered_data[var].to_dataframe().reset_index()
                # Convert to polars directly. nan_to_null defaults to True here,
                # which is why land cells reach the serialiser as null.
                data_polars = pl.from_pandas(data)
                data_polars = data_polars.with_columns([
                    pl.lit(var).alias("variable_type"),
                    pl.col(var).alias("value")
                ])
                # Drop original var columns if exist
                data_polars = data_polars.drop(var)
                result_list.append(data_polars)

    if not result_list:
        raise HTTPException(status_code=404, detail="No data found for the specified query parameters")

    # Concatenate the dataframes
    result_df = pl.concat(result_list, how="vertical")

    # Combine the parameter and variable type columns
    result_df = result_df.with_columns(
        (pl.col("parameters") + "_" + pl.col("variable_type")).alias("parameter_variable")
    )

    # Pivot to wide format. `on=` replaces the deprecated `columns=` (spec 001
    # section 4.5); verified to give identical contents and identical column order.
    result_df = result_df.pivot(
        index=["lon", "lat", "depth", "time_periods"],
        on="parameter_variable",
        values="value"
    )

    # Optionally rename {param}_mn to {param} if `mn` is present in the query variables
    if 'mn' in variables:
        rename_dict = {f"{param}_mn": param for param in pars if f"{param}_mn" in result_df.columns}
        if rename_dict:  # Check if there are columns to rename
            result_df = result_df.rename(rename_dict)

    result_df = result_df.rename({"time_periods": "time_period"})

    # A ZERO-ROW RESULT STILL CARRIES ITS VALUE COLUMNS.
    #
    # One query, one schema, whether or not it returns rows (spec 006 section 5.3,
    # option B). Without this the CSV header for a valid empty result is
    # `lon,lat,depth,time_period` while the same query with data returns
    # `lon,lat,depth,time_period,temperature` -- a consumer parsing by header position
    # would read a different table shape depending on whether anything matched.
    #
    # ONLY the columns this query would have produced are added, and only when the frame
    # is empty: with rows present the pivot has already produced them and this does
    # nothing. Nothing is renamed and nothing is dropped -- `canonical_column_order`
    # still orders whatever is here, and the caller's permutation assertion still holds.
    if result_df.height == 0:
        _missing = [c for c in expected_value_columns if c not in result_df.columns]
        if _missing:
            result_df = result_df.with_columns(
                [pl.lit(None, dtype=pl.Float64).alias(c) for c in _missing])

    # THE ROW-ORDER CONTRACT (spec 008 section 5). Rows ascend by
    # (time_period, depth, lat, lon), so a consumer can take a fixed time_period at a
    # fixed depth and get a bbox block laid out stably: lat is the outer key and lon
    # the inner, so longitude runs to the end of its range before latitude advances.
    #
    # Here, and only here, because both endpoints serialise this frame directly —
    # api.app's JSON path calls to_dicts() on it and the CSV path write_csv() — so one
    # sort governs both and no rule is written twice. It is also the last statement
    # before the return, after every unordered input above has had its say, so nothing
    # downstream can reintroduce the instability.
    #
    # `time_period` is a STRING column: api.config keys time_periods as '0'..'16' and
    # the Zarr coordinate carries the same strings. Sorting it as text gives
    # ['0','1','10','13','2'] — the contract says 0, 1, 2, ..., 13. Hence the cast,
    # which is a sort KEY only: the column keeps its string dtype and its value.
    #
    # The cast is deliberately strict. On an uncastable value polars raises rather
    # than yielding a null, and a loud failure is right here — silently sorting nulls
    # to one end would satisfy the sort and break the contract.
    #
    # An expression rather than a temporary column: a helper column would need a name,
    # and any name could collide with a {param}_{var} data column, where a colliding
    # `with_columns` alias overwrites real data rather than erroring.
    #
    # nulls_last decides a case that cannot arise today — lon, lat and depth come from
    # Zarr coordinates and are not null. It is specified anyway, because an
    # unspecified case is how a deterministic order stops being one later.
    #
    # Row order only. Column order was out of scope for 008 and is now settled separately,
    # by the canonical projection immediately below (spec 015).
    result_df = result_df.sort(
        [pl.col("time_period").cast(pl.Int32), "depth", "lat", "lon"],
        nulls_last=True,
    )

    # THE COLUMN-ORDER CONTRACT (spec 015 section 5). One explicit ordered projection, at
    # the one place both endpoints share, as the last statement before the return.
    #
    # Every unordered input above has now had its say. The sources are canonicalised at
    # source as well, which makes the intermediate steps deterministic, but this select is
    # what makes the OUTPUT a contract: pivot decides column order by first appearance, and
    # depending on the frame arriving in the right shape is depending on the thing that was
    # unreliable in the first place.
    #
    # `pm2G` is why: same query, same data, same 144 rows, and the CSV header came back
    # `temperature_an,temperature` before a restart and `temperature,temperature_an` after.
    # Stable within a process, different between processes — a hash-seeded container. The
    # values moved with the header, so a positional CSV reader silently swapped two columns.
    #
    # By NAME, never by position: the projection is built from the frame's own column names,
    # so a header can never end up over the wrong values.
    ordered = canonical_column_order(result_df.columns, pars, variables)
    # A PERMUTATION, asserted. Trading a visible ordering bug for a silent data-loss one
    # would be a bad trade, and `select` is perfectly happy to drop a column nobody named.
    if sorted(ordered) != sorted(result_df.columns):
        raise HTTPException(
            status_code=500,
            detail="internal column-order error: the canonical projection is not a "
                   "permutation of the result columns")
    result_df = result_df.select(ordered)

    end_time = datetime.now()
    print(f"Total time for this query taken: {(end_time - init_time).total_seconds()} seconds")
    return result_df
