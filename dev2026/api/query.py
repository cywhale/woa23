"""The WOA23 read pipeline. Port of `woa23_app.process_woa23_data`.

Two behavioural changes, both mandated by spec 001, and nothing else:

1. **No Dask.** `woa23_app.py:17` installs a distributed client as the process-wide
   default scheduler, so every `.compute()` — triggered inside `to_dataframe()` —
   round-trips through a scheduler backed by one worker shared with `tide_app` and
   `mhw_app`. Measured, that costs 1.35x-8.4x across every query shape from 102 to
   64,800 rows. `chunks=None` disables it; the selection materialises at
   `to_dataframe()`, which is where it materialised before.
2. **`pivot(on=)` instead of `pivot(columns=)`**, deprecated in polars 1.27.1 and
   emitting a warning on every request today. Verified equivalent — identical frame
   contents and identical column order.

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
  `PYTHONHASHSEED`. That is a real defect and it belongs to S2b; both arms of the
  contract gate run with the seed pinned so it cannot contaminate this comparison.
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

    variables = list(set([var.strip() for var in append.split(
        ',') if var.strip() in available_vars]))
    if not variables:
        raise HTTPException(
            status_code=400, detail=f"Invalid variables. Allowed variables are {', '.join(available_vars)}")

    if parameter is None:
        parameter = 'temperature'

    available_pars = ['temperature', 'salinity'] if gridSz == 0.25 else ['temperature', 'salinity', 'oxygen', 'o2sat', 'AOU', 'silicate', 'phosphate', 'nitrate']

    pars = list(set([c.strip() for c in parameter.split(',') if c.strip() in available_pars]))
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
            zarr_group_paths.add(f"{zarr_store_path}/{grid_path}/{subgroup}")

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
            parameters=list(selected_params),
            time_periods=list(selected_periods)
        )

        appending_start_time = datetime.now()
        for var in variables:
            if var in filtered_data:
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
    end_time = datetime.now()
    print(f"Total time for this query taken: {(end_time - init_time).total_seconds()} seconds")
    return result_df
