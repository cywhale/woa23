import xarray as xr
import pandas as pd
import numpy as np
import polars as pl
from fastapi import FastAPI, Query, HTTPException
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, ORJSONResponse, FileResponse
from contextlib import asynccontextmanager
from typing import Optional, List
from tempfile import NamedTemporaryFile
import json, math
from datetime import datetime
# from dask.distributed import Client
# client = Client('tcp://localhost:8786')
from src.dask_client_manager import get_dask_client
client = get_dask_client("woa23api")

def generate_custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="ODB WOA23 API",
        version="1.0.0",
        description=('Open API to query WOA2023 (WOA23) data, compiled by ODB.\n' +
                     '* Data source: Reagan, James R.; Boyer, Tim P.; García, Hernán E.; Locarnini, Ricardo A.; Baranova, Olga K.; Bouchard, Courtney; Cross, Scott L.; Mishonov, Alexey V.; Paver, Christopher R.; Seidov, Dan; Wang, Zhankun; Dukhovskoy, Dmitry. (2024). World Ocean Atlas 2023. NOAA National Centers for Environmental Information. Dataset: NCEI Accession 0270533.\n' +
                     '* WOA23 official spec (in PDF): https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DOCUMENTATION/WOA23_Product_Documentation.pdf'),
        routes=app.routes,
    )
    openapi_schema["servers"] = [
        {
            "url": "https://eco.odb.ntu.edu.tw"
        }
    ]
    app.openapi_schema = openapi_schema
    return app.openapi_schema


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("App start at ", datetime.now())
    yield
    # below code to execute when app is shutting down
    client.close()
    print("App end at ", datetime.now())


app = FastAPI(lifespan=lifespan, docs_url=None, default_response_class=ORJSONResponse)

@app.get("/api/swagger/woa23/openapi.json", include_in_schema=False)
async def custom_openapi():
    return JSONResponse(generate_custom_openapi())


@app.get("/api/swagger/woa23", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url="/api/swagger/woa23/openapi.json",
        title=app.title
    )

# Path to your Zarr store
zarr_store_path = "data/"

# Initialize global definitions
grid_resolutions = {'01': '1.00', '04': '0.25'}  # Two gridded resolutions data: 1-degree and 0.25-degree in WOA23
grid_dir = {'01': '1_degree', '04': '025_degree'}

parameters = {
    't': 'temperature',
    's': 'salinity',
    'o': 'oxygen',
    'O': 'o2sat',
    'A': 'AOU',
    'i': 'silicate',
    'p': 'phosphate',
    'n': 'nitrate'
}

parameters_name = {
    't': 'temperature',
    's': 'salinity',
    'o': 'dissolved oxygen',
    'O': 'percent oxygen saturation',
    'A': 'apparent oxygen utilization',
    'i': 'silicate',
    'p': 'phosphate',
    'n': 'nitrate'
}

time_periods = {
    '0': 'annual',
    '1': 'january',
    '2': 'february',
    '3': 'march',
    '4': 'april',
    '5': 'may',
    '6': 'june',
    '7': 'july',
    '8': 'august',
    '9': 'september',
    '10': 'october',
    '11': 'november',
    '12': 'december',
    '13': 'winter',
    '14': 'spring',
    '15': 'summer',
    '16': 'autumn'
}

available_vars = ['an', 'mn', 'dd', 'ma', 'sd', 'se', 'oa', 'gp', 'sdo', 'sea']

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
    if isinstance(obj, float):
        if np.isnan(obj) or np.isinf(obj):
            return None
    return obj

async def process_woa23_data(lon0: float, lat0: float, lon1: Optional[float], lat1: Optional[float], dep0: Optional[float], dep1: Optional[float], grid: Optional[str], append: Optional[str], parameter: Optional[str], time_period: Optional[str]):
    init_time = datetime.now()
    # start_time = datetime.now()

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
        dep1 = 5501 #max depth in WOA23 is 5500m

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
    # end_time = datetime.now()
    # print(f"Time taken to handle query parameters: {(end_time - start_time).total_seconds()} seconds")

    start_time = datetime.now()
    for zarr_group_path in zarr_group_paths:
        ds = xr.open_zarr(zarr_group_path)

        # Ensure the selected parameters exist in the dataset
        # intersect_params_start_time = datetime.now()
        existing_params = set(ds.coords['parameters'].values)
        selected_params = existing_params.intersection(pars)
        # end_time = datetime.now()
        # print(f"Time taken to intersect parameters: {(end_time - intersect_params_start_time).total_seconds()} seconds")

        if not selected_params:
            continue

        # Ensure the selected time periods exist in the dataset
        # intersect_periods_start_time = datetime.now()
        existing_periods = set(ds.coords['time_periods'].values)
        selected_periods = existing_periods.intersection(periods)
        # end_time = datetime.now()
        # print(f"Time taken to intersect periods: {(end_time - intersect_periods_start_time).total_seconds()} seconds")
        if not selected_periods:
            continue

        # Select the appropriate data based on the query parameters
        # filtering_start_time = datetime.now()
        filtered_data = ds.sel(
            lon=slice(lon_min, lon_max),
            lat=slice(lat_min, lat_max),
            depth=slice(depth_min, depth_max),
            parameters=list(selected_params),
            time_periods=list(selected_periods)
        )
        # end_time = datetime.now()
        # print(f"Time taken to filtering ds: {(end_time - filtering_start_time).total_seconds()} seconds")
        # Append the data variables to the result list
        appending_start_time = datetime.now()
        """ pandas version """
        for var in variables:
            if var in filtered_data:
                data = filtered_data[var].to_dataframe().reset_index()
                # Append the data variable to the DataFrame
                """ pandas version
                data[var] = data.apply(lambda row: row[var], axis=1)
                result_list.append(data)
                """
                # Convert to polars directly
                data_polars = pl.from_pandas(data)
                data_polars = data_polars.with_columns([
                    pl.lit(var).alias("variable_type"),
                    pl.col(var).alias("value")
                ])
                # Drop original var columns if exist
                data_polars = data_polars.drop(var)
                result_list.append(data_polars)

        # end_time = datetime.now()
        # print(f"Time taken to appending result_list: {(end_time - appending_start_time).total_seconds()} seconds")

    # end_time = datetime.now()
    # print(f"Time taken to open and filter Zarr stores: {(end_time - start_time).total_seconds()} seconds")

    if not result_list:
        raise HTTPException(status_code=404, detail="No data found for the specified query parameters")

    # print("result list: ", result_list)
    """ pandas version
    # Concatenate all dataframes in the result list
    result_df = pd.concat(result_list, ignore_index=True)

    # Pivot to wide format
    result_df = result_df.pivot_table(
        index=["lon", "lat", "depth", "time_periods"],
        columns="parameters",
        values=[var for var in variables],
        aggfunc='first'
    ).reset_index()

    # Flatten the column multi-index after pivoting
    result_df.columns = [f"{param}_{var}" if param != "lon" and param != "lat" and param != "depth" and param != "time_periods" else param for param, var in result_df.columns]

    # Optionally rename {param}_mn to {param} if `mn` is present in the query variables
    if 'mn' in variables:
        result_df.columns = [col.replace('mn_', '') for col in result_df.columns]

    result_df = result_df.rename(columns={"time_periods": "time_period"})
    result_data = result_df.to_dict(orient="records")

    return ORJSONResponse(content=result_data)
    """
    """ polars version """
    # Convert each dataframe in result_list to polars and add the variable type
    # start_time = datetime.now()

    # Concatenate the dataframes
    result_df = pl.concat(result_list, how="vertical")
    # print("result df: ", result_df)

    # Combine the parameter and variable type columns
    result_df = result_df.with_columns(
        (pl.col("parameters") + "_" + pl.col("variable_type")).alias("parameter_variable")
    )
    # end_time = datetime.now()
    # print(f"Time taken for converting to polars and concatenating: {(end_time - start_time).total_seconds()} seconds")

    # print("result_df after rename, before pivot: ", result_df)
    """ Check for duplicates
    duplicates = result_df.groupby(["lon", "lat", "depth", "time_periods", "parameter_variable"]).count()
    duplicated_rows = duplicates.filter(pl.col("count") > 1)

    if duplicated_rows.height > 0:
        # Get the duplicated keys
        duplicated_keys = duplicated_rows.select(["lon", "lat", "depth", "time_periods", "parameter_variable"])
        # Join with the original result_df to get all duplicated rows
        duplicated_data = result_df.join(duplicated_keys, on=["lon", "lat", "depth", "time_periods", "parameter_variable"], how="inner")
        # Sort the duplicated data
        duplicated_data = duplicated_data.sort(["lon", "lat", "depth", "time_periods", "parameter_variable"])
        print("Duplicated Rows:", duplicated_rows.height)
        print(duplicated_data)
    """
    # Pivot to wide format
    # start_time = datetime.now()
    result_df = result_df.pivot(
        index=["lon", "lat", "depth", "time_periods"],
        columns="parameter_variable",
        values="value"
    )
    # end_time = datetime.now()
    # print(f"Time taken for pivoting: {(end_time - start_time).total_seconds()} seconds")

    # Optionally rename {param}_mn to {param} if `mn` is present in the query variables
    # start_time = datetime.now()
    if 'mn' in variables:
        rename_dict = {f"{param}_mn": param for param in pars if f"{param}_mn" in result_df.columns}
        if rename_dict:  # Check if there are columns to rename
            result_df = result_df.rename(rename_dict)

    result_df = result_df.rename({"time_periods": "time_period"})
    end_time = datetime.now()
    # print(f"Time taken for renaming and generating JSON: {(end_time - start_time).total_seconds()} seconds")
    print(f"Total time for this query taken: {(end_time - init_time).total_seconds()} seconds")
    return result_df

@app.get("/api/woa23", tags=["WOA23"], summary="Query WOA23 data (in JSON)")
async def get_woa23(
    lon0: float = Query(...,
                        description="Minimum longitude, range: [-180, 180]."),
    lat0: float = Query(..., description="Minimum latitude, range: [-90, 90]."),
    lon1: Optional[float] = Query(
        None, description="Maximum longitude, range: [-180, 180]."),
    lat1: Optional[float] = Query(
        None, description="Maximum latitude, range: [-90, 90]."),
    dep0: Optional[float] = Query(
        None, description="Minimum depth. Optional, default is 0."),
    dep1: Optional[float] = Query(
        None, description="Maximum depth. Optional, default is maximum depth 5500m in WOA23."),
    grid: Optional[str] = Query(
        None, description="Grid resoultion: 1 for 1-degree, 0.25 for 0.25-degree. Default is 1."),
    append: Optional[str] = Query(
        None, description=f"Statistics to append, separated by commas. Default is 'mn': Statistical mean. Allowed: {', '.join(available_vars)}."),
    parameter: Optional[str] = Query(
        None,
        description="WOA23 parameteres, separated by commas. Default is 'temperature'. Allowed: temperature, salinity (both 0.25/1-degree data), oxygen, o2sat, AOU, silicate, phosphate, nitrate (only 1-degree data)."),
    time_period: Optional[str] = Query(
        None, description="Time periods for statistics, separated by commas. Default is '0' (annual). Allowed: 0 (annual). 1-12 (monthly), 13-16 (seasonal)."),
):
    """
    Query WOA23 data (in JSON), including sea temperature, salinity, dissolved oxygen, and nutrients.

    #### Usage
    * /api/woa23?lon0=125&lat0=15&dep0=100&grid=1&parameter=temperature,salinity&time_period=13,14,15,16
    * parameter: temperature, salinity, oxygen, o2sat, AOU, silicate, phosphate, nitrate
    """
    try:
        df = await process_woa23_data(lon0, lat0, lon1, lat1, dep0, dep1, grid, append, parameter, time_period)
        result_data = df.to_dicts()
        return ORJSONResponse(content=result_data)
    except HTTPException as herr:
        raise herr
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error. Please try it later or inform admin")

@app.get("/api/woa23/csv", tags=["WOA23"], summary="Query WOA23 data (in CSV)")
async def get_woa23_csv(
    lon0: float = Query(..., description="Minimum longitude, range: [-180, 180]."),
    lat0: float = Query(..., description="Minimum latitude, range: [-90, 90]."),
    lon1: Optional[float] = Query(None, description="Maximum longitude, range: [-180, 180]."),
    lat1: Optional[float] = Query(None, description="Maximum latitude, range: [-90, 90]."),
    dep0: Optional[float] = Query(None, description="Minimum depth. Optional, default is 0."),
    dep1: Optional[float] = Query(None, description="Maximum depth. Optional, default is maximum depth 5500m in WOA23."),
    grid: Optional[str] = Query(None, description="Grid resoultion: 1 for 1-degree, 0.25 for 0.25-degree. Default is 1."),
    append: Optional[str] = Query(None, description=f"Statistics to append, separated by commas. Default is 'mn': Statistical mean. Allowed: {', '.join(available_vars)}."),
    parameter: Optional[str] = Query(None, description="WOA23 parameteres, separated by commas. Default is 'temperature'. Allowed: temperature, salinity (both 0.25/1-degree data), oxygen, o2sat, AOU, silicate, phosphate, nitrate (only 1-degree data)."),
    time_period: Optional[str] = Query(None, description="Time periods for statistics, separated by commas. Default is '0' (annual). Allowed: 0 (annual). 1-12 (monthly), 13-16 (seasonal)."),
):
    """
    Query WOA23 data (in CSV), including sea temperature, salinity, dissolved oxygen, and nutrients.

    #### Usage
    * /api/woa23/csv?lon0=125&lat0=15&dep0=100&grid=1&parameter=temperature,salinity&time_period=13,14,15,16
    * parameter: temperature, salinity, oxygen, o2sat, AOU, silicate, phosphate, nitrate
    """
    try:
        df = await process_woa23_data(lon0, lat0, lon1, lat1, dep0, dep1, grid, append, parameter, time_period)
        if df.is_empty():
            raise HTTPException(status_code=400, detail="No data available for the given parameters.")

        temp_file = NamedTemporaryFile(delete=False)
        df.write_csv(temp_file.name)  # polars version
        out_file = f"woa23_from_ODB_{datetime.today().strftime('%Y-%m-%d')}.csv"
        return FileResponse(temp_file.name, media_type="text/csv", filename=out_file)

    except HTTPException as herr:
        raise herr
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error. Please try it later or inform admin")
