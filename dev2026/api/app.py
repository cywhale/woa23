"""Candidate WOA23 API. Port of `woa23_app.py` with Dask removed.

All four production routes are here. The Swagger pair is contract surface as much
as the data endpoints are: the OpenAPI document is generated from these signatures
and descriptions, so every summary, description and default below must match the
original character for character or the byte gate on
`/api/swagger/woa23/openapi.json` fails.

`servers` stays a hard-coded literal, exactly as in `woa23_app.py:30-34`. It must
not be derived from the request, or two instances on 8051 and 8052 would produce
different documents and the byte comparison would fail by construction.

`from __future__ import annotations` is deliberately absent — see `query.py`. It
would make FastAPI read these signatures as strings, and these signatures are what
the OpenAPI document is generated from.

The Dask client and its lifespan shutdown are gone. `src/dask_client_manager.py` is
untouched and the VM24 cluster keeps running — `tide_app` and `mhw_app` depend on
it; only this app's client is removed.
"""

import os
from contextlib import asynccontextmanager
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Optional

from fastapi import FastAPI, Query, HTTPException
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, ORJSONResponse, FileResponse

from api.config import available_vars, zarr_store_path
from api.query import process_woa23_data
from api.store_paths import anchor_path, describe


def generate_custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="ODB WOA23 API",
        version="1.1.0",
        description=('Open API to query WOA2023 (WOA23) data, compiled by ODB.\n' +
                     '* Data source: Reagan, James R.; Boyer, Tim P.; García, Hernán E.; Locarnini, Ricardo A.; Baranova, Olga K.; Bouchard, Courtney; Cross, Scott L.; Mishonov, Alexey V.; Paver, Christopher R.; Seidov, Dan; Wang, Zhankun; Dukhovskoy, Dmitry. (2024). World Ocean Atlas 2023. NOAA National Centers for Environmental Information. Dataset: NCEI Accession 0270533.\n' +
                     '* WOA23 official spec (in PDF): https://www.ncei.noaa.gov/data/oceans/woa/WOA23/DOCUMENTATION/WOA23_Product_Documentation.pdf\n' +
                     '* Row order (since 1.1.0): For successful responses containing multiple rows, rows are ordered by (time_period numeric ascending, depth ascending, lat ascending, lon ascending). Within a fixed time_period and depth block, latitude is the outer dimension and longitude varies fastest. JSON field order and CSV header order are unchanged.'),
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
    # Spec 004: the required anchor group must open as Zarr v2 metadata before this
    # worker serves anything. Raising here fails ASGI startup, so the worker exits
    # and never becomes ready — which is the point: an unusable store must not reach
    # the first data request.
    #
    # Reads Zarr METADATA FILES ONLY. No array data chunk is read, and that
    # includes coordinate chunks.
    #
    # Measured, not assumed. `xr.open_zarr(path, chunks=None)` is lazy about data
    # variables but materialises the COORDINATE arrays, which are themselves stored
    # as chunks: on a small fixture it opens depth/0, lat/0 and lon/0. It is
    # therefore not metadata-only and is not used here.
    #
    # What this opens instead is observed with an audit hook rather than claimed
    # from the API surface: on that fixture the call below opened exactly one file,
    # `.zgroup`. The assertion the tests make is the property, not the file list —
    # no opened path is a data-array or coordinate chunk — because which metadata
    # files a store needs depends on the store (consolidated or not).
    #
    # A chunk read here would warm the page cache before any measurement, which is
    # the mistake S1 already found in a readiness probe.
    #
    # Only the anchor. Whether any other group exists is a per-request question:
    # WOA23 publishes oxygen on one degree only and the nutrients on one degree and
    # the 'all' time span only, so a store legitimately lacking a combination must
    # still start and serve the rest (spec 004 section 24.1).
    import zarr
    _anchor = anchor_path(zarr_store_path)
    try:
        _group = zarr.open_group(_anchor, mode="r")
        list(_group.array_keys())
    except Exception as exc:
        raise RuntimeError(
            f"the WOA23 store's required anchor group could not be opened: "
            f"{_anchor} — store {describe(zarr_store_path, os.getcwd())}") from exc
    yield
    # below code to execute when app is shutting down
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

    #### Row order (since 1.1.0)
    * For successful responses containing multiple rows, rows are ordered by (time_period numeric ascending, depth ascending, lat ascending, lon ascending). Within a fixed time_period and depth block, latitude is the outer dimension and longitude varies fastest. JSON field order and CSV header order are unchanged.
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

    #### Row order (since 1.1.0)
    * For successful responses containing multiple rows, rows are ordered by (time_period numeric ascending, depth ascending, lat ascending, lon ascending). Within a fixed time_period and depth block, latitude is the outer dimension and longitude varies fastest. JSON field order and CSV header order are unchanged.
    """
    try:
        df = await process_woa23_data(lon0, lat0, lon1, lat1, dep0, dep1, grid, append, parameter, time_period)

        # A VALID QUERY THAT MATCHES ZERO ROWS IS A RESULT, NOT AN ERROR.
        #
        # This used to be `if df.is_empty(): raise HTTPException(400, "No data available
        # for the given parameters.")`, and it made the two routes disagree about the same
        # question. D-3 asked winter nitrate (time_period=13) at 3000-4000 m -- a
        # well-formed query whose depth range simply lies outside seasonal nitrate's
        # 0-800 m extent -- and got:
        #
        #   /api/woa23      200  []                                          (a result)
        #   /api/woa23/csv  400  {"detail":"No data available ..."}          (an error)
        #
        # The CSV body was also application/json, so a CSV client received neither CSV nor
        # a status it could treat as an empty response.
        #
        # THE BRANCH IS REMOVED, NOT WIDENED. Everything that is genuinely an error still
        # is: `process_woa23_data` raises HTTPException itself for unusable parameters and
        # for a query that matches no data arrays at all (404), ValueError becomes 400 and
        # anything else becomes 500 -- none of which reaches this line. What changes is
        # only the case where the query succeeded and the answer is "no rows", which the
        # JSON route has always expressed as 200 with an empty result.
        #
        # An empty frame keeps its schema, so `write_csv` emits the header row and the
        # response is a valid header-only CSV rather than a zero-byte body.

        # Leaks a file per request: NamedTemporaryFile(delete=False) is never
        # removed. Preserved verbatim — it is S2's to fix, and changing it here
        # would put a second variable in S1's before/after.
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
