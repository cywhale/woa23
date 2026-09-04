#!/usr/bin/env python3
"""The docs-only allowlist, exercised passing AND failing. Offline.

The allowlist exists to let a prose-only `api/` edit skip a C1/C2 re-run. That is a
real saving and therefore a real hazard: a checker that says YES too easily would let a
behaviour change through under a documentation label, and the run that would have
caught it is the one being skipped.

So every forbidden category below is exercised as a **rejection**. A checker that
cannot say NO is not a check.

    uv run python -m bench.test_docs_only_diff
"""

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from bench.docs_only_diff import normalised_dump  # noqa: E402
from bench.suite_summary import summary, summary_line   # noqa: E402

PASS = FAIL = 0


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


BASE = '''
"""Module docstring."""
from fastapi import FastAPI, Query
from fastapi.openapi.utils import get_openapi

app = FastAPI(lifespan=None, docs_url=None)


def generate_custom_openapi():
    openapi_schema = get_openapi(
        title="ODB WOA23 API",
        version="1.0.0",
        description="Open API to query WOA2023 data.",
        routes=app.routes,
    )
    return openapi_schema


@app.get("/api/woa23", tags=["WOA23"], summary="Query WOA23 data (in JSON)")
async def get_woa23(lon0: float = Query(..., description="Minimum longitude.")):
    """Endpoint docstring."""
    df = await process(lon0)
    return ORJSONResponse(content=df.to_dicts())
'''


def same(a: str, b: str) -> bool:
    return normalised_dump(a) == normalised_dump(b)


def main():
    print("what the allowlist PERMITS")
    check("an unchanged file is docs-only", True, same(BASE, BASE))
    check("bumping the OpenAPI info.version", True,
          same(BASE, BASE.replace('version="1.0.0"', 'version="1.1.0"')))
    check("rewriting the OpenAPI description", True,
          same(BASE, BASE.replace('description="Open API to query WOA2023 data."',
                                  'description="Open API. Rows are ordered by '
                                  '(time_period, depth, lat, lon)."')))
    check("rewriting an endpoint operation docstring", True,
          same(BASE, BASE.replace('"""Endpoint docstring."""',
                                  '"""Endpoint docstring.\n\n    #### Row order\n'
                                  '    * ordered by (time_period, depth, lat, lon)\n'
                                  '    """')))
    check("adding a docstring where there was none", True,
          same(BASE, BASE.replace("def generate_custom_openapi():",
                                  'def generate_custom_openapi():\n    """New."""')))
    check("changing the module docstring", True,
          same(BASE, BASE.replace('"""Module docstring."""', '"""Rewritten."""')))
    check("a comment or blank line", True,
          same(BASE, BASE.replace("app = FastAPI(",
                                  "# a new comment\n\napp = FastAPI(")))

    print("\nwhat it REFUSES — each of these needs a new C1/C2")
    check("changing a route path", False,
          same(BASE, BASE.replace('"/api/woa23"', '"/api/woa23/v2"')))
    check("changing an endpoint's summary (published, but not in the allowlist)", False,
          same(BASE, BASE.replace('summary="Query WOA23 data (in JSON)"',
                                  'summary="Query WOA23 data"')))
    check("changing a request parameter's description", False,
          same(BASE, BASE.replace('description="Minimum longitude."',
                                  'description="Min lon."')))
    check("adding a request parameter", False,
          same(BASE, BASE.replace("lon0: float = Query(..., description=\"Minimum "
                                  "longitude.\")",
                                  "lon0: float = Query(...), sort: str = Query(None)")))
    check("changing a parameter default", False,
          same(BASE, BASE.replace("Query(..., description=", "Query(0.0, description=")))
    check("changing handler logic", False,
          same(BASE, BASE.replace("df = await process(lon0)",
                                  "df = await process(lon0)\n    df = df.head(10)")))
    check("changing the serialisation", False,
          same(BASE, BASE.replace("ORJSONResponse(content=df.to_dicts())",
                                  "JSONResponse(content=df.to_dicts())")))
    check("changing runtime configuration", False,
          same(BASE, BASE.replace("FastAPI(lifespan=None, docs_url=None)",
                                  'FastAPI(lifespan=None, docs_url="/docs")')))
    check("changing the OpenAPI title (not in the allowlist)", False,
          same(BASE, BASE.replace('title="ODB WOA23 API"', 'title="WOA23"')))
    check("changing `routes=` (structure, not prose)", False,
          same(BASE, BASE.replace("routes=app.routes", "routes=[]")))
    check("adding an import", False,
          same(BASE, BASE.replace("from fastapi import FastAPI, Query",
                                  "from fastapi import FastAPI, Query\nimport os")))
    check("a description= OUTSIDE get_openapi is not allowlisted", False,
          same(BASE.replace('description="Minimum longitude."', 'description="A"'),
               BASE.replace('description="Minimum longitude."', 'description="B"')))

    print("\nthe normaliser must not be vacuous")
    # If it stripped everything, every pair above would compare equal and the
    # rejections would be false. The tree must still carry the executable code.
    dump = normalised_dump(BASE)
    for token in ("/api/woa23", "ORJSONResponse", "to_dicts", "Minimum longitude",
                  "ODB WOA23 API", "docs_url"):
        check(f"the normalised tree still contains {token!r}", True, token in dump)
    check("and the allowlisted strings are gone", True,
          "1.0.0" not in dump and "Open API to query WOA2023 data." not in dump)
    check("and docstrings are gone", True,
          "Endpoint docstring" not in dump and "Module docstring" not in dump)

    print()
    return summary(PASS, FAIL)
if __name__ == "__main__":
    sys.exit(main())
