"""Local smoke test: does the candidate import, route and describe itself correctly?

Runs entirely on this machine. No VM24 process, no Zarr store, no requests to any
backend — the store path is never opened, only read from the environment, so a
placeholder is enough to import the module.

It also does something more useful than a smoke test: it calls **both** Swagger
route handlers and byte-compares the `JSONResponse.body` each returns. That is
contract case C20, executed the way the gate executes it, and it needs neither a
running server nor D2a.

The distinction matters. An earlier version compared `json.dumps(dict)` of the two
schema objects — the right answer, arrived at by a route the gate never takes.
Serialisation is where a byte difference would appear, so evidence that skips it is
evidence about something else. Comparing the response body compares what a client
receives.

    uv run python -m bench.smoke_local
"""

import asyncio
import json
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent.parent

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {'ok  ' if cond else 'FAIL'} {name}" + (f"  {detail}" if not cond else ""))
    if not cond:
        failures.append(name)


def load_candidate():
    os.environ.setdefault("WOA23_ZARR_STORE", "/placeholder/not/opened")
    sys.path.insert(0, str(REPO / "dev2026"))
    from api import app as mod
    return mod


def load_original():
    """Import the production module without letting it reach a Dask scheduler.

    `woa23_app.py:17` connects at import. There is no scheduler on this machine, and
    `DaskClientManager._connect` swallows the failure and returns None — the short
    connect timeout below just stops it waiting 30 s to find that out. Nothing is
    started and nothing is contacted beyond a refused localhost connection.
    """
    os.environ.setdefault("DASK_DISTRIBUTED__COMM__TIMEOUTS__CONNECT", "1s")
    sys.path.insert(0, str(REPO))
    import woa23_app
    return woa23_app


def main() -> int:
    print("candidate imports")
    cand = load_candidate()
    check("api.app imports", cand.app is not None)

    routes = sorted((r.path, tuple(sorted(r.methods))) for r in cand.app.routes
                    if hasattr(r, "methods") and str(r.path).startswith("/api"))
    # GET only — FastAPI does not synthesise HEAD, which the first draft of this
    # test assumed.
    expected = [
        ("/api/swagger/woa23", ("GET",)),
        ("/api/swagger/woa23/openapi.json", ("GET",)),
        ("/api/woa23", ("GET",)),
        ("/api/woa23/csv", ("GET",)),
    ]
    check("all four routes present", routes == expected, f"got {routes}")

    print("\nOpenAPI generation")
    cand_doc = cand.generate_custom_openapi()
    check("document generates", isinstance(cand_doc, dict))
    check("servers is the fixed production URL",
          cand_doc.get("servers") == [{"url": "https://eco.odb.ntu.edu.tw"}],
          str(cand_doc.get("servers")))
    check("title and version match",
          (cand_doc["info"]["title"], cand_doc["info"]["version"])
          == ("ODB WOA23 API", "1.0.0"))

    print("\ncontract case C20 — raw response bodies from the Swagger routes")
    try:
        orig = load_original()
    except Exception as exc:                       # pragma: no cover
        check("original imports", False, repr(exc))
        return 1
    check("original imports", True)

    # The route handlers, not the schema dicts: this is the byte sequence a client
    # receives, and serialisation is where a difference would show.
    a = asyncio.run(orig.custom_openapi()).body
    b = asyncio.run(cand.custom_openapi()).body
    check(f"openapi.json bodies are byte-identical ({len(a)} bytes)", a == b,
          f"{len(a)} vs {len(b)} bytes")

    if a != b:
        # The byte result is the verdict; this only says where to look.
        orig_doc = orig.generate_custom_openapi()
        for key in sorted(set(orig_doc) | set(cand_doc)):
            if orig_doc.get(key) != cand_doc.get(key):
                print(f"       differs at top-level key {key!r}")
        for path in sorted(set(orig_doc.get("paths", {})) | set(cand_doc.get("paths", {}))):
            if orig_doc.get("paths", {}).get(path) != cand_doc.get("paths", {}).get(path):
                print(f"       differs at path {path}")

    # The HTML route is contract surface too, and it embeds the app title.
    ha = asyncio.run(orig.custom_swagger_ui_html()).body
    hb = asyncio.run(cand.custom_swagger_ui_html()).body
    check(f"swagger UI bodies are byte-identical ({len(ha)} bytes)", ha == hb,
          f"{len(ha)} vs {len(hb)} bytes")

    print(f"\n{'FAILED: ' + ', '.join(failures) if failures else 'all passed'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
