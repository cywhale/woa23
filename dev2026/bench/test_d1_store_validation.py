"""D1: store startup validation, offline, against synthetic isolated fixtures.

Spec 004. Six negative fixtures and a real-store control, three stages, and the two
properties that are easy to claim and hard to hold: that the shared builder still
produces the exact string the read path has always produced, and that startup reads
**no array data chunk at all**, coordinate chunks included.

**Nothing here touches production's store, the package clone or `~/python/woa23`.**
Every fixture is a synthetic Zarr v2 store built under a temporary directory with
`xarray.Dataset.to_zarr`. `P1` is a *real* Zarr group — openable, and structured the
way the read path needs — so "the valid case passes" is a claim about a store that
actually works rather than about a directory that merely exists.

    uv run python -m bench.test_d1_store_validation
"""

import asyncio
import importlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import xarray as xr

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

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


# --------------------------------------------------------------- the observer ---
# `open` is raised by builtins.open, io.open AND os.open — verified, not assumed —
# and mmap/np.memmap raise it too, plus `mmap.__new__`. Hooking both covers every
# route by which a chunk file can be opened, which is what the assertion needs: a
# chunk cannot be read without its file being opened.
#
# The hook cannot see `listdir`, because a directory listing is not an open. That is
# sound for this property and is the reason the assertion is phrased as "no chunk
# file was opened" rather than "nothing was consulted".
_OPENED: list[str] = []


def _hook(event, args):
    if event in ("open", "mmap.__new__") and args:
        target = args[0]
        if isinstance(target, (str, bytes, os.PathLike)):
            _OPENED.append(os.fspath(target) if not isinstance(target, bytes)
                           else target.decode("utf-8", "replace"))


sys.addaudithook(_hook)

# A Zarr v2 chunk key is dot-separated integers — `0`, `0.0`, `0.0.0` — as the final
# path component. Metadata files all begin with a dot (`.zgroup`, `.zarray`,
# `.zattrs`, `.zmetadata`), so they cannot collide with this.
_CHUNK = re.compile(r"(^|/)\d+(\.\d+)*$")


def chunk_opens(root: str) -> list[str]:
    """Every chunk file opened under `root` since the last reset."""
    out = []
    for p in _OPENED:
        try:
            rel = os.path.relpath(p, root)
        except ValueError:
            continue
        if rel.startswith(".."):
            continue
        if _CHUNK.search(rel):
            out.append(rel)
    return sorted(set(out))


# ---------------------------------------------------------------- the fixtures ---
def build_group(path: str) -> None:
    """A Zarr v2 group with the structure `api.query` actually reads.

    Not a token directory: the read path needs `parameters` and `time_periods`
    coordinates and a variable from `available_vars`. A fixture without them raises
    `KeyError: "No variable named 'parameters'"`, and a fixture too thin to serve a
    successful request cannot demonstrate that some *other* failure was
    request-level (spec 004 section 43).
    """
    lon, lat, dep = [135.5, 136.5], [15.5, 16.5], [0.0, 5.0, 10.0]
    pars, periods = ["temperature", "salinity"], ["0"]
    shape = (len(lon), len(lat), len(dep), len(pars), len(periods))
    xr.Dataset(
        {v: (("lon", "lat", "depth", "parameters", "time_periods"),
             np.zeros(shape, dtype="float32")) for v in ("an", "mn")},
        coords={"lon": lon, "lat": lat, "depth": dep,
                "parameters": pars, "time_periods": periods},
    ).to_zarr(path, mode="w", consolidated=True)


def make_fixtures(root: Path) -> dict:
    """N1..N6 and P1. Every one under `root`; nothing outside it is written."""
    f = {}
    f["N1"] = None                                    # unset variable
    f["N2"] = str(root / "does_not_exist")            # nothing there
    (root / "empty").mkdir()
    f["N3"] = str(root / "empty")                     # exists, empty
    (root / "afile").write_text("not a store\n")
    f["N4"] = str(root / "afile")                     # a regular file

    for name, mutate in (("N5", lambda p: Path(p, ".zgroup").write_text("not json\n")),
                         ("N6", lambda p: Path(p, ".zgroup").write_text(
                             json.dumps({"zarr_format": 99}))),
                         ("P1", None)):
        store = root / name
        build_group(str(store / "1_degree" / "annual" / "TS"))
        if mutate:
            mutate(str(store / "1_degree" / "annual" / "TS"))
        f[name] = str(store)
    return f


def fresh_api():
    """Re-import the api package so import-time validation runs again."""
    for mod in [m for m in list(sys.modules) if m == "api" or m.startswith("api.")]:
        del sys.modules[mod]
    return importlib.import_module("api.config")


def try_import(store):
    if store is None:
        os.environ.pop("WOA23_ZARR_STORE", None)
    else:
        os.environ["WOA23_ZARR_STORE"] = store
    try:
        fresh_api()
        return None
    except Exception as exc:
        return type(exc).__name__, str(exc)


def try_lifespan(store):
    os.environ["WOA23_ZARR_STORE"] = store
    for mod in [m for m in list(sys.modules) if m == "api" or m.startswith("api.")]:
        del sys.modules[mod]
    from api.app import app, lifespan

    async def go():
        async with lifespan(app):
            pass
    try:
        asyncio.run(go())
        return None
    except Exception as exc:
        return type(exc).__name__, str(exc)


# ================================================================== the builder ===
print("the shared builder reproduces query.py's expression exactly (D1-13a)")
os.environ["WOA23_ZARR_STORE"] = "/tmp"       # any importable value
from api.store_paths import (                  # noqa: E402
    ANCHOR_GRID, ANCHOR_SUBGROUP, anchor_path, describe, group_path, resolve)

INPUTS = [("data/", "trailing slash"), ("data", "bare relative"),
          ("/home/odbadmin/python/woa23/data", "absolute"),
          ("/home/odbadmin/python/woa23/data/", "absolute + trailing slash"),
          ("", "empty"), ("./data/", "dot-relative")]
for store, label in INPUTS:
    same = all(group_path(store, g, s) == f"{store}/{g}/{s}"
               for g in ("1_degree", "025_degree")
               for s in ("annual/TS", "seasonal/Oxy", "monthly/Nutrients"))
    check(f"builder == literal for {label}", True, same)
check("the double slash survives for a trailing-slash store",
      "data//1_degree/annual/TS", group_path("data/", "1_degree", "annual/TS"))
check("the anchor is the default combination", "1_degree/annual/TS",
      f"{ANCHOR_GRID}/{ANCHOR_SUBGROUP}")
check("anchor_path goes through the same builder",
      group_path("data/", ANCHOR_GRID, ANCHOR_SUBGROUP), anchor_path("data/"))

print()
print("query.py builds paths only through the builder (D1-13b)")
qsrc = (HERE / "api" / "query.py").read_text()
check("query.py imports the builder", True,
      "from api.store_paths import group_path" in qsrc)
check("and has no second path-building expression", 0,
      len(re.findall(r'f"\{zarr_store_path\}/', qsrc)))

print()
print("store_paths is pure — importing it does no filesystem I/O (D1-14)")
src = (HERE / "api" / "store_paths.py").read_text()
check("no open(), listdir, exists or isdir in the module", True,
      not any(t in src for t in ("open(", "listdir", "os.path.exists",
                                 "os.path.isdir", "Path(")))
check("resolve reports an absolute path", "/w/data/", resolve("data/", "/w"))
check("an absolute store is returned unchanged", "/abs/x", resolve("/abs/x", "/w"))
check("describe carries resolved path, configured value and cwd", True,
      all(t in describe("data/", "/w") for t in ("/w/data/", "'data/'", "'/w'")))

# ============================================================== the seven cases ===
print()
print("the seven fixtures across import and lifespan (D1-1 .. D1-7)")
root = Path(tempfile.mkdtemp(prefix="d1-"))
FX = make_fixtures(root)

EXPECT = {                       # (rejected at import?, rejected at lifespan?)
    "N1": (True, None), "N2": (True, None), "N3": (False, True),
    "N4": (True, None), "N5": (False, True), "N6": (False, True),
    "P1": (False, False),
}
for name, (imp_bad, life_bad) in EXPECT.items():
    imp = try_import(FX[name])
    check(f"{name}: import {'rejects' if imp_bad else 'passes'}", imp_bad, imp is not None)
    if life_bad is None:
        continue
    _OPENED.clear()
    life = try_lifespan(FX[name])
    check(f"{name}: lifespan {'rejects' if life_bad else 'passes'}",
          life_bad, life is not None)
    check(f"{name}: zero chunk files opened during startup (D1-10)",
          [], chunk_opens(FX[name]))

print()
print("failure messages name the resolved path, the value and the cwd (D1-9a)")
for name in ("N2", "N4"):
    _, msg = try_import(FX[name])
    check(f"{name}: message contains the resolved path", True, FX[name] in msg)
    check(f"{name}: and the configured value", True, repr(FX[name]) in msg)
    check(f"{name}: and the cwd", True, "cwd=" in msg)
for name in ("N3", "N5", "N6"):
    _, msg = try_lifespan(FX[name])
    check(f"{name}: message names the anchor group", True,
          "1_degree/annual/TS" in msg)
    check(f"{name}: and the store", True, FX[name] in msg)

print()
print("--check-config is a configuration check, not a store check (D1-11, split-only)")
CFG = {"N2": True, "N4": True, "N3": False, "N5": False, "N6": False, "P1": False}
for name, should_fail in CFG.items():
    env = dict(os.environ, WOA23_ZARR_STORE=FX[name])
    r = subprocess.run([sys.executable, "-m", "gunicorn", "api.app:app",
                        "--check-config"], cwd=str(HERE), env=env,
                       capture_output=True, text=True)
    check(f"--check-config {'fails' if should_fail else 'succeeds'} for {name}",
          should_fail, r.returncode != 0)

# ==================================================== request-level, not startup ===
print()
print("a missing non-anchor group fails that request only (D1-D3)")
os.environ["WOA23_ZARR_STORE"] = FX["P1"]
for mod in [m for m in list(sys.modules) if m == "api" or m.startswith("api.")]:
    del sys.modules[mod]
from api.app import app as _app, lifespan as _lifespan       # noqa: E402
from api.query import process_woa23_data                     # noqa: E402


async def d1_d3():
    results = {}
    _OPENED.clear()
    async with _lifespan(_app):
        results["startup_chunks"] = chunk_opens(FX["P1"])
        # 1_degree + nitrate is a legal, query-reachable combination — nutrients are
        # one-degree, and this is one-degree — so the request must pass the upstream
        # availability check at query.py:119 and fail at the group open instead.
        try:
            await process_woa23_data(135.5, 15.5, None, None, None, None,
                                     "1", "an", "nitrate", "13")
            results["missing"] = ("none", "")
        except Exception as exc:
            results["missing"] = (type(exc).__name__, str(exc))
        try:
            rows = await process_woa23_data(135.5, 15.5, None, None, None, None,
                                            "1", "an", "temperature", "0")
            results["after"] = len(rows)
        except Exception as exc:
            results["after"] = f"{type(exc).__name__}: {exc}"
    return results


R = asyncio.run(d1_d3())
check("(1)(2) the request reached the group open, not the 400", True,
      R["missing"][0] not in ("HTTPException", "none"))
check("(3) it failed at request level with FileNotFoundError",
      "FileNotFoundError", R["missing"][0])
check("(4) a subsequent anchor request still succeeds", True,
      isinstance(R["after"], int) and R["after"] > 0)
check("(5) lifespan did not fail for the valid store", [], R["startup_chunks"])
check("(6) production paths are never referenced by any fixture", True,
      all("/home/odbadmin" not in str(v) for v in FX.values() if v))

print()
print("the audit observer sees every route a chunk could be opened by")
probe = root / "probe.bin"
probe.write_bytes(b"x" * 32)
for label, fn in (("builtins.open", lambda: open(probe, "rb").read(1)),
                  ("os.open", lambda: os.close(os.open(probe, os.O_RDONLY)))):
    _OPENED.clear()
    fn()
    check(f"{label} is observed", True, any(str(probe) in p for p in _OPENED))

shutil.rmtree(root)

print()
if FAIL:
    print(f"FAILED {FAIL}/{PASS + FAIL}")
    raise SystemExit(1)
print(f"all passed ({PASS} assertions)")
