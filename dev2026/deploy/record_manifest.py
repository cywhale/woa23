#!/usr/bin/env python3
"""Record the COMPLETE dependency manifest of the interpreter running this script.

Why this exists
---------------
B7 asked whether eight named packages were present, and they were — at versions identical
to the validated venv's. That was the question asked, and it is not enough to describe a
runtime. S1 was caught by exactly this gap: the twelve *pinned* packages matched on both
arms while **23 shared transitive dependencies did not**, including ``fsspec`` and
``anyio``, which is why that A/B was only ever quotable as directional evidence.

So this records **every installed distribution**, not a chosen subset, and prints a digest
over the whole sorted list so two environments can be compared by one value.

What it does NOT do
-------------------
It **imports nothing**. ``importlib.metadata`` reads distribution metadata from disk, so
running this against an environment containing ``polars`` does not initialise the library
and does not emit the AVX2 warning of spec 012. That property is asserted by the offline
suite, because it is the difference between an inventory and a side effect.

It writes nothing, installs nothing, and starts nothing.

    python3 deploy/record_manifest.py [--core-only]
"""
from __future__ import annotations

import hashlib
import importlib.metadata as md
import platform
import sys

#: The packages the candidate's own import graph depends on, plus the ones whose version
#: a decision has been taken about. These are reported separately and MUST match between
#: the validated venv and any deployment venv — a difference here is a stop, whereas a
#: difference elsewhere in the full manifest is something to explain (§ spec 014).
#:
#: `polars` is in this set because spec 012 (B6) decided its version explicitly: mainline
#: 1.27.1. A deployment carrying a different polars is not the decided configuration.
CORE = (
    "polars",
    "orjson",
    "fastapi",
    "uvicorn",
    "gunicorn",
    "starlette",
    "pydantic",
    "zarr",
    "xarray",
    "numpy",
)


def distributions() -> list[tuple[str, str]]:
    """Every installed distribution as (normalised name, version), sorted.

    Names are lower-cased and ``_`` is folded to ``-`` because the same distribution can
    report either spelling depending on how it was built, and a manifest whose digest
    depends on that is not comparable between machines.
    """
    seen: dict[str, str] = {}
    for dist in md.distributions():
        name = (dist.metadata["Name"] or "").strip()
        if not name:
            continue
        key = name.lower().replace("_", "-")
        version = dist.version or "UNKNOWN"
        # A duplicate can appear when two paths on sys.path both provide a distribution.
        # Record it rather than silently keeping one: it is a real environment problem.
        if key in seen and seen[key] != version:
            seen[key] = f"{seen[key]}|DUPLICATE|{version}"
        else:
            seen.setdefault(key, version)
    return sorted(seen.items())


def digest_of(rows: list[tuple[str, str]]) -> str:
    body = "".join(f"{n}=={v}\n" for n, v in rows)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def main() -> int:
    core_only = "--core-only" in sys.argv
    rows = distributions()

    print(f"interpreter    : {sys.executable}")
    print(f"python_version : {'.'.join(str(x) for x in sys.version_info[:3])}")
    print(f"python_build   : {sys.version.replace(chr(10), ' ')}")
    print(f"platform       : {platform.system()} {platform.machine()}")
    print(f"distributions  : {len(rows)}")
    print(f"manifest_sha256: {digest_of(rows)}")
    print()

    print("== CORE (must match the validated venv exactly) ==")
    lookup = dict(rows)
    for name in CORE:
        print(f"  {name:<12} {lookup.get(name, 'ABSENT')}")
    core_rows = [(n, lookup.get(n, "ABSENT")) for n in CORE]
    print(f"  core_sha256  {digest_of(core_rows)}")

    if not core_only:
        print()
        print("== COMPLETE MANIFEST ==")
        for name, version in rows:
            print(f"  {name}=={version}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
