"""Fixed source manifests for the backends under test.

`--source a --source b` puts completeness in the operator's hands, and a file left
off the list is invisible: the record looks complete and simply omits the thing that
changed. So the manifests live here, in code, shared by the sidecar and the spec,
and they are **recursive glob patterns** rather than enumerated files — a new module
added to `api/`, at any depth, is covered without anyone remembering a flag.

A pattern that matches nothing is an error, not an empty result. That is what
catches a manifest gone stale against a moved directory.

Hashing a superset is safe; hashing a subset is not. `src/**/*.py` covers all four
modules even though `woa23_app.py` imports only `src.dask_client_manager` — the
other two (`config.py`, `woa23_utils.py`) are unused by the API path, and including
them costs nothing while guarding against that changing unnoticed.
"""

from __future__ import annotations

from pathlib import Path

# label -> glob patterns, relative to the backend process's cwd
# Recursive by design. An earlier version used `src/*.py` and `api/*.py`, which
# cover one directory level only — a module moved into a subpackage would have
# dropped silently out of the hash set while the record still looked complete. `**`
# removes that failure mode entirely, and costs nothing while the layouts stay flat.
MANIFESTS: dict[str, tuple[str, ...]] = {
    # Reference: the unmodified production app, cwd ~/python/woa23
    "reference": ("woa23_app.py", "src/**/*.py"),
    # Candidate: spec 001 section 4.1, cwd <candidate root>/dev2026
    "candidate": ("api/**/*.py",),
}


def expand(label: str, cwd: Path) -> list[Path]:
    """Resolve a manifest against a working directory.

    Raises when a pattern matches nothing, so a manifest that has drifted from the
    layout fails loudly instead of producing a confident, empty hash set.
    """
    try:
        patterns = MANIFESTS[label]
    except KeyError:
        raise SystemExit(
            f"unknown manifest {label!r}; expected one of {sorted(MANIFESTS)}")

    out: list[Path] = []
    for pattern in patterns:
        matched = sorted(cwd.glob(pattern))
        if not matched:
            raise SystemExit(
                f"manifest {label!r}: pattern {pattern!r} matched no files under "
                f"{cwd}. The manifest and the layout have diverged — fix one of "
                f"them rather than recording an incomplete source hash set."
            )
        out.extend(matched)
    return out
