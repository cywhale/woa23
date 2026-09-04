#!/usr/bin/env python3
"""Build the small synthetic Zarr store the PM2 staging validation serves from.

**What it is for.** `api.config` requires `WOA23_ZARR_STORE` at import and `api.app`'s
lifespan opens the anchor group before the worker serves anything, so a PM2 staging run
needs a store. It does **not** need production's.

**What it is NOT, and this matters more than what it is.** A response served from this
store says the deployment machinery works — PM2 starts the candidate, the lifespan
opens the anchor, readiness answers, the endpoints serialise, the rows come out in
contract order. It says **nothing whatever about real WOA23 data correctness**. That is
what C1 (`c1f`) and C2 (`c2g`) establish, against the real store, and no result from
this fixture may be quoted as if it were theirs.

**Why not production's store.** A read-only symlink to it would make the staging store
resolve *inside* the production tree, which `deploy/start_staging.sh` refuses by design.
The guard is not disabled for convenience; the fixture exists so it does not have to be.

**Deterministic.** Values come from `numpy.arange`, coordinates from fixed lists, and
nothing depends on the clock, the filesystem or a random seed. Two builds on two
machines produce the same file-list digest, which is what makes the size and digest in
the execution request checkable rather than decorative.

**Shape.** It carries what the query path actually touches, and only that:

- the **anchor group** `1_degree/annual/TS`, which the lifespan opens;
- `1_degree/monthly/TS` and `1_degree/seasonal/TS`, so a query can span **several
  time_period values across several groups** — which is what makes the row-order check
  meaningful rather than trivially satisfied by one group's natural order;
- coordinates `lon`, `lat`, `depth`, `parameters`, `time_periods`, matching the names
  `api/query.py` selects on;
- variables `an` and `mn` — `mn` because the default `append` is `mn` and the rename to
  the bare parameter name depends on it.

    uv run python -m deploy.make_staging_store /path/to/store
"""

import hashlib
import sys
from pathlib import Path

import numpy as np
import xarray as xr

#: `determine_subgroup` in api/query.py: period '0' is annual, 1-12 monthly, else
#: seasonal; temperature and salinity live in TS. These three groups are therefore what
#: a query over periods 0, 1, 2 and 13 has to open.
GROUPS = (
    ("1_degree/annual/TS", ["0"]),
    ("1_degree/monthly/TS", ["1", "2"]),
    ("1_degree/seasonal/TS", ["13"]),
)

#: Small, and on the 1-degree grid's half-degree centres so `to_lowest_grid_point`
#: snaps onto them. Four longitudes and three latitudes give 12 cells per depth per
#: period — enough that a row order is a real ordering and not a single row.
LON = [134.5, 135.5, 136.5, 137.5]
LAT = [14.5, 15.5, 16.5]
DEPTH = [0.0, 10.0, 100.0]
PARAMS = ["temperature", "salinity"]


def build_group(root: Path, rel: str, periods) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    shape = (len(LON), len(LAT), len(DEPTH), len(PARAMS), len(periods))
    n = int(np.prod(shape))
    # arange, not random: the store must be byte-reproducible.
    an = np.arange(n, dtype="float32").reshape(shape)
    xr.Dataset(
        {"an": (("lon", "lat", "depth", "parameters", "time_periods"), an),
         "mn": (("lon", "lat", "depth", "parameters", "time_periods"), an + 0.5)},
        coords={"lon": LON, "lat": LAT, "depth": DEPTH,
                "parameters": PARAMS, "time_periods": list(periods)},
    ).to_zarr(str(path), mode="w", consolidated=True)


def file_list_digest(root: Path):
    """The same shape of identity the archive verifier uses: per-file sha256 over a
    sorted relative-path listing, hashed. Reported in the request so VM24 can confirm
    it built the same thing."""
    entries = []
    total = 0
    for f in sorted(p for p in root.rglob("*") if p.is_file()):
        data = f.read_bytes()
        total += len(data)
        entries.append(f"{hashlib.sha256(data).hexdigest()}  "
                       f"{f.relative_to(root).as_posix()}")
    listing = "\n".join(entries) + "\n"
    return hashlib.sha256(listing.encode()).hexdigest(), len(entries), total


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    root = Path(sys.argv[1])
    if root.exists():
        # Refused rather than cleared. The campaign's rule everywhere else is that a
        # run verifies its target and does not delete it; a recursive delete built
        # from a path argument is one substitution away from catastrophic.
        print(f"refusing: {root} already exists. Name a path that does not.",
              file=sys.stderr)
        return 2
    root.mkdir(parents=True)
    for rel, periods in GROUPS:
        build_group(root, rel, periods)

    digest, n_files, total = file_list_digest(root)
    print(f"store            : {root.resolve()}")
    print(f"groups           : {len(GROUPS)}")
    for rel, periods in GROUPS:
        print(f"  {rel:24s} periods={','.join(periods)}")
    print(f"grid             : {len(LON)} lon x {len(LAT)} lat x {len(DEPTH)} depth "
          f"x {len(PARAMS)} params")
    print(f"variables        : an, mn")
    print(f"files            : {n_files}")
    print(f"bytes            : {total:,}")
    print(f"file_list_sha256 : {digest}")
    print()
    print("This store proves DEPLOYMENT MACHINERY only. It is not WOA23 data and no")
    print("result from it may be reported as real-store correctness — that is c1f and")
    print("c2g's, against the real store.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
