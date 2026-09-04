"""Store path resolution and group-path construction. Pure — no I/O.

One builder, used by every place that needs a Zarr group path: `api.query` on the
read path, and the two validation call sites in `api.config` and `api.app`. That is
the point of the module. Spec 004 revision 3 proposed asserting that two separate
expressions agreed; revision 4 replaced that with a single expression, because an
assertion only holds until someone edits one of the two sides.

**Nothing here touches the filesystem.** `os.path.join` and `os.path.isabs` are
string operations. Importing this module therefore has no side effect, which is what
lets the offline tests run with no store and no environment variable — and what
keeps `api.config`'s import-time check the only import-time I/O in the package.

**`group_path` concatenates and does not normalise.** With the store literal `data/`
it produces `data//1_degree/annual/TS`, double slash included, because that is what
`query.py` has always produced and what both arms produced in C1 and C2. Tidying it
would be a behavioural change wearing a refactor's clothes, and it would change the
string whose hash decides `set` iteration order — the mechanism spec 003 is about.
"""

import os

# The one group whose absence means no default request can be served: one-degree,
# annual, temperature/salinity. Stated, not derived from `grid_dir` /
# `determine_subgroup`, so that an unrelated edit to those cannot silently redefine
# what the service validates at startup (spec 004 section 17.2).
#
# WOA23 publishes this combination most broadly: per the product documentation,
# oxygen is one-degree only and the inorganic nutrients are one-degree and 'all'
# time span only, so no other grid/parameter pairing is a safe universal anchor.
ANCHOR_GRID = "1_degree"
ANCHOR_SUBGROUP = "annual/TS"


def group_path(store, grid_path, subgroup):
    """The Zarr group path, byte-identical to what `query.py` has always built."""
    return f"{store}/{grid_path}/{subgroup}"


def anchor_path(store):
    """The required anchor group's path under `store`."""
    return group_path(store, ANCHOR_GRID, ANCHOR_SUBGROUP)


def resolve(store, cwd):
    """The absolute path the read path will use. Reports resolution; never changes it.

    A relative `WOA23_ZARR_STORE` resolves against the process's working directory,
    and that is left exactly as it is — C1 and C2 both ran with the literal `data/`
    deliberately, to match `woa23_app.py:63`. This makes the resolution visible so a
    failure can name it.
    """
    return store if os.path.isabs(store) else os.path.join(cwd, store)


def describe(store, cwd):
    """The string every failure message must contain.

    "data/ not found" is not actionable; the same configuration means different
    stores depending on where the process was started, so the message carries all
    three facts a reader needs to tell which store was actually looked for.
    """
    return (f"{resolve(store, cwd)} "
            f"(WOA23_ZARR_STORE={store!r}, cwd={cwd!r})")
