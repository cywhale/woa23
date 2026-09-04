#!/usr/bin/env python3
"""The column-order contract: JSON field order and CSV header order. Offline.

Spec 015. `pm2G` restarted the staging candidate and the CSV header came back
`temperature_an,temperature` before and `temperature,temperature_an` after — same query,
same data, same 144 rows, and the VALUES moved with the header, so a positional CSV
reader silently swapped two columns.

The offline suite never saw it because a single interpreter cannot observe its own
`PYTHONHASHSEED` varying. **So the tests that matter here run the ordering in
SUBPROCESSES with explicit seeds.** A test that only calls the helper twice in one
process would pass against the bug it is supposed to catch.

Every gate is also exercised FAILING — including a negative control that removes the
canonical ordering and requires the suite to notice.

    uv run python -m bench.test_column_order
"""

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

# api.config refuses to import without a real store, and this suite never reads data —
# it is about column ORDER, which is decided from names. A scratch directory satisfies
# the import without pretending to be a store.
_SCRATCH = Path(os.environ.get("TMPDIR", "/tmp")) / "woa23-column-order-import"
_SCRATCH.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("WOA23_ZARR_STORE", str(_SCRATCH))
os.environ.setdefault("WOA23_ANCHOR_REL", ".")

from api.config import available_vars  # noqa: E402
from api.query import INDEX_COLUMNS, canonical_column_order  # noqa: E402
from bench.suite_summary import summary, summary_line   # noqa: E402

PASS = FAIL = 0

AVAILABLE_PARS_1DEG = ['temperature', 'salinity', 'oxygen', 'o2sat', 'AOU',
                       'silicate', 'phosphate', 'nitrate']


def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  ok   {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name}\n       expected {expected!r}\n       got      {actual!r}")


def canon(requested_pars, requested_vars):
    """The canonical `pars` / `variables` the endpoint would build, from a REQUEST SET.

    Mirrors what `process_woa23_data` does: dedup by set, order by declaration. Taking a
    set as input is deliberate — it makes it impossible for a test to accidentally smuggle
    the request's order in and then congratulate the implementation for preserving it.
    """
    pars = [p for p in AVAILABLE_PARS_1DEG if p in set(requested_pars)]
    variables = [v for v in available_vars if v in set(requested_vars)]
    return pars, variables


def columns_for(pars, variables, scrambled=None):
    """The frame's own column names, in a DELIBERATELY unhelpful order."""
    cols = list(INDEX_COLUMNS)
    for p in pars:
        for v in variables:
            cols.append(p if v == "mn" else f"{p}_{v}")
    if scrambled is not None:
        cols = [cols[i] for i in scrambled]
    return cols


# ----------------------------------------------------------- the decided canonical order
print("the canonical order is parameter-major, from the declarations (spec 015 §4)")

pars, variables = canon({"temperature", "salinity", "oxygen"}, {"mn", "an"})
check("available_vars puts 'an' before 'mn'", ["an", "mn"], variables)
check("available_pars orders the parameters", ["temperature", "salinity", "oxygen"], pars)

present = columns_for(pars, variables)
check("the PI's worked example, exactly",
      "lon,lat,depth,time_period,temperature_an,temperature,"
      "salinity_an,salinity,oxygen_an,oxygen",
      ",".join(canonical_column_order(present, pars, variables)))

check("the index columns come first and in their fixed order",
      list(INDEX_COLUMNS),
      canonical_column_order(present, pars, variables)[:4])

check("'mn' is renamed to the bare parameter but keeps mn's SLOT",
      ["temperature_an", "temperature"],
      canonical_column_order(columns_for(*canon({"temperature"}, {"an", "mn"})),
                             *canon({"temperature"}, {"an", "mn"}))[4:])

# ------------------------------------------------------- input order must not reach output
print()
print("the request's own order does not reach the output")

for req_vars in (["an", "mn"], ["mn", "an"]):
    p, v = canon({"temperature", "salinity"}, req_vars)
    check(f"append={','.join(req_vars)} gives the canonical order",
          "lon,lat,depth,time_period,temperature_an,temperature,salinity_an,salinity",
          ",".join(canonical_column_order(columns_for(p, v), p, v)))

for req_pars in (["temperature", "salinity"], ["salinity", "temperature"]):
    p, v = canon(req_pars, {"mn"})
    check(f"parameter={','.join(req_pars)} gives the canonical order",
          "lon,lat,depth,time_period,temperature,salinity",
          ",".join(canonical_column_order(columns_for(p, v), p, v)))

# A frame whose columns arrive in a hostile order — what pivot can hand over.
p, v = canon({"temperature", "salinity", "oxygen"}, {"an", "mn"})
hostile = ["oxygen", "salinity_an", "time_period", "temperature", "lon",
           "oxygen_an", "depth", "temperature_an", "lat", "salinity"]
check("a hostile incoming column order is fully normalised",
      "lon,lat,depth,time_period,temperature_an,temperature,"
      "salinity_an,salinity,oxygen_an,oxygen",
      ",".join(canonical_column_order(hostile, p, v)))

# ------------------------------------------------------------------- permutation property
print()
print("the projection is a permutation — nothing is dropped, nothing invented")

for label, cols in (("the worked example", present), ("the hostile order", hostile)):
    out = canonical_column_order(cols, p, v)
    check(f"{label}: same set of columns", sorted(cols), sorted(out))
    check(f"{label}: no duplicates", len(set(out)), len(out))

check("an unexpected extra column is KEPT, not dropped",
      True,
      "surprise" in canonical_column_order(present + ["surprise"], pars, variables))
check("  and it is placed deterministically, at the end",
      "surprise",
      canonical_column_order(present + ["surprise"], pars, variables)[-1])

# -------------------------------------------------- multi-group / multi-variable coverage
print()
print("multiple groups, multiple variables, mn and an together")

p, v = canon({"temperature", "nitrate"}, {"an", "mn", "sd"})
check("variables beyond an/mn follow available_vars too", ["an", "mn", "sd"], v)
check("parameters spanning different zarr groups stay canonical",
      "lon,lat,depth,time_period,temperature_an,temperature,temperature_sd,"
      "nitrate_an,nitrate,nitrate_sd",
      ",".join(canonical_column_order(columns_for(p, v), p, v)))

p, v = canon({"temperature"}, {"an"})
check("a single non-mn variable keeps its suffixed name",
      "lon,lat,depth,time_period,temperature_an",
      ",".join(canonical_column_order(columns_for(p, v), p, v)))

p, v = canon({"nitrate"}, {"mn"})
check("the spec-006 nitrate header is unchanged by this work",
      "lon,lat,depth,time_period,nitrate",
      ",".join(canonical_column_order(columns_for(p, v), p, v)))

# ---------------------------------------------------------- empty and degenerate frames
print()
print("degenerate shapes do not raise")

check("index columns only (an empty-result frame)",
      "lon,lat,depth,time_period",
      ",".join(canonical_column_order(list(INDEX_COLUMNS), pars, variables)))
check("an empty column list", [], canonical_column_order([], pars, variables))
check("a frame missing an index column places what is there",
      "lon,lat,time_period",
      ",".join(canonical_column_order(["time_period", "lon", "lat"], pars, variables)))

# ============================================================ THE PART THAT MATTERS ======
# Separate processes, separate hash seeds. A single interpreter cannot see its own seed
# vary, which is exactly why this bug reached VM24.
print()
print("SEPARATE PROCESSES, SEPARATE HASH SEEDS — the pm2G case, offline")

PROBE = r'''
import os, sys
sys.path.insert(0, %r)
os.environ.setdefault("WOA23_ZARR_STORE", %r)
os.environ.setdefault("WOA23_ANCHOR_REL", ".")
from api.config import available_vars
from api.query import canonical_column_order

AVAILABLE_PARS = ['temperature', 'salinity', 'oxygen', 'o2sat', 'AOU',
                  'silicate', 'phosphate', 'nitrate']
# Built from SETS, so this process's hash seed decides the iteration order of the
# request -- which is the thing that must not reach the output.
requested_pars = {"temperature", "salinity", "oxygen"}
requested_vars = {"an", "mn"}
pars = [p for p in AVAILABLE_PARS if p in requested_pars]
variables = [v for v in available_vars if v in requested_vars]

# The frame's columns, in whatever order THIS process's set iteration produces --
# standing in for pivot's first-appearance order.
cols = ["lon", "lat", "depth", "time_period"]
for p in requested_pars:                      # <-- set iteration, seed-dependent
    for v in requested_vars:                  # <-- set iteration, seed-dependent
        cols.append(p if v == "mn" else p + "_" + v)

print(",".join(canonical_column_order(cols, pars, variables)))
''' % (str(REPO), str(_SCRATCH))

EXPECTED = ("lon,lat,depth,time_period,temperature_an,temperature,"
            "salinity_an,salinity,oxygen_an,oxygen")

seeds = ["0", "1", "42", "12345", "99991", "random"]
results = {}
for seed in seeds:
    env = dict(os.environ, PYTHONHASHSEED=seed)
    out = subprocess.run([sys.executable, "-c", PROBE], capture_output=True, text=True,
                         env=env, cwd=str(REPO))
    results[seed] = out.stdout.strip()
    if out.returncode != 0:
        results[seed] = f"<rc={out.returncode}> {out.stderr.strip()[:200]}"

for seed in seeds:
    check(f"PYTHONHASHSEED={seed} gives the canonical order", EXPECTED, results[seed])

check("every seed agreed with every other", 1, len(set(results.values())))

# The same probe run repeatedly under PYTHONHASHSEED=random: independent processes,
# independent seeds. This is the restart in pm2G, reproduced offline.
randoms = []
for _ in range(8):
    env = dict(os.environ, PYTHONHASHSEED="random")
    out = subprocess.run([sys.executable, "-c", PROBE], capture_output=True, text=True,
                         env=env, cwd=str(REPO))
    randoms.append(out.stdout.strip())
check("8 independent processes with random seeds all agree", 1, len(set(randoms)))
check("  and they agree on the CANONICAL order", {EXPECTED}, set(randoms))

# --------------------------------------------------------------- the seeds really differ
# If the seeds did not actually change the set iteration, the block above would be
# testing nothing. Prove the input this suite feeds the helper is genuinely unstable.
print()
print("control: the seeds really do change set iteration (else the above proves nothing)")

RAW = r'''
import sys
requested_pars = {"temperature", "salinity", "oxygen"}
requested_vars = {"an", "mn"}
cols = []
for p in requested_pars:
    for v in requested_vars:
        cols.append(p if v == "mn" else p + "_" + v)
print(",".join(cols))
'''
raw = set()
for seed in ["0", "1", "42", "12345", "99991", "7", "31337", "8675309"]:
    env = dict(os.environ, PYTHONHASHSEED=seed)
    out = subprocess.run([sys.executable, "-c", RAW], capture_output=True, text=True,
                         env=env, cwd=str(REPO))
    raw.add(out.stdout.strip())
check("the UNORDERED input varies across seeds (>1 distinct order observed)",
      True, len(raw) > 1)
print(f"       ({len(raw)} distinct raw orders across 8 seeds)")

# ------------------------------------------------------------------- NEGATIVE CONTROL
# Remove the canonical ordering and require this suite's own criterion to fail. A gate
# that cannot fail proves nothing.
print()
print("negative control: without canonical ordering, the criterion FAILS")

BROKEN = r'''
import os, sys
sys.path.insert(0, %r)
os.environ.setdefault("WOA23_ZARR_STORE", %r)
os.environ.setdefault("WOA23_ANCHOR_REL", ".")
requested_pars = {"temperature", "salinity", "oxygen"}
requested_vars = {"an", "mn"}
# THE PRE-015 BEHAVIOUR: column order is whatever the sets produced. No projection.
cols = ["lon", "lat", "depth", "time_period"]
for p in requested_pars:
    for v in requested_vars:
        cols.append(p if v == "mn" else p + "_" + v)
print(",".join(cols))
''' % (str(REPO), str(_SCRATCH))

broken = set()
for seed in ["0", "1", "42", "12345", "99991", "7", "31337", "8675309"]:
    env = dict(os.environ, PYTHONHASHSEED=seed)
    out = subprocess.run([sys.executable, "-c", BROKEN], capture_output=True, text=True,
                         env=env, cwd=str(REPO))
    broken.add(out.stdout.strip())

check("the unordered implementation is NOT stable across seeds", True, len(broken) > 1)
check("  so it would NOT satisfy the canonical criterion", False, broken == {EXPECTED})
print(f"       ({len(broken)} distinct orders — this is what pm2G saw across a restart)")

# --------------------------------------------------------- the helper is actually used
print()
print("the endpoint really calls this, and the old deferral is gone")

src = (REPO / "api" / "query.py").read_text()
check("query.py calls canonical_column_order", True, "canonical_column_order(" in src)
check("  and selects by it", True, "result_df.select(ordered)" in src)
check("  and asserts the projection is a permutation", True,
      "sorted(ordered) != sorted(result_df.columns)" in src)
check("the stale 'column order deliberately untouched' comment is gone", False,
      "Column order still follows the `list(set(...))`" in src)
check("append no longer builds variables by bare list(set(...))", False,
      "variables = list(set(" in src)
check("parameters no longer built by bare list(set(...))", False,
      "pars = list(set(" in src)
check("zarr group paths are traversed sorted", True,
      "zarr_group_paths = sorted(zarr_group_paths)" in src)

print()
sys.exit(summary(PASS, FAIL))