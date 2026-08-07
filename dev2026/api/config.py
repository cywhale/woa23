"""Static tables and the Zarr store location.

Lifted from `woa23_app.py` unchanged except for the store path (see below). Spec
001 section 4.4 makes the whole candidate a two-line behavioural change, so the
tables are carried across verbatim — including the ones the read path never
touches. Dropping dead entries here would make the diff harder to review for no
benefit; tidying them is S2's business, not this step's.

Unreferenced in the original and still unreferenced here: `grid_resolutions`,
`parameters`, `parameters_name`. They are stated explicitly so a reviewer does not
have to work out whether their absence from `query.py` is an omission.
"""

import os

# Spec 001 section 4.2: explicit, mandatory, no relative fallback.
#
# `woa23_app.py:63` hard-codes `zarr_store_path = "data/"`, which resolves against
# the process cwd — correct only because production gunicorn runs from
# ~/python/woa23. The candidate runs from dev2026/, where the same string would
# point at a directory that does not exist. Failing at import is deliberate: a
# fallback that half-works is how you ship a benchmark that measured the wrong
# thing.
#
# The bare subscript is the spec's, verbatim. An earlier draft wrapped it to raise
# a friendlier RuntimeError — an undeclared difference, and exactly the kind of
# small improvement that makes a "no other changes" claim untrue.
#
# The identifier keeps its original name, so `process_woa23_data` needs no edit for
# it: only the value's source changed, and that change lives here.
zarr_store_path = os.environ["WOA23_ZARR_STORE"]

# Two gridded resolutions in WOA23: 1-degree and 0.25-degree.
grid_resolutions = {'01': '1.00', '04': '0.25'}   # unreferenced, carried over
grid_dir = {'01': '1_degree', '04': '025_degree'}

parameters = {                                    # unreferenced, carried over
    't': 'temperature',
    's': 'salinity',
    'o': 'oxygen',
    'O': 'o2sat',
    'A': 'AOU',
    'i': 'silicate',
    'p': 'phosphate',
    'n': 'nitrate'
}

parameters_name = {                               # unreferenced, carried over
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
