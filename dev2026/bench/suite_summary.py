"""The suite summary contract, for the Python suites.

The shell side of the same contract lives in ``scripts/lib_suite_summary.sh``, and the two
must agree exactly, because the runner parses one format and does not know which language
produced it.

    ASSERTIONS=<n> FAILED=<m>

WHY THIS EXISTS. The batch runner displayed each suite's LAST stdout line and the totals
were read off that display. ``test_staging_store.py`` printed the standard summary and then
a CAVEAT, so the caveat was displayed and its 24 assertions were invisible;
``test_bootstrap_delivery.sh`` used a private shape and lost 100 more. The batch log
reported 4667 where the truth was 4791, and reported it as though it were the total.

THE PROSE LINE IS STILL PRINTED for a person reading the log. It is not what is parsed.
Making one line serve both purposes is the whole of the original defect.

THE CONTRACT LINE IS LAST. Nothing may follow it. A suite with something to say after its
result must say it before.
"""

from __future__ import annotations

import sys


def summary_line(assertions: int, failed: int) -> None:
    """Print only the contract line, for a suite that controls its own exit path.

    The counts are validated rather than trusted: a summary carrying a non-number is not
    machine-readable, and emitting one would leave the runner guessing what was meant.
    """
    if not isinstance(assertions, int) or isinstance(assertions, bool):
        raise SystemExit(f"SUITE SUMMARY ERROR: assertions is not an int: {assertions!r}")
    if not isinstance(failed, int) or isinstance(failed, bool):
        raise SystemExit(f"SUITE SUMMARY ERROR: failed is not an int: {failed!r}")
    if assertions < 0 or failed < 0:
        raise SystemExit(
            f"SUITE SUMMARY ERROR: negative count: ASSERTIONS={assertions} FAILED={failed}"
        )
    # MORE FAILURES THAN ASSERTIONS IS NOT A COUNT, it is a bug in whoever is counting.
    if failed > assertions:
        raise SystemExit(
            f"SUITE SUMMARY ERROR: FAILED={failed} exceeds ASSERTIONS={assertions}"
        )
    print(f"ASSERTIONS={assertions} FAILED={failed}")
    sys.stdout.flush()


def summary(passed: int, failed: int) -> int:
    """Print the prose line, then the contract line; return the exit status.

    THE EXIT STATUS IS DERIVED FROM THE COUNT, never passed in beside it. A suite claiming
    ``FAILED=3`` while exiting 0 would be making two claims that cannot both be true, and
    the runner refuses that combination. Deriving it here means no suite can produce it by
    accident.

    Callers end with ``raise SystemExit(summary(PASS, FAIL))``.
    """
    total = passed + failed
    if failed:
        print(f"{failed} FAILED, {passed} passed")
    else:
        print(f"all passed ({total} assertions)")
    summary_line(total, failed)
    return 1 if failed else 0
