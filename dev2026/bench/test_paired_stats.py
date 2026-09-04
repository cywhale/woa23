"""Tests for the comparison statistics. No network — runs anywhere, in a second.

The live self-test that pointed both arms at public production has been retired:
it generated real traffic against a production service every time anyone wanted to
know whether the arithmetic was right. The null case it was checking is a property
of the statistics, so it is checked here with synthetic samples instead.

    uv run python -m bench.test_paired_stats
"""

from __future__ import annotations

import random
import sys

from bench.paired_stats import (
    ESCALATION_LADDER, PRACTICAL_MARGIN, bootstrap_ratio, improvement_verdict,
    next_rung, plan_rung, regression_verdict, required_n, warm,
)
from bench.suite_summary import summary, summary_line   # noqa: E402

failures: list[str] = []


passed: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        passed.append(name)
        print(f"  ok   {name}")
    else:
        print(f"  FAIL {name}  {detail}")
        failures.append(name)


def test_warm() -> None:
    check("warm drops the cold sample", warm([9.0, 1.0, 2.0]) == [1.0, 2.0])
    try:
        warm([1.0])
        check("warm rejects a too-short series", False, "no exception raised")
    except ValueError:
        check("warm rejects a too-short series", True)


def test_verdicts() -> None:
    m = PRACTICAL_MARGIN
    check("regression established", regression_verdict(1.06, 1.20, m) == "REGRESSION")
    check("no regression established", regression_verdict(0.90, 1.04, m) == "NO_REGRESSION")
    check("straddling is inconclusive", regression_verdict(0.99, 1.10, m) == "INCONCLUSIVE")
    check("boundary counts as no-regression",
          regression_verdict(0.90, 1.05, m) == "NO_REGRESSION")
    check("improvement established", improvement_verdict(0.70, 0.95) == "IMPROVED")
    check("absence of improvement established",
          improvement_verdict(1.01, 1.30) == "NOT_IMPROVED")
    check("improvement inconclusive", improvement_verdict(0.95, 1.05) == "INCONCLUSIVE")


def test_required_n_regression() -> None:
    """The exact case that exposed the symmetric-half-width bug.

    ratio 1.022, CI [1.003, 1.051], margin 5%: the verdict is INCONCLUSIVE because
    ci_high exceeds 1.05, so required_n must ask for MORE than the 21 samples in
    hand. The old symmetric-half-width version returned 21, contradicting the
    verdict it was supposed to explain.
    """
    n = required_n(21, 1.022, 1.003, 1.051, PRACTICAL_MARGIN)
    verdict = regression_verdict(1.003, 1.051, PRACTICAL_MARGIN)
    check("the failing case is still INCONCLUSIVE", verdict == "INCONCLUSIVE")
    check("required_n exceeds the current n when inconclusive",
          n is not None and n > 21, f"got {n}")

    # Asymmetry must actually be honoured: same width, opposite skew, different answer.
    wide_up = required_n(21, 1.00, 0.99, 1.20, PRACTICAL_MARGIN)
    wide_down = required_n(21, 1.10, 1.00, 1.11, PRACTICAL_MARGIN)
    check("upper-skewed interval binds on ci_high", wide_up is not None and wide_up > 21,
          f"got {wide_up}")
    check("estimate above threshold binds on ci_low",
          wide_down is not None and wide_down > 21, f"got {wide_down}")

    check("a decided case needs no more samples",
          required_n(21, 1.00, 0.98, 1.02, PRACTICAL_MARGIN) == 21)
    check("an effect exactly on the margin is unresolvable",
          required_n(21, 1.05, 1.00, 1.10, PRACTICAL_MARGIN) is None)


def test_ladder() -> None:
    check("ladder is ascending", list(ESCALATION_LADDER) == sorted(ESCALATION_LADDER))
    check("next rung after the first", next_rung(21) == 60)
    check("ladder terminates", next_rung(ESCALATION_LADDER[-1]) is None)


def test_null_case() -> None:
    """Two arms from one distribution: false verdicts at the nominal rate, no more.

    This is what the live production self-test was for. A first version of this test
    asserted the gate would *never* call a false verdict, and it failed on trial 2
    of 20 — correctly. A 95% interval is wrong 5% of the time by construction, and
    each verdict here is one-sided, so ~2.5% per direction is the floor, not a
    defect. What must hold is that the observed rate stays near nominal.

    The consequence for the gate is documented in spec 001 section 6.1.2: across
    eight cases, seeing one spurious REGRESSION now and then is expected, which is
    why a single flag escalates to the next rung of the sample ladder rather than
    condemning the change outright.
    """
    rng = random.Random(4242)
    trials = 100
    false_reg = false_imp = 0
    for trial in range(trials):
        a = [rng.lognormvariate(0, 0.25) * 100 for _ in range(21)]
        b = [rng.lognormvariate(0, 0.25) * 100 for _ in range(21)]
        s = bootstrap_ratio(a, b, rounds=800, seed=1000 + trial)
        if regression_verdict(s["ci95_low"], s["ci95_high"], PRACTICAL_MARGIN) == "REGRESSION":
            false_reg += 1
        if improvement_verdict(s["ci95_low"], s["ci95_high"]) == "IMPROVED":
            false_imp += 1
    print(f"       false REGRESSION {false_reg}/{trials}, "
          f"false IMPROVED {false_imp}/{trials} (nominal ~2.5% each)")
    check("false REGRESSION rate stays near nominal", false_reg <= 0.10 * trials,
          f"{false_reg}/{trials}")
    check("false IMPROVED rate stays near nominal", false_imp <= 0.10 * trials,
          f"{false_imp}/{trials}")


def test_shift_is_detected() -> None:
    """A real 40% slowdown must be caught, or the gate is decorative."""
    rng = random.Random(99)
    a = [rng.lognormvariate(0, 0.15) * 140 for _ in range(21)]
    b = [rng.lognormvariate(0, 0.15) * 100 for _ in range(21)]
    s = bootstrap_ratio(a, b, rounds=2000, seed=7)
    check("a 40% slowdown is called a REGRESSION",
          regression_verdict(s["ci95_low"], s["ci95_high"], PRACTICAL_MARGIN) == "REGRESSION",
          f"ratio {s['median_ratio']}")

    s2 = bootstrap_ratio(b, a, rounds=2000, seed=7)
    check("the mirror image is called an IMPROVEMENT",
          improvement_verdict(s2["ci95_low"], s2["ci95_high"]) == "IMPROVED",
          f"ratio {s2['median_ratio']}")


def test_plan_rung() -> None:
    check("an off-ladder estimate maps to the next usable rung",
          plan_rung(21, 23) == 60, f"got {plan_rung(21, 23)}")
    check("an estimate already met still advances the ladder",
          plan_rung(21, 21) == 60, f"got {plan_rung(21, 21)}")
    check("an estimate beyond the ladder returns None",
          plan_rung(21, 500) is None)
    check("an unresolvable estimate returns None", plan_rung(21, None) is None)


def simulate_ladder(a_scale: float, trials: int, seed: int) -> dict:
    """Run the full 21 -> 60 -> 150 escalation procedure against synthetic data.

    Escalation is what the gate actually does, and it takes several looks at the
    same question, so its error rate is not the single-comparison rate. This
    measures the procedure end to end rather than assuming it inherits the nominal
    2.5%.
    """
    rng = random.Random(seed)
    counts = {"REGRESSION": 0, "NO_REGRESSION": 0, "INCONCLUSIVE": 0, "IMPROVED": 0}
    rungs_used = []
    for trial in range(trials):
        reg = imp = "INCONCLUSIVE"
        used = ESCALATION_LADDER[0]
        for rung in ESCALATION_LADDER:
            used = rung
            a = [rng.lognormvariate(0, 0.25) * 100 * a_scale for _ in range(rung)]
            b = [rng.lognormvariate(0, 0.25) * 100 for _ in range(rung)]
            s = bootstrap_ratio(a, b, rounds=400, seed=seed + trial * 10 + rung)
            reg = regression_verdict(s["ci95_low"], s["ci95_high"], PRACTICAL_MARGIN)
            imp = improvement_verdict(s["ci95_low"], s["ci95_high"])
            # Stop only once the no-regression answer is established; a REGRESSION
            # flag escalates so that a one-off does not condemn the change.
            if reg == "NO_REGRESSION":
                break
        counts[reg] += 1
        if imp == "IMPROVED":
            counts["IMPROVED"] += 1
        rungs_used.append(used)
    counts["mean_rung"] = round(sum(rungs_used) / len(rungs_used), 1)
    return counts


def test_ladder_null_simulation() -> None:
    """Error rate of the *whole escalation procedure* under the null."""
    trials = 60
    c = simulate_ladder(1.0, trials, seed=8080)
    print(f"       null, {trials} trials through 21->60->150: "
          f"REGRESSION {c['REGRESSION']}, NO_REGRESSION {c['NO_REGRESSION']}, "
          f"INCONCLUSIVE {c['INCONCLUSIVE']}, false IMPROVED {c['IMPROVED']}, "
          f"mean final rung {c['mean_rung']}")
    check("escalation does not manufacture regressions under the null",
          c["REGRESSION"] <= 0.10 * trials, f"{c['REGRESSION']}/{trials}")
    check("escalation does not manufacture improvements under the null",
          c["IMPROVED"] <= 0.10 * trials, f"{c['IMPROVED']}/{trials}")


def test_ladder_detects_real_regression() -> None:
    """A genuine 20% slowdown must survive escalation, not be diluted by it."""
    trials = 20
    c = simulate_ladder(1.20, trials, seed=5150)
    print(f"       +20% slowdown, {trials} trials: REGRESSION {c['REGRESSION']}, "
          f"NO_REGRESSION {c['NO_REGRESSION']}, INCONCLUSIVE {c['INCONCLUSIVE']}")
    check("a real 20% slowdown is caught by the procedure",
          c["REGRESSION"] >= 0.80 * trials, f"{c['REGRESSION']}/{trials}")


def main() -> int:
    for fn in (test_warm, test_verdicts, test_required_n_regression, test_ladder,
               test_plan_rung, test_null_case, test_shift_is_detected,
               test_ladder_null_simulation, test_ladder_detects_real_regression):
        print(f"\n{fn.__name__}")
        fn()
    total = len(passed) + len(failures)
    # Reported rather than written into prose: every hand-maintained count in
    # the spec has gone stale within a revision or two.
    print()
    return summary(total - len(failures), len(failures))


if __name__ == "__main__":
    raise SystemExit(main())
