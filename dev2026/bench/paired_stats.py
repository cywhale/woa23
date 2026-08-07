"""Shared sampling and statistics rules for every latency comparison.

Both `paired_bench.py` (the gate) and `noise_pilot.py` (the tolerance estimator)
import from here, so a threshold measured by one is directly usable by the other.
Divergent definitions between the two were a real review finding: the pilot pooled
every sample while `http_bench.py` discarded the first as cold, which meant the
tolerance and the measurement it gated were not describing the same quantity.
"""

from __future__ import annotations

import random
import statistics

# The first request to a backend for a given case pays for connection setup, lazy
# imports, and a cold OS page cache. Every tool here discards exactly this many
# leading samples, and `http_bench.py` already reports its median over samples[1:],
# so all three agree.
WARMUP_REQUESTS = 1

BOOTSTRAP_ROUNDS = 5000
DEFAULT_SEED = 20260805


def warm(samples: list[float]) -> list[float]:
    """Drop the cold leading sample(s). Raises rather than silently returning []."""
    if len(samples) <= WARMUP_REQUESTS:
        raise ValueError(
            f"need more than {WARMUP_REQUESTS} sample(s) to have any warm ones; "
            f"got {len(samples)}"
        )
    return samples[WARMUP_REQUESTS:]


def bootstrap_ratio(a: list[float], b: list[float],
                    rounds: int = BOOTSTRAP_ROUNDS,
                    seed: int = DEFAULT_SEED) -> dict:
    """95% CI on median(a) / median(b), by resampling each arm with replacement.

    `a` is the candidate arm, `b` the reference arm, so a ratio below 1.0 means the
    candidate is faster.

    **This is an interleaved two-arm bootstrap, not an observation-paired one.**
    Each arm is resampled independently at its own size. Interleaving pairs the arms
    in *time*, which is what defends against host drift over the run, but two
    adjacent requests to different backends are not two measurements of one
    underlying quantity — there is no per-observation pairing to exploit, and none
    is claimed. A genuinely paired design would difference matched observations and
    resample the differences; that would be narrower, and it is not what this does.
    """
    rng = random.Random(seed)
    na, nb = len(a), len(b)
    ratios = []
    for _ in range(rounds):
        ra = statistics.median(rng.choices(a, k=na))
        rb = statistics.median(rng.choices(b, k=nb))
        ratios.append(ra / rb)
    ratios.sort()
    return {
        "median_ratio": round(statistics.median(a) / statistics.median(b), 4),
        "ci95_low": round(ratios[int(0.025 * rounds)], 4),
        "ci95_high": round(ratios[int(0.975 * rounds)], 4),
        "n_a": na,
        "n_b": nb,
        "bootstrap_rounds": rounds,
        "seed": seed,
    }


# How much slowdown we would actually care about.
#
# This is a PI / engineering threshold, not a statistic. Nothing in the data
# derives it; it encodes a judgement that a WOA23 query getting up to 5% slower is
# not worth blocking a change over. It is recorded here so it is visible and
# challengeable, and it needs PI sign-off rather than reviewer agreement. See the
# note on regression_verdict for why it must not be conflated with a noise estimate.
PRACTICAL_MARGIN = 0.05


def regression_verdict(ci_low: float, ci_high: float,
                       margin: float = PRACTICAL_MARGIN) -> str:
    """Three states. A comparison that cannot decide must say so.

    `margin` is how much slower we are willing to be before calling it a
    regression — a judgement about what matters, deliberately *not* an estimate of
    measurement noise.

    An earlier design fed a pilot-measured noise floor in here instead. A self-test
    with both arms pointing at the same backend showed why that fails: the pilot
    reported +/-3.9% for `point_profile` while the gate's own interval on a
    different window was [0.833, 1.211], five times wider. The two were estimating
    the same quantity from different samples and disagreeing, so a "tolerance"
    imported from one run could turn ordinary noise in another into a verdict.

    The confidence interval already carries the noise. So the threshold's only job
    is to encode what size of slowdown is worth acting on, and an interval too wide
    to clear it returns INCONCLUSIVE — meaning "collect more samples", never
    "close enough".
    """
    threshold = 1.0 + margin
    if ci_low > threshold:
        return "REGRESSION"          # established: the whole interval is worse
    if ci_high <= threshold:
        return "NO_REGRESSION"       # established: the whole interval is acceptable
    return "INCONCLUSIVE"            # the interval straddles the threshold


def required_n(current_n: int, median_ratio: float, ci_low: float, ci_high: float,
               margin: float = PRACTICAL_MARGIN) -> int | None:
    """Samples per arm needed to resolve an INCONCLUSIVE regression comparison.

    Only one side of the interval decides the verdict, and **bootstrap intervals
    are not symmetric**, so the symmetric half-width is the wrong quantity. An
    earlier version used it and produced a self-contradiction: for a ratio of 1.022
    with CI [1.003, 1.051] it reported "21 samples are enough" while the verdict
    was INCONCLUSIVE. The symmetric half-width was 0.0239, below the 0.0276 gap to
    the threshold — but the binding upper half-width was 0.0286, above it.

    So: when the estimate sits below the threshold, the binding side is `ci_high`;
    above it, `ci_low`. Bootstrap width shrinks roughly as 1/sqrt(n), so the
    requirement scales as (observed binding half-width / gap)^2.

    Returns None when the estimate sits exactly on the threshold, where no sample
    size resolves it — the effect is precisely the size we declared not worth
    acting on.
    """
    threshold = 1.0 + margin
    gap = abs(threshold - median_ratio)
    if gap < 1e-9:
        return None
    binding = (ci_high - median_ratio) if median_ratio < threshold \
        else (median_ratio - ci_low)
    if binding <= gap:
        return current_n
    return int(current_n * (binding / gap) ** 2 + 0.5)


# Pre-specified escalation ladder for an INCONCLUSIVE case. Fixed here, before any
# result exists, because "re-run until it resolves" is p-hacking: every extra peek
# at an accumulating sample raises the chance of eventually crossing a threshold by
# luck. Each rung is an independent run at that size — samples are not pooled
# across rungs — and the verdict is taken from the largest rung actually run.
ESCALATION_LADDER = (21, 60, 150)


def next_rung(current_n: int) -> int | None:
    """The next pre-specified sample size, or None when the ladder is exhausted."""
    for rung in ESCALATION_LADDER:
        if rung > current_n:
            return rung
    return None


def plan_rung(current_n: int, theoretical_n: int | None) -> int | None:
    """Translate a `required_n` estimate into a rung anyone can actually run.

    `required_n` answers "how many samples would resolve this", and its answer is
    usually not on the ladder — 23 is a valid estimate and an invalid instruction,
    because the ladder is 21/60/150 and running 23 would be an unplanned sample size
    chosen after seeing a result. This maps the estimate onto the smallest rung that
    meets or exceeds it, so the reported next step is executable and pre-specified.

    Returns None when the ladder cannot satisfy the estimate; that case goes to the
    PI rather than growing the ladder to chase a verdict.
    """
    if theoretical_n is None:
        return None
    for rung in ESCALATION_LADDER:
        if rung > current_n and rung >= theoretical_n:
            return rung
    return None


def improvement_verdict(ci_low: float, ci_high: float) -> str:
    """Whether a speedup is established, absent, or undecided."""
    if ci_high < 1.0:
        return "IMPROVED"
    if ci_low >= 1.0:
        return "NOT_IMPROVED"
    return "INCONCLUSIVE"


def noise_floor(samples: list[float], k: int,
                rounds: int = BOOTSTRAP_ROUNDS,
                seed: int = DEFAULT_SEED) -> dict:
    """Split-half estimate of the noise in a k-sample median ratio.

    Both arms are drawn from one backend's samples, so the **null hypothesis** is a
    ratio of 1.0. It is not a guarantee: the same backend still drifts with GC, page
    cache, and whatever else shares the host, and those effects are inside these
    samples rather than controlled away. Treat the result as a preliminary estimate
    of the noise floor, not a measured constant.
    """
    rng = random.Random(seed)
    ratios = []
    for _ in range(rounds):
        arm_a = statistics.median(rng.choices(samples, k=k))
        arm_b = statistics.median(rng.choices(samples, k=k))
        ratios.append(arm_a / arm_b)
    ratios.sort()
    lo = ratios[int(0.025 * rounds)]
    hi = ratios[int(0.975 * rounds)]
    # Resampling k values from a pool smaller than k cannot see noise the pool
    # never captured, and a short quiet window understates the tail badly: the same
    # case measured over 4 samples reported +/-1.3% and over 25 samples +/-10.2%.
    # Flag it rather than returning a confidently wrong tolerance.
    underpowered = len(samples) < k
    return {
        "k": k,
        "ci95_low": round(lo, 4),
        "ci95_high": round(hi, 4),
        # The tolerance a regression gate must allow so as not to fire on noise.
        "tolerance_pct": round(max(abs(lo - 1.0), abs(hi - 1.0)) * 100, 1),
        "pool_n": len(samples),
        "underpowered": underpowered,
    }
