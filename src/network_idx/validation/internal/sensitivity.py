"""
Sensitivity analysis of the index to its construction choices (Axis A1).

This is Step 8 of the OECD/JRC handbook and the single most important internal test:
because the index has no observed label, its credibility rests on being *robust* to the
subjective choices made when building it. We re-combine the three published sub-indices
under a grid of defensible alternatives — different bucket weights and a geometric
instead of a linear aggregation — and measure how far parcel ranks move relative to the
shipped baseline. Small, well-bounded rank movement means the ranking customers see is a
property of the data rather than an artifact of one arbitrary weighting.

The functions here are pure and operate on an in-memory `parcel_scores` frame that
already carries the three 0-100 sub-indices; they never touch BigQuery. Re-combination
mirrors `scoring.parcel_score` exactly: form the overall from the sub-indices under the
chosen weights and aggregation, then min-max rescale to 0-100 over the population.
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from network_idx.constants import SCORING_BUCKET_WEIGHTS

SUBINDEX_COLUMNS = {"growth": "idx_growth", "telecom": "idx_telecom", "demo": "idx_demo"}

# Suggested acceptance thresholds from the validation methodology (Axis A).
MAX_MEDIAN_RANK_SHIFT_PTS = 5.0
MIN_SPEARMAN_RHO = 0.95


def _rescale_0_100(raw: pd.Series) -> pd.Series:
    """Min-max rescale a raw overall onto 0-100, matching the scoring pushdown; a
    degenerate (constant) population collapses to zero exactly as the SQL CASE does."""
    lo, hi = float(raw.min()), float(raw.max())
    if hi - lo <= 0:
        return pd.Series(np.zeros(len(raw)), index=raw.index)
    return 100.0 * (raw - lo) / (hi - lo)


def recombine_overall(
    scores: pd.DataFrame,
    bucket_weights: Dict[str, float],
    mode: str = "linear",
) -> pd.Series:
    """
    Recompute the 0-100 overall index from the three sub-indices under a weighting.

    The linear mode reproduces the shipped aggregation (a bucket-weighted sum of the
    sub-indices); the geometric mode replaces it with a weighted geometric mean, which
    penalises imbalance across buckets and is the handbook's recommended alternative
    when compensability between buckets is a concern. Weights are renormalised to sum to
    one so scenarios that scale a single bucket stay comparable. The raw combination is
    min-max rescaled to 0-100 exactly as in `scoring.parcel_score`.
    """
    total = float(sum(bucket_weights.values()))
    if total <= 0:
        raise ValueError("bucket_weights must sum to a positive number")
    weights = {b: bucket_weights[b] / total for b in bucket_weights}

    if mode == "linear":
        raw = sum(weights[b] * scores[SUBINDEX_COLUMNS[b]] for b in weights)
    elif mode == "geometric":
        # Clamp at a small epsilon so a legitimate zero sub-index does not send the whole
        # geometric mean to zero and wipe out all rank information in that band.
        eps = 1e-6
        log_raw = sum(
            weights[b] * np.log(scores[SUBINDEX_COLUMNS[b]].clip(lower=eps)) for b in weights
        )
        raw = np.exp(log_raw)
    else:
        raise ValueError(f"Unknown aggregation mode: {mode!r}")
    return _rescale_0_100(pd.Series(raw, index=scores.index))


def rank_biased_overlap(order_a: List, order_b: List, p: float = 0.98) -> float:
    """
    Rank-biased overlap between two rankings, top-weighted by the persistence `p`.

    RBO compares two ordered lists with more weight on the top ranks, which is the right
    emphasis when customers act on the highest-scoring parcels. It returns 1.0 for
    identical orders and approaches 0 as they diverge; the extrapolated form is used so
    unequal-length prefixes still yield a value in [0, 1].
    """
    if not order_a or not order_b:
        return 0.0
    seen_a: set = set()
    seen_b: set = set()
    overlap = 0
    depth = min(len(order_a), len(order_b))
    rbo = 0.0
    for d in range(depth):
        seen_a.add(order_a[d])
        seen_b.add(order_b[d])
        overlap = len(seen_a & seen_b)
        agreement = overlap / (d + 1)
        rbo += (p ** d) * agreement
    rbo *= (1 - p)
    # Extrapolate the tail assuming the final agreement persists.
    rbo += (agreement) * (p ** depth)
    return float(min(1.0, rbo))


def rank_shift_stats(
    baseline: pd.Series, variant: pd.Series, bands: int = 10
) -> Dict[str, float]:
    """
    Summarise how far parcel ranks move between a baseline and a variant scoring.

    Ranks are expressed as percentiles so the shift is reported in percentile points
    (the unit the methodology's pass criterion uses); the Spearman correlation captures
    overall monotone agreement, and the band-agreement fraction reports how often the
    decile a customer sees is unchanged. RBO on the top-ranked order adds a top-weighted
    view of the same movement.
    """
    base_pct = baseline.rank(pct=True) * 100.0
    var_pct = variant.rank(pct=True) * 100.0
    abs_shift = (var_pct - base_pct).abs()

    base_band = np.minimum((base_pct / (100.0 / bands)).astype(int), bands - 1)
    var_band = np.minimum((var_pct / (100.0 / bands)).astype(int), bands - 1)

    order_a = list(baseline.sort_values(ascending=False).index)
    order_b = list(variant.sort_values(ascending=False).index)

    return {
        "median_abs_rank_shift": float(abs_shift.median()),
        "p90_abs_rank_shift": float(abs_shift.quantile(0.90)),
        "spearman_rho": float(baseline.corr(variant, method="spearman")),
        "band_agreement": float((base_band == var_band).mean()),
        "rank_biased_overlap": rank_biased_overlap(order_a, order_b),
    }


@dataclass(frozen=True)
class Scenario:
    """One defensible alternative construction: a name, bucket weights, and aggregation."""

    name: str
    bucket_weights: Dict[str, float]
    mode: str = "linear"


def default_scenarios(baseline_weights: Optional[Dict[str, float]] = None) -> List[Scenario]:
    """
    Build the standard grid of construction alternatives to stress the index against.

    The grid covers equal weighting, a geometric aggregation, and a one-standard-error
    style up/down perturbation of the dominant telecom bucket — the choices the
    methodology names as the ones most likely to move ranks. Callers can extend the list
    with model-specific variants (for example an alternative SHAP vintage).
    """
    base = dict(baseline_weights or SCORING_BUCKET_WEIGHTS)
    scenarios = [
        Scenario("equal_weights", {b: 1.0 for b in base}, "linear"),
        Scenario("geometric_aggregation", dict(base), "geometric"),
        Scenario(
            "telecom_up_10pct",
            {**base, "telecom": base["telecom"] * 1.10},
            "linear",
        ),
        Scenario(
            "telecom_down_10pct",
            {**base, "telecom": base["telecom"] * 0.90},
            "linear",
        ),
    ]
    return scenarios


def run_sensitivity(
    scores: pd.DataFrame,
    scenarios: Optional[List[Scenario]] = None,
    baseline_weights: Optional[Dict[str, float]] = None,
    baseline_mode: str = "linear",
) -> pd.DataFrame:
    """
    Run the full sensitivity sweep and return one scored row per scenario.

    Each scenario is compared against the baseline overall (recomputed from the same
    sub-indices so the comparison isolates the construction choice, not rounding), and
    the row carries the rank-shift statistics plus a `passed` flag against the suggested
    thresholds. The frame is the internal-axis contribution to the validation dossier.
    """
    base_weights = dict(baseline_weights or SCORING_BUCKET_WEIGHTS)
    scenarios = scenarios or default_scenarios(base_weights)
    baseline_overall = recombine_overall(scores, base_weights, baseline_mode)

    rows = []
    for sc in scenarios:
        variant_overall = recombine_overall(scores, sc.bucket_weights, sc.mode)
        stats = rank_shift_stats(baseline_overall, variant_overall)
        passed = (
            stats["median_abs_rank_shift"] < MAX_MEDIAN_RANK_SHIFT_PTS
            and stats["spearman_rho"] > MIN_SPEARMAN_RHO
        )
        rows.append({"scenario": sc.name, "mode": sc.mode, **stats, "passed": passed})
    return pd.DataFrame(rows)
