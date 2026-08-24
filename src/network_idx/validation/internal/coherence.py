"""
Internal coherence and redundancy of the index structure (Axis A2).

A composite indicator should measure a shared construct without any one bucket simply
re-stating the overall. This module quantifies that: it reports the correlation matrix
among the three sub-indices and the overall, Cronbach's alpha across the sub-indices as
a single coherence coefficient, and whether each bucket's empirical correlation with the
overall is consistent with the weight it was assigned. Very low alpha means the buckets
are unrelated and the overall is a mash-up; very high sub-index-to-overall correlation
means that bucket dominates and the others are decorative.

Everything here is a pure function of an in-memory `parcel_scores` frame carrying the
three 0-100 sub-indices and the overall; nothing reads BigQuery.
"""
from dataclasses import dataclass
from typing import Dict, List

import numpy as np
import pandas as pd

SUBINDEX_COLUMNS = ["idx_growth", "idx_telecom", "idx_demo"]
OVERALL_COLUMN = "idx_overall"

# A single sub-index correlating this strongly with the overall means it dominates.
MAX_SUBINDEX_OVERALL_CORR = 0.90


def correlation_matrix(scores: pd.DataFrame, method: str = "spearman") -> pd.DataFrame:
    """
    Return the correlation matrix among the three sub-indices and the overall.

    Spearman is the default because the product is a ranking; Pearson is available for a
    linear-strength view. The matrix is the raw evidence for the redundancy check — an
    off-diagonal near one flags two buckets that carry the same signal.
    """
    cols = [c for c in SUBINDEX_COLUMNS + [OVERALL_COLUMN] if c in scores.columns]
    return scores[cols].corr(method=method)


def cronbach_alpha(scores: pd.DataFrame, columns: List[str] = SUBINDEX_COLUMNS) -> float:
    """
    Cronbach's alpha across the sub-indices as a single internal-consistency coefficient.

    Alpha is the standard measure of whether a set of components move together as one
    construct: it rises with the number of items and their inter-correlation. A moderate
    value supports treating the buckets as facets of one latent "fiber opportunity"; a
    value near zero says they are unrelated and combining them into one number is hard to
    defend.
    """
    item = scores[columns].astype(float)
    k = item.shape[1]
    if k < 2:
        raise ValueError("Cronbach's alpha needs at least two items")
    item_var = item.var(axis=0, ddof=1)
    total_var = item.sum(axis=1).var(ddof=1)
    if total_var == 0:
        return 0.0
    return float((k / (k - 1)) * (1 - item_var.sum() / total_var))


def weight_correlation_consistency(
    scores: pd.DataFrame, bucket_weights: Dict[str, float], method: str = "spearman"
) -> pd.DataFrame:
    """
    Check that each bucket's correlation with the overall tracks its assigned weight.

    A bucket that is given a small weight yet correlates strongly with the overall (or
    vice versa) is the "variance dominance" pathology the methodology warns about: the
    index is being driven by something other than the intended weighting. The returned
    frame pairs each bucket's normalised weight with its normalised overall-correlation
    and their gap, so large mismatches are easy to spot.
    """
    bucket_to_col = {"growth": "idx_growth", "telecom": "idx_telecom", "demo": "idx_demo"}
    total_w = float(sum(bucket_weights.values()))
    corrs = {
        b: float(scores[col].corr(scores[OVERALL_COLUMN], method=method))
        for b, col in bucket_to_col.items()
        if col in scores.columns
    }
    total_c = float(sum(abs(v) for v in corrs.values())) or 1.0
    rows = []
    for b, c in corrs.items():
        norm_w = bucket_weights[b] / total_w
        norm_c = abs(c) / total_c
        rows.append(
            {
                "bucket": b,
                "weight_share": norm_w,
                "overall_corr": c,
                "corr_share": norm_c,
                "share_gap": norm_c - norm_w,
            }
        )
    return pd.DataFrame(rows)


@dataclass(frozen=True)
class CoherenceReport:
    """The coherence verdict: alpha, the strongest sub-index-to-overall tie, and flags."""

    cronbach_alpha: float
    max_subindex_overall_corr: float
    dominant_bucket: str
    bucket_dominates: bool


def run_coherence(scores: pd.DataFrame, bucket_weights: Dict[str, float]) -> CoherenceReport:
    """
    Assemble the coherence verdict for the internal axis of the dossier.

    It combines Cronbach's alpha with the largest sub-index-to-overall correlation and
    flags a bucket as dominant when that correlation exceeds the redundancy threshold, so
    the dossier can state plainly whether the overall is a balanced blend or a proxy for
    one bucket.
    """
    corr = correlation_matrix(scores, method="spearman")
    overall_corrs = {
        c: abs(float(corr.loc[c, OVERALL_COLUMN]))
        for c in SUBINDEX_COLUMNS
        if c in corr.columns
    }
    dominant = max(overall_corrs, key=overall_corrs.get)
    max_corr = overall_corrs[dominant]
    return CoherenceReport(
        cronbach_alpha=cronbach_alpha(scores),
        max_subindex_overall_corr=max_corr,
        dominant_bucket=dominant.replace("idx_", ""),
        bucket_dominates=max_corr > MAX_SUBINDEX_OVERALL_CORR,
    )
