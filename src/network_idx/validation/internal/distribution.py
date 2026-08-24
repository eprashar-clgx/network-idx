"""
Face-plausibility of the index distribution by business context (Axis A4).

The index should tell a story a domain expert recognises: established fiber metros ought
to skew low on *opportunity*, while fast-growing exurban and rural fringes ought to skew
high. This module summarises the distribution of the score (and, optionally, of the raw
input features) within business-meaningful strata — urban, suburban, exurban, rural — so
that a reviewer can confirm the expected ordering and catch a degenerate or U-shaped
distribution early. A stratum whose median opportunity contradicts the prior is a red
flag worth investigating before anything ships.

The functions are pure summaries over an in-memory frame. Callers either pass a frame
that already carries a context column or use the density-tier helper to derive one from a
density measure; no BigQuery access happens here.
"""
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd

# Coarse density tiers, from densest to least dense, used as the default business cut.
DENSITY_TIERS = ["urban", "suburban", "exurban", "rural"]


def assign_density_tier(
    density: pd.Series,
    edges: Sequence[float],
    labels: Sequence[str] = DENSITY_TIERS,
) -> pd.Series:
    """
    Bin a density measure into ordered business tiers.

    The caller supplies the cut points (for example housing-unit density quantiles) and
    the labels from densest to least dense, so the same helper serves whatever density
    signal the pipeline has on hand. The result is a categorical column suitable for
    grouping the distribution summaries below.
    """
    if len(edges) != len(labels) - 1:
        raise ValueError("edges must have exactly one fewer entry than labels")
    bins = [-np.inf, *edges, np.inf]
    return pd.cut(density, bins=bins, labels=list(labels), include_lowest=True)


def score_distribution_by_context(
    frame: pd.DataFrame,
    context_column: str,
    score_column: str = "idx_overall",
) -> pd.DataFrame:
    """
    Summarise the score distribution within each business-context stratum.

    For every stratum it reports the count and a five-number-style summary of the score
    (mean, median, and the 10th/25th/75th/90th percentiles), which is enough to judge
    both central tendency and skew. The ordering of stratum medians is what the reviewer
    checks against the prior that opportunity should fall as density rises.
    """
    grp = frame.groupby(context_column, observed=True)[score_column]
    out = pd.DataFrame(
        {
            "count": grp.size(),
            "mean": grp.mean(),
            "p10": grp.quantile(0.10),
            "p25": grp.quantile(0.25),
            "median": grp.median(),
            "p75": grp.quantile(0.75),
            "p90": grp.quantile(0.90),
        }
    )
    return out.reset_index()


def feature_distribution_by_context(
    frame: pd.DataFrame,
    context_column: str,
    feature_columns: List[str],
) -> pd.DataFrame:
    """
    Summarise input-feature distributions within each business-context stratum.

    This is the input-side companion to the score summary: it reports the mean, median,
    and null rate of each feature per stratum so that a surprising score distribution can
    be traced back to the inputs driving it. The frame is tidy (one row per stratum and
    feature) for easy inspection and dossier inclusion.
    """
    rows = []
    for ctx, sub in frame.groupby(context_column, observed=True):
        for f in feature_columns:
            col = sub[f]
            rows.append(
                {
                    "context": ctx,
                    "feature": f,
                    "count": int(col.size),
                    "mean": float(col.mean()),
                    "median": float(col.median()),
                    "null_rate": float(col.isna().mean()),
                }
            )
    return pd.DataFrame(rows)


def check_monotone_opportunity(
    summary: pd.DataFrame,
    context_column: str,
    tier_order: Sequence[str] = DENSITY_TIERS,
    stat: str = "median",
) -> bool:
    """
    Test whether opportunity rises monotonically from densest to least dense tier.

    Given a per-stratum score summary, it orders the strata from urban to rural and
    checks that the chosen statistic is non-decreasing, encoding the prior that fiber
    opportunity should be lowest in dense, already-served metros and highest on the
    sparse growing fringe. A False result is exactly the red flag the methodology asks
    the reviewer to surface.
    """
    ordered = summary.set_index(context_column).reindex(list(tier_order)).dropna(subset=[stat])
    vals = ordered[stat].to_numpy(dtype=float)
    return bool(np.all(np.diff(vals) >= 0))
