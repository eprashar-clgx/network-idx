"""
Explain *why* a training run produced the weights and segments it did.
======================================================================
``train`` tells you what the model learned; this module tells you whether it had
anything to learn from. It exists because the v2 refit produced two results that
looked like bugs and were actually the data speaking: the growth bucket's weight
collapsed, and one of the eight KMeans segments contained eight tracts. Neither is
visible in the fitted artifacts — both are properties of the training frame *after*
the canonical fill/winsorize contract has been applied, which is exactly the frame
nobody was looking at.

Three questions, three functions:

* :func:`feature_diagnostics` — what does the raw frame look like? Sparsity (a count
  feature that is 95% zero cannot carry much signal) and the quantiles the fill
  contract will cap against.
* :func:`fill_impact` — what did the fill contract *do*? The decisive column is
  ``post_nunique``: winsorizing a sparse count at its 99.5th percentile can collapse
  a 0-219 range into five distinct values, leaving a feature that no tree can split
  on usefully. ``std_ratio`` quantifies the variance destroyed.
* :func:`cluster_profile` / :func:`small_cluster_report` — are the segments market
  structure or data artefacts? A cluster whose median sits 100 standard deviations
  from the population mean is not a market; it is a handful of corrupt rows that
  captured a centroid.

Everything here is a pure function of a frame (plus labels), so the notebooks are
thin drivers and the findings are reproducible rather than re-derived by hand each
time someone is surprised by a weight.
"""

import numpy as np
import pandas as pd

from network_idx.constants import ALL_SCORING_FEATURES, MODEL_TO_SCORING_FEATURE
from network_idx.constants.scoring_contract import SCORING_BUCKETS
from network_idx.scoring.scaling import apply_feature_fills

# Feature → bucket, inverted from the contract's bucket → features mapping so the
# diagnostics tables can label each row without the caller doing the lookup.
FEATURE_BUCKET = {f: b for b, fs in SCORING_BUCKETS.items() for f in fs}


def prepare_raw(training_frame: pd.DataFrame) -> pd.DataFrame:
    """Rename model columns to canonical scoring names and select the thirteen
    features, *without* applying fills. This is the frame ``train`` starts from, and
    the baseline every fill-impact comparison is made against."""
    renamed = training_frame.rename(columns=MODEL_TO_SCORING_FEATURE)
    missing = [f for f in ALL_SCORING_FEATURES if f not in renamed.columns]
    if missing:
        raise KeyError(f"Training frame is missing scoring features: {missing}")
    return renamed[ALL_SCORING_FEATURES].astype("float64")


def feature_diagnostics(raw: pd.DataFrame, features=ALL_SCORING_FEATURES) -> pd.DataFrame:
    """Sparsity and tail shape of each raw feature, one row per feature.

    ``zero_pct`` is the number to read first: the fill contract winsorizes against a
    high quantile, so on a feature that is mostly zero that quantile sits just above
    zero and the cap lands in the body of the distribution rather than its tail.
    ``p995`` next to ``max`` shows how much range that cap is about to discard.
    """
    rows = []
    for f in features:
        col = raw[f].replace([np.inf, -np.inf], np.nan)
        rows.append({
            "feature": f,
            "bucket": FEATURE_BUCKET[f],
            "null_pct": col.isna().mean() * 100,
            "zero_pct": (col == 0).mean() * 100,
            "p50": col.median(),
            "p99": col.quantile(0.99),
            "p995": col.quantile(0.995),
            "max": col.max(),
        })
    return pd.DataFrame(rows)


def fill_impact(
    raw: pd.DataFrame,
    filled: pd.DataFrame = None,
    features=ALL_SCORING_FEATURES,
) -> pd.DataFrame:
    """Compare each feature before and after the canonical fill/winsorize contract.

    ``filled`` defaults to ``apply_feature_fills(raw)`` so callers normally pass only
    the raw frame. Returned columns, in order of how much they matter:

    * ``post_nunique`` — distinct values surviving the cap. Single digits means the
      feature has effectively become a coarse ordinal, whatever its raw range was.
    * ``std_ratio`` — post-fill standard deviation as a fraction of pre-fill. Values
      near 1.0 mean the contract left the feature alone; values near 0.25 mean three
      quarters of its spread was clipped away.
    * ``pct_clipped`` — share of rows whose value was actually reduced. Deliberately
      small for a 99.5th-percentile cap; a *small* ``pct_clipped`` alongside a *large*
      drop in ``std_ratio`` is the signature of a fat tail carrying the variance.
    """
    if filled is None:
        filled = apply_feature_fills(raw, features)
    rows = []
    for f in features:
        pre = raw[f].replace([np.inf, -np.inf], np.nan)
        post = filled[f]
        pre_std = pre.std()
        rows.append({
            "feature": f,
            "bucket": FEATURE_BUCKET[f],
            "pre_std": pre_std,
            "post_std": post.std(),
            "std_ratio": post.std() / pre_std if pre_std else np.nan,
            "pct_clipped": (pre > post).mean() * 100,
            "post_zero_pct": (post == 0).mean() * 100,
            "post_nunique": int(post.nunique()),
        })
    return pd.DataFrame(rows)


def cluster_profile(filled: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    """Median value of every feature within each segment, plus the segment's row count.

    Read down a column to see which feature separates the segments, and across a row
    to read a segment's character. Medians (not means) so a handful of extreme rows
    cannot misrepresent an otherwise coherent segment.
    """
    prof = filled.groupby(labels).median()
    prof.insert(0, "n", pd.Series(labels).value_counts().sort_index())
    prof.index.name = "cluster"
    return prof


def small_cluster_report(
    filled: pd.DataFrame,
    labels: np.ndarray,
    max_size: int = 1000,
) -> dict:
    """Explain each segment smaller than ``max_size`` by how far it sits from the
    population, keyed by cluster id.

    Each value is a frame sorted by ``z_of_median`` — the segment's median expressed
    in population standard deviations — so the feature responsible for the segment's
    existence appears first. A ``z_of_median`` in the tens or hundreds means the
    segment is a data-quality artefact rather than a market: real market segments
    differ from the population by small multiples of a standard deviation, not by
    two orders of magnitude.
    """
    pop_mean, pop_std = filled.mean(), filled.std()
    out = {}
    for c in sorted(set(labels)):
        mask = labels == c
        n = int(mask.sum())
        if n >= max_size:
            continue
        sub = filled[mask]
        rep = pd.DataFrame({
            "cluster_median": sub.median(),
            "cluster_max": sub.max(),
            "pop_median": filled.median(),
            "pop_p999": filled.quantile(0.999),
            "z_of_median": (sub.median() - pop_mean) / pop_std,
        })
        rep.index.name = "feature"
        out[c] = rep.sort_values("z_of_median", key=abs, ascending=False)
        out[c].attrs["n"] = n
    return out
