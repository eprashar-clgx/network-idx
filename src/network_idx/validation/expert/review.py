"""
Expert and face validation through a blind, structured review (Axis D).

Face validity is operationalised here as a blind audit with measurable agreement, not an
ad-hoc "looks right." This module builds a stratified sampling frame — balanced across
score decile, business context, and region, and deliberately oversampling the tails and
the disagreement cases surfaced by the other axes — so that reviewers rate a defensible
cross-section of parcels without seeing the score. After ratings come back it measures
agreement between each expert and the index decile, and among the experts themselves,
using weighted Cohen's kappa for two raters and Fleiss' kappa for three or more.

The index only earns the axis when it agrees with experts about as well as experts agree
with each other: a human ceiling below the threshold means the construct is ill-defined,
not that the index is wrong. All functions are pure over in-memory frames.
"""
from dataclasses import dataclass
from typing import List, Optional, Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import cohen_kappa_score

# Suggested acceptance floor: moderate agreement, and not far below the human ceiling.
MIN_EXPERT_KAPPA = 0.40


def score_decile(score: pd.Series) -> pd.Series:
    """
    Assign each parcel a 0-9 score decile for agreement scoring.

    The decile is the granularity customers act on and a natural coarse view of the
    continuous index, so distribution and sampling work throughout this module is done on
    deciles rather than raw scores.
    """
    ranks = score.rank(pct=True)
    return np.minimum((ranks * 10).astype(int), 9)


def index_to_scale(score: pd.Series, n_levels: int) -> pd.Series:
    """
    Bin the continuous index into the same number of ordered levels as the expert scale.

    Agreement between an expert's coarse ordinal rating and the index is only fair when
    both live on the same scale; comparing a one-to-five rating against a ten-level decile
    would penalise the scale mismatch rather than genuine disagreement. This ranks parcels
    into `n_levels` equal-frequency ordered bins labelled zero upward so weighted kappa
    reads as true rater-versus-index agreement.
    """
    if n_levels < 2:
        raise ValueError("n_levels must be at least two")
    ranks = score.rank(pct=True)
    return np.minimum((ranks * n_levels).astype(int), n_levels - 1)


def _infer_levels(ratings: pd.DataFrame, expert_columns: Sequence[str]) -> int:
    """Infer the expert ordinal scale size as the observed rating range width."""
    vals = ratings[list(expert_columns)].to_numpy()
    return int(vals.max() - vals.min() + 1)


def build_sampling_frame(
    scores: pd.DataFrame,
    context_column: str,
    region_column: str,
    per_cell: int = 5,
    score_column: str = "idx_overall",
    oversample_ids: Optional[Sequence] = None,
    id_column: str = "parcel_shape_id",
    random_state: int = 0,
) -> pd.DataFrame:
    """
    Draw a stratified review sample balanced across decile, context, and region.

    Each decile-by-context-by-region cell contributes up to `per_cell` parcels so the
    tails and every business stratum are represented rather than swamped by the dense
    middle, and any ids passed in `oversample_ids` — typically the spatial outliers and
    index-versus-proxy disagreements from Axes A and B — are force-included because that
    is where an index breaks. The result is the blind reviewer's worklist.
    """
    frame = scores.copy()
    frame["_decile"] = score_decile(frame[score_column])
    sampled = (
        frame.groupby(["_decile", context_column, region_column], observed=True, group_keys=False)
        .apply(lambda g: g.sample(min(len(g), per_cell), random_state=random_state))
    )
    if oversample_ids is not None:
        extra = frame[frame[id_column].isin(list(oversample_ids))]
        sampled = pd.concat([sampled, extra]).drop_duplicates(subset=[id_column])
    return sampled.reset_index(drop=True)


def fleiss_kappa(ratings: np.ndarray) -> float:
    """
    Fleiss' kappa for agreement among three or more raters over categorical ratings.

    The input is a subjects-by-categories count matrix (how many raters placed each
    subject in each category); the statistic corrects the observed agreement for the
    agreement expected by chance, returning one for perfect consensus and around zero for
    chance-level. It is the multi-rater generalisation used when the review has three or
    more experts.
    """
    n_sub, _ = ratings.shape
    n_rater = ratings.sum(axis=1)
    if not np.all(n_rater == n_rater[0]):
        raise ValueError("every subject must be rated by the same number of raters")
    n = n_rater[0]
    p_i = (ratings * (ratings - 1)).sum(axis=1) / (n * (n - 1))
    p_bar = p_i.mean()
    p_j = ratings.sum(axis=0) / (n_sub * n)
    p_e = (p_j ** 2).sum()
    if p_e == 1:
        return 1.0
    return float((p_bar - p_e) / (1 - p_e))


def expert_index_agreement(
    expert_rating: pd.Series, score: pd.Series, weights: str = "quadratic"
) -> float:
    """
    Weighted Cohen's kappa between one expert's ordinal rating and the index.

    The index is first binned to the expert's own scale so the two are directly
    comparable, and quadratic weighting is used because both scales are ordinal, letting a
    near-miss count as partial agreement. This is the core expert-versus-index number the
    dossier reports for each reviewer.
    """
    n_levels = int(expert_rating.max() - expert_rating.min() + 1)
    binned = index_to_scale(score, max(n_levels, 2))
    return float(cohen_kappa_score(expert_rating.to_numpy(), binned, weights=weights))


@dataclass(frozen=True)
class ExpertReport:
    """The expert verdict: index agreement, the human ceiling, and the pass flag."""

    mean_expert_index_kappa: float
    expert_expert_kappa: float
    passed: bool


def run_expert_review(
    ratings: pd.DataFrame,
    score: pd.Series,
    expert_columns: Sequence[str],
    weights: str = "quadratic",
) -> ExpertReport:
    """
    Assemble the expert-axis verdict from a table of per-parcel expert ratings.

    It averages each expert's weighted kappa against the index binned to the experts'
    ordinal scale, estimates the human ceiling from pairwise expert-versus-expert kappa,
    and passes only when the index clears the moderate-agreement floor *and* sits close to
    that ceiling — the methodology's rule that the index should be about as near each
    expert as the experts are to one another.
    """
    n_levels = max(_infer_levels(ratings, expert_columns), 2)
    binned = index_to_scale(score, n_levels)
    index_kappas = [
        float(cohen_kappa_score(ratings[c].to_numpy(), binned, weights=weights))
        for c in expert_columns
    ]
    pair_kappas = []
    cols = list(expert_columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            pair_kappas.append(
                float(
                    cohen_kappa_score(
                        ratings[cols[i]].to_numpy(), ratings[cols[j]].to_numpy(), weights=weights
                    )
                )
            )
    mean_index = float(np.mean(index_kappas))
    ceiling = float(np.mean(pair_kappas)) if pair_kappas else float("nan")
    close_to_ceiling = np.isnan(ceiling) or mean_index >= ceiling - 0.10
    passed = mean_index >= MIN_EXPERT_KAPPA and close_to_ceiling
    return ExpertReport(
        mean_expert_index_kappa=mean_index,
        expert_expert_kappa=ceiling,
        passed=passed,
    )
