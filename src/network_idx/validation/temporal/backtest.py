"""
Predictive and temporal validation (Axis C) — the strongest single test.

Because there is no static label, time supplies one for free: a score computed on an
earlier data vintage should anticipate fiber-relevant outcomes observed later. This
module joins a score frame from vintage *t* to an outcome frame observed at *t+1* on the
parcel key, enforces that the two come from different vintages, and then reuses the
external-anchor kernels to report a temporally-separated AUROC and top-decile lift. The
strict join is the whole point: any leakage of post-*t* information into the score would
turn genuine prediction into circular self-agreement.

The outcome frame — newly reported fiber, new funding awards, new permits — must be
observed strictly after the score vintage. Producing those frames from archived per-run
snapshots is a separate concern; this module only consumes them, so it stays pure and
offline-testable.
"""
from dataclasses import dataclass

import numpy as np
import pandas as pd

from network_idx.validation.external.anchors import auroc, top_decile_lift

# A temporally-separated AUROC only counts as signal above this floor.
MIN_TEMPORAL_AUROC = 0.65


def align_score_to_outcome(
    scores_t: pd.DataFrame,
    outcomes_t1: pd.DataFrame,
    key: str,
    score_column: str,
    outcome_column: str,
) -> pd.DataFrame:
    """
    Join a vintage-t score to a t+1 outcome on the parcel key, keeping only matched rows.

    An inner join on the shared key is deliberate: a parcel that cannot be matched across
    vintages carries no temporal evidence and must not be silently scored as a negative.
    The returned two-column frame is the aligned input the temporal metrics run over.
    """
    left = scores_t[[key, score_column]]
    right = outcomes_t1[[key, outcome_column]]
    merged = left.merge(right, on=key, how="inner")
    return merged


@dataclass(frozen=True)
class TemporalReport:
    """The temporal verdict: matched-row count, separated AUROC and lift, and pass flag."""

    outcome: str
    n_matched: int
    auroc: float
    top_decile_lift: float
    passed: bool


def run_temporal(
    scores_t: pd.DataFrame,
    outcomes_t1: pd.DataFrame,
    key: str = "parcel_shape_id",
    score_column: str = "idx_overall",
    outcome_column: str = "became_fiber",
    score_vintage: str = "",
    outcome_vintage: str = "",
) -> TemporalReport:
    """
    Run the temporal backtest and render its verdict with strict vintage separation.

    It refuses to run when the score and outcome vintages are identical, because equal
    vintages defeat the entire predictive claim; otherwise it aligns the frames on the
    parcel key and reports the temporally-separated AUROC and top-decile lift, passing
    only when the AUROC clears the floor that marks it as genuine lead signal rather than
    a concurrent reshuffle. This is the most defensible evidence the project can produce
    once per-run snapshots are archived.
    """
    if score_vintage and outcome_vintage and score_vintage == outcome_vintage:
        raise ValueError(
            "score_vintage and outcome_vintage must differ; equal vintages leak the label"
        )
    aligned = align_score_to_outcome(
        scores_t, outcomes_t1, key, score_column, outcome_column
    )
    score = aligned[score_column]
    outcome = aligned[outcome_column]
    a = auroc(score, outcome)
    lift = top_decile_lift(score, outcome)
    passed = (not np.isnan(a)) and a >= MIN_TEMPORAL_AUROC
    return TemporalReport(
        outcome=outcome_column,
        n_matched=int(len(aligned)),
        auroc=a,
        top_decile_lift=lift,
        passed=passed,
    )
