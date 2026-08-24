"""Predictive and temporal validation: testing whether an index built on an earlier data vintage anticipates future fiber build-out and funding awards. This requires archived per-run snapshots."""

from network_idx.validation.temporal.backtest import (
    TemporalReport,
    align_score_to_outcome,
    run_temporal,
)

__all__ = ["TemporalReport", "align_score_to_outcome", "run_temporal"]
