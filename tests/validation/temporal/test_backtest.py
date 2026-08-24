"""Tests for the temporal backtest (Axis C)."""
import numpy as np
import pandas as pd
import pytest

from network_idx.validation.temporal.backtest import (
    align_score_to_outcome,
    run_temporal,
)


def _frames(n=1000, seed=7):
    rng = np.random.default_rng(seed)
    ids = np.arange(n)
    score = rng.uniform(0, 100, n)
    prob = score / 100.0
    became = (rng.uniform(0, 1, n) < prob).astype(int)
    scores_t = pd.DataFrame({"parcel_shape_id": ids, "idx_overall": score})
    outcomes_t1 = pd.DataFrame({"parcel_shape_id": ids, "became_fiber": became})
    return scores_t, outcomes_t1


def test_align_inner_join_drops_unmatched():
    scores_t = pd.DataFrame({"parcel_shape_id": [1, 2, 3], "idx_overall": [10, 20, 30]})
    outcomes = pd.DataFrame({"parcel_shape_id": [2, 3, 4], "became_fiber": [1, 0, 1]})
    out = align_score_to_outcome(scores_t, outcomes, "parcel_shape_id", "idx_overall", "became_fiber")
    assert list(out["parcel_shape_id"]) == [2, 3]


def test_run_temporal_passes_when_score_leads_outcome():
    scores_t, outcomes_t1 = _frames(n=3000)
    rep = run_temporal(
        scores_t, outcomes_t1, score_vintage="2023H1", outcome_vintage="2024H1"
    )
    assert rep.n_matched == 3000
    assert rep.auroc > 0.65
    assert rep.passed is True


def test_run_temporal_rejects_equal_vintages():
    scores_t, outcomes_t1 = _frames()
    with pytest.raises(ValueError):
        run_temporal(scores_t, outcomes_t1, score_vintage="2024H1", outcome_vintage="2024H1")


def test_run_temporal_fails_on_noise_outcome():
    rng = np.random.default_rng(9)
    n = 1000
    ids = np.arange(n)
    scores_t = pd.DataFrame({"parcel_shape_id": ids, "idx_overall": rng.uniform(0, 100, n)})
    outcomes_t1 = pd.DataFrame(
        {"parcel_shape_id": ids, "became_fiber": rng.integers(0, 2, n)}
    )
    rep = run_temporal(scores_t, outcomes_t1, score_vintage="t", outcome_vintage="t1")
    assert rep.passed is False
