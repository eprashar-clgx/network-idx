"""Tests for external anchors (Axis B)."""
import numpy as np
import pandas as pd

from network_idx.validation.external.anchors import (
    auroc,
    average_precision,
    evaluate_binary_anchor,
    rank_correlation,
    top_decile_lift,
)


def _score_and_anchor(n=1000, seed=5):
    rng = np.random.default_rng(seed)
    score = pd.Series(rng.uniform(0, 100, n))
    # Rare positive class (low base rate) whose likelihood rises steeply with the score,
    # so a top-decile lift above 2x is attainable and the index separates it well.
    prob = (score / 100.0) ** 3
    anchor = pd.Series((rng.uniform(0, 1, n) < prob).astype(int))
    return score, anchor


def test_auroc_above_half_when_score_separates():
    score, anchor = _score_and_anchor()
    assert auroc(score, anchor) > 0.7


def test_auroc_nan_for_degenerate_anchor():
    score = pd.Series([1.0, 2.0, 3.0])
    assert np.isnan(auroc(score, pd.Series([0, 0, 0])))


def test_top_decile_lift_above_one():
    score, anchor = _score_and_anchor()
    assert top_decile_lift(score, anchor) > 1.5


def test_average_precision_reasonable():
    score, anchor = _score_and_anchor()
    ap = average_precision(score, anchor)
    assert 0 < ap <= 1


def test_rank_correlation_positive():
    rng = np.random.default_rng(6)
    score = pd.Series(rng.uniform(0, 100, 500))
    cont = score * 2 + rng.normal(0, 5, 500)
    assert rank_correlation(score, pd.Series(cont)) > 0.9


def test_evaluate_binary_anchor_passes_when_direction_and_thresholds_met():
    score, anchor = _score_and_anchor(n=2000)
    rep = evaluate_binary_anchor(score, anchor, "bead", expected_positive=True)
    assert rep.direction_ok is True
    assert rep.passed is True


def test_evaluate_binary_anchor_fails_on_wrong_direction():
    # Index scores high where anchor is NOT present -> expecting positive is violated.
    score, anchor = _score_and_anchor(n=2000)
    inverted = pd.Series(100 - score.to_numpy())
    rep = evaluate_binary_anchor(inverted, anchor, "bead", expected_positive=True)
    assert rep.direction_ok is False
    assert rep.passed is False
