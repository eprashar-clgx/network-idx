"""Tests for internal coherence, spatial coherence, and distribution checks (Axis A2-A4)."""
import numpy as np
import pandas as pd

from network_idx.validation.internal.coherence import (
    correlation_matrix,
    cronbach_alpha,
    run_coherence,
    weight_correlation_consistency,
)
from network_idx.validation.internal.spatial import (
    morans_i,
    local_morans_i,
    row_standardized_weights,
    run_spatial,
)
from network_idx.validation.internal.distribution import (
    assign_density_tier,
    check_monotone_opportunity,
    score_distribution_by_context,
)

WEIGHTS = {"growth": 0.169, "telecom": 0.591, "demo": 0.240}


def _scores(n=300, seed=2):
    rng = np.random.default_rng(seed)
    g = rng.uniform(0, 100, n)
    t = rng.uniform(0, 100, n)
    d = rng.uniform(0, 100, n)
    overall = 0.169 * g + 0.591 * t + 0.240 * d
    overall = 100 * (overall - overall.min()) / (overall.max() - overall.min())
    return pd.DataFrame(
        {"idx_growth": g, "idx_telecom": t, "idx_demo": d, "idx_overall": overall}
    )


# ── coherence ────────────────────────────────────────────────────────────────
def test_correlation_matrix_shape():
    m = correlation_matrix(_scores())
    assert m.shape == (4, 4)
    assert abs(m.loc["idx_growth", "idx_growth"] - 1.0) < 1e-9


def test_cronbach_alpha_high_for_correlated_items():
    rng = np.random.default_rng(0)
    base = rng.uniform(0, 100, 400)
    df = pd.DataFrame(
        {
            "idx_growth": base + rng.normal(0, 5, 400),
            "idx_telecom": base + rng.normal(0, 5, 400),
            "idx_demo": base + rng.normal(0, 5, 400),
        }
    )
    assert cronbach_alpha(df) > 0.8


def test_run_coherence_flags_dominance():
    # telecom is nearly identical to overall -> dominance
    rng = np.random.default_rng(3)
    overall = rng.uniform(0, 100, 300)
    df = pd.DataFrame(
        {
            "idx_growth": rng.uniform(0, 100, 300),
            "idx_telecom": overall + rng.normal(0, 0.5, 300),
            "idx_demo": rng.uniform(0, 100, 300),
            "idx_overall": overall,
        }
    )
    rep = run_coherence(df, WEIGHTS)
    assert rep.dominant_bucket == "telecom"
    assert rep.bucket_dominates is True


def test_weight_consistency_columns():
    out = weight_correlation_consistency(_scores(), WEIGHTS)
    assert set(out["bucket"]) == {"growth", "telecom", "demo"}
    assert {"weight_share", "corr_share", "share_gap"}.issubset(out.columns)


# ── spatial ──────────────────────────────────────────────────────────────────
def test_morans_i_positive_for_smooth_gradient():
    # A 1-D chain with a monotone gradient is strongly positively autocorrelated.
    ids = list(range(10))
    values = [float(i) for i in range(10)]
    neighbors = {i: [i - 1, i + 1] for i in ids}
    w = row_standardized_weights(ids, neighbors)
    assert morans_i(values, w) > 0.5


def test_morans_i_negative_for_checkerboard():
    ids = list(range(10))
    values = [float(i % 2) for i in range(10)]
    neighbors = {i: [i - 1, i + 1] for i in ids}
    w = row_standardized_weights(ids, neighbors)
    assert morans_i(values, w) < 0


def test_run_spatial_flags_outliers():
    ids = list(range(6))
    # one spike (index 3) amid a low field -> spatial outlier
    values = [1.0, 1.0, 1.0, 99.0, 1.0, 1.0]
    neighbors = {i: [i - 1, i + 1] for i in ids}
    frame = pd.DataFrame({"id": ids, "val": values})
    rep = run_spatial(frame, "id", "val", neighbors)
    assert 3 in rep.spatial_outlier_ids


def test_row_standardized_rows_sum_to_one():
    ids = [0, 1, 2]
    w = row_standardized_weights(ids, {0: [1, 2], 1: [0], 2: [0, 1]})
    assert np.allclose(w.sum(axis=1), 1.0)


# ── distribution ─────────────────────────────────────────────────────────────
def test_assign_density_tier():
    s = pd.Series([1, 50, 500, 5000])
    tiers = assign_density_tier(s, edges=[10, 100, 1000])
    # densest tier label is 'urban' for smallest bin per DENSITY_TIERS order
    assert list(tiers) == ["urban", "suburban", "exurban", "rural"]


def test_score_distribution_and_monotone_check():
    frame = pd.DataFrame(
        {
            "ctx": ["urban"] * 50 + ["suburban"] * 50 + ["exurban"] * 50 + ["rural"] * 50,
            "idx_overall": ([10] * 50) + ([30] * 50) + ([60] * 50) + ([90] * 50),
        }
    )
    summ = score_distribution_by_context(frame, "ctx")
    assert set(summ["ctx"]) == {"urban", "suburban", "exurban", "rural"}
    assert check_monotone_opportunity(summ, "ctx") is True
