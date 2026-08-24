"""Tests for the internal-axis sensitivity analysis (Axis A1)."""
import numpy as np
import pandas as pd

from network_idx.validation.internal.sensitivity import (
    Scenario,
    default_scenarios,
    rank_biased_overlap,
    rank_shift_stats,
    recombine_overall,
    run_sensitivity,
)


def _scores(n=200, seed=1):
    rng = np.random.default_rng(seed)
    return pd.DataFrame(
        {
            "idx_growth": rng.uniform(0, 100, n),
            "idx_telecom": rng.uniform(0, 100, n),
            "idx_demo": rng.uniform(0, 100, n),
        }
    )


def test_recombine_linear_rescales_to_0_100():
    df = _scores()
    out = recombine_overall(df, {"growth": 0.2, "telecom": 0.6, "demo": 0.2}, "linear")
    assert abs(out.min()) < 1e-9
    assert abs(out.max() - 100.0) < 1e-9


def test_recombine_weight_normalization_invariant():
    df = _scores()
    a = recombine_overall(df, {"growth": 1, "telecom": 3, "demo": 1}, "linear")
    b = recombine_overall(df, {"growth": 2, "telecom": 6, "demo": 2}, "linear")
    pd.testing.assert_series_equal(a, b)


def test_recombine_geometric_penalizes_imbalance():
    # An imbalanced parcel scores relatively lower under geometric than linear.
    df = pd.DataFrame(
        {"idx_growth": [50, 10], "idx_telecom": [50, 90], "idx_demo": [50, 50]}
    )
    w = {"growth": 1, "telecom": 1, "demo": 1}
    lin = recombine_overall(df, w, "linear")
    geo = recombine_overall(df, w, "geometric")
    # Balanced row (index 0) is the max in both; imbalanced row lower under geometric.
    assert geo.iloc[1] <= lin.iloc[1] + 1e-6


def test_rank_shift_identity_is_perfect():
    df = _scores()
    base = recombine_overall(df, {"growth": 0.2, "telecom": 0.6, "demo": 0.2}, "linear")
    stats = rank_shift_stats(base, base)
    assert stats["median_abs_rank_shift"] == 0.0
    assert abs(stats["spearman_rho"] - 1.0) < 1e-9
    assert stats["band_agreement"] == 1.0
    assert abs(stats["rank_biased_overlap"] - 1.0) < 1e-6


def test_rbo_disjoint_is_low():
    assert rank_biased_overlap([1, 2, 3], [4, 5, 6]) < 0.1


def test_run_sensitivity_returns_row_per_scenario():
    df = _scores()
    scenarios = default_scenarios()
    out = run_sensitivity(df, scenarios)
    assert len(out) == len(scenarios)
    assert {"scenario", "spearman_rho", "passed"}.issubset(out.columns)


def test_small_weight_perturbation_passes():
    df = _scores(n=500)
    out = run_sensitivity(
        df,
        [Scenario("tiny", {"growth": 0.17, "telecom": 0.60, "demo": 0.23}, "linear")],
        baseline_weights={"growth": 0.169, "telecom": 0.591, "demo": 0.240},
    )
    assert bool(out.iloc[0]["passed"]) is True
