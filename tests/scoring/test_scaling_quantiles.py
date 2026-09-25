"""Tests for the scaling contract's winsorize-quantile resolution.

These guard a bug that was silent rather than loud: ``APPROX_QUANTILES`` addressed
with a fixed 1000 buckets cannot express a quantile finer than a thousandth, and
instead of failing it returns the maximum — producing a cap that clips nothing.
Since the contract now mixes precisions (growth counts at P99.99, population
features at P99.9), the offset must be derived per feature.
"""

import pytest

from network_idx.constants import ALL_SCORING_FEATURES
from network_idx.constants.scoring_contract import SCALING_WINSORIZE_QUANTILE
from network_idx.scoring.scaling import build_stats_query, quantile_offset


class TestQuantileOffset:
    @pytest.mark.parametrize(
        "q,expected",
        [
            (0.995, (1000, 995)),
            (0.999, (1000, 999)),
            (0.9999, (10000, 9999)),
            (0.99999, (100000, 99999)),
        ],
    )
    def test_resolves_to_exact_bucket_and_offset(self, q, expected):
        assert quantile_offset(q) == expected

    def test_offset_is_always_below_bucket_count(self):
        """The failure mode being guarded: OFFSET == n is the maximum, not a cap."""
        for q in SCALING_WINSORIZE_QUANTILE.values():
            buckets, offset = quantile_offset(q)
            assert offset < buckets

    def test_rejects_a_no_op_cap(self):
        with pytest.raises(ValueError, match="no-op cap"):
            quantile_offset(1.0)

    def test_rejects_quantile_finer_than_bigquery_supports(self):
        with pytest.raises(ValueError, match="1,000,000-bucket limit"):
            quantile_offset(0.99999999)

    def test_bucket_count_is_minimal_for_the_precision(self):
        """A coarser quantile must not pay for a finer split than it needs."""
        assert quantile_offset(0.999)[0] == 1000


class TestWinsorizeContract:
    def test_population_features_are_bounded(self):
        """Left unbounded, corrupt tracts captured a KMeans centroid and dominated
        the SHAP attribution that sets the bucket weights."""
        assert "pop_ch_avg" in SCALING_WINSORIZE_QUANTILE
        assert "pop_pctch_avg" in SCALING_WINSORIZE_QUANTILE

    def test_population_cap_is_tighter_than_the_growth_cap(self):
        """P99.99 on the population features sits above the corruption and re-admits
        the degenerate cluster, so they are capped harder than the growth counts."""
        pop = max(SCALING_WINSORIZE_QUANTILE[f] for f in ("pop_ch_avg", "pop_pctch_avg"))
        growth = min(
            SCALING_WINSORIZE_QUANTILE[f]
            for f in SCALING_WINSORIZE_QUANTILE
            if f not in ("pop_ch_avg", "pop_pctch_avg")
        )
        assert pop < growth

    def test_every_winsorized_feature_is_a_scoring_feature(self):
        assert set(SCALING_WINSORIZE_QUANTILE) <= set(ALL_SCORING_FEATURES)


class TestStatsQuery:
    def test_emits_per_feature_bucket_counts(self):
        sql = build_stats_query("proj.ds.tbl")
        assert "APPROX_QUANTILES(COALESCE(`pop_ch_avg`, 0.0), 1000)[OFFSET(999)]" in sql
        assert (
            "APPROX_QUANTILES(COALESCE(`bldr_dev_qtr_mi_cnt`, 0.0), 10000)[OFFSET(9999)]"
            in sql
        )

    def test_every_winsorized_feature_gets_a_winmax(self):
        sql = build_stats_query("proj.ds.tbl")
        for f in SCALING_WINSORIZE_QUANTILE:
            assert f"`{f}__winmax`" in sql

    def test_winsorized_features_are_not_also_treated_as_constants(self):
        """A feature appearing in both branches would emit a duplicate alias."""
        sql = build_stats_query("proj.ds.tbl")
        for f in SCALING_WINSORIZE_QUANTILE:
            assert f"`{f}__max`" not in sql
