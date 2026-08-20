"""
Offline tests for the telecom feature distribution monitor.

These tests exercise the distribution snapshot query and the pure flagging logic without any
BigQuery access. They assert that the query summarises every block telecom feature over the
feature table with the configured quantiles and a leftover-free render, and that the flag
check catches a feature that has collapsed to a constant, one that sits entirely at zero, and
one that is unexpectedly null-heavy, while passing a healthy spread. A fake client returns a
canned snapshot frame so the run dispatch can be tested offline.
"""
import pandas as pd

from network_idx.monitoring import metrics as m


class _FakeQueryJob:
    def __init__(self, df):
        self._df = df

    def to_dataframe(self):
        return self._df


class FakeClient:
    def __init__(self, df):
        self.df = df
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return _FakeQueryJob(self.df)


def _row(feature, **over):
    base = dict(
        feature=feature,
        n=100,
        n_null=0,
        null_rate=0.0,
        n_zero=10,
        zero_rate=0.1,
        mean=0.5,
        stddev=0.2,
        min=0.0,
        max=1.0,
    )
    base.update(over)
    return base


def test_render_covers_all_block_features_over_the_table():
    sql = m.render_distribution_sql("proj.ds.tf")
    for feature in m.TELECOM_BLOCK_FEATURES:
        assert f"'{feature}' AS feature" in sql
    assert sql.count("FROM `proj.ds.tf`") == len(m.TELECOM_BLOCK_FEATURES)
    assert sql.count("UNION ALL") == len(m.TELECOM_BLOCK_FEATURES) - 1


def test_render_includes_configured_quantiles():
    sql = m.render_distribution_sql("proj.ds.tf")
    for pct, name in m.DISTRIBUTION_QUANTILES:
        assert f"[OFFSET({pct})] AS {name}" in sql


def test_render_leaves_no_unresolved_placeholders():
    sql = m.render_distribution_sql("proj.ds.tf")
    assert "{" not in sql and "}" not in sql


def test_healthy_distribution_passes():
    df = pd.DataFrame([_row("cable_penetration"), _row("fiber_opportunity_gap")])
    report = m.flag_distributions(df)
    assert report.passed
    assert report.n_features == 2
    assert report.flags == {}


def test_constant_feature_is_flagged():
    df = pd.DataFrame([_row("fiber_speed_top_tier", min=0.0, max=0.0, stddev=0.0)])
    report = m.flag_distributions(df)
    assert not report.passed
    assert "constant" in report.flags["fiber_speed_top_tier"]


def test_all_zero_feature_is_flagged():
    df = pd.DataFrame([_row("cable_penetration", zero_rate=1.0, mean=0.0, min=0.0, max=0.0, stddev=0.0)])
    report = m.flag_distributions(df)
    assert not report.passed
    assert "all_zero" in report.flags["cable_penetration"]


def test_high_null_feature_is_flagged():
    df = pd.DataFrame([_row("provider_competitive_landscape_ord", null_rate=0.2)])
    report = m.flag_distributions(df, max_null_rate=0.05)
    assert not report.passed
    assert "high_null" in report.flags["provider_competitive_landscape_ord"]


def test_run_reads_snapshot_and_returns_report():
    df = pd.DataFrame([_row("fiber_opportunity_gap")])
    client = FakeClient(df)
    frame, report = m.run(client=client)
    assert len(client.queries) == 1
    assert report.passed
    assert list(frame["feature"]) == ["fiber_opportunity_gap"]
