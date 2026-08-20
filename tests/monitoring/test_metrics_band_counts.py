"""
Offline tests for the telecom feature band-count monitor.

These tests exercise the per-state band-count query and the pure rollup without any BigQuery
access. They assert that the band labels split the edge range into half-open intervals with a
closed final band, that the query maps each tracked feature to its bands per state and keeps
nulls and out-of-range values in their own bands, that the render leaves no placeholders, and
that the national rollup sums block counts across states per feature and band. A fake client
returns a canned band frame so the run dispatch can be tested offline.
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


def test_band_labels_are_half_open_with_closed_final_band():
    labels = m.band_labels([0.0, 0.2, 0.4, 0.6, 0.8, 1.0])
    assert labels == ["[0.00,0.20)", "[0.20,0.40)", "[0.40,0.60)", "[0.60,0.80)", "[0.80,1.00]"]


def test_render_covers_tracked_features_per_state():
    sql = m.render_band_counts_sql("proj.ds.tf")
    for feature in m.BAND_COUNT_FEATURES:
        assert f"'{feature}' AS feature" in sql
    assert sql.count("GROUP BY state_fips, band") == len(m.BAND_COUNT_FEATURES)
    assert sql.count("UNION ALL") == len(m.BAND_COUNT_FEATURES) - 1


def test_render_keeps_null_and_out_of_range_bands():
    sql = m.render_band_counts_sql("proj.ds.tf")
    assert "THEN 'null'" in sql
    assert "THEN 'below_range'" in sql
    assert "ELSE 'above_range'" in sql


def test_render_final_band_is_closed_and_inclusive():
    sql = m.render_band_counts_sql("proj.ds.tf", features=["fiber_speed_top_tier"])
    assert "WHEN fiber_speed_top_tier <= 1.00 THEN '[0.80,1.00]'" in sql


def test_custom_edges_and_features_flow_through():
    sql = m.render_band_counts_sql(
        "proj.ds.tf", features=["cable_penetration"], edges=[0.0, 0.5, 1.0]
    )
    assert "'cable_penetration' AS feature" in sql
    assert "[0.00,0.50)" in sql
    assert "[0.50,1.00]" in sql
    assert sql.count("UNION ALL") == 0


def test_render_leaves_no_unresolved_placeholders():
    sql = m.render_band_counts_sql("proj.ds.tf")
    assert "{" not in sql and "}" not in sql


def test_national_band_totals_sums_across_states():
    band_df = pd.DataFrame(
        [
            ("fiber_opportunity_gap", "01", "[0.00,0.20)", 10),
            ("fiber_opportunity_gap", "02", "[0.00,0.20)", 5),
            ("fiber_opportunity_gap", "01", "[0.80,1.00]", 3),
        ],
        columns=["feature", "state_fips", "band", "n_blocks"],
    )
    national = m.national_band_totals(band_df)
    low = national[national["band"] == "[0.00,0.20)"]["n_blocks"].iloc[0]
    assert low == 15
    assert set(national.columns) == {"feature", "band", "n_blocks"}


def test_run_band_counts_reads_and_returns_frame():
    band_df = pd.DataFrame(
        [("fiber_opportunity_gap", "01", "[0.00,0.20)", 10)],
        columns=["feature", "state_fips", "band", "n_blocks"],
    )
    client = FakeClient(band_df)
    result = m.run_band_counts(client=client)
    assert len(client.queries) == 1
    assert list(result["feature"]) == ["fiber_opportunity_gap"]
