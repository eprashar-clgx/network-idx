"""
Offline tests for the business rollups of the delivered parcel index.

These tests exercise the three rollup queries and the run dispatch without any BigQuery
access. They assert that the quartile query counts parcels per index and quartile with a
leftover-free render, that the high-score query counts parcels at or above the threshold,
and that the per-state query keys on the first two digits of the block GEOID and carries a
fiber-potential count per quartile. A fake client returns canned frames so the run
dispatch can be tested offline and asserted to filter to the requested run.
"""
import pandas as pd

from network_idx.monitoring import business_rollups as br


class _FakeQueryJob:
    def __init__(self, df):
        self._df = df

    def to_dataframe(self):
        return self._df


class FakeClient:
    def __init__(self, frames):
        self._frames = list(frames)
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return _FakeQueryJob(self._frames[len(self.queries) - 1])


def test_index_quartiles_render_covers_all_indices():
    sql = br.render_index_quartiles_sql("proj.ds.parcel_scores", "run_x")
    for name in ("fiber_potential", "growth", "telecom", "demographic"):
        assert f"'{name}' AS index_name" in sql
    assert "`proj.ds.parcel_scores`" in sql
    assert "WHERE run_id = 'run_x'" in sql
    assert "{" not in sql and "}" not in sql


def test_index_quartiles_render_bands_are_half_open_with_closed_top():
    sql = br.render_index_quartiles_sql("t", "r")
    assert "WHEN idx_overall < 25.0 THEN 'Q1'" in sql
    assert "WHEN idx_overall < 75.0 THEN 'Q3'" in sql
    # The top quartile is closed so a perfect 100 lands in Q4.
    assert "WHEN idx_overall <= 100.0 THEN 'Q4'" in sql
    assert "IS NULL THEN 'null'" in sql


def test_high_score_render_uses_threshold():
    sql = br.render_high_score_sql("t", "r", threshold=85.0)
    assert "COUNTIF(idx_overall >= 85.0) AS n_at_or_above" in sql
    assert "COUNTIF(idx_telecom >= 85.0) AS n_at_or_above" in sql
    assert "COUNT(*) AS n_scored" in sql


def test_high_score_render_respects_custom_threshold():
    sql = br.render_high_score_sql("t", "r", threshold=90.0)
    assert "90.0) AS n_at_or_above" in sql
    assert "85.0" not in sql


def test_state_rollup_render_keys_on_state_and_counts_quartiles():
    sql = br.render_state_rollup_sql("proj.ds.parcel_scores", "run_x")
    assert "SUBSTR(block_geoid, 1, 2) AS state_fips" in sql
    assert "COUNT(*) AS total_parcels" in sql
    assert "fiber_potential_q1" in sql and "fiber_potential_q4" in sql
    assert "idx_overall <= 100.0 AND idx_overall >= 75.0) AS fiber_potential_q4" in sql
    assert "GROUP BY state_fips" in sql


def test_run_returns_three_frames_and_filters_run():
    quartiles = pd.DataFrame({"index_name": ["growth"], "quartile": ["Q1"], "n_parcels": [5]})
    high = pd.DataFrame({"index_name": ["growth"], "threshold": [85.0], "n_at_or_above": [1], "n_scored": [5]})
    state = pd.DataFrame({"state_fips": ["06"], "total_parcels": [5]})
    client = FakeClient([quartiles, high, state])

    out = br.run("run_x", client=client)

    assert set(out) == {"index_quartiles", "high_scores", "state_rollup"}
    assert len(client.queries) == 3
    assert all("run_x" in q for q in client.queries)
    assert out["state_rollup"].iloc[0]["state_fips"] == "06"
