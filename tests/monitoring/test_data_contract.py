"""
Offline tests for the FCC coverage block unit-conservation gate.

These tests exercise the two aggregation queries and the pure comparison without any
BigQuery access. They assert that each query filters to the right source, aggregates to the
right grain, and joins to the right totals table; that conservation passes when every
source's blocks sum back within the per-block rounding budget; that a real drift beyond the
budget fails and is recorded as a breach; that sources with no housing units are set aside
as inherently lossy rather than failed; and that the gate raises to halt the pipeline on a
breach only when halting is requested. A fake client returns canned aggregate frames so the
dispatch can be tested offline.
"""
import pandas as pd
import pytest

from network_idx.monitoring import data_contract as dc


def _agg_df(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "source_kind",
            "source_id",
            "source_units",
            "reconstructed_units",
            "housing_units",
            "n_blocks",
        ],
    )


class _FakeQueryJob:
    def __init__(self, df):
        self._df = df

    def to_dataframe(self):
        return self._df


class FakeClient:
    """Returns a place or residual frame depending on the source filter in the SQL."""

    def __init__(self, place_df, residual_df):
        self.place_df = place_df
        self.residual_df = residual_df
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        if "source = 'place'" in sql:
            return _FakeQueryJob(self.place_df)
        if "source = 'county_residual'" in sql:
            return _FakeQueryJob(self.residual_df)
        raise AssertionError("unexpected query")


def test_render_place_targets_tables_and_filters_source():
    sql = dc.render_place_conservation_sql("proj.ds.block", "proj.ds.summary")
    assert "`proj.ds.block`" in sql
    assert "`proj.ds.summary`" in sql
    assert "source = 'place'" in sql
    assert "geography_level = 'place'" in sql
    assert "GROUP BY place_geoid" in sql


def test_render_residual_targets_tables_and_filters_source():
    sql = dc.render_residual_conservation_sql("proj.ds.block", "proj.ds.residuals")
    assert "`proj.ds.block`" in sql
    assert "`proj.ds.residuals`" in sql
    assert "source = 'county_residual'" in sql
    assert "residual_units" in sql
    assert "GROUP BY county_geoid" in sql


def test_conservation_passes_when_totals_match():
    place = _agg_df([("place", "0100100", 400, 400, 250, 3)])
    residual = _agg_df([("county_residual", "01001", 600, 600, 900, 12)])
    result = dc.compare_conservation(place, residual)
    assert result.passed
    assert result.n_sources_checked == 2
    assert result.breaches == []


def test_rounding_within_budget_passes():
    # Off by 1 unit over 4 blocks: allowed is 0.5 * 4 = 2, so this is within budget.
    place = _agg_df([("place", "0100100", 400, 401, 250, 4)])
    residual = _agg_df([])
    result = dc.compare_conservation(place, residual)
    assert result.passed
    assert result.max_abs_diff == 1.0


def test_drift_beyond_budget_fails_and_records_breach():
    # Off by 50 units over 2 blocks: allowed is 0.5 * 2 = 1, so this is a real breach.
    place = _agg_df([("place", "0100100", 400, 350, 250, 2)])
    residual = _agg_df([])
    result = dc.compare_conservation(place, residual)
    assert not result.passed
    assert len(result.breaches) == 1
    breach = result.breaches[0]
    assert breach["source_id"] == "0100100"
    assert breach["abs_diff"] == 50.0
    assert breach["allowed"] == 1.0


def test_zero_housing_sources_are_set_aside_not_failed():
    # A place with units but no housing units cannot be spread; it must not fail the gate.
    place = _agg_df([("place", "0100100", 400, 0, 0, 5)])
    residual = _agg_df([("county_residual", "01001", 600, 600, 900, 12)])
    result = dc.compare_conservation(place, residual)
    assert result.passed
    assert result.n_lossy_zero_hu == 1
    assert result.lossy_units == 400
    assert result.n_sources_checked == 1


def test_worst_breach_is_reported_first():
    place = _agg_df(
        [
            ("place", "small", 100, 90, 50, 2),   # diff 10, allowed 1
            ("place", "big", 1000, 500, 300, 2),  # diff 500, allowed 1
        ]
    )
    result = dc.compare_conservation(place, _agg_df([]))
    assert not result.passed
    assert result.breaches[0]["source_id"] == "big"
    assert result.max_abs_diff == 500.0


def test_check_issues_two_queries_and_passes():
    place = _agg_df([("place", "0100100", 400, 400, 250, 3)])
    residual = _agg_df([("county_residual", "01001", 600, 600, 900, 12)])
    client = FakeClient(place, residual)
    result = dc.check(client=client)
    assert result.passed
    assert len(client.queries) == 2


def test_check_halts_on_breach_when_requested():
    place = _agg_df([("place", "0100100", 400, 100, 250, 2)])
    residual = _agg_df([])
    client = FakeClient(place, residual)
    with pytest.raises(dc.ConservationError):
        dc.check(client=client, halt=True)


def test_check_returns_without_raising_when_halt_disabled():
    place = _agg_df([("place", "0100100", 400, 100, 250, 2)])
    residual = _agg_df([])
    client = FakeClient(place, residual)
    result = dc.check(client=client, halt=False)
    assert not result.passed
