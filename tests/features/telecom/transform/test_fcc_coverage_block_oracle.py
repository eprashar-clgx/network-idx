"""
Offline tests for the FCC coverage block parity oracle.

These tests exercise the query rendering, the pandas oracle build, and the parity
comparison without any BigQuery access. The render tests confirm the sample-state filters
and column lists; the build test drives the real reference functions on tiny synthetic
inputs and confirms the oracle conserves units in FCC-unit space (block estimates sum back
to the place and county-residual totals); the comparison tests confirm parity passes when
the two paths agree and fails on both value drift and one-sided nulls.
"""
import pandas as pd

from network_idx.constants import (
    FCC_FIXED_COVERAGE_TECHNOLOGIES,
    FCC_COVERAGE_TIER_METRICS,
)
from network_idx.features.telecom.transform import fcc_coverage_block_oracle as oracle

PCT_COLS = [
    f"{tech.lower()}_{metric}"
    for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES
    for metric in FCC_COVERAGE_TIER_METRICS
]


def _pcts(value: float = 0.0) -> dict:
    return {col: value for col in PCT_COLS}


# ── Render tests ──────────────────────────────────────────────────────────────

def test_render_place_filters_level_and_states():
    sql = oracle.render_place_sql("proj.ds.summary", ["DE", "RI"])
    assert "geography_level = 'place'" in sql
    assert "SUBSTR(geography_id, 1, 2) IN ('10', '44')" in sql
    for col in PCT_COLS:
        assert col in sql


def test_render_county_filters_level_and_states():
    sql = oracle.render_county_sql("proj.ds.summary", ["DE"])
    assert "geography_level = 'county'" in sql
    assert "IN ('10')" in sql


def test_render_baf_and_acl_filter_states():
    baf = oracle.render_baf_sql("proj.ds.baf", ["RI"])
    acl = oracle.render_acl_sql("proj.ds.acl", ["RI"])
    assert "state_fips IN ('44')" in baf
    assert "SUBSTR(block_geoid, 1, 2) IN ('44')" in acl


# ── Oracle build / conservation ───────────────────────────────────────────────

def test_build_oracle_blocks_conserves_units():
    # One Delaware county (1000 units), one place (400 units), two place blocks and two
    # non-place blocks, with housing units chosen so the split is exact.
    county_df = pd.DataFrame([{"geography_id": "10001", "total_units": 1000, **_pcts(0.5)}])
    place_df = pd.DataFrame([{"geography_id": "1050000", "total_units": 400, **_pcts(0.8)}])
    baf_df = pd.DataFrame(
        [
            {"block_geoid": "100010001001", "state_fips": "10", "county_geoid": "10001",
             "tract_geoid": "10001000100", "place_geoid": "1050000"},
            {"block_geoid": "100010001002", "state_fips": "10", "county_geoid": "10001",
             "tract_geoid": "10001000100", "place_geoid": "1050000"},
            {"block_geoid": "100010001003", "state_fips": "10", "county_geoid": "10001",
             "tract_geoid": "10001000100", "place_geoid": None},
            {"block_geoid": "100010001004", "state_fips": "10", "county_geoid": "10001",
             "tract_geoid": "10001000100", "place_geoid": None},
        ]
    )
    acl_df = pd.DataFrame(
        [
            {"block_geoid": "100010001001", "total_housing_units": 100},
            {"block_geoid": "100010001002", "total_housing_units": 100},
            {"block_geoid": "100010001003", "total_housing_units": 300},
            {"block_geoid": "100010001004", "total_housing_units": 300},
        ]
    )

    blocks = oracle.build_oracle_blocks(place_df, county_df, baf_df, acl_df, ["DE"])

    assert len(blocks) == 4
    # Total estimated units reconstruct the county total.
    assert blocks["estimated_fcc_units"].sum() == 1000
    # Place blocks carry the place total; non-place blocks carry the county residual.
    place_units = blocks.loc[blocks["source"] == "place", "estimated_fcc_units"].sum()
    residual_units = blocks.loc[blocks["source"] == "county_residual", "estimated_fcc_units"].sum()
    assert place_units == 400
    assert residual_units == 600


# ── Parity comparison ─────────────────────────────────────────────────────────

def _block_frame(units, pct):
    return pd.DataFrame(
        [{"block_geoid": "b1", "estimated_fcc_units": units, **_pcts(pct)}]
    )


def test_compare_parity_passes_when_equal():
    sql = _block_frame(400, 0.8)
    ora = _block_frame(400, 0.8)
    result = oracle.compare_parity(sql, ora)
    assert result.passed
    assert result.n_blocks == 1


def test_compare_parity_passes_within_unit_rounding():
    sql = _block_frame(400, 0.8)
    ora = _block_frame(401, 0.8)  # one-unit rounding difference is within budget
    result = oracle.compare_parity(sql, ora)
    assert result.passed


def test_compare_parity_fails_on_value_drift():
    sql = _block_frame(400, 0.8)
    ora = _block_frame(410, 0.8)  # ten-unit difference exceeds the budget
    result = oracle.compare_parity(sql, ora)
    assert not result.passed
    assert result.max_abs_diff["estimated_fcc_units"] == 10.0


def test_compare_parity_fails_on_null_mismatch():
    sql = _block_frame(400, 0.8)
    ora = _block_frame(400, None)  # oracle nulled a percentage the SQL kept
    result = oracle.compare_parity(sql, ora)
    assert not result.passed
    assert result.null_mismatch["copper_speed_02_02_only"] == 1


# ── Table resolution ──────────────────────────────────────────────────────────

def test_table_refs_resolve():
    assert oracle.summary_table_ref().endswith("fcc_coverage_summary")
    assert oracle.block_table_ref().endswith("fcc_coverage_block")
    assert oracle.parity_table_ref().endswith("fcc_coverage_block_parity")
    assert oracle.baf_table_ref().endswith("census_baf_block")
    assert oracle.acl_table_ref().endswith("census_acl_block")
