"""
Offline tests for the telecom block-feature engineered module.

These tests exercise the SQL rendering and the build dispatch without any BigQuery access.
A fake client records the SQL it is asked to run, so the tests assert that the rendered
query targets the resolved output and the coverage and speeds block input tables, joins the
speeds counts onto the coverage spine, defaults missing counts to zero, produces the four
telecom features and both landscape representations, gates the top-tier fiber speed on
serviceable fiber, generates the label-to-ordinal ladder from the scoring contract, leaves
no unresolved placeholders, and that a dry run executes nothing while a normal build issues
exactly one query.
"""
from network_idx.constants import PROVIDER_LANDSCAPE_ORDER
from network_idx.features.telecom.engineered import telecom_features_block as feat


class _FakeQueryJob:
    def result(self):
        return None


class FakeClient:
    def __init__(self):
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return _FakeQueryJob()


def _render():
    return feat.render_sql(
        output_table="proj.ds.telecom_features_block",
        coverage_block_table="proj.ds.coverage_block",
        speeds_block_table="proj.ds.speeds_block",
    )


def test_render_targets_output_and_inputs():
    sql = _render()
    assert "CREATE OR REPLACE TABLE `proj.ds.telecom_features_block`" in sql
    assert "`proj.ds.coverage_block`" in sql
    assert "`proj.ds.speeds_block`" in sql


def test_coverage_is_the_spine_left_joined_to_speeds():
    sql = _render()
    assert "FROM `proj.ds.coverage_block` AS c" in sql
    assert "LEFT JOIN `proj.ds.speeds_block` AS s" in sql


def test_missing_speed_counts_default_to_zero():
    sql = _render()
    for tech in ("cable", "fiber", "copper"):
        assert f"COALESCE(s.{tech}_location_count, 0)" in sql
        assert f"COALESCE(s.{tech}_provider_count, 0)" in sql


def test_produces_the_four_features_and_both_landscape_columns():
    sql = _render()
    assert "AS cable_penetration" in sql
    assert "AS fiber_opportunity_gap" in sql
    assert "AS fiber_speed_top_tier" in sql
    assert "AS provider_competitive_landscape" in sql
    assert "AS provider_competitive_landscape_ord" in sql


def test_top_tier_fiber_is_gated_on_serviceable_fiber():
    sql = _render()
    assert "fiber_location_count > 0 AND fiber_provider_count > 0" in sql
    assert "fiber_speed_1000_100_only * has_fiber AS fiber_speed_top_tier" in sql


def test_ordinal_ladder_is_generated_from_the_scoring_contract():
    sql = _render()
    for label, rank in PROVIDER_LANDSCAPE_ORDER.items():
        assert f"WHEN '{label}' THEN {rank}" in sql


def test_render_leaves_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_build_issues_one_query():
    client = FakeClient()
    feat.build(client=client)
    assert len(client.queries) == 1
    assert "telecom_features_block" in client.queries[0]


def test_dry_run_executes_nothing():
    client = FakeClient()
    feat.build(client=client, dry_run=True)
    assert client.queries == []
