"""
Offline tests for the tract-grain FCC telecom feature grain transfer.

These exercise the SQL rendering and build dispatch of ``fcc_features_ct`` without any
BigQuery access. A fake client records the SQL it is asked to run, so the tests assert the
rendered query rolls the provider tables and coverage block up to tract, feeds the shared
engineered-telecom definition, emits the four telecom features and the tract keys, and
leaves no unresolved placeholders — and that a dry run builds the SQL without executing it.
"""
from network_idx.grain_transfer import fcc_features_ct


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
    return fcc_features_ct.render_sql(
        output_table="proj.ds.telecom_features_ct",
        coverage_block_table="proj.ds.coverage_block",
        copper_table="prod.ref.copper",
        cable_table="prod.ref.cable",
        fiber_table="prod.ref.fiber",
    )


def test_render_references_all_tables():
    sql = _render()
    assert "`proj.ds.telecom_features_ct`" in sql
    assert "`proj.ds.coverage_block`" in sql
    assert "`prod.ref.copper`" in sql
    assert "`prod.ref.cable`" in sql
    assert "`prod.ref.fiber`" in sql
    assert "{" not in sql and "}" not in sql


def test_render_rolls_providers_to_tract():
    sql = _render()
    # provider/location counts are distinct counts grouped to the 11-digit tract geoid
    assert "SUBSTR(block_geoid, 1, 11) AS tract_geoid" in sql
    assert "COUNT(DISTINCT provider_id)" in sql
    assert "COUNT(DISTINCT location_id)" in sql


def test_render_weights_top_tier_speed_by_units():
    sql = _render()
    # FCC-unit-weighted mean of the block top-tier coverage percentage
    assert "SUM(estimated_fcc_units * fiber_speed_1000_100_only)" in sql
    assert "NULLIF(SUM(estimated_fcc_units), 0)" in sql


def test_render_emits_the_four_features_and_keys():
    sql = _render()
    for col in (
        "tract_geoid",
        "state_fips",
        "cable_penetration",
        "fiber_opportunity_gap",
        "fiber_speed_top_tier",
        "provider_competitive_landscape_ord",
    ):
        assert col in sql


def test_render_clusters_by_state():
    sql = _render()
    assert "CLUSTER BY state_fips" in sql


def test_dry_run_does_not_execute():
    client = FakeClient()
    fcc_features_ct.build(client=client, dry_run=True)
    assert client.queries == []


def test_build_executes_once():
    client = FakeClient()
    fcc_features_ct.build(client=client, dry_run=False)
    assert len(client.queries) == 1
    assert "CREATE OR REPLACE TABLE" in client.queries[0]
