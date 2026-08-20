"""
Offline tests for the parcel feature assembly.

These tests exercise the SQL rendering and the build dispatch without any BigQuery access. A
fake client records the SQL it is asked to run, so the tests assert that the rendered query
targets the resolved output and the five input tables, uses the parcel growth table as a
left-joined spine, bridges the block and tract grains on the block GEOID and its tract prefix,
produces every one of the thirteen scoring-contract features, leaves no unresolved
placeholders, and that a dry run executes nothing while a normal build issues exactly one
query.
"""
from network_idx.constants import ALL_SCORING_FEATURES
from network_idx.features import parcel_features as pf


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
    return pf.render_sql(
        output_table="proj.ds.parcel_features",
        parcel_growth_table="proj.ds.growth",
        rextag_distance_table="proj.ds.rextag",
        hotspot_distance_table="proj.ds.hotspot",
        telecom_block_table="proj.ds.telecom",
        demo_tract_table="proj.ds.demo",
    )


def test_render_targets_output_and_all_inputs():
    sql = _render()
    assert "CREATE OR REPLACE TABLE `proj.ds.parcel_features`" in sql
    for table in ("growth", "rextag", "hotspot", "telecom", "demo"):
        assert f"`proj.ds.{table}`" in sql


def test_spine_is_growth_and_all_joins_are_left():
    sql = _render()
    assert "FROM `proj.ds.growth` AS p" in sql
    assert sql.count("LEFT JOIN") == 4


def test_block_and_tract_grains_are_bridged():
    sql = _render()
    assert "p.block_id = t.block_geoid" in sql
    assert "SUBSTR(p.block_id, 1, 11) = d.tract_geoid" in sql


def test_produces_every_scoring_contract_feature():
    sql = _render()
    for feature in ALL_SCORING_FEATURES:
        assert feature in sql, f"missing scoring feature {feature}"


def test_render_leaves_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_build_issues_one_query():
    client = FakeClient()
    pf.build(client=client)
    assert len(client.queries) == 1
    assert "parcel_features" in client.queries[0]


def test_dry_run_executes_nothing():
    client = FakeClient()
    pf.build(client=client, dry_run=True)
    assert client.queries == []
