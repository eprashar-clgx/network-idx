"""
Offline tests for the location hotspot-distance feature.

These tests exercise the SQL rendering and the build dispatch without any BigQuery
access. A fake client records the SQL it is asked to run, so the tests assert that
the rendered query references the parcel, concentrations, and output tables, injects
the maximum search distance and metres-per-mile conversion, emits the scoring-contract
column name `dist_to_nearest_hotspot_miles`, and leaves no unresolved placeholders,
and that a dry run builds the SQL without executing it.
"""
from network_idx.constants import (
    GROWTH_FEATURES,
    HOTSPOT_MAX_SEARCH_DIST_M,
    METERS_PER_MILE,
)
from network_idx.features.location.engineered import hotspot_distance


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
    return hotspot_distance.render_sql(
        output_table="proj.ds.dist",
        parcel_table="proj.ds.parcels",
        concentrations_table="proj.ds.hotspots",
    )


def test_render_sql_substitutes_all_tables():
    sql = _render()
    assert "`proj.ds.dist`" in sql
    assert "`proj.ds.parcels`" in sql
    assert "`proj.ds.hotspots`" in sql


def test_render_sql_injects_distance_and_conversion():
    sql = _render()
    assert str(HOTSPOT_MAX_SEARCH_DIST_M) in sql
    assert f"/ {METERS_PER_MILE}" in sql


def test_render_sql_emits_scoring_contract_column():
    sql = _render()
    # The column name must match the growth feature the scoring contract expects.
    assert "dist_to_nearest_hotspot_miles" in sql
    assert "dist_to_nearest_hotspot_miles" in GROWTH_FEATURES
    # The stale metres-suffixed name from the source proc must not survive.
    assert "dist_to_nearest_hotspot_m " not in sql
    assert "dist_to_nearest_hotspot_m," not in sql


def test_render_sql_has_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_tables_resolve_within_features_dataset():
    assert hotspot_distance.parcel_table_ref().endswith("loc_growth_cnts_parcel")
    assert hotspot_distance.concentrations_table_ref().endswith(
        "loc_growth_parcel_concentrations_h3r7"
    )
    assert hotspot_distance.output_table_ref().endswith("loc_growth_distance_parcel")


def test_build_executes_rendered_sql_with_client():
    client = FakeClient()
    hotspot_distance.build(client=client)
    assert len(client.queries) == 1
    assert hotspot_distance.output_table_ref() in client.queries[0].replace("`", "")


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    hotspot_distance.build(client=client, dry_run=True)
    assert client.queries == []
    assert "dist_to_nearest_hotspot_miles" in capsys.readouterr().out
