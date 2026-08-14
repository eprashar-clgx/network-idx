"""
Offline tests for the location growth-concentrations feature.

These tests exercise the SQL rendering and the build dispatch without any BigQuery
access. A fake client records the SQL it is asked to run, so the tests assert that
the rendered query references the resolved input and output tables, injects the
configured H3 resolution and thresholds, produces the expected hotspot columns, and
leaves no unresolved placeholders, and that a dry run builds the SQL without executing
it.
"""
from network_idx.constants import (
    GROWTH_HOTSPOT_H3_RES,
    GROWTH_HOTSPOT_TOTAL_FLAGS_THRESHOLD,
    GROWTH_HOTSPOT_VARIETY_THRESHOLD,
)
from network_idx.features.location.engineered import growth_concentrations


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
    return growth_concentrations.render_sql(
        output_table="proj.ds.hotspots",
        input_table="proj.ds.parcels",
        carto_project="carto-proj",
    )


def test_render_sql_substitutes_tables_and_carto():
    sql = _render()
    assert "`proj.ds.hotspots`" in sql
    assert "`proj.ds.parcels`" in sql
    assert "`carto-proj`.carto.H3_FROMGEOGPOINT" in sql
    assert "`carto-proj`.carto.H3_BOUNDARY" in sql


def test_render_sql_injects_resolution_and_thresholds():
    sql = _render()
    assert f"H3_FROMGEOGPOINT(parcel_centroid, {GROWTH_HOTSPOT_H3_RES})" in sql
    assert f"total_flags >= {GROWTH_HOTSPOT_TOTAL_FLAGS_THRESHOLD}" in sql
    assert f"flags_minus_greatest >= {GROWTH_HOTSPOT_VARIETY_THRESHOLD}" in sql


def test_render_sql_produces_hotspot_columns():
    sql = _render()
    for col in ["h3_id", "total_flags", "flags_minus_greatest", "geom"]:
        assert col in sql, col


def test_render_sql_has_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_input_output_resolve_within_features_dataset():
    assert growth_concentrations.input_table_ref().endswith("loc_growth_cnts_parcel")
    assert growth_concentrations.output_table_ref().endswith(
        "loc_growth_parcel_concentrations_h3r7"
    )


def test_build_executes_rendered_sql_with_client():
    client = FakeClient()
    growth_concentrations.build(client=client)
    assert len(client.queries) == 1
    assert growth_concentrations.output_table_ref() in client.queries[0].replace("`", "")


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    growth_concentrations.build(client=client, dry_run=True)
    assert client.queries == []
    assert "total_flags" in capsys.readouterr().out
