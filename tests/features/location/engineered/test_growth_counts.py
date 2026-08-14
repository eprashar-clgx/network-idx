"""
Offline tests for the location growth-counts feature.

These tests exercise the SQL rendering and the build dispatch without any BigQuery
access. A fake client records the SQL it is asked to run, so the tests assert that
the rendered query references every resolved source and output table, injects the
configured radius and H3 resolution, produces the expected growth-count columns, and
leaves no unresolved placeholders, and that a dry run builds the SQL without executing
it.
"""
from network_idx.constants import GROWTH_COUNT_RADIUS_M, GROWTH_PARCEL_H3_RES
from network_idx.features.location.engineered import growth_counts


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
    return growth_counts.render_sql(
        output_table="proj.ds.out",
        parcel_universe="proj.src.parcels",
        clip_to_parcel="proj.src.clip",
        growth_indicators="proj.src.growth",
        property_table="proj.src.property",
        parcel_lineage_event="proj.src.lineage",
        block_geometry="proj.src.blocks",
        carto_project="carto-proj",
    )


def test_render_sql_substitutes_all_sources_and_output():
    sql = _render()
    for ref in [
        "`proj.ds.out`",
        "`proj.src.parcels`",
        "`proj.src.clip`",
        "`proj.src.growth`",
        "`proj.src.property`",
        "`proj.src.lineage`",
        "`proj.src.blocks`",
        "`carto-proj`.carto.H3_FROMGEOGPOINT",
    ]:
        assert ref in sql, ref


def test_render_sql_injects_radius_and_h3_resolution():
    sql = _render()
    assert str(GROWTH_COUNT_RADIUS_M) in sql
    assert f"H3_FROMGEOGPOINT(pf.parcel_centroid, {GROWTH_PARCEL_H3_RES})" in sql


def test_render_sql_produces_growth_count_columns():
    sql = _render()
    for col in [
        "bldr_dev_qtr_mi_cnt",
        "landuse_change_qtr_mi_cnt",
        "new_permit_qtr_mi_cnt",
        "pre_early_dev_qtr_mi_cnt",
        "growth_parcel_qtr_mi_cnt",
    ]:
        assert col in sql, col


def test_render_sql_has_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_sources_resolve_from_registry():
    from network_idx.sources.registry import RAW_SOURCES_BQ

    for name in [
        growth_counts.SOURCE_PARCEL_UNIVERSE,
        growth_counts.SOURCE_CLIP_TO_PARCEL,
        growth_counts.SOURCE_GROWTH_INDICATORS,
        growth_counts.SOURCE_PROPERTY,
        growth_counts.SOURCE_PARCEL_LINEAGE_EVENT,
        growth_counts.SOURCE_BLOCK_GEOMETRY,
    ]:
        assert name in RAW_SOURCES_BQ


def test_build_executes_rendered_sql_with_client():
    client = FakeClient()
    growth_counts.build(client=client)
    assert len(client.queries) == 1
    assert growth_counts.output_table_ref() in client.queries[0].replace("`", "")


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    growth_counts.build(client=client, dry_run=True)
    assert client.queries == []
    printed = capsys.readouterr().out
    assert "growth_parcel_qtr_mi_cnt" in printed
