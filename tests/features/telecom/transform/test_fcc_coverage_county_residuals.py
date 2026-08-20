"""
Offline tests for the FCC coverage county-residuals transform.

These tests exercise the SQL rendering and the build dispatch without any BigQuery
access. A fake client records the SQL it is asked to run, so the tests assert that the
rendered query targets the resolved output and input tables, produces every residual
output column, weights place contributions by the block share, computes the residual as
county coverage minus place coverage over residual units, clamps and null-guards it,
resolves the crosswalk from the registry, leaves no unresolved placeholders, and that a
dry run executes nothing while a normal build issues exactly one query.
"""
from network_idx.constants import FCC_COVERAGE_COUNTY_RESIDUAL_OUTPUTS
from network_idx.features.telecom.transform import fcc_coverage_county_residuals as residuals


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
    return residuals.render_sql(
        output_table="proj.ds.county_residuals",
        summary_table="proj.ds.coverage_summary",
        baf_table="proj.ds.baf",
    )


def test_render_targets_output_and_inputs():
    sql = _render()
    assert "CREATE OR REPLACE TABLE `proj.ds.county_residuals`" in sql
    assert "`proj.ds.coverage_summary`" in sql
    assert "`proj.ds.baf`" in sql


def test_render_produces_all_residual_output_columns():
    sql = _render()
    for col in FCC_COVERAGE_COUNTY_RESIDUAL_OUTPUTS:
        assert col in sql, f"missing residual output column {col}"


def test_render_weights_places_by_block_share():
    sql = _render()
    assert "SAFE_DIVIDE(b.blocks_in_county, t.total_blocks_in_place) AS place_share" in sql
    assert "p.place_total_units * COALESCE(p.copper_speed_02_02_only, 0) * m.place_share" in sql


def test_render_residual_formula_is_clamped_and_null_guarded():
    sql = _render()
    assert "GREATEST(c.county_total_units - COALESCE(pa.places_total_units, 0), 0) AS residual_units" in sql
    assert (
        "CASE WHEN residual_units = 0 THEN NULL ELSE "
        "LEAST(GREATEST(SAFE_DIVIDE(county_total_units * "
        "COALESCE(fiber_speed_1000_100_only_county_pct, 0) - "
        "fiber_speed_1000_100_only_places_abs, residual_units), 0), 1) END "
        "AS fiber_speed_1000_100_only"
    ) in sql


def test_render_only_places_in_summary_contribute():
    sql = _render()
    assert "FROM county_place_map m\n  JOIN places p USING (place_geoid)" in sql


def test_render_has_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_input_tables_resolve_from_registry_and_config():
    assert residuals.baf_table_ref().endswith("census_baf_block")
    assert residuals.summary_table_ref().endswith("fcc_coverage_summary")
    assert residuals.output_table_ref().endswith("fcc_coverage_county_residuals")


def test_build_executes_single_query():
    client = FakeClient()
    residuals.build(client=client)
    assert len(client.queries) == 1
    assert "CREATE OR REPLACE TABLE" in client.queries[0]


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    residuals.build(client=client, dry_run=True)
    assert client.queries == []
    printed = capsys.readouterr().out
    assert "CREATE OR REPLACE TABLE" in printed
