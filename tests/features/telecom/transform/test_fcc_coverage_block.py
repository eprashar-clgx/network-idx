"""
Offline tests for the FCC coverage block interpolation transform.

These tests exercise the SQL rendering and the build dispatch without any BigQuery
access. A fake client records the SQL it is asked to run, so the tests assert that the
rendered query targets the resolved output and the four input tables, produces every
block output column, distributes source units by the housing-unit share, inherits place or
residual percentages, nulls percentages where a block has no estimated units, tags the
source, includes the generated state lookup, resolves the Census tables from the registry,
leaves no unresolved placeholders, and that a dry run executes nothing while a normal build
issues exactly one query.
"""
from network_idx.constants import FCC_COVERAGE_BLOCK_OUTPUTS
from network_idx.features.telecom.transform import fcc_coverage_block as block


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
    return block.render_sql(
        output_table="proj.ds.coverage_block",
        summary_table="proj.ds.coverage_summary",
        residuals_table="proj.ds.county_residuals",
        baf_table="proj.ds.baf",
        acl_table="proj.ds.acl",
    )


def test_render_targets_output_and_inputs():
    sql = _render()
    assert "CREATE OR REPLACE TABLE `proj.ds.coverage_block`" in sql
    assert "`proj.ds.coverage_summary`" in sql
    assert "`proj.ds.county_residuals`" in sql
    assert "`proj.ds.baf`" in sql
    assert "`proj.ds.acl`" in sql


def test_render_produces_all_block_output_columns():
    sql = _render()
    for col in FCC_COVERAGE_BLOCK_OUTPUTS:
        assert col in sql, f"missing block output column {col}"


def test_render_distributes_units_by_housing_unit_share():
    sql = _render()
    assert "SAFE_DIVIDE(h.census_housing_units, h.place_hu_total) * ps.place_total_units" in sql
    assert "SAFE_DIVIDE(h.census_housing_units, h.county_hu_total) * rs.residual_units" in sql


def test_render_inherits_place_or_residual_percentages():
    sql = _render()
    assert "IF(h.place_geoid IS NOT NULL, ps.fiber_speed_1000_100_only, rs.fiber_speed_1000_100_only) AS fiber_speed_1000_100_only" in sql


def test_render_nulls_percentages_when_no_estimated_units():
    sql = _render()
    assert "IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.copper_speed_02_02_only, 0)) AS copper_speed_02_02_only" in sql


def test_render_tags_source_and_state_lookup():
    sql = _render()
    assert "IF(h.place_geoid IS NOT NULL, 'place', 'county_residual') AS source" in sql
    assert "STRUCT('48' AS state_fips, 'TX' AS state_usps)" in sql


def test_render_has_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_input_tables_resolve_from_registry_and_config():
    assert block.baf_table_ref().endswith("census_baf_block")
    assert block.acl_table_ref().endswith("census_acl_block")
    assert block.summary_table_ref().endswith("fcc_coverage_summary")
    assert block.residuals_table_ref().endswith("fcc_coverage_county_residuals")
    assert block.output_table_ref().endswith("fcc_coverage_block")


def test_build_executes_single_query():
    client = FakeClient()
    block.build(client=client)
    assert len(client.queries) == 1
    assert "CREATE OR REPLACE TABLE" in client.queries[0]


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    block.build(client=client, dry_run=True)
    assert client.queries == []
    printed = capsys.readouterr().out
    assert "CREATE OR REPLACE TABLE" in printed
