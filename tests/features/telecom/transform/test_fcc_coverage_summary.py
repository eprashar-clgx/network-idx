"""
Offline tests for the FCC coverage summary transform.

These tests exercise the SQL rendering and the build dispatch without any BigQuery
access. A fake client records the SQL it is asked to run, so the tests assert that the
rendered query targets the resolved output and the two input tables, filters each source
correctly, zero-pads the place identifier, produces every pivoted tier column, leaves no
unresolved placeholders, and that a dry run executes nothing while a normal build issues
exactly one query.
"""
from network_idx.constants import (
    FCC_FIXED_COVERAGE_TECHNOLOGIES,
    FCC_COVERAGE_TIER_METRICS,
)
from network_idx.features.telecom.transform import fcc_coverage_summary

PCT_COLS = [
    f"{tech.lower()}_{metric}"
    for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES
    for metric in FCC_COVERAGE_TIER_METRICS
]


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
    return fcc_coverage_summary.render_sql(
        output_table="proj.ds.coverage_summary",
        place_table="proj.src.place",
        geography_table="proj.src.geography",
    )


def test_render_targets_output_and_inputs():
    sql = _render()
    assert "CREATE OR REPLACE TABLE `proj.ds.coverage_summary`" in sql
    assert "`proj.src.place`" in sql
    assert "`proj.src.geography`" in sql


def test_render_produces_all_tier_columns():
    sql = _render()
    for col in PCT_COLS:
        assert col in sql, f"missing tier column {col}"


def test_render_filters_sources():
    sql = _render()
    assert "area_data_type = 'Total'" in sql
    assert "biz_res = 'R'" in sql
    assert "geography_type = 'County'" in sql
    assert "IN ('Copper', 'Cable', 'Fiber')" in sql


def test_render_zero_pads_place_id():
    sql = _render()
    assert "LPAD(CAST(geography_id AS STRING), 7, '0')" in sql


def test_render_has_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_input_tables_resolve_from_registry():
    assert fcc_coverage_summary.place_table_ref().endswith("fcc_fixed_broadband_summary_census")
    assert fcc_coverage_summary.geography_table_ref().endswith("fcc_fixed_broadband_geography")


def test_build_executes_single_query():
    client = FakeClient()
    fcc_coverage_summary.build(client=client)
    assert len(client.queries) == 1
    assert "CREATE OR REPLACE TABLE" in client.queries[0]


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    fcc_coverage_summary.build(client=client, dry_run=True)
    assert client.queries == []
    printed = capsys.readouterr().out
    assert "CREATE OR REPLACE TABLE" in printed
