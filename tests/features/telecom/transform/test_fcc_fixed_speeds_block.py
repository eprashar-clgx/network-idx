"""
Offline tests for the FCC fixed-speed block transform.

These tests exercise the SQL rendering and the build dispatch without any BigQuery
access. A fake client records the SQL it is asked to run, so the tests assert that the
rendered query targets the resolved output and the three per-technology input tables,
produces the expected block-level output columns, leaves no unresolved placeholders,
and that a dry run executes nothing while a normal build issues exactly one query.
"""
from network_idx.constants import FCC_FIXED_SPEED_OUTPUTS
from network_idx.features.telecom.transform import fcc_fixed_speeds_block


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
    return fcc_fixed_speeds_block.render_sql(
        output_table="proj.ds.speeds_block",
        copper_table="proj.src.copper",
        cable_table="proj.src.cable",
        fiber_table="proj.src.fiber",
    )


def test_render_targets_output_and_inputs():
    sql = _render()
    assert "CREATE OR REPLACE TABLE `proj.ds.speeds_block`" in sql
    assert "`proj.src.copper`" in sql
    assert "`proj.src.cable`" in sql
    assert "`proj.src.fiber`" in sql


def test_render_produces_all_output_columns():
    sql = _render()
    for col in FCC_FIXED_SPEED_OUTPUTS:
        assert col in sql, f"missing output column {col}"


def test_render_has_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_input_tables_resolve_from_registry():
    assert fcc_fixed_speeds_block.copper_table_ref().endswith("fcc_copper_fixed_broadband")
    assert fcc_fixed_speeds_block.cable_table_ref().endswith("fcc_cable_fixed_broadband")
    assert fcc_fixed_speeds_block.fiber_table_ref().endswith("fcc_fiber_fixed_broadband")


def test_build_executes_single_query():
    client = FakeClient()
    fcc_fixed_speeds_block.build(client=client)
    assert len(client.queries) == 1
    assert "CREATE OR REPLACE TABLE" in client.queries[0]


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    fcc_fixed_speeds_block.build(client=client, dry_run=True)
    assert client.queries == []
    printed = capsys.readouterr().out
    assert "CREATE OR REPLACE TABLE" in printed
