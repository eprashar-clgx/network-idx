"""
Offline tests for the rextag fiber-optimize transform.

These tests exercise the procedure rendering and the deploy-and-run dispatch without
any BigQuery access. A fake client records the SQL it is asked to run, so the tests
assert that the rendered procedure references the resolved names, injects the boundary
UDF dataset and vertex threshold, defines a stored procedure, and leaves no unresolved
placeholders; that the call statement targets the same procedure; and that build
deploys then runs, deploy_only deploys without running, and a dry run does neither.
"""
from network_idx.constants import FIBER_SUBDIVIDE_MAX_VERTICES
from network_idx.features.rextag.transform import fiber_optimize


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
    return fiber_optimize.render_procedure_sql(
        proc_ref_="proj.ds.clean_optimize",
        output_table="proj.ds.optimized",
        input_view="proj.src.fiber_view",
        boundary_dataset="boundary_ds",
    )


def test_render_defines_procedure_and_substitutes_names():
    sql = _render()
    assert "CREATE OR REPLACE PROCEDURE `proj.ds.clean_optimize`()" in sql
    assert "`proj.ds.optimized`" in sql
    assert "`proj.src.fiber_view`" in sql
    assert "`boundary_ds`.st_subdivide16" in sql


def test_render_injects_subdivide_threshold():
    sql = _render()
    assert f"num_points > {FIBER_SUBDIVIDE_MAX_VERTICES}" in sql


def test_render_has_no_unresolved_placeholders():
    sql = _render()
    assert "{" not in sql and "}" not in sql


def test_input_view_resolves_from_registry():
    assert fiber_optimize.input_view_ref().endswith(
        "vw_rextag_telecommunications_fiber_optic_cables"
    )


def test_call_targets_the_procedure():
    call = fiber_optimize.render_call_sql()
    assert call == f"CALL `{fiber_optimize.proc_ref()}`();"


def test_build_deploys_then_runs():
    client = FakeClient()
    fiber_optimize.build(client=client)
    assert len(client.queries) == 2
    assert "CREATE OR REPLACE PROCEDURE" in client.queries[0]
    assert client.queries[1].startswith("CALL")


def test_build_deploy_only_does_not_call():
    client = FakeClient()
    fiber_optimize.build(client=client, deploy_only=True)
    assert len(client.queries) == 1
    assert "CREATE OR REPLACE PROCEDURE" in client.queries[0]


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    fiber_optimize.build(client=client, dry_run=True)
    assert client.queries == []
    printed = capsys.readouterr().out
    assert "CREATE OR REPLACE PROCEDURE" in printed
    assert "CALL" in printed
