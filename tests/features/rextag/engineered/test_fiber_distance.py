"""
Offline tests for the rextag parcel-to-fiber distance feature.

These tests exercise the rendering of the three stored procedures and the deploy-and-run
dispatch without any BigQuery access. A fake client records the SQL it is asked to run,
so the tests assert that each rendered procedure references the resolved names and leaves
no unresolved placeholders, that the driver embeds the shard-count CASE from configuration
and casts nothing away, that the worker casts the fiber id to a string and the assemble
step converts metres to miles, that the driver call formats the states array, and that
build deploys all three procedures then calls the driver and assemble, deploy_only skips
the calls, and a dry run executes nothing.
"""
from network_idx.constants import (
    FIBER_STATE_SHARD_COUNTS,
    FIBER_DEFAULT_SHARD_COUNT,
    METERS_PER_MILE,
)
from network_idx.features.rextag.engineered import fiber_distance


class _FakeQueryJob:
    def result(self):
        return None


class FakeClient:
    def __init__(self):
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return _FakeQueryJob()


def _worker():
    return fiber_distance.render_worker_sql(
        worker_proc="proj.ds.worker",
        calc_table="proj.ds.calc",
        parcel_table="proj.ds.parcels",
        fiber_optimized_table="proj.tel.fiber_opt",
    )


def _driver():
    return fiber_distance.render_driver_sql(
        driver_proc="proj.ds.driver",
        worker_proc="proj.ds.worker",
        calc_table="proj.ds.calc",
    )


def _assemble():
    return fiber_distance.render_assemble_sql(
        assemble_proc="proj.ds.assemble",
        distance_table="proj.ds.distance",
        parcel_table="proj.ds.parcels",
        calc_table="proj.ds.calc",
    )


def test_worker_defines_procedure_and_substitutes_names():
    sql = _worker()
    assert "CREATE OR REPLACE PROCEDURE `proj.ds.worker`(" in sql
    assert "INSERT INTO `proj.ds.calc`" in sql
    assert "`proj.ds.parcels`" in sql
    assert "`proj.tel.fiber_opt`" in sql


def test_worker_casts_fiber_id_to_string():
    sql = _worker()
    assert "CAST(" in sql
    assert "AS STRING" in sql
    assert ") AS nearest_fiber_id" in sql


def test_worker_has_no_unresolved_placeholders():
    sql = _worker()
    assert "{" not in sql and "}" not in sql


def test_driver_defines_procedure_and_embeds_shard_case():
    sql = _driver()
    assert "CREATE OR REPLACE PROCEDURE `proj.ds.driver`(" in sql
    assert "CALL `proj.ds.worker`(" in sql
    for fips, count in FIBER_STATE_SHARD_COUNTS.items():
        assert f"WHEN current_state = '{fips}' THEN {count}" in sql
    assert f"ELSE {FIBER_DEFAULT_SHARD_COUNT}" in sql


def test_driver_creates_staging_table_if_not_exists():
    sql = _driver()
    assert "CREATE TABLE IF NOT EXISTS `proj.ds.calc`" in sql


def test_driver_has_no_unresolved_placeholders():
    sql = _driver()
    assert "{" not in sql and "}" not in sql


def test_assemble_converts_metres_to_miles():
    sql = _assemble()
    assert f"/ {METERS_PER_MILE} AS dist_to_nearest_fiber_miles" in sql
    assert "CREATE OR REPLACE TABLE `proj.ds.distance`" in sql
    assert "LEFT JOIN `proj.ds.calc`" in sql


def test_assemble_has_no_unresolved_placeholders():
    sql = _assemble()
    assert "{" not in sql and "}" not in sql


def test_driver_call_formats_states_array():
    call = fiber_distance.render_driver_call_sql(states=["06", "48"])
    assert call == f"CALL `{fiber_distance.driver_proc_ref()}`(['06', '48'], 24140, 4828);"


def test_driver_call_defaults_to_fifty_states_plus_dc():
    call = fiber_distance.render_driver_call_sql()
    assert "'06'" in call and "'48'" in call
    # territories are excluded from the default set
    assert "'72'" not in call and "'78'" not in call


def test_assemble_call_targets_the_procedure():
    call = fiber_distance.render_assemble_call_sql()
    assert call == f"CALL `{fiber_distance.assemble_proc_ref()}`();"


def test_build_deploys_three_then_calls_driver_and_assemble():
    client = FakeClient()
    fiber_distance.build(client=client)
    assert len(client.queries) == 5
    assert "CREATE OR REPLACE PROCEDURE" in client.queries[0]
    assert "CREATE OR REPLACE PROCEDURE" in client.queries[1]
    assert "CREATE OR REPLACE PROCEDURE" in client.queries[2]
    assert client.queries[3].startswith("CALL")
    assert "rextag_calculate_parcel_dist_to_fiber" in client.queries[3]
    assert client.queries[4].startswith("CALL")


def test_build_deploy_only_does_not_call():
    client = FakeClient()
    fiber_distance.build(client=client, deploy_only=True)
    assert len(client.queries) == 3
    assert all("CREATE OR REPLACE PROCEDURE" in q for q in client.queries)


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    fiber_distance.build(client=client, dry_run=True)
    assert client.queries == []
    printed = capsys.readouterr().out
    assert "CREATE OR REPLACE PROCEDURE" in printed
    assert "CALL" in printed
