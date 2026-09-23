"""
Offline tests for the rextag parcel-to-fiber distance feature.

These tests exercise the rendering of the three stored procedures and the deploy-and-run
dispatch without any BigQuery access. A fake client records the SQL it is asked to run,
so the tests assert that each rendered procedure references the resolved names and leaves
no unresolved placeholders, that the driver embeds the shard-count CASE from configuration
and raises when a shard fails, that the worker pre-filters fiber to a state-boundary
buffer before its spatial join and stores an INT64 fiber id, that the assemble step maps
that id back to a stable string loc_id and converts metres to miles, that the driver call
formats the states array, and that build deploys all three procedures then calls the
driver and assemble, deploy_only skips the calls, and a dry run executes nothing.
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
        state_boundary_table="proj.ref.state_boundary",
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
        fiber_optimized_table="proj.tel.fiber_opt",
    )


def test_worker_defines_procedure_and_substitutes_names():
    sql = _worker()
    assert "CREATE OR REPLACE PROCEDURE `proj.ds.worker`(" in sql
    assert "INSERT INTO `proj.ds.calc`" in sql
    assert "`proj.ds.parcels`" in sql
    assert "`proj.tel.fiber_opt`" in sql
    assert "`proj.ref.state_boundary`" in sql


def test_worker_prefilters_fiber_to_state_boundary_buffer():
    sql = _worker()
    assert "ST_BUFFER(" in sql
    assert "ST_INTERSECTS(f.geometry, b.buffer_geom)" in sql
    assert "WHERE state_fips = current_state" in sql


def test_worker_stores_int_fiber_id_without_casting():
    sql = _worker()
    assert "AS nearest_fiber_id" in sql
    assert "CAST(" not in sql
    assert "spatial_fiber_id" in sql


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
    assert "nearest_fiber_id INT64" in sql


def test_driver_raises_after_processing_all_states_when_a_shard_fails():
    sql = _driver()
    assert "DECLARE failures ARRAY<STRING>" in sql
    assert "EXCEPTION WHEN ERROR THEN" in sql
    # the failure is recorded, not re-raised immediately, so every state/shard is
    # still attempted
    assert "SET failures = ARRAY_CONCAT(failures" in sql
    # then the whole procedure raises once every state/shard has been attempted
    assert "RAISE USING MESSAGE" in sql
    assert "IF ARRAY_LENGTH(failures) > 0 THEN" in sql


def test_calc_table_creates_staging_table_with_int_fiber_id():
    sql = fiber_distance.render_calc_table_sql(calc_table="proj.ds.calc")
    assert "CREATE TABLE IF NOT EXISTS `proj.ds.calc`" in sql
    assert "nearest_fiber_id INT64" in sql
    assert "CLUSTER BY state_fips, parcel_shape_id" in sql
    assert "{" not in sql and "}" not in sql


def test_driver_has_no_unresolved_placeholders():
    sql = _driver()
    assert "{" not in sql and "}" not in sql


def test_assemble_converts_metres_to_miles():
    sql = _assemble()
    assert f"/ {METERS_PER_MILE} AS dist_to_nearest_fiber_miles" in sql
    assert "CREATE OR REPLACE TABLE `proj.ds.distance`" in sql
    assert "LEFT JOIN `proj.ds.calc`" in sql


def test_assemble_maps_spatial_fiber_id_back_to_stable_loc_id():
    sql = _assemble()
    assert "fiber_lookup" in sql
    assert "`proj.tel.fiber_opt`" in sql
    assert "fl.loc_id AS nearest_fiber_id" in sql
    assert "ON c.nearest_fiber_id = fl.spatial_fiber_id" in sql


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
    assert len(client.queries) == 7
    assert "CREATE TABLE IF NOT EXISTS" in client.queries[0]
    assert "CREATE OR REPLACE PROCEDURE" in client.queries[1]
    assert "CREATE OR REPLACE PROCEDURE" in client.queries[2]
    assert "CREATE OR REPLACE PROCEDURE" in client.queries[3]
    assert client.queries[4].startswith("CALL")
    assert "rextag_calculate_parcel_dist_to_fiber" in client.queries[4]
    assert client.queries[5].startswith("CALL")
    # final query is the state-completeness guard
    assert client.queries[6].startswith("ASSERT")
    assert "processed_at IS NOT NULL" in client.queries[6]


def test_completeness_assert_checks_every_expected_state():
    sql = fiber_distance.render_completeness_assert_sql()
    n = len(fiber_distance.DEFAULT_STATES)
    assert sql.startswith("ASSERT")
    assert f"= {n}" in sql
    assert "COUNT(DISTINCT state_fips)" in sql
    assert "processed_at IS NOT NULL" in sql
    assert fiber_distance.distance_table_ref() in sql
    # a subset run asserts only the subset it processed
    sub = fiber_distance.render_completeness_assert_sql(states=["06", "48"])
    assert "= 2" in sub
    assert "'06', '48'" in sub
    assert "{" not in sql and "}" not in sql


def test_build_deploy_only_does_not_call():
    client = FakeClient()
    fiber_distance.build(client=client, deploy_only=True)
    assert len(client.queries) == 4
    assert "CREATE TABLE IF NOT EXISTS" in client.queries[0]
    assert all("CREATE OR REPLACE PROCEDURE" in q for q in client.queries[1:])


def test_build_dry_run_does_not_execute(capsys):
    client = FakeClient()
    fiber_distance.build(client=client, dry_run=True)
    assert client.queries == []
    printed = capsys.readouterr().out
    assert "CREATE OR REPLACE PROCEDURE" in printed
    assert "CALL" in printed
