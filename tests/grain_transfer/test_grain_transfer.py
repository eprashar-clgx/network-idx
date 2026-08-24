"""
Offline tests for the grain-transfer framework.

These tests exercise the promotion renderer and the two adapters without any BigQuery
access. The BigQuery adapter is checked at the string level — that it quotes with
backticks, wraps the SELECT in a create-or-replace, and spells each aggregate the
BigQuery way. The DuckDB adapter is checked for real: a handful of block rows are loaded
into an in-memory DuckDB, the FCC speeds block-to-tract promotion is executed, and the
resulting tract rows are asserted to have summed location counts and maxed speeds, which
validates the aggregate-up logic the same spec will run on BigQuery.
"""
import duckdb
import pytest

from network_idx.grain_transfer import (
    FCC_SPEEDS_CT_SPEC,
    BigQueryAdapter,
    DuckDBAdapter,
    promote,
    render_sql,
)
from network_idx.grain_transfer.specs import (
    AGGREGATE_UP,
    Aggregation,
    PromotionSpec,
)


def test_bigquery_render_wraps_and_quotes():
    adapter = BigQueryAdapter()
    sql = render_sql(FCC_SPEEDS_CT_SPEC, adapter, "proj.ds.speeds_block", "proj.ds.speeds_ct")
    assert sql.startswith("CREATE OR REPLACE TABLE `proj.ds.speeds_ct` AS")
    assert "`proj.ds.speeds_block`" in sql
    assert "SUBSTR(block_geoid, 1, 11) AS `tract_geoid`" in sql
    assert "SUM(cable_location_count) AS `cable_location_count`" in sql
    assert "MAX(fiber_max_download_speed) AS `fiber_max_download_speed`" in sql
    assert "GROUP BY `tract_geoid`" in sql
    # Provider counts are deliberately not re-aggregated from block-level counts.
    assert "provider_count" not in sql
    assert "{" not in sql and "}" not in sql


def test_bigquery_median_uses_approx_quantiles():
    adapter = BigQueryAdapter()
    spec = PromotionSpec(
        name="t", direction=AGGREGATE_UP, target_key="tract_geoid",
        key_expr="SUBSTR(block_geoid, 1, 11)",
        aggregations=(Aggregation("dist", "median"),),
    )
    sql = render_sql(spec, adapter, "p.d.src", "p.d.out")
    assert "APPROX_QUANTILES(dist, 2)[OFFSET(1)] AS `dist`" in sql


def test_duckdb_median_uses_median_function():
    adapter = DuckDBAdapter()
    spec = PromotionSpec(
        name="t", direction=AGGREGATE_UP, target_key="tract_geoid",
        key_expr="SUBSTR(block_geoid, 1, 11)",
        aggregations=(Aggregation("dist", "median"),),
    )
    sql = render_sql(spec, adapter, "src", "out")
    assert 'MEDIAN(dist) AS "dist"' in sql


def test_duckdb_executes_fcc_speeds_aggregation():
    con = duckdb.connect()
    # Two blocks in tract 01001020100, one block in tract 01001020200.
    con.execute(
        """
        CREATE TABLE speeds_block AS
        SELECT * FROM (VALUES
            ('010010201001000', 10, 100.0, 20.0, 5,  50.0, 10.0, 2, 40.0, 8.0),
            ('010010201001001',  4, 250.0, 25.0, 3,  60.0, 12.0, 1, 90.0, 9.0),
            ('010010202001000',  7, 100.0, 20.0, 2, 100.0, 20.0, 4, 30.0, 6.0)
        ) AS t(block_geoid,
               cable_location_count, cable_max_download_speed, cable_max_upload_speed,
               copper_location_count, copper_max_download_speed, copper_max_upload_speed,
               fiber_location_count, fiber_max_download_speed, fiber_max_upload_speed)
        """
    )
    adapter = DuckDBAdapter(con)
    promote(FCC_SPEEDS_CT_SPEC, adapter, "speeds_block", "speeds_ct")

    rows = con.execute(
        "SELECT tract_geoid, cable_location_count, cable_max_download_speed, "
        "fiber_location_count FROM speeds_ct ORDER BY tract_geoid"
    ).fetchall()

    assert rows[0][0] == "01001020100"
    assert rows[0][1] == 14          # 10 + 4 summed
    assert rows[0][2] == 250.0       # max(100, 250)
    assert rows[0][3] == 3           # 2 + 1 fiber summed
    assert rows[1][0] == "01001020200"
    assert rows[1][1] == 7           # single block passes through


def test_spec_rejects_ambiguous_key_derivation():
    with pytest.raises(ValueError):
        PromotionSpec(
            name="bad", direction=AGGREGATE_UP, target_key="tract_geoid",
            key_expr="SUBSTR(block_geoid, 1, 11)",
            xwalk_source_key="parcel_shape_id", xwalk_join_key="parcel_shape_id",
            xwalk_target_expr="x.tract_geoid",
            aggregations=(Aggregation("n", "sum"),),
        )


def test_spec_rejects_unknown_aggregate_op():
    with pytest.raises(ValueError):
        Aggregation("n", "stddev")


def test_render_requires_crosswalk_table_when_spec_uses_one():
    spec = PromotionSpec(
        name="parcel_up", direction=AGGREGATE_UP, target_key="tract_geoid",
        xwalk_source_key="parcel_shape_id", xwalk_join_key="parcel_shape_id",
        xwalk_target_expr="x.tract_geoid",
        aggregations=(Aggregation("radius_fiber_count", "sum"),),
    )
    adapter = DuckDBAdapter()
    with pytest.raises(ValueError):
        render_sql(spec, adapter, "rextag_parcel", "rextag_ct")
    # With a crosswalk table it renders the join.
    sql = render_sql(spec, adapter, "rextag_parcel", "rextag_ct", xwalk_table="parcel_tract_xwalk")
    assert 'LEFT JOIN "parcel_tract_xwalk" AS x' in sql
    assert 'x.tract_geoid AS "tract_geoid"' in sql
