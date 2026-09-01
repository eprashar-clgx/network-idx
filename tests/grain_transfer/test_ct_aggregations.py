"""
Offline tests for the bespoke parcel-to-tract grain-transfer aggregations.

These tests exercise the SQL rendering and the build dispatch of the two spatial
aggregations (location growth and rextag fiber distance) without any BigQuery access. A
fake client records the SQL it is asked to run, so the tests assert that the rendered
query references the parcel, tract-boundary, and output tables, performs the spatial join
and per-parcel dedup, emits the aggregated columns the modeling frame expects, and leaves
no unresolved placeholders, and that a dry run builds the SQL without executing it.
"""
from network_idx.grain_transfer import location_growth_ct, rextag_distance_ct


class _FakeQueryJob:
    def result(self):
        return None


class FakeClient:
    def __init__(self):
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return _FakeQueryJob()


# Location growth tract aggregation


def _render_location():
    return location_growth_ct.render_sql(
        output_table="proj.ds.loc_ct",
        growth_counts_parcel="proj.ds.growth_parcel",
        hotspot_distance_parcel="proj.ds.hotspot_parcel",
        tract_boundary="prod.ref.tract_geom",
    )


def test_location_render_references_all_tables():
    sql = _render_location()
    assert "`proj.ds.loc_ct`" in sql
    assert "`proj.ds.growth_parcel`" in sql
    assert "`proj.ds.hotspot_parcel`" in sql
    assert "`prod.ref.tract_geom`" in sql
    assert "{" not in sql and "}" not in sql


def test_location_render_has_spatial_join_and_dedup():
    sql = _render_location()
    assert "ST_INTERSECTS(p.parcel_centroid, ct.geometry)" in sql
    assert "ROW_NUMBER() OVER (PARTITION BY p.parcel_shape_id ORDER BY ct.geoid) = 1" in sql


def test_location_render_emits_expected_aggregates():
    sql = _render_location()
    for col in (
        "total_parcels",
        "growth_parcels",
        "pre_early_dev_parcels",
        "unique_locations",
        "median_bldr_dev_qtr_mi_cnt",
        "total_flags",
        "flags_minus_greatest",
        "mean_dist_nearest_hotspot_miles",
        "median_dist_nearest_hotspot",
    ):
        assert col in sql


def test_location_dry_run_does_not_execute():
    client = FakeClient()
    location_growth_ct.build(client=client, dry_run=True)
    assert client.queries == []


def test_location_build_executes_once():
    client = FakeClient()
    location_growth_ct.build(client=client, dry_run=False)
    assert len(client.queries) == 1
    assert "CREATE OR REPLACE TABLE" in client.queries[0]


# Rextag fiber distance tract aggregation


def _render_rextag():
    return rextag_distance_ct.render_sql(
        output_table="proj.ds.rextag_ct",
        growth_counts_parcel="proj.ds.growth_parcel",
        rextag_distance_parcel="proj.ds.rextag_parcel",
        tract_boundary="prod.ref.tract_geom",
    )


def test_rextag_render_references_all_tables():
    sql = _render_rextag()
    assert "`proj.ds.rextag_ct`" in sql
    assert "`proj.ds.growth_parcel`" in sql
    assert "`proj.ds.rextag_parcel`" in sql
    assert "`prod.ref.tract_geom`" in sql
    assert "{" not in sql and "}" not in sql


def test_rextag_render_emits_expected_aggregates():
    sql = _render_rextag()
    for col in (
        "total_growth_parcels",
        "mean_dist_nearest_fiber_miles",
        "median_dist_nearest_fiber_miles",
        "mean_radius_fiber_count",
        "median_radius_fiber_count",
    ):
        assert col in sql


def test_rextag_build_executes_once():
    client = FakeClient()
    rextag_distance_ct.build(client=client, dry_run=False)
    assert len(client.queries) == 1
    assert "CREATE OR REPLACE TABLE" in client.queries[0]
