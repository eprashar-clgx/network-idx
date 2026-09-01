"""
Offline tests for the tract-grain training-frame assembly (features_ct).

These exercise the SQL rendering and build dispatch of ``features_ct`` without any BigQuery
access. A fake client records the SQL it is asked to run, so the tests assert the assembly
references every family's tract table, left-joins the four families on the tract GEOID, and
emits exactly the model-named columns that ``MODEL_TO_SCORING_FEATURE`` renames to the full
thirteen-feature scoring contract — the train/score parity guarantee — and that a dry run
builds the SQL without executing it.
"""
from network_idx.constants.scoring_contract import (
    ALL_SCORING_FEATURES,
    MODEL_TO_SCORING_FEATURE,
)
from network_idx.grain_transfer import features_ct


class _FakeQueryJob:
    def result(self):
        return None


class FakeClient:
    def __init__(self):
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return _FakeQueryJob()


# Every model-named column the assembly is expected to emit (keys of the rename map that
# are relevant here) plus the identity-named features that need no rename.
_EXPECTED_MODEL_COLUMNS = [
    "median_landuse_change_qtr_mi_cnt",
    "median_pre_early_dev_qtr_mi_cnt",
    "median_bldr_dev_qtr_mi_cnt",
    "median_new_permit_qtr_mi_cnt",
    "median_dist_nearest_hotspot",
    "median_dist_nearest_fiber_miles",
    "cable_penetration",
    "fiber_opportunity_gap",
    "fiber_speed_top_tier",
    "provider_competitive_landscape_ord",
    "pop_ch_avg",
    "pop_pctch_avg",
    "estimated_census_housing_units",
]


def _render():
    return features_ct.render_sql(
        output_table="proj.ds.features_ct",
        telecom_features_ct="proj.ds.telecom_features_ct",
        loc_growth_ct="proj.ds.loc_growth_ct",
        rextag_distance_ct="proj.ds.rextag_distance_ct",
        demo_pop_ct="proj.ds.demo_pop_ct",
    )


def test_render_references_all_tables():
    sql = _render()
    assert "`proj.ds.features_ct`" in sql
    assert "`proj.ds.telecom_features_ct`" in sql
    assert "`proj.ds.loc_growth_ct`" in sql
    assert "`proj.ds.rextag_distance_ct`" in sql
    assert "`proj.ds.demo_pop_ct`" in sql
    assert "{" not in sql and "}" not in sql


def test_render_left_joins_four_families_on_tract():
    sql = _render()
    assert sql.count("LEFT JOIN") == 3  # telecom is the spine; three families joined
    assert "t.tract_geoid = g.tract_id" in sql
    assert "t.tract_geoid = rf.tract_id" in sql
    assert "t.tract_geoid = d.tract_geoid" in sql


def test_render_emits_every_model_named_column():
    sql = _render()
    for col in _EXPECTED_MODEL_COLUMNS:
        assert col in sql, f"missing model column {col}"


def test_emitted_columns_cover_the_full_scoring_contract_after_rename():
    """The renamed model columns must produce all thirteen scoring features."""
    renamed = {MODEL_TO_SCORING_FEATURE.get(c, c) for c in _EXPECTED_MODEL_COLUMNS}
    missing = [f for f in ALL_SCORING_FEATURES if f not in renamed]
    assert not missing, f"scoring features not covered by the CT frame: {missing}"


def test_dry_run_does_not_execute():
    client = FakeClient()
    features_ct.build(client=client, dry_run=True)
    assert client.queries == []


def test_build_executes_once():
    client = FakeClient()
    features_ct.build(client=client, dry_run=False)
    assert len(client.queries) == 1
    assert "CREATE OR REPLACE TABLE" in client.queries[0]
