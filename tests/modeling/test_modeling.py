"""
Offline tests for the modeling module (train, fit_rules, registry).
==============================================================================
These tests run the real cluster → classify → SHAP pipeline on a tiny synthetic tract
frame (no BigQuery, no saved joblibs) and assert the shape of every artifact: that the
frame is renamed to canonical names and filled through the one scaling contract, that
KMeans discovers exactly ``k`` segments, that the SHAP array lines up with the feature
columns and the requested sample size, and that a frame missing a scoring feature fails
loudly. They exercise the fit_rules orchestration seam with a fake BigQuery client and
canned SHAP array, and cover the run-registry record shape and its delete-then-append
write against a fake client.
"""
import numpy as np
import pandas as pd
import pytest

from network_idx.constants import ALL_SCORING_FEATURES, SCORING_RUN_K
from network_idx.modeling.train import train, ModelArtifacts
from network_idx.modeling import fit_rules, registry


# ── synthetic training frame in MODEL feature-name space ──────────────────────
MODEL_COLS = [
    "median_landuse_change_qtr_mi_cnt",
    "median_pre_early_dev_qtr_mi_cnt",
    "median_bldr_dev_qtr_mi_cnt",
    "median_new_permit_qtr_mi_cnt",
    "median_dist_nearest_hotspot",
    "cable_penetration",
    "fiber_opportunity_gap",
    "fiber_speed_top_tier",
    "median_dist_nearest_fiber_m",
    "provider_competitive_landscape_ord",
    "pop_ch_avg",
    "pop_pctch_avg",
    "estimated_census_housing_units",
]


def _synthetic_frame(n=400, seed=0):
    rng = np.random.default_rng(seed)
    # two well-separated blobs so KMeans has real structure to find
    half = n // 2
    base = np.vstack([
        rng.normal(0.0, 1.0, size=(half, len(MODEL_COLS))),
        rng.normal(6.0, 1.0, size=(n - half, len(MODEL_COLS))),
    ])
    df = pd.DataFrame(np.abs(base), columns=MODEL_COLS)
    df["tract_geoid"] = [f"{i:011d}" for i in range(n)]
    df["estimated_fcc_units"] = rng.integers(1, 100, size=n)
    df["provider_competitive_landscape"] = "greenfield"
    df["cable_future_gap"] = rng.normal(size=n)
    # sprinkle some nulls the fills must clean up
    df.loc[df.index[:5], "cable_penetration"] = np.nan
    df.loc[df.index[:5], "median_dist_nearest_fiber_m"] = np.nan
    return df


def test_train_returns_wellshaped_artifacts():
    frame = _synthetic_frame()
    art = train(frame, k=4, shap_sample=50, random_state=42, params={"n_estimators": 40})

    assert art.k == 4
    assert art.feature_cols == list(ALL_SCORING_FEATURES)
    # one label per input row, exactly k distinct segments
    assert len(art.cluster_labels) == len(frame)
    assert len(set(art.cluster_labels)) == 4
    # SHAP array: (sample_rows, features, classes)
    assert art.shap_values.ndim == 3
    assert art.shap_values.shape[0] == len(art.x_shap)
    assert art.shap_values.shape[1] == len(ALL_SCORING_FEATURES)
    assert art.shap_values.shape[2] == 4
    # x_shap speaks canonical names and carries no nulls (fills applied)
    assert list(art.x_shap.columns) == list(ALL_SCORING_FEATURES)
    assert not art.x_shap.isna().any().any()


def test_train_shap_sample_capped_to_test_split():
    frame = _synthetic_frame(n=200)
    art = train(frame, k=3, shap_sample=10_000, random_state=1, params={"n_estimators": 40})
    # sample can't exceed the 20% test split
    assert len(art.x_shap) <= 0.2 * len(frame) + 1


def test_train_default_k_is_production_segment_count():
    frame = _synthetic_frame(n=600)
    art = train(frame, shap_sample=30, params={"n_estimators": 40})
    assert art.k == SCORING_RUN_K


def test_train_missing_feature_raises():
    frame = _synthetic_frame().drop(columns=["fiber_opportunity_gap"])
    with pytest.raises(KeyError, match="missing scoring features"):
        train(frame, k=3, shap_sample=20, params={"n_estimators": 40})


# ── fit_rules orchestration seam ──────────────────────────────────────────────
class _FakeJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class _FakeScalingClient:
    """Returns a single aggregate row so compute_scaling_params_bq can assemble params."""

    def __init__(self):
        self.queries = []

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        # one row with every __min/__max/__winmax/__rawmax/__p99 alias the query asks for
        row = {}
        for f in ALL_SCORING_FEATURES:
            row[f"{f}__min"] = 0.0
            row[f"{f}__max"] = 10.0
            row[f"{f}__winmax"] = 9.0
            row[f"{f}__rawmax"] = 10.0
            row[f"{f}__p99"] = 8.0
        return _FakeJob([row])


def _canned_shap():
    frame = _synthetic_frame(n=120)
    art = train(frame, k=3, shap_sample=20, random_state=7, params={"n_estimators": 40})
    return art.shap_values, art.x_shap


def test_fit_scoring_rules_returns_both_artifacts():
    shap_values, x_shap = _canned_shap()
    client = _FakeScalingClient()
    out = fit_rules.fit_scoring_rules(
        shap_values, x_shap, "test_run",
        client=client, source_table="proj.ds.parcel_features",
        strict=False,
    )
    assert set(out) == {"weights", "scaling_params"}
    assert set(out["weights"]["feature"]) == set(ALL_SCORING_FEATURES)
    assert set(out["scaling_params"]["feature"]) == set(ALL_SCORING_FEATURES)
    assert (out["scaling_params"]["run_id"] == "test_run").all()
    assert "proj.ds.parcel_features" in client.queries[0]


# ── registry ──────────────────────────────────────────────────────────────────
def test_build_run_record_shape_and_defaults():
    rec = registry.build_run_record("lightgbm_k8_v1")
    assert list(rec.columns) == registry.SCORING_RUNS_COLUMNS
    assert len(rec) == 1
    assert rec["run_id"].iloc[0] == "lightgbm_k8_v1"
    assert rec["n_features"].iloc[0] == len(ALL_SCORING_FEATURES)
    assert rec["feature_weights_table"].iloc[0].endswith("feature_weights")
    assert rec["scaling_params_table"].iloc[0].endswith("scaling_params")


class _FakeRegistryClient:
    def __init__(self):
        self.deleted = []
        self.loaded = []

    def query(self, sql, job_config=None):
        self.deleted.append(sql)
        return _FakeJob(None)

    def load_table_from_dataframe(self, df, table_id, job_config=None):
        self.loaded.append((df, table_id))
        return _FakeJob(None)


def test_write_run_deletes_then_appends():
    client = _FakeRegistryClient()
    rec = registry.build_run_record("run_x")
    registry.write_run(client, rec, table_id="proj.ds.scoring_runs")
    assert any("DELETE FROM `proj.ds.scoring_runs`" in q for q in client.deleted)
    assert client.loaded and client.loaded[0][1] == "proj.ds.scoring_runs"
