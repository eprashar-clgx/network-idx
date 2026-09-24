"""
Offline tests for cluster_metrics, artifacts, and the run_training driver.
==============================================================================
These cover the three modules added alongside the ``train``/``fit_rules`` pair:

    * ``cluster_metrics`` — that each metric is computed over the population it is
      given, that well-separated blobs score better than overlapping ones (so the
      metrics actually measure separation rather than returning a constant), that
      cluster sizes are JSON-friendly and account for every row, and that the elbow
      helper refits across the requested k range;
    * ``artifacts`` — that the payload carries every field the registry and a future
      reader depend on, and that it survives a JSON round trip. The round trip is the
      real test: the payload mixes numpy scalars, numpy arrays, and pandas Timestamps
      that ``json.dumps`` cannot serialize natively, so a passing round trip proves
      the ``_json_default`` hook covers what the frames actually produce;
    * ``run_training`` — that the driver wires the pieces together in the right order
      against a fake BigQuery client: that a dry run writes nothing at all, and that a
      real run writes weights, scaling params, and the registry row (delete-then-append
      each) plus the artifact file.

Everything runs offline on a small synthetic frame — no BigQuery, no credentials.
Tests pass explicit ``path=`` arguments rather than reassigning ``ARTIFACTS_DIR_RUNS``,
because ``artifact_path`` binds that constant as a default argument at definition
time and rebinding the module global would not take effect.
"""
import json

import numpy as np
import pandas as pd
import pytest

from network_idx.constants import ALL_SCORING_FEATURES
from network_idx.modeling import artifacts as art
from network_idx.modeling import cluster_metrics as cm
from network_idx.modeling import run_training
from network_idx.modeling.train import train

from .test_modeling import _synthetic_frame


# ── fakes ─────────────────────────────────────────────────────────────────────
class _FakeJob:
    def __init__(self, rows=None, df=None):
        self._rows = rows or []
        self._df = df

    def result(self):
        return self._rows

    def to_dataframe(self):
        return self._df


class _FakeTrainingClient:
    """Serves the training frame for the ``SELECT *`` load and a canned stats row for
    the scaling scan, and records every query and write so the driver's effects are
    assertable without touching BigQuery."""

    def __init__(self, frame):
        self.frame = frame
        self.queries = []
        self.loaded = []

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        if sql.strip().upper().startswith("SELECT *"):
            return _FakeJob(df=self.frame)
        if sql.strip().upper().startswith("DELETE"):
            return _FakeJob()
        row = {}
        for f in ALL_SCORING_FEATURES:
            for suffix, val in [("min", 0.0), ("max", 10.0), ("rawmax", 10.0),
                                ("p99", 9.0), ("winmax", 9.5)]:
                row[f"{f}__{suffix}"] = val
        return _FakeJob(rows=[row])

    def load_table_from_dataframe(self, df, table_id, job_config=None):
        self.loaded.append((table_id, len(df)))
        return _FakeJob()


def _blobs(n=300, spread=6.0, dims=4, seed=0):
    """Two Gaussian blobs ``spread`` apart plus their KMeans fit — the fixture the
    cluster metrics are asserted against."""
    from sklearn.cluster import KMeans

    rng = np.random.default_rng(seed)
    half = n // 2
    x = np.vstack([
        rng.normal(0.0, 1.0, size=(half, dims)),
        rng.normal(spread, 1.0, size=(n - half, dims)),
    ])
    km = KMeans(n_clusters=2, random_state=42, n_init=10).fit(x)
    return x, km.labels_, km


# ── cluster_metrics ───────────────────────────────────────────────────────────
def test_cluster_sizes_counts_every_row_and_is_json_friendly():
    labels = np.array([0, 0, 1, 2, 2, 2])
    sizes = cm.cluster_sizes(labels)
    assert sizes == {"0": 2, "1": 1, "2": 3}
    assert sum(sizes.values()) == len(labels)
    # string keys and plain ints, so the dict drops straight into the run artifact
    json.dumps(sizes)


def test_inertia_matches_the_fitted_model():
    _, _, km = _blobs()
    assert cm.inertia(km) == pytest.approx(float(km.inertia_))


def test_separation_metrics_prefer_well_separated_blobs():
    x_far, labels_far, _ = _blobs(spread=10.0, seed=1)
    x_near, labels_near, _ = _blobs(spread=0.5, seed=1)

    # silhouette and Calinski-Harabasz: higher is better; Davies-Bouldin: lower is
    assert cm.silhouette(x_far, labels_far) > cm.silhouette(x_near, labels_near)
    assert cm.calinski_harabasz(x_far, labels_far) > cm.calinski_harabasz(x_near, labels_near)
    assert cm.davies_bouldin(x_far, labels_far) < cm.davies_bouldin(x_near, labels_near)


def test_silhouette_samples_when_population_exceeds_sample_size():
    x, labels, _ = _blobs(n=400)
    # a sample far smaller than the population must still return a valid coefficient
    score = cm.silhouette(x, labels, sample_size=50)
    assert -1.0 <= score <= 1.0


def test_inertia_by_k_covers_the_requested_range_and_decreases():
    x, _, _ = _blobs(n=200)
    curve = cm.inertia_by_k(x, k_range=range(2, 6))
    assert list(curve.keys()) == ["2", "3", "4", "5"]
    values = list(curve.values())
    # inertia is monotonically non-increasing in k by construction
    assert all(a >= b for a, b in zip(values, values[1:]))


def test_compute_cluster_metrics_returns_every_metric():
    x, labels, km = _blobs()
    metrics = cm.compute_cluster_metrics(x, labels, km)
    assert set(metrics) == {
        "inertia", "cluster_sizes", "silhouette", "davies_bouldin", "calinski_harabasz",
    }
    json.dumps(metrics)  # the artifact stores this dict verbatim


# ── artifacts ─────────────────────────────────────────────────────────────────
def _artifact_inputs():
    frame = _synthetic_frame(n=200)
    model = train(frame, k=3, shap_sample=20)
    n = len(ALL_SCORING_FEATURES)
    weights = pd.DataFrame({
        "run_id": ["r"] * n,
        "feature": ALL_SCORING_FEATURES,
        "bucket": ["growth"] * n,
        "mean_abs_shap": np.linspace(0.1, 1.0, n),
        "weight_overall": np.full(n, 1 / n),
        "weight_in_bucket": np.full(n, 1 / n),
        "bucket_weight": np.ones(n),
        "created_at": pd.Timestamp.now(tz="UTC"),
    })
    scaling = pd.DataFrame({
        "run_id": ["r"] * n,
        "feature": ALL_SCORING_FEATURES,
        "na_fill": np.zeros(n),
        "min_val": np.zeros(n),
        "max_val": np.ones(n),
        # bool and Timestamp columns are exactly what json.dumps cannot take natively
        "invert": [i % 2 == 0 for i in range(n)],
        "created_at": pd.Timestamp.now(tz="UTC"),
    })
    return model, weights, scaling


def _build(model, weights, scaling, run_id="r"):
    return art.build_run_artifact(
        run_id=run_id, model="lightgbm", k=model.k, version="v2",
        code_version="abc123", training_data_table="proj.ds.features_ct",
        training_data_rows=200, hyperparams=model.params,
        fit_metrics={"kmeans": {"inertia": 1.0}, "classifier": {"accuracy": 0.9}},
        feature_cols=model.feature_cols, scaler=model.scaler,
        cluster_centers=model.kmeans.cluster_centers_,
        feature_weights=weights, scaling_params=scaling,
    )


def test_build_run_artifact_carries_every_expected_field():
    model, weights, scaling = _artifact_inputs()
    payload = _build(model, weights, scaling)

    assert set(payload) == {
        "run_id", "model", "k", "version", "code_version", "training_data_table",
        "training_data_rows", "created_at", "hyperparams", "fit_metrics",
        "feature_cols", "scaler", "cluster_centers", "feature_weights", "scaling_params",
    }
    assert payload["feature_cols"] == list(ALL_SCORING_FEATURES)
    # the scaler is reduced to the two vectors needed to re-scale, not pickled whole
    assert len(payload["scaler"]["mean_"]) == len(ALL_SCORING_FEATURES)
    assert len(payload["scaler"]["scale_"]) == len(ALL_SCORING_FEATURES)
    # one centre per segment, in the scaled feature space
    assert len(payload["cluster_centers"]) == model.k
    assert len(payload["cluster_centers"][0]) == len(ALL_SCORING_FEATURES)
    assert len(payload["feature_weights"]) == len(ALL_SCORING_FEATURES)
    assert len(payload["scaling_params"]) == len(ALL_SCORING_FEATURES)


def test_run_artifact_round_trips_through_json(tmp_path):
    model, weights, scaling = _artifact_inputs()
    payload = _build(model, weights, scaling)

    path = art.write_run_artifact(payload, path=tmp_path / "r.json")
    assert path.exists()

    back = art.read_run_artifact("r", path=path)
    assert back["run_id"] == "r"
    assert back["code_version"] == "abc123"
    assert back["feature_cols"] == payload["feature_cols"]
    assert back["cluster_centers"] == payload["cluster_centers"]
    # the bool column survived serialization as a real JSON bool
    assert isinstance(back["scaling_params"][0]["invert"], bool)


def test_write_run_artifact_creates_missing_parent_directories(tmp_path):
    model, weights, scaling = _artifact_inputs()
    payload = _build(model, weights, scaling)
    path = art.write_run_artifact(payload, path=tmp_path / "deep" / "nested" / "r.json")
    assert path.exists()


def test_artifact_path_defaults_to_the_runs_directory():
    assert art.artifact_path("lightgbm_k8_v2").name == "lightgbm_k8_v2.json"
    assert art.artifact_path("r", base_dir="/tmp/x").as_posix() == "/tmp/x/r.json"


def test_json_default_rejects_genuinely_unserializable_objects():
    with pytest.raises(TypeError):
        art._json_default(object())


# ── run_training ──────────────────────────────────────────────────────────────
def test_dry_run_writes_nothing_but_still_produces_the_payload():
    client = _FakeTrainingClient(_synthetic_frame(n=200))
    result = run_training.run(run_id="t", k=3, version="v2", client=client, dry_run=True)

    assert client.loaded == []
    assert "artifact_file" not in result
    assert len(result["rules"]["weights"]) == len(ALL_SCORING_FEATURES)
    assert len(result["rules"]["scaling_params"]) == len(ALL_SCORING_FEATURES)
    assert set(result["payload"]["fit_metrics"]) == {"kmeans", "classifier"}


def test_run_writes_rules_registry_and_artifact(tmp_path, monkeypatch):
    client = _FakeTrainingClient(_synthetic_frame(n=200))
    # intercept at the function the driver calls: artifact_path binds its default at
    # def time, so rebinding ARTIFACTS_DIR_RUNS would not redirect the write
    monkeypatch.setattr(
        run_training, "write_run_artifact",
        lambda payload: art.write_run_artifact(
            payload, path=tmp_path / f"{payload['run_id']}.json"
        ),
    )
    result = run_training.run(run_id="t", k=3, version="v2", client=client)

    written = [t.split(".")[-1] for t, _ in client.loaded]
    assert written == ["feature_weights", "scaling_params", "scoring_runs"]
    # every rule table is replaced for this run rather than appended blindly
    assert sum(1 for q in client.queries if q.strip().upper().startswith("DELETE")) == 3
    assert result["artifact_file"].exists()


def test_run_is_fit_on_the_tract_grain_frame():
    client = _FakeTrainingClient(_synthetic_frame(n=200))
    run_training.run(run_id="t", k=3, client=client, dry_run=True)

    load_query = next(q for q in client.queries if q.strip().upper().startswith("SELECT *"))
    assert "features_ct" in load_query
    # the scaling scan reads the parcel population, not the training frame
    scan_query = next(q for q in client.queries if "APPROX_QUANTILES" in q)
    assert "parcel_features" in scan_query
