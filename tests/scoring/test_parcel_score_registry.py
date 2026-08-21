"""
Offline tests for scoring's run-registry resolution.
==============================================================================
Scoring resolves where a run's rules live from the run registry rather than assuming
the configured defaults. These tests confirm that when the registry has a row for the
run, scoring uses that row's scaling and weights table refs, and that when the run is
absent (or the registry is unreadable) scoring falls back to the configured analytics
tables so it stays runnable. A fake client returns canned registry frames.
"""
import pandas as pd

from network_idx.scoring import parcel_score


class _Job:
    def __init__(self, df):
        self._df = df

    def to_dataframe(self):
        return self._df

    def result(self):
        return None


class _FakeClient:
    def __init__(self, registry_df=None, raise_on_query=False):
        self.registry_df = registry_df
        self.raise_on_query = raise_on_query

    def query(self, sql, job_config=None):
        if self.raise_on_query:
            raise RuntimeError("registry table not found")
        return _Job(self.registry_df)


def test_resolve_uses_registry_row_when_present():
    reg = pd.DataFrame([{
        "scaling_params_table": "proj.ds.custom_scaling",
        "feature_weights_table": "proj.ds.custom_weights",
    }])
    scaling, weights = parcel_score.resolve_artifact_tables(_FakeClient(reg), "run_a")
    assert scaling == "proj.ds.custom_scaling"
    assert weights == "proj.ds.custom_weights"


def test_resolve_falls_back_when_run_absent():
    empty = pd.DataFrame(columns=["scaling_params_table", "feature_weights_table"])
    scaling, weights = parcel_score.resolve_artifact_tables(_FakeClient(empty), "run_missing")
    assert scaling.endswith("scaling_params")
    assert weights.endswith("feature_weights")


def test_resolve_falls_back_when_registry_unreadable():
    scaling, weights = parcel_score.resolve_artifact_tables(
        _FakeClient(raise_on_query=True), "run_a"
    )
    assert scaling.endswith("scaling_params")
    assert weights.endswith("feature_weights")
