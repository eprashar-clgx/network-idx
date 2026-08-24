"""
Offline tests for train/scoring parity monitoring.

These tests exercise the parity query rendering and the pure flagging logic without any
BigQuery access. They assert that the query injects each feature's fitted bounds and
measures clip saturation and null rate over the parcel feature table, and that the flag
check catches a feature whose scored population saturates the fitted cap or is mostly null
while passing a feature that sits comfortably inside its bounds. A fake client returns the
scaling parameters and a canned parity frame so the run dispatch can be tested offline.
"""
import pandas as pd

from network_idx.monitoring import parity as p


def _params():
    return pd.DataFrame(
        [
            {"run_id": "r", "feature": "a", "min_val": 0.0, "max_val": 10.0, "invert": False},
            {"run_id": "r", "feature": "b", "min_val": 0.0, "max_val": 1.0, "invert": False},
        ]
    )


def test_render_parity_sql_injects_bounds_and_measures():
    sql = p.render_parity_sql("proj.ds.parcel_features", _params())
    assert "'a' AS feature" in sql and "'b' AS feature" in sql
    assert "`proj.ds.parcel_features`" in sql
    assert "COUNTIF(a < 0.0)" in sql
    assert "COUNTIF(a > 10.0)" in sql
    assert "clip_low_rate" in sql and "clip_high_rate" in sql and "null_rate" in sql
    assert "{" not in sql and "}" not in sql


def _parity_row(feature, null_rate=0.0, clip_low_rate=0.0, clip_high_rate=0.0):
    return dict(
        feature=feature, min_val=0.0, max_val=1.0, n=100, n_null=0,
        null_rate=null_rate, clip_low_rate=clip_low_rate, clip_high_rate=clip_high_rate,
    )


def test_flag_parity_passes_healthy_feature():
    df = pd.DataFrame([_parity_row("a", clip_low_rate=0.01, clip_high_rate=0.01)])
    report = p.flag_parity(df)
    assert report.passed
    assert report.flags == {}


def test_flag_parity_catches_clip_saturation():
    df = pd.DataFrame([_parity_row("a", clip_high_rate=0.2)])
    report = p.flag_parity(df)
    assert "clip_saturation" in report.flags["a"]


def test_flag_parity_catches_high_null():
    df = pd.DataFrame([_parity_row("a", null_rate=0.5)])
    report = p.flag_parity(df)
    assert "high_null" in report.flags["a"]


class _FakeQueryJob:
    def __init__(self, df):
        self._df = df

    def to_dataframe(self):
        return self._df


class FakeClient:
    def __init__(self, params_df, parity_df):
        self._params = params_df
        self._parity = parity_df
        self.queries = []

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        # The scaling-params read is a parameterized SELECT *, the parity query is a UNION.
        if "UNION ALL" in sql or "clip_low_rate" in sql:
            return _FakeQueryJob(self._parity)
        return _FakeQueryJob(self._params)


def test_run_reads_params_then_flags_parity():
    parity_df = pd.DataFrame([_parity_row("a", clip_high_rate=0.3)])
    client = FakeClient(_params(), parity_df)
    report = p.run("r", client=client)
    assert not report.passed
    assert "clip_saturation" in report.flags["a"]
    assert len(client.queries) == 2
