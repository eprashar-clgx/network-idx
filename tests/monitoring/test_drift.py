"""
Offline tests for drift monitoring against a frozen baseline.

These tests exercise the pure drift comparison and the baseline persist/read dispatch
without any BigQuery access. They assert that a stable feature is not flagged, that a mean
shift, a null-rate move, and a shape shift are each caught, that features appearing or
disappearing between runs are flagged, and that writing a baseline stamps the run and kind
and reading it drops the bookkeeping columns. A fake client records the SQL and load calls
and returns a canned baseline frame.
"""
import pandas as pd

from network_idx.monitoring import drift as d


def _snapshot_row(feature, mean=0.5, null_rate=0.0, **over):
    base = dict(
        feature=feature,
        n=100, n_null=0, null_rate=null_rate, n_zero=10, zero_rate=0.1,
        mean=mean, stddev=0.2, min=0.0,
        p01=0.0, p05=0.05, p10=0.1, p25=0.25, p50=0.5, p75=0.75, p90=0.9, p95=0.95, p99=1.0,
        max=1.0,
    )
    base.update(over)
    return base


def test_stable_feature_not_flagged():
    frame = pd.DataFrame([_snapshot_row("a")])
    report = d.compute_drift(frame, frame)
    assert report.passed
    assert report.flags == {}
    assert report.n_features == 1


def test_mean_shift_is_flagged():
    base = pd.DataFrame([_snapshot_row("a", mean=0.5)])
    cur = pd.DataFrame([_snapshot_row("a", mean=1.0)])  # 100% relative move
    report = d.compute_drift(cur, base)
    assert not report.passed
    assert "mean_shift" in report.flags["a"]


def test_null_rate_move_is_flagged():
    base = pd.DataFrame([_snapshot_row("a", null_rate=0.0)])
    cur = pd.DataFrame([_snapshot_row("a", null_rate=0.2)])
    report = d.compute_drift(cur, base)
    assert "null_shift" in report.flags["a"]


def test_shape_shift_is_flagged():
    base = pd.DataFrame([_snapshot_row("a")])
    # Move the median far relative to the p01..p99 spread of 1.0.
    cur = pd.DataFrame([_snapshot_row("a", p50=0.95)])
    report = d.compute_drift(cur, base)
    assert "dist_shift" in report.flags["a"]


def test_appearing_and_disappearing_features_flagged():
    base = pd.DataFrame([_snapshot_row("only_base")])
    cur = pd.DataFrame([_snapshot_row("only_cur")])
    report = d.compute_drift(cur, base)
    assert report.flags["only_cur"] == ["missing_baseline"]
    assert report.flags["only_base"] == ["dropped"]
    assert not report.passed


class _FakeQueryJob:
    def __init__(self, df=None):
        self._df = df if df is not None else pd.DataFrame()

    def result(self):
        return None

    def to_dataframe(self):
        return self._df


class FakeClient:
    def __init__(self, read_df=None):
        self.queries = []
        self.loads = []
        self._read_df = read_df

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        if sql.strip().upper().startswith("SELECT"):
            return _FakeQueryJob(self._read_df)
        return _FakeQueryJob()

    def load_table_from_dataframe(self, df, table, job_config=None):
        self.loads.append((df, table))
        return _FakeQueryJob()


def test_write_baseline_stamps_run_and_kind():
    client = FakeClient()
    frame = pd.DataFrame([_snapshot_row("a"), _snapshot_row("b")])
    d.write_baseline(client, frame, "run_ref", d.SNAPSHOT_FEATURE, table_ref="p.d.baseline")
    assert len(client.loads) == 1
    loaded, table = client.loads[0]
    assert table == "p.d.baseline"
    assert list(loaded["run_id"].unique()) == ["run_ref"]
    assert list(loaded["snapshot_kind"].unique()) == ["feature"]
    # Delete-before-append keyed by run happened.
    assert any("DELETE" in q.upper() for q in client.queries)


def test_read_baseline_drops_bookkeeping_columns():
    stored = pd.DataFrame([{**{"run_id": "run_ref", "snapshot_kind": "feature"}, **_snapshot_row("a")}])
    client = FakeClient(read_df=stored)
    out = d.read_baseline(client, "run_ref", d.SNAPSHOT_FEATURE, table_ref="p.d.baseline")
    assert "run_id" not in out.columns and "snapshot_kind" not in out.columns
    assert "feature" in out.columns


def test_run_drift_reports_missing_baseline():
    client = FakeClient(read_df=pd.DataFrame())
    report = d.run_drift(client, pd.DataFrame([_snapshot_row("a")]), "absent", table_ref="p.d.baseline")
    assert not report.passed
    assert report.flags == {"__baseline__": ["no_baseline"]}
