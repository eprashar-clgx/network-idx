"""
Offline tests for the raw-source interface.

These tests exercise `network_idx.sources.get_raw` and the BigQuery read adapter
without any network access. A fake BigQuery client records the SQL it is asked to
run and returns a canned DataFrame, so the tests assert on the query the adapter
builds and on the dispatch behaviour for each kind of source.
"""
import pandas as pd
import pytest

from network_idx import sources
from network_idx.sources import bq_prod
from network_idx.sources.registry import BQSource, RAW_SOURCES_BQ


class _FakeQueryJob:
    def __init__(self, frame):
        self._frame = frame

    def to_dataframe(self):
        return self._frame


class FakeClient:
    """Records the SQL passed to query() and returns a canned DataFrame."""

    def __init__(self, frame=None):
        self.frame = frame if frame is not None else pd.DataFrame({"x": [1]})
        self.last_sql = None

    def query(self, sql):
        self.last_sql = sql
        return _FakeQueryJob(self.frame)


def test_build_select_sql_all_columns():
    source = BQSource("proj", "ds", "tbl")
    assert bq_prod.build_select_sql(source) == "SELECT * FROM `proj.ds.tbl`"


def test_build_select_sql_columns_and_limit():
    source = BQSource("proj", "ds", "tbl")
    sql = bq_prod.build_select_sql(source, columns=["a", "b"], limit=10)
    assert sql == "SELECT a, b FROM `proj.ds.tbl` LIMIT 10"


def test_get_raw_bq_source_runs_expected_query():
    client = FakeClient(pd.DataFrame({"a": [1, 2]}))
    frame = sources.get_raw("fcc_copper", client=client)

    expected_ref = RAW_SOURCES_BQ["fcc_copper"].table_ref
    assert client.last_sql == f"SELECT * FROM `{expected_ref}`"
    assert list(frame["a"]) == [1, 2]


def test_get_raw_passes_columns_and_limit():
    client = FakeClient()
    sources.get_raw("fcc_fiber", client=client, columns=["block_geoid"], limit=5)

    expected_ref = RAW_SOURCES_BQ["fcc_fiber"].table_ref
    assert client.last_sql == f"SELECT block_geoid FROM `{expected_ref}` LIMIT 5"


def test_get_raw_bq_source_without_client_raises():
    with pytest.raises(ValueError, match="requires a BigQuery client"):
        sources.get_raw("neighborhood_scout_tract")


def test_get_raw_download_source_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="census_baf"):
        sources.get_raw("census_baf")


def test_get_raw_unknown_source_raises_key_error():
    with pytest.raises(KeyError, match="Unknown source"):
        sources.get_raw("not_a_real_source")
