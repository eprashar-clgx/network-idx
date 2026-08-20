"""
Offline tests for the one-time Census block reference uploader.

These tests exercise target resolution, schema validation, and the load dispatch without
any BigQuery access. A fake client records the load jobs it is asked to run so the tests
assert that the first file replaces the table and the rest append, that schema validation
accepts a well-formed parquet and rejects one missing an expected column, that targets
resolve to the configured tables, and that the registry exposes the same tables.
"""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from google.cloud import bigquery

from network_idx.constants import CENSUS_BAF_OUTPUTS, CENSUS_ACL_OUTPUTS
from network_idx.processing import census_blocks_to_bq as uploader
from network_idx.sources.registry import RAW_SOURCES_BQ


class _FakeLoadJob:
    def result(self):
        return None


class FakeClient:
    def __init__(self):
        self.loads = []

    def load_table_from_file(self, f, table_id, job_config=None):
        self.loads.append((table_id, job_config.write_disposition))
        return _FakeLoadJob()


def _write_parquet(path, columns):
    table = pa.table({col: pa.array([], type=pa.string()) for col in columns})
    pq.write_table(table, path)


def test_resolve_target_table_ids():
    assert uploader.resolve_target("baf")[3].endswith("census_baf_block")
    assert uploader.resolve_target("acl")[3].endswith("census_acl_block")


def test_resolve_target_rejects_unknown():
    with pytest.raises(ValueError):
        uploader.resolve_target("nope")


def test_registry_exposes_block_tables():
    assert RAW_SOURCES_BQ["census_baf_block"].table_ref.endswith("census_baf_block")
    assert RAW_SOURCES_BQ["census_acl_block"].table_ref.endswith("census_acl_block")


def test_validate_schema_accepts_expected_columns(tmp_path):
    f = tmp_path / "census_baf_national.parquet"
    _write_parquet(f, CENSUS_BAF_OUTPUTS)
    assert uploader.validate_schema([f], CENSUS_BAF_OUTPUTS) is True


def test_validate_schema_rejects_missing_column(tmp_path):
    f = tmp_path / "census_acl_bad.parquet"
    _write_parquet(f, CENSUS_ACL_OUTPUTS[:-1])  # drop one expected column
    assert uploader.validate_schema([f], CENSUS_ACL_OUTPUTS) is False


def test_load_first_replaces_then_appends(tmp_path):
    files = []
    for i in range(3):
        f = tmp_path / f"census_acl_{i}.parquet"
        _write_parquet(f, CENSUS_ACL_OUTPUTS)
        files.append(f)

    client = FakeClient()
    for i, f in enumerate(files):
        uploader.load_parquet_to_bq(f, "proj.ds.census_acl_block", client, replace=(i == 0))

    assert len(client.loads) == 3
    assert client.loads[0][1] == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert client.loads[1][1] == bigquery.WriteDisposition.WRITE_APPEND
    assert client.loads[2][1] == bigquery.WriteDisposition.WRITE_APPEND
    assert all(table_id == "proj.ds.census_acl_block" for table_id, _ in client.loads)
