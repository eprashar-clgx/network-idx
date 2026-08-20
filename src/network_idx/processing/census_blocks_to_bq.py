"""
One-time upload of the Census block reference tables to BigQuery.

The dasymetric FCC-coverage interpolation runs in BigQuery and needs two Census
block-grain tables there: the block-assignment crosswalk (block to county, tract, and
place) and the address-count listing (housing units per block). Those tables are produced
locally by the processing stage from Census downloads; this module lands them in BigQuery
so the SQL interpolation can join to them.

It is a temporary bridge, not a steady-state pipeline step: it exists only until Data
Engineering persists these tables in the production project, after which the source
registry entries are repointed and this uploader is retired. It reads the national
crosswalk parquet and the per-state address-count parquets from the processed data
directories, validates that each file carries the expected columns, and loads them into
the configured BigQuery tables — replacing the target on the first file and appending the
rest — so a rerun fully refreshes the table. The BigQuery client is injected so production
supplies a real client while tests supply a fake one.

Usage:
    python -m network_idx.processing.census_blocks_to_bq --table baf
    python -m network_idx.processing.census_blocks_to_bq --table acl
    python -m network_idx.processing.census_blocks_to_bq --table all
"""
import argparse
import logging
from pathlib import Path

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    PROCESSED_DIR_CENSUS_BAF,
    PROCESSED_DIR_CENSUS_ACL,
    BQ_DATASET_CENSUS,
    BQ_TABLE_CENSUS_BAF_BLOCK,
    BQ_TABLE_CENSUS_ACL_BLOCK,
)
from network_idx.constants import (
    CENSUS_BAF_OUTPUTS,
    CENSUS_ACL_OUTPUTS,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _table_id(table_name: str) -> str:
    """Return the fully qualified BigQuery id for a Census block table."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_CENSUS}.{table_name}"


# Per target: the directory to read from, the filename glob, the expected columns, and the
# fully qualified BigQuery table the files are loaded into.
TABLE_CONFIG = {
    "baf": (
        PROCESSED_DIR_CENSUS_BAF,
        "census_baf_national.parquet",
        CENSUS_BAF_OUTPUTS,
        _table_id(BQ_TABLE_CENSUS_BAF_BLOCK),
    ),
    "acl": (
        PROCESSED_DIR_CENSUS_ACL,
        "census_acl_*.parquet",
        CENSUS_ACL_OUTPUTS,
        _table_id(BQ_TABLE_CENSUS_ACL_BLOCK),
    ),
}


def resolve_target(table_key: str) -> tuple[Path, str, list[str], str]:
    """Return the (source directory, glob, expected columns, table id) for a target key."""
    if table_key not in TABLE_CONFIG:
        raise ValueError(f"Unknown table key {table_key!r}; expected one of {list(TABLE_CONFIG)}")
    return TABLE_CONFIG[table_key]


def discover_files(table_key: str) -> list[Path]:
    """Return the sorted parquet files that make up one target, erroring if none are found."""
    source_dir, glob, _, _ = resolve_target(table_key)
    files = sorted(source_dir.glob(glob))
    if not files:
        raise FileNotFoundError(f"No files matching {glob} in {source_dir}")
    return files


def validate_schema(files: list[Path], expected_columns: list[str]) -> bool:
    """
    Check that every file carries the expected columns and that all files agree.

    Missing columns fail validation; extra columns are allowed but warned about. This
    guards against loading a stale or malformed parquet into BigQuery.
    """
    import pyarrow.parquet as pq

    expected_set = set(expected_columns)
    reference_cols: set[str] | None = None
    for f in files:
        file_cols = set(pq.read_schema(f).names)
        missing = expected_set - file_cols
        if missing:
            logger.error(f"Schema mismatch in {f.name}: missing columns {missing}")
            return False
        extra = file_cols - expected_set
        if extra:
            logger.warning(f"{f.name} has extra columns: {extra}")
        if reference_cols is None:
            reference_cols = file_cols
        elif file_cols != reference_cols:
            logger.error(f"Schema inconsistency: {f.name} columns differ from the first file.")
            return False
    logger.info(f"Schema validation passed for {len(files)} file(s).")
    return True


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def load_parquet_to_bq(file_path: Path, table_id: str, client, replace: bool):
    """
    Load one parquet file into a BigQuery table.

    When replace is true the target table is overwritten (used for the first file of a
    target); otherwise the file is appended, so a target made of several files accumulates
    into one table.
    """
    from google.cloud import bigquery

    write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
        if replace
        else bigquery.WriteDisposition.WRITE_APPEND
    )
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
    )
    with open(file_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)
    load_job.result()
    return load_job


def upload(table_key: str, client=None) -> str:
    """
    Upload one Census block target to BigQuery, replacing whatever is there.

    The target's files are discovered and schema-checked, then loaded into the configured
    table with the first file replacing the table and any remaining files appended. The
    fully qualified table id is returned.
    """
    _, _, expected_columns, table_id = resolve_target(table_key)
    files = discover_files(table_key)

    if not validate_schema(files, expected_columns):
        raise ValueError(f"Schema validation failed for target {table_key!r}; aborting upload.")

    if client is None:
        client = get_bq_client()

    for i, f in enumerate(files):
        logger.info(f"Loading {f.name} into {table_id} ({'replace' if i == 0 else 'append'})")
        load_parquet_to_bq(f, table_id, client, replace=(i == 0))

    logger.info(f"Done. Loaded {len(files)} file(s) into {table_id}.")
    return table_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload the Census block reference tables (BAF crosswalk, ACL housing units) to BigQuery."
    )
    parser.add_argument(
        "--table", type=str, default="all",
        choices=["baf", "acl", "all"],
        help="Which target to upload: the block-assignment crosswalk, the address-count listing, or both.",
    )
    args = parser.parse_args()

    targets = ["baf", "acl"] if args.table == "all" else [args.table]
    for t in targets:
        upload(t)
