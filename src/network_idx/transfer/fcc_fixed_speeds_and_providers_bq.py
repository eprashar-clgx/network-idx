"""
BQ Upload of FCC Fixed Speeds processed parquet files.
==============================================================
Loads state-level parquet files into BigQuery tables, one state at a time.

Target tables:
    * speeds          → {BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_BLOCK}
    * providers_block → {BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_PROVIDERS_BLOCK}
    * providers_h3    → {BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_PROVIDERS_H3}

Authentication:
    - Local (NETWORK_IDX_ENV=local): uses check_and_authenticate with ADC JSON file.
    - VM    (NETWORK_IDX_ENV=vm):    uses Google ADC from the metadata service automatically.

Usage:
    # Upload block-level speeds for all states
    python -m network_idx.transfer.fcc_fixed_speeds_and_providers_bq --table speeds --all

    # Upload provider-block data for selected states
    python -m network_idx.transfer.fcc_fixed_speeds_and_providers_bq --table providers_block --states AK AL

    # Upload provider-h3 data, overwriting existing table
    python -m network_idx.transfer.fcc_fixed_speeds_and_providers_bq --table providers_h3 --all --overwrite
"""

import argparse
import logging
import re
from pathlib import Path

import pyarrow.parquet as pq
from google.cloud import bigquery

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    PROCESSED_DIR_FCC_SPEEDS,
    BQ_DATASET_FCC_SPEEDS,
    BQ_TABLE_FCC_SPEEDS_BLOCK,
    BQ_TABLE_FCC_SPEEDS_PROVIDERS_BLOCK,
    BQ_TABLE_FCC_SPEEDS_PROVIDERS_H3,
)
from network_idx.constants import (
    STATE_USPS_TO_FIPS,
    FCC_FIXED_SPEED_OUTPUTS,
    FCC_FIXED_SPEEDS_PROVIDER_OUTPUTS,
    FCC_FIXED_SPEEDS_PROVIDER_H3_OUTPUTS,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Table config: (glob_pattern, expected_columns, fully-qualified BQ table ID) ─
TABLE_CONFIG = {
    "speeds": (
        "fcc_fixed_speeds_[A-Z][A-Z]_*.parquet",
        FCC_FIXED_SPEED_OUTPUTS,
        f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_BLOCK}",
    ),
    "providers_block": (
        "fcc_fixed_speeds_providers_block_*.parquet",
        FCC_FIXED_SPEEDS_PROVIDER_OUTPUTS,
        f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_PROVIDERS_BLOCK}",
    ),
    "providers_h3": (
        "fcc_fixed_speeds_providers_h3_*.parquet",
        FCC_FIXED_SPEEDS_PROVIDER_H3_OUTPUTS,
        f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_PROVIDERS_H3}",
    ),
}


# ── Helpers

def get_bq_client() -> bigquery.Client:
    """Returns an authenticated BigQuery client."""
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def parse_usps_from_filename(filename: str) -> str | None:
    """Extracts state USPS code from a processed parquet filename."""
    match = re.match(
        r"fcc_fixed_speeds_(?:providers_(?:block_|h3_))?([A-Z]{2})_\d{2}\.parquet",
        filename,
    )
    return match.group(1) if match else None

def get_existing_states(client: bigquery.Client, table_id: str) -> set[str]:
    """Returns the set of state_usps codes already present in the BQ table."""
    try:
        query = f"SELECT DISTINCT state_usps FROM `{table_id}`"
        rows = client.query(query).result()
        return {row.state_usps for row in rows}
    except Exception:
        return set()  # Table doesn't exist yet

def validate_schema(files: list[Path], expected_columns: list[str]) -> bool:
    """
    Validates that all parquet files share the same column set
    and match the expected output columns.
    Returns True if all schemas are consistent, False otherwise.
    """
    reference_cols: set[str] | None = None

    for f in files:
        schema = pq.read_schema(f)
        file_cols = set(schema.names)

        # Check against expected columns
        expected_set = set(expected_columns)
        missing = expected_set - file_cols
        extra = file_cols - expected_set
        if missing:
            logger.error(f"Schema mismatch in {f.name}: missing columns {missing}")
            return False
        if extra:
            logger.warning(f"{f.name} has extra columns (will be ignored by BQ schema): {extra}")

        # Check cross-file consistency
        if reference_cols is None:
            reference_cols = file_cols
        elif file_cols != reference_cols:
            logger.error(
                f"Schema inconsistency: {f.name} columns differ from first file. "
                f"Added: {file_cols - reference_cols}, Removed: {reference_cols - file_cols}"
            )
            return False

    logger.info(f"Schema validation passed for {len(files)} file(s).")
    return True


def load_parquet_to_bq(
    file_path: Path,
    table_id: str,
    client: bigquery.Client,
) -> bigquery.LoadJob:
    """Loads a single parquet file into a BQ table using WRITE_APPEND."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    with open(file_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)
    load_job.result()  # Wait for completion
    return load_job


# ── Main upload function

def upload_to_bq(
    table: str,
    states: list[str] | None = None,
    source_dir: Path = PROCESSED_DIR_FCC_SPEEDS,
    overwrite: bool = False,
) -> int:
    """
    Uploads state-level parquet files to a BigQuery table.
    Args:
        table:      One of "speeds", "providers_block", "providers_h3".
        states:     List of state USPS codes. If None, uploads all found files.
        source_dir: Directory containing the parquet files.
        overwrite:  If True, truncates the BQ table before loading.
    Returns:
        Number of files successfully loaded.
    """
    glob_pattern, expected_columns, table_id = TABLE_CONFIG[table]

    # Discover files
    all_files = sorted(source_dir.glob(glob_pattern))
    if not all_files:
        logger.error(f"No files matching '{glob_pattern}' in {source_dir}.")
        return 0

    # Filter by state
    if states:
        usps_filter = {s.upper() for s in states}
        files = [
            f for f in all_files
            if (usps := parse_usps_from_filename(f.name)) and usps in usps_filter
        ]
    else:
        files = all_files

    if not files:
        logger.error(f"No files found for the specified states.")
        return 0

    logger.info(f"Table type: {table}")
    logger.info(f"BQ target:  {table_id}")
    logger.info(f"Files:      {len(files)}")
    logger.info(f"Overwrite:  {overwrite}")

    # Validate schemas before uploading anything
    if not validate_schema(files, expected_columns):
        logger.error("Schema validation failed. Aborting upload.")
        return 0
    
    client = get_bq_client()

    # If overwrite, truncate table before loading
    if overwrite:
        logger.info(f"Overwrite requested — deleting table {table_id} if it exists.")
        client.delete_table(table_id, not_found_ok=True)
        existing_states = set()
    else:
        existing_states = get_existing_states(client, table_id)
        if existing_states:
            logger.info(f"States already in BQ: {sorted(existing_states)}")

    # Filter out already-loaded states
    files_to_load = [
        f for f in files
        if (parse_usps_from_filename(f.name) or "") not in existing_states
    ]
    skipped = len(files) - len(files_to_load)
    if skipped:
        logger.info(f"Skipping {skipped} file(s) already in BQ.")
    if not files_to_load:
        logger.info("All files already loaded. Nothing to do.")
        return 0

    loaded = 0
    for file in files_to_load:
        state_usps = parse_usps_from_filename(file.name) or "??"
        logger.info(f"Loading {state_usps} ({file.name}) → {table_id} ...")
        try:
            job = load_parquet_to_bq(file, table_id, client)
            logger.info(
                f"  {state_usps} complete — "
                f"{job.output_rows} rows loaded."
            )
            loaded += 1
        except Exception as e:
            logger.error(f"  {state_usps} FAILED: {e}")

    logger.info(f"Done. {loaded}/{len(files_to_load)} files loaded into {table_id}.")
    return loaded


# ── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load FCC Fixed Speeds parquet files into BigQuery."
    )
    parser.add_argument(
        "--table", type=str, required=True,
        choices=TABLE_CONFIG.keys(),
        help="Which table to load: speeds (block-level), providers_block, or providers_h3.",
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=None,
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help="States to upload (USPS codes). If omitted, uploads all.",
    )
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Upload all states (overrides --states).",
    )
    parser.add_argument(
        "--overwrite", action="store_true", default=False,
        help="Truncate the BQ table before loading. Defaults to False (append).",
    )
    parser.add_argument(
        "--source-dir", type=Path, default=PROCESSED_DIR_FCC_SPEEDS,
        help="Override source directory for parquet files.",
    )
    args = parser.parse_args()

    valid = list(STATE_USPS_TO_FIPS.keys())
    if args.states:
        bad = [s for s in args.states if s not in valid]
        if bad:
            parser.error(f"Invalid state codes: {bad}")

    states_to_upload = valid if args.all else args.states

    upload_to_bq(
        table=args.table,
        states=states_to_upload,
        source_dir=args.source_dir,
        overwrite=args.overwrite,
    )