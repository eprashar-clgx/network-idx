"""
BQ Upload of FCC Fixed Speeds tract-level feature parquet files.
==============================================================
Loads state-level tract feature parquet files into a BigQuery table, one state at a time.

Target table:
    {BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_SPEEDS_FEATURES_TRACT}

Authentication:
    - Local (NETWORK_IDX_ENV=local): uses check_and_authenticate with ADC JSON file.
    - VM    (NETWORK_IDX_ENV=vm):    uses Google ADC from the metadata service automatically.

Usage:
    # Upload for all states
    python -m network_idx.transfer.fcc_fixed_speeds_features_ct_bq --all

    # Upload for selected states
    python -m network_idx.transfer.fcc_fixed_speeds_features_ct_bq --states AK AL CA

    # Overwrite (delete table first, then reload)
    python -m network_idx.transfer.fcc_fixed_speeds_features_ct_bq --all --overwrite
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
    FEATURES_DIR_FCC_SPEEDS_TRACT,
    BQ_DATASET_FEATURES,
    BQ_TABLE_FCC_SPEEDS_FEATURES_TRACT,
)
from network_idx.constants import (
    STATE_USPS_TO_FIPS,
    FCC_FIXED_SPEED_TRACT_OUTPUTS,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

GLOB_PATTERN = "fcc_fixed_speeds_tract_*.parquet"
BQ_TABLE_ID = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_SPEEDS_FEATURES_TRACT}"


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def parse_usps_from_filename(filename: str) -> str | None:
    match = re.match(r"fcc_fixed_speeds_tract_([A-Z]{2})_\d{2}\.parquet", filename)
    return match.group(1) if match else None


def get_existing_states(client: bigquery.Client, table_id: str) -> set[str]:
    try:
        query = f"SELECT DISTINCT state_usps FROM `{table_id}`"
        rows = client.query(query).result()
        return {row.state_usps for row in rows}
    except Exception:
        return set()


def validate_schema(files: list[Path], expected_columns: list[str]) -> bool:
    reference_cols: set[str] | None = None
    for f in files:
        schema = pq.read_schema(f)
        file_cols = set(schema.names)
        expected_set = set(expected_columns)
        missing = expected_set - file_cols
        extra = file_cols - expected_set
        if missing:
            logger.error(f"Schema mismatch in {f.name}: missing columns {missing}")
            return False
        if extra:
            logger.warning(f"{f.name} has extra columns: {extra}")
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
    file_path: Path, table_id: str, client: bigquery.Client
) -> bigquery.LoadJob:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    with open(file_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_id, job_config=job_config)
    load_job.result()
    return load_job


# ── Main upload function

def upload_to_bq(
    states: list[str] | None = None,
    source_dir: Path = FEATURES_DIR_FCC_SPEEDS_TRACT,
    overwrite: bool = False,
) -> int:
    all_files = sorted(source_dir.glob(GLOB_PATTERN))
    if not all_files:
        logger.error(f"No files matching '{GLOB_PATTERN}' in {source_dir}.")
        return 0

    if states:
        usps_filter = {s.upper() for s in states}
        files = [
            f for f in all_files
            if (usps := parse_usps_from_filename(f.name)) and usps in usps_filter
        ]
    else:
        files = all_files

    if not files:
        logger.error("No files found for the specified states.")
        return 0

    logger.info(f"BQ target:  {BQ_TABLE_ID}")
    logger.info(f"Files:      {len(files)}")
    logger.info(f"Overwrite:  {overwrite}")

    if not validate_schema(files, FCC_FIXED_SPEED_TRACT_OUTPUTS):
        logger.error("Schema validation failed. Aborting upload.")
        return 0

    client = get_bq_client()

    if overwrite:
        logger.info(f"Overwrite requested — deleting table {BQ_TABLE_ID} if it exists.")
        client.delete_table(BQ_TABLE_ID, not_found_ok=True)
        existing_states = set()
    else:
        existing_states = get_existing_states(client, BQ_TABLE_ID)
        if existing_states:
            logger.info(f"States already in BQ: {sorted(existing_states)}")

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
        logger.info(f"Loading {state_usps} ({file.name}) → {BQ_TABLE_ID} ...")
        try:
            job = load_parquet_to_bq(file, BQ_TABLE_ID, client)
            logger.info(f"  {state_usps} complete — {job.output_rows} rows loaded.")
            loaded += 1
        except Exception as e:
            logger.error(f"  {state_usps} FAILED: {e}")

    logger.info(f"Done. {loaded}/{len(files_to_load)} files loaded into {BQ_TABLE_ID}.")
    return loaded


# ── CLI entry point

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load FCC Fixed Speeds tract-level features into BigQuery."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=None,
        choices=STATE_USPS_TO_FIPS.keys(), metavar="STATE",
        help="States to upload (USPS codes). If omitted, uploads all.",
    )
    parser.add_argument("--all", action="store_true", default=False, help="Upload all states.")
    parser.add_argument("--overwrite", action="store_true", default=False, help="Delete and recreate BQ table.")
    parser.add_argument("--source-dir", type=Path, default=FEATURES_DIR_FCC_SPEEDS_TRACT)
    args = parser.parse_args()

    valid = list(STATE_USPS_TO_FIPS.keys())
    if args.states:
        bad = [s for s in args.states if s not in valid]
        if bad:
            parser.error(f"Invalid state codes: {bad}")

    states_to_upload = valid if args.all else args.states
    upload_to_bq(states=states_to_upload, source_dir=args.source_dir, overwrite=args.overwrite)