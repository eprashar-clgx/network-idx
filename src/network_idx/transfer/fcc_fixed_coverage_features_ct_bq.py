"""
BQ Upload of FCC Coverage feature parquet files.
==============================================================
Loads state-level coverage feature parquets into BigQuery tables.

Target tables:
    * block            → {BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK}
    * county_residuals → {BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_COUNTY_RESIDUALS}
    * tract            → {BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT}

Authentication:
    - Local (NETWORK_IDX_ENV=local): uses check_and_authenticate with ADC JSON file.
    - VM    (NETWORK_IDX_ENV=vm):    uses Google ADC from the metadata service automatically.

Usage:
    # Upload tract-level for all states
    python -m network_idx.transfer.fcc_fixed_coverage_features_ct_bq --table tract --all

    # Upload block-level for selected states
    python -m network_idx.transfer.fcc_fixed_coverage_features_ct_bq --table block --states AK AL

    # Upload county residuals, overwrite existing
    python -m network_idx.transfer.fcc_fixed_coverage_features_ct_bq --table county_residuals --all --overwrite
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
    FEATURES_DIR_FCC_COVERAGE_BLOCK,
    FEATURES_DIR_FCC_COVERAGE_TRACT,
    FEATURES_DIR_FCC_COVERAGE_COUNTY_RESIDUALS,
    BQ_DATASET_FCC_COVERAGE,
    BQ_DATASET_FEATURES,
    BQ_TABLE_FCC_COVERAGE_BLOCK,
    BQ_TABLE_FCC_COVERAGE_COUNTY_RESIDUALS,
    BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT
)
from network_idx.constants import (
    STATE_USPS_TO_FIPS,
    FCC_COVERAGE_BLOCK_OUTPUTS,
    FCC_COVERAGE_TRACT_OUTPUTS,
    FCC_COVERAGE_COUNTY_RESIDUAL_OUTPUTS,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Table config: (source_dir, glob_pattern, expected_columns, fully-qualified BQ table ID)
TABLE_CONFIG = {
    "block": (
        FEATURES_DIR_FCC_COVERAGE_BLOCK,
        "fcc_coverage_block_*.parquet",
        FCC_COVERAGE_BLOCK_OUTPUTS,
        f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK}",
    ),
    "tract": (
        FEATURES_DIR_FCC_COVERAGE_TRACT,
        "fcc_coverage_tract_*.parquet",
        FCC_COVERAGE_TRACT_OUTPUTS,
        f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT}",
    ),
    "county_residuals": (
        FEATURES_DIR_FCC_COVERAGE_COUNTY_RESIDUALS,
        "fcc_coverage_county_residuals_*.parquet",
        FCC_COVERAGE_COUNTY_RESIDUAL_OUTPUTS,
        f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_COUNTY_RESIDUALS}",
    ),
}


# ── Helpers

def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def parse_usps_from_filename(filename: str) -> str | None:
    match = re.match(
        r"fcc_coverage_(?:block|tract|county_residuals)_([A-Z]{2})_\d{2}\.parquet",
        filename,
    )
    return match.group(1) if match else None


def get_existing_states(client: bigquery.Client, table_id: str, state_col: str) -> set[str]:
    try:
        query = f"SELECT DISTINCT {state_col} FROM `{table_id}`"
        rows = client.query(query).result()
        return {getattr(row, state_col) for row in rows}
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


# ── Main upload function ─────────────────────────────────────────────────────

def upload_to_bq(
    table: str,
    states: list[str] | None = None,
    source_dir: Path | None = None,
    overwrite: bool = False,
) -> int:
    default_dir, glob_pattern, expected_columns, table_id = TABLE_CONFIG[table]
    local_dir = source_dir or default_dir

    all_files = sorted(local_dir.glob(glob_pattern))
    if not all_files:
        logger.error(f"No files matching '{glob_pattern}' in {local_dir}.")
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

    logger.info(f"Table type: {table}")
    logger.info(f"BQ target:  {table_id}")
    logger.info(f"Files:      {len(files)}")
    logger.info(f"Overwrite:  {overwrite}")

    if not validate_schema(files, expected_columns):
        logger.error("Schema validation failed. Aborting upload.")
        return 0

    client = get_bq_client()

    # Determine the state column for dedup checks:
    # block & tract have state_usps; county_residuals only has state_fips
    if table == "county_residuals":
        state_col = "state_fips"
        fips_to_usps = {v: k for k, v in STATE_USPS_TO_FIPS.items()}
        existing_fips = get_existing_states(client, table_id, state_col) if not overwrite else set()
        existing_usps = {fips_to_usps.get(f, "") for f in existing_fips}
    else:
        state_col = "state_usps"
        existing_usps = set() if overwrite else get_existing_states(client, table_id, state_col)

    if overwrite:
        logger.info(f"Overwrite requested — deleting table {table_id} if it exists.")
        client.delete_table(table_id, not_found_ok=True)
    elif existing_usps:
        logger.info(f"States already in BQ: {sorted(existing_usps)}")

    files_to_load = [
        f for f in files
        if (parse_usps_from_filename(f.name) or "") not in existing_usps
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
            logger.info(f"  {state_usps} complete — {job.output_rows} rows loaded.")
            loaded += 1
        except Exception as e:
            logger.error(f"  {state_usps} FAILED: {e}")

    logger.info(f"Done. {loaded}/{len(files_to_load)} files loaded into {table_id}.")
    return loaded


# ── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load FCC Coverage feature parquets into BigQuery."
    )
    parser.add_argument(
        "--table", type=str, required=True,
        choices=TABLE_CONFIG.keys(),
        help="Which table to load: block, tract, or county_residuals.",
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=None,
        choices=STATE_USPS_TO_FIPS.keys(), metavar="STATE",
        help="States to upload (USPS codes). If omitted, uploads all.",
    )
    parser.add_argument("--all", action="store_true", default=False, help="Upload all states.")
    parser.add_argument("--overwrite", action="store_true", default=False, help="Delete and recreate BQ table.")
    parser.add_argument("--source-dir", type=Path, default=None, help="Override source directory.")
    args = parser.parse_args()

    valid = list(STATE_USPS_TO_FIPS.keys())
    if args.states:
        bad = [s for s in args.states if s not in valid]
        if bad:
            parser.error(f"Invalid state codes: {bad}")

    states_to_upload = valid if args.all else args.states
    upload_to_bq(table=args.table, states=states_to_upload, source_dir=args.source_dir, overwrite=args.overwrite)