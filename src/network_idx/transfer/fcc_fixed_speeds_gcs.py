"""
GCS Upload of FCC Fixed Speeds Data (Parquet files).
==============================================================
Uploads processed parquet files to GCS under:
    gs://{GCS_BUCKET_NAME}/processed/fcc/speeds/fcc_fixed_speeds_{STATE_USPS}_{STATE_FIPS}.parquet

Authentication:
Uses Google's ADC. Run `gcloud auth application-default login` to set up credentials locally.
Make sure the JSON path in .env, config.py and this file point to the correct location of your ADC JSON file.

Usage:
    # Upload all states
    python -m src.network_idx.transfer.fcc_fixed_speeds_gcs --all

    # Upload selected states
    python -m src.network_idx.transfer.fcc_fixed_speeds_gcs --states AK AL IL IN

    # Force re-upload
    python -m src.network_idx.transfer.fcc_fixed_speeds_gcs --states AK AL IL IN --overwrite 
"""

import argparse
from pathlib import Path
import re
from google.cloud import storage
import logging

from network_idx.config import (
    PROCESSED_DIR_FCC_SPEEDS,
    GCS_BUCKET_NAME, 
    GCS_PROJECT_ID, 
    GCS_ADC_JSON_PATH_EP_LOCAL,
    GCS_PREFIX_PROCESSED_FCC_SPEEDS, 
    UPLOAD_OVERWRITE, 
    UPLOAD_CHUNK_MB)
from network_idx.constants import STATE_USPS_TO_FIPS
from network_idx.utils import check_and_authenticate

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# GCS Helpers
def get_gcs_client() -> storage.Client:
    '''
    Initializes and returns a GCS client using ADC credentials.
    '''
    return storage.Client(project=GCS_PROJECT_ID)

def blob_exists(bucket: storage.Bucket, blob_name: str) -> bool:
    '''
    Checks if a blob with the given name exists in the specified bucket.
    '''
    return bucket.blob(blob_name).exists()

def upload_file(
        local_path: Path,
        bucket: storage.Bucket,
        blob_name: str,
        overwrite: bool = False,
        chunk_mb: int = 8
        ) -> bool:
    '''
    Uploads a file to GCS with options for overwrite and chunk size.
    Returns True if upload was successful, False otherwise.
    '''
    if not overwrite and blob_exists(bucket, blob_name):
        logger.warning(f"Blob {blob_name} already exists in bucket {bucket.name}. Skipping upload.")
        return False
    blob = bucket.blob(blob_name, chunk_size=chunk_mb * 1024 * 1024)
    logger.info(f"Uploading {local_path} to gs://{bucket.name}/{blob_name}...")
    blob.upload_from_filename(str(local_path))
    size_mb = local_path.stat().st_size / (1024 * 1024)
    logger.info(f"Upload complete: {local_path} ({size_mb:.2f} MB)")
    return True

# Filename helper
def parse_usps_fips_from_filename(filename: str) -> tuple[str, str] | None:
    '''
    Extracts state USPS and FIPS code from the filename.
    Pattern: fcc_fixed_speeds_{STATE_USPS}_{STATE_FIPS}.parquet
    '''
    match = re.match(r"fcc_fixed_speeds_([A-Z]{2})_(\d{2})\.parquet", filename)
    if match:
        return match.group(1), match.group(2)
    return None

# Main upload function
def upload_fcc_parquets(
        states: list[str] | None=None,
        processed_dir: Path=PROCESSED_DIR_FCC_SPEEDS,
        bucket_name: str=GCS_BUCKET_NAME,
        gcs_prefix: str=GCS_PREFIX_PROCESSED_FCC_SPEEDS,
        overwrite: bool=UPLOAD_OVERWRITE,
        chunk_mb: int=UPLOAD_CHUNK_MB
        ) -> list[str]:
    '''
    Uploads FCC fixed speeds parquet files to GCS.
    Args:
        states: List of state USPS codes to upload. If None, uploads all states.
        processed_dir: Directory containing processed parquet files.
        bucket_name: GCS bucket name.
        gcs_prefix: GCS prefix for uploaded files.
        overwrite: Whether to overwrite existing files in GCS.
        chunk_mb: Chunk size in MB for uploads.
    Returns:
        List of GCS blob names that were uploaded.
    '''
    # Authenticate and initialize GCS client
    # NOTE: JSON path must be set according to the user and environment in config.py and then referenced here
    check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)

    # Find parquet files
    all_parquet_files = sorted(processed_dir.glob("fcc_fixed_speeds_*.parquet"))
    if not all_parquet_files:
        logger.error(f"No parquet files found in {processed_dir}. Exiting.")
        return []
    
    # Filter to requested states if specified
    if states:
        states_upper = [s.upper() for s in states]
        parquets = [
            p for p in all_parquet_files if (parsed := parse_usps_fips_from_filename(p.name)) and parsed[0] in states_upper
        ]
        if not parquets:
            logger.error(f"No parquet files found for specified states: {states_upper}. Exiting.")
            return []
    else:
        parquets = all_parquet_files
        
    logger.info(f"Found {len(parquets)} parquet files to upload.")
    logger.info(f"Source directory: {processed_dir.resolve()}")
    logger.info(f"GCS Bucket: {bucket_name}, Prefix: {gcs_prefix}")
    logger.info(f"Files: {len(parquets)}")
    logger.info(f"Overwrite existing: {overwrite}")

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    uploaded_blobs = []

    for parquet in parquets:
        blob_name = f"{gcs_prefix}/{parquet.name}"
        success = upload_file(
            local_path=parquet,
            bucket=bucket,
            blob_name=blob_name,
            overwrite=overwrite,
            chunk_mb=chunk_mb
        )
        if success:
            uploaded_blobs.append(blob_name)
    logger.info(f"Upload process complete. Total files uploaded: {len(uploaded_blobs)}")
    return uploaded_blobs

# ── CLI entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload FCC Fixed Speeds parquet files to Google Cloud Storage."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=None,
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to upload - one or more of: {list(STATE_USPS_TO_FIPS.keys())}. If not specified, uploads all states."
        )
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Upload data for all states (overrides --states)"
        )
    parser.add_argument(
        "--overwrite", action="store_true", default=False,
        help="Whether to overwrite existing files in GCS. Defaults to False."
    )
    parser.add_argument(
        "--processed-dir", type=Path, default=PROCESSED_DIR_FCC_SPEEDS,
        help="Directory containing processed parquet files. Defaults to 'data/processed/fcc/speeds'"
    )
    args = parser.parse_args()
    valid = list(STATE_USPS_TO_FIPS.keys())
    bad = [s for s in (args.states or []) if s not in valid]
    if bad:
        logger.error(f"Invalid state USPS codes: {bad}. Must be one or more of: {list(valid)}")
    states_to_upload = valid if args.all else args.states
    upload_fcc_parquets(
        states=states_to_upload,
        processed_dir=args.processed_dir,
        overwrite=args.overwrite
    )