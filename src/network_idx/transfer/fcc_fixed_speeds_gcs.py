"""
GCS Upload of FCC Fixed Speeds Data (raw, extracted csv and processed parquet files).
==============================================================
Uploads:
    * raw zip files to GCS under:       gs://{BUCKET}/network_idx/raw/fcc/speeds/
    * extracted csv files to GCS under:  gs://{BUCKET}/network_idx/extracted/fcc/speeds/
    * processed parquet files to GCS under: gs://{BUCKET}/network_idx/processed/fcc/speeds/

Authentication:
    - Local (NETWORK_IDX_ENV=local): uses check_and_authenticate with ADC JSON file.
    - VM    (NETWORK_IDX_ENV=vm):    uses Google ADC from the metadata service automatically.

Usage:
    # Upload processed parquets for all states
    python -m network_idx.transfer.fcc_fixed_speeds_gcs --stage processed --all

    # Upload raw zips for selected states
    python -m network_idx.transfer.fcc_fixed_speeds_gcs --stage raw --states AK AL IL IN

    # Force re-upload
    python -m network_idx.transfer.fcc_fixed_speeds_gcs --stage extracted --states AK --overwrite
"""

import argparse
from pathlib import Path
import re
from google.cloud import storage
import logging

from network_idx.config import (
    NETWORK_IDX_ENV,
    RAW_DIR_FCC_SPEEDS,
    EXTRACTED_DIR_FCC_SPEEDS,
    PROCESSED_DIR_FCC_SPEEDS,
    GCS_BUCKET_NAME, 
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    GCS_PREFIX_RAW_FCC_SPEEDS,
    GCS_PREFIX_EXTRACTED_FCC_SPEEDS,
    GCS_PREFIX_PROCESSED_FCC_SPEEDS,
    UPLOAD_OVERWRITE,
    UPLOAD_CHUNK_MB
    )
from network_idx.constants import STATE_USPS_TO_FIPS
from network_idx.utils import check_and_authenticate

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Each stage maps to: (local_dir, glob_pattern, gcs_prefix)
STAGE_CONFIG = {
    "raw": (RAW_DIR_FCC_SPEEDS, "bdc_*.zip", GCS_PREFIX_RAW_FCC_SPEEDS),
    "extracted": (EXTRACTED_DIR_FCC_SPEEDS, "**/*.csv", GCS_PREFIX_EXTRACTED_FCC_SPEEDS),
    "processed": (PROCESSED_DIR_FCC_SPEEDS, "fcc_fixed_speeds_*.parquet", GCS_PREFIX_PROCESSED_FCC_SPEEDS),
}


# GCS Helpers
def get_gcs_client() -> storage.Client:
    '''
    Initializes and returns a GCS client using ADC credentials.
    On local, runs check_and_authenticate first; on VM, ADC is automatic.
    '''
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
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

# ── Filename parsers ─────────────────────────────────────────────────────────

def parse_fips_from_raw_filename(filename: str) -> str | None:
    '''
    Extracts the 2-digit FIPS code from a raw/extracted FCC filename.
    Pattern: bdc_{FIPS}_{Technology}_fixed_broadband_...
    '''
    match = re.match(r"bdc_(\d{2})_", filename)
    return match.group(1) if match else None

def parse_usps_from_processed_filename(filename: str) -> str | None:
    '''
    Extracts state USPS code from a processed parquet filename.
    Pattern: fcc_fixed_speeds_{STATE_USPS}_{STATE_FIPS}.parquet
    '''
    match = re.match(r"fcc_fixed_speeds_(?:providers_)?([A-Z]{2})_\d{2}\.parquet", filename)
    return match.group(1) if match else None

# Main upload function
def upload_fcc_files(
        stage: str,
        states: list[str] | None=None,
        source_dir: Path | None=None,
        bucket_name: str=GCS_BUCKET_NAME,
        overwrite: bool=UPLOAD_OVERWRITE,
        chunk_mb: int=UPLOAD_CHUNK_MB
        ) -> list[str]:
    '''
    Uploads FCC fixed speeds files to GCS for the given stage.
    Args:
        stage:      One of "raw", "extracted", "processed".
        states:     List of state USPS codes to upload. If None, uploads all.
        source_dir: Override the default local directory for this stage.
        bucket_name: GCS bucket name.
        overwrite:  Whether to overwrite existing blobs.
        chunk_mb:   Chunk size in MB for uploads.
    Returns:
        List of GCS blob names that were uploaded.
    '''
    default_dir, glob_pattern, gcs_prefix = STAGE_CONFIG[stage]
    local_dir = source_dir or default_dir

    # Build FIPS filter set from requested USPS codes
    fips_filter = None
    usps_filter = None
    if states:
        states_upper = [s.upper() for s in states]
        if stage in ("raw", "extracted"):
            fips_filter = {STATE_USPS_TO_FIPS[usps] for usps in states_upper if usps in STATE_USPS_TO_FIPS}
        else:
            usps_filter = set(states_upper)
    
    # Find local files
    all_files = sorted(local_dir.glob(glob_pattern))
    if not all_files:
        logger.error(f"No {stage} files matching '{glob_pattern}' found in {local_dir}. Exiting.")
        return []

     # Filter to requested states
    if fips_filter is not None:
        files = [f for f in all_files if (fips := parse_fips_from_raw_filename(f.name)) and fips in fips_filter]
    elif usps_filter is not None:
        files = [f for f in all_files if (usps := parse_usps_from_processed_filename(f.name)) and usps in usps_filter]
    else:
        files = all_files

    if not files:
        logger.error(f"No {stage} files found for specified states. Exiting.")
        return []
    
    logger.info(f"Stage: {stage}")
    logger.info(f"Source directory: {local_dir.resolve()}")
    logger.info(f"GCS Bucket: {bucket_name}, Prefix: {gcs_prefix}")
    logger.info(f"Files to upload: {len(files)}")
    logger.info(f"Overwrite existing: {overwrite}")

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    uploaded_blobs = []

    for file in files:
        blob_name = f"{gcs_prefix}/{file.name}"
        success = upload_file(
            local_path=file,
            bucket=bucket,
            blob_name=blob_name,
            overwrite=overwrite,
            chunk_mb=chunk_mb,
        )
        if success:
            uploaded_blobs.append(blob_name)

    logger.info(f"Upload complete. {len(uploaded_blobs)}/{len(files)} files uploaded.")
    return uploaded_blobs

# ── CLI entry point
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload FCC Fixed Speeds files to Google Cloud Storage."
    )
    parser.add_argument(
        "--stage", type=str, required=True,
        choices=STAGE_CONFIG.keys(),
        help="Which file stage to upload: raw (zips), extracted (csvs), or processed (parquets)."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=None,
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to upload (USPS codes). If not specified, uploads all."
    )
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Upload data for all states (overrides --states)."
    )
    parser.add_argument(
        "--overwrite", action="store_true", default=False,
        help="Overwrite existing files in GCS. Defaults to False."
    )
    parser.add_argument(
        "--source-dir", type=Path, default=None,
        help="Override the default source directory for the chosen stage."
    )
    args = parser.parse_args()

    valid = list(STATE_USPS_TO_FIPS.keys())
    bad = [s for s in (args.states or []) if s not in valid]
    if bad:
        parser.error(f"Invalid state USPS codes: {bad}. Must be one or more of: {valid}")

    states_to_upload = valid if args.all else args.states

    upload_fcc_files(
        stage=args.stage,
        states=states_to_upload,
        source_dir=args.source_dir,
        overwrite=args.overwrite,
    )