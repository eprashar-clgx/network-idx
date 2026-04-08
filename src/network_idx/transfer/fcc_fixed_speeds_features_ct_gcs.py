"""
GCS Upload of FCC Fixed Speeds tract-level feature parquets.
==============================================================
Uploads:
    * tract parquet files to GCS under: gs://{BUCKET}/network_idx/features/fcc/speeds/tract/

Authentication:
    - Local (NETWORK_IDX_ENV=local): uses check_and_authenticate with ADC JSON file.
    - VM    (NETWORK_IDX_ENV=vm):    uses Google ADC from the metadata service automatically.

Usage:
    # Upload all tract parquets
    python -m network_idx.transfer.fcc_fixed_speeds_features_ct_gcs --all

    # Upload for specific states
    python -m network_idx.transfer.fcc_fixed_speeds_features_ct_gcs --states AK AL CA

    # Force re-upload
    python -m network_idx.transfer.fcc_fixed_speeds_features_ct_gcs --states AK --overwrite
"""

import argparse
from pathlib import Path
import re
from google.cloud import storage
import logging

from network_idx.config import (
    NETWORK_IDX_ENV,
    FEATURES_DIR_FCC_SPEEDS_TRACT,
    GCS_BUCKET_NAME,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    GCS_PREFIX_FEATURES_FCC_SPEEDS_TRACT,
    UPLOAD_OVERWRITE,
    UPLOAD_CHUNK_MB,
)
from network_idx.constants import STATE_USPS_TO_FIPS
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ── GCS Helpers

def get_gcs_client() -> storage.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return storage.Client(project=GCS_PROJECT_ID)


def blob_exists(bucket: storage.Bucket, blob_name: str) -> bool:
    return bucket.blob(blob_name).exists()


def upload_file(
    local_path: Path,
    bucket: storage.Bucket,
    blob_name: str,
    overwrite: bool = False,
    chunk_mb: int = 8,
) -> bool:
    if not overwrite and blob_exists(bucket, blob_name):
        logger.warning(f"Blob {blob_name} already exists. Skipping.")
        return False
    blob = bucket.blob(blob_name, chunk_size=chunk_mb * 1024 * 1024)
    logger.info(f"Uploading {local_path} to gs://{bucket.name}/{blob_name}...")
    blob.upload_from_filename(str(local_path))
    size_mb = local_path.stat().st_size / (1024 * 1024)
    logger.info(f"Upload complete: {local_path} ({size_mb:.2f} MB)")
    return True


# ── Filename parser ───────────────────────────────────────────────────────────

def parse_usps_from_filename(filename: str) -> str | None:
    """
    Pattern: fcc_fixed_speeds_tract_{STATE_USPS}_{FIPS}.parquet
    """
    match = re.match(r"fcc_fixed_speeds_tract_([A-Z]{2})_\d{2}\.parquet", filename)
    return match.group(1) if match else None


# ── Main upload function ─────────────────────────────────────────────────────

def upload_tract_features(
    states: list[str] | None = None,
    source_dir: Path | None = None,
    bucket_name: str = GCS_BUCKET_NAME,
    overwrite: bool = UPLOAD_OVERWRITE,
    chunk_mb: int = UPLOAD_CHUNK_MB,
) -> list[str]:
    local_dir = source_dir or FEATURES_DIR_FCC_SPEEDS_TRACT
    gcs_prefix = GCS_PREFIX_FEATURES_FCC_SPEEDS_TRACT

    all_files = sorted(local_dir.glob("fcc_fixed_speeds_tract_*.parquet"))
    if not all_files:
        logger.error(f"No tract parquets found in {local_dir}. Exiting.")
        return []

    # Filter to requested states
    if states:
        usps_filter = {s.upper() for s in states}
        files = [f for f in all_files if (usps := parse_usps_from_filename(f.name)) and usps in usps_filter]
    else:
        files = all_files

    if not files:
        logger.error("No tract parquets found for specified states. Exiting.")
        return []

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


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload FCC Fixed Speeds tract-level feature parquets to GCS."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=None,
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help="States to upload (USPS codes). If not specified, uploads all.",
    )
    parser.add_argument("--all", action="store_true", default=False, help="Upload all states.")
    parser.add_argument("--overwrite", action="store_true", default=False, help="Overwrite existing GCS blobs.")
    parser.add_argument("--source-dir", type=Path, default=None, help="Override source directory.")
    args = parser.parse_args()

    valid = list(STATE_USPS_TO_FIPS.keys())
    bad = [s for s in (args.states or []) if s not in valid]
    if bad:
        parser.error(f"Invalid state codes: {bad}. Must be one or more of: {valid}")

    states_to_upload = valid if args.all else args.states

    upload_tract_features(
        states=states_to_upload,
        source_dir=args.source_dir,
        overwrite=args.overwrite,
    )