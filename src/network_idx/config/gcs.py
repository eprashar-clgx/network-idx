"""
Google Cloud Storage settings for the network_idx package.

This module holds the bucket, project, and credential settings used to talk to
Google Cloud Storage, the object-prefix layout that mirrors the local data stages
inside the bucket, and the upload behaviour flags. Secrets are never hard-coded
here; the bucket name, project id, and credential file path are read from the
environment so they can differ per user and per environment.
"""
import os
from pathlib import Path

# ── Connection settings ───────────────────────────────────────────────────────
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_PROJECT_ID = os.getenv("GCS_PROJECT_ID")

# Application Default Credentials JSON path varies per user and per environment.
GCS_ADC_JSON_PATH_EP_LOCAL = Path(os.getenv("GCS_ADC_JSON_PATH_EP_LOCAL", ""))

# ── Object prefixes (mirror the local data stages inside the bucket) ──────────
GCS_PREFIX_RAW_FCC_SPEEDS = "network_idx/raw/fcc/speeds"
GCS_PREFIX_EXTRACTED_FCC_SPEEDS = "network_idx/extracted/fcc/speeds"
GCS_PREFIX_PROCESSED_FCC_SPEEDS = "network_idx/processed/fcc/speeds"

# Feature outputs (tract level)
GCS_PREFIX_FEATURES_FCC_SPEEDS_TRACT = "network_idx/features/fcc/speeds/tract"

# Broadband coverage
GCS_PREFIX_RAW_FCC_BROADBAND_COVERAGE = "network_idx/raw/fcc/broadband_coverage"
GCS_PREFIX_EXTRACTED_FCC_BROADBAND_COVERAGE = "network_idx/extracted/fcc/broadband_coverage"
GCS_PREFIX_PROCESSED_FCC_BROADBAND_COVERAGE = "network_idx/processed/fcc/broadband_coverage"

# ── Upload behaviour ──────────────────────────────────────────────────────────
UPLOAD_OVERWRITE = False  # if False, skip blobs that already exist
UPLOAD_CHUNK_MB = 8       # chunk size for multipart uploads (in MB)
