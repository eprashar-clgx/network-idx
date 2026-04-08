"""
Configuration for network_idx package that will potentially be environment-specific. 
For example, file paths, API keys, or other settings that may differ within dev (local/remote) or across staging/production environments.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

# Environment
NETWORK_IDX_ENV = os.getenv("NETWORK_IDX_ENV", "local")  

# ── File Paths ────────────────────────────────────────────────────────────────
RAW_DIR = Path("data/raw")
RAW_DIR_FCC = Path("data/raw/fcc")
RAW_DIR_FCC_SPEEDS = Path("data/raw/fcc/speeds")
RAW_DIR_FCC_BROADBAND_COVERAGE = Path("data/raw/fcc/broadband_coverage")


EXTRACTED_DIR = Path("data/extracted")
EXTRACTED_DIR_FCC = Path("data/extracted/fcc")
EXTRACTED_DIR_FCC_SPEEDS = Path("data/extracted/fcc/speeds")
EXTRACTED_DIR_FCC_BROADBAND_COVERAGE = Path("data/extracted/fcc/broadband_coverage")

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR_FCC = Path("data/processed/fcc")
PROCESSED_DIR_FCC_SPEEDS = Path("data/processed/fcc/speeds")
PROCESSED_DIR_FCC_BROADBAND_COVERAGE = Path("data/processed/fcc/broadband_coverage")

# Feature engineering output paths
FEATURES_DIR_FCC_SPEEDS_TRACT = Path("data/features/fcc/speeds/tract")

# GCS Settings
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_PROJECT_ID = os.getenv("GCS_PROJECT_ID")

# JSON path varies per user per environment
GCS_ADC_JSON_PATH_EP_LOCAL = Path(os.getenv("GCS_ADC_JSON_PATH_EP_LOCAL", ""))

# GCS Storage settings
GCS_PREFIX_RAW_FCC_SPEEDS = "network_idx/raw/fcc/speeds"
GCS_PREFIX_EXTRACTED_FCC_SPEEDS = "network_idx/extracted/fcc/speeds"
GCS_PREFIX_PROCESSED_FCC_SPEEDS = "network_idx/processed/fcc/speeds"

# GCS Storage settings — Broadband Coverage
GCS_PREFIX_RAW_FCC_BROADBAND_COVERAGE = "network_idx/raw/fcc/broadband_coverage"
GCS_PREFIX_EXTRACTED_FCC_BROADBAND_COVERAGE = "network_idx/extracted/fcc/broadband_coverage"
GCS_PREFIX_PROCESSED_FCC_BROADBAND_COVERAGE = "network_idx/processed/fcc/broadband_coverage"

# GCS Upload settings
UPLOAD_OVERWRITE = False # if False, skip blobs that already exist
UPLOAD_CHUNK_MB = 8 # chunk size for multipart uploads (in MB)



