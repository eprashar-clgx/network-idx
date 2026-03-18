"""
Configuration for network_idx package that will potentially be environment-specific. 
For example, file paths, API keys, or other settings that may differ within dev (local/remote) or across staging/production environments.
"""
from pathlib import Path

# ── File Paths ────────────────────────────────────────────────────────────────
RAW_DIR = Path("data/raw")
RAW_DIR_FCC = Path("data/raw/fcc")
RAW_DIR_FCC_SPEEDS = Path("data/raw/fcc/speeds")

EXTRACTED_DIR = Path("data/extracted")
EXTRACTED_DIR_FCC = Path("data/extracted/fcc")
EXTRACTED_DIR_FCC_SPEEDS = Path("data/extracted/fcc/speeds")

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR_FCC = Path("data/processed/fcc")
PROCESSED_DIR_FCC_SPEEDS = Path("data/processed/fcc/speeds")


