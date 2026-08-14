"""
Local filesystem paths for the network_idx package.

This module holds every on-disk directory the pipeline reads from or writes to on
the local machine, organised by the stage that owns them: raw downloads, extracted
archives, processed tables, and the intermediate feature outputs that are still
produced on disk. Keeping the paths together makes it easy to see the full local
data layout in one place.
"""
from pathlib import Path

# ── Raw downloads ─────────────────────────────────────────────────────────────
RAW_DIR = Path("data/raw")
RAW_DIR_FCC = Path("data/raw/fcc")
RAW_DIR_FCC_SPEEDS = Path("data/raw/fcc/speeds")
RAW_DIR_FCC_BROADBAND_COVERAGE = Path("data/raw/fcc/broadband_coverage")

# Census Block Assignment Files
RAW_DIR_CENSUS_BAF = Path("data/raw/census/baf2020")

# Census Address Count Listing
RAW_DIR_CENSUS_ACL = Path("data/raw/census/addcountlisting2025")

# ── Extracted archives ────────────────────────────────────────────────────────
EXTRACTED_DIR = Path("data/extracted")
EXTRACTED_DIR_FCC = Path("data/extracted/fcc")
EXTRACTED_DIR_FCC_SPEEDS = Path("data/extracted/fcc/speeds")
EXTRACTED_DIR_FCC_BROADBAND_COVERAGE = Path("data/extracted/fcc/broadband_coverage")

# Census Block Assignment Files
EXTRACTED_DIR_CENSUS_BAF = Path("data/extracted/census/baf2020")

# ── Processed tables ──────────────────────────────────────────────────────────
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR_FCC = Path("data/processed/fcc")
PROCESSED_DIR_FCC_SPEEDS = Path("data/processed/fcc/speeds")
PROCESSED_DIR_FCC_BROADBAND_COVERAGE = Path("data/processed/fcc/broadband_coverage")

# Census Block Assignment Files
PROCESSED_DIR_CENSUS_BAF = Path("data/processed/census/baf2020")

# Census Address Count Listing
PROCESSED_DIR_CENSUS_ACL = Path("data/processed/census/addcountlisting2025")

# ── Feature engineering outputs (still written to disk) ───────────────────────
FEATURES_DIR_FCC_SPEEDS_TRACT = Path("data/features/fcc/speeds/tract")

# Broadband coverage county residuals
FEATURES_DIR_FCC_COVERAGE_COUNTY_RESIDUALS = Path("data/features/fcc/broadband_coverage/county_residuals")

# Broadband coverage block and tract cuts
FEATURES_DIR_FCC_COVERAGE_BLOCK = Path("data/features/fcc/broadband_coverage/block")
FEATURES_DIR_FCC_COVERAGE_TRACT = Path("data/features/fcc/broadband_coverage/tract")
