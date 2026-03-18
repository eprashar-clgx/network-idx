"""
Pipeline per state:
1. 3 zip files already exist
2. Unzip each file to extract csv
3. Save csv to a new folder with a consistent naming convention
4. Load csv as a dataframe, aggregate to block_geoid and create features that are defined upfront
5. Do this for all technology types (copper, cable, fiber) and merge into one dataframe per state
6. Save as one parquet file per state  

Output Schema (one row per block_geoid, 12 feature columns):
- block_geoid
- state_usps
- state_fips
- {tech}_location_count
- {tech}_provider_count
- {tech}_max_download_speed
- {tech}_max_upload_speed
...repeated for cable/copper/fiber

Usage:

"""

import argparse
import logging
import re
import zipfile
from pathlib import Path
import pandas as pd
from network_idx.constants import (
    STATE_FIPS,
    FIXED_TECHNOLOGIES_MAPPING
)
from network_idx.config import (
    RAW_DIR_FCC_SPEEDS,
    EXTRACTED_DIR_FCC_SPEEDS,
    PROCESSED_DIR_FCC_SPEEDS
)

# Adding logging for better visibility into the process
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
    )
logger = logging.getLogger(__name__)

# Filename parsing
def parse_fips_from_filename(filename:str) -> str | None:
    """
    Extracts 2-digit FIPS code from the filename
    Pattern: bdc_{FIPS}_{Technology}_fixed_broadband_...
    """
    match = re.match(r"bdc_(\d{2})_", filename)
    return match.group(1) if match else None

def get_fips_from_dir(raw_dir:Path) ->list[str]:
    """
    Scans the raw directory for zip files and extracts unique FIPS codes.
    """
    fips_codes = set()
    for file in raw_dir.glob("*.zip"):
        fips = parse_fips_from_filename(file.name)
        if fips:
            fips_codes.add(fips)
        else:
            logger.warning(f"Filename {file.name} does not match expected pattern.")
    return sorted(fips_codes)

# Unzipping and processing
def extract_zip_file(zip_path:Path, extract_to:Path) -> Path:
    """
    Unzips the given file to the specified directory.
    Returns the path to the extracted CSV file.
    """
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        csv_names = [n for n in zip_ref.namelist() if n.endswith('.csv')]
        if not csv_names:
            raise FileNotFoundError(f"No CSV file found in {zip_path}")
        if len(csv_names) > 1:
            logger.warning(f"Multiple CSV files found in {zip_path}. Extracting the first one: {csv_names[0]}")
        csv_name = csv_names[0]
        dest = extract_to / csv_name
        if dest.exists():
            logger.info(f"CSV {dest} already exists. Skipping extraction.")
            return dest
        logger.info(f"Extracting {csv_name} from {zip_path} to {dest}")
        zip_ref.extract(csv_name, path=extract_to)
    return dest