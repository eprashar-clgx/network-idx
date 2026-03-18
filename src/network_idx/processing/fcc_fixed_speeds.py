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
    STATE_USPS_TO_FIPS,
    FIXED_TECHNOLOGIES_MAPPING,
    FCC_FIXED_SPEED_INPUTS,
    FCC_FIXED_SPEED_OUTPUTS
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

# Loading
def load_csv_to_df(csv_path:Path) -> pd.DataFrame:
    """
    Loads the CSV file into a pandas DataFrame.
    """
    logger.info(f"Loading CSV {csv_path} into DataFrame")
    df = pd.read_csv(csv_path,
                     usecols=FCC_FIXED_SPEED_INPUTS,
                     dtype={
                         "block_geoid": str,
                         "location_id": str,
                         "provider_id": str,
                         "state_usps": str,
                         "technology": int,
                         "max_advertised_download_speed": float,
                         "max_advertised_upload_speed": float
                     })
    df = df[df["technology"].isin(FIXED_TECHNOLOGIES_MAPPING.values())].copy()
    df['technology_lbl'] = df['technology'].map({v: k for k, v in FIXED_TECHNOLOGIES_MAPPING.items()}).str.lower()
    return df

# Aggregation
def aggregate_to_block_geoid(df:pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates the DataFrame to block_geoid level and creates the required features.
    """
    logger.info("Aggregating data to block_geoid level")
    GROUP_COLS = ["block_geoid", "state_usps", "technology_lbl"]
    agg = df.groupby(GROUP_COLS, as_index=False).agg(
        location_count = ("location_id", "nunique"),
        provider_count = ("provider_id", "nunique"),
        max_download_speed = ("max_advertised_download_speed", "max"),
        max_upload_speed = ("max_advertised_upload_speed", "max")
    )
    # Pivot to get separate columns for each technology
    pivoted = agg.pivot_table(
        index=["block_geoid", "state_usps"],
        columns="technology_lbl",
        values=["location_count", "provider_count", "max_download_speed", "max_upload_speed"],
        aggfunc='first'
    )
    
    # Flatten MultiIndex columns
    pivoted.columns = [f"{tech}_{metric}" for metric, tech in pivoted.columns]
    pivoted = pivoted.reset_index()

    # Add state_fips
    pivoted['state_fips'] = pivoted['state_usps'].map(STATE_USPS_TO_FIPS)

    # Ensuring all 12 feature columns exist
    for tech in FIXED_TECHNOLOGIES_MAPPING.keys():
        for metric in ["location_count", "provider_count", "max_download_speed", "max_upload_speed"]:
            col_name = f"{tech}_{metric}"
            if col_name not in pivoted.columns:
                pivoted[col_name] = pd.NA
    
    existing = [c for c in FCC_FIXED_SPEED_OUTPUTS if c in pivoted.columns]
    return pivoted[existing]

# Function to process all files for a given state and save the final parquet
def process_state(
        state_usps:str,
        overwrite:bool=False
        ) -> Path | None:
    """
    Process all files for a given state and save the final parquet.
    """
    logger.info(f"Processing state {state_usps}")
    fips = STATE_USPS_TO_FIPS[state_usps]
    out_path = PROCESSED_DIR_FCC_SPEEDS / f"fcc_fixed_speeds_{state_usps}_{fips}.parquet"
    if out_path.exists() and not overwrite:
        logger.info(f"Processed file {out_path} already exists. Skipping processing for {state_usps}.")
        return out_path
    zip_files = list(RAW_DIR_FCC_SPEEDS.glob(f"bdc_{fips}_*_fixed_broadband_*.zip"))
    if not zip_files:
        logger.warning(f"No zip files found for state {state_usps} (FIPS {fips}). Skipping.")
        return None
    logger.info(f"Found {len(zip_files)} zip files for state {state_usps}. Beginning processing.")
    dfs = []
    for zip_file in zip_files:
        try:
            csv_path = extract_zip_file(zip_file, EXTRACTED_DIR_FCC_SPEEDS / state_usps)
            df = load_csv_to_df(csv_path)
            logger.info(f"Loaded data for {state_usps} from {csv_path} with {len(df)} rows. Aggregating...")
            dfs.append(df)
        except Exception as e:
            logger.error(f"Error processing {zip_file}: {e}")
    if not dfs:
        logger.warning(f"No valid dataframes created for state {state_usps}. Skipping saving.")
        return None
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined dataframe for {state_usps} has {len(combined_df)} rows. Aggregating to block_geoid...")
    block_features = aggregate_to_block_geoid(combined_df)
    logger.info(f"[Aggregation complete] Final dataframe for {state_usps} has {len(block_features)} rows. Saving to parquet...")
    # Create output directory if it doesn't exist
    out_path.parent.mkdir(parents=True, exist_ok=True)
    block_features.to_parquet(out_path, index=False)
    logger.info(f"Saved processed data for {state_usps} to {out_path}")
    return out_path

# ── CLI entry point 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create block-level features from FCC Fixed Broadband Availability data and save as parquet files."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=["AL"],
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to process - one or more of: {list(STATE_USPS_TO_FIPS.keys())}"
        )
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Process data for all states (overrides --states)"
        )
    parser.add_argument(
        "--overwrite", action="store_true", default=False,
        help="Whether to overwrite existing processed files. Defaults to False."
    )
    parser.add_argument(
        "--output-dir", type=Path, default=PROCESSED_DIR_FCC_SPEEDS,
        help="Data directory to save processed parquet files into. Defaults to 'data/processed/fcc/speeds'"
    )
    args = parser.parse_args()
    valid = list(STATE_USPS_TO_FIPS.keys())
    bad = [s for s in args.states if s not in valid]
    if bad:
        logger.error(f"Invalid state USPS codes: {bad}. Must be one or more of: {list(valid)}")
        exit(1)
    states_to_process = valid if args.all else args.states
    logger.info(f"Starting processing for states: {states_to_process}")
    for state in states_to_process:
        try:
            process_state(state, overwrite=args.overwrite)
        except Exception as e:
            logger.error(f"Error processing state {state}: {e}")