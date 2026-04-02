"""
Pipeline per state (FCC Fixed Broadband Coverage Summary):
1. 1 zip file per state already exists in data/raw/fcc/broadband_coverage
2. Unzip to extract CSV
3. Load CSV, filter and create features:
   - area_data_type == "Total"
   - biz_res == "R"
   - technology in ["Copper", "Cable", "Fiber"]
   - speed_100_20 as-is
   - less_than_100_20 = MAX(speed_02_02, speed_10_1, speed_25_3)
   - more_than_100_20 = MAX(speed_250_25, speed_1000_100)
4. Pivot by technology so each geography row has 9 feature columns (3 techs × 3 metrics)
5. Save as one parquet file per state
"""

import argparse
import logging
import re
import zipfile
from pathlib import Path
import pandas as pd
from network_idx.constants import (
    STATE_USPS_TO_FIPS,
    FCC_FIXED_COVERAGE_TECHNOLOGIES,
    FCC_FIXED_COVERAGE_INPUTS,
    FCC_FIXED_COVERAGE_OUTPUTS
    )
from network_idx.config import (
    RAW_DIR_FCC_BROADBAND_COVERAGE,
    EXTRACTED_DIR_FCC_BROADBAND_COVERAGE,
    PROCESSED_DIR_FCC_BROADBAND_COVERAGE
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
    # Create the extract directory if it doesn't exist
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
                     usecols=FCC_FIXED_COVERAGE_INPUTS,
                     dtype={
                         "area_data_type": str,
                         "geography_type": str,
                         "geography_id": int,
                         "geography_desc": str,
                         "geography_desc_full": str,
                         "total_units": int,
                         "biz_res": str,
                         "technology": str,
                         "speed_02_02":float,
                         "speed_10_1":float,
                         "speed_25_3":float,
                         "speed_100_20":float,
                         "speed_250_25":float,
                         "speed_1000_100":float,
                         })
    # Filters
    df = df[
        (df["area_data_type"] == "Total")
        & (df["biz_res"] == "R")
        & (df["technology"].isin(FCC_FIXED_COVERAGE_TECHNOLOGIES))
    ].copy()
    logger.info(f"After filtering: {len(df)} rows")
    return df

# Feature engineering
def create_coverage_features(df:pd.DataFrame) -> pd.DataFrame:
    """
    For each geography + technology row, compute:
      speed_100_20        — taken as-is
      less_than_100_20    — MAX(speed_02_02, speed_10_1, speed_25_3)
      more_than_100_20    — MAX(speed_250_25, speed_1000_100)
    Then pivot so each technology becomes its own set of columns.
    """
    # Create new features for less_than_100_20 and more_than_100_20
    df["less_than_100_20"] = df[["speed_02_02", "speed_10_1", "speed_25_3"]].max(axis=1)
    df["more_than_100_20"] = df[["speed_250_25", "speed_1000_100"]].max(axis=1)

    df["technology_lbl"] = df["technology"].str.lower()

    INDEX_COLS = ["geography_id", "geography_desc", "geography_desc_full", "total_units"]
    METRIC_COLS = ["speed_100_20", "less_than_100_20", "more_than_100_20"]

    pivoted = df.pivot_table(
        index=INDEX_COLS,
        columns="technology_lbl",
        values=METRIC_COLS,
        aggfunc="first",
    )

    # Flatten MultiIndex columns: e.g. ("speed_100_20", "copper") -> "copper_speed_100_20"
    pivoted.columns = [f"{tech}_{metric}" for metric, tech in pivoted.columns]
    pivoted = pivoted.reset_index()

    # Ensure all expected output columns exist
    for col in FCC_FIXED_COVERAGE_OUTPUTS:
        if col not in pivoted.columns:
            pivoted[col] = pd.NA

    existing = [c for c in FCC_FIXED_COVERAGE_OUTPUTS if c in pivoted.columns]
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
    out_path = PROCESSED_DIR_FCC_BROADBAND_COVERAGE / f"fcc_fixed_coverage_{state_usps}_{fips}.parquet"
    
    if out_path.exists() and not overwrite:
        logger.info(f"Processed file {out_path} already exists. Skipping processing for {state_usps}.")
        return out_path
    
    zip_files = list(RAW_DIR_FCC_BROADBAND_COVERAGE.glob(f"bdc_{fips}_*.zip"))
    if not zip_files:
        logger.warning(f"No zip files found for state {state_usps} (FIPS {fips}). Skipping.")
        return None
    
    logger.info(f"Found {len(zip_files)} zip files for state {state_usps}. Beginning processing.")
    # We know this table has just the one zip file so we can just take the first element    
    zip_file = zip_files[0]
    if len(zip_files) > 1:
        logger.warning(f"Multiple zip files found for state {state_usps} (FIPS {fips}). Processing only the first one: {zip_file.name}")
    csv_path = extract_zip_file(zip_file, EXTRACTED_DIR_FCC_BROADBAND_COVERAGE / state_usps)
    df = load_csv_to_df(csv_path)
    if df.empty:
        logger.warning(f"No data to process for state {state_usps} after loading CSV. Skipping.")
        return None
    
    logger.info(f"Created dataframe for {state_usps} with {len(df)} rows. Now creating features...")
    features = create_coverage_features(df)
    logger.info(f"Final dataframe for {state_usps} has {len(features)} rows and {len(features.columns)} columns. Saving to parquet...")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    features.to_parquet(out_path, index=False)
    logger.info(f"Saved processed data for {state_usps} to {out_path}")
    return out_path

# ── CLI entry point 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create place-level coverage features from FCC Fixed Broadband Summary data and save as parquet files."
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
        "--output-dir", type=Path, default=PROCESSED_DIR_FCC_BROADBAND_COVERAGE,
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