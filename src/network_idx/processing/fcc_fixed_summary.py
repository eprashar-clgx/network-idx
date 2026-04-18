"""
Pipeline for FCC Fixed Broadband Coverage Summary:

Place (per-state):
1. 1 zip file per state already exists in data/raw/fcc/broadband_coverage
2. Unzip to extract CSV
3. Load CSV, filter and create features
4. Pivot by technology so each geography row has 9 feature columns (3 techs × 3 metrics)
5. Save as one parquet file per state

County (nationwide):
1. 1 nationwide zip file in data/raw/fcc/broadband_coverage
2. Unzip to extract CSV
3. Load CSV, filter to geography_type == "County" and create features
4. Split by state FIPS prefix, pivot, and save one parquet per state

Common filters/features:
   - area_data_type == "Total"
   - biz_res == "R"
   - technology in ["Copper", "Cable", "Fiber"]
   - speed_100_20 as-is
   - other speeds as MECE buckets (e.g. speed_10_1_only = at least 10/1 Mbps but not 25/3 Mbps)
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

# Reverse mapping: FIPS -> USPS
FIPS_TO_USPS = {v: k for k, v in STATE_USPS_TO_FIPS.items()}

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
def load_csv_to_df(
        csv_path:Path,
        geography_type:str | None=None
        ) -> pd.DataFrame:
    """
    Loads the CSV file into a pandas DataFrame.
    If geography_type is provided, filters to that geography_type (e.g. "County").
    """
    logger.info(f"Loading CSV {csv_path} into DataFrame")
    df = pd.read_csv(csv_path,
                     usecols=FCC_FIXED_COVERAGE_INPUTS,
                     dtype={
                         "area_data_type": str,
                         "geography_type": str,
                         "geography_id": str,
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
    mask = (
        (df["area_data_type"] == "Total")
        & (df["biz_res"] == "R")
        & (df["technology"].isin(FCC_FIXED_COVERAGE_TECHNOLOGIES))
    )
    if geography_type:
        mask = mask & (df["geography_type"] == geography_type)

    df = df[mask].copy()
    logger.info(f"After filtering: {len(df)} rows")
    return df

# Feature engineering
def create_coverage_features(df:pd.DataFrame) -> pd.DataFrame:
    """
    For each geography + technology row, compute MECE buckets:
      - speed_02_02_only: at least 0.2/0.2 Mbps but not 10/1 Mbps
      - speed_10_1_only: at least 10/1 Mbps but not 25/3 Mbps
      - speed_25_3_only: at least 25/3 Mbps but not 100/20 Mbps
      - speed_100_20_only: at least 100/20 Mbps but not 250/25 Mbps
      - speed_250_25_only: at least 250/25 Mbps but not 1000/100 Mbps
      - speed_1000_100_only: at least 1000/100 Mbps
    Then pivot so each technology becomes its own set of columns.
    """
    df = df.copy()
    df["speed_02_02_only"] = df["speed_02_02"] - df["speed_10_1"]
    df["speed_10_1_only"] = df["speed_10_1"] - df["speed_25_3"]
    df["speed_25_3_only"] = df["speed_25_3"] - df["speed_100_20"]
    df["speed_100_20_only"] = df["speed_100_20"] - df["speed_250_25"]
    df["speed_250_25_only"] = df["speed_250_25"] - df["speed_1000_100"]
    df["speed_1000_100_only"] = df["speed_1000_100"]

    df["technology_lbl"] = df["technology"].str.lower()

    INDEX_COLS = ["geography_id", "geography_desc", "geography_desc_full", "total_units"]
    METRIC_COLS = [
        "speed_02_02_only",
        "speed_10_1_only",
        "speed_25_3_only",
        "speed_100_20_only",
        "speed_250_25_only",
        "speed_1000_100_only"
    ]

    pivoted = df.pivot_table(
        index=INDEX_COLS,
        columns="technology_lbl",
        values=METRIC_COLS,
        aggfunc="first",
    )

    # Flatten MultiIndex columns: e.g. ("speed_02_02_only", "copper") -> "copper_speed_02_02_only"
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

# ── County processing (nationwide zip file) 
def process_county(
        states_to_process:list[str],
        overwrite:bool=False
        ) -> list[Path]:
    """
    Process county-level coverage from the nationwide zip file.
    Extracts CSV once, filters to County rows, splits by state, and saves
    one parquet per state.
    """
    # Find the nationwide zip
    zip_files = list(RAW_DIR_FCC_BROADBAND_COVERAGE.glob("bdc_us_fixed_broadband_summary_by_geography_*.zip"))
    if not zip_files:
        logger.error("No nationwide zip file (bdc_us_*) found in raw directory. Run the downloader with --geography other first.")
        return []
    zip_file = zip_files[0]
    if len(zip_files) > 1:
        logger.warning(f"Multiple nationwide zips found. Using: {zip_file.name}")

    # Extract
    csv_path = extract_zip_file(zip_file, EXTRACTED_DIR_FCC_BROADBAND_COVERAGE / "us")

    # Load with County filter
    df = load_csv_to_df(csv_path, geography_type="County")
    if df.empty:
        logger.warning("No County rows found after filtering. Nothing to process.")
        return []

    # Derive state FIPS from the first 2 digits of geography_id (5-digit county FIPS)
    df["state_fips"] = df["geography_id"].str[:2]

    # Convert requested USPS codes to FIPS for filtering
    requested_fips = {STATE_USPS_TO_FIPS[s] for s in states_to_process}

    output_paths: list[Path] = []
    PROCESSED_DIR_FCC_BROADBAND_COVERAGE.mkdir(parents=True, exist_ok=True)

    for state_fips, state_df in df.groupby("state_fips"):
        if state_fips not in FIPS_TO_USPS:
            logger.warning(f"Unknown state FIPS {state_fips} in county data. Skipping.")
            continue
        if state_fips not in requested_fips:
            continue

        state_usps = FIPS_TO_USPS[state_fips]
        out_path = PROCESSED_DIR_FCC_BROADBAND_COVERAGE / f"fcc_fixed_coverage_county_{state_usps}_{state_fips}.parquet"

        if out_path.exists() and not overwrite:
            logger.info(f"{out_path} already exists. Skipping {state_usps}.")
            output_paths.append(out_path)
            continue

        # Drop the helper column before feature engineering
        state_df = state_df.drop(columns=["state_fips"])

        logger.info(f"Processing county data for {state_usps} ({len(state_df)} rows)")
        features = create_coverage_features(state_df)
        logger.info(f"County features for {state_usps}: {len(features)} rows, {len(features.columns)} columns")
        features.to_parquet(out_path, index=False)
        logger.info(f"Saved {out_path}")
        output_paths.append(out_path)

    return output_paths

# ── CLI entry point 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create coverage features from FCC Fixed Broadband Summary data and save as parquet files."
    )
    parser.add_argument(
        "--geography", type=str, default="place",
        choices=["place", "county"],
        help="Geography level to process: 'place' (per-state zips) or 'county' (nationwide zip)."
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
    args = parser.parse_args()
    valid = list(STATE_USPS_TO_FIPS.keys())
    bad = [s for s in args.states if s not in valid]
    if bad:
        logger.error(f"Invalid state USPS codes: {bad}. Must be one or more of: {list(valid)}")
        exit(1)
    states_to_process = valid if args.all else args.states
    logger.info(f"Starting {args.geography} processing for states: {states_to_process}")

    if args.geography == "place":
        for state in states_to_process:
            try:
                process_state(state, overwrite=args.overwrite)
            except Exception as e:
                logger.error(f"Error processing state {state}: {e}")
    else:
        try:
            process_county(states_to_process, overwrite=args.overwrite)
        except Exception as e:
            logger.error(f"Error processing county data: {e}")