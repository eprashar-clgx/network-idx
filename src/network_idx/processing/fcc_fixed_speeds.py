"""
Pipeline per state:
1. 3 zip files (one per technology: Cable, Copper, Fiber) already exist in data/raw/fcc/speeds/
2. Unzip each file to extract CSV into data/extracted/fcc/speeds/{STATE}/
3. From each CSV, produce three aggregations:
   a. Block-level: aggregate to block_geoid with location counts, provider counts, and max speeds per technology
   b. Provider-block-level: aggregate to (block_geoid, provider) with location counts and max speeds per technology
   c. Provider-h3-level: aggregate to (h3_res8_id, provider) with location counts and max speeds per technology
4. Combine all technology CSVs per state, pivot by technology, and save as parquet

Output Schema — Block (one row per block_geoid, 15 columns):
    state_usps, state_fips, block_geoid,
    {tech}_location_count, {tech}_provider_count,
    {tech}_max_download_speed, {tech}_max_upload_speed
    ...repeated for cable/copper/fiber

Output Schema — Provider-Block (one row per provider-block combination, 15 columns):
    state_usps, state_fips, block_geoid, frn, provider_id, brand_name,
    {tech}_location_count, {tech}_max_download_speed, {tech}_max_upload_speed
    ...repeated for cable/copper/fiber

Output Schema — H3-Block (one row per provider-h3 combination, 15 columns):
    state_usps, state_fips, h3_res8_id, frn, provider_id, brand_name,
    {tech}_location_count, {tech}_max_download_speed, {tech}_max_upload_speed
    ...repeated for cable/copper/fiber

Output files (in data/processed/fcc/speeds/):
    fcc_fixed_speeds_{STATE}_{FIPS}.parquet
    fcc_fixed_speeds_providers_block_{STATE}_{FIPS}.parquet
    fcc_fixed_speeds_providers_h3_{STATE}_{FIPS}.parquet

Usage:
    # Process a single state (default: AL)
    python -m network_idx.processing.fcc_fixed_speeds

    # Process specific states
    python -m network_idx.processing.fcc_fixed_speeds --states AK AL CA

    # Process all states
    python -m network_idx.processing.fcc_fixed_speeds --all

    # Overwrite existing parquet files
    python -m network_idx.processing.fcc_fixed_speeds --all --overwrite
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
    FCC_FIXED_SPEED_OUTPUTS,
    FCC_FIXED_SPEEDS_PROVIDER_INPUTS,
    FCC_FIXED_SPEEDS_PROVIDER_OUTPUTS,
    FCC_FIXED_SPEEDS_PROVIDER_H3_INPUTS,
    FCC_FIXED_SPEEDS_PROVIDER_H3_OUTPUTS
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

# Loading csv and creating features
def load_csv_for_block_df(csv_path:Path) -> pd.DataFrame:
    """
    Loads the CSV file into a pandas DataFrame at a block-level.
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

# Features for provider-level analysis
def load_csv_for_providers_df(csv_path: Path) -> pd.DataFrame:
    """
    Loads the CSV file into a pandas DataFrame at a provider-block level.
    """
    logger.info(f"Loading CSV {csv_path} for provider-level analysis")
    df = pd.read_csv(csv_path,
                     usecols=FCC_FIXED_SPEEDS_PROVIDER_INPUTS,
                     dtype={
                         "state_usps": str,
                         "block_geoid": str,
                         "frn": str,
                         "provider_id": str,
                         "brand_name": str,
                         "location_id": str,
                         "technology": int,
                         "max_advertised_download_speed": float,
                         "max_advertised_upload_speed": float
                     })
    df = df[df["technology"].isin(FIXED_TECHNOLOGIES_MAPPING.values())].copy()
    df["technology_lbl"] = df["technology"].map(
        {v: k for k, v in FIXED_TECHNOLOGIES_MAPPING.items()}
    ).str.lower()
    return df

# Function to load CSV for provider-H3 level analysis
def load_csv_for_providers_h3_df(csv_path: Path) -> pd.DataFrame:
    """
    Loads the CSV file into a DataFrame for provider-H3 level analysis.
    """
    logger.info(f"Loading CSV {csv_path} for provider-H3 level analysis")
    df = pd.read_csv(csv_path,
                     usecols=FCC_FIXED_SPEEDS_PROVIDER_H3_INPUTS,
                     dtype={
                         "state_usps": str,
                         "h3_res8_id": str,
                         "frn": str,
                         "provider_id": str,
                         "brand_name": str,
                         "location_id": str,
                         "technology": int,
                         "max_advertised_download_speed": float,
                         "max_advertised_upload_speed": float
                     })
    df = df[df["technology"].isin(FIXED_TECHNOLOGIES_MAPPING.values())].copy()
    df["technology_lbl"] = df["technology"].map(
        {v: k for k, v in FIXED_TECHNOLOGIES_MAPPING.items()}
    ).str.lower()
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

    # Ensuring all 12 feature columns exist in the final dataframe
    for tech in FIXED_TECHNOLOGIES_MAPPING.keys():
        for metric in ["location_count", "provider_count", "max_download_speed", "max_upload_speed"]:
            col_name = f"{tech.lower()}_{metric}"
            if col_name not in pivoted.columns:
                pivoted[col_name] = pd.NA
    
    existing = [c for c in FCC_FIXED_SPEED_OUTPUTS if c in pivoted.columns]
    return pivoted[existing]

# Function to aggregate to provider-block level
def aggregate_to_provider_block(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Aggregating data to provider + block_geoid level")
    GROUP_COLS = ["state_usps","block_geoid", "frn", "provider_id", "brand_name", "technology_lbl"]
    agg = df.groupby(GROUP_COLS, as_index=False).agg(
        location_count=("location_id", "nunique"),
        max_download_speed=("max_advertised_download_speed", "max"),
        max_upload_speed=("max_advertised_upload_speed", "max"),
    )
    pivoted = agg.pivot_table(
        index=["state_usps","block_geoid","frn", "provider_id", "brand_name"],
        columns="technology_lbl",
        values=["location_count", "max_download_speed", "max_upload_speed"],
        aggfunc="first",
    )
    pivoted.columns = [f"{tech}_{metric}" for metric, tech in pivoted.columns]
    pivoted = pivoted.reset_index()

    # Add state_fips
    pivoted['state_fips'] = pivoted['state_usps'].map(STATE_USPS_TO_FIPS)

    # Ensure all expected output columns exist
    for col in FCC_FIXED_SPEEDS_PROVIDER_OUTPUTS:
        if col not in pivoted.columns:
            pivoted[col] = pd.NA

    existing = [c for c in FCC_FIXED_SPEEDS_PROVIDER_OUTPUTS if c in pivoted.columns]
    return pivoted[existing]

# Function to aggregate to provider-H3 level
def aggregate_to_provider_h3(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregates to provider + h3_res8_id level, pivoted by technology."""
    logger.info("Aggregating data to provider + h3_res8_id level")
    GROUP_COLS = ["state_usps", "h3_res8_id", "frn", "provider_id", "brand_name", "technology_lbl"]
    agg = df.groupby(GROUP_COLS, as_index=False).agg(
        location_count=("location_id", "nunique"),
        max_download_speed=("max_advertised_download_speed", "max"),
        max_upload_speed=("max_advertised_upload_speed", "max"),
    )
    pivoted = agg.pivot_table(
        index=["state_usps", "h3_res8_id", "frn", "provider_id", "brand_name"],
        columns="technology_lbl",
        values=["location_count", "max_download_speed", "max_upload_speed"],
        aggfunc="first",
    )
    pivoted.columns = [f"{tech}_{metric}" for metric, tech in pivoted.columns]
    pivoted = pivoted.reset_index()

    pivoted['state_fips'] = pivoted['state_usps'].map(STATE_USPS_TO_FIPS)

    for col in FCC_FIXED_SPEEDS_PROVIDER_H3_OUTPUTS:
        if col not in pivoted.columns:
            pivoted[col] = pd.NA

    existing = [c for c in FCC_FIXED_SPEEDS_PROVIDER_H3_OUTPUTS if c in pivoted.columns]
    return pivoted[existing]

# Function to process all files for a given state and save the final parquet
def process_state(
        state_usps:str,
        overwrite:bool=False
        ) -> tuple[Path | None, Path | None, Path | None]:
    """
    Process all files for a given state and save the final parquet.
    """
    logger.info(f"Processing state {state_usps}")
    fips = STATE_USPS_TO_FIPS[state_usps]
    out_block = PROCESSED_DIR_FCC_SPEEDS / f"fcc_fixed_speeds_{state_usps}_{fips}.parquet"
    out_provider_block = PROCESSED_DIR_FCC_SPEEDS / f"fcc_fixed_speeds_providers_block_{state_usps}_{fips}.parquet"
    out_provider_h3 = PROCESSED_DIR_FCC_SPEEDS / f"fcc_fixed_speeds_providers_h3_{state_usps}_{fips}.parquet"

    block_done = out_block.exists() and not overwrite
    provider_block_done = out_provider_block.exists() and not overwrite
    provider_h3_done = out_provider_h3.exists() and not overwrite

    if block_done and provider_block_done and provider_h3_done:
        logger.info(f"Both processed files for {state_usps} already exist. Skipping.")
        return out_block, out_provider_block, out_provider_h3
    
    zip_files = list(RAW_DIR_FCC_SPEEDS.glob(f"bdc_{fips}_*_fixed_broadband_*.zip"))

    if not zip_files:
        logger.warning(f"No zip files found for state {state_usps} (FIPS {fips}). Skipping.")
        return None, None, None
    
    logger.info(f"Found {len(zip_files)} zip files for state {state_usps}. Beginning processing.")
    block_dfs = []
    provider_block_dfs = []
    provider_h3_dfs = []

    for zip_file in zip_files:
        try:
            csv_path = extract_zip_file(zip_file, EXTRACTED_DIR_FCC_SPEEDS / state_usps)
            if not block_done:
                block_dfs.append(load_csv_for_block_df(csv_path))
            if not provider_block_done:
                provider_block_dfs.append(load_csv_for_providers_df(csv_path))
            if not provider_h3_done:
                provider_h3_dfs.append(load_csv_for_providers_h3_df(csv_path))
            logger.info(f"Successfully extracted {zip_file} for state {state_usps}")
        except Exception as e:
            logger.error(f"Error processing {zip_file}: {e}")
    
    out_block_path = None
    out_provider_block_path = None
    out_provider_h3_path = None

    # Block-level parquet
    if block_done:
        out_block_path = out_block
    elif block_dfs:
        combined = pd.concat(block_dfs, ignore_index=True)
        logger.info(f"Combined block df for {state_usps}: {len(combined)} rows. Aggregating...")
        block_features = aggregate_to_block_geoid(combined)
        logger.info(f"Block features for {state_usps}: {len(block_features)} rows. Saving...")
        out_block.parent.mkdir(parents=True, exist_ok=True)
        block_features.to_parquet(out_block, index=False)
        logger.info(f"Saved block-level data to {out_block}")
        out_block_path = out_block

    # Provider-level parquet
    if provider_block_done:
        out_provider_block_path = out_provider_block
    elif provider_block_dfs:
        combined = pd.concat(provider_block_dfs, ignore_index=True)
        logger.info(f"Combined provider df for {state_usps}: {len(combined)} rows. Aggregating...")
        provider_block_features = aggregate_to_provider_block(combined)
        logger.info(f"Provider features for {state_usps}: {len(provider_block_features)} rows. Saving...")
        out_provider_block.parent.mkdir(parents=True, exist_ok=True)
        provider_block_features.to_parquet(out_provider_block, index=False)
        logger.info(f"Saved provider-level data to {out_provider_block}")
        out_provider_block_path = out_provider_block
    
    # Provider-H3-level parquet
     # Provider-H3 parquet
    if provider_h3_done:
        out_provider_h3_path = out_provider_h3
    elif provider_h3_dfs:
        combined = pd.concat(provider_h3_dfs, ignore_index=True)
        logger.info(f"Combined provider-H3 df for {state_usps}: {len(combined)} rows. Aggregating...")
        provider_h3_features = aggregate_to_provider_h3(combined)
        logger.info(f"Provider-H3 features for {state_usps}: {len(provider_h3_features)} rows. Saving...")
        out_provider_h3.parent.mkdir(parents=True, exist_ok=True)
        provider_h3_features.to_parquet(out_provider_h3, index=False)
        logger.info(f"Saved provider-H3 data to {out_provider_h3}")
        out_provider_h3_path = out_provider_h3

    return out_block_path, out_provider_block_path, out_provider_h3_path

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