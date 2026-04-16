"""
Feature engineering: aggregate block-level FCC speed data to census tract level.

Inputs (per state, from data/processed/fcc/speeds/):
    - fcc_fixed_speeds_{USPS}_{FIPS}.parquet        (block-level speeds)
    - fcc_fixed_speeds_providers_{USPS}_{FIPS}.parquet (block-level providers)

Processing:
    1. Derive tract_geoid = block_geoid[:11]
    2. From block parquet: SUM location counts, MAX speeds per tract
    3. From provider parquet: NUNIQUE provider_id per technology per tract
    4. Merge into one tract-level DataFrame

Output (per state, to data/features/fcc/speeds/tract/):
    - fcc_fixed_speeds_tract_{USPS}_{FIPS}.parquet

Usage:
    python -m network_idx.feature_engg.fcc_fixed_speeds_tract --states AK AL
    python -m network_idx.feature_engg.fcc_fixed_speeds_tract --all
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from network_idx.constants import (
    STATE_USPS_TO_FIPS,
    FCC_FIXED_SPEED_TRACT_OUTPUTS,
)
from network_idx.config import (
    PROCESSED_DIR_FCC_SPEEDS,
    FEATURES_DIR_FCC_SPEEDS_TRACT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TECHS = ["cable", "copper", "fiber"]


# Aggregation helpers

def aggregate_speeds_to_tract(
        block_df: pd.DataFrame
        ) -> pd.DataFrame:
    """
    SUM location counts, MAX speeds, grouped by tract_geoid.
    """
    # Create a tract identifier
    # Documentation: https://www.census.gov/programs-surveys/geography/guidance/geo-identifiers.html
    block_df["tract_geoid"] = block_df["block_geoid"].str[:11]

    # Define named aggregation dictionary
    agg_dict = {
        "state_usps": ("state_usps", "first"),
        "state_fips": ("state_fips", "first"),
    }
    for tech in TECHS:
        agg_dict[f"{tech}_location_count"] = (f"{tech}_location_count", "sum")
        agg_dict[f"{tech}_max_download_speed"] = (f"{tech}_max_download_speed", "max")
        agg_dict[f"{tech}_max_upload_speed"] = (f"{tech}_max_upload_speed", "max")

    tract_speeds = block_df.groupby("tract_geoid", as_index=False).agg(**agg_dict)
    return tract_speeds


def aggregate_providers_to_tract(
        provider_df: pd.DataFrame
        ) -> pd.DataFrame:
    """
    NUNIQUE provider_id per technology, grouped by tract_geoid.
    """
    # Create a tract identifier
    provider_df["tract_geoid"] = provider_df["block_geoid"].str[:11]

    # Initialize provider counts dataframe with unique tract IDs
    provider_counts = pd.DataFrame({"tract_geoid": provider_df["tract_geoid"].unique()})
    for tech in TECHS:
        loc_col = f"{tech}_location_count"
        tech_providers = (
            provider_df[provider_df[loc_col].notna()]
            .groupby("tract_geoid")["provider_id"]
            .nunique()
            .rename(f"{tech}_provider_count")
        )
        # Merge tech provider counts to unique tract ids to obtain the final dataframe
        provider_counts = provider_counts.merge(tech_providers, on="tract_geoid", how="left")

    return provider_counts


# ── Per-state orchestrator 

def process_state(
        state_usps: str, 
        overwrite: bool = False
        ) -> Path | None:
    # fips is needed for file extraction and naming of the final dataframe/parquet
    fips = STATE_USPS_TO_FIPS[state_usps]
    out_path = FEATURES_DIR_FCC_SPEEDS_TRACT / f"fcc_fixed_speeds_tract_{state_usps}_{fips}.parquet"

    if out_path.exists() and not overwrite:
        logger.info(f"Tract file for {state_usps} already exists. Skipping.")
        return out_path

    block_speeds_path = PROCESSED_DIR_FCC_SPEEDS / f"fcc_fixed_speeds_{state_usps}_{fips}.parquet"
    block_provider_path = PROCESSED_DIR_FCC_SPEEDS / f"fcc_fixed_speeds_providers_block_{state_usps}_{fips}.parquet"

    if not block_speeds_path.exists() or not block_provider_path.exists():
        logger.warning(f"Missing input parquets for {state_usps}. Skipping.")
        return None

    logger.info(f"Processing {state_usps}: loading block and provider parquets...")
    block_df = pd.read_parquet(block_speeds_path)
    provider_df = pd.read_parquet(block_provider_path)

    logger.info(f"{state_usps}: {len(block_df):,} block rows, {len(provider_df):,} provider rows")

    # Obtain tract-level location counts and max speeds
    tract_speeds = aggregate_speeds_to_tract(block_df)
    
    # Obtain tract-level provider counts
    provider_counts = aggregate_providers_to_tract(provider_df)

    # Merge speeds and provider counts into one dataframe
    tract_df = tract_speeds.merge(provider_counts, on="tract_geoid", how="left")

    # Ensure all output columns exist
    for col in FCC_FIXED_SPEED_TRACT_OUTPUTS:
        if col not in tract_df.columns:
            tract_df[col] = pd.NA

     # Cast provider counts to nullable integer to avoid float/int inconsistency across states
    for tech in TECHS:
        col = f"{tech}_provider_count"
        if col in tract_df.columns:
            tract_df[col] = tract_df[col].astype("Int64")

    tract_df = tract_df[[c for c in FCC_FIXED_SPEED_TRACT_OUTPUTS if c in tract_df.columns]]

    logger.info(f"{state_usps}: {len(tract_df):,} tract rows. Saving...")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tract_df.to_parquet(out_path, index=False)
    logger.info(f"Saved {out_path}")
    return out_path


# ── CLI 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Aggregate block-level FCC speed features to census tract level."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=["AL"],
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to process: {list(STATE_USPS_TO_FIPS.keys())}",
    )
    parser.add_argument("--all", action="store_true", default=False, help="Process all states")
    parser.add_argument("--overwrite", action="store_true", default=False, help="Overwrite existing files")
    args = parser.parse_args()

    valid = list(STATE_USPS_TO_FIPS.keys())
    bad = [s for s in args.states if s not in valid]
    if bad:
        logger.error(f"Invalid state codes: {bad}")
        exit(1)

    states_to_process = valid if args.all else args.states
    logger.info(f"Processing states: {states_to_process}")

    for state in states_to_process:
        try:
            process_state(state, overwrite=args.overwrite)
        except Exception as e:
            logger.error(f"Error processing {state}: {e}")