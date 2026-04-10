"""
Census Address Count Listing (ACL) 2025 — Processing

Reads per-state pipe-delimited txt files, standardises column names and
geographic identifiers, and saves per-state parquets.

Raw columns:
    STATE|COUNTY|TRACT|BLOCK|BLOCK_GEOID|TOTAL HOUSING UNITS|TOTAL GROUP QUARTERS

Output columns (per state, to data/processed/census/addcountlisting2025/):
    block_geoid, state_fips, state_usps, county_geoid, tract_geoid,
    total_housing_units, total_group_quarters

Output files:
    census_acl_{USPS}_{FIPS}.parquet

Usage:
    python -m network_idx.processing.census_addresscountlisting --states AL CA NY
    python -m network_idx.processing.census_addresscountlisting --all
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from network_idx.constants import (
    CENSUS_ACL_OUTPUTS,
    CENSUS_ACL_STATE_NAMES,
    STATE_USPS_TO_FIPS,
)
from network_idx.config import (
    PROCESSED_DIR_CENSUS_ACL,
    RAW_DIR_CENSUS_ACL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

FIPS_TO_USPS = {v: k for k, v in STATE_USPS_TO_FIPS.items()}


# ── Transformation

def load_and_transform(txt_path: Path, state_usps: str) -> pd.DataFrame:
    """
    Read a pipe-delimited ACL txt file, standardise columns, and derive
    geographic identifiers consistent with the rest of the pipeline.
    """
    df = pd.read_csv(
        txt_path,
        sep="|",
        dtype={"BLOCK_GEOID": str, "STATE": str, "COUNTY": str,
               "TRACT": str, "BLOCK": str,
               "TOTAL HOUSING UNITS": int, "TOTAL GROUP QUARTERS": int},
    )

    # Lowercase + underscore-normalise column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Derive standard GEOIDs from block_geoid (15 digits)
    df["state_fips"] = df["block_geoid"].str[:2]
    df["state_usps"] = state_usps
    df["county_geoid"] = df["block_geoid"].str[:5]
    df["tract_geoid"] = df["block_geoid"].str[:11]

    return df[[c for c in CENSUS_ACL_OUTPUTS if c in df.columns]]


# ── Per-state processing

def process_state(state_usps: str, overwrite: bool = False) -> Path | None:
    fips = STATE_USPS_TO_FIPS[state_usps]
    state_name = CENSUS_ACL_STATE_NAMES[state_usps]
    out_path = PROCESSED_DIR_CENSUS_ACL / f"census_acl_{state_usps}_{fips}.parquet"

    if out_path.exists() and not overwrite:
        logger.info(f"{out_path.name} already exists. Skipping.")
        return out_path

    # Find the downloaded txt (prefer 12 vintage over 07)
    txt_files = sorted(
        RAW_DIR_CENSUS_ACL.glob(f"{fips}_{state_name}_AddressBlockCountList_*.txt"),
        reverse=True,  # "122025" sorts after "072025"
    )
    if not txt_files:
        logger.warning(f"No ACL txt found for {state_usps} ({fips}). Skipping.")
        return None

    txt_path = txt_files[0]
    logger.info(f"Processing {txt_path.name}")

    df = load_and_transform(txt_path, state_usps)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    logger.info(f"Saved {out_path.name}  ({len(df):,} blocks)")
    return out_path


# ── CLI 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process Census 2025 Address Count Listing txt files into parquets."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=["AL"],
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to process: {list(STATE_USPS_TO_FIPS.keys())}",
    )
    parser.add_argument("--all", action="store_true", default=False, help="Process all states.")
    parser.add_argument("--overwrite", action="store_true", default=False, help="Overwrite existing files.")
    args = parser.parse_args()

    states_to_process = list(STATE_USPS_TO_FIPS.keys()) if args.all else args.states

    for i, state in enumerate(states_to_process, 1):
        logger.info(f"[{i}/{len(states_to_process)}] {state}")
        try:
            process_state(state, overwrite=args.overwrite)
        except Exception as e:
            logger.error(f"Failed {state}: {e}")