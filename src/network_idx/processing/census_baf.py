"""
Census Block Assignment File (BAF) 2020 — Processing

Extracts INCPLACE_CDP txt files from per-state BAF zips, derives geographic
identifiers from the 15-digit BLOCKID, and saves per-state parquets plus a
national reference file.

Derived columns from BLOCKID:
    state_fips   = BLOCKID[:2]
    county_geoid = BLOCKID[:5]
    tract_geoid  = BLOCKID[:11]
    block_geoid  = BLOCKID  (full 15 digits)
    place_geoid  = state_fips + PLACEFP  (null when PLACEFP is 99999 or NaN)

Output files (in data/processed/census/baf2020/):
    census_baf_AL_01.parquet
    ...
    census_baf_national.parquet   (--national flag)

Usage:
    python -m network_idx.processing.census_baf --states AL CA NY
    python -m network_idx.processing.census_baf --all
    python -m network_idx.processing.census_baf --all --national
"""

import argparse
import logging
import zipfile
from pathlib import Path

import pandas as pd

from network_idx.constants import (
    CENSUS_BAF_OUTPUTS,
    STATE_USPS_TO_FIPS,
)
from network_idx.config import (
    EXTRACTED_DIR_CENSUS_BAF,
    PROCESSED_DIR_CENSUS_BAF,
    RAW_DIR_CENSUS_BAF,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

FIPS_TO_USPS = {v: k for k, v in STATE_USPS_TO_FIPS.items()}


# ── Extraction

def extract_place_txt(zip_path: Path, extract_dir: Path) -> Path:
    """
    Extract the INCPLACE_CDP txt file from a BAF zip.

    Returns the path to the extracted file.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        matches = [n for n in zf.namelist() if "INCPLACE_CDP" in n and n.endswith(".txt")]
        if not matches:
            raise FileNotFoundError(f"No INCPLACE_CDP txt found in {zip_path}")
        txt_name = matches[0]
        dest = extract_dir / txt_name
        if dest.exists():
            logger.info(f"{dest.name} already extracted. Skipping.")
            return dest
        zf.extract(txt_name, path=extract_dir)
        logger.info(f"Extracted {txt_name}")
    return dest

# ── Transformation 

def build_crosswalk(txt_path: Path) -> pd.DataFrame:
    """
    Read a BAF INCPLACE_CDP pipe-delimited txt and derive geo columns.

    PLACEFP == 99999 or NaN → block is not inside any Census place;
    place_geoid is set to null for these rows.
    """
    df = pd.read_csv(txt_path, sep="|", dtype=str)

    # Standardise column names (some files use BLOCKID, others blockid)
    df.columns = df.columns.str.upper()

    df = df.rename(columns={"BLOCKID": "block_geoid", "PLACEFP": "place_fp"})

    # Derive geographic identifiers
    df["state_fips"] = df["block_geoid"].str[:2]
    df["county_geoid"] = df["block_geoid"].str[:5]
    df["tract_geoid"] = df["block_geoid"].str[:11]

    # Build full place_geoid (state_fips + place_fp); null out non-place blocks
    df["place_geoid"] = df["state_fips"] + df["place_fp"]
    not_in_place = df["place_fp"].isna() | (df["place_fp"] == "99999")
    df.loc[not_in_place, "place_geoid"] = None

    return df[CENSUS_BAF_OUTPUTS]


# ── Per-state processing 

def process_state(state_usps: str, overwrite: bool = False) -> Path | None:
    """
    Extract + transform the BAF for one state. Returns path to saved parquet.
    """
    fips = STATE_USPS_TO_FIPS[state_usps]
    out_path = PROCESSED_DIR_CENSUS_BAF / f"census_baf_{state_usps}_{fips}.parquet"

    if out_path.exists() and not overwrite:
        logger.info(f"{out_path.name} already exists. Skipping.")
        return out_path

    # Locate the downloaded zip
    zip_pattern = f"BlockAssign_ST{fips}_{state_usps}*.zip"
    zips = list(RAW_DIR_CENSUS_BAF.glob(zip_pattern))
    if not zips:
        logger.warning(f"No BAF zip found for {state_usps} ({fips}). Skipping.")
        return None

    zip_path = zips[0]
    txt_path = extract_place_txt(zip_path, EXTRACTED_DIR_CENSUS_BAF)
    df = build_crosswalk(txt_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    logger.info(f"Saved {out_path.name}  ({len(df):,} blocks)")
    return out_path


# ── National file ─────────────────────────────────────────────────────────────

def build_national(overwrite: bool = False) -> Path | None:
    """
    Concatenate all per-state parquets into a single national reference file.
    """
    out_path = PROCESSED_DIR_CENSUS_BAF / "census_baf_national.parquet"

    if out_path.exists() and not overwrite:
        logger.info(f"{out_path.name} already exists. Skipping.")
        return out_path

    parquets = sorted(PROCESSED_DIR_CENSUS_BAF.glob("census_baf_??_??.parquet"))
    if not parquets:
        logger.error("No per-state parquets found. Run state processing first.")
        return None

    logger.info(f"Concatenating {len(parquets)} state parquets into national file...")
    dfs = [pd.read_parquet(p) for p in parquets]
    national = pd.concat(dfs, ignore_index=True)
    national.to_parquet(out_path, index=False)
    logger.info(f"Saved {out_path.name}  ({len(national):,} blocks)")
    return out_path


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process Census 2020 BAF zips into block-place crosswalk parquets."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=["AL"],
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to process — one or more of: {list(STATE_USPS_TO_FIPS.keys())}",
    )
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Process all states (overrides --states).",
    )
    parser.add_argument(
        "--national", action="store_true", default=False,
        help="Also build the national reference parquet after state processing.",
    )
    parser.add_argument(
        "--overwrite", action="store_true", default=False,
        help="Re-process even if parquet already exists.",
    )
    args = parser.parse_args()

    states_to_process = list(STATE_USPS_TO_FIPS.keys()) if args.all else args.states

    for i, state in enumerate(states_to_process, 1):
        logger.info(f"[{i}/{len(states_to_process)}] Processing {state}")
        try:
            process_state(state, overwrite=args.overwrite)
        except Exception as e:
            logger.error(f"Failed to process {state}: {e}")

    if args.national:
        build_national(overwrite=args.overwrite)