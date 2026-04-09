"""
Census Block Assignment File (BAF) 2020 — Downloader

Downloads per-state BAF zip files from the Census Bureau.
Each zip contains pipe-delimited text files mapping blocks to various
geographies (Place, County, Tract, etc.).

URL pattern:
    https://www2.census.gov/geo/docs/maps-data/data/baf2020/BlockAssignFile_2020_{FIPS}.zip

Usage:
    # Download for specific states
    python -m network_idx.data.census_baf --states AL CA NY

    # Download for all states
    python -m network_idx.data.census_baf --all
"""

import argparse
import logging
from pathlib import Path

import requests

from network_idx.constants import (
    CENSUS_BAF_2020_BASE_URL,
    STATE_USPS_TO_FIPS,
)
from network_idx.config import RAW_DIR_CENSUS_BAF

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def download_baf(
    states: list[str],
    output_dir: Path,
    overwrite: bool = False,
) -> list[Path]:
    """
    Download Census BAF 2020 zip files for the given states.

    Parameters
    ----------
    states     : USPS codes, e.g. ["AL", "CA"].
    output_dir : Folder to save downloaded .zip files into.
    overwrite  : If False, skip files already on disk.

    Returns
    -------
    List of Paths to successfully downloaded files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []

    # Enumerate allows a counter which is useful for printing progress
    # Docs: https://docs.python.org/3/library/functions.html#enumerate
    for i, state_usps in enumerate(states, start=1):
        fips = STATE_USPS_TO_FIPS[state_usps]
        url_filename = f"BlockAssign_ST{fips}_{state_usps}.zip"
        destination_filename = f"BlockAssign_ST{fips}_{state_usps}_2020.zip"
        dest = output_dir / destination_filename

        if dest.exists() and not overwrite:
            logger.info(f"[{i}/{len(states)}] {destination_filename} already exists. Skipping.")
            saved.append(dest)
            continue

        url = f"{CENSUS_BAF_2020_BASE_URL}{url_filename}"
        logger.info(f"[{i}/{len(states)}] Downloading {url}")

        resp = requests.get(url, timeout=120)
        if resp.status_code != 200:
            logger.error(f"Failed to download {url} — HTTP {resp.status_code}")
            continue

        dest.write_bytes(resp.content)
        logger.info(f"  Saved {dest} ({len(resp.content) / 1024:.0f} KB)")
        saved.append(dest)

    logger.info(f"Downloaded {len(saved)}/{len(states)} BAF files.")
    return saved


# ── CLI entry point

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download Census 2020 Block Assignment Files (BAF) per state."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=["AL"],
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to download — one or more of: {list(STATE_USPS_TO_FIPS.keys())}",
    )
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Download for all states (overrides --states)",
    )
    parser.add_argument(
        "--overwrite", action="store_true", default=False,
        help="Re-download even if the file already exists.",
    )
    args = parser.parse_args()

    valid = list(STATE_USPS_TO_FIPS.keys())
    bad = [s for s in args.states if s not in valid]
    if bad:
        logger.error(f"Invalid state codes: {bad}")
        exit(1)

    states_to_download = valid if args.all else args.states
    download_baf(states_to_download, RAW_DIR_CENSUS_BAF, overwrite=args.overwrite)