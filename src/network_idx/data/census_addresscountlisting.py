"""
Census Address Count Listing (ACL) 2025 — Downloader

Downloads per-state address block count listing txt files from the Census Bureau.
Each file is pipe-delimited with housing unit and group quarters counts per block.

Filename pattern on Census FTP:
    {FIPS}_{StateName}_AddressBlockCountList_{MMYYYY}.txt

Strategy: try December 2025 (122025) first; fall back to July 2025 (072025).

Usage:
    python -m network_idx.data.census_addresscountlisting --states AL CA NY
    python -m network_idx.data.census_addresscountlisting --all
"""

import argparse
import logging
from pathlib import Path

import requests

from network_idx.constants import (
    CENSUS_ACL_2025_BASE_URL,
    CENSUS_ACL_STATE_NAMES,
    STATE_USPS_TO_FIPS,
)
from network_idx.config import RAW_DIR_CENSUS_ACL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# NOTE: Preferred vintage first, fallback second
VINTAGES = ["122025", "072025"]


def _build_filename(fips: str, state_name: str, vintage: str) -> str:
    return f"{fips}_{state_name}_AddressBlockCountList_{vintage}.txt"


def download_acl(
    states: list[str],
    output_dir: Path,
    overwrite: bool = False,
) -> list[Path]:
    """
    Download Census Address Count Listing txt files for the given states.

    Tries December 2025 first; if unavailable, falls back to July 2025.

    Parameters
    ----------
    states     : USPS codes, e.g. ["AL", "CA"].
    output_dir : Folder to save downloaded .txt files into.
    overwrite  : If False, skip files already on disk.

    Returns
    -------
    List of Paths to successfully downloaded files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []

    for i, state_usps in enumerate(states, 1):
        fips = STATE_USPS_TO_FIPS[state_usps]
        state_name = CENSUS_ACL_STATE_NAMES[state_usps]

        # Check if any vintage already exists on disk
        existing = [
            output_dir / _build_filename(fips, state_name, v)
            for v in VINTAGES
            if (output_dir / _build_filename(fips, state_name, v)).exists()
        ]
        if existing and not overwrite:
            logger.info(f"[{i}/{len(states)}] {existing[0].name} already exists. Skipping.")
            saved.append(existing[0])
            continue

        downloaded = False
        for vintage in VINTAGES:
            filename = _build_filename(fips, state_name, vintage)
            url = f"{CENSUS_ACL_2025_BASE_URL}{filename}"
            logger.info(f"[{i}/{len(states)}] Trying {filename}")

            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                dest = output_dir / filename
                dest.write_bytes(resp.content)
                logger.info(f"  Saved {dest.name} ({len(resp.content) / 1024:.0f} KB)")
                saved.append(dest)
                downloaded = True
                break # This vintage worked, no need to try older one
            else:
                logger.warning(f"  HTTP {resp.status_code} for {vintage} vintage")

        if not downloaded:
            logger.error(f"[{i}/{len(states)}] Failed to download ACL for {state_usps}")

    logger.info(f"Downloaded {len(saved)}/{len(states)} ACL files.")
    return saved


# ── CLI entry point

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download Census 2025 Address Count Listing files per state."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=["AL"],
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to download — one or more of: {list(STATE_USPS_TO_FIPS.keys())}",
    )
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Download for all states (overrides --states).",
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
    download_acl(states_to_download, RAW_DIR_CENSUS_ACL, overwrite=args.overwrite)