"""
FCC Broadband Map — Playwright Downloader

Downloads FCC Fixed Broadband Summary zip files by geography type.

Geography types:
    census-place : One zip per state (Census Place level data)
    other        : One nationwide zip (County, Congressional District, Tribal Areas, CBSA)

Usage:
    # Download Census Place files for specific states
    python -m network_idx.data.fcc_fixed_summary --geography census-place --states Alabama Alaska

    # Download Census Place files for all states
    python -m network_idx.data.fcc_fixed_summary --geography census-place --all

    # Download the Other Geographies file (county, congressional district, etc.)
    python -m network_idx.data.fcc_fixed_summary --geography other
"""

import argparse
import logging
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from network_idx.constants import (
    FCC_URL,
    STATE_FIPS
    )
from network_idx.config import RAW_DIR_FCC_BROADBAND_COVERAGE

# Adding logging for better visibility into the process
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
    )
logger = logging.getLogger(__name__)

def download_fcc_fixed_summary_place(
    states: list[str],
    output_dir: Path,
    overwrite: bool = False,
    headless: bool = True,
    pause_seconds: float = 2.0,
) -> list[Path]:
    """
    Download FCC Fixed Broadband Availability files using a headless browser.

    Parameters
    ----------
    states        : State/territory names. Defaults to Alabama.                    
    output_dir    : Folder to save downloaded .zip files into.
    overwrite     : If False, skip combinations already saved to disk.
    headless      : If False, opens a visible browser window (useful for debugging).
    pause_seconds : Wait between state iterations (be polite to the server).

    Returns
    -------
    List of Paths to successfully downloaded files.
    """
    # Check if output directory exists, if not create it
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define tasks / number of files to download upfront
    total   = len(states) 
    saved   = []
    counter = 0

    # Log the download configuration
    logger.info("FCC Broadband Playwright Downloader")
    logger.info(f"  States / Total Files      : {len(states)}")

    with sync_playwright() as p:
        # args specifically added to run it on the VM without getting blocked by the server.
        browser = p.chromium.launch(
            headless=headless, 
            channel="chrome",
            args=[
                "--disable-http2", 
                "--disable-blink-features=AutomationControlled"
                ]
                )
        # user_agent specifically added to run it on the VM without getting blocked by the server.
        context = browser.new_context(
            accept_downloads=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
            )
        page = context.new_page()

        logger.info("  Loading FCC page...")
        page.goto(FCC_URL, wait_until="domcontentloaded", timeout=60_000)
        
        # Wait for the dropdown to be present
        page.wait_for_selector("select#fixedCensusPlaceState", timeout=15_000)
        logger.info("FCC page loaded successfully.")

        # Scrape state - ID mapping from the live dropdown
        # Instead of hardcoding mapping from constants.py
        # Beautiful and elegant solution! 
        options = page.query_selector_all("select#fixedCensusPlaceState option[value]:not([value=''])")
        census_place_ids = {}
        for opt in options:
            val = opt.get_attribute("value")
            text = opt.inner_text().strip()
            if val and text:
                census_place_ids[text] = val
        logger.info(f"  Scraped {len(census_place_ids)} state IDs from dropdown")

        for state in states:
            fips = STATE_FIPS[state]
            # No need to import any mapping that changes
            census_place_id = census_place_ids[state]
            safe = state.replace(" ", "_")
            
            # Guard: ensure the state was found in the live dropdown
            if state not in census_place_ids:
                logger.error(f"  State '{state}' not found in dropdown. Skipping.")
                continue
            census_place_id = census_place_ids[state]
            
            logger.info(f"Processing state: {state} (FIPS: {fips})")

            # Check if "a" file exists for this state
            # Note that in case we want to download a newer version without over-writing older versions
            # We should add a timestamp in the glob pattern too
            if not overwrite and any(output_dir.glob(f"bdc_{fips}_fixed_broadband_summary*")):
                logger.info(f"  [{counter}/{total}] Skipping {state} (already exists)")
                continue

            try:
                # Select the state
                page.select_option("select#fixedCensusPlaceState", value=census_place_id)
                # Force Angular change detection
                page.eval_on_selector(
                    "select#fixedCensusPlaceState",
                    "el => el.dispatchEvent(new Event('change', { bubbles: true }))"
                    )
                # Wait for the table rows to appear
                # TODO: check if we still need this
                page.wait_for_selector(
                    "tr td.align-middle",
                    timeout=15_000,
                )
                page.wait_for_timeout(1000)
            except PlaywrightTimeout:
                logger.error(f"Timeout while loading data for {state}. Skipping to next state.")
                continue

            logger.info(f"[{counter}/{total}] Downloading {state}...")

            try:
                row_button = page.locator(
                    "button:not([disabled]):has(span.sr-only:text('Download zipped Census Place file'))"
                    )
                row_button.wait_for(state="visible", timeout=10_000)

                with page.expect_download(timeout=300_000) as dl_info:
                    row_button.click()

                download = dl_info.value
                suggested = download.suggested_filename
                dest = output_dir / (suggested if suggested else f"{safe}__{fips}_broadband_fixed_summary.zip")

                download.save_as(dest)
                size_mb = dest.stat().st_size / (1024 * 1024)
                logger.info(f"    Downloaded '{dest.name}' ({size_mb:.2f} MB)")
                saved.append(dest)
                counter += 1

            except PlaywrightTimeout:
                logger.error(f"    Timeout while trying to download {state}. Skipping this technology.")
            except Exception as e:
                logger.error(f"    Error while downloading {state} {e}")

            if state != states[-1]:
                time.sleep(pause_seconds)

        browser.close()

    logger.info(f"\nDownload complete. {len(saved)}/{total} files saved to '{output_dir}'.")
    return saved


# Other geographies (county, state, CBSA)
def download_fcc_fixed_summary_other(
    output_dir: Path,
    overwrite: bool = False,
    headless: bool = True,
) -> Path | None:
    """
    Download the FCC Fixed Broadband Summary — Other Geographies file.
    This is a single nationwide zip containing County, Congressional District,
    Tribal Areas, and CBSA (MSA) data.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Check if file already exists
    existing = list(output_dir.glob("bdc_us_fixed_broadband_summary_by_geography*"))
    if not overwrite and existing:
        logger.info(f"Other Geographies file already exists: {existing[0].name}. Skipping.")
        return existing[0]

    logger.info("FCC Broadband Playwright Downloader — Other Geographies")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            channel="chrome",
            args=[
                "--disable-http2",
                "--disable-blink-features=AutomationControlled"
            ]
        )
        context = browser.new_context(
            accept_downloads=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()

        logger.info("  Loading FCC page...")
        page.goto(FCC_URL, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(3000)
        logger.info("  FCC page loaded.")

        try:
            row_button = page.locator(
                "button:not([disabled]):has(span.sr-only:text('Download zipped Fixed Broadband Summary by Geography Type - Other Geographies file'))"
            )
            row_button.wait_for(state="visible", timeout=15_000)
            logger.info("  Found Other Geographies download button. Clicking...")

            with page.expect_download(timeout=300_000) as dl_info:
                row_button.click()

            download = dl_info.value
            suggested = download.suggested_filename
            dest = output_dir / (suggested if suggested else "bdc_us_fixed_broadband_summary_by_geography_other.zip")

            download.save_as(dest)
            size_mb = dest.stat().st_size / (1024 * 1024)
            logger.info(f"  Downloaded '{dest.name}' ({size_mb:.2f} MB)")
            browser.close()
            return dest

        except PlaywrightTimeout:
            logger.error("  Timeout while trying to download Other Geographies file.")
            browser.close()
            return None
        except Exception as e:
            logger.error(f"  Error downloading Other Geographies file: {e}")
            browser.close()
            return None


# ── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download FCC Fixed Broadband Summary files using a headless browser."
    )

    parser.add_argument(
        "--geography", type=str, default="census-place",
        choices=["census-place", "other"],
        help="Geography type to download: 'census-place' (per-state) or 'other' (nationwide county/CD/tribal/CBSA)."
    )

    parser.add_argument(
        "--states", type=str, nargs="+", default=["Alabama"],
        choices=STATE_FIPS.keys(),
        metavar="STATE",
        help=f"States for download (census-place only) - one or more of: {list(STATE_FIPS.keys())}"
    )

    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Download data for all states (overrides --states, census-place only)"
    )

    parser.add_argument(
        "--overwrite", action="store_true", default=False,
        help="Overwrite existing files. Defaults to False."
    )

    parser.add_argument(
        "--output-dir", type=Path, default=RAW_DIR_FCC_BROADBAND_COVERAGE,
        help="Data directory to dump .zip files into. Defaults to 'data/raw/fcc/broadband_coverage'"
    )

    HEADLESS = True  # Set to True to run without opening a browser window
    args = parser.parse_args()

    if args.geography == "census-place":
        states_to_process = list(STATE_FIPS.keys()) if args.all else args.states
        download_fcc_fixed_summary_place(
            states=states_to_process,
            output_dir=args.output_dir,
            overwrite=args.overwrite,
            headless=HEADLESS,
        )
    elif args.geography == "other":
        download_fcc_fixed_summary_other(
            output_dir=args.output_dir,
            overwrite=args.overwrite,
            headless=HEADLESS,
        )