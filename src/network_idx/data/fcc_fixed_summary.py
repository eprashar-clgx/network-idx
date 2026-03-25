"""
FCC Broadband Map — Playwright Downloader

Description


Usage:
"""
import argparse
import logging
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from network_idx.constants import (FCC_URL,
                                   STATE_FIPS)
from network_idx.config import RAW_DIR_FCC_SPEEDS

# Adding logging for better visibility into the process
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
    )
logger = logging.getLogger(__name__)

def download_fcc_broadband_summary(
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
    technologies  : Technology names ["Cable", "Copper", "Fiber to the Premises"]. Defaults to Fiber                    
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
        browser = p.chromium.launch(headless=headless, channel="chrome")
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        logger.info("  Loading FCC page...")
        page.goto(FCC_URL, wait_until="domcontentloaded", timeout=60_000)
        # Wait for the dropdown to be present
        page.wait_for_selector("select#state", timeout=15_000)
        logger.info("FCC page loaded successfully.")

        for state in states:
            fips = STATE_FIPS[state]
            safe = state.replace(" ", "_")

            logger.info(f"Processing state: {state} (FIPS: {fips})")

            try:
                # Select the state
                page.select_option("select#state", value=fips)
                # Force Angular change detection
                page.eval_on_selector(
                    "select#state",
                    "el => el.dispatchEvent(new Event('change', { bubbles: true }))"
                )
                # Wait for the table rows to appear
                page.wait_for_selector(
                    "tr td.align-middle",
                    timeout=15_000,
                )
                # TODO: wait constants can be passed in the function or should they be static?
                page.wait_for_timeout(1000)
            except PlaywrightTimeout:
                logger.error(f"Timeout while loading data for {state}. Skipping to next state.")
                continue

            logger.info(f"  State '{state}' loaded, processing technologies...")
            for tech in technologies:
                counter += 1
                safe_tech = tech.replace(" ", "_")

                if not overwrite and any(output_dir.glob(f"{safe}_{safe_tech}*")):
                    logger.info(f"  [{counter}/{total}] Skipping {state} / {tech} (already exists)")
                    continue

                logger.info(f"  [{counter}/{total}] Downloading {state} / {tech}...")

                try:
                    row_button = page.locator(
                        f"//tr[td[normalize-space()='{tech}']]//button"
                    )
                    row_button.wait_for(state="visible", timeout=10_000)

                    with page.expect_download(timeout=300_000) as dl_info:
                        row_button.click()

                    download = dl_info.value
                    suggested = download.suggested_filename
                    dest = output_dir / (suggested if suggested else f"{safe}_{safe_tech}_{fips}.zip")

                    download.save_as(dest)
                    size_mb = dest.stat().st_size / (1024 * 1024)
                    logger.info(f"    Downloaded '{dest.name}' ({size_mb:.2f} MB)")
                    saved.append(dest)

                except PlaywrightTimeout:
                    logger.error(f"    Timeout while trying to download {state} / {tech}. Skipping this technology.")
                except Exception as e:
                    logger.error(f"    Error while downloading {state} / {tech}: {e}")

            if state != states[-1]:
                time.sleep(pause_seconds)

        browser.close()

    logger.info(f"\nDownload complete. {len(saved)}/{total} files saved to '{output_dir}'.")
    return saved