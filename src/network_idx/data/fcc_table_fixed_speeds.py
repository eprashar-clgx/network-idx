"""
FCC Broadband Map — Playwright Downloader
Usage (edit the variables at the bottom, then run):
    python fcc_downloader_playwright.py

Or import and call directly:
    from fcc_downloader_playwright import download_fcc_data
    download_fcc_data(
        states=["Illinois", "Indiana"],
        technologies=["Cable", "Copper", "Fiber to the Premises"],
        output_dir="fcc_data",
    )
"""
import argparse
import logging
import sys
import os
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from network_idx.constants import (FCC_URL,
                                   FIXED_TECHNOLOGIES_FOR_DOWNLOAD,
                                   STATE_FIPS)

# Adding logging for better visibility into the process
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
    )
logger = logging.getLogger(__name__)

# Define file path to enable constants import regardless of current working directory
# Importing with package name<constants> is a better approach so commented this out
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Core download function 

def download_fcc_speeds(
    states: list[str],
    technologies: list[str],
    output_dir: str | Path = r"data/raw/fcc/speeds",
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
    states = states
    technologies = technologies
    output_dir = Path(output_dir)

    # Check if output directory exists, if not create it
    # TODO: check if this line needs to be modified
    output_dir.mkdir(parents=True, exist_ok=True)

    # Define tasks / number of files to download upfront
    total   = len(states) * len(technologies)
    saved   = []
    counter = 0

    # Log the download configuration
    logger.info(f"FCC Broadband Playwright Downloader")
    logger.info(f"  States       : {len(states)}")
    logger.info(f"  Technologies : {technologies}")
    logger.info(f"  Total files  : {total}\n")


    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless, channel="chrome")
        context = browser.new_context(accept_downloads=True)
        page    = context.new_page()

        print("  Loading FCC page...")
        page.goto(FCC_URL, wait_until="domcontentloaded", timeout=60_000)
        # Wait for the dropdown to be present
        page.wait_for_selector("select#state", timeout=15_000)
        print("  Page loaded.\n")

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
                print(f"  [ERROR] Timed out waiting for table after selecting {state}")
                continue

            logger.info(f"  State '{state}' loaded, processing technologies...")
            for tech in technologies:
                counter += 1
                safe_tech = tech.replace(" ", "_")

                if not overwrite and any(output_dir.glob(f"{safe}_{safe_tech}*")):
                    print(f"  [{counter}/{total}] [SKIP] {state} / {tech} already exists")
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
                    print(f"\n  [ERROR] Download timed out for {state} / {tech}")
                except Exception as e:
                    print(f"\n  [ERROR] {state} / {tech}: {e}")

            if state != states[-1]:
                time.sleep(pause_seconds)

        browser.close()

    logger.info(f"\nDownload complete. {len(saved)}/{total} files saved to '{output_dir}'.")
    return saved


# ── CLI entry point 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download FCC Fixed Broadband Availability files using a headless browser."
    )

    parser.add_argument(
        "--states", type=str, nargs="+", default=["Alabama"],
        choices=STATE_FIPS.keys(),
        help=f"States for download - one or more of of: {list(STATE_FIPS.keys())}"
        )
    
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Download data for all states (overrides --states)"
        )
    
    parser.add_argument(
        "--technologies", type=str, nargs="+", default=["Fiber to the Premises"],
        choices=FIXED_TECHNOLOGIES_FOR_DOWNLOAD,
        )
    
    parser.add_argument(
        "--output-dir", type=Path, default=r"data\raw\fcc\speeds",
        help="Data directory to dump .zip files into. Defaults to 'data/raw/fcc/speeds"
    )

    # Set headless=False to watch the browser — useful for debugging
    HEADLESS = False

    args = parser.parse_args()

    # Default states should be Alabama or whatever the user enters
    # However, value should be over-written if the user mentions all
    if args.all:
        args.states = list(STATE_FIPS.keys())
    
    for state in args.states:
        if state not in STATE_FIPS:
            raise SystemExit(f"Invalid state: {state}. Must be one of: {list(STATE_FIPS.keys())}")
        
    for tech in args.technologies:
        if tech not in FIXED_TECHNOLOGIES_FOR_DOWNLOAD:
            raise SystemExit(f"Invalid technology: {tech}. Must be one of: {FIXED_TECHNOLOGIES_FOR_DOWNLOAD}")
    
    download_fcc_speeds(
        states=args.states,
        technologies=args.technologies,
        output_dir=args.output_dir,
        headless=HEADLESS,
    )