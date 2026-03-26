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
                                   FIXED_TECHNOLOGIES_FOR_DOWNLOAD,
                                   STATE_FIPS)
from network_idx.config import RAW_DIR_FCC_SPEEDS

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
    total   = len(states) * len(technologies)
    saved   = []
    counter = 0

    # Log the download configuration
    logger.info("FCC Broadband Playwright Downloader")
    logger.info(f"  States       : {len(states)}")
    logger.info(f"  Technologies : {technologies}")
    logger.info(f"  Total files  : {total}")


    with sync_playwright() as p:
        browser = p.chromium.launch(
        headless=headless, 
        channel="chrome",
        args=[
            "--disable-http2", 
            "--disable-blink-features=AutomationControlled"  # Hides the "I am a bot" flag
        ]
        )
        
        context = browser.new_context(
        accept_downloads=True,
        # Fake a normal Windows machine
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        # Give the virtual monitor a standard desktop resolution so the table isn't squished
        viewport={"width": 1920, "height": 1080}
        )

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

            # Loop through technologies and attempt downloads
            for tech in technologies:
                counter += 1
                safe_tech = tech.replace(" ", "_")
                file_tech = tech.replace(" ", "")

                if not overwrite and any(output_dir.glob(f"*_{fips}_{file_tech}*")):
                    logger.info(f"  [{counter}/{total}] Skipping {state} / {tech} (already exists)")
                    continue

                logger.info(f"  [{counter}/{total}] Downloading {state} / {tech}...")

                # Adding retry logic in case request times out (it will)
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        row_button = page.locator(
                            f"//tr[td[normalize-space()='{tech}']]//button"
                            )
                        row_button.wait_for(state="visible", timeout=10_000)
                        
                        # Allow 5 minutes for download
                        with page.expect_download(timeout=300_000) as dl_info:
                            row_button.click()
                        
                        download = dl_info.value
                        suggested = download.suggested_filename
                        dest = output_dir / (suggested if suggested else f"{safe}_{file_tech}_{fips}.zip")
                        logger.info(f"    [Attempt {attempt + 1}/{max_retries}] Streaming data... waiting for file to save.")
                        
                        # Block until the download is complete and saved to disk
                        download.save_as(dest)
                        size_mb = dest.stat().st_size / (1024 * 1024)
                        logger.info(f"    Downloaded '{dest.name}' ({size_mb:.2f} MB)")
                        saved.append(dest)

                        # Break out of the retry loop if the file is saved.
                        # If not, check retry count and try again 
                        break

                    except PlaywrightTimeout:
                        if attempt < max_retries - 1:
                            logger.warning(f"    [Attempt {attempt + 1}/{max_retries}] Timeout from FCC. Waiting 10 seconds before retrying...")
                            time.sleep(10) # Retry after 10 seconds
                        else:
                            logger.error(f"    Failed to download {state} / {tech} after {max_retries} attempts. Moving on.")
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"    [Attempt {attempt + 1}/{max_retries}] Connection error: {e}. Retrying in 10 seconds...")
                            time.sleep(10)
                        else:
                            logger.error(f"    Fatal error downloading {state} / {tech}: {e}")

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
        metavar="STATE",
        help=f"States for download - one or more of of: {list(STATE_FIPS.keys())}"
        )
    
    parser.add_argument(
        "--all", action="store_true", default=False,
        help="Download data for all states (overrides --states)"
        )
    
    parser.add_argument(
        "--technologies", type=str, nargs="+", default=["Fiber to the Premises"],
        choices=FIXED_TECHNOLOGIES_FOR_DOWNLOAD,
        metavar="TECH",
        help=f"Technologies for download - one or more of: {FIXED_TECHNOLOGIES_FOR_DOWNLOAD}"
        )
    
    parser.add_argument(
        "--output-dir", type=Path, default=RAW_DIR_FCC_SPEEDS,
        help="Data directory to dump .zip files into. Defaults to 'data/raw/fcc/speeds'"
    )

    # Set headless=False to watch the browser — useful for debugging
    HEADLESS = True

    args = parser.parse_args()

    valid_states = list(STATE_FIPS.keys())
    valid_technologies = FIXED_TECHNOLOGIES_FOR_DOWNLOAD
    states_to_process = valid_states if args.all else args.states
    technologies_to_process = valid_technologies if args.all else args.technologies
    
    download_fcc_speeds(
        states=states_to_process,
        technologies=technologies_to_process,
        output_dir=args.output_dir,
        headless=HEADLESS,
    )