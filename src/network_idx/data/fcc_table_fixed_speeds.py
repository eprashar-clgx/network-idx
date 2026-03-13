"""
FCC Broadband Map — Playwright Downloader
==========================================
Downloads Fixed Broadband Availability Data (Cable / Copper / Fiber)
for any set of US states/territories.

Install dependencies:
    pip install playwright
    playwright install chromium

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

import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# ── FIPS code map (from the FCC dropdown HTML) ───────────────────────────────
# These are the <option value="..."> attributes in the state <select>.
# Used to select a state programmatically without fragile text matching.

FIPS = {
    "Alabama":                                      "01",
    "Alaska":                                       "02",
    "Arizona":                                      "04",
    "Arkansas":                                     "05",
    "California":                                   "06",
    "Colorado":                                     "08",
    "Connecticut":                                  "09",
    "Delaware":                                     "10",
    "District of Columbia":                         "11",
    "Florida":                                      "12",
    "Georgia":                                      "13",
    "Hawaii":                                       "15",
    "Idaho":                                        "16",
    "Illinois":                                     "17",
    "Indiana":                                      "18",
    "Iowa":                                         "19",
    "Kansas":                                       "20",
    "Kentucky":                                     "21",
    "Louisiana":                                    "22",
    "Maine":                                        "23",
    "Maryland":                                     "24",
    "Massachusetts":                                "25",
    "Michigan":                                     "26",
    "Minnesota":                                    "27",
    "Mississippi":                                  "28",
    "Missouri":                                     "29",
    "Montana":                                      "30",
    "Nebraska":                                     "31",
    "Nevada":                                       "32",
    "New Hampshire":                                "33",
    "New Jersey":                                   "34",
    "New Mexico":                                   "35",
    "New York":                                     "36",
    "North Carolina":                               "37",
    "North Dakota":                                 "38",
    "Ohio":                                         "39",
    "Oklahoma":                                     "40",
    "Oregon":                                       "41",
    "Pennsylvania":                                 "42",
    "Rhode Island":                                 "44",
    "South Carolina":                               "45",
    "South Dakota":                                 "46",
    "Tennessee":                                    "47",
    "Texas":                                        "48",
    "Utah":                                         "49",
    "Vermont":                                      "50",
    "Virginia":                                     "51",
    "Washington":                                   "53",
    "West Virginia":                                "54",
    "Wisconsin":                                    "55",
    "Wyoming":                                      "56",
    "American Samoa":                               "60",
    "Guam":                                         "66",
    "Commonwealth of the Northern Mariana Islands": "69",
    "Puerto Rico":                                  "72",
    "United States Virgin Islands":                 "78",
}

# ── Constants ─────────────────────────────────────────────────────────────────

FCC_URL = "https://broadbandmap.fcc.gov/data-download/nationwide-data?version=jun2025&pubDataVer=jun2025"

# Technology names must match the text in the first <td> of each table row
# exactly as it appears on the page.
VALID_TECHNOLOGIES = [
    "Cable",
    "Copper",
    "Fiber to the Premises",
]


# ── Core download function ────────────────────────────────────────────────────

def download_fcc_data(
    states: list[str] | None = None,
    technologies: list[str] | None = None,
    output_dir: str | Path = "fcc_data",
    overwrite: bool = False,
    headless: bool = True,
    pause_seconds: float = 2.0,
) -> list[Path]:
    """
    Download FCC Fixed Broadband Availability files using a headless browser.

    Parameters
    ----------
    states        : State/territory names. Defaults to all 56 entries.
    technologies  : Technology names. Defaults to all three:
                    ["Cable", "Copper", "Fiber to the Premises"]
    output_dir    : Folder to save downloaded .zip files into.
    overwrite     : If False, skip combinations already saved to disk.
    headless      : If False, opens a visible browser window (useful for debugging).
    pause_seconds : Wait between state iterations (be polite to the server).

    Returns
    -------
    List of Paths to successfully downloaded files.
    """
    states       = states       or list(FIPS.keys())
    technologies = technologies or VALID_TECHNOLOGIES
    output_dir   = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Validate inputs up front
    bad_states = [s for s in states if s not in FIPS]
    if bad_states:
        raise ValueError(f"Unknown state(s): {bad_states}\nValid: {list(FIPS.keys())}")

    bad_tech = [t for t in technologies if t not in VALID_TECHNOLOGIES]
    if bad_tech:
        raise ValueError(f"Unknown technology/ies: {bad_tech}\nValid: {VALID_TECHNOLOGIES}")

    total   = len(states) * len(technologies)
    saved   = []
    counter = 0

    print(f"\nFCC Broadband Playwright Downloader")
    print(f"  States       : {len(states)}")
    print(f"  Technologies : {technologies}")
    print(f"  Output dir   : {output_dir.resolve()}")
    print(f"  Total files  : {total}\n")

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
            fips = FIPS[state]
            safe = state.replace(" ", "_")

            print(f"── {state} (FIPS {fips}) ──")

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
                page.wait_for_timeout(1000)
            except PlaywrightTimeout:
                print(f"  [ERROR] Timed out waiting for table after selecting {state}")
                continue

            for tech in technologies:
                counter += 1
                safe_tech = tech.replace(" ", "_")

                if not overwrite and any(output_dir.glob(f"{safe}_{safe_tech}*")):
                    print(f"  [{counter}/{total}] [SKIP] {state} / {tech} already exists")
                    continue

                print(f"  [{counter}/{total}] [GET]  {state} / {tech}", end=" ... ")

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
                    print(f"saved → {dest.name} ({size_mb:.1f} MB)")
                    saved.append(dest)

                except PlaywrightTimeout:
                    print(f"\n  [ERROR] Download timed out for {state} / {tech}")
                except Exception as e:
                    print(f"\n  [ERROR] {state} / {tech}: {e}")

            if state != states[-1]:
                time.sleep(pause_seconds)

        browser.close()

    print(f"\nDone. {len(saved)}/{total} files saved to '{output_dir}/'")
    return saved


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":

    # ── Edit these to control what gets downloaded ────────────────────────────

    STATES = [
        "Alabama",
        #"Indiana",
    ]

    TECHNOLOGIES = [
        #"Cable",
        #"Copper",
        "Fiber to the Premises",
    ]
    
    OUTPUT_DIR = r"data\raw\fcc"

    # Set headless=False to watch the browser — useful for debugging
    HEADLESS = False

    # ─────────────────────────────────────────────────────────────────────────
    download_fcc_data(
        states=STATES,
        technologies=TECHNOLOGIES,
        output_dir=OUTPUT_DIR,
        overwrite=False,
        headless=HEADLESS,
        pause_seconds=2.0,
    )