import os
from pathlib import Path

## FCC CONSTANTS

FCC_URL = "https://broadbandmap.fcc.gov/data-download/nationwide-data?version=jun2025&pubDataVer=jun2025"

# Technology names must match the text in the first <td> of each table row
# exactly as it appears on the page.
FIXED_TECHNOLOGIES_FOR_DOWNLOAD = [
    "Cable",
    "Copper",
    "Fiber to the Premises",
]
 
## GEO CONSTANTS
# Constants to map geo identifiers to human-readable names and vice versa.
# ── FIPS code map (from the FCC dropdown HTML) ───────────────────────────────
# These are the <option value="..."> attributes in the state <select>.

STATE_FIPS = {
    "Alabama": "01",
    "Alaska": "02",
    "Arizona": "04",
    "Arkansas": "05",
    "California": "06",
    "Colorado": "08",
    "Connecticut": "09",
    "Delaware": "10",
    "District of Columbia": "11",
    "Florida": "12",
    "Georgia": "13",
    "Hawaii": "15",
    "Idaho": "16",
    "Illinois": "17",
    "Indiana": "18",
    "Iowa": "19",
    "Kansas": "20",
    "Kentucky": "21",
    "Louisiana": "22",
    "Maine": "23",
    "Maryland": "24",
    "Massachusetts": "25",
    "Michigan": "26",
    "Minnesota": "27",
    "Mississippi":"28",
    "Missouri": "29",
    "Montana": "30",
    "Nebraska": "31",
    "Nevada": "32",
    "New Hampshire": "33",
    "New Jersey": "34",
    "New Mexico": "35",
    "New York": "36",
    "North Carolina": "37",
    "North Dakota": "38",
    "Ohio": "39",
    "Oklahoma": "40",
    "Oregon": "41",
    "Pennsylvania": "42",
    "Rhode Island": "44",
    "South Carolina": "45",
    "South Dakota": "46",
    "Tennessee": "47",
    "Texas": "48",
    "Utah": "49",
    "Vermont": "50",
    "Virginia": "51",
    "Washington": "53",
    "West Virginia": "54",
    "Wisconsin": "55",
    "Wyoming": "56",
    "American Samoa": "60",
    "Guam": "66",
    "Commonwealth of the Northern Mariana Islands": "69",
    "Puerto Rico": "72",
    "United States Virgin Islands": "78",
}