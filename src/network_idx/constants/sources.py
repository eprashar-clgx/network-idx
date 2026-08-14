"""
External data-source locations and technology vocabulary for network_idx.

This module names where the downloadable raw data comes from (the FCC broadband
map and the Census base URLs) and the fixed-technology vocabulary used when
downloading and normalising FCC data. These are the stable, source-facing
constants that describe the outside world the pipeline pulls from.
"""

# FCC nationwide broadband map data-download page.
FCC_URL = "https://broadbandmap.fcc.gov/data-download/nationwide-data?version=jun2025&pubDataVer=jun2025"

# Census Block Assignment Files (2020) base URL.
CENSUS_BAF_2020_BASE_URL = "https://www2.census.gov/geo/docs/maps-data/data/baf2020/"

# Census Address Count Listing (2025) base URL.
CENSUS_ACL_2025_BASE_URL = "https://www2.census.gov/geo/pvs/addcountlisting/2025/"

# Technology names must match the text in the first <td> of each table row exactly
# as it appears on the FCC page.
FIXED_TECHNOLOGIES_FOR_DOWNLOAD = [
    "Cable",
    "Copper",
    "Fiber to the Premises",
]

# FCC technology label → numeric technology code.
FIXED_TECHNOLOGIES_MAPPING = {
    "Copper": 10,
    "Cable": 40,
    "Fiber": 50,
}
