import os
from pathlib import Path

## DATA SOURCES

FCC_URL = "https://broadbandmap.fcc.gov/data-download/nationwide-data?version=jun2025&pubDataVer=jun2025"

## CENSUS CONSTANTS
CENSUS_BAF_2020_BASE_URL = "https://www2.census.gov/geo/docs/maps-data/data/baf2020/"

# Census Address Count Listing
CENSUS_ACL_2025_BASE_URL = "https://www2.census.gov/geo/pvs/addcountlisting/2025/"

# Technology names must match the text in the first <td> of each table row
# exactly as it appears on the page.
FIXED_TECHNOLOGIES_FOR_DOWNLOAD = [
    "Cable",
    "Copper",
    "Fiber to the Premises",
]

FIXED_TECHNOLOGIES_MAPPING = {
    "Copper": 10,
    "Cable": 40,
    "Fiber": 50
}
 
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

# State abbreviations to FIPS mapping 
STATE_USPS_TO_FIPS = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "DC": "11",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
    "AS": "60",
    "GU": "66",
    "MP": "69",
    "PR": "72",
    "VI": "78",
}

# Mapping: STATE_USPS to filename stem used by Census for Address Count Listing.
# Census strips spaces/hyphens from state names in filenames.
CENSUS_ACL_STATE_NAMES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "DistrictofColumbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "NewHampshire",
    "NJ": "NewJersey",
    "NM": "NewMexico",
    "NY": "NewYork",
    "NC": "NorthCarolina",
    "ND": "NorthDakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "RhodeIsland",
    "SC": "SouthCarolina",
    "SD": "SouthDakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "WestVirginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "AS": "AmericanSamoa",
    "GU": "Guam",
    "MP": "CommonwealthoftheNorthernMarianaIslands",
    "PR": "PuertoRico",
    "VI": "UnitedStatesVirginIslands",
}

# Census Address Count Listing — Output columns
CENSUS_ACL_OUTPUTS = [
    "block_geoid",
    "state_fips",
    "state_usps",
    "county_geoid",
    "tract_geoid",
    "total_housing_units",
    "total_group_quarters",
]

# FCC Fixed columns for advertised speed analysis
FCC_FIXED_SPEED_INPUTS = [
    "location_id",
    "provider_id",
    "block_geoid",
    "state_usps",
    "technology",
    "max_advertised_download_speed",
    "max_advertised_upload_speed"
]

FCC_FIXED_SPEED_OUTPUTS= [
    "state_usps",
    "state_fips",
    "block_geoid",
    "cable_location_count",
    "cable_provider_count",
    "cable_max_download_speed",
    "cable_max_upload_speed",
    "copper_location_count",
    "copper_provider_count",
    "copper_max_download_speed",
    "copper_max_upload_speed",
    "fiber_location_count",
    "fiber_provider_count",
    "fiber_max_download_speed",
    "fiber_max_upload_speed"
]

FCC_FIXED_SPEED_TRACT_OUTPUTS = [
    "tract_geoid",
    "state_usps",
    "state_fips",
    "cable_location_count",
    "cable_provider_count",
    "cable_max_download_speed",
    "cable_max_upload_speed",
    "copper_location_count",
    "copper_provider_count",
    "copper_max_download_speed",
    "copper_max_upload_speed",
    "fiber_location_count",
    "fiber_provider_count",
    "fiber_max_download_speed",
    "fiber_max_upload_speed",
]

FCC_FIXED_SPEEDS_PROVIDER_INPUTS = [
    "state_usps",
    "block_geoid",
    "frn",
    "provider_id",
    "brand_name",
    "location_id",
    "technology",
    "max_advertised_download_speed",
    "max_advertised_upload_speed"
    ]

FCC_FIXED_SPEEDS_PROVIDER_OUTPUTS = [
    "state_usps",
    "state_fips",
    "block_geoid",
    "frn",
    "provider_id",
    "brand_name",
    "cable_location_count",
    "cable_max_download_speed",
    "cable_max_upload_speed",
    "copper_location_count",
    "copper_max_download_speed",
    "copper_max_upload_speed",
    "fiber_location_count",
    "fiber_max_download_speed",
    "fiber_max_upload_speed"
    ]

# FCC Fixed Coverage (Summary) columns
FCC_FIXED_COVERAGE_TECHNOLOGIES = ["Copper", "Cable", "Fiber"]

FCC_FIXED_COVERAGE_INPUTS = [
    "area_data_type",
    "geography_type",
    "geography_id",
    "geography_desc",
    "geography_desc_full",
    "total_units",
    "biz_res",
    "technology",
    "speed_02_02",
    "speed_10_1",
    "speed_25_3",
    "speed_100_20",
    "speed_250_25",
    "speed_1000_100",
]

FCC_FIXED_COVERAGE_OUTPUTS = [
    "geography_id",
    "geography_desc",
    "geography_desc_full",
    "total_units",
    "copper_speed_100_20",
    "copper_less_than_100_20",
    "copper_more_than_100_20",
    "cable_speed_100_20",
    "cable_less_than_100_20",
    "cable_more_than_100_20",
    "fiber_speed_100_20",
    "fiber_less_than_100_20",
    "fiber_more_than_100_20",
]

# Census BAF 2020 — Output columns
CENSUS_BAF_OUTPUTS = [
    "block_geoid",
    "state_fips",
    "county_geoid",
    "tract_geoid",
    "place_geoid",
]

FCC_COVERAGE_TIER_METRICS = ["speed_100_20", "less_than_100_20", "more_than_100_20"]

FCC_COVERAGE_COUNTY_RESIDUAL_OUTPUTS = [
    "county_geoid",
    "state_fips",
    "county_total_units",
    "places_total_units",
    "residual_units",
    "place_count",
] + [f"{tech.lower()}_{metric}" for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES for metric in FCC_COVERAGE_TIER_METRICS]

# Block-level coverage estimate outputs
FCC_COVERAGE_BLOCK_OUTPUTS = [
    "block_geoid",
    "state_fips",
    "state_usps",
    "county_geoid",
    "tract_geoid",
    "place_geoid",
    "source",
    "census_housing_units",
    "estimated_fcc_units",
] + [f"{tech.lower()}_{metric}" for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES for metric in FCC_COVERAGE_TIER_METRICS]

FCC_COVERAGE_TRACT_OUTPUTS = [
    "tract_geoid",
    "state_fips",
    "state_usps",
    "estimated_census_housing_units",
    "estimated_fcc_units",
] + [f"{tech.lower()}_{metric}" for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES for metric in FCC_COVERAGE_TIER_METRICS]