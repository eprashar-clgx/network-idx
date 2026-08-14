"""
Column contracts for the processing and feature stages of network_idx.

This module lists the exact input and output columns for each raw and derived
table the pipeline reads or produces: the Census Address Count Listing and Block
Assignment File outputs, the FCC fixed-speed block/provider/H3 inputs and outputs,
and the FCC coverage (summary) inputs and the county-residual, block, tract, and
speed-bucketed tract outputs. Keeping these column lists in one place makes the
schema each stage expects explicit and easy to keep in sync.
"""

# ── Census Address Count Listing ──────────────────────────────────────────────
CENSUS_ACL_OUTPUTS = [
    "block_geoid",
    "state_fips",
    "state_usps",
    "county_geoid",
    "tract_geoid",
    "total_housing_units",
    "total_group_quarters",
]

# ── FCC fixed speeds (block level) ────────────────────────────────────────────
FCC_FIXED_SPEED_INPUTS = [
    "location_id",
    "provider_id",
    "block_geoid",
    "state_usps",
    "technology",
    "max_advertised_download_speed",
    "max_advertised_upload_speed",
]

FCC_FIXED_SPEED_OUTPUTS = [
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
    "fiber_max_upload_speed",
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

# ── FCC fixed speeds (provider level) ─────────────────────────────────────────
FCC_FIXED_SPEEDS_PROVIDER_INPUTS = [
    "state_usps",
    "block_geoid",
    "frn",
    "provider_id",
    "brand_name",
    "location_id",
    "technology",
    "max_advertised_download_speed",
    "max_advertised_upload_speed",
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
    "fiber_max_upload_speed",
]

# ── FCC fixed speeds (provider level, H3 resolution 8) ────────────────────────
FCC_FIXED_SPEEDS_PROVIDER_H3_INPUTS = [
    "state_usps",
    "h3_res8_id",
    "frn",
    "provider_id",
    "brand_name",
    "location_id",
    "technology",
    "max_advertised_download_speed",
    "max_advertised_upload_speed",
]

FCC_FIXED_SPEEDS_PROVIDER_H3_OUTPUTS = [
    "state_usps",
    "state_fips",
    "h3_res8_id",
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
    "fiber_max_upload_speed",
]

# ── FCC fixed coverage (summary) ──────────────────────────────────────────────
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
    "copper_speed_02_02_only",
    "copper_speed_10_1_only",
    "copper_speed_25_3_only",
    "copper_speed_100_20_only",
    "copper_speed_250_25_only",
    "copper_speed_1000_100_only",
    "cable_speed_02_02_only",
    "cable_speed_10_1_only",
    "cable_speed_25_3_only",
    "cable_speed_100_20_only",
    "cable_speed_250_25_only",
    "cable_speed_1000_100_only",
    "fiber_speed_02_02_only",
    "fiber_speed_10_1_only",
    "fiber_speed_25_3_only",
    "fiber_speed_100_20_only",
    "fiber_speed_250_25_only",
    "fiber_speed_1000_100_only",
]

# ── Census Block Assignment File (2020) ───────────────────────────────────────
CENSUS_BAF_OUTPUTS = [
    "block_geoid",
    "state_fips",
    "county_geoid",
    "tract_geoid",
    "place_geoid",
]

# ── FCC coverage derived cuts (county residuals, block, tract, buckets) ───────
FCC_COVERAGE_TIER_METRICS = [
    "speed_02_02_only",
    "speed_10_1_only",
    "speed_25_3_only",
    "speed_100_20_only",
    "speed_250_25_only",
    "speed_1000_100_only",
]

FCC_COVERAGE_COUNTY_RESIDUAL_OUTPUTS = [
    "county_geoid",
    "state_fips",
    "county_total_units",
    "places_total_units",
    "residual_units",
    "place_count",
] + [f"{tech.lower()}_{metric}" for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES for metric in FCC_COVERAGE_TIER_METRICS]

# Block-level coverage estimate outputs.
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

FCC_COVERAGE_TRACT_BUCKETED_METRICS = [
    "speed_less_than_100_20",
    "speed_100_20_only",
    "speed_more_than_100_20",
]

FCC_COVERAGE_TRACT_BUCKETED_OUTPUTS = [
    "tract_geoid",
    "state_fips",
    "state_usps",
    "estimated_census_housing_units",
    "estimated_fcc_units",
] + [f"{tech.lower()}_{metric}" for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES for metric in FCC_COVERAGE_TRACT_BUCKETED_METRICS]
