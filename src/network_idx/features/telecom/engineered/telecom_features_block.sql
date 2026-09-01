-- Telecom engineered features (block grain): the `joined` input prelude.
--
-- This is the block-grain input CTE for the shared engineered-telecom-feature definition
-- (telecom_features.sql, rendered via _engineered_sql). The coverage block table is the
-- spine — it carries every block, its Census housing units, the estimated FCC units, and
-- the interpolated top-tier fiber coverage percentage — and is left-joined to the FCC
-- speeds block table for per-technology serviceable location and provider counts. Blocks
-- with no speeds row have no serviceable locations or providers, so those counts default
-- to zero. The shared definition then derives the four features from these columns; the
-- tables are rendered from configuration.
joined AS (
    SELECT
        c.block_geoid,
        c.state_fips,
        c.state_usps,
        c.county_geoid,
        c.tract_geoid,
        c.place_geoid,
        c.census_housing_units,
        c.estimated_fcc_units,
        c.fiber_speed_1000_100_only,
        COALESCE(s.cable_location_count, 0)  AS cable_location_count,
        COALESCE(s.fiber_location_count, 0)  AS fiber_location_count,
        COALESCE(s.copper_location_count, 0) AS copper_location_count,
        COALESCE(s.cable_provider_count, 0)  AS cable_provider_count,
        COALESCE(s.fiber_provider_count, 0)  AS fiber_provider_count,
        COALESCE(s.copper_provider_count, 0) AS copper_provider_count
    FROM `{coverage_block_table}` AS c
    LEFT JOIN `{speeds_block_table}` AS s
        ON c.block_geoid = s.block_geoid
)
