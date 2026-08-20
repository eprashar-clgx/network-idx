-- Derive the 4 block-level telecom features from FCC speeds + coverage blocks.
-- Spine = coverage block (has census_housing_units + speed-tier %); LEFT JOIN
-- speeds block (location/provider counts). Missing speed rows => counts default 0.
CREATE OR REPLACE TABLE `{output_table}` AS
WITH joined AS (
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
),
features AS (
    SELECT
        * EXCEPT(fiber_speed_1000_100_only),
        -- has_fiber gate: serviceable fiber requires both locations AND a provider
        CAST(fiber_location_count > 0 AND fiber_provider_count > 0 AS INT64) AS has_fiber,
        -- SAFE_DIVIDE returns NULL on zero denominator (never inf); fill per parity
        COALESCE(SAFE_DIVIDE(cable_location_count, census_housing_units), 0.0)
            AS cable_penetration,
        COALESCE(SAFE_DIVIDE(census_housing_units - fiber_location_count, census_housing_units), 1.0)
            AS fiber_opportunity_gap,
        COALESCE(fiber_speed_1000_100_only, 0.0) AS fiber_speed_1000_100_only,
        -- competitive landscape ladder (order matters; mirrors the notebook function)
        CASE
            WHEN copper_provider_count = 0 AND cable_provider_count = 0 AND fiber_provider_count = 0 THEN 'no_providers'
            WHEN copper_provider_count > 0 AND cable_provider_count = 0 AND fiber_provider_count = 0 THEN 'greenfield'
            WHEN cable_provider_count  > 0 AND fiber_provider_count = 0 THEN 'cable_but_no_fiber'
            WHEN fiber_provider_count = 1 THEN 'fiber_entry'
            WHEN fiber_provider_count = 2 THEN 'fiber_duopoly'
            WHEN fiber_provider_count = 3 THEN 'fiber_competitive'
            WHEN fiber_provider_count > 3 THEN 'fiber_saturated'
            ELSE 'other'
        END AS provider_competitive_landscape
    FROM joined
)
SELECT
    block_geoid,
    state_fips,
    state_usps,
    county_geoid,
    tract_geoid,
    place_geoid,
    census_housing_units,
    estimated_fcc_units,
    -- raw counts kept for QC (F5 degenerate-block check / ordinal sanity)
    cable_provider_count,
    fiber_provider_count,
    copper_provider_count,
    -- four scored telecom features
    cable_penetration,
    fiber_opportunity_gap,
    fiber_speed_1000_100_only * has_fiber AS fiber_speed_top_tier,
    provider_competitive_landscape,
    CASE provider_competitive_landscape
        WHEN 'no_providers'       THEN 0
        WHEN 'greenfield'         THEN 1
        WHEN 'cable_but_no_fiber' THEN 2
        WHEN 'fiber_entry'        THEN 3
        WHEN 'fiber_duopoly'      THEN 4
        WHEN 'fiber_competitive'  THEN 5
        WHEN 'fiber_saturated'    THEN 6
        ELSE NULL
    END AS provider_competitive_landscape_ord
FROM features