-- Telecom engineered features: derive the four block-level telecom model features.
--
-- The coverage block table is the spine — it carries every block, its Census housing units,
-- and the interpolated top-tier fiber coverage percentage — and is left-joined to the FCC
-- speeds block table for per-technology serviceable location and provider counts. Blocks
-- with no speeds row have no serviceable locations or providers, so those counts default to
-- zero. The four features and the provider-landscape label encode analytical decisions made
-- during exploratory modelling, which is why this lives in the engineered layer rather than
-- the transform layer. The label-to-ordinal ladder is generated from the scoring contract so
-- the two cannot drift, and the tables are rendered from configuration.
--
-- The features are: cable penetration (cable locations per housing unit), the fiber
-- opportunity gap (share of housing units without a serviceable fiber location), the
-- top-tier fiber speed coverage gated to blocks that actually have serviceable fiber, and
-- the provider competitive landscape as both a text label and its ordinal rank.
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
        -- Serviceable fiber requires both a location and a provider present in the block.
        CAST(fiber_location_count > 0 AND fiber_provider_count > 0 AS INT64) AS has_fiber,
        -- SAFE_DIVIDE yields NULL (never infinity) on a zero denominator; fill to the
        -- feature's neutral value so a block with no housing units carries no signal.
        COALESCE(SAFE_DIVIDE(cable_location_count, census_housing_units), 0.0)
            AS cable_penetration,
        COALESCE(SAFE_DIVIDE(census_housing_units - fiber_location_count, census_housing_units), 1.0)
            AS fiber_opportunity_gap,
        COALESCE(fiber_speed_1000_100_only, 0.0) AS fiber_speed_1000_100_only,
        -- Competitive landscape ladder: order matters, each branch assumes the ones above
        -- it did not match. Providers are counted per technology.
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
    -- Raw provider counts retained for quality checks on the ordinal.
    cable_provider_count,
    fiber_provider_count,
    copper_provider_count,
    -- The four scored telecom features.
    cable_penetration,
    fiber_opportunity_gap,
    fiber_speed_1000_100_only * has_fiber AS fiber_speed_top_tier,
    provider_competitive_landscape,
    CASE provider_competitive_landscape
        {landscape_ord_cases}
        ELSE NULL
    END AS provider_competitive_landscape_ord
FROM features
