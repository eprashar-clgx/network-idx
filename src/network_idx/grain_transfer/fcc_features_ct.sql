-- FCC engineered telecom features (tract grain): the `joined` input prelude.
--
-- This is the tract-grain input CTE for the shared engineered-telecom-feature definition
-- (telecom_features.sql, rendered via _engineered_sql). Unlike the block prelude, the four
-- engineered features are non-linear and cannot be rolled up from telecom_features_block,
-- so this re-derives them from the transform-layer inputs aggregated to tract (ADR-0007):
--
--   * location counts and the housing-unit-weighted top-tier fiber speed come from the
--     dasymetric FCC coverage block table, summed / weighted up to tract;
--   * provider counts are COUNT(DISTINCT provider_id) read straight from the raw
--     per-technology FCC provider tables and grouped to tract — a block provider count
--     cannot be re-aggregated to a distinct tract count without double counting providers
--     that span blocks;
--   * housing units and estimated FCC units are simple sums of the block values.
--
-- The shared definition then derives the four features from these columns. Every source
-- table is rendered from configuration.
copper_prov AS (
    SELECT
        SUBSTR(block_geoid, 1, 11) AS tract_geoid,
        COUNT(DISTINCT location_id) AS copper_location_count,
        COUNT(DISTINCT provider_id) AS copper_provider_count
    FROM `{copper_table}`
    GROUP BY tract_geoid
),
cable_prov AS (
    SELECT
        SUBSTR(block_geoid, 1, 11) AS tract_geoid,
        COUNT(DISTINCT location_id) AS cable_location_count,
        COUNT(DISTINCT provider_id) AS cable_provider_count
    FROM `{cable_table}`
    GROUP BY tract_geoid
),
fiber_prov AS (
    SELECT
        SUBSTR(block_geoid, 1, 11) AS tract_geoid,
        COUNT(DISTINCT location_id) AS fiber_location_count,
        COUNT(DISTINCT provider_id) AS fiber_provider_count
    FROM `{fiber_table}`
    GROUP BY tract_geoid
),
coverage_ct AS (
    SELECT
        tract_geoid,
        SUM(census_housing_units) AS census_housing_units,
        SUM(estimated_fcc_units) AS estimated_fcc_units,
        -- housing-unit(estimated_fcc_units)-weighted mean of the block top-tier coverage %
        SAFE_DIVIDE(
            SUM(estimated_fcc_units * fiber_speed_1000_100_only),
            NULLIF(SUM(estimated_fcc_units), 0)
        ) AS fiber_speed_1000_100_only
    FROM `{coverage_block_table}`
    GROUP BY tract_geoid
),
joined AS (
    SELECT
        cov.tract_geoid,
        -- the first two digits of the 11-digit tract GEOID are the state FIPS
        SUBSTR(cov.tract_geoid, 1, 2) AS state_fips,
        cov.census_housing_units,
        cov.estimated_fcc_units,
        cov.fiber_speed_1000_100_only,
        COALESCE(cab.cable_location_count, 0)  AS cable_location_count,
        COALESCE(fib.fiber_location_count, 0)  AS fiber_location_count,
        COALESCE(cop.copper_location_count, 0) AS copper_location_count,
        COALESCE(cab.cable_provider_count, 0)  AS cable_provider_count,
        COALESCE(fib.fiber_provider_count, 0)  AS fiber_provider_count,
        COALESCE(cop.copper_provider_count, 0) AS copper_provider_count
    FROM coverage_ct cov
    LEFT JOIN cable_prov  cab ON cov.tract_geoid = cab.tract_geoid
    LEFT JOIN fiber_prov  fib ON cov.tract_geoid = fib.tract_geoid
    LEFT JOIN copper_prov cop ON cov.tract_geoid = cop.tract_geoid
)
