-- =============================================================================
-- Engineered telecom (FCC) features at census-tract grain.
-- Module        : network_idx.grain_transfer.fcc_features_ct
-- Generated from : src/network_idx/grain_transfer/fcc_features_ct.py  (python -m network_idx.grain_transfer.fcc_features_ct --dry-run)
-- Run in         : CONSOLE (reads PROD)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

-- Telecom engineered features: the four telecom model features (grain-agnostic).
--
-- This is the single, grain-agnostic definition of the four engineered telecom features.
-- It is rendered by both the block feature (features/telecom/engineered/
-- telecom_features_block) and the tract grain transfer (grain_transfer/fcc_features_ct):
-- each caller supplies a joined_prelude — the WITH CTE(s) ending in a CTE named
-- `joined` that yields the standard per-grain input columns (census_housing_units,
-- estimated_fcc_units, fiber_speed_1000_100_only, and the per-technology location and
-- provider counts) — plus the passthrough key columns for that grain. Keeping the
-- derivation here means the block and tract telecom features can never drift (ADR-0007).
--
-- The features are: cable penetration (cable locations per housing unit), the fiber
-- opportunity gap (share of housing units without a serviceable fiber location), the
-- top-tier fiber speed coverage gated to blocks/tracts that actually have serviceable
-- fiber, and the provider competitive landscape as both a text label and its ordinal
-- rank. The label-to-ordinal ladder is generated from the scoring contract so it cannot
-- drift from the mapping the scorer relies on.
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_features.telecom_features_ct`
CLUSTER BY state_fips AS
WITH -- FCC engineered telecom features (tract grain): the `joined` input prelude.
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
    FROM `clgx-idap-bigquery-prd-a990.edr_ent_common_reference_ext.fcc_copper_fixed_broadband`
    GROUP BY tract_geoid
),
cable_prov AS (
    SELECT
        SUBSTR(block_geoid, 1, 11) AS tract_geoid,
        COUNT(DISTINCT location_id) AS cable_location_count,
        COUNT(DISTINCT provider_id) AS cable_provider_count
    FROM `clgx-idap-bigquery-prd-a990.edr_ent_common_reference_ext.fcc_cable_fixed_broadband`
    GROUP BY tract_geoid
),
fiber_prov AS (
    SELECT
        SUBSTR(block_geoid, 1, 11) AS tract_geoid,
        COUNT(DISTINCT location_id) AS fiber_location_count,
        COUNT(DISTINCT provider_id) AS fiber_provider_count
    FROM `clgx-idap-bigquery-prd-a990.edr_ent_common_reference_ext.fcc_fiber_fixed_broadband`
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
    FROM `clgx-gis-app-dev-06e3.teu_telecom.fcc_coverage_block`
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
,
features AS (
    SELECT
        * EXCEPT(fiber_speed_1000_100_only),
        -- Serviceable fiber requires both a location and a provider present.
        CAST(fiber_location_count > 0 AND fiber_provider_count > 0 AS INT64) AS has_fiber,
        -- SAFE_DIVIDE yields NULL (never infinity) on a zero denominator; fill to the
        -- feature's neutral value so a unit with no housing units carries no signal.
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
    tract_geoid, state_fips,
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
        WHEN 'no_providers' THEN 0
        WHEN 'greenfield' THEN 1
        WHEN 'cable_but_no_fiber' THEN 2
        WHEN 'fiber_entry' THEN 3
        WHEN 'fiber_duopoly' THEN 4
        WHEN 'fiber_competitive' THEN 5
        WHEN 'fiber_saturated' THEN 6
        ELSE NULL
    END AS provider_competitive_landscape_ord
FROM features

