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
CREATE OR REPLACE TABLE `{output_table}`{cluster_clause} AS
WITH {joined_prelude},
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
    {key_columns},
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
