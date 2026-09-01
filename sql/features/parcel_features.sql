-- =============================================================================
-- Final parcel-grain analysis frame assembly.
-- Module        : network_idx.features.parcel_features
-- Generated from : src/network_idx/features/parcel_features.py  (python -m network_idx.features.parcel_features --dry-run)
-- Run in         : VM (dev-only, BQ-validated)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

-- Parcel feature assembly: join every family's output into the parcel-grain scoring input.
--
-- This is the join spine that turns the per-family feature tables into the single wide table
-- the scorer reads: one row per parcel carrying the thirteen model features. The spine is the
-- parcel growth-counts table, which already holds one row per parcel; the other tables are
-- left-joined so a parcel is never dropped when a family has no row for it. The families sit
-- at three different grains and are bridged here: the growth counts and both distance tables
-- are already at parcel grain and join on the parcel id; the telecom features and the block
-- housing-unit count are at block grain and join on the parcel's block GEOID; the population
-- change is at tract grain and joins on the parcel's tract GEOID, taken as the first eleven
-- digits of the block GEOID.
--
-- Null fills are deliberately NOT applied here — the scaling step fills and winsorises each
-- feature according to the scoring contract, so this table preserves genuine missingness. The
-- distance columns already hold miles (converted upstream), so they are carried through
-- unchanged, never divided. Housing units are taken at block grain while population change
-- stays at tract grain, a documented grain divergence that is acceptable for the model.
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_features.parcel_features` AS
SELECT
    p.parcel_shape_id,
    p.block_id AS block_geoid,
    SUBSTR(p.block_id, 1, 11) AS tract_geoid,

    -- ── Growth features (parcel grain) ──
    p.landuse_change_qtr_mi_cnt,
    p.pre_early_dev_qtr_mi_cnt,
    p.bldr_dev_qtr_mi_cnt,
    p.new_permit_qtr_mi_cnt,
    hs.dist_to_nearest_hotspot_miles,

    -- ── Telecom features (block grain) ──
    t.cable_penetration,
    t.fiber_opportunity_gap,
    t.fiber_speed_top_tier,
    t.provider_competitive_landscape_ord,
    rf.dist_to_nearest_fiber_miles,
    rf.nearest_fiber_id,

    -- ── Demographic features: population change (tract) + housing units (block) ──
    d.pop_ch_avg,
    d.pop_pctch_avg,
    t.census_housing_units
FROM `clgx-gis-app-dev-06e3.teu_features.loc_growth_cnts_parcel` AS p
LEFT JOIN `clgx-gis-app-dev-06e3.teu_features.rextag_distance_parcel`  AS rf ON p.parcel_shape_id = rf.parcel_shape_id
LEFT JOIN `clgx-gis-app-dev-06e3.teu_features.loc_growth_distance_parcel` AS hs ON p.parcel_shape_id = hs.parcel_shape_id
LEFT JOIN `clgx-gis-app-dev-06e3.teu_features.telecom_features_block`    AS t  ON p.block_id = t.block_geoid
LEFT JOIN `clgx-gis-app-dev-06e3.teu_features.demo_pop_ct`       AS d  ON SUBSTR(p.block_id, 1, 11) = d.tract_geoid

