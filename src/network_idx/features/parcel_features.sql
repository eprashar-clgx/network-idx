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
CREATE OR REPLACE TABLE `{output_table}` AS
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
FROM `{parcel_growth_table}` AS p
LEFT JOIN `{rextag_distance_table}`  AS rf ON p.parcel_shape_id = rf.parcel_shape_id
LEFT JOIN `{hotspot_distance_table}` AS hs ON p.parcel_shape_id = hs.parcel_shape_id
LEFT JOIN `{telecom_block_table}`    AS t  ON p.block_id = t.block_geoid
LEFT JOIN `{demo_tract_table}`       AS d  ON SUBSTR(p.block_id, 1, 11) = d.tract_geoid
