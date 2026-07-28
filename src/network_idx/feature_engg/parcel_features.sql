-- Assemble the 13 scoring features at parcel grain (the join spine for scoring).
-- Spine = loc_growth_cnts_parcel (1 row per parcel). Telecom + housing units join
-- on block; pop change joins on tract (block_id[:11]). NA fills are NOT applied
-- here — scaling.apply_scaling() handles them per SCALING_NA_FILL_RULES.
--
-- Grain notes:
--   * Growth median_* names hold parcel raw counts (model was trained on tract medians).
--   * estimated_census_housing_units = block census_housing_units (decision: block grain;
--     pop_ch/pctch stay tract-level — documented divergence, fine for modelling).
CREATE OR REPLACE TABLE `{output_table}` AS
SELECT
    p.parcel_shape_id,
    p.block_id AS block_geoid,
    SUBSTR(p.block_id, 1, 11) AS tract_geoid,

    -- ── Growth (parcel grain) ──
    p.landuse_change_qtr_mi_cnt,
    p.pre_early_dev_qtr_mi_cnt,
    p.bldr_dev_qtr_mi_cnt,
    p.new_permit_qtr_mi_cnt,
    -- Distances: base `_m` columns already hold MILES (converted upstream in
    -- loc_growth_distance / rextag_distance); we only rename here — do NOT divide.
    hs.dist_to_nearest_hotspot_m AS dist_to_nearest_hotspot_miles,

    -- ── Telecom (block grain) ──
    t.cable_penetration,
    t.fiber_opportunity_gap,
    t.fiber_speed_top_tier,
    t.provider_competitive_landscape_ord,
    rf.dist_to_nearest_fiber_m AS dist_to_nearest_fiber_miles,
    rf.nearest_fiber_id,

    -- ── Demo: pop change (tract) + housing units (block) ──
    d.pop_ch_avg,
    d.pop_pctch_avg,
    t.census_housing_units
FROM `{parcel_growth_table}` AS p
LEFT JOIN `{rextag_distance_table}`  AS rf ON p.parcel_shape_id = rf.parcel_shape_id
LEFT JOIN `{hotspot_distance_table}` AS hs ON p.parcel_shape_id = hs.parcel_shape_id
LEFT JOIN `{telecom_block_table}`    AS t  ON p.block_id = t.block_geoid
LEFT JOIN `{demo_tract_table}`       AS d  ON SUBSTR(p.block_id, 1, 11) = d.tract_geoid