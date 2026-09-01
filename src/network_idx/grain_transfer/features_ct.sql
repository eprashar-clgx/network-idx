-- CT training-frame assembly: join every family's tract output into the training frame.
--
-- This is the tract-grain analogue of parcel_features: the join spine that turns the
-- per-family tract tables into the single wide table the model is fit on, one row per
-- census tract carrying the thirteen model features under their model-column names (the
-- keys of MODEL_TO_SCORING_FEATURE) so train.py can rename them to the canonical scoring
-- names. The model is fit at tract grain and scores parcels, so this frame and
-- parcel_features must carry the same thirteen features (train/score parity).
--
-- The telecom (FCC) tract feature table is the spine: it covers every populated tract (it
-- is rolled up from the FCC coverage block table, which spans every census block with
-- housing) and already carries the state FIPS, the census housing units, and the four
-- telecom features. The growth, rextag-distance, and population tract tables are
-- left-joined so a tract is never dropped when a family has no row for it (e.g. a tract
-- with no growth parcels). This yields all tracts; the training-population filter is
-- applied downstream in modeling, not here.
--
-- Null fills are deliberately NOT applied here — modeling fills each feature per the
-- scoring contract, so this frame preserves genuine missingness. The growth and rextag
-- tables key on `tract_id`; the telecom and population tables key on `tract_geoid`; both
-- are the eleven-digit census-tract GEOID. Housing units are aliased to the model name
-- (`estimated_census_housing_units`) the rename step maps to `census_housing_units`.
CREATE OR REPLACE TABLE `{output_table}`
CLUSTER BY state_fips AS
SELECT
    t.tract_geoid,
    t.state_fips,

    -- ── Growth features (tract medians, from the location growth CT table) ──
    g.median_landuse_change_qtr_mi_cnt,
    g.median_pre_early_dev_qtr_mi_cnt,
    g.median_bldr_dev_qtr_mi_cnt,
    g.median_new_permit_qtr_mi_cnt,
    g.median_dist_nearest_hotspot,

    -- ── Fiber distance (tract median, from the rextag distance CT table) ──
    rf.median_dist_nearest_fiber_miles,

    -- ── Telecom features (identity names, from the telecom CT table) ──
    t.cable_penetration,
    t.fiber_opportunity_gap,
    t.fiber_speed_top_tier,
    t.provider_competitive_landscape_ord,

    -- ── Demographic features: population change (tract) + housing units ──
    d.pop_ch_avg,
    d.pop_pctch_avg,
    t.census_housing_units AS estimated_census_housing_units
FROM `{telecom_features_ct}` AS t
LEFT JOIN `{loc_growth_ct}`      AS g  ON t.tract_geoid = g.tract_id
LEFT JOIN `{rextag_distance_ct}` AS rf ON t.tract_geoid = rf.tract_id
LEFT JOIN `{demo_pop_ct}`        AS d  ON t.tract_geoid = d.tract_geoid
