-- =============================================================================
-- Customer delivery table: raw + scaled features, weights, indices and geo, with customer-facing column names.
-- Module         : network_idx.scoring.parcel_score (run_delivery)
-- Console step   : 19
-- Run in         : VM or console (dev write; requires parcel_scores from step 18)
--
-- !! PIPELINE-GENERATED ARTIFACT — DO NOT HAND-EDIT.
--    The weights and min/max scaling constants below are baked in from
--    run_id 'lightgbm_k8_v2'. They change every time the model is refit.
--    Regenerate with:
--      poetry run python scripts/generate_scoring_sql.py
--
-- PROJECT SUBSTITUTION: PROD_PROJECT=clgx-idap-bigquery-prd-a990, DEV_PROJECT=clgx-gis-app-dev-06e3
-- =============================================================================

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_outputs.fiber_idx_v1_parcel`
CLUSTER BY census_block_id AS
SELECT
  pf.parcel_shape_id,
  g.parcel_polygon AS geometry,
  pf.block_geoid AS census_block_id,
  g.h3_res8 AS h3_id,
  CAST(pf.nearest_fiber_id AS STRING) AS nearest_fiber_id,
  ROUND(ps.idx_demo, 2) AS demographic_index,
  ROUND(ps.idx_growth, 2) AS growth_index,
  ROUND(ps.idx_telecom, 2) AS telecom_index,
  ROUND(ps.idx_overall, 2) AS fiber_potential_index,
  23.81 AS demographic_weight,
  19.98 AS growth_weight,
  56.21 AS telecom_weight,
  CAST(ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`landuse_change_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(pf.`landuse_change_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(pf.`landuse_change_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 113.0)) AS INT64) AS landuse_change_qtr_mi_cnt,
  CAST(ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`pre_early_dev_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(pf.`pre_early_dev_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(pf.`pre_early_dev_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 1158.0)) AS INT64) AS pre_early_dev_qtr_mi_cnt,
  CAST(ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`bldr_dev_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(pf.`bldr_dev_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(pf.`bldr_dev_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 243.0)) AS INT64) AS bldr_dev_qtr_mi_cnt,
  CAST(ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`new_permit_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(pf.`new_permit_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(pf.`new_permit_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 141.0)) AS INT64) AS new_permit_qtr_mi_cnt,
  ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`dist_to_nearest_hotspot_miles` AS FLOAT64)) OR IS_INF(CAST(pf.`dist_to_nearest_hotspot_miles` AS FLOAT64)), NULL, CAST(pf.`dist_to_nearest_hotspot_miles` AS FLOAT64)), 18.749921820071947), 0.0), 18.749921820071947), 4) AS dist_nearest_hotspot_miles,
  ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`cable_penetration` AS FLOAT64)) OR IS_INF(CAST(pf.`cable_penetration` AS FLOAT64)), NULL, CAST(pf.`cable_penetration` AS FLOAT64)), 0.0), 0.0), 1.0), 4) AS cable_penetration,
  ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`fiber_opportunity_gap` AS FLOAT64)) OR IS_INF(CAST(pf.`fiber_opportunity_gap` AS FLOAT64)), NULL, CAST(pf.`fiber_opportunity_gap` AS FLOAT64)), 1.0), 0.0), 1.0), 4) AS fiber_opportunity_gap,
  ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`fiber_speed_top_tier` AS FLOAT64)) OR IS_INF(CAST(pf.`fiber_speed_top_tier` AS FLOAT64)), NULL, CAST(pf.`fiber_speed_top_tier` AS FLOAT64)), 0.0), 0.0), 1.0), 4) AS fiber_speed_top_tier,
  ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`dist_to_nearest_fiber_miles` AS FLOAT64)) OR IS_INF(CAST(pf.`dist_to_nearest_fiber_miles` AS FLOAT64)), NULL, CAST(pf.`dist_to_nearest_fiber_miles` AS FLOAT64)), 12.577777836696999), 7.319728227357395e-10), 12.577777836696999), 4) AS dist_nearest_fiber_miles,
  LPAD(CAST(COALESCE(CAST(pf.`provider_competitive_landscape_ord` AS INT64), 0) AS STRING), 2, '0') AS provider_competitive_landscape_type_code,
  ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`pop_ch_avg` AS FLOAT64)) OR IS_INF(CAST(pf.`pop_ch_avg` AS FLOAT64)), NULL, CAST(pf.`pop_ch_avg` AS FLOAT64)), 0.0), -15579.0), 42473.0), 4) AS pop_ch_avg,
  ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`pop_pctch_avg` AS FLOAT64)) OR IS_INF(CAST(pf.`pop_pctch_avg` AS FLOAT64)), NULL, CAST(pf.`pop_pctch_avg` AS FLOAT64)), 0.0), -33.33), 7.27), 4) AS pop_pctch_avg,
  CAST(ROUND(LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`census_housing_units` AS FLOAT64)) OR IS_INF(CAST(pf.`census_housing_units` AS FLOAT64)), NULL, CAST(pf.`census_housing_units` AS FLOAT64)), 0.0), 0.0), 5432.0)) AS INT64) AS census_housing_units,
  2.18 AS landuse_change_qtr_mi_cnt_weight,
  6.43 AS pre_early_dev_qtr_mi_cnt_weight,
  2.65 AS bldr_dev_qtr_mi_cnt_weight,
  6.11 AS new_permit_qtr_mi_cnt_weight,
  82.63 AS dist_nearest_hotspot_miles_weight,
  18.21 AS cable_penetration_weight,
  28.23 AS fiber_opportunity_gap_weight,
  25.16 AS fiber_speed_top_tier_weight,
  15.01 AS dist_nearest_fiber_miles_weight,
  13.39 AS provider_competitive_landscape_type_code_weight,
  47.58 AS pop_ch_avg_weight,
  32.98 AS pop_pctch_avg_weight,
  19.44 AS census_housing_units_weight,
  ROUND((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`landuse_change_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(pf.`landuse_change_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(pf.`landuse_change_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 113.0) - 0.0) / 113.0, 4) AS landuse_change_qtr_mi_cnt_scaled,
  ROUND((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`pre_early_dev_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(pf.`pre_early_dev_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(pf.`pre_early_dev_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 1158.0) - 0.0) / 1158.0, 4) AS pre_early_dev_qtr_mi_cnt_scaled,
  ROUND((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`bldr_dev_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(pf.`bldr_dev_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(pf.`bldr_dev_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 243.0) - 0.0) / 243.0, 4) AS bldr_dev_qtr_mi_cnt_scaled,
  ROUND((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`new_permit_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(pf.`new_permit_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(pf.`new_permit_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 141.0) - 0.0) / 141.0, 4) AS new_permit_qtr_mi_cnt_scaled,
  ROUND((1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`dist_to_nearest_hotspot_miles` AS FLOAT64)) OR IS_INF(CAST(pf.`dist_to_nearest_hotspot_miles` AS FLOAT64)), NULL, CAST(pf.`dist_to_nearest_hotspot_miles` AS FLOAT64)), 18.749921820071947), 0.0), 18.749921820071947) - 0.0) / 18.749921820071947)), 4) AS dist_nearest_hotspot_miles_scaled,
  ROUND((1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`cable_penetration` AS FLOAT64)) OR IS_INF(CAST(pf.`cable_penetration` AS FLOAT64)), NULL, CAST(pf.`cable_penetration` AS FLOAT64)), 0.0), 0.0), 1.0) - 0.0) / 1.0)), 4) AS cable_penetration_scaled,
  ROUND((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`fiber_opportunity_gap` AS FLOAT64)) OR IS_INF(CAST(pf.`fiber_opportunity_gap` AS FLOAT64)), NULL, CAST(pf.`fiber_opportunity_gap` AS FLOAT64)), 1.0), 0.0), 1.0) - 0.0) / 1.0, 4) AS fiber_opportunity_gap_scaled,
  ROUND((1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`fiber_speed_top_tier` AS FLOAT64)) OR IS_INF(CAST(pf.`fiber_speed_top_tier` AS FLOAT64)), NULL, CAST(pf.`fiber_speed_top_tier` AS FLOAT64)), 0.0), 0.0), 1.0) - 0.0) / 1.0)), 4) AS fiber_speed_top_tier_scaled,
  ROUND((1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`dist_to_nearest_fiber_miles` AS FLOAT64)) OR IS_INF(CAST(pf.`dist_to_nearest_fiber_miles` AS FLOAT64)), NULL, CAST(pf.`dist_to_nearest_fiber_miles` AS FLOAT64)), 12.577777836696999), 7.319728227357395e-10), 12.577777836696999) - 7.319728227357395e-10) / 12.577777835965026)), 4) AS dist_nearest_fiber_miles_scaled,
  ROUND((1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`provider_competitive_landscape_ord` AS FLOAT64)) OR IS_INF(CAST(pf.`provider_competitive_landscape_ord` AS FLOAT64)), NULL, CAST(pf.`provider_competitive_landscape_ord` AS FLOAT64)), 0.0), 0.0), 6.0) - 0.0) / 6.0)), 4) AS provider_competitive_landscape_type_code_scaled,
  ROUND((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`pop_ch_avg` AS FLOAT64)) OR IS_INF(CAST(pf.`pop_ch_avg` AS FLOAT64)), NULL, CAST(pf.`pop_ch_avg` AS FLOAT64)), 0.0), -15579.0), 42473.0) - -15579.0) / 58052.0, 4) AS pop_ch_avg_scaled,
  ROUND((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`pop_pctch_avg` AS FLOAT64)) OR IS_INF(CAST(pf.`pop_pctch_avg` AS FLOAT64)), NULL, CAST(pf.`pop_pctch_avg` AS FLOAT64)), 0.0), -33.33), 7.27) - -33.33) / 40.599999999999994, 4) AS pop_pctch_avg_scaled,
  ROUND((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(pf.`census_housing_units` AS FLOAT64)) OR IS_INF(CAST(pf.`census_housing_units` AS FLOAT64)), NULL, CAST(pf.`census_housing_units` AS FLOAT64)), 0.0), 0.0), 5432.0) - 0.0) / 5432.0, 4) AS census_housing_units_scaled,
  g.growth_parcel_qtr_mi_cnt AS growth_parcels_qtr_mi_cnt,
  'lightgbm_k8_v2' AS run_id,
  CURRENT_TIMESTAMP() AS created_at
FROM `clgx-gis-app-dev-06e3.teu_features.parcel_features` pf
LEFT JOIN `clgx-gis-app-dev-06e3.teu_outputs.parcel_scores` ps ON ps.parcel_shape_id = pf.parcel_shape_id
LEFT JOIN `clgx-gis-app-dev-06e3.teu_features.loc_growth_cnts_parcel` g  ON g.parcel_shape_id  = pf.parcel_shape_id
