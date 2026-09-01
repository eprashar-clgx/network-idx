-- =============================================================================
-- Final parcel scoring pushdown: normalise, weight, recombine into 0-100 indices and overall score.
-- Module        : network_idx.scoring.parcel_score
-- Generated from : src/network_idx/scoring/parcel_score.py  (python -m network_idx.scoring.parcel_score --dry-run)
-- Run in         : VM (dev write; reads dev feature frame)
--
-- !! PIPELINE-GENERATED ARTIFACT — DO NOT HAND-EDIT.
--    The numeric weights and min/max scaling constants below are baked in from
--    a specific fitted model run (e.g. run_id 'lightgbm_k8_v1'). They change every
--    time the model is refit. Regenerate this file from the module rather than
--    editing values by hand. Shown here so the DE can review the scoring pushdown.
--
-- PROJECT SUBSTITUTION: PROD_PROJECT=clgx-idap-bigquery-prd-a990, DEV_PROJECT=clgx-gis-app-dev-06e3
-- =============================================================================

Error: Credentials file not found
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_outputs.parcel_scores`
CLUSTER BY block_geoid AS
WITH subidx AS (
  SELECT
    parcel_shape_id, block_geoid, tract_geoid,
    (0.09401710446626664 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`landuse_change_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(`landuse_change_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(`landuse_change_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 12.0) - 0.0) / 12.0)
      + (0.16317111192524045 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`pre_early_dev_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(`pre_early_dev_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(`pre_early_dev_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 280.0) - 0.0) / 280.0)
      + (0.09400739028567914 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`bldr_dev_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(`bldr_dev_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(`bldr_dev_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 33.0) - 0.0) / 33.0)
      + (0.14459864751052942 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`new_permit_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(`new_permit_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(`new_permit_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 22.0) - 0.0) / 22.0)
      + (0.5042057458122844 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`dist_to_nearest_hotspot_miles` AS FLOAT64)) OR IS_INF(CAST(`dist_to_nearest_hotspot_miles` AS FLOAT64)), NULL, CAST(`dist_to_nearest_hotspot_miles` AS FLOAT64)), 18.74992177175413), 0.0), 18.74992177175413) - 0.0) / 18.74992177175413))) AS raw_growth,
    (0.11043858818121391 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`cable_penetration` AS FLOAT64)) OR IS_INF(CAST(`cable_penetration` AS FLOAT64)), NULL, CAST(`cable_penetration` AS FLOAT64)), 0.0), 0.0), 1.0) - 0.0) / 1.0)))
      + (0.3056419656344645 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`fiber_opportunity_gap` AS FLOAT64)) OR IS_INF(CAST(`fiber_opportunity_gap` AS FLOAT64)), NULL, CAST(`fiber_opportunity_gap` AS FLOAT64)), 1.0), 0.0), 1.0) - 0.0) / 1.0)
      + (0.2702282628935786 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`fiber_speed_top_tier` AS FLOAT64)) OR IS_INF(CAST(`fiber_speed_top_tier` AS FLOAT64)), NULL, CAST(`fiber_speed_top_tier` AS FLOAT64)), 0.0), 0.0), 1.0) - 0.0) / 1.0)))
      + (0.16178931707799668 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`dist_to_nearest_fiber_miles` AS FLOAT64)) OR IS_INF(CAST(`dist_to_nearest_fiber_miles` AS FLOAT64)), NULL, CAST(`dist_to_nearest_fiber_miles` AS FLOAT64)), 12.825026485183868), 7.316986032802666e-10), 12.825026485183868) - 7.316986032802666e-10) / 12.825026484452168)))
      + (0.1519018662127464 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`provider_competitive_landscape_ord` AS FLOAT64)) OR IS_INF(CAST(`provider_competitive_landscape_ord` AS FLOAT64)), NULL, CAST(`provider_competitive_landscape_ord` AS FLOAT64)), 0.0), 0.0), 6.0) - 0.0) / 6.0))) AS raw_telecom,
    (0.3844095079143061 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`pop_ch_avg` AS FLOAT64)) OR IS_INF(CAST(`pop_ch_avg` AS FLOAT64)), NULL, CAST(`pop_ch_avg` AS FLOAT64)), 0.0), -15579.0), 2290917.0) - -15579.0) / 2306496.0)
      + (0.3858619247170955 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`pop_pctch_avg` AS FLOAT64)) OR IS_INF(CAST(`pop_pctch_avg` AS FLOAT64)), NULL, CAST(`pop_pctch_avg` AS FLOAT64)), 0.0), -33.33), 34.96) - -33.33) / 68.28999999999999)
      + (0.22972856736859834 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`census_housing_units` AS FLOAT64)) OR IS_INF(CAST(`census_housing_units` AS FLOAT64)), NULL, CAST(`census_housing_units` AS FLOAT64)), 0.0), 0.0), 5432.0) - 0.0) / 5432.0) AS raw_demo
  FROM `clgx-gis-app-dev-06e3.teu_features.parcel_features`
),
bounds AS (
  SELECT
    MIN(raw_growth) AS g_min, MAX(raw_growth) AS g_max,
    MIN(raw_telecom) AS t_min, MAX(raw_telecom) AS t_max,
    MIN(raw_demo) AS d_min, MAX(raw_demo) AS d_max
  FROM subidx
),
rescaled AS (
  SELECT
    s.parcel_shape_id, s.block_geoid, s.tract_geoid,
    s.raw_growth, s.raw_telecom, s.raw_demo,
    CASE WHEN (b.g_max - b.g_min) > 0
         THEN 100.0 * (s.raw_growth - b.g_min) / (b.g_max - b.g_min) ELSE 0.0 END AS idx_growth,
    CASE WHEN (b.t_max - b.t_min) > 0
         THEN 100.0 * (s.raw_telecom - b.t_min) / (b.t_max - b.t_min) ELSE 0.0 END AS idx_telecom,
    CASE WHEN (b.d_max - b.d_min) > 0
         THEN 100.0 * (s.raw_demo - b.d_min) / (b.d_max - b.d_min) ELSE 0.0 END AS idx_demo
  FROM subidx s CROSS JOIN bounds b
),
overall AS (
  SELECT *, (0.16874182668419666 * idx_growth + 0.5914783052023382 * idx_telecom + 0.2397798681134651 * idx_demo) AS raw_overall FROM rescaled
),
obounds AS (
  SELECT MIN(raw_overall) AS o_min, MAX(raw_overall) AS o_max FROM overall
)
SELECT
  o.parcel_shape_id, o.block_geoid, o.tract_geoid,
  ROUND(o.raw_growth, 4) AS raw_growth,
  ROUND(o.raw_telecom, 4) AS raw_telecom,
  ROUND(o.raw_demo, 4) AS raw_demo,
  ROUND(o.idx_growth, 2) AS idx_growth,
  ROUND(o.idx_telecom, 2) AS idx_telecom,
  ROUND(o.idx_demo, 2) AS idx_demo,
  ROUND(CASE WHEN (ob.o_max - ob.o_min) > 0
             THEN 100.0 * (o.raw_overall - ob.o_min) / (ob.o_max - ob.o_min)
             ELSE 0.0 END, 2) AS idx_overall,
  'lightgbm_k8_v1' AS run_id,
  CURRENT_TIMESTAMP() AS created_at
FROM overall o CROSS JOIN obounds ob
