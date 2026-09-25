-- =============================================================================
-- Final parcel scoring pushdown: normalise, weight, recombine into 0-100 indices and overall score.
-- Module         : network_idx.scoring.parcel_score
-- Console step   : 18
-- Run in         : VM or console (dev write; reads dev feature frame)
--
-- !! PIPELINE-GENERATED ARTIFACT — DO NOT HAND-EDIT.
--    The weights and min/max scaling constants below are baked in from
--    run_id 'lightgbm_k8_v2'. They change every time the model is refit.
--    Regenerate with:
--      poetry run python scripts/generate_scoring_sql.py
--
-- PROJECT SUBSTITUTION: PROD_PROJECT=clgx-idap-bigquery-prd-a990, DEV_PROJECT=clgx-gis-app-dev-06e3
-- =============================================================================

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_outputs.parcel_scores`
CLUSTER BY block_geoid AS
WITH subidx AS (
  SELECT
    parcel_shape_id, block_geoid, tract_geoid,
    (0.021770611137199806 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`landuse_change_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(`landuse_change_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(`landuse_change_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 113.0) - 0.0) / 113.0)
      + (0.06426807842254523 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`pre_early_dev_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(`pre_early_dev_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(`pre_early_dev_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 1158.0) - 0.0) / 1158.0)
      + (0.026525209139549408 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`bldr_dev_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(`bldr_dev_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(`bldr_dev_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 243.0) - 0.0) / 243.0)
      + (0.06114772679338768 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`new_permit_qtr_mi_cnt` AS FLOAT64)) OR IS_INF(CAST(`new_permit_qtr_mi_cnt` AS FLOAT64)), NULL, CAST(`new_permit_qtr_mi_cnt` AS FLOAT64)), 0.0), 0.0), 141.0) - 0.0) / 141.0)
      + (0.826288374507318 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`dist_to_nearest_hotspot_miles` AS FLOAT64)) OR IS_INF(CAST(`dist_to_nearest_hotspot_miles` AS FLOAT64)), NULL, CAST(`dist_to_nearest_hotspot_miles` AS FLOAT64)), 18.749921820071947), 0.0), 18.749921820071947) - 0.0) / 18.749921820071947))) AS raw_growth,
    (0.1820512176357995 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`cable_penetration` AS FLOAT64)) OR IS_INF(CAST(`cable_penetration` AS FLOAT64)), NULL, CAST(`cable_penetration` AS FLOAT64)), 0.0), 0.0), 1.0) - 0.0) / 1.0)))
      + (0.2823211324507433 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`fiber_opportunity_gap` AS FLOAT64)) OR IS_INF(CAST(`fiber_opportunity_gap` AS FLOAT64)), NULL, CAST(`fiber_opportunity_gap` AS FLOAT64)), 1.0), 0.0), 1.0) - 0.0) / 1.0)
      + (0.2516072297916633 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`fiber_speed_top_tier` AS FLOAT64)) OR IS_INF(CAST(`fiber_speed_top_tier` AS FLOAT64)), NULL, CAST(`fiber_speed_top_tier` AS FLOAT64)), 0.0), 0.0), 1.0) - 0.0) / 1.0)))
      + (0.1501001774546392 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`dist_to_nearest_fiber_miles` AS FLOAT64)) OR IS_INF(CAST(`dist_to_nearest_fiber_miles` AS FLOAT64)), NULL, CAST(`dist_to_nearest_fiber_miles` AS FLOAT64)), 12.577777836696999), 7.319728227357395e-10), 12.577777836696999) - 7.319728227357395e-10) / 12.577777835965026)))
      + (0.1339202426671548 * (1.0 - ((LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`provider_competitive_landscape_ord` AS FLOAT64)) OR IS_INF(CAST(`provider_competitive_landscape_ord` AS FLOAT64)), NULL, CAST(`provider_competitive_landscape_ord` AS FLOAT64)), 0.0), 0.0), 6.0) - 0.0) / 6.0))) AS raw_telecom,
    (0.4757783356098997 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`pop_ch_avg` AS FLOAT64)) OR IS_INF(CAST(`pop_ch_avg` AS FLOAT64)), NULL, CAST(`pop_ch_avg` AS FLOAT64)), 0.0), -15579.0), 42473.0) - -15579.0) / 58052.0)
      + (0.3297735539722515 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`pop_pctch_avg` AS FLOAT64)) OR IS_INF(CAST(`pop_pctch_avg` AS FLOAT64)), NULL, CAST(`pop_pctch_avg` AS FLOAT64)), 0.0), -33.33), 7.27) - -33.33) / 40.599999999999994)
      + (0.19444811041784882 * (LEAST(GREATEST(COALESCE(IF(IS_NAN(CAST(`census_housing_units` AS FLOAT64)) OR IS_INF(CAST(`census_housing_units` AS FLOAT64)), NULL, CAST(`census_housing_units` AS FLOAT64)), 0.0), 0.0), 5432.0) - 0.0) / 5432.0) AS raw_demo
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
  SELECT *, (0.19983419199351915 * idx_growth + 0.5620505956048739 * idx_telecom + 0.23811521240160702 * idx_demo) AS raw_overall FROM rescaled
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
  'lightgbm_k8_v2' AS run_id,
  CURRENT_TIMESTAMP() AS created_at
FROM overall o CROSS JOIN obounds ob
