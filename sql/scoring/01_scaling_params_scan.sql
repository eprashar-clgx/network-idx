-- =============================================================================
-- Min/max + winsorize-quantile scan over the parcel feature frame that feeds scaling_params (Python assembles and writes the params table).
-- Module         : network_idx.scoring.scaling (build_stats_query)
-- Console step   : 17 (review only)
-- Run in         : Review only — run_training executes this scan in-process
--
-- !! PIPELINE-GENERATED ARTIFACT — DO NOT HAND-EDIT.
--    The weights and min/max scaling constants below are baked in from
--    run_id 'lightgbm_k8_v2'. They change every time the model is refit.
--    Regenerate with:
--      poetry run python scripts/generate_scoring_sql.py
--
-- PROJECT SUBSTITUTION: PROD_PROJECT=clgx-idap-bigquery-prd-a990, DEV_PROJECT=clgx-gis-app-dev-06e3
-- =============================================================================

SELECT
    MIN(COALESCE(`fiber_speed_top_tier`, 0.0)) AS `fiber_speed_top_tier__min`,
    MAX(COALESCE(`fiber_speed_top_tier`, 0.0)) AS `fiber_speed_top_tier__max`,
    MIN(COALESCE(`provider_competitive_landscape_ord`, 0.0)) AS `provider_competitive_landscape_ord__min`,
    MAX(COALESCE(`provider_competitive_landscape_ord`, 0.0)) AS `provider_competitive_landscape_ord__max`,
    MIN(COALESCE(`census_housing_units`, 0.0)) AS `census_housing_units__min`,
    MAX(COALESCE(`census_housing_units`, 0.0)) AS `census_housing_units__max`,
    MIN(COALESCE(`landuse_change_qtr_mi_cnt`, 0.0)) AS `landuse_change_qtr_mi_cnt__min`,
    APPROX_QUANTILES(COALESCE(`landuse_change_qtr_mi_cnt`, 0.0), 10000)[OFFSET(9999)] AS `landuse_change_qtr_mi_cnt__winmax`,
    MIN(COALESCE(`pre_early_dev_qtr_mi_cnt`, 0.0)) AS `pre_early_dev_qtr_mi_cnt__min`,
    APPROX_QUANTILES(COALESCE(`pre_early_dev_qtr_mi_cnt`, 0.0), 10000)[OFFSET(9999)] AS `pre_early_dev_qtr_mi_cnt__winmax`,
    MIN(COALESCE(`bldr_dev_qtr_mi_cnt`, 0.0)) AS `bldr_dev_qtr_mi_cnt__min`,
    APPROX_QUANTILES(COALESCE(`bldr_dev_qtr_mi_cnt`, 0.0), 10000)[OFFSET(9999)] AS `bldr_dev_qtr_mi_cnt__winmax`,
    MIN(COALESCE(`new_permit_qtr_mi_cnt`, 0.0)) AS `new_permit_qtr_mi_cnt__min`,
    APPROX_QUANTILES(COALESCE(`new_permit_qtr_mi_cnt`, 0.0), 10000)[OFFSET(9999)] AS `new_permit_qtr_mi_cnt__winmax`,
    MIN(COALESCE(`pop_ch_avg`, 0.0)) AS `pop_ch_avg__min`,
    APPROX_QUANTILES(COALESCE(`pop_ch_avg`, 0.0), 1000)[OFFSET(999)] AS `pop_ch_avg__winmax`,
    MIN(COALESCE(`pop_pctch_avg`, 0.0)) AS `pop_pctch_avg__min`,
    APPROX_QUANTILES(COALESCE(`pop_pctch_avg`, 0.0), 1000)[OFFSET(999)] AS `pop_pctch_avg__winmax`,
    MIN(`dist_to_nearest_hotspot_miles`) AS `dist_to_nearest_hotspot_miles__min`,
    MAX(`dist_to_nearest_hotspot_miles`) AS `dist_to_nearest_hotspot_miles__rawmax`,
    APPROX_QUANTILES(`dist_to_nearest_hotspot_miles`, 100)[OFFSET(99)] AS `dist_to_nearest_hotspot_miles__p99`,
    MIN(`dist_to_nearest_fiber_miles`) AS `dist_to_nearest_fiber_miles__min`,
    MAX(`dist_to_nearest_fiber_miles`) AS `dist_to_nearest_fiber_miles__rawmax`,
    APPROX_QUANTILES(`dist_to_nearest_fiber_miles`, 100)[OFFSET(99)] AS `dist_to_nearest_fiber_miles__p99`
FROM `clgx-gis-app-dev-06e3.teu_features.parcel_features`
