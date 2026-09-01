-- =============================================================================
-- Min/max scan over feature frame that feeds scaling_params (Python assembles + writes the params table).
-- Module        : network_idx.scoring.build_scaling_params
-- Generated from : src/network_idx/scoring/build_scaling_params.py  (python -m network_idx.scoring.build_scaling_params --dry-run)
-- Run in         : VM (dev-only scan; frame assembled in Python)
--
-- !! PIPELINE-GENERATED ARTIFACT — DO NOT HAND-EDIT.
--    The numeric weights and min/max scaling constants below are baked in from
--    a specific fitted model run (e.g. run_id 'lightgbm_k8_v1'). They change every
--    time the model is refit. Regenerate this file from the module rather than
--    editing values by hand. Shown here so the DE can review the scoring pushdown.
--
-- PROJECT SUBSTITUTION: PROD_PROJECT=clgx-idap-bigquery-prd-a990, DEV_PROJECT=clgx-gis-app-dev-06e3
-- =============================================================================

SELECT
    MIN(COALESCE(`fiber_speed_top_tier`, 0.0)) AS `fiber_speed_top_tier__min`,
    MAX(COALESCE(`fiber_speed_top_tier`, 0.0)) AS `fiber_speed_top_tier__max`,
    MIN(COALESCE(`provider_competitive_landscape_ord`, 0.0)) AS `provider_competitive_landscape_ord__min`,
    MAX(COALESCE(`provider_competitive_landscape_ord`, 0.0)) AS `provider_competitive_landscape_ord__max`,
    MIN(COALESCE(`pop_ch_avg`, 0.0)) AS `pop_ch_avg__min`,
    MAX(COALESCE(`pop_ch_avg`, 0.0)) AS `pop_ch_avg__max`,
    MIN(COALESCE(`pop_pctch_avg`, 0.0)) AS `pop_pctch_avg__min`,
    MAX(COALESCE(`pop_pctch_avg`, 0.0)) AS `pop_pctch_avg__max`,
    MIN(COALESCE(`census_housing_units`, 0.0)) AS `census_housing_units__min`,
    MAX(COALESCE(`census_housing_units`, 0.0)) AS `census_housing_units__max`,
    MIN(COALESCE(`landuse_change_qtr_mi_cnt`, 0.0)) AS `landuse_change_qtr_mi_cnt__min`,
    APPROX_QUANTILES(COALESCE(`landuse_change_qtr_mi_cnt`, 0.0), 1000)[OFFSET(995)] AS `landuse_change_qtr_mi_cnt__winmax`,
    MIN(COALESCE(`pre_early_dev_qtr_mi_cnt`, 0.0)) AS `pre_early_dev_qtr_mi_cnt__min`,
    APPROX_QUANTILES(COALESCE(`pre_early_dev_qtr_mi_cnt`, 0.0), 1000)[OFFSET(995)] AS `pre_early_dev_qtr_mi_cnt__winmax`,
    MIN(COALESCE(`bldr_dev_qtr_mi_cnt`, 0.0)) AS `bldr_dev_qtr_mi_cnt__min`,
    APPROX_QUANTILES(COALESCE(`bldr_dev_qtr_mi_cnt`, 0.0), 1000)[OFFSET(995)] AS `bldr_dev_qtr_mi_cnt__winmax`,
    MIN(COALESCE(`new_permit_qtr_mi_cnt`, 0.0)) AS `new_permit_qtr_mi_cnt__min`,
    APPROX_QUANTILES(COALESCE(`new_permit_qtr_mi_cnt`, 0.0), 1000)[OFFSET(995)] AS `new_permit_qtr_mi_cnt__winmax`,
    MIN(`dist_to_nearest_hotspot_miles`) AS `dist_to_nearest_hotspot_miles__min`,
    MAX(`dist_to_nearest_hotspot_miles`) AS `dist_to_nearest_hotspot_miles__rawmax`,
    APPROX_QUANTILES(`dist_to_nearest_hotspot_miles`, 100)[OFFSET(99)] AS `dist_to_nearest_hotspot_miles__p99`,
    MIN(`dist_to_nearest_fiber_miles`) AS `dist_to_nearest_fiber_miles__min`,
    MAX(`dist_to_nearest_fiber_miles`) AS `dist_to_nearest_fiber_miles__rawmax`,
    APPROX_QUANTILES(`dist_to_nearest_fiber_miles`, 100)[OFFSET(99)] AS `dist_to_nearest_fiber_miles__p99`
FROM `clgx-gis-app-dev-06e3.teu_features.parcel_features`
