-- =============================================================================
-- Demographic population-change features.
-- Module        : network_idx.features.demographic.engineered.population_change
-- Generated from : src/network_idx/features/demographic/engineered/population_change.py  (python -m network_idx.features.demographic.engineered.population_change --dry-run)
-- Run in         : CONSOLE (reads PROD)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_features.demo_pop_ct` AS

WITH ns_q3 AS (
  SELECT 
    ct_key, 
    year, 
    pop_est_10mile
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_neighborhood.neighborhood_scout_census_tract`
  WHERE quarter = 3 
),
max_yr AS (
  SELECT MAX(year) AS yr FROM ns_q3
)

SELECT 
  a.ct_key AS tract_geoid,
  -- absolute change (1 year)
  (a.pop_est_10mile - b.pop_est_10mile) AS pop_ch_1yr,
  -- average annual absolute change since 2022
  round(safe_divide(a.pop_est_10mile - c.pop_est_10mile, m.yr - 2022)) AS pop_ch_avg,
  -- percentage change (1 year)
  round(100 * safe_divide(a.pop_est_10mile - b.pop_est_10mile, b.pop_est_10mile), 2) AS pop_pctch_1yr,
  -- average annual percentage change since 2022
  round(safe_divide(
    100 * safe_divide(a.pop_est_10mile - c.pop_est_10mile, c.pop_est_10mile),
    m.yr - 2022
  ), 2) AS pop_pctch_avg

FROM ns_q3 a
CROSS JOIN max_yr m
LEFT JOIN ns_q3 b
  ON a.ct_key = b.ct_key
  AND b.year = m.yr - 1
LEFT JOIN ns_q3 c
  ON a.ct_key = c.ct_key
  AND c.year = 2022
WHERE a.year = m.yr
