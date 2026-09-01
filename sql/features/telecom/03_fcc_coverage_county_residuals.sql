-- =============================================================================
-- County coverage residuals vs expected.
-- Module        : network_idx.features.telecom.transform.fcc_coverage_county_residuals
-- Generated from : src/network_idx/features/telecom/transform/fcc_coverage_county_residuals.py  (python -m network_idx.features.telecom.transform.fcc_coverage_county_residuals --dry-run)
-- Run in         : CONSOLE (reads PROD + census)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

-- FCC coverage county residuals: the county-level coverage not explained by Census places.
--
-- The dasymetric interpolation assigns each block either its place's coverage percentages
-- (place blocks) or its county's leftover, non-place coverage (everything else). This
-- statement computes that leftover. For each county and each technology and speed tier it
-- subtracts the coverage carried by the county's Census places — each place fractionally
-- allocated by the share of its blocks that fall in this county — from the county total,
-- and re-expresses the remainder as a percentage of the non-place units.
--
-- Place shares come from the block-assignment crosswalk: a place that straddles several
-- counties contributes to each in proportion to its block count there. Coverage
-- percentages are unit-weighted (percent times total units gives absolute units, which
-- subtract cleanly, then divide back out). Residual percentages are clamped to [0, 1] to
-- absorb rounding, and are null where a county has no non-place units to describe.
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_telecom.fcc_coverage_county_residuals`
CLUSTER BY state_fips AS
WITH baf AS (
  SELECT county_geoid, place_geoid
  FROM `clgx-gis-app-dev-06e3.teu_demographics.census_baf_block`
  WHERE place_geoid IS NOT NULL
),
county_place_blocks AS (
  SELECT county_geoid, place_geoid, COUNT(*) AS blocks_in_county
  FROM baf
  GROUP BY county_geoid, place_geoid
),
place_total_blocks AS (
  SELECT place_geoid, COUNT(*) AS total_blocks_in_place
  FROM baf
  GROUP BY place_geoid
),
county_place_map AS (
  -- share_i = blocks of the place in this county / all blocks of the place
  SELECT
    b.county_geoid,
    b.place_geoid,
    SAFE_DIVIDE(b.blocks_in_county, t.total_blocks_in_place) AS place_share
  FROM county_place_blocks b
  JOIN place_total_blocks t USING (place_geoid)
),
places AS (
  SELECT
    geography_id AS place_geoid,
    total_units  AS place_total_units,
    copper_speed_02_02_only,
    copper_speed_10_1_only,
    copper_speed_25_3_only,
    copper_speed_100_20_only,
    copper_speed_250_25_only,
    copper_speed_1000_100_only,
    cable_speed_02_02_only,
    cable_speed_10_1_only,
    cable_speed_25_3_only,
    cable_speed_100_20_only,
    cable_speed_250_25_only,
    cable_speed_1000_100_only,
    fiber_speed_02_02_only,
    fiber_speed_10_1_only,
    fiber_speed_25_3_only,
    fiber_speed_100_20_only,
    fiber_speed_250_25_only,
    fiber_speed_1000_100_only
  FROM `clgx-gis-app-dev-06e3.teu_telecom.fcc_coverage_summary`
  WHERE geography_level = 'place'
),
counties AS (
  SELECT
    geography_id AS county_geoid,
    total_units  AS county_total_units,
    copper_speed_02_02_only,
    copper_speed_10_1_only,
    copper_speed_25_3_only,
    copper_speed_100_20_only,
    copper_speed_250_25_only,
    copper_speed_1000_100_only,
    cable_speed_02_02_only,
    cable_speed_10_1_only,
    cable_speed_25_3_only,
    cable_speed_100_20_only,
    cable_speed_250_25_only,
    cable_speed_1000_100_only,
    fiber_speed_02_02_only,
    fiber_speed_10_1_only,
    fiber_speed_25_3_only,
    fiber_speed_100_20_only,
    fiber_speed_250_25_only,
    fiber_speed_1000_100_only
  FROM `clgx-gis-app-dev-06e3.teu_telecom.fcc_coverage_summary`
  WHERE geography_level = 'county'
),
place_agg AS (
  -- Only places that exist in the FCC place summary contribute (inner join).
  SELECT
    m.county_geoid,
    COUNT(*) AS place_count,
    SUM(p.place_total_units * m.place_share) AS places_total_units,
    SUM(p.place_total_units * COALESCE(p.copper_speed_02_02_only, 0) * m.place_share) AS copper_speed_02_02_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.copper_speed_10_1_only, 0) * m.place_share) AS copper_speed_10_1_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.copper_speed_25_3_only, 0) * m.place_share) AS copper_speed_25_3_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.copper_speed_100_20_only, 0) * m.place_share) AS copper_speed_100_20_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.copper_speed_250_25_only, 0) * m.place_share) AS copper_speed_250_25_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.copper_speed_1000_100_only, 0) * m.place_share) AS copper_speed_1000_100_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.cable_speed_02_02_only, 0) * m.place_share) AS cable_speed_02_02_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.cable_speed_10_1_only, 0) * m.place_share) AS cable_speed_10_1_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.cable_speed_25_3_only, 0) * m.place_share) AS cable_speed_25_3_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.cable_speed_100_20_only, 0) * m.place_share) AS cable_speed_100_20_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.cable_speed_250_25_only, 0) * m.place_share) AS cable_speed_250_25_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.cable_speed_1000_100_only, 0) * m.place_share) AS cable_speed_1000_100_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.fiber_speed_02_02_only, 0) * m.place_share) AS fiber_speed_02_02_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.fiber_speed_10_1_only, 0) * m.place_share) AS fiber_speed_10_1_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.fiber_speed_25_3_only, 0) * m.place_share) AS fiber_speed_25_3_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.fiber_speed_100_20_only, 0) * m.place_share) AS fiber_speed_100_20_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.fiber_speed_250_25_only, 0) * m.place_share) AS fiber_speed_250_25_only_places_abs,
    SUM(p.place_total_units * COALESCE(p.fiber_speed_1000_100_only, 0) * m.place_share) AS fiber_speed_1000_100_only_places_abs
  FROM county_place_map m
  JOIN places p USING (place_geoid)
  GROUP BY m.county_geoid
),
joined AS (
  SELECT
    c.county_geoid,
    SUBSTR(c.county_geoid, 1, 2) AS state_fips,
    c.county_total_units,
    COALESCE(pa.places_total_units, 0) AS places_total_units,
    GREATEST(c.county_total_units - COALESCE(pa.places_total_units, 0), 0) AS residual_units,
    COALESCE(pa.place_count, 0) AS place_count,
    c.copper_speed_02_02_only AS copper_speed_02_02_only_county_pct,
    COALESCE(pa.copper_speed_02_02_only_places_abs, 0) AS copper_speed_02_02_only_places_abs,
    c.copper_speed_10_1_only AS copper_speed_10_1_only_county_pct,
    COALESCE(pa.copper_speed_10_1_only_places_abs, 0) AS copper_speed_10_1_only_places_abs,
    c.copper_speed_25_3_only AS copper_speed_25_3_only_county_pct,
    COALESCE(pa.copper_speed_25_3_only_places_abs, 0) AS copper_speed_25_3_only_places_abs,
    c.copper_speed_100_20_only AS copper_speed_100_20_only_county_pct,
    COALESCE(pa.copper_speed_100_20_only_places_abs, 0) AS copper_speed_100_20_only_places_abs,
    c.copper_speed_250_25_only AS copper_speed_250_25_only_county_pct,
    COALESCE(pa.copper_speed_250_25_only_places_abs, 0) AS copper_speed_250_25_only_places_abs,
    c.copper_speed_1000_100_only AS copper_speed_1000_100_only_county_pct,
    COALESCE(pa.copper_speed_1000_100_only_places_abs, 0) AS copper_speed_1000_100_only_places_abs,
    c.cable_speed_02_02_only AS cable_speed_02_02_only_county_pct,
    COALESCE(pa.cable_speed_02_02_only_places_abs, 0) AS cable_speed_02_02_only_places_abs,
    c.cable_speed_10_1_only AS cable_speed_10_1_only_county_pct,
    COALESCE(pa.cable_speed_10_1_only_places_abs, 0) AS cable_speed_10_1_only_places_abs,
    c.cable_speed_25_3_only AS cable_speed_25_3_only_county_pct,
    COALESCE(pa.cable_speed_25_3_only_places_abs, 0) AS cable_speed_25_3_only_places_abs,
    c.cable_speed_100_20_only AS cable_speed_100_20_only_county_pct,
    COALESCE(pa.cable_speed_100_20_only_places_abs, 0) AS cable_speed_100_20_only_places_abs,
    c.cable_speed_250_25_only AS cable_speed_250_25_only_county_pct,
    COALESCE(pa.cable_speed_250_25_only_places_abs, 0) AS cable_speed_250_25_only_places_abs,
    c.cable_speed_1000_100_only AS cable_speed_1000_100_only_county_pct,
    COALESCE(pa.cable_speed_1000_100_only_places_abs, 0) AS cable_speed_1000_100_only_places_abs,
    c.fiber_speed_02_02_only AS fiber_speed_02_02_only_county_pct,
    COALESCE(pa.fiber_speed_02_02_only_places_abs, 0) AS fiber_speed_02_02_only_places_abs,
    c.fiber_speed_10_1_only AS fiber_speed_10_1_only_county_pct,
    COALESCE(pa.fiber_speed_10_1_only_places_abs, 0) AS fiber_speed_10_1_only_places_abs,
    c.fiber_speed_25_3_only AS fiber_speed_25_3_only_county_pct,
    COALESCE(pa.fiber_speed_25_3_only_places_abs, 0) AS fiber_speed_25_3_only_places_abs,
    c.fiber_speed_100_20_only AS fiber_speed_100_20_only_county_pct,
    COALESCE(pa.fiber_speed_100_20_only_places_abs, 0) AS fiber_speed_100_20_only_places_abs,
    c.fiber_speed_250_25_only AS fiber_speed_250_25_only_county_pct,
    COALESCE(pa.fiber_speed_250_25_only_places_abs, 0) AS fiber_speed_250_25_only_places_abs,
    c.fiber_speed_1000_100_only AS fiber_speed_1000_100_only_county_pct,
    COALESCE(pa.fiber_speed_1000_100_only_places_abs, 0) AS fiber_speed_1000_100_only_places_abs
  FROM counties c
  LEFT JOIN place_agg pa USING (county_geoid)
)
SELECT
  county_geoid,
  state_fips,
  county_total_units,
  CAST(ROUND(places_total_units) AS INT64) AS places_total_units,
  CAST(ROUND(residual_units) AS INT64) AS residual_units,
  place_count,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(copper_speed_02_02_only_county_pct, 0) - copper_speed_02_02_only_places_abs, residual_units), 0), 1) END AS copper_speed_02_02_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(copper_speed_10_1_only_county_pct, 0) - copper_speed_10_1_only_places_abs, residual_units), 0), 1) END AS copper_speed_10_1_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(copper_speed_25_3_only_county_pct, 0) - copper_speed_25_3_only_places_abs, residual_units), 0), 1) END AS copper_speed_25_3_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(copper_speed_100_20_only_county_pct, 0) - copper_speed_100_20_only_places_abs, residual_units), 0), 1) END AS copper_speed_100_20_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(copper_speed_250_25_only_county_pct, 0) - copper_speed_250_25_only_places_abs, residual_units), 0), 1) END AS copper_speed_250_25_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(copper_speed_1000_100_only_county_pct, 0) - copper_speed_1000_100_only_places_abs, residual_units), 0), 1) END AS copper_speed_1000_100_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(cable_speed_02_02_only_county_pct, 0) - cable_speed_02_02_only_places_abs, residual_units), 0), 1) END AS cable_speed_02_02_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(cable_speed_10_1_only_county_pct, 0) - cable_speed_10_1_only_places_abs, residual_units), 0), 1) END AS cable_speed_10_1_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(cable_speed_25_3_only_county_pct, 0) - cable_speed_25_3_only_places_abs, residual_units), 0), 1) END AS cable_speed_25_3_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(cable_speed_100_20_only_county_pct, 0) - cable_speed_100_20_only_places_abs, residual_units), 0), 1) END AS cable_speed_100_20_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(cable_speed_250_25_only_county_pct, 0) - cable_speed_250_25_only_places_abs, residual_units), 0), 1) END AS cable_speed_250_25_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(cable_speed_1000_100_only_county_pct, 0) - cable_speed_1000_100_only_places_abs, residual_units), 0), 1) END AS cable_speed_1000_100_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(fiber_speed_02_02_only_county_pct, 0) - fiber_speed_02_02_only_places_abs, residual_units), 0), 1) END AS fiber_speed_02_02_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(fiber_speed_10_1_only_county_pct, 0) - fiber_speed_10_1_only_places_abs, residual_units), 0), 1) END AS fiber_speed_10_1_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(fiber_speed_25_3_only_county_pct, 0) - fiber_speed_25_3_only_places_abs, residual_units), 0), 1) END AS fiber_speed_25_3_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(fiber_speed_100_20_only_county_pct, 0) - fiber_speed_100_20_only_places_abs, residual_units), 0), 1) END AS fiber_speed_100_20_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(fiber_speed_250_25_only_county_pct, 0) - fiber_speed_250_25_only_places_abs, residual_units), 0), 1) END AS fiber_speed_250_25_only,
  CASE WHEN residual_units = 0 THEN NULL ELSE LEAST(GREATEST(SAFE_DIVIDE(county_total_units * COALESCE(fiber_speed_1000_100_only_county_pct, 0) - fiber_speed_1000_100_only_places_abs, residual_units), 0), 1) END AS fiber_speed_1000_100_only
FROM joined
