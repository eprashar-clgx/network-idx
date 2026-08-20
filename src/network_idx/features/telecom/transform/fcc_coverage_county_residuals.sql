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
CREATE OR REPLACE TABLE `{output_table}`
CLUSTER BY state_fips AS
WITH baf AS (
  SELECT county_geoid, place_geoid
  FROM `{baf_table}`
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
    {place_pct_cols}
  FROM `{summary_table}`
  WHERE geography_level = 'place'
),
counties AS (
  SELECT
    geography_id AS county_geoid,
    total_units  AS county_total_units,
    {county_pct_cols}
  FROM `{summary_table}`
  WHERE geography_level = 'county'
),
place_agg AS (
  -- Only places that exist in the FCC place summary contribute (inner join).
  SELECT
    m.county_geoid,
    COUNT(*) AS place_count,
    SUM(p.place_total_units * m.place_share) AS places_total_units,
    {place_abs_exprs}
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
    {joined_metric_cols}
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
  {residual_case_exprs}
FROM joined
