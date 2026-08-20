-- FCC coverage block interpolation: dasymetric assignment of coverage to Census blocks.
--
-- Every block inherits coverage percentages from one of two sources and receives an
-- estimated count of FCC units. A block inside a Census place inherits that place's
-- percentages; a block outside every place inherits its county's non-place residual
-- percentages. The percentages are inherited verbatim — Census housing units never enter
-- the percentage, they only spread each source's total FCC units across its blocks in
-- proportion to housing units. Because the percentage is constant within a place (or
-- county residual), this spread conserves the source totals exactly in FCC-unit space:
-- summing the block estimates back up reproduces the place and county figures up to
-- per-block rounding.
--
-- Blocks that end up with zero estimated units get null percentages, since a percentage of
-- no units carries no information. The output is one row per block: its geographic keys,
-- which source it drew from, its Census housing units, its estimated FCC units, and the
-- inherited per-technology, per-tier coverage percentages.
CREATE OR REPLACE TABLE `{output_table}`
CLUSTER BY state_fips, county_geoid AS
WITH state_map AS (
  SELECT * FROM UNNEST([
    {state_map_values}
  ])
),
blocks AS (
  SELECT
    baf.block_geoid,
    baf.state_fips,
    baf.county_geoid,
    baf.tract_geoid,
    baf.place_geoid,
    COALESCE(CAST(acl.total_housing_units AS INT64), 0) AS census_housing_units
  FROM `{baf_table}` baf
  LEFT JOIN `{acl_table}` acl USING (block_geoid)
),
hu AS (
  -- Housing-unit totals used to spread source units: the place total over a place's
  -- blocks, and the county total over only the county's non-place blocks.
  SELECT
    *,
    SUM(census_housing_units) OVER (PARTITION BY place_geoid) AS place_hu_total,
    SUM(IF(place_geoid IS NULL, census_housing_units, 0)) OVER (PARTITION BY county_geoid) AS county_hu_total
  FROM blocks
),
place_src AS (
  SELECT
    geography_id AS place_geoid,
    total_units  AS place_total_units,
    {place_pct_cols}
  FROM `{summary_table}`
  WHERE geography_level = 'place'
),
resid_src AS (
  SELECT
    county_geoid,
    residual_units,
    {resid_pct_cols}
  FROM `{residuals_table}`
),
assigned AS (
  SELECT
    h.block_geoid,
    h.state_fips,
    h.county_geoid,
    h.tract_geoid,
    h.place_geoid,
    h.census_housing_units,
    IF(h.place_geoid IS NOT NULL, 'place', 'county_residual') AS source,
    CASE
      WHEN h.place_geoid IS NOT NULL
        THEN SAFE_DIVIDE(h.census_housing_units, h.place_hu_total) * ps.place_total_units
      ELSE SAFE_DIVIDE(h.census_housing_units, h.county_hu_total) * rs.residual_units
    END AS estimated_fcc_units_raw,
    {inherited_pct_exprs}
  FROM hu h
  LEFT JOIN place_src ps ON h.place_geoid = ps.place_geoid
  LEFT JOIN resid_src rs ON h.county_geoid = rs.county_geoid
),
finalized AS (
  SELECT
    *,
    CAST(ROUND(COALESCE(estimated_fcc_units_raw, 0)) AS INT64) AS estimated_fcc_units
  FROM assigned
)
SELECT
  f.block_geoid,
  f.state_fips,
  sm.state_usps,
  f.county_geoid,
  f.tract_geoid,
  f.place_geoid,
  f.source,
  f.census_housing_units,
  f.estimated_fcc_units,
  {final_pct_exprs}
FROM finalized f
LEFT JOIN state_map sm USING (state_fips)
