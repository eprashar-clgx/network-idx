-- =============================================================================
-- Block-level FCC coverage joined to census BAF/ACL.
-- Module        : network_idx.features.telecom.transform.fcc_coverage_block
-- Generated from : src/network_idx/features/telecom/transform/fcc_coverage_block.py  (python -m network_idx.features.telecom.transform.fcc_coverage_block --dry-run)
-- Run in         : VM once census blocks landed
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

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
CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_telecom.fcc_coverage_block`
CLUSTER BY state_fips, county_geoid AS
WITH state_map AS (
  SELECT * FROM UNNEST([
    STRUCT('01' AS state_fips, 'AL' AS state_usps),
    STRUCT('02' AS state_fips, 'AK' AS state_usps),
    STRUCT('04' AS state_fips, 'AZ' AS state_usps),
    STRUCT('05' AS state_fips, 'AR' AS state_usps),
    STRUCT('06' AS state_fips, 'CA' AS state_usps),
    STRUCT('08' AS state_fips, 'CO' AS state_usps),
    STRUCT('09' AS state_fips, 'CT' AS state_usps),
    STRUCT('10' AS state_fips, 'DE' AS state_usps),
    STRUCT('11' AS state_fips, 'DC' AS state_usps),
    STRUCT('12' AS state_fips, 'FL' AS state_usps),
    STRUCT('13' AS state_fips, 'GA' AS state_usps),
    STRUCT('15' AS state_fips, 'HI' AS state_usps),
    STRUCT('16' AS state_fips, 'ID' AS state_usps),
    STRUCT('17' AS state_fips, 'IL' AS state_usps),
    STRUCT('18' AS state_fips, 'IN' AS state_usps),
    STRUCT('19' AS state_fips, 'IA' AS state_usps),
    STRUCT('20' AS state_fips, 'KS' AS state_usps),
    STRUCT('21' AS state_fips, 'KY' AS state_usps),
    STRUCT('22' AS state_fips, 'LA' AS state_usps),
    STRUCT('23' AS state_fips, 'ME' AS state_usps),
    STRUCT('24' AS state_fips, 'MD' AS state_usps),
    STRUCT('25' AS state_fips, 'MA' AS state_usps),
    STRUCT('26' AS state_fips, 'MI' AS state_usps),
    STRUCT('27' AS state_fips, 'MN' AS state_usps),
    STRUCT('28' AS state_fips, 'MS' AS state_usps),
    STRUCT('29' AS state_fips, 'MO' AS state_usps),
    STRUCT('30' AS state_fips, 'MT' AS state_usps),
    STRUCT('31' AS state_fips, 'NE' AS state_usps),
    STRUCT('32' AS state_fips, 'NV' AS state_usps),
    STRUCT('33' AS state_fips, 'NH' AS state_usps),
    STRUCT('34' AS state_fips, 'NJ' AS state_usps),
    STRUCT('35' AS state_fips, 'NM' AS state_usps),
    STRUCT('36' AS state_fips, 'NY' AS state_usps),
    STRUCT('37' AS state_fips, 'NC' AS state_usps),
    STRUCT('38' AS state_fips, 'ND' AS state_usps),
    STRUCT('39' AS state_fips, 'OH' AS state_usps),
    STRUCT('40' AS state_fips, 'OK' AS state_usps),
    STRUCT('41' AS state_fips, 'OR' AS state_usps),
    STRUCT('42' AS state_fips, 'PA' AS state_usps),
    STRUCT('44' AS state_fips, 'RI' AS state_usps),
    STRUCT('45' AS state_fips, 'SC' AS state_usps),
    STRUCT('46' AS state_fips, 'SD' AS state_usps),
    STRUCT('47' AS state_fips, 'TN' AS state_usps),
    STRUCT('48' AS state_fips, 'TX' AS state_usps),
    STRUCT('49' AS state_fips, 'UT' AS state_usps),
    STRUCT('50' AS state_fips, 'VT' AS state_usps),
    STRUCT('51' AS state_fips, 'VA' AS state_usps),
    STRUCT('53' AS state_fips, 'WA' AS state_usps),
    STRUCT('54' AS state_fips, 'WV' AS state_usps),
    STRUCT('55' AS state_fips, 'WI' AS state_usps),
    STRUCT('56' AS state_fips, 'WY' AS state_usps),
    STRUCT('60' AS state_fips, 'AS' AS state_usps),
    STRUCT('66' AS state_fips, 'GU' AS state_usps),
    STRUCT('69' AS state_fips, 'MP' AS state_usps),
    STRUCT('72' AS state_fips, 'PR' AS state_usps),
    STRUCT('78' AS state_fips, 'VI' AS state_usps)
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
  FROM `clgx-gis-app-dev-06e3.teu_demographics.census_baf_block` baf
  LEFT JOIN `clgx-gis-app-dev-06e3.teu_demographics.census_acl_block` acl USING (block_geoid)
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
resid_src AS (
  SELECT
    county_geoid,
    residual_units,
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
  FROM `clgx-gis-app-dev-06e3.teu_telecom.fcc_coverage_county_residuals`
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
    IF(h.place_geoid IS NOT NULL, ps.copper_speed_02_02_only, rs.copper_speed_02_02_only) AS copper_speed_02_02_only,
    IF(h.place_geoid IS NOT NULL, ps.copper_speed_10_1_only, rs.copper_speed_10_1_only) AS copper_speed_10_1_only,
    IF(h.place_geoid IS NOT NULL, ps.copper_speed_25_3_only, rs.copper_speed_25_3_only) AS copper_speed_25_3_only,
    IF(h.place_geoid IS NOT NULL, ps.copper_speed_100_20_only, rs.copper_speed_100_20_only) AS copper_speed_100_20_only,
    IF(h.place_geoid IS NOT NULL, ps.copper_speed_250_25_only, rs.copper_speed_250_25_only) AS copper_speed_250_25_only,
    IF(h.place_geoid IS NOT NULL, ps.copper_speed_1000_100_only, rs.copper_speed_1000_100_only) AS copper_speed_1000_100_only,
    IF(h.place_geoid IS NOT NULL, ps.cable_speed_02_02_only, rs.cable_speed_02_02_only) AS cable_speed_02_02_only,
    IF(h.place_geoid IS NOT NULL, ps.cable_speed_10_1_only, rs.cable_speed_10_1_only) AS cable_speed_10_1_only,
    IF(h.place_geoid IS NOT NULL, ps.cable_speed_25_3_only, rs.cable_speed_25_3_only) AS cable_speed_25_3_only,
    IF(h.place_geoid IS NOT NULL, ps.cable_speed_100_20_only, rs.cable_speed_100_20_only) AS cable_speed_100_20_only,
    IF(h.place_geoid IS NOT NULL, ps.cable_speed_250_25_only, rs.cable_speed_250_25_only) AS cable_speed_250_25_only,
    IF(h.place_geoid IS NOT NULL, ps.cable_speed_1000_100_only, rs.cable_speed_1000_100_only) AS cable_speed_1000_100_only,
    IF(h.place_geoid IS NOT NULL, ps.fiber_speed_02_02_only, rs.fiber_speed_02_02_only) AS fiber_speed_02_02_only,
    IF(h.place_geoid IS NOT NULL, ps.fiber_speed_10_1_only, rs.fiber_speed_10_1_only) AS fiber_speed_10_1_only,
    IF(h.place_geoid IS NOT NULL, ps.fiber_speed_25_3_only, rs.fiber_speed_25_3_only) AS fiber_speed_25_3_only,
    IF(h.place_geoid IS NOT NULL, ps.fiber_speed_100_20_only, rs.fiber_speed_100_20_only) AS fiber_speed_100_20_only,
    IF(h.place_geoid IS NOT NULL, ps.fiber_speed_250_25_only, rs.fiber_speed_250_25_only) AS fiber_speed_250_25_only,
    IF(h.place_geoid IS NOT NULL, ps.fiber_speed_1000_100_only, rs.fiber_speed_1000_100_only) AS fiber_speed_1000_100_only
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
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.copper_speed_02_02_only, 0)) AS copper_speed_02_02_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.copper_speed_10_1_only, 0)) AS copper_speed_10_1_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.copper_speed_25_3_only, 0)) AS copper_speed_25_3_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.copper_speed_100_20_only, 0)) AS copper_speed_100_20_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.copper_speed_250_25_only, 0)) AS copper_speed_250_25_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.copper_speed_1000_100_only, 0)) AS copper_speed_1000_100_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.cable_speed_02_02_only, 0)) AS cable_speed_02_02_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.cable_speed_10_1_only, 0)) AS cable_speed_10_1_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.cable_speed_25_3_only, 0)) AS cable_speed_25_3_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.cable_speed_100_20_only, 0)) AS cable_speed_100_20_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.cable_speed_250_25_only, 0)) AS cable_speed_250_25_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.cable_speed_1000_100_only, 0)) AS cable_speed_1000_100_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.fiber_speed_02_02_only, 0)) AS fiber_speed_02_02_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.fiber_speed_10_1_only, 0)) AS fiber_speed_10_1_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.fiber_speed_25_3_only, 0)) AS fiber_speed_25_3_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.fiber_speed_100_20_only, 0)) AS fiber_speed_100_20_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.fiber_speed_250_25_only, 0)) AS fiber_speed_250_25_only,
  IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.fiber_speed_1000_100_only, 0)) AS fiber_speed_1000_100_only
FROM finalized f
LEFT JOIN state_map sm USING (state_fips)
