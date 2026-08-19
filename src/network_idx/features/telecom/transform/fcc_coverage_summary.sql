-- FCC fixed-broadband coverage summary: place and county rows with mutually exclusive
-- speed-tier percentages, one row per geography with the three technologies pivoted out.
--
-- Place rows come from the dedicated Census-place summary table; county rows come from
-- the multi-geography summary table filtered to County. Both are restricted to the
-- residential total-area rows and to the wired technologies the index uses. The raw
-- speed columns are cumulative (percent of units at >= each threshold); differencing
-- adjacent thresholds turns them into mutually exclusive tiers that sum to the
-- >= 0.2/0.2 coverage. The place identifier is stored zero-padded to seven characters so
-- it joins cleanly to the Census block-assignment place GEOID downstream.
CREATE OR REPLACE TABLE `{output_table}`
CLUSTER BY geography_level, geography_id AS
WITH place_rows AS (
  SELECT
    'place' AS geography_level,
    LPAD(CAST(geography_id AS STRING), 7, '0') AS geography_id,
    geography_desc,
    geography_desc_full,
    total_units,
    technology,
    speed_02_02, speed_10_1, speed_25_3, speed_100_20, speed_250_25, speed_1000_100
  FROM `{place_table}`
  WHERE area_data_type = 'Total'
    AND biz_res = 'R'
    AND technology IN ('Copper', 'Cable', 'Fiber')
),
county_rows AS (
  SELECT
    'county' AS geography_level,
    geography_id,
    geography_desc,
    geography_desc_full,
    total_units,
    technology,
    speed_02_02, speed_10_1, speed_25_3, speed_100_20, speed_250_25, speed_1000_100
  FROM `{geography_table}`
  WHERE area_data_type = 'Total'
    AND biz_res = 'R'
    AND geography_type = 'County'
    AND technology IN ('Copper', 'Cable', 'Fiber')
),
unioned AS (
  SELECT * FROM place_rows
  UNION ALL
  SELECT * FROM county_rows
),
tiers AS (
  SELECT
    geography_level,
    geography_id,
    geography_desc,
    geography_desc_full,
    total_units,
    LOWER(technology) AS tech,
    speed_02_02 - speed_10_1       AS speed_02_02_only,
    speed_10_1 - speed_25_3        AS speed_10_1_only,
    speed_25_3 - speed_100_20      AS speed_25_3_only,
    speed_100_20 - speed_250_25    AS speed_100_20_only,
    speed_250_25 - speed_1000_100  AS speed_250_25_only,
    speed_1000_100                 AS speed_1000_100_only
  FROM unioned
)
SELECT
  geography_level,
  geography_id,
  ANY_VALUE(geography_desc)      AS geography_desc,
  ANY_VALUE(geography_desc_full) AS geography_desc_full,
  ANY_VALUE(total_units)         AS total_units,

  MAX(IF(tech = 'copper', speed_02_02_only,   NULL)) AS copper_speed_02_02_only,
  MAX(IF(tech = 'copper', speed_10_1_only,    NULL)) AS copper_speed_10_1_only,
  MAX(IF(tech = 'copper', speed_25_3_only,    NULL)) AS copper_speed_25_3_only,
  MAX(IF(tech = 'copper', speed_100_20_only,  NULL)) AS copper_speed_100_20_only,
  MAX(IF(tech = 'copper', speed_250_25_only,  NULL)) AS copper_speed_250_25_only,
  MAX(IF(tech = 'copper', speed_1000_100_only, NULL)) AS copper_speed_1000_100_only,

  MAX(IF(tech = 'cable', speed_02_02_only,   NULL)) AS cable_speed_02_02_only,
  MAX(IF(tech = 'cable', speed_10_1_only,    NULL)) AS cable_speed_10_1_only,
  MAX(IF(tech = 'cable', speed_25_3_only,    NULL)) AS cable_speed_25_3_only,
  MAX(IF(tech = 'cable', speed_100_20_only,  NULL)) AS cable_speed_100_20_only,
  MAX(IF(tech = 'cable', speed_250_25_only,  NULL)) AS cable_speed_250_25_only,
  MAX(IF(tech = 'cable', speed_1000_100_only, NULL)) AS cable_speed_1000_100_only,

  MAX(IF(tech = 'fiber', speed_02_02_only,   NULL)) AS fiber_speed_02_02_only,
  MAX(IF(tech = 'fiber', speed_10_1_only,    NULL)) AS fiber_speed_10_1_only,
  MAX(IF(tech = 'fiber', speed_25_3_only,    NULL)) AS fiber_speed_25_3_only,
  MAX(IF(tech = 'fiber', speed_100_20_only,  NULL)) AS fiber_speed_100_20_only,
  MAX(IF(tech = 'fiber', speed_250_25_only,  NULL)) AS fiber_speed_250_25_only,
  MAX(IF(tech = 'fiber', speed_1000_100_only, NULL)) AS fiber_speed_1000_100_only
FROM tiers
GROUP BY geography_level, geography_id
