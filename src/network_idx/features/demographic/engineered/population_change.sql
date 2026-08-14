CREATE OR REPLACE TABLE `{output_table}` AS

WITH ns_q3 AS (
  SELECT 
    ct_key, 
    year, 
    pop_est_10mile
  FROM `{input_table}`
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