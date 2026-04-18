CREATE OR REPLACE TABLE `{output_table}` AS
SELECT 
    a.ct_key AS tract_geoid, 
    --absolute population change within 10 mile radius (based on municipal data)
    (a.pop_est_10mile - b.pop_est_10mile) AS pop_ch_1yr,
    --percentage change
    ROUND(100 * SAFE_DIVIDE((a.pop_est_10mile - b.pop_est_10mile), b.pop_est_10mile), 2) AS pop_pctch_1yr
FROM `{input_table}` a
LEFT JOIN `{input_table}` b
ON a.ct_key = b.ct_key
--start with population estimates from latest Q3 data
WHERE a.quarter = 3 AND a.year = (
  SELECT MAX(year) FROM `{input_table}`
)
--join population estimates from previous year Q3 data   
AND b.quarter = 3 AND b.year = (
  SELECT MAX(year) FROM `{input_table}`
) - 1