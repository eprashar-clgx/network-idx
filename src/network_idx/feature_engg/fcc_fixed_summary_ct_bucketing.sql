CREATE OR REPLACE TABLE `{output_table}` AS
SELECT 
    tract_geoid,
    state_fips,
    state_usps,
    estimated_census_housing_units,
    estimated_fcc_units,
    ROUND(copper_speed_02_02_only + copper_speed_10_1_only + copper_speed_25_3_only,2) AS copper_speed_less_than_100_20,
    ROUND(copper_speed_100_20_only,2) AS copper_speed_100_20_only,
    ROUND(copper_speed_250_25_only,2) AS copper_speed_250_25_only,
    ROUND(copper_speed_1000_100_only,2) AS copper_speed_1000_200_only,

    ROUND(cable_speed_02_02_only + cable_speed_10_1_only + cable_speed_25_3_only,2) AS cable_speed_less_than_100_20,
    ROUND(cable_speed_100_20_only,2) AS cable_speed_100_20_only,
    ROUND(cable_speed_250_25_only,2) AS cable_speed_250_25_only,
    ROUND(cable_speed_1000_100_only,2) AS cable_speed_1000_200_only,

    ROUND(fiber_speed_02_02_only + fiber_speed_10_1_only + fiber_speed_25_3_only,2) AS fiber_speed_less_than_100_20,
    ROUND(fiber_speed_100_20_only,2) AS fiber_speed_100_20_only,
    ROUND(fiber_speed_250_25_only,2) AS fiber_speed_250_25_only,
    ROUND(fiber_speed_1000_100_only,2) AS fiber_speed_1000_200_only
FROM `{input_table}`