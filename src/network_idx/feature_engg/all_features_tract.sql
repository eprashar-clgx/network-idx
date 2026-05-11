CREATE OR REPLACE TABLE `{output_table}`
CLUSTER BY tract_geoid AS
SELECT 
	a.tract_geoid,
    b.pop_ch_1yr,
    b.pop_ch_avg,
    b.pop_pctch_1yr,
    b.pop_pctch_avg,
    a.estimated_census_housing_units,
    a.estimated_fcc_units,
    a.copper_speed_less_than_100_20,
    a.copper_speed_100_20_only,
    a.copper_speed_250_25_only,
    a.copper_speed_1000_100_only,

    a.cable_speed_less_than_100_20,
    a.cable_speed_100_20_only,
    a.cable_speed_250_25_only,
    a.cable_speed_1000_100_only,
    
    a.fiber_speed_less_than_100_20,
    a.fiber_speed_100_20_only,
    a.fiber_speed_250_25_only,
    a.fiber_speed_1000_100_only,
    -- custom calculation for map
    a.fiber_speed_100_20_only + a.fiber_speed_250_25_only + a.fiber_speed_1000_100_only AS fiber_speed_equal_greater_than_100_20,
    c.cable_location_count,
    c.cable_provider_count,
    c.cable_max_download_speed,
    c.cable_max_upload_speed,
    c.copper_location_count,
    c.copper_provider_count,
    c.copper_max_download_speed,
    c.copper_max_upload_speed,
    c.fiber_location_count,
    c.fiber_provider_count,
    c.fiber_max_download_speed,
    c.fiber_max_upload_speed,
    d.* EXCEPT(tract_id),
    e.mean_dist_nearest_fiber_m,
    e.median_dist_nearest_fiber_m,
    f.geometry
FROM `{coverage_bucketed_table}` a
LEFT JOIN `{demo_pop_table}` b ON a.tract_geoid = b.tract_geoid
LEFT JOIN `{speeds_table}` c ON a.tract_geoid = c.tract_geoid
LEFT JOIN `{loc_parcels_growth_table}` d ON a.tract_geoid = d.tract_id
LEFT JOIN `{rextag_distance_table}` e ON a.tract_geoid = e.tract_id
LEFT JOIN `{ct_crosswalk_table}` x ON a.tract_geoid = x.GEOID20
LEFT JOIN `{census_tract_boundary_table}` f ON COALESCE(x.GEOID, a.tract_geoid) = f.GEOID;