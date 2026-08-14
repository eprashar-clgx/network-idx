-- Distance from each parcel to the nearest growth hotspot, in miles.
--
-- For every parcel, finds the distance to the closest growth-hotspot cell within a
-- maximum search distance and reports it in miles. A distance of 0 means the parcel
-- sits inside a hotspot cell. The parcel table, the hotspot table, the output table,
-- the maximum search distance, and the metres-per-mile conversion are rendered from
-- configuration.

CREATE OR REPLACE TABLE `{output_table}` AS

WITH primary_proximity AS (
    -- nearest hotspot within the search distance (0 when the parcel is inside one)
    SELECT
        p.parcel_shape_id,
        MIN(ST_DISTANCE(p.parcel_centroid, h.geom)) AS dist_primary_m
    FROM `{parcel_table}` p
    JOIN `{concentrations_table}` h
        ON ST_DWITHIN(p.parcel_centroid, h.geom, {max_dist_threshold})
    GROUP BY 1
)

SELECT
    p.parcel_shape_id,
    -- convert the nearest-hotspot distance from metres to miles
    pri.dist_primary_m / {meters_per_mile} AS dist_to_nearest_hotspot_miles,
    CASE WHEN pri.dist_primary_m = 0 THEN TRUE ELSE FALSE END AS is_inside_hotspot
FROM `{parcel_table}` p
LEFT JOIN primary_proximity pri USING (parcel_shape_id);
