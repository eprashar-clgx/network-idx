-- Aggregate parcel-level rextag fiber distance up to census tract (rextag_distance_ct).
--
-- This is the authoritative port of the create_fiber_agg_ct stored procedure. Parcels
-- are mapped to a census tract by a spatial join of the parcel centroid (from the
-- growth-counts parcel table, the one parcel table that carries a centroid) against the
-- tract-boundary geometry, deduplicated to one tract per parcel. The rextag distance
-- table is then left-joined on the parcel id and aggregated to tract: a count of parcels
-- and the mean and median of both the distance to the nearest fiber and the radius fiber
-- count. Every source table, the tract boundary, and the output table are rendered from
-- configuration.
--
-- The distance is read in miles (`dist_to_nearest_fiber_miles`) to match the scoring
-- contract and the parcel-grain feature; the aggregated columns carry the `_miles` suffix
-- so the unit is unambiguous end to end.

CREATE OR REPLACE TABLE `{output_table}`
CLUSTER BY tract_id AS
WITH parcel_to_ct AS (
    SELECT
        ct.geoid AS tract_id,
        p.*
    FROM `{growth_counts_parcel}` p
    JOIN `{tract_boundary}` ct
        ON ST_INTERSECTS(p.parcel_centroid, ct.geometry)
    -- dedup: one tract per parcel when a centroid falls on a boundary between polygons
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.parcel_shape_id ORDER BY ct.geoid) = 1
)
SELECT
    p.tract_id,
    COUNT(p.parcel_shape_id) AS total_growth_parcels,
    AVG(rd.dist_to_nearest_fiber_miles) AS mean_dist_nearest_fiber_miles,
    APPROX_QUANTILES(rd.dist_to_nearest_fiber_miles, 2)[OFFSET(1)] AS median_dist_nearest_fiber_miles,
    AVG(COALESCE(rd.radius_fiber_count, 0)) AS mean_radius_fiber_count,
    APPROX_QUANTILES(COALESCE(rd.radius_fiber_count, 0), 2)[OFFSET(1)] AS median_radius_fiber_count
FROM parcel_to_ct p
LEFT JOIN `{rextag_distance_parcel}` rd
    ON p.parcel_shape_id = rd.parcel_shape_id
GROUP BY 1;
