-- Parcel-to-fiber distance: sharded spatial worker (engineered feature).
--
-- Measures, for one state and one shard of that state's parcels, each parcel's
-- distance to the nearest optimised fiber line within the maximum search distance and
-- the number of distinct fiber lines within the radius, appending the results to the
-- staging table. It is deployed as a stored procedure so the data-engineering pipeline
-- can chain it with CALL, and it is sharded because the parcel-to-fiber spatial join is
-- too large to run in a single pass for the heaviest states. The distance and radius
-- thresholds are analytical choices, which is why this belongs in the engineered layer.
--
-- The staging table, parcel table, and optimised-fiber table are rendered from
-- configuration; the state, shard count, shard index, and the two distance thresholds
-- are runtime parameters supplied by the driver. nearest_fiber_id is cast to a string
-- because the staging column is a string; note that the fiber id is only stable within
-- a single optimise run, so it is an auxiliary/QA field rather than a scoring feature.

CREATE OR REPLACE PROCEDURE `{worker_proc_ref}`(
  current_state STRING,
  shard_count INT64,
  current_shard INT64,
  max_dist_threshold_m INT64,
  radius_threshold_m INT64
)
BEGIN

  INSERT INTO `{calc_table}`
  WITH state_parcels AS (
    SELECT
      parcel_shape_id,
      parcel_centroid,
      SUBSTR(fips, 1, 2) AS state_fips
    FROM `{parcel_table}`
    WHERE SUBSTR(fips, 1, 2) = current_state
      AND parcel_centroid IS NOT NULL
      -- when shard_count is 1 this always passes for shard 0
      AND ABS(MOD(parcel_shape_id, shard_count)) = current_shard
  ),

  spatial_matches AS (
    SELECT
      p.parcel_shape_id,
      f.original_fiber_id,
      ST_DISTANCE(p.parcel_centroid, f.geometry) AS dist
    FROM state_parcels p
    JOIN `{fiber_optimized_table}` f
      ON ST_DWITHIN(p.parcel_centroid, f.geometry, GREATEST(max_dist_threshold_m, radius_threshold_m))
  )

  SELECT
    p.parcel_shape_id,
    p.state_fips,
    MIN(CASE WHEN sm.dist <= max_dist_threshold_m THEN sm.dist ELSE NULL END) AS dist_to_nearest_fiber_m,
    CAST(
      ARRAY_AGG(
        CASE WHEN sm.dist <= max_dist_threshold_m THEN sm.original_fiber_id ELSE NULL END
        IGNORE NULLS
        ORDER BY sm.dist ASC
        LIMIT 1
      )[SAFE_OFFSET(0)] AS STRING
    ) AS nearest_fiber_id,
    COUNT(DISTINCT CASE WHEN sm.dist <= radius_threshold_m THEN sm.original_fiber_id ELSE NULL END) AS radius_fiber_count,
    CURRENT_TIMESTAMP() AS processed_at
  FROM state_parcels p
  LEFT JOIN spatial_matches sm ON p.parcel_shape_id = sm.parcel_shape_id
  GROUP BY 1, 2;

END;
