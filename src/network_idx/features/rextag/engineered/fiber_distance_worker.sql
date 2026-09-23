-- Parcel-to-fiber distance: sharded spatial worker (engineered feature).
--
-- Measures, for one state and one shard of that state's parcels, each parcel's
-- distance to the nearest optimised fiber line within the maximum search distance and
-- the number of distinct fiber lines within the radius, appending the results to the
-- staging table. It is deployed as a stored procedure so the data-engineering pipeline
-- can chain it with CALL, and it is sharded because the parcel-to-fiber spatial join is
-- too large to run in a single pass for the heaviest states.
--
-- Before joining, the fiber table is pre-filtered to only the lines that fall within
-- the search distance of the target state's boundary (buffered accordingly). Without
-- this filter, every shard scans the full, nationwide fiber table regardless of how few
-- parcels the shard covers, and the join's CPU cost exceeds BigQuery's on-demand
-- CPU-to-bytes ratio limit for the denser states. The distance and radius thresholds are
-- analytical choices, which is why this belongs in the engineered layer.
--
-- The staging table, parcel table, optimised-fiber table, and state-boundary table are
-- rendered from configuration; the state, shard count, shard index, and the two distance
-- thresholds are runtime parameters supplied by the driver. nearest_fiber_id stores the
-- optimised fiber's within-run spatial_fiber_id (an INT64 surrogate); the assemble step
-- maps it back to the stable, string loc_id, because the surrogate is only stable within
-- a single optimise run.

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

  -- buffer the state boundary by the search distance so the fiber pre-filter below
  -- catches every line that could be the nearest fiber to a parcel in this state
  state_boundary AS (
    SELECT
      ST_BUFFER(
        ST_SIMPLIFY(ST_GEOGFROMTEXT(geometry), 1000),
        GREATEST(max_dist_threshold_m, radius_threshold_m) + 1000
      ) AS buffer_geom
    FROM `{state_boundary_table}`
    WHERE state_fips = current_state
  ),

  -- pre-filter the nationwide fiber table down to lines near this state, so the join
  -- below only ever compares this state's parcels against nearby fiber rather than
  -- the whole country's fiber
  state_fiber AS (
    SELECT f.spatial_fiber_id, f.geometry
    FROM `{fiber_optimized_table}` f
    JOIN state_boundary b
      ON ST_INTERSECTS(f.geometry, b.buffer_geom)
  ),

  spatial_matches AS (
    SELECT
      p.parcel_shape_id,
      f.spatial_fiber_id,
      ST_DISTANCE(p.parcel_centroid, f.geometry) AS dist
    FROM state_parcels p
    JOIN state_fiber f
      ON ST_DWITHIN(p.parcel_centroid, f.geometry, GREATEST(max_dist_threshold_m, radius_threshold_m))
  )

  SELECT
    p.parcel_shape_id,
    p.state_fips,
    MIN(CASE WHEN sm.dist <= max_dist_threshold_m THEN sm.dist ELSE NULL END) AS dist_to_nearest_fiber_m,
    ARRAY_AGG(
      CASE WHEN sm.dist <= max_dist_threshold_m THEN sm.spatial_fiber_id ELSE NULL END
      IGNORE NULLS
      ORDER BY sm.dist ASC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS nearest_fiber_id,
    COUNT(DISTINCT CASE WHEN sm.dist <= radius_threshold_m THEN sm.spatial_fiber_id ELSE NULL END) AS radius_fiber_count,
    CURRENT_TIMESTAMP() AS processed_at
  FROM state_parcels p
  LEFT JOIN spatial_matches sm ON p.parcel_shape_id = sm.parcel_shape_id
  GROUP BY 1, 2;

END;
