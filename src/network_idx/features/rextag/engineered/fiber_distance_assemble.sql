-- Parcel-to-fiber distance: assemble final per-parcel table (engineered feature).
--
-- Assembles the final parcel-to-fiber table from the sharded staging results. It
-- left-joins the full parcel master onto the staging table so every parcel appears
-- (parcels with no fiber within the search distance get a null distance), converts the
-- nearest-fiber distance from metres to miles to match the scoring contract, maps the
-- staging table's within-run INT64 spatial_fiber_id back to the stable, string loc_id
-- (via a distinct lookup against the optimised-fiber table) so the final, customer-
-- facing nearest_fiber_id is reproducible across optimise runs, carries the radius
-- fiber count through, and stamps each parcel's state. It is deployed as a stored
-- procedure so the data-engineering pipeline can chain it with CALL after the shards
-- finish.
--
-- The output table, the parcel master table, the staging table, the optimised-fiber
-- table, and the metres-per-mile conversion are rendered from configuration. The
-- distance is renamed with a miles suffix so the scoring feature name is unambiguous.

CREATE OR REPLACE PROCEDURE `{assemble_proc_ref}`()
BEGIN

  CREATE OR REPLACE TABLE `{distance_table}`
  CLUSTER BY state_fips AS

  -- distinct lookup from the optimised fiber table's within-run integer id back to its
  -- stable, string loc_id
  WITH fiber_lookup AS (
    SELECT DISTINCT spatial_fiber_id, loc_id
    FROM `{fiber_optimized_table}`
  )

  SELECT
    m.parcel_shape_id,
    -- use the processed state, falling back to the first two digits of the parcel's FIPS
    COALESCE(c.state_fips, SUBSTR(m.fips, 1, 2)) AS state_fips,
    -- convert the nearest-fiber distance from metres to miles (null when no fiber found)
    c.dist_to_nearest_fiber_m / {meters_per_mile} AS dist_to_nearest_fiber_miles,
    -- stable string loc_id of the nearest fiber cable (null if none within threshold)
    fl.loc_id AS nearest_fiber_id,
    -- count defaults to 0 for parcels with no matches
    COALESCE(c.radius_fiber_count, 0) AS radius_fiber_count,
    -- timestamp stays null for parcels never processed by the worker
    c.processed_at
  FROM `{parcel_table}` m
  LEFT JOIN `{calc_table}` c
    ON m.parcel_shape_id = c.parcel_shape_id
  LEFT JOIN fiber_lookup fl
    ON c.nearest_fiber_id = fl.spatial_fiber_id;

END;
