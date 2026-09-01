-- Parcel-to-fiber distance: staging table definition (engineered feature).
--
-- Creates the sharded worker's staging table up front, before the worker procedure is
-- deployed, so the worker's INSERT body validates at CREATE-PROCEDURE time even on a
-- fresh environment where the table does not yet exist. It is idempotent (IF NOT
-- EXISTS) so re-running the pipeline is safe; the driver clears rows per state on each
-- run. nearest_fiber_id is a string because the fiber id is only stable within a single
-- optimise run and is an auxiliary/QA field rather than a scoring feature.

CREATE TABLE IF NOT EXISTS `{calc_table}` (
  parcel_shape_id INT64,
  state_fips STRING,
  dist_to_nearest_fiber_m FLOAT64,
  nearest_fiber_id STRING,
  radius_fiber_count INT64,
  processed_at TIMESTAMP
) CLUSTER BY state_fips, parcel_shape_id;
