-- Parcel-to-fiber distance: state/shard orchestration driver (engineered feature).
--
-- Orchestrates the sharded worker across a list of target states: it ensures the
-- staging table exists, then for each state deletes any prior rows (so re-runs are
-- idempotent), decides how many shards that state needs, and calls the worker once per
-- shard inside an exception handler so a single failing shard does not abort the run.
-- It is deployed as a stored procedure so the data-engineering pipeline can chain it
-- with CALL. The distance and radius defaults and the per-state shard counts are
-- rendered from configuration; the states to process are a runtime parameter so the
-- caller can launch disjoint batches in parallel.

CREATE OR REPLACE PROCEDURE `{driver_proc_ref}`(
  target_states ARRAY<STRING>,
  max_dist_threshold_m INT64,
  radius_threshold_m INT64
)
BEGIN
  DECLARE i INT64 DEFAULT 0;
  DECLARE s INT64 DEFAULT 0;
  DECLARE current_state STRING;
  DECLARE state_shard_limit INT64;

  SET max_dist_threshold_m = COALESCE(max_dist_threshold_m, {max_dist_default});
  SET radius_threshold_m = COALESCE(radius_threshold_m, {radius_default});

  -- ensure the staging table exists before the first insert
  CREATE TABLE IF NOT EXISTS `{calc_table}` (
    parcel_shape_id INT64,
    state_fips STRING,
    dist_to_nearest_fiber_m FLOAT64,
    nearest_fiber_id STRING,
    radius_fiber_count INT64,
    processed_at TIMESTAMP
  ) CLUSTER BY state_fips, parcel_shape_id;

  WHILE i < ARRAY_LENGTH(target_states) DO
    SET current_state = target_states[OFFSET(i)];

    -- clear any prior rows for this state so a re-run is idempotent
    DELETE FROM `{calc_table}` WHERE state_fips = current_state;

    -- heavier states are split into more shards to keep each pass tractable
    SET state_shard_limit = {shard_case};

    SET s = 0;
    WHILE s < state_shard_limit DO
      BEGIN
        CALL `{worker_proc_ref}`(
          current_state,
          state_shard_limit,
          s,
          max_dist_threshold_m,
          radius_threshold_m
        );
      EXCEPTION WHEN ERROR THEN
        -- surface the failing state/shard without aborting the whole run
        SELECT FORMAT("Error in state %s, shard %d: %s", current_state, s, @@error.message);
      END;
      SET s = s + 1;
    END WHILE;

    SET i = i + 1;
  END WHILE;
END;
