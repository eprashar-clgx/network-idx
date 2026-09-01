-- =============================================================================
-- Sharded parcel distance-to-nearest-fiber (3 stored procedures).
-- Module        : network_idx.features.rextag.engineered.fiber_distance
-- Generated from : src/network_idx/features/rextag/engineered/fiber_distance.py  (python -m network_idx.features.rextag.engineered.fiber_distance --dry-run)
-- Run in         : VM / DEV-only (reads dev tables loc_growth_cnts_parcel + int_rextag_fiberopticcables_optimized; no direct PROD read)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

-- Parcel-to-fiber distance: staging table definition (engineered feature).
--
-- Creates the sharded worker's staging table up front, before the worker procedure is
-- deployed, so the worker's INSERT body validates at CREATE-PROCEDURE time even on a
-- fresh environment where the table does not yet exist. It is idempotent (IF NOT
-- EXISTS) so re-running the pipeline is safe; the driver clears rows per state on each
-- run. nearest_fiber_id is a string because the fiber id is only stable within a single
-- optimise run and is an auxiliary/QA field rather than a scoring feature.

CREATE TABLE IF NOT EXISTS `clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel` (
  parcel_shape_id INT64,
  state_fips STRING,
  dist_to_nearest_fiber_m FLOAT64,
  nearest_fiber_id STRING,
  radius_fiber_count INT64,
  processed_at TIMESTAMP
) CLUSTER BY state_fips, parcel_shape_id;

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

CREATE OR REPLACE PROCEDURE `clgx-gis-app-dev-06e3.teu_features.rextag_run_spatial_shard_worker`(
  current_state STRING,
  shard_count INT64,
  current_shard INT64,
  max_dist_threshold_m INT64,
  radius_threshold_m INT64
)
BEGIN

  INSERT INTO `clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel`
  WITH state_parcels AS (
    SELECT
      parcel_shape_id,
      parcel_centroid,
      SUBSTR(fips, 1, 2) AS state_fips
    FROM `clgx-gis-app-dev-06e3.teu_features.loc_growth_cnts_parcel`
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
    JOIN `clgx-gis-app-dev-06e3.teu_telecom.int_rextag_fiberopticcables_optimized` f
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

CREATE OR REPLACE PROCEDURE `clgx-gis-app-dev-06e3.teu_features.rextag_calculate_parcel_dist_to_fiber`(
  target_states ARRAY<STRING>,
  max_dist_threshold_m INT64,
  radius_threshold_m INT64
)
BEGIN
  DECLARE i INT64 DEFAULT 0;
  DECLARE s INT64 DEFAULT 0;
  DECLARE current_state STRING;
  DECLARE state_shard_limit INT64;

  SET max_dist_threshold_m = COALESCE(max_dist_threshold_m, 24140);
  SET radius_threshold_m = COALESCE(radius_threshold_m, 4828);

  -- ensure the staging table exists before the first insert
  CREATE TABLE IF NOT EXISTS `clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel` (
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
    DELETE FROM `clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel` WHERE state_fips = current_state;

    -- heavier states are split into more shards to keep each pass tractable
    SET state_shard_limit = CASE
      WHEN current_state = '48' THEN 4
      WHEN current_state = '34' THEN 2
      WHEN current_state = '36' THEN 2
      ELSE 1
    END;

    SET s = 0;
    WHILE s < state_shard_limit DO
      BEGIN
        CALL `clgx-gis-app-dev-06e3.teu_features.rextag_run_spatial_shard_worker`(
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

-- Parcel-to-fiber distance: assemble final per-parcel table (engineered feature).
--
-- Assembles the final parcel-to-fiber table from the sharded staging results. It
-- left-joins the full parcel master onto the staging table so every parcel appears
-- (parcels with no fiber within the search distance get a null distance), converts the
-- nearest-fiber distance from metres to miles to match the scoring contract, carries
-- the fiber id and the radius fiber count through, and stamps each parcel's state. It
-- is deployed as a stored procedure so the data-engineering pipeline can chain it with
-- CALL after the shards finish.
--
-- The output table, the parcel master table, the staging table, and the metres-per-mile
-- conversion are rendered from configuration. The distance is renamed with a miles
-- suffix so the scoring feature name is unambiguous.

CREATE OR REPLACE PROCEDURE `clgx-gis-app-dev-06e3.teu_features.rextag_distance_assemble_parcel`()
BEGIN

  CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_features.rextag_distance_parcel`
  CLUSTER BY state_fips AS

  SELECT
    m.parcel_shape_id,
    -- use the processed state, falling back to the first two digits of the parcel's FIPS
    COALESCE(c.state_fips, SUBSTR(m.fips, 1, 2)) AS state_fips,
    -- convert the nearest-fiber distance from metres to miles (null when no fiber found)
    c.dist_to_nearest_fiber_m / 1609.34 AS dist_to_nearest_fiber_miles,
    -- line id of the nearest fiber cable (null if none within threshold)
    c.nearest_fiber_id,
    -- count defaults to 0 for parcels with no matches
    COALESCE(c.radius_fiber_count, 0) AS radius_fiber_count,
    -- timestamp stays null for parcels never processed by the worker
    c.processed_at
  FROM `clgx-gis-app-dev-06e3.teu_features.loc_growth_cnts_parcel` m
  LEFT JOIN `clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel` c
    ON m.parcel_shape_id = c.parcel_shape_id;

END;

CALL `clgx-gis-app-dev-06e3.teu_features.rextag_calculate_parcel_dist_to_fiber`(['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56'], 24140, 4828);
CALL `clgx-gis-app-dev-06e3.teu_features.rextag_distance_assemble_parcel`();
