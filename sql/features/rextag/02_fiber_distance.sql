-- =============================================================================
-- Parcel-to-fiber distance (sharded spatial worker + driver + assemble).
-- Module        : network_idx.features.rextag.engineered.fiber_distance
-- Generated from : src/network_idx/features/rextag/engineered/fiber_distance.py  (python -m network_idx.features.rextag.engineered.fiber_distance --dry-run)
-- Run in         : CONSOLE (worker reads a PROD state-boundary view to pre-filter fiber; no longer VM-runnable)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
--
-- NOTE (F7 fix): the worker now pre-filters the optimised-fiber table to a buffer
-- around the target state's boundary before its spatial join, and shard counts are
-- retuned per state, so a state's join no longer scans the entire nationwide fiber
-- table and exceed BigQuery's on-demand CPU-to-bytes ratio limit. The driver also
-- raises if any shard failed, and the trailing ASSERT independently verifies every
-- expected state has at least one processed row before this table is considered done.
-- =============================================================================

-- Parcel-to-fiber distance: staging table definition (engineered feature).
--
-- Creates the sharded worker's staging table up front, before the worker procedure is
-- deployed, so the worker's INSERT body validates at CREATE-PROCEDURE time even on a
-- fresh environment where the table does not yet exist. It is idempotent (IF NOT
-- EXISTS) so re-running the pipeline is safe; the driver clears rows per state on each
-- run. nearest_fiber_id stores the optimised fiber's within-run INT64 spatial_fiber_id,
-- not the stable string loc_id, because that surrogate is only stable within a single
-- optimise run; the assemble step maps it back to loc_id for the final table.

CREATE TABLE IF NOT EXISTS `clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel` (
  parcel_shape_id INT64,
  state_fips STRING,
  dist_to_nearest_fiber_m FLOAT64,
  nearest_fiber_id INT64,
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

  -- buffer the state boundary by the search distance so the fiber pre-filter below
  -- catches every line that could be the nearest fiber to a parcel in this state
  state_boundary AS (
    SELECT
      ST_BUFFER(
        ST_SIMPLIFY(ST_GEOGFROMTEXT(geometry), 1000),
        GREATEST(max_dist_threshold_m, radius_threshold_m) + 1000
      ) AS buffer_geom
    FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_geospatial_admin_boundaries.vw_geospatial_admin_boundaries_state`
    WHERE state_fips = current_state
  ),

  -- pre-filter the nationwide fiber table down to lines near this state, so the join
  -- below only ever compares this state's parcels against nearby fiber rather than
  -- the whole country's fiber
  state_fiber AS (
    SELECT f.spatial_fiber_id, f.geometry
    FROM `clgx-gis-app-dev-06e3.teu_telecom.int_rextag_fiberopticcables_optimized` f
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

-- Parcel-to-fiber distance: state/shard orchestration driver (engineered feature).
--
-- Orchestrates the sharded worker across a list of target states: it ensures the
-- staging table exists, then for each state deletes any prior rows (so re-runs are
-- idempotent), decides how many shards that state needs, and calls the worker once per
-- shard. A failing shard is recorded rather than aborting the run, so every requested
-- state and shard is still attempted; but if any shard failed, the whole procedure
-- raises at the end (naming every failing state/shard) instead of completing as if
-- nothing went wrong, so a partial run can never look like a clean success. It is
-- deployed as a stored procedure so the data-engineering pipeline can chain it with
-- CALL. The distance and radius defaults and the per-state shard counts are rendered
-- from configuration; the states to process are a runtime parameter so the caller can
-- launch disjoint batches in parallel.

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
  DECLARE failures ARRAY<STRING> DEFAULT [];

  SET max_dist_threshold_m = COALESCE(max_dist_threshold_m, 24140);
  SET radius_threshold_m = COALESCE(radius_threshold_m, 4828);

  -- ensure the staging table exists before the first insert
  CREATE TABLE IF NOT EXISTS `clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel` (
    parcel_shape_id INT64,
    state_fips STRING,
    dist_to_nearest_fiber_m FLOAT64,
    nearest_fiber_id INT64,
    radius_fiber_count INT64,
    processed_at TIMESTAMP
  ) CLUSTER BY state_fips, parcel_shape_id;

  WHILE i < ARRAY_LENGTH(target_states) DO
    SET current_state = target_states[OFFSET(i)];

    -- clear any prior rows for this state so a re-run is idempotent
    DELETE FROM `clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel` WHERE state_fips = current_state;

    -- heavier states are split into more shards to keep each pass tractable
    SET state_shard_limit = CASE
      WHEN current_state = '06' THEN 32
      WHEN current_state = '12' THEN 32
      WHEN current_state = '13' THEN 32
      WHEN current_state = '17' THEN 32
      WHEN current_state = '18' THEN 32
      WHEN current_state = '25' THEN 32
      WHEN current_state = '34' THEN 32
      WHEN current_state = '36' THEN 32
      WHEN current_state = '37' THEN 32
      WHEN current_state = '39' THEN 32
      WHEN current_state = '42' THEN 32
      WHEN current_state = '45' THEN 32
      WHEN current_state = '48' THEN 32
      WHEN current_state = '51' THEN 24
      WHEN current_state = '29' THEN 16
      WHEN current_state = '01' THEN 12
      WHEN current_state = '04' THEN 12
      WHEN current_state = '21' THEN 12
      WHEN current_state = '55' THEN 12
      WHEN current_state = '08' THEN 8
      WHEN current_state = '19' THEN 8
      WHEN current_state = '24' THEN 8
      WHEN current_state = '26' THEN 8
      WHEN current_state = '27' THEN 8
      WHEN current_state = '49' THEN 8
      WHEN current_state = '05' THEN 6
      WHEN current_state = '09' THEN 6
      WHEN current_state = '22' THEN 6
      WHEN current_state = '23' THEN 6
      WHEN current_state = '31' THEN 6
      WHEN current_state = '47' THEN 6
      WHEN current_state = '53' THEN 6
      WHEN current_state = '16' THEN 4
      WHEN current_state = '40' THEN 4
      WHEN current_state = '41' THEN 4
      WHEN current_state = '44' THEN 4
      WHEN current_state = '46' THEN 4
      WHEN current_state = '02' THEN 2
      WHEN current_state = '10' THEN 2
      WHEN current_state = '11' THEN 2
      WHEN current_state = '20' THEN 2
      WHEN current_state = '28' THEN 2
      WHEN current_state = '30' THEN 2
      WHEN current_state = '32' THEN 2
      WHEN current_state = '33' THEN 2
      WHEN current_state = '35' THEN 2
      WHEN current_state = '38' THEN 2
      WHEN current_state = '50' THEN 2
      WHEN current_state = '54' THEN 2
      WHEN current_state = '56' THEN 2
      ELSE 2
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
        -- record the failing state/shard and keep going, so one bad shard does not
        -- stop every other state/shard from being attempted
        SET failures = ARRAY_CONCAT(failures, [FORMAT("state %s shard %d: %s", current_state, s, @@error.message)]);
      END;
      SET s = s + 1;
    END WHILE;

    SET i = i + 1;
  END WHILE;

  -- surface every failure as a hard error, so a partial run cannot complete silently
  IF ARRAY_LENGTH(failures) > 0 THEN
    RAISE USING MESSAGE = FORMAT(
      "fiber_distance driver: %d shard(s) failed and were skipped: %s",
      ARRAY_LENGTH(failures),
      ARRAY_TO_STRING(failures, " | ")
    );
  END IF;
END;

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

CREATE OR REPLACE PROCEDURE `clgx-gis-app-dev-06e3.teu_features.rextag_distance_assemble_parcel`()
BEGIN

  CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_features.rextag_distance_parcel`
  CLUSTER BY state_fips AS

  -- distinct lookup from the optimised fiber table's within-run integer id back to its
  -- stable, string loc_id
  WITH fiber_lookup AS (
    SELECT DISTINCT spatial_fiber_id, loc_id
    FROM `clgx-gis-app-dev-06e3.teu_telecom.int_rextag_fiberopticcables_optimized`
  )

  SELECT
    m.parcel_shape_id,
    -- use the processed state, falling back to the first two digits of the parcel's FIPS
    COALESCE(c.state_fips, SUBSTR(m.fips, 1, 2)) AS state_fips,
    -- convert the nearest-fiber distance from metres to miles (null when no fiber found)
    c.dist_to_nearest_fiber_m / 1609.34 AS dist_to_nearest_fiber_miles,
    -- stable string loc_id of the nearest fiber cable (null if none within threshold)
    fl.loc_id AS nearest_fiber_id,
    -- count defaults to 0 for parcels with no matches
    COALESCE(c.radius_fiber_count, 0) AS radius_fiber_count,
    -- timestamp stays null for parcels never processed by the worker
    c.processed_at
  FROM `clgx-gis-app-dev-06e3.teu_features.loc_growth_cnts_parcel` m
  LEFT JOIN `clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel` c
    ON m.parcel_shape_id = c.parcel_shape_id
  LEFT JOIN fiber_lookup fl
    ON c.nearest_fiber_id = fl.spatial_fiber_id;

END;

CALL `clgx-gis-app-dev-06e3.teu_features.rextag_calculate_parcel_dist_to_fiber`(['01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56'], 24140, 4828);
CALL `clgx-gis-app-dev-06e3.teu_features.rextag_distance_assemble_parcel`();
ASSERT (
  (SELECT COUNT(DISTINCT state_fips)
   FROM `clgx-gis-app-dev-06e3.teu_features.rextag_distance_parcel`
   WHERE processed_at IS NOT NULL AND state_fips IN ('01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56'))
  = 51
) AS 'fiber_distance completeness check failed: not every expected state has worker-processed rows (some state_fips have processed_at IS NULL) — a partial run was detected. Re-run the driver over the missing states, then re-run assemble, before using this table downstream.';
