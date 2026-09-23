-- =============================================================================
-- Clean/optimise raw fiber-optic cable geometry (stored procedure).
-- Module        : network_idx.features.rextag.transform.fiber_optimize
-- Generated from : src/network_idx/features/rextag/transform/fiber_optimize.py  (python -m network_idx.features.rextag.transform.fiber_optimize --dry-run)
-- Run in         : CONSOLE (reads PROD view + boundary UDF)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

-- Fiber-optic cable cleanup and optimisation (deterministic transform).
--
-- Deployed as a stored procedure so the data-engineering pipeline can chain it with
-- CALL alongside the other rextag procedures. It reads the raw fiber-optic cable
-- geometry view, separates multi-line geometries into single lines, de-duplicates
-- identical paths (keeping the smallest loc_id per unique path so the id is
-- deterministic across runs), assigns each unique path a within-run integer id used
-- for the downstream spatial joins, subdivides very long lines so the downstream
-- proximity join stays efficient, and drops geometry collections. There is no
-- analytical choice here — it is a mechanical reshape — so it belongs in the
-- transform layer. The procedure name, output table, input view, spatial-UDF dataset,
-- and the subdivision vertex threshold are rendered from configuration so the same
-- logic deploys to any environment's project.
--
-- loc_id (the rextag source id) is carried through so the fiber-distance feature's
-- assemble step can map the integer spatial_fiber_id used for the spatial join back to
-- a stable, string id for the final, customer-facing nearest_fiber_id column.

CREATE OR REPLACE PROCEDURE `clgx-gis-app-dev-06e3.teu_telecom.clean_optimize_rextag_fiberopticcables`()
BEGIN

  CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_telecom.int_rextag_fiberopticcables_optimized`
  CLUSTER BY geometry AS

  -- separate multi-line geometries into single lines
  WITH mp_sp AS (
    WITH multis AS (
      SELECT loc_id, status, geometry
      FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_energy_infrastructure.vw_rextag_telecommunications_fiber_optic_cables`
      WHERE st_geometrytype(geometry) = 'ST_MultiLineString'
    )
    SELECT loc_id, status, dump AS geometry
    FROM multis, UNNEST(ST_DUMP(multis.geometry)) AS dump

    UNION ALL

    -- original single-part lines, merged back with the newly separated parts
    SELECT loc_id, status, geometry
    FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_energy_infrastructure.vw_rextag_telecommunications_fiber_optic_cables`
    WHERE loc_id NOT IN (SELECT loc_id FROM multis)
  ),

  -- dedup fiber into unique paths (excluding abandoned lines), keeping the smallest
  -- loc_id per path so the id is deterministic across runs
  raw_fiber_geoms AS (
    SELECT
      MIN(loc_id) AS loc_id,
      ST_ASGEOJSON(geometry) AS geometry
    FROM mp_sp
    WHERE status != 'Abandoned'
    GROUP BY ST_ASGEOJSON(geometry)
  ),
  unique_fiber AS (
    SELECT
      loc_id,
      ST_GEOGFROMGEOJSON(geometry) AS geometry,
      ST_NUMPOINTS(ST_GEOGFROMGEOJSON(geometry)) AS num_points
    FROM raw_fiber_geoms
  ),

  -- assign a within-run numeric id used for the spatial join
  base AS (
    SELECT
      ROW_NUMBER() OVER() AS spatial_fiber_id,
      *
    FROM unique_fiber
  ),

  -- subdivide lines longer than the vertex threshold to keep the join efficient
  subdivided AS (
    SELECT
      i.* EXCEPT(geometry),
      subdivided_geom
    FROM base i,
    UNNEST(
      CASE
        WHEN i.num_points > 16 THEN `boundary`.st_subdivide16(i.geometry)
        ELSE [i.geometry]
      END
    ) AS subdivided_geom
  ),

  layout AS (
    SELECT * EXCEPT(subdivided_geom), subdivided_geom AS geometry
    FROM subdivided
  )

  SELECT *
  FROM layout
  WHERE ST_GEOMETRYTYPE(geometry) != 'ST_GeometryCollection';

END;

CALL `clgx-gis-app-dev-06e3.teu_telecom.clean_optimize_rextag_fiberopticcables`();
