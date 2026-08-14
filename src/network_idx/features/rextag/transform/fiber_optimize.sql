-- Fiber-optic cable cleanup and optimisation (deterministic transform).
--
-- Deployed as a stored procedure so the data-engineering pipeline can chain it with
-- CALL alongside the other rextag procedures. It reads the raw fiber-optic cable
-- geometry view, separates multi-line geometries into single lines, de-duplicates
-- identical paths and assigns each a within-run id, subdivides very long lines so the
-- downstream proximity join stays efficient, and drops geometry collections. There is
-- no analytical choice here — it is a mechanical reshape — so it belongs in the
-- transform layer. The procedure name, output table, input view, spatial-UDF dataset,
-- and the subdivision vertex threshold are rendered from configuration so the same
-- logic deploys to any environment's project.

CREATE OR REPLACE PROCEDURE `{proc_ref}`()
BEGIN

  CREATE OR REPLACE TABLE `{output_table}`
  CLUSTER BY geometry AS

  -- separate multi-line geometries into single lines
  WITH mp_sp AS (
    WITH multis AS (
      SELECT loc_id, status, geometry
      FROM `{input_view}`
      WHERE st_geometrytype(geometry) = 'ST_MultiLineString'
    )
    SELECT loc_id, status, dump AS geometry
    FROM multis, UNNEST(ST_DUMP(multis.geometry)) AS dump

    UNION ALL

    -- original single-part lines, merged back with the newly separated parts
    SELECT loc_id, status, geometry
    FROM `{input_view}`
    WHERE loc_id NOT IN (SELECT loc_id FROM multis)
  ),

  -- dedup fiber into unique paths (excluding abandoned lines)
  raw_fiber_geoms AS (
    SELECT ST_ASGEOJSON(geometry) AS geometry
    FROM mp_sp
    WHERE status != 'Abandoned'
    GROUP BY 1
  ),
  unique_fiber AS (
    SELECT ST_GEOGFROMGEOJSON(geometry) AS geometry
    FROM raw_fiber_geoms
  ),

  -- assign a within-run numeric id and count vertices
  base AS (
    SELECT
      ROW_NUMBER() OVER() AS original_fiber_id,
      ST_NUMPOINTS(geometry) AS num_points,
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
        WHEN i.num_points > {subdivide_max_vertices} THEN `{boundary_dataset}`.st_subdivide16(i.geometry)
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
