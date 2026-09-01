-- =============================================================================
-- Aggregate rextag distance to census-tract grain.
-- Module        : network_idx.grain_transfer.rextag_distance_ct
-- Generated from : src/network_idx/grain_transfer/rextag_distance_ct.py  (python -m network_idx.grain_transfer.rextag_distance_ct --dry-run)
-- Run in         : CONSOLE (reads PROD)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

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
-- NOTE: the source procedure reads `dist_to_nearest_fiber_m` and `radius_fiber_count`
-- from the rextag distance parcel table. The rearchitected rextag feature currently emits
-- the distance as `dist_to_nearest_fiber_miles`; that column name (and its unit) must be
-- reconciled with this promotion before the pipeline runs end to end.

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_features.rextag_distance_ct`
CLUSTER BY tract_id AS
WITH parcel_to_ct AS (
    SELECT
        ct.geoid AS tract_id,
        p.*
    FROM `clgx-gis-app-dev-06e3.teu_features.loc_growth_cnts_parcel` p
    JOIN `clgx-idap-bigquery-prd-a990.edr_ent_common_reference_data.vw_country_boundary_sdp_us_census_tract` ct
        ON ST_INTERSECTS(p.parcel_centroid, ct.geometry)
    -- dedup: one tract per parcel when a centroid falls on a boundary between polygons
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.parcel_shape_id ORDER BY ct.geoid) = 1
)
SELECT
    p.tract_id,
    COUNT(p.parcel_shape_id) AS total_growth_parcels,
    AVG(rd.dist_to_nearest_fiber_m) AS mean_dist_nearest_fiber_m,
    APPROX_QUANTILES(rd.dist_to_nearest_fiber_m, 2)[OFFSET(1)] AS median_dist_nearest_fiber_m,
    AVG(COALESCE(rd.radius_fiber_count, 0)) AS mean_radius_fiber_count,
    APPROX_QUANTILES(COALESCE(rd.radius_fiber_count, 0), 2)[OFFSET(1)] AS median_radius_fiber_count
FROM parcel_to_ct p
LEFT JOIN `clgx-gis-app-dev-06e3.teu_features.rextag_distance_parcel` rd
    ON p.parcel_shape_id = rd.parcel_shape_id
GROUP BY 1;
