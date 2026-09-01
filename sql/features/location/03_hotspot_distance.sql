-- =============================================================================
-- Distance-to-growth-hotspot features.
-- Module        : network_idx.features.location.engineered.hotspot_distance
-- Generated from : src/network_idx/features/location/engineered/hotspot_distance.py  (python -m network_idx.features.location.engineered.hotspot_distance --dry-run)
-- Run in         : VM (dev-only, BQ-validated)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

-- Distance from each parcel to the nearest growth hotspot, in miles.
--
-- For every parcel, finds the distance to the closest growth-hotspot cell within a
-- maximum search distance and reports it in miles. A distance of 0 means the parcel
-- sits inside a hotspot cell. The parcel table, the hotspot table, the output table,
-- the maximum search distance, and the metres-per-mile conversion are rendered from
-- configuration.

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_features.loc_growth_distance_parcel` AS

WITH primary_proximity AS (
    -- nearest hotspot within the search distance (0 when the parcel is inside one)
    SELECT
        p.parcel_shape_id,
        MIN(ST_DISTANCE(p.parcel_centroid, h.geom)) AS dist_primary_m
    FROM `clgx-gis-app-dev-06e3.teu_features.loc_growth_cnts_parcel` p
    JOIN `clgx-gis-app-dev-06e3.teu_features.loc_growth_parcel_concentrations_h3r7` h
        ON ST_DWITHIN(p.parcel_centroid, h.geom, 24140)
    GROUP BY 1
)

SELECT
    p.parcel_shape_id,
    -- convert the nearest-hotspot distance from metres to miles
    pri.dist_primary_m / 1609.34 AS dist_to_nearest_hotspot_miles,
    CASE WHEN pri.dist_primary_m = 0 THEN TRUE ELSE FALSE END AS is_inside_hotspot
FROM `clgx-gis-app-dev-06e3.teu_features.loc_growth_cnts_parcel` p
LEFT JOIN primary_proximity pri USING (parcel_shape_id);
