-- =============================================================================
-- Aggregate location growth to census-tract grain.
-- Module        : network_idx.grain_transfer.location_growth_ct
-- Generated from : src/network_idx/grain_transfer/location_growth_ct.py  (python -m network_idx.grain_transfer.location_growth_ct --dry-run)
-- Run in         : CONSOLE (reads PROD)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

-- Aggregate parcel-level growth signals up to census tract (loc_parcels_growth_ct).
--
-- This is the authoritative port of the create_parcel_growth_agg_ct stored procedure.
-- Parcels are mapped to a census tract by a spatial join of the parcel centroid against
-- the tract-boundary geometry (deduplicated to one tract per parcel), then two passes
-- aggregate to tract: one over the growth-counts parcel table (flag counts, unique
-- locations, and medians of the quarter-mile spatial counts) and one over the
-- hotspot-distance parcel table (mean and median distance to the nearest growth hotspot).
-- A pair of derived concentration metrics (total flags, and flags minus the single
-- largest flag) is computed from the aggregated counts. Every source table, the tract
-- boundary, and the output table are rendered from configuration.

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_features.loc_parcels_growth_ct`
CLUSTER BY tract_id AS
WITH parcel_to_ct AS (
    SELECT
        ct.geoid AS tract_id,
        p.parcel_shape_id,
        p.is_growth_parcel,
        p.is_pre_early_dev_parcel,
        p.builder_developer_ownership_indicator,
        p.landuse_change_indicator,
        p.recent_new_con_bldg_permit_indicator,
        p.new_clip_indicator,
        p.recent_parcel_split,
        p.bldr_dev_qtr_mi_cnt,
        p.landuse_change_qtr_mi_cnt,
        p.new_permit_qtr_mi_cnt,
        p.pre_early_dev_qtr_mi_cnt,
        p.parcel_centroid
    FROM `clgx-gis-app-dev-06e3.teu_features.loc_growth_cnts_parcel` p
    JOIN `clgx-idap-bigquery-prd-a990.edr_ent_common_reference_data.vw_country_boundary_sdp_us_census_tract` ct
        ON ST_INTERSECTS(p.parcel_centroid, ct.geometry)
    -- one tract per parcel when a centroid falls on a boundary between polygons
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.parcel_shape_id ORDER BY ct.geoid) = 1
),
parcel_to_ct_agg AS (
    SELECT
        tract_id,
        -- all parcels in the tract
        COUNT(*) AS total_parcels,
        -- subset of growth parcels
        COUNTIF(is_growth_parcel) AS growth_parcels,
        COUNTIF(is_pre_early_dev_parcel) AS pre_early_dev_parcels,
        COUNT(DISTINCT ST_ASTEXT(parcel_centroid)) AS unique_locations,
        -- original indicators
        COUNTIF(builder_developer_ownership_indicator = 'Y') AS builder_developer_count,
        COUNTIF(landuse_change_indicator = 'Y') AS landuse_change_count,
        COUNTIF(recent_new_con_bldg_permit_indicator = 'Y') AS building_permit_count,
        COUNTIF(new_clip_indicator = 'Y') AS new_clip_count,
        COUNTIF(recent_parcel_split = 'Y') AS parcel_split_count,
        -- median of the quarter-mile spatial counts
        APPROX_QUANTILES(bldr_dev_qtr_mi_cnt, 2)[OFFSET(1)] AS median_bldr_dev_qtr_mi_cnt,
        APPROX_QUANTILES(landuse_change_qtr_mi_cnt, 2)[OFFSET(1)] AS median_landuse_change_qtr_mi_cnt,
        APPROX_QUANTILES(new_permit_qtr_mi_cnt, 2)[OFFSET(1)] AS median_new_permit_qtr_mi_cnt,
        APPROX_QUANTILES(pre_early_dev_qtr_mi_cnt, 2)[OFFSET(1)] AS median_pre_early_dev_qtr_mi_cnt
    FROM parcel_to_ct
    GROUP BY 1
),
ct_dist_stats AS (
    SELECT
        p.tract_id,
        AVG(pd.dist_to_nearest_hotspot_m) AS mean_dist_nearest_hotspot_m,
        APPROX_QUANTILES(pd.dist_to_nearest_hotspot_m, 2)[OFFSET(1)] AS median_dist_nearest_hotspot
    FROM parcel_to_ct p
    LEFT JOIN `clgx-gis-app-dev-06e3.teu_features.loc_growth_distance_parcel` pd
        ON p.parcel_shape_id = pd.parcel_shape_id
    GROUP BY 1
)
SELECT
    pca.*,
    -- concentration metrics derived from the aggregated flag counts
    (pca.landuse_change_count + pca.builder_developer_count + pca.building_permit_count +
        GREATEST(pca.new_clip_count, pca.parcel_split_count)) AS total_flags,
    (pca.landuse_change_count + pca.builder_developer_count + pca.building_permit_count +
        GREATEST(pca.new_clip_count, pca.parcel_split_count)) -
        GREATEST(pca.landuse_change_count, pca.builder_developer_count,
                 pca.building_permit_count, pca.new_clip_count, pca.parcel_split_count)
        AS flags_minus_greatest,
    cds.mean_dist_nearest_hotspot_m,
    cds.median_dist_nearest_hotspot
FROM parcel_to_ct_agg pca
LEFT JOIN ct_dist_stats cds ON pca.tract_id = cds.tract_id;
