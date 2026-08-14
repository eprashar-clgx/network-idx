-- Growth-hotspot concentrations at the H3 resolution-7 grain.
--
-- Aggregates the parcel-level growth signals into H3 cells and keeps only the cells
-- that qualify as growth "hotspots" by volume (total flags) and diversity (variety)
-- of signals. The resulting cells are the reference geography the hotspot-distance
-- feature measures each parcel against. The input table, output table, Carto H3
-- project, H3 resolution, and both thresholds are rendered from configuration.

CREATE OR REPLACE TABLE `{output_table}`
CLUSTER BY h3_id AS

WITH h3_growth_summary AS (
    SELECT
        `{carto_project}`.carto.H3_FROMGEOGPOINT(parcel_centroid, {hotspot_h3_res}) AS h3_id,
        COUNT(*) AS growth_parcels,
        -- some parcel locations may be stacked; count unique locations too
        COUNT(DISTINCT ST_ASTEXT(parcel_centroid)) AS unique_locations,
        SUM(CASE WHEN landuse_change_indicator = "Y" THEN 1 ELSE 0 END) AS landuse_change_count,
        SUM(CASE WHEN builder_developer_ownership_indicator = "Y" THEN 1 ELSE 0 END) AS builder_developer_count,
        SUM(CASE WHEN recent_new_con_bldg_permit_indicator = "Y" THEN 1 ELSE 0 END) AS building_permit_count,
        SUM(CASE WHEN new_clip_indicator = "Y" THEN 1 ELSE 0 END) AS new_clip_count,
        -- include both new-clip and split-parcel counts
        SUM(CASE WHEN recent_parcel_split = "Y" THEN 1 ELSE 0 END) AS parcel_split_count
    FROM `{input_table}`
    WHERE is_growth_parcel IS TRUE
    GROUP BY 1
),

h3_growth_calc AS (
    SELECT
        *,
        -- total growth flags in the cell (taking the greater of clip/split)
        landuse_change_count + builder_developer_count + building_permit_count +
        GREATEST(new_clip_count, parcel_split_count) AS total_flags,

        -- variety: total flags minus the single largest signal category
        (landuse_change_count + builder_developer_count + building_permit_count + GREATEST(new_clip_count, parcel_split_count)) -
        GREATEST(landuse_change_count, builder_developer_count, building_permit_count, new_clip_count, parcel_split_count) AS flags_minus_greatest,

        `{carto_project}`.carto.H3_BOUNDARY(h3_id) AS geom
    FROM h3_growth_summary
)

-- keep only cells that clear both the volume and variety thresholds
SELECT *
FROM h3_growth_calc
WHERE total_flags >= {total_flags_threshold}
  AND flags_minus_greatest >= {variety_threshold};
