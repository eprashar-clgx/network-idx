BEGIN

/*
  This procedure flags parcels with a variety of growth signals, 
  appends H3 & Census Block spatial indices, and calculates counts 
  of flagged parcels within a 1/4 mile radius (402.336 meters) of each parcel. 

  Tables and views used external to this project:
  -------------------------------------
  - clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.parcel_universe
  - clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.vw_clip_to_parcel
  - clgx-idap-bigquery-prd-a990.edr_ent_property_enriched.vw_edr_panoramiq_growth_indicators_v2
  - clgx-idap-bigquery-prd-a990.edr_ent_property_fulfillment.vw_property_v1
  - clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.vw_parcel_lineage_event
  - clgx-idap-bigquery-prd-a990.edr_ent_common_reference_data.vw_country_boundary_sdp_us_census_block

*/

  -- Set radius for spatial count calculations
  DECLARE radius_m FLOAT64 DEFAULT 402.336;

  CREATE OR REPLACE TABLE `teu_features.loc_growth_cnts_parcel` 
  CLUSTER BY fips, parcel_polygon, parcel_centroid AS

  -- Step 1: base data gathering and metric assignment

  -- get parcel shape id and lat/lon from parcel universe view with pre-filtered active status parcels only
  WITH parcels AS (
      SELECT parcel_shape_id, parcel_latitude, parcel_longitude, fips, parcel_polygon
      FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.vw_parcel_universe`
  ),
  --get clip to parcel relationships
  parcel_clip AS (
      SELECT parcel_shape_id, clip  
      FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.vw_clip_to_parcel`
      WHERE status = 'A' AND clip IS NOT NULL
  ),
  --get growth v2 indicators
  growth_v2 AS( 
      SELECT DISTINCT puid, growth_stage, landuse_change_indicator, previous_land_use_code, new_clip_indicator,
             recent_new_con_bldg_permit_indicator, builder_developer_ownership_indicator
      FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_enriched.vw_edr_panoramiq_growth_indicators_v2`
  ),
  --get current land use and year built from property v1
  property AS (
      SELECT puid, irislandusecd, COALESCE(yybltactdt, yyblteffdt) AS year_built  
      FROM `clgx-idap-bigquery-prd-a990.edr_ent_property_fulfillment.vw_property_v1`
  ),
  --refine growth indicators and include year built for all properties
  growth_rev AS (
      SELECT 
          p.puid, p.year_built,
          g.growth_stage, g.builder_developer_ownership_indicator, g.recent_new_con_bldg_permit_indicator,
          g.new_clip_indicator,
          -- land use change only from vacant or agricutural to residential or commerical
          CASE WHEN (g.previous_land_use_code LIKE '4%' OR g.previous_land_use_code LIKE '5%') AND
              (p.irislandusecd LIKE '1%' OR p.irislandusecd LIKE '2%')
              THEN g.landuse_change_indicator END AS landuse_change_indicator    
      FROM property p
      LEFT JOIN growth_v2 g ON p.puid = g.puid
  ),
  --aggregate growth indicators and year built to parcels
  parcel_growth AS (
      SELECT 
          parcel_shape_id, 
          MAX(year_built) AS year_built,
          CASE WHEN countif(new_clip_indicator = 'Y')>0 THEN 'Y' END AS new_clip_indicator,
          CASE WHEN countif(recent_new_con_bldg_permit_indicator = 'Y')>0 THEN 'Y' END AS recent_new_con_bldg_permit_indicator,
          CASE WHEN countif(landuse_change_indicator = 'Y')>0 THEN 'Y' END AS landuse_change_indicator,            
          CASE WHEN countif(builder_developer_ownership_indicator = 'Y')>0 THEN 'Y' END AS builder_developer_ownership_indicator,
          CASE WHEN countif(growth_stage = 'Recently Completed')>0 THEN 'Recently Completed'
               WHEN countif(growth_stage = 'Ongoing Growth')>0 THEN 'Ongoing Growth'
               WHEN countif(growth_stage = 'Early Growth')>0 THEN 'Early Growth'
               END AS growth_stage    
      FROM parcel_clip
      LEFT JOIN growth_rev ON clip = CAST(puid AS STRING)
      GROUP BY parcel_shape_id
  ),
  --get recent parcel splits
  splits AS (
      SELECT
          CHILD_PARCEL_SHAPE_ID AS parcel_shape_id,
          CASE WHEN countif(DATE_DIFF(CURRENT_DATE(), EVENT_RECORD_DATE, DAY) <= 365*2) > 0 THEN 'Y' END AS recent_parcel_split
      FROM `clgx-idap-bigquery-prd-a990.edr_pmd_property_pipeline.vw_parcel_lineage_event`
      --get splits from the last two years
      WHERE PARCEL_CHANGE_EVENT_TYPE IN ('SP')
      GROUP BY CHILD_PARCEL_SHAPE_ID
  ),
  prep_finish AS (
      SELECT 
          a.parcel_shape_id,
          a.fips,
          b.landuse_change_indicator,
          b.new_clip_indicator, 
          b.builder_developer_ownership_indicator,
          b.recent_new_con_bldg_permit_indicator,
          c.recent_parcel_split,
          -- any signal present and not "recently completed" and no year built
          CASE WHEN (b.landuse_change_indicator = 'Y' 
                     OR b.new_clip_indicator = 'Y' 
                     OR b.builder_developer_ownership_indicator = 'Y'
                     OR b.recent_new_con_bldg_permit_indicator = 'Y'
                     OR c.recent_parcel_split = 'Y')
                AND (b.growth_stage IS NULL OR b.growth_stage <> 'Recently Completed')
                AND (b.year_built IS NULL)
               THEN TRUE ELSE FALSE END AS is_growth_parcel,

          -- consolidated early dev flag
          CASE WHEN (b.new_clip_indicator = 'Y' OR c.recent_parcel_split = 'Y') 
               THEN TRUE ELSE FALSE END AS is_pre_early_dev_parcel,

          ST_GEOGPOINT(a.parcel_longitude, a.parcel_latitude) AS parcel_centroid,
          a.parcel_polygon
      FROM parcels a
      LEFT JOIN parcel_growth b ON a.parcel_shape_id = b.parcel_shape_id
      LEFT JOIN splits c ON a.parcel_shape_id = c.parcel_shape_id
  ),

  -- Spatial join and append Census Block ID (BQ optimized spatial outer join)
  prep_with_block_0 AS (
    SELECT 
        aa.parcel_shape_id, 
        bb.geoid AS block_id
    FROM 
    prep_finish aa
    LEFT OUTER JOIN
    (
      SELECT
        a.parcel_shape_id,
        b.geoid
      FROM prep_finish a
      JOIN `clgx-idap-bigquery-prd-a990.edr_ent_common_reference_data.vw_country_boundary_sdp_us_census_block` b
      ON
        ST_INTERSECTS(a.parcel_centroid, b.geometry)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY a.parcel_shape_id ORDER BY b.geoid) = 1
    ) bb
    ON aa.parcel_shape_id = bb.parcel_shape_id
  ),
  -- join append Census Block ID back to rest of "prep finish" parcel attributes
  prep_with_block AS (
    SELECT
        pf.*,
        pwba.block_id
    FROM prep_finish pf
    LEFT JOIN prep_with_block_0 pwba
    ON pf.parcel_shape_id = pwba.parcel_shape_id
  ),

  -- Step 2: Spatial Calculation (filter first optimization)
  
  -- 2a. isolate only parcels that meet the strict active growth definitions
  flagged_targets AS (
      SELECT 
          parcel_shape_id,
          parcel_centroid,
          builder_developer_ownership_indicator,
          landuse_change_indicator,
          recent_new_con_bldg_permit_indicator,
          is_pre_early_dev_parcel,
          is_growth_parcel 
      FROM prep_with_block
      WHERE parcel_centroid IS NOT NULL
        AND is_growth_parcel = TRUE 
  ),

  -- 2b. apply search radius variable using inner join + st_dwithin()
  spatial_matches AS (
      SELECT 
          p.parcel_shape_id,
          t.builder_developer_ownership_indicator,
          t.landuse_change_indicator,
          t.recent_new_con_bldg_permit_indicator,
          t.is_pre_early_dev_parcel,
          t.is_growth_parcel 
      FROM prep_with_block p
      INNER JOIN flagged_targets t 
          ON ST_DWITHIN(p.parcel_centroid, t.parcel_centroid, radius_m)
  ),

  -- 2c. aggregate the matches
  aggregated_counts AS (
      SELECT 
          parcel_shape_id,
          COUNTIF(builder_developer_ownership_indicator = 'Y') AS bldr_dev_qtr_mi_cnt,
          COUNTIF(landuse_change_indicator = 'Y') AS landuse_change_qtr_mi_cnt,
          COUNTIF(recent_new_con_bldg_permit_indicator = 'Y') AS new_permit_qtr_mi_cnt,
          COUNTIF(is_pre_early_dev_parcel = TRUE) AS pre_early_dev_qtr_mi_cnt,
          COUNTIF(is_growth_parcel = TRUE) AS growth_parcel_qtr_mi_cnt 
      FROM spatial_matches
      GROUP BY 1
  )

  -- Step 3: final layout (indicator flags + spatial counts + spatial indexes)

  SELECT 
      pf.parcel_shape_id,
      `carto-os`.carto.H3_FROMGEOGPOINT(pf.parcel_centroid, 8) AS h3_res8, -- Spatially appended H3 Resolution 8 index
      pf.block_id, -- Appended Census Block ID
      pf.fips,
      pf.landuse_change_indicator,
      pf.new_clip_indicator,
      pf.builder_developer_ownership_indicator,
      pf.recent_new_con_bldg_permit_indicator,
      pf.recent_parcel_split,
      pf.is_growth_parcel,
      pf.is_pre_early_dev_parcel,
      COALESCE(ac.bldr_dev_qtr_mi_cnt, 0) AS bldr_dev_qtr_mi_cnt,
      COALESCE(ac.landuse_change_qtr_mi_cnt, 0) AS landuse_change_qtr_mi_cnt,
      COALESCE(ac.new_permit_qtr_mi_cnt, 0) AS new_permit_qtr_mi_cnt,
      COALESCE(ac.pre_early_dev_qtr_mi_cnt, 0) AS pre_early_dev_qtr_mi_cnt,
      COALESCE(ac.growth_parcel_qtr_mi_cnt, 0) AS growth_parcel_qtr_mi_cnt, 
      pf.parcel_polygon,
      pf.parcel_centroid

  FROM prep_with_block pf
  LEFT JOIN aggregated_counts ac ON pf.parcel_shape_id = ac.parcel_shape_id;

END