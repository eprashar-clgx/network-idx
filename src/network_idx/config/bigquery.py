"""
BigQuery dataset, table, and view identifiers for the network_idx package.

This module is the single place that names every BigQuery object the pipeline
reads or writes, grouped by the stage that owns it: the FCC speed and coverage
tables, the tract-level feature tables, the demographic and other source tables,
the analytics tables produced during exploratory analysis, and the parcel-level
scoring tables (feature spine, weights, scaling parameters, final scores, the
customer delivery table, and its quality-assurance tables). Dataset names are read
from the environment where they may differ between environments, with sensible
defaults for the common case.
"""
import os

# ── FCC speeds ────────────────────────────────────────────────────────────────
BQ_DATASET_FCC_SPEEDS = os.getenv("BQ_DATASET_FCC_SPEEDS", "teu_telecom")
BQ_TABLE_FCC_SPEEDS_BLOCK = "fcc_fixed_speeds_block"
BQ_TABLE_FCC_SPEEDS_PROVIDERS_BLOCK = "fcc_fixed_speeds_providers_block"
BQ_TABLE_FCC_SPEEDS_PROVIDERS_H3 = "fcc_fixed_speeds_providers_h3"

# ── FCC coverage ──────────────────────────────────────────────────────────────
BQ_DATASET_FCC_COVERAGE = os.getenv("BQ_DATASET_FCC_COVERAGE", "teu_telecom")
BQ_TABLE_FCC_COVERAGE_BLOCK = "fcc_coverage_block"
BQ_TABLE_FCC_COVERAGE_COUNTY_RESIDUALS = "fcc_coverage_county_residuals"

# ── Tract-level feature tables ────────────────────────────────────────────────
BQ_DATASET_FEATURES = os.getenv("BQ_DATASET_FEATURES", "teu_features")
BQ_TABLE_ALL_FEATURES_TRACT = "all_features_tract"
BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT = "fcc_fixed_coverage_ct"
BQ_TABLE_FCC_SPEEDS_FEATURES_TRACT = "fcc_fixed_speeds_ct"
BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT_BUCKETED = "fcc_fixed_coverage_ct_bucketed_speeds"

# ── Raw production sources (BigQuery-prod, authoritative; cross-project) ───────
# For the FCC and demographic families the authoritative raw data lives in the
# BigQuery production project, and the pipeline reads it from there rather than
# from local downloads. These are the physical locations the sources adapter
# reads; the sources registry maps each logical source name to one of them.
BQ_PROJECT_PROD = os.getenv("BQ_PROJECT_PROD", "clgx-idap-bigquery-prd-a990")

# FCC fixed broadband (raw) — dataset edr_ent_common_reference_ext.
BQ_PROD_DATASET_FCC = "edr_ent_common_reference_ext"
BQ_PROD_TABLE_FCC_COPPER = "fcc_copper_fixed_broadband"
BQ_PROD_TABLE_FCC_CABLE = "fcc_cable_fixed_broadband"
BQ_PROD_TABLE_FCC_FIBER = "fcc_fiber_fixed_broadband"
BQ_PROD_TABLE_FCC_GEOGRAPHY = "fcc_fixed_broadband_geography"
BQ_PROD_TABLE_FCC_SUMMARY = "fcc_fixed_broadband_summary_census"

# Demographic (raw) — NeighborhoodScout census-tract table and the tract geometry view.
BQ_PROD_DATASET_NEIGHBORHOOD = "edr_ent_property_neighborhood"
BQ_PROD_TABLE_NEIGHBORHOOD_SCOUT_CT = "neighborhood_scout_census_tract"
BQ_PROD_DATASET_REFERENCE = "edr_ent_common_reference_data"
BQ_PROD_VIEW_TRACT_GEOMETRY = "vw_country_boundary_sdp_us_census_tract"
BQ_PROD_VIEW_BLOCK_GEOMETRY = "vw_country_boundary_sdp_us_census_block"

# Location (raw) — in-house property-pipeline production views. The parcel growth
# features read parcel geometry and lineage from the property pipeline, the growth
# indicators from the enriched property views, current land use from the property
# fulfilment views, and census-block geometry from the common reference data.
BQ_PROD_DATASET_PROPERTY_PIPELINE = "edr_pmd_property_pipeline"
BQ_PROD_VIEW_PARCEL_UNIVERSE = "vw_parcel_universe"
BQ_PROD_VIEW_CLIP_TO_PARCEL = "vw_clip_to_parcel"
BQ_PROD_VIEW_PARCEL_LINEAGE_EVENT = "vw_parcel_lineage_event"
BQ_PROD_DATASET_PROPERTY_ENRICHED = "edr_ent_property_enriched"
BQ_PROD_VIEW_GROWTH_INDICATORS = "vw_edr_panoramiq_growth_indicators_v2"
BQ_PROD_DATASET_PROPERTY_FULFILLMENT = "edr_ent_property_fulfillment"
BQ_PROD_VIEW_PROPERTY = "vw_property_v1"

# Carto spatial-analytics project that hosts the H3 helper functions used by the
# location growth features (H3_FROMGEOGPOINT, H3_BOUNDARY).
BQ_PROJECT_CARTO = os.getenv("BQ_PROJECT_CARTO", "carto-os")

# ── Demographics / population ─────────────────────────────────────────────────
BQ_TABLE_DEMO_POP_TRACT = "demo_pop_ct"
BQ_SOURCE_NEIGHBORHOOD_SCOUT_CT = os.getenv(
    "BQ_SOURCE_NEIGHBORHOOD_SCOUT_CT",
    f"{BQ_PROJECT_PROD}.{BQ_PROD_DATASET_NEIGHBORHOOD}.{BQ_PROD_TABLE_NEIGHBORHOOD_SCOUT_CT}",
)

# ── Other source tables ───────────────────────────────────────────────────────
BQ_TABLE_LOC_PARCELS_GROWTH_CT = "loc_parcels_growth_ct"
BQ_TABLE_REXTAG_DISTANCE_CT = "rextag_distance_ct"
BQ_DATASET_BOUNDARY = "boundary"
BQ_TABLE_CENSUS_TRACT_BOUNDARY = "census_tract_optimized"

# Connecticut tract crosswalk (2020 GEOID → current GEOID)
GCS_CT_CROSSWALK_PATH = "gs://geospatial-projects/location_inc/spatial/us/tiger/2022/connecticut_ct_crosswalk.csv"
BQ_TABLE_CT_TRACT_CROSSWALK = "ct_tract_crosswalk_2020"

# ── Analytics output tables (exploratory analysis) ────────────────────────────
BQ_DATASET_ANALYTICS = "teu_analytics"
BQ_FEATURES_ENGG_TRACT = "all_feature_engg_tract"
BQ_FEATURES_CORRELATIONS_TRACT = "post_corr_all_features_for_clustering_tract"
BQ_CLUSTERING_TRACTS = "results_clustering_k8_tract"

# ── Parcel-level scoring pipeline ─────────────────────────────────────────────
# teu_features and teu_analytics are defined above; teu_outputs holds final scores.
BQ_DATASET_OUTPUTS = os.getenv("BQ_DATASET_OUTPUTS", "teu_outputs")

# teu_features — source parcel tables (join spine + distances)
BQ_TABLE_PARCEL_GROWTH = os.getenv("BQ_TABLE_PARCEL_GROWTH", "loc_growth_cnts_parcel")
BQ_TABLE_HOTSPOT_CONCENTRATIONS_H3 = "loc_growth_parcel_concentrations_h3r7"  # H3-r7 growth hotspots
BQ_TABLE_REXTAG_DISTANCE_PARCEL = "rextag_distance_parcel"       # dist_to_nearest_fiber_m
BQ_TABLE_HOTSPOT_DISTANCE_PARCEL = "loc_growth_distance_parcel"  # dist_to_nearest_hotspot_miles

# teu_features — feature tables
BQ_TABLE_TELECOM_FEATURES_BLOCK = "telecom_features_block"  # derived telecom features @ block
BQ_TABLE_PARCEL_FEATURES = "parcel_features"               # final joined feature list @ parcel

# teu_analytics — weights & scaling
BQ_TABLE_FEATURE_WEIGHTS = "feature_weights"   # slim: 1 row per feature per run
BQ_TABLE_SCALING_PARAMS = "scaling_params"     # frozen country-wide scaling stats per run

# teu_outputs — final scores
BQ_TABLE_PARCEL_SCORES = "parcel_scores"       # scaled features + parcel index (run_id)

# teu_outputs — customer delivery table
BQ_TABLE_FIBER_IDX_PARCEL = "fiber_idx_v1_parcel"

# teu_outputs — delivery quality-assurance tables
BQ_TABLE_FIBER_IDX_PARCEL_QA_MINMAX = "fiber_idx_v1_parcel_qa_minmax"
BQ_TABLE_FIBER_IDX_PARCEL_QA_FILLRATES = "fiber_idx_v1_parcel_qa_fillrates"
BQ_TABLE_FIBER_IDX_PARCEL_QA_INDEX_BUCKETS = "fiber_idx_v1_parcel_qa_index_buckets"
