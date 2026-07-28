"""
BQ Feature: consolidated tract-level model/scoring feature table.
================================================================
Reads the raw tract feature table (all_features_tract), derives the telecom
features, then applies the canonical rename + NA-fill/winsorize rule layer
(single source of truth = constants + apply_feature_fills). Writes the table
consumed by correlation -> clustering -> classification and by the weight/
scaling builds.

Source: {GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_ALL_FEATURES_TRACT}
Output: {GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_FEATURES_ENGG_TRACT}

Usage:
    python -m network_idx.feature_engg.all_features_engg_tract_bq
    python -m network_idx.feature_engg.all_features_engg_tract_bq --dry-run
"""

import argparse
import logging

import pandas as pd
from google.cloud import bigquery

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_ALL_FEATURES_TRACT,
    BQ_DATASET_ANALYTICS,
    BQ_FEATURES_ENGG_TRACT,
)
from network_idx.constants import (
    MODEL_TO_SCORING_FEATURE,
    ALL_SCORING_FEATURES,
    PROVIDER_LANDSCAPE_ORDER,
)
from network_idx.scoring.scaling import apply_feature_fills
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ── Column-drop lists (faithful to 04_01_eda_feature_engg_telecom corr_df) ─────
# Verbose raw columns dropped up front (notebook cols_to_drop). Distance names
# adapted to the new *_miles schema; errors="ignore" tolerates already-EXCEPT'd cols.
_COLS_TO_DROP = [
    "copper_speed_less_than_100_20", "copper_speed_100_20_only", "copper_speed_250_25_only",
    "cable_speed_less_than_100_20", "cable_speed_100_20_only", "cable_speed_250_25_only",
    "fiber_speed_less_than_100_20", "fiber_speed_100_20_only", "fiber_speed_250_25_only",
    "fiber_speed_equal_greater_than_100_20",
    "total_parcels", "growth_parcels", "unique_locations", "total_flags",
    "flags_minus_greatest", "new_clip_count", "parcel_split_count",
    "mean_dist_nearest_hotspot_m",       # already EXCEPT'd in all_features_tract.sql
    "mean_dist_nearest_fiber_miles",     # was mean_dist_nearest_fiber_m in the notebook
]

# Redundant / correlated columns dropped after derivation (notebook cols_to_drop_for_corr).
_COLS_TO_DROP_FOR_CORR = [
    "copper_provider_count", "cable_provider_count", "fiber_provider_count",
    "fiber_provider_density_per_1000_hhs", "log1p_fiber_provider_density_per_1000_hhs",
    "copper_location_count", "cable_location_count", "fiber_location_count", "fiber_penetration",
    "copper_max_download_speed", "cable_max_upload_speed", "fiber_max_upload_speed",
    "has_fiber", "copper_speed_1000_100_only", "cable_speed_100_to_sub_gig",
    "cable_speed_1000_100_only", "fiber_speed_100_to_sub_gig", "fiber_speed_1000_100_only",
    "fiber_opportunity_score",
]

_BINARY_P90_COLS = [
    "pre_early_dev_parcels", "landuse_change_count",
    "builder_developer_count", "building_permit_count",
]

HOUSING_UNIT_THRESHOLD = 50  # water-body / tiny-tract exclusion (matches telecom notebook)


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def _provider_competitive_landscape(row) -> str:
    """Categorical competitive-landscape label from provider counts.
    Lifted verbatim from 04_01_eda_feature_engg_telecom. Provider counts are
    fillna(0)-ed before this runs, so NaN rows resolve to 'no_providers'."""
    if row["copper_provider_count"] == 0 and row["cable_provider_count"] == 0 and row["fiber_provider_count"] == 0:
        return "no_providers"
    elif row["copper_provider_count"] > 0 and row["cable_provider_count"] == 0 and row["fiber_provider_count"] == 0:
        return "greenfield"
    elif row["cable_provider_count"] > 0 and row["fiber_provider_count"] == 0:
        return "cable_but_no_fiber"
    elif row["fiber_provider_count"] == 1:
        return "fiber_entry"
    elif row["fiber_provider_count"] == 2:
        return "fiber_duopoly"
    elif row["fiber_provider_count"] == 3:
        return "fiber_competitive"
    elif row["fiber_provider_count"] > 3:
        return "fiber_saturated"
    else:
        return "Other"


def add_telecom_features(df: pd.DataFrame) -> pd.DataFrame:
    """Derive telecom model features on RAW tract columns (pre-rename), exactly as
    justified in 04_01_eda_feature_engg_telecom. Penetration / opportunity ratios are
    clipped to [0, 1] (negatives and >100% coverage are not meaningful); raw ratios may
    still emit inf/NaN, which apply_feature_fills cleans downstream."""
    out = df.copy()
    out["cable_penetration"] = (
        out["cable_location_count"] / out["estimated_census_housing_units"]
    ).clip(0, 1)
    out["fiber_opportunity_gap"] = (
        (out["estimated_census_housing_units"] - out["fiber_location_count"])
        / out["estimated_census_housing_units"]
    ).clip(0, 1)
    has_fiber = ((out["fiber_location_count"] > 0) & (out["fiber_provider_count"] > 0)).astype(int)
    out["fiber_speed_top_tier"] = out["fiber_speed_1000_100_only"] * has_fiber
    out["provider_competitive_landscape"] = out.apply(_provider_competitive_landscape, axis=1)
    out["provider_competitive_landscape_ord"] = out["provider_competitive_landscape"].map(PROVIDER_LANDSCAPE_ORDER)
    return out


def build(df: pd.DataFrame) -> pd.DataFrame:
    """Raw all_features_tract -> canonical, rule-filled feature frame.

    Faithful transcription of 04_01_eda_feature_engg_telecom's corr_df pipeline
    (same derivations, same two column-drops, same row filters, same ordering),
    followed by the single canonical rename + rule layer. Penetration / opportunity
    ratios are clipped to [0, 1] (v2 refinement); apply_feature_fills cleans the rest.
    """
    out = df.copy()

    # 1. Binary growth indicators at P90 — computed on the UNFILTERED frame (notebook order).
    for col in _BINARY_P90_COLS:
        threshold = out[col].quantile(0.90)
        out[f"{col}_bin"] = (out[col] >= threshold).astype(int)

    # 2. Drop verbose raw columns (notebook cols_to_drop).
    out = out.drop(columns=_COLS_TO_DROP, errors="ignore")

    # 3. Row filters -> training population (~83,313 tracts).
    out = out.dropna(subset=["pop_ch_1yr", "pop_pctch_1yr"])
    out = out[out["estimated_census_housing_units"] >= HOUSING_UNIT_THRESHOLD]

    # 4. Provider counts: null -> 0 (null == no providers), BEFORE the landscape function.
    for tech in ["copper", "cable", "fiber"]:
        out[f"{tech}_provider_count"] = out[f"{tech}_provider_count"].fillna(0).astype(int)

    # 5. Fiber provider density (raw + capped-at-3 attribute).
    out["fiber_provider_density_per_1000_hhs"] = (
        out["fiber_provider_count"] / out["estimated_census_housing_units"] * 1000
    )
    out["fiber_provider_density_per_1000_hhs_capped"] = (
        out["fiber_provider_density_per_1000_hhs"].clip(upper=3)
    )

    # 6. Provider-present indicators + competitive landscape (string + ordinal).
    out["copper_provider_present"] = (out["copper_provider_count"] > 0).astype(int)
    out["cable_provider_present"] = (out["cable_provider_count"] > 0).astype(int)
    out["provider_competitive_landscape"] = out.apply(_provider_competitive_landscape, axis=1)
    out["provider_competitive_landscape_ord"] = (
        out["provider_competitive_landscape"].map(PROVIDER_LANDSCAPE_ORDER)
    )

    # 7. Penetration / opportunity gap / interaction (ratios clipped to [0, 1] — v2 refinement).
    out["cable_penetration"] = (
        out["cable_location_count"] / out["estimated_census_housing_units"]
    ).clip(0, 1)
    out["fiber_opportunity_gap"] = (
        (out["estimated_census_housing_units"] - out["fiber_location_count"])
        / out["estimated_census_housing_units"]
    ).clip(0, 1)
    out["fiber_cable_interaction"] = out["fiber_opportunity_gap"] * out["cable_penetration"]

    # 8. has_fiber -> fiber_speed_top_tier.
    has_fiber = ((out["fiber_location_count"] > 0) & (out["fiber_provider_count"] > 0)).astype(int)
    out["fiber_speed_top_tier"] = out["fiber_speed_1000_100_only"] * has_fiber

    # 9. cable_future_gap -> fiber_opportunity_score.
    out["cable_future_gap"] = 1 - out["cable_speed_1000_100_only"]
    out["fiber_opportunity_score"] = (
        out["fiber_opportunity_gap"] * out["cable_penetration"] * (1 + out["cable_future_gap"])
    )

    # 10. Drop redundant / correlated columns (notebook cols_to_drop_for_corr).
    out = out.drop(columns=_COLS_TO_DROP_FOR_CORR, errors="ignore")

    # 11. Canonical rename + ONE rule layer (constants-driven single source of truth).
    out = out.rename(columns=MODEL_TO_SCORING_FEATURE)     # median_*/distances/housing -> canonical
    out = apply_feature_fills(out, ALL_SCORING_FEATURES)   # NA-fill / winsorize
    return out


def run(dry_run: bool = False) -> None:
    client = get_bq_client()
    source_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_ALL_FEATURES_TRACT}"
    output_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_FEATURES_ENGG_TRACT}"

    logger.info(f"Reading {source_table} ...")
    df = client.query(f"SELECT * EXCEPT(geometry) FROM `{source_table}`").to_arrow().to_pandas()
    logger.info(f"Read {len(df):,} rows, {df.shape[1]} columns.")

    df = build(df)
    logger.info(f"Built feature frame: {len(df):,} rows, {df.shape[1]} columns.")

    if dry_run:
        logger.info("Dry run — not writing. Scoring-feature summary:")
        print(df[ALL_SCORING_FEATURES].describe().round(3).to_string())
        return

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, output_table, job_config=job_config)
    job.result()
    logger.info(f"Done. Wrote {job.output_rows:,} rows to {output_table}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build the consolidated tract-level feature-engg table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Build the frame and print a summary without writing to BigQuery.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)