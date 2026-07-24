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
    python -m network_idx.feature_engg.all_feature_engg_tract_bq
    python -m network_idx.feature_engg.all_feature_engg_tract_bq --dry-run
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


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def _provider_competitive_landscape(row) -> str:
    """Categorical competitive-landscape label from provider counts.
    Lifted verbatim from 04_01_eda_feature_engg_telecom. Rows with NaN provider
    counts fall through to 'Other' (-> NaN ord -> filled 0.0 by apply_feature_fills)."""
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
    """raw all_features_tract -> canonical, rule-filled feature frame."""
    df = add_telecom_features(df)                        # derive telecom features (raw names)
    df = df.rename(columns=MODEL_TO_SCORING_FEATURE)     # median_* / distances / housing -> canonical
    df = apply_feature_fills(df, ALL_SCORING_FEATURES)   # ONE rule layer, constants-driven
    return df


def run(dry_run: bool = False) -> None:
    client = get_bq_client()
    source_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_ALL_FEATURES_TRACT}"
    output_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_FEATURES_ENGG_TRACT}"

    logger.info(f"Reading {source_table} ...")
    df = client.query(f"SELECT * FROM `{source_table}`").to_arrow().to_pandas()
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