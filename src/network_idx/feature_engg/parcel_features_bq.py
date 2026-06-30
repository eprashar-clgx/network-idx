"""
BQ Feature: assemble the 13 scoring features at parcel grain.
==============================================================
Joins the parcel growth/distance tables (parcel), telecom_features_block (block),
and demo_pop_ct (tract) into one wide parcel-grain table used by the scorer.

Output:
    {GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_FEATURES}

Usage:
    python -m network_idx.feature_engg.parcel_features_bq
    python -m network_idx.feature_engg.parcel_features_bq --dry-run
"""

import argparse
import logging
from pathlib import Path

from google.cloud import bigquery

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_PARCEL_GROWTH,
    BQ_TABLE_REXTAG_DISTANCE_PARCEL,
    BQ_TABLE_HOTSPOT_DISTANCE_PARCEL,
    BQ_TABLE_TELECOM_FEATURES_BLOCK,
    BQ_TABLE_DEMO_POP_TRACT,
    BQ_TABLE_PARCEL_FEATURES,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "parcel_features.sql"


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(dry_run: bool = False) -> None:
    ds = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}"
    sql = SQL_PATH.read_text().format(
        output_table=f"{ds}.{BQ_TABLE_PARCEL_FEATURES}",
        parcel_growth_table=f"{ds}.{BQ_TABLE_PARCEL_GROWTH}",
        rextag_distance_table=f"{ds}.{BQ_TABLE_REXTAG_DISTANCE_PARCEL}",
        hotspot_distance_table=f"{ds}.{BQ_TABLE_HOTSPOT_DISTANCE_PARCEL}",
        telecom_block_table=f"{ds}.{BQ_TABLE_TELECOM_FEATURES_BLOCK}",
        demo_tract_table=f"{ds}.{BQ_TABLE_DEMO_POP_TRACT}",
    )

    logger.info(f"Output: {ds}.{BQ_TABLE_PARCEL_FEATURES}")
    if dry_run:
        logger.info("Dry run — rendered SQL:")
        print(sql)
        return

    client = get_bq_client()
    logger.info("Executing query...")
    client.query(sql).result()
    logger.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Assemble parcel-grain scoring features in BigQuery.")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Print the rendered SQL without executing it.")
    args = parser.parse_args()
    run(dry_run=args.dry_run)