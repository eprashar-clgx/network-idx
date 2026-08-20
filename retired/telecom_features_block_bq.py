"""
BQ Feature: derive block-level telecom features for parcel scoring.
==============================================================
Joins the FCC speeds block table and the FCC coverage block table on block_geoid
and computes the four scored telecom features (cable_penetration,
fiber_opportunity_gap, fiber_speed_top_tier, provider_competitive_landscape[_ord]).

Input tables:
    {GCS_PROJECT_ID}.{BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_BLOCK}
    {GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK}

Output table:
    {GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_TELECOM_FEATURES_BLOCK}

Usage:
    python -m network_idx.feature_engg.telecom_features_block_bq
    python -m network_idx.feature_engg.telecom_features_block_bq --dry-run
"""

import argparse
import logging
from pathlib import Path

from google.cloud import bigquery

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FCC_SPEEDS,
    BQ_TABLE_FCC_SPEEDS_BLOCK,
    BQ_DATASET_FCC_COVERAGE,
    BQ_TABLE_FCC_COVERAGE_BLOCK,
    BQ_DATASET_FEATURES,
    BQ_TABLE_TELECOM_FEATURES_BLOCK,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "telecom_features_block.sql"


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(dry_run: bool = False) -> None:
    speeds_block_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_BLOCK}"
    coverage_block_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK}"
    output_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_TELECOM_FEATURES_BLOCK}"

    sql = SQL_PATH.read_text().format(
        speeds_block_table=speeds_block_table,
        coverage_block_table=coverage_block_table,
        output_table=output_table,
    )

    logger.info(f"Speeds block:   {speeds_block_table}")
    logger.info(f"Coverage block: {coverage_block_table}")
    logger.info(f"Output table:   {output_table}")

    if dry_run:
        logger.info("Dry run — rendered SQL:")
        print(sql)
        return

    client = get_bq_client()
    logger.info("Executing query...")
    job = client.query(sql)
    job.result()
    logger.info(f"Done. Table {output_table} created/replaced.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Derive block-level telecom features in BigQuery.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)