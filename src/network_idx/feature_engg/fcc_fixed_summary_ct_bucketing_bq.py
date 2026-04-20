"""
BQ Feature: re-bucket MECE tract-level FCC coverage tiers into 3 speed bands.
==============================================================
Reads the SQL template in fcc_fixed_summary_ct_bucketing.sql, renders it with
the configured input/output table names, and executes it in BigQuery.

Input table:
    {GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT}

Output table:
    {GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT_BUCKETED}

Usage:
    python -m network_idx.feature_engg.fcc_fixed_summary_ct_bucketing_bq
    python -m network_idx.feature_engg.fcc_fixed_summary_ct_bucketing_bq --dry-run
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
    BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT,
    BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT_BUCKETED,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "fcc_fixed_summary_ct_bucketing.sql"


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(dry_run: bool = False) -> None:
    input_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT}"
    output_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT_BUCKETED}"

    sql_template = SQL_PATH.read_text()
    sql = sql_template.format(
        input_table=input_table,
        output_table=output_table,
    )

    logger.info(f"Input table:  {input_table}")
    logger.info(f"Output table: {output_table}")

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
        description="Re-bucket tract-level FCC coverage MECE tiers into 3 speed bands in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)