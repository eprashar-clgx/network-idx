"""
BQ Feature: tract-level population change from Neighborhood Scout data.
==============================================================
Reads the SQL template in demo_population_tract.sql, renders it with
the configured input/output table names, and executes it in BigQuery.

Output table:
    {GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_DEMO_POP_TRACT}

Usage:
    python -m network_idx.feature_engg.demo_population_tract_bq
    python -m network_idx.feature_engg.demo_population_tract_bq --dry-run
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
    BQ_TABLE_DEMO_POP_TRACT,
    BQ_SOURCE_NEIGHBORHOOD_SCOUT_CT,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "demo_population_tract.sql"


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(dry_run: bool = False) -> None:
    output_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_DEMO_POP_TRACT}"
    input_table = BQ_SOURCE_NEIGHBORHOOD_SCOUT_CT

    sql_template = SQL_PATH.read_text()
    sql = sql_template.format(
        output_table=output_table,
        input_table=input_table,
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
        description="Create tract-level population change table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)