"""
BQ Feature: tract-level feature creation using SQL in BigQuery.
==============================================================
Reads the SQL template in all_features_tract.sql, renders it with
the configured input/output table names, and executes it in BigQuery.

Output table:
    {GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_ALL_FEATURES_TRACT}

Usage:
    python -m network_idx.feature_engg.all_features_tract_bq
    python -m network_idx.feature_engg.all_features_tract_bq --dry-run
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
    BQ_DATASET_BOUNDARY,
    BQ_TABLE_ALL_FEATURES_TRACT,
    BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT_BUCKETED,
    BQ_TABLE_FCC_SPEEDS_FEATURES_TRACT, 
    BQ_TABLE_DEMO_POP_TRACT,
    BQ_TABLE_LOC_PARCELS_GROWTH_CT,
    BQ_TABLE_REXTAG_DISTANCE_CT,
    BQ_TABLE_CENSUS_TRACT_BOUNDARY
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "all_features_tract.sql"


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(dry_run: bool = False) -> None:
    """
    Executes the SQL to create the tract-level feature table in BigQuery.
    """
    sql_template = SQL_PATH.read_text()
    sql = sql_template.format(
    output_table=f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_ALL_FEATURES_TRACT}",
    coverage_bucketed_table=f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_COVERAGE_FEATURES_TRACT_BUCKETED}",
    demo_pop_table=f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_DEMO_POP_TRACT}",
    speeds_table=f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_FCC_SPEEDS_FEATURES_TRACT}",
    loc_parcels_growth_table=f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.loc_parcels_growth_ct",
    rextag_distance_table=f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.rextag_distance_ct",
    census_tract_boundary_table=f"{GCS_PROJECT_ID}.boundary.census_tract_optimized"
    )
    logger.info("SQL template read and rendered.")
    if dry_run:
        logger.info("Dry run — rendered SQL:")
        print(sql)
        return

    client = get_bq_client()
    logger.info("Executing query...")
    job = client.query(sql)
    job.result()
    logger.info(f"Done. Table {BQ_TABLE_ALL_FEATURES_TRACT} created/replaced.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create all census tract level features table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    run(dry_run=args.dry_run)