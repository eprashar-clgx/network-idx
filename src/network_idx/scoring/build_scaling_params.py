"""
Build the country-wide scaling_params for a scoring run.
==============================================================
Computes MIN / MAX / P99 caps over the parcel feature table in one BigQuery scan
and persists them (keyed by run_id) so scoring is deterministic across runs.

Input:  {GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_FEATURES}
Output: {GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_SCALING_PARAMS}

Usage:
    python -m network_idx.scoring.build_scaling_params
    python -m network_idx.scoring.build_scaling_params --dry-run
    python -m network_idx.scoring.build_scaling_params --run-id lightgbm_k8_v1
"""

import argparse
import logging

from google.cloud import bigquery

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_PARCEL_FEATURES,
    BQ_DATASET_ANALYTICS,
    BQ_TABLE_SCALING_PARAMS,
)
from network_idx.constants import SCORING_RUN_ID
from network_idx.scoring.scaling import (
    build_stats_query,
    compute_scaling_params_bq,
    write_scaling_params,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(run_id: str, dry_run: bool = False) -> None:
    source_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_FEATURES}"
    output_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_SCALING_PARAMS}"

    logger.info(f"Source: {source_table}")
    logger.info(f"Output: {output_table}")
    logger.info(f"run_id: {run_id}")

    if dry_run:
        logger.info("Dry run — stats query:")
        print(build_stats_query(source_table))
        return

    client = get_bq_client()
    params = compute_scaling_params_bq(client, source_table, run_id)
    print(params.to_string(index=False))
    write_scaling_params(client, params, output_table, run_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build scaling_params for a scoring run.")
    parser.add_argument("--run-id", default=SCORING_RUN_ID, help="Scoring run identifier.")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Print the stats query without executing.")
    args = parser.parse_args()
    run(run_id=args.run_id, dry_run=args.dry_run)