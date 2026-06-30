"""
Build the feature_weights table for a scoring run from saved SHAP artifacts.
==============================================================
Loads the k=8 classifier SHAP joblibs, derives the three weight views, validates
bucket weights against the locked v1 shares, and persists them (keyed by run_id).

Default artifacts (v1 = lightgbm_k8_v1):
    notebooks/data/shap_values_**.joblib   (samples, features, classes)
    notebooks/data/X_shap_**.joblib        (DataFrame; columns = model feature names)
    use --strict or --no-strict flags while running the script to check weights 
    are in line with values stored in SCORING_BUCKET_WEIGHTS in config file

Output: {GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_FEATURE_WEIGHTS}

Usage:
    python -m network_idx.scoring.build_weights
    python -m network_idx.scoring.build_weights --dry-run
    python -m network_idx.scoring.build_weights --run-id lightgbm_k8_v1
    python -m network_idx.scoring.build_weights \
        --shap-values notebooks/data/shap_values_k8.joblib \
        --x-shap notebooks/data/X_shap_k8.joblib
"""

import argparse
import logging
from pathlib import Path

import joblib
from google.cloud import bigquery

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_ANALYTICS,
    BQ_TABLE_FEATURE_WEIGHTS,
)
from network_idx.constants import SCORING_RUN_ID
from network_idx.scoring.weights import (
    compute_feature_weights,
    write_feature_weights,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_SHAP_VALUES = Path("notebooks/data/shap_values_k8_lightgbm_v1.joblib")
DEFAULT_X_SHAP = Path("notebooks/data/X_shap_k8_lightgbm_v1.joblib")


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(
    run_id: str,
    shap_values_path: Path,
    x_shap_path: Path,
    bucket_tol: float,
    strict: bool,
    dry_run: bool = False,
) -> None:
    output_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_FEATURE_WEIGHTS}"
    logger.info(f"SHAP values: {shap_values_path}")
    logger.info(f"X_shap:      {x_shap_path}")
    logger.info(f"Output:      {output_table}")
    logger.info(f"run_id:      {run_id}")

    for p in (shap_values_path, x_shap_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing SHAP artifact: {p}")

    shap_values = joblib.load(shap_values_path)
    x_shap = joblib.load(x_shap_path)

    weights = compute_feature_weights(
        shap_values, x_shap, run_id, bucket_tol=bucket_tol, strict=strict
    )
    print(weights.to_string(index=False))

    if dry_run:
        logger.info("Dry run — not writing to BigQuery.")
        return

    client = get_bq_client()
    write_feature_weights(client, weights, output_table, run_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build feature_weights for a scoring run.")
    parser.add_argument("--run-id", default=SCORING_RUN_ID, help="Scoring run identifier.")
    parser.add_argument("--shap-values", type=Path, default=DEFAULT_SHAP_VALUES,
                        help="Path to shap_values joblib (samples, features, classes).")
    parser.add_argument("--x-shap", type=Path, default=DEFAULT_X_SHAP,
                        help="Path to X_shap joblib (DataFrame with model feature columns).")
    parser.add_argument("--bucket-tol", type=float, default=0.01,
                        help="Max allowed |Δ| of bucket weights vs locked v1 shares.")
    parser.add_argument("--no-strict", action="store_true", default=False,
                        help="Warn (don't raise) on bucket-weight drift — use for new versions.")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Compute and print weights without writing to BigQuery.")
    args = parser.parse_args()

    run(
        run_id=args.run_id,
        shap_values_path=args.shap_values,
        x_shap_path=args.x_shap,
        bucket_tol=args.bucket_tol,
        strict=not args.no_strict,
        dry_run=args.dry_run,
    )