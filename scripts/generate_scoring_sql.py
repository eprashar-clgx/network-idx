"""
Regenerate the console SQL artifacts for a scoring run.
=======================================================
The scripts under ``sql/scoring/`` are not hand-written: they are the rendered
pushdown queries with a specific run's weights and scaling constants baked in as
literals, so a data engineer can review and execute them in the BigQuery console
without a Python environment. They must therefore be regenerated after every refit
— stale literals here mean the console and the pipeline score differently.

This exists as a script rather than a shell pipeline because the obvious
``python -m ... --dry-run > file.sql`` is unsafe: the package prints credential and
progress messages to stdout, and a previous regeneration captured
``Error: Credentials file not found`` into the middle of a committed ``.sql`` file.
Rendering in-process and writing the file directly removes that whole class of
corruption, and lets the header record which run the literals came from.

Usage::

    GCS_PROJECT_ID=clgx-gis-app-dev-06e3 poetry run python scripts/generate_scoring_sql.py
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from network_idx.config import GCS_PROJECT_ID  # noqa: E402
from network_idx.config.bigquery import (  # noqa: E402
    BQ_DATASET_FEATURES,
    BQ_DATASET_OUTPUTS,
    BQ_PROJECT_PROD,
    BQ_TABLE_FIBER_IDX_PARCEL,
    BQ_TABLE_PARCEL_FEATURES,
    BQ_TABLE_PARCEL_GROWTH,
    BQ_TABLE_PARCEL_SCORES,
)
from network_idx.constants import SCORING_RUN_ID  # noqa: E402
from network_idx.scoring.parcel_score import (  # noqa: E402
    build_delivery_query,
    build_scoring_query,
    get_bq_client,
    resolve_artifact_tables,
)
from network_idx.scoring.scaling import build_stats_query, read_scaling_params  # noqa: E402
from network_idx.scoring.weights import read_feature_weights  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "scoring"


def header(title: str, module: str, run_in: str, run_id: str, step: str) -> str:
    """The banner every generated artifact carries.

    It names the run whose constants are baked in, because the most dangerous
    failure mode for these files is looking current while holding a previous run's
    numbers.
    """
    bar = "-- " + "=" * 77
    return "\n".join([
        bar,
        f"-- {title}",
        f"-- Module         : {module}",
        f"-- Console step   : {step}",
        f"-- Run in         : {run_in}",
        "--",
        "-- !! PIPELINE-GENERATED ARTIFACT — DO NOT HAND-EDIT.",
        "--    The weights and min/max scaling constants below are baked in from",
        f"--    run_id '{run_id}'. They change every time the model is refit.",
        "--    Regenerate with:",
        "--      poetry run python scripts/generate_scoring_sql.py",
        "--",
        f"-- PROJECT SUBSTITUTION: PROD_PROJECT={BQ_PROJECT_PROD}, "
        f"DEV_PROJECT={GCS_PROJECT_ID}",
        bar,
        "",
        "",
    ])


def generate(run_id: str) -> list:
    """Render both scoring artifacts for ``run_id`` and write them to ``sql/scoring/``."""
    client = get_bq_client()
    scaling_table, weights_table = resolve_artifact_tables(client, run_id)
    params = read_scaling_params(client, scaling_table, run_id)
    weights = read_feature_weights(client, weights_table, run_id)
    if params.empty or weights.empty:
        raise ValueError(
            f"No scaling_params/feature_weights found for run_id={run_id}. "
            f"Run `python -m network_idx.modeling.run_training` first."
        )
    logger.info(
        f"Loaded {len(weights)} weight rows and {len(params)} scaling rows "
        f"for run_id={run_id}."
    )

    features_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_FEATURES}"
    growth_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_GROWTH}"
    scores_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_PARCEL_SCORES}"
    delivery_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_FIBER_IDX_PARCEL}"

    written = []

    # Step 17 is a review artifact, not an execution step: the pipeline runs this
    # same scan in-process (compute_scaling_params_bq) and assembles the params
    # table from it. It is regenerated here so the DE can see the exact quantile
    # offsets the contract currently uses — those change whenever a winsorize
    # quantile does, and a stale copy silently misrepresents the live policy.
    path = SQL_DIR / "01_scaling_params_scan.sql"
    path.write_text(
        header(
            "Min/max + winsorize-quantile scan over the parcel feature frame that "
            "feeds scaling_params (Python assembles and writes the params table).",
            "network_idx.scoring.scaling (build_stats_query)",
            "Review only — run_training executes this scan in-process",
            run_id,
            "17 (review only)",
        )
        + build_stats_query(features_table).rstrip()
        + "\n"
    )
    written.append(path)

    scoring_sql = build_scoring_query(
        features_table, scores_table, params, weights, run_id
    )
    path = SQL_DIR / "02_parcel_score.sql"
    path.write_text(
        header(
            "Final parcel scoring pushdown: normalise, weight, recombine into "
            "0-100 indices and overall score.",
            "network_idx.scoring.parcel_score",
            "VM or console (dev write; reads dev feature frame)",
            run_id,
            "18",
        )
        + scoring_sql.rstrip()
        + "\n"
    )
    written.append(path)

    delivery_sql = build_delivery_query(
        features_table, scores_table, growth_table, delivery_table,
        params, weights, run_id,
    )
    path = SQL_DIR / "03_delivery.sql"
    path.write_text(
        header(
            "Customer delivery table: raw + scaled features, weights, indices and "
            "geo, with customer-facing column names.",
            "network_idx.scoring.parcel_score (run_delivery)",
            "VM or console (dev write; requires parcel_scores from step 18)",
            run_id,
            "19",
        )
        + delivery_sql.rstrip()
        + "\n"
    )
    written.append(path)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=SCORING_RUN_ID)
    args = parser.parse_args()

    root = SQL_DIR.parents[1]
    for path in generate(args.run_id):
        logger.info(f"Wrote {path.relative_to(root)} ({path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
