"""
End-to-end training run: train -> fit rules -> persist everything.
==============================================================================
Where :mod:`modeling.train` and :mod:`modeling.fit_rules` are the two deep-module
interfaces, and :mod:`modeling.cluster_metrics`/:mod:`modeling.artifacts` are the
building blocks around them, this is the one driver that ties all of them together
into the command a retrain actually runs. It replaces what used to be spread across
the ``05_modeling_clustering``/``05_modeling_classification_lightgbm`` notebooks plus
the standalone ``scoring.build_weights`` script: those notebooks become thin drivers
that call :func:`run` (or the pieces it composes) and visualize the result, holding
no product logic of their own.

One call to :func:`run`:

    1. loads the tract-grain training frame (``features_ct``);
    2. calls :func:`modeling.train.train` to cluster, classify, and explain it;
    3. calls :func:`modeling.cluster_metrics.compute_cluster_metrics` on the fitted
       KMeans, and combines it with the classifier's held-out accuracy/macro-F1 into
       one ``fit_metrics`` dict;
    4. calls :func:`modeling.fit_rules.fit_scoring_rules` to turn the SHAP importances
       and the current parcel feature population into ``feature_weights`` and
       ``scaling_params`` (``strict=False`` by default -- the bucket-weight tolerance
       in :func:`scoring.weights.compute_feature_weights` is checked against the locked
       v1 shares, and a new run version is expected to drift from them);
    5. assembles and writes the JSON run artifact (:mod:`modeling.artifacts`);
    6. writes ``feature_weights``/``scaling_params`` to BigQuery and registers the run
       in the ``scoring_runs`` ledger, pointing it at the artifact file.

``dry_run=True`` runs every step in memory (so the fit is exercised against real data)
but skips every write, for a rehearsal before committing a new artifact.
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
    BQ_TABLE_FEATURES_CT,
    BQ_TABLE_PARCEL_FEATURES,
    BQ_DATASET_ANALYTICS,
    BQ_TABLE_FEATURE_WEIGHTS,
    BQ_TABLE_SCALING_PARAMS,
)
from network_idx.constants import (
    SCORING_RUN_ID,
    SCORING_RUN_MODEL,
    SCORING_RUN_K,
    SCORING_RUN_VERSION,
)
from network_idx.modeling.train import train
from network_idx.modeling.cluster_metrics import compute_cluster_metrics
from network_idx.modeling.fit_rules import fit_scoring_rules
from network_idx.modeling.artifacts import (
    build_run_artifact,
    current_code_version,
    write_run_artifact,
)
from network_idx.modeling.registry import build_run_record, write_run
from network_idx.scoring.weights import write_feature_weights
from network_idx.scoring.scaling import write_scaling_params
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_bq_client() -> bigquery.Client:
    """Create an authenticated BigQuery client, authenticating first when local."""
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def features_ct_table_ref() -> str:
    """Fully-qualified tract-grain training frame this run is fit on."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_FEATURES_CT}"


def parcel_features_table_ref() -> str:
    """Fully-qualified parcel-grain frame the scaling params are scanned over."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_FEATURES}"


def feature_weights_table_ref() -> str:
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_FEATURE_WEIGHTS}"


def scaling_params_table_ref() -> str:
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_SCALING_PARAMS}"


def load_training_frame(client: bigquery.Client, source_table: str = None) -> pd.DataFrame:
    """Load the full tract-grain training frame into memory. No filtering happens
    here -- the training-population selection (which tracts are eligible to train on)
    already happened when ``features_ct`` was assembled in ``grain_transfer``."""
    source_table = source_table or features_ct_table_ref()
    logger.info(f"Loading training frame from {source_table} ...")
    frame = client.query(f"SELECT * FROM `{source_table}`").to_dataframe()
    logger.info(f"Loaded {len(frame)} rows.")
    return frame


def run(
    run_id: str = SCORING_RUN_ID,
    model: str = SCORING_RUN_MODEL,
    k: int = SCORING_RUN_K,
    version: str = SCORING_RUN_VERSION,
    client: bigquery.Client = None,
    strict: bool = False,
    dry_run: bool = False,
) -> dict:
    """Execute one full training run end to end and return everything it produced:
    ``artifacts`` (the :class:`modeling.train.ModelArtifacts`), ``rules`` (the
    ``{"weights", "scaling_params"}`` dict from :func:`fit_scoring_rules`), ``payload``
    (the run-artifact dict), and ``artifact_file`` (the path it was written to, absent
    on a dry run).

    ``strict`` is forwarded to :func:`fit_scoring_rules`: pass ``True`` to require the
    new run's bucket weights to match the locked v1 shares within tolerance instead of
    only warning on drift.
    """
    if client is None:
        client = get_bq_client()

    training_table = features_ct_table_ref()
    frame = load_training_frame(client, training_table)

    artifacts = train(frame, k=k)

    kmeans_metrics = compute_cluster_metrics(
        artifacts.x_scaled, artifacts.cluster_labels, artifacts.kmeans
    )
    fit_metrics = {
        "kmeans": kmeans_metrics,
        "classifier": {
            "accuracy": artifacts.classifier_accuracy,
            "macro_f1": artifacts.classifier_macro_f1,
        },
    }
    logger.info(f"Fit metrics: {fit_metrics}")

    rules = fit_scoring_rules(
        artifacts.shap_values, artifacts.x_shap, run_id, client,
        source_table=parcel_features_table_ref(), strict=strict,
    )

    payload = build_run_artifact(
        run_id=run_id,
        model=model,
        k=k,
        version=version,
        code_version=current_code_version(),
        training_data_table=training_table,
        training_data_rows=len(frame),
        hyperparams=artifacts.params,
        fit_metrics=fit_metrics,
        feature_cols=artifacts.feature_cols,
        scaler=artifacts.scaler,
        cluster_centers=artifacts.kmeans.cluster_centers_,
        feature_weights=rules["weights"],
        scaling_params=rules["scaling_params"],
    )

    result = {"artifacts": artifacts, "rules": rules, "payload": payload}

    if dry_run:
        logger.info("Dry run -- skipping all BigQuery and artifact writes.")
        return result

    write_feature_weights(client, rules["weights"], feature_weights_table_ref(), run_id)
    write_scaling_params(client, rules["scaling_params"], scaling_params_table_ref(), run_id)
    artifact_file = write_run_artifact(payload)

    record = build_run_record(
        run_id=run_id,
        model=model,
        k=k,
        version=version,
        feature_weights_table=feature_weights_table_ref(),
        scaling_params_table=scaling_params_table_ref(),
        artifact_path=str(artifact_file),
        code_version=payload["code_version"],
        training_data_table=training_table,
        notes=(
            f"silhouette={kmeans_metrics['silhouette']:.3f} "
            f"classifier_macro_f1={artifacts.classifier_macro_f1:.3f}"
        ),
    )
    write_run(client, record)

    logger.info(f"Run {run_id} complete. Artifact written to {artifact_file}.")
    result["artifact_file"] = artifact_file
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Run the full training pipeline: cluster, classify, explain, fit scoring "
            "rules, and persist the run artifact, registry row, and BQ rule tables."
        )
    )
    parser.add_argument("--run-id", default=SCORING_RUN_ID, help="Scoring run identifier.")
    parser.add_argument("--model", default=SCORING_RUN_MODEL)
    parser.add_argument("--k", type=int, default=SCORING_RUN_K)
    parser.add_argument("--version", default=SCORING_RUN_VERSION)
    parser.add_argument(
        "--strict", action="store_true", default=False,
        help="Require bucket weights to match the locked v1 shares within tolerance "
             "instead of only warning on drift.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Run the full fit in memory but skip all BigQuery and artifact writes.",
    )
    args = parser.parse_args()
    run(
        run_id=args.run_id, model=args.model, k=args.k, version=args.version,
        strict=args.strict, dry_run=args.dry_run,
    )
