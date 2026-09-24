"""
Run registry — one durable row of metadata per scoring run.
==============================================================================
A scoring run is identified by a ``run_id`` (for the shipped model,
``lightgbm_k8_v1``) and produces two rule artifacts — ``feature_weights`` and
``scaling_params`` — plus the model itself. Those artifacts are keyed by ``run_id``
inside their own tables, but nothing records that a run happened, what model and
segment count produced it, or where its artifacts live. This module is that ledger:
it writes and reads one row per run so scoring can discover the active run and its
artifact locations instead of hard-coding them, and so runs are auditable after the
fact.

The record is deliberately small and stable: the run identity (id, model, segment
count ``k``, version), the fully-qualified tables its weights and scaling params were
written to, the feature count, a free-text note, and a creation timestamp. It also
points at the run's git-committed JSON run artifact (``artifact_path``, see
``modeling.artifacts``) and records the code version (git SHA) and the training-data
table the run was fit on, so a run is fully traceable to the code and data that
produced it without duplicating the artifact's contents into BigQuery. Writing a run
replaces any existing row for that ``run_id`` (delete-then-append) so a re-run
overwrites its own ledger entry without disturbing other runs, mirroring how the
weight and scaling writers behave.
"""

import logging

import pandas as pd
from google.cloud import bigquery

from network_idx.config import (
    GCS_PROJECT_ID,
    BQ_DATASET_ANALYTICS,
    BQ_TABLE_SCORING_RUNS,
    BQ_TABLE_FEATURE_WEIGHTS,
    BQ_TABLE_SCALING_PARAMS,
)
from network_idx.constants import (
    ALL_SCORING_FEATURES,
    SCORING_RUN_MODEL,
    SCORING_RUN_K,
    SCORING_RUN_VERSION,
)

logger = logging.getLogger(__name__)

SCORING_RUNS_COLUMNS = [
    "run_id", "model", "k", "version",
    "feature_weights_table", "scaling_params_table",
    "n_features", "artifact_path", "code_version", "training_data_table",
    "notes", "created_at",
]


def scoring_runs_table_ref() -> str:
    """Fully-qualified run-registry table for the configured project."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_SCORING_RUNS}"


def _default_artifact_tables() -> tuple:
    weights = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_FEATURE_WEIGHTS}"
    scaling = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_SCALING_PARAMS}"
    return weights, scaling


def build_run_record(
    run_id: str,
    model: str = SCORING_RUN_MODEL,
    k: int = SCORING_RUN_K,
    version: str = SCORING_RUN_VERSION,
    feature_weights_table: str = None,
    scaling_params_table: str = None,
    n_features: int = len(ALL_SCORING_FEATURES),
    artifact_path: str = "",
    code_version: str = "",
    training_data_table: str = "",
    notes: str = "",
) -> pd.DataFrame:
    """Assemble the single-row registry frame for a run. Pure: builds no client and
    performs no I/O, so it is trivially testable. Artifact table refs default to the
    configured ``teu_analytics`` weight and scaling tables when not supplied.
    ``artifact_path`` should point at the run's JSON artifact (see
    ``modeling.artifacts.artifact_path``); ``code_version`` at the git SHA the run was
    produced under; ``training_data_table`` at the fully-qualified frame it was fit on
    (typically ``features_ct``)."""
    default_weights, default_scaling = _default_artifact_tables()
    record = {
        "run_id": run_id,
        "model": model,
        "k": k,
        "version": version,
        "feature_weights_table": feature_weights_table or default_weights,
        "scaling_params_table": scaling_params_table or default_scaling,
        "n_features": n_features,
        "artifact_path": artifact_path,
        "code_version": code_version,
        "training_data_table": training_data_table,
        "notes": notes,
        "created_at": pd.Timestamp.now(tz="UTC"),
    }
    return pd.DataFrame.from_records([record], columns=SCORING_RUNS_COLUMNS)


def write_run(client: bigquery.Client, record: pd.DataFrame, table_id: str = None) -> None:
    """Replace this run's ledger row (delete-then-append) so a re-run overwrites only
    its own entry. Tolerates the table not yet existing on the first ever write."""
    table_id = table_id or scoring_runs_table_ref()
    run_id = str(record["run_id"].iloc[0])
    try:
        client.query(
            f"DELETE FROM `{table_id}` WHERE run_id = @run_id",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            ]),
        ).result()
    except Exception as e:  # table may not exist yet on first run
        logger.info(f"Skipping delete (table may not exist yet): {e}")

    job = client.load_table_from_dataframe(
        record, table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    logger.info(f"Registered run_id={run_id} in {table_id}")


def read_run(client: bigquery.Client, run_id: str, table_id: str = None) -> pd.DataFrame:
    """Read the ledger row(s) for a run_id (empty frame if the run is unknown)."""
    table_id = table_id or scoring_runs_table_ref()
    sql = f"SELECT * FROM `{table_id}` WHERE run_id = @run_id"
    return client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]),
    ).to_dataframe()
