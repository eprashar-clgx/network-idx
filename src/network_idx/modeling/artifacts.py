"""
Run artifact — the git-committed JSON payload for a single training run.
==============================================================================
Every training run needs a durable record of what it produced beyond the two BQ
rule tables (``feature_weights``, ``scaling_params``): the fit-quality metrics that
justify shipping it, the exact hyperparameters and code version it ran under, and
enough of the fitted scaler/KMeans geometry to interpret a segment without
re-fitting. Rather than pickling the model (the LightGBM classifier alone is
~4,000 trees at the shipped ``k=8`` × 500-estimator setting — too large and too
opaque a binary to review in a pull request), this module writes one small,
human-readable JSON file per run to ``artifacts/runs/<run_id>.json`` and commits it
to git. Nothing in it needs GCS or a pickle: the classifier and raw SHAP tensor are
deterministic given the frozen random seed and are cheap to reproduce by rerunning
``train()``, so only the small numeric artifacts that are expensive or impossible
to regenerate byte-for-byte are persisted (see ``build_run_artifact``).

The BQ ``scoring_runs`` registry (``modeling.registry``) stays a thin ledger row
that points at this file via ``artifact_path`` — it never duplicates the payload.
"""

import json
import logging
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd

from network_idx.config import ARTIFACTS_DIR_RUNS

logger = logging.getLogger(__name__)


def artifact_path(run_id: str, base_dir: Path = ARTIFACTS_DIR_RUNS) -> Path:
    """Path this run's JSON artifact is written to / read from."""
    return Path(base_dir) / f"{run_id}.json"


def current_code_version() -> str:
    """Git SHA of HEAD, or ``"unknown"`` if this isn't a git checkout or git isn't
    on PATH (e.g. a packaged install running off-VM)."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception as e:
        logger.warning(f"Could not resolve git SHA for code_version: {e}")
        return "unknown"


def _json_default(obj):
    """``json.dumps(default=...)`` hook for the numpy/pandas types that appear in
    the payload but aren't natively JSON-serializable: numpy integer and boolean
    scalars (which ``DataFrame.to_dict(orient="records")`` produces even from
    columns built with plain Python ints/bools) and pandas ``Timestamp``.
    ``numpy.float64`` subclasses ``float`` and needs no help."""
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def build_run_artifact(
    run_id: str,
    model: str,
    k: int,
    version: str,
    code_version: str,
    training_data_table: str,
    training_data_rows: int,
    hyperparams: dict,
    fit_metrics: dict,
    feature_cols: list,
    scaler,
    cluster_centers: np.ndarray,
    feature_weights: pd.DataFrame,
    scaling_params: pd.DataFrame,
) -> dict:
    """Assemble the run-artifact payload. Pure: no I/O.

    ``scaler`` is the fitted ``StandardScaler`` ``train()`` returns; only its
    ``mean_``/``scale_`` are persisted (the fitted object itself isn't JSON-
    serializable and isn't needed downstream — any code that needs to re-scale
    a feature can do so with these two vectors). ``cluster_centers`` are stored
    in the scaled space KMeans was fit in; combine with ``scaler`` above to
    interpret them in raw feature units. ``feature_weights``/``scaling_params``
    mirror the BQ tables of the same name, recorded as lists of records, so the
    artifact is a self-contained record of every rule the run produced,
    independent of BigQuery.
    """
    return {
        "run_id": run_id,
        "model": model,
        "k": k,
        "version": version,
        "code_version": code_version,
        "training_data_table": training_data_table,
        "training_data_rows": training_data_rows,
        "created_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "hyperparams": hyperparams,
        "fit_metrics": fit_metrics,
        "feature_cols": list(feature_cols),
        "scaler": {
            "mean_": np.asarray(scaler.mean_).tolist(),
            "scale_": np.asarray(scaler.scale_).tolist(),
        },
        "cluster_centers": np.asarray(cluster_centers).tolist(),
        "feature_weights": feature_weights.to_dict(orient="records"),
        "scaling_params": scaling_params.to_dict(orient="records"),
    }


def write_run_artifact(payload: dict, path: Path = None) -> Path:
    """Serialize the payload to JSON, creating parent directories as needed.
    Defaults to ``artifact_path(payload["run_id"])`` when ``path`` isn't given."""
    path = Path(path) if path is not None else artifact_path(payload["run_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default))
    logger.info(f"Wrote run artifact for run_id={payload['run_id']} to {path}")
    return path


def read_run_artifact(run_id: str, path: Path = None) -> dict:
    """Load a previously-written run artifact by ``run_id`` (or an explicit path)."""
    path = Path(path) if path is not None else artifact_path(run_id)
    return json.loads(path.read_text())
