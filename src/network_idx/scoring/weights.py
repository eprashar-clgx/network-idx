"""
SHAP → feature weights for the parcel-level index.
==============================================================
Loads the saved multiclass SHAP artifacts from the k=8 classifier, collapses them
to mean(|SHAP|) per feature (identical to the notebook), renames model feature
names to parcel_features canonical names, and produces a tidy `feature_weights`
frame (one row per feature per run) carrying the three weight views used downstream:

    weight_overall    feature share of total |SHAP|        (Σ over 13 = 1)
    weight_in_bucket  feature share within its bucket       (Σ within bucket = 1)
    bucket_weight     bucket share of total |SHAP|          (Σ over 3 buckets = 1)

Overall index = weighted avg of sub-indices: idx = Σ_bucket bucket_weight · idx_bucket,
with idx_bucket = Σ_{f∈bucket} weight_in_bucket_f · scaled_f.
"""

import logging

import numpy as np
import pandas as pd
from google.cloud import bigquery

from network_idx.constants import (
    ALL_SCORING_FEATURES,
    SCORING_BUCKETS,
    SCORING_BUCKET_WEIGHTS,
    MODEL_TO_SCORING_FEATURE,
)

logger = logging.getLogger(__name__)

FEATURE_WEIGHTS_COLUMNS = [
    "run_id", "feature", "bucket", "mean_abs_shap",
    "weight_overall", "weight_in_bucket", "bucket_weight", "created_at",
]

# feature -> bucket lookup (built once from the single source of truth)
_FEATURE_TO_BUCKET = {f: b for b, feats in SCORING_BUCKETS.items() for f in feats}


def mean_abs_shap_by_feature(shap_values, x_shap: pd.DataFrame) -> pd.Series:
    """
    Collapse multiclass SHAP to mean(|SHAP|) per feature, keyed by MODEL feature name.

    Mirrors the notebook exactly: np.abs(shap_values).mean(axis=(0, 2)) over a
    (samples, features, classes) array. Feature names come from X_shap columns.
    """
    grand_mean = np.abs(np.asarray(shap_values)).mean(axis=(0, 2))
    model_features = list(x_shap.columns)
    if len(grand_mean) != len(model_features):
        raise ValueError(
            f"SHAP feature axis ({len(grand_mean)}) != X_shap columns "
            f"({len(model_features)}); check the joblib pair."
        )
    return pd.Series(grand_mean, index=model_features, name="mean_abs_shap")


def compute_feature_weights(
    shap_values,
    x_shap: pd.DataFrame,
    run_id: str,
    bucket_tol: float = 0.01,
    strict: bool = True,
) -> pd.DataFrame:
    """Build the tidy feature_weights frame and validate bucket weights vs the locked v1."""
    s = mean_abs_shap_by_feature(shap_values, x_shap)

    # model names -> canonical parcel_features names (identity where unmapped)
    s.index = [MODEL_TO_SCORING_FEATURE.get(f, f) for f in s.index]

    missing = set(ALL_SCORING_FEATURES) - set(s.index)
    extra = set(s.index) - set(ALL_SCORING_FEATURES)
    if missing:
        raise KeyError(f"SHAP missing scoring features after rename: {sorted(missing)}")
    if extra:
        raise KeyError(f"SHAP has features not in ALL_SCORING_FEATURES: {sorted(extra)}")

    s = s.reindex(ALL_SCORING_FEATURES)
    total = float(s.sum())
    bucket_sums = {b: float(s[feats].sum()) for b, feats in SCORING_BUCKETS.items()}

    now = pd.Timestamp.now(tz="UTC")
    records = []
    for f in ALL_SCORING_FEATURES:
        bucket = _FEATURE_TO_BUCKET[f]
        val = float(s[f])
        records.append({
            "run_id": run_id,
            "feature": f,
            "bucket": bucket,
            "mean_abs_shap": val,
            "weight_overall": val / total,
            "weight_in_bucket": val / bucket_sums[bucket],
            "bucket_weight": bucket_sums[bucket] / total,
            "created_at": now,
        })

    df = pd.DataFrame.from_records(records, columns=FEATURE_WEIGHTS_COLUMNS)

    # validate bucket weights against the locked v1 shares
    for b, expected in SCORING_BUCKET_WEIGHTS.items():
        actual = bucket_sums[b] / total
        delta = actual - expected
        msg = f"bucket '{b}': actual {actual:.3f} vs expected {expected:.2f} (Δ {delta:+.3f})"
        if abs(delta) > bucket_tol:
            if strict:
                raise ValueError(
                    f"Bucket weight drift exceeds tol={bucket_tol}: {msg}. "
                    f"Pass strict=False to override (e.g. a new run version)."
                )
            logger.warning(msg)
        else:
            logger.info(msg)

    return df


def write_feature_weights(
    client: bigquery.Client, weights: pd.DataFrame, table_id: str, run_id: str
) -> None:
    """Replace this run's rows (delete-then-append) so multiple runs can coexist."""
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
        weights, table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    logger.info(f"Wrote {len(weights)} weight rows for run_id={run_id} to {table_id}")


def read_feature_weights(
    client: bigquery.Client, table_id: str, run_id: str
) -> pd.DataFrame:
    sql = f"SELECT * FROM `{table_id}` WHERE run_id = @run_id"
    return client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]),
    ).to_dataframe()