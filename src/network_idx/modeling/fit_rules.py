"""
Fit the frozen scoring rules — feature weights and scaling parameters — for a run.
==============================================================================
This is the second modeling interface from ADR-0002. Where :mod:`modeling.train`
produces the model and its SHAP importances (rerun only when the model is refreshed),
this module turns those importances plus the current feature population into the two
artifacts scoring actually applies, and is rerun whenever the feature population
changes:

    * feature weights  — SHAP importances collapsed into per-feature and per-bucket
      weights, validated against the locked bucket shares;
    * scaling params   — the country-wide min / max / winsorise caps that map each raw
      feature onto a common 0–1 range.

The numeric logic is the single source of truth in :mod:`scoring.weights` and
:mod:`scoring.scaling`; this module is the thin orchestration seam over them so that
the ``build_weights`` / ``build_scaling_params`` drivers, the notebooks, and any future
caller all fit rules the same way. :func:`fit_scoring_rules` fits both in one call and
returns them as a dict, matching the ADR's ``fit(training_frame, shap) → {weights,
scaling_params}`` shape; the two ``fit_*`` helpers exist for callers that only need one
side (the weight builder has SHAP but no BigQuery scan; the scaling builder has the scan
but no SHAP).
"""

import logging

import pandas as pd

from network_idx.scoring.weights import compute_feature_weights
from network_idx.scoring.scaling import compute_scaling_params_bq

logger = logging.getLogger(__name__)


def fit_feature_weights(
    shap_values,
    x_shap: pd.DataFrame,
    run_id: str,
    bucket_tol: float = 0.01,
    strict: bool = True,
) -> pd.DataFrame:
    """Collapse the run's SHAP importances into the tidy feature-weights frame and
    validate the bucket shares against the locked values. Thin wrapper over
    :func:`scoring.weights.compute_feature_weights`."""
    return compute_feature_weights(
        shap_values, x_shap, run_id, bucket_tol=bucket_tol, strict=strict
    )


def fit_scaling_params(client, source_table: str, run_id: str) -> pd.DataFrame:
    """Scan the feature population once and assemble the frozen scaling parameters.
    Thin wrapper over :func:`scoring.scaling.compute_scaling_params_bq`."""
    return compute_scaling_params_bq(client, source_table, run_id)


def fit_scoring_rules(
    shap_values,
    x_shap: pd.DataFrame,
    run_id: str,
    client,
    source_table: str,
    bucket_tol: float = 0.01,
    strict: bool = True,
) -> dict:
    """Fit both scoring rules for ``run_id`` and return them keyed by name.

    ``shap_values`` / ``x_shap`` come from :func:`modeling.train.train` (or the saved
    joblibs for the shipped run); ``client`` / ``source_table`` point at the feature
    population the scaling caps are derived from. Returns
    ``{"weights": DataFrame, "scaling_params": DataFrame}``.
    """
    weights = fit_feature_weights(
        shap_values, x_shap, run_id, bucket_tol=bucket_tol, strict=strict
    )
    scaling_params = fit_scaling_params(client, source_table, run_id)
    logger.info(
        "Fitted scoring rules for run_id=%s: %d weight rows, %d scaling rows.",
        run_id, len(weights), len(scaling_params),
    )
    return {"weights": weights, "scaling_params": scaling_params}
