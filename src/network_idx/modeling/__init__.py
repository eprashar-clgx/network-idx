"""Derives the scoring rules from the training population. It exposes two operations: one trains the model through clustering, classification, and SHAP importances and is rerun only when the model is refreshed; the other fits the scoring rules, namely the feature weights and scaling parameters, and is rerun whenever the feature population changes. It also writes the run registry. Notebooks are thin drivers over this module and hold no product logic of their own."""

from network_idx.modeling.train import ModelArtifacts, train
from network_idx.modeling.fit_rules import (
    fit_feature_weights,
    fit_scaling_params,
    fit_scoring_rules,
)
from network_idx.modeling.registry import (
    build_run_record,
    read_run,
    scoring_runs_table_ref,
    write_run,
)

__all__ = [
    "ModelArtifacts",
    "train",
    "fit_feature_weights",
    "fit_scaling_params",
    "fit_scoring_rules",
    "build_run_record",
    "read_run",
    "scoring_runs_table_ref",
    "write_run",
]
