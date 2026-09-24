"""Derives the scoring rules from the training population. It exposes two operations: one trains the model through clustering, classification, and SHAP importances and is rerun only when the model is refreshed; the other fits the scoring rules, namely the feature weights and scaling parameters, and is rerun whenever the feature population changes. It also computes cluster-quality metrics, assembles/persists the JSON run artifact, and writes the run registry. Notebooks are thin drivers over this module and hold no product logic of their own."""

from network_idx.modeling.train import ModelArtifacts, train
from network_idx.modeling.cluster_metrics import (
    calinski_harabasz,
    cluster_sizes,
    compute_cluster_metrics,
    davies_bouldin,
    inertia,
    inertia_by_k,
    silhouette,
)
from network_idx.modeling.artifacts import (
    artifact_path,
    build_run_artifact,
    current_code_version,
    read_run_artifact,
    write_run_artifact,
)
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
    "calinski_harabasz",
    "cluster_sizes",
    "compute_cluster_metrics",
    "davies_bouldin",
    "inertia",
    "inertia_by_k",
    "silhouette",
    "artifact_path",
    "build_run_artifact",
    "current_code_version",
    "read_run_artifact",
    "write_run_artifact",
    "fit_feature_weights",
    "fit_scaling_params",
    "fit_scoring_rules",
    "build_run_record",
    "read_run",
    "scoring_runs_table_ref",
    "write_run",
]
