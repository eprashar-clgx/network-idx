"""
Train the unsupervised-then-supervised model that produces the SHAP importances.
==============================================================================
This is the first of the two modeling interfaces described in ADR-0002. It takes a
tract-grain training frame and runs the full model pipeline that used to live across
the ``05_modeling_clustering`` and ``05_modeling_classification_lightgbm`` notebooks,
returning every artifact the downstream rule-fitting step needs. The pipeline is:

    1. rename model feature names to the canonical scoring names, so the frame speaks
       the same vocabulary as the scoring contract regardless of which grain produced
       it (``median_dist_nearest_fiber_m`` → ``dist_to_nearest_fiber_miles`` etc.);
    2. apply the one canonical null-fill / winsorise contract (``apply_feature_fills``)
       instead of the notebooks' hand-written fills, so training preprocessing and
       scoring preprocessing can never drift apart;
    3. StandardScale the thirteen features and fit KMeans to discover ``k`` latent
       market segments (the unsupervised labels);
    4. train a LightGBM multiclass classifier to predict those segments from the
       unscaled filled features (trees don't need scaling), which turns the opaque
       cluster geometry into a differentiable model we can explain;
    5. run a SHAP TreeExplainer on a held-out sample to get per-feature, per-class
       attributions — the raw material the weight builder collapses into bucket and
       feature weights.

The returned :class:`ModelArtifacts` carries the fitted scaler, KMeans, classifier,
the discovered cluster labels, and the SHAP values with the sample they were computed
on. Because the whole pipeline is a pure function of the frame and the seed, a run is
reproducible and testable; the notebooks become thin drivers that load a frame, call
:func:`train`, and visualise the result.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from network_idx.constants import (
    ALL_SCORING_FEATURES,
    MODEL_TO_SCORING_FEATURE,
    SCORING_RUN_K,
)
from network_idx.scoring.scaling import apply_feature_fills

logger = logging.getLogger(__name__)

# LightGBM classifier hyper-parameters, frozen from the k=8 notebook run so a retrain
# reproduces the shipped model exactly.
LGBM_PARAMS = {
    "objective": "multiclass",
    "n_estimators": 500,
    "learning_rate": 0.05,
    "num_leaves": 31,
    "n_jobs": -1,
}

# Default size of the held-out sample the SHAP explainer runs on (capped at the size
# of the test split when the frame is smaller, e.g. in tests).
DEFAULT_SHAP_SAMPLE = 2000
DEFAULT_TEST_SIZE = 0.2
DEFAULT_RANDOM_STATE = 42


@dataclass
class ModelArtifacts:
    """Everything a single training run produces, keyed to the run's feature order.

    ``feature_cols`` is the canonical column order the scaler, classifier, and SHAP
    array all share, so downstream code can line up SHAP columns with feature names
    without guessing. ``shap_values`` has shape (sample_rows, features, classes);
    ``x_shap`` is the frame those attributions were computed on.
    """

    scaler: Any
    kmeans: Any
    classifier: Any
    feature_cols: list
    cluster_labels: np.ndarray
    shap_values: np.ndarray
    x_shap: pd.DataFrame
    k: int
    random_state: int
    params: dict = field(default_factory=lambda: dict(LGBM_PARAMS))


def _prepare_features(training_frame: pd.DataFrame) -> pd.DataFrame:
    """Rename model names to canonical names, select the thirteen scoring features, and
    apply the canonical fills. Raises if any required feature is absent so a malformed
    frame fails loudly rather than training on a silently-wrong feature set."""
    renamed = training_frame.rename(columns=MODEL_TO_SCORING_FEATURE)
    missing = [f for f in ALL_SCORING_FEATURES if f not in renamed.columns]
    if missing:
        raise KeyError(
            f"Training frame is missing scoring features after renaming: {missing}. "
            f"Present columns: {sorted(renamed.columns)}"
        )
    features = renamed[ALL_SCORING_FEATURES].astype("float64")
    return apply_feature_fills(features, ALL_SCORING_FEATURES)


def train(
    training_frame: pd.DataFrame,
    k: int = SCORING_RUN_K,
    shap_sample: int = DEFAULT_SHAP_SAMPLE,
    test_size: float = DEFAULT_TEST_SIZE,
    random_state: int = DEFAULT_RANDOM_STATE,
    params: dict = None,
) -> ModelArtifacts:
    """Run cluster → classify → explain over ``training_frame`` and return the artifacts.

    ``k`` defaults to the production segment count (``SCORING_RUN_K``). ``shap_sample``
    is clamped to the test-split size so small frames (tests) still produce a valid,
    if tiny, SHAP array. ``params`` overrides individual LightGBM hyper-parameters on
    top of the frozen :data:`LGBM_PARAMS` (used for sweeps or fast tests); leave it
    ``None`` to reproduce the shipped model exactly. The frame is treated read-only;
    a copy is filled internally.
    """
    from sklearn.cluster import KMeans
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    import lightgbm as lgb
    import shap

    lgbm_params = {**LGBM_PARAMS, **(params or {})}

    features = _prepare_features(training_frame)
    logger.info("Training on %d rows × %d features (k=%d).",
                len(features), len(ALL_SCORING_FEATURES), k)

    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(features)

    kmeans = KMeans(n_clusters=k, random_state=random_state, n_init=10)
    cluster_labels = kmeans.fit_predict(x_scaled)

    x_train, x_test, y_train, _ = train_test_split(
        features, cluster_labels,
        test_size=test_size, random_state=random_state, stratify=cluster_labels,
    )

    classifier = lgb.LGBMClassifier(
        random_state=random_state, verbose=-1, **lgbm_params
    )
    classifier.fit(x_train, y_train)

    n_sample = min(shap_sample, len(x_test))
    x_shap = x_test.sample(n=n_sample, random_state=random_state)
    explainer = shap.TreeExplainer(classifier)
    shap_values = np.asarray(explainer.shap_values(x_shap))

    logger.info("SHAP array shape %s over %d sample rows.", shap_values.shape, n_sample)

    return ModelArtifacts(
        scaler=scaler,
        kmeans=kmeans,
        classifier=classifier,
        feature_cols=list(ALL_SCORING_FEATURES),
        cluster_labels=cluster_labels,
        shap_values=shap_values,
        x_shap=x_shap,
        k=k,
        random_state=random_state,
        params=lgbm_params,
    )
