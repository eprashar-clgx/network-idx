"""
Cluster-quality metrics for a fitted KMeans segmentation.
==============================================================================
``train()`` returns the fitted ``kmeans``, its ``cluster_labels``, and the
``x_scaled`` matrix those labels were assigned from. This module turns that triple
into the numbers that answer "are these clusters any good?" — inertia (how tight),
silhouette / Davies-Bouldin / Calinski-Harabasz (how well-separated), and cluster
sizes (how balanced) — so the question can be answered from a run artifact instead
of eyeballing a notebook plot.

Every function here is pure and takes its inputs directly (no BQ client, no file
I/O) so they can be called from ``run_training.py`` for the production fit metrics,
or imported straight into a notebook for exploration. ``inertia_by_k`` is the one
exception to "just report on the existing fit": it refits KMeans across a range of
``k`` to produce an elbow curve. It is notebook-exploration tooling only — nothing
in the production path calls it — kept here because it shares the same inputs and
audience as the rest of this module.

Silhouette is O(n^2) in the number of points, so ``silhouette`` samples down to
``sample_size`` rows before computing it; Davies-Bouldin and Calinski-Harabasz are
O(n) and computed over the full population, and serve as a cross-check that the
sampled silhouette score isn't a sampling artifact.
"""

import logging

import numpy as np

logger = logging.getLogger(__name__)

DEFAULT_SILHOUETTE_SAMPLE = 5000
DEFAULT_RANDOM_STATE = 42


def inertia(kmeans) -> float:
    """Sum of squared distances of samples to their closest cluster centre, as fit."""
    return float(kmeans.inertia_)


def cluster_sizes(labels: np.ndarray) -> dict:
    """Row count per cluster label, keyed by segment id as a string (JSON-friendly)."""
    values, counts = np.unique(labels, return_counts=True)
    return {str(int(v)): int(c) for v, c in zip(values, counts)}


def silhouette(
    x_scaled: np.ndarray,
    labels: np.ndarray,
    sample_size: int = DEFAULT_SILHOUETTE_SAMPLE,
    random_state: int = DEFAULT_RANDOM_STATE,
) -> float:
    """Mean silhouette coefficient, sampled down to ``sample_size`` rows since the
    exact computation is O(n^2) and infeasible at full training-population size.
    Uses sklearn's own ``sample_size`` sampling when the population exceeds it, so
    the same random rows are used for the distance and label arrays."""
    from sklearn.metrics import silhouette_score

    n = min(sample_size, len(labels))
    return float(silhouette_score(
        x_scaled, labels, sample_size=n, random_state=random_state,
    ))


def davies_bouldin(x_scaled: np.ndarray, labels: np.ndarray) -> float:
    """Davies-Bouldin index (lower is better); O(n), computed over every row."""
    from sklearn.metrics import davies_bouldin_score

    return float(davies_bouldin_score(x_scaled, labels))


def calinski_harabasz(x_scaled: np.ndarray, labels: np.ndarray) -> float:
    """Calinski-Harabasz index (higher is better); O(n), computed over every row."""
    from sklearn.metrics import calinski_harabasz_score

    return float(calinski_harabasz_score(x_scaled, labels))


def inertia_by_k(
    x_scaled: np.ndarray,
    k_range=range(2, 16),
    random_state: int = DEFAULT_RANDOM_STATE,
) -> dict:
    """Elbow-curve helper: refit KMeans at each ``k`` in ``k_range`` and report its
    inertia, keyed by ``k`` as a string. Notebook-exploration only — refits KMeans
    len(k_range) times, so it is never called from the production training path."""
    from sklearn.cluster import KMeans

    curve = {}
    for k in k_range:
        fit = KMeans(n_clusters=k, random_state=random_state, n_init=10).fit(x_scaled)
        curve[str(int(k))] = float(fit.inertia_)
    return curve


def compute_cluster_metrics(
    x_scaled: np.ndarray,
    labels: np.ndarray,
    kmeans,
    sample_size: int = DEFAULT_SILHOUETTE_SAMPLE,
    random_state: int = DEFAULT_RANDOM_STATE,
) -> dict:
    """Aggregate every cluster-quality metric into the dict a run artifact's
    ``fit_metrics.kmeans`` section stores. This is the single call site
    ``run_training.py`` uses; the individual functions above exist for notebook use."""
    return {
        "inertia": inertia(kmeans),
        "cluster_sizes": cluster_sizes(labels),
        "silhouette": silhouette(x_scaled, labels, sample_size, random_state),
        "davies_bouldin": davies_bouldin(x_scaled, labels),
        "calinski_harabasz": calinski_harabasz(x_scaled, labels),
    }
