"""
Spatial coherence of the index (Axis A3).

Fiber opportunity is a property of places, so adjacent parcels should share reality: the
index ought to be spatially smooth (positive spatial autocorrelation) without being
trivially constant. This module computes global Moran's I to confirm that contiguity
exists but does not saturate, and local Moran's I (LISA) to surface hot- and cold-spot
clusters and, more usefully, spatial outliers — a high-scoring unit surrounded by
low-scoring neighbours — which are prime candidates for the expert review.

The weights that define "neighbour" are supplied by the caller as an adjacency list (or
a prebuilt row-standardised matrix), keeping this module free of any geometry or
BigQuery dependency so it is fully testable offline. Building adjacency from tract or
block geometry is a thin, separate concern layered on top of these kernels.
"""
from dataclasses import dataclass
from typing import Dict, List, Sequence

import numpy as np
import pandas as pd

# A significant but sub-saturation Moran's I is the healthy band the methodology names.
MAX_HEALTHY_MORANS_I = 0.70


def row_standardized_weights(
    ids: Sequence, neighbors: Dict[object, List[object]]
) -> np.ndarray:
    """
    Build a row-standardised spatial weight matrix from an adjacency list.

    Each unit's neighbours are weighted equally and its row sums to one, which is the
    conventional normalisation for Moran's I so the statistic reads as a correlation
    between a unit's value and the average of its neighbours. Unknown or self neighbours
    are ignored, and an isolated unit contributes a zero row.
    """
    index = {u: i for i, u in enumerate(ids)}
    n = len(ids)
    w = np.zeros((n, n), dtype=float)
    for u, nbrs in neighbors.items():
        if u not in index:
            continue
        i = index[u]
        valid = [index[v] for v in nbrs if v in index and v != u]
        if not valid:
            continue
        for j in valid:
            w[i, j] = 1.0 / len(valid)
    return w


def morans_i(values: Sequence[float], weights: np.ndarray) -> float:
    """
    Global Moran's I: the spatial autocorrelation of a value over a weight matrix.

    It ranges around zero for no spatial pattern up towards one for strong clustering of
    like values. The methodology expects a positive, significant, but clearly
    sub-saturation value — contiguity should exist because geography is real, yet an I
    near one would mean the index is spatially degenerate.
    """
    x = np.asarray(values, dtype=float)
    n = len(x)
    z = x - x.mean()
    s0 = weights.sum()
    denom = (z ** 2).sum()
    if s0 == 0 or denom == 0:
        return 0.0
    num = float(z @ weights @ z)
    return float((n / s0) * (num / denom))


def local_morans_i(values: Sequence[float], weights: np.ndarray) -> np.ndarray:
    """
    Local Moran's I (LISA) per unit, decomposing the global statistic spatially.

    Positive values mark units embedded in a like-valued cluster (high-high or low-low);
    negative values mark spatial outliers whose neighbourhood disagrees with them. Those
    outliers are the rows worth auditing, so this returns the per-unit vector rather than
    a single number.
    """
    x = np.asarray(values, dtype=float)
    n = len(x)
    z = x - x.mean()
    m2 = (z ** 2).sum() / n
    if m2 == 0:
        return np.zeros(n)
    return (z / m2) * (weights @ z)


@dataclass(frozen=True)
class SpatialReport:
    """The spatial verdict: global I, its healthy-band flag, and outlier unit ids."""

    morans_i: float
    positive_and_healthy: bool
    spatial_outlier_ids: List


def run_spatial(
    frame: pd.DataFrame,
    id_column: str,
    value_column: str,
    neighbors: Dict[object, List[object]],
) -> SpatialReport:
    """
    Assemble the spatial-coherence verdict for the internal axis.

    It builds the row-standardised weights from the supplied adjacency, computes global
    Moran's I, flags whether the result sits in the positive-but-not-saturated band, and
    lists the ids of spatial outliers (negative local I) for downstream expert sampling.
    """
    ids = list(frame[id_column])
    values = frame[value_column].to_numpy(dtype=float)
    w = row_standardized_weights(ids, neighbors)
    gi = morans_i(values, w)
    lisa = local_morans_i(values, w)
    outliers = [ids[i] for i in range(len(ids)) if lisa[i] < 0]
    return SpatialReport(
        morans_i=gi,
        positive_and_healthy=(gi > 0) and (gi < MAX_HEALTHY_MORANS_I),
        spatial_outlier_ids=outliers,
    )
