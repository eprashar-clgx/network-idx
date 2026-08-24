"""
External convergent and criterion validation against independent proxies (Axis B).

Because the index has no label, its strongest external evidence is agreement with
datasets that *should* correlate with fiber opportunity: FCC availability, BEAD and RDOF
funding eligibility, and published peer indices such as the Purdue Digital Divide Index.
Each proxy is biased and none is ground truth, so the method is to treat the external
signal as a binary or continuous anchor and measure how well the index separates or
ranks it — AUROC and top-decile lift for a binary anchor, rank correlation for a
continuous one — always checking that the *direction* of the association matches the
prior stated up front.

The kernels here are pure metrics over aligned in-memory arrays; loading the external
tables and joining them to `parcel_scores` is a separate wiring step, since that data is
not yet available offline. Keeping the metrics pure means the temporal axis can reuse
them unchanged with a strict time separation.
"""
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

# Suggested acceptance thresholds from the methodology's Axis B scorecard.
MIN_ANCHOR_AUROC = 0.70
MIN_TOP_DECILE_LIFT = 2.0


def auroc(score: pd.Series, anchor_binary: pd.Series) -> float:
    """
    Area under the ROC curve of the index against a binary external anchor.

    It answers "how well does the index rank a funded or eligible location above a random
    one," with 0.5 being no separation. A degenerate anchor (all one class) has no ROC
    and returns NaN so the dossier can mark the test not-applicable rather than fail it.
    """
    y = anchor_binary.to_numpy()
    if len(np.unique(y)) < 2:
        return float("nan")
    return float(roc_auc_score(y, score.to_numpy()))


def average_precision(score: pd.Series, anchor_binary: pd.Series) -> float:
    """
    Average precision (area under precision-recall) against a binary anchor.

    This complements AUROC when the positive class is rare — as BEAD-eligible or
    newly-fibered locations usually are — because precision-recall stays informative
    under heavy class imbalance where ROC can look deceptively strong.
    """
    y = anchor_binary.to_numpy()
    if len(np.unique(y)) < 2:
        return float("nan")
    return float(average_precision_score(y, score.to_numpy()))


def top_decile_lift(score: pd.Series, anchor_binary: pd.Series, top_fraction: float = 0.10) -> float:
    """
    Lift of the anchor's positive rate in the top-scoring fraction versus the base rate.

    A lift of two means the top decile captures twice the density of funded or eligible
    locations that random selection would, which is the actionable statement for a
    customer targeting the highest-scoring parcels. It returns NaN when the anchor has no
    positives to concentrate.
    """
    base_rate = float(anchor_binary.mean())
    if base_rate == 0:
        return float("nan")
    cutoff = score.quantile(1.0 - top_fraction)
    top = anchor_binary[score >= cutoff]
    if len(top) == 0:
        return float("nan")
    return float(top.mean() / base_rate)


def rank_correlation(score: pd.Series, anchor_continuous: pd.Series) -> float:
    """
    Spearman rank correlation of the index against a continuous external anchor.

    This is the right measure when the proxy is a magnitude — dollars per location, a
    peer index value — rather than a yes/no label; its sign and moderate magnitude are
    interpreted against the stated prior, since perfect correlation would mean the index
    adds nothing new and zero would mean it is noise.
    """
    return float(score.corr(anchor_continuous, method="spearman"))


@dataclass(frozen=True)
class AnchorReport:
    """The verdict for one external anchor: its name, metrics, expected sign, and pass."""

    anchor: str
    auroc: float
    average_precision: float
    top_decile_lift: float
    expected_positive: bool
    direction_ok: bool
    passed: bool


def evaluate_binary_anchor(
    score: pd.Series,
    anchor_binary: pd.Series,
    anchor_name: str,
    expected_positive: bool = True,
) -> AnchorReport:
    """
    Evaluate the index against one binary external anchor and render its verdict.

    It computes AUROC, average precision, and top-decile lift, checks that the direction
    of separation matches the stated prior (an anchor the index should score *high* must
    have AUROC above one half), and passes only when both the direction is right and the
    AUROC and lift clear the suggested thresholds. Encoding the prior is deliberate: the
    methodology's honesty guardrail is that predicting the *wrong* anchor well is a
    validity failure, not a success.
    """
    a = auroc(score, anchor_binary)
    lift = top_decile_lift(score, anchor_binary)
    if np.isnan(a):
        direction_ok = False
        passed = False
    else:
        direction_ok = (a >= 0.5) == expected_positive
        passed = (
            direction_ok
            and a >= MIN_ANCHOR_AUROC
            and (not np.isnan(lift))
            and lift >= MIN_TOP_DECILE_LIFT
        )
    return AnchorReport(
        anchor=anchor_name,
        auroc=a,
        average_precision=average_precision(score, anchor_binary),
        top_decile_lift=lift,
        expected_positive=expected_positive,
        direction_ok=direction_ok,
        passed=passed,
    )
