"""
Assembly of the construct-validity dossier (the acceptance scorecard).

No single number passes a composite indicator; credibility is cumulative. This module
collects the verdicts produced by the four axes — internal, external, temporal, expert —
into one tidy scorecard with, for each test, its metric, the suggested threshold, the
observed value, and a pass flag, plus a headline verdict over the whole set. The
scorecard is the artifact a reader opens instead of looking for an accuracy number, and
it is the shape the periodic dossier is published in.

The assembler takes the already-computed per-axis reports so it stays a pure formatting
step with no statistics of its own; each axis module owns its own maths. Absent axes are
simply omitted, which lets the dossier grow as external data and archived snapshots
become available.
"""
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

from network_idx.validation.expert.review import ExpertReport
from network_idx.validation.external.anchors import AnchorReport
from network_idx.validation.internal.coherence import CoherenceReport
from network_idx.validation.internal.spatial import SpatialReport
from network_idx.validation.temporal.backtest import TemporalReport


@dataclass(frozen=True)
class ScorecardRow:
    """One dossier line: axis, test, metric name, threshold text, value, and pass."""

    axis: str
    test: str
    metric: str
    threshold: str
    value: float
    passed: bool


def _sensitivity_rows(sensitivity: pd.DataFrame) -> List[ScorecardRow]:
    """Turn the per-scenario sensitivity frame into worst-case internal-axis rows."""
    if sensitivity is None or sensitivity.empty:
        return []
    worst_shift = float(sensitivity["median_abs_rank_shift"].max())
    worst_rho = float(sensitivity["spearman_rho"].min())
    return [
        ScorecardRow(
            "internal", "sensitivity", "max median rank shift (pts)", "< 5",
            worst_shift, worst_shift < 5.0,
        ),
        ScorecardRow(
            "internal", "sensitivity", "min Spearman rho", "> 0.95",
            worst_rho, worst_rho > 0.95,
        ),
    ]


def build_scorecard(
    sensitivity: Optional[pd.DataFrame] = None,
    coherence: Optional[CoherenceReport] = None,
    spatial: Optional[SpatialReport] = None,
    anchors: Optional[List[AnchorReport]] = None,
    temporal: Optional[List[TemporalReport]] = None,
    expert: Optional[ExpertReport] = None,
) -> pd.DataFrame:
    """
    Fold the per-axis verdicts into a single tidy acceptance scorecard.

    Each supplied report contributes its headline metric, threshold, value, and pass flag
    as one or more rows; omitted axes are skipped so the dossier reflects only what has
    been run. The returned frame is ordered by axis and is the publishable summary that
    accompanies the full methodology write-up.
    """
    rows: List[ScorecardRow] = []
    rows.extend(_sensitivity_rows(sensitivity))

    if coherence is not None:
        rows.append(
            ScorecardRow(
                "internal", "redundancy", "max sub-index/overall corr", "< 0.90",
                coherence.max_subindex_overall_corr, not coherence.bucket_dominates,
            )
        )
    if spatial is not None:
        rows.append(
            ScorecardRow(
                "internal", "spatial coherence", "Moran's I", "positive & < 0.70",
                spatial.morans_i, spatial.positive_and_healthy,
            )
        )
    for a in anchors or []:
        rows.append(
            ScorecardRow(
                "external", f"anchor:{a.anchor}", "AUROC", ">= 0.70",
                a.auroc, a.passed,
            )
        )
    for t in temporal or []:
        rows.append(
            ScorecardRow(
                "temporal", f"future:{t.outcome}", "temporal AUROC", ">= 0.65",
                t.auroc, t.passed,
            )
        )
    if expert is not None:
        rows.append(
            ScorecardRow(
                "expert", "blind review", "weighted kappa vs experts", ">= 0.40",
                expert.mean_expert_index_kappa, expert.passed,
            )
        )
    return pd.DataFrame([r.__dict__ for r in rows])


@dataclass(frozen=True)
class Dossier:
    """The whole dossier: the scorecard frame and its cumulative pass counts."""

    scorecard: pd.DataFrame
    n_tests: int
    n_passed: int

    @property
    def all_passed(self) -> bool:
        """True only when every recorded test cleared its threshold."""
        return self.n_tests > 0 and self.n_passed == self.n_tests


def assemble_dossier(scorecard: pd.DataFrame) -> Dossier:
    """
    Wrap a scorecard with its cumulative pass counts into a dossier verdict.

    The counts make the cumulative-evidence stance explicit: the dossier reports how many
    independent tests were run and how many passed rather than collapsing to a single
    accuracy-like number, which is exactly the composite-indicator framing the project
    committed to.
    """
    n_tests = int(len(scorecard))
    n_passed = int(scorecard["passed"].sum()) if n_tests else 0
    return Dossier(scorecard=scorecard, n_tests=n_tests, n_passed=n_passed)
