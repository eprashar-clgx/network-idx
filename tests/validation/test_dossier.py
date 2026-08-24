"""Tests for the dossier assembler."""
import numpy as np
import pandas as pd

from network_idx.validation.dossier import assemble_dossier, build_scorecard
from network_idx.validation.internal.coherence import CoherenceReport
from network_idx.validation.internal.spatial import SpatialReport
from network_idx.validation.external.anchors import AnchorReport
from network_idx.validation.temporal.backtest import TemporalReport
from network_idx.validation.expert.review import ExpertReport


def _sensitivity_frame(passing=True):
    shift = 2.0 if passing else 20.0
    rho = 0.99 if passing else 0.5
    return pd.DataFrame(
        [{"scenario": "equal", "median_abs_rank_shift": shift, "spearman_rho": rho}]
    )


def test_build_scorecard_collects_all_axes():
    sc = build_scorecard(
        sensitivity=_sensitivity_frame(),
        coherence=CoherenceReport(0.7, 0.6, "telecom", False),
        spatial=SpatialReport(0.4, True, []),
        anchors=[AnchorReport("bead", 0.75, 0.4, 2.2, True, True, True)],
        temporal=[TemporalReport("became_fiber", 1000, 0.7, 2.0, True)],
        expert=ExpertReport(0.5, 0.55, True),
    )
    assert set(sc["axis"]) == {"internal", "external", "temporal", "expert"}
    assert sc["passed"].all()


def test_absent_axes_are_omitted():
    sc = build_scorecard(coherence=CoherenceReport(0.7, 0.6, "telecom", False))
    assert list(sc["axis"]) == ["internal"]


def test_assemble_dossier_counts_and_verdict():
    sc = build_scorecard(
        sensitivity=_sensitivity_frame(passing=False),
        coherence=CoherenceReport(0.7, 0.95, "telecom", True),
    )
    dossier = assemble_dossier(sc)
    assert dossier.n_tests == 3
    assert dossier.n_passed == 0
    assert dossier.all_passed is False


def test_assemble_dossier_all_passed():
    sc = build_scorecard(coherence=CoherenceReport(0.7, 0.5, "growth", False))
    dossier = assemble_dossier(sc)
    assert dossier.all_passed is True
