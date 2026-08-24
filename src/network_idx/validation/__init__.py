"""Produces the periodic construct-validity dossier for the index. Because the index is a composite indicator that measures a latent construct with no directly observed label, it is never judged by accuracy; instead its credibility is built from convergent evidence gathered across internal, external, temporal, and expert axes."""

from network_idx.validation.dossier import (
    Dossier,
    ScorecardRow,
    assemble_dossier,
    build_scorecard,
)

__all__ = ["Dossier", "ScorecardRow", "assemble_dossier", "build_scorecard"]
