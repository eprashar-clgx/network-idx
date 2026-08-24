"""Internal and structural validation: sensitivity analysis of construction choices, sub-index coherence and redundancy, spatial coherence, and distribution checks by business context."""

from network_idx.validation.internal.sensitivity import (
    Scenario,
    default_scenarios,
    rank_shift_stats,
    recombine_overall,
    run_sensitivity,
)
from network_idx.validation.internal.coherence import (
    CoherenceReport,
    correlation_matrix,
    cronbach_alpha,
    run_coherence,
    weight_correlation_consistency,
)
from network_idx.validation.internal.spatial import (
    SpatialReport,
    local_morans_i,
    morans_i,
    row_standardized_weights,
    run_spatial,
)
from network_idx.validation.internal.distribution import (
    assign_density_tier,
    check_monotone_opportunity,
    feature_distribution_by_context,
    score_distribution_by_context,
)

__all__ = [
    "Scenario",
    "default_scenarios",
    "rank_shift_stats",
    "recombine_overall",
    "run_sensitivity",
    "CoherenceReport",
    "correlation_matrix",
    "cronbach_alpha",
    "run_coherence",
    "weight_correlation_consistency",
    "SpatialReport",
    "local_morans_i",
    "morans_i",
    "row_standardized_weights",
    "run_spatial",
    "assign_density_tier",
    "check_monotone_opportunity",
    "feature_distribution_by_context",
    "score_distribution_by_context",
]
