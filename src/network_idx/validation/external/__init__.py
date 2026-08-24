"""External convergent validation: comparison of the index against independent proxies such as ACS broadband adoption, BEAD and RDOF funding data, and published peer indices."""

from network_idx.validation.external.anchors import (
    AnchorReport,
    auroc,
    average_precision,
    evaluate_binary_anchor,
    rank_correlation,
    top_decile_lift,
)

__all__ = [
    "AnchorReport",
    "auroc",
    "average_precision",
    "evaluate_binary_anchor",
    "rank_correlation",
    "top_decile_lift",
]
