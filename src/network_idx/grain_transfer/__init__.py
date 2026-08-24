"""Moves a feature from its native grain to another, either by aggregating upward from parcel to block to tract, or by broadcasting downward from tract to block to parcel. The promotion SQL is generated from a declarative per-family specification and executed through a pluggable backend."""
from network_idx.grain_transfer.adapters import (
    BigQueryAdapter,
    DuckDBAdapter,
    GrainAdapter,
)
from network_idx.grain_transfer.promote import promote, render_select, render_sql
from network_idx.grain_transfer.specs import (
    AGGREGATE_UP,
    BROADCAST_DOWN,
    FCC_SPEEDS_CT_SPEC,
    Aggregation,
    PromotionSpec,
)

__all__ = [
    "promote",
    "render_sql",
    "render_select",
    "PromotionSpec",
    "Aggregation",
    "AGGREGATE_UP",
    "BROADCAST_DOWN",
    "FCC_SPEEDS_CT_SPEC",
    "GrainAdapter",
    "BigQueryAdapter",
    "DuckDBAdapter",
]
