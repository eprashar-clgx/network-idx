"""
Shared, grain-agnostic renderer for the four engineered telecom features.

Both the block feature (``telecom_features_block``) and the tract grain transfer
(``grain_transfer.fcc_features_ct``) derive the identical four telecom features —
cable penetration, the fiber opportunity gap, the top-tier fiber speed gated on
serviceable fiber, and the provider competitive landscape label and ordinal. Keeping
that derivation in one place means the block-grain and tract-grain features can never
drift apart (ADR-0007).

A caller supplies the ``joined_prelude`` — the ``WITH`` CTE(s), ending in a CTE named
``joined``, that yield the standard per-grain input columns
(``census_housing_units``, ``estimated_fcc_units``, ``fiber_speed_1000_100_only``, and
the per-technology ``{tech}_location_count`` / ``{tech}_provider_count``) — plus the
``key_columns`` to carry through for that grain and an optional ``CLUSTER BY`` clause.
The renderer wraps them in the shared ``CREATE OR REPLACE TABLE`` statement and generates
the label-to-ordinal ladder from the scoring contract. It is a pure function so the SQL
can be inspected and unit tested without a BigQuery client.
"""
from pathlib import Path

from network_idx.constants import PROVIDER_LANDSCAPE_ORDER

TEMPLATE_PATH = Path(__file__).parent / "telecom_features.sql"


def render_landscape_ord_cases() -> str:
    """
    Render the provider-landscape label-to-ordinal WHEN clauses from the scoring contract.

    Generating these from the single canonical ordering guarantees the ordinal written
    into the feature table matches the mapping the scorer relies on, so the two cannot
    silently drift apart.
    """
    return "\n        ".join(
        f"WHEN '{label}' THEN {rank}" for label, rank in PROVIDER_LANDSCAPE_ORDER.items()
    )


def render_engineered_telecom_sql(
    output_table: str,
    joined_prelude: str,
    key_columns: str,
    cluster_clause: str = "",
) -> str:
    """
    Render the shared engineered-telecom-feature statement for one grain.

    ``joined_prelude`` is the WITH CTE chain (ending in a ``joined`` CTE) that produces
    the standard input columns for this grain; ``key_columns`` is the comma-separated list
    of passthrough identifier columns to carry into the output; ``cluster_clause`` is an
    optional clustering clause (e.g. ``"\\nCLUSTER BY state_fips"``). This is a pure
    function performing no input or output of its own.
    """
    template = TEMPLATE_PATH.read_text()
    return template.format(
        output_table=output_table,
        cluster_clause=cluster_clause,
        joined_prelude=joined_prelude,
        key_columns=key_columns,
        landscape_ord_cases=render_landscape_ord_cases(),
    )
