"""
The DuckDB execution backend for grain transfer.

This adapter mirrors the BigQuery one so a promotion can be rendered and executed
locally, in-process, without touching BigQuery. It quotes identifiers with double quotes,
wraps the promotion in ``CREATE OR REPLACE TABLE ... AS``, and spells each logical
aggregate with its DuckDB function — a median via ``MEDIAN`` and a distinct count via
``COUNT(DISTINCT ...)``. Its purpose is testing: a spec can be run against a handful of
in-memory rows and the aggregated result checked, which validates the promotion logic
that will later run on BigQuery.
"""
from __future__ import annotations

from network_idx.grain_transfer.adapters.base import (
    COUNT,
    COUNT_DISTINCT,
    COUNTIF,
    FIRST,
    MAX,
    MEAN,
    MEDIAN,
    MIN,
    SUM,
    GrainAdapter,
)


class DuckDBAdapter(GrainAdapter):
    """Render and execute grain-transfer promotions against DuckDB."""

    name = "duckdb"

    def __init__(self, connection=None):
        """Store the DuckDB connection used to run statements (may be ``None`` for rendering only)."""
        self._connection = connection

    def quote_ident(self, ident: str) -> str:
        """Return a column or alias wrapped in double quotes for DuckDB."""
        return f'"{ident}"'

    def quote_table(self, table: str) -> str:
        """Return a table reference wrapped in double quotes for DuckDB."""
        return f'"{table}"'

    def aggregate_sql(self, op: str, expr: str) -> str:
        """Return the DuckDB SQL for one logical aggregate over a column expression."""
        self._check_op(op)
        if op == SUM:
            return f"SUM({expr})"
        if op == MEAN:
            return f"AVG({expr})"
        if op == MEDIAN:
            return f"MEDIAN({expr})"
        if op == MAX:
            return f"MAX({expr})"
        if op == MIN:
            return f"MIN({expr})"
        if op == COUNT:
            return f"COUNT({expr})"
        if op == COUNT_DISTINCT:
            return f"COUNT(DISTINCT {expr})"
        if op == COUNTIF:
            # DuckDB has no COUNTIF; a filtered count is the portable equivalent.
            return f"COUNT(*) FILTER (WHERE {expr})"
        if op == FIRST:
            return f"ANY_VALUE({expr})"
        raise AssertionError("unreachable")  # pragma: no cover

    def create_or_replace(self, output_table: str, select_sql: str) -> str:
        """Wrap a SELECT in DuckDB's ``CREATE OR REPLACE TABLE`` form."""
        return (
            f"CREATE OR REPLACE TABLE {self.quote_table(output_table)} AS\n{select_sql}"
        )

    def execute(self, sql: str) -> None:
        """Run a statement on the DuckDB connection."""
        if self._connection is None:
            raise RuntimeError(
                "DuckDBAdapter has no connection; construct it with one to execute."
            )
        self._connection.execute(sql)
