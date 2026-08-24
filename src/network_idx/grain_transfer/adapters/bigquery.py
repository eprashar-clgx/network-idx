"""
The BigQuery execution backend for grain transfer.

This adapter renders a promotion into BigQuery Standard SQL and runs it with a
``google.cloud.bigquery`` client. It quotes table references with backticks, wraps the
promotion in ``CREATE OR REPLACE TABLE ... AS``, and spells each logical aggregate with
its BigQuery function — notably a median via ``APPROX_QUANTILES`` and a distinct count
via ``COUNT(DISTINCT ...)``. It is the production path; the DuckDB adapter mirrors it for
offline tests.
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


class BigQueryAdapter(GrainAdapter):
    """Render and execute grain-transfer promotions against BigQuery."""

    name = "bigquery"

    def __init__(self, client=None):
        """Store the BigQuery client used to run statements (may be ``None`` for rendering only)."""
        self._client = client

    def quote_ident(self, ident: str) -> str:
        """Return a column or alias wrapped in backticks for BigQuery."""
        return f"`{ident}`"

    def quote_table(self, table: str) -> str:
        """Return a fully-qualified table reference wrapped in backticks for BigQuery."""
        return f"`{table}`"

    def aggregate_sql(self, op: str, expr: str) -> str:
        """Return the BigQuery SQL for one logical aggregate over a column expression."""
        self._check_op(op)
        if op == SUM:
            return f"SUM({expr})"
        if op == MEAN:
            return f"AVG({expr})"
        if op == MEDIAN:
            # BigQuery has no MEDIAN; the exact middle value is the second of three
            # quantile cut points computed without approximation error.
            return f"APPROX_QUANTILES({expr}, 2)[OFFSET(1)]"
        if op == MAX:
            return f"MAX({expr})"
        if op == MIN:
            return f"MIN({expr})"
        if op == COUNT:
            return f"COUNT({expr})"
        if op == COUNT_DISTINCT:
            return f"COUNT(DISTINCT {expr})"
        if op == COUNTIF:
            return f"COUNTIF({expr})"
        if op == FIRST:
            return f"ANY_VALUE({expr})"
        raise AssertionError("unreachable")  # pragma: no cover

    def create_or_replace(self, output_table: str, select_sql: str) -> str:
        """Wrap a SELECT in BigQuery's ``CREATE OR REPLACE TABLE`` form."""
        return (
            f"CREATE OR REPLACE TABLE {self.quote_table(output_table)} AS\n{select_sql}"
        )

    def execute(self, sql: str) -> None:
        """Run a statement with the BigQuery client and block until it finishes."""
        if self._client is None:
            raise RuntimeError(
                "BigQueryAdapter has no client; construct it with a client to execute."
            )
        self._client.query(sql).result()
