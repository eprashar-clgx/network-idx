"""
The execution-backend contract for grain transfer.

A grain-transfer promotion is described once, in a dialect-neutral specification, and
then rendered into SQL by an adapter. The adapter is the only place that knows how a
particular SQL engine spells things: how identifiers and table references are quoted,
how a "create or replace" statement is written, and — most importantly — how each
logical aggregate operation (sum, mean, median, distinct-count, ...) is expressed. This
lets the same promotion run against BigQuery in production and against DuckDB in a test,
with the adapter absorbing every dialect difference so the specification stays portable.
"""
from __future__ import annotations

from abc import ABC, abstractmethod

# The logical aggregate operations a promotion may request. Each adapter maps these to
# the concrete function call its SQL engine understands; keeping the set small and
# explicit means a spec can never smuggle an engine-specific function past the adapter.
SUM = "sum"
MEAN = "mean"
MEDIAN = "median"
MAX = "max"
MIN = "min"
COUNT = "count"
COUNT_DISTINCT = "count_distinct"
COUNTIF = "countif"
FIRST = "first"

SUPPORTED_OPS = frozenset(
    {SUM, MEAN, MEDIAN, MAX, MIN, COUNT, COUNT_DISTINCT, COUNTIF, FIRST}
)


class GrainAdapter(ABC):
    """
    The backend a promotion is rendered and executed against.

    Concrete adapters translate the dialect-neutral pieces of a promotion — identifier
    quoting, the create-or-replace wrapper, and each logical aggregate — into the SQL of
    one engine, and run the resulting statement. The rendering methods are pure so a
    promotion can be inspected without a live connection, and only ``execute`` touches
    the engine.
    """

    #: A short name used in logs and tests to identify the dialect (e.g. "bigquery").
    name: str = "base"

    @abstractmethod
    def quote_ident(self, ident: str) -> str:
        """Return a single identifier (a column or alias) quoted for this dialect."""

    @abstractmethod
    def quote_table(self, table: str) -> str:
        """Return a possibly-qualified table reference quoted for this dialect."""

    @abstractmethod
    def aggregate_sql(self, op: str, expr: str) -> str:
        """
        Return the SQL for one logical aggregate applied to a column expression.

        ``op`` is one of the module-level operation constants and ``expr`` is a column
        name or boolean condition (for ``countif``); the adapter renders the engine's
        equivalent, including any dialect-specific spelling such as BigQuery's
        ``APPROX_QUANTILES`` for a median.
        """

    @abstractmethod
    def create_or_replace(self, output_table: str, select_sql: str) -> str:
        """Wrap a SELECT statement in this dialect's create-or-replace-table form."""

    @abstractmethod
    def execute(self, sql: str) -> None:
        """Run a statement against the backend, discarding any result rows."""

    def _check_op(self, op: str) -> None:
        """Raise ``ValueError`` when an unsupported aggregate operation is requested."""
        if op not in SUPPORTED_OPS:
            raise ValueError(
                f"Unsupported aggregate operation {op!r}; "
                f"expected one of {sorted(SUPPORTED_OPS)}."
            )
