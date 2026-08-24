"""
Render and execute grain-transfer promotions.

``promote`` is the single entry point that turns a :class:`PromotionSpec` plus a backend
adapter into a ``CREATE OR REPLACE TABLE`` statement and, unless asked only to render,
runs it. The rendering is a pure function so a promotion's SQL can be inspected — in a
dry run or a test — without a live connection, and the adapter absorbs every dialect
difference so the same spec produces valid BigQuery or DuckDB. Only aggregate-up
promotions are generated here; the downward broadcast is described by the spec model but
not yet emitted.
"""
from __future__ import annotations

import logging

from network_idx.grain_transfer.adapters.base import GrainAdapter
from network_idx.grain_transfer.specs import AGGREGATE_UP, PromotionSpec

logger = logging.getLogger(__name__)


def render_select(spec: PromotionSpec, adapter: GrainAdapter, source_table: str,
                  xwalk_table: str = "") -> str:
    """
    Render the SELECT that computes a promotion, without the create-or-replace wrapper.

    The target grain key is derived either from ``spec.key_expr`` over the source row or,
    when the spec uses a crosswalk, from the crosswalk's ``xwalk_target_expr`` after
    joining it on. Every carried column becomes ``<aggregate> AS <out_col>`` in the spec's
    order, an optional filter is applied before grouping, and the rows are grouped by the
    single target-key column. This is a pure function; it performs no I/O.
    """
    if spec.direction != AGGREGATE_UP:
        raise NotImplementedError(
            f"Only aggregate-up promotions are rendered; {spec.name!r} is {spec.direction!r}."
        )

    key_source = spec.key_expr if not spec.uses_crosswalk else spec.xwalk_target_expr
    select_lines = [f"  {key_source} AS {adapter.quote_ident(spec.target_key)},"]
    agg_count = len(spec.aggregations)
    for i, agg in enumerate(spec.aggregations):
        comma = "," if i < agg_count - 1 else ""
        select_lines.append(
            f"  {adapter.aggregate_sql(agg.op, agg.expr)} AS {adapter.quote_ident(agg.out_col)}{comma}"
        )

    lines = ["SELECT", "\n".join(select_lines), f"FROM {adapter.quote_table(source_table)} AS s"]
    if spec.uses_crosswalk:
        if not xwalk_table:
            raise ValueError(
                f"Promotion {spec.name!r} needs a crosswalk table, but none was supplied."
            )
        lines.append(
            f"LEFT JOIN {adapter.quote_table(xwalk_table)} AS x "
            f"ON s.{adapter.quote_ident(spec.xwalk_source_key)} "
            f"= x.{adapter.quote_ident(spec.xwalk_join_key)}"
        )
    if spec.filter_sql:
        lines.append(f"WHERE {spec.filter_sql}")
    lines.append(f"GROUP BY {adapter.quote_ident(spec.target_key)}")
    return "\n".join(lines)


def render_sql(spec: PromotionSpec, adapter: GrainAdapter, source_table: str,
               output_table: str, xwalk_table: str = "") -> str:
    """
    Render the full create-or-replace statement for a promotion.

    This wraps :func:`render_select` in the adapter's create-or-replace-table form so the
    result is a single executable statement. It is pure and does no I/O, so it can be
    printed in a dry run or asserted on in a test.
    """
    select_sql = render_select(spec, adapter, source_table, xwalk_table=xwalk_table)
    return adapter.create_or_replace(output_table, select_sql)


def promote(spec: PromotionSpec, adapter: GrainAdapter, source_table: str,
            output_table: str, xwalk_table: str = "", dry_run: bool = False) -> str:
    """
    Render a promotion and, unless this is a dry run, execute it through the adapter.

    The rendered SQL is always returned so the caller can log or inspect it; when
    ``dry_run`` is true the statement is only rendered, and otherwise it is run against the
    adapter's backend to create or replace ``output_table``. Errors from rendering (an
    unsupported direction, a missing crosswalk) surface before any execution is attempted.
    """
    sql = render_sql(spec, adapter, source_table, output_table, xwalk_table=xwalk_table)
    logger.info("Promotion %s: %s -> %s", spec.name, source_table, output_table)
    if dry_run:
        logger.info("Dry run — rendered SQL:\n%s", sql)
        return sql
    adapter.execute(sql)
    logger.info("Promotion %s complete; table %s created/replaced.", spec.name, output_table)
    return sql
