"""
BigQuery-production read adapter for the network_idx pipeline.

This module is the thin, testable boundary between the pipeline and BigQuery. It
turns a request for a raw source into a `SELECT` query and returns the result as a
pandas DataFrame, taking the BigQuery client as an argument rather than creating one
itself. Passing the client in means tests can supply a fake client with no network
access, and production code can supply a real authenticated client. The optional
column list and row limit let callers read only what they need while exploring.
"""
from typing import Optional, Sequence

import pandas as pd

from network_idx.sources.registry import BQSource


def build_select_sql(
    source: BQSource,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
) -> str:
    """
    Build a `SELECT` statement that reads a raw BigQuery source.

    When no columns are given the query selects every column; when a limit is given
    it caps the number of rows returned.
    """
    projection = ", ".join(columns) if columns else "*"
    sql = f"SELECT {projection} FROM `{source.table_ref}`"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql


def read_table(
    source: BQSource,
    client,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Read a raw BigQuery source into a DataFrame using the supplied client.

    The client is injected so this function performs no authentication or client
    creation of its own; it only builds the query, runs it, and returns the result.
    """
    sql = build_select_sql(source, columns=columns, limit=limit)
    return client.query(sql).to_dataframe()
