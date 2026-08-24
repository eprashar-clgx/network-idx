"""Execution backends for grain transfer. BigQuery is used in production, and DuckDB is used as a local stand-in so the promotion logic can be tested without touching BigQuery."""
from network_idx.grain_transfer.adapters.base import GrainAdapter
from network_idx.grain_transfer.adapters.bigquery import BigQueryAdapter
from network_idx.grain_transfer.adapters.duckdb import DuckDBAdapter

__all__ = ["GrainAdapter", "BigQueryAdapter", "DuckDBAdapter"]
