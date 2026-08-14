"""Ingests raw data for the pipeline behind a single interface, so that callers never depend on where the data physically comes from. The authoritative production source for the FCC, demographic, location, and rextag families is the BigQuery production database; the local file downloads are a development and backfill path only. The Census families are always downloaded."""
from typing import Optional, Sequence

import pandas as pd

from network_idx.sources import bq_prod
from network_idx.sources.registry import RAW_SOURCES_BQ, RAW_SOURCES_DOWNLOAD


def get_raw(
    source: str,
    client=None,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Return the raw data for a logically named source as a DataFrame.

    BigQuery-production sources are read through the injected BigQuery client, which
    the caller must supply; the optional column list and row limit are passed through
    to the read. Download sources are not readable through this interface because they
    are fetched to local disk by their own command-line entry point, so requesting one
    raises NotImplementedError with the command to run. An unknown source name raises
    KeyError so that typos fail loudly rather than silently returning nothing.
    """
    if source in RAW_SOURCES_BQ:
        if client is None:
            raise ValueError(
                f"Reading BigQuery source '{source}' requires a BigQuery client; "
                f"pass client=... to get_raw()."
            )
        return bq_prod.read_table(
            RAW_SOURCES_BQ[source], client, columns=columns, limit=limit
        )

    if source in RAW_SOURCES_DOWNLOAD:
        raise NotImplementedError(
            f"Source '{source}' is a local download, not a readable table. "
            f"Fetch it first with: {RAW_SOURCES_DOWNLOAD[source].command_hint}"
        )

    known = sorted(RAW_SOURCES_BQ) + sorted(RAW_SOURCES_DOWNLOAD)
    raise KeyError(f"Unknown source '{source}'. Known sources: {known}")
