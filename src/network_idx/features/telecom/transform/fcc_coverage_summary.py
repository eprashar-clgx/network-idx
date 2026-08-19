"""
Telecom transform: FCC fixed-broadband coverage summary from BigQuery-production.

This transform builds the place-level and county-level coverage summary that the
dasymetric block interpolation downstream reads from. It reads two production tables — the
dedicated Census-place summary and the multi-geography summary (filtered to counties) —
restricts them to the residential total-area rows for the three wired technologies, turns
the cumulative speed-threshold percentages into mutually exclusive speed tiers by
differencing adjacent thresholds, and pivots the three technologies into columns so each
geography is a single row. Place identifiers are zero-padded to the seven-character Census
place GEOID so they join to the block-assignment crosswalk later.

Because it is a mechanical filter, difference, and pivot with no analytical choice, it
lives in the transform layer. It replaces the earlier pandas pipeline that unzipped
per-state CSVs and produced separate place and county parquet files; here both geography
levels land in one table distinguished by a geography_level column. The input tables are
resolved from the source registry and the output table from configuration, the SQL is
rendered separately from execution so it can be inspected without a BigQuery client, and
the client is injected so production supplies a real client while tests supply a fake one.
"""
import argparse
import logging
from pathlib import Path

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FCC_COVERAGE,
    BQ_TABLE_FCC_COVERAGE_SUMMARY,
)
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "fcc_coverage_summary.sql"

# Logical names in the source registry: the dedicated Census-place summary table and the
# multi-geography summary table from which the county rows are extracted.
PLACE_SOURCE = "fcc_summary"
GEOGRAPHY_SOURCE = "fcc_geography"


def output_table_ref() -> str:
    """Return the fully qualified coverage summary table this transform writes to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_SUMMARY}"


def place_table_ref() -> str:
    """Return the fully qualified raw Census-place coverage summary table from the registry."""
    return RAW_SOURCES_BQ[PLACE_SOURCE].table_ref


def geography_table_ref() -> str:
    """Return the fully qualified raw multi-geography coverage summary table from the registry."""
    return RAW_SOURCES_BQ[GEOGRAPHY_SOURCE].table_ref


def render_sql(
    output_table: str,
    place_table: str,
    geography_table: str,
) -> str:
    """
    Render the coverage-summary SQL with its output and input tables.

    This is a pure function: it reads the SQL template and substitutes the output table
    and the two input tables, performing no input or output of its own so it can be unit
    tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        place_table=place_table,
        geography_table=geography_table,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the FCC coverage summary table in BigQuery.

    The output and input tables are resolved from configuration and the source registry,
    the SQL is rendered, and — unless this is a dry run — the query is executed with the
    supplied client. When no client is given and this is not a dry run, an authenticated
    client is created.
    """
    output_table = output_table_ref()
    sql = render_sql(
        output_table=output_table,
        place_table=place_table_ref(),
        geography_table=geography_table_ref(),
    )

    logger.info(f"Place table:      {place_table_ref()}")
    logger.info(f"Geography table:  {geography_table_ref()}")
    logger.info(f"Output table:     {output_table}")

    if dry_run:
        logger.info("Dry run — rendered SQL:")
        print(sql)
        return

    if client is None:
        client = get_bq_client()
    logger.info("Executing query...")
    client.query(sql).result()
    logger.info(f"Done. Table {output_table} created/replaced.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build the FCC fixed-broadband coverage summary table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
