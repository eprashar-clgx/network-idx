"""
Demographic engineered feature: tract-level population change.

This feature reads the NeighborhoodScout tract table from the source registry and
computes the population-change quantities the model uses at the census-tract grain:
the average annual absolute change and the average annual percentage change in the
ten-mile population estimate since 2022, alongside the one-year change variants. The
"since 2022" window and the annualisation are analytical choices made during
exploratory analysis, which is why this lives in the engineered layer rather than the
transform layer. The SQL is kept in population_change.sql next to this module.

The rendering of the SQL is separated from its execution so that the query can be
built and inspected without a BigQuery client, and the client is injected into the
build step so that production supplies a real client while tests supply a fake one.
"""
import argparse
import logging
from pathlib import Path

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_DEMO_POP_TRACT,
)
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "population_change.sql"

# The logical name of the raw NeighborhoodScout tract source in the registry.
SOURCE_NAME = "neighborhood_scout_tract"


def output_table_ref() -> str:
    """Return the fully qualified BigQuery table this feature writes to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_DEMO_POP_TRACT}"


def input_table_ref() -> str:
    """Return the fully qualified NeighborhoodScout source table from the registry."""
    return RAW_SOURCES_BQ[SOURCE_NAME].table_ref


def render_sql(output_table: str, input_table: str) -> str:
    """
    Render the population-change SQL with its input and output table references.

    This is a pure function: it reads the SQL template and substitutes the table
    names, performing no input or output of its own so it can be unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(output_table=output_table, input_table=input_table)


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the tract-level population-change table in BigQuery.

    The output and input table references are resolved from configuration and the
    source registry, the SQL is rendered, and — unless this is a dry run — the query
    is executed with the supplied client. When no client is given and this is not a
    dry run, an authenticated client is created.
    """
    output_table = output_table_ref()
    input_table = input_table_ref()
    sql = render_sql(output_table, input_table)

    logger.info(f"Input table:  {input_table}")
    logger.info(f"Output table: {output_table}")

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
        description="Create the tract-level population-change table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
