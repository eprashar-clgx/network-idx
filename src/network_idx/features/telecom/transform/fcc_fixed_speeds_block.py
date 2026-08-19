"""
Telecom transform: FCC fixed-speed block aggregation from BigQuery-production.

This transform aggregates the raw FCC Broadband Data Collection fixed-broadband location
records — the three per-technology tables (copper, cable, fiber) in the FCC production
dataset — up to one row per census block. Per technology it produces the count of distinct
serviceable locations and providers and the maximum advertised download and upload speed,
yielding the fifteen-column block speed table the engineered telecom features build on.
Because it is a mechanical group-and-pivot with no analytical choice, it lives in the
transform layer.

It replaces the earlier download-and-parse pipeline (which unzipped per-state CSVs and
aggregated them in pandas) with a single BigQuery statement that reads the authoritative
data straight from production. The three input tables are resolved from the source
registry and the output table from configuration; the SQL is rendered separately from
execution so it can be inspected without a BigQuery client, and the client is injected so
production supplies a real client while tests supply a fake one.
"""
import argparse
import logging
from pathlib import Path

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FCC_SPEEDS,
    BQ_TABLE_FCC_SPEEDS_BLOCK,
)
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "fcc_fixed_speeds_block.sql"

# Logical names of the three per-technology raw FCC sources in the registry.
COPPER_SOURCE = "fcc_copper"
CABLE_SOURCE = "fcc_cable"
FIBER_SOURCE = "fcc_fiber"


def output_table_ref() -> str:
    """Return the fully qualified block speed table this transform writes to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_BLOCK}"


def copper_table_ref() -> str:
    """Return the fully qualified raw copper fixed-broadband table from the registry."""
    return RAW_SOURCES_BQ[COPPER_SOURCE].table_ref


def cable_table_ref() -> str:
    """Return the fully qualified raw cable fixed-broadband table from the registry."""
    return RAW_SOURCES_BQ[CABLE_SOURCE].table_ref


def fiber_table_ref() -> str:
    """Return the fully qualified raw fiber fixed-broadband table from the registry."""
    return RAW_SOURCES_BQ[FIBER_SOURCE].table_ref


def render_sql(
    output_table: str,
    copper_table: str,
    cable_table: str,
    fiber_table: str,
) -> str:
    """
    Render the block-aggregation SQL with its output and input tables.

    This is a pure function: it reads the SQL template and substitutes the output table
    and the three per-technology input tables, performing no input or output of its own
    so it can be unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        copper_table=copper_table,
        cable_table=cable_table,
        fiber_table=fiber_table,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the FCC fixed-speed block table in BigQuery.

    The output and input tables are resolved from configuration and the source registry,
    the SQL is rendered, and — unless this is a dry run — the query is executed with the
    supplied client. When no client is given and this is not a dry run, an authenticated
    client is created.
    """
    output_table = output_table_ref()
    sql = render_sql(
        output_table=output_table,
        copper_table=copper_table_ref(),
        cable_table=cable_table_ref(),
        fiber_table=fiber_table_ref(),
    )

    logger.info(f"Copper table: {copper_table_ref()}")
    logger.info(f"Cable table:  {cable_table_ref()}")
    logger.info(f"Fiber table:  {fiber_table_ref()}")
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
        description="Aggregate FCC fixed-speed records to the block table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
