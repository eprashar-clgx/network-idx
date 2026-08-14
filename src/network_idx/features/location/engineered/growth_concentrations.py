"""
Location engineered feature: growth-hotspot concentrations at H3 resolution 7.

This feature aggregates the parcel-level growth signals produced by the
growth-counts feature into H3 resolution-7 cells and keeps only the cells that
qualify as growth "hotspots": those with enough total growth signals (volume) and
enough signals outside their single largest category (variety). The resulting
hotspot cells are the reference geography that the hotspot-distance feature measures
each parcel against.

The H3 resolution and the two hotspot thresholds are analytical choices made during
exploratory analysis, which is why this lives in the engineered layer. The SQL is
kept in growth_concentrations.sql next to this module, the rendering of the SQL is
separated from its execution so the query can be inspected without a BigQuery client,
and the client is injected into the build step so production supplies a real client
while tests supply a fake one.
"""
import argparse
import logging
from pathlib import Path

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_PARCEL_GROWTH,
    BQ_TABLE_HOTSPOT_CONCENTRATIONS_H3,
    BQ_PROJECT_CARTO,
)
from network_idx.constants import (
    GROWTH_HOTSPOT_H3_RES,
    GROWTH_HOTSPOT_TOTAL_FLAGS_THRESHOLD,
    GROWTH_HOTSPOT_VARIETY_THRESHOLD,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "growth_concentrations.sql"


def _features_table_ref(table: str) -> str:
    """Return a fully qualified reference to a table in the features dataset."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{table}"


def output_table_ref() -> str:
    """Return the fully qualified BigQuery table this feature writes to."""
    return _features_table_ref(BQ_TABLE_HOTSPOT_CONCENTRATIONS_H3)


def input_table_ref() -> str:
    """Return the parcel growth-counts table this feature reads from."""
    return _features_table_ref(BQ_TABLE_PARCEL_GROWTH)


def render_sql(
    output_table: str,
    input_table: str,
    carto_project: str = BQ_PROJECT_CARTO,
    hotspot_h3_res: int = GROWTH_HOTSPOT_H3_RES,
    total_flags_threshold: int = GROWTH_HOTSPOT_TOTAL_FLAGS_THRESHOLD,
    variety_threshold: int = GROWTH_HOTSPOT_VARIETY_THRESHOLD,
) -> str:
    """
    Render the growth-concentrations SQL with its tables and parameter values.

    This is a pure function: it reads the SQL template and substitutes the input and
    output tables, the Carto project that hosts the H3 helpers, the H3 resolution, and
    the volume and variety thresholds, performing no input or output of its own so it
    can be unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        input_table=input_table,
        carto_project=carto_project,
        hotspot_h3_res=hotspot_h3_res,
        total_flags_threshold=total_flags_threshold,
        variety_threshold=variety_threshold,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the H3 growth-hotspot concentrations table in BigQuery.

    The input and output tables are resolved from configuration and the parameters
    from the location constants; the SQL is rendered, and — unless this is a dry run —
    the query is executed with the supplied client. When no client is given and this
    is not a dry run, an authenticated client is created.
    """
    output_table = output_table_ref()
    input_table = input_table_ref()
    sql = render_sql(output_table=output_table, input_table=input_table)

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
        description="Create the H3 growth-hotspot concentrations table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
