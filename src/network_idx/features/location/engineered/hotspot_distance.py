"""
Location engineered feature: distance from each parcel to the nearest growth hotspot.

This feature measures, for every parcel, the distance to the closest growth-hotspot
cell within a maximum search distance, reported in miles; a distance of zero means
the parcel sits inside a hotspot cell. It reads the parcel growth-counts table and
the H3 growth-hotspot concentrations table and produces the model feature
`dist_to_nearest_hotspot_miles` at the parcel grain.

The maximum search distance is an analytical choice made during exploratory analysis,
which is why this lives in the engineered layer. Note that the underlying colleague
procedure named this output column with a metres suffix even though it already divides
by the metres-per-mile constant; the column is genuinely in miles, so this feature
names it `dist_to_nearest_hotspot_miles` to match the scoring contract. The SQL is
kept in hotspot_distance.sql next to this module, the rendering of the SQL is
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
    BQ_TABLE_HOTSPOT_DISTANCE_PARCEL,
)
from network_idx.constants import HOTSPOT_MAX_SEARCH_DIST_M, METERS_PER_MILE
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "hotspot_distance.sql"


def _features_table_ref(table: str) -> str:
    """Return a fully qualified reference to a table in the features dataset."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{table}"


def output_table_ref() -> str:
    """Return the fully qualified BigQuery table this feature writes to."""
    return _features_table_ref(BQ_TABLE_HOTSPOT_DISTANCE_PARCEL)


def parcel_table_ref() -> str:
    """Return the parcel growth-counts table this feature reads parcels from."""
    return _features_table_ref(BQ_TABLE_PARCEL_GROWTH)


def concentrations_table_ref() -> str:
    """Return the H3 growth-hotspot concentrations table this feature measures against."""
    return _features_table_ref(BQ_TABLE_HOTSPOT_CONCENTRATIONS_H3)


def render_sql(
    output_table: str,
    parcel_table: str,
    concentrations_table: str,
    max_dist_threshold: int = HOTSPOT_MAX_SEARCH_DIST_M,
    meters_per_mile: float = METERS_PER_MILE,
) -> str:
    """
    Render the hotspot-distance SQL with its tables and parameter values.

    This is a pure function: it reads the SQL template and substitutes the parcel,
    concentrations, and output tables, the maximum search distance, and the
    metres-per-mile conversion, performing no input or output of its own so it can be
    unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        parcel_table=parcel_table,
        concentrations_table=concentrations_table,
        max_dist_threshold=max_dist_threshold,
        meters_per_mile=meters_per_mile,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the parcel-to-nearest-hotspot distance table in BigQuery.

    The parcel, concentrations, and output tables are resolved from configuration and
    the parameters from the location constants; the SQL is rendered, and — unless this
    is a dry run — the query is executed with the supplied client. When no client is
    given and this is not a dry run, an authenticated client is created.
    """
    output_table = output_table_ref()
    parcel_table = parcel_table_ref()
    concentrations_table = concentrations_table_ref()
    sql = render_sql(
        output_table=output_table,
        parcel_table=parcel_table,
        concentrations_table=concentrations_table,
    )

    logger.info(f"Parcel table:        {parcel_table}")
    logger.info(f"Concentrations table: {concentrations_table}")
    logger.info(f"Output table:        {output_table}")

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
        description="Create the parcel-to-nearest-hotspot distance table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
