"""
Location engineered feature: parcel-level growth-signal counts.

This feature builds the base parcel table for the location family. It reads the
in-house property-pipeline views from the source registry (the parcel universe, the
clip-to-parcel map, the growth indicators, current land use, and parcel lineage
events), flags each parcel that shows a growth signal, appends an H3 index and the
census-block id, and counts flagged parcels within a quarter-mile radius of every
parcel. The output is the four quarter-mile growth counts the model consumes
(builder/developer, land-use change, new permit, pre-early-development) plus a
total growth-parcel count, at the parcel grain.

The growth definitions, the quarter-mile radius, and the H3 resolution are analytical
choices made during exploratory analysis, which is why this lives in the engineered
layer. The SQL is kept in growth_counts.sql next to this module, the rendering of the
SQL is separated from its execution so the query can be inspected without a BigQuery
client, and the client is injected into the build step so production supplies a real
client while tests supply a fake one.
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
    BQ_PROJECT_CARTO,
)
from network_idx.constants import GROWTH_COUNT_RADIUS_M, GROWTH_PARCEL_H3_RES
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "growth_counts.sql"

# The logical names of the raw property-pipeline sources this feature reads.
SOURCE_PARCEL_UNIVERSE = "parcel_universe"
SOURCE_CLIP_TO_PARCEL = "clip_to_parcel"
SOURCE_GROWTH_INDICATORS = "growth_indicators"
SOURCE_PROPERTY = "property"
SOURCE_PARCEL_LINEAGE_EVENT = "parcel_lineage_event"
SOURCE_BLOCK_GEOMETRY = "block_geometry"


def output_table_ref() -> str:
    """Return the fully qualified BigQuery table this feature writes to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_GROWTH}"


def _source_ref(name: str) -> str:
    """Return the fully qualified table reference for a registry source name."""
    return RAW_SOURCES_BQ[name].table_ref


def render_sql(
    output_table: str,
    parcel_universe: str,
    clip_to_parcel: str,
    growth_indicators: str,
    property_table: str,
    parcel_lineage_event: str,
    block_geometry: str,
    carto_project: str = BQ_PROJECT_CARTO,
    radius_m: float = GROWTH_COUNT_RADIUS_M,
    parcel_h3_res: int = GROWTH_PARCEL_H3_RES,
) -> str:
    """
    Render the growth-counts SQL with its source, output, and parameter values.

    This is a pure function: it reads the SQL template and substitutes the six source
    tables, the output table, the Carto project that hosts the H3 helpers, the
    quarter-mile search radius, and the H3 resolution, performing no input or output
    of its own so it can be unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        parcel_universe=parcel_universe,
        clip_to_parcel=clip_to_parcel,
        growth_indicators=growth_indicators,
        property=property_table,
        parcel_lineage_event=parcel_lineage_event,
        block_geometry=block_geometry,
        carto_project=carto_project,
        radius_m=radius_m,
        parcel_h3_res=parcel_h3_res,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the parcel-level growth-counts table in BigQuery.

    The output table is resolved from configuration, the six input tables from the
    source registry, and the parameters from the location constants; the SQL is
    rendered, and — unless this is a dry run — the query is executed with the supplied
    client. When no client is given and this is not a dry run, an authenticated client
    is created.
    """
    output_table = output_table_ref()
    sql = render_sql(
        output_table=output_table,
        parcel_universe=_source_ref(SOURCE_PARCEL_UNIVERSE),
        clip_to_parcel=_source_ref(SOURCE_CLIP_TO_PARCEL),
        growth_indicators=_source_ref(SOURCE_GROWTH_INDICATORS),
        property_table=_source_ref(SOURCE_PROPERTY),
        parcel_lineage_event=_source_ref(SOURCE_PARCEL_LINEAGE_EVENT),
        block_geometry=_source_ref(SOURCE_BLOCK_GEOMETRY),
    )

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
        description="Create the parcel-level growth-counts table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
