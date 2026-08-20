"""
Parcel feature assembly: join every family's output into the parcel-grain scoring input.

This module builds the single wide table the scorer reads — one row per parcel carrying the
thirteen model features — by joining the per-family feature tables across their three grains.
The parcel growth-counts table is the spine (one row per parcel); the rextag and hotspot
distance tables join at parcel grain, the telecom feature table and its block housing-unit
count join at block grain on the parcel's block GEOID, and the population-change table joins
at tract grain on the first eleven digits of that GEOID. Every join is a left join so no
parcel is dropped when a family lacks a row for it.

Null fills are intentionally left to the scaling step rather than applied here, so this table
preserves genuine missingness for the scorer's contract to handle. Because the assembly is a
deterministic join with no analytical choices of its own it sits at the top of the features
package rather than inside any one family. The SQL is rendered separately from execution so it
can be inspected without a client, and the client is injected so production supplies a real
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
    BQ_TABLE_REXTAG_DISTANCE_PARCEL,
    BQ_TABLE_HOTSPOT_DISTANCE_PARCEL,
    BQ_TABLE_TELECOM_FEATURES_BLOCK,
    BQ_TABLE_DEMO_POP_TRACT,
    BQ_TABLE_PARCEL_FEATURES,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "parcel_features.sql"


def _table_ref(table: str) -> str:
    """Return a fully qualified table reference in the features dataset."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{table}"


def output_table_ref() -> str:
    """Return the fully qualified parcel feature table this assembly writes to."""
    return _table_ref(BQ_TABLE_PARCEL_FEATURES)


def parcel_growth_table_ref() -> str:
    """Return the parcel growth-counts table used as the join spine."""
    return _table_ref(BQ_TABLE_PARCEL_GROWTH)


def rextag_distance_table_ref() -> str:
    """Return the parcel-grain rextag fiber-distance table."""
    return _table_ref(BQ_TABLE_REXTAG_DISTANCE_PARCEL)


def hotspot_distance_table_ref() -> str:
    """Return the parcel-grain growth-hotspot distance table."""
    return _table_ref(BQ_TABLE_HOTSPOT_DISTANCE_PARCEL)


def telecom_block_table_ref() -> str:
    """Return the block-grain telecom feature table."""
    return _table_ref(BQ_TABLE_TELECOM_FEATURES_BLOCK)


def demo_tract_table_ref() -> str:
    """Return the tract-grain population-change table."""
    return _table_ref(BQ_TABLE_DEMO_POP_TRACT)


def render_sql(
    output_table: str,
    parcel_growth_table: str,
    rextag_distance_table: str,
    hotspot_distance_table: str,
    telecom_block_table: str,
    demo_tract_table: str,
) -> str:
    """
    Render the parcel assembly SQL with its output and five input tables.

    This is a pure function: it reads the SQL template and substitutes the table names,
    performing no input or output of its own so it can be unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        parcel_growth_table=parcel_growth_table,
        rextag_distance_table=rextag_distance_table,
        hotspot_distance_table=hotspot_distance_table,
        telecom_block_table=telecom_block_table,
        demo_tract_table=demo_tract_table,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the parcel-grain feature table in BigQuery.

    The output and the five input tables are resolved from configuration, the SQL is
    rendered, and — unless this is a dry run — the query is executed with the supplied
    client. When no client is given and this is not a dry run, an authenticated client is
    created.
    """
    output_table = output_table_ref()
    sql = render_sql(
        output_table=output_table,
        parcel_growth_table=parcel_growth_table_ref(),
        rextag_distance_table=rextag_distance_table_ref(),
        hotspot_distance_table=hotspot_distance_table_ref(),
        telecom_block_table=telecom_block_table_ref(),
        demo_tract_table=demo_tract_table_ref(),
    )

    logger.info(f"Spine (parcel growth): {parcel_growth_table_ref()}")
    logger.info(f"Rextag distance:       {rextag_distance_table_ref()}")
    logger.info(f"Hotspot distance:      {hotspot_distance_table_ref()}")
    logger.info(f"Telecom block:         {telecom_block_table_ref()}")
    logger.info(f"Demo tract:            {demo_tract_table_ref()}")
    logger.info(f"Output table:          {output_table}")

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
        description="Assemble the parcel-grain scoring feature table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
