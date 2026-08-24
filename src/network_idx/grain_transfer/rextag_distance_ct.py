"""
Grain transfer: aggregate parcel-level rextag fiber distance up to census tract.

This module builds ``rextag_distance_ct``, the tract-grain fiber-distance table the
modeling frame consumes. It is the authoritative port of the ``create_fiber_agg_ct``
stored procedure: it spatially joins each parcel centroid (from the growth-counts parcel
table) to a census tract, left-joins the rextag distance parcel table, and aggregates to
tract the count of parcels and the mean and median of both the distance to the nearest
fiber and the radius fiber count.

The aggregation is bespoke — a spatial join followed by aggregation across two parcel
tables — so it is kept as SQL next to this runner rather than generated from a promotion
spec. The SQL rendering is separated from execution so the query can be inspected without
a BigQuery client, and the client is injected so production supplies a real client while
tests supply a fake one.

NOTE: the source procedure reads ``dist_to_nearest_fiber_m`` and ``radius_fiber_count``
from the rextag distance parcel table, while the rearchitected rextag feature currently
emits the distance as ``dist_to_nearest_fiber_miles``. That column name and unit must be
reconciled with this promotion before the pipeline runs end to end.
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
    BQ_TABLE_REXTAG_DISTANCE_CT,
)
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "rextag_distance_ct.sql"

# The logical name of the tract-boundary source used for the parcel-to-tract spatial join.
SOURCE_TRACT_BOUNDARY = "tract_geometry"


def _feature_table_ref(table: str) -> str:
    """Return the fully qualified reference for a table in the features dataset."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{table}"


def output_table_ref() -> str:
    """Return the fully qualified BigQuery table this promotion writes to."""
    return _feature_table_ref(BQ_TABLE_REXTAG_DISTANCE_CT)


def render_sql(
    output_table: str,
    growth_counts_parcel: str,
    rextag_distance_parcel: str,
    tract_boundary: str,
) -> str:
    """
    Render the rextag distance tract-aggregation SQL with its source and output tables.

    This is a pure function: it reads the SQL template and substitutes the growth-counts
    parcel table (which supplies the parcel centroids), the rextag distance parcel table,
    the tract-boundary table, and the output table, performing no input or output of its
    own so it can be unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        growth_counts_parcel=growth_counts_parcel,
        rextag_distance_parcel=rextag_distance_parcel,
        tract_boundary=tract_boundary,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the tract-level rextag fiber-distance table in BigQuery.

    The output table is resolved from configuration, the parcel inputs from the features
    dataset, and the tract boundary from the source registry; the SQL is rendered, and —
    unless this is a dry run — the query is executed with the supplied client. When no
    client is given and this is not a dry run, an authenticated client is created.
    """
    output_table = output_table_ref()
    sql = render_sql(
        output_table=output_table,
        growth_counts_parcel=_feature_table_ref(BQ_TABLE_PARCEL_GROWTH),
        rextag_distance_parcel=_feature_table_ref(BQ_TABLE_REXTAG_DISTANCE_PARCEL),
        tract_boundary=RAW_SOURCES_BQ[SOURCE_TRACT_BOUNDARY].table_ref,
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
        description="Aggregate parcel-level rextag fiber distance up to census tract in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
