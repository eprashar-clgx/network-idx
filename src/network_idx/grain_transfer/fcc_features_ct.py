"""
Grain transfer: the four engineered telecom (FCC) features at census-tract grain.

This module builds ``telecom_features_ct``, the tract-grain analogue of
``telecom_features_block``. The model is fit at tract grain but scores parcels, so the
training frame needs the four telecom features aggregated to tract. Those features are
non-linear (a serviceability gate, ratios against housing units, a hand-ordered provider
landscape ladder), so they cannot be rolled up from the block feature table — they are
re-derived at tract from the same transform-layer inputs (ADR-0007).

The tract ``joined`` prelude (``fcc_features_ct.sql``) sums housing and estimated FCC
units and takes an FCC-unit-weighted mean of the top-tier fiber coverage from the FCC
coverage block table, and reads distinct provider and location counts per technology
straight from the raw FCC provider tables grouped to tract. That prelude is then handed to
the shared engineered-telecom renderer — the identical definition the block feature uses —
so the block-grain and tract-grain telecom features can never drift.

Following the codebase convention, SQL rendering is separated from execution so the query
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
    BQ_TABLE_TELECOM_FEATURES_CT,
    BQ_DATASET_FCC_COVERAGE,
    BQ_TABLE_FCC_COVERAGE_BLOCK,
)
from network_idx.features.telecom.engineered._engineered_sql import (
    render_engineered_telecom_sql,
)
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "fcc_features_ct.sql"

# Raw per-technology FCC provider tables (source registry keys). Distinct provider and
# location counts are read from these and grouped to tract.
COPPER_SOURCE = "fcc_copper"
CABLE_SOURCE = "fcc_cable"
FIBER_SOURCE = "fcc_fiber"

# Passthrough identifier columns carried into the tract feature table.
TRACT_KEY_COLUMNS = "tract_geoid, state_fips"

# The tract feature table is clustered by state so downstream by-state reads prune.
TRACT_CLUSTER_CLAUSE = "\nCLUSTER BY state_fips"


def output_table_ref() -> str:
    """Return the fully qualified tract telecom feature table this feature writes to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_TELECOM_FEATURES_CT}"


def coverage_block_table_ref() -> str:
    """Return the fully qualified FCC coverage block table this feature rolls up to tract."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK}"


def render_sql(
    output_table: str,
    coverage_block_table: str,
    copper_table: str,
    cable_table: str,
    fiber_table: str,
) -> str:
    """
    Render the tract telecom-feature SQL from the shared engineered definition.

    This is a pure function: it reads the tract-grain ``joined`` prelude, substitutes the
    coverage block table and the three raw provider tables, and hands the prelude to the
    shared engineered-telecom renderer with the tract passthrough columns and clustering.
    Keeping the four feature definitions in the shared renderer means the block and tract
    telecom features cannot drift (ADR-0007). It performs no input or output of its own so
    it can be unit tested.
    """
    joined_prelude = SQL_PATH.read_text().format(
        coverage_block_table=coverage_block_table,
        copper_table=copper_table,
        cable_table=cable_table,
        fiber_table=fiber_table,
    )
    return render_engineered_telecom_sql(
        output_table=output_table,
        joined_prelude=joined_prelude,
        key_columns=TRACT_KEY_COLUMNS,
        cluster_clause=TRACT_CLUSTER_CLAUSE,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the tract-level telecom feature table in BigQuery.

    The output resolves from configuration, the coverage block from the FCC coverage
    dataset, and the three raw provider tables from the source registry; the SQL is
    rendered, and — unless this is a dry run — executed with the supplied client. When no
    client is given and this is not a dry run, an authenticated client is created.
    """
    output_table = output_table_ref()
    coverage_block_table = coverage_block_table_ref()
    copper_table = RAW_SOURCES_BQ[COPPER_SOURCE].table_ref
    cable_table = RAW_SOURCES_BQ[CABLE_SOURCE].table_ref
    fiber_table = RAW_SOURCES_BQ[FIBER_SOURCE].table_ref
    sql = render_sql(
        output_table=output_table,
        coverage_block_table=coverage_block_table,
        copper_table=copper_table,
        cable_table=cable_table,
        fiber_table=fiber_table,
    )

    logger.info(f"Coverage block table: {coverage_block_table}")
    logger.info(f"Copper table:         {copper_table}")
    logger.info(f"Cable table:          {cable_table}")
    logger.info(f"Fiber table:          {fiber_table}")
    logger.info(f"Output table:         {output_table}")

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
        description="Build the tract-level telecom (FCC) feature table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
