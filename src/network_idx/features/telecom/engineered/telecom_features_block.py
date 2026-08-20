"""
Telecom engineered feature: the four block-level telecom model features.

This feature reads the FCC coverage block table (the spine, carrying every block, its
Census housing units, and the interpolated top-tier fiber coverage) and the FCC speeds
block table (per-technology serviceable location and provider counts), and derives the four
telecom features the parcel model consumes at the block grain: cable penetration, the fiber
opportunity gap, the top-tier fiber speed coverage gated to blocks that actually have
serviceable fiber, and the provider competitive landscape as both a text label and an
ordinal rank. These are analytical decisions taken during exploratory modelling — a
serviceability gate, a neutral-value null fill, and a hand-ordered landscape ladder — so
this lives in the engineered layer rather than the transform layer.

The provider-landscape label-to-ordinal ladder is generated here from the scoring contract
so the SQL ordinal can never drift from the canonical mapping the scorer uses. The SQL is
rendered separately from execution so the query can be inspected without a client, and the
client is injected so production supplies a real client while tests supply a fake one.
"""
import argparse
import logging
from pathlib import Path

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_TELECOM_FEATURES_BLOCK,
    BQ_DATASET_FCC_COVERAGE,
    BQ_TABLE_FCC_COVERAGE_BLOCK,
    BQ_DATASET_FCC_SPEEDS,
    BQ_TABLE_FCC_SPEEDS_BLOCK,
)
from network_idx.constants import PROVIDER_LANDSCAPE_ORDER
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "telecom_features_block.sql"


def output_table_ref() -> str:
    """Return the fully qualified block feature table this feature writes to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_TELECOM_FEATURES_BLOCK}"


def coverage_block_table_ref() -> str:
    """Return the fully qualified FCC coverage block table this feature reads as its spine."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK}"


def speeds_block_table_ref() -> str:
    """Return the fully qualified FCC speeds block table this feature joins for counts."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_SPEEDS}.{BQ_TABLE_FCC_SPEEDS_BLOCK}"


def _landscape_ord_cases() -> str:
    """
    Render the provider-landscape label-to-ordinal WHEN clauses from the scoring contract.

    Generating these from the single canonical ordering guarantees the ordinal written into
    the block feature table matches the mapping the scorer relies on, so the two cannot
    silently drift apart.
    """
    return "\n        ".join(
        f"WHEN '{label}' THEN {rank}" for label, rank in PROVIDER_LANDSCAPE_ORDER.items()
    )


def render_sql(output_table: str, coverage_block_table: str, speeds_block_table: str) -> str:
    """
    Render the telecom block-feature SQL with its tables and the generated ordinal ladder.

    This is a pure function: it reads the SQL template, generates the label-to-ordinal WHEN
    clauses from the scoring contract, and substitutes them together with the table names,
    performing no input or output of its own so it can be unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        coverage_block_table=coverage_block_table,
        speeds_block_table=speeds_block_table,
        landscape_ord_cases=_landscape_ord_cases(),
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the block-level telecom feature table in BigQuery.

    The output and the two input tables are resolved from configuration, the SQL is
    rendered, and — unless this is a dry run — the query is executed with the supplied
    client. When no client is given and this is not a dry run, an authenticated client is
    created.
    """
    output_table = output_table_ref()
    coverage_block_table = coverage_block_table_ref()
    speeds_block_table = speeds_block_table_ref()
    sql = render_sql(output_table, coverage_block_table, speeds_block_table)

    logger.info(f"Coverage block table: {coverage_block_table}")
    logger.info(f"Speeds block table:   {speeds_block_table}")
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
        description="Create the block-level telecom feature table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
