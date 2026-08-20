"""
Telecom transform: FCC coverage county residuals from the coverage summary.

The dasymetric interpolation assigns each Census block either the coverage of the place it
sits in or, if it is outside every place, the coverage left over in its county once the
places are accounted for. This transform computes that county leftover. For each county and
each technology and speed tier it subtracts the coverage carried by the county's places —
each place fractionally allocated by the share of its blocks that fall in the county — from
the county total and re-expresses the remainder as a percentage of the non-place units.

It reads the unified place-and-county coverage summary and the block-assignment crosswalk,
both from BigQuery, and writes one row per county to the county-residuals table. Because it
is a deterministic algebraic reshape with no modelling choice it lives in the transform
layer. The repetitive per-metric SQL (one expression per technology and tier) is generated
here from the shared tier-metric contract so the output columns cannot drift from the
schema; the table names are the only free parameters. The SQL is rendered separately from
execution so it can be inspected without a client, and the client is injected so production
supplies a real client while tests supply a fake one.
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
    BQ_TABLE_FCC_COVERAGE_COUNTY_RESIDUALS,
)
from network_idx.constants import (
    FCC_FIXED_COVERAGE_TECHNOLOGIES,
    FCC_COVERAGE_TIER_METRICS,
)
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "fcc_coverage_county_residuals.sql"

# Logical name in the source registry for the block-assignment crosswalk table.
BAF_SOURCE = "census_baf_block"

# The technology-and-tier metric columns, in the same order the schema contract builds them.
PCT_COLS = [
    f"{tech.lower()}_{metric}"
    for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES
    for metric in FCC_COVERAGE_TIER_METRICS
]


def summary_table_ref() -> str:
    """Return the fully qualified coverage summary table this transform reads."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_SUMMARY}"


def baf_table_ref() -> str:
    """Return the fully qualified block-assignment crosswalk table from the registry."""
    return RAW_SOURCES_BQ[BAF_SOURCE].table_ref


def output_table_ref() -> str:
    """Return the fully qualified county-residuals table this transform writes to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_COUNTY_RESIDUALS}"


def _metric_sql_blocks() -> dict[str, str]:
    """
    Build the repetitive per-metric SQL fragments from the tier-metric contract.

    For every technology-and-tier column this produces the four fragments the template
    needs: the pass-through column lists for the place and county sources, the weighted
    absolute-unit sums that accumulate each place's contribution, the pass-through of the
    county percentage and the aggregated place absolute units, and the final residual
    percentage expression (county absolute minus place absolute, over residual units,
    clamped to the unit interval and null when there are no residual units).
    """
    indent = "\n    "
    place_pct_cols = ",\n    ".join(PCT_COLS)
    county_pct_cols = ",\n    ".join(PCT_COLS)
    place_abs_exprs = ",\n    ".join(
        f"SUM(p.place_total_units * COALESCE(p.{col}, 0) * m.place_share) AS {col}_places_abs"
        for col in PCT_COLS
    )
    joined_metric_cols = ",\n    ".join(
        f"c.{col} AS {col}_county_pct,\n    COALESCE(pa.{col}_places_abs, 0) AS {col}_places_abs"
        for col in PCT_COLS
    )
    residual_case_exprs = ",\n  ".join(
        (
            "CASE WHEN residual_units = 0 THEN NULL ELSE "
            f"LEAST(GREATEST(SAFE_DIVIDE("
            f"county_total_units * COALESCE({col}_county_pct, 0) - {col}_places_abs, "
            f"residual_units), 0), 1) END AS {col}"
        )
        for col in PCT_COLS
    )
    return {
        "place_pct_cols": place_pct_cols,
        "county_pct_cols": county_pct_cols,
        "place_abs_exprs": place_abs_exprs,
        "joined_metric_cols": joined_metric_cols,
        "residual_case_exprs": residual_case_exprs,
    }


def render_sql(
    output_table: str,
    summary_table: str,
    baf_table: str,
) -> str:
    """
    Render the county-residuals SQL with its tables and generated per-metric blocks.

    This is a pure function: it reads the SQL template, generates the repetitive per-metric
    fragments from the tier-metric contract, and substitutes them together with the table
    names, performing no input or output of its own so it can be unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        summary_table=summary_table,
        baf_table=baf_table,
        **_metric_sql_blocks(),
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the FCC coverage county-residuals table in BigQuery.

    The tables are resolved from configuration and the source registry, the SQL is
    rendered, and — unless this is a dry run — the query is executed with the supplied
    client. When no client is given and this is not a dry run, an authenticated client is
    created.
    """
    output_table = output_table_ref()
    sql = render_sql(
        output_table=output_table,
        summary_table=summary_table_ref(),
        baf_table=baf_table_ref(),
    )

    logger.info(f"Summary table: {summary_table_ref()}")
    logger.info(f"BAF table:     {baf_table_ref()}")
    logger.info(f"Output table:  {output_table}")

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
        description="Build the FCC coverage county-residuals table in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
