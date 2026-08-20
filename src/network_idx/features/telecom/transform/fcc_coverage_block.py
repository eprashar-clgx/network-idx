"""
Telecom transform: FCC coverage block interpolation from BigQuery.

This transform is the dasymetric interpolation that turns place-level and county-residual
coverage into block-level coverage. Every Census block inherits coverage percentages from
one source and receives an estimated FCC unit count: a block inside a Census place inherits
that place's percentages and a housing-unit share of the place's units, while a block
outside every place inherits its county's non-place residual percentages and a housing-unit
share of the residual units. Percentages are inherited verbatim; Census housing units only
distribute each source's unit total across its blocks, which conserves the source totals in
FCC-unit space because the percentage is constant within a place or county residual. Blocks
that end up with zero estimated units get null percentages.

It reads the coverage summary, the county residuals, the block-assignment crosswalk, and
the address-count housing units from BigQuery, and writes one row per block. Because it is
a deterministic reshape it lives in the transform layer. The repetitive per-tier SQL and
the state FIPS-to-USPS lookup are generated here from the shared contracts so the output
columns cannot drift from the schema; the table names are the only free parameters. The
SQL is rendered separately from execution so it can be inspected without a client, and the
client is injected so production supplies a real client while tests supply a fake one.
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
    BQ_TABLE_FCC_COVERAGE_BLOCK,
)
from network_idx.constants import (
    FCC_FIXED_COVERAGE_TECHNOLOGIES,
    FCC_COVERAGE_TIER_METRICS,
    STATE_USPS_TO_FIPS,
)
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "fcc_coverage_block.sql"

# Logical names in the source registry for the two Census block reference tables.
BAF_SOURCE = "census_baf_block"
ACL_SOURCE = "census_acl_block"

# The technology-and-tier metric columns, in the same order the schema contract builds them.
PCT_COLS = [
    f"{tech.lower()}_{metric}"
    for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES
    for metric in FCC_COVERAGE_TIER_METRICS
]


def summary_table_ref() -> str:
    """Return the fully qualified coverage summary table this transform reads."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_SUMMARY}"


def residuals_table_ref() -> str:
    """Return the fully qualified county-residuals table this transform reads."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_COUNTY_RESIDUALS}"


def baf_table_ref() -> str:
    """Return the fully qualified block-assignment crosswalk table from the registry."""
    return RAW_SOURCES_BQ[BAF_SOURCE].table_ref


def acl_table_ref() -> str:
    """Return the fully qualified address-count housing-unit table from the registry."""
    return RAW_SOURCES_BQ[ACL_SOURCE].table_ref


def output_table_ref() -> str:
    """Return the fully qualified block coverage table this transform writes to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK}"


def _state_map_values() -> str:
    """Render the state FIPS-to-USPS lookup as UNNEST struct literals from the contract."""
    return ",\n    ".join(
        f"STRUCT('{fips}' AS state_fips, '{usps}' AS state_usps)"
        for usps, fips in STATE_USPS_TO_FIPS.items()
    )


def _metric_sql_blocks() -> dict[str, str]:
    """
    Build the repetitive per-metric SQL fragments from the tier-metric contract.

    For every technology-and-tier column this produces the pass-through column lists for
    the place and residual sources, the inherited-percentage expressions that pick the
    place value for place blocks and the residual value otherwise, and the final
    expressions that keep the inherited percentage (defaulting a missing value to zero)
    only where the block has estimated units and null it out otherwise.
    """
    place_pct_cols = ",\n    ".join(PCT_COLS)
    resid_pct_cols = ",\n    ".join(PCT_COLS)
    inherited_pct_exprs = ",\n    ".join(
        f"IF(h.place_geoid IS NOT NULL, ps.{col}, rs.{col}) AS {col}" for col in PCT_COLS
    )
    final_pct_exprs = ",\n  ".join(
        f"IF(f.estimated_fcc_units = 0, NULL, COALESCE(f.{col}, 0)) AS {col}" for col in PCT_COLS
    )
    return {
        "place_pct_cols": place_pct_cols,
        "resid_pct_cols": resid_pct_cols,
        "inherited_pct_exprs": inherited_pct_exprs,
        "final_pct_exprs": final_pct_exprs,
    }


def render_sql(
    output_table: str,
    summary_table: str,
    residuals_table: str,
    baf_table: str,
    acl_table: str,
) -> str:
    """
    Render the block-interpolation SQL with its tables and generated blocks.

    This is a pure function: it reads the SQL template, generates the state lookup and the
    per-metric fragments from the shared contracts, and substitutes them together with the
    table names, performing no input or output of its own so it can be unit tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        summary_table=summary_table,
        residuals_table=residuals_table,
        baf_table=baf_table,
        acl_table=acl_table,
        state_map_values=_state_map_values(),
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
    Build the FCC coverage block table in BigQuery.

    The tables are resolved from configuration and the source registry, the SQL is
    rendered, and — unless this is a dry run — the query is executed with the supplied
    client. When no client is given and this is not a dry run, an authenticated client is
    created.
    """
    output_table = output_table_ref()
    sql = render_sql(
        output_table=output_table,
        summary_table=summary_table_ref(),
        residuals_table=residuals_table_ref(),
        baf_table=baf_table_ref(),
        acl_table=acl_table_ref(),
    )

    logger.info(f"Summary table:   {summary_table_ref()}")
    logger.info(f"Residuals table: {residuals_table_ref()}")
    logger.info(f"BAF table:       {baf_table_ref()}")
    logger.info(f"ACL table:       {acl_table_ref()}")
    logger.info(f"Output table:    {output_table}")

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
        description="Build the FCC coverage block table (dasymetric interpolation) in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
