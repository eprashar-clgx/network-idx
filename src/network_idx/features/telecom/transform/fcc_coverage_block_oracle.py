"""
Telecom parity oracle: pandas cross-check of the SQL coverage block interpolation.

The dasymetric interpolation runs in BigQuery as the production path; this module is its
one-time safety net. It reproduces the same block estimates in pandas by driving the
original reference implementation — the county-residual and block-interpolation functions
from the exploratory feature-engineering code — over the exact same inputs the SQL reads,
for a small sample of states, and writes the result to a separate parity table. A parity
comparison then confirms the two paths agree within a rounding budget; if they do, the SQL
becomes the single source of truth and this oracle is retired.

It is deliberately thin: it reads the coverage summary, the block-assignment crosswalk, and
the address-count housing units from BigQuery, reshapes them into the DataFrames the
reference functions expect, runs those functions per state, and compares against the SQL
block table. The reshaping, per-state orchestration, and comparison are pure functions so
they can be unit tested offline; only the read/write dispatch needs a BigQuery client, which
is injected so tests can supply a fake one.
"""
import argparse
import logging
from dataclasses import dataclass, field

import pandas as pd

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FCC_COVERAGE,
    BQ_TABLE_FCC_COVERAGE_SUMMARY,
    BQ_TABLE_FCC_COVERAGE_BLOCK,
    BQ_TABLE_FCC_COVERAGE_BLOCK_PARITY,
)
from network_idx.constants import (
    FCC_FIXED_COVERAGE_TECHNOLOGIES,
    FCC_COVERAGE_TIER_METRICS,
    FCC_COVERAGE_BLOCK_OUTPUTS,
    STATE_USPS_TO_FIPS,
)
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate
from network_idx.feature_engg.fcc_fixed_summary_county_residuals import (
    compute_residuals,
    _build_county_place_map,
)
from network_idx.feature_engg.fcc_fixed_summary_est_ct_block import estimate_block_coverage

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BAF_SOURCE = "census_baf_block"
ACL_SOURCE = "census_acl_block"

# The technology-and-tier metric columns, in the schema-contract order.
PCT_COLS = [
    f"{tech.lower()}_{metric}"
    for tech in FCC_FIXED_COVERAGE_TECHNOLOGIES
    for metric in FCC_COVERAGE_TIER_METRICS
]

# The block-level values compared between the SQL and pandas paths.
PARITY_VALUE_COLS = ["estimated_fcc_units"] + PCT_COLS

# Default rounding budget: estimated units are integers, so a per-block difference of one
# unit is within the rounding budget; percentages are inherited verbatim so should match
# very tightly.
DEFAULT_UNIT_TOLERANCE = 1.0
DEFAULT_PCT_TOLERANCE = 1e-6


def summary_table_ref() -> str:
    """Return the fully qualified coverage summary table the oracle reads."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_SUMMARY}"


def block_table_ref() -> str:
    """Return the fully qualified SQL block table the oracle is compared against."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK}"


def parity_table_ref() -> str:
    """Return the fully qualified parity table the oracle writes its pandas blocks to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK_PARITY}"


def baf_table_ref() -> str:
    """Return the fully qualified block-assignment crosswalk table from the registry."""
    return RAW_SOURCES_BQ[BAF_SOURCE].table_ref


def acl_table_ref() -> str:
    """Return the fully qualified address-count housing-unit table from the registry."""
    return RAW_SOURCES_BQ[ACL_SOURCE].table_ref


def _fips_in_list(states_usps: list[str]) -> str:
    """Render a comma-separated quoted list of state FIPS for a set of USPS codes."""
    return ", ".join(f"'{STATE_USPS_TO_FIPS[s]}'" for s in states_usps)


def render_place_sql(summary_table: str, states_usps: list[str]) -> str:
    """Render the query that pulls the place rows of the coverage summary for the sample."""
    cols = ",\n    ".join(PCT_COLS)
    return (
        f"SELECT\n    geography_id,\n    total_units,\n    {cols}\n"
        f"FROM `{summary_table}`\n"
        f"WHERE geography_level = 'place'\n"
        f"  AND SUBSTR(geography_id, 1, 2) IN ({_fips_in_list(states_usps)})"
    )


def render_county_sql(summary_table: str, states_usps: list[str]) -> str:
    """Render the query that pulls the county rows of the coverage summary for the sample."""
    cols = ",\n    ".join(PCT_COLS)
    return (
        f"SELECT\n    geography_id,\n    total_units,\n    {cols}\n"
        f"FROM `{summary_table}`\n"
        f"WHERE geography_level = 'county'\n"
        f"  AND SUBSTR(geography_id, 1, 2) IN ({_fips_in_list(states_usps)})"
    )


def render_baf_sql(baf_table: str, states_usps: list[str]) -> str:
    """Render the query that pulls the block-assignment crosswalk for the sample states."""
    return (
        f"SELECT block_geoid, state_fips, county_geoid, tract_geoid, place_geoid\n"
        f"FROM `{baf_table}`\n"
        f"WHERE state_fips IN ({_fips_in_list(states_usps)})"
    )


def render_acl_sql(acl_table: str, states_usps: list[str]) -> str:
    """Render the query that pulls the address-count housing units for the sample states."""
    return (
        f"SELECT block_geoid, total_housing_units\n"
        f"FROM `{acl_table}`\n"
        f"WHERE SUBSTR(block_geoid, 1, 2) IN ({_fips_in_list(states_usps)})"
    )


def build_oracle_blocks(
    place_df: pd.DataFrame,
    county_df: pd.DataFrame,
    baf_df: pd.DataFrame,
    acl_df: pd.DataFrame,
    states_usps: list[str],
) -> pd.DataFrame:
    """
    Reproduce the block coverage estimates in pandas, one state at a time.

    For each state the inputs are sliced to that state, the county-place block-share map is
    built, the county residuals are computed, and the block interpolation is run — all using
    the original reference functions — and the per-state block frames are concatenated. This
    is the pandas counterpart of the SQL block table, driven from the same inputs.
    """
    frames = []
    for usps in states_usps:
        fips = STATE_USPS_TO_FIPS[usps]
        baf_s = baf_df[baf_df["state_fips"] == fips].copy()
        acl_s = acl_df[acl_df["block_geoid"].str[:2] == fips].copy()
        county_s = county_df[county_df["geography_id"].str[:2] == fips].copy()
        place_s = place_df[place_df["geography_id"].str[:2] == fips].copy()

        county_place_map = _build_county_place_map(baf_s)
        residuals = compute_residuals(county_s, place_s, county_place_map)
        blocks = estimate_block_coverage(place_s.copy(), residuals, baf_s, acl_s, usps)
        frames.append(blocks)

    if not frames:
        return pd.DataFrame(columns=FCC_COVERAGE_BLOCK_OUTPUTS)
    return pd.concat(frames, ignore_index=True)


@dataclass
class ParityResult:
    """The outcome of comparing the SQL and pandas block tables."""

    passed: bool
    n_blocks: int
    max_abs_diff: dict[str, float] = field(default_factory=dict)
    null_mismatch: dict[str, int] = field(default_factory=dict)


def compare_parity(
    sql_df: pd.DataFrame,
    oracle_df: pd.DataFrame,
    unit_tolerance: float = DEFAULT_UNIT_TOLERANCE,
    pct_tolerance: float = DEFAULT_PCT_TOLERANCE,
    key: str = "block_geoid",
) -> ParityResult:
    """
    Compare the SQL and pandas block tables block by block within a rounding budget.

    The two frames are joined on the block key; for each compared value the maximum absolute
    difference over blocks where both sides are non-null is measured against its tolerance
    (one unit for the estimated counts, a tiny epsilon for the inherited percentages), and
    the number of blocks where exactly one side is null is counted as a disagreement. Parity
    passes only when every value is within tolerance and no side-specific nulls remain.
    """
    merged = sql_df.merge(oracle_df, on=key, suffixes=("_sql", "_oracle"))

    max_abs_diff: dict[str, float] = {}
    null_mismatch: dict[str, int] = {}
    passed = True

    for col in PARITY_VALUE_COLS:
        left = merged[f"{col}_sql"]
        right = merged[f"{col}_oracle"]
        both = left.notna() & right.notna()
        one = left.notna() ^ right.notna()

        diff = (left[both].astype(float) - right[both].astype(float)).abs()
        col_max = float(diff.max()) if len(diff) else 0.0
        max_abs_diff[col] = col_max

        mismatch = int(one.sum())
        null_mismatch[col] = mismatch

        tol = unit_tolerance if col == "estimated_fcc_units" else pct_tolerance
        if col_max > tol or mismatch > 0:
            passed = False

    return ParityResult(
        passed=passed,
        n_blocks=len(merged),
        max_abs_diff=max_abs_diff,
        null_mismatch=null_mismatch,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def _read_df(client, sql: str) -> pd.DataFrame:
    """Run a query and return the result as a DataFrame."""
    return client.query(sql).to_dataframe()


def run(client=None, states_usps: list[str] | None = None, write: bool = True) -> ParityResult:
    """
    Build the pandas oracle for a sample of states and compare it to the SQL block table.

    The coverage summary, crosswalk, and housing-unit inputs are read from BigQuery for the
    sample states, the pandas block estimates are rebuilt and — when requested — written to
    the parity table, the SQL block table is read for the same states, and the two are
    compared. The parity result is logged and returned.
    """
    if states_usps is None:
        states_usps = ["DE", "RI"]
    if client is None:
        client = get_bq_client()

    place_df = _read_df(client, render_place_sql(summary_table_ref(), states_usps))
    county_df = _read_df(client, render_county_sql(summary_table_ref(), states_usps))
    baf_df = _read_df(client, render_baf_sql(baf_table_ref(), states_usps))
    acl_df = _read_df(client, render_acl_sql(acl_table_ref(), states_usps))

    oracle_df = build_oracle_blocks(place_df, county_df, baf_df, acl_df, states_usps)

    if write:
        client.load_table_from_dataframe(oracle_df, parity_table_ref()).result()
        logger.info(f"Wrote {len(oracle_df):,} oracle blocks to {parity_table_ref()}.")

    sql_df = _read_df(
        client,
        f"SELECT * FROM `{block_table_ref()}` "
        f"WHERE state_fips IN ({_fips_in_list(states_usps)})",
    )

    result = compare_parity(sql_df, oracle_df)
    logger.info(f"Parity {'PASSED' if result.passed else 'FAILED'} over {result.n_blocks:,} blocks.")
    logger.info(f"Max abs diff: {result.max_abs_diff}")
    logger.info(f"Null mismatches: {result.null_mismatch}")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cross-check the SQL coverage block interpolation against the pandas reference."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=["DE", "RI"],
        choices=STATE_USPS_TO_FIPS.keys(), metavar="STATE",
        help="Sample states (USPS codes) to cross-check.",
    )
    parser.add_argument(
        "--no-write", action="store_true", default=False,
        help="Compare only; do not write the pandas oracle to the parity table.",
    )
    args = parser.parse_args()
    run(states_usps=args.states, write=not args.no_write)
