"""
Monitoring gate: per-source unit conservation for the FCC coverage block table.

The dasymetric interpolation spreads each source's FCC unit total across its blocks in
proportion to Census housing units. Because the spread only redistributes a total it must
never create or destroy units: every Census place's blocks must sum back to that place's
FCC unit total, and every county's non-place blocks must sum back to that county's residual
unit total. This module checks exactly that and is meant to run as a hard gate — a breach
means the interpolation corrupted the totals, so the run should halt rather than publish a
table that silently invented or lost coverage.

The check is deliberately narrow. It verifies unit conservation, which is exact by
construction (the percentage is constant within a source, so the housing-unit spread
cancels and the block estimates sum back to the source total up to per-block rounding). It
does not attempt to reconstruct county coverage percentages from blocks, because the county
residual is allocated to places by block count while the block interpolation spreads by
housing units — those two shares differ for places that straddle counties, so a
percentage-level reconstruction carries a real cross-county bias rather than mere rounding
and is a soft coherence signal, not a halt gate.

Sources with no housing units to spread over are inherently lossy — units cannot be placed
where there are no blocks with housing — so they are reported separately and excluded from
the pass or fail decision. For every other source the tolerance is the exact worst-case
rounding bound of half a unit per block: any discrepancy larger than that cannot come from
rounding and therefore signals a genuine algebra or data error.

The two aggregation queries are rendered from the table names and executed through an
injected client so the rendering and the comparison stay pure and unit testable offline,
while production supplies a real BigQuery client.
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
    BQ_TABLE_FCC_COVERAGE_COUNTY_RESIDUALS,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Half a unit per block is the exact worst case for rounding each block estimate to an
# integer, so a source that drifts by more than this cannot be explained by rounding.
DEFAULT_TOLERANCE_PER_BLOCK = 0.5


class ConservationError(RuntimeError):
    """Raised when the block table fails per-source unit conservation and the gate halts."""


def summary_table_ref() -> str:
    """Return the fully qualified coverage summary table the place check reads."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_SUMMARY}"


def residuals_table_ref() -> str:
    """Return the fully qualified county-residuals table the residual check reads."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_COUNTY_RESIDUALS}"


def block_table_ref() -> str:
    """Return the fully qualified block coverage table this gate audits."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FCC_COVERAGE}.{BQ_TABLE_FCC_COVERAGE_BLOCK}"


def render_place_conservation_sql(block_table: str, summary_table: str) -> str:
    """
    Render the query that sums place-source block estimates back to their place totals.

    Every block that inherited a Census place is aggregated to its place, counting blocks,
    housing units, and reconstructed FCC units, and joined to the place's FCC unit total
    from the coverage summary. This is a pure function that only builds the query text.
    """
    return f"""
WITH block_agg AS (
  SELECT
    place_geoid,
    COUNT(*) AS n_blocks,
    SUM(census_housing_units) AS housing_units,
    SUM(estimated_fcc_units) AS reconstructed_units
  FROM `{block_table}`
  WHERE source = 'place'
  GROUP BY place_geoid
)
SELECT
  'place' AS source_kind,
  a.place_geoid AS source_id,
  s.total_units AS source_units,
  a.reconstructed_units,
  a.housing_units,
  a.n_blocks
FROM block_agg a
JOIN `{summary_table}` s
  ON s.geography_id = a.place_geoid AND s.geography_level = 'place'
""".strip()


def render_residual_conservation_sql(block_table: str, residuals_table: str) -> str:
    """
    Render the query that sums residual-source block estimates back to residual totals.

    Every block that inherited its county's non-place residual is aggregated to its county,
    counting blocks, housing units, and reconstructed FCC units, and joined to the county's
    residual unit total. This is a pure function that only builds the query text.
    """
    return f"""
WITH block_agg AS (
  SELECT
    county_geoid,
    COUNT(*) AS n_blocks,
    SUM(census_housing_units) AS housing_units,
    SUM(estimated_fcc_units) AS reconstructed_units
  FROM `{block_table}`
  WHERE source = 'county_residual'
  GROUP BY county_geoid
)
SELECT
  'county_residual' AS source_kind,
  a.county_geoid AS source_id,
  r.residual_units AS source_units,
  a.reconstructed_units,
  a.housing_units,
  a.n_blocks
FROM block_agg a
JOIN `{residuals_table}` r USING (county_geoid)
""".strip()


@dataclass
class ConservationResult:
    """The outcome of the per-source unit conservation check."""

    passed: bool
    n_sources_checked: int = 0
    n_lossy_zero_hu: int = 0
    lossy_units: int = 0
    max_abs_diff: float = 0.0
    breaches: list[dict] = field(default_factory=list)


def compare_conservation(
    place_df: pd.DataFrame,
    residual_df: pd.DataFrame,
    tolerance_per_block: float = DEFAULT_TOLERANCE_PER_BLOCK,
) -> ConservationResult:
    """
    Decide whether every source's blocks conserve its FCC unit total within rounding.

    The place and residual aggregates are combined; sources with no housing units cannot be
    spread over and are counted as inherently lossy and set aside rather than failed. For
    every remaining source the reconstructed units must match the source total to within
    half a unit per block, the exact worst case for integer rounding; any larger gap is
    recorded as a breach. The check passes only when no non-lossy source breaches, so a
    caller can treat a failure as a halt condition. This function performs no input or
    output and is pure so it can be unit tested offline.
    """
    combined = pd.concat([place_df, residual_df], ignore_index=True)

    lossy = combined["housing_units"].fillna(0) == 0
    lossy_units = int(combined.loc[lossy, "source_units"].fillna(0).sum())
    checked = combined.loc[~lossy].copy()

    checked["abs_diff"] = (
        checked["reconstructed_units"].fillna(0) - checked["source_units"].fillna(0)
    ).abs()
    checked["allowed"] = tolerance_per_block * checked["n_blocks"]

    breached = checked.loc[checked["abs_diff"] > checked["allowed"]]
    breaches = [
        {
            "source_kind": row.source_kind,
            "source_id": row.source_id,
            "source_units": int(row.source_units) if pd.notna(row.source_units) else None,
            "reconstructed_units": int(row.reconstructed_units)
            if pd.notna(row.reconstructed_units)
            else None,
            "abs_diff": float(row.abs_diff),
            "allowed": float(row.allowed),
        }
        for row in breached.sort_values("abs_diff", ascending=False).itertuples(index=False)
    ]

    return ConservationResult(
        passed=len(breaches) == 0,
        n_sources_checked=int(len(checked)),
        n_lossy_zero_hu=int(lossy.sum()),
        lossy_units=lossy_units,
        max_abs_diff=float(checked["abs_diff"].max()) if len(checked) else 0.0,
        breaches=breaches,
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


def check(client=None, halt: bool = True) -> ConservationResult:
    """
    Run the conservation gate against the block table and optionally halt on breach.

    The two aggregation queries are rendered and read from BigQuery, the reconstructed
    source totals are compared to the FCC place and county-residual totals, and the outcome
    is logged. When the check fails and halting is requested a ConservationError is raised so
    the pipeline stops before publishing a corrupted table; when halting is disabled the
    result is returned for a caller to inspect. A client is created if one is not supplied.
    """
    if client is None:
        client = get_bq_client()

    place_df = _read_df(client, render_place_conservation_sql(block_table_ref(), summary_table_ref()))
    residual_df = _read_df(
        client, render_residual_conservation_sql(block_table_ref(), residuals_table_ref())
    )

    result = compare_conservation(place_df, residual_df)

    logger.info(
        f"Conservation {'PASSED' if result.passed else 'FAILED'}: "
        f"{result.n_sources_checked:,} sources checked, "
        f"{result.n_lossy_zero_hu:,} lossy zero-housing sources set aside "
        f"({result.lossy_units:,} unspreadable units), max abs diff {result.max_abs_diff:g}."
    )
    for b in result.breaches[:10]:
        logger.warning(
            f"  breach {b['source_kind']} {b['source_id']}: "
            f"source={b['source_units']} reconstructed={b['reconstructed_units']} "
            f"diff={b['abs_diff']:g} > allowed={b['allowed']:g}"
        )

    if not result.passed and halt:
        raise ConservationError(
            f"FCC coverage block table failed unit conservation: "
            f"{len(result.breaches)} source(s) breached the rounding budget."
        )
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check per-source unit conservation of the FCC coverage block table."
    )
    parser.add_argument(
        "--no-halt", action="store_true", default=False,
        help="Report breaches without raising, instead of halting the pipeline.",
    )
    args = parser.parse_args()
    check(halt=not args.no_halt)
