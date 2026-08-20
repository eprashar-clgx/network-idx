"""
Feature monitoring: per-run value distributions of the telecom block features.

This is the feature-monitoring half of the every-run health seam. It reads the block-grain
telecom feature table and summarises each numeric feature's distribution for the run — the
count and null rate, the share sitting at the neutral zero value, the mean and spread, and a
spread of approximate quantiles from the first to the ninety-ninth percentile. The intent is
to catch a feature that has gone degenerate (collapsed to a single value) or has visibly
shifted before those features reach the scorer, especially the two derived features the
index leans on most, the fiber opportunity gap and the top-tier fiber speed.

The distribution snapshot is one row per feature so it is easy to eyeball or diff across
runs, and a small pure check flags features that look degenerate — constant, entirely at
zero, or unexpectedly null-heavy — so a caller can alert on them. Comparing a run against a
frozen baseline (drift) is a separate concern handled elsewhere; this module only describes
the current run. The SQL is generated from the feature list so it cannot drift from the
contract, rendering is separated from execution so it can be inspected without a client, and
the client is injected so tests can supply a fake one.
"""
import argparse
import logging
from dataclasses import dataclass, field

import pandas as pd

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_TELECOM_FEATURES_BLOCK,
)
from network_idx.constants import TELECOM_FEATURES
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# The telecom features that exist at the block grain: the full telecom bucket minus the
# fiber-distance feature, which is computed later at the parcel grain rather than the block.
TELECOM_BLOCK_FEATURES = [f for f in TELECOM_FEATURES if f != "dist_to_nearest_fiber_miles"]

# The approximate quantiles reported for each feature, as (percentile, column-name) pairs.
DISTRIBUTION_QUANTILES = [
    (1, "p01"),
    (5, "p05"),
    (10, "p10"),
    (25, "p25"),
    (50, "p50"),
    (75, "p75"),
    (90, "p90"),
    (95, "p95"),
    (99, "p99"),
]

# Default thresholds for flagging a feature's distribution as suspect.
DEFAULT_MAX_NULL_RATE = 0.05

# The two derived telecom features whose band counts are tracked per run, and the default
# band edges that split their [0, 1] range into five equal bands. Both are configurable.
BAND_COUNT_FEATURES = ["fiber_opportunity_gap", "fiber_speed_top_tier"]
DEFAULT_BAND_EDGES = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0]


def features_table_ref() -> str:
    """Return the fully qualified block telecom feature table this monitor reads."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_TELECOM_FEATURES_BLOCK}"


def _feature_distribution_select(feature: str, table: str) -> str:
    """
    Render the one-row distribution summary SELECT for a single feature.

    The row carries the row count, null count and rate, the count and rate of values sitting
    at zero, the mean, standard deviation, minimum and maximum, and the configured
    approximate quantiles, all computed over the feature table in a single pass.
    """
    quantile_exprs = ",\n    ".join(
        f"APPROX_QUANTILES({feature}, 100)[OFFSET({pct})] AS {name}"
        for pct, name in DISTRIBUTION_QUANTILES
    )
    return f"""SELECT
    '{feature}' AS feature,
    COUNT(*) AS n,
    COUNTIF({feature} IS NULL) AS n_null,
    SAFE_DIVIDE(COUNTIF({feature} IS NULL), COUNT(*)) AS null_rate,
    COUNTIF({feature} = 0) AS n_zero,
    SAFE_DIVIDE(COUNTIF({feature} = 0), COUNT(*)) AS zero_rate,
    AVG({feature}) AS mean,
    STDDEV({feature}) AS stddev,
    MIN({feature}) AS min,
    {quantile_exprs},
    MAX({feature}) AS max
  FROM `{table}`"""


def render_distribution_sql(features_table: str, features: list[str] | None = None) -> str:
    """
    Render the per-feature distribution snapshot query as a union over the features.

    This is a pure function: it builds one summary SELECT per feature from the feature list
    and unions them, performing no input or output so it can be unit tested. The result is
    one row per feature describing that feature's distribution for the run.
    """
    if features is None:
        features = TELECOM_BLOCK_FEATURES
    return "\nUNION ALL\n".join(
        _feature_distribution_select(feature, features_table) for feature in features
    )


@dataclass
class DistributionReport:
    """The outcome of scanning the per-feature distribution snapshot for problems."""

    passed: bool
    n_features: int = 0
    flags: dict[str, list[str]] = field(default_factory=dict)


def flag_distributions(
    dist_df: pd.DataFrame,
    max_null_rate: float = DEFAULT_MAX_NULL_RATE,
) -> DistributionReport:
    """
    Flag features whose distribution looks degenerate or unexpectedly null-heavy.

    A feature is flagged 'constant' when it has no spread (its minimum equals its maximum, or
    its standard deviation is zero or undefined), 'all_zero' when every value sits at zero,
    and 'high_null' when its null rate exceeds the threshold. The report passes only when no
    feature raises a flag, so a caller can treat a failure as a reason to alert before
    scoring. This function is pure and performs no input or output.
    """
    flags: dict[str, list[str]] = {}
    for row in dist_df.itertuples(index=False):
        reasons: list[str] = []
        stddev = getattr(row, "stddev")
        if row.min == row.max or pd.isna(stddev) or stddev == 0:
            reasons.append("constant")
        if row.zero_rate == 1:
            reasons.append("all_zero")
        if pd.notna(row.null_rate) and row.null_rate > max_null_rate:
            reasons.append("high_null")
        if reasons:
            flags[row.feature] = reasons

    return DistributionReport(
        passed=len(flags) == 0,
        n_features=int(len(dist_df)),
        flags=flags,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(client=None, features: list[str] | None = None) -> tuple[pd.DataFrame, DistributionReport]:
    """
    Compute the telecom feature distribution snapshot for the run and flag problems.

    The snapshot query is rendered and read from BigQuery, the resulting one-row-per-feature
    frame is scanned for degenerate or null-heavy features, and both the frame and the flag
    report are logged and returned. A client is created if one is not supplied.
    """
    if client is None:
        client = get_bq_client()

    sql = render_distribution_sql(features_table_ref(), features)
    dist_df = client.query(sql).to_dataframe()
    report = flag_distributions(dist_df)

    logger.info(
        f"Telecom feature distributions: {report.n_features} features, "
        f"{'no flags' if report.passed else 'FLAGS: ' + str(report.flags)}."
    )
    return dist_df, report


# ── Feature band counts and per-state rollups ─────────────────────────────────


def band_labels(edges: list[float]) -> list[str]:
    """
    Return the ordered band labels for a set of edges, as half-open intervals.

    Each label covers the interval from one edge up to the next and is written half-open
    except for the final band, which is closed so the top edge itself lands in a band. The
    labels are returned in ascending order so a caller can present or sort the counts
    consistently.
    """
    labels = []
    for i in range(len(edges) - 1):
        closing = "]" if i == len(edges) - 2 else ")"
        labels.append(f"[{edges[i]:.2f},{edges[i + 1]:.2f}{closing}")
    return labels


def _band_case(feature: str, edges: list[float]) -> str:
    """
    Render the CASE expression that maps a feature value to its band label.

    Null values map to a 'null' band and values outside the edge range map to 'below_range'
    or 'above_range', so no block is silently dropped; every other value falls in exactly one
    half-open band, with the top edge included in the final band.
    """
    labels = band_labels(edges)
    whens = [
        f"WHEN {feature} IS NULL THEN 'null'",
        f"WHEN {feature} < {edges[0]:.2f} THEN 'below_range'",
    ]
    for i, label in enumerate(labels):
        if i == len(labels) - 1:
            whens.append(f"WHEN {feature} <= {edges[i + 1]:.2f} THEN '{label}'")
        else:
            whens.append(f"WHEN {feature} < {edges[i + 1]:.2f} THEN '{label}'")
    whens.append("ELSE 'above_range'")
    indented = "\n      ".join(whens)
    return f"CASE\n      {indented}\n    END"


def _feature_band_select(feature: str, table: str, edges: list[float]) -> str:
    """
    Render the per-state band-count SELECT for a single feature.

    Each row counts the blocks in one state whose feature value falls in one band, using the
    block table's state FIPS (the first two digits of the block GEOID) as the state key.
    """
    return f"""SELECT
    '{feature}' AS feature,
    state_fips,
    {_band_case(feature, edges)} AS band,
    COUNT(*) AS n_blocks
  FROM `{table}`
  GROUP BY state_fips, band"""


def render_band_counts_sql(
    features_table: str,
    features: list[str] | None = None,
    edges: list[float] | None = None,
) -> str:
    """
    Render the per-state band-count query as a union over the tracked features.

    This is a pure function: it builds one per-state band-count SELECT per feature from the
    feature list and band edges and unions them, performing no input or output so it can be
    unit tested. The result is one row per feature, state, and band giving the block count.
    """
    if features is None:
        features = BAND_COUNT_FEATURES
    if edges is None:
        edges = DEFAULT_BAND_EDGES
    return "\nUNION ALL\n".join(
        _feature_band_select(feature, features_table, edges) for feature in features
    )


def national_band_totals(band_df: pd.DataFrame) -> pd.DataFrame:
    """
    Roll the per-state band counts up to national totals per feature and band.

    This is a pure function that sums the block counts across states, returning one row per
    feature and band, so a caller can see the national shape without re-querying.
    """
    return (
        band_df.groupby(["feature", "band"], as_index=False)["n_blocks"]
        .sum()
        .sort_values(["feature", "band"])
        .reset_index(drop=True)
    )


def run_band_counts(
    client=None,
    features: list[str] | None = None,
    edges: list[float] | None = None,
) -> pd.DataFrame:
    """
    Compute the per-state band counts for the tracked telecom features.

    The band-count query is rendered and read from BigQuery, and the per-state
    one-row-per-band frame is logged and returned. A client is created if one is not
    supplied; the national rollup is available from national_band_totals.
    """
    if client is None:
        client = get_bq_client()

    sql = render_band_counts_sql(features_table_ref(), features, edges)
    band_df = client.query(sql).to_dataframe()

    logger.info(
        f"Telecom feature band counts: {len(band_df)} feature-state-band rows across "
        f"{band_df['state_fips'].nunique() if len(band_df) else 0} states."
    )
    return band_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Report per-run value distributions of the telecom block features."
    )
    parser.parse_args()
    frame, _ = run()
    print(frame.to_string(index=False))
