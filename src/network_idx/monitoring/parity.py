"""
Train/scoring parity: do the training-fit scaling rules still fit the scored universe?

The scaling parameters — each feature's winsorize bounds and null fill — are fit once on
the tract-grain training population and then frozen. Scoring applies those same frozen
bounds to every parcel. Parity monitoring checks that this transfer is safe: for each
scoring feature it measures, over the parcel population, the share of values that fall
below the fitted floor or above the fitted cap (clip saturation) and the share that are
null and take the fill. A feature whose scored population piles up against a training-fit
cap, or is mostly null, is one where the frozen rule no longer represents the data it is
being applied to, which biases its sub-index.

This is the production risk that a model tuned on a training slice silently degrades on the
full scored universe. The check reads the frozen scaling parameters for the run and the
raw parcel feature frame, so it compares the exact rule against the exact population it is
applied to. The SQL is generated from the parameters so it cannot drift from them, the
rendering is separated from execution so it can be inspected without a client, the flagging
is pure, and the client is injected so tests supply a fake one.
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
    BQ_DATASET_ANALYTICS,
    BQ_TABLE_PARCEL_FEATURES,
    BQ_TABLE_SCALING_PARAMS,
)
from network_idx.scoring.scaling import read_scaling_params
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Default parity thresholds: a feature is flagged when more than five percent of parcels are
# pinned against the fitted floor or cap (clip saturation), or when more than a fifth take
# the null fill. The null bar is looser because some features are legitimately sparse at the
# parcel grain.
DEFAULT_MAX_SATURATION = 0.05
DEFAULT_MAX_NULL_RATE = 0.20


def features_table_ref() -> str:
    """Return the fully qualified parcel feature table the parity check reads."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_FEATURES}"


def scaling_params_table_ref() -> str:
    """Return the fully qualified scaling-params table holding the frozen rules."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_SCALING_PARAMS}"


def _f(x) -> str:
    """Render a numeric bound as a plain SQL literal without scientific notation."""
    return repr(float(x))


def _feature_parity_select(feature: str, lo: float, hi: float, table: str) -> str:
    """
    Render the parity SELECT for one feature against its fitted bounds.

    The row reports, over the parcel feature table, the row count, the null count and rate,
    and the counts and rates of values below the fitted floor and above the fitted cap, so a
    caller can see how much of the scored population the frozen rule clips.
    """
    return f"""SELECT
    '{feature}' AS feature,
    {_f(lo)} AS min_val,
    {_f(hi)} AS max_val,
    COUNT(*) AS n,
    COUNTIF({feature} IS NULL) AS n_null,
    SAFE_DIVIDE(COUNTIF({feature} IS NULL), COUNT(*)) AS null_rate,
    SAFE_DIVIDE(COUNTIF({feature} < {_f(lo)}), COUNT(*)) AS clip_low_rate,
    SAFE_DIVIDE(COUNTIF({feature} > {_f(hi)}), COUNT(*)) AS clip_high_rate
  FROM `{table}`"""


def render_parity_sql(features_table: str, params: pd.DataFrame) -> str:
    """
    Render the per-feature parity query as a union over the scaling parameters.

    This is a pure function: it builds one parity SELECT per feature from the frozen bounds
    in the scaling-params frame and unions them, performing no input or output so it can be
    unit tested. The result is one row per feature giving its null rate and its low/high clip
    saturation over the scored parcel population.
    """
    selects = [
        _feature_parity_select(row.feature, row.min_val, row.max_val, features_table)
        for row in params.itertuples(index=False)
    ]
    return "\nUNION ALL\n".join(selects)


@dataclass
class ParityReport:
    """The outcome of checking the scored population against the frozen scaling rules."""

    passed: bool
    n_features: int = 0
    flags: dict[str, list[str]] = field(default_factory=dict)
    parity: pd.DataFrame = field(default_factory=pd.DataFrame)


def flag_parity(
    parity_df: pd.DataFrame,
    max_saturation: float = DEFAULT_MAX_SATURATION,
    max_null_rate: float = DEFAULT_MAX_NULL_RATE,
) -> ParityReport:
    """
    Flag features whose scored population strains the training-fit scaling rules.

    A feature is flagged 'clip_saturation' when the combined share pinned below the floor or
    above the cap exceeds the threshold and 'high_null' when its null rate exceeds the null
    threshold. The report passes only when nothing is flagged, so a caller can treat a
    failure as a sign the frozen rule no longer fits the scored universe. This function is
    pure and does no I/O.
    """
    flags: dict[str, list[str]] = {}
    for row in parity_df.itertuples(index=False):
        reasons: list[str] = []
        saturation = (row.clip_low_rate or 0.0) + (row.clip_high_rate or 0.0)
        if saturation > max_saturation:
            reasons.append("clip_saturation")
        if pd.notna(row.null_rate) and row.null_rate > max_null_rate:
            reasons.append("high_null")
        if reasons:
            flags[row.feature] = reasons

    return ParityReport(
        passed=len(flags) == 0,
        n_features=int(len(parity_df)),
        flags=flags,
        parity=parity_df.reset_index(drop=True),
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(run_id: str, client=None) -> ParityReport:
    """
    Check the scored parcel population against a run's frozen scaling rules.

    The run's scaling parameters are read, the parity query is rendered against the parcel
    feature table and executed, and the resulting per-feature frame is flagged for clip
    saturation and high null rates; the report is logged and returned. A client is created if
    one is not supplied. Raises when the run has no scaling parameters, since there is no rule
    to check parity against.
    """
    if client is None:
        client = get_bq_client()

    params = read_scaling_params(client, scaling_params_table_ref(), run_id)
    if params.empty:
        raise ValueError(f"No scaling_params rows for run_id={run_id}; nothing to check parity against.")

    sql = render_parity_sql(features_table_ref(), params)
    parity_df = client.query(sql).to_dataframe()
    report = flag_parity(parity_df)

    logger.info(
        f"Train/scoring parity for run_id={run_id}: {report.n_features} features, "
        f"{'no flags' if report.passed else 'FLAGS: ' + str(report.flags)}."
    )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check the scored parcel population against a run's frozen scaling rules."
    )
    parser.add_argument("--run-id", required=True, help="The scoring run whose rules to check.")
    args = parser.parse_args()
    result = run(args.run_id)
    print(result.parity.to_string(index=False))
