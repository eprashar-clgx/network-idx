"""
Score and feature drift against a frozen baseline.

Drift monitoring answers "has this run moved away from a known-good reference?" The
reference is a frozen distribution snapshot — the one-row-per-feature summary the metrics
monitor already produces — pinned to a baseline run and stored in a baseline table. On a
later run the same snapshot is computed and compared column by column against the pinned
baseline: a feature is flagged when its mean shifts materially, when its null rate moves,
or when its shape (the quantile spread) shifts relative to the baseline's own spread, and
also when it appears or disappears between the two runs.

Storing the baseline as a snapshot keyed by run and snapshot kind (feature or score) keeps
the reference explicit and versioned, so promoting a new baseline is just writing a new
run's snapshot rather than editing thresholds. The comparison is a pure function so it can
be unit tested offline, the persistence is a delete-then-append keyed by run and kind so a
baseline can be rewritten idempotently, and the client is injected so production supplies a
real BigQuery client while tests supply a fake one.
"""
import argparse
import logging
from dataclasses import dataclass, field

import pandas as pd

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_ANALYTICS,
    BQ_TABLE_MONITORING_BASELINE,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# The two kinds of snapshot the baseline table holds: block-feature distributions and
# parcel-score distributions. The kind is stored alongside the run id so one table can pin
# both references without their features colliding.
SNAPSHOT_FEATURE = "feature"
SNAPSHOT_SCORE = "score"

# The distribution quantile columns compared when measuring a shape shift, and a tiny
# floor that keeps the relative measures finite when a baseline value is near zero.
QUANTILE_COLUMNS = ["p01", "p05", "p10", "p25", "p50", "p75", "p90", "p95", "p99"]
EPS = 1e-9

# Default drift thresholds: a feature is flagged when its mean moves by more than a quarter
# of its baseline magnitude, its null rate moves by more than five points, or its quantile
# spread shifts by more than a quarter of the baseline's own inter-percentile range.
DEFAULT_MEAN_REL_SHIFT = 0.25
DEFAULT_NULL_RATE_DELTA = 0.05
DEFAULT_DIST_SHIFT = 0.25


def baseline_table_ref() -> str:
    """Return the fully qualified baseline snapshot table drift reads and writes."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_MONITORING_BASELINE}"


@dataclass
class DriftReport:
    """The outcome of comparing a run's distribution snapshot against a baseline."""

    passed: bool
    n_features: int = 0
    flags: dict[str, list[str]] = field(default_factory=dict)
    drift: pd.DataFrame = field(default_factory=pd.DataFrame)


def compute_drift(
    current_df: pd.DataFrame,
    baseline_df: pd.DataFrame,
    mean_rel_shift: float = DEFAULT_MEAN_REL_SHIFT,
    null_rate_delta: float = DEFAULT_NULL_RATE_DELTA,
    dist_shift: float = DEFAULT_DIST_SHIFT,
) -> DriftReport:
    """
    Compare a run's distribution snapshot against a baseline and flag drifted features.

    For every feature present in both snapshots the relative mean shift, the null-rate
    change, and a shape shift (the largest quantile move divided by the baseline's
    first-to-ninety-ninth percentile spread) are computed and checked against the
    thresholds; a feature only in the current run is flagged 'missing_baseline' and one only
    in the baseline is flagged 'dropped'. The report passes when nothing is flagged, and it
    carries a per-feature drift frame for logging. This function is pure and does no I/O.
    """
    cur = current_df.set_index("feature")
    base = baseline_df.set_index("feature")
    flags: dict[str, list[str]] = {}
    records = []

    for feature in cur.index.union(base.index):
        if feature not in base.index:
            flags[feature] = ["missing_baseline"]
            continue
        if feature not in cur.index:
            flags[feature] = ["dropped"]
            continue

        c, b = cur.loc[feature], base.loc[feature]
        mean_move = abs(c["mean"] - b["mean"]) / max(abs(b["mean"]), EPS)
        null_move = abs(c["null_rate"] - b["null_rate"])
        spread = max(abs(b.get("p99", 0.0) - b.get("p01", 0.0)), EPS)
        shape_move = max(
            (abs(c[q] - b[q]) for q in QUANTILE_COLUMNS if q in c and q in b),
            default=0.0,
        ) / spread

        reasons = []
        if mean_move > mean_rel_shift:
            reasons.append("mean_shift")
        if null_move > null_rate_delta:
            reasons.append("null_shift")
        if shape_move > dist_shift:
            reasons.append("dist_shift")
        if reasons:
            flags[feature] = reasons

        records.append(
            dict(
                feature=feature,
                mean_rel_shift=mean_move,
                null_rate_delta=null_move,
                dist_shift=shape_move,
                flagged=bool(reasons),
            )
        )

    drift = pd.DataFrame.from_records(records).sort_values("feature").reset_index(drop=True) \
        if records else pd.DataFrame(columns=["feature", "mean_rel_shift", "null_rate_delta", "dist_shift", "flagged"])
    return DriftReport(
        passed=len(flags) == 0,
        n_features=int(len(records)),
        flags=flags,
        drift=drift,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def write_baseline(
    client,
    dist_df: pd.DataFrame,
    run_id: str,
    snapshot_kind: str = SNAPSHOT_FEATURE,
    table_ref: str | None = None,
) -> str:
    """
    Persist a distribution snapshot as the drift baseline for a run and kind.

    The snapshot frame is stamped with the run id and the snapshot kind, this run-and-kind's
    existing rows are deleted, and the stamped rows are appended, so writing a baseline is
    idempotent and promoting a new baseline is just another write. The baseline table
    reference is resolved from configuration when not supplied, and the reference is returned.
    """
    from google.cloud import bigquery

    if snapshot_kind not in (SNAPSHOT_FEATURE, SNAPSHOT_SCORE):
        raise ValueError(f"Unknown snapshot_kind {snapshot_kind!r}.")
    table_ref = table_ref or baseline_table_ref()

    stamped = dist_df.copy()
    stamped.insert(0, "run_id", run_id)
    stamped.insert(1, "snapshot_kind", snapshot_kind)

    try:
        client.query(
            f"DELETE FROM `{table_ref}` WHERE run_id = @run_id AND snapshot_kind = @kind",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                bigquery.ScalarQueryParameter("kind", "STRING", snapshot_kind),
            ]),
        ).result()
    except Exception as e:  # table may not exist on first write
        logger.info(f"Skipping baseline delete (table may not exist yet): {e}")

    client.load_table_from_dataframe(
        stamped, table_ref,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        ),
    ).result()
    logger.info(f"Wrote {len(stamped)} baseline rows for run_id={run_id} kind={snapshot_kind}.")
    return table_ref


def read_baseline(
    client,
    run_id: str,
    snapshot_kind: str = SNAPSHOT_FEATURE,
    table_ref: str | None = None,
) -> pd.DataFrame:
    """
    Read the frozen distribution snapshot pinned to a baseline run and kind.

    The baseline table reference is resolved from configuration when not supplied, and the
    run's rows for the requested kind are returned with the run and kind bookkeeping columns
    dropped so the frame matches a freshly computed snapshot for comparison.
    """
    from google.cloud import bigquery

    table_ref = table_ref or baseline_table_ref()
    df = client.query(
        f"SELECT * FROM `{table_ref}` WHERE run_id = @run_id AND snapshot_kind = @kind",
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("kind", "STRING", snapshot_kind),
        ]),
    ).to_dataframe()
    return df.drop(columns=[c for c in ("run_id", "snapshot_kind") if c in df.columns])


def run_drift(
    client,
    current_df: pd.DataFrame,
    baseline_run_id: str,
    snapshot_kind: str = SNAPSHOT_FEATURE,
    table_ref: str | None = None,
) -> DriftReport:
    """
    Compare a computed snapshot against a pinned baseline run and return the drift report.

    The baseline snapshot for the run and kind is read and the current snapshot is compared
    against it with the default thresholds; the resulting report is logged and returned. When
    the baseline is empty the report fails with a single 'no_baseline' flag so a caller can
    tell an empty reference apart from a clean comparison.
    """
    baseline_df = read_baseline(client, baseline_run_id, snapshot_kind, table_ref)
    if baseline_df.empty:
        logger.warning(f"No baseline rows for run_id={baseline_run_id} kind={snapshot_kind}.")
        return DriftReport(passed=False, flags={"__baseline__": ["no_baseline"]})

    report = compute_drift(current_df, baseline_df)
    logger.info(
        f"Drift vs baseline {baseline_run_id} ({snapshot_kind}): {report.n_features} features, "
        f"{'no drift' if report.passed else 'FLAGS: ' + str(report.flags)}."
    )
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare a distribution snapshot against a frozen baseline run."
    )
    parser.add_argument("--baseline-run-id", required=True, help="The pinned baseline run.")
    parser.add_argument(
        "--kind", default=SNAPSHOT_FEATURE, choices=[SNAPSHOT_FEATURE, SNAPSHOT_SCORE]
    )
    args = parser.parse_args()
    from network_idx.monitoring import metrics

    client = get_bq_client()
    snapshot, _ = metrics.run(client=client)
    result = run_drift(client, snapshot, args.baseline_run_id, args.kind)
    print(result.drift.to_string(index=False))
