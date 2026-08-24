"""
Business monitoring: rollups of the delivered parcel index.

This is the business half of the monitoring seam. Where the feature and score metrics ask
"is the pipeline healthy?", these rollups answer "what does the delivered index look like
to the business?" by summarising the parcel scores table into the counts stakeholders
track: how many parcels fall in each quartile band of each index, how many clear the
high-opportunity score threshold, and — per state — the parcel total and the fiber-potential
quartile spread. The four indices are the overall fiber-potential index and its three
sub-indices (growth, telecom, demographic).

Each rollup is a single grouped query over the parcel scores table, so the numbers always
reflect the persisted run rather than a re-derivation. The SQL is generated from the index
list and the band edges so it cannot drift from the scoring output, the rendering is
separated from execution so a query can be inspected without a client, and the client is
injected so production supplies a real BigQuery client while tests supply a fake one. The
module reads and reports; it does not write, leaving persistence to the caller.
"""
import argparse
import logging

import pandas as pd

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_OUTPUTS,
    BQ_TABLE_PARCEL_SCORES,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# The delivered indices and the business-facing name for each. idx_overall is the
# fiber-potential index; the other three are its within-bucket sub-indices.
INDEX_COLUMNS = {
    "idx_overall": "fiber_potential",
    "idx_growth": "growth",
    "idx_telecom": "telecom",
    "idx_demo": "demographic",
}

# The quartile band edges over the 0-100 index range and the ordered labels for them. A
# value lands in the first band it is below, with 100 included in the top band.
QUARTILE_EDGES = [0.0, 25.0, 50.0, 75.0, 100.0]
QUARTILE_LABELS = ["Q1", "Q2", "Q3", "Q4"]

# Parcels at or above this index score are the high-opportunity set the business tracks.
DEFAULT_HIGH_SCORE_THRESHOLD = 85.0


def scores_table_ref() -> str:
    """Return the fully qualified parcel scores table these rollups read."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_PARCEL_SCORES}"


def _quartile_case(index_col: str) -> str:
    """
    Render the CASE expression that maps an index value to its quartile label.

    Null values map to a 'null' band so no parcel is silently dropped; every non-null value
    falls in exactly one quartile, with the top edge (100) landing in the final quartile.
    """
    whens = [f"WHEN {index_col} IS NULL THEN 'null'"]
    for i, label in enumerate(QUARTILE_LABELS):
        upper = QUARTILE_EDGES[i + 1]
        if i == len(QUARTILE_LABELS) - 1:
            whens.append(f"WHEN {index_col} <= {upper:.1f} THEN '{label}'")
        else:
            whens.append(f"WHEN {index_col} < {upper:.1f} THEN '{label}'")
    indented = "\n      ".join(whens)
    return f"CASE\n      {indented}\n    END"


def render_index_quartiles_sql(scores_table: str, run_id: str) -> str:
    """
    Render the per-index quartile-count query for one run.

    This is a pure function: it builds one grouped SELECT per index that counts parcels in
    each quartile band and unions them, filtering to the given run, so the result is one row
    per index and quartile giving the parcel count. It performs no input or output.
    """
    selects = []
    for index_col, name in INDEX_COLUMNS.items():
        selects.append(
            f"""SELECT
    '{name}' AS index_name,
    {_quartile_case(index_col)} AS quartile,
    COUNT(*) AS n_parcels
  FROM `{scores_table}`
  WHERE run_id = '{run_id}'
  GROUP BY quartile"""
        )
    return "\nUNION ALL\n".join(selects)


def render_high_score_sql(
    scores_table: str, run_id: str, threshold: float = DEFAULT_HIGH_SCORE_THRESHOLD
) -> str:
    """
    Render the per-index high-opportunity count query for one run.

    This is a pure function: it builds one grouped SELECT per index that counts parcels
    scoring at or above the threshold alongside the run total, so the result is one row per
    index with the high-score count and the parcels-scored count. It does no I/O.
    """
    selects = []
    for index_col, name in INDEX_COLUMNS.items():
        selects.append(
            f"""SELECT
    '{name}' AS index_name,
    {threshold:.1f} AS threshold,
    COUNTIF({index_col} >= {threshold:.1f}) AS n_at_or_above,
    COUNT(*) AS n_scored
  FROM `{scores_table}`
  WHERE run_id = '{run_id}'"""
        )
    return "\nUNION ALL\n".join(selects)


def render_state_rollup_sql(scores_table: str, run_id: str) -> str:
    """
    Render the per-state fiber-potential rollup query for one run.

    This is a pure function: it groups the run's parcels by state — the first two digits of
    the block GEOID — and returns, per state, the parcel total and the count of parcels in
    each fiber-potential quartile, so a caller can see both scale and the score spread by
    state. It performs no input or output.
    """
    counts = [
        f"COUNTIF(idx_overall {'<' if i < len(QUARTILE_LABELS) - 1 else '<='} "
        f"{QUARTILE_EDGES[i + 1]:.1f} "
        f"AND idx_overall >= {QUARTILE_EDGES[i]:.1f}) AS fiber_potential_{label.lower()}"
        for i, label in enumerate(QUARTILE_LABELS)
    ]
    count_exprs = ",\n    ".join(counts)
    return f"""SELECT
    SUBSTR(block_geoid, 1, 2) AS state_fips,
    COUNT(*) AS total_parcels,
    {count_exprs}
  FROM `{scores_table}`
  WHERE run_id = '{run_id}'
  GROUP BY state_fips
  ORDER BY state_fips"""


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def run(
    run_id: str,
    client=None,
    threshold: float = DEFAULT_HIGH_SCORE_THRESHOLD,
) -> dict[str, pd.DataFrame]:
    """
    Compute the three business rollups for a scored run and return them.

    The quartile-count, high-opportunity, and per-state queries are rendered against the
    parcel scores table and read from BigQuery, and the three frames are logged and returned
    keyed by name ('index_quartiles', 'high_scores', 'state_rollup'). A client is created if
    one is not supplied; the module does not persist the rollups.
    """
    if client is None:
        client = get_bq_client()

    scores_table = scores_table_ref()
    quartiles = client.query(render_index_quartiles_sql(scores_table, run_id)).to_dataframe()
    high = client.query(render_high_score_sql(scores_table, run_id, threshold)).to_dataframe()
    state = client.query(render_state_rollup_sql(scores_table, run_id)).to_dataframe()

    logger.info(
        f"Business rollups for run_id={run_id}: "
        f"{len(quartiles)} index-quartile rows, {len(high)} index high-score rows, "
        f"{len(state)} states."
    )
    return {"index_quartiles": quartiles, "high_scores": high, "state_rollup": state}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Report business rollups of the delivered parcel index."
    )
    parser.add_argument("--run-id", required=True, help="The scoring run to roll up.")
    args = parser.parse_args()
    rollups = run(args.run_id)
    for name, frame in rollups.items():
        print(f"\n=== {name} ===")
        print(frame.to_string(index=False))
