"""
Country-wide min-max scaling for the parcel-level index.
==============================================================
Builds a frozen `scaling_params` table (one row per feature per run) via a single
BigQuery aggregation scan, then applies it deterministically to any feature frame.

Per feature we persist: na_fill, min_val, max_val, invert. Scaling is:
    x = fillna(x, na_fill)            # also maps +/-inf -> na_fill
    x = clip(x, min_val, max_val)     # winsorize (distance caps; no-op otherwise)
    s = (x - min_val) / (max_val - min_val)   # 0..1
    if invert: s = 1 - s
Inverted features (lower raw -> higher score) come from INVERTED_FEATURES.
"""

import logging

import numpy as np
import pandas as pd
from google.cloud import bigquery

from network_idx.constants import (
    ALL_SCORING_FEATURES,
    INVERTED_FEATURES,
    SCALING_NA_FILL_RULES,
    SCALING_CAP_AS_MAX,
)

logger = logging.getLogger(__name__)

SCALING_PARAMS_COLUMNS = [
    "run_id", "feature", "na_fill", "min_val", "max_val", "invert", "created_at",
]


def build_stats_query(source_table: str) -> str:
    """Single-scan aggregation for all features (NULLs ignored by MIN/MAX/quantiles)."""
    const_features = [f for f in ALL_SCORING_FEATURES if f not in SCALING_CAP_AS_MAX]
    cap_features = [f for f in ALL_SCORING_FEATURES if f in SCALING_CAP_AS_MAX]

    parts = []
    for f in const_features:
        fill = SCALING_NA_FILL_RULES[f]
        # min/max over the FILLED column so the constant fill is reflected in the range
        parts.append(f"MIN(COALESCE(`{f}`, {fill})) AS `{f}__min`")
        parts.append(f"MAX(COALESCE(`{f}`, {fill})) AS `{f}__max`")
    for f in cap_features:
        parts.append(f"MIN(`{f}`) AS `{f}__min`")
        parts.append(f"MAX(`{f}`) AS `{f}__rawmax`")
        parts.append(f"APPROX_QUANTILES(`{f}`, 100)[OFFSET(99)] AS `{f}__p99`")

    body = ",\n    ".join(parts)
    return f"SELECT\n    {body}\nFROM `{source_table}`"


def compute_scaling_params_bq(
    client: bigquery.Client, source_table: str, run_id: str
) -> pd.DataFrame:
    """Run the aggregation and assemble the tidy scaling_params frame."""
    sql = build_stats_query(source_table)
    logger.info(f"Computing scaling stats over {source_table} ...")
    row = list(client.query(sql).result())[0]

    now = pd.Timestamp.now(tz="UTC")
    records = []
    for f in ALL_SCORING_FEATURES:
        invert = f in INVERTED_FEATURES
        min_val = float(row[f"{f}__min"])

        if f in SCALING_CAP_AS_MAX:
            rule = SCALING_NA_FILL_RULES[f]
            if rule == "p99":
                cap = float(row[f"{f}__p99"])
            elif rule == "max_x1.25":
                cap = float(row[f"{f}__rawmax"]) * 1.25
            else:
                raise ValueError(f"Unknown cap rule for {f}: {rule!r}")
            max_val = cap
            na_fill = cap
        else:
            max_val = float(row[f"{f}__max"])
            na_fill = float(SCALING_NA_FILL_RULES[f])

        if max_val <= min_val:
            logger.warning(f"{f}: max_val ({max_val}) <= min_val ({min_val}); "
                           f"scaled values will be 0 (constant feature).")

        records.append({
            "run_id": run_id,
            "feature": f,
            "na_fill": na_fill,
            "min_val": min_val,
            "max_val": max_val,
            "invert": invert,
            "created_at": now,
        })

    return pd.DataFrame.from_records(records, columns=SCALING_PARAMS_COLUMNS)


def apply_scaling(
    df: pd.DataFrame, params: pd.DataFrame, suffix: str = "_scaled"
) -> pd.DataFrame:
    """Apply persisted params to df; adds one `<feature><suffix>` column per feature."""
    out = df.copy()
    for p in params.itertuples(index=False):
        f = p.feature
        if f not in out.columns:
            raise KeyError(f"Feature '{f}' missing from frame; cannot scale.")
        lo, hi = p.min_val, p.max_val
        x = out[f].replace([np.inf, -np.inf], np.nan).fillna(p.na_fill).clip(lo, hi)
        denom = hi - lo
        s = (x - lo) / denom if denom > 0 else pd.Series(0.0, index=out.index)
        out[f + suffix] = (1.0 - s) if p.invert else s
    return out


def write_scaling_params(
    client: bigquery.Client, params: pd.DataFrame, table_id: str, run_id: str
) -> None:
    """Replace this run's rows (delete-then-append) so multiple runs can coexist."""
    try:
        client.query(
            f"DELETE FROM `{table_id}` WHERE run_id = @run_id",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            ]),
        ).result()
    except Exception as e:  # table may not exist yet on first run
        logger.info(f"Skipping delete (table may not exist yet): {e}")

    job = client.load_table_from_dataframe(
        params, table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    logger.info(f"Wrote {len(params)} scaling rows for run_id={run_id} to {table_id}")


def read_scaling_params(
    client: bigquery.Client, table_id: str, run_id: str
) -> pd.DataFrame:
    sql = f"SELECT * FROM `{table_id}` WHERE run_id = @run_id"
    return client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]),
    ).to_dataframe()