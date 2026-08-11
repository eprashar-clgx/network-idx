"""
Score every parcel into a 0-100 opportunity index (BigQuery pushdown).
==============================================================
Reads the frozen scaling_params and feature_weights for a run, generates a single
BigQuery query that:
  1. scales each of the 13 features to [0,1] (COALESCE NaN/Inf -> na_fill, clip to
     [min,max], min-max, invert per INVERTED_FEATURES) — identical math to
     scaling.apply_scaling, but expressed in SQL so it runs over all ~160M parcels;
  2. forms each sub-index as the within-bucket weighted sum of scaled features
     (raw 0..1);
  3. min-max rescales each sub-index to 0..100 over the full population;
  4. idx_overall = Σ_bucket bucket_weight · idx_bucket   (weighted avg of sub-indices) 
  5. Rescale the idx_overall to 0-100  

Output: {GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_PARCEL_SCORES}

Usage:
    python -m network_idx.scoring.parcel_score
    python -m network_idx.scoring.parcel_score --dry-run
    python -m network_idx.scoring.parcel_score --run-id lightgbm_k8_v1
"""

import argparse
import logging
import pandas as pd
from google.cloud import bigquery

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_PARCEL_FEATURES,
    BQ_TABLE_PARCEL_GROWTH,
    BQ_DATASET_ANALYTICS,
    BQ_TABLE_SCALING_PARAMS,
    BQ_TABLE_FEATURE_WEIGHTS,
    BQ_DATASET_OUTPUTS,
    BQ_TABLE_PARCEL_SCORES,
    BQ_TABLE_FIBER_IDX_PARCEL,
    BQ_TABLE_FIBER_IDX_PARCEL_QA_MINMAX,
    BQ_TABLE_FIBER_IDX_PARCEL_QA_FILLRATES,
    BQ_TABLE_FIBER_IDX_PARCEL_QA_INDEX_BUCKETS
)
from network_idx.constants import (
    ALL_SCORING_FEATURES,
    SCORING_BUCKETS,
    SCORING_RUN_ID,
    DELIVERY_FEATURE_NAMES,
)

from network_idx.scoring.scaling import read_scaling_params
from network_idx.scoring.weights import read_feature_weights
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# QA / rounding
ROUND_INDEX = 2   # 0-100 indices & sub-indices
ROUND_VALUE = 4   # 0-1 scaled, weights, raw feature floats, raw sub-index sums
NUMERIC_BQ_TYPES = {"INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC"}
INTEGER_RAW_FEATURES = {
    "census_housing_units",
    "landuse_change_qtr_mi_cnt",
    "pre_early_dev_qtr_mi_cnt",
    "bldr_dev_qtr_mi_cnt",
    "new_permit_qtr_mi_cnt"
    }  # don't ROUND (would coerce INT->FLOAT)
INDEX_COLUMNS = ["demographic_index", "growth_index", "telecom_index", "fiber_potential_index"]
INDEX_BINS = [0, 25, 50, 75, 100]  # fixed score bands; last band is inclusive of 100
ROUND_WEIGHT = 2   # weights emitted as PERCENTAGES (0-100), display-only


def get_bq_client() -> bigquery.Client:
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def _f(x) -> str:
    """Round-trip float literal for embedding in SQL."""
    return repr(float(x))


def _scaled_expr(feature: str, lo: float, hi: float, na_fill: float,
                 invert: bool, alias: str | None = None) -> str:
    """SQL mirror of scaling.apply_scaling for one feature -> value in [0,1]."""
    col = f"{alias}.`{feature}`" if alias else f"`{feature}`"
    xf = f"CAST({col} AS FLOAT64)"
    clean = f"COALESCE(IF(IS_NAN({xf}) OR IS_INF({xf}), NULL, {xf}), {_f(na_fill)})"
    denom = hi - lo
    if denom > 0:
        s = f"(LEAST(GREATEST({clean}, {_f(lo)}), {_f(hi)}) - {_f(lo)}) / {_f(denom)}"
    else:
        s = "0.0"  # constant feature -> contributes nothing
    return f"(1.0 - ({s}))" if invert else s


def _bucket_raw_expr(bucket: str, params_by_feat: dict, w_in_bucket: dict) -> str:
    """Within-bucket weighted sum of scaled features -> raw sub-index in [0,1]."""
    terms = []
    for f in SCORING_BUCKETS[bucket]:
        p = params_by_feat[f]
        se = _scaled_expr(f, p["min_val"], p["max_val"], p["na_fill"], bool(p["invert"]))
        terms.append(f"({_f(w_in_bucket[f])} * {se})")
    return "\n      + ".join(terms)


def build_scoring_query(
    features_table: str,
    output_table: str,
    params,            # scaling_params frame
    weights,           # feature_weights frame
    run_id: str,
) -> str:
    params_by_feat = params.set_index("feature").to_dict("index")
    w_in_bucket = weights.set_index("feature")["weight_in_bucket"].to_dict()

    # bucket weights, renormalized to sum to 1 (defensive; persisted values already do)
    bw = weights.groupby("bucket")["bucket_weight"].first().to_dict()
    bw_sum = sum(bw.values())
    bw = {b: v / bw_sum for b, v in bw.items()}

    missing_p = set(ALL_SCORING_FEATURES) - set(params_by_feat)
    missing_w = set(ALL_SCORING_FEATURES) - set(w_in_bucket)
    if missing_p:
        raise KeyError(f"scaling_params missing features for run {run_id}: {sorted(missing_p)}")
    if missing_w:
        raise KeyError(f"feature_weights missing features for run {run_id}: {sorted(missing_w)}")

    raw_growth = _bucket_raw_expr("growth", params_by_feat, w_in_bucket)
    raw_telecom = _bucket_raw_expr("telecom", params_by_feat, w_in_bucket)
    raw_demo = _bucket_raw_expr("demo", params_by_feat, w_in_bucket)

    # overall = bucket-weighted sum of the 0-100 sub-indices, built from the SAME bw
    # dict used everywhere else (no hard-coded weights), then min-max rescaled to
    # 0-100 over the population — same pattern as the per-bucket rescale above.
    overall_expr = (f"{_f(bw['growth'])} * idx_growth"
                    f" + {_f(bw['telecom'])} * idx_telecom"
                    f" + {_f(bw['demo'])} * idx_demo")

    return f"""\
CREATE OR REPLACE TABLE `{output_table}`
CLUSTER BY block_geoid AS
WITH subidx AS (
  SELECT
    parcel_shape_id, block_geoid, tract_geoid,
    {raw_growth} AS raw_growth,
    {raw_telecom} AS raw_telecom,
    {raw_demo} AS raw_demo
  FROM `{features_table}`
),
bounds AS (
  SELECT
    MIN(raw_growth) AS g_min, MAX(raw_growth) AS g_max,
    MIN(raw_telecom) AS t_min, MAX(raw_telecom) AS t_max,
    MIN(raw_demo) AS d_min, MAX(raw_demo) AS d_max
  FROM subidx
),
rescaled AS (
  SELECT
    s.parcel_shape_id, s.block_geoid, s.tract_geoid,
    s.raw_growth, s.raw_telecom, s.raw_demo,
    CASE WHEN (b.g_max - b.g_min) > 0
         THEN 100.0 * (s.raw_growth - b.g_min) / (b.g_max - b.g_min) ELSE 0.0 END AS idx_growth,
    CASE WHEN (b.t_max - b.t_min) > 0
         THEN 100.0 * (s.raw_telecom - b.t_min) / (b.t_max - b.t_min) ELSE 0.0 END AS idx_telecom,
    CASE WHEN (b.d_max - b.d_min) > 0
         THEN 100.0 * (s.raw_demo - b.d_min) / (b.d_max - b.d_min) ELSE 0.0 END AS idx_demo
  FROM subidx s CROSS JOIN bounds b
),
overall AS (
  SELECT *, ({overall_expr}) AS raw_overall FROM rescaled
),
obounds AS (
  SELECT MIN(raw_overall) AS o_min, MAX(raw_overall) AS o_max FROM overall
)
SELECT
  o.parcel_shape_id, o.block_geoid, o.tract_geoid,
  ROUND(o.raw_growth, {ROUND_VALUE}) AS raw_growth,
  ROUND(o.raw_telecom, {ROUND_VALUE}) AS raw_telecom,
  ROUND(o.raw_demo, {ROUND_VALUE}) AS raw_demo,
  ROUND(o.idx_growth, {ROUND_INDEX}) AS idx_growth,
  ROUND(o.idx_telecom, {ROUND_INDEX}) AS idx_telecom,
  ROUND(o.idx_demo, {ROUND_INDEX}) AS idx_demo,
  ROUND(CASE WHEN (ob.o_max - ob.o_min) > 0
             THEN 100.0 * (o.raw_overall - ob.o_min) / (ob.o_max - ob.o_min)
             ELSE 0.0 END, {ROUND_INDEX}) AS idx_overall,
  '{run_id}' AS run_id,
  CURRENT_TIMESTAMP() AS created_at
FROM overall o CROSS JOIN obounds ob
"""

def _filled_raw_expr(feature: str, lo: float, hi: float, na_fill: float, alias: str) -> str:
    """Fill NaN/Inf -> na_fill, then winsorize into [lo, hi] — the RAW value that
    actually fed the index (no min-max, no invert). parcel_features keeps the true
    unclipped raw for drift/null monitoring; this fill+clip view lives only in
    parcel_scores / fiber_idx_v1_parcel so we can measure the impact of the clip rules."""
    xf = f"CAST({alias}.`{feature}` AS FLOAT64)"
    clean = f"COALESCE(IF(IS_NAN({xf}) OR IS_INF({xf}), NULL, {xf}), {_f(na_fill)})"
    return f"LEAST(GREATEST({clean}, {_f(lo)}), {_f(hi)})"


def _provider_type_code_expr(alias: str) -> str:
    """provider_competitive_landscape_ord -> zero-padded STRING type code '00'..'06'.
    A NULL/'other' ordinal maps to '00' (its na_fill = 0.0) so the column has no nulls."""
    ord_col = f"CAST({alias}.`provider_competitive_landscape_ord` AS INT64)"
    return f"LPAD(CAST(COALESCE({ord_col}, 0) AS STRING), 2, '0')"


def build_delivery_query(
    features_table: str,
    scores_table: str,
    growth_table: str,
    output_table: str,
    params,            # scaling_params frame
    weights,           # feature_weights frame
    run_id: str,
) -> str:
    """Assemble the customer-facing fiber_idx_v1_parcel table (raw + scaled + weights + indices + geo)."""
    params_by_feat = params.set_index("feature").to_dict("index")
    w_in_bucket = weights.set_index("feature")["weight_in_bucket"].to_dict()
    bw = weights.groupby("bucket")["bucket_weight"].first().to_dict()

    missing = set(ALL_SCORING_FEATURES) - (set(params_by_feat) & set(w_in_bucket))
    if missing:
        raise KeyError(f"params/weights missing features for run {run_id}: {sorted(missing)}")

    cols = []
    # identity + geo
    cols.append("pf.parcel_shape_id")
    cols.append("g.parcel_polygon AS geometry")
    cols.append("pf.block_geoid AS census_block_id")
    cols.append("g.h3_res8 AS h3_id")
    cols.append("CAST(pf.nearest_fiber_id AS STRING) AS nearest_fiber_id")

    # indices (0-100) -> 2 dp
    cols.append(f"ROUND(ps.idx_demo, {ROUND_INDEX}) AS demographic_index")
    cols.append(f"ROUND(ps.idx_growth, {ROUND_INDEX}) AS growth_index")
    cols.append(f"ROUND(ps.idx_telecom, {ROUND_INDEX}) AS telecom_index")
    cols.append(f"ROUND(ps.idx_overall, {ROUND_INDEX}) AS fiber_potential_index")

    # bucket weights as PERCENTAGES (0-100), 2 dp — display only; scoring uses fractions
    cols.append(f"{_f(round(bw['demo'] * 100, ROUND_WEIGHT))} AS demographic_weight")
    cols.append(f"{_f(round(bw['growth'] * 100, ROUND_WEIGHT))} AS growth_weight")
    cols.append(f"{_f(round(bw['telecom'] * 100, ROUND_WEIGHT))} AS telecom_weight")

    # raw feature values: fill NA + winsorize/clip per scaling_params (distances in
    # MILES; provider -> zero-padded type code '00'..'06'; census_housing_units kept
    # INTEGER). NOTE: parcel_features holds the true UNCLIPPED raw for drift/null
    # monitoring; here we deliver the fill+clip values so the delivered raw matches
    # exactly what fed the index (lets us monitor the impact of the clip rules).
    for f in ALL_SCORING_FEATURES:
        dn = DELIVERY_FEATURE_NAMES[f]
        if f == "provider_competitive_landscape_ord":
            cols.append(f"{_provider_type_code_expr('pf')} AS {dn}")
            continue
        p = params_by_feat[f]
        fr = _filled_raw_expr(f, p["min_val"], p["max_val"], p["na_fill"], alias="pf")
        if f in INTEGER_RAW_FEATURES:
            cols.append(f"CAST(ROUND({fr}) AS INT64) AS {dn}")
        else:
            cols.append(f"ROUND({fr}, {ROUND_VALUE}) AS {dn}")

    # per-feature weights (within-bucket; constants; pre-rounded)
    # per-feature weights as PERCENTAGES within bucket (0-100), 2 dp
    for f in ALL_SCORING_FEATURES:
        dn = DELIVERY_FEATURE_NAMES[f]
        cols.append(f"{_f(round(w_in_bucket[f] * 100, ROUND_WEIGHT))} AS {dn}_weight")

    # per-feature scaled [0,1] -> 4 dp
    for f in ALL_SCORING_FEATURES:
        dn = DELIVERY_FEATURE_NAMES[f]
        p = params_by_feat[f]
        se = _scaled_expr(f, p["min_val"], p["max_val"], p["na_fill"], bool(p["invert"]), alias="pf")
        cols.append(f"ROUND({se}, {ROUND_VALUE}) AS {dn}_scaled")

    # extra raw passthrough
    cols.append("g.growth_parcel_qtr_mi_cnt AS growth_parcels_qtr_mi_cnt")

    # run identity
    cols.append(f"'{run_id}' AS run_id")
    cols.append("CURRENT_TIMESTAMP() AS created_at")

    select_block = ",\n  ".join(cols)
    return f"""\
CREATE OR REPLACE TABLE `{output_table}`
CLUSTER BY census_block_id AS
SELECT
  {select_block}
FROM `{features_table}` pf
LEFT JOIN `{scores_table}` ps ON ps.parcel_shape_id = pf.parcel_shape_id
LEFT JOIN `{growth_table}` g  ON g.parcel_shape_id  = pf.parcel_shape_id
"""

def run(run_id: str, dry_run: bool = False) -> None:
    features_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_FEATURES}"
    scaling_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_SCALING_PARAMS}"
    weights_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_FEATURE_WEIGHTS}"
    output_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_PARCEL_SCORES}"

    logger.info(f"Features: {features_table}")
    logger.info(f"Output:   {output_table}")
    logger.info(f"run_id:   {run_id}")

    client = get_bq_client()
    params = read_scaling_params(client, scaling_table, run_id)
    weights = read_feature_weights(client, weights_table, run_id)
    if params.empty:
        raise ValueError(f"No scaling_params rows for run_id={run_id}. Run build_scaling_params first.")
    if weights.empty:
        raise ValueError(f"No feature_weights rows for run_id={run_id}. Run build_weights first.")

    sql = build_scoring_query(features_table, output_table, params, weights, run_id)
    if dry_run:
        logger.info("Dry run — rendered scoring SQL:")
        print(sql)
        return
    logger.info("Executing scoring query...")
    client.query(sql).result()
    logger.info("Done.")


def run_delivery(run_id: str, dry_run: bool = False) -> None:
    features_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_FEATURES}"
    growth_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_PARCEL_GROWTH}"
    scaling_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_SCALING_PARAMS}"
    weights_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_ANALYTICS}.{BQ_TABLE_FEATURE_WEIGHTS}"
    scores_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_PARCEL_SCORES}"
    output_table = f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_FIBER_IDX_PARCEL}"

    logger.info(f"Delivery output: {output_table}")
    logger.info(f"run_id:          {run_id}")

    client = get_bq_client()
    params = read_scaling_params(client, scaling_table, run_id)
    weights = read_feature_weights(client, weights_table, run_id)
    if params.empty or weights.empty:
        raise ValueError(f"Missing scaling_params/feature_weights for run_id={run_id}.")

    sql = build_delivery_query(
        features_table, scores_table, growth_table, output_table, params, weights, run_id
    )
    if dry_run:
        logger.info("Dry run — rendered delivery SQL:")
        print(sql)
        return
    logger.info("Building delivery table...")
    client.query(sql).result()
    logger.info("Done.")

def _write_qa(client: bigquery.Client, df: pd.DataFrame, table_id: str, run_id: str) -> None:
    """Replace this run's QA rows (delete-then-append)."""
    try:
        client.query(
            f"DELETE FROM `{table_id}` WHERE run_id = @run_id",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            ]),
        ).result()
    except Exception as e:  # table may not exist yet
        logger.info(f"Skipping delete (table may not exist yet): {e}")
    client.load_table_from_dataframe(
        df, table_id,
        job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND),
    ).result()
    logger.info(f"Wrote {len(df)} QA rows to {table_id}")


def run_qa(run_id: str, dry_run: bool = False) -> None:
    """Profile the delivery table: Table 1 = min/max (numeric cols), Table 2 = fill rates (all cols)."""
    target = f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_FIBER_IDX_PARCEL}"
    minmax_out = f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_FIBER_IDX_PARCEL_QA_MINMAX}"
    fill_out = f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_FIBER_IDX_PARCEL_QA_FILLRATES}"

    client = get_bq_client()
    schema = client.get_table(target).schema
    numeric_cols = [f.name for f in schema if f.field_type in NUMERIC_BQ_TYPES]
    all_cols = [f.name for f in schema]

    # single-scan wide aggregations
    mm_parts = []
    for c in numeric_cols:
        mm_parts.append(f"MIN(`{c}`) AS `{c}__min`")
        mm_parts.append(f"MAX(`{c}`) AS `{c}__max`")
    minmax_sql = "SELECT\n  " + ",\n  ".join(mm_parts) + f"\nFROM `{target}`"

    fill_parts = ["COUNT(*) AS `__total`"] + [f"COUNT(`{c}`) AS `{c}__nn`" for c in all_cols]
    fill_sql = "SELECT\n  " + ",\n  ".join(fill_parts) + f"\nFROM `{target}`"

    logger.info(f"QA target: {target}")
    if dry_run:
        logger.info("Dry run — min/max SQL:")
        print(minmax_sql)
        logger.info("Dry run — fill-rate SQL:")
        print(fill_sql)
        return

    now = pd.Timestamp.now(tz="UTC")

    mm = list(client.query(minmax_sql).result())[0]
    minmax_df = pd.DataFrame([{
        "run_id": run_id,
        "column_name": c,
        "min_val": None if mm[f"{c}__min"] is None else float(mm[f"{c}__min"]),
        "max_val": None if mm[f"{c}__max"] is None else float(mm[f"{c}__max"]),
        "created_at": now,
    } for c in numeric_cols])

    fr = list(client.query(fill_sql).result())[0]
    total = int(fr["__total"])
    fill_df = pd.DataFrame([{
        "run_id": run_id,
        "column_name": c,
        "non_null": int(fr[f"{c}__nn"]),
        "total": total,
        "fill_rate": round(fr[f"{c}__nn"] / total, 4) if total else None,
        "created_at": now,
    } for c in all_cols])

    print(minmax_df.to_string(index=False))
    print(fill_df.to_string(index=False))
    _write_qa(client, minmax_df, minmax_out, run_id)
    _write_qa(client, fill_df, fill_out, run_id)

    buckets_out = f"{GCS_PROJECT_ID}.{BQ_DATASET_OUTPUTS}.{BQ_TABLE_FIBER_IDX_PARCEL_QA_INDEX_BUCKETS}"

    band_parts = []
    for c in INDEX_COLUMNS:
        for i in range(len(INDEX_BINS) - 1):
            lo, hi = INDEX_BINS[i], INDEX_BINS[i + 1]
            op = "<=" if i == len(INDEX_BINS) - 2 else "<"  # last band inclusive of 100
            band_parts.append(f"COUNTIF(`{c}` >= {lo} AND `{c}` {op} {hi}) AS `{c}__b{i}`")
    bands_sql = ("SELECT\n  COUNT(*) AS `__total`,\n  "
                 + ",\n  ".join(band_parts) + f"\nFROM `{target}`")

    if dry_run:
        logger.info("Dry run — index-band SQL:")
        print(bands_sql)
        # (return already happened above for min/max + fill; keep the earlier return)

    br = list(client.query(bands_sql).result())[0]
    total_b = int(br["__total"])
    bucket_rows = []

    for c in INDEX_COLUMNS:
        for i in range(len(INDEX_BINS) - 1):
            lo, hi = INDEX_BINS[i], INDEX_BINS[i + 1]
            cnt = int(br[f"{c}__b{i}"])
            bucket_rows.append({
                "run_id": run_id,
                "index_name": c,
                "band": f"{lo}-{hi}",
                "parcel_count": cnt,
                "total": total_b,
                "pct": round(cnt / total_b, 4) if total_b else None,
                "created_at": now,
            })
    buckets_df = pd.DataFrame(bucket_rows)
    print(buckets_df.to_string(index=False))
    _write_qa(client, buckets_df, buckets_out, run_id)

    logger.info("QA done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Score all parcels / build the delivery table in BigQuery.")
    parser.add_argument("--run-id", default=SCORING_RUN_ID, help="Scoring run identifier.")
    parser.add_argument("--delivery", action="store_true", default=False,
                        help="Build the customer fiber_idx_v1_parcel table (requires parcel_scores to exist).")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Print the rendered SQL without executing it.")
    parser.add_argument("--qa", action="store_true", default=False,
                        help="Profile the delivery table: min/max + fill-rate QA tables.")
    args = parser.parse_args()
    if args.qa:
        run_qa(run_id=args.run_id, dry_run=args.dry_run)
    elif args.delivery:
        run_delivery(run_id=args.run_id, dry_run=args.dry_run)
    else:
        run(run_id=args.run_id, dry_run=args.dry_run)