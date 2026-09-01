"""
Grain transfer: assemble the tract-grain training frame (features_ct).

This module builds ``features_ct``, the tract-grain analogue of ``parcel_features`` and the
frame the model is fit on. The model is fit at tract grain but scores parcels, so this
frame must carry the same thirteen model features as the parcel scoring input — here under
their model-column names (the keys of ``MODEL_TO_SCORING_FEATURE``) which ``train.py``
renames to the canonical scoring names.

It joins the four tract family tables — the telecom (FCC) features, the location growth
aggregation, the rextag fiber-distance aggregation, and the population change — on the
census-tract GEOID. The telecom table is the spine (it covers every populated tract), and
the others are left-joined so a tract is never dropped when a family has no row for it,
yielding all tracts; the training-population filter is applied downstream in modeling.

Following the codebase convention, SQL rendering is separated from execution so the query
can be inspected without a client, and the client is injected so production supplies a real
client while tests supply a fake one.
"""
import argparse
import logging
from pathlib import Path

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_FEATURES_CT,
    BQ_TABLE_TELECOM_FEATURES_CT,
    BQ_TABLE_LOC_PARCELS_GROWTH_CT,
    BQ_TABLE_REXTAG_DISTANCE_CT,
    BQ_TABLE_DEMO_POP_TRACT,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "features_ct.sql"


def _feature_table_ref(table: str) -> str:
    """Return the fully qualified reference for a table in the features dataset."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{table}"


def output_table_ref() -> str:
    """Return the fully qualified BigQuery table this assembly writes to."""
    return _feature_table_ref(BQ_TABLE_FEATURES_CT)


def render_sql(
    output_table: str,
    telecom_features_ct: str,
    loc_growth_ct: str,
    rextag_distance_ct: str,
    demo_pop_ct: str,
) -> str:
    """
    Render the CT training-frame assembly SQL with its source and output tables.

    This is a pure function: it reads the SQL template and substitutes the four tract input
    tables and the output table, performing no input or output of its own so it can be unit
    tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        output_table=output_table,
        telecom_features_ct=telecom_features_ct,
        loc_growth_ct=loc_growth_ct,
        rextag_distance_ct=rextag_distance_ct,
        demo_pop_ct=demo_pop_ct,
    )


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False) -> None:
    """
    Build the tract-grain training frame in BigQuery.

    The output and all four inputs are resolved from configuration in the features dataset,
    the SQL is rendered, and — unless this is a dry run — the query is executed with the
    supplied client. When no client is given and this is not a dry run, an authenticated
    client is created.
    """
    output_table = output_table_ref()
    sql = render_sql(
        output_table=output_table,
        telecom_features_ct=_feature_table_ref(BQ_TABLE_TELECOM_FEATURES_CT),
        loc_growth_ct=_feature_table_ref(BQ_TABLE_LOC_PARCELS_GROWTH_CT),
        rextag_distance_ct=_feature_table_ref(BQ_TABLE_REXTAG_DISTANCE_CT),
        demo_pop_ct=_feature_table_ref(BQ_TABLE_DEMO_POP_TRACT),
    )

    logger.info(f"Output table: {output_table}")

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
        description="Assemble the tract-grain training frame (features_ct) in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered SQL without executing it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run)
