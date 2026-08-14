"""
Rextag transformed feature: fiber-optic cable cleanup and optimisation.

This transform deterministically reshapes the raw fiber-optic cable geometry into a
form the distance features can join against efficiently: multi-line geometries are
split into single lines, identical paths are de-duplicated, and very long lines are
subdivided. Because it involves no analytical choice, it lives in the transform layer.

Unlike the demographic and location features (which are single set-based queries), the
rextag steps are kept as BigQuery stored procedures so the data-engineering pipeline
can chain them with CALL. This module therefore has a deploy-and-run shape: it renders
the CREATE OR REPLACE PROCEDURE statement from configuration, deploys it to the target
environment's project, and can then issue the CALL that materialises the optimised
table. The rendering is separated from execution so the SQL can be inspected without a
BigQuery client, and the client is injected so production supplies a real client while
tests supply a fake one.
"""
import argparse
import logging
from pathlib import Path

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_TELECOM,
    BQ_TABLE_REXTAG_FIBER_OPTIMIZED,
    BQ_DATASET_BOUNDARY,
)
from network_idx.constants import FIBER_SUBDIVIDE_MAX_VERTICES
from network_idx.sources.registry import RAW_SOURCES_BQ
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SQL_PATH = Path(__file__).parent / "fiber_optimize.sql"

# The name of the stored procedure this module deploys (unqualified).
PROCEDURE_NAME = "clean_optimize_rextag_fiberopticcables"

# The logical name of the raw rextag fiber source in the registry.
SOURCE_NAME = "rextag_fiber"


def proc_ref() -> str:
    """Return the fully qualified name of the stored procedure this module deploys."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_TELECOM}.{PROCEDURE_NAME}"


def output_table_ref() -> str:
    """Return the fully qualified optimised-fiber table the procedure writes to."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_TELECOM}.{BQ_TABLE_REXTAG_FIBER_OPTIMIZED}"


def input_view_ref() -> str:
    """Return the fully qualified raw rextag fiber view from the registry."""
    return RAW_SOURCES_BQ[SOURCE_NAME].table_ref


def render_procedure_sql(
    proc_ref_: str,
    output_table: str,
    input_view: str,
    boundary_dataset: str = BQ_DATASET_BOUNDARY,
    subdivide_max_vertices: int = FIBER_SUBDIVIDE_MAX_VERTICES,
) -> str:
    """
    Render the CREATE OR REPLACE PROCEDURE statement that defines the transform.

    This is a pure function: it reads the SQL template and substitutes the procedure
    name, the output table, the input view, the dataset that hosts the subdivision UDF,
    and the vertex threshold, performing no input or output of its own so it can be unit
    tested.
    """
    template = SQL_PATH.read_text()
    return template.format(
        proc_ref=proc_ref_,
        output_table=output_table,
        input_view=input_view,
        boundary_dataset=boundary_dataset,
        subdivide_max_vertices=subdivide_max_vertices,
    )


def render_call_sql() -> str:
    """Return the CALL statement that runs the deployed procedure."""
    return f"CALL `{proc_ref()}`();"


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(client=None, dry_run: bool = False, deploy_only: bool = False) -> None:
    """
    Deploy the fiber-optimize procedure and, unless deploy_only, run it.

    The procedure and table references are resolved from configuration and the source
    registry, the CREATE OR REPLACE PROCEDURE statement is rendered and — unless this is
    a dry run — executed to (re)deploy the procedure, and then the CALL is issued to
    materialise the optimised table. Passing deploy_only deploys the procedure without
    running it, which is what a data-engineering pipeline that owns the CALL chain wants.
    When no client is given and this is not a dry run, an authenticated client is created.
    """
    procedure_sql = render_procedure_sql(
        proc_ref_=proc_ref(),
        output_table=output_table_ref(),
        input_view=input_view_ref(),
    )
    call_sql = render_call_sql()

    logger.info(f"Procedure:    {proc_ref()}")
    logger.info(f"Input view:   {input_view_ref()}")
    logger.info(f"Output table: {output_table_ref()}")

    if dry_run:
        logger.info("Dry run — rendered procedure and call:")
        print(procedure_sql)
        print(call_sql)
        return

    if client is None:
        client = get_bq_client()

    logger.info("Deploying procedure...")
    client.query(procedure_sql).result()

    if deploy_only:
        logger.info("Deploy-only — procedure deployed, not called.")
        return

    logger.info("Running procedure...")
    client.query(call_sql).result()
    logger.info(f"Done. Table {output_table_ref()} created/replaced.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Deploy and run the fiber-optimize procedure in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered procedure and call without executing them.",
    )
    parser.add_argument(
        "--deploy-only", action="store_true", default=False,
        help="Deploy the procedure without calling it.",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run, deploy_only=args.deploy_only)
