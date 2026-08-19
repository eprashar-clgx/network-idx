"""
Rextag engineered feature: parcel-to-fiber distance.

This feature measures, for every parcel, the distance in miles to the nearest fiber
line and the count of distinct fiber lines within a radius, producing the scoring
feature `dist_to_nearest_fiber_miles` (plus the auxiliary `radius_fiber_count` and
`nearest_fiber_id`) at the parcel grain. The distance and radius thresholds are
analytical choices made during exploratory analysis, which is why this lives in the
engineered layer.

Like the fiber-optimize transform, the work is kept as BigQuery stored procedures so
the data-engineering pipeline can chain them with CALL, and because the state/shard
orchestration needs imperative control flow that a single set-based query cannot
express. There are three procedures: a sharded worker that computes distances for one
state and shard into a staging table, a driver that loops over the requested states and
calls the worker once per shard, and an assemble step that joins the staging results
back onto the full parcel master and converts metres to miles.

This module therefore has a deploy-and-run shape: it renders the three CREATE OR REPLACE
PROCEDURE statements from configuration, deploys them, and — unless deploy_only — calls
the driver over the requested states and then the assemble step. The states are a
parameter so a caller can launch disjoint batches in parallel. The rendering is
separated from execution so the SQL can be inspected without a BigQuery client, and the
client is injected so production supplies a real client while tests supply a fake one.
"""
import argparse
import logging
from pathlib import Path

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_DATASET_TELECOM,
    BQ_TABLE_PARCEL_GROWTH,
    BQ_TABLE_REXTAG_FIBER_OPTIMIZED,
    BQ_TABLE_REXTAG_CALCULATION_PARCEL,
    BQ_TABLE_REXTAG_DISTANCE_PARCEL,
)
from network_idx.constants import (
    FIBER_MAX_SEARCH_DIST_M,
    FIBER_RADIUS_COUNT_M,
    FIBER_STATE_SHARD_COUNTS,
    FIBER_DEFAULT_SHARD_COUNT,
    METERS_PER_MILE,
    STATE_FIPS,
)
from network_idx.utils import check_and_authenticate

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

WORKER_SQL_PATH = Path(__file__).parent / "fiber_distance_worker.sql"
DRIVER_SQL_PATH = Path(__file__).parent / "fiber_distance_driver.sql"
ASSEMBLE_SQL_PATH = Path(__file__).parent / "fiber_distance_assemble.sql"

# Unqualified names of the three stored procedures this module deploys.
WORKER_PROCEDURE_NAME = "rextag_run_spatial_shard_worker"
DRIVER_PROCEDURE_NAME = "rextag_calculate_parcel_dist_to_fiber"
ASSEMBLE_PROCEDURE_NAME = "rextag_distance_assemble_parcel"

# Default states to process when a caller does not pass an explicit list: the fifty
# states plus DC (FIPS 01-56), excluding the outlying territories which have no parcels.
DEFAULT_STATES = sorted(fips for fips in STATE_FIPS.values() if int(fips) <= 56)


def _features_proc_ref(name: str) -> str:
    """Return a fully qualified reference to a stored procedure in the features dataset."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{name}"


def _features_table_ref(table: str) -> str:
    """Return a fully qualified reference to a table in the features dataset."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{table}"


def worker_proc_ref() -> str:
    """Return the fully qualified name of the sharded worker procedure."""
    return _features_proc_ref(WORKER_PROCEDURE_NAME)


def driver_proc_ref() -> str:
    """Return the fully qualified name of the state/shard driver procedure."""
    return _features_proc_ref(DRIVER_PROCEDURE_NAME)


def assemble_proc_ref() -> str:
    """Return the fully qualified name of the assemble procedure."""
    return _features_proc_ref(ASSEMBLE_PROCEDURE_NAME)


def calc_table_ref() -> str:
    """Return the staging table the worker appends to and the driver manages."""
    return _features_table_ref(BQ_TABLE_REXTAG_CALCULATION_PARCEL)


def parcel_table_ref() -> str:
    """Return the parcel master (growth-counts) table this feature reads parcels from."""
    return _features_table_ref(BQ_TABLE_PARCEL_GROWTH)


def distance_table_ref() -> str:
    """Return the final per-parcel distance-to-fiber table this feature writes to."""
    return _features_table_ref(BQ_TABLE_REXTAG_DISTANCE_PARCEL)


def fiber_optimized_table_ref() -> str:
    """Return the optimised-fiber table the worker joins parcels against."""
    return f"{GCS_PROJECT_ID}.{BQ_DATASET_TELECOM}.{BQ_TABLE_REXTAG_FIBER_OPTIMIZED}"


def render_shard_case() -> str:
    """
    Render the shard-count CASE expression from the per-state shard configuration.

    Each configured state maps to its shard count and every other state falls through
    to the default, producing the SQL that the driver assigns to state_shard_limit.
    """
    lines = ["CASE"]
    for fips, count in FIBER_STATE_SHARD_COUNTS.items():
        lines.append(f"      WHEN current_state = '{fips}' THEN {count}")
    lines.append(f"      ELSE {FIBER_DEFAULT_SHARD_COUNT}")
    lines.append("    END")
    return "\n".join(lines)


def render_worker_sql(
    worker_proc: str,
    calc_table: str,
    parcel_table: str,
    fiber_optimized_table: str,
) -> str:
    """
    Render the worker CREATE OR REPLACE PROCEDURE statement.

    This is a pure function: it reads the worker template and substitutes the procedure
    name, the staging table, the parcel table, and the optimised-fiber table, performing
    no input or output of its own so it can be unit tested.
    """
    template = WORKER_SQL_PATH.read_text()
    return template.format(
        worker_proc_ref=worker_proc,
        calc_table=calc_table,
        parcel_table=parcel_table,
        fiber_optimized_table=fiber_optimized_table,
    )


def render_driver_sql(
    driver_proc: str,
    worker_proc: str,
    calc_table: str,
    max_dist_default: int = FIBER_MAX_SEARCH_DIST_M,
    radius_default: int = FIBER_RADIUS_COUNT_M,
) -> str:
    """
    Render the driver CREATE OR REPLACE PROCEDURE statement.

    This is a pure function: it reads the driver template and substitutes the driver and
    worker procedure names, the staging table, the distance and radius defaults, and the
    shard-count CASE rendered from configuration, performing no input or output of its
    own so it can be unit tested.
    """
    template = DRIVER_SQL_PATH.read_text()
    return template.format(
        driver_proc_ref=driver_proc,
        worker_proc_ref=worker_proc,
        calc_table=calc_table,
        max_dist_default=max_dist_default,
        radius_default=radius_default,
        shard_case=render_shard_case(),
    )


def render_assemble_sql(
    assemble_proc: str,
    distance_table: str,
    parcel_table: str,
    calc_table: str,
    meters_per_mile: float = METERS_PER_MILE,
) -> str:
    """
    Render the assemble CREATE OR REPLACE PROCEDURE statement.

    This is a pure function: it reads the assemble template and substitutes the procedure
    name, the output distance table, the parcel master, the staging table, and the
    metres-per-mile conversion, performing no input or output of its own so it can be
    unit tested.
    """
    template = ASSEMBLE_SQL_PATH.read_text()
    return template.format(
        assemble_proc_ref=assemble_proc,
        distance_table=distance_table,
        parcel_table=parcel_table,
        calc_table=calc_table,
        meters_per_mile=meters_per_mile,
    )


def render_driver_call_sql(
    states=None,
    max_dist_threshold_m: int = FIBER_MAX_SEARCH_DIST_M,
    radius_threshold_m: int = FIBER_RADIUS_COUNT_M,
) -> str:
    """Return the CALL statement that runs the driver over the given states."""
    states = list(states) if states is not None else DEFAULT_STATES
    states_literal = "[" + ", ".join(f"'{s}'" for s in states) + "]"
    return f"CALL `{driver_proc_ref()}`({states_literal}, {max_dist_threshold_m}, {radius_threshold_m});"


def render_assemble_call_sql() -> str:
    """Return the CALL statement that runs the assemble procedure."""
    return f"CALL `{assemble_proc_ref()}`();"


def get_bq_client():
    """Create an authenticated BigQuery client, authenticating first when local."""
    from google.cloud import bigquery

    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)
    return bigquery.Client(project=GCS_PROJECT_ID)


def build(
    client=None,
    dry_run: bool = False,
    deploy_only: bool = False,
    states=None,
) -> None:
    """
    Deploy the three fiber-distance procedures and, unless deploy_only, run them.

    The procedure and table references are resolved from configuration, the three
    CREATE OR REPLACE PROCEDURE statements are rendered and — unless this is a dry run —
    executed to (re)deploy the worker, driver, and assemble procedures. Then, unless
    deploy_only, the driver is called over the requested states (defaulting to the fifty
    states plus DC) to populate the staging table, and the assemble procedure is called
    to build the final per-parcel distance table. Passing deploy_only deploys the
    procedures without running them, which is what a data-engineering pipeline that owns
    the CALL chain wants. When no client is given and this is not a dry run, an
    authenticated client is created.
    """
    worker_sql = render_worker_sql(
        worker_proc=worker_proc_ref(),
        calc_table=calc_table_ref(),
        parcel_table=parcel_table_ref(),
        fiber_optimized_table=fiber_optimized_table_ref(),
    )
    driver_sql = render_driver_sql(
        driver_proc=driver_proc_ref(),
        worker_proc=worker_proc_ref(),
        calc_table=calc_table_ref(),
    )
    assemble_sql = render_assemble_sql(
        assemble_proc=assemble_proc_ref(),
        distance_table=distance_table_ref(),
        parcel_table=parcel_table_ref(),
        calc_table=calc_table_ref(),
    )
    driver_call = render_driver_call_sql(states=states)
    assemble_call = render_assemble_call_sql()

    logger.info(f"Worker proc:    {worker_proc_ref()}")
    logger.info(f"Driver proc:    {driver_proc_ref()}")
    logger.info(f"Assemble proc:  {assemble_proc_ref()}")
    logger.info(f"Parcel table:   {parcel_table_ref()}")
    logger.info(f"Fiber table:    {fiber_optimized_table_ref()}")
    logger.info(f"Staging table:  {calc_table_ref()}")
    logger.info(f"Output table:   {distance_table_ref()}")

    if dry_run:
        logger.info("Dry run — rendered procedures and calls:")
        print(worker_sql)
        print(driver_sql)
        print(assemble_sql)
        print(driver_call)
        print(assemble_call)
        return

    if client is None:
        client = get_bq_client()

    logger.info("Deploying worker procedure...")
    client.query(worker_sql).result()
    logger.info("Deploying driver procedure...")
    client.query(driver_sql).result()
    logger.info("Deploying assemble procedure...")
    client.query(assemble_sql).result()

    if deploy_only:
        logger.info("Deploy-only — procedures deployed, not called.")
        return

    logger.info("Running driver over states...")
    client.query(driver_call).result()
    logger.info("Running assemble...")
    client.query(assemble_call).result()
    logger.info(f"Done. Table {distance_table_ref()} created/replaced.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Deploy and run the parcel-to-fiber distance procedures in BigQuery."
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Print the rendered procedures and calls without executing them.",
    )
    parser.add_argument(
        "--deploy-only", action="store_true", default=False,
        help="Deploy the procedures without calling them.",
    )
    parser.add_argument(
        "--states", nargs="*", default=None,
        help="Two-digit state FIPS codes to process (default: the fifty states plus DC).",
    )
    args = parser.parse_args()
    build(dry_run=args.dry_run, deploy_only=args.deploy_only, states=args.states)
