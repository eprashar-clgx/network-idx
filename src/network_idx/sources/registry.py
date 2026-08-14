"""
Registry of raw sources for the network_idx pipeline.

This module names every raw source the pipeline can ingest and records where each
one physically lives, so that callers ask for a source by a stable logical name and
never hard-code a table reference or a download path. Two kinds of source are
described here. BigQuery-production sources (the authoritative FCC and demographic
raw data, and the census tract geometry used as a reference for spatial joins) are
described by a project, dataset, and table. Download sources (the Census families,
which are still fetched from the Census website) point at the command that produces
them locally. The physical identifiers themselves come from the config package, so
this registry stays a thin logical-to-physical map.

Location raw sources are the in-house property-pipeline production views the growth
features read from; they are registered here alongside the FCC and demographic
sources. Rextag raw sources are an in-house pipeline whose raw identifiers have not
been wired in yet; they are intentionally left out of the registry for now and
tracked as a to-do.
"""
from dataclasses import dataclass

from network_idx import config


@dataclass(frozen=True)
class BQSource:
    """A raw source that lives as a BigQuery table or view."""

    project: str
    dataset: str
    table: str

    @property
    def table_ref(self) -> str:
        """Return the fully qualified `project.dataset.table` reference."""
        return f"{self.project}.{self.dataset}.{self.table}"


@dataclass(frozen=True)
class DownloadSource:
    """A raw source that is fetched to local disk by a command-line entry point."""

    command_hint: str


# BigQuery-production raw sources, keyed by their stable logical name.
RAW_SOURCES_BQ = {
    "fcc_copper": BQSource(
        config.BQ_PROJECT_PROD, config.BQ_PROD_DATASET_FCC, config.BQ_PROD_TABLE_FCC_COPPER
    ),
    "fcc_cable": BQSource(
        config.BQ_PROJECT_PROD, config.BQ_PROD_DATASET_FCC, config.BQ_PROD_TABLE_FCC_CABLE
    ),
    "fcc_fiber": BQSource(
        config.BQ_PROJECT_PROD, config.BQ_PROD_DATASET_FCC, config.BQ_PROD_TABLE_FCC_FIBER
    ),
    "fcc_geography": BQSource(
        config.BQ_PROJECT_PROD, config.BQ_PROD_DATASET_FCC, config.BQ_PROD_TABLE_FCC_GEOGRAPHY
    ),
    "fcc_summary": BQSource(
        config.BQ_PROJECT_PROD, config.BQ_PROD_DATASET_FCC, config.BQ_PROD_TABLE_FCC_SUMMARY
    ),
    "neighborhood_scout_tract": BQSource(
        config.BQ_PROJECT_PROD,
        config.BQ_PROD_DATASET_NEIGHBORHOOD,
        config.BQ_PROD_TABLE_NEIGHBORHOOD_SCOUT_CT,
    ),
    "tract_geometry": BQSource(
        config.BQ_PROJECT_PROD,
        config.BQ_PROD_DATASET_REFERENCE,
        config.BQ_PROD_VIEW_TRACT_GEOMETRY,
    ),
    # Location — in-house property-pipeline production views.
    "parcel_universe": BQSource(
        config.BQ_PROJECT_PROD,
        config.BQ_PROD_DATASET_PROPERTY_PIPELINE,
        config.BQ_PROD_VIEW_PARCEL_UNIVERSE,
    ),
    "clip_to_parcel": BQSource(
        config.BQ_PROJECT_PROD,
        config.BQ_PROD_DATASET_PROPERTY_PIPELINE,
        config.BQ_PROD_VIEW_CLIP_TO_PARCEL,
    ),
    "parcel_lineage_event": BQSource(
        config.BQ_PROJECT_PROD,
        config.BQ_PROD_DATASET_PROPERTY_PIPELINE,
        config.BQ_PROD_VIEW_PARCEL_LINEAGE_EVENT,
    ),
    "growth_indicators": BQSource(
        config.BQ_PROJECT_PROD,
        config.BQ_PROD_DATASET_PROPERTY_ENRICHED,
        config.BQ_PROD_VIEW_GROWTH_INDICATORS,
    ),
    "property": BQSource(
        config.BQ_PROJECT_PROD,
        config.BQ_PROD_DATASET_PROPERTY_FULFILLMENT,
        config.BQ_PROD_VIEW_PROPERTY,
    ),
    "block_geometry": BQSource(
        config.BQ_PROJECT_PROD,
        config.BQ_PROD_DATASET_REFERENCE,
        config.BQ_PROD_VIEW_BLOCK_GEOMETRY,
    ),
}

# Download raw sources, keyed by their stable logical name.
RAW_SOURCES_DOWNLOAD = {
    "census_baf": DownloadSource(
        "python -m network_idx.sources.data_download.census_baf"
    ),
    "census_acl": DownloadSource(
        "python -m network_idx.sources.data_download.census_addresscountlisting"
    ),
}
