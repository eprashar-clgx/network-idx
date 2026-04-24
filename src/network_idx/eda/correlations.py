"""
Correlation analysis utilities for the network-idx feature table.

All functions expect a pandas DataFrame with tract-level features
(output of all_features_tract.sql). The geometry and tract_geoid
columns are automatically excluded from numeric analysis.
"""

from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery

from network_idx.config import (
    NETWORK_IDX_ENV,
    GCS_PROJECT_ID,
    GCS_ADC_JSON_PATH_EP_LOCAL,
    BQ_DATASET_FEATURES,
    BQ_TABLE_ALL_FEATURES_TRACT
    )
from network_idx.utils import check_and_authenticate

# Columns to exclude from correlation analysis
_NON_NUMERIC_COLS = ['tract_geoid', 'geometry']

# ── Known feature groups (for grouped correlation) 
COVERAGE_SPEED_BUCKET_COLS = [
    "copper_speed_less_than_100_20",
    "copper_speed_100_20_only",
    "copper_speed_more_than_100_20",
    "cable_speed_less_than_100_20",
    "cable_speed_100_20_only",
    "cable_speed_more_than_100_20",
    "fiber_speed_less_than_100_20",
    "fiber_speed_100_20_only",
    "fiber_speed_more_than_100_20",
    "fiber_speed_equal_greater_than_100_20",
]

SPEED_PROVIDER_COLS = [
    "cable_location_count",
    "cable_provider_count",
    "cable_max_download_speed",
    "cable_max_upload_speed",
    "copper_location_count",
    "copper_provider_count",
    "copper_max_download_speed",
    "copper_max_upload_speed",
    "fiber_location_count",
    "fiber_provider_count",
    "fiber_max_download_speed",
    "fiber_max_upload_speed",
]

DEMOGRAPHIC_COLS = [
    "pop_ch_1yr",
    "pop_pctch_1yr",
]

UNIT_ESTIMATE_COLS = [
    "estimated_census_housing_units",
    "estimated_fcc_units",
]

GROWTH_COLS = [
    "pre_early_dev_parcels",
    "unique_locations",
    "builder_developer_count",
    "landuse_change_count",
    "building_permit_count",
    "new_clip_count",
    "parcel_split_count",
    "mean_dist_nearest_hotspot_m",
    "median_dist_nearest_hotspot"
]

REXTAG_COLS = [
    "mean_dist_nearest_fiber_m",
    "median_dist_nearest_fiber_m",
    "mean_radius_fiber_count",
    "median_radius_fiber_count"
]

# Adding helpers
def fetch_data_from_bq() -> pd.DataFrame:
    """Fetch the all-features tract table from BigQuery and return as a DataFrame.

    Provide either *sql_path* (reads SQL from file) or *query* (inline SQL string).
    If neither is given, defaults to reading all_features_tract.sql.
    """
    if NETWORK_IDX_ENV == "local":
        check_and_authenticate(GCS_ADC_JSON_PATH_EP_LOCAL)

    table_id = f"{GCS_PROJECT_ID}.{BQ_DATASET_FEATURES}.{BQ_TABLE_ALL_FEATURES_TRACT}"
    client = bigquery.Client(project=GCS_PROJECT_ID)
    # Exclude geometry and for now, Rextag distance columns for correlation analysis
    query = f"""
        SELECT * EXCEPT(
            mean_dist_nearest_fiber_m,
            median_dist_nearest_fiber_m,
            mean_radius_fiber_count,
            median_radius_fiber_count,
            geometry
        )
        FROM `{table_id}`
    """
    return client.query(query).to_arrow().to_pandas()
    
def _numeric_cols(df: pd.DataFrame) -> list:
    """Return list of numeric columns for correlation analysis."""
    return [c for c in df.select_dtypes(include="number").columns 
            if c not in _NON_NUMERIC_COLS
            ]

def _validate_columns(df: pd.DataFrame, columns: list[str] | None) -> list[str]:
    """Resolve column list: use provided list or fall back to all numeric cols."""
    if columns is None:
        return _numeric_cols(df)
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(f"Columns not found in DataFrame: {missing}")
    return columns

# Core correlation functions
# One function to compute both pearson and spearman matrices at the outset
def compute_all_correlations(df, columns=None):
    """Return dict of {'pearson': matrix, 'spearman': matrix}."""
    cols = _validate_columns(df, columns)
    return {m: df[cols].corr(method=m) for m in ["pearson", "spearman"]}

# Choosing one correlation method and passing that downstream
def compute_correlation_matrix(
        df: pd.DataFrame, 
        columns: list[str] | None = None,
        method:str = "pearson",
    ) -> pd.DataFrame:
    """
    Compute a correlation matrix for the given columns.

    Parameters
    ----------
    df : pd.DataFrame
        Tract-level feature table.
    columns : list[str] | None
        Columns to include. Defaults to all numeric columns.
    method : str
        Correlation method – 'pearson', 'spearman', or 'kendall'.

    Returns
    -------
    pd.DataFrame  (n×n correlation matrix)
    """
    cols = _validate_columns(df, columns)
    return df[cols].corr(method=method)

def get_top_correlations(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    method: str = "pearson",
    n: int = 20,
    min_abs_corr: float = 0.0,
) -> pd.DataFrame:
    """
    Return the top-n strongest pairwise correlations (excluding self-pairs).

    Parameters
    ----------
    n : int
        Number of pairs to return.
    min_abs_corr : float
        Minimum |r| threshold to include a pair.

    Returns
    -------
    pd.DataFrame  with columns [feature_1, feature_2, correlation].
    """
    corr = compute_correlation_matrix(df, columns=columns, method=method)

    # Upper triangle only (no duplicates, no diagonal)
    mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
    pairs = (
        corr.where(mask)
        .stack()
        .reset_index()
    )
    pairs.columns = ["feature_1", "feature_2", "correlation"]
    pairs["abs_corr"] = pairs["correlation"].abs()
    pairs = pairs[pairs["abs_corr"] >= min_abs_corr]
    return (
        pairs
        .sort_values("abs_corr", ascending=False)
        .head(n)
        .drop(columns="abs_corr")
        .reset_index(drop=True)
    )


def correlations_with_target(
    df: pd.DataFrame,
    target: str,
    columns: list[str] | None = None,
    method: str = "pearson",
) -> pd.DataFrame:
    """
    Correlate every feature against a single target column, sorted by |r|.

    Parameters
    ----------
    target : str
        The column to correlate against.

    Returns
    -------
    pd.DataFrame  with columns [feature, correlation], sorted descending by |r|.
    """
    cols = _validate_columns(df, columns)
    if target not in df.columns:
        raise ValueError(f"Target column '{target}' not found in DataFrame")
    cols = [c for c in cols if c != target]

    corr_values = df[cols].corrwith(df[target], method=method)
    result = (
        corr_values
        .reset_index()
        .rename(columns={"index": "feature", 0: "correlation"})
        .assign(abs_corr=lambda x: x["correlation"].abs())
        .sort_values("abs_corr", ascending=False)
        .drop(columns="abs_corr")
        .reset_index(drop=True)
    )
    return result


# ── Visualization ─────────────────────────────────────────────────────────────

def plot_correlation_heatmap(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    method: str = "pearson",
    figsize: tuple[int, int] = (16, 14),
    annot: bool = True,
    fmt: str = ".2f",
    cmap: str = "RdYlGn",
    title: str | None = None,
) -> plt.Figure:
    """
    Plot a seaborn heatmap of the correlation matrix.

    Returns the matplotlib Figure so callers can save / display as needed.
    """
    corr = compute_correlation_matrix(df, columns=columns, method=method)
    fig, ax = plt.subplots(figsize=figsize)
    sns.heatmap(
        corr,
        annot=annot,
        fmt=fmt,
        cmap=cmap,
        center=0,
        vmin=-1,
        vmax=1,
        square=True,
        linewidths=0.5,
        ax=ax,
    )
    ax.set_title(title or f"{method.title()} Correlation Matrix", fontsize=14)
    plt.tight_layout()
    return fig


def plot_target_correlations(
    df: pd.DataFrame,
    target: str,
    columns: list[str] | None = None,
    method: str = "pearson",
    n: int = 20,
    figsize: tuple[int, int] = (10, 8),
    palette: str = "RdBu_r",
) -> plt.Figure:
    """
    Horizontal bar chart of top-n features correlated with *target*.
    """
    result = correlations_with_target(df, target, columns=columns, method=method)
    plot_df = result.head(n).sort_values("correlation")

    fig, ax = plt.subplots(figsize=figsize)
    colors = sns.color_palette(palette, as_cmap=True)(
        (plot_df["correlation"].values + 1) / 2   # map [-1,1] → [0,1]
    )
    ax.barh(plot_df["feature"], plot_df["correlation"], color=colors)
    ax.set_xlabel(f"{method.title()} r")
    ax.set_title(f"Top {n} features correlated with '{target}'")
    ax.axvline(0, color="black", linewidth=0.8)
    plt.tight_layout()
    return fig


def plot_grouped_heatmaps(
    df: pd.DataFrame,
    method: str = "pearson",
    figsize_per_group: tuple[int, int] = (12, 10),
) -> list[plt.Figure]:
    """
    Plot separate heatmaps for each predefined feature group that exists in *df*.

    Returns a list of figures (one per group).
    """
    groups = {
        "Coverage Speed Buckets": COVERAGE_SPEED_BUCKET_COLS,
        "Speed & Providers": SPEED_PROVIDER_COLS,
        "Demographics": DEMOGRAPHIC_COLS,
        "Unit Estimates": UNIT_ESTIMATE_COLS,
        "Growth Indicators": GROWTH_COLS,
        "Rextag Proximity": REXTAG_COLS,
    }

    figs = []
    for label, cols in groups.items():
        present = [c for c in cols if c in df.columns]
        if len(present) < 2:
            continue
        fig = plot_correlation_heatmap(
            df,
            columns=present,
            method=method,
            figsize=figsize_per_group,
            title=f"{method.title()} Correlation — {label}",
        )
        figs.append(fig)
    return figs
