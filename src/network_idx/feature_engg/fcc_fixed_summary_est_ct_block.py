"""
Feature engineering: dasymetric interpolation of FCC coverage to blocks & tracts.

For each block, assigns coverage percentages from either:
    - Case 1 (place block):    block is inside a Census place → use place-level pcts
    - Case 2 (non-place block): block is outside any place → use county residual pcts

Then weights by Census housing units and aggregates to tract level.

Tract-level formula (per metric):
    tract_pct = Σ (block_housing_units × block_pct) / Σ block_housing_units

Inputs:
    data/processed/fcc/broadband_coverage/fcc_fixed_coverage_{USPS}_{FIPS}.parquet      (place)
    data/features/fcc/broadband_coverage/county_residuals/fcc_coverage_county_residuals_{USPS}_{FIPS}.parquet
    data/processed/census/baf2020/census_baf_{USPS}_{FIPS}.parquet
    data/processed/census/addcountlisting2025/census_acl_{USPS}_{FIPS}.parquet

Output — block (to data/features/fcc/broadband_coverage/block/):
    fcc_coverage_block_{USPS}_{FIPS}.parquet

Output — tract (to data/features/fcc/broadband_coverage/tract/):
    fcc_coverage_tract_{USPS}_{FIPS}.parquet

Usage:
    # Block-level only
    python -m network_idx.feature_engg.fcc_fixed_summary_est_ct_block --states AL CA

    # Block + tract rollup
    python -m network_idx.feature_engg.fcc_fixed_summary_est_ct_block --all --tract
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from network_idx.constants import (
    FCC_COVERAGE_BLOCK_OUTPUTS,
    FCC_COVERAGE_TRACT_OUTPUTS,
    FCC_COVERAGE_TIER_METRICS,
    FCC_FIXED_COVERAGE_TECHNOLOGIES,
    STATE_USPS_TO_FIPS,
)
from network_idx.config import (
    FEATURES_DIR_FCC_COVERAGE_BLOCK,
    FEATURES_DIR_FCC_COVERAGE_COUNTY_RESIDUALS,
    FEATURES_DIR_FCC_COVERAGE_TRACT,
    PROCESSED_DIR_CENSUS_ACL,
    PROCESSED_DIR_CENSUS_BAF,
    PROCESSED_DIR_FCC_BROADBAND_COVERAGE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

FIPS_TO_USPS = {v: k for k, v in STATE_USPS_TO_FIPS.items()}

TECHS = [t.lower() for t in FCC_FIXED_COVERAGE_TECHNOLOGIES]
PCT_COLS = [f"{tech}_{metric}" for tech in TECHS for metric in FCC_COVERAGE_TIER_METRICS]


# ── Loading helpers 

def _load_inputs(
    state_usps: str,
    fips: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load place coverage, county residuals, BAF crosswalk, and ACL address counts.
    """
    place_path = PROCESSED_DIR_FCC_BROADBAND_COVERAGE / f"fcc_fixed_coverage_{state_usps}_{fips}.parquet"
    residuals_path = FEATURES_DIR_FCC_COVERAGE_COUNTY_RESIDUALS / f"fcc_coverage_county_residuals_{state_usps}_{fips}.parquet"
    baf_path = PROCESSED_DIR_CENSUS_BAF / f"census_baf_{state_usps}_{fips}.parquet"
    acl_path = PROCESSED_DIR_CENSUS_ACL / f"census_acl_{state_usps}_{fips}.parquet"

    for p in (place_path, residuals_path, baf_path, acl_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing input: {p}")

    place_df = pd.read_parquet(place_path)
    residuals_df = pd.read_parquet(residuals_path)
    baf_df = pd.read_parquet(baf_path)
    acl_df = pd.read_parquet(acl_path, columns=["block_geoid", "total_housing_units"])

    return place_df, residuals_df, baf_df, acl_df


# ── Block-level estimates 
def estimate_block_coverage(
    place_df: pd.DataFrame,
    residuals_df: pd.DataFrame,
    baf_df: pd.DataFrame,
    acl_df: pd.DataFrame,
    state_usps: str,
) -> pd.DataFrame:
    """
    Assign coverage percentages and distribute unit counts to each block.

    For place blocks:  estimated_units = (block_hu / Σ block_hu in place) × place_total_units
    For residual blocks: estimated_units = (block_hu / Σ block_hu in county residual) × residual_units
    Percentages come directly from the place or county residual source.
    """
    blocks = baf_df.copy()

    # Join address counts
    blocks = blocks.merge(acl_df, on="block_geoid", how="left")
    blocks["census_housing_units"] = blocks["total_housing_units"].fillna(0).astype(int)

    in_place = blocks["place_geoid"].notna()

    # ── Case 1: blocks inside a place
    place_indexed = place_df.set_index("geography_id")

    # Compute housing unit share within each place
    place_hu_totals = (
        blocks.loc[in_place]
        .groupby("place_geoid")["census_housing_units"]
        .transform("sum")
    )
    blocks.loc[in_place, "_place_hu_total"] = place_hu_totals.values
    blocks.loc[in_place, "_weight"] = (
        blocks.loc[in_place, "census_housing_units"]
        / blocks.loc[in_place, "_place_hu_total"].replace(0, float("nan"))
    ).fillna(0)

    # Distribute place total_units to blocks
    blocks.loc[in_place, "estimated_fcc_units"] = (
        blocks.loc[in_place, "_weight"]
        * blocks.loc[in_place, "place_geoid"].map(place_indexed["total_units"])
    )

    # Assign place percentages
    for col in PCT_COLS:
        if col in place_indexed.columns:
            blocks.loc[in_place, col] = (
                blocks.loc[in_place, "place_geoid"].map(place_indexed[col])
            )
        else:
            blocks.loc[in_place, col] = 0.0

    # ── Case 2: blocks NOT in a place → county residual
    residuals_indexed = residuals_df.set_index("county_geoid")
    not_in_place = ~in_place

    # Compute housing unit share within each county (for non-place blocks only)
    county_hu_totals = (
        blocks.loc[not_in_place]
        .groupby("county_geoid")["census_housing_units"]
        .transform("sum")
    )
    blocks.loc[not_in_place, "_county_hu_total"] = county_hu_totals.values
    blocks.loc[not_in_place, "_weight"] = (
        blocks.loc[not_in_place, "census_housing_units"]
        / blocks.loc[not_in_place, "_county_hu_total"].replace(0, float("nan"))
    ).fillna(0)

    # Distribute county residual_units to blocks
    blocks.loc[not_in_place, "estimated_fcc_units"] = (
        blocks.loc[not_in_place, "_weight"]
        * blocks.loc[not_in_place, "county_geoid"].map(residuals_indexed["residual_units"])
    )

    # Assign residual percentages
    for col in PCT_COLS:
        if col in residuals_indexed.columns:
            blocks.loc[not_in_place, col] = (
                blocks.loc[not_in_place, "county_geoid"].map(residuals_indexed[col])
            )
        else:
            blocks.loc[not_in_place, col] = 0.0

    # Clean up
    blocks[PCT_COLS] = blocks[PCT_COLS].fillna(0.0)
    blocks["estimated_fcc_units"] = blocks["estimated_fcc_units"].fillna(0.0).round(0).astype(int)
    # We will not inherit percentages if there are zero estimated units, as the percentage values are not meaningful in such cases
    blocks.loc[blocks["estimated_fcc_units"] == 0, PCT_COLS] = None
    blocks["source"] = "county_residual"
    blocks.loc[in_place, "source"] = "place"
    blocks["state_usps"] = state_usps
    blocks = blocks.drop(columns=["_place_hu_total", "_county_hu_total", "_weight"], errors="ignore")

    return blocks[[c for c in FCC_COVERAGE_BLOCK_OUTPUTS if c in blocks.columns]]


# ── Tract-level aggregation ──────────────────────────────────────────────────
def aggregate_blocks_to_tract(block_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate block estimates to tract level.

    tract_estimated_units = Σ block_estimated_units
    tract_pct = Σ (block_estimated_units × block_pct) / tract_estimated_units
    """
    for col in PCT_COLS:
        block_df[f"_w_{col}"] = block_df["estimated_fcc_units"] * block_df[col]

    weighted_cols = [f"_w_{col}" for col in PCT_COLS]

    agg_dict = {
        "state_fips": ("state_fips", "first"),
        "state_usps": ("state_usps", "first"),
        "estimated_fcc_units": ("estimated_fcc_units", "sum"),
        "estimated_census_housing_units": ("census_housing_units", "sum"),
    }
    for wc in weighted_cols:
        agg_dict[wc] = (wc, "sum")

    tract_df = block_df.groupby("tract_geoid", as_index=False).agg(**agg_dict)

    for col in PCT_COLS:
        tract_df[col] = (
            tract_df[f"_w_{col}"] / tract_df["estimated_fcc_units"].replace(0, float("nan"))
        ).fillna(0.0).round(4)

    tract_df = tract_df.drop(columns=weighted_cols)

    return tract_df[[c for c in FCC_COVERAGE_TRACT_OUTPUTS if c in tract_df.columns]]


# ── Per-state orchestrator ────────────────────────────────────────────────────

def process_state(
    state_usps: str,
    overwrite: bool = False,
    compute_tract: bool = False,
) -> tuple[Path | None, Path | None]:
    fips = STATE_USPS_TO_FIPS[state_usps]

    block_out = FEATURES_DIR_FCC_COVERAGE_BLOCK / f"fcc_coverage_block_{state_usps}_{fips}.parquet"
    tract_out = FEATURES_DIR_FCC_COVERAGE_TRACT / f"fcc_coverage_tract_{state_usps}_{fips}.parquet"

    block_done = block_out.exists() and not overwrite
    tract_done = tract_out.exists() and not overwrite

    if block_done and (not compute_tract or tract_done):
        logger.info(f"{state_usps}: output(s) already exist. Skipping.")
        return block_out, tract_out if tract_done else None

    # Load all inputs
    place_df, residuals_df, baf_df, acl_df = _load_inputs(state_usps, fips)

    # ── Block estimates
    if not block_done:
        logger.info(f"{state_usps}: estimating block-level coverage ({len(baf_df):,} blocks)")
        block_df = estimate_block_coverage(place_df, residuals_df, baf_df, acl_df, state_usps)

        block_out.parent.mkdir(parents=True, exist_ok=True)
        block_df.to_parquet(block_out, index=False)
        logger.info(f"Saved {block_out.name}  ({len(block_df):,} blocks)")
    else:
        block_df = pd.read_parquet(block_out)

    block_path = block_out

    # ── Tract aggregation
    tract_path = None
    if compute_tract and not tract_done:
        logger.info(f"{state_usps}: aggregating to tract level")
        tract_df = aggregate_blocks_to_tract(block_df)

        tract_out.parent.mkdir(parents=True, exist_ok=True)
        tract_df.to_parquet(tract_out, index=False)
        logger.info(f"Saved {tract_out.name}  ({len(tract_df):,} tracts)")
        tract_path = tract_out
    elif compute_tract and tract_done:
        tract_path = tract_out

    return block_path, tract_path


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Dasymetric interpolation: assign FCC coverage to blocks and optionally aggregate to tracts."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=["AL"],
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to process: {list(STATE_USPS_TO_FIPS.keys())}",
    )
    parser.add_argument("--all", action="store_true", default=False, help="Process all states.")
    parser.add_argument("--tract", action="store_true", default=False, help="Also aggregate blocks to tract level.")
    parser.add_argument("--overwrite", action="store_true", default=False, help="Overwrite existing files.")
    args = parser.parse_args()

    states_to_process = list(STATE_USPS_TO_FIPS.keys()) if args.all else args.states

    for i, state in enumerate(states_to_process, 1):
        logger.info(f"[{i}/{len(states_to_process)}] {state}")
        try:
            process_state(state, overwrite=args.overwrite, compute_tract=args.tract)
        except FileNotFoundError as e:
            logger.error(f"Skipping {state}: {e}")
        except Exception as e:
            logger.error(f"Failed {state}: {e}")