"""
Feature engineering: county-level coverage residuals.

For each county, compute the coverage that is NOT explained by Census places,
so that non-place blocks can be assigned county-residual percentages in the
dasymetric interpolation step.

Formula (per technology × speed-tier):
    county_abs        = county_total_units × county_pct
    places_abs        = Σ (place_total_units_i × place_pct_i × share_i)
    residual_abs      = county_abs − places_abs
    residual_units    = county_total_units − Σ (place_total_units_i × share_i)
    residual_pct      = residual_abs / residual_units          (null when residual_units == 0)

    where share_i     = blocks_in_county / total_blocks_in_place
                        (1.0 for single-county places, <1.0 for multi-county places)

Notes:
    - Places that span multiple counties are fractionally allocated using
      block-count ratios derived from the Census BAF crosswalk.
    - Residual percentages are capped at [0, 1] to handle rounding artefacts.
    - Counties where residual_units == 0 get null percentages — there is no
      non-place population to distribute.

Inputs:
    data/processed/fcc/broadband_coverage/fcc_fixed_coverage_county_{USPS}_{FIPS}.parquet
    data/processed/fcc/broadband_coverage/fcc_fixed_coverage_{USPS}_{FIPS}.parquet
    data/processed/census/baf2020/census_baf_{USPS}_{FIPS}.parquet

Output (per state, to data/features/fcc/broadband_coverage/county_residuals/):
    fcc_coverage_county_residuals_{USPS}_{FIPS}.parquet

Usage:
    python -m network_idx.feature_engg.fcc_fixed_summary_county_residuals --states AL CA
    python -m network_idx.feature_engg.fcc_fixed_summary_county_residuals --all
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

from network_idx.constants import (
    FCC_COVERAGE_COUNTY_RESIDUAL_OUTPUTS,
    STATE_USPS_TO_FIPS,
)
from network_idx.config import (
    FEATURES_DIR_FCC_COVERAGE_COUNTY_RESIDUALS,
    PROCESSED_DIR_CENSUS_BAF,
    PROCESSED_DIR_FCC_BROADBAND_COVERAGE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TECHS = ["copper", "cable", "fiber"]
TIER_METRICS = ["speed_100_20", "less_than_100_20", "more_than_100_20"]
PCT_COLS = [f"{tech}_{metric}" for tech in TECHS for metric in TIER_METRICS]


# ── Helpers 
# Private helpers cannot be imported in other scripts
def _load_inputs(
    state_usps: str,
    fips: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Load county parquet, place parquet, and BAF crosswalk for one state.
    """
    county_path = PROCESSED_DIR_FCC_BROADBAND_COVERAGE / f"fcc_fixed_coverage_county_{state_usps}_{fips}.parquet"
    place_path = PROCESSED_DIR_FCC_BROADBAND_COVERAGE / f"fcc_fixed_coverage_{state_usps}_{fips}.parquet"
    baf_path = PROCESSED_DIR_CENSUS_BAF / f"census_baf_{state_usps}_{fips}.parquet"

    for p in (county_path, place_path, baf_path):
        if not p.exists():
            raise FileNotFoundError(f"Missing input: {p}")

    county_df = pd.read_parquet(county_path)
    place_df = pd.read_parquet(place_path)
    baf_df = pd.read_parquet(baf_path, columns=["county_geoid", "place_geoid"])

    return county_df, place_df, baf_df


def _build_county_place_map(baf_df: pd.DataFrame) -> pd.DataFrame:
    """
    From the BAF crosswalk, build (county_geoid, place_geoid, place_share).

    place_share = blocks_in_county / total_blocks_in_place.
    For single-county places this is 1.0; for multi-county places it is
    proportional to the block count in each county.
    """
    # Drop blocks not in any place
    cp = baf_df[baf_df["place_geoid"].notna()].copy()

    # Count blocks per (county, place)
    county_place_blocks = (
        cp.groupby(["county_geoid", "place_geoid"])
        .size()
        .rename("blocks_in_county")
        .reset_index()
    )

    # Total blocks per place (across all counties)
    place_total_blocks = (
        cp.groupby("place_geoid")
        .size()
        .rename("total_blocks_in_place")
        .reset_index()
    )

    # Merge and compute share
    merged = county_place_blocks.merge(place_total_blocks, on="place_geoid")
    merged["place_share"] = merged["blocks_in_county"] / merged["total_blocks_in_place"]

    return merged[["county_geoid", "place_geoid", "place_share"]]


def compute_residuals(
    county_df: pd.DataFrame,
    place_df: pd.DataFrame,
    county_place_map: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each county, subtract fractionally-allocated place-level absolute
    units from the county total to get residual coverage for non-place blocks.
    """
    # Pre-index place_df by geography_id for fast lookup
    place_indexed = place_df.set_index("geography_id")

    rows = []

    for _, county_row in county_df.iterrows():
        county_geoid = str(county_row["geography_id"])
        county_units = county_row["total_units"]

        # Get all places in this county with their shares
        links = county_place_map[county_place_map["county_geoid"] == county_geoid]

        # Match to FCC place data (inner join — only places that exist in FCC as well as census mapping
        places_with_share = links[links["place_geoid"].isin(place_indexed.index)].copy()

        if len(places_with_share):
            # Look up total_units for each place
            places_with_share["place_total_units"] = (
                places_with_share["place_geoid"]
                .map(place_indexed["total_units"])
            )
            # Weighted sum: Σ (place_total_units × share)
            places_units = (places_with_share["place_total_units"] * places_with_share["place_share"]).sum()
        else:
            places_units = 0

        residual_units = max(county_units - places_units, 0)

        rec = {
            "county_geoid": county_geoid,
            "state_fips": county_geoid[:2],
            "county_total_units": county_units,
            "places_total_units": round(places_units),
            "residual_units": round(residual_units),
            "place_count": len(places_with_share),
        }

        for col in PCT_COLS:
            if residual_units == 0:
                rec[col] = None
            else:
                county_abs = county_units * county_row.get(col, 0)

                if len(places_with_share) and col in place_indexed.columns:
                    # Σ (place_total_units × place_pct × share)
                    place_pcts = (
                        places_with_share["place_geoid"]
                        .map(place_indexed[col])
                        .fillna(0)
                    )
                    places_abs = (
                        places_with_share["place_total_units"]
                        * place_pcts
                        * places_with_share["place_share"]
                    ).sum()
                else:
                    places_abs = 0

                residual_abs = county_abs - places_abs
                residual_pct = residual_abs / residual_units
                # Cap to [0, 1]
                rec[col] = max(0.0, min(residual_pct, 1.0))

        rows.append(rec)

    result = pd.DataFrame(rows)
    return result[[c for c in FCC_COVERAGE_COUNTY_RESIDUAL_OUTPUTS if c in result.columns]]


# ── Per-state orchestrator

def process_state(state_usps: str, overwrite: bool = False) -> Path | None:
    fips = STATE_USPS_TO_FIPS[state_usps]
    out_path = FEATURES_DIR_FCC_COVERAGE_COUNTY_RESIDUALS / f"fcc_coverage_county_residuals_{state_usps}_{fips}.parquet"

    if out_path.exists() and not overwrite:
        logger.info(f"{out_path.name} already exists. Skipping.")
        return out_path

    county_df, place_df, baf_df = _load_inputs(state_usps, fips)
    county_place_map = _build_county_place_map(baf_df)

    logger.info(
        f"{state_usps}: {len(county_df)} counties, "
        f"{len(place_df)} places, "
        f"{len(county_place_map)} county-place links"
    )

    residuals = compute_residuals(county_df, place_df, county_place_map)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    residuals.to_parquet(out_path, index=False)
    logger.info(f"Saved {out_path.name}  ({len(residuals)} counties)")
    return out_path


# ── CLI 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compute county-level coverage residuals (county minus fractional places)."
    )
    parser.add_argument(
        "--states", type=str, nargs="+", default=["AL"],
        choices=STATE_USPS_TO_FIPS.keys(),
        metavar="STATE",
        help=f"States to process: {list(STATE_USPS_TO_FIPS.keys())}",
    )
    parser.add_argument("--all", action="store_true", default=False, help="Process all states.")
    parser.add_argument("--overwrite", action="store_true", default=False, help="Overwrite existing files.")
    args = parser.parse_args()

    states_to_process = list(STATE_USPS_TO_FIPS.keys()) if args.all else args.states

    for i, state in enumerate(states_to_process, 1):
        logger.info(f"[{i}/{len(states_to_process)}] {state}")
        try:
            process_state(state, overwrite=args.overwrite)
        except FileNotFoundError as e:
            logger.error(f"Skipping {state}: {e}")
        except Exception as e:
            logger.error(f"Failed {state}: {e}")