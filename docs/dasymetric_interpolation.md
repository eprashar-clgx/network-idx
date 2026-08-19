# FCC Fixed Coverage: Block-Level Dasymetric Interpolation

## Overview
Disaggregate FCC place-level and county-level broadband coverage summary data to census blocks using **census housing-unit counts** (from the Census Address Count Listing) as the ancillary weighting variable. Then aggregate blocks to census tracts, weighting each block's inherited percentages by the FCC unit count distributed to it.

> **Two different weights — do not conflate them.**
> - **Distribution weight** = a block's *census housing-unit share*, used to split an FCC *unit total* across the blocks of a place / county-residual (produces `estimated_fcc_units`, `U(b)` below).
> - **Roll-up weight** = the distributed FCC unit count `U(b)` itself, used to recombine block *percentages* back into a tract/county percentage.
>
> Census housing units decide only *how units spread across blocks within a place*; the roll-up then weights by those distributed units, not by housing units.

## Data Sources

| Source | Grain | Key fields |
|---|---|---|
| FCC place summary | Per place × technology | `total_units`, `speed_02_02`...`speed_1000_100` (percentages, 0–1) |
| FCC county summary | Per county × technology | Same schema |
| Census Address Count Listing (ACL) | Per block | `total_housing_units` — the ancillary **weight** variable |
| Crosswalk (Census BAF) | Per block | `place_geoid` (nullable), `county_fips` |

> Block-level location / provider counts (from the FCC speeds table) are **not** an interpolation input — they are joined downstream in `telecom_features_block`, not used to weight the disaggregation.

## Block Classification

Using centroid assignment (Census Block Assignment Files), each block is either:
- **Case 1**: Inside a place (`place_geoid` is not null)
- **Case 2**: Outside any place (`place_geoid` is null / `99999`)

No Case 3 (straddling blocks) — the BAF already handles centroid assignment.

---

## Case 1: Block Inside a Place

1. **Weight** for each block within the place (census housing-unit share):

   w(b) = census_housing_units(b) / sum(census_housing_units(b') for b' in place)

2. **Distribute the place's total units** to the block:

   U(b) = total_units(place) × w(b)

3. **Speed percentages** — assign the place's percentages directly:

   pct_speed_X(b) = pct_speed_X(place)

4. **Absolute counts** per speed bucket (needed for tract rollup):

   count_speed_X(b) = U(b) × pct_speed_X(place)

---

## Case 2: Block Outside Any Place

1. **County residual** — subtract what places already account for:

   U(county_residual) = total_units(county) - sum(total_units(place) for place in county)

   Clamp to 0 if negative and log a warning.

2. **Weights** across only the non-place blocks in that county (census housing-unit share):

   w(b) = census_housing_units(b) / sum(census_housing_units(b') for b' in county, no place)

3. **Distribute the residual:**

   U(b) = U(county_residual) × w(b)

4. **Speed percentages** — use the county's percentages:

   pct_speed_X(b) = pct_speed_X(county)

5. **Absolute counts:**

   count_speed_X(b) = U(b) × pct_speed_X(county)

---

## Block → Tract Rollup

For each tract, sum absolute counts and reconstruct percentages (weighting by the
distributed unit count `U(b)`, **not** by housing units):

   U(tract) = sum(U(b) for b in tract)

   pct_speed_X(tract) = sum(count_speed_X(b) for b in tract) / U(tract)

---

## Conservation Invariant

Because `U(b)` sums by construction to the place / county-residual total it was
distributed from, the block estimates **reconstruct the FCC source total exactly**, per
(technology × speed tier), when re-aggregated to the county:

   sum(count_speed_X(b) for b in county)
     = sum(place_pct_X × place_total_units) + residual_pct_X × residual_units
     = places_abs + residual_abs
     = county_abs = county_pct_X × county_total_units

The census housing-unit distribution cancels out of this reconstruction: `pct_speed_X(b)`
is constant within a place, so how units spread across that place's blocks does not change
the sum. Equality is exact up to (a) per-block integer rounding of `U(b)`, (b) the
multi-county place `share_i` allocation (Edge Case 3), and (c) zero-unit blocks whose
percentages are nulled (they contribute 0 to the numerator anyway).

This exact-by-construction reconstruction is the basis for the **dasymetric conservation
check** (`monitoring.data_contract`): re-aggregate the block table to county grain, compare
`county_abs` against `county_total_units × county_pct` from the FCC source, and halt if the
gap exceeds the rounding budget — a breach signals a bug in the share / residual algebra,
not statistical drift.

---

## Edge Cases

1. **Negative residuals**: `U(county) - sum(U(place))` can go negative due to data inconsistencies. Clamp to 0 and log.
2. **Zero-weight denominators**: Blocks in the crosswalk but with zero census housing units → w(b) = 0, receives zero units. Guard against division-by-zero when sum(census_housing_units) = 0 for an entire place or county residual.
3. **Places straddling county lines** (~15% of places): Split place unit count by county using address weights before computing county residuals:

   U(place_in_county_A) = U(place) × A(place_in_county_A) / A(place_total)

---

## Implementation Order

1. Download county-level summary (one nationwide file from FCC)
2. Build block → place / county crosswalk (Census BAF, centroid-based)
3. Compute county residuals per technology
4. Distribute unit counts to blocks using address weights
5. Assign percentage features (place or county depending on case)
6. Aggregate blocks → tracts with unit-weighted percentages
7. Save tract-level parquets