# FCC Fixed Coverage: Block-Level Dasymetric Interpolation

## Overview
Disaggregate FCC place-level and county-level broadband coverage summary data to census blocks using location counts as the ancillary weighting variable. Then aggregate blocks to census tracts.

## Data Sources

| Source | Grain | Key fields |
|---|---|---|
| FCC place summary | Per place × technology | `total_units`, `speed_02_02`...`speed_1000_100` (percentages, 0–1) |
| FCC county summary | Per county × technology | Same schema |
| Speeds parquet (block-level) | Per block × technology | `location_count` (BSLs per block — weight variable) |
| Crosswalk (Census BAF) | Per block | `place_geoid` (nullable), `county_fips` |

## Block Classification

Using centroid assignment (Census Block Assignment Files), each block is either:
- **Case 1**: Inside a place (`place_geoid` is not null)
- **Case 2**: Outside any place (`place_geoid` is null / `99999`)

No Case 3 (straddling blocks) — the BAF already handles centroid assignment.

---

## Case 1: Block Inside a Place

1. **Weight** for each block within the place:

   w(b) = location_count(b) / sum(location_count(b') for b' in place)

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

2. **Weights** across only the non-place blocks in that county:

   w(b) = location_count(b) / sum(location_count(b') for b' in county, no place)

3. **Distribute the residual:**

   U(b) = U(county_residual) × w(b)

4. **Speed percentages** — use the county's percentages:

   pct_speed_X(b) = pct_speed_X(county)

5. **Absolute counts:**

   count_speed_X(b) = U(b) × pct_speed_X(county)

---

## Block → Tract Rollup

For each tract, sum absolute counts and reconstruct percentages:

   U(tract) = sum(U(b) for b in tract)

   pct_speed_X(tract) = sum(count_speed_X(b) for b in tract) / U(tract)

---

## Edge Cases

1. **Negative residuals**: `U(county) - sum(U(place))` can go negative due to data inconsistencies. Clamp to 0 and log.
2. **Zero-weight denominators**: Blocks in the crosswalk but with zero location count → w(b) = 0, receives zero units. Guard against division-by-zero when sum(location_count) = 0 for an entire place or county residual.
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