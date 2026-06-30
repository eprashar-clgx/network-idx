# Parcel-Level Scoring — QA & Parity Notes

Tracking the gotchas and verification steps for productionizing the tract-trained
LightGBM (k=8, v1) weights into a parcel-level index. Goal: replicate the tract
feature definitions at the correct grain (FCC → block, growth → parcel, demo → tract)
with parity, then score every parcel via the weighted-average-of-sub-indices method.

Grain map: **FCC → block**, **growth → parcel**, **demo → tract**.
Join spine: parcel table (`block_geoid` + growth features) → derive
`tract_geoid = block_geoid[:11]` for the demo join, join FCC on `block_geoid`.

---

## Cross-cutting (pipeline-wide)

### C1. Global stats must be persisted, not just min/max
Three quantities are country-wide and must be **frozen per `run_id`** so new or
re-scored parcels don't shift over time:
- **min-max bounds** per feature (for 0–1 scaling),
- **fiber-distance P99** (cap for `median_dist_nearest_fiber_m`),
- **hotspot max** (for the `1.25 × max` fill on `median_dist_nearest_hotspot`).

Store as a small `scaling_params` artifact (or fold into the run metadata) keyed by
`run_id`. Without this, scoring is **not deterministic** across runs.

### C2. Connecticut GEOID vintage
`feature_engg/all_features_tract.sql` applies a CT crosswalk (planning-region remap).
The parcel growth table (`block_geoid`), `fcc_coverage_block` / `fcc_fixed_speeds_block`,
and the demo tract table must all be on the **same CT vintage**, or the
`block_geoid[:11] → tract_geoid` derivation and the demo join will **silently mismatch**
for CT.
- **Action:** join-coverage check — count parcels that fail to match FCC and/or demo,
  broken down by state; confirm CT isn't disproportionately dropping.

### C3. Inf guard, not just NaN
Any ratio feature can produce `inf` (divide-by-zero) in addition to `NaN` (0/0).
Use `SAFE_DIVIDE` in SQL and apply NA fills to **both** `NaN` and `inf`
(`replace([inf, -inf], nan)` then fill) in pandas. Applies to FCC penetration/gap
features (see F1) — verify nothing leaks an `inf` into scaling.

### C4. Weights map 1:1 — no renormalization
Because fiber-distance and hotspot are now sourced at **parcel** grain, all **13**
model features are present at scoring time. The LightGBM k=8 weight vector is used
**as-is** — no within-bucket renormalization needed. Bucket group weights stay:
Growth **0.40**, FCC **0.44**, Demo **0.16**.

### C5. Training vs scoring population differs (acknowledge, no action)
The tract model dropped `< 50` housing-unit tracts (water bodies) and NA pop-change
tracts. At parcel level we **score everything**. The NA rules cover degenerate blocks,
so no code change — just be aware the **scored universe is broader than the trained one**,
and degenerate blocks (0 housing units, no providers) will receive scores.

### C6. Inversion direction (single source of truth)
Inverted features (lower raw value → higher score):
`median_dist_nearest_hotspot`, `median_dist_nearest_fiber_m`, `cable_penetration`,
`fiber_speed_top_tier`, `provider_competitive_landscape_ord`.
Keep this set in one constant; the scorer must read from it (don't hardcode per-feature).

---

## Demo block (tract grain)

Features: `pop_ch_avg`, `pop_pctch_avg`, `estimated_census_housing_units`.
Already engineered upstream — broadcast from tract to every parcel in the tract.

### D1. Use the `_avg` columns, not the `_1yr` columns
The model uses **`pop_ch_avg`** and **`pop_pctch_avg`**. These are **averaged over 5 years**,
not single-year. The `pop_ch_1yr` / `pop_pctch_1yr` columns seen in the EDA notebook are
**dropped later** and must **not** be used for scoring.
- **Action:** confirm the demo tract table name and that the `_avg` (5-year) columns
  exist there with the expected coverage.

### D2. Two different "housing units" — keep distinct
- **Demo feature** `estimated_census_housing_units` (tract grain) — a scored & weighted
  model feature.
- **FCC denominator** `census_housing_units` (block grain) — an **intermediate only**,
  used to compute penetration/gap (see F1), never scored directly.

Same underlying concept, **different grain and role**. Easy to conflate — name them
distinctly in code and never cross-wire the two.

### D3. NA handling for demo
Tract pipeline **dropped** NA pop-change rows. At parcel we don't drop — decide the
parcel-level fill for any tract with NA `pop_ch_avg` / `pop_pctch_avg` (e.g. 0, or
exclude from demo sub-index). Document the choice and apply consistently.

---

## Growth block (parcel grain)

Features: `median_landuse_change_qtr_mi_cnt`, `median_pre_early_dev_qtr_mi_cnt`,
`median_bldr_dev_qtr_mi_cnt`, `median_new_permit_qtr_mi_cnt`,
`median_dist_nearest_hotspot`. Sourced directly from the parcel table.

### G1. Hotspot distance NA fill = 1.25 × (country-wide max)
`median_dist_nearest_hotspot` is **inverted** (closer = higher score). NA fill =
`1.25 × max` (computed country-wide; persist the max — see C1).
- **Known artifact:** at k=8 this extreme fill inflated the feature's importance
  (the fill value showed up as the cluster median in NA-majority clusters). The fill
  is intentionally being carried for parity, but **verify in the notebook** it behaves
  before locking it into SQL — if it distorts the distribution, switch to an explicit
  `hotspot_missing` flag or a gentler fill.

### G2. Distance cutoffs are tunable
Both distance cutoffs (hotspot `1.25 × max`, fiber P99 — see F4) are to be
**implemented and verified in the notebook first**. If the parcel-level distribution
differs from tract, change the formula in the notebook before committing it to SQL.

---

## FCC block (block grain)

Two source tables, joined on `block_geoid`:
- `fcc_fixed_speeds_block` — `{cable,copper,fiber}_location_count`, `_provider_count`,
  `_max_download_speed`, `_max_upload_speed`.
- `fcc_coverage_block` — `census_housing_units`, `estimated_fcc_units`, and the full
  tier metrics `{cable,copper,fiber}_speed_*_only` incl. `*_speed_1000_100_only`.

Four scored features: `cable_penetration`, `fiber_opportunity_gap`,
`fiber_speed_top_tier`, `provider_competitive_landscape_ord`.

### F1. Penetration / gap definitions + fills

cable_penetration = SAFE_DIVIDE(cable_location_count, census_housing_units) -> NA/inf fill 0
fiber_opportunity_gap = SAFE_DIVIDE(census_housing_units - fiber_location_count,
census_housing_units) -> NA/inf fill 1.0

- Denominator is the **block** `census_housing_units` (not the demo
  `estimated_census_housing_units` — see D2).
- **Unclipped on purpose:** `cable_penetration` can exceed 1 (cable locations > housing)
  and `fiber_opportunity_gap` can go negative (fiber locations > housing). The tract
  pipeline left these unclipped — **keep parity** and let country-wide min-max absorb
  the range. Do **not** add a `[0,1]` clip (the `between(0,1)` filter in the tract
  notebook was only for a plot, not the feature).

### F2. `fiber_speed_top_tier`
has_fiber = (fiber_location_count > 0) AND (fiber_provider_count > 0)
fiber_speed_top_tier = fiber_speed_1000_100_only * has_fiber

- **Inverted** feature (lower → higher score; less existing top-tier fiber = more upgrade
  opportunity).
- Requires both `fiber_location_count` (speeds table) and `fiber_provider_count`
  (speeds/providers table) for `has_fiber`.
- `fiber_speed_1000_100_only` comes from the **coverage** table and is a **fraction (0–1)**;
  verify the block estimate is on the same 0–1 scale as the tract version (parity check).

### F3. `provider_competitive_landscape` → `_ord`
Keep **both** columns: text label (interpretability) and ordinal `0–6` (scaling).
The ladder (note the cable/fiber precedence — fiber count dominates once any cable exists):

no_providers : copper=0 & cable=0 & fiber=0 -> 0
greenfield : copper>0 & cable=0 & fiber=0 -> 1
cable_but_no_fiber : cable>0 & fiber=0 -> 2
fiber_entry : fiber=1 -> 3
fiber_duopoly : fiber=2 -> 4
fiber_competitive : fiber=3 -> 5
fiber_saturated : fiber>3 -> 6

- **Provider counts at block**: derive via `COUNT(DISTINCT provider_id)` per tech from
  the providers table; **fill NA provider counts with 0** before applying the ladder
  (parity with the notebook's `fillna(0)`).
- `_ord` is **inverted** (fewer providers = more opportunity).
- **`Other` catch-all:** the function has an `else 'Other'` branch. For non-negative
  integer counts it's unreachable, but guard it in SQL (map to NULL and assert zero
  rows) so a bad input can't silently produce an unscored category.

### F4. Fiber-distance is FCC-bucket but sourced at parcel
`median_dist_nearest_fiber_m` belongs to the FCC bucket in the weights, but is sourced
from a **parcel** BQ distance table (not from `fcc_*_block`). **Inverted**; NA fill =
**P99** (country-wide; persist the value — see C1). Tunable/verify-in-notebook first
(see G2).

### F5. `_ord` inversion sanity check (post-scoring QC)
With `_ord` inverted, ordinal `0 = no_providers` scores as **max opportunity** — but
"no providers" can also mean uninhabited / water. No code change; **QC pass after scoring**
to confirm empty/degenerate blocks aren't inflating the FCC sub-index (cross-check against
`census_housing_units = 0` and `estimated_fcc_units = 0`).

### D4 / F6 — Delivery distances are in METERS, not miles
`fiber_idx_v1_parcel.dist_nearest_hotspot` and `dist_nearest_fiber` are emitted as the
raw source values, which are in **meters** (`dist_to_nearest_hotspot_m`,
`dist_to_nearest_fiber_m`). The data-dictionary wording ("Distance (in miles)") is
aspirational — no unit conversion is applied in the pipeline. Either (a) update the
customer definition to say "meters", or (b) add a `/ 1609.344` conversion in
`build_delivery_query` before publishing externally. Scaling/scoring are unaffected
either way (min-max is unit-invariant).

---

## Pre-flight checklist

- [ ] FCC block tables populated for **all states** (`fcc_fixed_speeds_block`,
      `fcc_fixed_speeds_providers_block`, `fcc_coverage_block`).
- [ ] CT vintage aligned across parcel / FCC / demo (C2); join-coverage check passes.
- [ ] Demo `_avg` (5-year) columns confirmed present (D1).
- [ ] `scaling_params` persisted for `run_id` (C1).
- [ ] No `inf` / unexpected `NaN` reaches scaling (C3, F1).
- [ ] Weights load 1:1, 13 features, bucket weights 0.40 / 0.44 / 0.16 (C4).
- [ ] Post-scoring QC: degenerate blocks not inflating FCC sub-index (F5).

## End-to-End Flow

```mermaid
flowchart TD
    %% ── Telecom: block grain ──
    subgraph TELECOM["Telecom (block grain)"]
        SP_RAW["processing/fcc_fixed_speeds.py<br/>data/processed/fcc/speeds/*.parquet"]
        SP_BQ["transfer/fcc_fixed_speeds_and_providers_bq.py"]
        SP_TBL["teu_telecom.fcc_fixed_speeds_block<br/>teu_telecom.fcc_fixed_speeds_providers_block"]
        CV_RAW["feature_engg/fcc_fixed_summary_est_ct_block.py<br/>data/features/fcc/broadband_coverage/block/*.parquet"]
        CV_BQ["transfer/fcc_fixed_coverage_features_ct_bq.py"]
        CV_TBL["teu_telecom.fcc_coverage_block"]
        TEL_SQL["feature_engg/telecom_features_block.sql<br/>(telecom_features_block_bq.py)"]
        TEL_TBL["teu_features.telecom_features_block"]

        SP_RAW --> SP_BQ --> SP_TBL --> TEL_SQL
        CV_RAW --> CV_BQ --> CV_TBL --> TEL_SQL
        TEL_SQL --> TEL_TBL
    end

    %% ── Growth + Demo (already engineered) ──
    subgraph GD["Growth (parcel) + Demo (tract)"]
        GROWTH_TBL["teu_features.&lt;parcel growth + distance&gt;<br/>(BQ_TABLE_PARCEL_GROWTH, has block_geoid)"]
        DEMO_TBL["teu_features.demo_pop_ct<br/>(pop_ch_avg / pop_pctch_avg = 5-yr avg)"]
    end

    %% ── Join spine -> parcel features ──
    JOIN["Join on block_geoid (telecom)<br/>+ tract_geoid = block_geoid[:11] (demo)"]
    PARCEL_FEAT["teu_features.parcel_features<br/>(13 features @ parcel grain)"]
    TEL_TBL --> JOIN
    GROWTH_TBL --> JOIN
    DEMO_TBL --> JOIN
    JOIN --> PARCEL_FEAT

    %% ── Weights (from trained model) ──
    subgraph W["Weights"]
        MODEL["LightGBM k=8 (04_eda_classification_v2)<br/>SHAP -> raw weights"]
        BUILD_W["scoring/build_weights.py"]
        W_TBL["teu_analytics.feature_weights<br/>(run_id = lightgbm_k8_v1)"]
        MODEL --> BUILD_W --> W_TBL
    end

    %% ── Scaling params (country-wide, frozen) ──
    BUILD_S["scoring/build_scaling_params.py<br/>(1 BQ scan: MIN/MAX/P99)"]
    S_TBL["teu_analytics.scaling_params<br/>(run_id-keyed: min, max, na_fill, invert)"]
    PARCEL_FEAT --> BUILD_S --> S_TBL

    %% ── Scoring ──
    SCORE["scoring/parcel_score.py<br/>apply_scaling + weighted-avg sub-indices"]
    OUT["teu_outputs.parcel_scores<br/>(scaled feats, idx_growth/telecom/demo, idx_overall_wa)"]
    PARCEL_FEAT --> SCORE
    W_TBL --> SCORE
    S_TBL --> SCORE
    SCORE --> OUT