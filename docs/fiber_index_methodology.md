# Fiber Potential Index — Methodology

**Audience:** Analytics Engineering
**Purpose:** Primary methodology reference for the parcel-level Fiber Potential Index and its
three sub-indices (demographic, telecom, growth). Walks backwards from the delivered output to
the features and the decisions behind their cutoffs, so that engineering knows exactly which
outputs to track and why each value is what it is.

**Supporting documents (attach in Confluence):**
- `weightage_methodology.MD` — weight types, formulas, scaling-method trade-offs.
- `parcel_scoring_qa.md` — parity/QA gotchas, per-feature fills, pre-flight checklist.

> **Weight vintage note.** The current authoritative weights are the **LightGBM k=8 v1**
> run (`run_id = lightgbm_k8_v1`). Bucket weights: **Growth 0.169 / Telecom 0.591 / Demo 0.240**.
> Earlier drafts of the supporting documents quote an older Random Forest run
> (0.40 / 0.44 / 0.16) — those numbers illustrate the *method*, not the current production
> weights. The single source of truth is the constants file (`SCORING_BUCKET_WEIGHTS`),
> materialized per `run_id` in the `feature_weights` table.

---

## 1. How the index is built

### 1.1 Pipeline & roll-ups

```text
                          FEATURE BUCKETS (13 features)
  - Demographic : pop_ch_avg, pop_pctch_avg, census_housing_units
  - Telecom     : cable_penetration, fiber_opportunity_gap, fiber_speed_top_tier,
                  dist_to_nearest_fiber_m, provider_competitive_landscape_ord
  - Growth      : landuse_change_qtr_mi_cnt, pre_early_dev_qtr_mi_cnt, bldr_dev_qtr_mi_cnt,
                  new_permit_qtr_mi_cnt, dist_to_nearest_hotspot_m


  A) WEIGHT DERIVATION  (tract grain)          B) INDEX SCORING  (native / lowest grain)
  -----------------------------------          ------------------------------------------
  Extract all 13 features at the               Extract each feature at its LOWEST grain:
  CENSUS-TRACT level, then normalize             growth  -> parcel
        |                                        telecom -> census block
        v                                        demo    -> census tract
  Correlation analysis                                |
    -> drop redundant / collinear                     v
        |                                        Scale each feature to [0,1]
        v                                          (country-wide min-max;
  K-means -> cluster label (k = 8)                  invert where flagged)
        |                                               |
        v                                               |
  LightGBM on cluster labels -> SHAP                    |
        |                                               |
        v                                               |
  Feature weights + roll-ups                            |
  (within-bucket + bucket weights)                      |
        |                                               |
        +----------------> (weights) <-----------------+
                               |
                               v
            PARCEL-LEVEL INDICES  (one row per parcel)
      within-bucket weighted sum of scaled features  ->  sub-indices (rescaled 0-100)
      bucket-weighted average of sub-indices         ->  fiber_potential_index
```

**Two tracks.** Track **A** (left) uses tract-grain features, clustering, and SHAP purely to
*derive the weights*. Track **B** (right) builds the actual *scored values* from features extracted
at their lowest grain (growth → parcel, telecom → block, demo → tract) and scaled to `[0, 1]`. The
two meet only at scoring: the weights from A are applied to the scaled features from B to produce
the parcel-level indices. Clustering and SHAP are **never** used to compute the index values
themselves — only to set the weights.

The pipeline produces **one 0–100 score per US parcel**, plus three 0–100 sub-scores and the raw
+ scaled feature values that feed them. Delivered indices:

| Index | Internal name | Bucket weight (v1) | Meaning |
|---|---|---|---|
| `fiber_potential_index` | `idx_overall` | — | Overall opportunity; weighted average of the three sub-indices |
| `demographic_index` | `idx_demo` | **0.240** | Population and housing base |
| `growth_index` | `idx_growth` | **0.169** | Development / growth momentum around the parcel |
| `telecom_index` | `idx_telecom` | **0.591** | Broadband-market opportunity (under-served / greenfield) |

**Scaling.** Before weighting, each feature is scaled to `[0, 1]` country-wide (min-max). Inverted
features take `1 − s` so that a *lower* raw value scores *higher*. Let `s(f)` denote the scaled
value of feature `f`.

**Sub-index roll-up.** Each sub-index is a within-bucket weighted sum of scaled features, then
min-max rescaled to 0–100 over the full parcel population:

```text
demographic_index = rescale_0-100( a1·s(pop_ch_avg) + a2·s(pop_pctch_avg) + a3·s(census_housing_units) )

growth_index      = rescale_0-100( g1·s(landuse_change) + g2·s(pre_early_dev) + g3·s(bldr_dev)
                                   + g4·s(new_permit) + g5·s(dist_hotspot) )

telecom_index     = rescale_0-100( t1·s(cable_pen) + t2·s(fiber_gap) + t3·s(fiber_top_tier)
                                   + t4·s(dist_fiber) + t5·s(provider) )
```

where `a1..a3`, `g1..g5`, `t1..t5` are the within-bucket weights (each set sums to 1).

**Overall roll-up.** The overall index is the bucket-weighted average of the three sub-indices —
decomposable by design ("X% growth + Y% telecom + Z% demo"):

```text
fiber_potential_index = 0.240·demographic_index + 0.169·growth_index + 0.591·telecom_index
```

**Three weight views** (all derived from `mean(|SHAP|)`; see §3):

| View | Normalization | Used for |
|---|---|---|
| Overall feature weight | Σ over 13 = 1 | reporting / audit |
| Within-bucket weight (`a`, `g`, `t` sets) | Σ within a bucket = 1 | sub-index roll-up |
| Bucket weight | Σ over 3 buckets = 1 | overall roll-up |

### 1.2 Outputs to track (per parcel, per `run_id`)

Every row in `fiber_idx_v1_parcel` carries, for the 13 features:
- **raw value** (e.g. `cable_penetration`, `dist_nearest_fiber`) — distances emitted in **meters**;
- **scaled value** (`<feature>_scaled`, 0–1) — recomputed from frozen `scaling_params`;
- **within-bucket weight** (`<feature>_weight`) — constant, broadcast to every row;

plus the four indices, the three bucket weights, geometry (`geometry`, `h3_id`), identifiers
(`parcel_shape_id`, `census_block_id`, `tract_geoid`), and run metadata (`run_id`, `created_at`).

> **Engineering: what must be monitored for determinism.** Scores are only reproducible if the
> per-`run_id` artifacts are frozen. Track these three tables together as a versioned unit:
> `feature_weights`, `scaling_params`, and the output `parcel_scores` / `fiber_idx_v1_parcel`.
> A change in any upstream global statistic (min, max, P99, hotspot max) silently shifts every
> score — see §3.3.

---

## 2. Features & cutoff decisions

Three buckets, 13 features, at three grains. **Grain map: growth → parcel, telecom → block,
demo → tract.** The parcel is the scoring grain; telecom features are joined at census-block
level and demographic features are broadcast from the census tract.

All per-feature rules live in the constants file. In plain terms:
- **`INVERTED_FEATURES`** — flags features where a *lower* raw value means *higher* opportunity
  (the scaled value is inverted).
- **`SCALING_NA_FILL_RULES`** — the missing / `inf` fill value applied to each feature before scaling.
- **`SCALING_CAP_AS_MAX`** — the distance features that are winsorized (clipped) at a country-wide cap.

*Illustrative within-bucket weights* convey relative importance — the authoritative numbers live
in the `feature_weights` table per `run_id`.

### 2.1 Growth bucket (parcel grain) — weight 0.169

Sourced directly from the parcel table. Captures nearby development activity.

| Feature | Definition | Inverted? | NA fill | Illustrative within-bucket wt |
|---|---|---|---|---|
| `landuse_change_qtr_mi_cnt` | Land-use-change parcels within ¼ mile | No | `0` | 0.18 |
| `pre_early_dev_qtr_mi_cnt` | Pre/early-development parcels within ¼ mile | No | `0` | 0.22 |
| `bldr_dev_qtr_mi_cnt` | Builder/developer parcels within ¼ mile | No | `0` | 0.16 |
| `new_permit_qtr_mi_cnt` | New-permit parcels within ¼ mile | No | `0` | 0.14 |
| `dist_to_nearest_hotspot_m` | Distance (m) to nearest growth hotspot | **Yes** | `1.25 × country-wide max` | 0.30 |

**Cutoff decisions:**
- **¼-mile radius** for all four count features: development activity is treated as local; a
  quarter mile is the neighborhood band used consistently across the growth features.
- **`dist_to_nearest_hotspot_m` NA fill = `1.25 × max`**. A missing hotspot means "no growth
  hotspot nearby" → we place it *beyond* the observed maximum so that, after inversion, it scores
  as **lowest** opportunity. **Known artifact:** at k=8 this extreme fill inflated the feature's
  importance where most tracts were missing; it is carried deliberately but flagged for review, so
  verify the distribution before locking.
- **Winsorize-at-cap:** the feature's upper scaling bound equals the NA cap. Values above the cap
  clip to it; NA-filled rows sit exactly at the cap (scaled = 1 → inverted score = 0).

### 2.2 Telecom bucket (block grain) — weight 0.591

Built from two FCC block-level sources — fixed-broadband speeds (location/provider counts, speeds)
and coverage (housing units, tier metrics) — joined at census-block level. This is the dominant
bucket.

| Feature | Definition | Inverted? | NA/inf fill | Illustrative within-bucket wt |
|---|---|---|---|---|
| `cable_penetration` | `cable_location_count / census_housing_units` | **Yes** | `0` | 0.16 |
| `fiber_opportunity_gap` | `(census_housing_units − fiber_location_count) / census_housing_units` | No | `1.0` | 0.28 |
| `fiber_speed_top_tier` | `fiber_speed_1000_100_only × has_fiber` (fraction 0–1) | **Yes** | `0` | 0.22 |
| `dist_to_nearest_fiber_m` | Distance (m) to nearest existing fiber | **Yes** | `P99` | 0.24 |
| `provider_competitive_landscape_ord` | Ordinal 0–6 market-structure ladder | **Yes** | `0` | 0.10 |

**Cutoff decisions:**
- **Denominator is the block-level `census_housing_units`** (not the tract-level demographic
  housing count). Same concept, different grain and role — never cross-wire the two.
- **Unclipped on purpose:** `cable_penetration` can exceed 1 (cable locations > housing) and
  `fiber_opportunity_gap` can go negative (fiber locations > housing). These are left unclipped and
  the country-wide min-max absorbs the range — **do not** add a `[0,1]` clip.
- **`fiber_opportunity_gap` NA fill = `1.0`:** a block with unknown/zero housing denominator is
  treated as a **maximum gap** (fully un-served) — conservative toward "opportunity present."
- **`cable_penetration` inverted, NA fill = `0`:** less existing cable = more greenfield
  opportunity; a missing value scores as no penetration.
- **`fiber_speed_top_tier` inverted, NA fill = `0`:** `has_fiber = (fiber_location_count > 0) AND
  (fiber_provider_count > 0)`. Lower existing top-tier fiber = more upgrade headroom.
- **`dist_to_nearest_fiber_m` NA fill = `P99`, winsorize at P99:** the P99 cap tames the long
  right tail; unknown distance is treated as "far" → lowest opportunity after inversion. Tunable —
  verify the distribution before locking.
- **Provider competitive landscape ladder** (`_ord`, inverted — fewer providers = more
  opportunity). Note the cable/fiber precedence: once any cable exists, fiber count dominates.

  | Ordinal | Label | Condition |
  |---|---|---|
  | 0 | `no_providers` | copper=0 & cable=0 & fiber=0 |
  | 1 | `greenfield` | copper>0 & cable=0 & fiber=0 |
  | 2 | `cable_but_no_fiber` | cable>0 & fiber=0 |
  | 3 | `fiber_entry` | fiber=1 |
  | 4 | `fiber_duopoly` | fiber=2 |
  | 5 | `fiber_competitive` | fiber=3 |
  | 6 | `fiber_saturated` | fiber>3 |

  Provider counts at block are distinct provider counts per technology; NA counts filled with `0`
  before applying the ladder. **QC after scoring:** ordinal `0` (max opportunity after inversion)
  can also mean uninhabited / water — cross-check against blocks with zero housing units or zero
  FCC-served units so degenerate blocks don't inflate the telecom sub-index.

### 2.3 Demo bucket (tract grain) — weight 0.240

Engineered upstream at tract grain, broadcast to every parcel in the tract.

| Feature | Definition | Inverted? | NA fill | Illustrative within-bucket wt |
|---|---|---|---|---|
| `pop_ch_avg` | 5-year **average** population change | No | `0` | 0.34 |
| `pop_pctch_avg` | 5-year **average** population % change | No | `0` | 0.33 |
| `census_housing_units` | Estimated census housing units (tract) | No | `0` | 0.33 |

**Cutoff decisions:**
- **Use the `_avg` (5-year) columns, not `_1yr`.** The model uses `pop_ch_avg` / `pop_pctch_avg`
  (averaged over 5 years). The single-year `_1yr` columns are dropped and must **not** be scored.
- **NA fill = `0`.** Training dropped NA pop-change tracts; at parcel grain we score everything, so
  NA tracts fill with 0 (neutral). Apply consistently.

### 2.4 Inversion — single source of truth

Five features are inverted (lower raw → higher score): `dist_to_nearest_hotspot_m`,
`dist_to_nearest_fiber_m`, `cable_penetration`, `fiber_speed_top_tier`,
`provider_competitive_landscape_ord`. The scorer reads this set from the constants file — it is
never hardcoded per feature.

---

## 3. Clustering & weight derivation

The weights are **not hand-set** — they are learned. The 13 tract-level features (normalized, and
pruned for redundancy via correlation analysis) are clustered with **K-means** to group census
tracts into market archetypes; a **LightGBM** classifier is then trained to predict those cluster
labels, and its SHAP importances become the feature weights.

**Choice of k = 8.** Cluster separation is weak-but-stable across all k (silhouette ≈ 0.12–0.15),
as expected for continuous socio-infrastructure data with no hard natural groupings. Silhouette
peaks at **k = 6 (0.149)** with **k = 8 (0.146)** close behind, while inertia keeps falling as k
grows. We chose **k = 8** for finer, more actionable segmentation at effectively no cost to cluster
cohesion (Δsilhouette ≈ 0.004 vs k = 6). This is a **revisitable** decision — the full
elbow/silhouette sweep lives in `04_eda_clustering.ipynb`.

| k | inertia | silhouette |
|---|---|---|
| 2 | 969,181 | 0.123 |
| 3 | 886,949 | 0.116 |
| 4 | 809,451 | 0.132 |
| 5 | 750,849 | 0.141 |
| **6** | 694,392 | **0.149** |
| 7 | 657,888 | 0.142 |
| **8** | 620,605 | 0.146 |

### 3.1 From clusters to weights
1. K-means assigns each tract one of 8 cluster labels.
2. A **LightGBM multiclass classifier** is trained to predict the cluster label from the 13
   features.
3. **SHAP** values are computed and collapsed to `mean(|SHAP|)` per feature (averaged over samples
   and classes).
4. The three weight views (within-bucket, bucket, overall) are derived, and the bucket weights are
   checked against the locked v1 shares before publishing.

### 3.2 Why weighted-average-of-sub-indices (not a flat 13-feature sum)
Decomposability. A flat weighted sum makes the overall score non-explainable ("why doesn't growth
+ telecom + demo equal overall?") and lets one high-variance feature dominate. Averaging bounded
0–100 sub-indices contains per-feature variance within its bucket and yields a clean
"X% growth + Y% telecom + Z% demo" narrative. Full pros/cons in the weightage methodology doc.

### 3.3 Determinism — freeze global stats per `run_id`
Because scaling uses **country-wide** statistics, these must be frozen per `run_id` (in the
`scaling_params` table) or scores drift across runs:
- min/max bounds per feature (0–1 scaling),
- `dist_to_nearest_fiber_m` **P99** cap,
- `dist_to_nearest_hotspot_m` **max** (for the `1.25 × max` fill).

At scoring time all 13 features are present at parcel grain, so the weights map **1:1** — no
within-bucket renormalization is needed.

---

## 4. Feature parameter reference

Consolidated view of every feature's grain, inversion, missing-value fill, cap, and (illustrative)
within-bucket weight. Rules are defined once in the constants file; the numeric weights shown are
illustrative — the authoritative values live in the `feature_weights` table per `run_id`.

| Feature | Bucket | Grain | Inverted | NA / inf fill | Cap (winsorize) | Within-bucket wt* |
|---|---|---|---|---|---|---|
| `landuse_change_qtr_mi_cnt` | growth | parcel | No | 0 | — | 0.18 |
| `pre_early_dev_qtr_mi_cnt` | growth | parcel | No | 0 | — | 0.22 |
| `bldr_dev_qtr_mi_cnt` | growth | parcel | No | 0 | — | 0.16 |
| `new_permit_qtr_mi_cnt` | growth | parcel | No | 0 | — | 0.14 |
| `dist_to_nearest_hotspot_m` | growth | parcel | **Yes** | 1.25 × max | 1.25 × max | 0.30 |
| `cable_penetration` | telecom | block | **Yes** | 0 | — | 0.16 |
| `fiber_opportunity_gap` | telecom | block | No | 1.0 | — | 0.28 |
| `fiber_speed_top_tier` | telecom | block | **Yes** | 0 | — | 0.22 |
| `dist_to_nearest_fiber_m` | telecom | block | **Yes** | P99 | P99 | 0.24 |
| `provider_competitive_landscape_ord` | telecom | block | **Yes** | 0 | — | 0.10 |
| `pop_ch_avg` | demo | tract | No | 0 | — | 0.34 |
| `pop_pctch_avg` | demo | tract | No | 0 | — | 0.33 |
| `census_housing_units` | demo | tract | No | 0 | — | 0.33 |

\*Illustrative; authoritative within-bucket weights are stored per `run_id`.

**Scaling method:** country-wide min-max, frozen per `run_id` — per-feature `min` and `max`, plus
the P99 cap for `dist_to_nearest_fiber_m` and the `max × 1.25` fill for `dist_to_nearest_hotspot_m`.
**Bucket weights (v1):** growth **0.169**, telecom **0.591**, demo **0.240**.

---

## 5. Related documents
- `weightage_methodology.MD` — weight types, formulas, min-max vs rank scaling trade-offs.
- `parcel_scoring_qa.md` — full QA/parity notes, per-feature fills, CT vintage, pre-flight checklist.
- `validation_methodology.md` — composite-indicator validation strategy (no ground-truth label).
