# Index Construction: Weight Types & Methodology

> **Vintage.** All numbers on this page are the **LightGBM k=8 v2** run
> (`run_id = lightgbm_k8_v2`, fit 2026-09-25 on 85,395 tracts; artifact
> `artifacts/runs/lightgbm_k8_v2.json`). Total raw SHAP mass = **7.1087**. Earlier
> revisions of this document quoted a Random Forest run (group shares 0.40 / 0.44 / 0.16);
> those numbers illustrated the *method* only and are superseded. The single source of
> truth is `constants/scoring_contract.py :: SCORING_BUCKET_WEIGHTS`, materialized per
> `run_id` in the `feature_weights` table.

## Weight Types Produced

### 1. Individual Feature Weights (13 features)
**What:** Each feature's SHAP importance normalized so all 13 sum to 1.

**Formula:**
$$w_i = \frac{\text{mean}(|\text{SHAP}_i|)}{\sum_{j=1}^{13} \text{mean}(|\text{SHAP}_j|)}$$

**Example:**
- `dist_to_nearest_hotspot_miles`: 1.1738 / 7.1087 = **0.1651**
- `fiber_opportunity_gap`: 1.1280 / 7.1087 = **0.1587**
- `fiber_speed_top_tier`: 1.0053 / 7.1087 = **0.1414**
- ...all 13 sum to 1.0

**Used for:** reporting a feature's global contribution. *Not* used for the delivered
overall index — see "Decision" below.

---

### 2. Feature Weights Normalized to Bucket (13 features, grouped into 3 buckets)
**What:** Same raw SHAP values, but normalized within each group so each group sums to 1 independently.

**Formula (e.g., Growth):**
$$w_i^{\text{growth}} = \frac{\text{mean}(|\text{SHAP}_i|)}{\sum_{j \in \text{growth}} \text{mean}(|\text{SHAP}_j|)}$$

**Example (Growth group, raw SHAP sum = 1.4206):**
- `dist_to_nearest_hotspot_miles`: 1.1738 / 1.4206 = **0.826**
- `pre_early_dev_qtr_mi_cnt`: 0.0913 / 1.4206 = **0.064**
- `new_permit_qtr_mi_cnt`: 0.0869 / 1.4206 = **0.061**
- `bldr_dev_qtr_mi_cnt`: 0.0377 / 1.4206 = **0.027**
- `landuse_change_qtr_mi_cnt`: 0.0309 / 1.4206 = **0.022**
- ...5 features sum to 1.0

**Used for:** Sub-index (growth / telecom / demo) calculation.

---

### 3. Sub-Index Scores (3 scores per parcel: growth, telecom, demo)
**What:** Weighted sum of scaled feature values within each bucket.

**Steps:**
1. Scale each feature's raw values to [0, 1] (country-wide min-max, frozen per `run_id`)
2. Invert features where required (see section below on all features that are inverted)
3. Multiply each scaled value by its bucket-normalized weight
4. Sum across features in the bucket → raw score in [0, 1]
5. Rescale to [0, 100]

**Formula:**
$$\text{idx\_growth} = \text{rescale}_{0-100}\left(\sum_{i \in \text{growth}} w_i^{\text{growth}} \times v_i^{\text{scaled}}\right)$$

---

### 4. Overall Index Score (1 score per parcel)
**What:** Bucket-weighted average of the three sub-indices.

**Formula:**
$$\text{idx\_overall} = \sum_{b \in \{\text{growth},\ \text{telecom},\ \text{demo}\}} W_b \times \text{idx}_b$$

$$\text{idx\_overall} = 0.200 \times \text{idx\_growth} + 0.562 \times \text{idx\_telecom} + 0.238 \times \text{idx\_demo}$$

---

## Decision: overall = weighted average of sub-indices (ADOPTED)

Two constructions were considered. **The weighted-average-of-sub-indices approach was
adopted** and is what the pipeline ships; the flat 13-feature construction is retained here
only as the rejected alternative.

### Adopted — Overall = weighted average of 3 sub-indices

**How it works:**
- Compute sub-indices as above (growth, telecom, demo each 0–100)
- Group weights = each group's share of total raw SHAP:
  - Growth: 1.4206 / 7.1087 = **0.200**
  - Telecom: 3.9955 / 7.1087 = **0.562**
  - Demo: 1.6927 / 7.1087 = **0.238**
- Overall = 0.200 × idx_growth + 0.562 × idx_telecom + 0.238 × idx_demo

**Why:**
- Fully decomposable: "Your overall score is 20% growth + 56% telecom + 24% demographic"
- Easy to explain to customers and stakeholders
- Sub-indices are meaningful building blocks of the overall
- Natural guard against one feature dominating (variance is contained within each sub-index's 0–100 range before combining)
- Correlation between sub-indices and overall is more balanced

**Accepted costs:**
- Two levels of normalization (within-group then across-group) slightly distort the raw SHAP proportions
- A feature's effective overall weight depends on both its within-group weight AND its group weight, which may not exactly match its global SHAP proportion
- Sub-index scaling before combining means the overall index is sensitive to the distributional shape of each sub-index

### Rejected — Overall computed independently from all 13 features

**How it would work:**
- Normalize SHAP across all 13 features (sum to 1)
- Compute weighted sum of all 13 scaled features
- Rescale to 0–100

**Why rejected:**
- Sub-indices and overall are **not decomposable** — you can't explain overall as "X% growth + Y% telecom + Z% demo"
- Weight normalization differs between sub-indices and overall (0.826 in growth vs 0.165 overall for hotspot distance)
- Harder to explain to customers: "Why doesn't my growth score + telecom score = overall score?"
- Variance dominance: a single high-variance feature (hotspot distance) can dominate the overall index, causing high correlation with one sub-index

---

## Scaling Methods

| Method | Mechanism | Pros | Cons |
|--------|-----------|------|------|
| **Min-max** (adopted) | $(v - \min) / (\max - \min)$ | Preserves relative distances between values; interpretable | Sensitive to outliers; skewed distributions create uneven spread — mitigated by winsorizing (below) |
| **Rank (percentile)** | Each value → its percentile rank [0, 1] | Robust to outliers and skew; uniform variance across features | Destroys magnitude information; a tract at P90 vs P95 looks the same distance apart as P50 vs P55 |

Min-max is used, with outlier sensitivity handled by **winsorizing** rather than by
switching to rank — the delivered feature columns are a product in their own right, and
rank scaling would strip the magnitudes customers need. Caps (`SCALING_WINSORIZE_QUANTILE`,
`SCALING_CAP_AS_MAX`, `SCALING_DOMAIN_BOUNDS`):

| Features | Cap |
|---|---|
| 4 growth counts | P99.99 |
| `pop_ch_avg`, `pop_pctch_avg` | P99.9 |
| `dist_to_nearest_fiber_miles` | P99 (also the NA fill) |
| `dist_to_nearest_hotspot_miles` | 1.25 × max (also the NA fill) |
| `cable_penetration`, `fiber_opportunity_gap` | domain-bounded to `[0, 1]` |

## Inverted Features (lower raw value = higher index score)
- `dist_to_nearest_hotspot_miles` — closer to growth hotspot = better
- `dist_to_nearest_fiber_miles` — closer to existing fiber = better
- `cable_penetration` — less cable = more greenfield opportunity
- `fiber_speed_top_tier` — lower existing fiber speed = more upgrade opportunity
- `provider_competitive_landscape_ord` — fewer providers = less saturated market

## Open Decisions

- [x] Cap/winsorize hotspot distance before scaling as an alternative to rank — **done**;
      winsorizing adopted across six features plus two domain bounds (see table above).
- [x] Switch from Random Forest to LightGBM for faster SHAP iteration — **done**; v1 and v2
      are both LightGBM k=8.
- [ ] **Growth bucket is hollow.** `dist_to_nearest_hotspot_miles` carries **82.6%** of the
      growth bucket, and refits show this is not a fill artifact — the four counts carry
      little signal, and the distance is censored at a 15-mile collection radius. Either
      strengthen the growth features or rename/shrink the bucket. See `QA.md` QA-11.
- [ ] **Zero-housing blocks score maximum telecom opportunity.** ~85% of the telecom bucket
      (weight 0.562) reaches its maximum when the housing denominator is zero or missing.
      See `QA.md` QA-1 and QA-9.
