# Index Construction: Weight Types & Methodology

## Weight Types Produced

### 1. Individual Feature Weights (13 features)
**What:** Each feature's SHAP importance normalized so all 13 sum to 1.

**Formula:**
$$w_i = \frac{\text{mean}(|\text{SHAP}_i|)}{\sum_{j=1}^{13} \text{mean}(|\text{SHAP}_j|)}$$

**Example:**
- median_dist_nearest_hotspot: 0.0570 / 0.347 = **0.164**
- fiber_opportunity_gap: 0.0486 / 0.347 = **0.140**
- ...all 13 sum to 1.0

**Used for:** Overall index calculation.

---

### 2. Feature Weights Normalized to Bucket (13 features, grouped into 3 buckets)
**What:** Same raw SHAP values, but normalized within each group so each group sums to 1 independently.

**Formula (e.g., Growth):**
$$w_i^{\text{growth}} = \frac{\text{mean}(|\text{SHAP}_i|)}{\sum_{j \in \text{growth}} \text{mean}(|\text{SHAP}_j|)}$$

**Example (Growth group):**
- median_dist_nearest_hotspot: 0.0570 / 0.1377 = **0.414**
- pre_early_dev_parcels: 0.0302 / 0.1377 = **0.219**
- ...5 features sum to 1.0

**Used for:** Sub-index (growth/FCC/demo) calculation.

---

### 3. Sub-Index Scores (3 scores per tract: growth, FCC, demo)
**What:** Weighted sum of scaled feature values within each bucket.

**Steps:**
1. Scale each feature's raw values to [0, 1] (via min-max or rank)
2. Invert distance features (1 - scaled) so closer = higher
3. Multiply each scaled value by its bucket-normalized weight
4. Sum across features in the bucket → raw score in [0, 1]
5. Min-max rescale to [0, 100]

**Formula:**
$$\text{idx\_growth} = \text{rescale}_{0-100}\left(\sum_{i \in \text{growth}} w_i^{\text{growth}} \times v_i^{\text{scaled}}\right)$$

---

### 4. Overall Index Score (1 score per tract)
**What:** Weighted sum of scaled feature values across ALL 13 features, using individual feature weights (type 1).

**Formula:**
$$\text{idx\_overall} = \text{rescale}_{0-100}\left(\sum_{i=1}^{13} w_i \times v_i^{\text{scaled}}\right)$$

---

## Current Approach vs. Weighted Average of Sub-Indices

### Current: Overall index computed independently from all 13 features

**How it works:**
- Normalize SHAP across all 13 features (sum to 1)
- Compute weighted sum of all 13 scaled features
- Rescale to 0-100

**Pros:**
- Theoretically consistent: each feature's contribution is proportional to its global SHAP importance
- No information loss from intermediate aggregation

**Cons:**
- Sub-indices and overall are **not decomposable** — you can't explain overall as "X% growth + Y% FCC + Z% demo"
- Weight normalization differs between sub-indices and overall (0.414 in growth vs 0.164 overall for hotspot distance)
- Harder to explain to customers: "Why doesn't my growth score + FCC score = overall score?"
- Variance dominance issue: a single high-variance feature (hotspot distance) can dominate the overall index, causing high correlation with one sub-index (growth-overall ρ = 0.946)

---

### Alternative: Overall = weighted average of 3 sub-indices

**How it works:**
- Compute sub-indices as before (growth, FCC, demo each 0-100)
- Group weights = each group's share of total raw SHAP:
  - Growth: 0.1377 / 0.347 = **0.40**
  - FCC: 0.1541 / 0.347 = **0.44**
  - Demo: 0.0552 / 0.347 = **0.16**
- Overall = 0.40 × idx_growth + 0.44 × idx_fcc + 0.16 × idx_demo

**Pros:**
- Fully decomposable: "Your overall score is 40% growth + 44% FCC + 16% demographic"
- Easy to explain to customers and stakeholders
- Sub-indices are meaningful building blocks of the overall
- Natural guard against one feature dominating (variance is contained within each sub-index's 0-100 range before combining)
- Correlation between sub-indices and overall will be more balanced

**Cons:**
- Two levels of normalization (within-group then across-group) slightly distort the raw SHAP proportions
- A feature's effective overall weight depends on both its within-group weight AND its group weight, which may not exactly match its global SHAP proportion
- Sub-index scaling (0-100) before combining means the overall index is sensitive to the distributional shape of each sub-index

---

## Scaling Methods

| Method | Mechanism | Pros | Cons |
|--------|-----------|------|------|
| **Min-max** | $(v - \min) / (\max - \min)$ | Preserves relative distances between values; interpretable | Sensitive to outliers; skewed distributions create uneven spread (hotspot distance: 59% variance) |
| **Rank (percentile)** | Each value → its percentile rank [0, 1] | Robust to outliers and skew; uniform variance across features | Destroys magnitude information; a tract at P90 vs P95 looks the same distance apart as P50 vs P55 |