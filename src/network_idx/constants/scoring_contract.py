"""
Parcel-level scoring contract for network_idx.

This module is the single source of truth for how the parcel-level Fiber Potential
Index is built: the thirteen model features and which bucket (growth, telecom, or
demographic) each belongs to, the group weights derived from the model, which
features are inverted so that a lower raw value means a higher opportunity score,
the provider-landscape ordinal map, the null-fill and winsorising rules used when
scaling features onto a common range, the mapping from trained-model feature names
to canonical pipeline names, the run identity, and the customer-facing delivery
column and index names. Anything about which features exist, how they are scaled,
and how they are named for delivery lives here.
"""

# ── Feature buckets (the thirteen model features) ─────────────────────────────
GROWTH_FEATURES = [
    "landuse_change_qtr_mi_cnt",
    "pre_early_dev_qtr_mi_cnt",
    "bldr_dev_qtr_mi_cnt",
    "new_permit_qtr_mi_cnt",
    "dist_to_nearest_hotspot_miles",
]

TELECOM_FEATURES = [
    "cable_penetration",
    "fiber_opportunity_gap",
    "fiber_speed_top_tier",
    "dist_to_nearest_fiber_miles",
    "provider_competitive_landscape_ord",
]

DEMO_FEATURES = [
    "pop_ch_avg",
    "pop_pctch_avg",
    "census_housing_units",
]

# Canonical column order for the weight vector (growth → telecom → demographic).
ALL_SCORING_FEATURES = GROWTH_FEATURES + TELECOM_FEATURES + DEMO_FEATURES

SCORING_BUCKETS = {
    "growth": GROWTH_FEATURES,
    "telecom": TELECOM_FEATURES,
    "demo": DEMO_FEATURES,
}

# v1 group weights (raw-SHAP group shares). The weight builder recomputes these from
# the model and asserts they match before writing the run.
SCORING_BUCKET_WEIGHTS = {
    "growth": 0.169,
    "telecom": 0.591,
    "demo": 0.240,
}

# Features where a LOWER raw value means a HIGHER opportunity score (invert on scaling).
INVERTED_FEATURES = {
    "dist_to_nearest_hotspot_miles",
    "dist_to_nearest_fiber_miles",
    "cable_penetration",
    "fiber_speed_top_tier",
    "provider_competitive_landscape_ord",
}

# provider_competitive_landscape text label → ordinal (kept alongside the label).
PROVIDER_LANDSCAPE_ORDER = {
    "no_providers": 0,
    "greenfield": 1,
    "cable_but_no_fiber": 2,
    "fiber_entry": 3,
    "fiber_duopoly": 4,
    "fiber_competitive": 5,
    "fiber_saturated": 6,
}

# Telecom block-feature null fills (applied to both NaN and +/-inf).
TELECOM_FEATURE_NA_FILL = {
    "cable_penetration": 0.0,
    "fiber_opportunity_gap": 1.0,
}

# ── Run identity ──────────────────────────────────────────────────────────────
SCORING_RUN_MODEL = "lightgbm"
SCORING_RUN_K = 8
SCORING_RUN_VERSION = "v1"
SCORING_RUN_ID = f"{SCORING_RUN_MODEL}_k{SCORING_RUN_K}_{SCORING_RUN_VERSION}"

# ── Scaling rules for the parcel index ────────────────────────────────────────
# Null fill per feature. A numeric value fills with that constant. The strings
# 'p99' and 'max_x1.25' are data-derived country-wide caps computed once and
# persisted in the scaling parameters.
SCALING_NA_FILL_RULES = {
    "landuse_change_qtr_mi_cnt": 0.0,
    "pre_early_dev_qtr_mi_cnt": 0.0,
    "bldr_dev_qtr_mi_cnt": 0.0,
    "new_permit_qtr_mi_cnt": 0.0,
    "dist_to_nearest_hotspot_miles": "max_x1.25",
    "cable_penetration": 0.0,
    "fiber_opportunity_gap": 1.0,
    "fiber_speed_top_tier": 0.0,
    "dist_to_nearest_fiber_miles": "p99",
    "provider_competitive_landscape_ord": 0.0,
    "pop_ch_avg": 0.0,
    "pop_pctch_avg": 0.0,
    "census_housing_units": 0.0,
}

# Distance features whose UPPER scaling bound equals their null cap (winsorize at
# the cap): values above the cap clip to it, and null-filled rows sit exactly at
# the cap (scaled = 1 → inverted score = 0, i.e. "far / unknown = lowest opportunity").
SCALING_CAP_AS_MAX = {
    "dist_to_nearest_fiber_miles",
    "dist_to_nearest_hotspot_miles",
}

# Upper winsorize caps that are NOT the null fill (the fill stays per
# SCALING_NA_FILL_RULES). Growth counts have a fat right tail; the model was
# trained winsorised at the 99.5th percentile.
SCALING_WINSORIZE_QUANTILE = {
    "landuse_change_qtr_mi_cnt": 0.995,
    "pre_early_dev_qtr_mi_cnt": 0.995,
    "bldr_dev_qtr_mi_cnt": 0.995,
    "new_permit_qtr_mi_cnt": 0.995,
}

# Proportion features bounded to the fixed domain [0, 1] at scoring time. Values
# outside (data artefacts — e.g. fiber_location_count far exceeding
# census_housing_units giving a negative fiber_opportunity_gap, or
# cable_location_count exceeding housing giving cable_penetration above 1) clip into
# range, restoring parity with the tract builder's clip used for weight derivation.
# The raw unclipped value is kept in parcel_features for drift and null monitoring;
# the clip only takes effect in the scaling parameters, so its impact can be measured.
SCALING_DOMAIN_BOUNDS = {
    "cable_penetration": (0.0, 1.0),
    "fiber_opportunity_gap": (0.0, 1.0),
}

# Maps trained-model feature names → parcel_features canonical column names. Used by
# the weight builder when exporting SHAP weights.
MODEL_TO_SCORING_FEATURE = {
    "median_landuse_change_qtr_mi_cnt": "landuse_change_qtr_mi_cnt",
    "median_pre_early_dev_qtr_mi_cnt": "pre_early_dev_qtr_mi_cnt",
    "median_bldr_dev_qtr_mi_cnt": "bldr_dev_qtr_mi_cnt",
    "median_new_permit_qtr_mi_cnt": "new_permit_qtr_mi_cnt",
    "median_dist_nearest_hotspot": "dist_to_nearest_hotspot_miles",
    "median_dist_nearest_fiber_m": "dist_to_nearest_fiber_miles",
    "median_dist_nearest_hotspot_miles": "dist_to_nearest_hotspot_miles",
    "median_dist_nearest_fiber_miles": "dist_to_nearest_fiber_miles",
    "estimated_census_housing_units": "census_housing_units",
    # telecom + pop_ch/pctch names already match — identity
}

# ── Delivery table (fiber_idx_v1_parcel) naming ───────────────────────────────
# Canonical pipeline feature → customer-facing delivery column name.
DELIVERY_FEATURE_NAMES = {
    "pop_ch_avg": "pop_ch_avg",
    "pop_pctch_avg": "pop_pctch_avg",
    "census_housing_units": "census_housing_units",
    "pre_early_dev_qtr_mi_cnt": "pre_early_dev_qtr_mi_cnt",
    "bldr_dev_qtr_mi_cnt": "bldr_dev_qtr_mi_cnt",
    "landuse_change_qtr_mi_cnt": "landuse_change_qtr_mi_cnt",
    "new_permit_qtr_mi_cnt": "new_permit_qtr_mi_cnt",
    "dist_to_nearest_hotspot_miles": "dist_nearest_hotspot_miles",
    "dist_to_nearest_fiber_miles": "dist_nearest_fiber_miles",
    "provider_competitive_landscape_ord": "provider_competitive_landscape_type_code",  # zero-padded STRING '00'..'06'
    "cable_penetration": "cable_penetration",
    "fiber_opportunity_gap": "fiber_opportunity_gap",
    "fiber_speed_top_tier": "fiber_speed_top_tier",
}

# Sub-index / overall → delivery names.
DELIVERY_INDEX_NAMES = {
    "demo": "demographic_index",
    "growth": "growth_index",
    "telecom": "telecom_index",
    "overall": "fiber_potential_index",
}

# ord → text label (inverse of PROVIDER_LANDSCAPE_ORDER) for the STRING column.
PROVIDER_LANDSCAPE_LABEL = {v: k for k, v in PROVIDER_LANDSCAPE_ORDER.items()}
