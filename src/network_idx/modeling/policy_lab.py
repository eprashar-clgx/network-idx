"""
Compare candidate scaling policies by refitting the model under each.
=====================================================================
The scaling contract's winsorize quantiles are not a tuning knob that can be
chosen by staring at distributions. A cap interacts with the model in two ways a
histogram cannot show: it decides whether corrupt rows survive to capture a KMeans
centroid, and it decides how much variance each feature retains — which is what
SHAP converts into the bucket weights the product ships. The only honest way to
compare two caps is to refit under both and read the weights.

That is what this module does. :func:`build_filled` applies a *candidate* policy
in place of the committed one, and :func:`fit_policy` runs the same
cluster → classify → explain pipeline as ``train`` over the result. The deliberate
duplication of that pipeline is the point: this is a laboratory for policies that
are *not* in the contract, so it must not depend on the contract's current values.

The two knobs are separate because the two feature families fail differently. The
growth counts are sparse (87-96% zero), so a cap set too low lands in the body of
the distribution and collapses them to a handful of distinct values. The
population features are dense but contain outright corruption, so a cap set too
high fails to exclude it. Optimising them jointly is what surfaced that the
best growth cap (P99.99) and the best population cap (P99.9) are not the same
number — see ``notebooks/05_modeling_diagnostics.ipynb``.
"""

import numpy as np
import pandas as pd

from network_idx.constants import ALL_SCORING_FEATURES
from network_idx.constants.scoring_contract import (
    SCALING_CAP_AS_MAX,
    SCALING_DOMAIN_BOUNDS,
    SCALING_NA_FILL_RULES,
    SCORING_BUCKETS,
)

# The two families under test. Every other feature keeps its committed treatment
# so a comparison isolates the policy rather than confounding it.
GROWTH_COUNT_FEATURES = [
    "landuse_change_qtr_mi_cnt",
    "pre_early_dev_qtr_mi_cnt",
    "bldr_dev_qtr_mi_cnt",
    "new_permit_qtr_mi_cnt",
]
POP_FEATURES = ["pop_ch_avg", "pop_pctch_avg"]

FEATURE_BUCKET = {f: b for b, fs in SCORING_BUCKETS.items() for f in fs}


def build_filled(
    raw: pd.DataFrame,
    qg: float = None,
    qp: float = None,
    log1p: bool = False,
) -> pd.DataFrame:
    """Apply a candidate policy to ``raw`` and return the frame a model would fit on.

    ``qg`` caps the growth counts and ``qp`` the population features; ``log1p``
    overrides both with a log transform (evaluated for completeness — it is
    disqualified on product grounds, since these features ship as customer-facing
    columns and a logged count is not an actionable quantity). Features outside the
    two families follow the committed contract unchanged.

    Mirrors ``apply_feature_fills`` rather than calling it, because the whole point
    is to evaluate quantiles that are *not* the committed ones.
    """
    if not log1p and (qg is None or qp is None):
        raise ValueError("Provide both qg and qp, or set log1p=True.")

    out = raw.copy()
    for f in ALL_SCORING_FEATURES:
        col = out[f].replace([np.inf, -np.inf], np.nan)
        rule = SCALING_NA_FILL_RULES[f]

        if f in SCALING_CAP_AS_MAX:
            cap = col.quantile(0.99) if rule == "p99" else col.max() * 1.25
            out[f] = col.clip(upper=cap).fillna(cap)
        elif f in GROWTH_COUNT_FEATURES or f in POP_FEATURES:
            if log1p:
                out[f] = np.log1p(col.clip(lower=0).fillna(0.0))
            else:
                q = qg if f in GROWTH_COUNT_FEATURES else qp
                out[f] = col.clip(upper=col.quantile(q)).fillna(0.0)
        elif f in SCALING_DOMAIN_BOUNDS:
            lo, hi = SCALING_DOMAIN_BOUNDS[f]
            out[f] = col.clip(lower=lo, upper=hi).fillna(float(rule))
        else:
            out[f] = col.fillna(float(rule))
    return out


def fit_policy(filled: pd.DataFrame, k: int = 8, random_state: int = 42) -> dict:
    """Run cluster → classify → explain over ``filled`` and return the comparison metrics.

    Returns ``weights`` (per-feature SHAP share), ``bucket`` (those shares summed per
    bucket — the number the product cares about), ``sizes`` (ascending cluster sizes,
    whose first element exposes a degenerate segment), and the held-out ``acc`` /
    ``f1`` plus ``silhouette``.
    """
    from sklearn.cluster import KMeans
    from sklearn.metrics import accuracy_score, f1_score, silhouette_score
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler
    import lightgbm as lgb
    import shap

    x_scaled = StandardScaler().fit_transform(filled)
    kmeans = KMeans(n_clusters=k, random_state=random_state, n_init=10)
    labels = kmeans.fit_predict(x_scaled)
    sil = float(silhouette_score(
        x_scaled, labels, sample_size=min(10000, len(filled)), random_state=random_state
    ))

    x_tr, x_te, y_tr, y_te = train_test_split(
        filled, labels, test_size=0.2, random_state=random_state, stratify=labels
    )
    clf = lgb.LGBMClassifier(
        objective="multiclass", n_estimators=500, learning_rate=0.05,
        num_leaves=31, n_jobs=-1, random_state=random_state, verbose=-1,
    ).fit(x_tr, y_tr)
    pred = clf.predict(x_te)

    sample = x_te.sample(n=min(2000, len(x_te)), random_state=random_state)
    shap_values = np.asarray(shap.TreeExplainer(clf).shap_values(sample))
    grand_mean = np.abs(shap_values).mean(axis=(0, 2))
    weights = pd.Series(grand_mean / grand_mean.sum(), index=list(filled.columns))

    return {
        "weights": weights,
        "bucket": weights.groupby(FEATURE_BUCKET).sum(),
        "bucket_of": FEATURE_BUCKET,
        "sizes": sorted(pd.Series(labels).value_counts().values.tolist()),
        "acc": float(accuracy_score(y_te, pred)),
        "f1": float(f1_score(y_te, pred, average="macro")),
        "silhouette": sil,
        "labels": labels,
    }


def compare_policies(raw: pd.DataFrame, grid: dict, k: int = 8) -> tuple:
    """Refit under every ``{name: (qg, qp)}`` in ``grid`` and return
    ``(summary_frame, results_by_name)``.

    The summary's ``smallest`` column is the one to read alongside ``macro_f1``: a
    policy can post a respectable accuracy while still admitting a degenerate
    segment, and that combination means the cap failed to exclude corrupt rows.
    """
    results, rows = {}, []
    for name, (qg, qp) in grid.items():
        res = fit_policy(build_filled(raw, qg=qg, qp=qp), k=k)
        results[name] = res
        rows.append({
            "policy": name,
            "growth_q": qg,
            "pop_q": qp,
            "accuracy": res["acc"],
            "macro_f1": res["f1"],
            "silhouette": res["silhouette"],
            "smallest": res["sizes"][0],
            **{b: res["bucket"].get(b, 0.0) for b in ("growth", "telecom", "demo")},
        })
    return pd.DataFrame(rows).set_index("policy"), results
