# network-idx — Domain Model

The parcel-level **Fiber Potential Index**: a composite indicator that scores every US
parcel 0–100 for fiber build-out opportunity, from five data sources across three
geographic grains. This glossary fixes the vocabulary the codebase and its docs use.
It is a glossary only — no implementation details.

## Product / index

**Fiber Potential Index**:
The delivered 0–100 opportunity score per parcel, and its three sub-indices. Higher =
more opportunity (less existing service, more growth).
_Avoid_: fiber score, network index, the model output.

**Composite indicator**:
A measure of a latent construct that has no directly observed label. "Fiber potential"
is one, so the index is judged by construct validity, never accuracy.
_Avoid_: model prediction, classifier output.

**Sub-index**:
One of the three thematic components of the index — **growth**, **telecom**,
**demographic** — each a weighted blend of its bucket's features, rescaled 0–100.
_Avoid_: category score, group.

**Opportunity direction**:
The convention that a *lower* raw value on an inverted feature means *higher*
opportunity (e.g. less existing fiber, fewer providers).
_Avoid_: polarity, sign.

**Run**:
A frozen scoring configuration identified by `run_id` (model + k + version). Weights and
scaling bounds are frozen per run so re-scored parcels never shift within a run.
_Avoid_: version, batch, job.

## Grains & populations

**Grain**:
The geographic resolution of a table — **parcel**, **block**, or **tract**. Every
feature has one native grain and is moved to others by grain transfer.
_Avoid_: level, resolution, granularity (informally OK, but "grain" in code/docs).

**Training population**:
The tract rows the model was trained on (housing-unit and pop-change filters applied).
_Avoid_: train set (it has no label — it is a clustering/EDA population).

**Scored universe**:
Every parcel the index is computed for — deliberately broader than the training
population.
_Avoid_: test set, scoring set.

## Source families

**Source family**:
One of the five origins of features — **FCC**, **Census**, **Demographic** (in-house),
**Location** (in-house), **Rextag** (in-house). The `features` module is split by family.
_Avoid_: dataset, feed, vendor.

**Dasymetric interpolation**:
Disaggregating FCC place/county coverage summaries down to blocks using census
housing-unit counts as the weight, then rolling blocks up to tracts weighted by each
block's distributed FCC unit count.
_Avoid_: areal interpolation, downscaling.

## Features

**Transformed feature**:
A source column reshaped to its native grain by a deterministic transformation
(dasymetric interpolation, pivot, grain cut) — no analytical choice involved. Lives in
`features/<family>/transform`.
_Avoid_: base feature, raw feature, primitive.

**Engineered feature**:
A feature whose *definition* was chosen through EDA/analysis (penetration, opportunity
gap, provider landscape ordinal, top-tier), but whose *computation* is deterministic
given the run's constants. Lives in `features/<family>/engineered`. The analysis that
discovers it lives in modeling/validation; only the codified definition lives here.
_Avoid_: derived feature, computed feature, EDA feature.

## Pipeline concepts (deepened modules)

**Grain transfer**:
Moving a feature from its native grain to another — *aggregating up*
(parcel→block→tract) or *broadcasting down* (tract→block→parcel). Its own seam, not a
step inside feature building.
_Avoid_: transfer, promotion, rollup (use "grain transfer" as the noun).

**Modeling**:
Deriving the scoring rules from the training population: training the classifier
(cluster→classify→SHAP) and fitting the scoring rules (weights + scaling params). Reruns
on a model refresh; notebooks are thin drivers over it.
_Avoid_: training, ML, analytics.

**Scoring**:
Applying frozen weights and scaling params to the scored universe to produce the index
and the delivery table.
_Avoid_: prediction, inference.

**Scaling params**:
The frozen per-feature rule set (na_fill, min, max, invert, winsorize cap) for a run,
applied identically in training and scoring — the training/scoring parity guarantee.
_Avoid_: normalization config, scaler.

**Data contract check**:
A per-run **input gate** at the sources→processing seam — schema match, null/anomaly
thresholds, row-count sanity — that halts and alerts on failure. An input-side concern
reported through monitoring. Distinct from validation.
_Avoid_: data validation (collides with the construct-validity module), schema test.

**Run artifact**:
The single git-committed JSON file for a `run_id` (`artifacts/runs/<run_id>.json`)
holding everything needed to audit and reproduce a run: code version, the training-data
table it was fit on, hyperparameters, fit metrics (cluster inertia/silhouette, classifier
macro-F1), the scaler and cluster-center profiles, feature weights, and scaling params.
Nothing in it is a serialized model object (scaler/kmeans/classifier) beyond the small
numeric arrays needed for interpretation — the classifier itself is disposable once its
SHAP attributions are collapsed into feature weights, since a rerun with the same frozen
seed reproduces it exactly.
_Avoid_: pickle, joblib, model binary.

**Run registry**:
The `run_id`-keyed BigQuery ledger row pointing at a run's artifact — model, k, version,
`artifact_path` (into the git-committed run artifact), code version, training-data id,
created-at. A thin index, not the payload; the backbone for temporal validation. Written
by modeling, read by scoring.
_Avoid_: model registry, artifact store.

**Monitoring**:
Cheap, automated, every-run health of the pipeline and its outputs — null/fill rates,
drift vs the frozen baseline, train/scoring parity, score-band and business rollups.
Splits into **feature monitoring** and **score monitoring**.
_Avoid_: QA, observability, logging.

**Feature monitoring**:
Every-run health of the *input features themselves*, at their native grain — null/fill
and inf rates, value distributions, quantile-band counts, and per-state rollups of each
feature before it is scored. Distinct from the data contract check (which gates raw
inputs) and from score monitoring (which watches the output index).
_Avoid_: feature validation, feature QA, profiling.

**Score monitoring**:
Every-run health of the *output index* — score-band counts, index-quartile business
rollups (per-state, scores>85), and drift of the delivered score vs the frozen baseline.
_Avoid_: output validation, business QA.

**Dasymetric conservation check**:
A deterministic correctness assertion on the FCC block interpolation: re-aggregated block
unit counts must reconstruct the FCC source total per county × technology × speed tier,
within a rounding budget. A data-contract-style gate that halts on breach — it catches
share/residual algebra bugs, not statistical drift.
_Avoid_: interpolation test, mass-balance QA.

**Validation**:
The periodic construct-validity dossier for the composite indicator — sensitivity,
internal coherence, spatial coherence, external convergent checks (ACS, BEAD, peer
indices), and expert review. Distinct from monitoring.
_Avoid_: testing, QA, evaluation.
