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
Disaggregating FCC place/county coverage summaries down to blocks using location counts
as the weight, then rolling blocks up to tracts.
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

**Run registry**:
The `run_id`-keyed record of everything needed to reproduce and score a run — the model
artifact, feature weights, scaling params, code version, training-data id, and fit
metrics. Written by modeling, read by scoring; the backbone for temporal validation.
_Avoid_: model registry, artifact store.

**Monitoring**:
Cheap, automated, every-run health of the pipeline and its outputs — null/fill rates,
drift vs the frozen baseline, train/scoring parity, score-band and business rollups.
_Avoid_: QA, observability, logging.

**Validation**:
The periodic construct-validity dossier for the composite indicator — sensitivity,
internal coherence, spatial coherence, external convergent checks (ACS, BEAD, peer
indices), and expert review. Distinct from monitoring.
_Avoid_: testing, QA, evaluation.
