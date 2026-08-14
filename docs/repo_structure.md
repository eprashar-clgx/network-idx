# Repo structure & pipeline architecture (target state)

This document describes the **production-ready target architecture** for the parcel-level
**Fiber Potential Index**. It is the map the rearchitecture is executing toward: the
module spine, the data flow, and — most importantly — **where each piece of logic lives**.

- Vocabulary is fixed in [`../CONTEXT.md`](../CONTEXT.md) (module, grain, transformed vs
  engineered feature, modeling, scoring, monitoring, validation, run registry, …).
- Load-bearing decisions are recorded in [`adr/`](./adr): monitoring↔validation split
  (0001), modeling two-interface seam (0002), BigQuery-prod source-of-truth (0003),
  composite-indicator validation stance (0004).
- The previous (current-state) description is preserved at
  [`repo_structure.prev.md`](./repo_structure.prev.md) during migration.

---

## 1. The module spine

Nine purpose-named modules replace the old `data / processing / feature_engg / transfer /
scoring / eda` layout. Each module answers exactly one question; the old `feature_engg`
grab-bag is dissolved.

```mermaid
flowchart LR
  SRC[sources] --> PROC[processing]
  PROC --> FEAT[features]
  FEAT <--> GT[grain_transfer]
  FEAT --> MODEL[modeling]
  MODEL --> SCORE[scoring]
  FEAT --> SCORE
  SCORE --> DELIV[(delivery + outputs)]
  SCORE --> MON[monitoring]
  DELIV --> VAL[validation]
  MODEL -. run registry .-> SCORE
  classDef deep fill:#0f172a,color:#fff,stroke:#0f172a;
  class MODEL,SCORE deep
```

| Module | One-line purpose | Cadence |
| --- | --- | --- |
| `sources` | Ingest raw data behind adapters (ADR-0003) | per refresh |
| `processing` | Reshape raw → **one canonical block-level frame** | per refresh |
| `features` | Build features per **source family**, in `transform` + `engineered` layers | per refresh |
| `grain_transfer` | Move features across grains (aggregate-up / broadcast-down) | per refresh |
| `modeling` | Derive scoring rules: `train` + `fit_scoring_rules` (ADR-0002) | model ≪ data |
| `scoring` | Apply frozen weights + scaling params → index + delivery | per refresh |
| `monitoring` | Every-run health + data-contract gate + business rollups | every run |
| `validation` | Periodic construct-validity dossier (ADR-0004) | periodic |
| `config` / `constants` / `utils` | Shared configuration, the feature contract, helpers | — |

---

## 2. Target `src/network_idx` tree

```
src/network_idx/
├── config.py                 # env-specific settings (paths, BQ datasets, creds)
├── constants.py              # THE feature contract: 13 features, buckets, invert set,
│                             #   scaling rules, run identity, delivery names
├── utils.py
│
├── sources/                  # ADR-0003 — ingestion behind a single interface
│   ├── __init__.py           #   get_raw(family, ...) — callers don't know the origin
│   ├── bq_prod.py            #   BigQuery-prod adapter (FCC, demographic, location, rextag)
│   └── data_download/        #   dev/backfill adapter (was `data/`)
│       ├── census_baf.py         # Census BAF (still downloaded)
│       ├── census_acl.py         # Census Address Count Listing (still downloaded)
│       ├── fcc_fixed_summary.py  # FCC download — backfill only
│       └── fcc_fixed_speeds.py   # FCC download — backfill only
│
├── processing/               # → ONE canonical block-level frame (copper/fiber/cable cols)
│   ├── block_frame.py        #   build_block_frame() -> wide, one row per block
│   ├── census_baf.py
│   ├── census_addresscountlisting.py
│   ├── fcc_fixed_speeds.py
│   └── fcc_fixed_summary.py
│
├── features/                 # split by SOURCE FAMILY; two layers per family
│   ├── parcel_features.py     # assemble family outputs → parcel-grain feature frame
│   │   (+ parcel_features.sql)#   (the scoring input: 13 features @ parcel)
│   ├── telecom/
│   │   ├── transform/         # BATCH 1 — dasymetric interpolation, block/tract cuts, pivots
│   │   │   ├── fcc_coverage_block.py  (+ .sql)
│   │   │   └── fcc_speeds_block.py
│   │   └── engineered/        # BATCH 2 — penetration, opportunity gap, landscape ord, top-tier
│   │       └── telecom_features_block.py (+ .sql)
│   ├── demographic/           # tract-native; pop_ch_avg / pop_pctch_avg (5-yr), housing units
│   │   ├── transform/         (+ demo_population_tract.sql)
│   │   └── engineered/
│   ├── location/              # parcel-native
│   │   ├── transform/
│   │   └── engineered/        # growth counts within qtr-mi, dist-to-nearest-hotspot
│   └── rextag/                # parcel-native
│       ├── transform/
│       └── engineered/        # dist-to-nearest-fiber
│
├── grain_transfer/           # aggregate-up / broadcast-down; SQL generated from a spec
│   ├── promote.py            #   promote(source_table, spec) -> target-grain table
│   ├── specs.py              #   per-family agg (sum/mean/housing-wtd) & broadcast specs
│   └── adapters/
│       ├── bigquery.py       #   prod
│       └── duckdb.py         #   test stand-in (local-substitutable)
│
├── modeling/                 # ADR-0002 — two interfaces; notebooks are thin drivers
│   ├── train.py              #   train(frame) -> model_artifacts (cluster→classify→SHAP)
│   ├── fit_rules.py          #   fit_scoring_rules(frame, shap) -> {weights, scaling_params}
│   └── registry.py           #   run registry: write run_id-keyed artifacts + metadata
│
├── scoring/                  # apply frozen rules; produces index + delivery
│   ├── scaling.py            #   scaling_params fit/apply (the parity layer)
│   ├── weights.py            #   feature_weights read/write
│   ├── build_scaling_params.py   # thin driver over modeling.fit_rules
│   ├── build_weights.py          # thin driver over modeling.fit_rules
│   └── parcel_score.py       #   scale → sub-indices → weighted-avg → 0-100 + delivery
│
├── monitoring/               # every-run health (simple/frequent)
│   ├── data_contract.py      #   INPUT GATE (Q12): schema/null/anomaly/row-count → halt+alert
│   ├── metrics.py            #   null/fill rates, drift vs baseline, train/score parity, bands
│   └── business_rollups.py   #   index-quartile counts, scores>85, per-state rollups (TODO+)
│
└── validation/               # periodic construct-validity dossier (complex/infrequent)
    ├── internal/             # Axis A — sensitivity, coherence/redundancy, spatial, distribution
    ├── external/             # Axis B — ACS, BEAD/RDOF, peer indices (Purdue DDI)
    ├── temporal/             # Axis C — predictive/temporal (needs run-registry snapshots)
    └── expert/               # Axis D — blind expert-review harness

notebooks/                    # THIN DRIVERS ONLY: load frame → call modeling/validation → visualize
```

---

## 3. Data flow (mapped to MLOps pipeline stages)

```mermaid
flowchart TD
    subgraph INGEST["Data Ingestion — sources"]
        BQ[(BQ prod raw<br/>FCC/demo/location/rextag)]
        DL[data_download<br/>Census BAF/ACL]
    end
    GATE{{Data Validation — monitoring.data_contract<br/>schema · nulls · anomalies · row counts}}
    subgraph PREP["Data Preparation — processing + features + grain_transfer"]
        PROC[processing.build_block_frame<br/>canonical block frame]
        FT[features.*.transform<br/>batch-1 reshapes]
        FE[features.*.engineered<br/>batch-2 features]
        GTn[grain_transfer.promote<br/>up-agg / down-broadcast]
        PF[features.parcel_features<br/>13 feats @ parcel]
    end
    subgraph TRAIN["Model Training — modeling"]
        TR[modeling.train<br/>cluster→classify→SHAP]
        FR[modeling.fit_rules<br/>weights + scaling_params]
        REG[(run registry<br/>run_id-keyed)]
    end
    subgraph EVAL["Model Evaluation — validation"]
        VAL[construct validity<br/>4 axes]
    end
    subgraph SCOREG["Scoring — scoring"]
        SC[parcel_score<br/>scale→sub-idx→weighted→0-100]
        OUT[(fiber_idx_v1_parcel<br/>+ QA/outputs)]
    end
    MON[monitoring<br/>drift · parity · bands · business rollups]

    BQ & DL --> GATE --> PROC --> FT --> FE --> GTn --> PF
    PF --> TR --> FR --> REG
    REG --> SC
    PF --> SC --> OUT
    OUT --> MON
    OUT --> VAL
    classDef deep fill:#0f172a,color:#fff,stroke:#0f172a;
    class TR,FR,SC deep
```

**Stage map:** Ingestion→`sources` · Data Validation→`monitoring.data_contract` (input
gate) · Data Preparation→`processing`+`features`+`grain_transfer` · Model
Training→`modeling.train` · Model Evaluation→`modeling.fit_rules`+`validation` (no
held-out accuracy — ADR-0004) · Model Registration→**run registry**.

---

## 4. Logic-location table (where each responsibility lives)

| Responsibility | Module / file | Grain | Notes |
| --- | --- | --- | --- |
| Pull raw (prod) | `sources/bq_prod.py` | native | FCC/demo/location/rextag from BQ (ADR-0003) |
| Pull raw (dev/backfill) | `sources/data_download/*` | native | Census always; FCC/others for backfill |
| Input gate | `monitoring/data_contract.py` | — | halt+alert on schema/null/anomaly (Q12) |
| Canonical block frame | `processing/block_frame.py` | block | one wide frame, copper/fiber/cable cols |
| Dasymetric interpolation, block/tract cuts | `features/telecom/transform/*` | block/tract | **Transformed** features (batch 1) |
| Penetration, gap, landscape ord, top-tier | `features/telecom/engineered/*` | block | **Engineered** features (batch 2) |
| Demographic pop-change (5-yr avg) | `features/demographic/*` | tract | broadcast down to parcel |
| Growth counts, hotspot distance | `features/location/engineered/*` | parcel | aggregate up to block/tract for tract branch |
| Nearest-fiber distance | `features/rextag/engineered/*` | parcel | in **miles** (from BQ distance tables) |
| Grain promotion (up/down) | `grain_transfer/promote.py` + `specs.py` | any→any | SQL from spec; BigQuery + DuckDB adapters |
| Parcel feature assembly | `features/parcel_features.py` (+ .sql) | parcel | the scoring input (13 features) |
| Cluster → classify → SHAP | `modeling/train.py` | tract (training pop) | reruns on model refresh (≈annual) |
| Weights + scaling params | `modeling/fit_rules.py` | tract | reruns on data refresh (≈monthly) |
| Run registry (artifacts + metadata) | `modeling/registry.py` | — | run_id-keyed; backbone for temporal validation |
| Scale + weighted index + delivery | `scoring/parcel_score.py` | parcel | SQL pushdown over ~160M parcels |
| Scaling parity layer | `scoring/scaling.py` + `constants.py` | — | same rules in training & scoring |
| Per-run health metrics | `monitoring/metrics.py` | — | nulls, drift, parity, score bands |
| Business rollups | `monitoring/business_rollups.py` | — | quartiles, >85, per-state (TODO expand) |
| Construct-validity dossier | `validation/{internal,external,temporal,expert}` | — | 4 axes; periodic (ADR-0004) |

---

## 5. Carried-forward contracts (unchanged by the rearchitecture)

These remain the invariants; the rearchitecture relocates *where* they live, not *what*
they are.

- **The 13-feature contract & grain map** (`constants.py`): Growth @ parcel, FCC/telecom
  @ block, Demo @ tract. Canonical order Growth → Telecom → Demo.
- **Training/scoring parity:** the same rule set (na_fill / winsorize / invert / min-max)
  is applied in `modeling.fit_rules` (training) and `scoring.scaling` (scoring), frozen
  per `run_id` in `scaling_params`. This is the parity guarantee — do not fork it.
- **Scored universe ⊃ training population:** the model trains on filtered tracts; scoring
  covers every parcel. NA rules cover degenerate blocks.
- **Weights source of truth:** `constants.py SCORING_BUCKET_WEIGHTS`. (The stale
  `parcel_scoring_qa.md` numbers are to be refreshed after the rewire + rerun.)

---

## 6. Production metrics surface (goal #2)

### 6.1 `monitoring` — every run
1. **Feature null / fill rate** — per feature, incl. inf-as-null.
2. **Raw-vs-clipped drift** — impact of winsorize / domain-bound rules (raw kept in
   `parcel_features`, clipped in scores).
3. **Train/scoring parity** — same rule set both sides; alert on divergence.
4. **Distribution shift** — vs the frozen `run_id` baseline.
5. **Score-band stability** — the 0-25/25-50/50-75/75-100 bands per index.
6. **Join coverage** — CT-vintage / GEOID match rate (QA C2).
7. **Business rollups** — index-quartile parcel counts; parcels scoring >85 per index;
   per-state (block_geoid[:2]) totals + quartile scores. _TODO: expand this set — many
   more business cuts to add._
8. **Data-contract gate** — schema / null / anomaly / row-count pre-conditions (Q12).

### 6.2 `validation` — periodic (four axes, ADR-0004)

| Axis | Test | Metric | Your item |
| --- | --- | --- | --- |
| A internal | Sensitivity (weights/norm/aggr) | rank shift; Spearman ρ | #3 |
| A internal | Redundancy / coherence | sub-index corr; Cronbach α | #4 |
| A internal | Spatial coherence | Moran's I / LISA | #2 |
| A internal | Distribution by context | urban/rural/semi histograms | #1 |
| B external | ACS broadband adoption | rank corr / directional | #5 |
| B external | BEAD/RDOF funding | AUROC; top-decile lift | #6 |
| B external | Peer index (Purdue DDI) | Spearman ρ | — |
| C temporal | Future fiber / funding | temporal AUROC | — |
| D expert | Blind review | weighted κ vs experts | — |

> Full methodology, sources, and pass thresholds: [`validation_methodology.md`](./validation_methodology.md).

---

## 7. Open TODOs

- [ ] Expand `monitoring/business_rollups.py` beyond the seed set (many more business cuts).
- [ ] Refine the `monitoring` ↔ `validation` line and add `validation` sub-modules as the
      dossier grows (ADR-0001 anticipates this).
- [ ] Refresh `constants.py` weights + `parcel_scoring_qa.md` after the rewire and a full rerun.
- [ ] Confirm distance-table units end-to-end (miles) once wiring is complete.
- [ ] Start archiving per-`run_id` feature+score snapshots so the temporal axis (C) is possible.
