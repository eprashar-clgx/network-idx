# `sql/` — Data-Engineering SQL Runbook

Raw, runnable SQL for every BigQuery compute step in the Fiber Potential Index
pipeline, one folder per source module. These files are **generated** from the
Python modules under `src/network_idx/` (via `python -m <module> --dry-run`) and
mirror exactly what the pipeline submits to BigQuery.

Hand this folder to the data engineer to run the heavy BQ compute in the console.
The Python pipeline still orchestrates the ML-specific steps (model fit, weight
extraction, scaling-param assembly) — those are **not** in this folder.

---

## 1. Project substitution (the only thing that changes per environment)

Every dataset and table name in these files is **concrete**. Only two project
identifiers need swapping for another environment:

| Placeholder    | This environment              | Role                          |
|----------------|-------------------------------|-------------------------------|
| `PROD_PROJECT` | `clgx-idap-bigquery-prd-a990` | Raw source reads (read-only)  |
| `DEV_PROJECT`  | `clgx-gis-app-dev-06e3`       | Feature / output writes       |

To retarget: find-and-replace those two project strings across the folder.

## 2. PROD vs. DEV reads

Steps marked **`Reads: PROD`** in the run-order table read a raw source table or
view from `PROD_PROJECT` and therefore require read access to that project. All
other steps read only `DEV_PROJECT` tables produced by earlier steps. Every step
**writes** to `DEV_PROJECT`. Each `.sql` file header also notes where it runs.

## 3. Prerequisites (land before Stage 1)

- **Census blocks (BLOCKER):** `DEV_PROJECT.teu_demographics.census_baf_block`
  and `.census_acl_block` must exist. These are produced from Census downloads by
  `network_idx.processing.census_blocks_to_bq` (Python/pandas load — not SQL, so
  not in this folder). Steps `telecom/03` and `telecom/04` depend on them.
- Raw FCC (copper/cable/fiber) tables and the rextag fiber-optic view already
  exist in `PROD_PROJECT`.

## 4. Run order

Run the scripts in **this exact sequence**. "Depends on" lists the step number(s)
that must finish first; steps sharing the same dependency may run in any order.
"Reads" shows whether a step reads a raw source in `PROD_PROJECT` (needs prod
read access) or only `DEV_PROJECT` tables produced by earlier steps.

```
01 fixed_speeds ───────────────────────────────┐
02 coverage_summary ──> 03 residuals ──> 04 coverage_block ──> 05 telecom_features ──┐
06 growth_counts ──> 07 concentrations                                              │
                 └─> 08 hotspot_distance ───────────────────────────────────────────┤
09 fiber_optimize ──> 10 fiber_distance ────────────────────────────────────────────┤
11 population_change ─────────────────────────────────────────────────────────────────┤
07,08 ──> 12 location_growth_ct                                                      │
10 ─────> 13 rextag_distance_ct ─────────────────────────────────────────────────────┤
                       └──> 14 parcel_features ──> 15 scaling_params
                                              └──> 16 parcel_score
```
(05 also needs 01; 04 also needs 02 + census blocks.)

| Seq | File | Reads | Depends on | Produces (dataset.table) |
|-----|------|-------|-----------|--------------------------|
| 1 | `features/telecom/01_fcc_fixed_speeds_block.sql` | PROD | — | `teu_telecom.fcc_fixed_speeds_block` |
| 2 | `features/telecom/02_fcc_coverage_summary.sql` | PROD | — | `teu_telecom.fcc_coverage_summary` |
| 3 | `features/telecom/03_fcc_coverage_county_residuals.sql` | DEV | 2 + census blocks | `teu_telecom.fcc_coverage_county_residuals` |
| 4 | `features/telecom/04_fcc_coverage_block.sql` | DEV | 2, 3 + census blocks | `teu_telecom.fcc_coverage_block` |
| 5 | `features/telecom/05_telecom_features_block.sql` | DEV | 1, 4 | `teu_features.telecom_features_block` |
| 6 | `features/location/01_growth_counts.sql` | PROD | — | `teu_features.loc_growth_cnts_parcel` |
| 7 | `features/location/02_growth_concentrations.sql` | DEV | 6 | `teu_features.loc_growth_parcel_concentrations_h3r7` |
| 8 | `features/location/03_hotspot_distance.sql` | DEV | 6 | `teu_features.loc_growth_distance_parcel` |
| 9 | `features/rextag/01_fiber_optimize.sql` | PROD | — | `teu_telecom.int_rextag_fiberopticcables_optimized` |
| 10 | `features/rextag/02_fiber_distance.sql` | PROD + DEV | 9, 6 | `teu_features.rextag_distance_parcel` |
| 11 | `features/demographic/01_population_change.sql` | PROD | — | `teu_features.demo_pop_ct` |
| 12 | `grain_transfer/01_location_growth_ct.sql` | PROD + DEV | 7, 8 | `teu_features.loc_parcels_growth_ct` |
| 13 | `grain_transfer/02_rextag_distance_ct.sql` | PROD + DEV | 10 | `teu_features.rextag_distance_ct` |
| 14 | `features/parcel_features.sql` | DEV | 5, 7, 8, 10, 11, 12, 13 | `teu_features.parcel_features` |
| 15 | `scoring/01_scaling_params_scan.sql` | DEV | 14 | `teu_analytics.scaling_params` |
| 16 | `scoring/02_parcel_score.sql` | DEV | 14 + fitted weights/scaling | `teu_outputs.parcel_scores` |

Steps 3 & 4 read the Census block tables from Stage-0 in addition to step 2's
`fcc_coverage_summary`.

## 5. Scoring files are pipeline-generated

`scoring/01_*` and `scoring/02_*` bake in numeric weights and min/max scaling
constants from a **specific fitted model run**. They change every time the model
is refit. Regenerate them from the module rather than hand-editing values. They
are included so the DE can review the final scoring pushdown.

## 6. Regenerating this folder

```bash
# from repo root, with the pipeline venv active and GCS_PROJECT_ID set
python -m network_idx.features.telecom.transform.fcc_fixed_speeds_block --dry-run
# ... each module prints its concrete SQL to stdout (logs go to stderr).
```
The generator scripts used to build this folder capture that stdout and prepend
the metadata banner.
