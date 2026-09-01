# Refactoring Check — Module Testing & Table Inventory

Living tracker for the step-by-step module walk of the rearchitected pipeline.
Goal: run every module, validate its SQL against live BigQuery schemas, inventory
every table it touches, and record what still needs doing before the end-to-end
production run.

**Status legend:** ✅ done · 🟡 in progress / blocked · ⬜ not started
**Run-location legend:** `CONSOLE` = reads a PROD raw source (user runs in BQ
console) · `VM` = dev-only, runnable from the build VM.

**BQ permission split:** the build VM's service account can read/write **only**
`clgx-gis-app-dev-06e3` (dev). It **cannot** read PROD (`clgx-idap-bigquery-prd-a990`);
those steps are validated/run in the console.

**Validation technique:** BigQuery **server-side dry-run**
(`QueryJobConfig(dry_run=True)`) checks each module's assembled SQL against live
table schemas — free, non-destructive, catches missing tables/columns/type errors.

---

## Run sequence (console-driven setup)

Run each `sql/` script in the BigQuery console in this order, then report back so
§5 gets updated. "After" = the step(s) that must land first. Prerequisite
(census blocks) is already ✅ done. `Reads PROD` = needs prod read access.

| Seq | `sql/` script | Reads | After |
|-----|---------------|-------|-------|
| 1 | `features/telecom/01_fcc_fixed_speeds_block.sql` | PROD | — |
| 2 | `features/telecom/02_fcc_coverage_summary.sql` | PROD | — |
| 3 | `features/telecom/03_fcc_coverage_county_residuals.sql` | DEV | 2 (+census ✅) |
| 4 | `features/telecom/04_fcc_coverage_block.sql` | DEV | 2 (+census ✅) |
| 5 | `features/telecom/05_telecom_features_block.sql` | DEV | 1, 4 |
| 6 | `features/location/01_growth_counts.sql` | PROD | — |
| 7 | `features/location/02_growth_concentrations.sql` | DEV | 6 |
| 8 | `features/location/03_hotspot_distance.sql` | DEV | 6 |
| 9 | `features/rextag/01_fiber_optimize.sql` | PROD | — |
| 10 | `features/rextag/02_fiber_distance.sql` | PROD+DEV | 9, 6 |
| 11 | `features/demographic/01_population_change.sql` | PROD | — |
| 12 | `grain_transfer/01_location_growth_ct.sql` | PROD+DEV | 7, 8 |
| 13 | `grain_transfer/02_rextag_distance_ct.sql` | PROD+DEV | 10 |
| 14 | `features/parcel_features.sql` | DEV | 5, 7, 8, 10, 11, 12, 13 |
| 15 | `scoring/01_scaling_params_scan.sql` | DEV | 14 |
| 16 | `scoring/02_parcel_score.sql` | DEV | 14 + fitted weights/scaling |

**Critical gate:** step 2 (`fcc_coverage_summary`) must land before steps 3 & 4.

---

## 1. Module-by-module status

| # | Module | Run in | Dry-run verdict | Output table | Status |
|---|--------|--------|-----------------|--------------|--------|
| 1 | `features.telecom.transform.fcc_fixed_speeds_block` | CONSOLE | 403 prod (expected) | `teu_telecom.fcc_fixed_speeds_block` | 🟡 console-run pending |
| 2 | `features.telecom.transform.fcc_coverage_summary` | CONSOLE | 403 prod (expected) | `teu_telecom.fcc_coverage_summary` | 🟡 console-run pending |
| 3 | `features.telecom.transform.fcc_coverage_county_residuals` | VM | dep on step 2 | `teu_telecom.fcc_coverage_county_residuals` | 🟡 waits on `fcc_coverage_summary` |
| 4 | `features.telecom.transform.fcc_coverage_block` | VM | dep on step 2 | `teu_telecom.fcc_coverage_block` | 🟡 waits on `fcc_coverage_summary` |
| 5 | `features.telecom.engineered.telecom_features_block` | VM | ✅ bq-valid (0.85 GB) | `teu_features.telecom_features_block` | ✅ validated |
| 6 | `features.location.engineered.growth_counts` | CONSOLE | 403 prod (expected) | `teu_features.loc_growth_cnts_parcel` | 🟡 console-run pending |
| 7 | `features.location.engineered.growth_concentrations` | VM | ✅ bq-valid (6.36 GB) | `teu_features.loc_growth_parcel_concentrations_h3r7` | ✅ validated |
| 8 | `features.location.engineered.hotspot_distance` | VM | ✅ bq-valid (7.42 GB) | `teu_features.loc_growth_distance_parcel` | ✅ validated |
| 9 | `features.rextag.transform.fiber_optimize` | CONSOLE | 400 proc-body (expected) | `teu_telecom.int_rextag_fiberopticcables_optimized` | 🟡 console-run pending |
| 10 | `features.rextag.engineered.fiber_distance` | CONSOLE | renders (3 procs) | `teu_features.rextag_distance_parcel` (miles) | 🟡 console-run pending |
| 11 | `features.demographic.engineered.population_change` | CONSOLE | 403 prod (expected) | `teu_features.demo_pop_ct` | 🟡 console-run pending |
| 12 | `grain_transfer.location_growth_ct` | CONSOLE | 403 prod (expected) | `teu_features.loc_parcels_growth_ct` | 🟡 console-run pending |
| 13 | `grain_transfer.rextag_distance_ct` | CONSOLE | 403 prod (expected) | `teu_features.rextag_distance_ct` | 🟡 unit drift (see F3) |
| 14 | `features.parcel_features` | VM | ✅ bq-valid (14.93 GB) | `teu_features.parcel_features` | ✅ validated |
| 15 | `scoring.build_scaling_params` | VM | ✅ renders | `teu_analytics.scaling_params` | ✅ validated |
| 16 | `scoring.build_weights` | VM | ✅ renders | `teu_analytics.feature_weights` | ✅ validated |
| 17 | `scoring.parcel_score` | VM | ✅ renders (weights baked) | `teu_outputs.parcel_scores` | ✅ validated |

Monitoring and validation modules are pure Python (read + return); not SQL steps.

## 2. Open findings

- **F1 — Census blocks landed ✅ (RESOLVED 2026-09-01).**
  `teu_demographics.census_baf_block` (8,174,955 rows) and `census_acl_block`
  (8,284,328 rows) uploaded to dev from local parquet via
  `processing.census_blocks_to_bq` (gcloud-ADC client injected to bypass the
  local Windows ADC path). Steps 3 & 4 no longer 404 on census — they now simply
  wait on `fcc_coverage_summary` (step 2, a console step), which is normal
  dependency order.
- **F2 — New tables not yet created.** `teu_analytics.scoring_runs` (run
  registry) and `teu_analytics.monitoring_baseline` don't exist yet — created on
  the first `modeling.registry` / `monitoring.drift` run. Expected, not an error.
- **F3 — Rextag unit drift (confirmed live).** `grain_transfer.rextag_distance_ct`
  reads `dist_to_nearest_fiber_m` (metres); `rextag_distance_parcel` emits miles.
  Scoring reads `dist_to_nearest_fiber_miles` (OK). Reconcile the metres/miles
  seam on the modeling / `all_features_tract` side before any modeling rerun.
- **F4 — Table clutter in `teu_features`.** Backup/experiment variants
  (`*_back`, `*_temp_back`, `*_half_mi`, `*_sample`) are not in the canonical
  inventory — cleanup candidates once the run is confirmed.
- **F5 — `fiber_optimize` 400 on dry-run.** "Error validating procedure body" —
  the body references a prod rextag view + boundary UDF; validate in the console.
  Not necessarily a code bug.

## 3. Deliverables in flight

- **DE raw-SQL folder (`sql/`) — 🟡 in progress.** One sub-folder per module with
  concrete, runnable SQL (only project ids parameterized) + `sql/README.md`
  runbook. 16 SQL files generated from module `--dry-run`. Doubles as the console
  SQL pack for the 7 prod-reading steps.
- **DE runbook prerequisites** — census-block landing documented as a Stage-0
  prerequisite in `sql/README.md`.

## 4. Next steps

1. ~~Land census blocks in dev~~ ✅ done (F1).
2. User runs the 7 CONSOLE steps from `sql/` in dependency order and reports
   each; assistant records results in §5. Order matters: **step 2
   (`fcc_coverage_summary`) unblocks steps 3 & 4.**
3. Reconcile rextag metres/miles drift (F3).
4. Confirm `scoring_runs` / `monitoring_baseline` create cleanly on first run (F2).
5. Clean up `teu_features` backup/experiment tables (F4) after a green run.
6. Deep-dive `docs/validation_methodology.md` and define next steps.

## 5. Console run log

As each `sql/` script is run in the BigQuery console, record the result here:
what ran, the table produced, row count, and whether the schema matched expectations.

| Step | SQL file | Run at | Table produced | Rows | Schema OK | Notes |
|------|----------|--------|----------------|------|-----------|-------|
| _(none yet — census-block landing done via Python uploader, not a `sql/` file)_ | | | | | | |
