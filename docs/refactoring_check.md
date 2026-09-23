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

| Seq | `sql/` script | Reads | After | Produces (dataset.table) |
|-----|---------------|-------|-------|--------------------------|
| 1 | `features/telecom/01_fcc_fixed_speeds_block.sql` | PROD | — | `teu_telecom.fcc_fixed_speeds_block` |
| 2 | `features/telecom/02_fcc_coverage_summary.sql` | PROD | — | `teu_telecom.fcc_coverage_summary` |
| 3 | `features/telecom/03_fcc_coverage_county_residuals.sql` | DEV | 2 (+census ✅) | `teu_telecom.fcc_coverage_county_residuals` |
| 4 | `features/telecom/04_fcc_coverage_block.sql` | DEV | 2, 3 (+census ✅) | `teu_telecom.fcc_coverage_block` |
| 5 | `features/telecom/05_telecom_features_block.sql` | DEV | 1, 4 | `teu_features.telecom_features_block` |
| 6 | `features/location/01_growth_counts.sql` | PROD | — | `teu_features.loc_growth_cnts_parcel` |
| 7 | `features/location/02_growth_concentrations.sql` | DEV | 6 | `teu_features.loc_growth_parcel_concentrations_h3r7` |
| 8 | `features/location/03_hotspot_distance.sql` | DEV | 7 | `teu_features.loc_growth_distance_parcel` |
| 9 | `features/rextag/01_fiber_optimize.sql` | PROD | — | `teu_telecom.int_rextag_fiberopticcables_optimized` |
| 10 | `features/rextag/02_fiber_distance.sql` | DEV | 9, 6 | `teu_features.rextag_distance_parcel` |
| 11 | `features/demographic/01_population_change.sql` | PROD | — | `teu_features.demo_pop_ct` |
| 12 | `grain_transfer/01_location_growth_ct.sql` | PROD+DEV | 7, 8 | `teu_features.loc_parcels_growth_ct` |
| 13 | `grain_transfer/02_rextag_distance_ct.sql` | PROD+DEV | 10 | `teu_features.rextag_distance_ct` |
| 14 | `grain_transfer/03_fcc_features_ct.sql` | PROD+DEV | 4 | `teu_features.telecom_features_ct` |
| 15 | `grain_transfer/04_features_ct.sql` | DEV | 11, 12, 13, 14 | `teu_features.features_ct` (tract training frame) |
| 16 | `features/parcel_features.sql` | DEV | 5, 7, 8, 10, 11, 12, 13 | `teu_features.parcel_features` |
| 17 | `scoring/01_scaling_params_scan.sql` | DEV | 16 | `teu_analytics.scaling_params` |
| 18 | `scoring/02_parcel_score.sql` | DEV | 16 + fitted weights/scaling | `teu_outputs.parcel_scores` |

**Critical gate:** step 2 (`fcc_coverage_summary`) must land before steps 3 & 4.

---

## 1. Module-by-module status

| # | Module | Run in | Dry-run verdict | Output table | Status |
|---|--------|--------|-----------------|--------------|--------|
| 1 | `features.telecom.transform.fcc_fixed_speeds_block` | CONSOLE | 403 prod (expected) | `teu_telecom.fcc_fixed_speeds_block` | ✅ run (5.94M rows) |
| 2 | `features.telecom.transform.fcc_coverage_summary` | CONSOLE | 403 prod (expected) | `teu_telecom.fcc_coverage_summary` | ✅ run (35,304 rows) |
| 3 | `features.telecom.transform.fcc_coverage_county_residuals` | VM | RUN ✅ | `teu_telecom.fcc_coverage_county_residuals` | ✅ run (3,232 rows) |
| 4 | `features.telecom.transform.fcc_coverage_block` | VM | RUN ✅ | `teu_telecom.fcc_coverage_block` | ✅ run (8.17M rows) |
| 5 | `features.telecom.engineered.telecom_features_block` | VM | RUN ✅ | `teu_features.telecom_features_block` | ✅ run (8.17M rows) |
| 6 | `features.location.engineered.growth_counts` | CONSOLE | RUN ✅ | `teu_features.loc_growth_cnts_parcel` | ✅ run (154.6M rows) |
| 7 | `features.location.engineered.growth_concentrations` | VM | RUN ✅ | `teu_features.loc_growth_parcel_concentrations_h3r7` | ✅ run (7,340 rows) |
| 8 | `features.location.engineered.hotspot_distance` | VM | RUN ✅ | `teu_features.loc_growth_distance_parcel` | ✅ run (154.6M rows, 3 cols) |
| 9 | `features.rextag.transform.fiber_optimize` | CONSOLE | RUN ✅ | `teu_telecom.int_rextag_fiberopticcables_optimized` | ✅ run (2.71M rows) |
| 10 | `features.rextag.engineered.fiber_distance` | CONSOLE (was VM; worker now reads a PROD state-boundary view) | RUN ✅ (redesigned) | `teu_features.rextag_distance_parcel` (miles) | 🔴 re-run pending — F7 true root cause found: on-demand CPU/bytes ratio limit, not just partial coverage. Worker now pre-filters fiber per state, shard tiers retuned, driver raises on any shard failure, `nearest_fiber_id` now the stable `loc_id`. Awaiting clean console run |
| 11 | `features.demographic.engineered.population_change` | CONSOLE | 403 prod (expected) | `teu_features.demo_pop_ct` | ⏭️ skipped — no read perm on neighborhood_scout; existing `demo_pop_ct` reused |
| 12 | `grain_transfer.location_growth_ct` | CONSOLE | 403 prod (expected) | `teu_features.loc_parcels_growth_ct` | ✅ run (85,064 tracts; miles fix live) |
| 13 | `grain_transfer.rextag_distance_ct` | CONSOLE | 403 prod (expected) | `teu_features.rextag_distance_ct` | 🔴 rebuild after F7 step-10 rerun (currently reflects 16-state data) |
| 14 | `grain_transfer.fcc_features_ct` | CONSOLE | 403 prod (expected) | `teu_features.telecom_features_ct` | ✅ run (85,395 tracts; FCC features re-derived at tract, ADR-0007) |
| 15 | `grain_transfer.features_ct` | VM | ✅ renders | `teu_features.features_ct` | 🔴 rebuild after F7 (fiber-distance column currently ~91% spurious-null) |
| 16 | `features.parcel_features` | VM | ✅ bq-valid (14.93 GB) | `teu_features.parcel_features` | 🟡 re-run pending (miles fix applied) |
| 17 | `scoring.build_scaling_params` | VM | ✅ renders | `teu_analytics.scaling_params` | ✅ validated |
| 18 | `scoring.build_weights` | VM | ✅ renders | `teu_analytics.feature_weights` | ✅ validated |
| 19 | `scoring.parcel_score` | VM | ✅ renders (weights baked) | `teu_outputs.parcel_scores` | ✅ validated |

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
- **F3 — Rextag/hotspot unit drift (RESOLVED).** The parcel distance tables emit
  miles (`rextag_distance_parcel.dist_to_nearest_fiber_miles`,
  `loc_growth_distance_parcel.dist_to_nearest_hotspot_miles`), but three consumers
  still read the old metres columns (`*_m`): `grain_transfer.rextag_distance_ct`,
  `grain_transfer.location_growth_ct`, and `features.parcel_features`. All three
  were updated to read the `_miles` columns and emit `_miles`-suffixed aggregates
  (`mean/median_dist_nearest_fiber_miles`, `mean_dist_nearest_hotspot_miles`);
  `median_dist_nearest_hotspot` keeps its name (already mapped). Unit change is
  safe — features are inverted + min-max scaled and scaling params are recomputed
  per grain. CT-aggregation tests updated to the `_miles` names; SQL regenerated.
- **F4 — Table clutter in `teu_features`.** Backup/experiment variants
  (`*_back`, `*_temp_back`, `*_half_mi`, `*_sample`) are not in the canonical
  inventory — cleanup candidates once the run is confirmed.
- **F5 — `fiber_optimize` 400 on dry-run.** "Error validating procedure body" —
  the body references a prod rextag view + boundary UDF; validate in the console.
  Not necessarily a code bug.

- **F6 — Stale legacy output tables block `CREATE OR REPLACE` (found 2026-09-01).**
  `teu_telecom.fcc_coverage_county_residuals` already exists from **legacy** code
  (Apr 2026, 3,221 rows, old schema: `copper_speed_100_20`… with no `*_only`
  columns). Two symptoms surfaced once `fcc_coverage_summary` landed:
  step 3 fails with *"Cannot replace a table with a different partitioning"* and
  step 4 fails with *"Unrecognized name: copper_speed_02_02_only"* (it reads the
  stale legacy residuals table).
  → Fix: **drop the stale table, then run step 3**:
  `DROP TABLE \`clgx-gis-app-dev-06e3.teu_telecom.fcc_coverage_county_residuals\`;`
  Watch for the same on other tables that predate this rebuild. **Confirmed
  recurrence:** legacy `fcc_coverage_block` (8.1M rows, unclustered) blocks
  step 4 the same way — drop it before running step 4. General rule: drop a stale
  output table if a `CREATE OR REPLACE` errors on partitioning/spec.
  **Type-clash variant (step 10):** the July `rextag_calculation_parcel` has
  `nearest_fiber_id INT64`; the new worker uses `CREATE TABLE IF NOT EXISTS` (so
  the stale table survives) and inserts STRING, so procedure-body validation
  fails ("STRING cannot be inserted into … INT64"). Fix:
  `DROP TABLE IF EXISTS \`clgx-gis-app-dev-06e3.teu_features.rextag_calculation_parcel\`;`
  then re-run step 10. The new code is STRING-consistent end-to-end (scratch,
  final `rextag_distance_parcel`, and `scoring.parcel_score` all use STRING).
  **Ordering variant (step 10, follow-on):** after dropping the scratch table, the
  worker `CREATE PROCEDURE` then failed with *"Table … rextag_calculation_parcel
  was not found"* — the worker's `INSERT` body was validated (strict mode) before
  the driver's runtime `CREATE TABLE IF NOT EXISTS` had ever run, so on a fresh
  environment the staging table did not yet exist at deploy time. **Fix (code, not
  a manual step):** `fiber_distance` now emits a standalone
  `CREATE TABLE IF NOT EXISTS {calc_table}` (STRING schema, `CLUSTER BY
  state_fips, parcel_shape_id`) as the **first** statement of the script — before
  the worker procedure — so validation passes with strict mode intact. New
  template `fiber_distance_calc_table.sql`; `sql/features/rextag/02_fiber_distance.sql`
  regenerated. The generated file is now fully re-runnable top-to-bottom (drop of
  the stale INT64 table is still required once, to clear the July schema).

- **F7 — Step 10 (`rextag_distance_parcel`) ran on only 16 of 51 states
  (root-caused 2026-09-08).** Initially surfaced as "fiber distance NULL for ~90%
  of parcels." **Not sparsity and not the CT rollup or the miles change** — the
  step-10 run was incomplete. Evidence:
  - Per-state null rate is lumpy, not uniform: 16 states have real distances, 35
    (incl. CA, TX, FL, NY, PA, IL, OH, MI, NC, GA — the bulk of US parcels) are
    100% null.
  - The 35 null states have `processed_at IS NULL`. The worker stamps
    `CURRENT_TIMESTAMP()` on every row it writes, so those parcels never went
    through the worker; the assemble step's `LEFT JOIN` of the full 154M-parcel
    master onto staging fills them with null distance + null timestamp, so a
    **partial run looks like a complete table** (the assemble comment even says so).
  - Proof it's data-present: 200 Michigan parcels sit a **median 178 m** (min 21 m)
    from Michigan fiber that IS in `int_rextag_fiberopticcables_optimized`.
  - **Step 9 verified good:** 2.71M lines, 0 null/collection geoms; every mainland
    state + DC has tens of thousands of fiber lines in its parcel footprint. Only
    offshore areas (HI, PR, Guam, N. Mariana, VI) carry no rextag fiber — HI is
    worker-processed (null distance, non-null ts); territories are excluded from
    `DEFAULT_STATES`.
  → **Fix:** re-run step 10's driver over the 35 missing states, then re-run
    assemble, then rebuild steps 13 (`rextag_distance_ct`) and 15 (`features_ct`).
    The driver is idempotent (per-state `DELETE` first).
  → **Guardrail added (this session):** `fiber_distance` now emits a final `ASSERT`
    (in `build()` and in the generated `sql/features/rextag/02_fiber_distance.sql`)
    that fails the run unless every expected state has ≥1 worker-processed row
    (`processed_at IS NOT NULL`), so a partial run can never silently pass again.
    New `render_completeness_assert_sql()`; test added; SQL regenerated.
  → **True root cause found on re-run (2026-09-23):** the ASSERT did its job — the
    console re-run failed loudly instead of silently. The actual failure was
    BigQuery's **on-demand CPU-to-bytes ratio limit**: the worker's `ST_DWITHIN`
    searches a 15-mile band against the *entire* nationwide fiber table
    (`int_rextag_fiberopticcables_optimized`, ~664 MB) on every shard, so CPU cost
    scales with total fiber regardless of how few parcels a shard covers. Even
    moderate states (AL, AZ, AR) failed at 1 shard; the driver's per-shard
    `EXCEPTION` handler silently swallowed each failure — the second, deeper layer
    of the same masking bug. Sharding alone (more shards per state) does not fix
    this: each shard still scans the full fiber table, so bytes-billed stays flat
    while only CPU drops — it would need 10s of shards per dense state, pure
    per-state guesswork.
  → **Redesign shipped (2026-09-23), ported from a colleague's rewrite:**
    - **Worker pre-filters fiber per state** (`fiber_distance_worker.sql`): a new
      `state_boundary` CTE reads a PROD state-boundary view (`state_boundary` in
      the source registry), buffers it by the search distance, and a `state_fiber`
      CTE keeps only fiber lines intersecting that buffer — so a shard's join only
      ever touches nearby fiber, not the whole country's. This is the real fix for
      the ratio-limit failure.
    - **`FIBER_SUBDIVIDE_MAX_VERTICES` 256 → 16** and **`FIBER_STATE_SHARD_COUNTS`**
      replaced with a data-driven, production-derived tier map (2–32 shards per
      state sized from each state's actual parcel/fiber cross-product;
      `constants/rextag.py`).
    - **Stable `nearest_fiber_id`:** `fiber_optimize` now carries the source
      `loc_id` through dedup (`MIN(loc_id)` per unique path) alongside a new
      within-run `spatial_fiber_id` (renamed from `original_fiber_id`) used only for
      the cheap spatial join; the staging table stores the INT64
      `spatial_fiber_id`, and `fiber_distance_assemble.sql` maps it back to the
      stable, string `loc_id` for the final table via a `fiber_lookup` CTE — so
      `nearest_fiber_id` is now reproducible across optimise re-runs (previously a
      within-run row id).
    - **Driver hardens against silent partial runs a second way:** it still
      attempts every state/shard (a failure no longer aborts the loop), but now
      accumulates failures in a `failures` array and `RAISE`s once at the end
      naming every failing state/shard — so a run with any failed shard can no
      longer complete looking clean, independent of the completeness `ASSERT`.
    - **Run location changed:** because the worker now reads a PROD view (state
      boundary) in addition to PROD fiber, step 10 is CONSOLE-only (was VM
      dev-only); `sql/README.md` and the module-status table below updated.
    - Ported into `constants/rextag.py`, the three `fiber_distance_*.sql`
      templates, `fiber_optimize.sql`, `fiber_distance.py` (new
      `state_boundary_table_ref()`, updated `render_worker_sql` /
      `render_assemble_sql` signatures), `config/bigquery.py` (new
      `BQ_PROD_DATASET_ADMIN_BOUNDARIES` / `BQ_PROD_VIEW_STATE_BOUNDARY`),
      `sources/registry.py` (new `state_boundary` source); tests updated (264
      total pass). Both `sql/features/rextag/01_fiber_optimize.sql` and
      `02_fiber_distance.sql` regenerated. **Awaiting a clean console re-run of
      step 10** before rebuilding 13 → 15.

## 3. Deliverables in flight

- **DE raw-SQL folder (`sql/`) — 🟡 in progress.** One sub-folder per module with
  concrete, runnable SQL (only project ids parameterized) + `sql/README.md`
  runbook. 18 SQL files generated from module `--dry-run`. Doubles as the console
  SQL pack for the prod-reading steps.
- **CT training-frame path — ✅ built + run (this session).** The model is fit at tract
  grain but scores parcels, so a tract training frame is now assembled alongside
  the parcel scoring input:
  - `grain_transfer.fcc_features_ct` (step 14, PROD+DEV) re-derives the four
    engineered telecom features at tract via the shared grain-agnostic fragment
    (`telecom/engineered/telecom_features.sql` + `_engineered_sql.py`), rolling
    provider counts as `COUNT(DISTINCT provider_id)` from the raw FCC tables and
    the top-tier fiber speed as an `estimated_fcc_units`-weighted mean from
    `fcc_coverage_block` (ADR-0007). `telecom_features_block` was refactored onto
    the same fragment so block and tract features cannot drift.
  - `grain_transfer.features_ct` (step 15, DEV) joins the four tract families on
    the tract GEOID and emits the 13 model-named columns (`median_*` growth +
    telecom identity + `median_dist_nearest_fiber_miles` +
    `estimated_census_housing_units` + `pop_*`) that `train.py` renames to the
    scoring contract — verified by a test that the renamed set covers all 13.
  - Offline tests added (`tests/grain_transfer/test_fcc_features_ct.py`,
    `test_features_ct.py`); config constants `BQ_TABLE_TELECOM_FEATURES_CT`,
    `BQ_TABLE_FEATURES_CT` added.
- **DE runbook prerequisites** — census-block landing documented as a Stage-0
  prerequisite in `sql/README.md`.

## 4. Next steps

1. ~~Land census blocks in dev~~ ✅ done (F1).
2. User runs the CONSOLE steps from `sql/` in dependency order and reports
   each; assistant records results in §5. Order matters: **step 2
   (`fcc_coverage_summary`) unblocks steps 3 & 4.**
3. ~~Reconcile rextag/hotspot metres/miles drift~~ ✅ done (F3).
4. Run the new CT path in console: step 14 (`fcc_features_ct`, PROD+DEV) then
   step 15 (`features_ct`, DEV). Re-run step 16 (`parcel_features`) after the
   miles fix.
5. Confirm `scoring_runs` / `monitoring_baseline` create cleanly on first run (F2).
6. Clean up `teu_features` backup/experiment tables (F4) after a green run.
7. Deep-dive `docs/validation_methodology.md` and define next steps.

## 5. Console run log

As each `sql/` script is run in the BigQuery console, record the result here:
what ran, the table produced, row count, and whether the schema matched expectations.

| Step | SQL file | Run at | Table produced | Rows | Schema OK | Notes |
|------|----------|--------|----------------|------|-----------|-------|
| 1 | `features/telecom/01_fcc_fixed_speeds_block.sql` | 2026-09-01 12:22Z | `teu_telecom.fcc_fixed_speeds_block` | 5,937,564 | ✅ | 15 cols: per-tech (copper/cable/fiber) location+provider counts and max down/up speeds. |
| 2 | `features/telecom/02_fcc_coverage_summary.sql` | 2026-09-01 12:27Z | `teu_telecom.fcc_coverage_summary` | 35,304 | ✅ | 23 cols: geography_level/id/desc + total_units + per-tech speed-tier buckets (`*_only`). |
| 3 | `features/telecom/03_fcc_coverage_county_residuals.sql` | 2026-09-01 12:34Z | `teu_telecom.fcc_coverage_county_residuals` | 3,232 | ✅ | Dropped stale legacy table first (F6). New schema: county keys + residual_units + place_count + per-tech `*_only` buckets. |
| 4 | `features/telecom/04_fcc_coverage_block.sql` | 2026-09-01 12:37Z | `teu_telecom.fcc_coverage_block` | 8,174,955 | ✅ | Dropped legacy 8.1M table first (F6). Dasymetric block interpolation; clustered; one row per census block. |
| 5 | `features/telecom/05_telecom_features_block.sql` | 2026-09-01 12:41Z | `teu_features.telecom_features_block` | 8,174,955 | ✅ | Engineered block features: cable_penetration, fiber_opportunity_gap, fiber_speed_top_tier, provider_competitive_landscape(+_ord). |
| 6 | `features/location/01_growth_counts.sql` | 2026-09-01 12:55Z | `teu_features.loc_growth_cnts_parcel` | 154,563,179 | ✅ | Parcel growth indicators + `*_qtr_mi_cnt` neighborhood counts + geometry. No F6 (spec matched). |
| 7 | `features/location/02_growth_concentrations.sql` | 2026-09-01 13:00Z | `teu_features.loc_growth_parcel_concentrations_h3r7` | 7,340 | ✅ | H3-r7 hotspot cells: growth/permit/landuse/builder counts + total_flags + geom. |
| 8 | `features/location/03_hotspot_distance.sql` | 2026-09-01 13:18Z | `teu_features.loc_growth_distance_parcel` | 154,563,179 | ✅ | 3 cols: parcel_shape_id, dist_to_nearest_hotspot_miles, is_inside_hotspot. Rows match parcel master. Reads step 7. |
| 9 | `features/rextag/01_fiber_optimize.sql` | 2026-09-01 13:03Z | `teu_telecom.int_rextag_fiberopticcables_optimized` | 2,712,222 | ✅ | Stored proc CREATE+CALL. Cleaned/optimised fiber geometry: original_fiber_id, num_points, geometry. Clustered. No F6. |
| 10 | `features/rextag/02_fiber_distance.sql` | 2026-09-08 11:55Z | `teu_features.rextag_distance_parcel` | 154,563,179 | ✅ | 6 cols. `nearest_fiber_id` STRING (type fix confirmed), `dist_to_nearest_fiber_miles` FLOAT, `radius_fiber_count`. Rows = parcel master. Required one-time DROP of stale INT64 scratch (F6); staging-table ordering fix worked. |
| 12 | `grain_transfer/01_location_growth_ct.sql` | 2026-09-08 11:59Z | `teu_features.loc_parcels_growth_ct` | 85,064 | ✅ | PROD+DEV. 18 cols, ~85K tracts. F3 miles fix live: `mean_dist_nearest_hotspot_miles` (FLOAT) + `median_dist_nearest_hotspot`; no stale `_m`. Reads steps 6+8 + PROD tract boundary. (Step 11 skipped — no read perm on neighborhood_scout; existing `demo_pop_ct` reused.) |
| 13 | `grain_transfer/02_rextag_distance_ct.sql` | 2026-09-08 12:02Z | `teu_features.rextag_distance_ct` | 85,064 | ✅ | PROD+DEV. 6 cols. F3 miles fix live: `mean/median_dist_nearest_fiber_miles` (FLOAT); no `_m`. 91% of tracts NULL median fiber dist — inherited from step-10 threshold gating (see F7), not a rollup bug. |
| 14 | `grain_transfer/03_fcc_features_ct.sql` | 2026-09-08 12:02Z | `teu_features.telecom_features_ct` | 85,395 | ✅ | PROD+DEV. 12 cols. FCC features re-derived at tract via shared fragment (ADR-0007). Spine = 85,395 populated tracts; cable/housing 0 nulls. |
| 15 | `grain_transfer/04_features_ct.sql` | 2026-09-08 12:02Z | `teu_features.features_ct` | 85,395 | ✅ | DEV. Tract **training frame**. 15 cols = 2 keys + exactly the 13 model-named features `train.py` renames. Telecom spine; left-joins leave 362 growth-null, 1,301 pop-null, 78,012 fiber-null (all expected; fiber p99-filled downstream). |
