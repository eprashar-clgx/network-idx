# Retired code

Modules here are **superseded but not yet deleted** — kept for reference until their
replacement is validated end-to-end, then removed. They are outside the `network_idx`
package on purpose: nothing imports them, and they are not collected by the test suite.
Each entry records what replaced it and what still needs doing before deletion.

## `fcc_fixed_speeds.py`

The old FCC fixed-speed transform: it downloaded per-state BDC zip files, unzipped the
CSVs, and aggregated them to block / provider-block / provider-H3 grain in pandas.

- **Replaced by** `network_idx.features.telecom.transform.fcc_fixed_speeds_block`, which
  reads the three per-technology tables straight from BigQuery-production and aggregates
  to the block grain in one CTAS.
- **Still to deal with before deletion:** the old module also produced two outputs the
  new block transform does not — `fcc_fixed_speeds_providers_block` and
  `fcc_fixed_speeds_providers_h3`. Confirm nothing downstream consumes those (or port
  them) before deleting this file.

## `fcc_fixed_summary.py`

The old FCC fixed-summary coverage transform: it read the place and county summary tables
and reshaped the cumulative speed thresholds into mutually exclusive tiers per technology
in pandas.

- **Replaced by** `network_idx.features.telecom.transform.fcc_coverage_summary`, which does
  the same tiering and pivot in one CTAS reading the two BigQuery-production summary tables,
  and by the county-residual and block-interpolation transforms that build on it.
- **Still to deal with before deletion:** nothing — the whole coverage summary → residuals
  → block SQL path is in place, cross-checked by the pandas parity oracle. Delete once the
  parity oracle has signed off on production data.

## `telecom_features_block.sql` and `telecom_features_block_bq.py`

The old reference SQL and its thin BigQuery driver that derived the four block-level telecom
features (cable penetration, fiber opportunity gap, top-tier fiber speed, provider
competitive landscape) from the coverage and speeds block tables.

- **Replaced by** `network_idx.features.telecom.engineered.telecom_features_block`, which
  carries the same logic with the label-to-ordinal ladder generated from the scoring
  contract, offline tests, and an injected client.
- **Still to deal with before deletion:** nothing outside this pair — confirm the new
  engineered module's output matches on production data, then delete.

Note: the pandas reference functions in `network_idx.feature_engg.fcc_fixed_summary_county_residuals`
and `fcc_fixed_summary_est_ct_block` are deliberately **not** retired: the coverage block
parity oracle reuses them to cross-check the SQL until parity is signed off.
