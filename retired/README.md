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
