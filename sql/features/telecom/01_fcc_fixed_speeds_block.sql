-- =============================================================================
-- Block-level fixed broadband speed tiers by technology.
-- Module        : network_idx.features.telecom.transform.fcc_fixed_speeds_block
-- Generated from : src/network_idx/features/telecom/transform/fcc_fixed_speeds_block.py  (python -m network_idx.features.telecom.transform.fcc_fixed_speeds_block --dry-run)
-- Run in         : CONSOLE (reads PROD)
--
-- PROJECT SUBSTITUTION (only these two identifiers change per environment):
--   PROD_PROJECT = clgx-idap-bigquery-prd-a990   (raw source reads)
--   DEV_PROJECT  = clgx-gis-app-dev-06e3         (feature/output writes)
-- All dataset and table names are concrete. See sql/README.md for run order.
-- =============================================================================

-- FCC fixed-speed block aggregation (deterministic transform).
--
-- Aggregates the raw FCC Broadband Data Collection fixed-broadband location records —
-- read from the three per-technology BigQuery-production tables (copper, cable, fiber) —
-- up to one row per census block, producing, per technology, the count of distinct
-- serviceable locations and providers and the maximum advertised download/upload speed.
-- There is no analytical choice here — it is a mechanical group-and-pivot — so it belongs
-- in the transform layer. The output, input tables, and state-FIPS derivation are rendered
-- from configuration so the same logic runs against any environment's project.
--
-- Each source table already contains a single technology, so no technology-code filter is
-- applied; the technology is carried by which table a row came from.

CREATE OR REPLACE TABLE `clgx-gis-app-dev-06e3.teu_telecom.fcc_fixed_speeds_block` AS

WITH copper AS (
  SELECT
    block_geoid,
    state_usps,
    COUNT(DISTINCT location_id) AS copper_location_count,
    COUNT(DISTINCT provider_id) AS copper_provider_count,
    MAX(max_advertised_download_speed) AS copper_max_download_speed,
    MAX(max_advertised_upload_speed) AS copper_max_upload_speed
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_common_reference_ext.fcc_copper_fixed_broadband`
  GROUP BY block_geoid, state_usps
),

cable AS (
  SELECT
    block_geoid,
    state_usps,
    COUNT(DISTINCT location_id) AS cable_location_count,
    COUNT(DISTINCT provider_id) AS cable_provider_count,
    MAX(max_advertised_download_speed) AS cable_max_download_speed,
    MAX(max_advertised_upload_speed) AS cable_max_upload_speed
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_common_reference_ext.fcc_cable_fixed_broadband`
  GROUP BY block_geoid, state_usps
),

fiber AS (
  SELECT
    block_geoid,
    state_usps,
    COUNT(DISTINCT location_id) AS fiber_location_count,
    COUNT(DISTINCT provider_id) AS fiber_provider_count,
    MAX(max_advertised_download_speed) AS fiber_max_download_speed,
    MAX(max_advertised_upload_speed) AS fiber_max_upload_speed
  FROM `clgx-idap-bigquery-prd-a990.edr_ent_common_reference_ext.fcc_fiber_fixed_broadband`
  GROUP BY block_geoid, state_usps
)

SELECT
  COALESCE(cab.state_usps, cop.state_usps, fib.state_usps) AS state_usps,
  -- the first two digits of the 15-digit block GEOID are the state FIPS
  SUBSTR(COALESCE(cab.block_geoid, cop.block_geoid, fib.block_geoid), 1, 2) AS state_fips,
  COALESCE(cab.block_geoid, cop.block_geoid, fib.block_geoid) AS block_geoid,
  cab.cable_location_count,
  cab.cable_provider_count,
  cab.cable_max_download_speed,
  cab.cable_max_upload_speed,
  cop.copper_location_count,
  cop.copper_provider_count,
  cop.copper_max_download_speed,
  cop.copper_max_upload_speed,
  fib.fiber_location_count,
  fib.fiber_provider_count,
  fib.fiber_max_download_speed,
  fib.fiber_max_upload_speed
FROM cable cab
FULL OUTER JOIN copper cop
  ON cab.block_geoid = cop.block_geoid AND cab.state_usps = cop.state_usps
FULL OUTER JOIN fiber fib
  ON COALESCE(cab.block_geoid, cop.block_geoid) = fib.block_geoid
  AND COALESCE(cab.state_usps, cop.state_usps) = fib.state_usps;
