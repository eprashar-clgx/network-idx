# BigQuery-prod is the source of truth; sources adapter seam

For the FCC, demographic, location, and rextag families, the authoritative raw data
lives in the BigQuery production database — that is the production source. The local
download path (renamed `data` → `data_download`) is a **dev/backfill adapter** behind a
single `sources` interface. Only the Census families (BAF, Address Count Listing) are
still downloaded. We record this because there are now two paths to the same FCC data;
without this note someone would delete the "redundant" download path or the BQ path.
Downstream code depends on the `sources` interface, not on where the raw came from.
