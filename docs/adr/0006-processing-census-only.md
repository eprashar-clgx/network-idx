# Processing is Census-only; FCC reshapes live in the telecom transform layer

The processing module originally existed because the FCC broadband data had to be
downloaded as files and then pivoted and interpolated into block-level shape. Now
that the authoritative FCC raw data is available directly in the BigQuery production
database, that download-then-reshape reason no longer applies to FCC.

We therefore keep in `processing` only the reshapes that are still tied to files
downloaded from the Census website: the Block Assignment Files (block → state,
county, tract, place) and the Address Count Listing (housing units per block). The
FCC deterministic reshapes — the fixed-speeds pivot to one wide row per block with
copper, cable, and fiber columns, and the dasymetric interpolation of county and
place coverage down to blocks — move into `features/telecom/transform`, which reads
FCC raw straight from BigQuery.

A consequence is that there is no single `processing.block_frame` step that joins
FCC and Census into one canonical block frame. The FCC block reshapes and the join
that combines coverage with speeds live in the telecom transform layer, and the
Census block tables are produced by `processing`. We record this because it moves a
visible chunk of logic across a module boundary and removes the block-frame step
that an earlier draft of the architecture described.
