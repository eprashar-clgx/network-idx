# FCC features at tract grain are re-derived from a shared engineered definition

The model trains on a **tract**-grain frame but scores the **parcel** universe (see
ADR-0002), so every scoring feature needs a tract-grain counterpart the model can fit on.
For the four engineered telecom features (`cable_penetration`, `fiber_opportunity_gap`,
`fiber_speed_top_tier`, `provider_competitive_landscape_ord`) this is not a grain
transfer: the definitions are non-linear ratios/ordinals over block inputs, so they cannot
be aggregated up from `telecom_features_block`. Instead we **roll the transform-layer
inputs up to tract** (location counts summed, housing units summed, top-tier speed as an
`estimated_fcc_units`-weighted mean, and — critically — provider counts as `COUNT(DISTINCT
provider_id)` from the raw provider tables, *not* a sum of block provider counts) and then
apply the **same** engineered definitions at tract.

To guarantee the block and tract features can never drift, the four engineered expressions
live in one shared, grain-agnostic SQL fragment that both `telecom_features_block`
(features/telecom/engineered) and the new `fcc_features_ct` (grain_transfer) render. This
deliberately bends the glossary seam — `grain_transfer` is defined as *moving* a feature
between grains, not *building* one — because hosting the tract roll-up plus the shared
derivation in a single `grain_transfer` script keeps the data-engineering surface to one
runnable script per grain while the shared fragment preserves definitional parity. The
tract training frame (`features_ct`) then joins the four families at tract on `tract_geoid`
and emits every parcel-scoring feature's tract counterpart under the model's column names.

## Consequences

- The CT feature set is defined once and reused, so a change to an engineered telecom
  definition updates block and tract together.
- `grain_transfer/fcc_features_ct` does more than a pure grain transfer (it also derives
  engineered features); this is intentional and documented here so it is not "corrected"
  into the engineered layer later.
- Provider counts at tract require reading the raw provider-grain FCC tables, so the
  FCC-CT step reads production sources (like the block speeds step), not only dev tables.
