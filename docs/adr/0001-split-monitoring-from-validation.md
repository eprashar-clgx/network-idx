# Split monitoring from validation

We keep two separate modules for output quality. `monitoring` is cheap and runs on
**every** scoring run (null/fill rates, drift vs the frozen baseline, train/scoring
parity, score-band and business rollups; it can halt/alert). `validation` runs
**periodically** and is heavy — the construct-validity dossier for the composite
indicator (sensitivity, coherence, spatial, external ACS/BEAD/peer benchmarks, expert
review). We split them because their cadence and cost differ: simple/frequent vs
complex/infrequent. `validation` is expected to grow internal sub-modules, and the exact
line between the two may be refined (some checks may migrate between them), but the
division itself is deliberate.
