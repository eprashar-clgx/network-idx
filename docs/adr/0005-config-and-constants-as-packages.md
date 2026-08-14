# Config and constants are packages split by concern, not single files

`config.py` and `constants.py` had each grown into a single large file mixing many
unrelated concerns (local file paths, GCS settings, BigQuery identifiers, geographic
lookup tables, column contracts, and the parcel-scoring feature rules). This made them
hard to navigate and every module imported from one giant namespace.

We turn each into a package whose submodules are split by concern, while a single
`__init__.py` re-exports every public name. `config/` is split into `environment`,
`paths`, `gcs`, and `bigquery`; `constants/` is split into `sources`, `geo`, `schemas`,
and `scoring_contract`. Because the package `__init__` re-exports everything, every
existing `from network_idx.config import X` and `from network_idx.constants import X`
keeps working with no change at any call site.

This gives us two things at once: locality (a concern lives in its own file next to its
peers) and a single discoverable place to look for any path, identifier, or contract.
New values are added to the submodule that matches their concern. We record this because
the layout is load-bearing — modules across the codebase depend on the package import
surface staying stable, so the re-export in each `__init__.py` must be preserved.
