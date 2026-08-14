"""
Environment selector for the network_idx package.

This module reads the single environment flag that tells the rest of the package
which environment it is running in (for example a developer's local machine, a
remote development box, or a production run). Values that genuinely differ between
environments are resolved from environment variables so the same code runs
everywhere without edits.
"""
import os

# Which environment this process is running in. Defaults to "local" when unset.
NETWORK_IDX_ENV = os.getenv("NETWORK_IDX_ENV", "local")
