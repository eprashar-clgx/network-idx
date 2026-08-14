"""
Domain constants for the network_idx package.

Constants are split by concern into submodules so that each kind of constant lives
next to its peers: `sources` (external data-source URLs and technology vocabulary),
`geo` (geographic identifier lookup tables), `location` (the location family's
feature-engineering parameters), `schemas` (the input and output column contracts
for each processing and feature table), and `scoring_contract` (the parcel-level
scoring feature buckets, weights, scaling rules, and delivery names).
This package re-exports every public name from those submodules, so existing imports
such as `from network_idx.constants import STATE_USPS_TO_FIPS` continue to work
unchanged. To add a new constant, place it in the submodule that matches its concern;
it becomes importable from `network_idx.constants` automatically.
"""
from .sources import *  # noqa: F401,F403
from .geo import *  # noqa: F401,F403
from .location import *  # noqa: F401,F403
from .schemas import *  # noqa: F401,F403
from .scoring_contract import *  # noqa: F401,F403
