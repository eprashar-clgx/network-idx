"""
Rextag family feature-engineering parameters for network_idx.

These are the analytical and execution choices that define how the parcel-to-fiber
distance features are built: the maximum distance searched when finding the nearest
fiber line, the radius within which nearby fiber lines are counted, the vertex count
above which a fiber line is subdivided before the spatial join, and the per-state
shard counts that keep the very large parcel-to-fiber spatial join tractable. The
distance thresholds were chosen during exploratory analysis; the subdivision and
shard settings are execution tuning. They live here as named constants so both the
Python driver and the deployed SQL procedures read the same values.
"""

# Maximum distance, in metres, to search for the nearest fiber line when measuring
# each parcel's distance to fiber (fifteen miles is 24140 metres).
FIBER_MAX_SEARCH_DIST_M = 24140

# Radius, in metres, within which to count distinct fiber lines around each parcel
# (three miles is 4828 metres).
FIBER_RADIUS_COUNT_M = 4828

# Fiber lines with more than this many vertices are subdivided before the spatial
# join, to keep the proximity computation efficient.
FIBER_SUBDIVIDE_MAX_VERTICES = 256

# Per-state shard counts for the parcel-to-fiber distance calculation. The spatial
# join is too large to run in a single pass for the heaviest states, so those states
# are split into this many shards by a modulus of the parcel id; every other state
# runs as a single shard.
FIBER_STATE_SHARD_COUNTS = {
    "48": 4,  # Texas
    "34": 2,  # New Jersey
    "36": 2,  # New York
}

# The number of shards used for any state not listed in FIBER_STATE_SHARD_COUNTS.
FIBER_DEFAULT_SHARD_COUNT = 1
