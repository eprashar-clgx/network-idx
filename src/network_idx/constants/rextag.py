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

The subdivision threshold and the per-state shard counts were retuned after an initial
run of the fiber-distance worker exceeded BigQuery's on-demand CPU-to-bytes ratio limit
for every state with more than a small amount of fiber: each shard's spatial join was
scanning the full nationwide optimised-fiber table, so the CPU cost scaled with total
fiber regardless of how few parcels a shard covered. The worker now pre-filters fiber to
a buffer around the target state before joining (see fiber_distance_worker.sql), and the
values below were sized from the state's actual parcel/fiber cross-product so every
state fits under the ratio limit with headroom.
"""

# Maximum distance, in metres, to search for the nearest fiber line when measuring
# each parcel's distance to fiber (fifteen miles is 24140 metres).
FIBER_MAX_SEARCH_DIST_M = 24140

# Radius, in metres, within which to count distinct fiber lines around each parcel
# (three miles is 4828 metres).
FIBER_RADIUS_COUNT_M = 4828

# Fiber lines with more than this many vertices are subdivided before the spatial
# join, to keep the proximity computation efficient. Sixteen (down from an initial 256)
# keeps subdivided segments small enough that the per-state fiber pre-filter and the
# spatial join stay within the on-demand CPU-to-bytes ratio limit at production scale.
FIBER_SUBDIVIDE_MAX_VERTICES = 16

# Per-state shard counts for the parcel-to-fiber distance calculation, sized from each
# state's actual parcel/fiber cross-product. The spatial join is too large to run in a
# single pass for denser states, so those states are split into this many shards by a
# modulus of the parcel id; every state below is in exactly one tier.
_FIBER_SHARD_TIERS = {
    32: ["06", "12", "13", "17", "18", "25", "34", "36", "37", "39", "42", "45", "48"],  # CA FL GA IL IN MA NJ NY NC OH PA SC TX
    24: ["51"],  # VA
    16: ["29"],  # MO
    12: ["01", "04", "21", "55"],  # AL AZ KY WI
    8: ["08", "19", "24", "26", "27", "49"],  # CO IA MD MI MN UT
    6: ["05", "09", "22", "23", "31", "47", "53"],  # AR CT LA ME NE TN WA
    4: ["16", "40", "41", "44", "46"],  # ID OK OR RI SD
    2: ["02", "10", "11", "20", "28", "30", "32", "33", "35", "38", "50", "54", "56"],  # AK DE DC KS MS MT NV NH NM ND VT WV WY
}
FIBER_STATE_SHARD_COUNTS = {
    fips: shard_count
    for shard_count, states in _FIBER_SHARD_TIERS.items()
    for fips in states
}

# The number of shards used for any state not listed in FIBER_STATE_SHARD_COUNTS
# (currently only Hawaii, which was not part of the tiering exercise since it has
# negligible fiber; two shards keeps it consistent with the lightest explicit tier).
FIBER_DEFAULT_SHARD_COUNT = 2
