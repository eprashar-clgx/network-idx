"""
Location family feature-engineering parameters for network_idx.

These are the analytical choices that define how the parcel-level growth-signal
features are built: the quarter-mile search radius used when counting nearby
flagged parcels, the H3 resolutions used to index each parcel and to aggregate
growth "hotspot" cells, the volume and variety thresholds that qualify an H3 cell
as a growth hotspot, the maximum search distance when measuring how far a parcel
sits from the nearest hotspot, and the number of metres in a mile used to report
that distance in miles. They were chosen during exploratory analysis, which is why
they live here as named constants rather than being buried inside the SQL.
"""

# Search radius, in metres, for counting flagged parcels near each parcel
# (a quarter mile is 402.336 metres).
GROWTH_COUNT_RADIUS_M = 402.336

# H3 resolution used to spatially index each parcel in the growth-counts table.
GROWTH_PARCEL_H3_RES = 8

# H3 resolution used to aggregate growth parcels into candidate hotspot cells.
GROWTH_HOTSPOT_H3_RES = 7

# A hotspot cell must contain at least this many total growth signals.
GROWTH_HOTSPOT_TOTAL_FLAGS_THRESHOLD = 50

# A hotspot cell must have at least this many signals outside its single largest
# signal category (its "variety").
GROWTH_HOTSPOT_VARIETY_THRESHOLD = 10

# Maximum distance, in metres, to search for a nearby hotspot when measuring the
# distance from a parcel to the nearest hotspot (fifteen miles is 24140 metres).
HOTSPOT_MAX_SEARCH_DIST_M = 24140

# Number of metres in a mile, used to convert distances to miles for reporting.
METERS_PER_MILE = 1609.34
