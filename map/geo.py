"""
Spherical-earth geo helpers for the map service.

The frontend (`map/frontend/src/lib/rangeRings.ts`) carries the *forward*
calculation -- a destination point given a start, bearing and distance --
for drawing the fixed range rings. This module is the *inverse*: the
bearing and great-circle distance *between* two points, used by
`map/range_outline.py` to bucket every received position by its compass
direction and distance from the configured "center" reference point.

Both use a spherical earth (mean radius). At the ranges an ADS-B receiver
covers (a few hundred nautical miles) the error versus an ellipsoidal
model is well under the one-degree bearing bucket the range outline uses.
"""

from __future__ import annotations

import math

# Mean earth radius (IUGG), in nautical miles -- matches
# map/frontend/src/lib/rangeRings.ts's EARTH_RADIUS_NM so the frontend's
# rings and this module's outline share one earth model.
EARTH_RADIUS_NM = 3440.065


def great_circle_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle (haversine) distance between two lat/lon points, in
    nautical miles."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    )
    return 2 * EARTH_RADIUS_NM * math.asin(min(1.0, math.sqrt(a)))


def initial_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial great-circle bearing from point 1 to point 2, in degrees
    clockwise from true north, normalised to [0, 360)."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_lambda = math.radians(lon2 - lon1)
    y = math.sin(d_lambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(d_lambda)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0
