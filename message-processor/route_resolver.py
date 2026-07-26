"""
Resolves a completed flight's origin/destination from its route:{ident} entry
(already normalized into an ordered array of full airport records by
shared/lua/route_airports.lua), reconciled against the flight's own observed
position/heading/altitude.

See message-processor/README.md's "Route Leg Resolution" section for the
full design rationale and worked examples.

All functions here are pure — no Redis, no I/O — so the resolution and
sanity-check logic can be unit tested independently of the Lua round trip
that produces the `airports` array.
"""

from __future__ import annotations

import math
from typing import Optional

EARTH_RADIUS_NM = 3440.065

# Below this altitude, proximity to a route waypoint (plus climb/descent
# direction) is treated as a near-dispositive signal for which leg is
# active — near an airport at low altitude almost certainly means
# just-departed or about-to-land.
LOW_ALTITUDE_FT = 10000

# "Near" a waypoint, for the low-altitude proximity check.
PROXIMITY_NM = 25

# Cruise heading vs. candidate-leg bearing: the closest candidate must be
# within this many degrees of the observed heading to be considered a match
# at all.
HEADING_TOLERANCE_DEG = 30

# ...and must beat the second-closest candidate by at least this many
# degrees to count as unambiguous — prevents picking the "least-bad" match
# among several similarly-plausible candidates (e.g. a genuine multi-city
# route whose legs happen to share a similar general direction).
HEADING_MARGIN_DEG = 15

# Cross-track sanity check: threshold_nm = max(floor, percentage * route_distance).
# See the #498 issue discussion for why a flat nm threshold doesn't work at
# either end of the route-length spectrum.
CROSS_TRACK_FLOOR_NM = 150
CROSS_TRACK_PERCENTAGE = 0.30


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points, in nautical miles."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return EARTH_RADIUS_NM * 2 * math.asin(min(1.0, math.sqrt(a)))


def initial_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial great-circle bearing from point 1 to point 2, in degrees (0-360)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    y = math.sin(dlambda) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def cross_track_distance_nm(
    point: tuple[float, float], a: tuple[float, float], b: tuple[float, float]
) -> float:
    """Perpendicular distance from `point` to the great-circle line a->b, in nm."""
    angular_dist_ap = haversine_nm(a[0], a[1], point[0], point[1]) / EARTH_RADIUS_NM
    bearing_ap = math.radians(initial_bearing_deg(a[0], a[1], point[0], point[1]))
    bearing_ab = math.radians(initial_bearing_deg(a[0], a[1], b[0], b[1]))
    return abs(math.asin(math.sin(angular_dist_ap) * math.sin(bearing_ap - bearing_ab)) * EARTH_RADIUS_NM)


def _heading_diff_deg(a: float, b: float) -> float:
    """Smallest angular difference between two headings, 0-180."""
    d = abs(a - b) % 360
    return d if d <= 180 else 360 - d


def _coords(airport: dict) -> Optional[tuple[float, float]]:
    lat, lon = airport.get("latitude"), airport.get("longitude")
    if lat is None or lon is None:
        return None
    return (lat, lon)


def _resolve_by_proximity(
    airports: list[dict], positions: list[dict], velocities: list[dict]
) -> Optional[tuple[dict, dict]]:
    """Low-altitude signal: is the flight's earliest position near a route
    waypoint, and is it climbing away (departing) or descending toward
    (arriving at) it? Returns None — deferring to the cruise heading
    heuristic — on any ambiguity (no nearby waypoint, or no clear vertical
    trend).

    A waypoint can appear more than once in a round-trip route (e.g.
    KMIA-KJFK-KMIA), so "nearby" alone doesn't uniquely pick an index —
    climbing only makes structural sense for a nearby occurrence that has a
    next hop (an origin), descending only for one with a previous hop (a
    destination). That structural filter is usually enough to disambiguate
    duplicate occurrences on its own; if more than one nearby occurrence
    still survives it, this is genuinely ambiguous and defers to heading.
    """
    if not positions:
        return None
    first_pos = positions[0]
    altitude = first_pos.get("altitude")
    if altitude is None or altitude >= LOW_ALTITUDE_FT:
        return None
    lat, lon = first_pos["latitude"], first_pos["longitude"]

    nearby = [
        i for i, airport in enumerate(airports)
        if (coords := _coords(airport)) is not None
        and haversine_nm(coords[0], coords[1], lat, lon) <= PROXIMITY_NM
    ]
    if not nearby:
        return None

    vertical_speed = next(
        (v["vertical_speed"] for v in velocities if v.get("vertical_speed") is not None), None
    )
    if not vertical_speed:
        return None

    if vertical_speed > 0:
        candidates = [i for i in nearby if i + 1 < len(airports)]
        if len(candidates) != 1:
            return None
        idx = candidates[0]
        return airports[idx], airports[idx + 1]
    else:
        candidates = [i for i in nearby if i - 1 >= 0]
        if len(candidates) != 1:
            return None
        idx = candidates[0]
        return airports[idx - 1], airports[idx]


def _resolve_by_heading(airports: list[dict], velocities: list[dict]) -> Optional[tuple[dict, dict]]:
    """Cruise signal: compare the flight's most recent observed heading
    against each candidate leg's great-circle bearing. Resolves only when
    exactly one candidate is both within tolerance and clear of the
    runner-up by the required margin."""
    heading = next(
        (v["heading"] for v in reversed(velocities) if v.get("heading") is not None), None
    )
    if heading is None:
        return None

    scored = []
    for i in range(len(airports) - 1):
        a, b = _coords(airports[i]), _coords(airports[i + 1])
        if a is None or b is None:
            continue
        bearing = initial_bearing_deg(a[0], a[1], b[0], b[1])
        scored.append((i, _heading_diff_deg(heading, bearing)))

    if not scored:
        return None
    scored.sort(key=lambda pair: pair[1])
    best_idx, best_diff = scored[0]
    if best_diff > HEADING_TOLERANCE_DEG:
        return None
    if len(scored) > 1 and scored[1][1] - best_diff < HEADING_MARGIN_DEG:
        return None
    return airports[best_idx], airports[best_idx + 1]


def select_candidate_leg(
    airports: list[dict], positions: list[dict], velocities: list[dict]
) -> Optional[tuple[dict, dict]]:
    """Picks the one adjacent airport pair the flight is most likely flying.
    A 2-airport route has no ambiguity. A 3+ airport (multi-leg) route is
    resolved low-altitude-first (proximity + climb/descent), falling back to
    cruise heading-vs-bearing. Returns None — leave unresolved rather than
    guess — if neither heuristic produces a confident single answer."""
    if len(airports) < 2:
        return None
    if len(airports) == 2:
        return airports[0], airports[1]
    return (
        _resolve_by_proximity(airports, positions, velocities)
        or _resolve_by_heading(airports, velocities)
    )


def passes_cross_track_check(positions: list[dict], origin: dict, destination: dict) -> bool:
    """Rejects a candidate origin/destination pair whose great-circle line
    the flight's actual track never came close to — the VRS standing-data
    source is community-maintained and a callsign can carry a stale or
    mismatched route with no way to detect that from the string alone.
    Requires at least one position to check against; with none available
    there's nothing to verify the pair against, so it's rejected rather
    than trusted unconditionally."""
    a, b = _coords(origin), _coords(destination)
    if a is None or b is None or a == b or not positions:
        return False

    route_distance_nm = haversine_nm(a[0], a[1], b[0], b[1])
    if route_distance_nm == 0:
        return False
    threshold_nm = max(CROSS_TRACK_FLOOR_NM, CROSS_TRACK_PERCENTAGE * route_distance_nm)

    return all(
        cross_track_distance_nm((pos["latitude"], pos["longitude"]), a, b) <= threshold_nm
        for pos in positions
    )


def resolve_origin_destination(
    airports: list[dict], positions: list[dict], velocities: list[dict]
) -> tuple[Optional[str], Optional[str]]:
    """Top-level entry point. Returns (origin_icao, destination_icao), both
    None unless exactly one unambiguous, sanity-checked leg was resolved —
    never a partial or best-guess pair."""
    leg = select_candidate_leg(airports, positions, velocities)
    if leg is None:
        return None, None
    origin, destination = leg
    if not passes_cross_track_check(positions, origin, destination):
        return None, None
    return origin.get("icao_code"), destination.get("icao_code")
