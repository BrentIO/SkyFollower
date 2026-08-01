"""
Resolves a flight's origin/destination from its route:{ident} entry (already
normalized into an ordered array of full airport records by
shared/lua/route_airports.lua), reconciled against the flight's own observed
position/heading/altitude. Called mid-flight, the moment ident/position/
altitude/heading are all known -- see message_processor.main's
_maybe_resolve_route -- not at archive time.

See message-processor/README.md's "Route Leg Resolution" section for the
full design rationale and worked examples, including the holding-pattern
case (an aircraft circling well below cruise groundspeed near a route's
midpoint can momentarily present a heading matching a *different* leg's
bearing, and a position that coincidentally isn't far off that other leg's
line either) that motivates the along-track bound and heading-stability
requirement below.

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
# A single flat nm value doesn't work at either end of the route-length
# spectrum -- too loose for a short hop (nearly any position "counts" as on
# the route) and too tight for a long one (routine GPS/heading noise on a
# multi-thousand-nm leg would fail the check), so the threshold scales with
# the route's own distance, floored so a very short route still gets a
# sane minimum.
CROSS_TRACK_FLOOR_NM = 150
CROSS_TRACK_PERCENTAGE = 0.30

# Along-track sanity check: a position must project onto the great-circle
# line somewhere between the origin and destination (plus this much slack
# for ordinary departure/arrival maneuvering) to count as "on this leg" at
# all. Cross-track distance alone only measures perpendicular distance to
# the *infinite* line through both airports -- a position far beyond one
# endpoint, or off along a completely different (but coincidentally
# similarly-bearing) leg, can still land well within the cross-track
# threshold. A small fixed allowance, not a percentage of route length, is
# deliberate: unlike lateral routing variance (jet stream, ATC, weather),
# there's no legitimate reason for a position to be materially further from
# the destination than the destination itself.
ALONG_TRACK_SLACK_NM = 50

# Heading-vs-bearing stability: a single instantaneous heading reading is
# unreliable while an aircraft is circling/holding (e.g. an IFR holding
# pattern) -- its heading sweeps through a full circle and can momentarily
# align with an entirely wrong leg's bearing, passing both the heading
# tolerance/margin checks *and* the cross-track check (a holding pattern
# near one end of a leg can sit well within cross-track distance of a
# totally different, similarly-oriented leg). Requiring several recent
# headings to agree before trusting the heuristic catches this: cruise
# flight naturally produces consistent consecutive headings, while
# circling does not.
MIN_HEADING_SAMPLES = 3
HEADING_STABILITY_TOLERANCE_DEG = 20


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


def along_track_distance_nm(
    point: tuple[float, float], a: tuple[float, float], b: tuple[float, float]
) -> float:
    """Signed distance along the great-circle line a->b at which `point`'s
    perpendicular projection falls, in nm. Zero at a, positive toward and
    past b, negative on the far side of a (behind the start of the leg)."""
    angular_dist_ap = haversine_nm(a[0], a[1], point[0], point[1]) / EARTH_RADIUS_NM
    bearing_ap = initial_bearing_deg(a[0], a[1], point[0], point[1])
    bearing_ab = initial_bearing_deg(a[0], a[1], b[0], b[1])
    bearing_diff = math.radians(bearing_ap - bearing_ab)
    cross_track_ang = math.asin(math.sin(angular_dist_ap) * math.sin(bearing_diff))
    along_track_ang = math.acos(min(1.0, math.cos(angular_dist_ap) / math.cos(cross_track_ang)))
    sign = 1.0 if math.cos(bearing_diff) >= 0 else -1.0
    return sign * along_track_ang * EARTH_RADIUS_NM


def _heading_diff_deg(a: float, b: float) -> float:
    """Smallest angular difference between two headings, 0-180."""
    d = abs(a - b) % 360
    return d if d <= 180 else 360 - d


def heading_is_stable(velocities: list[dict]) -> bool:
    """True once at least MIN_HEADING_SAMPLES recent headings all agree
    within HEADING_STABILITY_TOLERANCE_DEG of each other. False both when
    there isn't enough data yet and when the aircraft is genuinely
    circling/holding -- either way, the heading-vs-bearing heuristic isn't
    safe to trust yet, and the caller should treat this as "try again once
    more data arrives" rather than a settled answer."""
    headings = [v["heading"] for v in velocities if v.get("heading") is not None]
    if len(headings) < MIN_HEADING_SAMPLES:
        return False
    recent = headings[-MIN_HEADING_SAMPLES:]
    return max(
        _heading_diff_deg(a, b) for a in recent for b in recent
    ) <= HEADING_STABILITY_TOLERANCE_DEG


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
) -> tuple[Optional[tuple[dict, dict]], bool]:
    """Picks the one adjacent airport pair the flight is most likely flying.
    A 2-airport route has no ambiguity. A 3+ airport (multi-leg) route is
    resolved low-altitude-first (proximity + climb/descent), falling back to
    cruise heading-vs-bearing.

    Returns (leg, is_final):
    - leg is the (origin, destination) dict pair, or None if unresolved.
    - is_final is True once this is a settled determination -- resolved, or
      confidently ruled out -- and False only when heading data exists but
      hasn't yet stabilized enough to trust (see heading_is_stable): more
      messages may still turn this into a real answer, so the caller should
      try again later rather than treating None as final."""
    if len(airports) < 2:
        return None, True
    if len(airports) == 2:
        return (airports[0], airports[1]), True

    leg = _resolve_by_proximity(airports, positions, velocities)
    if leg is not None:
        return leg, True

    if not heading_is_stable(velocities):
        return None, False

    return _resolve_by_heading(airports, velocities), True


def _sanity_check_violation(
    positions: list[dict], origin: dict, destination: dict
) -> Optional[str]:
    """Returns None if the pair passes both sanity checks; otherwise a short
    human-readable description of which check failed, at which position,
    and by how much -- used to build a diagnostic log message when a
    candidate is rejected. See passes_cross_track_check for what each check
    means; this is the same logic, just reporting *why* instead of a bare
    bool."""
    a, b = _coords(origin), _coords(destination)
    if a is None or b is None:
        return "origin or destination airport record is missing latitude/longitude"
    if a == b:
        return "origin and destination resolve to the same coordinates"
    if not positions:
        return "no positions recorded yet to sanity-check against"

    route_distance_nm = haversine_nm(a[0], a[1], b[0], b[1])
    if route_distance_nm == 0:
        return "origin and destination resolve to the same coordinates"
    cross_track_threshold_nm = max(CROSS_TRACK_FLOOR_NM, CROSS_TRACK_PERCENTAGE * route_distance_nm)

    for pos in positions:
        point = (pos["latitude"], pos["longitude"])
        xtrack = cross_track_distance_nm(point, a, b)
        if xtrack > cross_track_threshold_nm:
            return (
                f"cross-track distance {xtrack:.1f}nm at position {point} exceeds "
                f"threshold {cross_track_threshold_nm:.1f}nm (route distance {route_distance_nm:.1f}nm)"
            )
        along_track = along_track_distance_nm(point, a, b)
        if along_track < -ALONG_TRACK_SLACK_NM or along_track > route_distance_nm + ALONG_TRACK_SLACK_NM:
            return (
                f"along-track projection {along_track:.1f}nm at position {point} falls outside "
                f"[0, {route_distance_nm:.1f}]nm (+/-{ALONG_TRACK_SLACK_NM}nm slack)"
            )
    return None


def passes_cross_track_check(positions: list[dict], origin: dict, destination: dict) -> bool:
    """Rejects a candidate origin/destination pair whose great-circle line
    the flight's actual track never came close to, or which the track only
    approaches well beyond one of the two endpoints — the VRS standing-data
    source is community-maintained and a callsign can carry a stale or
    mismatched route with no way to detect that from the string alone.
    Requires at least one position to check against; with none available
    there's nothing to verify the pair against, so it's rejected rather
    than trusted unconditionally.

    Two independent checks, both against every recorded position:
    - Cross-track: perpendicular distance to the great-circle line, capped
      at max(150nm, 30% of route distance) -- generous for genuine
      long-haul routing variance (jet stream, ATC, weather).
    - Along-track: the position's projection onto that line must fall
      within [0, route_distance] (plus a small fixed slack for ordinary
      terminal-area maneuvering) -- catches a position that's coincidentally
      near the line's bearing but nowhere close to the actual leg, e.g. well
      past the destination (see this module's own docstring for the
      holding-pattern case this specifically catches)."""
    return _sanity_check_violation(positions, origin, destination) is None


def resolve_origin_destination(
    airports: list[dict], positions: list[dict], velocities: list[dict]
) -> tuple[Optional[str], Optional[str], bool, Optional[str]]:
    """Top-level entry point. Returns (origin_icao, destination_icao,
    is_final, rejection_reason):
    - The ICAO pair is both None unless exactly one unambiguous,
      sanity-checked leg was resolved -- never a partial or best-guess pair.
    - is_final is False only for the "heading not yet stable" case (see
      select_candidate_leg) -- the caller should not treat a (None, None,
      False, None) result as a permanent answer.
    - rejection_reason is None whenever a pair was resolved (or is_final is
      False -- nothing to report yet); otherwise a short human-readable
      string describing why a final result has no origin/destination, for
      the caller to log alongside what Redis returned."""
    leg, is_final = select_candidate_leg(airports, positions, velocities)
    if leg is None:
        if not is_final:
            return None, None, False, None
        return (
            None, None, True,
            "no candidate leg could be confidently determined "
            "(low-altitude proximity was inconclusive, and heading-vs-bearing "
            "either lacked a match within tolerance or wasn't clear of the runner-up)",
        )
    origin, destination = leg
    violation = _sanity_check_violation(positions, origin, destination)
    if violation is not None:
        reason = (
            f"candidate leg {origin.get('icao_code')}->{destination.get('icao_code')} "
            f"failed the sanity check: {violation}"
        )
        return None, None, True, reason
    return origin.get("icao_code"), destination.get("icao_code"), True, None
