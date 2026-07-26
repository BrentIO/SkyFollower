"""
Tests for message-processor/route_resolver.py — pure geo math and route-leg
resolution logic, independent of Redis/route_airports.lua (see #500 for that
integration test, already covered in shared/tests/test_route_airports_lua.py).
"""

from __future__ import annotations

import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_MESSAGE_PROCESSOR_DIR = os.path.dirname(_HERE)
if _MESSAGE_PROCESSOR_DIR not in sys.path:
    sys.path.insert(0, _MESSAGE_PROCESSOR_DIR)

from route_resolver import (  # noqa: E402
    cross_track_distance_nm,
    haversine_nm,
    initial_bearing_deg,
    passes_cross_track_check,
    resolve_origin_destination,
    select_candidate_leg,
)

# Real-world-flavored fixtures (approximate published coordinates).
KMSP = {"icao_code": "KMSP", "latitude": 44.882, "longitude": -93.222}
KMKE = {"icao_code": "KMKE", "latitude": 42.947, "longitude": -87.897}
KJFK = {"icao_code": "KJFK", "latitude": 40.6398, "longitude": -73.7789}
KATL = {"icao_code": "KATL", "latitude": 33.6367, "longitude": -84.4281}
MMMX = {"icao_code": "MMMX", "latitude": 19.4363, "longitude": -99.0721}
EGLL = {"icao_code": "EGLL", "latitude": 51.4706, "longitude": -0.4619}
KMIA = {"icao_code": "KMIA", "latitude": 25.7959, "longitude": -80.2870}


def _pos(lat: float, lon: float, altitude=None) -> dict:
    return {"latitude": lat, "longitude": lon, "altitude": altitude}


def _vel(heading=None, vertical_speed=None) -> dict:
    return {"heading": heading, "vertical_speed": vertical_speed}


# ---------------------------------------------------------------------------
# Pure geo math
# ---------------------------------------------------------------------------

class TestHaversine:
    def test_same_point_is_zero(self):
        assert haversine_nm(10, 10, 10, 10) == pytest.approx(0.0, abs=1e-9)

    def test_one_degree_longitude_at_equator(self):
        # 1 nm is defined as 1 arcminute of latitude; a degree of longitude
        # at the equator covers the same angular distance along a great circle.
        assert haversine_nm(0, 0, 0, 1) == pytest.approx(60.04, abs=0.1)


class TestInitialBearing:
    def test_due_east(self):
        assert initial_bearing_deg(0, 0, 0, 10) == pytest.approx(90, abs=0.01)

    def test_due_north(self):
        assert initial_bearing_deg(0, 0, 10, 0) == pytest.approx(0, abs=0.01)

    def test_due_south(self):
        assert initial_bearing_deg(10, 0, 0, 0) == pytest.approx(180, abs=0.01)

    def test_due_west(self):
        assert initial_bearing_deg(0, 10, 0, 0) == pytest.approx(270, abs=0.01)


class TestCrossTrackDistance:
    def test_point_on_line_is_zero(self):
        a, b = (0, 0), (0, 10)
        assert cross_track_distance_nm((0, 5), a, b) == pytest.approx(0.0, abs=1e-6)

    def test_point_off_line_is_positive(self):
        a, b = (0, 0), (0, 10)
        # Roughly 5 degrees of latitude north of the equatorial line ~= 300nm.
        assert cross_track_distance_nm((5, 5), a, b) == pytest.approx(300, abs=5)


# ---------------------------------------------------------------------------
# select_candidate_leg — 2-airport direct pass-through
# ---------------------------------------------------------------------------

class TestSelectCandidateLegDirect:
    def test_two_airports_no_ambiguity(self):
        leg = select_candidate_leg([KJFK, KATL], [], [])
        assert leg == (KJFK, KATL)

    def test_fewer_than_two_airports_is_none(self):
        assert select_candidate_leg([KJFK], [], []) is None
        assert select_candidate_leg([], [], []) is None


# ---------------------------------------------------------------------------
# select_candidate_leg — low-altitude proximity heuristic
# ---------------------------------------------------------------------------

class TestSelectCandidateLegProximity:
    def test_climbing_near_departure_resolves_first_leg(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 1.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 2.0}
        positions = [_pos(0.01, 0.01, altitude=2000)]
        velocities = [_vel(vertical_speed=1500)]
        assert select_candidate_leg([a, b, c], positions, velocities) == (a, b)

    def test_descending_near_arrival_resolves_last_leg(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 1.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 2.0}
        positions = [_pos(0.0, 2.01, altitude=1500)]
        velocities = [_vel(vertical_speed=-1200)]
        assert select_candidate_leg([a, b, c], positions, velocities) == (b, c)

    def test_duplicate_waypoint_disambiguated_by_climb_direction(self):
        """Round-trip route (KMIA appears at both ends) — being near KMIA
        while climbing can only structurally mean departing on the first
        leg, since the KMIA occurrence at the end has no next hop."""
        positions = [_pos(KMIA["latitude"] + 0.01, KMIA["longitude"] + 0.01, altitude=1000)]
        velocities = [_vel(vertical_speed=1800)]
        assert select_candidate_leg([KMIA, KJFK, KMIA], positions, velocities) == (KMIA, KJFK)

    def test_duplicate_waypoint_disambiguated_by_descent_direction(self):
        positions = [_pos(KMIA["latitude"] + 0.01, KMIA["longitude"] + 0.01, altitude=900)]
        velocities = [_vel(vertical_speed=-1600)]
        assert select_candidate_leg([KMIA, KJFK, KMIA], positions, velocities) == (KJFK, KMIA)

    def test_no_nearby_waypoint_falls_back_to_heading(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 1.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 2.0}
        # Low altitude, but far from every waypoint -- and no usable
        # heading either, so nothing resolves at all.
        positions = [_pos(10.0, 10.0, altitude=1000)]
        velocities = [_vel(vertical_speed=500)]
        assert select_candidate_leg([a, b, c], positions, velocities) is None

    def test_cruise_altitude_skips_proximity_uses_heading(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 10.0}
        c = {"icao_code": "CCCC", "latitude": 10.0, "longitude": 10.0}  # leg (b, c) heads north, not east
        # High altitude near "a" -- proximity heuristic must not fire.
        positions = [_pos(0.0, 0.01, altitude=35000)]
        velocities = [_vel(heading=90)]  # due east, matches leg (a, b) only
        assert select_candidate_leg([a, b, c], positions, velocities) == (a, b)


# ---------------------------------------------------------------------------
# select_candidate_leg — cruise heading-vs-bearing heuristic
# ---------------------------------------------------------------------------

class TestSelectCandidateLegHeading:
    def test_round_trip_resolves_return_leg_by_heading(self):
        # KMIA -> KJFK bearing ~= 18deg; KJFK -> KMIA (return) bearing ~= 202deg.
        positions = []  # no low-altitude signal available
        velocities = [_vel(heading=205)]
        assert select_candidate_leg([KMIA, KJFK, KMIA], positions, velocities) == (KJFK, KMIA)

    def test_round_trip_resolves_outbound_leg_by_heading(self):
        velocities = [_vel(heading=20)]
        assert select_candidate_leg([KMIA, KJFK, KMIA], [], velocities) == (KMIA, KJFK)

    def test_no_heading_available_is_unresolved(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 1.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 2.0}
        assert select_candidate_leg([a, b, c], [], [_vel(heading=None)]) is None

    def test_ambiguous_heading_between_similar_legs_is_unresolved(self):
        # Two colinear legs (same bearing) -- heading matches both equally
        # well, so the margin-over-runner-up check must reject it.
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 10.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 20.0}
        velocities = [_vel(heading=90)]
        assert select_candidate_leg([a, b, c], [], velocities) is None

    def test_heading_outside_tolerance_is_unresolved(self):
        velocities = [_vel(heading=110)]  # ~90deg off both KMIA<->KJFK bearings
        assert select_candidate_leg([KMIA, KJFK, KMIA], [], velocities) is None


# ---------------------------------------------------------------------------
# passes_cross_track_check
# ---------------------------------------------------------------------------

class TestCrossTrackCheck:
    def test_no_positions_rejected(self):
        assert passes_cross_track_check([], KJFK, KATL) is False

    def test_positions_on_track_accepted(self):
        positions = [_pos(37.0, -79.0)]  # roughly between KJFK and KATL
        assert passes_cross_track_check(positions, KJFK, KATL) is True

    def test_kmsp_kmke_bogus_route_rejected(self):
        """Real-world case from the issue: a flight's actual track ran
        ~800nm away from the KMSP-KMKE great-circle line at closest
        approach -- the route entry was bogus for this flight."""
        positions = [_pos(25.0, -90.0)]  # Gulf of Mexico -- nowhere near either
        assert passes_cross_track_check(positions, KMSP, KMKE) is False

    def test_long_haul_generous_threshold_accepts_real_routing_variance(self):
        # MMMX -> EGLL is ~4800nm; a few hundred nm of real-world routing
        # deviation is well within the 30%-of-route-distance threshold.
        positions = [_pos(29.0, -83.0)]  # near Orlando -- plausible routing
        assert passes_cross_track_check(positions, MMMX, EGLL) is True

    def test_short_route_tight_threshold_rejects_implausible_deviation(self):
        # KJFK -> KATL (~660nm, threshold ~198nm): ~300nm off the corridor
        # exceeds the percentage-based threshold, even though a flat 500nm
        # window would have let it through.
        positions = [_pos(44.0, -79.0)]
        assert passes_cross_track_check(positions, KJFK, KATL) is False

    def test_identical_endpoints_rejected(self):
        assert passes_cross_track_check([_pos(0, 0)], KJFK, KJFK) is False


# ---------------------------------------------------------------------------
# resolve_origin_destination — top-level, all-or-nothing
# ---------------------------------------------------------------------------

class TestResolveOriginDestination:
    def test_direct_route_resolves(self):
        positions = [_pos(37.0, -79.0)]
        origin, destination = resolve_origin_destination([KJFK, KATL], positions, [])
        assert (origin, destination) == ("KJFK", "KATL")

    def test_direct_route_sanity_check_rejection_leaves_both_none(self):
        positions = [_pos(25.0, -90.0)]
        origin, destination = resolve_origin_destination([KMSP, KMKE], positions, [])
        assert (origin, destination) == (None, None)

    def test_unresolvable_multi_leg_leaves_both_none(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 10.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 20.0}
        origin, destination = resolve_origin_destination([a, b, c], [], [_vel(heading=90)])
        assert (origin, destination) == (None, None)

    def test_multi_leg_resolved_and_sanity_checked(self):
        positions = [_pos(37.0, -76.0)]  # roughly on the KJFK->KMIA return corridor
        origin, destination = resolve_origin_destination(
            [KMIA, KJFK, KMIA], positions, [_vel(heading=205)]
        )
        assert (origin, destination) == ("KJFK", "KMIA")

    def test_never_partial_never_guesses(self):
        """Every branch returns either a fully-resolved pair or (None, None) --
        there is no code path that can set one field without the other."""
        origin, destination = resolve_origin_destination([], [], [])
        assert origin is None and destination is None
