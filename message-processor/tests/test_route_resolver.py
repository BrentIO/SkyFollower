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
    along_track_distance_nm,
    cross_track_distance_nm,
    haversine_nm,
    heading_is_stable,
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
KMCO = {"icao_code": "KMCO", "latitude": 28.4294, "longitude": -81.3089}


def _pos(lat: float, lon: float, altitude=None) -> dict:
    return {"latitude": lat, "longitude": lon, "altitude": altitude}


def _vel(heading=None, vertical_speed=None) -> dict:
    return {"heading": heading, "vertical_speed": vertical_speed}


def _vels(*headings: float) -> list[dict]:
    """Several consecutive velocity samples at the given headings -- enough
    (see route_resolver.MIN_HEADING_SAMPLES) for heading_is_stable to
    actually evaluate them, standing in for consistent consecutive cruise
    readings."""
    return [_vel(heading=h) for h in headings]


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


class TestAlongTrackDistance:
    def test_point_at_origin_is_zero(self):
        a, b = (0, 0), (0, 10)
        assert along_track_distance_nm((0, 0), a, b) == pytest.approx(0.0, abs=1e-6)

    def test_point_at_destination_equals_route_distance(self):
        a, b = (0, 0), (0, 10)
        assert along_track_distance_nm((0, 10), a, b) == pytest.approx(
            haversine_nm(*a, *b), abs=0.5
        )

    def test_point_between_endpoints_is_positive_and_less_than_route_distance(self):
        a, b = (0, 0), (0, 10)
        d = along_track_distance_nm((0, 5), a, b)
        assert 0 < d < haversine_nm(*a, *b)

    def test_point_behind_origin_is_negative(self):
        a, b = (0, 0), (0, 10)
        assert along_track_distance_nm((0, -5), a, b) < 0

    def test_point_well_past_destination_exceeds_route_distance(self):
        a, b = (0, 0), (0, 10)
        d = along_track_distance_nm((0, 20), a, b)
        assert d > haversine_nm(*a, *b)


# ---------------------------------------------------------------------------
# heading_is_stable
# ---------------------------------------------------------------------------

class TestHeadingIsStable:
    def test_fewer_than_minimum_samples_is_unstable(self):
        assert heading_is_stable(_vels(90, 91)) is False

    def test_consistent_recent_samples_are_stable(self):
        assert heading_is_stable(_vels(88, 90, 91, 89, 90)) is True

    def test_widely_varying_samples_are_unstable(self):
        # A circling/holding aircraft's heading sweeps through a full circle.
        assert heading_is_stable(_vels(10, 100, 190, 280, 10)) is False

    def test_none_headings_are_ignored_when_counting_samples(self):
        headings = [_vel(heading=None), _vel(heading=90), _vel(heading=91), _vel(heading=90)]
        assert heading_is_stable(headings) is True

    def test_no_headings_at_all_is_unstable(self):
        assert heading_is_stable([_vel(vertical_speed=500)]) is False


# ---------------------------------------------------------------------------
# select_candidate_leg — 2-airport direct pass-through
# ---------------------------------------------------------------------------

class TestSelectCandidateLegDirect:
    def test_two_airports_no_ambiguity(self):
        leg, is_final = select_candidate_leg([KJFK, KATL], [], [])
        assert leg == (KJFK, KATL)
        assert is_final is True

    def test_fewer_than_two_airports_is_none(self):
        assert select_candidate_leg([KJFK], [], []) == (None, True)
        assert select_candidate_leg([], [], []) == (None, True)


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
        assert select_candidate_leg([a, b, c], positions, velocities) == ((a, b), True)

    def test_descending_near_arrival_resolves_last_leg(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 1.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 2.0}
        positions = [_pos(0.0, 2.01, altitude=1500)]
        velocities = [_vel(vertical_speed=-1200)]
        assert select_candidate_leg([a, b, c], positions, velocities) == ((b, c), True)

    def test_duplicate_waypoint_disambiguated_by_climb_direction(self):
        """Round-trip route (KMIA appears at both ends) — being near KMIA
        while climbing can only structurally mean departing on the first
        leg, since the KMIA occurrence at the end has no next hop."""
        positions = [_pos(KMIA["latitude"] + 0.01, KMIA["longitude"] + 0.01, altitude=1000)]
        velocities = [_vel(vertical_speed=1800)]
        assert select_candidate_leg([KMIA, KJFK, KMIA], positions, velocities) == ((KMIA, KJFK), True)

    def test_duplicate_waypoint_disambiguated_by_descent_direction(self):
        positions = [_pos(KMIA["latitude"] + 0.01, KMIA["longitude"] + 0.01, altitude=900)]
        velocities = [_vel(vertical_speed=-1600)]
        assert select_candidate_leg([KMIA, KJFK, KMIA], positions, velocities) == ((KJFK, KMIA), True)

    def test_no_nearby_waypoint_falls_back_to_heading(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 1.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 2.0}
        # Low altitude, but far from every waypoint -- and no usable
        # heading either, so nothing resolves at all.
        positions = [_pos(10.0, 10.0, altitude=1000)]
        velocities = [_vel(vertical_speed=500)]
        assert select_candidate_leg([a, b, c], positions, velocities) == (None, False)

    def test_cruise_altitude_skips_proximity_uses_heading(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 10.0}
        c = {"icao_code": "CCCC", "latitude": 10.0, "longitude": 10.0}  # leg (b, c) heads north, not east
        # High altitude near "a" -- proximity heuristic must not fire.
        positions = [_pos(0.0, 0.01, altitude=35000)]
        velocities = _vels(89, 90, 91)  # due east, matches leg (a, b) only
        assert select_candidate_leg([a, b, c], positions, velocities) == ((a, b), True)


# ---------------------------------------------------------------------------
# select_candidate_leg — cruise heading-vs-bearing heuristic
# ---------------------------------------------------------------------------

class TestSelectCandidateLegHeading:
    def test_round_trip_resolves_return_leg_by_heading(self):
        # KMIA -> KJFK bearing ~= 18deg; KJFK -> KMIA (return) bearing ~= 202deg.
        positions = []  # no low-altitude signal available
        velocities = _vels(204, 205, 206)
        assert select_candidate_leg([KMIA, KJFK, KMIA], positions, velocities) == ((KJFK, KMIA), True)

    def test_round_trip_resolves_outbound_leg_by_heading(self):
        velocities = _vels(19, 20, 21)
        assert select_candidate_leg([KMIA, KJFK, KMIA], [], velocities) == ((KMIA, KJFK), True)

    def test_single_sample_is_not_final(self):
        """A single instantaneous heading reading is never trusted, even if
        it would otherwise cleanly resolve -- not enough samples yet to
        confirm it isn't a momentary reading during a turn/hold."""
        velocities = [_vel(heading=20)]
        assert select_candidate_leg([KMIA, KJFK, KMIA], [], velocities) == (None, False)

    def test_no_heading_available_is_not_final(self):
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 1.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 2.0}
        assert select_candidate_leg([a, b, c], [], [_vel(heading=None)]) == (None, False)

    def test_ambiguous_heading_between_similar_legs_is_final_but_unresolved(self):
        # Two colinear legs (same bearing) -- heading matches both equally
        # well, so the margin-over-runner-up check must reject it. Stable
        # samples, so this is a settled "can't disambiguate", not a
        # not-enough-data case.
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 10.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 20.0}
        velocities = _vels(89, 90, 91)
        assert select_candidate_leg([a, b, c], [], velocities) == (None, True)

    def test_heading_outside_tolerance_is_final_but_unresolved(self):
        velocities = _vels(109, 110, 111)  # ~90deg off both KMIA<->KJFK bearings
        assert select_candidate_leg([KMIA, KJFK, KMIA], [], velocities) == (None, True)

    def test_unstable_heading_is_not_final(self):
        """The holding-pattern case: heading sweeps through a full circle,
        so even though a single sample might momentarily match a leg's
        bearing, the instability itself must defer resolution rather than
        risk a wrong guess."""
        velocities = [_vel(heading=h) for h in (10, 100, 190, 280)]
        assert select_candidate_leg([KMIA, KJFK, KMIA], [], velocities) == (None, False)


# ---------------------------------------------------------------------------
# passes_cross_track_check — cross-track + along-track sanity checks
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

    def test_position_well_past_destination_rejected_by_along_track(self):
        """A position whose cross-track (perpendicular) distance is well
        within tolerance can still be nowhere near the actual leg if it's
        far beyond one endpoint -- the holding-pattern scenario below is a
        real case of exactly this."""
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 10.0}  # ~600nm route
        # Cross-track distance here is 0 (dead on the line) but the point
        # is ~600nm past b.
        assert passes_cross_track_check([_pos(0.0, 20.0)], a, b) is False

    def test_holding_pattern_east_of_jacksonville_rejected(self):
        """The exact scenario reported: aircraft actually flying KJFK->KMIA
        is holding at 22,000ft east of Jacksonville due to weather, on the
        route KJFK-KMIA-KMCO-KJFK. A wrong candidate leg (KMIA->KMCO) whose
        bearing happens to roughly match the holding pattern's northbound
        leg passes the cross-track check (~52nm, well under the ~150nm
        threshold) but must be rejected here: the holding point projects
        ~100nm past KMCO, well outside the along-track bound."""
        hold_point = _pos(30.3, -81.0)
        assert passes_cross_track_check([hold_point], KMIA, KMCO) is False


# ---------------------------------------------------------------------------
# resolve_origin_destination — top-level, all-or-nothing, is_final semantics
# ---------------------------------------------------------------------------

class TestResolveOriginDestination:
    def test_direct_route_resolves(self):
        positions = [_pos(37.0, -79.0)]
        origin, destination, is_final = resolve_origin_destination([KJFK, KATL], positions, [])
        assert (origin, destination) == ("KJFK", "KATL")
        assert is_final is True

    def test_direct_route_sanity_check_rejection_leaves_both_none(self):
        positions = [_pos(25.0, -90.0)]
        origin, destination, is_final = resolve_origin_destination([KMSP, KMKE], positions, [])
        assert (origin, destination) == (None, None)
        assert is_final is True

    def test_unstable_heading_multi_leg_is_not_final(self):
        """Not enough heading history yet (or genuinely circling) -- the
        caller must retry later rather than treat this as a settled
        unresolvable case."""
        a = {"icao_code": "AAAA", "latitude": 0.0, "longitude": 0.0}
        b = {"icao_code": "BBBB", "latitude": 0.0, "longitude": 10.0}
        c = {"icao_code": "CCCC", "latitude": 0.0, "longitude": 20.0}
        origin, destination, is_final = resolve_origin_destination([a, b, c], [], [_vel(heading=90)])
        assert (origin, destination) == (None, None)
        assert is_final is False

    def test_multi_leg_resolved_and_sanity_checked(self):
        positions = [_pos(37.0, -76.0)]  # roughly on the KJFK->KMIA return corridor
        origin, destination, is_final = resolve_origin_destination(
            [KMIA, KJFK, KMIA], positions, _vels(204, 205, 206)
        )
        assert (origin, destination) == ("KJFK", "KMIA")
        assert is_final is True

    def test_holding_pattern_scenario_end_to_end(self):
        """Full reproduction of the reported scenario: aircraft actually
        flying KJFK->KMIA is holding at 22,000ft east of Jacksonville on
        route KJFK-KMIA-KMCO-KJFK. A stable-but-misleading northbound
        holding heading (~350deg) would, without the along-track check,
        confidently resolve to the wrong leg (KMIA->KMCO); with it, the
        pair is rejected -- unresolved is correct here, not a wrong guess."""
        airports = [KJFK, KMIA, KMCO, KJFK]
        hold_point = _pos(30.3, -81.0, altitude=22000)
        velocities = _vels(349, 350, 351, 350, 349)
        origin, destination, is_final = resolve_origin_destination(
            airports, [hold_point], velocities
        )
        assert (origin, destination) == (None, None)
        assert is_final is True

    def test_never_partial_never_guesses(self):
        """Every branch returns either a fully-resolved pair or (None, None) --
        there is no code path that can set one field without the other."""
        origin, destination, _is_final = resolve_origin_destination([], [], [])
        assert origin is None and destination is None
