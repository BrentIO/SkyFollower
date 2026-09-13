"""
Tests for tools/map-load-generator's pure lane-motion, fleet-variety, and
packet-construction functions.

This tool has no --seed -- start/finish points, squawk-code selection, and
emergency-squawk migration are all true per-run randomness (an intentional
tradeoff, see README). Tests therefore check *bounds and rules* (e.g.
"altitude is always one of the valid hemispheric values for that lane's
direction", "vs never exceeds +/-800", "VFR aircraft always squawk 1200")
rather than exact values, and run randomized checks across many samples/
seats/ticks to keep false negatives astronomically unlikely.

A live UDP send isn't unit-testable (no real map instance in this suite),
but `FleetSimulator` -- the piece that decides *what* gets sent -- has no
socket I/O and is exercised directly here, matching
tools/traffic-replayer/tests/test_replay.py's existing convention for this
directory: main() itself (argument parsing, opening the real socket) is
intentionally not covered, since it is thin glue with no branching logic of
its own beyond what's already exercised below.
"""

from __future__ import annotations

import importlib.util
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL_DIR = os.path.dirname(_HERE)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "map_load_generator_main",
        os.path.join(_TOOL_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["map_load_generator_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

# Constants / tables
_ALL_EAST_ALTITUDES = _mod._ALL_EAST_ALTITUDES
_ALL_WEST_ALTITUDES = _mod._ALL_WEST_ALTITUDES
_HELICOPTER_TYPES = _mod._HELICOPTER_TYPES
_MILITARY_TYPES = _mod._MILITARY_TYPES
_LIVERY_TYPE_DESIGNATOR = _mod._LIVERY_TYPE_DESIGNATOR
_LIVERY_NAME = _mod._LIVERY_NAME
_BALLOON_TYPE_DESIGNATOR = _mod._BALLOON_TYPE_DESIGNATOR
_EMERGENCY_SQUAWK_CODES = _mod._EMERGENCY_SQUAWK_CODES
_RECEIVER_SOURCE_POOL = _mod._RECEIVER_SOURCE_POOL
_RESERVED_SQUAWKS = _mod._RESERVED_SQUAWKS

# Functions
is_valid_hemispheric_altitude = _mod.is_valid_hemispheric_altitude
altitude_flight_rule = _mod.altitude_flight_rule
build_lanes = _mod.build_lanes
lane_position = _mod.lane_position
initial_start_distance_nm = _mod.initial_start_distance_nm
respawn_start_distance_nm = _mod.respawn_start_distance_nm
groundspeed_kt_for_altitude = _mod.groundspeed_kt_for_altitude
clamp_vertical_speed_fpm = _mod.clamp_vertical_speed_fpm
climb_altitude_and_vs = _mod.climb_altitude_and_vs
climb_altitude_pair = _mod.climb_altitude_pair
balloon_position = _mod.balloon_position
tick_elapsed_seconds = _mod.tick_elapsed_seconds
synthetic_icao_hex = _mod.synthetic_icao_hex
synthetic_ident = _mod.synthetic_ident
synthetic_squawk = _mod.synthetic_squawk
random_receiver_sources = _mod.random_receiver_sources
assign_seat_roles = _mod.assign_seat_roles
build_seats = _mod.build_seats
spawn_occupant = _mod.spawn_occupant
build_position_packet = _mod.build_position_packet
build_heartbeat_packet = _mod.build_heartbeat_packet
build_metadata_packet = _mod.build_metadata_packet
FleetSimulator = _mod.FleetSimulator
Lane = _mod.Lane


_CENTER_LAT = 39.8283
_CENTER_LON = -98.5795
_AREA_RADIUS_NM = 15.0


def _simulator(**overrides) -> FleetSimulator:
    kwargs = dict(
        aircraft_count=10,
        center_lat=_CENTER_LAT,
        center_lon=_CENTER_LON,
        area_radius_nm=_AREA_RADIUS_NM,
        climbing_count=2,
        processor_id="load-gen-1",
        matched_rule=None,
        ramp_up_seconds=0.0,
        duration=0.0,
    )
    kwargs.update(overrides)
    return FleetSimulator(**kwargs)


class TestHemisphericAltitudeTable:
    """Cross-checks against literal values from the issue's own FAA
    hemispheric altitude table (14 CFR 91.159/91.179)."""

    def test_below_fl180_ifr_eastbound_values(self):
        for alt in (3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000):
            assert is_valid_hemispheric_altitude(alt, eastbound=True)
            assert not is_valid_hemispheric_altitude(alt, eastbound=False)

    def test_below_fl180_ifr_westbound_values(self):
        for alt in (4000, 6000, 8000, 10000, 12000, 14000, 16000):
            assert is_valid_hemispheric_altitude(alt, eastbound=False)
            assert not is_valid_hemispheric_altitude(alt, eastbound=True)

    def test_below_fl180_vfr_eastbound_values(self):
        for alt in (3500, 5500, 7500, 9500, 11500, 13500, 15500, 17500):
            assert is_valid_hemispheric_altitude(alt, eastbound=True)
            assert altitude_flight_rule(alt) == "VFR"

    def test_below_fl180_vfr_westbound_values(self):
        for alt in (4500, 6500, 8500, 10500, 12500, 14500, 16500):
            assert is_valid_hemispheric_altitude(alt, eastbound=False)
            assert altitude_flight_rule(alt) == "VFR"

    def test_at_above_fl180_eastbound_flight_levels(self):
        for alt in (19000, 21000, 23000, 25000, 27000, 29000):
            assert is_valid_hemispheric_altitude(alt, eastbound=True)
            assert altitude_flight_rule(alt) == "IFR"
            assert not is_valid_hemispheric_altitude(alt, eastbound=False)

    def test_at_above_fl180_westbound_flight_levels(self):
        for alt in (20000, 22000, 24000, 26000, 28000):
            assert is_valid_hemispheric_altitude(alt, eastbound=False)
            assert altitude_flight_rule(alt) == "IFR"
            assert not is_valid_hemispheric_altitude(alt, eastbound=True)

    def test_at_above_fl180_has_no_vfr_variant(self):
        # No +500 flight-level values exist in either table.
        for alt in list(_ALL_EAST_ALTITUDES) + list(_ALL_WEST_ALTITUDES):
            if alt >= 18000:
                assert alt % 1000 == 0

    def test_arbitrary_altitude_is_invalid_both_directions(self):
        assert not is_valid_hemispheric_altitude(1234, eastbound=True)
        assert not is_valid_hemispheric_altitude(1234, eastbound=False)

    def test_ifr_and_vfr_never_overlap_within_a_direction(self):
        east_ifr = {a for a in _ALL_EAST_ALTITUDES if altitude_flight_rule(a) == "IFR"}
        east_vfr = {a for a in _ALL_EAST_ALTITUDES if altitude_flight_rule(a) == "VFR"}
        assert east_ifr.isdisjoint(east_vfr)


class TestBuildLanes:
    def test_both_orientations_represented(self):
        lanes = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        orientations = {lane.orientation for lane in lanes}
        assert orientations == {"EW", "NS"}

    def test_both_directions_represented(self):
        lanes = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        directions = {lane.eastbound for lane in lanes}
        assert directions == {True, False}

    def test_every_lane_altitude_is_hemispherically_correct(self):
        lanes = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        for lane in lanes:
            assert is_valid_hemispheric_altitude(lane.altitude_ft, lane.eastbound)

    def test_ew_lane_heading_is_east_or_west(self):
        lanes = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        for lane in lanes:
            if lane.orientation == "EW":
                assert lane.heading in (90.0, 270.0)

    def test_ns_lane_heading_is_north_or_south(self):
        lanes = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        for lane in lanes:
            if lane.orientation == "NS":
                assert lane.heading in (0.0, 180.0)

    def test_lane_length_matches_area_diameter(self):
        lanes = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        for lane in lanes:
            assert lane.length_nm == 2.0 * _AREA_RADIUS_NM

    def test_lanes_are_geographically_distinct_within_an_orientation(self):
        lanes = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        ew_fixed_coords = [lane.fixed_coord for lane in lanes if lane.orientation == "EW"]
        assert len(set(ew_fixed_coords)) == len(ew_fixed_coords)

    def test_deterministic_no_randomness(self):
        a = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        b = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        assert a == b


class TestLanePosition:
    def test_start_and_end_match_lane_endpoints(self):
        lane = Lane("EW", True, 5000.0, 30.0, 39.0, -99.0, -97.0)
        lat0, lon0 = lane_position(lane, 0.0)
        lat1, lon1 = lane_position(lane, 30.0)
        assert (lat0, lon0) == (39.0, -99.0)
        assert (lat1, lon1) == (39.0, -97.0)

    def test_midpoint_is_between_endpoints(self):
        lane = Lane("NS", True, 5000.0, 30.0, -98.0, 38.0, 40.0)
        lat, lon = lane_position(lane, 15.0)
        assert 38.0 < lat < 40.0
        assert lon == -98.0

    def test_clamped_outside_range(self):
        lane = Lane("EW", True, 5000.0, 30.0, 39.0, -99.0, -97.0)
        lat, lon = lane_position(lane, 999.0)
        assert lon == -97.0


class TestSpeedAndVerticalRateModel:
    def test_speed_at_low_altitude_bound(self):
        assert groundspeed_kt_for_altitude(100.0) == 60.0

    def test_speed_at_high_altitude_bound(self):
        assert groundspeed_kt_for_altitude(40000.0) == 500.0

    def test_speed_below_low_bound_is_clamped(self):
        assert groundspeed_kt_for_altitude(0.0) == 60.0

    def test_speed_above_high_bound_is_clamped(self):
        assert groundspeed_kt_for_altitude(60000.0) == 500.0

    def test_speed_increases_monotonically_with_altitude(self):
        prev = groundspeed_kt_for_altitude(100.0)
        for alt in range(1000, 40001, 1000):
            cur = groundspeed_kt_for_altitude(alt)
            assert cur >= prev
            prev = cur

    def test_speed_midpoint(self):
        # Linear interpolation: halfway between 100 and 40000 ft.
        mid_alt = (100.0 + 40000.0) / 2.0
        expected = (60.0 + 500.0) / 2.0
        assert abs(groundspeed_kt_for_altitude(mid_alt) - expected) < 0.01

    def test_vertical_speed_clamp_bounds(self):
        assert clamp_vertical_speed_fpm(10000.0) == 800.0
        assert clamp_vertical_speed_fpm(-10000.0) == -800.0
        assert clamp_vertical_speed_fpm(0.0) == 0.0
        assert clamp_vertical_speed_fpm(799.0) == 799.0


class TestClimbAltitudeAndVs:
    def test_never_exceeds_vertical_speed_clamp(self):
        for elapsed in range(0, 3600, 17):
            alt, vs = climb_altitude_and_vs(float(elapsed), 5000.0, 7000.0)
            assert -800.0 <= vs <= 800.0

    def test_altitude_stays_within_bounds(self):
        for elapsed in range(0, 3600, 13):
            alt, vs = climb_altitude_and_vs(float(elapsed), 5000.0, 7000.0)
            assert 5000.0 <= alt <= 7000.0

    def test_starts_at_lo_climbing(self):
        alt, vs = climb_altitude_and_vs(0.0, 5000.0, 7000.0)
        assert alt == 5000.0
        assert vs > 0

    def test_reaches_hi_then_descends(self):
        leg_minutes = (7000.0 - 5000.0) / 500.0  # _CLIMB_RATE_FPM default
        alt_at_top, vs_at_top = climb_altitude_and_vs(leg_minutes * 60.0, 5000.0, 7000.0)
        assert abs(alt_at_top - 7000.0) < 0.01
        _, vs_just_after = climb_altitude_and_vs(leg_minutes * 60.0 + 1.0, 5000.0, 7000.0)
        assert vs_just_after < 0

    def test_degenerate_span_holds_level(self):
        alt, vs = climb_altitude_and_vs(100.0, 5000.0, 5000.0)
        assert alt == 5000.0
        assert vs == 0.0


class TestClimbAltitudePair:
    def test_both_endpoints_are_hemispherically_valid(self):
        lane = Lane("EW", True, 5000.0, 30.0, 39.0, -99.0, -97.0)
        lo, hi = climb_altitude_pair(lane)
        assert is_valid_hemispheric_altitude(lo, True)
        assert is_valid_hemispheric_altitude(hi, True)

    def test_endpoints_are_distinct_when_pool_has_more_than_one_value(self):
        lane = Lane("EW", True, 5000.0, 30.0, 39.0, -99.0, -97.0)
        lo, hi = climb_altitude_pair(lane)
        assert lo != hi

    def test_wraps_at_top_of_table(self):
        # The highest eastbound altitude wraps to the lowest.
        lane = Lane("EW", True, float(_ALL_EAST_ALTITUDES[-1]), 30.0, 39.0, -99.0, -97.0)
        lo, hi = climb_altitude_pair(lane)
        assert {lo, hi} == {float(_ALL_EAST_ALTITUDES[-1]), float(_ALL_EAST_ALTITUDES[0])}


class TestBalloonPosition:
    def test_altitude_stays_near_60000ft(self):
        for elapsed in range(0, 3600, 23):
            fields = balloon_position(
                float(elapsed), spawn_lat=39.0, spawn_lon=-98.0, base_heading_deg=45.0, drift_kt=20.0
            )
            assert 59000.0 <= fields["alt"] <= 61000.0

    def test_velocity_matches_configured_drift(self):
        fields = balloon_position(100.0, spawn_lat=39.0, spawn_lon=-98.0, base_heading_deg=45.0, drift_kt=18.5)
        assert fields["velocity"] == 18.5

    def test_vertical_speed_within_shared_clamp(self):
        for elapsed in range(0, 3600, 23):
            fields = balloon_position(
                float(elapsed), spawn_lat=39.0, spawn_lon=-98.0, base_heading_deg=45.0, drift_kt=20.0
            )
            assert -800.0 <= fields["vs"] <= 800.0

    def test_heading_wanders_around_base_heading(self):
        headings = [
            balloon_position(float(t), spawn_lat=39.0, spawn_lon=-98.0, base_heading_deg=135.0, drift_kt=20.0)["hdg"]
            for t in range(0, 400, 40)
        ]
        assert len(set(headings)) > 1  # genuinely wanders, not frozen
        for h in headings:
            diff = min(abs(h - 135.0), 360.0 - abs(h - 135.0))
            assert diff <= 25.0  # wander amplitude is 20 degrees

    def test_position_moves_over_time(self):
        p0 = balloon_position(0.0, spawn_lat=39.0, spawn_lon=-98.0, base_heading_deg=45.0, drift_kt=20.0)
        p1 = balloon_position(600.0, spawn_lat=39.0, spawn_lon=-98.0, base_heading_deg=45.0, drift_kt=20.0)
        assert (p0["lat"], p0["lon"]) != (p1["lat"], p1["lon"])

    def test_heading_is_off_the_lane_grid(self):
        # The balloon's base headings are all diagonal (not 0/90/180/270).
        for base in _mod._BALLOON_BASE_HEADINGS_DEG:
            assert base % 90.0 != 0.0


class TestTickElapsedSeconds:
    def test_scales_by_position_rate(self):
        assert tick_elapsed_seconds(4, 2.0) == 2.0
        assert tick_elapsed_seconds(4, 1.0) == 4.0

    def test_zero_rate_falls_back_to_tick_count(self):
        assert tick_elapsed_seconds(7, 0) == 7.0


class TestSyntheticIdentity:
    def test_icao_hex_is_six_hex_characters(self):
        for i in (0, 1, 15, 16, 255, 4095):
            hexval = synthetic_icao_hex(i)
            assert len(hexval) == 6
            int(hexval, 16)  # raises if not valid hex

    def test_icao_hex_is_deterministic(self):
        assert synthetic_icao_hex(42) == synthetic_icao_hex(42)

    def test_icao_hex_is_unique_per_number(self):
        values = [synthetic_icao_hex(i) for i in range(500)]
        assert len(set(values)) == len(values)

    def test_icao_hex_uses_the_given_prefix(self):
        assert synthetic_icao_hex(1, prefix="AB").startswith("AB")

    def test_ident_is_deterministic_and_distinct(self):
        assert synthetic_ident(3) == synthetic_ident(3)
        assert synthetic_ident(3) != synthetic_ident(4)

    def test_squawk_is_four_octal_digits(self):
        for i in range(500):
            code = synthetic_squawk(i)
            assert len(code) == 4
            assert all(c in "01234567" for c in code)

    def test_squawk_is_deterministic(self):
        assert synthetic_squawk(99) == synthetic_squawk(99)

    def test_squawk_never_collides_with_reserved_codes(self):
        for i in range(2000):
            assert synthetic_squawk(i) not in _RESERVED_SQUAWKS


class TestRandomReceiverSources:
    def test_size_is_between_one_and_three(self):
        for _ in range(200):
            sources = random_receiver_sources()
            assert 1 <= len(sources) <= 3

    def test_values_are_real_source_tags(self):
        for _ in range(200):
            for source in random_receiver_sources():
                assert source in _RECEIVER_SOURCE_POOL

    def test_no_duplicates_within_one_call(self):
        for _ in range(200):
            sources = random_receiver_sources()
            assert len(sources) == len(set(sources))


class TestAssignSeatRoles:
    def test_every_seat_gets_exactly_one_role(self):
        roles = assign_seat_roles(20, climbing_count=2)
        assert set(roles.keys()) == set(range(20))

    def test_role_counts_match_fixed_small_defaults(self):
        roles = assign_seat_roles(20, climbing_count=2)
        counts = {}
        for role in roles.values():
            counts[role] = counts.get(role, 0) + 1
        assert counts.get("military", 0) == 1
        assert counts.get("livery", 0) == 1
        assert counts.get("no_metadata", 0) == 2
        assert counts.get("helicopter", 0) == 3
        assert counts.get("climbing", 0) == 2

    def test_graceful_with_too_few_seats(self):
        # Must not raise even when there aren't enough seats for every role.
        roles = assign_seat_roles(2, climbing_count=2)
        assert set(roles.keys()) == {0, 1}

    def test_zero_seats_returns_empty(self):
        assert assign_seat_roles(0, climbing_count=2) == {}


class TestBuildSeatsTypeVariety:
    def _seats(self, lane_seat_count=30, climbing_count=2):
        lanes = build_lanes(_CENTER_LAT, _CENTER_LON, _AREA_RADIUS_NM)
        return build_seats(lanes, lane_seat_count, climbing_count)

    def test_at_least_three_distinct_helicopter_designators(self):
        seats = self._seats()
        heli_types = {s.type_designator for s in seats if s.role == "helicopter"}
        assert heli_types == set(_HELICOPTER_TYPES)
        assert len(heli_types) >= 3

    def test_livery_seat_uses_osprey_designator(self):
        seats = self._seats()
        livery_seats = [s for s in seats if s.role == "livery"]
        assert len(livery_seats) == 1
        assert livery_seats[0].type_designator == _LIVERY_TYPE_DESIGNATOR

    def test_climbing_seats_carry_climb_altitudes(self):
        seats = self._seats()
        climbing_seats = [s for s in seats if s.role == "climbing"]
        assert len(climbing_seats) == 2
        for s in climbing_seats:
            assert s.climb_altitudes is not None
            assert s.climb_altitudes[0] != s.climb_altitudes[1]

    def test_no_metadata_seats_have_no_fixed_type(self):
        seats = self._seats()
        no_meta_seats = [s for s in seats if s.role == "no_metadata"]
        assert len(no_meta_seats) == 2
        for s in no_meta_seats:
            assert s.type_designator is None

    def test_regular_and_helicopter_seats_use_real_recognized_designators(self):
        # Every designator here must be one aircraftIconResolver.ts actually
        # resolves (verified by hand against that file -- see main.py's
        # fleet-variety comment block for the exact resolution path of each).
        recognized = set(_mod._REGULAR_TYPE_POOL) | set(_HELICOPTER_TYPES) | {_BALLOON_TYPE_DESIGNATOR}
        seats = self._seats()
        for s in seats:
            if s.role in ("regular", "climbing", "helicopter"):
                assert s.type_designator in recognized


class TestSpawnOccupant:
    def _seat(self, role, altitude_ft, eastbound=True, type_designator="A320", climb_altitudes=None):
        lane = Lane("EW", eastbound, altitude_ft, 30.0, 39.0, -99.0, -97.0)
        return _mod.Seat(0, lane, role, type_designator, climb_altitudes)

    def _origin_destination(self):
        return {"icao_code": "ZZ00", "name": "x"}, {"icao_code": "ZZ01", "name": "y"}

    def test_vfr_seat_squawks_1200_no_operator_or_route(self):
        origin, destination = self._origin_destination()
        seat = self._seat("regular", 3500.0)  # VFR altitude
        for i in range(20):
            occ = spawn_occupant(seat, i, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
            assert occ.squawk == "1200"
            assert occ.operator is None
            assert occ.origin is None
            assert occ.destination is None

    def test_ifr_seat_has_operator_and_fixed_route(self):
        origin, destination = self._origin_destination()
        seat = self._seat("regular", 3000.0)  # IFR altitude
        for i in range(20):
            occ = spawn_occupant(seat, i, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
            assert occ.squawk != "1200"
            assert occ.operator is not None
            assert occ.origin == origin
            assert occ.destination == destination

    def test_ifr_squawk_never_the_vfr_code(self):
        origin, destination = self._origin_destination()
        seat = self._seat("regular", 19000.0)  # FL190, always IFR
        for i in range(200):
            occ = spawn_occupant(seat, i, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
            assert occ.squawk != "1200"

    def test_above_fl180_always_ifr_with_operator(self):
        origin, destination = self._origin_destination()
        seat = self._seat("regular", 21000.0)
        occ = spawn_occupant(seat, 1, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
        assert occ.operator is not None
        assert occ.origin is not None
        assert occ.destination is not None

    def test_no_metadata_seat_carries_only_icao_hex_and_emitter_category(self):
        origin, destination = self._origin_destination()
        seat = self._seat("no_metadata", 3000.0, type_designator=None)
        occ = spawn_occupant(seat, 1, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
        assert set(occ.aircraft_extra.keys()) == {"emitter_category"}

    def test_military_seat_flags_military_true_with_recognized_designator(self):
        origin, destination = self._origin_destination()
        seat = self._seat("military", 3000.0, type_designator=None)
        for i in range(20):
            occ = spawn_occupant(seat, i, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
            assert occ.aircraft_extra["military"] is True
            assert occ.aircraft_extra["type_designator"] in _MILITARY_TYPES

    def test_livery_seat_carries_special_livery_name(self):
        origin, destination = self._origin_destination()
        seat = self._seat("livery", 3000.0, type_designator=_LIVERY_TYPE_DESIGNATOR)
        occ = spawn_occupant(seat, 1, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
        assert occ.aircraft_extra["special_livery"] == _LIVERY_NAME
        assert occ.aircraft_extra["type_designator"] == _LIVERY_TYPE_DESIGNATOR
        assert occ.aircraft_extra["description_code"] == "T"

    def test_regular_seat_carries_wake_turbulence_category(self):
        origin, destination = self._origin_destination()
        seat = self._seat("regular", 3000.0)
        occ = spawn_occupant(seat, 1, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
        assert occ.aircraft_extra["wake_turbulence_category"] in ("light", "medium", "heavy")

    def test_icao_hex_and_ident_derive_from_aircraft_number(self):
        origin, destination = self._origin_destination()
        seat = self._seat("regular", 3000.0)
        occ1 = spawn_occupant(seat, 5, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
        occ2 = spawn_occupant(seat, 6, spawn_elapsed_seconds=0.0, start_distance_nm=0.0, fixed_origin=origin, fixed_destination=destination)
        assert occ1.icao_hex != occ2.icao_hex
        assert occ1.ident != occ2.ident


class TestFleetSimulatorLaneMotion:
    """Straight lane-based motion, replacing the old orbit model."""

    def test_aircraft_move_in_straight_lines_not_circles(self):
        sim = _simulator(ramp_up_seconds=0.0)
        seat = sim.seats[0]
        occupant = sim.occupants[0]
        positions = []
        for t in range(0, 20):
            occupant.spawn_elapsed_seconds = 0.0  # pin so we sample the same occupant
            frame = sim.tick(float(t))
            for seat_index, occ, fields in frame:
                if seat_index == 0:
                    positions.append((fields["lat"], fields["lon"]))
        # A straight lane holds either lat or lon constant throughout.
        lats = {p[0] for p in positions}
        lons = {p[1] for p in positions}
        assert len(lats) == 1 or len(lons) == 1

    def test_altitude_is_always_hemispherically_valid_for_non_climbing_seats(self):
        sim = _simulator(climbing_count=0, ramp_up_seconds=0.0)
        for t in range(0, 5):
            for seat_index, occ, fields in sim.tick(float(t)):
                if seat_index is None:
                    continue
                seat = sim.seats[seat_index]
                assert is_valid_hemispheric_altitude(fields["alt"], seat.lane.eastbound)

    def test_vs_never_exceeds_clamp_across_many_ticks(self):
        sim = _simulator(ramp_up_seconds=0.0)
        for t in range(0, 200, 5):
            for _, _, fields in sim.tick(float(t)):
                assert -800.0 <= fields["vs"] <= 800.0

    def test_heading_matches_lane_orientation(self):
        sim = _simulator(ramp_up_seconds=0.0)
        for seat_index, occ, fields in sim.tick(0.0):
            if seat_index is None:
                continue
            seat = sim.seats[seat_index]
            assert fields["hdg"] == seat.lane.heading

    def test_velocity_matches_speed_formula_for_displayed_altitude(self):
        # Excludes the balloon, which is deliberately outside this formula.
        sim = _simulator(ramp_up_seconds=0.0)
        for seat_index, _, fields in sim.tick(0.0):
            if seat_index is None:
                continue
            expected = round(groundspeed_kt_for_altitude(fields["alt"]), 1)
            assert fields["velocity"] == expected


class TestFleetSimulatorRampUp:
    def test_zero_ramp_up_makes_all_seats_active_immediately(self):
        sim = _simulator(ramp_up_seconds=0.0)
        frame = sim.tick(0.0)
        active_seat_indices = {s for s, _, _ in frame if s is not None}
        assert active_seat_indices == set(range(sim.lane_seat_count))

    def test_nonzero_ramp_up_staggers_start_times(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=60.0, duration=0.0)
        assert sim.start_offsets[0] == 0.0
        assert sim.start_offsets[1] > sim.start_offsets[0]
        # Last seat before the balloon should start noticeably later.
        last_seat_index = sim.lane_seat_count - 1
        assert sim.start_offsets[last_seat_index] < 60.0

    def test_ramp_up_disabled_when_duration_shorter(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=60.0, duration=10.0)
        assert sim.ramp_up_seconds == 0.0
        assert all(v == 0.0 for v in sim.start_offsets.values())

    def test_ramp_up_not_disabled_when_duration_is_zero(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=60.0, duration=0.0)
        assert sim.ramp_up_seconds == 60.0

    def test_ramp_up_not_disabled_when_duration_longer(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=60.0, duration=120.0)
        assert sim.ramp_up_seconds == 60.0

    def test_seat_inactive_before_its_start_offset(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=60.0, duration=0.0)
        last_seat_index = sim.lane_seat_count - 1
        start_at = sim.start_offsets[last_seat_index]
        if start_at > 0:
            frame = sim.tick(start_at / 2.0)
            assert all(s != last_seat_index for s, _, _ in frame)


class TestFleetSimulatorLaneExitReplacement:
    def test_exiting_seat_gets_a_fresh_never_reused_hex(self):
        sim = _simulator(aircraft_count=6, ramp_up_seconds=0.0)
        seat = sim.seats[0]
        original_hex = sim.occupants[0].icao_hex
        # Force the occupant near the finish line, then tick past it.
        sim.occupants[0].start_distance_nm = seat.lane.length_nm - 0.001
        sim.occupants[0].spawn_elapsed_seconds = 0.0
        sim.tick(1.0)
        new_hex = sim.occupants[0].icao_hex
        assert new_hex != original_hex

    def test_exited_seat_produces_no_packet_that_tick(self):
        sim = _simulator(aircraft_count=6, ramp_up_seconds=0.0)
        seat = sim.seats[0]
        sim.occupants[0].start_distance_nm = seat.lane.length_nm - 0.001
        sim.occupants[0].spawn_elapsed_seconds = 0.0
        frame = sim.tick(1.0)
        # Seat 0's exiting occupant does not appear this tick (no synthetic
        # "eviction" packet -- it simply goes silent).
        assert all(s != 0 for s, _, _ in frame)

    def test_seat_role_persists_across_replacement(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=0.0)
        heli_seat = next(s for s in sim.seats if s.role == "helicopter")
        original_type = sim.occupants[heli_seat.seat_index].aircraft_extra.get("type_designator")
        sim.occupants[heli_seat.seat_index].start_distance_nm = heli_seat.lane.length_nm - 0.001
        sim.occupants[heli_seat.seat_index].spawn_elapsed_seconds = 0.0
        sim.tick(1.0)
        new_type = sim.occupants[heli_seat.seat_index].aircraft_extra.get("type_designator")
        assert new_type == original_type
        assert new_type in _HELICOPTER_TYPES


class TestFleetSimulatorEmergencySquawkMigration:
    def test_initial_emergency_seats_use_reserved_codes(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=0.0)
        assert len(sim.emergency_seats) == min(2, sim.lane_seat_count)
        for code in sim.emergency_seats.values():
            assert code in _EMERGENCY_SQUAWK_CODES

    def test_emergency_count_stays_constant_across_many_exits(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=0.0)
        expected_count = len(sim.emergency_seats)
        for _ in range(50):
            # Force every seat to exit in turn.
            for seat in sim.seats:
                occ = sim.occupants[seat.seat_index]
                occ.start_distance_nm = seat.lane.length_nm - 0.001
                occ.spawn_elapsed_seconds = 0.0
            sim.tick(1.0)
            assert len(sim.emergency_seats) == expected_count

    def test_migration_moves_to_a_different_seat(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=0.0)
        # Pick a seat currently holding an emergency code and force its exit.
        emergency_seat_index = next(iter(sim.emergency_seats))
        seat = sim.seats[emergency_seat_index]
        occ = sim.occupants[emergency_seat_index]
        occ.start_distance_nm = seat.lane.length_nm - 0.001
        occ.spawn_elapsed_seconds = 0.0
        sim.tick(1.0)
        assert emergency_seat_index not in sim.emergency_seats

    def test_occupant_squawk_reflects_emergency_override(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=0.0)
        seat_index, code = next(iter(sim.emergency_seats.items()))
        occupant = sim.occupants[seat_index]
        assert sim.occupant_squawk(seat_index, occupant) == code

    def test_balloon_never_carries_emergency_squawk(self):
        sim = _simulator(aircraft_count=10, ramp_up_seconds=0.0)
        assert sim.occupant_squawk(None, sim.balloon_occupant) == "1200"


class TestFleetSimulatorSecondProcessor:
    def test_secondary_processor_id_is_derived_and_distinct(self):
        sim = _simulator()
        assert sim.secondary_processor_id != sim.processor_id
        assert sim.processor_id in sim.secondary_processor_id


class TestFleetSimulatorWtcOriginDestinationReceiverSources:
    def test_wtc_values_are_canonical(self):
        sim = _simulator(aircraft_count=20, ramp_up_seconds=0.0)
        for occ in sim.occupants.values():
            wtc = occ.aircraft_extra.get("wake_turbulence_category")
            if wtc is not None:
                assert wtc in ("light", "medium", "heavy")

    def test_ifr_occupants_share_one_fixed_origin_destination_pair(self):
        sim = _simulator(aircraft_count=20, ramp_up_seconds=0.0)
        ifr_pairs = {
            (occ.origin["icao_code"], occ.destination["icao_code"])
            for occ in sim.occupants.values()
            if occ.origin is not None
        }
        assert len(ifr_pairs) <= 1

    def test_vfr_occupants_never_carry_origin_or_destination(self):
        sim = _simulator(aircraft_count=20, ramp_up_seconds=0.0)
        for seat in sim.seats:
            occ = sim.occupants[seat.seat_index]
            if seat.lane.flight_rule == "VFR":
                assert occ.origin is None
                assert occ.destination is None

    def test_receiver_sources_randomly_sized(self):
        sim = _simulator(aircraft_count=20, ramp_up_seconds=0.0)
        sizes = {len(occ.receiver_sources) for occ in sim.occupants.values()}
        assert sizes.issubset({1, 2, 3})


class TestBuildPositionPacket:
    def test_shape_matches_the_real_wire_protocol(self):
        packet = build_position_packet(
            "FF0001", "load-gen-1", 123.5,
            {"lat": 1.0, "lon": 2.0, "alt": 3.0, "velocity": 4.0, "hdg": 5.0, "vs": 6.0},
        )
        assert packet == {
            "type": "position",
            "icao_hex": "FF0001",
            "ts": 123.5,
            "processor_id": "load-gen-1",
            "lat": 1.0,
            "lon": 2.0,
            "alt": 3.0,
            "velocity": 4.0,
            "hdg": 5.0,
            "vs": 6.0,
        }

    def test_icao_hex_is_top_level_not_nested(self):
        packet = build_position_packet("FF0001", "load-gen-1", 1.0, {})
        assert packet["icao_hex"] == "FF0001"
        assert "aircraft" not in packet


class TestBuildHeartbeatPacket:
    def test_shape(self):
        assert build_heartbeat_packet("load-gen-1", 100.0) == {
            "type": "heartbeat",
            "processor_id": "load-gen-1",
            "ts": 100.0,
        }


class TestBuildMetadataPacket:
    def test_icao_hex_is_nested_under_aircraft_not_top_level(self):
        packet = build_metadata_packet("FF0001", "LOAD0001", "load-gen-1", "2024-01-01T00:00:00Z")
        assert packet["aircraft"] == {"icao_hex": "FF0001"}
        assert "icao_hex" not in packet

    def test_has_no_ts_field(self):
        packet = build_metadata_packet("FF0001", "LOAD0001", "load-gen-1", "2024-01-01T00:00:00Z")
        assert "ts" not in packet

    def test_last_message_is_present_and_parseable(self):
        from datetime import datetime

        iso = "2024-06-01T12:34:56.789Z"
        packet = build_metadata_packet("FF0001", "LOAD0001", "load-gen-1", iso)
        assert packet["last_message"] == iso
        parsed = datetime.fromisoformat(packet["last_message"].replace("Z", "+00:00"))
        assert parsed.timestamp() > 0

    def test_optional_fields_omitted_when_not_given(self):
        packet = build_metadata_packet("FF0001", "LOAD0001", "load-gen-1", "2024-01-01T00:00:00Z")
        for key in ("operator", "origin", "destination", "squawk", "matched_rules", "receiver_sources"):
            assert key not in packet

    def test_optional_fields_included_when_given(self):
        packet = build_metadata_packet(
            "FF0001", "LOAD0001", "load-gen-1", "2024-01-01T00:00:00Z",
            operator={"airline_designator": "LG1"},
            origin={"icao_code": "ZZ00"},
            destination={"icao_code": "ZZ01"},
            squawk="1200",
            matched_rules=["some-rule"],
            receiver_sources=["1090"],
        )
        assert packet["operator"] == {"airline_designator": "LG1"}
        assert packet["origin"] == {"icao_code": "ZZ00"}
        assert packet["destination"] == {"icao_code": "ZZ01"}
        assert packet["squawk"] == "1200"
        assert packet["matched_rules"] == ["some-rule"]
        assert packet["receiver_sources"] == ["1090"]

    def test_empty_matched_rules_is_treated_as_not_given(self):
        packet = build_metadata_packet(
            "FF0001", "LOAD0001", "load-gen-1", "2024-01-01T00:00:00Z", matched_rules=[]
        )
        assert "matched_rules" not in packet

    def test_aircraft_extra_merges_into_aircraft_dict(self):
        packet = build_metadata_packet(
            "FF0001", "LOAD0001", "load-gen-1", "2024-01-01T00:00:00Z",
            aircraft_extra={"type_designator": "B738", "category": "Land"},
        )
        assert packet["aircraft"] == {"icao_hex": "FF0001", "type_designator": "B738", "category": "Land"}

    def test_no_aircraft_extra_leaves_aircraft_dict_minimal(self):
        packet = build_metadata_packet("FF0001", "LOAD0001", "load-gen-1", "2024-01-01T00:00:00Z")
        assert packet["aircraft"] == {"icao_hex": "FF0001"}
