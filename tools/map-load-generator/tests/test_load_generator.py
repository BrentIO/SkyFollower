"""
Tests for tools/map-load-generator's pure synthetic-motion and
packet-construction functions.

A live UDP send isn't unit-testable (no real map instance in this suite),
but everything that decides *what* gets sent -- deterministic aircraft
position at a given tick, and the position/metadata/heartbeat packet dicts
built from it -- has no socket I/O and is covered directly here, matching
tools/traffic-replayer/tests/test_replay.py's existing convention for this
directory: main() itself (argument parsing, opening the real socket) is
intentionally not covered, since it is thin glue with no branching logic of
its own beyond what's already exercised below.
"""

from __future__ import annotations

import importlib.util
import math
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
synthetic_icao_hex = _mod.synthetic_icao_hex
synthetic_ident = _mod.synthetic_ident
synthetic_squawk = _mod.synthetic_squawk
aircraft_position = _mod.aircraft_position
tick_elapsed_seconds = _mod.tick_elapsed_seconds
build_position_packet = _mod.build_position_packet
build_heartbeat_packet = _mod.build_heartbeat_packet
build_metadata_packet = _mod.build_metadata_packet
synthetic_metadata_fields = _mod.synthetic_metadata_fields


_ORBIT_KWARGS = dict(
    aircraft_count=10,
    center_lat=39.8283,
    center_lon=-98.5795,
    radius_nm=15.0,
    orbit_period_seconds=300.0,
    altitude_ft=35000.0,
)


class TestSyntheticIdentity:
    """icao_hex/ident/squawk generators must be deterministic (same index
    always yields the same value) and obviously synthetic/well-formed, so
    re-running the tool reproduces the exact same simulated fleet."""

    def test_icao_hex_is_six_hex_characters(self):
        for i in (0, 1, 15, 16, 255, 4095):
            hexval = synthetic_icao_hex(i)
            assert len(hexval) == 6
            int(hexval, 16)  # raises if not valid hex

    def test_icao_hex_is_deterministic(self):
        assert synthetic_icao_hex(42) == synthetic_icao_hex(42)

    def test_icao_hex_is_unique_per_index(self):
        values = [synthetic_icao_hex(i) for i in range(500)]
        assert len(set(values)) == len(values)

    def test_icao_hex_uses_the_given_prefix(self):
        assert synthetic_icao_hex(1, prefix="AB").startswith("AB")

    def test_ident_is_deterministic_and_distinct(self):
        assert synthetic_ident(3) == synthetic_ident(3)
        assert synthetic_ident(3) != synthetic_ident(4)

    def test_squawk_is_four_octal_digits(self):
        for i in range(200):
            code = synthetic_squawk(i)
            assert len(code) == 4
            assert all(c in "01234567" for c in code)

    def test_squawk_is_deterministic(self):
        assert synthetic_squawk(99) == synthetic_squawk(99)


class TestTickElapsedSeconds:
    def test_scales_by_position_rate(self):
        assert tick_elapsed_seconds(4, 2.0) == 2.0
        assert tick_elapsed_seconds(4, 1.0) == 4.0

    def test_zero_rate_falls_back_to_tick_count(self):
        assert tick_elapsed_seconds(7, 0) == 7.0


class TestAircraftPosition:
    """The synthetic orbit: deterministic, genuinely moving over time, and
    spreading aircraft around the circle rather than stacking them."""

    def test_deterministic_for_the_same_inputs(self):
        a = aircraft_position(0, 10.0, **_ORBIT_KWARGS)
        b = aircraft_position(0, 10.0, **_ORBIT_KWARGS)
        assert a == b

    def test_position_changes_as_elapsed_time_advances(self):
        p0 = aircraft_position(0, 0.0, **_ORBIT_KWARGS)
        p1 = aircraft_position(0, 30.0, **_ORBIT_KWARGS)
        assert (p0["lat"], p0["lon"]) != (p1["lat"], p1["lon"])

    def test_different_aircraft_are_phase_offset_at_the_same_instant(self):
        p0 = aircraft_position(0, 0.0, **_ORBIT_KWARGS)
        p1 = aircraft_position(1, 0.0, **_ORBIT_KWARGS)
        assert (p0["lat"], p0["lon"]) != (p1["lat"], p1["lon"])

    def test_full_orbit_returns_to_the_starting_point(self):
        p0 = aircraft_position(0, 0.0, **_ORBIT_KWARGS)
        period = _ORBIT_KWARGS["orbit_period_seconds"]
        p1 = aircraft_position(0, period, **_ORBIT_KWARGS)
        assert p0["lat"] == p1["lat"]
        assert p0["lon"] == p1["lon"]

    def test_stays_within_radius_of_center(self):
        kwargs = dict(_ORBIT_KWARGS)
        for t in range(0, 300, 17):
            p = aircraft_position(2, float(t), **kwargs)
            # Rough check: displacement in degrees latitude should not
            # exceed radius_nm / 60 by more than a small epsilon.
            max_deg = kwargs["radius_nm"] / 60.0
            assert abs(p["lat"] - kwargs["center_lat"]) <= max_deg + 1e-6

    def test_zero_aircraft_count_does_not_raise(self):
        kwargs = dict(_ORBIT_KWARGS)
        kwargs["aircraft_count"] = 0
        p = aircraft_position(0, 5.0, **kwargs)
        assert isinstance(p["lat"], float)

    def test_returns_all_six_fields(self):
        p = aircraft_position(0, 0.0, **_ORBIT_KWARGS)
        assert set(p.keys()) == {"lat", "lon", "alt", "velocity", "hdg", "vs"}


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
        """metadata packets carry no ts -- map's _extract_timestamp falls
        back to last_message for this packet type."""
        packet = build_metadata_packet("FF0001", "LOAD0001", "load-gen-1", "2024-01-01T00:00:00Z")
        assert "ts" not in packet

    def test_last_message_is_present_and_parseable(self):
        from datetime import datetime

        iso = "2024-06-01T12:34:56.789Z"
        packet = build_metadata_packet("FF0001", "LOAD0001", "load-gen-1", iso)
        assert packet["last_message"] == iso
        # Mirrors map/main.py's _extract_timestamp exactly.
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


class TestSyntheticMetadataFields:
    def test_deterministic(self):
        a = synthetic_metadata_fields(3, None)
        b = synthetic_metadata_fields(3, None)
        assert a == b

    def test_matched_rule_stamped_when_given(self):
        fields = synthetic_metadata_fields(0, "load-test-rule")
        assert fields["matched_rules"] == ["load-test-rule"]

    def test_matched_rules_empty_when_not_given(self):
        fields = synthetic_metadata_fields(0, None)
        assert fields["matched_rules"] == []

    def test_squawk_is_valid_octal(self):
        fields = synthetic_metadata_fields(17, None)
        assert all(c in "01234567" for c in fields["squawk"])

    def test_receiver_source_is_a_real_source_tag(self):
        for i in range(10):
            fields = synthetic_metadata_fields(i, None)
            assert fields["receiver_sources"][0] in ("1090", "978", "EXTERNAL")
