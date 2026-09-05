"""Tests for shared/flight_path.py."""

from __future__ import annotations

import pytest

from shared.flight_path import _interpolate_altitudes, _interpolate_speeds, _parse_epoch_seconds, build_flight_path


# ---------------------------------------------------------------------------
# Altitude interpolation
# ---------------------------------------------------------------------------

class TestInterpolateAltitudes:
    def test_no_nones_unchanged(self):
        positions = [
            {"altitude": 1000},
            {"altitude": 2000},
            {"altitude": 3000},
        ]
        result = _interpolate_altitudes(positions)
        assert result == [1000, 2000, 3000]

    def test_single_missing_middle_interpolated(self):
        positions = [
            {"altitude": 0},
            {"altitude": None},
            {"altitude": 4000},
        ]
        result = _interpolate_altitudes(positions)
        assert result[1] == 2000

    def test_multiple_missing_middle_interpolated(self):
        positions = [
            {"altitude": 0},
            {"altitude": None},
            {"altitude": None},
            {"altitude": 3000},
        ]
        result = _interpolate_altitudes(positions)
        assert result[1] == 1000
        assert result[2] == 2000

    def test_missing_at_start_stays_none(self):
        # No preceding known altitude — can't interpolate
        positions = [
            {"altitude": None},
            {"altitude": 5000},
        ]
        result = _interpolate_altitudes(positions)
        assert result[0] is None
        assert result[1] == 5000

    def test_missing_at_end_stays_none(self):
        positions = [
            {"altitude": 5000},
            {"altitude": None},
        ]
        result = _interpolate_altitudes(positions)
        assert result[0] == 5000
        assert result[1] is None

    def test_all_none_stays_none(self):
        positions = [{"altitude": None}, {"altitude": None}]
        result = _interpolate_altitudes(positions)
        assert result == [None, None]

    def test_rounding(self):
        # 0 -> None -> 3 should give 1 or 2 (rounded)
        positions = [
            {"altitude": 0},
            {"altitude": None},
            {"altitude": 3},
        ]
        result = _interpolate_altitudes(positions)
        assert isinstance(result[1], int)


# ---------------------------------------------------------------------------
# GeoJSON builder
# ---------------------------------------------------------------------------

def _positions_3d():
    return [
        {"latitude": 33.6367, "longitude": -84.4281, "altitude": 1000, "timestamp": "2026-07-31T12:00:00+00:00"},
        {"latitude": 34.0, "longitude": -85.0, "altitude": 2000, "timestamp": "2026-07-31T12:00:10+00:00"},
        {"latitude": 35.0, "longitude": -86.0, "altitude": 3000, "timestamp": "2026-07-31T12:00:20+00:00"},
    ]


class TestBuildFlightPath:
    def test_returns_none_for_zero_positions(self):
        assert build_flight_path([]) is None

    def test_returns_none_for_one_position(self):
        positions = [{"latitude": 33.0, "longitude": -84.0, "altitude": 1000}]
        assert build_flight_path(positions) is None

    def test_valid_feature_structure(self):
        feature = build_flight_path(_positions_3d())
        assert feature is not None
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] == "LineString"
        assert "coordinates" in feature["geometry"]
        # coordTimes is always present; coordSpeeds only when velocities is
        # explicitly passed (not the case here) -- see TestCoordTimes/TestCoordSpeeds.
        assert set(feature["properties"]) == {"coordTimes"}

    def test_coordinates_have_correct_lon_lat_order(self):
        feature = build_flight_path(_positions_3d())
        coords = feature["geometry"]["coordinates"]
        # GeoJSON: [longitude, latitude, altitude]
        assert coords[0][0] == pytest.approx(-84.4281)
        assert coords[0][1] == pytest.approx(33.6367)

    def test_3d_coordinates_when_altitude_present(self):
        feature = build_flight_path(_positions_3d())
        coords = feature["geometry"]["coordinates"]
        for c in coords:
            assert len(c) == 3

    def test_2d_coordinates_when_altitude_none_and_uninterpolatable(self):
        # Two positions, both without altitude and no surrounding known alt
        positions = [
            {"latitude": 33.0, "longitude": -84.0, "altitude": None},
            {"latitude": 34.0, "longitude": -85.0, "altitude": None},
        ]
        feature = build_flight_path(positions)
        coords = feature["geometry"]["coordinates"]
        for c in coords:
            assert len(c) == 2

    def test_mixed_altitude_interpolated(self):
        # Middle position altitude=None, should be interpolated
        positions = [
            {"latitude": 33.0, "longitude": -84.0, "altitude": 0},
            {"latitude": 34.0, "longitude": -85.0, "altitude": None},
            {"latitude": 35.0, "longitude": -86.0, "altitude": 4000},
        ]
        feature = build_flight_path(positions)
        coords = feature["geometry"]["coordinates"]
        # Middle coord should be 3D with interpolated altitude
        assert len(coords[1]) == 3


# ---------------------------------------------------------------------------
# coordTimes / coordSpeeds (tar1090-style trace points, #1441)
# ---------------------------------------------------------------------------

class TestParseEpochSeconds:
    def test_iso_with_offset(self):
        assert _parse_epoch_seconds("2026-07-31T12:00:00+00:00") == pytest.approx(1785499200.0)

    def test_iso_with_z_suffix(self):
        # datetime.fromisoformat only accepts 'Z' on Python >= 3.11 --
        # _parse_epoch_seconds normalises it either way.
        assert _parse_epoch_seconds("2026-07-31T12:00:00Z") == _parse_epoch_seconds("2026-07-31T12:00:00+00:00")

    def test_numeric_epoch_passthrough(self):
        assert _parse_epoch_seconds(1785499200) == 1785499200.0

    def test_none_stays_none(self):
        assert _parse_epoch_seconds(None) is None

    def test_unparseable_string_returns_none(self):
        assert _parse_epoch_seconds("not-a-timestamp") is None


class TestCoordTimes:
    def test_always_present_regardless_of_velocities(self):
        feature = build_flight_path(_positions_3d())
        assert "coordTimes" in feature["properties"]
        assert len(feature["properties"]["coordTimes"]) == 3

    def test_epoch_seconds_ints_in_position_order(self):
        feature = build_flight_path(_positions_3d())
        times = feature["properties"]["coordTimes"]
        assert times == [1785499200, 1785499210, 1785499220]
        assert all(isinstance(t, int) for t in times)

    def test_missing_timestamp_is_none_not_a_crash(self):
        positions = [
            {"latitude": 33.0, "longitude": -84.0, "altitude": 0},
            {"latitude": 34.0, "longitude": -85.0, "altitude": 1000, "timestamp": "2026-07-31T12:00:10+00:00"},
        ]
        feature = build_flight_path(positions)
        assert feature["properties"]["coordTimes"] == [None, 1785499210]


class TestCoordSpeeds:
    def test_absent_when_velocities_not_passed(self):
        feature = build_flight_path(_positions_3d())
        assert "coordSpeeds" not in feature["properties"]

    def test_exact_timestamp_match(self):
        velocities = [{"timestamp": "2026-07-31T12:00:10+00:00", "velocity": 450.0}]
        feature = build_flight_path(_positions_3d(), velocities)
        assert feature["properties"]["coordSpeeds"][1] == 450

    def test_linear_interpolation_between_two_samples(self):
        velocities = [
            {"timestamp": "2026-07-31T12:00:00+00:00", "velocity": 400.0},
            {"timestamp": "2026-07-31T12:00:20+00:00", "velocity": 420.0},
        ]
        feature = build_flight_path(_positions_3d(), velocities)
        # Middle position is exactly halfway between the two velocity samples.
        assert feature["properties"]["coordSpeeds"][1] == 410

    def test_nearest_match_extrapolation_before_first_sample(self):
        velocities = [
            {"timestamp": "2026-07-31T12:00:10+00:00", "velocity": 450.0},
            {"timestamp": "2026-07-31T12:00:20+00:00", "velocity": 460.0},
        ]
        feature = build_flight_path(_positions_3d(), velocities)
        # First position (12:00:00) is before any velocity sample.
        assert feature["properties"]["coordSpeeds"][0] == 450

    def test_nearest_match_extrapolation_after_last_sample(self):
        velocities = [{"timestamp": "2026-07-31T12:00:00+00:00", "velocity": 400.0}]
        feature = build_flight_path(_positions_3d(), velocities)
        # Every position is at or after the single velocity sample.
        assert feature["properties"]["coordSpeeds"] == [400, 400, 400]

    def test_no_velocity_readings_all_none(self):
        feature = build_flight_path(_positions_3d(), [])
        assert feature["properties"]["coordSpeeds"] == [None, None, None]

    def test_velocity_reading_with_none_speed_is_ignored(self):
        # A velocity report with a heading/vertical_speed but no velocity
        # value (see shared/models.py's Velocity) must not become a
        # zero/garbage sample.
        velocities = [
            {"timestamp": "2026-07-31T12:00:00+00:00", "velocity": None},
            {"timestamp": "2026-07-31T12:00:20+00:00", "velocity": 420.0},
        ]
        feature = build_flight_path(_positions_3d(), velocities)
        # Only one usable sample -- nearest-match for everything.
        assert feature["properties"]["coordSpeeds"] == [420, 420, 420]

    def test_unparseable_position_timestamp_gives_none_speed(self):
        positions = [
            {"latitude": 33.0, "longitude": -84.0, "altitude": 0},
            {"latitude": 34.0, "longitude": -85.0, "altitude": 1000, "timestamp": "2026-07-31T12:00:10+00:00"},
        ]
        velocities = [{"timestamp": "2026-07-31T12:00:10+00:00", "velocity": 450.0}]
        feature = build_flight_path(positions, velocities)
        assert feature["properties"]["coordSpeeds"] == [None, 450]


class TestInterpolateSpeedsDirect:
    """Direct unit tests of _interpolate_speeds, independent of build_flight_path's timestamp parsing."""

    def test_empty_velocities_returns_all_none(self):
        assert _interpolate_speeds([100.0, 200.0], []) == [None, None]

    def test_unsorted_velocities_still_correct(self):
        # samples arrive out of order; _interpolate_speeds must sort them.
        velocities = [
            {"timestamp": 20.0, "velocity": 420.0},
            {"timestamp": 0.0, "velocity": 400.0},
        ]
        assert _interpolate_speeds([10.0], velocities) == [410]
