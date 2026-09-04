"""Tests for shared/flight_path.py."""

from __future__ import annotations

import pytest

from shared.flight_path import _interpolate_altitudes, build_flight_path


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
        {"latitude": 33.6367, "longitude": -84.4281, "altitude": 1000},
        {"latitude": 34.0, "longitude": -85.0, "altitude": 2000},
        {"latitude": 35.0, "longitude": -86.0, "altitude": 3000},
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
        assert feature["properties"] == {}

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
        assert coords[1][2] == 2000
