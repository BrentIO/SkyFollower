"""Unit tests for map/geo.py -- bearing + great-circle distance."""

from __future__ import annotations

import math

import pytest

from map.geo import EARTH_RADIUS_NM, great_circle_nm, initial_bearing


def test_bearing_cardinal_directions():
    assert initial_bearing(0, 0, 1, 0) == 0.0        # due north
    assert round(initial_bearing(0, 0, 0, 1), 6) == 90.0   # due east
    assert round(initial_bearing(0, 0, -1, 0), 6) == 180.0  # due south
    assert round(initial_bearing(0, 0, 0, -1), 6) == 270.0  # due west


def test_bearing_is_normalised_to_0_360():
    b = initial_bearing(51.5, -0.1, 40.7, -74.0)  # London -> New York, roughly WNW
    assert 0.0 <= b < 360.0
    assert 280 < b < 300


def test_great_circle_one_degree_of_latitude_is_about_60nm():
    # ~60 nm per degree of latitude (exactly 60.04 for this mean radius).
    assert great_circle_nm(0, 0, 1, 0) == pytest.approx(60.04, abs=0.05)
    assert great_circle_nm(45, 10, 46, 10) == pytest.approx(60.04, abs=0.05)  # longitude-independent


def test_great_circle_zero_distance():
    assert great_circle_nm(33.9, -118.4, 33.9, -118.4) == 0.0


def test_great_circle_antipodal_is_half_circumference():
    half = math.pi * EARTH_RADIUS_NM
    assert abs(great_circle_nm(0, 0, 0, 180) - half) < 1e-6
