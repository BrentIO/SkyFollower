"""
Unit tests for map/main.py's GET /api/config endpoint (`get_config`).

Calls the route function directly against a monkeypatched `_cfg`, the same
lightweight, Redis-free convention test_udp_handling.py uses for
`_handle_packet` -- this endpoint only ever reads `_cfg`, so a full
TestClient/live-server setup (test_api.py's heavier, Redis-backed
integration style) would be exercising nothing this doesn't already cover.
"""

from __future__ import annotations

import map.main as map_main


def test_get_config_reports_center_when_both_coordinates_set(monkeypatch):
    monkeypatch.setattr(
        map_main,
        "_cfg",
        {"map_center_latitude": 33.9425, "map_center_longitude": -118.4081},
    )
    assert map_main.get_config() == {
        "center": {"latitude": 33.9425, "longitude": -118.4081}
    }


def test_get_config_reports_null_center_when_unset(monkeypatch):
    """Matches shared/config.py's map_config() default -- both fields
    absent/None when MAP_CENTER_LATITUDE/MAP_CENTER_LONGITUDE are unset."""
    monkeypatch.setattr(
        map_main,
        "_cfg",
        {"map_center_latitude": None, "map_center_longitude": None},
    )
    assert map_main.get_config() == {"center": None}


def test_get_config_reports_null_center_when_only_one_coordinate_set(monkeypatch):
    """A partially-configured center (e.g. an operator who only filled in one
    prompt) must not surface a half-valid marker -- both or neither."""
    monkeypatch.setattr(
        map_main,
        "_cfg",
        {"map_center_latitude": 33.9425, "map_center_longitude": None},
    )
    assert map_main.get_config() == {"center": None}


def test_get_config_tolerates_missing_keys_entirely(monkeypatch):
    """Defensive: even if `_cfg` were built without map_config() (e.g. a
    future refactor), missing keys must not raise -- center is simply null."""
    monkeypatch.setattr(map_main, "_cfg", {})
    assert map_main.get_config() == {"center": None}
