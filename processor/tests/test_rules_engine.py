"""
Tests for processor/rules_engine.py.

Uses a lightweight FlightStub instead of the full Flight class so this test
module has no dependency on the processor's main.py or SQLite.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock

import pytest

from processor.rules_engine import RulesEngine


# ---------------------------------------------------------------------------
# Minimal flight and sub-object stubs
# ---------------------------------------------------------------------------

@dataclass
class PosStub:
    timestamp: float = 0.0
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude: Optional[int] = None


@dataclass
class VelStub:
    timestamp: float = 0.0
    velocity: Optional[float] = None
    heading: Optional[float] = None
    vertical_speed: Optional[int] = None


@dataclass
class FlightStub:
    icao_hex: str = "A8AE7F"
    ident: str = ""
    squawk: str = ""
    aircraft: dict = field(default_factory=dict)
    operator: dict = field(default_factory=dict)
    positions: list = field(default_factory=list)
    velocities: list = field(default_factory=list)
    matched_rules: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _engine_with_rules(rules: list, areas: dict | None = None) -> RulesEngine:
    engine = _bare_engine()
    # Areas must be loaded before rules so that area condition validation can
    # check that the referenced area name exists.
    if areas:
        ok2 = engine.load_areas_json(json.dumps(areas))
        assert ok2, "Failed to load areas in test setup"
    ok = engine.load_rules_json(json.dumps(rules))
    assert ok, "Failed to load rules in test setup"
    return engine


def _bare_engine() -> RulesEngine:
    redis = MagicMock()
    return RulesEngine(redis)


def _rule(identifier: str, conditions: list, enabled: bool = True) -> dict:
    return {
        "name": identifier,
        "identifier": identifier,
        "enabled": enabled,
        "conditions": conditions,
    }


def _cond(ctype: str, operator: str, value) -> dict:
    return {"type": ctype, "operator": operator, "value": value}


# ---------------------------------------------------------------------------
# Rules loading
# ---------------------------------------------------------------------------

class TestLoadRules:
    def test_empty_array_loads(self):
        engine = _bare_engine()
        assert engine.load_rules_json("[]") is True

    def test_invalid_json_rejected(self):
        engine = _bare_engine()
        assert engine.load_rules_json("{not json}") is False

    def test_not_array_rejected(self):
        engine = _bare_engine()
        assert engine.load_rules_json('{"key": "value"}') is False

    def test_disabled_rule_skipped(self):
        engine = _engine_with_rules([_rule("r1", [_cond("altitude", "maximum", "5000")], enabled=False)])
        assert engine._rules == []

    def test_duplicate_identifier_rejected(self):
        rules = [
            _rule("dup", [_cond("altitude", "maximum", "5000")]),
            _rule("dup", [_cond("altitude", "minimum", "1000")]),
        ]
        engine = _bare_engine()
        assert engine.load_rules_json(json.dumps(rules)) is False

    def test_previous_rules_kept_on_invalid_reload(self):
        engine = _engine_with_rules([_rule("r1", [_cond("altitude", "maximum", "5000")])])
        engine.load_rules_json("{bad json}")
        assert len(engine._rules) == 1

    def test_conditions_sorted_by_priority(self):
        rules = [_rule("r1", [
            _cond("area", "equals", "NOWHERE"),  # priority 1000 — but area won't exist
            _cond("altitude", "maximum", "5000"),  # priority 100
        ])]
        # area validation will fail if areas not loaded; test sorting with two cheap conditions
        rules2 = [_rule("r1", [
            _cond("ident", "equals", "DAL1"),     # priority 305
            _cond("altitude", "maximum", "5000"),  # priority 100
        ])]
        engine = _engine_with_rules(rules2)
        assert engine._rules[0]["conditions"][0]["type"] == "altitude"
        assert engine._rules[0]["conditions"][1]["type"] == "ident"


# ---------------------------------------------------------------------------
# Condition type validation
# ---------------------------------------------------------------------------

class TestConditionValidation:
    def test_altitude_rejects_equals(self):
        engine = _bare_engine()
        assert engine.load_rules_json(json.dumps([
            _rule("r", [_cond("altitude", "equals", "5000")])
        ])) is False

    def test_velocity_rejects_equals(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("velocity", "equals", "300")])
        ])) is False

    def test_heading_rejects_non_equals(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("heading", "minimum", "090,180")])
        ])) is False

    def test_heading_rejects_single_value(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("heading", "equals", "090")])
        ])) is False

    def test_heading_rejects_out_of_range(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("heading", "equals", "090,400")])
        ])) is False

    def test_squawk_must_be_4_digits(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("squawk", "equals", "123")])
        ])) is False

    def test_aircraft_icao_hex_must_be_6_chars(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("aircraft_icao_hex", "equals", "ABC")])
        ])) is False

    def test_aircraft_type_must_be_4_chars(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("aircraft_type_designator", "equals", "B7")])
        ])) is False

    def test_operator_designator_must_be_3_chars(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("operator_airline_designator", "equals", "DA")])
        ])) is False

    def test_wake_turbulence_category_rejects_unknown(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("wake_turbulence_category", "equals", "jumbo")])
        ])) is False

    def test_matched_rules_must_be_list(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("matched_rules", "in_list", "r1")])
        ])) is False

    def test_matched_rules_rejects_equals(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("matched_rules", "equals", ["r1"])])
        ])) is False

    def test_date_yyyy_mm_dd_valid(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("date", "equals", "2026-12-24")])
        ])) is True

    def test_date_datetime_z_valid(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("date", "minimum", "2026-12-24T22:00Z")])
        ])) is True

    def test_date_datetime_without_z_rejected(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("date", "minimum", "2026-12-24T22:00")])
        ])) is False

    def test_date_invalid_format_rejected(self):
        assert _bare_engine().load_rules_json(json.dumps([
            _rule("r", [_cond("date", "equals", "24/12/2026")])
        ])) is False


# ---------------------------------------------------------------------------
# Altitude evaluation
# ---------------------------------------------------------------------------

class TestAltitudeEval:
    def _engine(self):
        return _engine_with_rules([
            _rule("below_10k", [_cond("altitude", "maximum", "10000")]),
            _rule("above_5k",  [_cond("altitude", "minimum",  "5000")]),
        ])

    def test_below_max(self):
        e = self._engine()
        f = FlightStub(positions=[PosStub(altitude=8000)])
        matched = [r["identifier"] for r in e.evaluate(f)]
        assert "below_10k" in matched
        assert "above_5k" in matched

    def test_above_max_no_match(self):
        e = self._engine()
        f = FlightStub(positions=[PosStub(altitude=15000)])
        assert not any(r["identifier"] == "below_10k" for r in e.evaluate(f))

    def test_no_position_no_match(self):
        e = self._engine()
        f = FlightStub()
        assert e.evaluate(f) == []

    def test_none_altitude_no_match(self):
        e = self._engine()
        f = FlightStub(positions=[PosStub(altitude=None)])
        assert e.evaluate(f) == []


# ---------------------------------------------------------------------------
# Heading evaluation — including wrap-around
# ---------------------------------------------------------------------------

class TestHeadingEval:
    def _engine(self):
        return _engine_with_rules([
            _rule("northbound", [_cond("heading", "equals", "340,020")]),
            _rule("eastbound",  [_cond("heading", "equals", "045,135")]),
        ])

    def test_northbound_at_000(self):
        e = self._engine()
        f = FlightStub(velocities=[VelStub(heading=0)])
        matched = [r["identifier"] for r in e.evaluate(f)]
        assert "northbound" in matched

    def test_northbound_at_355(self):
        e = self._engine()
        f = FlightStub(velocities=[VelStub(heading=355)])
        assert "northbound" in [r["identifier"] for r in e.evaluate(f)]

    def test_northbound_at_020_boundary(self):
        e = self._engine()
        f = FlightStub(velocities=[VelStub(heading=20)])
        assert "northbound" in [r["identifier"] for r in e.evaluate(f)]

    def test_northbound_at_021_no_match(self):
        e = self._engine()
        f = FlightStub(velocities=[VelStub(heading=21)])
        assert "northbound" not in [r["identifier"] for r in e.evaluate(f)]

    def test_eastbound_at_090(self):
        e = self._engine()
        f = FlightStub(velocities=[VelStub(heading=90)])
        assert "eastbound" in [r["identifier"] for r in e.evaluate(f)]

    def test_southbound_no_match(self):
        e = self._engine()
        f = FlightStub(velocities=[VelStub(heading=180)])
        assert e.evaluate(f) == []


# ---------------------------------------------------------------------------
# Vertical speed — sign-aware
# ---------------------------------------------------------------------------

class TestVerticalSpeedEval:
    """
    Vertical speed semantics for negative thresholds (legacy-compatible):
      minimum: N  (N < 0)  → vs < 0 AND vs <= N  ("descending at least |N| fpm")
      maximum: N  (N < 0)  → vs < 0 AND vs >= N  ("descending at most |N| fpm")
    So "minimum: -100" matches -500 (steeper); "maximum: -100" matches -50 (shallower).
    """

    def _engine(self):
        return _engine_with_rules([
            _rule("climbing",   [_cond("vertical_speed", "minimum", "500")]),
            _rule("steep_desc", [_cond("vertical_speed", "minimum", "-100")]),
            _rule("mild_desc",  [_cond("vertical_speed", "maximum", "-100")]),
        ])

    def test_climbing_1000_fpm(self):
        e = self._engine()
        f = FlightStub(velocities=[VelStub(vertical_speed=1000)])
        assert "climbing" in [r["identifier"] for r in e.evaluate(f)]

    def test_level_no_climb_match(self):
        e = self._engine()
        f = FlightStub(velocities=[VelStub(vertical_speed=0)])
        assert "climbing" not in [r["identifier"] for r in e.evaluate(f)]

    def test_steep_descent_matches_minimum_negative(self):
        # minimum: -100 → vs <= -100; -500 qualifies
        e = self._engine()
        f = FlightStub(velocities=[VelStub(vertical_speed=-500)])
        assert "steep_desc" in [r["identifier"] for r in e.evaluate(f)]

    def test_shallow_descent_does_not_match_minimum_negative(self):
        # minimum: -100 → vs <= -100; -50 does not qualify
        e = self._engine()
        f = FlightStub(velocities=[VelStub(vertical_speed=-50)])
        assert "steep_desc" not in [r["identifier"] for r in e.evaluate(f)]

    def test_mild_descent_matches_maximum_negative(self):
        # maximum: -100 → vs >= -100 (and vs < 0); -50 qualifies
        e = self._engine()
        f = FlightStub(velocities=[VelStub(vertical_speed=-50)])
        assert "mild_desc" in [r["identifier"] for r in e.evaluate(f)]

    def test_steep_descent_does_not_match_maximum_negative(self):
        # maximum: -100 → vs >= -100; -500 does not qualify
        e = self._engine()
        f = FlightStub(velocities=[VelStub(vertical_speed=-500)])
        assert "mild_desc" not in [r["identifier"] for r in e.evaluate(f)]

    def test_positive_vs_does_not_match_negative_threshold_rule(self):
        e = self._engine()
        f = FlightStub(velocities=[VelStub(vertical_speed=200)])
        assert "steep_desc" not in [r["identifier"] for r in e.evaluate(f)]
        assert "mild_desc" not in [r["identifier"] for r in e.evaluate(f)]


# ---------------------------------------------------------------------------
# Date condition
# ---------------------------------------------------------------------------

class TestDateEval:
    def test_date_minimum_past_date_matches(self):
        engine = _engine_with_rules([_rule("r", [_cond("date", "minimum", "2000-01-01")])])
        f = FlightStub()
        assert engine.evaluate(f) != []

    def test_date_maximum_far_future_no_match(self):
        engine = _engine_with_rules([_rule("r", [_cond("date", "maximum", "2000-01-01")])])
        f = FlightStub()
        assert engine.evaluate(f) == []

    def test_datetime_minimum_past_matches(self):
        engine = _engine_with_rules([_rule("r", [_cond("date", "minimum", "2000-01-01T00:00Z")])])
        f = FlightStub()
        assert engine.evaluate(f) != []

    def test_datetime_maximum_far_future_no_match(self):
        engine = _engine_with_rules([_rule("r", [_cond("date", "maximum", "2000-01-01T00:00Z")])])
        f = FlightStub()
        assert engine.evaluate(f) == []


# ---------------------------------------------------------------------------
# Other condition types
# ---------------------------------------------------------------------------

class TestIdentEval:
    def test_match(self):
        e = _engine_with_rules([_rule("r", [_cond("ident", "equals", "DAL659")])])
        assert e.evaluate(FlightStub(ident="DAL659")) != []

    def test_case_insensitive(self):
        e = _engine_with_rules([_rule("r", [_cond("ident", "equals", "DAL659")])])
        assert e.evaluate(FlightStub(ident="dal659")) != []

    def test_no_match(self):
        e = _engine_with_rules([_rule("r", [_cond("ident", "equals", "DAL659")])])
        assert e.evaluate(FlightStub(ident="UAL1")) == []


class TestMilitaryEval:
    def test_military_true(self):
        e = _engine_with_rules([_rule("r", [_cond("military", "equals", "true")])])
        f = FlightStub(aircraft={"military": True})
        assert e.evaluate(f) != []

    def test_military_false_no_match_for_true_rule(self):
        e = _engine_with_rules([_rule("r", [_cond("military", "equals", "true")])])
        f = FlightStub(aircraft={"military": False})
        assert e.evaluate(f) == []

    def test_missing_military_no_match(self):
        e = _engine_with_rules([_rule("r", [_cond("military", "equals", "true")])])
        assert e.evaluate(FlightStub()) == []


class TestOperatorEval:
    def test_match(self):
        e = _engine_with_rules([_rule("r", [_cond("operator_airline_designator", "equals", "DAL")])])
        f = FlightStub(operator={"airline_designator": "DAL"})
        assert e.evaluate(f) != []

    def test_case_insensitive_storage(self):
        # value is normalised to upper at load time
        e = _engine_with_rules([_rule("r", [_cond("operator_airline_designator", "equals", "dal")])])
        f = FlightStub(operator={"airline_designator": "DAL"})
        assert e.evaluate(f) != []


class TestMatchedRulesEval:
    def test_in_list_match(self):
        e = _engine_with_rules([_rule("r2", [_cond("matched_rules", "in_list", ["r1"])])])
        f = FlightStub(matched_rules=["r1"])
        assert e.evaluate(f) != []

    def test_in_list_no_match(self):
        e = _engine_with_rules([_rule("r2", [_cond("matched_rules", "in_list", ["r1"])])])
        f = FlightStub(matched_rules=[])
        assert e.evaluate(f) == []

    def test_not_in_list_match(self):
        e = _engine_with_rules([_rule("r2", [_cond("matched_rules", "not_in_list", ["r1"])])])
        f = FlightStub(matched_rules=[])
        assert e.evaluate(f) != []

    def test_not_in_list_no_match(self):
        e = _engine_with_rules([_rule("r2", [_cond("matched_rules", "not_in_list", ["r1"])])])
        f = FlightStub(matched_rules=["r1"])
        assert e.evaluate(f) == []


class TestRuleFiresOnce:
    def test_already_matched_rule_skipped(self):
        e = _engine_with_rules([_rule("r1", [_cond("altitude", "maximum", "10000")])])
        f = FlightStub(
            positions=[PosStub(altitude=5000)],
            matched_rules=["r1"],  # already matched
        )
        assert e.evaluate(f) == []


class TestWakeTurbulenceEval:
    def test_heavy_match(self):
        e = _engine_with_rules([_rule("r", [_cond("wake_turbulence_category", "equals", "Heavy")])])
        f = FlightStub(aircraft={"wake_turbulence_category": "Heavy"})
        assert e.evaluate(f) != []

    def test_case_insensitive(self):
        e = _engine_with_rules([_rule("r", [_cond("wake_turbulence_category", "equals", "heavy")])])
        f = FlightStub(aircraft={"wake_turbulence_category": "Heavy"})
        assert e.evaluate(f) != []


# ---------------------------------------------------------------------------
# Area evaluation
# ---------------------------------------------------------------------------

LONG_ISLAND = {
    "type": "FeatureCollection",
    "features": [{
        "type": "Feature",
        "properties": {"name": "LI"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-73.8, 40.83], [-73.97, 40.73], [-74.04, 40.55],
                [-73.77, 40.54], [-73.25, 40.59], [-72.71, 40.74],
                [-71.86, 40.97], [-71.81, 41.04], [-71.90, 41.15],
                [-72.12, 41.22], [-72.36, 41.17], [-72.70, 41.04],
                [-72.84, 41.05], [-73.20, 40.99], [-73.55, 40.95],
                [-73.8, 40.83],
            ]],
        },
    }],
}


class TestAreaEval:
    def _engine(self):
        return _engine_with_rules(
            [_rule("over_li", [_cond("area", "equals", "LI")])],
            areas=LONG_ISLAND,
        )

    def test_point_inside(self):
        e = self._engine()
        # Middle of Long Island
        f = FlightStub(positions=[PosStub(latitude=40.8, longitude=-73.0)])
        assert e.evaluate(f) != []

    def test_point_outside(self):
        e = self._engine()
        # Over New Jersey
        f = FlightStub(positions=[PosStub(latitude=40.7, longitude=-74.5)])
        assert e.evaluate(f) == []

    def test_no_position_no_match(self):
        e = self._engine()
        assert e.evaluate(FlightStub()) == []

    def test_bounding_box_fast_path(self):
        e = self._engine()
        # Way outside — should short-circuit at bounding box
        f = FlightStub(positions=[PosStub(latitude=51.5, longitude=-0.1)])
        assert e.evaluate(f) == []


# ---------------------------------------------------------------------------
# Reload behaviour
# ---------------------------------------------------------------------------

class TestReloadIfChanged:
    def test_reloads_when_version_changes(self):
        redis = MagicMock()
        engine = RulesEngine(redis)

        rules_json = json.dumps([_rule("r1", [_cond("altitude", "maximum", "5000")])])
        areas_json = json.dumps({"type": "FeatureCollection", "features": []})

        redis.get.side_effect = lambda key: {
            "config:rules:version": "v1",
            "config:areas:version": "a1",
            "config:rules": rules_json,
            "config:areas": areas_json,
        }.get(key)

        engine.reload_if_changed()
        assert len(engine._rules) == 1
        assert engine._rules_version == "v1"

    def test_no_reload_when_version_unchanged(self):
        redis = MagicMock()
        engine = RulesEngine(redis)
        engine._rules_version = "v1"
        engine._areas_version = "a1"

        redis.get.side_effect = lambda key: {
            "config:rules:version": "v1",
            "config:areas:version": "a1",
        }.get(key)

        reloaded = engine.reload_if_changed()
        assert reloaded is False

    def test_redis_error_does_not_crash(self):
        redis = MagicMock()
        redis.get.side_effect = ConnectionError("Redis down")
        engine = RulesEngine(redis)
        engine.reload_if_changed()  # should not raise
