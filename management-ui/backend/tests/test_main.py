"""
Tests for management-ui/backend/main.py.

Redis is faked with a tiny in-memory dict (FakeRedis below) rather than a
MagicMock, since the per-item CRUD endpoints are read-modify-write against
the full stored array/collection -- a static MagicMock return value can't
reflect a POST/PUT/DELETE's effect on a subsequent GET within the same test.

main.py is loaded directly by file path rather than via a normal package
import -- the hyphen in "management-ui" isn't a valid Python identifier, so
it can't be imported as management_ui.backend.main the way "shared" or "ui"
(no hyphen) could be.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import sys
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

_BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location("management_ui_main", os.path.join(_BACKEND_DIR, "main.py"))
ui_main = importlib.util.module_from_spec(_spec)
sys.modules["management_ui_main"] = ui_main
_spec.loader.exec_module(ui_main)


def _deep_merge(base: dict, update: dict) -> None:
    for k, v in update.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v


# Redis key prefix each RediSearch index covers -- lets the fake's search()
# just scan FakeRedis.store for matching JSON docs instead of needing a
# separate, parallel index data structure kept in sync by hand.
_FAKE_INDEX_PREFIXES = {
    "idx:aircraft:mictronics": "aircraft:mictronics:",
    "idx:aircraft:registry": "aircraft:registry:",
    "idx:airport": "airport:",
}


class _FakeDoc:
    def __init__(self, id: str):
        self.id = id


class _FakeSearchResult:
    def __init__(self, docs: list[_FakeDoc]):
        self.docs = docs


class _FakeFt:
    """Minimal stand-in for redis.Redis.ft(index) -- only supports the
    single-tag exact-match `@field:{value}` queries main.py's _search_one()
    actually issues, resolved by scanning FakeRedis.store."""

    def __init__(self, redis: "FakeRedis", index: str):
        self._redis = redis
        self._index = index

    def search(self, query):
        match = re.match(r"@(\w+):\{(.+)\}$", query.query_string())
        field, raw_value = match.group(1), match.group(2)
        value = re.sub(r"\\(.)", r"\1", raw_value)  # undo _escape_tag's backslash-escaping
        prefix = _FAKE_INDEX_PREFIXES[self._index]
        matches = [
            _FakeDoc(key)
            for key, raw in self._redis.store.items()
            if key.startswith(prefix) and json.loads(raw).get(field) == value
        ]
        return _FakeSearchResult(matches[:1])


class _FakeJson:
    """Minimal stand-in for redis.Redis.json() -- operator:/airport: keys are
    real RedisJSON documents (a plain GET raises WRONGTYPE against them,
    verified against a live Redis Stack instance), so main.py reads them via
    .json().get() instead. Real redis-py returns the decoded dict directly,
    which this matches by json.loads()-ing whatever FakeRedis.store holds."""

    def __init__(self, redis: "FakeRedis"):
        self._redis = redis

    def get(self, key: str) -> dict | None:
        raw = self._redis.store.get(key)
        return json.loads(raw) if raw else None


class FakeRedis:
    """
    Minimal in-memory stand-in for redis.Redis's get/set/script_load/evalsha/
    ft().search. evalsha is special-cased per script body (there are only
    ever two: merge_aircraft.lua and route_airports.lua, distinguished by a
    substring unique to each) rather than a real Lua interpreter --
    replicating just enough of each script's documented behavior for the
    reference-data lookup endpoints' own tests.
    """

    def __init__(self):
        self.store: dict[str, str] = {}
        self.get_error: Exception | None = None
        self.set_error: Exception | None = None
        self._scripts: dict[str, str] = {}

    def get(self, key):
        if self.get_error:
            raise self.get_error
        return self.store.get(key)

    def set(self, key, value, **kwargs):
        if self.set_error:
            raise self.set_error
        self.store[key] = value

    def delete(self, key):
        self.store.pop(key, None)

    def sadd(self, key, *members):
        """No archive_search:* records ever exist in these rules/areas
        tests -- this only needs to satisfy create_archive_search-style
        callers, none of which this file's tests actually exercise."""

    def srem(self, key, *members):
        pass

    def smembers(self, key):
        """Always empty here -- this only needs to satisfy lifespan()'s
        unconditional startup reconciliation sweep (see
        _reconcile_stuck_archive_searches), which no test in this file
        depends on finding anything."""
        return set()

    def json(self):
        return _FakeJson(self)

    def script_load(self, script: str) -> str:
        sha = hashlib.sha1(script.encode()).hexdigest()
        self._scripts[sha] = script
        return sha

    def evalsha(self, sha: str, numkeys: int, *args):
        script = self._scripts[sha]
        if "aircraft:mictronics:" in script:
            return self._eval_merge_aircraft(args[0])
        if "route:" in script:
            return self._eval_route_airports(args[0])
        raise NotImplementedError("FakeRedis.evalsha: unrecognised script")

    def _eval_merge_aircraft(self, icao_hex: str) -> str | None:
        icao_hex = icao_hex.upper()
        raws = [
            self.store.get(f"aircraft:mictronics:{icao_hex}"),
            self.store.get(f"aircraft:registry:{icao_hex}"),
            self.store.get(f"aircraft:livery:{icao_hex}"),
        ]
        if not any(raws):
            return None
        result: dict = {}
        sources = []
        for raw in raws:
            if not raw:
                continue
            doc = json.loads(raw)
            source = doc.pop("source", None)
            if source:
                sources.append(source)
            _deep_merge(result, doc)
        if sources:
            result["data_sources"] = sources
        return json.dumps(result)

    def _eval_route_airports(self, ident: str) -> str:
        route_raw = self.store.get(f"route:{ident.upper()}")
        if not route_raw:
            return "[]"
        airports = []
        for code in route_raw.split("-"):
            raw = self.store.get(f"airport:{code.upper()}")
            if not raw:
                return "[]"
            airports.append(json.loads(raw))
        return json.dumps(airports)

    def ft(self, index: str) -> _FakeFt:
        return _FakeFt(self, index)


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def client(tmp_path, monkeypatch, fake_redis):
    _configure_env(tmp_path, monkeypatch)

    with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis):
        with TestClient(ui_main.app) as c:
            yield c


def _rule(identifier="test-rule", **overrides) -> dict:
    rule = {
        "name": "Test rule",
        "description": "",
        "identifier": identifier,
        "enabled": True,
        "conditions": [{"type": "altitude", "operator": "minimum", "value": "1000"}],
    }
    rule.update(overrides)
    return rule


def _area(identifier="LI", **overrides) -> dict:
    area = {
        "identifier": identifier,
        "name": "Long Island",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
        },
        "locked": False,
    }
    area.update(overrides)
    return area


def _configure_env(tmp_path, monkeypatch, data_dir=None) -> None:
    """SETTINGS_PATH/DATA_DIR setup shared by the `client` fixture and the
    TestConfigBackup tests below, which need to control DATA_DIR's content
    *before* the TestClient context manager triggers lifespan()'s restore
    check -- too early for the `client` fixture's own fixed setup order."""
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(json.dumps({"redis": {"host": "localhost", "port": 6379}}))
    monkeypatch.setenv("SETTINGS_PATH", str(settings_path))
    monkeypatch.setenv("DATA_DIR", str(data_dir if data_dir is not None else tmp_path / "data"))


class TestListRules:
    def test_empty_returns_200_empty_array(self, client):
        resp = client.get("/api/rules")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_stored_rules(self, client):
        client.post("/api/rules", json=_rule("r1"))
        resp = client.get("/api/rules")
        assert resp.status_code == 200
        assert [r["identifier"] for r in resp.json()] == ["r1"]

    def test_returns_500_on_redis_error(self, client, fake_redis):
        fake_redis.get_error = ui_main.redis_lib.RedisError("boom")
        resp = client.get("/api/rules")
        assert resp.status_code == 500


class TestGetRule:
    def test_found(self, client):
        client.post("/api/rules", json=_rule("r1"))
        resp = client.get("/api/rules/r1")
        assert resp.status_code == 200
        assert resp.json()["identifier"] == "r1"

    def test_not_found_404(self, client):
        resp = client.get("/api/rules/nope")
        assert resp.status_code == 404


class TestCreateRule:
    def test_valid_creates_201(self, client, fake_redis):
        resp = client.post("/api/rules", json=_rule("r1"))
        assert resp.status_code == 201
        assert resp.json()["identifier"] == "r1"

        # Stored/hashed body now comes from Rule.model_dump(), which fills
        # in defaults (e.g. force_archive) the raw _rule() dict omits --
        # compare against the same round-trip rather than the literal input.
        expected_rules = [ui_main.Rule(**_rule("r1")).model_dump()]
        body = json.dumps(expected_rules)
        expected_version = ui_main.hashlib.sha256(body.encode()).hexdigest()
        assert json.loads(fake_redis.store[ui_main.config_rules_key()]) == expected_rules
        assert fake_redis.store[ui_main.config_rules_version_key()] == expected_version

    def test_duplicate_identifier_returns_409(self, client):
        client.post("/api/rules", json=_rule("dup"))
        resp = client.post("/api/rules", json=_rule("dup"))
        assert resp.status_code == 409

    def test_empty_conditions_returns_422(self, client):
        resp = client.post("/api/rules", json=_rule("bad", conditions=[]))
        assert resp.status_code == 422
        errors = resp.json()["detail"]
        assert any(err["loc"] == ["body", "conditions"] for err in errors)

    def test_identifier_with_space_returns_422(self, client):
        resp = client.post("/api/rules", json=_rule("my rule"))
        assert resp.status_code == 422
        errors = resp.json()["detail"]
        assert any(err["loc"] == ["body", "identifier"] for err in errors)

    def test_rejected_rule_does_not_persist(self, client):
        client.post("/api/rules", json=_rule("bad", conditions=[]))
        resp = client.get("/api/rules")
        assert resp.json() == []


class TestUpdateRule:
    def test_valid_update_returns_200(self, client):
        client.post("/api/rules", json=_rule("r1", name="Original"))
        resp = client.put("/api/rules/r1", json=_rule("r1", name="Updated"))
        assert resp.status_code == 200
        assert resp.json()["name"] == "Updated"

        get_resp = client.get("/api/rules/r1")
        assert get_resp.json()["name"] == "Updated"

    def test_not_found_returns_404(self, client):
        resp = client.put("/api/rules/nope", json=_rule("nope"))
        assert resp.status_code == 404

    def test_body_identifier_mismatch_returns_400(self, client):
        client.post("/api/rules", json=_rule("r1"))
        resp = client.put("/api/rules/r1", json=_rule("different"))
        assert resp.status_code == 400

    def test_invalid_update_returns_422_and_keeps_original(self, client):
        client.post("/api/rules", json=_rule("r1", name="Original"))
        resp = client.put("/api/rules/r1", json=_rule("r1", conditions=[]))
        assert resp.status_code == 422

        get_resp = client.get("/api/rules/r1")
        assert get_resp.json()["name"] == "Original"


class TestDeleteRule:
    def test_deletes_and_returns_204(self, client):
        client.post("/api/rules", json=_rule("r1"))
        resp = client.delete("/api/rules/r1")
        assert resp.status_code == 204
        assert client.get("/api/rules/r1").status_code == 404

    def test_not_found_returns_404(self, client):
        resp = client.delete("/api/rules/nope")
        assert resp.status_code == 404


class TestListAreas:
    def test_empty_returns_200_empty_array(self, client):
        resp = client.get("/api/areas")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_flattened_areas(self, client):
        client.post("/api/areas", json=_area("LI"))
        resp = client.get("/api/areas")
        assert resp.status_code == 200
        assert resp.json() == [_area("LI")]


class TestGetArea:
    def test_found(self, client):
        client.post("/api/areas", json=_area("LI"))
        resp = client.get("/api/areas/LI")
        assert resp.status_code == 200
        assert resp.json()["identifier"] == "LI"

    def test_not_found_404(self, client):
        resp = client.get("/api/areas/nope")
        assert resp.status_code == 404


class TestCreateArea:
    def test_valid_creates_201(self, client, fake_redis):
        resp = client.post("/api/areas", json=_area("LI"))
        assert resp.status_code == 201
        assert resp.json()["identifier"] == "LI"
        assert ui_main.config_areas_key() in fake_redis.store

    def test_duplicate_identifier_returns_409(self, client):
        client.post("/api/areas", json=_area("LI"))
        resp = client.post("/api/areas", json=_area("LI"))
        assert resp.status_code == 409

    def test_identifier_with_space_returns_422(self, client):
        resp = client.post("/api/areas", json=_area("Long Island"))
        assert resp.status_code == 422

    def test_point_geometry_creates_201(self, client):
        # Point/LineString areas are valid -- only usable as an
        # `area` rule condition's value is restricted to Polygon.
        resp = client.post("/api/areas", json=_area(
            "SHREK2", geometry={"type": "Point", "coordinates": [0, 0]},
        ))
        assert resp.status_code == 201

    def test_linestring_geometry_creates_201(self, client):
        resp = client.post("/api/areas", json=_area(
            "ILS17L", geometry={"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
        ))
        assert resp.status_code == 201

    def test_unknown_geometry_type_returns_422(self, client):
        resp = client.post("/api/areas", json=_area(
            "LI", geometry={"type": "MultiPolygon", "coordinates": []},
        ))
        assert resp.status_code == 422

    def test_self_intersecting_polygon_returns_400(self, client):
        # A bowtie ring: valid per Pydantic (floats in the right shape) but
        # rejected by shapely's is_valid check in RulesEngine._load_areas --
        # exercises _save_areas_array's "did it actually survive" safety
        # net, which only applies to Polygon areas.
        resp = client.post("/api/areas", json=_area("LI", geometry={
            "type": "Polygon",
            "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]],
        }))
        assert resp.status_code == 400

    def test_rejected_area_does_not_persist(self, client):
        client.post("/api/areas", json=_area("Long Island"))  # space in identifier
        resp = client.get("/api/areas")
        assert resp.json() == []


class TestUpdateArea:
    def test_valid_update_returns_200(self, client):
        client.post("/api/areas", json=_area("LI", name="Original"))
        resp = client.put("/api/areas/LI", json=_area("LI", name="Updated"))
        assert resp.status_code == 200
        assert resp.json()["name"] == "Updated"

    def test_not_found_returns_404(self, client):
        resp = client.put("/api/areas/nope", json=_area("nope"))
        assert resp.status_code == 404

    def test_body_identifier_mismatch_returns_400(self, client):
        client.post("/api/areas", json=_area("LI"))
        resp = client.put("/api/areas/LI", json=_area("different"))
        assert resp.status_code == 400


class TestAreaLocked:
    def test_defaults_to_false_when_omitted(self, client):
        area = _area("LI")
        del area["locked"]
        resp = client.post("/api/areas", json=area)
        assert resp.status_code == 201
        assert resp.json()["locked"] is False

    def test_create_with_locked_true(self, client):
        resp = client.post("/api/areas", json=_area("LI", locked=True))
        assert resp.status_code == 201
        assert resp.json()["locked"] is True

    def test_update_toggles_locked(self, client):
        client.post("/api/areas", json=_area("LI", locked=False))
        resp = client.put("/api/areas/LI", json=_area("LI", locked=True))
        assert resp.status_code == 200
        assert resp.json()["locked"] is True

        get_resp = client.get("/api/areas/LI")
        assert get_resp.json()["locked"] is True


class TestAreaStyle:
    """simplestyle-spec style properties -- persisted via config:areas'
    GeoJSON FeatureCollection (_area_to_feature/_feature_to_area), a
    separate boundary from Area's own alias-based (de)serialization that
    both need to agree on the same hyphenated key names."""

    def test_style_fields_round_trip_through_get(self, client):
        area = _area("LI", **{
            "fill": "#ff0000",
            "fill-opacity": 0.5,
            "stroke": "#00ff00",
            "stroke-width": 3,
            "stroke-opacity": 0.8,
        })
        resp = client.post("/api/areas", json=area)
        assert resp.status_code == 201
        assert resp.json()["fill"] == "#ff0000"
        assert resp.json()["fill-opacity"] == 0.5
        assert resp.json()["stroke-width"] == 3

        get_resp = client.get("/api/areas/LI")
        assert get_resp.json()["fill"] == "#ff0000"
        assert get_resp.json()["stroke"] == "#00ff00"
        assert get_resp.json()["stroke-opacity"] == 0.8

    def test_marker_style_round_trips_for_point_area(self, client):
        area = _area("PT", geometry={"type": "Point", "coordinates": [0, 0]}, **{
            "marker-color": "#123456",
            "marker-size": "large",
            "marker-symbol": "airport",
        })
        resp = client.post("/api/areas", json=area)
        assert resp.status_code == 201
        assert resp.json()["marker-color"] == "#123456"
        assert resp.json()["marker-size"] == "large"
        assert resp.json()["marker-symbol"] == "airport"

    def test_unset_style_fields_are_omitted_not_null(self, client):
        resp = client.post("/api/areas", json=_area("LI"))
        assert resp.status_code == 201
        for key in ("fill", "fill-opacity", "stroke", "stroke-width", "stroke-opacity", "marker-color", "marker-size", "marker-symbol"):
            assert key not in resp.json(), f"Unexpected key present with no value set: {key!r}"

        get_resp = client.get("/api/areas/LI")
        for key in ("fill", "fill-opacity", "stroke", "stroke-width", "stroke-opacity", "marker-color", "marker-size", "marker-symbol"):
            assert key not in get_resp.json()

    def test_invalid_marker_size_returns_422(self, client):
        resp = client.post("/api/areas", json=_area(
            "PT", geometry={"type": "Point", "coordinates": [0, 0]}, **{"marker-size": "huge"},
        ))
        assert resp.status_code == 422

    def test_update_changes_style(self, client):
        client.post("/api/areas", json=_area("LI", **{"stroke": "#111111"}))
        resp = client.put("/api/areas/LI", json=_area("LI", **{"stroke": "#222222"}))
        assert resp.status_code == 200
        assert resp.json()["stroke"] == "#222222"

    def test_update_clears_style_when_omitted(self, client):
        client.post("/api/areas", json=_area("LI", **{"stroke": "#111111"}))
        resp = client.put("/api/areas/LI", json=_area("LI"))
        assert resp.status_code == 200
        assert "stroke" not in resp.json()


class TestDeleteArea:
    def test_deletes_and_returns_204(self, client):
        client.post("/api/areas", json=_area("LI"))
        resp = client.delete("/api/areas/LI")
        assert resp.status_code == 204
        assert client.get("/api/areas/LI").status_code == 404

    def test_not_found_returns_404(self, client):
        resp = client.delete("/api/areas/nope")
        assert resp.status_code == 404


class TestAreaConditionCrossValidation:
    def test_rule_referencing_existing_area_succeeds(self, client):
        client.post("/api/areas", json=_area("LI"))
        rule = _rule("area-rule", conditions=[{"type": "area", "operator": "equals", "value": "LI"}])
        resp = client.post("/api/rules", json=rule)
        assert resp.status_code == 201

    def test_rule_referencing_unknown_area_returns_400(self, client):
        rule = _rule("area-rule", conditions=[{"type": "area", "operator": "equals", "value": "NOWHERE"}])
        resp = client.post("/api/rules", json=rule)
        assert resp.status_code == 400
        assert "not found in areas config" in resp.json()["detail"]

    def test_rule_matches_area_by_identifier_not_name(self, client):
        # LI has name "Long Island" but identifier "LI" -- the area
        # condition must match "LI", not the display name.
        client.post("/api/areas", json=_area("LI", name="Long Island"))
        rule = _rule("area-rule", conditions=[
            {"type": "area", "operator": "equals", "value": "Long Island"},
        ])
        resp = client.post("/api/rules", json=rule)
        assert resp.status_code == 400


class TestConditionOperatorEnforcement:
    """
    Condition is now a type-discriminated union (see main.py) -- every
    per-type model's `operator` Literal should be enforced by FastAPI/
    Pydantic at ingress, before RulesEngine ever sees the request.
    """

    # One operator invalid for that type, per CLAUDE.md's Conditions table.
    _INVALID_COMBINATIONS = [
        {"type": "altitude", "operator": "equals", "value": "1000"},
        {"type": "velocity", "operator": "equals", "value": "100"},
        {"type": "vertical_speed", "operator": "equals", "value": "100"},
        {"type": "date", "operator": "equals", "value": "2026-01-01"},
        {"type": "date", "operator": "in_list", "value": ["2026-01-01"]},
        {"type": "heading", "operator": "minimum", "value": "340,020"},
        {"type": "ident", "operator": "minimum", "value": "DAL2"},
        {"type": "squawk", "operator": "minimum", "value": "1200"},
        {"type": "military", "operator": "minimum", "value": "true"},
        {"type": "receiver_source", "operator": "in_list", "value": ["1090"]},
        {"type": "operator_airline_designator", "operator": "minimum", "value": "UAL"},
        {"type": "aircraft_type_designator", "operator": "minimum", "value": "B752"},
        {"type": "aircraft_registration", "operator": "minimum", "value": "N659DL"},
        {"type": "aircraft_icao_hex", "operator": "minimum", "value": "A8AE7F"},
        {"type": "wake_turbulence_category", "operator": "minimum", "value": "heavy"},
        {"type": "matched_rules", "operator": "equals", "value": ["r1"]},
        {"type": "area", "operator": "minimum", "value": "LI"},
    ]

    @pytest.mark.parametrize("condition", _INVALID_COMBINATIONS, ids=lambda c: f"{c['type']}-{c['operator']}")
    def test_invalid_operator_for_type_returns_422(self, client, condition):
        resp = client.post("/api/rules", json=_rule("bad", conditions=[condition]))
        assert resp.status_code == 422
        errors = resp.json()["detail"]
        assert any(err["loc"][:2] == ["body", "conditions"] for err in errors)

    # The cheapest valid operator for each type, per the same table.
    _VALID_COMBINATIONS = [
        {"type": "altitude", "operator": "minimum", "value": "1000"},
        {"type": "velocity", "operator": "maximum", "value": "100"},
        {"type": "vertical_speed", "operator": "minimum", "value": "-100"},
        {"type": "date", "operator": "minimum", "value": "2026-01-01"},
        {"type": "heading", "operator": "equals", "value": "340,020"},
        {"type": "ident", "operator": "equals", "value": "DAL2"},
        {"type": "squawk", "operator": "equals", "value": "1200"},
        {"type": "military", "operator": "equals", "value": "true"},
        {"type": "receiver_source", "operator": "equals", "value": ["1090", "978"]},
        {"type": "operator_airline_designator", "operator": "equals", "value": "UAL"},
        {"type": "aircraft_type_designator", "operator": "equals", "value": "B752"},
        {"type": "aircraft_registration", "operator": "equals", "value": "N659DL"},
        {"type": "aircraft_icao_hex", "operator": "equals", "value": "A8AE7F"},
        {"type": "aircraft_powerplant_count", "operator": "equals", "value": "2"},
        {"type": "wake_turbulence_category", "operator": "equals", "value": "heavy"},
        {"type": "matched_rules", "operator": "in_list", "value": ["other-rule"]},
        {"type": "area", "operator": "equals", "value": "LI"},
    ]

    @pytest.mark.parametrize("condition", _VALID_COMBINATIONS, ids=lambda c: f"{c['type']}-{c['operator']}")
    def test_valid_operator_for_type_returns_201(self, client, condition):
        client.post("/api/areas", json=_area("LI"))  # only needed by the 'area' case
        resp = client.post("/api/rules", json=_rule("ok", conditions=[condition]))
        assert resp.status_code == 201


class TestReceiverSourceCondition:
    """
    receiver_source's list-shaped value has constraints beyond a plain
    operator check (1-2 elements, no duplicates, only 1090/978/MLAT) --
    covered separately from TestConditionOperatorEnforcement's single
    valid/invalid-operator table.
    """

    def test_empty_list_returns_422(self, client):
        cond = {"type": "receiver_source", "operator": "equals", "value": []}
        resp = client.post("/api/rules", json=_rule("bad", conditions=[cond]))
        assert resp.status_code == 422

    def test_three_values_rejected(self, client):
        cond = {"type": "receiver_source", "operator": "equals", "value": ["1090", "978", "MLAT"]}
        resp = client.post("/api/rules", json=_rule("bad", conditions=[cond]))
        assert resp.status_code == 422

    def test_duplicate_values_rejected(self, client):
        cond = {"type": "receiver_source", "operator": "equals", "value": ["1090", "1090"]}
        resp = client.post("/api/rules", json=_rule("bad", conditions=[cond]))
        assert resp.status_code == 422

    def test_unknown_value_rejected(self, client):
        cond = {"type": "receiver_source", "operator": "equals", "value": ["4096"]}
        resp = client.post("/api/rules", json=_rule("bad", conditions=[cond]))
        assert resp.status_code == 422

    def test_single_value_accepted(self, client):
        cond = {"type": "receiver_source", "operator": "equals", "value": ["MLAT"]}
        resp = client.post("/api/rules", json=_rule("ok", conditions=[cond]))
        assert resp.status_code == 201


class TestConfigBackup:
    """
    config:rules/config:areas are the only two Redis keys representing
    user-authored state with no automatic regeneration path (see CLAUDE.md's
    Redis Key Schema) -- these cover the file-backup-on-write and restore-
    on-missing-key behavior that backs them up to DATA_DIR independently of
    Redis's own AOF.
    """

    def test_rule_save_writes_backup_file(self, client, tmp_path):
        client.post("/api/rules", json=_rule("r1"))
        backup_path = tmp_path / "data" / "rules-backup.json"
        assert backup_path.exists()
        assert json.loads(backup_path.read_text()) == [ui_main.Rule(**_rule("r1")).model_dump()]

    def test_area_save_writes_backup_file(self, client, tmp_path):
        client.post("/api/areas", json=_area("LI"))
        backup_path = tmp_path / "data" / "areas-backup.json"
        assert backup_path.exists()
        collection = json.loads(backup_path.read_text())
        assert collection["type"] == "FeatureCollection"
        assert collection["features"][0]["properties"]["identifier"] == "LI"

    def test_restores_rules_from_backup_when_redis_key_missing(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        rules = [ui_main.Rule(**_rule("r1")).model_dump()]
        (data_dir / "rules-backup.json").write_text(json.dumps(rules))
        _configure_env(tmp_path, monkeypatch, data_dir=data_dir)

        fake_redis = FakeRedis()
        with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis):
            with TestClient(ui_main.app) as c:
                resp = c.get("/api/rules")
                assert resp.status_code == 200
                assert [r["identifier"] for r in resp.json()] == ["r1"]

        assert ui_main.config_rules_key() in fake_redis.store
        assert ui_main.config_rules_version_key() in fake_redis.store

    def test_restores_areas_from_backup_when_redis_key_missing(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        collection = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"identifier": "LI", "name": "Long Island"},
                "geometry": _area("LI")["geometry"],
            }],
        }
        (data_dir / "areas-backup.json").write_text(json.dumps(collection))
        _configure_env(tmp_path, monkeypatch, data_dir=data_dir)

        fake_redis = FakeRedis()
        with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis):
            with TestClient(ui_main.app) as c:
                resp = c.get("/api/areas")
                assert resp.status_code == 200
                assert [a["identifier"] for a in resp.json()] == ["LI"]

        assert ui_main.config_areas_key() in fake_redis.store
        assert ui_main.config_areas_version_key() in fake_redis.store

    def test_does_not_restore_when_redis_key_already_present(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        # Backup file deliberately names a different rule than what's
        # already in Redis -- if restore ran anyway despite the key already
        # existing, that rule would show up in the response below.
        stale_rules = [ui_main.Rule(**_rule("stale-from-backup")).model_dump()]
        (data_dir / "rules-backup.json").write_text(json.dumps(stale_rules))
        _configure_env(tmp_path, monkeypatch, data_dir=data_dir)

        fake_redis = FakeRedis()
        existing_rules = [ui_main.Rule(**_rule("already-in-redis")).model_dump()]
        existing_body = json.dumps(existing_rules)
        fake_redis.store[ui_main.config_rules_key()] = existing_body
        fake_redis.store[ui_main.config_rules_version_key()] = (
            ui_main.hashlib.sha256(existing_body.encode()).hexdigest()
        )

        with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis):
            with TestClient(ui_main.app) as c:
                resp = c.get("/api/rules")
                assert [r["identifier"] for r in resp.json()] == ["already-in-redis"]

    def test_seeds_rules_backup_file_from_redis_when_file_missing(self, tmp_path, monkeypatch):
        # data_dir deliberately not created -- an existing deployment
        # upgrading to this feature has real data in Redis but has never
        # written a backup file (only a save does that).
        data_dir = tmp_path / "data"
        _configure_env(tmp_path, monkeypatch, data_dir=data_dir)

        fake_redis = FakeRedis()
        existing_rules = [ui_main.Rule(**_rule("already-in-redis")).model_dump()]
        fake_redis.store[ui_main.config_rules_key()] = json.dumps(existing_rules)

        with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis):
            with TestClient(ui_main.app):
                pass

        backup_path = data_dir / "rules-backup.json"
        assert backup_path.exists()
        assert json.loads(backup_path.read_text()) == existing_rules

    def test_seeds_areas_backup_file_from_redis_when_file_missing(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        _configure_env(tmp_path, monkeypatch, data_dir=data_dir)

        fake_redis = FakeRedis()
        collection = {
            "type": "FeatureCollection",
            "features": [{
                "type": "Feature",
                "properties": {"identifier": "LI", "name": "Long Island"},
                "geometry": _area("LI")["geometry"],
            }],
        }
        fake_redis.store[ui_main.config_areas_key()] = json.dumps(collection)

        with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis):
            with TestClient(ui_main.app):
                pass

        backup_path = data_dir / "areas-backup.json"
        assert backup_path.exists()
        assert json.loads(backup_path.read_text()) == collection

    def test_does_not_overwrite_existing_backup_file_when_redis_has_data(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        stale_backup = [ui_main.Rule(**_rule("stale-backup")).model_dump()]
        (data_dir / "rules-backup.json").write_text(json.dumps(stale_backup))
        _configure_env(tmp_path, monkeypatch, data_dir=data_dir)

        fake_redis = FakeRedis()
        existing_rules = [ui_main.Rule(**_rule("already-in-redis")).model_dump()]
        fake_redis.store[ui_main.config_rules_key()] = json.dumps(existing_rules)

        with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis):
            with TestClient(ui_main.app):
                pass

        # The pre-existing file is untouched -- seeding only fires when the
        # file is missing, never as an overwrite.
        assert json.loads((data_dir / "rules-backup.json").read_text()) == stale_backup

    def test_corrupt_backup_file_does_not_crash_startup(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "rules-backup.json").write_text("{not valid json")
        _configure_env(tmp_path, monkeypatch, data_dir=data_dir)

        fake_redis = FakeRedis()
        with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis):
            with TestClient(ui_main.app) as c:
                resp = c.get("/api/rules")
                assert resp.status_code == 200
                assert resp.json() == []

    def test_missing_backup_file_does_not_crash_startup(self, tmp_path, monkeypatch):
        # DATA_DIR itself doesn't even exist yet -- a fresh install.
        _configure_env(tmp_path, monkeypatch)

        fake_redis = FakeRedis()
        with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis):
            with TestClient(ui_main.app) as c:
                resp = c.get("/api/rules")
                assert resp.status_code == 200
                assert resp.json() == []


class TestConditionValueConstraints:
    """
    Numeric bounds and charset patterns on Condition.value, moved from
    UI-only client-side checks (RuleForm.tsx's validateCondition) into the
    same per-type models that already discriminate each condition's
    operator, so the backend is the actual source of truth per
    management-ui/README.md's own framing.
    """

    _VALID = [
        {"type": "altitude", "operator": "minimum", "value": "0"},
        {"type": "altitude", "operator": "maximum", "value": "65000"},
        {"type": "velocity", "operator": "minimum", "value": "0"},
        {"type": "velocity", "operator": "maximum", "value": "1334"},
        {"type": "vertical_speed", "operator": "minimum", "value": "-10000"},
        {"type": "vertical_speed", "operator": "maximum", "value": "10000"},
        {"type": "aircraft_powerplant_count", "operator": "minimum", "value": "0"},
        {"type": "aircraft_powerplant_count", "operator": "maximum", "value": "99"},
        {"type": "squawk", "operator": "equals", "value": "1200"},
        {"type": "aircraft_icao_hex", "operator": "equals", "value": "A8AE7F"},
        {"type": "aircraft_icao_hex", "operator": "equals", "value": "a8ae7f"},
        {"type": "aircraft_registration", "operator": "equals", "value": "N659DL"},
        {"type": "aircraft_registration", "operator": "equals", "value": "RA-12345"},
    ]

    @pytest.mark.parametrize("condition", _VALID, ids=lambda c: f"{c['type']}-{c['value']}")
    def test_valid_value_returns_201(self, client, condition):
        resp = client.post("/api/rules", json=_rule("ok", conditions=[condition]))
        assert resp.status_code == 201

    _INVALID = [
        {"type": "altitude", "operator": "minimum", "value": "-1"},
        {"type": "altitude", "operator": "maximum", "value": "65001"},
        {"type": "altitude", "operator": "minimum", "value": "not-a-number"},
        {"type": "velocity", "operator": "maximum", "value": "1335"},
        {"type": "vertical_speed", "operator": "minimum", "value": "-10001"},
        {"type": "vertical_speed", "operator": "maximum", "value": "10001"},
        {"type": "aircraft_powerplant_count", "operator": "minimum", "value": "-1"},
        {"type": "aircraft_powerplant_count", "operator": "maximum", "value": "100"},
        {"type": "squawk", "operator": "equals", "value": "0589"},  # 8 isn't octal
        {"type": "squawk", "operator": "equals", "value": "120"},  # too short
        {"type": "squawk", "operator": "equals", "value": "12000"},  # too long
        {"type": "aircraft_icao_hex", "operator": "equals", "value": "GGGGGG"},
        {"type": "aircraft_icao_hex", "operator": "equals", "value": "A8AE7"},  # too short
        {"type": "aircraft_registration", "operator": "equals", "value": "-N659DL"},
        {"type": "aircraft_registration", "operator": "equals", "value": "N659DL-"},
        {"type": "aircraft_registration", "operator": "equals", "value": "N"},  # too short
    ]

    @pytest.mark.parametrize("condition", _INVALID, ids=lambda c: f"{c['type']}-{c['value']}")
    def test_invalid_value_returns_422(self, client, condition):
        resp = client.post("/api/rules", json=_rule("bad", conditions=[condition]))
        assert resp.status_code == 422
        errors = resp.json()["detail"]
        assert any(err["loc"][:2] == ["body", "conditions"] for err in errors)


class TestRuleFieldLengths:
    def test_name_at_max_length_returns_201(self, client):
        resp = client.post("/api/rules", json=_rule("ok", name="x" * 64))
        assert resp.status_code == 201

    def test_name_over_max_length_returns_422(self, client):
        resp = client.post("/api/rules", json=_rule("bad", name="x" * 65))
        assert resp.status_code == 422
        errors = resp.json()["detail"]
        assert any(err["loc"] == ["body", "name"] for err in errors)

    def test_identifier_at_max_length_returns_201(self, client):
        resp = client.post("/api/rules", json=_rule("i" * 64))
        assert resp.status_code == 201

    def test_identifier_over_max_length_returns_422(self, client):
        resp = client.post("/api/rules", json=_rule("i" * 65))
        assert resp.status_code == 422
        errors = resp.json()["detail"]
        assert any(err["loc"] == ["body", "identifier"] for err in errors)

    def test_description_at_max_length_returns_201(self, client):
        resp = client.post("/api/rules", json=_rule("ok", description="x" * 2000))
        assert resp.status_code == 201

    def test_description_over_max_length_returns_422(self, client):
        resp = client.post("/api/rules", json=_rule("bad", description="x" * 2001))
        assert resp.status_code == 422
        errors = resp.json()["detail"]
        assert any(err["loc"] == ["body", "description"] for err in errors)


# ---------------------------------------------------------------------------
# Reference-data lookup (aircraft/operator/airport/route)
# ---------------------------------------------------------------------------

class TestAircraftLookup:
    def test_hex_hit_returns_merged_flattened_record(self, client, fake_redis):
        fake_redis.store["aircraft:mictronics:A8AE7F"] = json.dumps({
            "icao_hex": "A8AE7F",
            "registration": "N659DL",
            "military": False,
            "source": "mictronics",
            "aircraft": {
                "type_designator": "B752",
                "manufacturer": "Boeing",
            },
        })
        resp = client.get("/api/aircraft", params={"icao_hex": "A8AE7F"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["icao_hex"] == "A8AE7F"
        assert body["registration"] == "N659DL"
        assert body["type_designator"] == "B752"
        assert body["data_sources"] == ["mictronics"]

    def test_hex_merges_registry_over_mictronics(self, client, fake_redis):
        fake_redis.store["aircraft:mictronics:A8AE7F"] = json.dumps({
            "icao_hex": "A8AE7F", "registration": "N659DL", "military": False, "source": "mictronics",
        })
        fake_redis.store["aircraft:registry:A8AE7F"] = json.dumps({
            "icao_hex": "A8AE7F", "registration": "N659DL", "military": False,
            "source": "us-faa-registry",
            "aircraft": {"serial_number": "12345", "manufactured_date": "2005-01-01"},
        })
        resp = client.get("/api/aircraft", params={"icao_hex": "A8AE7F"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["serial_number"] == "12345"
        assert body["manufactured_date"] == "2005-01-01"
        assert body["data_sources"] == ["mictronics", "us-faa-registry"]

    def test_hex_survives_type_category_seats_manufacturer_model_registrant(self, client, fake_redis):
        """Registry-only fields not on Mictronics -- including the top-level
        (not aircraft-nested) registrant sub-object, matching real runner
        output shape (e.g. au-casa-registry's _build_record) -- must survive
        merge_aircraft.lua's merge and _flatten_aircraft_doc()'s promotion."""
        fake_redis.store["aircraft:mictronics:A8AE7F"] = json.dumps({
            "icao_hex": "A8AE7F", "registration": "N659DL", "military": False, "source": "mictronics",
        })
        fake_redis.store["aircraft:registry:A8AE7F"] = json.dumps({
            "icao_hex": "A8AE7F", "registration": "N659DL", "military": False,
            "source": "us-faa-registry",
            "aircraft": {
                "type": "Airplane",
                "category": "Land",
                "seats": 189,
                "manufacturer_model": "BOEING 767-332ER",
            },
            "registrant": {
                "names": ["Delta Air Lines Inc"],
                "street": ["1030 Delta Blvd"],
                "city": "Atlanta",
                "administrative_area": "GA",
                "postal_code": "30354",
                "country": "US",
                "type": "Corporation",
            },
        })
        resp = client.get("/api/aircraft", params={"icao_hex": "A8AE7F"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["type"] == "Airplane"
        assert body["category"] == "Land"
        assert body["seats"] == 189
        assert body["manufacturer_model"] == "BOEING 767-332ER"
        assert body["registrant"] == {
            "names": ["Delta Air Lines Inc"],
            "street": ["1030 Delta Blvd"],
            "city": "Atlanta",
            "administrative_area": "GA",
            "postal_code": "30354",
            "country": "US",
            "type": "Corporation",
        }

    def test_hex_miss_returns_404(self, client):
        resp = client.get("/api/aircraft", params={"icao_hex": "FFFFFF"})
        assert resp.status_code == 404

    def test_registration_hit_via_mictronics(self, client, fake_redis):
        fake_redis.store["aircraft:mictronics:A8AE7F"] = json.dumps({
            "icao_hex": "A8AE7F", "registration": "N659DL", "military": False, "source": "mictronics",
        })
        resp = client.get("/api/aircraft", params={"registration": "N659DL"})
        assert resp.status_code == 200
        assert resp.json()["icao_hex"] == "A8AE7F"

    def test_registration_hit_via_registry_when_not_in_mictronics(self, client, fake_redis):
        fake_redis.store["aircraft:registry:ABC123"] = json.dumps({
            "icao_hex": "ABC123", "registration": "N12345", "military": False, "source": "us-faa-registry",
        })
        resp = client.get("/api/aircraft", params={"registration": "N12345"})
        assert resp.status_code == 200
        assert resp.json()["icao_hex"] == "ABC123"

    def test_registration_miss_returns_404(self, client):
        resp = client.get("/api/aircraft", params={"registration": "UNKNOWN"})
        assert resp.status_code == 404

    def test_both_params_returns_422(self, client):
        resp = client.get("/api/aircraft", params={"icao_hex": "A8AE7F", "registration": "N659DL"})
        assert resp.status_code == 422

    def test_neither_param_returns_422(self, client):
        resp = client.get("/api/aircraft")
        assert resp.status_code == 422


class TestOperatorLookup:
    def test_hit(self, client, fake_redis):
        fake_redis.store["operator:DAL"] = json.dumps({"airline_designator": "DAL", "name": "Delta Air Lines"})
        resp = client.get("/api/operators/DAL")
        assert resp.status_code == 200
        assert resp.json()["name"] == "Delta Air Lines"

    def test_miss_returns_404(self, client):
        resp = client.get("/api/operators/ZZZ")
        assert resp.status_code == 404


class TestAirportLookup:
    def test_icao_hit(self, client, fake_redis):
        fake_redis.store["airport:KJFK"] = json.dumps({"icao_code": "KJFK", "name": "John F Kennedy Intl"})
        resp = client.get("/api/airports/KJFK")
        assert resp.status_code == 200
        assert resp.json()["name"] == "John F Kennedy Intl"

    def test_iata_hit(self, client, fake_redis):
        fake_redis.store["airport:KJFK"] = json.dumps({
            "icao_code": "KJFK", "iata_code": "JFK", "name": "John F Kennedy Intl",
        })
        resp = client.get("/api/airports/JFK")
        assert resp.status_code == 200
        assert resp.json()["icao_code"] == "KJFK"

    def test_miss_returns_404(self, client):
        resp = client.get("/api/airports/ZZZZ")
        assert resp.status_code == 404

    def test_invalid_length_returns_404_not_400(self, client):
        # Neither a 3-char IATA nor a 4-char ICAO code can ever match --
        # treated the same as any other miss (see main.py's get_airport).
        resp = client.get("/api/airports/ZZ")
        assert resp.status_code == 404


class TestRouteLookup:
    def test_hit_returns_origin_destination_and_stops(self, client, fake_redis):
        fake_redis.store["route:AAL15"] = "KMIA-KJFK-KMIA"
        fake_redis.store["airport:KMIA"] = json.dumps({"icao_code": "KMIA", "name": "Miami Intl"})
        fake_redis.store["airport:KJFK"] = json.dumps({"icao_code": "KJFK", "name": "JFK Intl"})
        resp = client.get("/api/routes/AAL15")
        assert resp.status_code == 200
        body = resp.json()
        assert body["ident"] == "AAL15"
        assert body["origin"]["icao_code"] == "KMIA"
        assert body["destination"]["icao_code"] == "KMIA"
        assert len(body["stops"]) == 3
        assert body["stops"][1]["icao_code"] == "KJFK"

    def test_ident_echoed_uppercased_regardless_of_request_case(self, client, fake_redis):
        fake_redis.store["route:AAL15"] = "KMIA-KJFK"
        fake_redis.store["airport:KMIA"] = json.dumps({"icao_code": "KMIA", "name": "Miami Intl"})
        fake_redis.store["airport:KJFK"] = json.dumps({"icao_code": "KJFK", "name": "JFK Intl"})
        resp = client.get("/api/routes/aal15")
        assert resp.status_code == 200
        assert resp.json()["ident"] == "AAL15"

    def test_no_route_returns_404(self, client):
        resp = client.get("/api/routes/UNKNOWN1")
        assert resp.status_code == 404

    def test_partial_route_missing_airport_returns_404(self, client, fake_redis):
        fake_redis.store["route:AAL16"] = "KMIA-UNKNOWN"
        fake_redis.store["airport:KMIA"] = json.dumps({"icao_code": "KMIA", "name": "Miami Intl"})
        resp = client.get("/api/routes/AAL16")
        assert resp.status_code == 404

    def test_operator_resolved_from_ident_prefix(self, client, fake_redis):
        fake_redis.store["route:AAL15"] = "KMIA-KJFK"
        fake_redis.store["airport:KMIA"] = json.dumps({"icao_code": "KMIA", "name": "Miami Intl"})
        fake_redis.store["airport:KJFK"] = json.dumps({"icao_code": "KJFK", "name": "JFK Intl"})
        fake_redis.store["operator:AAL"] = json.dumps(
            {"airline_designator": "AAL", "name": "American Airlines", "callsign": "AMERICAN"}
        )
        resp = client.get("/api/routes/AAL15")
        assert resp.status_code == 200
        assert resp.json()["operator"] == {
            "airline_designator": "AAL",
            "name": "American Airlines",
            "callsign": "AMERICAN",
        }

    def test_missing_operator_omitted_not_fatal(self, client, fake_redis):
        fake_redis.store["route:AAL15"] = "KMIA-KJFK"
        fake_redis.store["airport:KMIA"] = json.dumps({"icao_code": "KMIA", "name": "Miami Intl"})
        fake_redis.store["airport:KJFK"] = json.dumps({"icao_code": "KJFK", "name": "JFK Intl"})
        resp = client.get("/api/routes/AAL15")
        assert resp.status_code == 200
        assert resp.json()["operator"] is None

    def test_short_ident_prefix_omits_operator(self, client, fake_redis):
        fake_redis.store["route:N12345"] = "KMIA-KJFK"
        fake_redis.store["airport:KMIA"] = json.dumps({"icao_code": "KMIA", "name": "Miami Intl"})
        fake_redis.store["airport:KJFK"] = json.dumps({"icao_code": "KJFK", "name": "JFK Intl"})
        fake_redis.store["operator:N"] = json.dumps({"airline_designator": "N", "name": "Not A Real Operator"})
        resp = client.get("/api/routes/N12345")
        assert resp.status_code == 200
        assert resp.json()["operator"] is None
