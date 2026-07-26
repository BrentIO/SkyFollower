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

import importlib.util
import json
import os
import sys
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

_BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location("management_ui_main", os.path.join(_BACKEND_DIR, "main.py"))
ui_main = importlib.util.module_from_spec(_spec)
sys.modules["management_ui_main"] = ui_main
_spec.loader.exec_module(ui_main)


class FakeRedis:
    """Minimal in-memory stand-in for redis.Redis's get/set."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.get_error: Exception | None = None
        self.set_error: Exception | None = None

    def get(self, key):
        if self.get_error:
            raise self.get_error
        return self.store.get(key)

    def set(self, key, value):
        if self.set_error:
            raise self.set_error
        self.store[key] = value


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def client(tmp_path, monkeypatch, fake_redis):
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(json.dumps({"redis": {"host": "localhost", "port": 6379}}))
    monkeypatch.setenv("SETTINGS_PATH", str(settings_path))

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
    }
    area.update(overrides)
    return area


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

        body = json.dumps([_rule("r1")])
        expected_version = ui_main.hashlib.sha256(body.encode()).hexdigest()
        assert fake_redis.store[ui_main.config_rules_key()] == body
        assert fake_redis.store[ui_main.config_rules_version_key()] == expected_version

    def test_duplicate_identifier_returns_409(self, client):
        client.post("/api/rules", json=_rule("dup"))
        resp = client.post("/api/rules", json=_rule("dup"))
        assert resp.status_code == 409

    def test_invalid_rule_returns_400(self, client):
        resp = client.post("/api/rules", json=_rule("bad", conditions=[]))
        assert resp.status_code == 400
        assert "no conditions" in resp.json()["detail"]

    def test_identifier_with_space_returns_400(self, client):
        resp = client.post("/api/rules", json=_rule("my rule"))
        assert resp.status_code == 400
        assert "space" in resp.json()["detail"]

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

    def test_invalid_update_returns_400_and_keeps_original(self, client):
        client.post("/api/rules", json=_rule("r1", name="Original"))
        resp = client.put("/api/rules/r1", json=_rule("r1", conditions=[]))
        assert resp.status_code == 400

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

    def test_identifier_with_space_returns_400(self, client):
        resp = client.post("/api/areas", json=_area("Long Island"))
        assert resp.status_code == 400

    def test_non_polygon_geometry_returns_400(self, client):
        resp = client.post("/api/areas", json=_area(
            "LI", geometry={"type": "Point", "coordinates": [0, 0]},
        ))
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
