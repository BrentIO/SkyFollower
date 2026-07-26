"""
Tests for ui/backend/main.py.

Redis is mocked via unittest.mock, matching the
patch("message_processor.main.redis_lib.Redis") convention used in
message-processor/tests/test_processor.py.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ui.backend import main as ui_main


@pytest.fixture
def mock_redis():
    redis = MagicMock()
    redis.get.return_value = None
    return redis


@pytest.fixture
def client(tmp_path, monkeypatch, mock_redis):
    settings_path = tmp_path / "settings.json"
    settings_path.write_text(json.dumps({"redis": {"host": "localhost", "port": 6379}}))
    monkeypatch.setenv("SETTINGS_PATH", str(settings_path))

    with patch.object(ui_main.redis_lib, "Redis", return_value=mock_redis):
        with TestClient(ui_main.app) as c:
            yield c


VALID_RULE = {
    "name": "Test rule",
    "description": "",
    "identifier": "test-rule",
    "enabled": True,
    "conditions": [
        {"type": "altitude", "operator": "minimum", "value": 1000},
    ],
}

VALID_FEATURE_COLLECTION = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"name": "LI"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
        }
    ],
}


class TestRules:
    def test_get_rules_returns_204_when_unset(self, client):
        resp = client.get("/api/rules")
        assert resp.status_code == 204

    def test_get_rules_returns_stored_rules(self, client, mock_redis):
        mock_redis.get.return_value = json.dumps([VALID_RULE])
        resp = client.get("/api/rules")
        assert resp.status_code == 200
        assert resp.json() == [VALID_RULE]

    def test_get_rules_returns_500_on_redis_error(self, client, mock_redis):
        mock_redis.get.side_effect = ui_main.redis_lib.RedisError("boom")
        resp = client.get("/api/rules")
        assert resp.status_code == 500

    def test_put_rules_valid_writes_and_echoes(self, client, mock_redis):
        resp = client.put("/api/rules", json=[VALID_RULE])
        assert resp.status_code == 200
        assert resp.json() == [VALID_RULE]

        body = json.dumps([VALID_RULE])
        expected_version = ui_main.hashlib.sha256(body.encode()).hexdigest()
        mock_redis.set.assert_any_call(ui_main.config_rules_key(), body)
        mock_redis.set.assert_any_call(ui_main.config_rules_version_key(), expected_version)

    def test_put_rules_invalid_returns_400_with_detail(self, client):
        bad_rule = {**VALID_RULE, "conditions": []}
        resp = client.put("/api/rules", json=[bad_rule])
        assert resp.status_code == 400
        assert "no conditions" in resp.json()["detail"]

    def test_put_rules_duplicate_identifier_returns_400(self, client):
        resp = client.put("/api/rules", json=[VALID_RULE, VALID_RULE])
        assert resp.status_code == 400
        assert "duplicate identifier" in resp.json()["detail"]


class TestAreas:
    def test_get_areas_returns_204_when_unset(self, client):
        resp = client.get("/api/areas")
        assert resp.status_code == 204

    def test_get_areas_returns_stored_areas(self, client, mock_redis):
        mock_redis.get.return_value = json.dumps(VALID_FEATURE_COLLECTION)
        resp = client.get("/api/areas")
        assert resp.status_code == 200
        assert resp.json() == VALID_FEATURE_COLLECTION

    def test_put_areas_valid_writes_and_echoes(self, client, mock_redis):
        resp = client.put("/api/areas", json=VALID_FEATURE_COLLECTION)
        assert resp.status_code == 200
        assert resp.json() == VALID_FEATURE_COLLECTION
        assert mock_redis.set.call_count == 2

    def test_put_areas_invalid_returns_400(self, client):
        resp = client.put("/api/areas", json={"type": "NotAFeatureCollection"})
        assert resp.status_code == 400
        assert "FeatureCollection" in resp.json()["detail"]

    def test_area_condition_validates_against_saved_areas(self, client, mock_redis):
        put_resp = client.put("/api/areas", json=VALID_FEATURE_COLLECTION)
        assert put_resp.status_code == 200

        rule_with_area = {
            **VALID_RULE,
            "identifier": "area-rule",
            "conditions": [{"type": "area", "operator": "equals", "value": "LI"}],
        }
        resp = client.put("/api/rules", json=[rule_with_area])
        assert resp.status_code == 200

    def test_area_condition_rejects_unknown_area(self, client):
        rule_with_area = {
            **VALID_RULE,
            "identifier": "area-rule",
            "conditions": [{"type": "area", "operator": "equals", "value": "NOWHERE"}],
        }
        resp = client.put("/api/rules", json=[rule_with_area])
        assert resp.status_code == 400
        assert "not found in areas config" in resp.json()["detail"]


class TestStubs:
    def test_aircraft_by_hex_not_implemented(self, client):
        resp = client.get("/api/aircraft/A8AE7F")
        assert resp.status_code == 501

    def test_aircraft_by_registration_not_implemented(self, client):
        resp = client.get("/api/aircraft", params={"registration": "N12345"})
        assert resp.status_code == 501

    def test_missing_operators_not_implemented(self, client):
        resp = client.get("/api/operators/missing")
        assert resp.status_code == 501
