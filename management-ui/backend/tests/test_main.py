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

    def test_non_polygon_geometry_returns_422(self, client):
        resp = client.post("/api/areas", json=_area(
            "LI", geometry={"type": "Point", "coordinates": [0, 0]},
        ))
        assert resp.status_code == 422

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
