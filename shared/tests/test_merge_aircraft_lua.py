"""
Integration tests for shared/lua/merge_aircraft.lua, run against a live Redis
(RedisJSON + Lua scripting required — a redis-stack instance).

These tests exercise the actual Lua script via EVALSHA, the same way the
processor calls it, rather than mocking the merge behavior. There's no way to
verify Lua semantics (e.g. cjson.null handling) by testing Python code alone.

Requires a reachable Redis at REDIS_TEST_HOST:REDIS_TEST_PORT (defaults to
localhost:6379). If none is reachable, every test in this module is skipped
rather than failed, since CI does not run a Redis service for this workflow.
"""

from __future__ import annotations

import os
import pathlib
import uuid

import pytest

redis = pytest.importorskip("redis")

_LUA_PATH = pathlib.Path(__file__).parent.parent / "lua" / "merge_aircraft.lua"
_REDIS_HOST = os.environ.get("REDIS_TEST_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("REDIS_TEST_PORT", "6379"))


@pytest.fixture(scope="module")
def redis_client():
    client = redis.Redis(
        host=_REDIS_HOST, port=_REDIS_PORT, decode_responses=True, socket_connect_timeout=2,
    )
    try:
        client.ping()
    except (redis.exceptions.RedisError, OSError):
        pytest.skip(f"No Redis reachable at {_REDIS_HOST}:{_REDIS_PORT} for live Lua script testing")
    yield client
    client.close()


@pytest.fixture(scope="module")
def merge_sha(redis_client):
    return redis_client.script_load(_LUA_PATH.read_text())


@pytest.fixture
def icao_hex(redis_client):
    """A fresh, collision-free test hex per test, cleaned up afterward."""
    hex_ = "FFFE" + uuid.uuid4().hex[:2].upper()
    yield hex_
    redis_client.delete(f"aircraft:mictronics:{hex_}", f"aircraft:registry:{hex_}")


def _merge(redis_client, merge_sha, hex_):
    raw = redis_client.evalsha(merge_sha, 0, hex_)
    if raw is None:
        return None
    import json

    return json.loads(raw)


class TestManufacturerModelFallback:
    def test_manufacturer_and_model_present_composes_fallback(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer": "GULFSTREAM AEROSPACE", "model": "GV-SP (G550)"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["aircraft"]["manufacturer_model"] == "GULFSTREAM AEROSPACE GV-SP (G550)"

    def test_manufacturer_only_composes_fallback(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"aircraft": {"manufacturer": "DJI"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["aircraft"]["manufacturer_model"] == "DJI"

    def test_model_only_composes_fallback(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"aircraft": {"model": "AGRAS T30"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["aircraft"]["manufacturer_model"] == "AGRAS T30"

    def test_neither_present_leaves_manufacturer_model_unset(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(f"aircraft:registry:{icao_hex}", "$", {"military": False})
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "manufacturer_model" not in result.get("aircraft", {})

    def test_no_aircraft_object_does_not_crash_or_create_one(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(f"aircraft:registry:{icao_hex}", "$", {"military": False})
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "aircraft" not in result

    def test_existing_mictronics_manufacturer_model_not_overwritten(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$",
            {"aircraft": {"manufacturer_model": "BOEING 757-200", "type_designator": "B752"}},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer": "BOEING", "model": "757-2Q8"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["aircraft"]["manufacturer_model"] == "BOEING 757-200"

    def test_explicit_null_manufacturer_model_triggers_fallback(self, redis_client, merge_sha, icao_hex):
        """cjson.decode turns JSON null into cjson.null, not Lua nil — the
        fallback must still fire, not silently skip this case."""
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer_model": None, "manufacturer": "PIPER", "model": "J3C-65"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["aircraft"]["manufacturer_model"] == "PIPER J3C-65"

    def test_values_are_trimmed_and_joined_with_single_space(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer": "  PIPER  ", "model": "  J3C-65  "}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["aircraft"]["manufacturer_model"] == "PIPER J3C-65"

    def test_empty_string_manufacturer_treated_as_absent(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer": "", "model": "J3C-65"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["aircraft"]["manufacturer_model"] == "J3C-65"

    def test_both_keys_absent_still_returns_none(self, redis_client, merge_sha, icao_hex):
        assert _merge(redis_client, merge_sha, icao_hex) is None
