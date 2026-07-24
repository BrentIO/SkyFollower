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
    redis_client.delete(
        f"aircraft:mictronics:{hex_}", f"aircraft:registry:{hex_}", f"aircraft:livery:{hex_}",
    )


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


class TestLiveryLayer:
    """Covers the third merge tier — aircraft:livery:{icao_hex}, written by
    the airportwebcams-special-liveries runner — added on top of the existing
    mictronics/registry two-key merge (see #490)."""

    def test_livery_absent_merge_unaffected(self, redis_client, merge_sha, icao_hex):
        """An aircraft with no special livery (the common case) must merge
        exactly as it did before this key existed — no special_livery key
        at all. Mirrors the real N659DL/A8AE7F and N62770/A833A4 control
        cases documented in the runner's README."""
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$",
            {"aircraft": {"manufacturer_model": "BOEING 757-200"}},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"registration": "N659DL", "military": False},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["registration"] == "N659DL"
        assert "special_livery" not in result

    def test_livery_only_key_present(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"special_livery": "America250"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["special_livery"] == "America250"

    def test_livery_layered_on_top_of_mictronics_and_registry(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$",
            {"aircraft": {"manufacturer_model": "AIRBUS A320"}},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"registration": "N775JB", "military": False},
        )
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"special_livery": "America250"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["aircraft"]["manufacturer_model"] == "AIRBUS A320"
        assert result["registration"] == "N775JB"
        assert result["military"] is False
        assert result["special_livery"] == "America250"

    def test_livery_wins_on_field_overlap_with_registry(self, redis_client, merge_sha, icao_hex):
        """Livery is deep-merged last, so it must win over registry on any
        overlapping field — proving the requested stacking order (Mictronics
        -> registry -> livery) even though no real field overlaps today."""
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"registration": "REGISTRY-VALUE"},
        )
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"registration": "LIVERY-VALUE"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["registration"] == "LIVERY-VALUE"

    def test_all_three_keys_absent_still_returns_none(self, redis_client, merge_sha, icao_hex):
        assert _merge(redis_client, merge_sha, icao_hex) is None


class TestDataSources:
    """Covers merge_aircraft.lua's data_sources aggregation (#494) — every
    present key's own scalar `source` collected into an array, instead of
    the old deep_merge behaviour where only the last-written key's `source`
    survived."""

    def test_no_source_fields_present_no_data_sources_key(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(f"aircraft:registry:{icao_hex}", "$", {"registration": "N659DL"})
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "data_sources" not in result
        assert "source" not in result

    def test_single_source_mictronics_only(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["mictronics"]

    def test_two_sources_mictronics_and_registry_order_preserved(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa", "registration": "N659DL"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["mictronics", "us-faa"]

    def test_three_sources_all_present_mictronics_registry_livery_order(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa"},
        )
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"source": "airportwebcams-special-liveries", "special_livery": "America250"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["mictronics", "us-faa", "airportwebcams-special-liveries"]

    def test_mictronics_absent_registry_and_livery_still_both_collected(self, redis_client, merge_sha, icao_hex):
        """Regression guard: an earlier draft of this aggregation used Lua's
        ipairs() over {mictronics_doc, registry_doc, livery_doc}, which
        silently stops at the first nil element. With mictronics absent
        (nil, index 1), that would have skipped registry/livery too even
        though both are present — this is exactly the case that catches it."""
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa"},
        )
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"source": "airportwebcams-special-liveries"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["us-faa", "airportwebcams-special-liveries"]

    def test_bare_source_scalar_never_leaks_through(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "source" not in result
        assert result["data_sources"] == ["mictronics", "us-faa"]

    def test_key_present_without_source_field_does_not_crash_or_gap_the_array(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"aircraft": {"manufacturer_model": "BOEING 757-200"}},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["us-faa"]
