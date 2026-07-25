"""
Integration tests for shared/lua/route_airports.lua, run against a live Redis
(RedisJSON + Lua scripting required — a redis-stack instance).

These tests exercise the actual Lua script via EVALSHA, the same way a
caller would, rather than mocking the resolution behavior. There's no way to
verify Lua semantics (e.g. cjson's empty-table-as-object quirk) by testing
Python code alone.

Requires a reachable Redis at REDIS_TEST_HOST:REDIS_TEST_PORT (defaults to
localhost:6379). If none is reachable, every test in this module is skipped
rather than failed, since CI does not run a Redis service for this workflow.
"""

from __future__ import annotations

import json
import os
import pathlib

import pytest

redis = pytest.importorskip("redis")

_LUA_PATH = pathlib.Path(__file__).parent.parent / "lua" / "route_airports.lua"
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
def route_airports_sha(redis_client):
    return redis_client.script_load(_LUA_PATH.read_text())


def _resolve(redis_client, route_airports_sha, ident):
    raw = redis_client.evalsha(route_airports_sha, 0, ident)
    return json.loads(raw)


def _seed(redis_client, ident, route, airports):
    """Writes route:{ident} and each airport:{code} in `airports` (a dict of
    code -> record), and returns a teardown callable that deletes them all."""
    redis_client.set(f"route:{ident}", route)
    for code, record in airports.items():
        redis_client.json().set(f"airport:{code}", "$", record)

    def _teardown():
        redis_client.delete(f"route:{ident}", *(f"airport:{code}" for code in airports))

    return _teardown


class TestMultiLegRoutes:
    def test_five_airport_route(self, redis_client, route_airports_sha):
        codes = ["CYQG", "CYTZ", "CYOW", "CYHZ", "CYYT"]
        airports = {code: {"icao_code": code, "name": code + " Airport"} for code in codes}
        teardown = _seed(redis_client, "POE255", "-".join(codes), airports)
        try:
            result = _resolve(redis_client, route_airports_sha, "POE255")
            assert [a["icao_code"] for a in result] == codes
        finally:
            teardown()

    def test_four_airport_route(self, redis_client, route_airports_sha):
        codes = ["CYHZ", "CYUL", "CYTZ", "CYQT"]
        airports = {code: {"icao_code": code, "name": code + " Airport"} for code in codes}
        teardown = _seed(redis_client, "POE468", "-".join(codes), airports)
        try:
            result = _resolve(redis_client, route_airports_sha, "POE468")
            assert [a["icao_code"] for a in result] == codes
        finally:
            teardown()

    def test_six_airport_route(self, redis_client, route_airports_sha):
        codes = ["YBBN", "YGLA", "YBRK", "YBMK", "YBTL", "YBCS"]
        airports = {code: {"icao_code": code, "name": code + " Airport"} for code in codes}
        teardown = _seed(redis_client, "QFA2308", "-".join(codes), airports)
        try:
            result = _resolve(redis_client, route_airports_sha, "QFA2308")
            assert [a["icao_code"] for a in result] == codes
        finally:
            teardown()

    def test_reverse_route_order_matters_not_just_membership(self, redis_client, route_airports_sha):
        """QFA2307 is the exact reverse of QFA2308's route — same six
        airports, opposite order. Confirms the result reflects route order,
        not just which airports are members of the route."""
        codes = ["YBCS", "YBTL", "YBMK", "YBRK", "YGLA", "YBBN"]
        airports = {code: {"icao_code": code, "name": code + " Airport"} for code in codes}
        teardown = _seed(redis_client, "QFA2307", "-".join(codes), airports)
        try:
            result = _resolve(redis_client, route_airports_sha, "QFA2307")
            assert [a["icao_code"] for a in result] == codes
        finally:
            teardown()


class TestRoundTrip:
    def test_duplicate_airport_preserved_at_both_positions(self, redis_client, route_airports_sha):
        airports = {
            "KJFK": {"icao_code": "KJFK", "name": "John F Kennedy Intl"},
            "KMIA": {"icao_code": "KMIA", "name": "Miami Intl"},
        }
        teardown = _seed(redis_client, "RT1", "KJFK-KMIA-KJFK", airports)
        try:
            result = _resolve(redis_client, route_airports_sha, "RT1")
            assert [a["icao_code"] for a in result] == ["KJFK", "KMIA", "KJFK"]
            assert result[0] == result[2]
        finally:
            teardown()


class TestNoRouteKnown:
    def test_absent_route_key_returns_empty_array(self, redis_client, route_airports_sha):
        assert _resolve(redis_client, route_airports_sha, "NOSUCHIDENT") == []


class TestMissingAirportRecord:
    def test_one_missing_airport_empties_whole_result(self, redis_client, route_airports_sha):
        """A partial route can't reliably give an origin/destination, so a
        single missing leg must empty the whole result, not just omit that
        one element."""
        airports = {"KJFK": {"icao_code": "KJFK", "name": "John F Kennedy Intl"}}
        teardown = _seed(redis_client, "RT2", "KJFK-KXXX", airports)
        try:
            assert _resolve(redis_client, route_airports_sha, "RT2") == []
        finally:
            teardown()
