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

# Under pytest-xdist, a "module"-scoped fixture is instantiated once per
# worker *process*, not once globally -- if this module's tests get split
# across workers (which happens unpredictably once there's enough other
# work in the full suite to balance against), each worker independently
# seeds/tears down the shared airport:{code} records below against the
# one live Redis every worker actually talks to, racing each other. Pinning
# every test in this module to a single xdist group keeps them all on one
# worker, so the module-scoped fixtures below are only ever really
# instantiated once.
pytestmark = pytest.mark.xdist_group(name="route_airports_lua")

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


# Every airport:{code} record any test in this module needs, seeded once for
# the whole module rather than per-test. Two tests deliberately reuse the
# same six ICAO codes (test_six_airport_route / test_reverse_route_order_
# matters_not_just_membership -- same airports, opposite order, to prove the
# result reflects route order and not just membership), and record content
# is identical regardless of which test "owns" it, so sharing one seed is
# correct. Per-test create/delete of these same keys used to race under
# pytest-xdist: two tests sharing a code could land on different worker
# processes and run concurrently against the one shared live Redis, so one
# test's teardown could delete a key out from under another test's
# still-in-progress EVALSHA.
_ALL_AIRPORTS = {
    code: {"icao_code": code, "name": code + " Airport"}
    for code in [
        "CYQG", "CYTZ", "CYOW", "CYHZ", "CYYT",  # POE255
        "CYUL", "CYQT",  # POE468 (CYHZ/CYTZ already covered above)
        "YBBN", "YGLA", "YBRK", "YBMK", "YBTL", "YBCS",  # QFA2308 / QFA2307
    ]
}
_ALL_AIRPORTS["KJFK"] = {"icao_code": "KJFK", "name": "John F Kennedy Intl"}
_ALL_AIRPORTS["KMIA"] = {"icao_code": "KMIA", "name": "Miami Intl"}


@pytest.fixture(scope="module", autouse=True)
def _seed_airports(redis_client):
    for code, record in _ALL_AIRPORTS.items():
        redis_client.json().set(f"airport:{code}", "$", record)
    yield
    redis_client.delete(*(f"airport:{code}" for code in _ALL_AIRPORTS))


def _resolve(redis_client, route_airports_sha, ident):
    raw = redis_client.evalsha(route_airports_sha, 0, ident)
    return json.loads(raw)


def _seed_route(redis_client, ident, route):
    """Writes route:{ident} and returns a teardown callable that deletes it.
    Every ident used across this module's tests is unique, so -- unlike the
    airport records above -- there's no cross-test key overlap to race on."""
    redis_client.set(f"route:{ident}", route)
    return lambda: redis_client.delete(f"route:{ident}")


class TestMultiLegRoutes:
    def test_five_airport_route(self, redis_client, route_airports_sha):
        codes = ["CYQG", "CYTZ", "CYOW", "CYHZ", "CYYT"]
        teardown = _seed_route(redis_client, "POE255", "-".join(codes))
        try:
            result = _resolve(redis_client, route_airports_sha, "POE255")
            assert [a["icao_code"] for a in result] == codes
        finally:
            teardown()

    def test_four_airport_route(self, redis_client, route_airports_sha):
        codes = ["CYHZ", "CYUL", "CYTZ", "CYQT"]
        teardown = _seed_route(redis_client, "POE468", "-".join(codes))
        try:
            result = _resolve(redis_client, route_airports_sha, "POE468")
            assert [a["icao_code"] for a in result] == codes
        finally:
            teardown()

    def test_six_airport_route(self, redis_client, route_airports_sha):
        codes = ["YBBN", "YGLA", "YBRK", "YBMK", "YBTL", "YBCS"]
        teardown = _seed_route(redis_client, "QFA2308", "-".join(codes))
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
        teardown = _seed_route(redis_client, "QFA2307", "-".join(codes))
        try:
            result = _resolve(redis_client, route_airports_sha, "QFA2307")
            assert [a["icao_code"] for a in result] == codes
        finally:
            teardown()


class TestRoundTrip:
    def test_duplicate_airport_preserved_at_both_positions(self, redis_client, route_airports_sha):
        teardown = _seed_route(redis_client, "RT1", "KJFK-KMIA-KJFK")
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
        one element. KJFK is present (via the module-wide airport seed),
        KXXX never seeded anywhere -- that absence is the point of the test."""
        teardown = _seed_route(redis_client, "RT2", "KJFK-KXXX")
        try:
            assert _resolve(redis_client, route_airports_sha, "RT2") == []
        finally:
            teardown()
