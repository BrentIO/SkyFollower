"""
Integration tests for shared/lua/incr_period_counter.lua, run against a live
Redis (Lua scripting only -- no RedisJSON needed, unlike merge_aircraft.lua).

These tests exercise the actual Lua script via EVALSHA, the same way the
receiver (and eventually the message processor) calls it, rather than mocking
the increment/expiry behavior. There's no way to verify the
exists-then-conditionally-expire semantics by testing Python code alone.

Requires a reachable Redis at REDIS_TEST_HOST:REDIS_TEST_PORT (defaults to
localhost:6379). If none is reachable, every test in this module is skipped
rather than failed, since CI does not run a Redis service for this workflow.
"""

from __future__ import annotations

import os
import pathlib
import time
import uuid

import pytest

redis = pytest.importorskip("redis")

# See shared/tests/test_merge_aircraft_lua.py's own comment on this pattern --
# a module-scoped fixture is per pytest-xdist *worker*, so tests in this
# module racing another worker's tests against the same live Redis is a real
# risk once the full suite is large enough to get split across workers.
pytestmark = pytest.mark.xdist_group(name="incr_period_counter_lua")

_LUA_PATH = pathlib.Path(__file__).parent.parent / "lua" / "incr_period_counter.lua"
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
def script_sha(redis_client):
    return redis_client.script_load(_LUA_PATH.read_text())


@pytest.fixture
def counter_key(redis_client):
    key = f"test:incr_period_counter:{uuid.uuid4().hex}"
    yield key
    redis_client.delete(key)


def _incr(redis_client, script_sha, key, amount, expires_at):
    return redis_client.evalsha(script_sha, 0, key, amount, expires_at)


class TestIncrPeriodCounter:
    def test_creates_key_with_initial_value(self, redis_client, script_sha, counter_key):
        expires_at = int(time.time()) + 3600
        result = _incr(redis_client, script_sha, counter_key, 5, expires_at)
        assert result == 5
        assert redis_client.get(counter_key) == "5"

    def test_sets_expiry_only_on_creation(self, redis_client, script_sha, counter_key):
        expires_at = int(time.time()) + 3600
        _incr(redis_client, script_sha, counter_key, 1, expires_at)
        ttl = redis_client.ttl(counter_key)
        assert 0 < ttl <= 3600

    def test_existing_key_accumulates_without_resetting_expiry(self, redis_client, script_sha, counter_key):
        first_expiry = int(time.time()) + 3600
        _incr(redis_client, script_sha, counter_key, 3, first_expiry)
        ttl_after_first = redis_client.ttl(counter_key)

        # A much later "expires_at" on the second call must be ignored --
        # only the call that actually creates the key sets EXPIREAT.
        second_expiry = int(time.time()) + 999999
        result = _incr(redis_client, script_sha, counter_key, 4, second_expiry)

        assert result == 7
        assert redis_client.get(counter_key) == "7"
        ttl_after_second = redis_client.ttl(counter_key)
        # Still bounded by the original ~3600s expiry, not pushed out to
        # the second call's much later one.
        assert 0 < ttl_after_second <= ttl_after_first + 5

    def test_returns_new_value(self, redis_client, script_sha, counter_key):
        expires_at = int(time.time()) + 3600
        _incr(redis_client, script_sha, counter_key, 10, expires_at)
        result = _incr(redis_client, script_sha, counter_key, 2, expires_at)
        assert result == 12

    def test_missing_key_before_first_call_is_absent(self, redis_client, counter_key):
        assert redis_client.get(counter_key) is None
