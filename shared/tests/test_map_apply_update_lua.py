"""
Integration tests for shared/lua/map_apply_update.lua, run against a live
Redis (Lua scripting required -- a redis-stack instance, same as
test_merge_aircraft_lua.py/test_route_airports_lua.py in this directory).

These exercise the actual script via EVALSHA, the same way map/state_store.py's
FlightStateStore.apply_update calls it, rather than mocking the TTL behavior --
there's no way to verify Lua semantics (e.g. which SET/EXPIRE calls actually
run for a given msg_type) by testing Python code alone.

Focus: #1966's fix -- flight:live:{icao_hex}'s TTL (the "stale"/"live"
signal) must only be refreshed by `position` packets, never by `metadata`
(including message-processor's unconditional 60s metadata resend, which
carries the same frozen last_message timestamp as before and would
otherwise make an idle aircraft cycle stale/live forever). flight:detail
and flight:visible must keep refreshing on *both* packet types -- that
resync/keep-on-screen behavior is unrelated to the stale/live distinction
and must not regress.

Requires a reachable Redis at REDIS_TEST_HOST:REDIS_TEST_PORT (defaults to
localhost:6379). If none is reachable, every test in this module is skipped
rather than failed, since CI does not run a Redis service for this workflow.
"""

from __future__ import annotations

import json
import os
import pathlib
import uuid

import pytest

redis = pytest.importorskip("redis")

# See shared/tests/test_route_airports_lua.py for why xdist tests sharing
# live, mutable Redis state must be pinned to one worker.
pytestmark = pytest.mark.xdist_group(name="map_apply_update_lua")

_LUA_PATH = pathlib.Path(__file__).parent.parent / "lua" / "map_apply_update.lua"
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
def apply_update_sha(redis_client):
    return redis_client.script_load(_LUA_PATH.read_text())


@pytest.fixture
def icao_hex(redis_client):
    """A fresh, collision-free test hex per test, cleaned up afterward."""
    hex_ = "FFFD" + uuid.uuid4().hex[:2].upper()
    yield hex_
    redis_client.delete(
        f"flight:detail:{hex_}", f"flight:live:{hex_}", f"flight:visible:{hex_}", f"flight:trail:{hex_}",
    )


_STALE_SECONDS = 15
_HIDE_SECONDS = 60
_EVICT_SECONDS = 300
_MAX_TRAIL_POINTS = 25000


def _apply_update(
    redis_client, apply_update_sha, hex_, msg_type, timestamp, fields,
    stale_seconds=_STALE_SECONDS, hide_seconds=_HIDE_SECONDS, evict_seconds=_EVICT_SECONDS,
):
    field_names = list(fields.keys())
    field_values = [json.dumps(v) for v in fields.values()]
    raw = redis_client.evalsha(
        apply_update_sha, 0,
        hex_, msg_type, timestamp,
        json.dumps(field_names), json.dumps(field_values),
        stale_seconds, evict_seconds, hide_seconds,
        _MAX_TRAIL_POINTS,
    )
    return None if raw is None else json.loads(raw)


def _live_key(hex_):
    return f"flight:live:{hex_}"


def _visible_key(hex_):
    return f"flight:visible:{hex_}"


def _detail_key(hex_):
    return f"flight:detail:{hex_}"


class TestLiveTtlRefreshByMsgType:
    """#1966 -- flight:live's TTL is the "stale" signal
    (map/state_store.py's module docstring); it must move only on real
    `position` data, never on a `metadata` resend."""

    def test_position_packet_sets_live_ttl(self, redis_client, apply_update_sha, icao_hex):
        _apply_update(redis_client, apply_update_sha, icao_hex, "position", 1000.0, {"lat": 1.0, "lon": 2.0})
        ttl = redis_client.ttl(_live_key(icao_hex))
        assert 0 < ttl <= _STALE_SECONDS

    def test_metadata_only_packet_never_creates_live_key(self, redis_client, apply_update_sha, icao_hex):
        """An aircraft whose first-ever packet is `metadata` (no position
        yet) must not get a flight:live key at all -- there's no real
        position data yet for "stale" to be measured against."""
        _apply_update(redis_client, apply_update_sha, icao_hex, "metadata", 1000.0, {"ident": "TST1"})
        assert redis_client.exists(_live_key(icao_hex)) == 0

    def test_metadata_packet_does_not_refresh_an_existing_live_ttl(self, redis_client, apply_update_sha, icao_hex):
        """The core regression case: a `position` packet establishes
        flight:live, time passes (simulated here via a direct EXPIRE rather
        than a real sleep, for a fast/deterministic test) so the TTL is
        already low, and a `metadata` packet arrives (e.g. the 60s resend)
        with the *same* timestamp -- the out-of-order guard accepts it
        (equal timestamps aren't dropped), but it must leave flight:live's
        TTL untouched. Before the fix, this SET...EX call ran unconditionally
        and would have reset the TTL back to the full stale_seconds here."""
        _apply_update(redis_client, apply_update_sha, icao_hex, "position", 1000.0, {"lat": 1.0, "lon": 2.0})
        assert redis_client.ttl(_live_key(icao_hex)) <= _STALE_SECONDS

        redis_client.expire(_live_key(icao_hex), 3)  # Simulate most of stale_seconds having elapsed.

        _apply_update(redis_client, apply_update_sha, icao_hex, "metadata", 1000.0, {"ident": "TST1"})
        ttl_after_metadata = redis_client.ttl(_live_key(icao_hex))
        assert 0 < ttl_after_metadata <= 3

    def test_metadata_packet_does_not_resurrect_an_expired_live_key(self, redis_client, apply_update_sha, icao_hex):
        """Once flight:live has actually expired (the aircraft has already
        dimmed), a later metadata resend must not bring it back -- the
        literal "dims once and stays dimmed" acceptance criterion."""
        _apply_update(redis_client, apply_update_sha, icao_hex, "position", 1000.0, {"lat": 1.0, "lon": 2.0})
        redis_client.delete(_live_key(icao_hex))  # Simulate the key having already expired.

        _apply_update(redis_client, apply_update_sha, icao_hex, "metadata", 1000.0, {"ident": "TST1"})
        assert redis_client.exists(_live_key(icao_hex)) == 0

    def test_position_packet_refreshes_an_aged_live_ttl_back_to_full(self, redis_client, apply_update_sha, icao_hex):
        """Positive control: unlike metadata, a real `position` packet must
        still refresh flight:live's TTL back up, even if it had already
        partially counted down -- this is the legitimate "aircraft is
        genuinely still transmitting" case #1966 must not break."""
        _apply_update(redis_client, apply_update_sha, icao_hex, "position", 1000.0, {"lat": 1.0, "lon": 2.0})
        redis_client.expire(_live_key(icao_hex), 2)

        _apply_update(redis_client, apply_update_sha, icao_hex, "position", 1001.0, {"lat": 1.1, "lon": 2.1})
        ttl_after_position = redis_client.ttl(_live_key(icao_hex))
        assert ttl_after_position > 2


class TestOtherTtlsStayUnconditional:
    """flight:detail/flight:visible's refresh-on-every-accepted-packet
    behavior is the legitimate resync/keep-on-screen mechanism the metadata
    resend loop depends on -- #1966 must not touch it."""

    def test_metadata_packet_refreshes_visible_ttl(self, redis_client, apply_update_sha, icao_hex):
        _apply_update(redis_client, apply_update_sha, icao_hex, "position", 1000.0, {"lat": 1.0, "lon": 2.0})
        redis_client.expire(_visible_key(icao_hex), 3)

        _apply_update(redis_client, apply_update_sha, icao_hex, "metadata", 1000.0, {"ident": "TST1"})
        assert redis_client.ttl(_visible_key(icao_hex)) > 3

    def test_metadata_packet_refreshes_detail_ttl(self, redis_client, apply_update_sha, icao_hex):
        _apply_update(redis_client, apply_update_sha, icao_hex, "position", 1000.0, {"lat": 1.0, "lon": 2.0})
        redis_client.expire(_detail_key(icao_hex), 3)

        _apply_update(redis_client, apply_update_sha, icao_hex, "metadata", 1000.0, {"ident": "TST1"})
        assert redis_client.ttl(_detail_key(icao_hex)) > 3

    def test_metadata_only_packet_still_creates_detail_and_visible_keys(self, redis_client, apply_update_sha, icao_hex):
        """Restart-recovery relies on this: even a metadata-only resend for
        a brand-new-to-this-Redis-instance aircraft must still populate
        detail/visible (just not live) -- see map map-service-restart
        acceptance criterion in #1966."""
        merged = _apply_update(redis_client, apply_update_sha, icao_hex, "metadata", 1000.0, {"ident": "TST1"})
        assert merged is not None
        assert redis_client.exists(_detail_key(icao_hex))
        assert redis_client.exists(_visible_key(icao_hex))
        assert redis_client.exists(_live_key(icao_hex)) == 0


class TestPositionBehaviorUnaffected:
    """Regression guard: the merge/out-of-order/trail behavior this script
    already had must be completely unaffected by the msg_type-conditional
    live-TTL change -- covered more fully by test_merge_aircraft_lua.py's
    sibling suites and map/tests/test_state_store.py, just pinned here too
    since this is the file that actually changed."""

    def test_merge_still_returns_both_position_and_metadata_fields(self, redis_client, apply_update_sha, icao_hex):
        _apply_update(redis_client, apply_update_sha, icao_hex, "position", 1000.0, {"lat": 1.0, "lon": 2.0})
        merged = _apply_update(redis_client, apply_update_sha, icao_hex, "metadata", 1000.0, {"ident": "TST1"})
        assert merged["lat"] == 1.0
        assert merged["ident"] == "TST1"

    def test_out_of_order_packet_still_dropped(self, redis_client, apply_update_sha, icao_hex):
        _apply_update(redis_client, apply_update_sha, icao_hex, "position", 5000.0, {"lat": 10.0, "lon": 20.0})
        result = _apply_update(redis_client, apply_update_sha, icao_hex, "position", 4000.0, {"lat": 99.0, "lon": 99.0})
        assert result is None
