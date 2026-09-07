"""
Integration tests for map/state_store.py, run against a live Redis (same
pattern as shared/tests/test_route_airports_lua.py -- these exercise the
actual merge/TTL/keyspace-notification behavior, not mocks, since that's
exactly the part most likely to have a subtle bug).

Requires a reachable Redis at REDIS_TEST_HOST:REDIS_TEST_PORT (defaults to
localhost:6379, matching .github/workflows/run-tests.yaml's redis-stack
service). If none is reachable, every test in this module is skipped.

Uses Redis db 15 (SELECT), not db 0, so this module's FLUSHDB-heavy setup
never touches whatever any other component's test suite has seeded on db 0
of the same live server.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

redis = pytest.importorskip("redis")

# See shared/tests/test_route_airports_lua.py for why xdist tests sharing
# live, mutable Redis state must be pinned to one worker.
pytestmark = pytest.mark.xdist_group(name="map_state_store")

_REDIS_HOST = os.environ.get("REDIS_TEST_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("REDIS_TEST_PORT", "6379"))
_TEST_DB = 15


@pytest.fixture(scope="module")
def redis_client():
    client = redis.Redis(
        host=_REDIS_HOST, port=_REDIS_PORT, db=_TEST_DB,
        decode_responses=True, socket_connect_timeout=2,
    )
    try:
        client.ping()
    except (redis.exceptions.RedisError, OSError):
        pytest.skip(f"No Redis reachable at {_REDIS_HOST}:{_REDIS_PORT} for live state-store testing")
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture(autouse=True)
def _clean_db(redis_client):
    redis_client.flushdb()
    yield


def _hex() -> str:
    """A fresh, unique-looking ICAO hex per test -- avoids any chance of
    cross-test key collision even without the autouse flushdb above."""
    return uuid.uuid4().hex[:6].upper()


from map.state_store import (  # noqa: E402
    FlightStateStore,
    flight_detail_key,
    flight_live_key,
    flight_trail_key,
    overall_processor_status,
    parse_expired_key,
    processor_status,
)


# ---------------------------------------------------------------------------
# parse_expired_key -- pure function, no Redis needed, but co-located here
# since it's this module's own key-naming scheme.
# ---------------------------------------------------------------------------

def test_parse_expired_key_live():
    assert parse_expired_key("flight:live:A8AE7F") == ("live", "A8AE7F")


def test_parse_expired_key_detail():
    assert parse_expired_key("flight:detail:A8AE7F") == ("detail", "A8AE7F")


def test_parse_expired_key_unrelated():
    assert parse_expired_key("flight:trail:A8AE7F") is None
    assert parse_expired_key("some:other:key") is None


# ---------------------------------------------------------------------------
# Merge semantics
# ---------------------------------------------------------------------------

def test_partial_position_updates_merge_without_erasing_each_other(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    icao_hex = _hex()

    merged = store.apply_update(
        icao_hex, "position", 1000.0,
        {"latitude": 33.9, "longitude": -118.4, "altitude": 5000},
    )
    assert merged["latitude"] == 33.9
    assert merged["longitude"] == -118.4
    assert merged["altitude"] == 5000
    assert "velocity" not in merged
    assert "heading" not in merged

    merged = store.apply_update(
        icao_hex, "position", 1001.0,
        {"velocity": 250.0, "heading": 90.0, "vertical_speed": -500},
    )
    # The heading/velocity-only update must not blank out the previously
    # known position, and must add its own fields alongside it.
    assert merged["latitude"] == 33.9
    assert merged["longitude"] == -118.4
    assert merged["altitude"] == 5000
    assert merged["velocity"] == 250.0
    assert merged["heading"] == 90.0
    assert merged["vertical_speed"] == -500

    # get_flight() independently reflects the same fully-merged record.
    assert store.get_flight(icao_hex) == merged


def test_metadata_merges_alongside_position_fields(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    icao_hex = _hex()

    store.apply_update(icao_hex, "position", 2000.0, {"latitude": 1.0, "longitude": 2.0})
    merged = store.apply_update(
        icao_hex, "metadata", 2000.0,
        {
            "aircraft": {"icao_hex": icao_hex, "registration": "N12345"},
            "ident": "DAL123",
            "squawk": "1200",
            "matched_rules": [],
            "receiver_sources": ["1090"],
            "first_message": "2026-09-06T00:00:00+00:00",
            "last_message": "2026-09-06T00:00:00+00:00",
            "total_messages": 5,
        },
    )
    # Position fields from the earlier update survive the metadata merge.
    assert merged["latitude"] == 1.0
    assert merged["longitude"] == 2.0
    # Metadata fields, including a nested dict, round-trip intact.
    assert merged["aircraft"] == {"icao_hex": icao_hex, "registration": "N12345"}
    assert merged["ident"] == "DAL123"
    assert merged["squawk"] == "1200"
    assert merged["receiver_sources"] == ["1090"]
    assert merged["total_messages"] == 5
    assert merged["icao_hex"] == icao_hex


# ---------------------------------------------------------------------------
# Out-of-order guard
# ---------------------------------------------------------------------------

def test_out_of_order_packet_is_dropped(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    icao_hex = _hex()

    store.apply_update(icao_hex, "position", 5000.0, {"latitude": 10.0, "longitude": 20.0})
    result = store.apply_update(icao_hex, "position", 4000.0, {"latitude": 99.0, "longitude": 99.0})

    assert result is None
    # Displayed state must not have "rewound" to the older packet's values.
    current = store.get_flight(icao_hex)
    assert current["latitude"] == 10.0
    assert current["longitude"] == 20.0


def test_equal_timestamp_is_accepted_not_dropped(redis_client):
    """A `position` packet and a same-tick `metadata` packet for one source
    ADS-B message share message-processor's exact `received_at` value --
    equal timestamps must not be treated as out-of-order, or metadata would
    silently never apply the first time an aircraft is seen."""
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    icao_hex = _hex()

    store.apply_update(icao_hex, "position", 7000.0, {"latitude": 5.0, "longitude": 6.0})
    merged = store.apply_update(
        icao_hex, "metadata", 7000.0,
        {"ident": "UAL456", "aircraft": {"icao_hex": icao_hex}},
    )
    assert merged is not None
    assert merged["ident"] == "UAL456"
    assert merged["latitude"] == 5.0


# ---------------------------------------------------------------------------
# Trail accumulation
# ---------------------------------------------------------------------------

def test_trail_accumulates_across_position_updates(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    icao_hex = _hex()

    store.apply_update(icao_hex, "position", 1.0, {"latitude": 1.0, "longitude": 1.0, "altitude": 1000})
    store.apply_update(icao_hex, "position", 2.0, {"latitude": 1.1, "longitude": 1.1})
    store.apply_update(icao_hex, "position", 3.0, {"latitude": 1.2, "longitude": 1.2, "altitude": 3000})

    trail = store.get_trail(icao_hex)
    assert trail == [
        {"latitude": 1.0, "longitude": 1.0, "altitude": 1000},
        {"latitude": 1.1, "longitude": 1.1, "altitude": 1000},
        {"latitude": 1.2, "longitude": 1.2, "altitude": 3000},
    ]


def test_trail_not_appended_before_position_known(redis_client):
    """A velocity/heading-only position packet, before any lat/lon has ever
    been seen for this aircraft, has nothing meaningful to plot yet."""
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    icao_hex = _hex()

    store.apply_update(icao_hex, "position", 1.0, {"velocity": 200.0, "heading": 45.0})
    assert store.get_trail(icao_hex) == []

    store.apply_update(icao_hex, "position", 2.0, {"latitude": 9.0, "longitude": 9.0})
    assert store.get_trail(icao_hex) == [{"latitude": 9.0, "longitude": 9.0, "altitude": None}]


def test_metadata_packets_do_not_append_to_trail(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    icao_hex = _hex()

    store.apply_update(icao_hex, "position", 1.0, {"latitude": 1.0, "longitude": 1.0})
    store.apply_update(icao_hex, "metadata", 2.0, {"ident": "TST1"})

    assert len(store.get_trail(icao_hex)) == 1


# ---------------------------------------------------------------------------
# list_flights / get_flight shape parity
# ---------------------------------------------------------------------------

def test_list_flights_returns_one_entry_per_tracked_aircraft(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    hex_a, hex_b = _hex(), _hex()

    store.apply_update(hex_a, "position", 1.0, {"latitude": 1.0, "longitude": 1.0})
    store.apply_update(hex_b, "position", 1.0, {"latitude": 2.0, "longitude": 2.0})

    flights = {f["icao_hex"]: f for f in store.list_flights()}
    assert set(flights) == {hex_a, hex_b}
    assert flights[hex_a]["latitude"] == 1.0
    assert flights[hex_b]["latitude"] == 2.0


def test_get_flight_returns_none_for_unknown_aircraft(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    assert store.get_flight(_hex()) is None


# ---------------------------------------------------------------------------
# Round-trip counts -- the whole point of the Lua collapse / pipelined
# list_flights (#1566). Each test wraps a real, live-Redis client method
# with a counter and delegates to the original, so these prove the actual
# number of client<->server exchanges, not just that the right data comes
# back (already covered above).
# ---------------------------------------------------------------------------

def _count_calls(monkeypatch, obj, name):
    """Wraps obj.name with a call counter that still delegates to the real
    (bound) method, and returns a list whose length grows by one per call."""
    calls: list = []
    original = getattr(obj, name)

    def counting(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(obj, name, counting)
    return calls


def test_apply_update_issues_exactly_one_round_trip(redis_client, monkeypatch):
    """The old implementation issued an HGET, a pipeline (HSET+EXPIRE+SET),
    a get_flight() HGETALL, and (for position packets) a second pipeline
    (RPUSH+EXPIRE) -- 3-4 round trips. The EVALSHA-based implementation
    must issue exactly one evalsha call and zero direct HGET/HSET/EXPIRE/
    SET/RPUSH/pipeline calls -- every one of those now happens inside the
    Lua script, invisible at the client-command level."""
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    icao_hex = _hex()

    evalsha_calls = _count_calls(monkeypatch, redis_client, "evalsha")
    other_call_names = ("hget", "hset", "expire", "set", "rpush", "pipeline")
    other_calls = {name: _count_calls(monkeypatch, redis_client, name) for name in other_call_names}

    merged = store.apply_update(icao_hex, "position", 1.0, {"latitude": 1.0, "longitude": 1.0})

    assert merged == {"icao_hex": icao_hex, "latitude": 1.0, "longitude": 1.0}
    assert len(evalsha_calls) == 1
    for name, calls in other_calls.items():
        assert calls == [], f"expected no direct {name} calls, got {len(calls)}"


def test_list_flights_issues_one_pipelined_round_trip_not_n_plus_one(redis_client, monkeypatch):
    """The old implementation issued one SCAN (itself possibly more than
    one round trip on a large keyspace) plus one HGETALL per tracked
    aircraft -- N+1. The pipelined implementation must still issue exactly
    one execute() (one round trip for every aircraft's HGETALL combined),
    and never call HGETALL directly (outside a pipeline) at all."""
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    hexes = [_hex() for _ in range(5)]
    for i, icao_hex in enumerate(hexes):
        store.apply_update(icao_hex, "position", 1.0, {"latitude": float(i), "longitude": float(i)})

    direct_hgetall_calls = _count_calls(monkeypatch, redis_client, "hgetall")

    original_pipeline = redis_client.pipeline
    execute_calls: list = []

    def counting_pipeline(*args, **kwargs):
        pipe = original_pipeline(*args, **kwargs)
        original_execute = pipe.execute

        def counting_execute(*a, **kw):
            execute_calls.append(1)
            return original_execute(*a, **kw)

        pipe.execute = counting_execute
        return pipe

    monkeypatch.setattr(redis_client, "pipeline", counting_pipeline)

    flights = store.list_flights()

    assert {f["icao_hex"] for f in flights} == set(hexes)
    assert len(execute_calls) == 1, "expected exactly one pipelined round trip for all aircraft"
    assert direct_hgetall_calls == [], "expected no HGETALL issued outside the pipeline"


def test_list_flights_empty_store_issues_no_pipeline_at_all(redis_client, monkeypatch):
    """No tracked aircraft -- scan_iter finds nothing, so there's nothing
    to pipeline; must not construct an empty pipeline just to execute it."""
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)

    pipeline_calls = _count_calls(monkeypatch, redis_client, "pipeline")

    assert store.list_flights() == []
    assert pipeline_calls == []


# ---------------------------------------------------------------------------
# TTL refresh + real keyspace-notification eviction (the part most likely
# to have a subtle timing bug -- exercised end to end, not mocked).
# ---------------------------------------------------------------------------

def _drain_expired_events(redis_client, store, deadline: float) -> list[dict]:
    pubsub = redis_client.pubsub()
    pubsub.psubscribe("__keyevent@15__:expired")
    events: list[dict] = []
    try:
        while time.monotonic() < deadline:
            message = pubsub.get_message(timeout=0.5)
            if message is None or message.get("type") != "pmessage":
                continue
            event = store.handle_expired_key(message["data"])
            if event is not None:
                events.append(event)
    finally:
        pubsub.close()
    return events


def test_eviction_fires_stale_then_remove_at_correct_ttls(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=1, evict_seconds=2)
    store.enable_keyspace_notifications()
    icao_hex = _hex()

    store.apply_update(icao_hex, "position", time.time(), {"latitude": 1.0, "longitude": 1.0})

    # Both keys exist immediately after the update.
    assert redis_client.exists(flight_live_key(icao_hex))
    assert redis_client.exists(flight_detail_key(icao_hex))

    events = _drain_expired_events(redis_client, store, deadline=time.monotonic() + 4.0)
    kinds_for_hex = [e["type"] for e in events if e["icao_hex"] == icao_hex]

    assert kinds_for_hex == ["stale", "remove"], (
        f"expected stale then remove for {icao_hex}, got {kinds_for_hex} "
        f"(all events observed: {events})"
    )
    # The detail hash and trail are both actually gone once "remove" fires.
    assert store.get_flight(icao_hex) is None
    assert redis_client.exists(flight_trail_key(icao_hex)) == 0


def test_update_refreshes_ttl_so_live_aircraft_never_goes_stale(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=1, evict_seconds=3)
    store.enable_keyspace_notifications()
    icao_hex = _hex()

    store.apply_update(icao_hex, "position", time.time(), {"latitude": 1.0, "longitude": 1.0})
    time.sleep(0.6)
    # Refresh before the 1s stale TTL would otherwise fire.
    store.apply_update(icao_hex, "position", time.time(), {"latitude": 1.1, "longitude": 1.1})
    time.sleep(0.6)

    # 1.2s of elapsed time > stale_seconds=1, but the refresh reset the
    # clock -- the live key must still exist.
    assert redis_client.exists(flight_live_key(icao_hex))
    assert redis_client.exists(flight_detail_key(icao_hex))


# ---------------------------------------------------------------------------
# Processor status thresholds -- pure functions, no Redis needed. Final
# thresholds: green <=15s, amber 15-60s, red >60s or never seen (see
# shared/timing.py's MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS /
# MAP_PROCESSOR_AMBER_MAX_AGE_SECONDS).
# ---------------------------------------------------------------------------

def test_processor_status_green_at_or_under_threshold():
    assert processor_status(last_seen=985.0, now=1000.0) == "green"  # 15s exactly
    assert processor_status(last_seen=990.0, now=1000.0) == "green"  # 10s


def test_processor_status_amber_between_thresholds():
    assert processor_status(last_seen=970.0, now=1000.0) == "amber"  # 30s
    assert processor_status(last_seen=940.0, now=1000.0) == "amber"  # 60s exactly


def test_processor_status_red_beyond_amber_threshold():
    assert processor_status(last_seen=939.0, now=1000.0) == "red"  # 61s
    assert processor_status(last_seen=900.0, now=1000.0) == "red"  # 100s


def test_processor_status_red_when_never_seen():
    assert processor_status(last_seen=None, now=1000.0) == "red"


def test_overall_status_green_when_all_processors_green():
    assert overall_processor_status(["green", "green"]) == "green"


def test_overall_status_amber_when_mixed_with_at_least_one_green():
    assert overall_processor_status(["green", "amber"]) == "amber"
    assert overall_processor_status(["green", "red"]) == "amber"


def test_overall_status_red_when_all_red():
    assert overall_processor_status(["red", "red"]) == "red"


def test_overall_status_red_when_roster_empty():
    """Nothing has ever been received -- treated the same as "all red",
    not as some neutral/unknown state."""
    assert overall_processor_status([]) == "red"


# ---------------------------------------------------------------------------
# Processor roster -- live Redis, same fixtures/pattern as the aircraft
# state tests above.
# ---------------------------------------------------------------------------

def test_record_and_get_processor_roster(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    store.record_processor_seen("mp-1", 1000.0)
    store.record_processor_seen("mp-2", 1005.0)

    assert store.get_processor_roster() == {"mp-1": 1000.0, "mp-2": 1005.0}


def test_record_processor_seen_overwrites_last_seen(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    store.record_processor_seen("mp-1", 1000.0)
    store.record_processor_seen("mp-1", 2000.0)

    assert store.get_processor_roster() == {"mp-1": 2000.0}


def test_get_processor_roster_empty_when_nothing_seen(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    assert store.get_processor_roster() == {}


def test_get_processor_statuses_reflects_thresholds(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    now = 100000.0
    store.record_processor_seen("mp-green", now - 5)
    store.record_processor_seen("mp-amber", now - 30)
    store.record_processor_seen("mp-red", now - 120)

    statuses = {p["processor_id"]: p["status"] for p in store.get_processor_statuses(now=now)}
    assert statuses == {"mp-green": "green", "mp-amber": "amber", "mp-red": "red"}


def test_get_processor_statuses_sorted_by_processor_id(redis_client):
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    store.record_processor_seen("mp-2", 1.0)
    store.record_processor_seen("mp-1", 1.0)

    ids = [p["processor_id"] for p in store.get_processor_statuses(now=2.0)]
    assert ids == ["mp-1", "mp-2"]


def test_get_processor_statuses_defaults_now_to_current_time(redis_client):
    """Calling without an explicit `now` (the real GET /api/processors
    code path) must use the current wall clock, not treat every entry as
    infinitely old/new."""
    store = FlightStateStore(redis_client, stale_seconds=30, evict_seconds=300)
    store.record_processor_seen("mp-1", time.time())

    statuses = {p["processor_id"]: p["status"] for p in store.get_processor_statuses()}
    assert statuses["mp-1"] == "green"


def test_processor_roster_resets_on_fresh_redis_state():
    """Simulates a full map + map-redis container restart: this
    no-persistence Redis instance loses all data on its own restart (see
    map/README.md), which this module's autouse _clean_db fixture's
    flushdb() reproduces exactly -- a fresh FlightStateStore against that
    flushed Redis must report an empty roster, never a stale one."""
    # _clean_db's autouse flushdb() already ran before this test via the
    # module-scoped redis_client fixture; asserting against a brand-new
    # store instance (not just the same one reused) confirms there's no
    # in-memory roster state anywhere that could survive independently of
    # Redis.
    import redis as redis_module

    client = redis_module.Redis(
        host=_REDIS_HOST, port=_REDIS_PORT, db=_TEST_DB, decode_responses=True,
    )
    try:
        store = FlightStateStore(client, stale_seconds=30, evict_seconds=300)
        assert store.get_processor_roster() == {}
    finally:
        client.close()
