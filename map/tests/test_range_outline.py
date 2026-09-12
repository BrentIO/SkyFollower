"""
Tests for map/range_outline.py against a live Redis (db 14, separate from
test_state_store.py's db 15) and a tmp snapshot directory.

The UTC date is controlled by monkeypatching map.range_outline._utc_today
so rollover/boot behaviour can be exercised without waiting for midnight.
"""

from __future__ import annotations

import json
import math
import os

import pytest

redis = pytest.importorskip("redis")

import map.range_outline as ro
from map.range_outline import OUTLINE_KEY, RangeOutlineStore

pytestmark = pytest.mark.xdist_group(name="map_range_outline")

_REDIS_HOST = os.environ.get("REDIS_TEST_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("REDIS_TEST_PORT", "6379"))
_TEST_DB = 14

# Center: 34.0N, 118.0W. One degree of latitude north is ~60 nm; going north
# is bearing 0.
_CENTER_LAT = 34.0
_CENTER_LON = -118.0
_EARTH_RADIUS_NM = 3440.065


def _dest(bearing_deg: float, nm: float) -> tuple[float, float]:
    """Destination lat/lon `nm` from center along `bearing_deg` -- the
    forward calc, so a test can place a point in an exact bearing bucket."""
    ang = nm / _EARTH_RADIUS_NM
    brg = math.radians(bearing_deg)
    phi1 = math.radians(_CENTER_LAT)
    lam1 = math.radians(_CENTER_LON)
    phi2 = math.asin(math.sin(phi1) * math.cos(ang) + math.cos(phi1) * math.sin(ang) * math.cos(brg))
    lam2 = lam1 + math.atan2(
        math.sin(brg) * math.sin(ang) * math.cos(phi1),
        math.cos(ang) - math.sin(phi1) * math.sin(phi2),
    )
    return math.degrees(phi2), math.degrees(lam2)


@pytest.fixture(scope="module")
def redis_client():
    client = redis.Redis(
        host=_REDIS_HOST, port=_REDIS_PORT, db=_TEST_DB,
        decode_responses=True, socket_connect_timeout=2,
    )
    try:
        client.ping()
    except (redis.exceptions.RedisError, OSError):
        pytest.skip(f"No Redis reachable at {_REDIS_HOST}:{_REDIS_PORT}")
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture(autouse=True)
def _clean(redis_client):
    redis_client.flushdb()
    yield


@pytest.fixture
def frozen_date(monkeypatch):
    holder = {"date": "2026-09-10"}
    monkeypatch.setattr(ro, "_utc_today", lambda: holder["date"])
    return holder


def _store(redis_client, tmp_path, center=(_CENTER_LAT, _CENTER_LON)):
    return RangeOutlineStore(
        redis_client,
        center_latitude=center[0] if center else None,
        center_longitude=center[1] if center else None,
        snapshot_dir=tmp_path,
        retention_seconds=30 * 86400,
    )


# -- accumulation -----------------------------------------------------------

def test_disabled_without_center(redis_client, tmp_path):
    store = _store(redis_client, tmp_path, center=None)
    assert not store.enabled
    store.record_position(35.0, -118.0, 30000)
    assert redis_client.exists(OUTLINE_KEY) == 0
    assert store.get_outline()["features"] == []


def test_records_farthest_point_per_bearing_band(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    # ~60 nm due north at FL300 -> bucket "0:20000-30000"
    store.record_position(35.0, -118.0, 25000)
    raw = redis_client.hget(OUTLINE_KEY, "0:20000-30000")
    point = json.loads(raw)
    assert point["nm"] == pytest.approx(60.0, abs=0.5)
    assert point["lat"] == 35.0 and point["lon"] == -118.0
    assert point["alt"] == 25000


def test_farther_replaces_nearer_but_not_vice_versa(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(35.0, -118.0, 25000)          # ~60 nm N
    store.record_position(34.5, -118.0, 25000)          # ~30 nm N -- nearer, ignored
    assert json.loads(redis_client.hget(OUTLINE_KEY, "0:20000-30000"))["lat"] == 35.0
    store.record_position(35.5, -118.0, 25000)          # ~90 nm N -- farther, wins
    assert json.loads(redis_client.hget(OUTLINE_KEY, "0:20000-30000"))["lat"] == 35.5


def test_rejects_position_beyond_max_range(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(40.0, -118.0, 30000)  # ~360 nm N, past MAX_RANGE_NM (325)
    assert redis_client.exists(OUTLINE_KEY) == 0


def test_first_detection_in_a_direction_is_accepted_at_any_range(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(*_dest(0, 250), 25000)  # empty bucket -> accepted (<= MAX_RANGE_NM)
    assert json.loads(redis_client.hget(OUTLINE_KEY, "0:20000-30000"))["nm"] == pytest.approx(250, abs=1)


def test_rejects_outlier_jump_without_neighbour_support(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(*_dest(0, 60), 25000)    # establish ~60 nm at bearing 0
    store.record_position(*_dest(0, 200), 25000)   # +140 over, no neighbour support
    assert json.loads(redis_client.hget(OUTLINE_KEY, "0:20000-30000"))["nm"] == pytest.approx(60, abs=1)


def test_accepts_outlier_jump_with_neighbour_support(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(*_dest(0, 60), 25000)    # bearing 0, ~60 nm
    store.record_position(*_dest(3, 190), 25000)   # bearing 3 (within the neighbour window), ~190 nm
    store.record_position(*_dest(0, 200), 25000)   # now allowed -- a neighbour supports ~190 nm
    assert json.loads(redis_client.hget(OUTLINE_KEY, "0:20000-30000"))["nm"] == pytest.approx(200, abs=1)


def test_outlier_neighbour_window_boundary_exactly_three_degrees_passes(redis_client, tmp_path, frozen_date):
    """The real angular tolerance of the outlier-neighbour guard must stay
    +/-3 degrees after switching bearing buckets to a half-degree index --
    a neighbour exactly 3.0 degrees away still supports the jump."""
    store = _store(redis_client, tmp_path)
    store.record_position(*_dest(0, 60), 25000)      # establish bearing 0 at ~60 nm
    store.record_position(*_dest(3.0, 190), 25000)   # neighbour exactly 3.0 degrees away, ~190 nm
    store.record_position(*_dest(0, 200), 25000)     # accepted -- neighbour at the boundary supports it
    assert json.loads(redis_client.hget(OUTLINE_KEY, "0:20000-30000"))["nm"] == pytest.approx(200, abs=1)


def test_outlier_neighbour_window_beyond_three_degrees_fails(redis_client, tmp_path, frozen_date):
    """A neighbour just past 3.0 degrees (3.5) must NOT support the jump --
    pins that doubling the bucket count didn't silently halve the real
    tolerance to +/-1.5 degrees."""
    store = _store(redis_client, tmp_path)
    store.record_position(*_dest(0, 60), 25000)      # establish bearing 0 at ~60 nm
    store.record_position(*_dest(3.5, 190), 25000)   # neighbour just outside the +/-3 degree window
    store.record_position(*_dest(0, 200), 25000)     # rejected -- no neighbour support within +/-3 degrees
    assert json.loads(redis_client.hget(OUTLINE_KEY, "0:20000-30000"))["nm"] == pytest.approx(60, abs=1)


def test_half_degree_bearing_lands_in_its_own_bucket(redis_client, tmp_path, frozen_date):
    """A point at a genuine half-degree bearing (23.5) must occupy its own
    distinct bucket -- index 47 -- rather than merging with the whole-degree
    buckets for 23 (index 46) or 24 (index 48)."""
    store = _store(redis_client, tmp_path)
    lat, lon = _dest(23.5, 80)
    store.record_position(lat, lon, 25000)
    point = json.loads(redis_client.hget(OUTLINE_KEY, "47:20000-30000"))
    assert point["nm"] == pytest.approx(80.0, abs=0.5)
    assert redis_client.hget(OUTLINE_KEY, "46:20000-30000") is None
    assert redis_client.hget(OUTLINE_KEY, "48:20000-30000") is None


def test_safety_ttl_is_set(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(35.0, -118.0, 25000)
    ttl = redis_client.ttl(OUTLINE_KEY)
    assert 0 < ttl <= 2 * 86400


# -- rollover -------------------------------------------------------------

def test_rollover_snapshots_old_day_and_starts_new_empty(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(35.0, -118.0, 25000)
    assert redis_client.hlen(OUTLINE_KEY) == 1

    frozen_date["date"] = "2026-09-11"
    store.record_position(35.2, -118.0, 25000)  # first packet of the new day triggers rollover

    assert (tmp_path / "2026-09-10.json").is_file()
    saved = json.loads((tmp_path / "2026-09-10.json").read_text())
    assert "0:20000-30000" in saved["buckets"]
    assert saved["date"] == "2026-09-10"
    # New day's hash holds only the new point.
    assert redis_client.hlen(OUTLINE_KEY) == 1
    assert json.loads(redis_client.hget(OUTLINE_KEY, "0:20000-30000"))["lat"] == 35.2


def test_tick_rolls_over_with_no_traffic(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(35.0, -118.0, 25000)
    frozen_date["date"] = "2026-09-11"
    store.tick()
    assert (tmp_path / "2026-09-10.json").is_file()
    assert redis_client.exists(OUTLINE_KEY) == 0


# -- snapshot / boot ----------------------------------------------------------

def test_snapshot_round_trips_through_disk(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(35.0, -118.0, 25000)
    store.record_position(34.0, -117.0, 5000)
    store.tick()  # writes 2026-09-10.json
    before = redis_client.hgetall(OUTLINE_KEY)

    redis_client.flushdb()
    fresh = _store(redis_client, tmp_path)
    fresh.load_from_disk()
    assert redis_client.hgetall(OUTLINE_KEY) == before


def test_boot_spanning_midnight_starts_new_day_empty(redis_client, tmp_path, frozen_date):
    # Day 10 accumulated and snapshotted.
    store = _store(redis_client, tmp_path)
    store.record_position(35.0, -118.0, 25000)
    store.tick()
    redis_client.flushdb()

    # Reboot: the process comes back on day 11, before any day-11 snapshot exists.
    frozen_date["date"] = "2026-09-11"
    fresh = _store(redis_client, tmp_path)
    fresh.load_from_disk()

    assert redis_client.exists(OUTLINE_KEY) == 0            # new day starts empty
    assert (tmp_path / "2026-09-10.json").is_file()          # yesterday's record kept


def test_cleanup_deletes_snapshots_past_retention(redis_client, tmp_path, frozen_date):
    (tmp_path / "2026-09-10.json").write_text('{"buckets":{}}')
    (tmp_path / "2026-07-01.json").write_text('{"buckets":{}}')   # >30 days before 09-10
    (tmp_path / "not-a-date.json").write_text("{}")
    store = _store(redis_client, tmp_path)
    store.record_position(35.0, -118.0, 25000)
    store.tick()
    assert (tmp_path / "2026-09-10.json").is_file()
    assert not (tmp_path / "2026-07-01.json").exists()
    assert (tmp_path / "not-a-date.json").is_file()          # untouched -- not a dated snapshot


def test_reload_after_redis_emptied(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    store.record_position(35.0, -118.0, 25000)
    store.tick()
    before = redis_client.hgetall(OUTLINE_KEY)

    redis_client.delete(OUTLINE_KEY)  # simulate a map-redis restart mid-day
    store.tick()
    assert redis_client.hgetall(OUTLINE_KEY) == before


# -- GeoJSON API ----------------------------------------------------------

def _fan(store, bearings_deg, band_alt=25000, nm=80.0):
    """Record one point at each given bearing, `nm` from center."""
    for brg in bearings_deg:
        lat, lon = _dest(brg, nm)
        store.record_position(lat, lon, band_alt)


def test_get_outline_builds_polygon_per_band_plus_envelope(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    _fan(store, [0, 90, 180, 270], band_alt=25000)
    _fan(store, [45, 135, 225], band_alt=5000)

    fc = store.get_outline()
    assert fc["type"] == "FeatureCollection"
    bands = {f["properties"]["band"] for f in fc["features"]}
    assert "20000-30000" in bands
    assert "5000-10000" in bands
    assert "envelope" in bands

    band_feature = next(f for f in fc["features"] if f["properties"]["band"] == "20000-30000")
    ring = band_feature["geometry"]["coordinates"][0]
    assert band_feature["geometry"]["type"] == "Polygon"
    assert ring[0] == ring[-1]                       # closed
    assert len(ring[0]) == 3                          # [lon, lat, alt]
    assert fc["properties"]["date"] == "2026-09-10"


def test_get_outline_point_count_reflects_half_degree_resolution(redis_client, tmp_path, frozen_date):
    """Four bearings only 0.5 degrees apart must all survive as distinct
    points -- under the old whole-degree scheme, 0/0.5 and 1.0/1.5 would
    have collapsed into just 2 buckets instead of 4, proving the outline
    now resolves up to 720 possible points rather than 360."""
    store = _store(redis_client, tmp_path)
    _fan(store, [0, 0.5, 1.0, 1.5], band_alt=25000, nm=80.0)
    fc = store.get_outline()
    assert fc["properties"]["point_count"] == 4
    band_feature = next(f for f in fc["features"] if f["properties"]["band"] == "20000-30000")
    assert band_feature["properties"]["point_count"] == 4


def test_get_outline_band_filter(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    _fan(store, [0, 90, 180, 270], band_alt=25000)
    _fan(store, [10, 100, 190], band_alt=5000)

    fc = store.get_outline(band="20000-30000")
    assert {f["properties"]["band"] for f in fc["features"]} == {"20000-30000"}

    env = store.get_outline(band="envelope")
    assert {f["properties"]["band"] for f in env["features"]} == {"envelope"}


def test_get_outline_skips_bands_with_fewer_than_three_points(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    _fan(store, [0, 90], band_alt=25000)  # only two bearings
    fc = store.get_outline()
    assert fc["features"] == []


def test_get_outline_from_snapshot_file(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    _fan(store, [0, 90, 180, 270])
    store.tick()

    frozen_date["date"] = "2026-09-11"  # move "today" forward
    fc = store.get_outline(date="2026-09-10")
    assert fc["properties"]["date"] == "2026-09-10"
    assert any(f["properties"]["band"] == "envelope" for f in fc["features"])


def test_get_outline_missing_date_raises(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    with pytest.raises(FileNotFoundError):
        store.get_outline(date="2020-01-01")


def test_get_outline_bad_date_raises_value_error(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    with pytest.raises(ValueError):
        store.get_outline(date="last-tuesday")


def test_available_dates(redis_client, tmp_path, frozen_date):
    (tmp_path / "2026-09-08.json").write_text('{"buckets":{}}')
    (tmp_path / "2026-09-10.json").write_text('{"buckets":{}}')
    store = _store(redis_client, tmp_path)
    assert store.available_dates() == ["today", "2026-09-10", "2026-09-08"]


def test_clear_today(redis_client, tmp_path, frozen_date):
    store = _store(redis_client, tmp_path)
    _fan(store, [0, 90, 180, 270])
    store.tick()
    assert (tmp_path / "2026-09-10.json").is_file()

    store.clear_today()
    assert redis_client.exists(OUTLINE_KEY) == 0
    assert not (tmp_path / "2026-09-10.json").exists()
