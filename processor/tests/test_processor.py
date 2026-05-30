"""
Tests for processor/main.py components that don't require live infrastructure.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import time
from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest

from processor.main import (
    Flight,
    Processor,
    _ArchiveFallbackQueue,
    _RateTracker,
    _TimeTracker,
    _category_to_wtc,
)
from shared.models import Position, Velocity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    from processor.main import _SCHEMA
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.executescript(_SCHEMA)
    return db


def _minimal_config() -> dict:
    return {
        "redis": {"host": "localhost"},
        "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        "flight_ttl_seconds": 300,
        "telemetry_interval_seconds": 30,
        "data_dir": tempfile.mkdtemp(),
    }


# ---------------------------------------------------------------------------
# _category_to_wtc
# ---------------------------------------------------------------------------

class TestCategoryToWtc:
    def test_all_known_categories(self):
        assert _category_to_wtc(1) == "Light"
        assert _category_to_wtc(2) == "Medium 1"
        assert _category_to_wtc(3) == "Medium 2"
        assert _category_to_wtc(4) == "High Vortex Aircraft"
        assert _category_to_wtc(5) == "Heavy"
        assert _category_to_wtc(6) == "High Performance"
        assert _category_to_wtc(7) == "Rotorcraft"

    def test_unknown_returns_none(self):
        assert _category_to_wtc(0) is None
        assert _category_to_wtc(8) is None
        assert _category_to_wtc(99) is None


# ---------------------------------------------------------------------------
# Flight SQLite operations
# ---------------------------------------------------------------------------

class TestFlight:
    def test_load_returns_false_for_unknown(self):
        db = _make_db()
        f = Flight(db)
        assert f.load("AAAAAA") is False

    def test_save_and_reload(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 2000.0
        f.total_messages = 42
        f.aircraft = {"icao_hex": "A8AE7F", "registration": "N659DL"}
        f.ident = "DAL659"
        f.source = "1090"
        f.save()

        f2 = Flight(db)
        assert f2.load("A8AE7F") is True
        assert f2.ident == "DAL659"
        assert f2.total_messages == 42
        assert f2.aircraft["registration"] == "N659DL"

    def test_add_position(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 1000.0
        f.total_messages = 1
        f.source = "1090"
        f.save()
        f.add_position(Position(timestamp=1000.0, latitude=40.0, longitude=-73.0, altitude=5000))

        f2 = Flight(db)
        f2.load("A8AE7F")
        assert len(f2.positions) == 1
        assert f2.positions[0].altitude == 5000

    def test_add_velocity(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "BBBBBB"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.source = "1090"
        f.save()
        f.add_velocity(Velocity(timestamp=1.0, velocity=450.0, heading=270.0, vertical_speed=500))

        f2 = Flight(db)
        f2.load("BBBBBB")
        assert len(f2.velocities) == 1
        assert f2.velocities[0].velocity == 450.0

    def test_delete_removes_all_rows(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "CCCCCC"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.source = "1090"
        f.save()
        f.add_position(Position(timestamp=1.0, latitude=0.0, longitude=0.0))
        f.add_velocity(Velocity(timestamp=1.0, velocity=100.0))
        f.delete()

        f2 = Flight(db)
        assert f2.load("CCCCCC") is False
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM positions WHERE icao_hex='CCCCCC'")
        assert cur.fetchone()[0] == 0

    def test_to_completed_flight_shape(self):
        from datetime import datetime
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1717100000.0
        f.last_message = 1717100060.0
        f.total_messages = 10
        f.aircraft = {"icao_hex": "A8AE7F", "registration": "N659DL", "military": False}
        f.ident = "DAL659"
        f.operator = {"airline_designator": "DAL", "source": "mictronics"}
        f.squawk = "1234"
        f.origin = "KATL"
        f.destination = "KLAX"
        f.matched_rules = ["rule_1"]
        f.source = "1090"
        f.save()

        cf = f.to_completed_flight()

        # _id is UUID-v7 string
        assert isinstance(cf.id, str)
        assert "-" in cf.id

        # military=False stripped
        assert "military" not in cf.aircraft

        # operator source key stripped
        assert "source" not in cf.operator

        # timestamps are UTC-aware datetimes
        assert cf.first_message.tzinfo is not None
        assert cf.destination == "KLAX"
        assert cf.matched_rules == ["rule_1"]
        assert cf.source == "1090"

    def test_to_completed_flight_no_aircraft_sets_icao_hex(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "FFFFFF"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.source = "978"
        f.save()
        cf = f.to_completed_flight()
        assert cf.aircraft["icao_hex"] == "FFFFFF"


# ---------------------------------------------------------------------------
# _ArchiveFallbackQueue
# ---------------------------------------------------------------------------

class TestArchiveFallbackQueue:
    def test_put_and_depth(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _ArchiveFallbackQueue(tmp.name)
            assert q.depth() == 0
            q.put('{"test": 1}')
            q.put('{"test": 2}')
            assert q.depth() == 2

    def test_drain_calls_publish_in_order(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _ArchiveFallbackQueue(tmp.name)
            q.put("first")
            q.put("second")
            published = []
            q.drain(published.append)
            assert published == ["first", "second"]
            assert q.depth() == 0

    def test_drain_stops_on_exception(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _ArchiveFallbackQueue(tmp.name)
            q.put("first")
            q.put("second")

            def fail(_):
                raise ConnectionError("gone")

            q.drain(fail)
            assert q.depth() == 2  # nothing removed

    def test_survives_reopen(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            path = tmp.name
        q = _ArchiveFallbackQueue(path)
        q.put("persistent")
        del q
        q2 = _ArchiveFallbackQueue(path)
        assert q2.depth() == 1


# ---------------------------------------------------------------------------
# _RateTracker
# ---------------------------------------------------------------------------

class TestRateTracker:
    def test_zero_rate_when_empty(self):
        rt = _RateTracker(window=30)
        assert rt.rate() == 0.0

    def test_rate_reflects_recent_messages(self):
        rt = _RateTracker(window=30)
        for _ in range(30):
            rt.record()
        assert 0.9 <= rt.rate() <= 1.1  # ~1/s


# ---------------------------------------------------------------------------
# _TimeTracker
# ---------------------------------------------------------------------------

class TestTimeTracker:
    def test_avg(self):
        tt = _TimeTracker()
        tt.record(100.0)
        tt.record(200.0)
        assert tt.avg_ms() == 150.0

    def test_hwm_resets_on_read(self):
        tt = _TimeTracker()
        tt.record_hwm(500)
        tt.record_hwm(200)
        assert tt.hwm_ms_and_reset() == 500
        assert tt.hwm_ms_and_reset() == 0  # reset

    def test_reset_clears_avg(self):
        tt = _TimeTracker()
        tt.record(100.0)
        tt.reset()
        assert tt.avg_ms() == 0.0


# ---------------------------------------------------------------------------
# Processor enrichment logic (unit tests with mocked Redis)
# ---------------------------------------------------------------------------

class TestProcessorEnrichment:
    def _make_processor(self):
        cfg = _minimal_config()
        with patch("processor.main.redis_lib.Redis") as MockRedis, \
             patch("processor.main.RulesEngine"), \
             patch.object(Processor, "_claim_processor_id"):
            mock_redis = MagicMock()
            MockRedis.return_value = mock_redis
            p = Processor(cfg, processor_id=0)
            p._redis = mock_redis
            p._db = _make_db()
            return p, mock_redis

    def test_enrich_aircraft_populates_from_redis(self):
        p, mock_redis = self._make_processor()
        aircraft_data = {"icao_hex": "A8AE7F", "registration": "N659DL", "type_designator": "B763"}
        mock_redis.get.return_value = json.dumps(aircraft_data)

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        p._enrich_aircraft(f)

        assert f.aircraft["registration"] == "N659DL"

    def test_enrich_aircraft_increments_miss_on_cache_miss(self):
        p, mock_redis = self._make_processor()
        mock_redis.get.return_value = None

        f = Flight(p._db)
        f.icao_hex = "ZZZZZZ"
        p._enrich_aircraft(f)

        mock_redis.incr.assert_called()
        # Should still set icao_hex on aircraft
        assert f.aircraft.get("icao_hex") == "ZZZZZZ"

    def test_enrich_operator_skips_us_tail_number(self):
        p, mock_redis = self._make_processor()
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "N12345"  # US registration
        f.aircraft = {}
        p._enrich_operator(f)
        mock_redis.get.assert_not_called()

    def test_enrich_operator_skips_military(self):
        p, mock_redis = self._make_processor()
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "DAL659"
        f.aircraft = {"military": True}
        p._enrich_operator(f)
        mock_redis.get.assert_not_called()

    def test_enrich_operator_extracts_prefix(self):
        p, mock_redis = self._make_processor()
        mock_redis.get.return_value = json.dumps({"airline_designator": "DAL", "name": "Delta"})

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "DAL659"
        f.aircraft = {}
        f.operator = {}
        p._enrich_operator(f)

        assert f.operator["airline_designator"] == "DAL"

    def test_enrich_flight_sets_origin_destination(self):
        p, mock_redis = self._make_processor()
        from shared.models import FlightEnrichment
        fe = FlightEnrichment(
            ident="DAL659",
            origin={"icao_code": "KATL"},
            destination={"icao_code": "KLAX"},
        )
        mock_redis.get.return_value = fe.model_dump_json()

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "DAL659"
        f.operator = {}
        p._enrich_flight(f)

        assert f.origin == "KATL"
        assert f.destination == "KLAX"
