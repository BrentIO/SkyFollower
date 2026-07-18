"""
Tests for processor/main.py components that don't require live infrastructure.
"""

from __future__ import annotations

import json
import logging
import os
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
    _SCHEMA,
    _category_to_wtc,
)
from shared.models import InboundMessage, Position, Velocity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
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


def _make_processor(cfg: dict | None = None) -> tuple[Processor, MagicMock]:
    """Construct a real Processor (file-backed active store) with Redis/rules/
    processor-ID-claim mocked out, matching TestProcessorEnrichment's pattern
    but keeping the real on-disk DB instead of swapping in an in-memory one —
    needed for the crash-recovery/message-clock tests below."""
    cfg = cfg or _minimal_config()
    with patch("processor.main.redis_lib.Redis") as MockRedis, \
         patch("processor.main.RulesEngine"), \
         patch("processor.main.pathlib.Path"), \
         patch.object(Processor, "_claim_processor_id"):
        mock_redis = MagicMock()
        mock_redis.script_load.return_value = "abc123sha"
        MockRedis.return_value = mock_redis
        p = Processor(cfg, processor_id=0)
        p._redis = mock_redis
        p._merge_sha = "abc123sha"
        p._rules_engine.evaluate.return_value = []
        return p, mock_redis


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
             patch("processor.main.pathlib.Path"), \
             patch.object(Processor, "_claim_processor_id"):
            mock_redis = MagicMock()
            mock_redis.script_load.return_value = "abc123sha"
            MockRedis.return_value = mock_redis
            p = Processor(cfg, processor_id=0)
            p._redis = mock_redis
            p._merge_sha = "abc123sha"
            p._db = _make_db()
            return p, mock_redis

    def test_enrich_aircraft_populates_from_redis(self):
        p, mock_redis = self._make_processor()
        aircraft_data = {"icao_hex": "A8AE7F", "registration": "N659DL", "type_designator": "B763"}
        mock_redis.evalsha.return_value = json.dumps(aircraft_data)

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        p._enrich_aircraft(f)

        assert f.aircraft["registration"] == "N659DL"
        mock_redis.evalsha.assert_called_once_with("abc123sha", 0, "A8AE7F")

    def test_enrich_aircraft_increments_miss_on_cache_miss(self):
        p, mock_redis = self._make_processor()
        mock_redis.evalsha.return_value = None

        f = Flight(p._db)
        f.icao_hex = "ZZZZZZ"
        p._enrich_aircraft(f)

        mock_redis.incr.assert_called()
        assert f.aircraft.get("icao_hex") == "ZZZZZZ"

    def test_enrich_aircraft_no_registration_key_written(self):
        p, mock_redis = self._make_processor()
        aircraft_data = {"icao_hex": "A8AE7F", "registration": "N659DL"}
        mock_redis.evalsha.return_value = json.dumps(aircraft_data)

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        p._enrich_aircraft(f)

        for call in mock_redis.set.call_args_list:
            assert not str(call).startswith("registration:")

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


# ---------------------------------------------------------------------------
# Telemetry — single JSON payload
# ---------------------------------------------------------------------------

class TestTelemetryPayload:
    """Tests for _publish_telemetry() single-JSON-payload behaviour."""

    def _make_processor(self) -> Processor:
        with patch("processor.main.redis_lib.Redis"):
            p = Processor(_minimal_config(), processor_id=0)
        return p

    def test_single_publish_call(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_telemetry()
        assert mock_mqtt.publish.call_count == 1

    def test_correct_topic(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_telemetry()
        topic = mock_mqtt.publish.call_args[0][0]
        assert topic == "SkyFollower/processor/0/statistics"

    def test_retained(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_telemetry()
        assert mock_mqtt.publish.call_args[1].get("retain") is True

    def test_payload_fields(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_telemetry()
        payload = json.loads(mock_mqtt.publish.call_args[0][1])
        expected = {
            "started_at", "messages_per_second", "processing_time_hwm_ms",
            "rules_engine_hwm_ms", "rabbitmq_input_queue_depth",
            "local_archive_queue_depth", "active_flights",
            "registration_misses_hour", "registration_misses_today",
            "aircraft_type_misses_hour", "aircraft_type_misses_today",
        }
        assert expected.issubset(payload.keys())

    def test_processing_time_hwm_not_avg(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._processing_time.record(10.0)
        p._processing_time.record_hwm(10.0)
        p._processing_time.record(50.0)
        p._processing_time.record_hwm(50.0)
        p._publish_telemetry()
        payload = json.loads(mock_mqtt.publish.call_args[0][1])
        assert payload["processing_time_hwm_ms"] == 50

    def test_no_publish_when_mqtt_not_connected(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = False
        p._publish_telemetry()
        mock_mqtt.publish.assert_not_called()

    def test_ha_autodiscovery_uses_value_template(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        stats_topic = "SkyFollower/processor/0/statistics"
        for call in mock_mqtt.publish.call_args_list:
            if call[0][0].startswith("homeassistant/"):
                cfg = json.loads(call[0][1])
                assert "value_template" in cfg
                assert cfg["state_topic"] == stats_topic

    def test_ha_autodiscovery_no_avg_processing_time(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        for call in mock_mqtt.publish.call_args_list:
            assert "avg_processing_time_ms" not in call[0][0]


# ---------------------------------------------------------------------------
# Crash-durable active store
# ---------------------------------------------------------------------------

class TestCrashRecovery:
    """Active store is file-backed; a process restart (crash or deliberate
    stop, handled identically) must recover it without eagerly archiving
    based on wall-clock time elapsed while the process was down."""

    def _write_active_flights_db(self, data_dir, icao_hex, last_message, flight_id="pre-crash-id"):
        path = os.path.join(data_dir, "active_flights.db")
        db = sqlite3.connect(path)
        db.executescript(_SCHEMA)
        db.execute(
            "INSERT INTO flights (icao_hex, flight_id, first_message, last_message, "
            "total_messages, aircraft, ident, operator, squawk, origin, destination, "
            "matched_rules, source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (icao_hex, flight_id, last_message - 10, last_message, 5,
             "{}", "", "{}", "", None, None, '["rule_a"]', "1090"),
        )
        db.commit()
        db.close()

    def test_recovers_flight_without_archiving(self):
        data_dir = tempfile.mkdtemp()
        # 10 minutes old — would look wall-clock-stale against a 300s TTL,
        # the exact scenario a naive wall-clock eviction check gets wrong.
        old_last_message = time.time() - 600
        self._write_active_flights_db(data_dir, "A8AE7F", old_last_message)

        cfg = _minimal_config()
        cfg["data_dir"] = data_dir
        p, _ = _make_processor(cfg)

        # message_clock floors at the recovered flight's last_message, not
        # at wall-clock "now" — see Processor.__init__.
        assert p._message_clock == pytest.approx(old_last_message)

        f = Flight(p._db)
        assert f.load("A8AE7F") is True
        assert f.matched_rules == ["rule_a"]
        assert f.flight_id == "pre-crash-id"

        # A periodic eviction sweep right after startup must not archive it
        # — message_clock hasn't advanced past its TTL window yet.
        p._evict_stale()
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True

    def test_empty_store_uses_wall_clock(self):
        cfg = _minimal_config()
        p, _ = _make_processor(cfg)
        assert p._message_clock == pytest.approx(time.time(), abs=5)


# ---------------------------------------------------------------------------
# Per-message gap check
# ---------------------------------------------------------------------------

class TestPerMessageGapCheck:
    def test_gap_beyond_ttl_archives_old_and_starts_new(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None  # aircraft enrichment miss

        old_time = 1_700_000_000.0
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "old-flight-id"
        f.first_message = old_time - 100
        f.last_message = old_time
        f.total_messages = 5
        f.matched_rules = ["rule_a"]
        f.source = "1090"
        f.save()

        ttl = p._cfg.get("flight_ttl_seconds", 300)
        new_time = old_time + ttl + 50  # gap exceeds ttl

        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=new_time, source="1090")
        data = {"icao_hex": "A8AE7F"}

        with p._db_lock:
            p._update_flight(data, msg)

        # Old flight landed in the local fallback (no RabbitMQ connected in
        # this test — Processor was never start()ed).
        assert p._fallback.depth() == 1

        # A fresh row now exists for the same icao_hex, not an extension of
        # the old one.
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True
        assert f2.flight_id != "old-flight-id"
        assert f2.matched_rules == []
        assert f2.total_messages == 1
        assert f2.first_message == new_time

    def test_gap_within_ttl_extends_existing_flight(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        old_time = 1_700_000_000.0
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "same-flight-id"
        f.first_message = old_time - 100
        f.last_message = old_time
        f.total_messages = 5
        f.source = "1090"
        f.save()

        new_time = old_time + 10  # well within the default 300s ttl
        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=new_time, source="1090")
        data = {"icao_hex": "A8AE7F"}

        with p._db_lock:
            p._update_flight(data, msg)

        assert p._fallback.depth() == 0
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True
        assert f2.flight_id == "same-flight-id"
        assert f2.total_messages == 6


# ---------------------------------------------------------------------------
# Flight ID assigned at creation, not archive time
# ---------------------------------------------------------------------------

class TestFlightIdStability:
    def test_persists_across_save_and_reload(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "abc-123"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.source = "1090"
        f.save()

        f2 = Flight(db)
        f2.load("A8AE7F")
        assert f2.flight_id == "abc-123"

    def test_to_completed_flight_reuses_flight_id(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "abc-123"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.source = "1090"
        f.save()

        # A duplicate archive attempt (e.g. a crash between the archive
        # commit and the active-store delete) reuses the same _id, landing
        # as an idempotent overwrite of the same S3 object rather than a
        # duplicate record.
        cf1 = f.to_completed_flight()
        cf2 = f.to_completed_flight()
        assert cf1.id == "abc-123"
        assert cf2.id == "abc-123"


# ---------------------------------------------------------------------------
# message_clock gates eviction, not wall-clock time
# ---------------------------------------------------------------------------

class TestMessageClockGatesEviction:
    def test_does_not_evict_ahead_of_message_clock(self):
        p, _ = _make_processor()
        ttl = p._cfg.get("flight_ttl_seconds", 300)

        last_message = time.time() - 600  # 10 minutes old by wall-clock

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-1"
        f.first_message = last_message - 10
        f.last_message = last_message
        f.total_messages = 1
        f.source = "1090"
        f.save()

        # Backlog replay has only reached 100s past this flight's last
        # message so far — well within the ttl window, even though real
        # wall-clock time has moved on much further.
        p._message_clock = last_message + 100
        p._evict_stale()
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True  # not evicted

        # Once message_clock actually catches up past the ttl window, the
        # normal eviction outcome applies.
        p._message_clock = last_message + ttl + 1
        p._evict_stale()
        f3 = Flight(p._db)
        assert f3.load("A8AE7F") is False  # now evicted


# ---------------------------------------------------------------------------
# MQTT rule-notification flood guard
# ---------------------------------------------------------------------------

class TestMqttLagGuard:
    def _make_flight(self, p) -> Flight:
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-1"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.source = "1090"
        f.save()
        return f

    def test_suppresses_backlogged_message_notification(self, caplog):
        p, _ = _make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        f = self._make_flight(p)

        old_received_at = time.time() - 3600  # an hour old — backlog replay
        with caplog.at_level(logging.DEBUG, logger="processor"):
            p._publish_rule_notification(f, {"identifier": "rule_a"}, old_received_at)

        mock_mqtt.publish.assert_not_called()
        assert "rule_a" in caplog.text
        assert "A8AE7F" in caplog.text

    def test_publishes_recent_message_notification(self):
        p, _ = _make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        f = self._make_flight(p)

        recent_received_at = time.time() - 1
        p._publish_rule_notification(f, {"identifier": "rule_a"}, recent_received_at)
        mock_mqtt.publish.assert_called_once()
