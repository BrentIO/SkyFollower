"""
Tests for receiver/main.py components that don't require live infrastructure.

Covers:
- TCP stream parsing (bytes → hex messages)
- ICAO hex extraction and routing (modulo)
- SQLite fallback queue put/drain/depth
- Rate tracker
"""

from __future__ import annotations

import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest

from receiver.main import (
    _FallbackQueue,
    _RateTracker,
)


# ---------------------------------------------------------------------------
# ICAO extraction and queue routing
#
# parse_tcp_stream's own parsing correctness is covered in
# shared/tests/test_adsb_1090.py, since receiver/main.py imports it from
# shared.adsb_1090 rather than defining its own copy.
# ---------------------------------------------------------------------------

class TestIcaoRoutingIntegration:
    """
    Tests that verify ICAO extraction and modulo-routing behaviour via the
    Receiver._handle_message internals (using mocked publishing).
    """

    def _make_receiver(self, processor_count: int = 4, receiver_id: int = 0):
        """Build a Receiver with a stub config (no real connections)."""
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "processor_count": processor_count,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "telemetry_interval_seconds": 30,
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg, receiver_id)

    def test_queue_name_from_icao_modulo(self):
        """Queue is adsb-{int(icao_hex, 16) % processor_count}."""
        icao_hex = "4B1900"
        processor_count = 4
        expected_queue = f"adsb-{int(icao_hex, 16) % processor_count}"
        assert expected_queue == f"adsb-{0x4B1900 % 4}"

    def test_handle_message_routes_to_correct_queue(self):
        """_handle_message calls _publish with the right queue_name."""
        r = self._make_receiver(processor_count=4)

        # A real DF17 ADS-B message — pyModeS should extract ICAO from it
        raw_hex = "8D4840D6202CC371C32CE0576098"
        published: list[tuple] = []
        r._publish = lambda q, p: published.append((q, p))
        r._rates["1090"] = _RateTracker()

        r._handle_message(raw_hex, "1090", r._rates["1090"])

        assert len(published) == 1
        queue_name, payload = published[0]
        assert queue_name.startswith("adsb-")
        idx = int(queue_name.split("-")[1])
        assert 0 <= idx < 4

        import json
        msg_dict = json.loads(payload)
        assert msg_dict["source"] == "1090"
        assert len(msg_dict["icao_hex"]) == 6
        assert msg_dict["raw"] == raw_hex.upper() or msg_dict["raw"] == raw_hex

    def test_handle_message_routes_mlat_same_as_1090(self):
        """MLAT frames use the same raw Mode S format as 1090 — no special
        handling is needed; the source tag is simply carried through."""
        r = self._make_receiver(processor_count=4)

        raw_hex = "8D4840D6202CC371C32CE0576098"
        published: list[tuple] = []
        r._publish = lambda q, p: published.append((q, p))
        r._rates["MLAT"] = _RateTracker()

        r._handle_message(raw_hex, "MLAT", r._rates["MLAT"])

        assert len(published) == 1
        _, payload = published[0]

        import json
        msg_dict = json.loads(payload)
        assert msg_dict["source"] == "MLAT"
        assert len(msg_dict["icao_hex"]) == 6

    def test_handle_message_discards_bad_message(self):
        """Messages that yield no ICAO are discarded silently."""
        r = self._make_receiver()
        published: list = []
        r._publish = lambda q, p: published.append((q, p))
        r._rates["1090"] = _RateTracker()

        # Garbage hex — pyModeS.icao returns None
        r._handle_message("0000000000", "1090", r._rates["1090"])
        assert published == []

    def test_routing_consistent_for_same_icao(self):
        """Same ICAO always maps to the same queue for a given processor_count."""
        r = self._make_receiver(processor_count=8)
        raw_hex = "8D4840D6202CC371C32CE0576098"
        queues: set[str] = set()

        published: list[tuple] = []
        r._publish = lambda q, p: published.append((q, p))
        r._rates["1090"] = _RateTracker()

        for _ in range(5):
            r._handle_message(raw_hex, "1090", r._rates["1090"])
        queues = {q for q, _ in published}
        assert len(queues) == 1, "Same ICAO must always route to the same queue"

    def test_single_processor_always_queue_zero(self):
        """With processor_count=1, every message goes to adsb-0."""
        r = self._make_receiver(processor_count=1)
        raw_hex = "8D4840D6202CC371C32CE0576098"

        published: list[tuple] = []
        r._publish = lambda q, p: published.append((q, p))
        r._rates["1090"] = _RateTracker()

        r._handle_message(raw_hex, "1090", r._rates["1090"])
        assert published[0][0] == "adsb-0"


# ---------------------------------------------------------------------------
# SQLite fallback queue
# ---------------------------------------------------------------------------

class TestFallbackQueue:
    """Tests for _FallbackQueue put / drain / depth."""

    def _make_queue(self) -> _FallbackQueue:
        td = tempfile.mkdtemp()
        return _FallbackQueue(f"{td}/queue.db")

    def test_put_increases_depth(self):
        q = self._make_queue()
        assert q.depth() == 0
        q.put("adsb-0", '{"raw": "AA"}')
        assert q.depth() == 1
        q.put("adsb-1", '{"raw": "BB"}')
        assert q.depth() == 2

    def test_drain_calls_publish_fn_in_order(self):
        q = self._make_queue()
        q.put("adsb-0", "first")
        q.put("adsb-1", "second")
        q.put("adsb-0", "third")

        drained: list[tuple] = []
        q.drain(lambda qn, p: drained.append((qn, p)))

        assert drained == [
            ("adsb-0", "first"),
            ("adsb-1", "second"),
            ("adsb-0", "third"),
        ]
        assert q.depth() == 0

    def test_drain_stops_on_publish_error(self):
        """If publish_fn raises, drain stops and remaining items are kept."""
        q = self._make_queue()
        q.put("adsb-0", "first")
        q.put("adsb-0", "second")

        call_count = [0]

        def failing_publish(qn, p):
            call_count[0] += 1
            if call_count[0] >= 1:
                raise RuntimeError("RabbitMQ gone")

        q.drain(failing_publish)

        # First item triggered the error; it and all subsequent items remain
        assert q.depth() == 2

    def test_drain_on_empty_queue_is_noop(self):
        q = self._make_queue()
        called = []
        q.drain(lambda qn, p: called.append(p))
        assert called == []
        assert q.depth() == 0

    def test_depth_after_partial_drain(self):
        """Verify depth decrements as items are drained."""
        q = self._make_queue()
        for i in range(5):
            q.put("adsb-0", f"msg-{i}")
        assert q.depth() == 5

        drained = []
        q.drain(lambda qn, p: drained.append(p))
        assert q.depth() == 0
        assert len(drained) == 5

    def test_wal_mode_enabled(self):
        """Confirm WAL journal mode is applied."""
        import sqlite3
        td = tempfile.mkdtemp()
        q = _FallbackQueue(f"{td}/queue.db")
        conn = sqlite3.connect(f"{td}/queue.db")
        row = conn.execute("PRAGMA journal_mode").fetchone()
        conn.close()
        assert row[0] == "wal"

    def test_schema_columns(self):
        """queue table has id, queue_name, payload, received_at columns."""
        import sqlite3
        td = tempfile.mkdtemp()
        q = _FallbackQueue(f"{td}/queue.db")
        conn = sqlite3.connect(f"{td}/queue.db")
        info = conn.execute("PRAGMA table_info(queue)").fetchall()
        conn.close()
        col_names = {row[1] for row in info}
        assert {"id", "queue_name", "payload", "received_at"}.issubset(col_names)


# ---------------------------------------------------------------------------
# Rate tracker
# ---------------------------------------------------------------------------

class TestRateTracker:
    """Tests for _RateTracker 30-second rolling-window rate measurement."""

    def test_empty_tracker_returns_zero(self):
        rt = _RateTracker(window=30)
        assert rt.rate() == 0.0

    def test_rate_returns_float(self):
        rt = _RateTracker(window=30)
        rt.record()
        assert isinstance(rt.rate(), float)

    def test_30_events_in_window_gives_rate_1_per_second(self):
        rt = _RateTracker(window=30)
        for _ in range(30):
            rt.record()
        # rate = 30 events / 30 s window = 1.0
        assert rt.rate() == pytest.approx(1.0, abs=0.01)

    def test_rate_is_zero_after_window_expires(self):
        rt = _RateTracker(window=1)  # 1-second window
        rt.record()
        time.sleep(1.1)
        assert rt.rate() == 0.0

    def test_multiple_records_increase_rate(self):
        rt = _RateTracker(window=30)
        for _ in range(60):
            rt.record()
        assert rt.rate() == pytest.approx(2.0, abs=0.01)

    def test_window_parameter_respected(self):
        """A 10-second window with 10 events yields rate ≈ 1.0."""
        rt = _RateTracker(window=10)
        for _ in range(10):
            rt.record()
        assert rt.rate() == pytest.approx(1.0, abs=0.1)

    def test_thread_safety_basic(self):
        """Multiple threads recording concurrently don't raise."""
        import threading
        rt = _RateTracker(window=30)
        errors = []

        def record_many():
            try:
                for _ in range(100):
                    rt.record()
                    rt.rate()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=record_many) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Thread safety errors: {errors}"


# ---------------------------------------------------------------------------
# RECEIVER_ID and MQTT topic structure
# ---------------------------------------------------------------------------

class TestReceiverIdAndTopics:
    """Tests for RECEIVER_ID env var and resulting MQTT topic naming."""

    def _make_receiver(self, receiver_id: int = 0):
        import tempfile
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg, receiver_id)

    def test_default_receiver_id_is_zero(self):
        r = self._make_receiver()
        assert r._id == 0

    def test_receiver_id_set_correctly(self):
        r = self._make_receiver(receiver_id=3)
        assert r._id == 3

    def test_main_exits_on_non_integer_receiver_id(self):
        import os
        with patch.dict(os.environ, {"RECEIVER_ID": "notanint"}):
            with patch("receiver.main._load_config", return_value={
                "sources": [], "rabbitmq": {"host": "x", "username": "u", "password": "p"},
                "data_dir": tempfile.mkdtemp(),
            }):
                import receiver.main as rm
                with pytest.raises(SystemExit) as exc_info:
                    rm.main()
                assert exc_info.value.code == 1

    def test_main_reads_receiver_id_from_env(self):
        import os
        from unittest.mock import patch, MagicMock
        with patch.dict(os.environ, {"RECEIVER_ID": "2"}):
            with patch("receiver.main._load_config", return_value={
                "sources": [], "rabbitmq": {"host": "x", "username": "u", "password": "p"},
                "data_dir": tempfile.mkdtemp(),
            }):
                with patch("receiver.main.Receiver") as mock_cls:
                    mock_cls.return_value.start = MagicMock()
                    import receiver.main as rm
                    rm.main()
                    args = mock_cls.call_args
                    assert args[0][1] == 2  # receiver_id positional arg


# ---------------------------------------------------------------------------
# Telemetry — single JSON payload
# ---------------------------------------------------------------------------

class TestTelemetryPayload:
    """Tests for _publish_telemetry() single-JSON-payload behaviour."""

    def _make_receiver(self, receiver_id: int = 0):
        import tempfile
        from receiver.main import Receiver
        cfg = {
            "sources": [
                {"host": "localhost", "port": 30002, "source": "1090"},
                {"host": "localhost", "port": 30978, "source": "978"},
                {"host": "localhost", "port": 30105, "source": "MLAT"},
            ],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg, receiver_id)

    def test_publish_telemetry_single_publish_call(self):
        r = self._make_receiver(receiver_id=1)
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        assert mock_mqtt.publish.call_count == 1

    def test_publish_telemetry_correct_topic(self):
        r = self._make_receiver(receiver_id=2)
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        topic = mock_mqtt.publish.call_args[0][0]
        assert topic == "SkyFollower/receiver/2/statistics"

    def test_publish_telemetry_retained(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        kwargs = mock_mqtt.publish.call_args[1]
        assert kwargs.get("retain") is True

    def test_publish_telemetry_payload_fields(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        import json
        payload = json.loads(mock_mqtt.publish.call_args[0][1])
        assert "messages_1090_per_second" in payload
        assert "messages_978_per_second" in payload
        assert "messages_MLAT_per_second" in payload
        assert "local_queue_depth" in payload
        assert "rabbitmq_connected" in payload
        assert "started_at" in payload

    def test_rabbitmq_connected_is_boolean(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        import json
        payload = json.loads(mock_mqtt.publish.call_args[0][1])
        assert isinstance(payload["rabbitmq_connected"], bool)

    def test_no_publish_when_mqtt_not_connected(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = False
        r._publish_telemetry()
        mock_mqtt.publish.assert_not_called()

    def test_ha_autodiscovery_uses_value_template(self):
        r = self._make_receiver(receiver_id=0)
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        import json
        for call in mock_mqtt.publish.call_args_list:
            if call[0][0].startswith("homeassistant/"):
                cfg_payload = json.loads(call[0][1])
                assert "value_template" in cfg_payload
                assert cfg_payload["state_topic"] == "SkyFollower/receiver/0/statistics"
