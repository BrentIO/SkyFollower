"""
Tests for receiver/main.py components that don't require live infrastructure.

Covers:
- TCP stream parsing (bytes → hex messages)
- ICAO hex extraction and routing (modulo)
- The receiver-specific queue_name wrap/unwrap around shared.FallbackQueue
  (the queue itself -- put/drain/depth/dead-lettering -- is covered in
  shared/tests/test_fallback_queue.py)
- Rate tracker
"""

from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from receiver.main import (
    _RateTracker,
    parse_978_line,
)


# ---------------------------------------------------------------------------
# _source_loop dispatch — 978 vs. 1090
#
# parse_978_line's own parsing correctness is covered in
# shared/tests/test_uat.py, since receiver/main.py imports it from
# shared.uat rather than defining its own copy. These tests only cover
# receiver-specific behavior: dispatch and message routing.
# ---------------------------------------------------------------------------

class TestSourceLoopDispatch:
    """Confirms _source_loop routes to the 978-specific reader only for
    source == "978", and 1090/other sources are unaffected."""

    def _make_receiver(self, source: str):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 1, "source": source}],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg)

    def _run_dispatch(self, r, source: str):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        host, port = server.getsockname()

        def _accept_then_close():
            conn, _ = server.accept()
            conn.close()

        acceptor = threading.Thread(target=_accept_then_close, daemon=True)
        acceptor.start()

        # Setting _shutdown from within the mocked reader is what ends
        # _source_loop's outer reconnect loop -- otherwise it would spin
        # forever trying to reconnect after the mocked "read" returns.
        calls = []

        def _record_1090(*a, **k):
            calls.append("1090")
            r._shutdown.set()

        def _record_978(*a, **k):
            calls.append("978")
            r._shutdown.set()

        r._read_1090_stream = _record_1090
        r._read_978_stream = _record_978

        r._source_loop({"host": host, "port": port, "source": source})
        acceptor.join(timeout=5)
        server.close()
        return calls

    def test_978_source_dispatches_to_978_reader(self):
        r = self._make_receiver("978")
        calls = self._run_dispatch(r, "978")
        assert calls == ["978"]

    def test_1090_source_dispatches_to_1090_reader(self):
        r = self._make_receiver("1090")
        calls = self._run_dispatch(r, "1090")
        assert calls == ["1090"]

    def test_mlat_source_dispatches_to_1090_reader(self):
        r = self._make_receiver("MLAT")
        calls = self._run_dispatch(r, "MLAT")
        assert calls == ["1090"]


class TestConnectionConnectedState:
    """Tests for the per-connection live up/down state (_connected dict)
    _source_loop tracks: True while the TCP socket is open, False on any
    connection error or detected closed connection."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 1, "source": "1090"}],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg)

    def test_connected_true_while_reader_runs_then_false_after(self):
        r = self._make_receiver()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        host, port = server.getsockname()
        key = (host, port)
        r._connected[key] = False

        def _accept_then_close():
            conn, _ = server.accept()
            conn.close()

        acceptor = threading.Thread(target=_accept_then_close, daemon=True)
        acceptor.start()

        seen_connected = []

        def _fake_reader(*a, **k):
            seen_connected.append(r._connected[key])
            r._shutdown.set()

        r._read_1090_stream = _fake_reader
        r._source_loop({"host": host, "port": port, "source": "1090"})
        acceptor.join(timeout=5)
        server.close()

        assert seen_connected == [True]
        assert r._connected[key] is False

    def test_connected_false_after_connection_error(self):
        r = self._make_receiver()
        key = ("localhost", 1)
        r._connected[key] = True  # simulate a prior successful connection

        def _fake_sleep(seconds):
            r._shutdown.set()

        with patch("receiver.main.time.sleep", _fake_sleep):
            r._source_loop({"host": "localhost", "port": 1, "source": "1090"})

        assert r._connected[key] is False


class TestReconnectCounter:
    """Tests for the per-connection reconnect count -- distinguishes a
    rock-solid connection from one that's currently connected but
    flapping every few minutes."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 1, "source": "1090"}],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg)

    def test_increments_on_repeated_connection_errors(self):
        r = self._make_receiver()
        key = ("localhost", 1)
        attempts = {"n": 0}

        def _fake_sleep(seconds):
            attempts["n"] += 1
            if attempts["n"] >= 3:
                r._shutdown.set()

        with patch("receiver.main.time.sleep", _fake_sleep):
            r._source_loop({"host": "localhost", "port": 1, "source": "1090"})

        assert r._reconnect_counts[key] == 3

    def test_increments_after_closed_connection_break(self):
        r = self._make_receiver()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        host, port = server.getsockname()
        key = (host, port)
        r._reconnect_counts[key] = 0

        def _accept_then_close():
            conn, _ = server.accept()
            conn.close()

        acceptor = threading.Thread(target=_accept_then_close, daemon=True)
        acceptor.start()

        def _fake_reader(*a, **k):
            pass  # simulates the closed-connection break -- returns cleanly

        def _fake_sleep(seconds):
            r._shutdown.set()

        r._read_1090_stream = _fake_reader
        with patch("receiver.main.time.sleep", _fake_sleep):
            r._source_loop({"host": host, "port": port, "source": "1090"})
        acceptor.join(timeout=5)
        server.close()

        assert r._reconnect_counts[key] == 1

    def test_does_not_increment_on_clean_shutdown_without_drop(self):
        """If the reader itself requests shutdown (a real stop, not a
        drop), there's nothing to retry -- must not count as a reconnect."""
        r = self._make_receiver()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        host, port = server.getsockname()
        key = (host, port)

        def _accept_then_close():
            conn, _ = server.accept()
            conn.close()

        acceptor = threading.Thread(target=_accept_then_close, daemon=True)
        acceptor.start()

        def _fake_reader(*a, **k):
            r._shutdown.set()

        r._read_1090_stream = _fake_reader
        r._source_loop({"host": host, "port": port, "source": "1090"})
        acceptor.join(timeout=5)
        server.close()

        assert r._reconnect_counts.get(key, 0) == 0


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

    def _make_receiver(self, processor_count: int = 4):
        """Build a Receiver with a stub config (no real connections)."""
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "processor_count": processor_count,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "telemetry_interval_seconds": 30,
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg)

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

    def test_handle_978_message_routes_correctly(self):
        """978 UAT messages skip pyModeS entirely -- icao_hex/received_at come
        from parse_978_line, not from decoding raw as Mode S."""
        r = self._make_receiver(processor_count=4)

        raw_hex, icao_hex, received_at = parse_978_line(
            "-00a3d3e328a71f8c647004e9009c2d401a00;rs=6;rssi=0.3;t=1782561034.334;"
        )
        published: list[tuple] = []
        r._publish = lambda q, p: published.append((q, p))
        r._rates["978"] = _RateTracker()

        r._handle_978_message(raw_hex, icao_hex, received_at, "978", r._rates["978"])

        assert len(published) == 1
        _, payload = published[0]

        import json
        msg_dict = json.loads(payload)
        assert msg_dict["source"] == "978"
        assert msg_dict["icao_hex"] == "A3D3E3"
        assert msg_dict["raw"] == "-00A3D3E328A71F8C647004E9009C2D401A00"
        assert msg_dict["received_at"] == 1782561034.334

    def test_handle_978_message_discards_bad_icao_length(self):
        r = self._make_receiver()
        published: list = []
        r._publish = lambda q, p: published.append((q, p))
        r._rates["978"] = _RateTracker()

        r._handle_978_message("-BAD", "SHORT", time.time(), "978", r._rates["978"])
        assert published == []

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

class TestFallbackPutWrapsQueueName:
    """FallbackQueue (shared/fallback_queue.py) is payload-only -- Receiver
    wraps queue_name into the JSON payload it puts, since queue_name is the
    target RabbitMQ routing key computed once at insert time and has to
    survive being persisted alongside the payload rather than recomputed
    on drain."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg)

    def test_fallback_put_wraps_queue_name_and_payload(self):
        r = self._make_receiver()
        r._fallback_put("adsb-0", '{"raw": "AA"}')
        assert r._fallback.depth() == 1

        captured = []
        r._fallback.drain(captured.append)
        item = json.loads(captured[0])
        assert item == {"queue_name": "adsb-0", "payload": '{"raw": "AA"}'}


# ---------------------------------------------------------------------------
# Receiver._drain_fallback — and its periodic-tick trigger from
# _telemetry_loop, alongside the existing RabbitMQ-reconnect trigger
# ---------------------------------------------------------------------------

def _synchronous_drain_thread():
    """Patch threading.Thread so drain_in_background's spawned thread runs
    synchronously in the caller's thread instead of racing the test's own
    assertions against a real background thread. Production code still
    spawns a genuine thread; this only affects the test."""
    class _ImmediateThread:
        def __init__(self, target=None, daemon=None, name=None):
            self._target = target

        def start(self):
            if self._target:
                self._target()

    return patch("receiver.main.threading.Thread", _ImmediateThread)


class TestDrainFallback:
    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg)

    def test_drain_fallback_publishes_queued_items(self):
        r = self._make_receiver()
        r._fallback_put("adsb-0", '{"raw": "AA"}')
        mock_channel = MagicMock()
        r._rmq_channel = mock_channel

        with _synchronous_drain_thread():
            r._drain_fallback()

        assert r._fallback.depth() == 0
        mock_channel.basic_publish.assert_called_once()
        assert mock_channel.basic_publish.call_args.kwargs["routing_key"] == "adsb-0"

    def test_drain_fallback_leaves_items_queued_if_channel_gone(self):
        r = self._make_receiver()
        r._fallback_put("adsb-0", '{"raw": "AA"}')
        r._rmq_channel = None

        with _synchronous_drain_thread():
            r._drain_fallback()

        assert r._fallback.depth() == 1

    def test_drain_fallback_resets_rmq_connected_on_publish_failure(self):
        """A failed basic_publish during draining is just as much evidence
        the connection is broken as a failed live-path publish — mirror
        _publish()'s handling."""
        r = self._make_receiver()
        r._fallback_put("adsb-0", '{"raw": "AA"}')
        mock_channel = MagicMock()
        mock_channel.basic_publish.side_effect = RuntimeError("boom")
        r._rmq_channel = mock_channel
        r._rmq_connected = True

        with _synchronous_drain_thread():
            r._drain_fallback()

        assert r._rmq_connected is False
        assert r._fallback.depth() == 1

    def _run_one_telemetry_tick(self, r) -> None:
        """Run the real _telemetry_loop for exactly one iteration, by
        making the mocked time.sleep set _shutdown so the loop body runs
        once and then exits — rather than re-implementing the loop's
        conditional in the test, which wouldn't actually exercise it."""
        def fake_sleep(_seconds):
            r._shutdown.set()

        with patch("receiver.main.time.sleep", side_effect=fake_sleep), \
             patch.object(r, "_publish_telemetry"):
            r._telemetry_loop()

    def test_telemetry_loop_drains_when_connected(self):
        """The periodic tick must attempt a drain when RabbitMQ is
        connected — this is what lets a stuck/missed reconnect-triggered
        drain still recover on the next tick."""
        r = self._make_receiver()
        r._fallback_put("adsb-0", '{"raw": "AA"}')
        r._rmq_channel = MagicMock()
        r._rmq_connected = True

        with _synchronous_drain_thread():
            self._run_one_telemetry_tick(r)

        assert r._fallback.depth() == 0

    def test_telemetry_tick_does_not_drain_when_disconnected(self):
        r = self._make_receiver()
        r._fallback_put("adsb-0", '{"raw": "AA"}')
        r._rmq_connected = False

        self._run_one_telemetry_tick(r)

        assert r._fallback.depth() == 1


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
# Receiver identity: auto-generated, persisted, decoupled from name
# ---------------------------------------------------------------------------

class TestLoadOrCreateReceiverId:
    """Tests for _load_or_create_receiver_id() in isolation."""

    def test_generates_a_uuid_like_id(self):
        from receiver.main import _load_or_create_receiver_id
        rid = _load_or_create_receiver_id(tempfile.mkdtemp())
        assert len(rid) == 36
        assert rid.count("-") == 4

    def test_persists_across_calls_with_the_same_data_dir(self):
        from receiver.main import _load_or_create_receiver_id
        data_dir = tempfile.mkdtemp()
        first = _load_or_create_receiver_id(data_dir)
        second = _load_or_create_receiver_id(data_dir)
        assert first == second

    def test_different_data_dirs_get_different_ids(self):
        from receiver.main import _load_or_create_receiver_id
        first = _load_or_create_receiver_id(tempfile.mkdtemp())
        second = _load_or_create_receiver_id(tempfile.mkdtemp())
        assert first != second

    def test_writes_id_to_receiver_id_file(self):
        from receiver.main import _load_or_create_receiver_id
        data_dir = tempfile.mkdtemp()
        rid = _load_or_create_receiver_id(data_dir)
        path = os.path.join(data_dir, "receiver_id")
        assert os.path.exists(path)
        assert open(path).read().strip() == rid


class TestReceiverIdAndTopics:
    """Tests for the Receiver-level identity and resulting MQTT topic naming."""

    def _make_receiver(self, data_dir: str | None = None, name: str | None = None):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": data_dir or tempfile.mkdtemp(),
        }
        if name is not None:
            cfg["name"] = name
        return Receiver(cfg)

    def test_id_is_auto_generated(self):
        r = self._make_receiver()
        assert len(r._id) == 36

    def test_id_persists_across_restarts_with_same_data_dir(self):
        data_dir = tempfile.mkdtemp()
        r1 = self._make_receiver(data_dir=data_dir)
        r2 = self._make_receiver(data_dir=data_dir)
        assert r1._id == r2._id

    def test_name_defaults_to_none_when_unset(self):
        r = self._make_receiver()
        assert r._name is None

    def test_name_read_from_config_when_set(self):
        r = self._make_receiver(name="Attic 1090")
        assert r._name == "Attic 1090"

    def test_main_constructs_receiver_from_config_only(self):
        """main() no longer reads any RECEIVER_ID env var -- Receiver is
        constructed from config alone."""
        with patch("receiver.main._load_config", return_value={
            "sources": [], "rabbitmq": {"host": "x", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }):
            with patch("receiver.main.Receiver") as mock_cls:
                mock_cls.return_value.start = MagicMock()
                import receiver.main as rm
                rm.main()
                mock_cls.assert_called_once()
                args, kwargs = mock_cls.call_args
                assert len(args) == 1 and not kwargs


# ---------------------------------------------------------------------------
# Telemetry — one retained topic per stat
# ---------------------------------------------------------------------------

class TestTelemetryPayload:
    """Tests for _publish_telemetry()'s one-retained-topic-per-stat behaviour."""

    def _make_receiver(self):
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
        return Receiver(cfg)

    def test_publish_telemetry_correct_base_topic(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        topics = [c.args[0] for c in mock_mqtt.publish.call_args_list]
        assert all(t.startswith(f"SkyFollower/receiver/{r._id}/statistic/") for t in topics)

    def test_publish_telemetry_retained(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        for call in mock_mqtt.publish.call_args_list:
            assert call.kwargs.get("retain") is True

    def test_publish_telemetry_payload_fields(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        base = f"SkyFollower/receiver/{r._id}/statistic"
        topics = {c.args[0] for c in mock_mqtt.publish.call_args_list}
        assert f"{base}/messages_localhost_30002_per_second" in topics
        assert f"{base}/messages_localhost_30978_per_second" in topics
        assert f"{base}/messages_localhost_30105_per_second" in topics
        assert f"{base}/localhost_30002_connected" in topics
        assert f"{base}/localhost_30978_connected" in topics
        assert f"{base}/localhost_30105_connected" in topics
        assert f"{base}/localhost_30002_reconnect_count" in topics
        assert f"{base}/localhost_30978_reconnect_count" in topics
        assert f"{base}/localhost_30105_reconnect_count" in topics
        assert f"{base}/local_queue_depth" in topics
        assert f"{base}/rabbitmq_connected" in topics
        assert f"{base}/started_at" in topics

    def test_connection_connected_value(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._connected[("localhost", 30002)] = True
        r._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        base = f"SkyFollower/receiver/{r._id}/statistic"
        assert calls[f"{base}/localhost_30002_connected"] == "True"
        assert calls[f"{base}/localhost_30978_connected"] == "False"

    def test_connection_reconnect_count_value(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._reconnect_counts[("localhost", 30002)] = 4
        r._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        base = f"SkyFollower/receiver/{r._id}/statistic"
        assert calls[f"{base}/localhost_30002_reconnect_count"] == "4"
        assert calls[f"{base}/localhost_30978_reconnect_count"] == "0"

    def test_rabbitmq_connected_value(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._rmq_connected = True
        r._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert calls[f"SkyFollower/receiver/{r._id}/statistic/rabbitmq_connected"] == "True"

    def test_no_publish_when_mqtt_not_connected(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = False
        r._publish_telemetry()
        mock_mqtt.publish.assert_not_called()

    def test_ha_autodiscovery_uses_direct_state_topic(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        for call in mock_mqtt.publish.call_args_list:
            if call.args[0].startswith("homeassistant/"):
                cfg_payload = json.loads(call.args[1])
                assert "value_template" not in cfg_payload
                assert cfg_payload["state_topic"].startswith(
                    f"SkyFollower/receiver/{r._id}/statistic/"
                )

    def test_ha_device_configuration_url_points_to_own_docs_page(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        for call in mock_mqtt.publish.call_args_list:
            if call.args[0].startswith("homeassistant/"):
                cfg_payload = json.loads(call.args[1])
                assert cfg_payload["device"]["configuration_url"] == (
                    "https://brentio.github.io/SkyFollower/components/receiver.html"
                )


class TestPublishTelemetryVersion:
    """Tests for the version MQTT statistic published alongside started_at."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }
        return Receiver(cfg)

    def test_reads_version_env_var(self):
        with patch.dict(os.environ, {"VERSION": "2026.08.01"}):
            r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True

        r._publish_telemetry()

        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert calls[f"SkyFollower/receiver/{r._id}/statistic/version"] == "2026.08.01"

    def test_falls_back_to_dev_when_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("VERSION", None)
            r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True

        r._publish_telemetry()

        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert calls[f"SkyFollower/receiver/{r._id}/statistic/version"] == "dev"


class TestHaDeviceNameFallback:
    """Tests for the friendly-name vs. auto-generated-id-fallback display
    label used in HA name/model/sensor labels."""

    def _make_receiver(self, name: str | None = None):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "processor_count": 1,
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "data_dir": tempfile.mkdtemp(),
        }
        if name is not None:
            cfg["name"] = name
        return Receiver(cfg)

    def test_uses_friendly_name_when_set(self):
        r = self._make_receiver(name="Attic 1090")
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        payload = json.loads(mock_mqtt.publish.call_args_list[0].args[1])
        assert payload["device"]["name"] == "SkyFollower Attic 1090"
        assert payload["device"]["model"] == "Attic 1090"

    def test_falls_back_to_short_id_when_name_unset(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        payload = json.loads(mock_mqtt.publish.call_args_list[0].args[1])
        assert payload["device"]["model"] == f"Receiver {r._id[:8]}"

    def test_identifier_and_topics_use_full_id_regardless_of_name(self):
        """The full persisted UUID stays the stable identifier/topic key
        even when a short friendly name is displayed instead."""
        r = self._make_receiver(name="Attic 1090")
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        payload = json.loads(mock_mqtt.publish.call_args_list[0].args[1])
        assert payload["device"]["ids"] == f"SkyFollower_receiver_{r._id}"
        assert payload["state_topic"].startswith(f"SkyFollower/receiver/{r._id}/statistic/")
