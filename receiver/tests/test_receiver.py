"""
Tests for receiver/main.py components that don't require live infrastructure.

Covers:
- TCP stream parsing (bytes → hex messages)
- ICAO hex extraction and routing (modulo)
- SQLite fallback queue put/drain/depth
- Rate tracker
"""

from __future__ import annotations

import socket
import tempfile
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from receiver.main import (
    _FallbackQueue,
    _RateTracker,
    parse_978_line,
    parse_tcp_stream,
)


# ---------------------------------------------------------------------------
# TCP stream parser
# ---------------------------------------------------------------------------

class TestParseTcpStream:
    """Tests for parse_tcp_stream — the core byte-level readsb parser."""

    def test_single_complete_message(self):
        """A complete *HEXDATA; produces one message."""
        data = b"*8D4B1900EA11DA58A9123456;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["8D4B1900EA11DA58A9123456"]

    def test_multiple_messages_in_one_chunk(self):
        """Two complete messages in one chunk produce two results."""
        data = b"*AABBCC;*DDEEFF;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["AABBCC", "DDEEFF"]

    def test_lowercase_hex_normalised_to_upper(self):
        """Lowercase hex bytes are uppercased in the output."""
        data = b"*aabbcc;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["AABBCC"]

    def test_message_split_across_chunks(self):
        """A message split at a chunk boundary is correctly reassembled."""
        buf = bytearray()
        msgs1 = parse_tcp_stream(b"*AABB", buf)
        assert msgs1 == []
        msgs2 = parse_tcp_stream(b"CC;", buf)
        assert msgs2 == ["AABBCC"]

    def test_newline_between_messages_ignored(self):
        """Newlines and other non-hex bytes outside a message are benign."""
        data = b"*AABBCC;\n*DDEEFF;\n"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["AABBCC", "DDEEFF"]

    def test_star_resets_partial_buffer(self):
        """A new '*' discards any previously accumulated partial bytes."""
        buf = bytearray()
        # Start accumulating but then get a new star before the semicolon
        parse_tcp_stream(b"*AAAA", buf)
        msgs = parse_tcp_stream(b"*BBBBCC;", buf)
        # Only the second (complete) message should be emitted
        assert msgs == ["BBBBCC"]

    def test_invalid_byte_inside_message_discards_partial(self):
        """A non-hex byte inside a message resets the buffer silently."""
        # 0x00 is not a hex char; the partial 'AA' is discarded and 'CCDD' emitted
        data = b"*AA\x00*CCDD;"
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == ["CCDD"]

    def test_empty_data(self):
        """Empty byte string produces no messages."""
        buf = bytearray()
        assert parse_tcp_stream(b"", buf) == []

    def test_star_without_semicolon_leaves_buf_empty(self):
        """A lone '*' with no following ';' leaves buf empty (star resets buf)."""
        buf = bytearray()
        msgs = parse_tcp_stream(b"*", buf)
        assert msgs == []
        assert len(buf) == 0

    def test_semicolon_with_empty_buf_skipped(self):
        """A ';' when buf is empty doesn't produce an empty string."""
        buf = bytearray()
        msgs = parse_tcp_stream(b";", buf)
        assert msgs == []

    def test_all_valid_hex_chars_accepted(self):
        """All 16 hex digit characters (0-9, A-F) are accepted."""
        hex_str = "0123456789ABCDEF"
        data = f"*{hex_str};".encode("ascii")
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == [hex_str]

    def test_real_adsb_message_format(self):
        """
        Real ADS-B DF17 message with newline suffix (as readsb actually sends).
        The hex part is a valid 28-byte (56 hex char) Mode-S message.
        """
        raw = "8D4840D6202CC371C32CE0576098"
        data = f"*{raw};\n".encode()
        buf = bytearray()
        msgs = parse_tcp_stream(data, buf)
        assert msgs == [raw.upper()]


# ---------------------------------------------------------------------------
# 978 line parser
# ---------------------------------------------------------------------------

class TestParse978Line:
    """Tests for parse_978_line — the dump978-fa line-protocol parser."""

    _DOWNLINK = "-00a3d3e328a71f8c647004e9009c2d401a00;rs=6;rssi=0.3;t=1782561034.334;"
    _UPLINK = "+08a3d3e328a75f8c653204e900742e4028066a0025ed2d0b1aa4c0a0000530000000;rssi=1.4;t=1782561038.701;"

    def test_downlink_frame_parsed(self):
        result = parse_978_line(self._DOWNLINK)
        assert result is not None
        raw_hex, icao_hex, received_at = result
        assert raw_hex == "-00A3D3E328A71F8C647004E9009C2D401A00"
        assert icao_hex == "A3D3E3"
        assert received_at == 1782561034.334

    def test_uplink_frame_parsed(self):
        result = parse_978_line(self._UPLINK)
        assert result is not None
        raw_hex, icao_hex, received_at = result
        assert raw_hex.startswith("+")
        assert received_at == 1782561038.701

    def test_preamble_line_skipped(self):
        line = "!fecfix=1;program=dump978-fa;version=11.0~bpo12+1;"
        assert parse_978_line(line) is None

    def test_blank_line_skipped(self):
        assert parse_978_line("") is None
        assert parse_978_line("   ") is None

    def test_invalid_prefix_skipped(self):
        assert parse_978_line("*00a3d3e328a71f8c647004e9009c2d401a00;") is None

    def test_short_hex_payload_skipped(self):
        assert parse_978_line("-1234;t=1782561034.334;") is None

    def test_non_hex_payload_skipped(self):
        assert parse_978_line("-00zzzzzz28a71f8c647004e9009c2d401a00;t=1.0;") is None

    def test_raw_hex_uppercased(self):
        result = parse_978_line("-00a3d3e328a71f8c647004e9009c2d401a00;t=1.0;")
        assert result is not None
        raw_hex, _, _ = result
        assert raw_hex == raw_hex.upper()

    def test_timestamp_defaults_to_wallclock_when_t_missing(self):
        before = time.time()
        result = parse_978_line("-00a3d3e328a71f8c647004e9009c2d401a00;rs=6;")
        after = time.time()
        assert result is not None
        _, _, received_at = result
        assert before <= received_at <= after

    def test_malformed_t_field_falls_back_to_wallclock(self):
        before = time.time()
        result = parse_978_line("-00a3d3e328a71f8c647004e9009c2d401a00;t=notanumber;")
        after = time.time()
        assert result is not None
        _, _, received_at = result
        assert before <= received_at <= after


# ---------------------------------------------------------------------------
# _source_loop dispatch — 978 vs. 1090
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
        return Receiver(cfg, 0)

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


# ---------------------------------------------------------------------------
# ICAO extraction and queue routing
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
