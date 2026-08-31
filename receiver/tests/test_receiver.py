"""
Tests for receiver/main.py components that don't require live infrastructure.

Covers:
- TCP stream parsing (bytes → hex messages)
- ICAO hex extraction and the routing key handed to the consistent-hash exchange
- The receiver-specific routing_key wrap/unwrap around shared.FallbackQueue
  (the queue itself -- put/drain/depth/dead-lettering -- is covered in
  shared/tests/test_fallback_queue.py)
- Rate tracker
"""

from __future__ import annotations

import json
import logging
import os
import queue
import re
import socket
import tempfile
import threading
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import receiver.main as receiver_main
from receiver.main import (
    _RateTracker,
    _enable_tcp_keepalive,
    _sanitize_mqtt_id,
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
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
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

    def test_external_source_dispatches_to_1090_reader(self):
        r = self._make_receiver("EXTERNAL")
        calls = self._run_dispatch(r, "EXTERNAL")
        assert calls == ["1090"]


class TestConnectionConnectedState:
    """Tests for the per-connection live up/down state (_connected dict)
    _source_loop tracks: True while the TCP socket is open, False on any
    connection error or detected closed connection."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 1, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
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
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
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

    def _listener(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(8)
        return server

    def test_resets_after_a_connection_holds_past_the_reset_threshold(self):
        """A connection that flapped, then stayed up continuously for at
        least RECONNECT_COUNT_RESET_AGE_SECONDS, starts its next flapping
        episode from 1 rather than continuing the stale total."""
        from receiver.main import RECONNECT_COUNT_RESET_AGE_SECONDS
        r = self._make_receiver()
        server = self._listener()
        host, port = server.getsockname()
        key = (host, port)
        r._reconnect_counts[key] = 5  # stale history from an earlier episode

        clock = {"t": 1000.0}
        threading.Thread(
            target=lambda: [c.close() for c in [server.accept()[0]]], daemon=True
        ).start()

        def _fake_reader(*a, **k):
            # The connection held this long before dropping.
            clock["t"] += RECONNECT_COUNT_RESET_AGE_SECONDS + 1

        def _fake_sleep(_s):
            r._shutdown.set()

        r._read_1090_stream = _fake_reader
        with patch("receiver.main.time.monotonic", lambda: clock["t"]), \
             patch("receiver.main.time.sleep", _fake_sleep):
            r._source_loop({"host": host, "port": port, "source": "1090"})
        server.close()

        assert r._reconnect_counts[key] == 1

    def test_does_not_reset_while_flapping_faster_than_the_threshold(self):
        """Each reconnection holds for less than the reset threshold, so
        the count keeps climbing across the flapping episode."""
        from receiver.main import RECONNECT_COUNT_RESET_AGE_SECONDS
        r = self._make_receiver()
        server = self._listener()
        host, port = server.getsockname()
        key = (host, port)

        clock = {"t": 1000.0}
        accepted = []

        def _accept_loop():
            while len(accepted) < 3:
                try:
                    conn, _ = server.accept()
                except OSError:
                    return
                conn.close()
                accepted.append(conn)

        threading.Thread(target=_accept_loop, daemon=True).start()

        cycles = {"n": 0}

        def _fake_reader(*a, **k):
            clock["t"] += RECONNECT_COUNT_RESET_AGE_SECONDS - 1  # under the threshold

        def _fake_sleep(_s):
            cycles["n"] += 1
            if cycles["n"] >= 3:
                r._shutdown.set()

        r._read_1090_stream = _fake_reader
        with patch("receiver.main.time.monotonic", lambda: clock["t"]), \
             patch("receiver.main.time.sleep", _fake_sleep):
            r._source_loop({"host": host, "port": port, "source": "1090"})
        server.close()

        assert r._reconnect_counts[key] == 3  # never reset mid-flap

    def test_never_resets_when_the_connection_never_succeeds(self):
        """An endpoint that refuses every attempt has no _connected_since
        timestamp, so no reset ever fires -- the count just accumulates."""
        r = self._make_receiver()
        key = ("localhost", 1)
        r._reconnect_counts[key] = 4

        attempts = {"n": 0}

        def _fake_sleep(_s):
            attempts["n"] += 1
            if attempts["n"] >= 2:
                r._shutdown.set()

        with patch("receiver.main.time.sleep", _fake_sleep):
            r._source_loop({"host": "localhost", "port": 1, "source": "1090"})

        assert r._reconnect_counts[key] == 6  # 4 + 2, never reset
        assert r._connected_since[key] is None


# ---------------------------------------------------------------------------
# TCP keepalive on source sockets
#
# A peer that vanishes without a clean FIN/RST leaves the receiver holding a
# half-open socket forever (it only ever reads, never writes, so it never
# provokes an RST). Keepalive makes the kernel probe an idle connection and
# tear it down when the peer stops answering, after which _source_loop's
# existing reconnect path takes over. The three timer values are fixed
# module constants, not configuration.
# ---------------------------------------------------------------------------

class TestTcpKeepalive:
    def test_keepalive_constants_match_90s_detection_budget(self):
        # 60s idle + 3 probes * 10s = ~90s detection budget.
        assert receiver_main.TCP_KEEPIDLE_SECONDS == 60
        assert receiver_main.TCP_KEEPINTVL_SECONDS == 10
        assert receiver_main.TCP_KEEPALIVE_PROBES == 3

    def test_enable_tcp_keepalive_sets_so_keepalive(self):
        # A real TCP socket -- the tuned timer options are only valid on
        # one, and _enable_tcp_keepalive is always handed a fresh
        # create_connection socket in production.
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            _enable_tcp_keepalive(s)
            # Truthy, not == 1: macOS getsockopt reports SO_KEEPALIVE as the
            # raw option bit (8), not a normalised boolean.
            assert s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
        finally:
            s.close()

    @pytest.mark.skipif(
        not hasattr(socket, "TCP_KEEPIDLE"),
        reason="TCP_KEEPIDLE is Linux-only",
    )
    def test_enable_tcp_keepalive_sets_tuned_timers_on_linux(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            _enable_tcp_keepalive(s)
            assert s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE) == 60
            assert s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL) == 10
            assert s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT) == 3
        finally:
            s.close()

    def test_enable_tcp_keepalive_tolerates_unsupported_timer_options(self):
        """A non-TCP socket (or a platform that rejects the timer options)
        still gets SO_KEEPALIVE and does not raise -- the timers are
        best-effort."""
        a, b = socket.socketpair()
        try:
            _enable_tcp_keepalive(a)
            assert a.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
        finally:
            a.close()
            b.close()

    def test_enable_tcp_keepalive_guards_each_linux_option_with_hasattr(self):
        """With none of the Linux-only names present, only the portable
        SO_KEEPALIVE setsockopt call is made -- the rest are skipped, not
        errored (this is what keeps local macOS pytest working)."""
        fake_sock = MagicMock()
        real_hasattr = hasattr

        def fake_hasattr(obj, name):
            if obj is receiver_main.socket and name in (
                "TCP_KEEPIDLE", "TCP_KEEPINTVL", "TCP_KEEPCNT"
            ):
                return False
            return real_hasattr(obj, name)

        with patch.object(receiver_main, "hasattr", fake_hasattr, create=True):
            _enable_tcp_keepalive(fake_sock)

        fake_sock.setsockopt.assert_called_once_with(
            socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1
        )

    def test_source_loop_enables_keepalive_on_connect(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 1, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            r = Receiver(cfg)

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        host, port = server.getsockname()

        def _accept_then_close():
            conn, _ = server.accept()
            conn.close()

        acceptor = threading.Thread(target=_accept_then_close, daemon=True)
        acceptor.start()

        seen = []

        def _fake_reader(*a, **k):
            r._shutdown.set()

        r._read_1090_stream = _fake_reader
        with patch("receiver.main._enable_tcp_keepalive", side_effect=seen.append):
            r._source_loop({"host": host, "port": port, "source": "1090"})
        acceptor.join(timeout=5)
        server.close()

        assert len(seen) == 1
        assert isinstance(seen[0], socket.socket)


# ---------------------------------------------------------------------------
# ICAO extraction and exchange routing
#
# parse_tcp_stream's own parsing correctness is covered in
# shared/tests/test_adsb_1090.py, since receiver/main.py imports it from
# shared.adsb_1090 rather than defining its own copy.
# ---------------------------------------------------------------------------

class TestIcaoRoutingIntegration:
    """
    Tests that verify ICAO extraction and the routing key handed to the
    consistent-hash exchange, via the Receiver._handle_message internals
    (using mocked publishing).
    """

    def _make_receiver(self):
        """Build a Receiver with a stub config (no real connections)."""
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def test_handle_message_routes_by_icao_hex(self):
        """_handle_message enqueues the message with the ICAO hex as routing
        key — the exchange, not the receiver, decides which processor gets it."""
        r = self._make_receiver()

        # A real DF17 ADS-B message — pyModeS should extract ICAO from it
        raw_hex = "8D4840D6202CC371C32CE0576098"
        published: list[tuple] = []
        r._enqueue_live = lambda q, p: published.append((q, p))
        r._rates["1090"] = _RateTracker()

        r._handle_message(raw_hex, "1090", r._rates["1090"], ("localhost", 30002))

        assert len(published) == 1
        routing_key, payload = published[0]

        import json
        msg_dict = json.loads(payload)
        assert routing_key == msg_dict["icao_hex"]
        assert msg_dict["source"] == "1090"
        assert len(msg_dict["icao_hex"]) == 6
        assert msg_dict["raw"] == raw_hex.upper() or msg_dict["raw"] == raw_hex

    def test_handle_message_routes_external_same_as_1090(self):
        """EXTERNAL-tagged frames use the same raw Mode S format as 1090 —
        no special handling is needed; the source tag is simply carried
        through."""
        r = self._make_receiver()

        raw_hex = "8D4840D6202CC371C32CE0576098"
        published: list[tuple] = []
        r._enqueue_live = lambda q, p: published.append((q, p))
        r._rates["EXTERNAL"] = _RateTracker()

        r._handle_message(raw_hex, "EXTERNAL", r._rates["EXTERNAL"], ("localhost", 30002))

        assert len(published) == 1
        _, payload = published[0]

        import json
        msg_dict = json.loads(payload)
        assert msg_dict["source"] == "EXTERNAL"
        assert len(msg_dict["icao_hex"]) == 6

    def test_handle_978_message_routes_correctly(self):
        """978 UAT messages skip pyModeS entirely -- icao_hex/received_at come
        from parse_978_line, not from decoding raw as Mode S."""
        r = self._make_receiver()

        raw_hex, icao_hex, received_at = parse_978_line(
            "-00a3d3e328a71f8c647004e9009c2d401a00;rs=6;rssi=0.3;t=1782561034.334;"
        )
        published: list[tuple] = []
        r._enqueue_live = lambda q, p: published.append((q, p))
        r._rates["978"] = _RateTracker()

        r._handle_978_message(raw_hex, icao_hex, received_at, "978", r._rates["978"], ("localhost", 30002))

        assert len(published) == 1
        routing_key, payload = published[0]
        assert routing_key == "A3D3E3"

        import json
        msg_dict = json.loads(payload)
        assert msg_dict["source"] == "978"
        assert msg_dict["icao_hex"] == "A3D3E3"
        assert msg_dict["raw"] == "-00A3D3E328A71F8C647004E9009C2D401A00"
        assert msg_dict["received_at"] == 1782561034.334

    def test_handle_978_message_discards_bad_icao_length(self):
        r = self._make_receiver()
        published: list = []
        r._enqueue_live = lambda q, p: published.append((q, p))
        r._rates["978"] = _RateTracker()

        r._handle_978_message("-BAD", "SHORT", time.time(), "978", r._rates["978"], ("localhost", 30002))
        assert published == []

    def test_handle_message_discards_bad_message(self):
        """Messages that yield no ICAO are discarded silently."""
        r = self._make_receiver()
        published: list = []
        r._enqueue_live = lambda q, p: published.append((q, p))
        r._rates["1090"] = _RateTracker()

        # Garbage hex — pyModeS.icao returns None
        r._handle_message("0000000000", "1090", r._rates["1090"], ("localhost", 30002))
        assert published == []

    def test_routing_key_consistent_for_same_icao(self):
        """Same ICAO always yields the same routing key, which is what pins
        an aircraft to one queue on the exchange's side."""
        r = self._make_receiver()
        raw_hex = "8D4840D6202CC371C32CE0576098"

        published: list[tuple] = []
        r._enqueue_live = lambda q, p: published.append((q, p))
        r._rates["1090"] = _RateTracker()

        for _ in range(5):
            r._handle_message(raw_hex, "1090", r._rates["1090"], ("localhost", 30002))
        routing_keys = {q for q, _ in published}
        assert len(routing_keys) == 1, "Same ICAO must always route the same way"

    def test_receiver_declares_no_processor_queues(self):
        """The receiver knows nothing about how many processors exist: it
        declares only the exchange topology, never a
        skyfollower-message-processor-{id} queue."""
        r = self._make_receiver()
        ch = MagicMock()

        with patch("receiver.main.pika.BlockingConnection") as MockConn, \
             patch("receiver.main.time.sleep", side_effect=lambda _s: r._shutdown.set()):
            MockConn.return_value.channel.return_value = ch
            MockConn.return_value.process_data_events.side_effect = (
                lambda **_kw: r._shutdown.set()
            )
            r._rmq_loop()

        declared_queues = [c.kwargs.get("queue") for c in ch.queue_declare.call_args_list]
        assert declared_queues == ["adsb-unroutable"]
        ch.exchange_declare.assert_any_call(
            exchange="adsb",
            exchange_type="x-consistent-hash",
            durable=True,
            arguments={"alternate-exchange": "adsb-unroutable"},
        )

    def test_handle_message_sets_last_message_at(self):
        r = self._make_receiver()
        r._enqueue_live = lambda q, p: None
        key = ("localhost", 30002)
        r._last_message_at[key] = None

        r._handle_message("8D4840D6202CC371C32CE0576098", "1090", _RateTracker(), key)

        assert r._last_message_at[key] is not None

    def test_handle_message_sets_last_message_at_even_when_discarded(self):
        """A message that fails ICAO extraction is still evidence the
        connection is alive and emitting frames -- last_message_at tracks
        traffic seen, not traffic successfully routed."""
        r = self._make_receiver()
        r._enqueue_live = lambda q, p: None
        key = ("localhost", 30002)
        r._last_message_at[key] = None

        r._handle_message("0000000000", "1090", _RateTracker(), key)

        assert r._last_message_at[key] is not None

    def test_handle_978_message_sets_last_message_at(self):
        r = self._make_receiver()
        r._enqueue_live = lambda q, p: None
        key = ("localhost", 30978)
        r._last_message_at[key] = None
        raw_hex, icao_hex, received_at = parse_978_line(
            "-00a3d3e328a71f8c647004e9009c2d401a00;rs=6;rssi=0.3;t=1782561034.334;"
        )

        r._handle_978_message(raw_hex, icao_hex, received_at, "978", _RateTracker(), key)

        assert r._last_message_at[key] is not None


# ---------------------------------------------------------------------------
# Unparseable-line logging (978 and 1090 read loops)
#
# Feeds real bytes through a connected socketpair directly into
# _read_978_stream / _read_1090_stream (rather than mocking parse_978_line /
# parse_tcp_stream) so the blank-line / !-preamble carve-out is exercised
# through the actual parsers, not an assumption about how they behave.
# ---------------------------------------------------------------------------

class TestUnparseableLineLogging:
    """A line/chunk that looks like real data but fails to parse must be
    visible from receiver logs -- unconditionally at debug, and via a
    rate-limited warning at default log level -- without treating routine
    input (blank lines, 978's !-preamble) as a failure."""

    def _make_receiver(self, source: str = "978"):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 1, "source": source}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            r = Receiver(cfg)
        r._enqueue_live = lambda q, p: None
        return r

    def _run_978(self, r, data: bytes):
        client, server = socket.socketpair()
        client.sendall(data)
        client.close()
        r._read_978_stream(server, "localhost", 30978, "978", _RateTracker())
        server.close()

    def _run_1090(self, r, data: bytes):
        client, server = socket.socketpair()
        client.sendall(data)
        client.close()
        r._read_1090_stream(server, "localhost", 30002, "1090", _RateTracker())
        server.close()

    def test_978_blank_and_preamble_lines_are_not_logged(self, caplog):
        r = self._make_receiver("978")
        data = b"!fecfix=1;program=dump978-fa;version=3.8.1\n\n\n"

        with caplog.at_level(logging.DEBUG, logger="receiver"):
            self._run_978(r, data)

        assert not any("unparse" in rec.getMessage().lower() for rec in caplog.records)

    def test_978_malformed_line_logs_debug_unconditionally(self, caplog):
        r = self._make_receiver("978")
        # Starts with '-' like real UAT data, but the payload isn't valid
        # hex -- parse_978_line returns None for a non-blank, non-! line.
        data = b"-notvalidhexpayload;rs=1;\n"

        with caplog.at_level(logging.DEBUG, logger="receiver"):
            self._run_978(r, data)

        debug_records = [rec for rec in caplog.records if rec.levelno == logging.DEBUG]
        assert any("978" in rec.getMessage() for rec in debug_records)

    def test_978_repeated_malformed_lines_trigger_rate_limited_warning(self, caplog):
        r = self._make_receiver("978")
        data = b"-badline1;\n-badline2;\n-badline3;\n"

        with patch("receiver.main.UNPARSEABLE_WARNING_INTERVAL_SECONDS", 0), \
             caplog.at_level(logging.DEBUG, logger="receiver"):
            self._run_978(r, data)

        unparseable_warnings = [
            rec for rec in caplog.records
            if rec.levelno == logging.WARNING and "closed connection" not in rec.getMessage()
        ]
        assert len(unparseable_warnings) >= 1
        assert any("3" in rec.getMessage() for rec in unparseable_warnings)

    def test_1090_malformed_data_logs_debug_and_rate_limited_warning(self, caplog):
        r = self._make_receiver("1090")
        # Never forms a valid *hex; frame at all.
        data = b"THIS IS NOT A VALID 1090 FRAME AT ALL"

        with patch("receiver.main.UNPARSEABLE_WARNING_INTERVAL_SECONDS", 0), \
             caplog.at_level(logging.DEBUG, logger="receiver"):
            self._run_1090(r, data)

        debug_records = [rec for rec in caplog.records if rec.levelno == logging.DEBUG]
        unparseable_warnings = [
            rec for rec in caplog.records
            if rec.levelno == logging.WARNING and "closed connection" not in rec.getMessage()
        ]
        assert any("1090" in rec.getMessage() for rec in debug_records)
        assert len(unparseable_warnings) >= 1

    def test_1090_valid_message_does_not_log_unparseable(self, caplog):
        r = self._make_receiver("1090")
        data = b"*8D4840D6202CC371C32CE0576098;"

        with caplog.at_level(logging.DEBUG, logger="receiver"):
            self._run_1090(r, data)

        assert not any("unparse" in rec.getMessage().lower() for rec in caplog.records)


# ---------------------------------------------------------------------------
# Live publish path — the source threads hand each parsed message to an
# in-memory queue and loop straight back to sock.recv(); nothing on that
# path ever touches pika or waits on the broker.
# ---------------------------------------------------------------------------

class TestEnqueueLive:
    """_enqueue_live() is the entire live path off the source threads:
    drop the message on the in-memory queue and return. A full queue
    spills to the durable SQLite fallback rather than blocking the caller,
    so getting messages off the TCP socket is never delayed by the broker
    or by backlog drain."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def test_hands_message_to_the_in_memory_queue(self):
        r = self._make_receiver()
        r._enqueue_live("4B1900", '{"raw": "AA"}')
        assert r._live_queue.get_nowait() == ("4B1900", '{"raw": "AA"}')
        assert r._fallback.depth() == 0

    def test_never_touches_rabbitmq(self):
        """No connection, no channel, broker unreachable -- the source
        thread must still return at once with the message buffered."""
        r = self._make_receiver()
        r._rmq_connection = None
        r._rmq_channel = None
        r._rmq_connected = False
        r._enqueue_live("4B1900", "x")
        assert r._live_queue.qsize() == 1
        assert r._fallback.depth() == 0

    def test_full_live_queue_spills_to_the_overflow_queue_not_disk(self):
        """A full live queue hands the message to the in-memory overflow
        queue -- the SQLite write is the overflow-writer thread's job, never
        the source thread's."""
        r = self._make_receiver()
        r._live_queue = queue.Queue(maxsize=2)
        r._enqueue_live("A", "1")
        r._enqueue_live("B", "2")

        completed = threading.Event()

        def _enqueue_third():
            r._enqueue_live("C", "3")  # live queue full -- goes to overflow
            completed.set()

        t = threading.Thread(target=_enqueue_third)
        t.start()
        t.join(timeout=2)

        assert completed.is_set(), "_enqueue_live blocked on a full queue"
        assert r._live_queue.qsize() == 2
        assert r._overflow_queue.get_nowait() == ("C", "3")
        assert r._fallback.depth() == 0

    def test_both_queues_full_falls_back_to_a_direct_disk_write(self):
        """Last-resort pressure valve: only when the overflow queue has
        also filled does a source thread take the synchronous SQLite write
        itself -- still without blocking or dropping."""
        r = self._make_receiver()
        r._live_queue = queue.Queue(maxsize=1)
        r._overflow_queue = queue.Queue(maxsize=1)
        r._enqueue_live("A", "1")  # fills live queue
        r._enqueue_live("B", "2")  # fills overflow queue

        completed = threading.Event()

        def _enqueue_third():
            r._enqueue_live("C", "3")
            completed.set()

        t = threading.Thread(target=_enqueue_third)
        t.start()
        t.join(timeout=2)

        assert completed.is_set(), "_enqueue_live blocked with both queues full"
        assert r._fallback.depth() == 1
        captured: list[str] = []
        r._fallback.drain(captured.append)
        assert json.loads(captured[0]) == {"routing_key": "C", "payload": "3"}

    def test_route_message_uses_enqueue_live(self):
        r = self._make_receiver()
        seen = []
        r._enqueue_live = lambda q, p: seen.append((q, p))
        r._route_message(
            "8D4840D6202CC371C32CE0576098", "4CA1FA", 1.0, "1090", _RateTracker()
        )
        assert len(seen) == 1
        assert seen[0][0] == "4CA1FA"

    def test_source_read_loop_does_not_block_when_broker_is_gone(self):
        """A full read of a socket's worth of frames completes even with no
        RabbitMQ connection at all -- the read loop never calls a pika
        method, so a stalled broker cannot back-pressure sock.recv()."""
        r = self._make_receiver()
        r._rmq_connected = False
        r._rmq_connection = None
        client, server = socket.socketpair()
        client.sendall(b"*8D4840D6202CC371C32CE0576098;\n" * 50)
        client.close()

        done = threading.Event()

        def _run():
            r._read_1090_stream(server, "localhost", 30002, "1090", _RateTracker())
            done.set()

        t = threading.Thread(target=_run)
        t.start()
        t.join(timeout=5)
        server.close()

        assert done.is_set(), "read loop blocked with the broker unavailable"
        assert r._live_queue.qsize() == 50


class TestOverflowWriter:
    """The overflow-writer thread is the sole consumer of _overflow_queue:
    it batches overflow messages into the durable SQLite fallback with one
    commit per pass, so a sustained outage never puts a per-message
    fsync-class write on a socket-read thread."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def test_flush_batches_the_whole_queue_into_one_put_many(self):
        r = self._make_receiver()
        for i in range(2000):
            r._overflow_queue.put_nowait((f"HEX{i:04d}", str(i)))

        with patch.object(r._fallback, "put_many", wraps=r._fallback.put_many) as pm:
            r._flush_overflow_batch(None)

        assert pm.call_count == 1
        assert r._fallback.depth() == 2000
        assert r._overflow_queue.empty()

    def test_flush_preserves_order_and_the_routing_key_wrap(self):
        r = self._make_receiver()
        r._overflow_queue.put_nowait(("AAA111", "first"))
        r._overflow_queue.put_nowait(("BBB222", "second"))

        r._flush_overflow_batch(None)

        captured: list[str] = []
        r._fallback.drain(captured.append)
        assert [json.loads(c) for c in captured] == [
            {"routing_key": "AAA111", "payload": "first"},
            {"routing_key": "BBB222", "payload": "second"},
        ]

    def test_flush_caps_a_single_pass_at_the_batch_max(self):
        r = self._make_receiver()
        with patch("receiver.main._OVERFLOW_WRITE_BATCH_MAX", 10):
            for i in range(25):
                r._overflow_queue.put_nowait((f"H{i}", str(i)))
            r._flush_overflow_batch(None)

        assert r._fallback.depth() == 10
        assert r._overflow_queue.qsize() == 15

    def test_flush_is_a_noop_on_an_empty_queue(self):
        r = self._make_receiver()
        with patch.object(r._fallback, "put_many") as pm:
            r._flush_overflow_batch(None)
        pm.assert_not_called()

    def test_writer_loop_drains_a_final_time_on_shutdown(self):
        """Nothing buffered in RAM is dropped on a clean stop -- the loop
        makes one last flush pass after _shutdown is set."""
        r = self._make_receiver()
        r._overflow_queue.put_nowait(("LATE01", "x"))
        r._shutdown.set()

        r._overflow_writer_loop()  # returns at once: _shutdown already set

        assert r._fallback.depth() == 1

    def test_writer_loop_persists_messages_arriving_while_it_runs(self):
        r = self._make_receiver()

        t = threading.Thread(target=r._overflow_writer_loop, name="overflow-writer")
        t.start()
        try:
            for i in range(500):
                r._overflow_queue.put_nowait((f"HEX{i:04d}", str(i)))
            deadline = time.time() + 3
            while r._fallback.depth() < 500 and time.time() < deadline:
                time.sleep(0.02)
        finally:
            r._shutdown.set()
            t.join(timeout=3)

        assert r._fallback.depth() == 500

    def test_start_wires_the_overflow_writer_thread(self):
        r = self._make_receiver()
        r._shutdown.set()  # start() ends on self._shutdown.wait()
        with patch.object(r, "_connect_mqtt"), \
             patch("receiver.main.threading.Thread") as MockThread:
            r.start()
        thread_names = [c.kwargs.get("name") for c in MockThread.call_args_list]
        assert "overflow-writer" in thread_names


class TestPublishOne:
    """_publish_one() runs only on the rabbitmq thread and calls
    channel.basic_publish directly -- no cross-thread hand-off. A failure
    must persist the message to SQLite (never a silent drop) and latch the
    connection unhealthy so _rmq_loop reconnects and re-validates."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def test_publishes_with_expected_args(self):
        r = self._make_receiver()
        ch = MagicMock()
        assert r._publish_one(ch, "4B1900", '{"raw": "AA"}') is True
        kwargs = ch.basic_publish.call_args.kwargs
        assert kwargs["exchange"] == "adsb"
        assert kwargs["routing_key"] == "4B1900"
        assert kwargs["body"] == b'{"raw": "AA"}'

    def test_failure_routes_to_fallback_and_latches_disconnected(self):
        r = self._make_receiver()
        r._rmq_connected = True
        ch = MagicMock()
        ch.basic_publish.side_effect = RuntimeError("boom")
        assert r._publish_one(ch, "4B1900", '{"raw": "AA"}') is False
        assert r._fallback.depth() == 1
        assert r._rmq_connected is False

    def test_fallback_row_publish_failure_reraises_and_latches(self):
        r = self._make_receiver()
        r._rmq_connected = True
        ch = MagicMock()
        ch.basic_publish.side_effect = RuntimeError("boom")
        wrapped = json.dumps({"routing_key": "4B1900", "payload": "x"})
        with pytest.raises(RuntimeError, match="boom"):
            r._publish_fallback_row(ch, wrapped)
        assert r._rmq_connected is False


class TestFallbackPutWrapsRoutingKey:
    """FallbackQueue (shared/fallback_queue.py) is payload-only -- Receiver
    wraps the routing key into the JSON payload it puts, so the drain path
    is identical to the live publish path and never has to re-parse a
    stored message body to work out where it was going."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def test_fallback_put_wraps_routing_key_and_payload(self):
        r = self._make_receiver()
        r._fallback_put("4B1900", '{"raw": "AA"}')
        assert r._fallback.depth() == 1

        captured = []
        r._fallback.drain(captured.append)
        item = json.loads(captured[0])
        assert item == {"routing_key": "4B1900", "payload": '{"raw": "AA"}'}


# ---------------------------------------------------------------------------
# _rmq_publish_loop — the sole publishing thread. Strict priority for live
# messages off the sockets; the fallback backlog is advanced one bounded
# batch (_FALLBACK_DRAIN_BATCH_MAX rows) at a time, and only when nothing
# is waiting to go out live. The live queue is re-checked between batches,
# never mid-batch.
# ---------------------------------------------------------------------------

class TestRmqPublishLoop:
    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def test_publishes_queued_live_messages(self):
        r = self._make_receiver()
        r._rmq_connected = True
        r._live_queue.put_nowait(("4B1900", '{"raw": "AA"}'))

        published = []
        ch = MagicMock()

        def record(**kw):
            published.append(kw["routing_key"])
            r._shutdown.set()

        ch.basic_publish.side_effect = record
        r._rmq_publish_loop(MagicMock(), ch)

        assert published == ["4B1900"]
        assert ch.basic_publish.call_args.kwargs["exchange"] == "adsb"

    def test_live_messages_publish_before_any_backlog_row(self):
        """Strict priority: everything queued off the sockets goes out
        before a single fallback-backlog row is touched."""
        r = self._make_receiver()
        r._rmq_connected = True
        for i in range(3):
            r._fallback_put(f"BACK{i}", "x")
        r._live_queue.put_nowait(("LIVE0", "y"))
        r._live_queue.put_nowait(("LIVE1", "y"))

        published = []
        ch = MagicMock()

        def record(**kw):
            published.append(kw["routing_key"])
            if len(published) >= 5:
                r._shutdown.set()

        ch.basic_publish.side_effect = record
        r._rmq_publish_loop(MagicMock(), ch)

        assert published[:2] == ["LIVE0", "LIVE1"]
        assert set(published[2:]) == {"BACK0", "BACK1", "BACK2"}

    def test_live_message_arriving_mid_drain_jumps_ahead_of_the_next_batch(self):
        """A large backlog is draining, no live traffic -- then one live
        message arrives partway through a batch. It publishes before the
        *next* batch starts (the live queue is re-checked between batches),
        but not mid-batch: whatever rows the current batch already selected
        still go out first."""
        r = self._make_receiver()
        r._rmq_connected = True
        for i in range(5):
            r._fallback_put(f"BACK{i}", "x")

        published = []
        ch = MagicMock()

        def record(**kw):
            rk = kw["routing_key"]
            published.append(rk)
            # Arrives during the first batch (batch size patched to 2).
            if rk == "BACK0":
                r._live_queue.put_nowait(("LIVE", "y"))
            if len(published) >= 4:
                r._shutdown.set()

        ch.basic_publish.side_effect = record
        with patch("receiver.main._FALLBACK_DRAIN_BATCH_MAX", 2):
            r._rmq_publish_loop(MagicMock(), ch)

        # BACK1 was already in the running batch, so it precedes LIVE;
        # LIVE then precedes BACK2, the first row of the next batch.
        assert published[:4] == ["BACK0", "BACK1", "LIVE", "BACK2"]

    def test_backlog_drains_in_bounded_batches(self):
        """Each backlog pass removes at most _FALLBACK_DRAIN_BATCH_MAX
        rows, with a single DELETE+commit per batch rather than per row."""
        r = self._make_receiver()
        r._rmq_connected = True
        for i in range(10):
            r._fallback_put(f"BACK{i}", "x")

        published = []
        ch = MagicMock()

        def record(**kw):
            published.append(kw["routing_key"])
            if len(published) >= 10:
                r._shutdown.set()

        ch.basic_publish.side_effect = record
        with patch("receiver.main._FALLBACK_DRAIN_BATCH_MAX", 4):
            r._rmq_publish_loop(MagicMock(), ch)

        assert published == [f"BACK{i}" for i in range(10)]
        assert r._fallback.depth() == 0

    def test_publish_failure_returns_and_persists_the_message(self):
        r = self._make_receiver()
        r._rmq_connected = True
        r._live_queue.put_nowait(("4B1900", "y"))
        ch = MagicMock()
        ch.basic_publish.side_effect = RuntimeError("boom")

        # Returns rather than spinning -- _rmq_loop then reconnects.
        r._rmq_publish_loop(MagicMock(), ch)

        assert r._rmq_connected is False
        assert r._fallback.depth() == 1

    def test_returns_when_rmq_connected_is_latched_false(self):
        r = self._make_receiver()
        r._rmq_connected = False
        # No shutdown set: the latch alone must end the loop.
        r._rmq_publish_loop(MagicMock(), MagicMock())

    def test_idle_loop_wakes_and_publishes_the_next_live_message(self):
        r = self._make_receiver()
        r._rmq_connected = True

        published = []
        ch = MagicMock()

        def record(**kw):
            published.append(kw["routing_key"])
            r._shutdown.set()

        ch.basic_publish.side_effect = record

        def _feed():
            time.sleep(0.05)
            r._live_queue.put_nowait(("LATE", "y"))

        with patch("receiver.main._RMQ_IDLE_POLL_SECONDS", 0.5):
            threading.Thread(target=_feed).start()
            r._rmq_publish_loop(MagicMock(), ch)

        assert published == ["LATE"]


# ---------------------------------------------------------------------------
# _rmq_loop must rebuild the connection when the publish path latches
# _rmq_connected False -- the broker blocking publishers (resource alarm) is
# exactly this: basic_publish fails but process_data_events keeps
# succeeding. Preserves the #1136 fix intent under the sole-publisher model.
# ---------------------------------------------------------------------------


class TestRmqLoopRecoversFromLatchedDisconnect:
    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def test_publish_failure_on_healthy_connection_forces_reconnect_and_recovers(self):
        r = self._make_receiver()
        r._live_queue.put_nowait(("4B1900", '{"raw": "AA"}'))

        state: dict = {"connects": 0}

        def make_conn(*_args, **_kwargs):
            state["connects"] += 1
            n = state["connects"]
            conn = MagicMock()
            channel = MagicMock()
            conn.channel.return_value = channel
            conn.process_data_events.side_effect = lambda **_kw: None
            if n == 1:
                # Broker has publishers blocked: publish fails, the
                # connection itself keeps answering process_data_events.
                channel.basic_publish.side_effect = TimeoutError("blocked")
            else:
                def ok(**_kw):
                    state["connected_on_reconnect"] = r._rmq_connected
                    r._shutdown.set()

                channel.basic_publish.side_effect = ok
            return conn

        with patch("receiver.main.pika.BlockingConnection", side_effect=make_conn), \
             patch("receiver.main.time.sleep", lambda _s: None):
            r._rmq_loop()

        # The first connection did not stay latched False forever: a fresh
        # connect was forced even though process_data_events never raised.
        assert state["connects"] >= 2
        # Publishing works again -> the flag is re-validated True...
        assert state["connected_on_reconnect"] is True
        # ...and the message that failed on connection 1 drained from the
        # SQLite fallback on connection 2, no restart.
        assert r._fallback.depth() == 0
        assert r._live_queue.qsize() == 0


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
# _RateTracker Redis-backed period counters
# ---------------------------------------------------------------------------

class TestRateTrackerPeriodCounters:
    """record() must stay pure in-memory arithmetic; flush_to_redis() is
    the only thing that ever touches Redis or does boundary detection."""

    def test_record_increments_all_three_counters(self):
        rt = _RateTracker()
        rt.record()
        rt.record()
        assert rt.hour_count == 2
        assert rt.today_count == 2
        assert rt.lifetime_count == 2

    def test_record_never_touches_redis(self):
        """No redis client is even passed to record() -- if it tried to
        use one, this would AttributeError instead of silently no-op'ing."""
        rt = _RateTracker()
        rt.record()  # Must not raise despite no Redis client existing anywhere.
        assert rt.lifetime_count == 1

    def test_record_has_no_message_count_flush_trigger(self):
        """Telemetry is purely time-based: recording many messages in a
        burst must not carry any early-flush side effect -- just the
        in-memory counters advancing."""
        rt = _RateTracker()
        for _ in range(500):
            rt.record()
        assert rt.lifetime_count == 500
        assert not hasattr(rt, "_flush_event")
        assert not hasattr(rt, "_pending_since_flush")

    def _now(self, **kwargs):
        base = datetime(2026, 8, 23, 14, 30, 0, tzinfo=timezone.utc)
        return base.replace(**kwargs) if kwargs else base

    def test_flush_pushes_delta_via_evalsha_for_hour_and_today(self):
        rt = _RateTracker()
        rt.record()
        rt.record()
        mock_redis = MagicMock()
        rt.flush_to_redis(
            redis_client=mock_redis,
            script_sha="sha123",
            key_fn=lambda period: f"key:{period}",
            now=self._now(),
        )
        calls = {c.args[2]: c.args for c in mock_redis.evalsha.call_args_list}
        assert calls["key:hour"][:4] == ("sha123", 0, "key:hour", 2)
        assert calls["key:today"][:4] == ("sha123", 0, "key:today", 2)

    def test_flush_never_writes_lifetime_to_redis(self):
        """lifetime_count is a device-local, in-memory figure the receiver
        publishes directly -- flush_to_redis must never touch Redis for it
        (no INCRBY, no key named ...:lifetime)."""
        rt = _RateTracker()
        rt.record()
        rt.record()
        rt.record()
        mock_redis = MagicMock()
        rt.flush_to_redis(
            redis_client=mock_redis,
            script_sha="sha123",
            key_fn=lambda period: f"key:{period}",
            now=self._now(),
        )
        mock_redis.incrby.assert_not_called()
        flushed_keys = [c.args[2] for c in mock_redis.evalsha.call_args_list]
        assert "key:lifetime" not in flushed_keys
        # The in-memory counter still tracks it for the receiver's own publish.
        assert rt.lifetime_count == 3

    def test_flush_with_no_new_messages_calls_nothing(self):
        rt = _RateTracker()
        mock_redis = MagicMock()
        rt.flush_to_redis(
            redis_client=mock_redis,
            script_sha="sha123",
            key_fn=lambda period: f"key:{period}",
            now=self._now(),
        )
        mock_redis.evalsha.assert_not_called()
        mock_redis.incrby.assert_not_called()

    def test_flush_only_sends_incremental_delta_on_second_flush(self):
        rt = _RateTracker()
        mock_redis = MagicMock()
        rt.record()
        rt.record()
        rt.flush_to_redis(mock_redis, "sha", lambda p: f"key:{p}", self._now())
        rt.record()
        mock_redis.reset_mock()
        rt.flush_to_redis(mock_redis, "sha", lambda p: f"key:{p}", self._now())
        calls = {c.args[2]: c.args[3] for c in mock_redis.evalsha.call_args_list}
        assert calls["key:hour"] == 1
        assert calls["key:today"] == 1

    def test_hour_rollover_resets_local_hour_count_only(self):
        rt = _RateTracker()
        rt.record()
        rt.record()
        mock_redis = MagicMock()
        rt.flush_to_redis(mock_redis, "sha", lambda p: f"key:{p}", self._now(minute=59))
        rt.record()
        # Next hour -- hour_count must reset; today_count/lifetime_count
        # (same day) must not.
        rt.flush_to_redis(mock_redis, "sha", lambda p: f"key:{p}", self._now(hour=15, minute=0))
        assert rt.hour_count == 0
        assert rt.today_count == 3
        assert rt.lifetime_count == 3

    def test_day_rollover_resets_local_today_count_only(self):
        rt = _RateTracker()
        rt.record()
        mock_redis = MagicMock()
        rt.flush_to_redis(mock_redis, "sha", lambda p: f"key:{p}", self._now(hour=23, minute=59))
        rt.record()
        next_day = datetime(2026, 8, 24, 0, 5, 0, tzinfo=timezone.utc)
        rt.flush_to_redis(mock_redis, "sha", lambda p: f"key:{p}", next_day)
        assert rt.today_count == 0
        assert rt.lifetime_count == 2

    def test_rollover_does_not_flush_the_dropped_remainder(self):
        """The small leftover counted toward the now-closed period is
        dropped rather than misattributed -- see flush_to_redis's own
        comment on why (a stale EXPIREAT could delete the key outright)."""
        rt = _RateTracker()
        rt.record()
        mock_redis = MagicMock()
        rt.flush_to_redis(mock_redis, "sha", lambda p: f"key:{p}", self._now(minute=59))
        # No further record() -- the rollover flush should see hour_delta
        # dropped to 0, not the stale 1 message from the old hour.
        mock_redis.reset_mock()
        rt.flush_to_redis(mock_redis, "sha", lambda p: f"key:{p}", self._now(hour=15, minute=0))
        hour_calls = [c for c in mock_redis.evalsha.call_args_list if c.args[1] == "key:hour"]
        assert hour_calls == []


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
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        if name is not None:
            cfg["name"] = name
        with patch("receiver.main.DATA_DIR", data_dir or tempfile.mkdtemp()):
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
        with patch("receiver.main.load_config", return_value={
            "sources": [], "rabbitmq": {"host": "x", "username": "u", "password": "p"},
        }):
            with patch("receiver.main.Receiver") as mock_cls:
                mock_cls.return_value.start = MagicMock()
                import receiver.main as rm
                rm.main()
                mock_cls.assert_called_once()
                args, kwargs = mock_cls.call_args
                assert len(args) == 1 and not kwargs


# ---------------------------------------------------------------------------
# Redis-backed identity claim -- three-case startup
# resolution, mirroring _claim_message_processor_id()/_heartbeat_loop.
# ---------------------------------------------------------------------------

def _make_receiver_with_redis(cfg=None, data_dir=None, mock_redis=None):
    """Constructs a real Receiver with Redis mocked out, the same way
    message-processor/tests/test_processor.py's _make_processor() mocks
    Redis -- patching redis_lib.Redis (the actual `redis` package's own
    Redis class, shared by every module that does `import redis as
    redis_lib`, including shared.redis_client.build_redis_client) rather
    than receiver.main's own names."""
    from receiver.main import Receiver

    cfg = cfg or {
        "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
        "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        "name": "ATTIC",
        "redis": {"host": "redis.example.com", "port": 6379, "password": "secret"},
    }
    mock_redis = mock_redis if mock_redis is not None else MagicMock()
    with patch("receiver.main.DATA_DIR", data_dir or tempfile.mkdtemp()), \
         patch("receiver.main.redis_lib.Redis", return_value=mock_redis):
        r = Receiver(cfg)
    return r, mock_redis


class TestReceiverIdentityRedisClaim:
    def test_case_1_persisted_identity_skips_redis_entirely(self):
        """A local identity file from a prior claim (or the legacy UUID
        scheme) means zero Redis calls at startup -- this is what lets a
        restart survive Redis being unreachable."""
        data_dir = tempfile.mkdtemp()
        with open(os.path.join(data_dir, "receiver_id"), "w") as f:
            f.write("PREVIOUSLY-CLAIMED")
        r, mock_redis = _make_receiver_with_redis(data_dir=data_dir)
        assert r._id == "PREVIOUSLY-CLAIMED"
        mock_redis.set.assert_not_called()

    def test_case_2_claims_configured_name_via_set_nx(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = _make_receiver_with_redis(mock_redis=mock_redis)
        assert r._id == "ATTIC"
        mock_redis.set.assert_called_once()
        args, kwargs = mock_redis.set.call_args
        assert args[0] == "skyfollower-receiver-ATTIC"
        assert kwargs.get("nx") is True
        assert kwargs.get("ex") == 60  # HEARTBEAT_TTL_SECONDS

    def test_case_2_success_persists_identity_locally(self):
        data_dir = tempfile.mkdtemp()
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        _make_receiver_with_redis(data_dir=data_dir, mock_redis=mock_redis)
        with open(os.path.join(data_dir, "receiver_id")) as f:
            assert f.read().strip() == "ATTIC"

    def test_case_2_name_already_claimed_exits(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = None  # SET NX returns nil when the key already exists.
        with pytest.raises(SystemExit):
            _make_receiver_with_redis(mock_redis=mock_redis)

    def test_case_3_redis_unreachable_exits(self):
        mock_redis = MagicMock()
        mock_redis.set.side_effect = ConnectionError("refused")
        with pytest.raises(SystemExit):
            _make_receiver_with_redis(mock_redis=mock_redis)

    def test_no_redis_configured_falls_back_to_uuid(self):
        """REDIS_HOST unset entirely (no 'redis' block, or one with a
        blank host) must never call SET NX -- legacy behavior."""
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        from receiver.main import Receiver
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            r = Receiver(cfg)
        assert r._redis is None
        assert len(r._id) == 36  # UUID4 string length

    def test_redis_configured_with_blank_host_is_treated_as_unset(self):
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "redis": {"host": "", "port": 6379, "password": ""},
        }
        from receiver.main import Receiver
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            r = Receiver(cfg)
        assert r._redis is None


class TestReceiverHeartbeatLoop:
    """Mirrors message-processor's _heartbeat_loop exactly: sleep, then an
    unconditional EXPIRE (never a second SET NX), fail-soft on any error."""

    def test_heartbeat_refreshes_expiry(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = _make_receiver_with_redis(mock_redis=mock_redis)

        def fake_sleep(_seconds):
            r._shutdown.set()

        with patch("receiver.main.time.sleep", side_effect=fake_sleep):
            r._heartbeat_loop()

        mock_redis.expire.assert_called_once_with("skyfollower-receiver-ATTIC", 60)

    def test_heartbeat_never_calls_set_nx(self):
        """The ongoing heartbeat must never re-run the claim -- only ever
        refresh the TTL of a key it already knows is its own."""
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = _make_receiver_with_redis(mock_redis=mock_redis)
        mock_redis.reset_mock()

        def fake_sleep(_seconds):
            r._shutdown.set()

        with patch("receiver.main.time.sleep", side_effect=fake_sleep):
            r._heartbeat_loop()

        # _register_with_core_health() legitimately calls .set() for the
        # registration entry -- what must never happen again is a SET NX
        # against the claim key.
        for call in mock_redis.set.call_args_list:
            assert call.kwargs.get("nx") is not True

    def test_heartbeat_fails_soft_on_redis_error(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = _make_receiver_with_redis(mock_redis=mock_redis)
        mock_redis.expire.side_effect = ConnectionError("boom")

        def fake_sleep(_seconds):
            r._shutdown.set()

        with patch("receiver.main.time.sleep", side_effect=fake_sleep):
            r._heartbeat_loop()  # Must not raise.


class TestCoreHealthRegistration:
    """A small index SET + per-receiver registration entry
    for core-health to enumerate live receivers without a keyspace SCAN."""

    def test_register_adds_to_index_set(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = _make_receiver_with_redis(mock_redis=mock_redis)
        mock_redis.reset_mock()
        r._register_with_core_health()
        mock_redis.sadd.assert_called_once_with("receiver:index", "ATTIC")

    def test_register_writes_source_list_with_heartbeat_ttl(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = _make_receiver_with_redis(mock_redis=mock_redis)
        mock_redis.reset_mock()
        r._register_with_core_health()
        mock_redis.set.assert_called_once_with(
            "receiver:registration:ATTIC",
            json.dumps([{"host": "localhost", "port": 30002, "source": "1090"}]),
            ex=60,
        )

    def test_register_is_a_noop_without_redis(self):
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        from receiver.main import Receiver
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            r = Receiver(cfg)
        r._register_with_core_health()  # Must not raise despite self._redis being None.

    def test_register_fails_soft_on_redis_error(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = _make_receiver_with_redis(mock_redis=mock_redis)
        mock_redis.sadd.side_effect = ConnectionError("boom")
        r._register_with_core_health()  # Must not raise.

    def test_heartbeat_loop_reregisters_every_tick(self):
        """Refreshing the registration alongside the heartbeat is what
        re-registers a receiver that resumed an already-persisted identity
        (case 1) without ever calling _register_with_core_health() at
        claim time."""
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = _make_receiver_with_redis(mock_redis=mock_redis)
        mock_redis.reset_mock()

        def fake_sleep(_seconds):
            r._shutdown.set()

        with patch("receiver.main.time.sleep", side_effect=fake_sleep):
            r._heartbeat_loop()

        mock_redis.sadd.assert_called_once_with("receiver:index", "ATTIC")


# ---------------------------------------------------------------------------
# start() thread wiring
# ---------------------------------------------------------------------------

class TestStartWiresHeartbeatConditionally:
    """start() must only spin up the heartbeat thread (and register with
    core-health once up front) when Redis is actually configured."""

    def test_start_starts_heartbeat_thread_when_redis_configured(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = _make_receiver_with_redis(mock_redis=mock_redis)
        # start() blocks on self._shutdown.wait() at the very end --
        # pre-setting it makes that call return immediately.
        r._shutdown.set()
        with patch.object(r, "_connect_mqtt"), \
             patch.object(r, "_register_with_core_health") as mock_register, \
             patch("receiver.main.threading.Thread") as MockThread:
            r.start()
        mock_register.assert_called_once()
        thread_names = [c.kwargs.get("name") for c in MockThread.call_args_list]
        assert "heartbeat" in thread_names

    def test_start_does_not_start_heartbeat_thread_without_redis(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            r = Receiver(cfg)
        r._shutdown.set()
        with patch.object(r, "_connect_mqtt"), \
             patch("receiver.main.threading.Thread") as MockThread:
            r.start()
        thread_names = [c.kwargs.get("name") for c in MockThread.call_args_list]
        assert "heartbeat" not in thread_names


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
                {"host": "localhost", "port": 30105, "source": "EXTERNAL"},
            ],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
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

    def test_connected_attributes_not_published_before_first_message(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        topics = {c.args[0] for c in mock_mqtt.publish.call_args_list}
        base = f"SkyFollower/receiver/{r._id}/statistic"
        assert f"{base}/localhost_30002_connected_attributes" not in topics

    def test_connected_attributes_published_once_last_message_set(self):
        """last_message_at folds into the sibling _connected sensor's
        json_attributes_topic as a `last_message_received` JSON key,
        rather than existing as its own standalone entity/state topic."""
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._last_message_at[("localhost", 30002)] = "2026-01-15T10:00:00+00:00"
        r._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        base = f"SkyFollower/receiver/{r._id}/statistic"
        assert json.loads(calls[f"{base}/localhost_30002_connected_attributes"]) == {
            "last_message_received": "2026-01-15T10:00:00+00:00"
        }
        assert f"{base}/localhost_30978_connected_attributes" not in calls
        assert f"{base}/localhost_30002_last_message_at" not in calls

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

    def test_last_message_at_has_no_standalone_discovery_entry(self):
        """Folded into the sibling _connected sensor's json_attributes_topic
        (item #3) -- it must no longer exist as its own entity."""
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        topics = {c.args[0] for c in mock_mqtt.publish.call_args_list}
        assert (
            f"homeassistant/sensor/SkyFollower_receiver_{r._id}_localhost_30002_last_message_at/config"
            not in topics
        )

    def test_connected_sensor_has_json_attributes_topic(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        topic = f"homeassistant/sensor/SkyFollower_receiver_{r._id}_localhost_30002_connected/config"
        payloads = {c.args[0]: json.loads(c.args[1]) for c in mock_mqtt.publish.call_args_list}
        assert payloads[topic]["json_attributes_topic"] == (
            f"SkyFollower/receiver/{r._id}/statistic/localhost_30002_connected_attributes"
        )

    def test_has_entity_name_set_on_every_discovery_payload(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        discovery_calls = [c for c in mock_mqtt.publish.call_args_list if c.args[0].startswith("homeassistant/")]
        assert discovery_calls
        for call in discovery_calls:
            assert json.loads(call.args[1])["has_entity_name"] is True

    def test_no_entity_name_repeats_the_display_value(self):
        """Entity names must not bake in the receiver's display name --
        has_entity_name + the device block cover that. The {host}:{port}
        (and {source}, for Messages/sec) qualifiers on per-source sensors
        are unrelated to this and must survive."""
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "name": "Attic 1090",
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            r = Receiver(cfg)
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        for call in mock_mqtt.publish.call_args_list:
            if call.args[0].startswith("homeassistant/"):
                assert "Attic 1090" not in json.loads(call.args[1])["name"]


# ---------------------------------------------------------------------------
# Period-counter sensors published alongside telemetry/HA discovery --
# only present at all when Redis is configured.
# ---------------------------------------------------------------------------

class TestPeriodCounterTelemetryAndDiscovery:
    def _make_receiver(self, mock_redis=None):
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "name": "ATTIC",
            "redis": {"host": "redis.example.com", "port": 6379, "password": "secret"},
        }
        return _make_receiver_with_redis(cfg=cfg, mock_redis=mock_redis)

    # core-health is the sole publisher of messages_*_total_{hour,today}
    # (value and HA discovery), reading the cross-restart-durable Redis
    # counters the receiver's flush feeds. The receiver must not publish
    # those two topics itself -- doing so caused two retained publishers on
    # one topic to alternate their values. _total_lifetime is the exception:
    # it is a device-local in-memory figure the receiver DOES publish
    # directly (and its discovery), resetting on every receiver restart.

    def test_publish_telemetry_publishes_only_lifetime_not_hour_today(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = self._make_receiver(mock_redis=mock_redis)
        r._rates[("localhost", 30002)].hour_count = 3
        r._rates[("localhost", 30002)].today_count = 7
        r._rates[("localhost", 30002)].lifetime_count = 42
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        topics = set(calls)
        assert not any("_total_hour" in t or "_total_today" in t for t in topics)
        base = f"SkyFollower/receiver/{r._id}/statistic"
        # lifetime IS self-published, straight from the in-memory counter.
        assert calls[f"{base}/messages_localhost_30002_total_lifetime"] == "42"
        # The per-second rate and connection topics are still published.
        assert f"{base}/messages_localhost_30002_per_second" in topics

    def test_publish_telemetry_publishes_lifetime_without_redis(self):
        """The lifetime counter is in-memory only, so it is published even
        with no Redis configured at all."""
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            r = Receiver(cfg)
        r._rates[("localhost", 30002)].lifetime_count = 5
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        base = f"SkyFollower/receiver/{r._id}/statistic"
        assert calls[f"{base}/messages_localhost_30002_total_lifetime"] == "5"

    def test_ha_autodiscovery_publishes_only_lifetime_not_hour_today(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        r, _ = self._make_receiver(mock_redis=mock_redis)
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        payloads = {c.args[0]: json.loads(c.args[1]) for c in mock_mqtt.publish.call_args_list}
        topics = set(payloads)
        assert not any("_total_hour" in t or "_total_today" in t for t in topics)
        # The per-connection sensors it does own are still there.
        assert any("_per_second/config" in t for t in topics)
        # lifetime discovery IS published, as total_increasing (correct for
        # a counter that legitimately resets on a device restart).
        cfg_topic = (
            f"homeassistant/sensor/SkyFollower_receiver_{r._id}"
            f"_messages_localhost_30002_total_lifetime/config"
        )
        payload = payloads[cfg_topic]
        assert payload["state_class"] == "total_increasing"
        assert "unit_of_measurement" not in payload
        assert payload["state_topic"] == (
            f"SkyFollower/receiver/{r._id}/statistic/messages_localhost_30002_total_lifetime"
        )
        assert payload["unique_id"] == (
            f"SkyFollower_receiver_{r._id}_messages_localhost_30002_total_lifetime"
        )


class TestFlushPeriodCounters:
    def _make_receiver(self, mock_redis=None):
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "name": "ATTIC",
            "redis": {"host": "redis.example.com", "port": 6379, "password": "secret"},
        }
        return _make_receiver_with_redis(cfg=cfg, mock_redis=mock_redis)

    def test_flush_lazily_loads_lua_script_once(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_redis.script_load.return_value = "shaXYZ"
        r, _ = self._make_receiver(mock_redis=mock_redis)
        assert r._incr_period_counter_sha is None
        r._flush_period_counters()
        assert r._incr_period_counter_sha == "shaXYZ"
        mock_redis.script_load.assert_called_once()
        r._flush_period_counters()
        mock_redis.script_load.assert_called_once()  # Not reloaded on the second call.

    def test_flush_period_counters_calls_flush_to_redis_per_connection(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_redis.script_load.return_value = "shaXYZ"
        r, _ = self._make_receiver(mock_redis=mock_redis)
        r._rates[("localhost", 30002)].record()
        r._flush_period_counters()
        flushed = {c.args[2]: c.args[3] for c in mock_redis.evalsha.call_args_list}
        assert flushed["metrics:receiver:ATTIC:localhost_30002:messages:hour"] == 1
        assert flushed["metrics:receiver:ATTIC:localhost_30002:messages:today"] == 1
        # lifetime is never written to Redis -- no INCRBY, no lifetime key.
        mock_redis.incrby.assert_not_called()
        assert not any(
            "messages:lifetime" in c.args[2] for c in mock_redis.evalsha.call_args_list
        )

    def test_flush_period_counters_fails_soft_on_script_load_error(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_redis.script_load.side_effect = ConnectionError("boom")
        r, _ = self._make_receiver(mock_redis=mock_redis)
        r._flush_period_counters()  # Must not raise.
        assert r._incr_period_counter_sha is None

    def test_flush_period_counters_fails_soft_per_connection(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_redis.script_load.return_value = "shaXYZ"
        mock_redis.evalsha.side_effect = ConnectionError("boom")
        r, _ = self._make_receiver(mock_redis=mock_redis)
        r._rates[("localhost", 30002)].record()
        r._flush_period_counters()  # Must not raise.


class TestTelemetryLoopFlushIntegration:
    """_telemetry_loop must call _flush_period_counters() only when Redis
    is configured, on a fixed time-based cadence (MQTT_PUBLISH_INTERVAL_SECONDS)
    with no message-count trigger."""

    def _run_one_tick(self, r):
        def fake_wait(timeout=None):
            r._shutdown.set()
            return True

        with patch.object(r._shutdown, "wait", side_effect=fake_wait), \
             patch.object(r, "_publish_telemetry"):
            r._telemetry_loop()

    def test_telemetry_tick_flushes_when_redis_configured(self):
        mock_redis = MagicMock()
        mock_redis.set.return_value = True
        mock_redis.script_load.return_value = "shaXYZ"
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "name": "ATTIC",
            "redis": {"host": "redis.example.com", "port": 6379, "password": "secret"},
        }
        r, _ = _make_receiver_with_redis(cfg=cfg, mock_redis=mock_redis)
        with patch.object(r, "_flush_period_counters") as mock_flush:
            self._run_one_tick(r)
        mock_flush.assert_called_once()

    def test_telemetry_tick_does_not_flush_without_redis(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            r = Receiver(cfg)
        with patch.object(r, "_flush_period_counters") as mock_flush:
            self._run_one_tick(r)
        mock_flush.assert_not_called()


class TestPublishTelemetryVersion:
    """Tests for the version MQTT statistic published alongside started_at."""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
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
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        if name is not None:
            cfg["name"] = name
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def test_uses_friendly_name_when_set(self):
        r = self._make_receiver(name="Attic 1090")
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        payload = json.loads(mock_mqtt.publish.call_args_list[0].args[1])
        assert payload["device"]["name"] == "SkyFollower Receiver Attic 1090"
        assert payload["device"]["model"] == "Receiver"

    def test_falls_back_to_short_id_when_name_unset(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        payload = json.loads(mock_mqtt.publish.call_args_list[0].args[1])
        assert payload["device"]["name"] == f"SkyFollower Receiver {r._id[:8]}"
        assert payload["device"]["model"] == "Receiver"

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


# ---------------------------------------------------------------------------
# _sanitize_mqtt_id — dotted hosts must not break MQTT topics / HA identifiers
# ---------------------------------------------------------------------------

class TestSanitizeMqttId:
    """Regression coverage for _sanitize_mqtt_id() against representative
    illegal inputs -- dotted IPv4, FQDN, and IPv6 -- per Home Assistant's
    discovery charset requirement (^[a-zA-Z0-9_-]+$)."""

    _VALID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")

    def test_dotted_ipv4_matches_allowed_charset(self):
        result = _sanitize_mqtt_id("192.168.10.107")
        assert self._VALID_PATTERN.match(result)
        assert result == "192-168-10-107"

    def test_fqdn_matches_allowed_charset(self):
        result = _sanitize_mqtt_id("receiver.attic.example.com")
        assert self._VALID_PATTERN.match(result)
        assert result == "receiver-attic-example-com"

    def test_ipv6_matches_allowed_charset(self):
        result = _sanitize_mqtt_id("fe80::1ff:fe23:4567:890a")
        assert self._VALID_PATTERN.match(result)
        assert result == "fe80--1ff-fe23-4567-890a"

    def test_already_valid_value_is_unchanged(self):
        assert _sanitize_mqtt_id("localhost") == "localhost"
        assert _sanitize_mqtt_id("30002") == "30002"

    def test_underscore_and_hyphen_are_preserved(self):
        assert _sanitize_mqtt_id("my_host-01") == "my_host-01"


# ---------------------------------------------------------------------------
# Dotted-host end-to-end sanitization -- runtime state topics and HA
# discovery must agree on the identical sanitized name
# ---------------------------------------------------------------------------

class TestDottedHostSanitization:
    """Confirms _publish_telemetry() and _publish_ha_autodiscovery() both
    sanitize a dotted-IP source host, and that they agree on the same
    sanitized segment end-to-end (no drift between state and discovery)."""

    _DOTTED_HOST = "192.168.10.107"

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": self._DOTTED_HOST, "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def test_telemetry_topics_contain_no_illegal_characters(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        base = f"SkyFollower/receiver/{r._id}/statistic"
        topics = {c.args[0] for c in mock_mqtt.publish.call_args_list}
        assert f"{base}/messages_192-168-10-107_30002_per_second" in topics
        assert f"{base}/192-168-10-107_30002_connected" in topics
        assert f"{base}/192-168-10-107_30002_reconnect_count" in topics
        assert not any("." in t.rsplit("/", 1)[-1] for t in topics)

    def test_ha_discovery_object_id_and_unique_id_contain_no_illegal_characters(self):
        r = self._make_receiver()
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        found_per_source_sensor = False
        for call in mock_mqtt.publish.call_args_list:
            if call.args[0].startswith("homeassistant/"):
                cfg_payload = json.loads(call.args[1])
                assert re.match(r"^[a-zA-Z0-9_-]+$", cfg_payload["object_id"])
                assert re.match(r"^[a-zA-Z0-9_-]+$", cfg_payload["unique_id"])
                if "192-168-10-107" in cfg_payload["object_id"]:
                    found_per_source_sensor = True
        assert found_per_source_sensor

    def test_state_topic_and_discovery_state_topic_agree(self):
        """The discovery payload's state_topic must be byte-identical to a
        topic _publish_telemetry() actually publishes -- otherwise HA's
        entity would point at a topic that never receives data."""
        r = self._make_receiver()

        telemetry_mqtt = MagicMock()
        r._mqtt = telemetry_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        published_topics = {c.args[0] for c in telemetry_mqtt.publish.call_args_list}

        discovery_mqtt = MagicMock()
        r._mqtt = discovery_mqtt
        r._publish_ha_autodiscovery()
        discovery_state_topics = {
            json.loads(c.args[1])["state_topic"]
            for c in discovery_mqtt.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        }

        assert discovery_state_topics <= published_topics

    def test_connected_sensor_json_attributes_topic_agrees_with_publish(self):
        """The _connected sensor's json_attributes_topic must be
        byte-identical to the topic _publish_telemetry() actually publishes
        the {"last_message_received": ...} JSON to."""
        r = self._make_receiver()
        r._last_message_at[(self._DOTTED_HOST, 30002)] = "2026-01-15T10:00:00+00:00"

        telemetry_mqtt = MagicMock()
        r._mqtt = telemetry_mqtt
        r._mqtt_connected = True
        r._publish_telemetry()
        published_topics = {c.args[0] for c in telemetry_mqtt.publish.call_args_list}

        discovery_mqtt = MagicMock()
        r._mqtt = discovery_mqtt
        r._publish_ha_autodiscovery()
        topic = f"homeassistant/sensor/SkyFollower_receiver_{r._id}_192-168-10-107_30002_connected/config"
        payloads = {c.args[0]: json.loads(c.args[1]) for c in discovery_mqtt.publish.call_args_list}

        assert payloads[topic]["json_attributes_topic"] in published_topics


# ---------------------------------------------------------------------------
# HA discovery for started_at / version -- companion to the dotted-host fix,
# separate root cause: these fields were never added to the sensors list at
# all, so no discovery config was ever attempted for them.
# ---------------------------------------------------------------------------

class TestHaDiscoveryStartedAt:
    """started_at is already published as a state topic by
    _publish_telemetry(); _publish_ha_autodiscovery() must also announce
    it so Home Assistant creates an entity for it. (version is not given
    its own sensor -- build_ha_device() already reports it as the
    device's sw_version.)"""

    def _make_receiver(self):
        from receiver.main import Receiver
        cfg = {
            "sources": [{"host": "localhost", "port": 30002, "source": "1090"}],
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        }
        with patch("receiver.main.DATA_DIR", tempfile.mkdtemp()):
            return Receiver(cfg)

    def _discovery_payloads(self, r):
        mock_mqtt = MagicMock()
        r._mqtt = mock_mqtt
        r._mqtt_connected = True
        r._publish_ha_autodiscovery()
        return {
            call.args[0]: json.loads(call.args[1])
            for call in mock_mqtt.publish.call_args_list
            if call.args[0].startswith("homeassistant/")
        }

    def test_started_at_has_a_discovery_entry(self):
        r = self._make_receiver()
        payloads = self._discovery_payloads(r)
        topic = f"homeassistant/sensor/SkyFollower_receiver_{r._id}_started_at/config"
        assert topic in payloads
        assert payloads[topic]["state_topic"] == f"SkyFollower/receiver/{r._id}/statistic/started_at"

    def test_started_at_sensor_has_timestamp_device_class(self):
        r = self._make_receiver()
        payloads = self._discovery_payloads(r)
        topic = f"homeassistant/sensor/SkyFollower_receiver_{r._id}_started_at/config"
        assert payloads[topic]["device_class"] == "timestamp"

    def test_version_has_no_discovery_entry(self):
        """version is not a discovery sensor -- build_ha_device() already
        reports it as the device's sw_version, so a dedicated entity
        would be redundant."""
        r = self._make_receiver()
        payloads = self._discovery_payloads(r)
        topic = f"homeassistant/sensor/SkyFollower_receiver_{r._id}_version/config"
        assert topic not in payloads

    def test_started_at_name_reads_start_time(self):
        r = self._make_receiver()
        payloads = self._discovery_payloads(r)
        topic = f"homeassistant/sensor/SkyFollower_receiver_{r._id}_started_at/config"
        assert payloads[topic]["name"] == "Start Time"

    def test_started_at_object_id_and_unique_id_use_full_receiver_id(self):
        r = self._make_receiver()
        payloads = self._discovery_payloads(r)
        topic = f"homeassistant/sensor/SkyFollower_receiver_{r._id}_started_at/config"
        payload = payloads[topic]
        assert payload["object_id"] == f"SkyFollower_receiver_{r._id}_started_at"
        assert payload["unique_id"] == f"SkyFollower_receiver_{r._id}_started_at"
