"""
Tests for tools/tcp-traffic-replayer.

The tool's core logic - loading/filtering/sorting a capture, formatting a
readsb wire frame, and deciding per message whether to pace and what to send
in each mode - is all separated from the actual listen socket and main()
glue, so it is exercised here with a fake sink and a fake clock. No real
socket is opened and no wall-clock sleeping happens.

main() and _serve()/_wait_for_disconnect() (argument parsing, binding the
listen socket, the accept/reconnect loop) are intentionally not covered - they
are thin glue around a live socket with no branching logic that isn't already
exercised, directly or indirectly, by the tests below.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import os
import sys
import threading

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.abspath(os.path.join(_TOOL_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.adsb_1090 import parse_tcp_stream  # noqa: E402


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "tcp_traffic_replayer_main",
        os.path.join(_TOOL_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["tcp_traffic_replayer_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()
replay = _mod.replay
format_frame = _mod.format_frame
load_messages = _mod.load_messages


class FakeSink:
    """Stands in for a connected socket. Records every sendall() payload;
    optionally advances a FakeClock on each send (to simulate a replay that
    takes real time on the wire) and/or raises OSError after N sends (to
    simulate the connection dropping mid-replay)."""

    def __init__(self, clock=None, send_delay=0.0, fail_after=None):
        self.sent: list[bytes] = []
        self._clock = clock
        self._send_delay = send_delay
        self._fail_after = fail_after

    def sendall(self, data: bytes) -> None:
        if self._fail_after is not None and len(self.sent) >= self._fail_after:
            raise ConnectionResetError("connection reset by peer")
        self.sent.append(bytes(data))
        if self._clock is not None and self._send_delay:
            self._clock.now += self._send_delay


class FakeClock:
    """Replaces time.monotonic/time.sleep so relative-mode pacing can be
    asserted deterministically without the suite actually waiting."""

    def __init__(self, start: float = 1000.0):
        self.now = start
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _messages(*received_ats: float) -> list[dict]:
    return [
        {
            "raw": f"8D4840D{i:01X}202CC371C32CE0576098",
            "icao_hex": "A8AE7F" if i % 2 == 0 else "A00001",
            "received_at": ts,
            "source": "1090",
        }
        for i, ts in enumerate(received_ats)
    ]


class TestFormatFrame:
    """The tool must emit exactly what shared/adsb_1090.py's parse_tcp_stream
    (the receiver's own parser) expects: hex between a leading '*' and a
    trailing ';'."""

    def test_wraps_hex_in_star_and_semicolon_with_newline(self):
        assert format_frame("8D4840D6202CC371") == b"*8D4840D6202CC371;\n"

    def test_round_trips_through_the_receivers_parser(self):
        raws = ["8D4840D6202CC371C32CE0576098", "0200019905F658", "A0001838"]
        stream = b"".join(format_frame(r) for r in raws)

        # Feed it to the parser in one chunk, and again byte-by-byte, to
        # prove framing survives arbitrary TCP segmentation.
        assert parse_tcp_stream(stream, bytearray()) == raws

        buf = bytearray()
        collected: list[str] = []
        for byte in stream:
            collected.extend(parse_tcp_stream(bytes([byte]), buf))
        assert collected == raws


class TestLoadMessages:
    """Only source==1090 rows are served; everything else is filtered out and
    counted (never silently dropped), and the kept rows are sorted by
    received_at regardless of file order."""

    def _write(self, path, rows, gzipped=False):
        opener = gzip.open if gzipped else open
        with opener(path, "wt") as handle:
            for row in rows:
                handle.write(json.dumps(row) + "\n")

    def test_filters_non_1090_rows_and_reports_the_count(self, tmp_path):
        rows = [
            {"raw": "AA", "icao_hex": "A1", "received_at": 3.0, "source": "1090"},
            {"raw": "BB", "icao_hex": "A2", "received_at": 1.0, "source": "978"},
            {"raw": "CC", "icao_hex": "A3", "received_at": 2.0, "source": "1090"},
            {"raw": "DD", "icao_hex": "A4", "received_at": 4.0, "source": "EXTERNAL"},
        ]
        path = tmp_path / "capture.ndjson"
        self._write(path, rows)

        kept, discarded = load_messages(str(path))

        assert discarded == 2
        assert [m["raw"] for m in kept] == ["CC", "AA"]  # sorted by received_at

    def test_reads_a_gzip_capture_by_extension(self, tmp_path):
        rows = [
            {"raw": "AA", "icao_hex": "A1", "received_at": 2.0, "source": "1090"},
            {"raw": "BB", "icao_hex": "A2", "received_at": 1.0, "source": "1090"},
        ]
        path = tmp_path / "capture.ndjson.gz"
        self._write(path, rows, gzipped=True)

        kept, discarded = load_messages(str(path))

        assert discarded == 0
        assert [m["raw"] for m in kept] == ["BB", "AA"]

    def test_skips_malformed_json_lines(self, tmp_path):
        path = tmp_path / "capture.ndjson"
        with open(path, "wt") as handle:
            handle.write('{"raw":"AA","icao_hex":"A1","received_at":1.0,"source":"1090"}\n')
            handle.write("not json at all\n")
            handle.write("\n")
            handle.write('{"raw":"BB","icao_hex":"A2","received_at":2.0,"source":"1090"}\n')

        kept, discarded = load_messages(str(path))

        assert [m["raw"] for m in kept] == ["AA", "BB"]
        assert discarded == 0


class TestReplayStressMode:
    """Stress mode exists to drive the receiver's TCP ingest past its own CPU
    ceiling - it must never sleep, whatever the capture's original timing."""

    def test_sends_every_frame_with_no_pacing(self, monkeypatch):
        monkeypatch.setattr(
            _mod.time, "sleep", lambda *_: pytest.fail("stress mode must not sleep")
        )
        sink = FakeSink()
        messages = _messages(100.0, 100.5, 101.0)

        outcome = replay(messages, sink, "stress", threading.Event())

        assert outcome.complete
        assert outcome.sent == 3
        assert sink.sent == [format_frame(m["raw"]) for m in messages]

    def test_empty_capture_completes_with_zero_sent(self):
        outcome = replay([], FakeSink(), "stress", threading.Event())
        assert outcome.complete
        assert outcome.sent == 0


class TestReplayRelativeMode:
    """Relative mode reconstructs the gaps between the capture's original
    received_at timestamps."""

    def test_sleeps_to_preserve_original_inter_message_timing(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_mod.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(_mod.time, "sleep", clock.sleep)
        sink = FakeSink()
        messages = _messages(500.0, 502.0, 505.0)

        outcome = replay(messages, sink, "relative", threading.Event())

        assert outcome.sent == 3
        # First frame goes out immediately; each later one waits exactly the
        # gap since the previous frame's original timestamp.
        assert clock.sleeps == [2.0, 3.0]

    def test_does_not_burst_to_catch_up_once_behind_schedule(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_mod.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(_mod.time, "sleep", clock.sleep)
        # 10s "on the wire" per frame puts the replay well behind its target
        # schedule; later frames must then go out immediately rather than
        # sleeping a now-in-the-past interval.
        sink = FakeSink(clock=clock, send_delay=10.0)
        messages = _messages(500.0, 501.0, 502.0)

        outcome = replay(messages, sink, "relative", threading.Event())

        assert outcome.sent == 3
        assert clock.sleeps == []


class TestReplayConnectionDropped:
    """A connection dropping before the whole capture is sent is the one case
    that is real message loss and must be surfaced distinctly."""

    def test_reports_partial_send_when_sink_fails_midway(self):
        sink = FakeSink(fail_after=2)
        messages = _messages(1.0, 2.0, 3.0, 4.0, 5.0)

        outcome = replay(messages, sink, "stress", threading.Event())

        assert not outcome.complete
        assert outcome.reason == "connection-closed"
        assert outcome.sent == 2
        assert outcome.total == 5


class TestReplayStopEvent:
    def test_stops_immediately_when_stop_event_already_set(self):
        sink = FakeSink()
        stop_event = threading.Event()
        stop_event.set()

        outcome = replay(_messages(1.0, 2.0, 3.0), sink, "stress", stop_event)

        assert outcome.reason == "stopped"
        assert outcome.sent == 0
        assert sink.sent == []
