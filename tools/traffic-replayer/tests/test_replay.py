"""
Tests for tools/traffic-replayer's replay() function and queue routing.

The replayer's core logic is pika-based RabbitMQ publishing under two
timing modes; these tests fake both the channel and the clock so no real
broker connection and no actual wall-clock sleeping is required.

main() itself (CLI argument parsing, opening a real RabbitMQ connection,
loading/sorting the NDJSON capture file) is intentionally not covered here
-- it is thin glue around a live broker connection with nothing in it that
isn't already exercised, directly or indirectly, by the tests below.
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


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "traffic_replayer_main",
        os.path.join(_TOOL_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["traffic_replayer_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()
replay = _mod.replay
_load_capture = _mod._load_capture
ADSB_EXCHANGE = _mod.ADSB_EXCHANGE


class FakeChannel:
    """Stands in for a pika BlockingChannel. Records every basic_publish
    call; optionally advances a FakeClock on each publish to simulate a
    replay that takes real time to send (see TestReplayRelativeMode)."""

    def __init__(self, clock: "FakeClock | None" = None, publish_delay: float = 0.0):
        self.published: list[tuple[str, str, str]] = []
        self._clock = clock
        self._publish_delay = publish_delay

    def basic_publish(self, exchange, routing_key, body, properties=None):
        self.published.append((exchange, routing_key, body))
        if self._clock is not None and self._publish_delay:
            self._clock.now += self._publish_delay


class FakeClock:
    """Replaces time.monotonic/time.sleep so relative-mode timing can be
    asserted deterministically, without the test suite actually waiting."""

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
            "raw": f"raw{i}",
            "icao_hex": "A8AE7F" if i % 2 == 0 else "A00001",
            "received_at": ts,
            "source": "1090",
        }
        for i, ts in enumerate(received_ats)
    ]


class TestExchangeRouting:
    """The replayer routes exactly as the receiver does: one publish to the
    consistent-hash exchange per message, keyed by ICAO hex, leaving the
    broker to pick which message processor receives it."""

    def test_publishes_to_the_hash_exchange_keyed_by_icao_hex(self):
        channel = FakeChannel()
        messages = _messages(100.0, 100.5)

        replay(messages, channel, mode="stress", stop_event=threading.Event())

        assert [(e, k) for e, k, _ in channel.published] == [
            (ADSB_EXCHANGE, "A8AE7F"),
            (ADSB_EXCHANGE, "A00001"),
        ]

    def test_never_publishes_to_the_default_exchange(self):
        """Publishing with exchange="" would address a queue by name, which
        is the routing scheme this tool no longer uses."""
        channel = FakeChannel()
        messages = _messages(1.0, 2.0, 3.0, 4.0)

        replay(messages, channel, mode="stress", stop_event=threading.Event())

        assert [e for e, _k, _b in channel.published] == [ADSB_EXCHANGE] * 4


class TestReplayStressMode:
    """Stress mode exists to load-test the pipeline as fast as RabbitMQ
    will accept -- it must never sleep, regardless of the captured
    messages' original timing."""

    def test_publishes_every_message_with_no_delay(self, monkeypatch):
        monkeypatch.setattr(
            _mod.time, "sleep", lambda *_: pytest.fail("stress mode must not sleep")
        )
        channel = FakeChannel()
        messages = _messages(100.0, 100.5, 101.0)

        count = replay(
            messages, channel, mode="stress", stop_event=threading.Event()
        )

        assert count == 3
        assert len(channel.published) == 3
        for (exchange, routing_key, body), msg in zip(channel.published, messages):
            assert exchange == ADSB_EXCHANGE
            assert routing_key == msg["icao_hex"]
            assert msg["raw"] in body

    def test_empty_message_list_returns_zero(self):
        channel = FakeChannel()
        count = replay(
            [], channel, mode="stress", stop_event=threading.Event()
        )
        assert count == 0
        assert channel.published == []


class TestReplayRelativeMode:
    """Relative mode exists to reproduce production-like message spacing
    against the live pipeline, so it must sleep to reconstruct the gaps
    between the capture's original received_at timestamps."""

    def test_sleeps_to_preserve_original_inter_message_timing(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(_mod.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(_mod.time, "sleep", clock.sleep)
        channel = FakeChannel()
        # Second message arrived 2s after the first; third arrived another
        # 3s after that.
        messages = _messages(500.0, 502.0, 505.0)

        count = replay(
            messages, channel, mode="relative", stop_event=threading.Event()
        )

        assert count == 3
        # The first message publishes immediately (nothing to catch up to
        # yet); each later message sleeps for exactly the gap since the
        # previous one's original timestamp.
        assert clock.sleeps == [2.0, 3.0]

    def test_does_not_sleep_once_replay_has_fallen_behind_schedule(self, monkeypatch):
        # A slow publish (10s per message here) can put the replay behind
        # its own target schedule; once that happens, later messages must
        # publish immediately rather than sleeping further, since their
        # target time is already in the past.
        clock = FakeClock()
        monkeypatch.setattr(_mod.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(_mod.time, "sleep", clock.sleep)
        channel = FakeChannel(clock=clock, publish_delay=10.0)
        messages = _messages(500.0, 501.0, 502.0)

        count = replay(
            messages, channel, mode="relative", stop_event=threading.Event()
        )

        assert count == 3
        assert clock.sleeps == []


_CAPTURE_LINES = [
    {"raw": "8D4840D6202CC371C32CE0576098", "icao_hex": "4840D6",
     "received_at": 1.0, "source": "1090"},
    {"raw": "8D40621D58C382D690C8AC2863A7", "icao_hex": "40621D",
     "received_at": 2.5, "source": "978"},
    {"raw": "8DA8AE7F9911088D3020009BD3F0", "icao_hex": "A8AE7F",
     "received_at": 0.5, "source": "MLAT"},
]


def _write_ndjson(path, records) -> None:
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _write_ndjson_gz(path, records) -> None:
    with gzip.open(path, "wt") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


class TestLoadCapture:
    """_load_capture() reads either a plain .ndjson file or a gzip-compressed
    .ndjson.gz file (chosen by extension), with identical per-line parsing
    once the file object is yielding text."""

    def test_plain_ndjson_loads_every_line(self, tmp_path):
        p = tmp_path / "capture.ndjson"
        _write_ndjson(p, _CAPTURE_LINES)
        assert _load_capture(str(p)) == _CAPTURE_LINES

    def test_gzip_capture_parses_to_the_same_list_as_its_uncompressed_form(self, tmp_path):
        plain = tmp_path / "capture.ndjson"
        gz = tmp_path / "capture.ndjson.gz"
        _write_ndjson(plain, _CAPTURE_LINES)
        _write_ndjson_gz(gz, _CAPTURE_LINES)

        assert _load_capture(str(gz)) == _load_capture(str(plain)) == _CAPTURE_LINES

    def test_blank_lines_are_ignored_in_both_forms(self, tmp_path):
        gz = tmp_path / "capture.ndjson.gz"
        with gzip.open(gz, "wt") as f:
            f.write(json.dumps(_CAPTURE_LINES[0]) + "\n")
            f.write("\n")
            f.write("   \n")
            f.write(json.dumps(_CAPTURE_LINES[1]) + "\n")

        assert _load_capture(str(gz)) == _CAPTURE_LINES[:2]

    def test_malformed_line_is_skipped_with_a_warning(self, tmp_path, capsys):
        p = tmp_path / "capture.ndjson"
        with open(p, "w") as f:
            f.write(json.dumps(_CAPTURE_LINES[0]) + "\n")
            f.write("{ not json\n")
            f.write(json.dumps(_CAPTURE_LINES[1]) + "\n")

        assert _load_capture(str(p)) == _CAPTURE_LINES[:2]
        assert "skipping line 2" in capsys.readouterr().err

    def test_gz_path_that_is_not_gzip_fails_with_one_clear_error(self, tmp_path, capsys):
        """A .gz file that is really plain text must raise a single clear
        error, not emit a skipped-line warning for every line in the file."""
        gz = tmp_path / "capture.ndjson.gz"
        _write_ndjson(gz, _CAPTURE_LINES)  # plain NDJSON, never compressed

        with pytest.raises(SystemExit) as exc:
            _load_capture(str(gz))

        assert "gzip" in str(exc.value).lower()
        assert "skipping line" not in capsys.readouterr().err

    def test_truncated_gz_fails_with_one_clear_error(self, tmp_path, capsys):
        gz = tmp_path / "capture.ndjson.gz"
        _write_ndjson_gz(gz, _CAPTURE_LINES)
        data = gz.read_bytes()
        gz.write_bytes(data[: len(data) // 2])

        with pytest.raises(SystemExit) as exc:
            _load_capture(str(gz))

        assert "gzip" in str(exc.value).lower()
        assert "skipping line" not in capsys.readouterr().err


class TestReplayStopEvent:
    def test_stops_early_when_stop_event_already_set(self):
        channel = FakeChannel()
        stop_event = threading.Event()
        stop_event.set()
        messages = _messages(1.0, 2.0, 3.0)

        count = replay(
            messages, channel, mode="stress", stop_event=stop_event
        )

        assert count == 0
        assert channel.published == []
