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

import importlib.util
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
_queue_name = _mod._queue_name


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


class TestQueueName:
    def test_matches_icao_hex_modulo_processor_count(self):
        assert _queue_name("A8AE7F", 4) == f"adsb-{int('A8AE7F', 16) % 4}"

    def test_single_processor_always_queue_zero(self):
        assert _queue_name("FFFFFF", 1) == "adsb-0"


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
            messages, channel, processor_count=2, mode="stress", stop_event=threading.Event()
        )

        assert count == 3
        assert len(channel.published) == 3
        for (exchange, routing_key, body), msg in zip(channel.published, messages):
            assert exchange == ""
            assert routing_key == _queue_name(msg["icao_hex"], 2)
            assert msg["raw"] in body

    def test_empty_message_list_returns_zero(self):
        channel = FakeChannel()
        count = replay(
            [], channel, processor_count=1, mode="stress", stop_event=threading.Event()
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
            messages, channel, processor_count=1, mode="relative", stop_event=threading.Event()
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
            messages, channel, processor_count=1, mode="relative", stop_event=threading.Event()
        )

        assert count == 3
        assert clock.sleeps == []


class TestReplayStopEvent:
    def test_stops_early_when_stop_event_already_set(self):
        channel = FakeChannel()
        stop_event = threading.Event()
        stop_event.set()
        messages = _messages(1.0, 2.0, 3.0)

        count = replay(
            messages, channel, processor_count=1, mode="stress", stop_event=stop_event
        )

        assert count == 0
        assert channel.published == []
