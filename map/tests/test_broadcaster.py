"""
Unit tests for map/broadcaster.py's batching behavior. Pure asyncio, no
Redis/network -- driven with asyncio.run() rather than pytest-asyncio,
since CI's `run-tests.yaml` only installs pytest/pytest-xdist/pyyaml/httpx2
for every component (see that workflow's "Install dependencies" step),
not pytest-asyncio.
"""

from __future__ import annotations

import asyncio

from map.broadcaster import ConnectionManager


class _FakeWebSocket:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.sent: list[list[dict]] = []

    async def send_json(self, data):
        if self.fail:
            raise RuntimeError("connection closed")
        self.sent.append(data)


def test_publish_batches_multiple_events_into_one_frame():
    async def scenario():
        manager = ConnectionManager()
        ws = _FakeWebSocket()
        manager.register(ws)

        manager.publish({"type": "position", "icao_hex": "AAAAAA"})
        manager.publish({"type": "position", "icao_hex": "BBBBBB"})
        manager.publish({"type": "stale", "icao_hex": "CCCCCC"})

        sent_count = await manager.flush_once()
        assert sent_count == 3
        assert len(ws.sent) == 1  # one WS frame, not three
        assert ws.sent[0] == [
            {"type": "position", "icao_hex": "AAAAAA"},
            {"type": "position", "icao_hex": "BBBBBB"},
            {"type": "stale", "icao_hex": "CCCCCC"},
        ]

    asyncio.run(scenario())


def test_flush_with_nothing_buffered_sends_no_frame():
    async def scenario():
        manager = ConnectionManager()
        ws = _FakeWebSocket()
        manager.register(ws)

        sent_count = await manager.flush_once()
        assert sent_count == 0
        assert ws.sent == []

    asyncio.run(scenario())


def test_flush_reaches_every_registered_connection():
    async def scenario():
        manager = ConnectionManager()
        ws1, ws2 = _FakeWebSocket(), _FakeWebSocket()
        manager.register(ws1)
        manager.register(ws2)

        manager.publish({"type": "remove", "icao_hex": "DDDDDD"})
        await manager.flush_once()

        assert ws1.sent == [[{"type": "remove", "icao_hex": "DDDDDD"}]]
        assert ws2.sent == [[{"type": "remove", "icao_hex": "DDDDDD"}]]

    asyncio.run(scenario())


def test_dead_connection_is_dropped_without_affecting_others():
    async def scenario():
        manager = ConnectionManager()
        good, bad = _FakeWebSocket(), _FakeWebSocket(fail=True)
        manager.register(good)
        manager.register(bad)

        manager.publish({"type": "metadata", "icao_hex": "EEEEEE"})
        await manager.flush_once()

        assert good.sent == [[{"type": "metadata", "icao_hex": "EEEEEE"}]]
        assert bad not in manager._connections
        assert good in manager._connections

    asyncio.run(scenario())


def test_unregister_stops_future_sends():
    async def scenario():
        manager = ConnectionManager()
        ws = _FakeWebSocket()
        manager.register(ws)
        manager.unregister(ws)

        manager.publish({"type": "position", "icao_hex": "FFFFFF"})
        await manager.flush_once()

        assert ws.sent == []

    asyncio.run(scenario())


def test_publish_is_safe_to_call_from_a_worker_thread():
    """publish() is the cross-thread entrypoint the UDP listener and Redis
    expiry-listener threads actually use in map/main.py -- confirm it works
    when called from a real background thread, not just from within the
    event loop."""
    import threading

    async def scenario():
        manager = ConnectionManager()
        ws = _FakeWebSocket()
        manager.register(ws)

        def worker():
            manager.publish({"type": "position", "icao_hex": "010101"})

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        await manager.flush_once()
        assert ws.sent == [[{"type": "position", "icao_hex": "010101"}]]

    asyncio.run(scenario())
