"""
WebSocket fan-out with batching for the map service.

Events (`position`/`metadata` from the UDP listener, `stale`/`remove` from
the Redis expiry listener) are produced on background threads (see
map/main.py); ConnectionManager.publish() is the thread-safe entrypoint
those threads call. A single asyncio task (flush_loop, started from the
FastAPI app's lifespan) wakes every MAP_WS_BATCH_INTERVAL_SECONDS and sends
whatever accumulated since the last tick to every connected browser as one
JSON array frame -- coalescing several updates in the same window into one
WebSocket frame per connection instead of one frame per update.

publish() only appends to a lock-protected plain list; it never touches
asyncio primitives itself, so it's safe to call from any thread without
needing call_soon_threadsafe/run_coroutine_threadsafe plumbing -- the
asyncio side only ever reads the buffer from within the event loop, on
flush_loop's own turn.
"""

from __future__ import annotations

import asyncio
import logging
import threading

from fastapi import WebSocket

from shared.timing import MAP_WS_BATCH_INTERVAL_SECONDS

logger = logging.getLogger("map.broadcaster")


class ConnectionManager:
    def __init__(self, batch_interval_seconds: float = MAP_WS_BATCH_INTERVAL_SECONDS) -> None:
        self._batch_interval_seconds = batch_interval_seconds
        self._connections: set[WebSocket] = set()
        self._buffer: list[dict] = []
        self._lock = threading.Lock()

    def register(self, websocket: WebSocket) -> None:
        self._connections.add(websocket)

    def unregister(self, websocket: WebSocket) -> None:
        self._connections.discard(websocket)

    def publish(self, event: dict) -> None:
        """Thread-safe. Called from the UDP listener thread and the Redis
        expiry-listener thread; never from the asyncio event loop itself."""
        with self._lock:
            self._buffer.append(event)

    def _take_buffer(self) -> list[dict]:
        with self._lock:
            if not self._buffer:
                return []
            batch, self._buffer = self._buffer, []
            return batch

    async def flush_once(self) -> int:
        """Sends one batched frame (if there's anything buffered) to every
        connected client, dropping any connection whose send fails. Split
        out from flush_loop so tests can call it directly instead of
        sleeping through the real interval. Returns the number of events
        sent (0 if the buffer was empty -- no frame sent to anyone)."""
        batch = self._take_buffer()
        if not batch:
            return 0
        dead: list[WebSocket] = []
        for websocket in list(self._connections):
            try:
                await websocket.send_json(batch)
            except Exception as exc:
                logger.debug("Dropping WebSocket connection after send failure: %r", exc)
                dead.append(websocket)
        for websocket in dead:
            self._connections.discard(websocket)
        return len(batch)

    async def flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._batch_interval_seconds)
            await self.flush_once()
