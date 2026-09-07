#!/usr/bin/env python3
"""
SkyFollower Map Service

Backend for the live-map frontend (`frontend/`, a separate Vite project --
see map/README.md). Three independent jobs, plus serving the built
frontend itself:

1. A UDP listener that receives `position`/`metadata` datagrams from any/all
   message-processor instances (message-processor/main.py's
   `_MapUdpPublisher`) and merges each into per-aircraft current-state held
   in a dedicated Redis instance -- never core Redis.
2. A Redis keyspace-notification listener that turns key expiry into
   `stale`/`remove` WebSocket events (no app-level timer loop scanning for
   expired aircraft -- expiry itself is the signal).
3. A FastAPI app exposing `GET /api/flights` (a snapshot) and `WS /ws`
   (a live, batched relay of position/metadata/stale/remove events), and
   serving the frontend's built static assets (`frontend/dist/`, Vite's
   `base: '/map/'` output) under `/map` with SPA-fallback routing.

One process runs all three; there is exactly one map service instance (no
MESSAGE_PROCESSOR_ID-style horizontal scaling here -- see map/README.md).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import socket
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

# Add the repo root to sys.path so shared/ is importable when this module is
# run outside Docker (e.g. tests, local `uvicorn map.main:app`). In the
# Docker image PYTHONPATH=/app already covers this.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.config import load_config  # noqa: E402
from shared.logging_setup import configure_logging  # noqa: E402
from shared.redis_client import build_redis_client  # noqa: E402
from shared.timing import (  # noqa: E402
    HEALTHCHECK_INTERVAL_SECONDS,
    RECONNECT_BACKOFF_SECONDS,
)

from map.broadcaster import ConnectionManager  # noqa: E402
from map.state_store import POSITION_FIELDS, FlightStateStore  # noqa: E402

logger = logging.getLogger("map")

# Same fixed path every other long-running SkyFollower service writes its
# Docker HEALTHCHECK heartbeat to -- see shared/healthcheck.py.
_HEALTHCHECK_HEARTBEAT_PATH = "/app/health/heartbeat"

# recvfrom() bound so the UDP listener thread wakes periodically to check
# the shutdown event, rather than blocking forever on a socket with no
# incoming traffic.
_UDP_RECV_TIMEOUT_SECONDS = 1.0
_UDP_MAX_DATAGRAM_BYTES = 65535

# Vite's build output (map/frontend/vite.config.ts sets base: '/map/' so
# built asset URLs already point at this same sub-path). Only present in
# the Docker image (map/Dockerfile's frontend-build stage) or after a
# manual `npm run build` in map/frontend/ -- see the mount guard below,
# which degrades to a 404-only /map rather than failing app startup when
# it's absent (e.g. bare `pytest map/tests`, `uvicorn map.main:app`
# outside Docker).
_FRONTEND_DIST_DIR = os.path.join(_HERE, "frontend", "dist")


class _SPAStaticFiles(StaticFiles):
    """Serves the built frontend with real single-page-app fallback: any
    GET/HEAD under /map/* that doesn't resolve to an actual file in dist/
    (a deep link into a client-side route, or a plain refresh of one) gets
    index.html instead of a bare HTTP 404, so client-side routing survives
    a full page reload. Plain `html=True` alone only covers the mount's
    own directory index (`/map`/`/map/`) -- it does not fall back to
    index.html for an arbitrary unmatched sub-path, which is what SPA
    deep-link support actually requires."""

    async def get_response(self, path: str, scope):
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == 404 and scope["method"] in ("GET", "HEAD"):
                return await super().get_response("index.html", scope)
            raise


# Module-level state, built in lifespan() -- same convention
# management-ui/backend/main.py uses (globals rather than app.state), since
# every route handler and background-thread callback needs a plain
# reference to these without threading a request/app object through.
#
# _connections is rebuilt fresh in lifespan() on every startup, same as
# _redis/_store -- it is NOT a long-lived singleton. A WebSocket object is
# tied to the ASGI event loop/portal it was accepted on; reusing one
# ConnectionManager (and the connections registered in it) across more than
# one lifespan cycle in the same process would let a connection from a
# previous, now-torn-down loop generation sit in `_connections` and hang
# flush_once()'s `await websocket.send_json()` on it forever, starving
# every other, currently-live connection sharing the same flush loop. In
# production this never matters (lifespan runs exactly once per process
# lifetime); it matters a great deal in tests, which construct a fresh
# TestClient(app) -- and therefore a fresh lifespan cycle -- per test.
_cfg: dict = {}
_redis = None
_store: Optional[FlightStateStore] = None
_connections = ConnectionManager()
_shutdown = threading.Event()
_threads: list[threading.Thread] = []


def _extract_timestamp(payload: dict) -> Optional[float]:
    """The out-of-order guard's comparison key for one UDP packet.

    `position` packets carry a numeric `timestamp` (message-processor's
    `received_at`) directly. `metadata` packets don't -- they're shaped
    like message-processor's CompletedFlight notification payload, whose
    closest equivalent is `last_message` (an ISO-8601 string). Both are
    stamped from the exact same `received_at` value for one source ADS-B
    message (see message-processor/main.py's `_update_flight`), so this
    keeps the two packet types on one comparable clock."""
    if "timestamp" in payload:
        try:
            return float(payload["timestamp"])
        except (TypeError, ValueError):
            return None
    last_message = payload.get("last_message")
    if last_message:
        try:
            return datetime.fromisoformat(str(last_message).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def _handle_packet(payload: dict) -> None:
    """Dispatches one decoded UDP datagram: applies it to Redis state (out-
    of-order guard + merge, see FlightStateStore.apply_update) and, if
    accepted, publishes the corresponding live WebSocket event."""
    msg_type = payload.get("type")
    if msg_type == "position":
        icao_hex = payload.get("icao_hex")
    elif msg_type == "metadata":
        # metadata packets are message-processor's CompletedFlight-shape
        # payload -- icao_hex only ever appears nested inside `aircraft`,
        # never as a top-level key (see CompletedFlight.aircraft in
        # shared/models.py, always populated with icao_hex).
        icao_hex = (payload.get("aircraft") or {}).get("icao_hex")
    else:
        logger.debug("Ignoring UDP datagram with unknown type %r", msg_type)
        return

    if not icao_hex:
        logger.debug("Ignoring UDP datagram with no icao_hex: %r", payload)
        return

    timestamp = _extract_timestamp(payload)
    if timestamp is None:
        logger.debug("Ignoring UDP datagram with no usable timestamp: %r", payload)
        return

    if msg_type == "position":
        fields = {k: payload[k] for k in POSITION_FIELDS if k in payload}
    else:
        fields = {k: v for k, v in payload.items() if k not in ("type", "icao_hex")}

    merged = _store.apply_update(icao_hex, msg_type, timestamp, fields)
    if merged is None:
        return  # Dropped as out-of-order.

    if msg_type == "position":
        event = {"type": "position", "icao_hex": icao_hex}
        for field in POSITION_FIELDS:
            if field in merged:
                event[field] = merged[field]
    else:
        # metadata events carry the full merged current-state -- both
        # position and metadata fields -- so a client that only just
        # connected (and so missed any earlier `position` events) still has
        # everything needed to place and label the aircraft. This is
        # deliberately the same shape GET /api/flights returns.
        event = dict(merged)
        event["type"] = "metadata"
    _connections.publish(event)


def _udp_loop() -> None:
    listen_host = _cfg["map_listen_host"]
    listen_port = _cfg["map_listen_port"]
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((listen_host, listen_port))
    sock.settimeout(_UDP_RECV_TIMEOUT_SECONDS)
    logger.info("UDP listener bound to %s:%s.", listen_host, listen_port)

    try:
        while not _shutdown.is_set():
            try:
                data, _addr = sock.recvfrom(_UDP_MAX_DATAGRAM_BYTES)
            except socket.timeout:
                continue
            except OSError as exc:
                if _shutdown.is_set():
                    break
                logger.warning("UDP socket error: %r", exc)
                continue

            try:
                payload = json.loads(data.decode("utf-8"))
            except Exception as exc:
                logger.debug("Unparseable UDP datagram (%d bytes): %r", len(data), exc)
                continue

            try:
                _handle_packet(payload)
            except Exception:
                logger.exception("Error handling UDP datagram: %r", payload)
    finally:
        sock.close()


def _eviction_loop() -> None:
    """Subscribes to Redis's `expired` keyevent notifications and turns
    each flight:live/flight:detail expiry into a `stale`/`remove`
    WebSocket event -- eviction is driven entirely by this signal, no
    app-level scan/timer loop. Reconnects (re-enabling keyspace
    notifications each time, since it's a runtime CONFIG SET this
    no-persistence Redis instance never remembers across its own restart)
    on any pubsub error."""
    while not _shutdown.is_set():
        try:
            _store.enable_keyspace_notifications()
            pubsub = _redis.pubsub()
            pubsub.psubscribe("__keyevent@0__:expired")
            try:
                while not _shutdown.is_set():
                    message = pubsub.get_message(timeout=1.0)
                    if message is None or message.get("type") != "pmessage":
                        continue
                    event = _store.handle_expired_key(message["data"])
                    if event is not None:
                        _connections.publish(event)
            finally:
                pubsub.close()
        except Exception as exc:
            if _shutdown.is_set():
                break
            logger.warning(
                "Eviction listener error: %r; reconnecting in %ss…",
                exc, RECONNECT_BACKOFF_SECONDS,
            )
            time.sleep(RECONNECT_BACKOFF_SECONDS)


def _healthcheck_loop() -> None:
    """Touches a heartbeat file while genuinely connected to Redis, for
    Docker's HEALTHCHECK to check the mtime of -- same mechanism/constants
    as message-processor's own _healthcheck_loop (shared/healthcheck.py).

    The `mkdir` is inside the same try/except as the ping/touch below (not
    hoisted above the loop) so a missing/read-only `/app/health` -- always
    present in the real container via the Dockerfile's COPY + Compose bind
    mount, but not when this module runs bare (a local `pytest map/tests`
    run, `uvicorn map.main:app` outside Docker) -- degrades to a silently
    skipped heartbeat write each cycle instead of crashing this thread
    outright on its very first iteration."""
    heartbeat_path = pathlib.Path(_HEALTHCHECK_HEARTBEAT_PATH)
    while not _shutdown.is_set():
        try:
            heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            _redis.ping()
            heartbeat_path.touch()
        except Exception as exc:
            logger.debug("Healthcheck heartbeat write failed: %r", exc)
        _shutdown.wait(HEALTHCHECK_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cfg, _redis, _store, _connections

    _cfg = load_config("map_redis", "map")
    configure_logging(_cfg.get("log_level"))

    _redis = build_redis_client(_cfg["map_redis"])
    _store = FlightStateStore(
        _redis,
        stale_seconds=_cfg["map_stale_seconds"],
        evict_seconds=_cfg["map_evict_seconds"],
    )
    _store.enable_keyspace_notifications()
    # Fresh every startup -- see the module-level comment on _connections
    # above for why this must never be reused across lifespan cycles.
    _connections = ConnectionManager()

    _shutdown.clear()
    _threads.clear()
    for target, name in (
        (_udp_loop, "udp-listener"),
        (_eviction_loop, "eviction-listener"),
        (_healthcheck_loop, "healthcheck"),
    ):
        thread = threading.Thread(target=target, daemon=True, name=name)
        thread.start()
        _threads.append(thread)

    flush_task = asyncio.ensure_future(_connections.flush_loop())

    logger.info("Map service started.")
    yield
    logger.info("Map service shutting down.")

    flush_task.cancel()
    try:
        await flush_task
    except asyncio.CancelledError:
        pass
    _shutdown.set()
    for thread in _threads:
        thread.join(timeout=5)


app = FastAPI(
    title="SkyFollower Map",
    description="Live aircraft position/metadata feed for the map "
    "frontend: a UDP listener fed by message-processor instances, a "
    "dedicated Redis instance holding current per-aircraft state, a "
    "GET /api/flights snapshot, and a batched WS /ws live relay. Also "
    "serves the built frontend SPA itself, mounted at /map.",
    version="9999.99.99",
    lifespan=lifespan,
)


@app.get("/api/flights", tags=["flights"])
def get_flights() -> list[dict]:
    """One object per currently-tracked aircraft -- the same merged
    current-state shape a WebSocket `metadata` message carries (see
    _handle_packet)."""
    return _store.list_flights()


@app.websocket("/ws")
async def flights_ws(websocket: WebSocket) -> None:
    """One connection per browser. Never sends a snapshot -- only
    `position`/`metadata`/`stale`/`remove` events, batched by
    ConnectionManager.flush_loop() (see map/broadcaster.py). Callers should
    GET /api/flights first for the initial snapshot, then open this for
    live updates."""
    await websocket.accept()
    _connections.register(websocket)
    try:
        while True:
            # This service never expects a client message -- just waits
            # for the connection to close (WebSocketDisconnect) so it can
            # unregister. The transport layer answers ping/pong itself.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _connections.unregister(websocket)


# Registered last, after both API routes above -- a Mount only ever
# matches paths starting with /map (Starlette compiles it to
# "/map/{path:path}"), so it can never shadow /api/flights or /ws
# regardless of registration order, but this ordering keeps the specific
# routes visually grouped ahead of the catch-all frontend mount. Guarded
# on the directory actually existing so importing this module (e.g. `pytest
# map/tests`, `uvicorn map.main:app` outside Docker) never fails just
# because `npm run build` hasn't been run locally -- the Docker image
# always has it (see map/Dockerfile's frontend-build stage).
if os.path.isdir(_FRONTEND_DIST_DIR):
    app.mount(
        "/map",
        _SPAStaticFiles(directory=_FRONTEND_DIST_DIR, html=True),
        name="map-frontend",
    )
else:
    logger.warning(
        "Frontend build not found at %s -- /map will 404 until `npm run "
        "build` has been run in map/frontend/ (always present in the "
        "Docker image).",
        _FRONTEND_DIST_DIR,
    )


def main() -> None:  # pragma: no cover -- exercised via `python -m map.main`
    import uvicorn

    cfg = load_config("map_redis", "map")
    uvicorn.run(
        app,
        host=cfg["map_http_host"],
        port=cfg["map_http_port"],
        log_config=None,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
