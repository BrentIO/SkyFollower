#!/usr/bin/env python3
"""
SkyFollower Map Service

Backend for the live-map frontend (`frontend/`). Three jobs, plus serving
the built frontend: a UDP listener merging `position`/`metadata`/
`heartbeat` datagrams from message-processor instances into per-aircraft
state and a per-processor liveness roster (both in a dedicated Redis
instance); a Redis keyspace-notification listener turning key expiry into
`stale`/`remove` WebSocket events; and a FastAPI app exposing
`GET /api/flights`, `GET /api/processors`, and `WS /ws`.

One process runs all three; there is exactly one map service instance.
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

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

# Add the repo root to sys.path so shared/ is importable when this module is
# run outside Docker (PYTHONPATH=/app already covers that case).
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.config import load_config  # noqa: E402
from shared.logging_setup import configure_logging  # noqa: E402
from shared.mqtt_presence import MqttPresence  # noqa: E402
from shared.redis_client import build_redis_client  # noqa: E402
from shared.timing import (  # noqa: E402
    HEALTHCHECK_INTERVAL_SECONDS,
    MAP_RANGE_OUTLINE_SNAPSHOT_INTERVAL_SECONDS,
    MAP_RANGE_OUTLINE_TTL_SECONDS,
    RECONNECT_BACKOFF_SECONDS,
)

from map.broadcaster import ConnectionManager  # noqa: E402
from map.range_outline import RangeOutlineStore  # noqa: E402
from map.state_store import (  # noqa: E402
    POSITION_FIELDS,
    FlightStateStore,
    overall_processor_status,
)

logger = logging.getLogger("map")

# Fixed path every long-running SkyFollower service writes its Docker
# HEALTHCHECK heartbeat to -- see shared/healthcheck.py.
_HEALTHCHECK_HEARTBEAT_PATH = "/app/health/heartbeat"

# Fixed in-container TLS mount point (docker-compose.map.yaml's
# ./data/map/tls bind mount, populated by scripts/install.sh or an
# operator's own cert/key dropped in under these filenames).
_TLS_CERT_PATH = "/app/tls/cert.pem"
_TLS_KEY_PATH = "/app/tls/key.pem"

# Where the daily range-outline snapshots are written. MAP_RANGE_OUTLINE_DIR
# overrides it only so the test suite can point it at a tmp directory.
_RANGE_OUTLINE_DIR = os.environ.get("MAP_RANGE_OUTLINE_DIR", "/app/range-outline")

# recvfrom() bound so the UDP listener thread wakes periodically to check
# the shutdown event, rather than blocking forever on a socket with no
# incoming traffic.
_UDP_RECV_TIMEOUT_SECONDS = 1.0
_UDP_MAX_DATAGRAM_BYTES = 65535

# Requested kernel receive buffer size for the UDP socket -- best-effort,
# the OS caps this at its own configured maximum. Bigger than the default
# gives _handle_packet's single-threaded processing more slack to fall
# behind briefly without the kernel dropping datagrams in the meantime.
_UDP_RECV_BUFFER_BYTES = 1024 * 1024

# Vite's build output. Only present in the Docker image or after a manual
# `npm run build` -- the mount guard below degrades to a 404-only /map
# rather than failing app startup when it's absent.
_FRONTEND_DIST_DIR = os.path.join(_HERE, "frontend", "dist")

# The only files under dist/assets/ that aren't content-hashed by Vite --
# must match vite.config.ts's MAPLIBRE_WORKER_FILES exactly (see that
# file's comment for why they're copied verbatim under stable names).
_UNHASHED_ASSET_NAMES = frozenset(("maplibre-gl-worker.mjs", "maplibre-gl-shared.mjs"))


class _SPAStaticFiles(StaticFiles):
    """Serves the built frontend with real single-page-app fallback: any
    unmatched GET/HEAD under /map/* gets index.html instead of a 404, so
    client-side routing survives a full page reload. Plain `html=True`
    alone only covers the mount's own directory index, not an arbitrary
    unmatched sub-path."""

    async def get_response(self, path: str, scope):
        try:
            return await super().get_response(path, scope)
        except StarletteHTTPException as exc:
            if exc.status_code == 404 and scope["method"] in ("GET", "HEAD"):
                return await super().get_response("index.html", scope)
            raise

    def file_response(self, full_path, stat_result, scope, status_code=200):
        """Adds Cache-Control on top of Starlette's ETag/Last-Modified
        conditional-GET support -- content-hashed assets can be cached
        forever, everything else must always revalidate (see issue #2054)."""
        response = super().file_response(full_path, stat_result, scope, status_code)
        full_path = pathlib.PurePath(full_path)
        is_hashed_asset = full_path.parent.name == "assets" and full_path.name not in _UNHASHED_ASSET_NAMES
        response.headers["cache-control"] = (
            "public, max-age=31536000, immutable" if is_hashed_asset else "no-cache"
        )
        return response


# Module-level state, built in lifespan() -- globals rather than app.state
# since background-thread callbacks need a plain reference without
# threading a request/app object through.
#
# _connections is rebuilt fresh in lifespan() on every startup, not a
# long-lived singleton: a WebSocket is tied to the ASGI event loop it was
# accepted on, so reusing one ConnectionManager across lifespan cycles
# could hang flush_once()'s send on a connection from a torn-down loop,
# starving every other connection sharing the flush loop.
_cfg: dict = {}
_redis = None
_store: Optional[FlightStateStore] = None
_range_outline: Optional[RangeOutlineStore] = None
_connections = ConnectionManager()
_shutdown = threading.Event()
_threads: list[threading.Thread] = []
# Minimal MQTT presence (Home Assistant discovery + version + started_at),
# only when MQTT_HOST is configured -- see shared/mqtt_presence.py.
_mqtt_presence: Optional[MqttPresence] = None


def _extract_timestamp(payload: dict) -> Optional[float]:
    """The out-of-order guard's comparison key for one UDP packet.

    `position`/`heartbeat` packets carry a numeric `ts` directly. `metadata`
    packets don't -- their closest equivalent is `last_message` (an
    ISO-8601 string), stamped from the same underlying timestamp so both
    packet types land on one comparable clock."""
    if "ts" in payload:
        try:
            return float(payload["ts"])
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
    accepted, publishes the corresponding live WebSocket event.

    Every message type carries `processor_id`, and each one updates that
    processor's liveness roster entry first, before type-specific
    handling -- so ordinary position/metadata traffic keeps a busy
    processor "green" without needing a standalone heartbeat."""
    msg_type = payload.get("type")

    processor_id = payload.get("processor_id")
    if processor_id:
        _store.record_processor_seen(processor_id, time.time())

    if msg_type == "heartbeat":
        return  # Liveness-only -- no flight state to apply, nothing to broadcast.

    if msg_type == "position":
        icao_hex = payload.get("icao_hex")
    elif msg_type == "metadata":
        # icao_hex only ever appears nested inside `aircraft` on a
        # metadata packet, never as a top-level key.
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
        # processor_id describes the sender, not the aircraft -- excluded
        # so it never leaks into the per-aircraft merged state.
        fields = {k: v for k, v in payload.items() if k not in ("type", "icao_hex", "processor_id")}

    merged = _store.apply_update(icao_hex, msg_type, timestamp, fields)
    if merged is None:
        return  # Dropped as out-of-order.

    if msg_type == "position":
        # Fold the merged (not raw) position into the range outline, so a
        # velocity-only packet still contributes once lat/lon are known.
        _range_outline.record_position(merged.get("lat"), merged.get("lon"), merged.get("alt"))
        event = {"type": "position", "icao_hex": icao_hex}
        for field in POSITION_FIELDS:
            if field in merged:
                event[field] = merged[field]
    else:
        # metadata events carry the full merged current-state, matching
        # GET /api/flights' shape, so a client that just connected still
        # has everything needed to place and label the aircraft.
        event = dict(merged)
        event["type"] = "metadata"
    _connections.publish(event)


def _udp_loop() -> None:
    listen_host = _cfg["map_listen_host"]
    listen_port = _cfg["map_listen_port"]
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, _UDP_RECV_BUFFER_BYTES)
    except OSError as exc:
        logger.warning("Could not raise UDP SO_RCVBUF: %r", exc)
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
    notifications each time -- a runtime CONFIG SET this no-persistence
    Redis instance never remembers across restart) on any pubsub error."""
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
    Docker's HEALTHCHECK to check the mtime of -- see shared/healthcheck.py.

    The `mkdir` stays inside the per-cycle try/except (not hoisted above
    the loop) so a missing/read-only `/app/health` outside Docker degrades
    to a skipped write each cycle instead of crashing the thread outright."""
    heartbeat_path = pathlib.Path(_HEALTHCHECK_HEARTBEAT_PATH)
    while not _shutdown.is_set():
        try:
            heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
            _redis.ping()
            heartbeat_path.touch()
        except Exception as exc:
            logger.debug("Healthcheck heartbeat write failed: %r", exc)
        _shutdown.wait(HEALTHCHECK_INTERVAL_SECONDS)


def _range_outline_loop() -> None:
    """Drives the daily range-outline snapshot on a fixed cadence; all the
    real work is in RangeOutlineStore.tick() (no-op when disabled)."""
    while not _shutdown.is_set():
        try:
            _range_outline.tick()
        except Exception:
            logger.exception("Range outline tick failed")
        _shutdown.wait(MAP_RANGE_OUTLINE_SNAPSHOT_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cfg, _redis, _store, _range_outline, _connections, _mqtt_presence

    _cfg = load_config("map_redis", "map", "mqtt")
    configure_logging(_cfg.get("log_level"))

    _redis = build_redis_client(_cfg["map_redis"])
    _store = FlightStateStore(
        _redis,
        stale_seconds=_cfg["map_stale_seconds"],
        hide_seconds=_cfg["map_hide_seconds"],
        evict_seconds=_cfg["map_evict_seconds"],
    )
    _store.enable_keyspace_notifications()

    _range_outline = RangeOutlineStore(
        _redis,
        center_latitude=_cfg.get("map_center_latitude"),
        center_longitude=_cfg.get("map_center_longitude"),
        snapshot_dir=_RANGE_OUTLINE_DIR,
        retention_seconds=MAP_RANGE_OUTLINE_TTL_SECONDS,
    )
    _range_outline.load_from_disk()

    # Fresh every startup -- see the module-level comment on _connections.
    _connections = ConnectionManager()

    _shutdown.clear()
    _threads.clear()
    for target, name in (
        (_udp_loop, "udp-listener"),
        (_eviction_loop, "eviction-listener"),
        (_healthcheck_loop, "healthcheck"),
        (_range_outline_loop, "range-outline"),
    ):
        thread = threading.Thread(target=target, daemon=True, name=name)
        thread.start()
        _threads.append(thread)

    flush_task = asyncio.ensure_future(_connections.flush_loop())

    # Optional MQTT presence -- inert unless MQTT_HOST is set.
    _mqtt_presence = MqttPresence(
        _cfg.get("mqtt"),
        component="map",
        device_identifier="SkyFollower_map",
        device_name="SkyFollower Map",
        device_model="Map",
        configuration_url="https://brentio.github.io/SkyFollower/components/map.html",
    )
    _mqtt_presence.start()

    logger.info("Map service started.")
    yield
    logger.info("Map service shutting down.")

    _mqtt_presence.stop()

    flush_task.cancel()
    try:
        await flush_task
    except asyncio.CancelledError:
        pass
    _shutdown.set()
    for thread in _threads:
        thread.join(timeout=5)

    # Persist the in-progress day so a restart resumes it.
    if _range_outline is not None:
        try:
            _range_outline.snapshot_now()
        except Exception:
            logger.exception("Final range-outline snapshot failed")


app = FastAPI(
    title="SkyFollower Map",
    description="Live aircraft position/metadata feed for the map "
    "frontend: a UDP listener fed by message-processor instances, a "
    "dedicated Redis instance holding current per-aircraft state, a "
    "GET /api/flights snapshot, a batched WS /ws live relay, and a daily "
    "reception range outline at GET /api/range-outline. Also serves the "
    "built frontend SPA itself, mounted at /map.",
    version="9999.99.99",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def redirect_root_to_map() -> RedirectResponse:
    """Redirects to the frontend SPA's directory index. The trailing slash
    matters: `/map` (no slash) would take a second redirect through
    Starlette's own mount handling first."""
    return RedirectResponse(url="/map/")


@app.get("/api/flights", tags=["flights"])
def get_flights() -> list[dict]:
    """One object per currently-tracked aircraft -- the same merged
    current-state shape a WebSocket `metadata` message carries (see
    _handle_packet)."""
    return _store.list_flights()


@app.get("/api/flights/{icao_hex}", tags=["flights"])
def get_flight(icao_hex: str) -> dict:
    """One aircraft's merged current-state (same shape as a GET /api/flights
    array element) plus a `trail` array: every accumulated `{lat, lon, alt}`
    point for the current flight, oldest first, `alt` null where unknown.
    Lets a client reconstruct the whole flight's trail on selection, not
    just what it has seen since connecting.

    404 when the aircraft isn't currently tracked. `trail` is `[]` when the
    aircraft has only ever sent velocity/heading-only position packets."""
    flight = _store.get_flight(icao_hex)
    if flight is None:
        raise HTTPException(status_code=404, detail=f"aircraft {icao_hex} is not currently tracked")
    flight["trail"] = _store.get_trail(icao_hex)
    return flight


class FlightsBatchRequest(BaseModel):
    icao_hex: list[str]


@app.post("/api/flights/batch", tags=["flights"])
def get_flights_batch(payload: FlightsBatchRequest) -> list[dict]:
    """Batched counterpart to GET /api/flights/{icao_hex} -- same
    per-aircraft shape, but for many hexes in two pipelined Redis round
    trips instead of one HTTP request per aircraft. Untracked hexes are
    silently omitted."""
    return _store.get_flights_batch(payload.icao_hex)


@app.get("/api/processors", tags=["flights"])
def get_processor_status() -> dict:
    """Per-message-processor liveness roster derived from UDP traffic
    carrying `processor_id`, plus the aggregated `overall` status the
    frontend's connection indicator renders. Polled rather than pushed
    over `WS /ws` -- status can change purely from time passing (green
    ageing into amber/red), so it's computed fresh on each request."""
    processors = _store.get_processor_statuses()
    return {
        "overall": overall_processor_status([p["status"] for p in processors]),
        "processors": processors,
    }


@app.get("/api/range-outline", tags=["range-outline"])
def get_range_outline(date: Optional[str] = None, band: Optional[str] = None) -> dict:
    """The reception range outline as a GeoJSON FeatureCollection: one 3-D
    `Polygon` per altitude band (vertices `[lon, lat, alt]`, the farthest
    aircraft received per compass bearing), plus an `envelope` polygon
    (farthest per bearing across all bands).

    No `date` -> today's live outline. `date=YYYY-MM-DD` -> that day's
    finalised snapshot from disk (404 past the retention window).
    `band=<label>` narrows to one band; `band=envelope` returns just the
    envelope. Empty FeatureCollection when no center is configured."""
    try:
        return _range_outline.get_outline(date=date, band=band)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"no range outline snapshot for {date}")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/range-outline/dates", tags=["range-outline"])
def get_range_outline_dates() -> dict:
    """Which range-outline snapshots can be requested: `"today"` (the live
    outline) plus every finalised `YYYY-MM-DD` still on disk, newest
    first."""
    return {"dates": _range_outline.available_dates()}


@app.delete("/api/range-outline", tags=["range-outline"], status_code=204)
def reset_range_outline() -> None:
    """Reset the in-progress day -- drops today's live outline and its
    snapshot file. Finalised past days are untouched."""
    _range_outline.clear_today()


@app.get("/api/config", tags=["config"])
def get_config() -> dict:
    """Runtime configuration the frontend can't otherwise get at -- Vite
    bakes VITE_* values into the bundle at build time, so a per-deployment
    "center" reference point needs a runtime channel like this one instead.

    A flat object with named sub-keys, not a bare value, so a later
    addition doesn't need a breaking shape change."""
    latitude = _cfg.get("map_center_latitude")
    longitude = _cfg.get("map_center_longitude")
    center = None
    if latitude is not None and longitude is not None:
        center = {"latitude": latitude, "longitude": longitude}
    return {"center": center}


@app.websocket("/ws")
async def flights_ws(websocket: WebSocket) -> None:
    """One connection per browser. Never sends a snapshot -- only
    `position`/`metadata`/`stale`/`remove` events, batched by
    ConnectionManager.flush_loop(). Callers should GET /api/flights first
    for the initial snapshot, then open this for live updates."""
    await websocket.accept()
    _connections.register(websocket)
    try:
        while True:
            # No client message is ever expected -- just wait for the
            # connection to close so it can be unregistered.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _connections.unregister(websocket)


# Guarded on the directory existing so importing this module never fails
# just because `npm run build` hasn't been run locally -- the Docker image
# always has it.
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


def _uvicorn_tls_kwargs() -> dict:
    """uvicorn.run() SSL kwargs when a cert/key pair exists at the fixed
    TLS mount point, else an empty dict. Degrades to plain HTTP with a
    logged warning rather than raising, for running standalone outside the
    installer flow where no TLS directory was ever generated."""
    if os.path.isfile(_TLS_CERT_PATH) and os.path.isfile(_TLS_KEY_PATH):
        return {"ssl_certfile": _TLS_CERT_PATH, "ssl_keyfile": _TLS_KEY_PATH}
    logger.warning(
        "No TLS cert/key found at %s / %s -- serving plain HTTP. Run "
        "scripts/install.sh for the map role to generate a self-signed "
        "pair, or drop your own cert.pem/key.pem there.",
        _TLS_CERT_PATH, _TLS_KEY_PATH,
    )
    return {}


def main() -> None:  # pragma: no cover -- exercised via `python -m map.main`
    import uvicorn

    cfg = load_config("map_redis", "map")
    uvicorn.run(
        app,
        host=cfg["map_http_host"],
        port=cfg["map_http_port"],
        log_config=None,
        **_uvicorn_tls_kwargs(),
    )


if __name__ == "__main__":  # pragma: no cover
    main()
