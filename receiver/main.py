#!/usr/bin/env python3
"""
SkyFollower Receiver

Connects to one or more TCP sources — readsb (1090 MHz Mode S, including any
EXTERNAL-tagged 1090-style feed) or dump978-fa (978 MHz UAT) — extracts each
message's ICAO hex (via pyModeS for 1090/EXTERNAL, directly from the UAT
payload for 978), and publishes it to
RabbitMQ's consistent-hash exchange keyed by that hex, leaving the broker to
pick which message processor handles the aircraft.  Falls back to a local
SQLite queue when RabbitMQ is unavailable, and drains the fallback on
reconnect.

One container handles all configured sources concurrently (one thread per source).
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import pathlib
import queue
import re
import signal
import socket
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

import paho.mqtt.client as mqtt
import pika
import pyModeS as pms
import redis as redis_lib

from shared.adsb_1090 import parse_tcp_stream
from shared.config import DATA_DIR, ConfigError, load_config
from shared.fallback_queue import DRAIN_EMPTY, DRAIN_PROGRESSED, FallbackQueue
from shared.ha_discovery import build_ha_device
from shared.logging_setup import configure_logging
from shared.models import InboundMessage
from shared.mqtt import build_mqtt_client
from shared.mqtt_register import publish_register
from shared.rabbitmq_topology import ADSB_EXCHANGE, declare_adsb_topology
from shared.redis_client import build_redis_client
from shared.redis_keys import (
    receiver_heartbeat_key,
    receiver_message_count_key,
    receiver_registration_key,
    receiver_registry_index_key,
)
from shared.timing import (
    HEALTHCHECK_INTERVAL_SECONDS,
    HEARTBEAT_INTERVAL_SECONDS,
    HEARTBEAT_TTL_SECONDS,
    MQTT_PUBLISH_INTERVAL_SECONDS,
    RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS,
    RATE_WINDOW_SECONDS,
    RECONNECT_BACKOFF_SECONDS,
    RECONNECT_COUNT_RESET_AGE_SECONDS,
    TCP_KEEPALIVE_PROBES,
    TCP_KEEPIDLE_SECONDS,
    TCP_KEEPINTVL_SECONDS,
    UNPARSEABLE_WARNING_INTERVAL_SECONDS,
)
from shared.uat import parse_978_line

logger = logging.getLogger("receiver")

# tmpfs-mounted (docker-compose.receiver.yaml) -- must stay ephemeral;
# only /app/data (fallback SQLite queue) persists across restarts.
_HEALTHCHECK_HEARTBEAT_PATH = "/app/health/heartbeat"

# In-memory hand-off from source threads to the rabbitmq thread; bounded so
# a broker outage can't grow it unbounded -- once full, new messages spill
# to the overflow queue (see _OVERFLOW_QUEUE_MAXSIZE) instead of blocking.
_LIVE_QUEUE_MAXSIZE = 10_000

# Second-stage buffer once _live_queue is full. The overflow-writer thread
# batches entries into the SQLite fallback so that write stays off the
# socket-read threads; if this also fills, a source thread writes directly.
_OVERFLOW_QUEUE_MAXSIZE = 50_000

# Rows the overflow-writer pulls into a single executemany + commit.
_OVERFLOW_WRITE_BATCH_MAX = 5_000

# Poll timeout for the overflow-writer when its queue is empty -- short
# enough to exit promptly on shutdown, long enough to avoid busy-spinning.
_OVERFLOW_WRITER_IDLE_SECONDS = 1.0

# Cap on live messages published per pass so heartbeats and the broker's
# blocked/unblocked signals stay serviced under load; the remainder is
# taken on the next pass, still ahead of fallback drain.
_LIVE_PUBLISH_BATCH_MAX = 2_000

# Poll timeout while idle -- keeps pika's heartbeat serviced without
# busy-spinning.
_RMQ_IDLE_POLL_SECONDS = 1.0

# Backlog rows drained per pass once the live queue is empty. Batching the
# delete+commit lifts catch-up throughput above one-commit-per-row. Kept
# well below _LIVE_PUBLISH_BATCH_MAX so a live message is delayed by at
# most this many publishes during catch-up, not more.
_FALLBACK_DRAIN_BATCH_MAX = 100

# ---------------------------------------------------------------------------
# Rate tracker
# ---------------------------------------------------------------------------


class _RateTracker:
    def __init__(self, window: int = RATE_WINDOW_SECONDS) -> None:
        self._window = window
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

        # record() only adds to these; flush_to_redis() (telemetry thread
        # only) is what resets them on a real hour/day boundary.
        # hour_count/today_count feed the Redis-backed cross-restart
        # counters core-health publishes. lifetime_count is never written
        # to Redis -- it's a device-local total that resets to zero on
        # every receiver restart by design (see receiver/README.md).
        self.hour_count = 0
        self.today_count = 0
        self.lifetime_count = 0
        # Bookkeeping for flush_to_redis(): last value flushed per counter
        # (to send only the delta) and each counter's current hour/day
        # bucket, to detect a real rollover.
        self._flushed_hour = 0
        self._flushed_today = 0
        self._hour_bucket: Optional[datetime] = None
        self._day_bucket: Optional[datetime] = None

    def record(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._timestamps.append(now)
            cutoff = now - self._window
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            self.hour_count += 1
            self.today_count += 1
            self.lifetime_count += 1

    def rate(self) -> float:
        now = time.monotonic()
        with self._lock:
            cutoff = now - self._window
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            return len(self._timestamps) / self._window

    def flush_to_redis(
        self,
        redis_client,
        script_sha: str,
        key_fn: Callable[[str], str],
        now: datetime,
    ) -> None:
        """Push the hour/today delta since the last flush into Redis,
        resetting hour_count/today_count locally on an observed UTC
        hour/midnight rollover. lifetime_count is never flushed. Called
        only from the telemetry thread, never the record() hot path."""
        hour_bucket = now.replace(minute=0, second=0, microsecond=0)
        day_bucket = now.replace(hour=0, minute=0, second=0, microsecond=0)

        with self._lock:
            if self._hour_bucket is None:
                self._hour_bucket = hour_bucket
            if self._day_bucket is None:
                self._day_bucket = day_bucket

            # On a rollover, drop the small remainder already counted
            # toward the closed period rather than flush it -- attributing
            # it to the old bucket risks an EXPIREAT already in the past;
            # to the new bucket would double-count. Bounded to at most one
            # flush interval's worth each real rollover (see
            # receiver/README.md).
            if hour_bucket != self._hour_bucket:
                hour_delta = 0
                self.hour_count = 0
                self._flushed_hour = 0
                self._hour_bucket = hour_bucket
            else:
                hour_delta = self.hour_count - self._flushed_hour
                self._flushed_hour = self.hour_count

            if day_bucket != self._day_bucket:
                today_delta = 0
                self.today_count = 0
                self._flushed_today = 0
                self._day_bucket = day_bucket
            else:
                today_delta = self.today_count - self._flushed_today
                self._flushed_today = self.today_count

        if hour_delta:
            next_hour = hour_bucket + timedelta(hours=1)
            redis_client.evalsha(
                script_sha, 0, key_fn("hour"), hour_delta, int(next_hour.timestamp())
            )
        if today_delta:
            next_day = day_bucket + timedelta(days=1)
            redis_client.evalsha(
                script_sha, 0, key_fn("today"), today_delta, int(next_day.timestamp())
            )


def _load_or_create_receiver_id(data_dir: str) -> str:
    """Load the persisted receiver identity, generating one on first run
    so restarts keep the same MQTT topics/HA identifiers."""
    path = os.path.join(data_dir, "receiver_id")
    if os.path.exists(path):
        existing = open(path).read().strip()
        if existing:
            return existing
    new_id = str(uuid.uuid4())
    with open(path, "w") as f:
        f.write(new_id)
    return new_id


def _enable_tcp_keepalive(sock: socket.socket) -> None:
    """Enable TCP keepalive with tuned timers on a source socket.

    SO_KEEPALIVE is portable; the three timer options are Linux-only, so
    each is hasattr-guarded (absent on macOS, where tests run) and wrapped
    in try/except OSError, since a non-TCP test socket rejects them too.
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    for _name, _value in (
        ("TCP_KEEPIDLE", TCP_KEEPIDLE_SECONDS),
        ("TCP_KEEPINTVL", TCP_KEEPINTVL_SECONDS),
        ("TCP_KEEPCNT", TCP_KEEPALIVE_PROBES),
    ):
        if not hasattr(socket, _name):
            continue
        try:
            sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, _name), _value)
        except OSError:
            pass


def _sanitize_mqtt_id(value: str) -> str:
    """Replace any character outside [a-zA-Z0-9_-] with '-'.

    Home Assistant discovery requires object_id/unique_id to match this
    pattern; used wherever a source host/port becomes a topic/id segment.
    """
    return re.sub(r"[^a-zA-Z0-9_-]", "-", value)


# ---------------------------------------------------------------------------
# Receiver
# ---------------------------------------------------------------------------

class Receiver:

    def __init__(self, config: dict) -> None:
        self._cfg = config
        self._started_at = datetime.now(timezone.utc).isoformat()
        self._shutdown = threading.Event()

        # Keyed by (host, port), not source tag -- multiple connections
        # can share a tag (e.g. two EXTERNAL feeds) and need independent
        # tracking.
        self._rates: dict[tuple[str, int], _RateTracker] = {}
        # True only while this connection's TCP socket is open.
        self._connected: dict[tuple[str, int], bool] = {}
        # Drop-and-retry cycles per connection, so a flapping connection is
        # distinguishable from a stable one. Reset once a connection holds
        # for RECONNECT_COUNT_RESET_AGE_SECONDS (see _source_loop).
        self._reconnect_counts: dict[tuple[str, int], int] = {}
        # time.monotonic() when a connection came up, or None; used only
        # to decide whether to reset _reconnect_counts on the next drop.
        self._connected_since: dict[tuple[str, int], Optional[float]] = {}
        # ISO-8601 timestamp of the last message per connection, or None;
        # lets a silent feed be seen directly rather than inferred from a
        # decayed rate.
        self._last_message_at: dict[tuple[str, int], Optional[str]] = {}

        # Optional: an unset REDIS_HOST leaves this None and skips all
        # identity-claim/heartbeat/core-health-registration behavior
        # below, preserving the original random-UUID, no-Redis behavior.
        rc = config.get("redis") or {}
        self._redis = build_redis_client(rc) if rc.get("host") else None
        # Lazily script_load()'d on first flush, not here, so startup
        # makes zero Redis calls when identity is already persisted.
        self._incr_period_counter_sha: Optional[str] = None

        for src in config.get("sources", []):
            key = (src["host"], src["port"])
            self._rates[key] = _RateTracker()
            self._connected[key] = False
            self._reconnect_counts[key] = 0
            self._connected_since[key] = None
            self._last_message_at[key] = None

        # Fallback SQLite queue
        os.makedirs(DATA_DIR, exist_ok=True)
        self._fallback = FallbackQueue(os.path.join(DATA_DIR, "queue.db"))

        self._id = self._resolve_identity()
        # Human-friendly label for HA name/model/sensor labels. When Redis
        # is configured this equals self._id; in the legacy no-Redis path
        # self._id is a UUID and this is the only human-readable label.
        # self._id remains the stable identifier for topic paths/
        # unique_id either way.
        self._name = config.get("name")
        self._version = os.environ.get("VERSION", "dev")

        # RabbitMQ state
        self._rmq_connection: Optional[pika.BlockingConnection] = None
        self._rmq_channel = None
        self._rmq_connected = False
        self._rmq_lock = threading.Lock()

        # Ordering gate: set as soon as a message can't go out immediately
        # (queue full or a publish failed). While set, new messages route
        # to the overflow queue only, so the live queue can't hold
        # anything newer than what's still waiting in queue.db
        # (_enqueue_live, _rmq_publish_loop enforce this). A
        # threading.Event, not a bool, so is_set()/set()/clear() need no
        # additional locking from either the source or rabbitmq threads.
        self._backlogged = threading.Event()
        # Informational only -- age of the last message actually
        # published, refreshed on each publish (live or backlog); exposed
        # via telemetry.
        self._staleness_seconds: float = 0.0

        # Parsed messages waiting for the rabbitmq thread to publish (see
        # _LIVE_QUEUE_MAXSIZE). Each item carries received_at so the
        # rabbitmq thread can compute staleness without re-parsing the
        # payload.
        self._live_queue: queue.Queue[tuple[str, str, float]] = queue.Queue(
            maxsize=_LIVE_QUEUE_MAXSIZE
        )

        # Fed once _live_queue is full, or while self._backlogged is set
        # (see _enqueue_live); waiting for the overflow-writer thread to
        # batch into the SQLite fallback (see _OVERFLOW_QUEUE_MAXSIZE).
        self._overflow_queue: queue.Queue[tuple[str, str, float]] = queue.Queue(
            maxsize=_OVERFLOW_QUEUE_MAXSIZE
        )

        # MQTT state
        self._mqtt: Optional[mqtt.Client] = None
        self._mqtt_connected = False

    # ------------------------------------------------------------------
    # Identity -- claim-and-persist, mirroring message-processor/main.py.
    # ------------------------------------------------------------------

    def _resolve_identity(self) -> str:
        """Resolve this receiver's identity at startup.

        1. Local identity already persisted -- use it, zero Redis calls.
        2. No local identity, Redis unconfigured -- fall back to a
           random UUID.
        3. No local identity, Redis configured -- SET NX the configured
           RECEIVER_NAME; success persists it locally. Any failure (name
           already claimed, or Redis unreachable) is a critical + exit,
           since first-time identity establishment needs Redis to verify
           uniqueness. Every later boot is case 1.
        """
        path = os.path.join(DATA_DIR, "receiver_id")
        if os.path.exists(path):
            existing = open(path).read().strip()
            if existing:
                return existing

        if self._redis is None:
            return _load_or_create_receiver_id(DATA_DIR)

        configured_name = self._cfg.get("name")
        if not configured_name:
            logger.critical(
                "RECEIVER_NAME must be set to claim a receiver identity via Redis."
            )
            sys.exit(1)

        key = receiver_heartbeat_key(configured_name)
        try:
            claimed = self._redis.set(key, "1", nx=True, ex=HEARTBEAT_TTL_SECONDS)
        except Exception as exc:
            logger.critical(
                "Cannot reach Redis to claim receiver identity %r: %s. Exiting.",
                configured_name, exc,
            )
            sys.exit(1)

        if not claimed:
            logger.critical(
                "RECEIVER_NAME %r is already claimed by another instance. Exiting.",
                configured_name,
            )
            sys.exit(1)

        with open(path, "w") as f:
            f.write(configured_name)
        logger.info("Receiver identity %r claimed.", configured_name)
        return configured_name

    def _register_with_core_health(self) -> None:
        """Add/refresh this receiver's entry in core-health's discovery
        index: an idempotent SADD into the index SET plus a per-receiver
        registration entry (source list), TTL'd like the heartbeat.
        Called at startup and on every heartbeat tick so a resumed
        identity re-registers as promptly as a freshly-claimed one.
        Fails soft -- best-effort discovery plumbing only."""
        if self._redis is None:
            return
        try:
            self._redis.sadd(receiver_registry_index_key(), self._id)
            self._redis.set(
                receiver_registration_key(self._id),
                json.dumps(self._cfg.get("sources", [])),
                ex=HEARTBEAT_TTL_SECONDS,
            )
        except Exception:
            pass

    def _heartbeat_loop(self) -> None:
        """Mirrors message-processor's _heartbeat_loop: refresh the claim
        key's TTL on each tick (fail-soft), and refresh the core-health
        registration -- this is what re-registers a receiver that resumed
        a persisted identity."""
        while not self._shutdown.is_set():
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)
            try:
                self._redis.expire(
                    receiver_heartbeat_key(self._id), HEARTBEAT_TTL_SECONDS
                )
            except Exception:
                pass
            self._register_with_core_health()

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._setup_logging()
        logger.info(f"Starting SkyFollower Receiver {self._id} {self._version}")
        self._connect_mqtt()

        if self._redis is not None:
            self._register_with_core_health()
            threading.Thread(
                target=self._heartbeat_loop, daemon=True, name="heartbeat"
            ).start()

        threading.Thread(
            target=self._rmq_loop, daemon=True, name="rabbitmq"
        ).start()

        # Batches live-queue overflow into the SQLite fallback off the
        # source threads.
        threading.Thread(
            target=self._overflow_writer_loop, daemon=True, name="overflow-writer"
        ).start()

        threading.Thread(
            target=self._telemetry_loop, daemon=True, name="telemetry"
        ).start()

        threading.Thread(
            target=self._healthcheck_loop, daemon=True, name="healthcheck"
        ).start()

        source_threads = []
        for src_cfg in self._cfg.get("sources", []):
            t = threading.Thread(
                target=self._source_loop,
                args=(src_cfg,),
                daemon=True,
                name=f"source-{src_cfg['source']}",
            )
            t.start()
            source_threads.append(t)

        self._shutdown.wait()

    def _setup_logging(self) -> None:
        configure_logging(self._cfg.get("log_level"))

    # ------------------------------------------------------------------
    # Source TCP loop
    # ------------------------------------------------------------------

    def _source_loop(self, src_cfg: dict) -> None:
        """Connect to readsb TCP port, parse the stream, and route messages."""
        host = src_cfg["host"]
        port = src_cfg["port"]
        source = src_cfg["source"]
        key = (host, port)
        rate_tracker = self._rates.get(key, _RateTracker())

        while not self._shutdown.is_set():
            try:
                logger.info("Connecting to readsb at %s:%s (source=%s)…", host, port, source)
                with socket.create_connection((host, port), timeout=10) as sock:
                    _enable_tcp_keepalive(sock)
                    sock.settimeout(5.0)
                    self._connected[key] = True
                    self._connected_since[key] = time.monotonic()
                    logger.info("Connected to %s:%s (source=%s).", host, port, source)
                    try:
                        if source == "978":
                            self._read_978_stream(sock, host, port, source, rate_tracker)
                        else:
                            self._read_1090_stream(sock, host, port, source, rate_tracker)
                    finally:
                        self._connected[key] = False

            except OSError as exc:
                self._connected[key] = False
                logger.warning(
                    "Cannot connect to readsb %s:%s: %s — retrying in %ss…",
                    host, port, exc, RECONNECT_BACKOFF_SECONDS,
                )
            except Exception as exc:
                self._connected[key] = False
                logger.error(
                    "Source %s:%s error: %s — retrying in %ss…",
                    host, port, exc, RECONNECT_BACKOFF_SECONDS,
                )

            if not self._shutdown.is_set():
                # A drop-and-retry: reset the flap count only if the
                # dropped connection held for RECONNECT_COUNT_RESET_AGE_SECONDS
                # -- a connection still flapping never accumulates that much
                # uptime. Clear _connected_since regardless, so a cycle that
                # fails before connecting can't reuse a stale timestamp from
                # an older connection.
                up_since = self._connected_since.get(key)
                if (
                    up_since is not None
                    and time.monotonic() - up_since >= RECONNECT_COUNT_RESET_AGE_SECONDS
                ):
                    self._reconnect_counts[key] = 0
                self._connected_since[key] = None
                self._reconnect_counts[key] = self._reconnect_counts.get(key, 0) + 1
                time.sleep(RECONNECT_BACKOFF_SECONDS)

    def _read_1090_stream(
        self, sock: socket.socket, host: str, port: int, source: str, rate_tracker: _RateTracker
    ) -> None:
        buf = bytearray()
        unparseable_count = 0
        unparseable_window_start = time.monotonic()

        def _maybe_warn_unparseable() -> None:
            nonlocal unparseable_count, unparseable_window_start
            now = time.monotonic()
            if unparseable_count and now - unparseable_window_start >= UNPARSEABLE_WARNING_INTERVAL_SECONDS:
                logger.warning(
                    "%d unparseable 1090 message(s) from %s:%s (source=%s) in the last %ds — "
                    "check the upstream feed format.",
                    unparseable_count, host, port, source, UNPARSEABLE_WARNING_INTERVAL_SECONDS,
                )
                unparseable_count = 0
                unparseable_window_start = now

        while not self._shutdown.is_set():
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                _maybe_warn_unparseable()
                continue
            if not chunk:
                logger.warning(
                    "readsb %s:%s closed connection — reconnecting.", host, port
                )
                break

            messages = parse_tcp_stream(chunk, buf)
            if not messages:
                logger.debug(
                    "Received data on %s:%s (source=%s) that did not parse as a "
                    "complete 1090 message: %r",
                    host, port, source, chunk[:64],
                )
                unparseable_count += 1
            for raw_hex in messages:
                self._handle_message(raw_hex, source, rate_tracker, (host, port))
            _maybe_warn_unparseable()

    def _read_978_stream(
        self, sock: socket.socket, host: str, port: int, source: str, rate_tracker: _RateTracker
    ) -> None:
        line_buf = b""
        unparseable_count = 0
        unparseable_window_start = time.monotonic()

        def _maybe_warn_unparseable() -> None:
            nonlocal unparseable_count, unparseable_window_start
            now = time.monotonic()
            if unparseable_count and now - unparseable_window_start >= UNPARSEABLE_WARNING_INTERVAL_SECONDS:
                logger.warning(
                    "%d unparseable 978 line(s) from %s:%s (source=%s) in the last %ds — "
                    "check the upstream feed format.",
                    unparseable_count, host, port, source, UNPARSEABLE_WARNING_INTERVAL_SECONDS,
                )
                unparseable_count = 0
                unparseable_window_start = now

        while not self._shutdown.is_set():
            try:
                chunk = sock.recv(4096)
            except socket.timeout:
                _maybe_warn_unparseable()
                continue
            if not chunk:
                logger.warning(
                    "readsb %s:%s closed connection — reconnecting.", host, port
                )
                break

            line_buf += chunk
            while b"\n" in line_buf:
                raw_line, line_buf = line_buf.split(b"\n", 1)
                decoded_line = raw_line.decode("ascii", errors="ignore")
                result = parse_978_line(decoded_line)
                if result:
                    raw_hex, icao_hex, received_at = result
                    self._handle_978_message(
                        raw_hex, icao_hex, received_at, source, rate_tracker, (host, port)
                    )
                else:
                    # !-preambles and blank lines are routine -- only
                    # count lines that looked like real data but still
                    # failed to parse.
                    stripped = decoded_line.strip()
                    if stripped and not stripped.startswith("!"):
                        logger.debug(
                            "Unparseable 978 line from %s:%s (source=%s): %r",
                            host, port, source, decoded_line,
                        )
                        unparseable_count += 1
            _maybe_warn_unparseable()

    def _handle_message(
        self, raw_hex: str, source: str, rate_tracker: _RateTracker, key: tuple[str, int]
    ) -> None:
        """Extract ICAO from a 1090 Mode S message, then route it."""
        self._last_message_at[key] = datetime.now(timezone.utc).isoformat()
        try:
            decoded = pms.decode(raw_hex)
            icao_hex = decoded.get("icao") if decoded else None
        except Exception:
            icao_hex = None

        if not icao_hex:
            return  # Bad or unrecognisable message — discard silently

        icao_hex = icao_hex.upper()
        if len(icao_hex) != 6:
            return

        self._route_message(raw_hex, icao_hex, time.time(), source, rate_tracker)

    def _handle_978_message(
        self,
        raw_hex: str,
        icao_hex: str,
        received_at: float,
        source: str,
        rate_tracker: _RateTracker,
        key: tuple[str, int],
    ) -> None:
        """Route an already-parsed 978 UAT message (icao_hex/received_at
        extracted by parse_978_line — no pyModeS decode needed, UAT is not
        Mode S)."""
        self._last_message_at[key] = datetime.now(timezone.utc).isoformat()
        if len(icao_hex) != 6:
            return

        self._route_message(raw_hex, icao_hex, received_at, source, rate_tracker)

    def _route_message(
        self,
        raw: str,
        icao_hex: str,
        received_at: float,
        source: str,
        rate_tracker: _RateTracker,
    ) -> None:
        """Build the InboundMessage envelope and publish it keyed by ICAO hex."""
        msg = InboundMessage(
            raw=raw,
            icao_hex=icao_hex,
            received_at=received_at,
            source=source,  # type: ignore[arg-type]
        )
        payload = msg.model_dump_json()

        rate_tracker.record()
        self._enqueue_live(icao_hex, payload, received_at)

    def _enqueue_live(self, routing_key: str, payload: str, received_at: float) -> None:
        """Hand a message off for publishing without blocking or
        touching disk.

        Gated on self._backlogged, not just on live-queue fullness: while
        backlogged, every new message goes to the overflow queue
        regardless of live-queue space, so nothing newer is published
        ahead of an older row still in queue.db. A full live queue while
        not yet backlogged is what SETS backlogged. If the overflow
        queue is also full, falls back to a direct synchronous write."""
        if not self._backlogged.is_set():
            try:
                self._live_queue.put_nowait((routing_key, payload, received_at))
                return
            except queue.Full:
                self._backlogged.set()
        try:
            self._overflow_queue.put_nowait((routing_key, payload, received_at))
        except queue.Full:
            self._fallback_put(routing_key, payload, received_at)

    def _overflow_writer_loop(self) -> None:
        """Sole consumer of self._overflow_queue; batches messages into
        the durable SQLite fallback with one commit per pass, keeping
        fsync-class writes off the socket-read threads. Makes one final
        pass on shutdown."""
        while not self._shutdown.is_set():
            try:
                first = self._overflow_queue.get(
                    timeout=_OVERFLOW_WRITER_IDLE_SECONDS
                )
            except queue.Empty:
                continue
            self._flush_overflow_batch(first)
        # Final drain -- flush whatever the source threads left buffered.
        self._flush_overflow_batch(None)

    def _flush_overflow_batch(self, first: Optional[tuple[str, str, float]]) -> None:
        """Collect up to _OVERFLOW_WRITE_BATCH_MAX queued messages
        (starting with `first`, if already dequeued) and persist them in
        one FallbackQueue.put_many() call."""
        batch: list[tuple[str, str, float]] = []
        if first is not None:
            batch.append(first)
        while len(batch) < _OVERFLOW_WRITE_BATCH_MAX:
            try:
                batch.append(self._overflow_queue.get_nowait())
            except queue.Empty:
                break
        if not batch:
            return
        wrapped = [
            json.dumps({"routing_key": rk, "payload": p, "received_at": ra})
            for rk, p, ra in batch
        ]
        try:
            self._fallback.put_many(wrapped)
        except Exception as exc:
            logger.error(
                "Overflow writer could not persist %d message(s): %s",
                len(wrapped), exc,
            )

    # ------------------------------------------------------------------
    # RabbitMQ
    # ------------------------------------------------------------------

    def _rmq_params(self) -> pika.ConnectionParameters:
        rc = self._cfg["rabbitmq"]
        creds = pika.PlainCredentials(rc["username"], rc["password"])
        return pika.ConnectionParameters(
            host=rc["host"],
            port=rc.get("port", 5672),
            credentials=creds,
            heartbeat=60,
            # A broker resource alarm (disk-free/high memory) blocks
            # publishers while leaving the TCP connection up; without this
            # a publish would wedge forever. pika drops the connection once
            # blocked exceeds this, and _rmq_loop reconnects.
            blocked_connection_timeout=RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS,
        )

    def _rmq_loop(self) -> None:
        """Own the RabbitMQ connection -- the only thread that touches
        its channel. Source threads never wait on the broker; this loop
        publishes queued messages and only advances the SQLite backlog
        once nothing is waiting to go out live. Reconnects on failure."""
        while not self._shutdown.is_set():
            conn = None
            try:
                logger.info("Connecting to RabbitMQ…")
                conn = pika.BlockingConnection(self._rmq_params())
                ch = conn.channel()

                declare_adsb_topology(ch)

                with self._rmq_lock:
                    self._rmq_connection = conn
                    self._rmq_channel = ch
                    self._rmq_connected = True

                logger.info("RabbitMQ connected.")
                self._rmq_publish_loop(conn, ch)

            except pika.exceptions.AMQPConnectionError as exc:
                logger.warning(
                    "RabbitMQ unavailable: %s. Retrying in %ss…",
                    exc, RECONNECT_BACKOFF_SECONDS,
                )
            except Exception as exc:
                logger.error(
                    "RabbitMQ error: %s. Retrying in %ss…",
                    exc, RECONNECT_BACKOFF_SECONDS,
                )
            finally:
                with self._rmq_lock:
                    self._rmq_connected = False
                    self._rmq_channel = None
                    self._rmq_connection = None
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

            if not self._shutdown.is_set():
                time.sleep(RECONNECT_BACKOFF_SECONDS)

    def _rmq_publish_loop(self, conn: pika.BlockingConnection, ch) -> None:
        """Inner loop while connected: pump pika, drain the live queue,
        then -- only once it's empty -- advance the fallback backlog by
        one bounded batch (_FALLBACK_DRAIN_BATCH_MAX rows). Returns (so
        _rmq_loop reconnects) on shutdown or any publish/connection
        failure. Draining the live queue first is safe because
        _enqueue_live guarantees it never holds content newer than the
        backlog while backlogged."""
        while not self._shutdown.is_set():
            # A publish failure can latch _rmq_connected False without
            # the connection raising (a resource-alarm-blocked broker).
            # Reconnect to re-validate rather than loop forever routing
            # to the fallback.
            with self._rmq_lock:
                if not self._rmq_connected:
                    logger.warning(
                        "RabbitMQ publish path reported a failure; "
                        "reconnecting to re-validate."
                    )
                    return

            # Service heartbeats and the broker's blocked/unblocked signals.
            try:
                conn.process_data_events(time_limit=0)
            except pika.exceptions.AMQPConnectionError:
                return
            except Exception:
                return

            published_live = self._publish_live_batch(ch)
            with self._rmq_lock:
                if not self._rmq_connected:
                    return
            if published_live:
                continue

            # Live queue empty this pass -- advance the backlog by one
            # bounded batch, then loop back to re-check the live queue.
            step = self._fallback.drain_batch(
                lambda wrapped: self._publish_fallback_row(ch, wrapped),
                _FALLBACK_DRAIN_BATCH_MAX,
            )
            if step == DRAIN_EMPTY and self._backlogged.is_set():
                # Confirmed empty on both sides this pass -- only now is
                # it safe to let new messages resume entering the live
                # queue directly. Clearing on anything less than a
                # confirmed empty drain would reopen the ordering bug
                # this flag exists to prevent.
                self._backlogged.clear()
            if step == DRAIN_PROGRESSED:
                continue

            # A failed backlog row latches the connection unhealthy --
            # reconnect immediately rather than idling first.
            with self._rmq_lock:
                if not self._rmq_connected:
                    continue

            if self._shutdown.is_set():
                return

            if self._backlogged.is_set():
                # The head-of-queue row is in retry cooldown and
                # _enqueue_live won't feed the live queue while
                # backlogged -- wait rather than busy-spin.
                time.sleep(_RMQ_IDLE_POLL_SECONDS)
                continue

            # Fully idle -- wait for the next live message rather than
            # busy-spin.
            try:
                routing_key, payload, received_at = self._live_queue.get(
                    timeout=_RMQ_IDLE_POLL_SECONDS
                )
            except queue.Empty:
                continue
            self._publish_one(ch, routing_key, payload, received_at)

    def _publish_live_batch(self, ch) -> bool:
        """Publish up to _LIVE_PUBLISH_BATCH_MAX queued messages, oldest
        first. Returns True if at least one was dequeued, whether or not
        it published cleanly; stops on the first failure so the caller
        can reconnect."""
        published = 0
        while published < _LIVE_PUBLISH_BATCH_MAX:
            try:
                routing_key, payload, received_at = self._live_queue.get_nowait()
            except queue.Empty:
                break
            published += 1
            if not self._publish_one(ch, routing_key, payload, received_at):
                break
        return published > 0

    def _publish_one(self, ch, routing_key: str, payload: str, received_at: float) -> bool:
        """basic_publish one message on the rabbitmq thread. On failure,
        latch the connection unhealthy, set self._backlogged, and persist
        to the SQLite fallback so the message is never dropped."""
        try:
            ch.basic_publish(
                exchange=ADSB_EXCHANGE,
                routing_key=routing_key,
                body=payload.encode(),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            self._staleness_seconds = time.time() - received_at
            return True
        except Exception as exc:
            logger.debug("RabbitMQ publish failed: %s — writing to fallback.", exc)
            with self._rmq_lock:
                self._rmq_connected = False
            self._backlogged.set()
            self._fallback_put(routing_key, payload, received_at)
            return False

    def _publish_fallback_row(self, ch, wrapped: str) -> None:
        """process_fn for FallbackQueue.drain_batch: unwrap the stored
        {routing_key, payload, received_at} and publish it. Raises on
        failure so drain_batch keeps the row queued and retries, and
        latches the connection unhealthy so the publish loop reconnects."""
        item = json.loads(wrapped)
        try:
            ch.basic_publish(
                exchange=ADSB_EXCHANGE,
                routing_key=item["routing_key"],
                body=item["payload"].encode(),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            # .get(), not [...]: a pre-upgrade row in queue.db may lack
            # "received_at" -- skip the staleness update rather than
            # raise and retry forever.
            received_at = item.get("received_at")
            if received_at is not None:
                self._staleness_seconds = time.time() - received_at
        except Exception:
            with self._rmq_lock:
                self._rmq_connected = False
            raise

    def _fallback_put(self, routing_key: str, payload: str, received_at: float) -> None:
        """FallbackQueue is payload-only, with no routing_key column, so
        the routing key and received_at are wrapped into one JSON string
        here and unwrapped again in _publish_fallback_row."""
        self._fallback.put(json.dumps(
            {"routing_key": routing_key, "payload": payload, "received_at": received_at}
        ))

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    def _connect_mqtt(self) -> None:
        mc = self._cfg.get("mqtt")
        if not mc:
            return

        self._mqtt = build_mqtt_client(
            mc, will_topic=f"SkyFollower/receiver/{self._id}/status"
        )
        self._mqtt.on_connect = self._on_mqtt_connect
        self._mqtt.on_disconnect = self._on_mqtt_disconnect
        try:
            self._mqtt.connect_async(mc["host"], port=mc.get("port", 1883), keepalive=60)
            self._mqtt.loop_start()
        except Exception as exc:
            logger.warning("MQTT connect failed: %s", exc)

    def _on_mqtt_connect(
        self, client, userdata, flags, reason_code, properties
    ) -> None:
        self._mqtt_connected = True
        client.publish(
            f"SkyFollower/receiver/{self._id}/status", "ONLINE", retain=True
        )
        self._publish_ha_autodiscovery()
        logger.info("MQTT connected.")

    def _on_mqtt_disconnect(
        self, client, userdata, flags, reason_code, properties
    ) -> None:
        self._mqtt_connected = False

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------

    def _telemetry_loop(self) -> None:
        while not self._shutdown.is_set():
            # Fixed cadence, never message-count-triggered. Waiting on
            # _shutdown (not sleep) lets this exit promptly on stop.
            self._shutdown.wait(timeout=MQTT_PUBLISH_INTERVAL_SECONDS)

            if self._redis is not None:
                self._flush_period_counters()

            # Backlog draining happens inside _rmq_publish_loop already --
            # no separate trigger needed here.
            self._publish_telemetry()

    def _flush_period_counters(self) -> None:
        """Push each connection's accumulated message count into Redis.
        Lazily loads incr_period_counter.lua on first use rather than at
        __init__, so startup makes no Redis call when identity is already
        persisted locally. Fails soft: a Redis hiccup just defers this
        cycle's counts."""
        if self._incr_period_counter_sha is None:
            try:
                lua_path = (
                    pathlib.Path(__file__).parent.parent / "shared" / "lua" / "incr_period_counter.lua"
                )
                self._incr_period_counter_sha = self._redis.script_load(lua_path.read_text())
            except Exception as exc:
                logger.debug("incr_period_counter.lua load failed: %s", exc)
                return

        now = datetime.now(timezone.utc)
        for (host, port), tracker in self._rates.items():
            connection_id = f"{_sanitize_mqtt_id(str(host))}_{_sanitize_mqtt_id(str(port))}"
            try:
                tracker.flush_to_redis(
                    redis_client=self._redis,
                    script_sha=self._incr_period_counter_sha,
                    key_fn=lambda period, cid=connection_id: receiver_message_count_key(
                        self._id, cid, period
                    ),
                    now=now,
                )
            except Exception as exc:
                logger.debug("Period counter flush failed for %s:%s: %s", host, port, exc)

    def _publish_telemetry(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return

        with self._rmq_lock:
            rmq_connected = self._rmq_connected

        base = f"SkyFollower/receiver/{self._id}/statistic"

        self._mqtt.publish(f"{base}/started_at", self._started_at, retain=True)
        self._mqtt.publish(f"{base}/version", self._version, retain=True)
        for src in self._cfg.get("sources", []):
            host, port = src["host"], src["port"]
            tracker = self._rates.get((host, port))
            if tracker is None:
                continue
            mqtt_host, mqtt_port = _sanitize_mqtt_id(str(host)), _sanitize_mqtt_id(str(port))
            self._mqtt.publish(
                f"{base}/messages_{mqtt_host}_{mqtt_port}_per_second",
                str(round(tracker.rate(), 2)),
                retain=True,
            )
            # In-memory only, resets on every restart -- hour/today are
            # the Redis-backed, cross-restart counters, published solely
            # by core-health.
            self._mqtt.publish(
                f"{base}/messages_{mqtt_host}_{mqtt_port}_total_lifetime",
                str(tracker.lifetime_count),
                retain=True,
            )
            self._mqtt.publish(
                f"{base}/{mqtt_host}_{mqtt_port}_connected",
                str(self._connected.get((host, port), False)),
                retain=True,
            )
            self._mqtt.publish(
                f"{base}/{mqtt_host}_{mqtt_port}_reconnect_count",
                str(self._reconnect_counts.get((host, port), 0)),
                retain=True,
            )
            last_message_at = self._last_message_at.get((host, port))
            if last_message_at is not None:
                self._mqtt.publish(
                    f"{base}/{mqtt_host}_{mqtt_port}_connected_attributes",
                    json.dumps({"last_message_received": last_message_at}),
                    retain=True,
                )
            # messages_*_total_{hour,today} are published by core-health,
            # not here -- with REDIS_HOST unset, those sensors simply
            # don't exist.
        # Includes in-memory queues, not just the durable SQLite depth,
        # so a broker blip absorbed entirely in RAM is still visible.
        local_queue_depth = (
            self._fallback.depth()
            + self._live_queue.qsize()
            + self._overflow_queue.qsize()
        )
        self._mqtt.publish(f"{base}/local_queue_depth", str(local_queue_depth), retain=True)
        self._mqtt.publish(
            f"{base}/dead_letter_queue_depth", str(self._fallback.dead_letter_depth()), retain=True
        )
        self._mqtt.publish(f"{base}/rabbitmq_connected", str(rmq_connected), retain=True)
        # Informational only -- age of the last message actually
        # published. Near zero while caught up; grows while draining a
        # backlog.
        self._mqtt.publish(
            f"{base}/backlog_age_seconds", str(round(self._staleness_seconds, 1)), retain=True
        )

    # ------------------------------------------------------------------
    # Docker healthcheck (heartbeat file)
    # ------------------------------------------------------------------

    def _healthcheck_loop(self) -> None:
        """Touch a heartbeat file while connected to both RabbitMQ and
        MQTT, for Docker's HEALTHCHECK to check the mtime of."""
        heartbeat_path = pathlib.Path(_HEALTHCHECK_HEARTBEAT_PATH)
        heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        while not self._shutdown.is_set():
            with self._rmq_lock:
                rmq_connected = self._rmq_connected
            if rmq_connected and self._mqtt_connected:
                try:
                    heartbeat_path.touch()
                except OSError:
                    pass
            time.sleep(HEALTHCHECK_INTERVAL_SECONDS)

    # ------------------------------------------------------------------
    # HA autodiscovery
    # ------------------------------------------------------------------

    def _publish_ha_autodiscovery(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return

        rid = self._id
        # `display` is the human-readable label; `rid` stays the stable
        # identifier for topic paths/unique_id regardless.
        display = self._name or rid[:8]
        base = f"SkyFollower/receiver/{rid}/statistic"
        device = build_ha_device(
            identifier=f"SkyFollower_receiver_{rid}",
            name=f"SkyFollower Receiver {display}",
            model="Receiver",
            configuration_url="https://brentio.github.io/SkyFollower/components/receiver.html",
        )
        publish_register(self._mqtt, device)
        availability = {
            "availability_topic": f"SkyFollower/receiver/{rid}/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }

        # Entity names omit `display` -- has_entity_name below has HA
        # compose the label from device.name instead. The
        # {host}:{port}/{source} qualifiers stay since they distinguish
        # sources on the same receiver.
        sensors = []
        for src in self._cfg.get("sources", []):
            host, port, source = src["host"], src["port"], src["source"]
            mqtt_host, mqtt_port = _sanitize_mqtt_id(str(host)), _sanitize_mqtt_id(str(port))
            field = f"messages_{mqtt_host}_{mqtt_port}_per_second"
            sensors.append((field, f"{host}:{port} {source} Messages/sec",
                             "mdi:broadcast", "measurement", "msg/s", None))
            sensors.append((f"{mqtt_host}_{mqtt_port}_connected", f"{host}:{port} Connected",
                             "mdi:lan-connect", None, None,
                             f"{base}/{mqtt_host}_{mqtt_port}_connected_attributes"))
            sensors.append((f"{mqtt_host}_{mqtt_port}_reconnect_count", f"{host}:{port} Reconnect Count",
                             "mdi:refresh", "total_increasing", None, None))
            # total_increasing is correct HA semantics even though this
            # counter resets on restart (in-memory, not Redis-backed).
            sensors.append((f"messages_{mqtt_host}_{mqtt_port}_total_lifetime",
                             f"{host}:{port} Messages Total (Lifetime)",
                             "mdi:counter", "total_increasing", None, None))
            # core-health is the sole publisher of
            # messages_*_total_{hour,today} discovery configs too, so this
            # doesn't compete for the same unique_id.
        sensors += [
            ("started_at", "Start Time",
             "mdi:clock-start", None, None, None),
            ("local_queue_depth", "Local Queue Depth",
             "mdi:tray-full", "measurement", None, None),
            ("dead_letter_queue_depth", "Dead Letter Queue Depth",
             "mdi:skull-crossbones", "measurement", None, None),
            ("rabbitmq_connected", "RabbitMQ Connected",
             "mdi:rabbit", None, None, None),
            ("backlog_age_seconds", "Backlog Age",
             "mdi:history", "measurement", "s", None),
        ]

        for field, desc, icon, state_class, unit, json_attributes_topic in sensors:
            payload: dict = {
                **availability,
                "state_topic": f"{base}/{field}",
                "name": desc,
                "has_entity_name": True,
                "unique_id": f"SkyFollower_receiver_{rid}_{field}",
                "object_id": f"SkyFollower_receiver_{rid}_{field}",
                "device": device,
                "icon": icon,
            }
            if state_class:
                payload["state_class"] = state_class
            if unit:
                payload["unit_of_measurement"] = unit
            if json_attributes_topic:
                payload["json_attributes_topic"] = json_attributes_topic
            if field == "started_at":
                payload["device_class"] = "timestamp"

            self._mqtt.publish(
                f"homeassistant/sensor/SkyFollower_receiver_{rid}_{field}/config",
                json.dumps(payload),
                retain=True,
            )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        logger.info("Shutdown requested.")
        self._shutdown.set()

        if self._mqtt:
            self._mqtt.publish(
                f"SkyFollower/receiver/{self._id}/status", "OFFLINE", retain=True
            )
            self._mqtt.loop_stop()

        with self._rmq_lock:
            if self._rmq_connection:
                try:
                    self._rmq_connection.close()
                except Exception:
                    pass

        logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        config = load_config("rabbitmq", "mqtt", "receiver")
    except ConfigError as exc:
        configure_logging()
        logger.critical("%s", exc)
        sys.exit(1)

    receiver = Receiver(config)

    def _handle_signal(sig, frame):
        receiver.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    receiver.start()


if __name__ == "__main__":
    main()
