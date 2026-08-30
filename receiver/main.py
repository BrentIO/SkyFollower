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
from shared.fallback_queue import DRAIN_PROGRESSED, FallbackQueue
from shared.ha_discovery import build_ha_device
from shared.logging_setup import configure_logging
from shared.models import InboundMessage
from shared.mqtt import build_mqtt_client
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
    TCP_KEEPALIVE_PROBES,
    TCP_KEEPIDLE_SECONDS,
    TCP_KEEPINTVL_SECONDS,
    UNPARSEABLE_WARNING_INTERVAL_SECONDS,
)
from shared.uat import parse_978_line

logger = logging.getLogger("receiver")

# tmpfs-mounted in docker-compose.receiver.yaml -- these writes must never
# hit the host's eMMC/SD storage, only /app/data (the fallback SQLite
# queue) is durable/persistent. Every timing value the receiver uses is a
# named constant from shared/timing.py -- imported above, not redefined here.
_HEALTHCHECK_HEARTBEAT_PATH = "/app/health/heartbeat"

# In-memory hand-off between the socket-read threads and the sole
# "rabbitmq" publishing thread. A source thread parses a message, drops it
# here, and loops straight back to sock.recv() -- it never touches pika and
# never waits on the broker, so backlog drain can't delay live intake. The
# bound keeps a broker outage from growing this without limit: once it's
# full, a source thread routes straight to the durable SQLite fallback
# instead (still without blocking). ~10k messages is a few seconds of
# buffer at the reference message rate and a small, bounded amount of RAM
# to lose on a hard crash mid-outage (a clean reconnect drains it first).
_LIVE_QUEUE_MAXSIZE = 10_000

# Cap on how many live messages the rabbitmq thread publishes in one pass
# before returning to process_data_events -- purely so heartbeats and the
# broker's blocked/unblocked signals still get serviced under sustained
# load. Whatever is left stays queued and is taken on the next pass, still
# ahead of any fallback-drain row.
_LIVE_PUBLISH_BATCH_MAX = 2_000

# How long the rabbitmq thread blocks waiting for the next live message
# when both the live queue and the fallback backlog are empty -- short
# enough to keep pika's heartbeat serviced and to pick up the first
# message after an idle period promptly, long enough not to busy-spin.
_RMQ_IDLE_POLL_SECONDS = 1.0

# ---------------------------------------------------------------------------
# Rate tracker — RATE_WINDOW_SECONDS rolling window (copied from message
# processor pattern)
# ---------------------------------------------------------------------------


class _RateTracker:
    def __init__(self, window: int = RATE_WINDOW_SECONDS) -> None:
        self._window = window
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

        # Redis-backed period counters. Pure in-memory
        # running totals for *this process's own lifetime* -- record()
        # only ever adds to them, never reads/writes Redis and never
        # resets them for a real hour/day boundary (that happens in
        # flush_to_redis(), from the telemetry thread, never here). There
        # is deliberately no persisted store behind these three fields, so
        # a receiver restart resets the receiver's own published totals to
        # zero even though Redis (what core-health reads) keeps
        # accumulating across that restart -- see receiver/README.md's
        # "known limitation" note.
        self.hour_count = 0
        self.today_count = 0
        self.lifetime_count = 0
        # Bookkeeping only flush_to_redis() touches: the last value of
        # each counter already pushed to Redis (so it can INCRBY just the
        # delta), and the hour/day "bucket" (floored to the boundary) each
        # counter was last flushed against, to detect a real rollover.
        self._flushed_hour = 0
        self._flushed_today = 0
        self._flushed_lifetime = 0
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
        """Pushes the delta accumulated since the last flush into Redis
        (via incr_period_counter.lua for hour/today, a plain INCRBY for
        lifetime since it never expires), and resets hour_count/today_count
        locally on an actually-observed UTC hour/midnight rollover.

        Called only from the receiver's telemetry thread -- never the
        per-message hot path record() runs on.
        """
        hour_bucket = now.replace(minute=0, second=0, microsecond=0)
        day_bucket = now.replace(hour=0, minute=0, second=0, microsecond=0)

        with self._lock:
            if self._hour_bucket is None:
                self._hour_bucket = hour_bucket
            if self._day_bucket is None:
                self._day_bucket = day_bucket

            # On a detected rollover, the small remainder of messages
            # already counted toward the now-closed period is dropped
            # rather than flushed -- attributing it to the old bucket risks
            # computing an EXPIREAT that's already in the past (if Redis's
            # own TTL beat this flush to the real boundary, that would
            # delete the key the instant it's written), and attributing it
            # to the new bucket would double-count messages that were
            # already local-only. Bounded to at most one flush cycle's
            # worth (MQTT_PUBLISH_INTERVAL_SECONDS) each real rollover --
            # see receiver/README.md.
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

            lifetime_delta = self.lifetime_count - self._flushed_lifetime
            self._flushed_lifetime = self.lifetime_count

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
        if lifetime_delta:
            redis_client.incrby(key_fn("lifetime"), lifetime_delta)


def _load_or_create_receiver_id(data_dir: str) -> str:
    """Load this receiver's persisted identity, generating one on first run.

    Unlike a manually-set RECEIVER_ID, this needs no operator input and
    can't collide between instances -- generated once and reused across
    restarts so MQTT topics/HA identifiers stay stable for this container's
    whole lifetime regardless of how many times its display name changes.
    """
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

    Called for every source connection (1090 and 978 alike) immediately
    after it opens. ``SO_KEEPALIVE`` is the load-bearing part and is
    portable. The three timer options are Linux-only: on macOS (where the
    dev test suite runs) the names don't exist, so each is ``hasattr``
    guarded; and even where a name exists it is only valid on a real TCP
    socket, so the call is wrapped in ``try``/``except OSError`` -- a
    non-TCP socket (a unit test's ``socketpair``) or an unusual platform
    degrades to "keepalive on, default timers" rather than raising. A real
    source socket from ``create_connection`` on a Linux container always
    accepts them. See ``shared/timing.py``'s ``TCP_KEEPIDLE_SECONDS`` block
    for the timing rationale.
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

    Home Assistant discovery requires object_id/unique_id to match
    ^[a-zA-Z0-9_-]+$. Applied wherever a source's host/port is turned into
    a topic segment or identifier, so the runtime state topic and the
    discovery topic/object_id/unique_id use the identical sanitized name.
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

        # Per-connection rate trackers keyed by (host, port) -- not the
        # source tag, since more than one connection can share a tag (e.g.
        # two EXTERNAL feeds) and needs independent tracking rather than a
        # summed rate.
        self._rates: dict[tuple[str, int], _RateTracker] = {}
        # Live up/down state per connection -- True only while the TCP
        # socket to that connection's readsb instance is open.
        self._connected: dict[tuple[str, int], bool] = {}
        # Count of drop-and-retry cycles per connection -- distinguishes a
        # rock-solid connection from one that's currently "Connected: True"
        # but flapping every few minutes.
        self._reconnect_counts: dict[tuple[str, int], int] = {}
        # UTC ISO-8601 timestamp of the last message processed for each
        # connection -- None until the first one arrives, so a low-traffic
        # feed's silence is visible directly instead of only inferred from
        # its rate having decayed to zero.
        self._last_message_at: dict[tuple[str, int], Optional[str]] = {}

        # Redis is entirely optional for the receiver -- an unset
        # REDIS_HOST leaves this None and none of the identity-claim/
        # heartbeat/period-counter/core-health-registration behavior below
        # runs at all, matching the receiver's original behavior exactly
        # (random-UUID identity, no Redis interaction).
        rc = config.get("redis") or {}
        self._redis = build_redis_client(rc) if rc.get("host") else None
        # Lazily script_load()'d on first flush rather than here -- loading
        # it during __init__ would be a Redis call on every startup, even
        # the "local identity already persisted, zero Redis calls" case
        # below.
        self._incr_period_counter_sha: Optional[str] = None

        for src in config.get("sources", []):
            key = (src["host"], src["port"])
            self._rates[key] = _RateTracker()
            self._connected[key] = False
            self._reconnect_counts[key] = 0
            self._last_message_at[key] = None

        # Fallback SQLite queue
        os.makedirs(DATA_DIR, exist_ok=True)
        self._fallback = FallbackQueue(os.path.join(DATA_DIR, "queue.db"))

        self._id = self._resolve_identity()
        # Optional human-friendly label for HA name/model/sensor labels --
        # when Redis is configured, this is the same value as self._id
        # (the claimed name IS the identity now); in the legacy no-Redis
        # path self._id is a UUID and this stays the only human-readable
        # label. self._id stays the stable identifier used for topic
        # paths/unique_id regardless of whether this is set or changes.
        self._name = config.get("name")
        self._version = os.environ.get("VERSION", "dev")

        # RabbitMQ state
        self._rmq_connection: Optional[pika.BlockingConnection] = None
        self._rmq_channel = None
        self._rmq_connected = False
        self._rmq_lock = threading.Lock()

        # Live messages parsed off the source sockets, waiting for the
        # "rabbitmq" thread to publish them -- see _LIVE_QUEUE_MAXSIZE.
        self._live_queue: queue.Queue[tuple[str, str]] = queue.Queue(
            maxsize=_LIVE_QUEUE_MAXSIZE
        )

        # MQTT state
        self._mqtt: Optional[mqtt.Client] = None
        self._mqtt_connected = False

    # ------------------------------------------------------------------
    # Identity -- claim-and-persist, mirroring
    # _claim_message_processor_id()/_heartbeat_loop in
    # message-processor/main.py exactly.
    # ------------------------------------------------------------------

    def _resolve_identity(self) -> str:
        """Three-case startup identity resolution:

        1. Local identity already persisted ({data_dir}/receiver_id, from
           a prior successful claim, or the legacy UUID scheme) -- use it
           immediately, zero Redis calls. Works with Redis/RabbitMQ both
           unreachable, and is every boot after the first.
        2. No local identity, Redis not configured at all (REDIS_HOST
           unset) -- fall back to the original random-UUID scheme
           unconditionally; none of the Redis-backed behavior applies.
        3. No local identity, Redis configured:
           a. reachable -- SET NX the configured RECEIVER_NAME; success
              persists it locally forever after. Failure (name already
              claimed by another live receiver) is a critical + exit.
           b. unreachable -- critical + exit. Deliberately not a
              fallback-and-proceed case: first-time identity establishment
              requires Redis reachability to safely verify uniqueness.
              Every later boot is case 1.
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
        """Adds/refreshes this receiver's entry in core-health's small
        discovery index: an idempotent SADD of the
        receiver's name into the shared index SET, plus a per-receiver
        JSON registration entry (just the source list -- host/port/source
        triples) TTL'd the same as the heartbeat. Called once at the end
        of every startup (whichever identity-resolution case ran) and
        again on every subsequent heartbeat tick, so a receiver that
        resumed an already-persisted identity re-registers itself just as
        promptly as a freshly-claimed one. Lets core-health enumerate live
        receivers via SMEMBERS + direct key reads instead of a keyspace
        SCAN.

        Fails soft -- this is best-effort discovery plumbing, never a
        reason to affect the receiver's own startup or heartbeat.
        """
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
        """Mirrors message-processor's _heartbeat_loop exactly: sleep
        HEARTBEAT_INTERVAL_SECONDS, then refresh (unconditional EXPIRE,
        never a second SET NX) the claim key's TTL to HEARTBEAT_TTL_SECONDS,
        fail-soft on any Redis error. Also refreshes the core-health
        registration on the same cadence -- this is what re-registers a
        receiver that resumed an already-persisted identity (case 1 above)
        without ever calling _register_with_core_health() at claim time."""
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

        # Start RabbitMQ connection in a background thread
        threading.Thread(
            target=self._rmq_loop, daemon=True, name="rabbitmq"
        ).start()

        # Start telemetry loop
        threading.Thread(
            target=self._telemetry_loop, daemon=True, name="telemetry"
        ).start()

        # Start healthcheck heartbeat loop
        threading.Thread(
            target=self._healthcheck_loop, daemon=True, name="healthcheck"
        ).start()

        # One thread per source
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

        # Block main thread until shutdown
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
                # Reaching here always means a drop-and-retry: either the
                # try block above raised (OSError/other exception) or the
                # stream reader returned via its closed-connection break --
                # a clean shutdown skips this entirely via the is_set() check.
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
                    # !-preambles and blank lines are routine, expected
                    # input -- only count/log lines that looked like real
                    # data but still failed to parse.
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

        # Normalise to 6-char uppercase
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
        self._enqueue_live(icao_hex, payload)

    def _enqueue_live(self, routing_key: str, payload: str) -> None:
        """Hand a parsed message to the in-memory publish queue and return
        at once -- the source thread never blocks on RabbitMQ. If the queue
        is full (the broker has been unreachable long enough that even this
        buffer backed up), the message goes straight to the durable SQLite
        fallback rather than stalling the socket read behind the backlog."""
        try:
            self._live_queue.put_nowait((routing_key, payload))
        except queue.Full:
            self._fallback_put(routing_key, payload)

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
            # The publishing thread calls basic_publish directly; a broker
            # resource alarm (disk-free / high memory) blocks publishers
            # while leaving the TCP connection up, so without this a publish
            # would wedge forever. pika tears the connection down when the
            # blocked state outlasts this, and _rmq_loop reconnects.
            blocked_connection_timeout=RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS,
        )

    def _rmq_loop(self) -> None:
        """Own the RabbitMQ connection and be the *only* thread that ever
        touches its channel. Source threads drop parsed messages into
        self._live_queue and never wait on the broker; this loop publishes
        them, and only advances the SQLite fallback backlog when nothing is
        waiting to go out live. Reconnects on any failure."""
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
        """Inner loop while a connection is up: pump pika, publish live
        messages with strict priority, then -- only if the live queue is
        empty -- advance the fallback backlog one row. Returns (so
        _rmq_loop reconnects) on shutdown or any publish/connection
        failure."""
        while not self._shutdown.is_set():
            # A publish failure latches _rmq_connected False without the
            # connection necessarily raising (broker blocking publishers on
            # a resource alarm). Reconnect to re-validate rather than
            # looping forever routing everything to the fallback.
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

            # Strict priority: everything queued off the sockets goes out
            # before a single backlog row is touched.
            published_live = self._publish_live_batch(ch)
            with self._rmq_lock:
                if not self._rmq_connected:
                    return
            if published_live:
                continue

            # Nothing waiting live -- move the backlog forward by one row,
            # then loop straight back to re-check the live queue.
            step = self._fallback.drain_one(
                lambda wrapped: self._publish_fallback_row(ch, wrapped)
            )
            if step == DRAIN_PROGRESSED:
                continue

            # A backlog row that failed to publish latches the connection
            # unhealthy -- go straight back to the top to reconnect rather
            # than idling first.
            with self._rmq_lock:
                if not self._rmq_connected:
                    continue

            # Fully idle, or the head-of-queue row is in its retry cooldown.
            # Wait for the next live message rather than busy-spinning, but
            # wake often enough to keep pika's heartbeat serviced.
            if self._shutdown.is_set():
                return
            try:
                routing_key, payload = self._live_queue.get(
                    timeout=_RMQ_IDLE_POLL_SECONDS
                )
            except queue.Empty:
                continue
            self._publish_one(ch, routing_key, payload)

    def _publish_live_batch(self, ch) -> bool:
        """Publish up to _LIVE_PUBLISH_BATCH_MAX queued live messages,
        oldest-first. Returns True if at least one was dequeued this call
        (whether or not it published cleanly -- a failure latches
        _rmq_connected False, which the caller checks). Stops early on the
        first failure so the caller can reconnect promptly."""
        published = 0
        while published < _LIVE_PUBLISH_BATCH_MAX:
            try:
                routing_key, payload = self._live_queue.get_nowait()
            except queue.Empty:
                break
            published += 1
            if not self._publish_one(ch, routing_key, payload):
                break
        return published > 0

    def _publish_one(self, ch, routing_key: str, payload: str) -> bool:
        """basic_publish one message directly on the rabbitmq thread. On
        failure, latch the connection unhealthy and persist the message to
        the SQLite fallback so it is never dropped. Returns False on
        failure."""
        try:
            ch.basic_publish(
                exchange=ADSB_EXCHANGE,
                routing_key=routing_key,
                body=payload.encode(),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            return True
        except Exception as exc:
            logger.debug("RabbitMQ publish failed: %s — writing to fallback.", exc)
            with self._rmq_lock:
                self._rmq_connected = False
            self._fallback_put(routing_key, payload)
            return False

    def _publish_fallback_row(self, ch, wrapped: str) -> None:
        """process_fn for FallbackQueue.drain_one: unwrap the stored
        {routing_key, payload} and publish it on the rabbitmq thread.
        Raises on failure so the row stays queued (drain_one owns the
        retry/dead-letter accounting) and latches the connection unhealthy
        so the publish loop reconnects."""
        item = json.loads(wrapped)
        try:
            ch.basic_publish(
                exchange=ADSB_EXCHANGE,
                routing_key=item["routing_key"],
                body=item["payload"].encode(),
                properties=pika.BasicProperties(delivery_mode=2),
            )
        except Exception:
            with self._rmq_lock:
                self._rmq_connected = False
            raise

    def _fallback_put(self, routing_key: str, payload: str) -> None:
        """FallbackQueue (shared/fallback_queue.py) is payload-only -- it
        has no routing_key column of its own, unlike this component's
        previous hand-rolled fallback queue. Persisting the routing key
        alongside the payload keeps the drain path identical to the live
        publish path, with no need to re-parse a stored message body to
        work out where it was going -- so it's wrapped into one JSON string
        here and unwrapped again in _publish_fallback_row."""
        self._fallback.put(json.dumps({"routing_key": routing_key, "payload": payload}))

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
            # Purely time-based: telemetry publishes on a fixed cadence,
            # never early on a message-count trigger. Waiting on _shutdown
            # rather than sleeping lets the loop exit promptly on stop.
            self._shutdown.wait(timeout=MQTT_PUBLISH_INTERVAL_SECONDS)

            if self._redis is not None:
                self._flush_period_counters()

            # Draining the fallback backlog is intrinsic to _rmq_publish_loop
            # -- it works a backlog row whenever no live message is waiting,
            # every iteration, for as long as the connection holds -- so
            # there is no separate drain trigger to fire here.
            self._publish_telemetry()

    def _flush_period_counters(self) -> None:
        """Pushes each connection's accumulated message count into Redis.
        Lazily loads incr_period_counter.lua on first use
        rather than at __init__ time -- see the comment on
        self._incr_period_counter_sha. Fails soft: a Redis hiccup here
        just means this cycle's counts stay pending and get folded into
        the next successful flush."""
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
            if self._redis is not None:
                # Local running totals, not a Redis read -- see
                # _RateTracker's docstring on why these reset on receiver
                # restart even though Redis (core-health's source) doesn't.
                for period, count in (
                    ("hour", tracker.hour_count),
                    ("today", tracker.today_count),
                    ("lifetime", tracker.lifetime_count),
                ):
                    self._mqtt.publish(
                        f"{base}/messages_{mqtt_host}_{mqtt_port}_total_{period}",
                        str(count),
                        retain=True,
                    )
        # Both backlogs an operator cares about: the durable SQLite queue
        # plus whatever is still buffered in memory waiting for the
        # rabbitmq thread, so a broker blip absorbed entirely in RAM is
        # still visible rather than reading as zero.
        local_queue_depth = self._fallback.depth() + self._live_queue.qsize()
        self._mqtt.publish(f"{base}/local_queue_depth", str(local_queue_depth), retain=True)
        self._mqtt.publish(
            f"{base}/dead_letter_queue_depth", str(self._fallback.dead_letter_depth()), retain=True
        )
        self._mqtt.publish(f"{base}/rabbitmq_connected", str(rmq_connected), retain=True)

    # ------------------------------------------------------------------
    # Docker healthcheck (heartbeat file)
    # ------------------------------------------------------------------

    def _healthcheck_loop(self) -> None:
        """Touch a heartbeat file while genuinely connected to both RabbitMQ
        and MQTT, for Docker's HEALTHCHECK to check the mtime of. Runs at
        HEALTHCHECK_INTERVAL_SECONDS, tuned against HEALTHCHECK_MAX_AGE_SECONDS
        (see shared/timing.py) independent of the MQTT publish cadence."""
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
        # The friendly name (when set) is what a human actually reads;
        # rid stays the stable identifier underneath for topic paths/
        # unique_id regardless of which label is shown here.
        display = self._name or rid[:8]
        base = f"SkyFollower/receiver/{rid}/statistic"
        device = build_ha_device(
            identifier=f"SkyFollower_receiver_{rid}",
            name=f"SkyFollower Receiver {display}",
            model="Receiver",
            configuration_url="https://brentio.github.io/SkyFollower/components/receiver.html",
        )
        availability = {
            "availability_topic": f"SkyFollower/receiver/{rid}/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }

        # Entity names deliberately omit `display` -- has_entity_name below
        # tells HA to compose the displayed label from device.name + this
        # short name instead. The {host}:{port} (and {source}, for the
        # per-source Messages/sec sensor) qualifiers stay: unlike `display`,
        # they distinguish between multiple sources on the same receiver
        # and aren't redundant with the device block.
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
            if self._redis is not None:
                for period, label in (("hour", "Hour"), ("today", "Today"), ("lifetime", "Lifetime")):
                    sensors.append((
                        f"messages_{mqtt_host}_{mqtt_port}_total_{period}",
                        f"{host}:{port} {source} Messages ({label})",
                        "mdi:counter", "total_increasing", None, None,
                    ))
        sensors += [
            ("started_at", "Start Time",
             "mdi:clock-start", None, None, None),
            ("local_queue_depth", "Local Queue Depth",
             "mdi:tray-full", "measurement", None, None),
            ("dead_letter_queue_depth", "Dead Letter Queue Depth",
             "mdi:skull-crossbones", "measurement", None, None),
            ("rabbitmq_connected", "RabbitMQ Connected",
             "mdi:rabbit", None, None, None),
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
