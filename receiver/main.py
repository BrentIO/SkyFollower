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
import re
import signal
import socket
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Callable, Optional

import paho.mqtt.client as mqtt
import pika
import pyModeS as pms

from shared.adsb_1090 import parse_tcp_stream
from shared.config import DATA_DIR, ConfigError, load_config
from shared.fallback_queue import FallbackQueue
from shared.ha_discovery import build_ha_device
from shared.logging_setup import configure_logging
from shared.models import InboundMessage
from shared.mqtt import build_mqtt_client
from shared.rabbitmq_topology import ADSB_EXCHANGE, declare_adsb_topology
from shared.uat import parse_978_line

logger = logging.getLogger("receiver")

# tmpfs-mounted in docker-compose.receiver.yaml -- these writes must never
# hit the host's eMMC/SD storage, only /app/data (the fallback SQLite
# queue) is durable/persistent.
_HEALTHCHECK_HEARTBEAT_PATH = "/app/health/heartbeat"
_HEALTHCHECK_INTERVAL_SECONDS = 15

# Minimum spacing between "N unparseable lines" summary warnings, per
# connection -- keeps a genuine format mismatch visible at default log
# level without flooding logs at high traffic volume.
_UNPARSEABLE_WARNING_INTERVAL_SECONDS = 60

# ---------------------------------------------------------------------------
# Rate tracker — 30-second rolling window (copied from message processor pattern)
# ---------------------------------------------------------------------------

class _RateTracker:
    def __init__(self, window: int = 30) -> None:
        self._window = window
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()

    def record(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._timestamps.append(now)
            cutoff = now - self._window
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()

    def rate(self) -> float:
        now = time.monotonic()
        with self._lock:
            cutoff = now - self._window
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            return len(self._timestamps) / self._window


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
        for src in config.get("sources", []):
            key = (src["host"], src["port"])
            self._rates[key] = _RateTracker()
            self._connected[key] = False
            self._reconnect_counts[key] = 0
            self._last_message_at[key] = None

        # Fallback SQLite queue
        os.makedirs(DATA_DIR, exist_ok=True)
        self._fallback = FallbackQueue(os.path.join(DATA_DIR, "queue.db"))

        self._id = _load_or_create_receiver_id(DATA_DIR)
        # Optional human-friendly label for HA name/model/sensor labels --
        # self._id (the persisted UUID) stays the stable identifier used for
        # topic paths/unique_id regardless of whether this is set or changes.
        self._name = config.get("name")
        self._version = os.environ.get("VERSION", "dev")

        # RabbitMQ state
        self._rmq_connection: Optional[pika.BlockingConnection] = None
        self._rmq_channel = None
        self._rmq_connected = False
        self._rmq_lock = threading.Lock()

        # MQTT state
        self._mqtt: Optional[mqtt.Client] = None
        self._mqtt_connected = False

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._setup_logging()
        logger.info(f"Starting SkyFollower Receiver {self._id} {self._version}")
        self._connect_mqtt()

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
                    "Cannot connect to readsb %s:%s: %s — retrying in 5s…", host, port, exc
                )
            except Exception as exc:
                self._connected[key] = False
                logger.error(
                    "Source %s:%s error: %s — retrying in 5s…", host, port, exc
                )

            if not self._shutdown.is_set():
                # Reaching here always means a drop-and-retry: either the
                # try block above raised (OSError/other exception) or the
                # stream reader returned via its closed-connection break --
                # a clean shutdown skips this entirely via the is_set() check.
                self._reconnect_counts[key] = self._reconnect_counts.get(key, 0) + 1
                time.sleep(5)

    def _read_1090_stream(
        self, sock: socket.socket, host: str, port: int, source: str, rate_tracker: _RateTracker
    ) -> None:
        buf = bytearray()
        unparseable_count = 0
        unparseable_window_start = time.monotonic()

        def _maybe_warn_unparseable() -> None:
            nonlocal unparseable_count, unparseable_window_start
            now = time.monotonic()
            if unparseable_count and now - unparseable_window_start >= _UNPARSEABLE_WARNING_INTERVAL_SECONDS:
                logger.warning(
                    "%d unparseable 1090 message(s) from %s:%s (source=%s) in the last %ds — "
                    "check the upstream feed format.",
                    unparseable_count, host, port, source, _UNPARSEABLE_WARNING_INTERVAL_SECONDS,
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
            if unparseable_count and now - unparseable_window_start >= _UNPARSEABLE_WARNING_INTERVAL_SECONDS:
                logger.warning(
                    "%d unparseable 978 line(s) from %s:%s (source=%s) in the last %ds — "
                    "check the upstream feed format.",
                    unparseable_count, host, port, source, _UNPARSEABLE_WARNING_INTERVAL_SECONDS,
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
        self._publish(icao_hex, payload)

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
        )

    def _rmq_loop(self) -> None:
        """Maintain a persistent RabbitMQ connection, reconnecting on failure."""
        while not self._shutdown.is_set():
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

                # _drain_fallback() spawns its own background thread (or
                # skips if one is already running) -- see drain_in_background.
                self._drain_fallback()

                # Keep the connection alive with process_data_events
                while not self._shutdown.is_set():
                    try:
                        conn.process_data_events(time_limit=1)
                    except pika.exceptions.AMQPConnectionError:
                        break
                    except Exception:
                        break

            except pika.exceptions.AMQPConnectionError as exc:
                logger.warning("RabbitMQ unavailable: %s. Retrying in 10s…", exc)
            except Exception as exc:
                logger.error("RabbitMQ error: %s. Retrying in 10s…", exc)
            finally:
                with self._rmq_lock:
                    self._rmq_connected = False
                    self._rmq_channel = None
                    self._rmq_connection = None

            if not self._shutdown.is_set():
                time.sleep(10)

    def _pika_invoke(self, fn: Callable[[], None], timeout: float = 5.0) -> None:
        """Run fn() on the "rabbitmq" thread via pika's own thread-safe
        callback hand-off, blocking the calling thread until fn() completes
        or raises.

        pika.BlockingConnection (and the async transport underneath it) is
        not safe to call concurrently from multiple threads -- every actual
        channel/connection call (basic_publish, process_data_events,
        queue_declare, ...) must happen from a single thread. _rmq_loop
        already owns the connection and drives process_data_events() from
        the dedicated "rabbitmq" thread; every *other* thread (a source
        thread publishing a live message, the fallback-drain thread
        replaying a backlog) must route its pika calls through here instead
        of touching the channel directly, or two threads can end up inside
        pika's transport internals at the same moment, corrupting its
        buffers and crashing the connection.

        Raises RuntimeError if there's no live connection to schedule
        against, TimeoutError if the rabbitmq thread doesn't run fn()
        within `timeout` seconds (e.g. the connection died between being
        captured here and the callback actually running), or whatever fn()
        itself raised, re-raised in the calling thread.
        """
        with self._rmq_lock:
            conn = self._rmq_connection

        if conn is None:
            raise RuntimeError("No RabbitMQ connection")

        done = threading.Event()
        outcome: dict = {}

        def _runner() -> None:
            try:
                fn()
            except Exception as exc:
                outcome["exc"] = exc
            finally:
                done.set()

        conn.add_callback_threadsafe(_runner)

        if not done.wait(timeout=timeout):
            raise TimeoutError("Timed out waiting for the rabbitmq thread to run a pika call")

        if "exc" in outcome:
            raise outcome["exc"]

    def _do_publish(self, routing_key: str, payload: str) -> None:
        """The only place that actually calls channel.basic_publish().
        Must only ever run on the rabbitmq thread -- callers reach this via
        _pika_invoke(), never directly."""
        with self._rmq_lock:
            channel = self._rmq_channel
        if channel is None:
            raise RuntimeError("RabbitMQ channel gone")
        channel.basic_publish(
            exchange=ADSB_EXCHANGE,
            routing_key=routing_key,
            body=payload.encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )

    def _publish(self, routing_key: str, payload: str) -> None:
        """Publish to RabbitMQ; fall back to SQLite on failure."""
        with self._rmq_lock:
            connected = self._rmq_connected

        if connected:
            try:
                self._pika_invoke(lambda: self._do_publish(routing_key, payload))
                return
            except Exception as exc:
                logger.debug("RabbitMQ publish failed: %s — writing to fallback.", exc)
                with self._rmq_lock:
                    self._rmq_connected = False

        self._fallback_put(routing_key, payload)

    def _fallback_put(self, routing_key: str, payload: str) -> None:
        """FallbackQueue (shared/fallback_queue.py) is payload-only -- it
        has no routing_key column of its own, unlike this component's
        previous hand-rolled fallback queue. Persisting the routing key
        alongside the payload keeps the drain path identical to the live
        publish path, with no need to re-parse a stored message body to
        work out where it was going -- so it's wrapped into one JSON string
        here and unwrapped again in _drain_fallback's process_fn."""
        self._fallback.put(json.dumps({"routing_key": routing_key, "payload": payload}))

    def _drain_fallback(self) -> None:
        def process_fn(wrapped: str) -> None:
            item = json.loads(wrapped)
            routing_key = item["routing_key"]
            payload = item["payload"]

            try:
                self._pika_invoke(lambda: self._do_publish(routing_key, payload))
            except Exception:
                with self._rmq_lock:
                    self._rmq_connected = False
                raise

        self._fallback.drain_in_background(process_fn)

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
        interval = self._cfg.get("telemetry_interval_seconds", 30)
        while not self._shutdown.is_set():
            time.sleep(interval)
            # Independent of _rmq_loop's reconnect-triggered drain: a
            # publish failure can pin _rmq_connected False (or leave
            # messages queued) without the underlying connection ever
            # raising AMQPConnectionError, in which case _rmq_loop never
            # re-enters its reconnect branch and _drain_fallback never
            # runs again on its own. This periodic sweep is a cheap
            # no-op when the queue is empty and doesn't depend on that
            # edge-triggered detection ever firing. _drain_fallback()
            # itself spawns the actual drain in the background (or skips
            # if one's already running from the reconnect path), so this
            # call returns immediately and never delays the telemetry
            # publish below it.
            with self._rmq_lock:
                rmq_connected = self._rmq_connected
            if rmq_connected:
                self._drain_fallback()
            self._publish_telemetry()

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
        self._mqtt.publish(f"{base}/local_queue_depth", str(self._fallback.depth()), retain=True)
        self._mqtt.publish(
            f"{base}/dead_letter_queue_depth", str(self._fallback.dead_letter_depth()), retain=True
        )
        self._mqtt.publish(f"{base}/rabbitmq_connected", str(rmq_connected), retain=True)

    # ------------------------------------------------------------------
    # Docker healthcheck (heartbeat file)
    # ------------------------------------------------------------------

    def _healthcheck_loop(self) -> None:
        """Touch a heartbeat file while genuinely connected to both RabbitMQ
        and MQTT, for Docker's HEALTHCHECK to check the mtime of. A fixed
        interval independent of telemetry_interval_seconds (which is
        user-configurable and not tuned for this), so healthcheck timing
        stays predictable."""
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
            time.sleep(_HEALTHCHECK_INTERVAL_SECONDS)

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
            model=display,
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
        config = load_config("rabbitmq", "mqtt", "telemetry", "receiver")
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
