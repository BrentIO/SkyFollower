#!/usr/bin/env python3
"""
SkyFollower Receiver

Connects to one or more readsb TCP ports (raw ADS-B format), extracts ICAO hex
from each message using pyModeS, and routes it to the appropriate RabbitMQ queue
based on a modulo-bucketing scheme.  Falls back to a local SQLite queue when
RabbitMQ is unavailable, and drains the fallback on reconnect.

One container handles all configured sources concurrently (one thread per source).
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import signal
import socket
import sqlite3
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import pika
import pyModeS as pms

from shared.models import InboundMessage

logger = logging.getLogger("receiver")

# ---------------------------------------------------------------------------
# TCP stream parser
# ---------------------------------------------------------------------------

# readsb sends messages as:   *{hex_chars};\n
# We collect hex chars between '*' (0x2A) and ';' (0x3B).
_STAR = 0x2A   # '*'
_SEMI = 0x3B   # ';'

_HEX_BYTES = frozenset(
    list(range(0x30, 0x3A))   # 0-9
    + list(range(0x41, 0x47)) # A-F
    + list(range(0x61, 0x67)) # a-f
)


def parse_tcp_stream(data: bytes, buf: bytearray) -> list[str]:
    """
    Parse a chunk of raw TCP data from a readsb stream.

    *buf* is a mutable bytearray that carries state between calls (partial
    message accumulator).  Returns a list of complete uppercase hex strings
    (without the surrounding ``*`` / ``;``).

    The buffer starts in "waiting for star" mode when empty.  It accumulates
    hex bytes after a ``*`` and emits a message when ``;`` is encountered.
    Non-hex bytes between ``*`` and ``;`` cause the current partial to be
    discarded and collection to restart.
    """
    messages: list[str] = []

    for byte in data:
        if byte == _STAR:
            buf.clear()
        elif byte == _SEMI:
            if buf:
                messages.append(buf.decode("ascii").upper())
                buf.clear()
        elif byte in _HEX_BYTES:
            buf.extend([byte])
        else:
            # Invalid byte inside message — discard partial
            buf.clear()

    return messages


# ---------------------------------------------------------------------------
# Rate tracker — 30-second rolling window (copied from processor pattern)
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


# ---------------------------------------------------------------------------
# SQLite fallback queue
# ---------------------------------------------------------------------------

class _FallbackQueue:
    """
    Persistent SQLite queue used when RabbitMQ is unavailable.
    Written to /app/data/queue.db (host-mounted volume).
    """

    _SCHEMA = (
        "CREATE TABLE IF NOT EXISTS queue "
        "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
        " queue_name TEXT, "
        " payload TEXT, "
        " received_at REAL)"
    )

    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(self._SCHEMA)
        self._conn.commit()
        self._lock = threading.Lock()

    def put(self, queue_name: str, payload: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO queue (queue_name, payload, received_at) VALUES (?,?,?)",
                (queue_name, payload, time.time()),
            )
            self._conn.commit()

    def drain(self, publish_fn) -> None:
        """Drain all queued items oldest-first via publish_fn(queue_name, payload).

        Stops immediately if publish_fn raises (RabbitMQ went away again).
        """
        while True:
            with self._lock:
                cur = self._conn.execute(
                    "SELECT id, queue_name, payload FROM queue ORDER BY id ASC LIMIT 1"
                )
                row = cur.fetchone()
                if row is None:
                    break
                row_id, queue_name, payload = row

            try:
                publish_fn(queue_name, payload)
                with self._lock:
                    self._conn.execute("DELETE FROM queue WHERE id=?", (row_id,))
                    self._conn.commit()
            except Exception:
                break  # RabbitMQ went away; stop draining

    def depth(self) -> int:
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) FROM queue")
            return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Receiver
# ---------------------------------------------------------------------------

class Receiver:

    def __init__(self, config: dict) -> None:
        self._cfg = config
        self._started_at = datetime.now(timezone.utc).isoformat()
        self._shutdown = threading.Event()

        # Per-source rate trackers keyed by source string (e.g. "1090", "978")
        self._rates: dict[str, _RateTracker] = {}
        for src in config.get("sources", []):
            self._rates[src["source"]] = _RateTracker()

        # Fallback SQLite queue
        data_dir = config.get("data_dir", "/app/data")
        os.makedirs(data_dir, exist_ok=True)
        self._fallback = _FallbackQueue(os.path.join(data_dir, "queue.db"))

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
        self._connect_mqtt()

        # Start RabbitMQ connection in a background thread
        threading.Thread(
            target=self._rmq_loop, daemon=True, name="rabbitmq"
        ).start()

        # Start telemetry loop
        threading.Thread(
            target=self._telemetry_loop, daemon=True, name="telemetry"
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
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(fmt)
        logging.getLogger().addHandler(h)
        logging.getLogger().setLevel(
            logging.DEBUG if self._cfg.get("log_level", "info") == "debug"
            else logging.INFO
        )

    # ------------------------------------------------------------------
    # Source TCP loop
    # ------------------------------------------------------------------

    def _source_loop(self, src_cfg: dict) -> None:
        """Connect to readsb TCP port, parse the stream, and route messages."""
        host = src_cfg["host"]
        port = src_cfg["port"]
        source = src_cfg["source"]
        rate_tracker = self._rates.get(source, _RateTracker())

        while not self._shutdown.is_set():
            try:
                logger.info("Connecting to readsb at %s:%s (source=%s)…", host, port, source)
                with socket.create_connection((host, port), timeout=10) as sock:
                    sock.settimeout(5.0)
                    logger.info("Connected to %s:%s (source=%s).", host, port, source)
                    buf = bytearray()
                    while not self._shutdown.is_set():
                        try:
                            chunk = sock.recv(4096)
                        except socket.timeout:
                            continue
                        if not chunk:
                            logger.warning(
                                "readsb %s:%s closed connection — reconnecting.", host, port
                            )
                            break

                        for raw_hex in parse_tcp_stream(chunk, buf):
                            self._handle_message(raw_hex, source, rate_tracker)

            except OSError as exc:
                logger.warning(
                    "Cannot connect to readsb %s:%s: %s — retrying in 5s…", host, port, exc
                )
            except Exception as exc:
                logger.error(
                    "Source %s:%s error: %s — retrying in 5s…", host, port, exc
                )

            if not self._shutdown.is_set():
                time.sleep(5)

    def _handle_message(
        self, raw_hex: str, source: str, rate_tracker: _RateTracker
    ) -> None:
        """Extract ICAO, compute queue, and publish or enqueue."""
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

        processor_count = self._cfg.get("processor_count", 1)
        queue_name = f"adsb-{int(icao_hex, 16) % processor_count}"

        msg = InboundMessage(
            raw=raw_hex,
            icao_hex=icao_hex,
            received_at=time.time(),
            source=source,  # type: ignore[arg-type]
        )
        payload = msg.model_dump_json()

        rate_tracker.record()
        self._publish(queue_name, payload)

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

                # Pre-declare all queues
                processor_count = self._cfg.get("processor_count", 1)
                for i in range(processor_count):
                    ch.queue_declare(queue=f"adsb-{i}", durable=True)

                with self._rmq_lock:
                    self._rmq_connection = conn
                    self._rmq_channel = ch
                    self._rmq_connected = True

                logger.info("RabbitMQ connected.")

                # Drain fallback queue in background
                threading.Thread(
                    target=self._drain_fallback, daemon=True, name="drain-fallback"
                ).start()

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

    def _publish(self, queue_name: str, payload: str) -> None:
        """Publish to RabbitMQ; fall back to SQLite on failure."""
        with self._rmq_lock:
            connected = self._rmq_connected
            channel = self._rmq_channel

        if connected and channel:
            try:
                channel.basic_publish(
                    exchange="",
                    routing_key=queue_name,
                    body=payload.encode(),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                return
            except Exception as exc:
                logger.debug("RabbitMQ publish failed: %s — writing to fallback.", exc)
                with self._rmq_lock:
                    self._rmq_connected = False

        self._fallback.put(queue_name, payload)

    def _drain_fallback(self) -> None:
        def publish_fn(queue_name: str, payload: str) -> None:
            with self._rmq_lock:
                channel = self._rmq_channel
            if channel is None:
                raise RuntimeError("RabbitMQ channel gone")
            channel.basic_publish(
                exchange="",
                routing_key=queue_name,
                body=payload.encode(),
                properties=pika.BasicProperties(delivery_mode=2),
            )

        self._fallback.drain(publish_fn)

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    def _connect_mqtt(self) -> None:
        mc = self._cfg.get("mqtt")
        if not mc:
            return

        self._mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._mqtt.will_set("SkyFollower/receiver/status", "OFFLINE", retain=True)
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
        client.publish("SkyFollower/receiver/status", "ONLINE", retain=True)
        client.publish(
            "SkyFollower/receiver/statistic/started_at",
            self._started_at,
            retain=True,
        )
        self._publish_ha_autodiscovery()
        logger.info("MQTT connected.")

    def _on_mqtt_disconnect(
        self, client, userdata, flags, reason_code, properties
    ) -> None:
        self._mqtt_connected = False

    def _mqtt_publish(self, topic: str, value) -> None:
        if self._mqtt and self._mqtt_connected:
            self._mqtt.publish(topic, str(value))

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------

    def _telemetry_loop(self) -> None:
        interval = self._cfg.get("telemetry_interval_seconds", 30)
        while not self._shutdown.is_set():
            time.sleep(interval)
            self._publish_telemetry()

    def _publish_telemetry(self) -> None:
        base = "SkyFollower/receiver/statistic"

        with self._rmq_lock:
            rmq_connected = self._rmq_connected

        # Build per-source message rates
        stats: dict[str, object] = {}
        for source, tracker in self._rates.items():
            stat_name = f"messages_{source}_per_second"
            stats[stat_name] = round(tracker.rate(), 2)

        stats["local_queue_depth"] = self._fallback.depth()
        stats["rabbitmq_connected"] = "true" if rmq_connected else "false"

        for name, value in stats.items():
            self._mqtt_publish(f"{base}/{name}", value)

    # ------------------------------------------------------------------
    # HA autodiscovery
    # ------------------------------------------------------------------

    def _publish_ha_autodiscovery(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return

        device = {
            "ids": "SkyFollower_receiver",
            "name": "SkyFollower Receiver",
            "manufacturer": "P5Software, LLC",
        }
        availability = {
            "availability_topic": "SkyFollower/receiver/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }

        # Build list of stat sensors from configured sources
        stats = []
        for source in self._rates:
            stat_name = f"messages_{source}_per_second"
            stats.append((
                stat_name,
                f"Messages {source} per Second",
                "mdi:broadcast",
                "measurement",
                "msg/s",
            ))
        stats += [
            ("local_queue_depth", "Local Queue Depth", "mdi:tray-full", "measurement", None),
            ("rabbitmq_connected", "RabbitMQ Connected", "mdi:rabbit", None, None),
        ]

        for name, desc, icon, state_class, unit in stats:
            payload: dict = {
                **availability,
                "state_topic": f"SkyFollower/receiver/statistic/{name}",
                "name": desc,
                "unique_id": f"SkyFollower_receiver_{name}",
                "object_id": f"SkyFollower_receiver_{name}",
                "device": device,
                "icon": icon,
            }
            if state_class:
                payload["state_class"] = state_class
            if unit:
                payload["unit_of_measurement"] = unit

            self._mqtt.publish(
                f"homeassistant/sensor/SkyFollower_receiver_{name}/config",
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
            self._mqtt.publish("SkyFollower/receiver/status", "OFFLINE", retain=True)
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

def _load_config() -> dict:
    path = os.environ.get("SETTINGS_PATH", "/app/settings.json")
    with open(path) as f:
        return json.load(f)


def main() -> None:
    config = _load_config()
    receiver = Receiver(config)

    def _handle_signal(sig, frame):
        receiver.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    receiver.start()


if __name__ == "__main__":
    main()
