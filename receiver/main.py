#!/usr/bin/env python3
"""
SkyFollower Receiver

Connects to one or more TCP sources — readsb (1090 MHz Mode S / MLAT) or
dump978-fa (978 MHz UAT) — extracts each message's ICAO hex (via pyModeS for
1090/MLAT, directly from the UAT payload for 978), and routes it to the
appropriate RabbitMQ queue based on a modulo-bucketing scheme.  Falls back to
a local SQLite queue when RabbitMQ is unavailable, and drains the fallback on
reconnect.

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

from shared.adsb_1090 import parse_tcp_stream
from shared.models import InboundMessage
from shared.mqtt import build_mqtt_client
from shared.uat import parse_978_line

logger = logging.getLogger("receiver")

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

    def __init__(self, config: dict, receiver_id: int = 0) -> None:
        self._cfg = config
        self._id = receiver_id
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
                    if source == "978":
                        self._read_978_stream(sock, host, port, source, rate_tracker)
                    else:
                        self._read_1090_stream(sock, host, port, source, rate_tracker)

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

    def _read_1090_stream(
        self, sock: socket.socket, host: str, port: int, source: str, rate_tracker: _RateTracker
    ) -> None:
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

    def _read_978_stream(
        self, sock: socket.socket, host: str, port: int, source: str, rate_tracker: _RateTracker
    ) -> None:
        line_buf = b""
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

            line_buf += chunk
            while b"\n" in line_buf:
                raw_line, line_buf = line_buf.split(b"\n", 1)
                result = parse_978_line(raw_line.decode("ascii", errors="ignore"))
                if result:
                    raw_hex, icao_hex, received_at = result
                    self._handle_978_message(raw_hex, icao_hex, received_at, source, rate_tracker)

    def _handle_message(
        self, raw_hex: str, source: str, rate_tracker: _RateTracker
    ) -> None:
        """Extract ICAO from a 1090 Mode S message, then route it."""
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
    ) -> None:
        """Route an already-parsed 978 UAT message (icao_hex/received_at
        extracted by parse_978_line — no pyModeS decode needed, UAT is not
        Mode S)."""
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
        """Compute the target queue, build the InboundMessage envelope, and publish."""
        processor_count = self._cfg.get("processor_count", 1)
        queue_name = f"adsb-{int(icao_hex, 16) % processor_count}"

        msg = InboundMessage(
            raw=raw,
            icao_hex=icao_hex,
            received_at=received_at,
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
            self._publish_telemetry()

    def _publish_telemetry(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return

        with self._rmq_lock:
            rmq_connected = self._rmq_connected

        payload: dict = {"started_at": self._started_at}
        for source, tracker in self._rates.items():
            payload[f"messages_{source}_per_second"] = round(tracker.rate(), 2)
        payload["local_queue_depth"] = self._fallback.depth()
        payload["rabbitmq_connected"] = rmq_connected

        self._mqtt.publish(
            f"SkyFollower/receiver/{self._id}/statistics",
            json.dumps(payload),
            retain=True,
        )

    # ------------------------------------------------------------------
    # HA autodiscovery
    # ------------------------------------------------------------------

    def _publish_ha_autodiscovery(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return

        rid = self._id
        stats_topic = f"SkyFollower/receiver/{rid}/statistics"
        device = {
            "ids": f"SkyFollower_receiver_{rid}",
            "name": f"SkyFollower Receiver {rid}",
            "manufacturer": "P5Software, LLC",
        }
        availability = {
            "availability_topic": f"SkyFollower/receiver/{rid}/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }

        sensors = []
        for source in self._rates:
            field = f"messages_{source}_per_second"
            sensors.append((field, f"Receiver {rid} — {source} Messages/sec",
                             "mdi:broadcast", "measurement", "msg/s"))
        sensors += [
            ("local_queue_depth", f"Receiver {rid} — Local Queue Depth",
             "mdi:tray-full", "measurement", None),
            ("rabbitmq_connected", f"Receiver {rid} — RabbitMQ Connected",
             "mdi:rabbit", None, None),
        ]

        for field, desc, icon, state_class, unit in sensors:
            payload: dict = {
                **availability,
                "state_topic": stats_topic,
                "value_template": f"{{{{ value_json.{field} }}}}",
                "name": desc,
                "unique_id": f"SkyFollower_receiver_{rid}_{field}",
                "object_id": f"SkyFollower_receiver_{rid}_{field}",
                "device": device,
                "icon": icon,
            }
            if state_class:
                payload["state_class"] = state_class
            if unit:
                payload["unit_of_measurement"] = unit

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

def _load_config() -> dict:
    path = os.environ.get("SETTINGS_PATH", "/app/settings.json")
    with open(path) as f:
        return json.load(f)


def main() -> None:
    receiver_id_str = os.environ.get("RECEIVER_ID", "0")
    try:
        receiver_id = int(receiver_id_str)
    except ValueError:
        print(f"RECEIVER_ID must be an integer, got: {receiver_id_str!r}", file=sys.stderr)
        sys.exit(1)
    config = _load_config()
    receiver = Receiver(config, receiver_id)

    def _handle_signal(sig, frame):
        receiver.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    receiver.start()


if __name__ == "__main__":
    main()
