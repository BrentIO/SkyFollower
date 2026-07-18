#!/usr/bin/env python3
"""
SkyFollower Archive Processor

Consumes completed flight records from the RabbitMQ 'archive' queue,
builds a 3D GeoJSON LineString with altitude interpolation, writes
gzip-compressed JSON to AWS S3, maintains a local Parquet metadata index
via DuckDB, and falls back to SQLite when S3 is unavailable.
"""

from __future__ import annotations

import gzip
import json
import logging
import logging.handlers
import os
import re
import signal
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import boto3
import duckdb
import paho.mqtt.client as mqtt
import pika
import redis as redis_lib
from botocore.exceptions import BotoCoreError, ClientError

# Add /app to sys.path so shared/ is importable whether running from
# /app/archive-processor or /app.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.models import CompletedFlight
from shared.mqtt import build_mqtt_client
from shared.redis_keys import metrics_flights_archived_key

logger = logging.getLogger("archive-processor")


# ---------------------------------------------------------------------------
# GeoJSON builder
# ---------------------------------------------------------------------------

def _interpolate_altitudes(positions: list[dict]) -> list[Optional[int]]:
    """
    Return a list of altitudes (possibly interpolated) for the given position
    list.  For each position whose altitude is None, linearly interpolate from
    the nearest preceding and following positions that do have an altitude.
    If no surrounding positions have an altitude, leave as None.
    """
    alts: list[Optional[int]] = [p.get("altitude") for p in positions]
    n = len(alts)

    for i in range(n):
        if alts[i] is not None:
            continue
        # Find the previous known altitude
        prev_idx = None
        for j in range(i - 1, -1, -1):
            if alts[j] is not None:
                prev_idx = j
                break
        # Find the next known altitude
        next_idx = None
        for j in range(i + 1, n):
            if alts[j] is not None:
                next_idx = j
                break

        if prev_idx is not None and next_idx is not None:
            # Linear interpolation
            span = next_idx - prev_idx
            frac = (i - prev_idx) / span
            alts[i] = int(round(alts[prev_idx] + frac * (alts[next_idx] - alts[prev_idx])))
        # If only one side is available, leave as None — the coordinate will
        # fall back to 2D.

    return alts


def build_geojson_feature(flight: CompletedFlight) -> Optional[dict]:
    """
    Build a GeoJSON LineString Feature from flight.positions.
    Returns None when there are fewer than 2 positions.
    """
    positions = flight.positions
    if len(positions) < 2:
        return None

    alts = _interpolate_altitudes(positions)

    coordinates = []
    for pos, alt in zip(positions, alts):
        lon = pos.get("longitude") if isinstance(pos, dict) else pos.longitude
        lat = pos.get("latitude") if isinstance(pos, dict) else pos.latitude
        if alt is not None:
            coordinates.append([lon, lat, alt])
        else:
            coordinates.append([lon, lat])

    return {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": coordinates,
        },
        "properties": {},
    }


# ---------------------------------------------------------------------------
# S3 key builder
# ---------------------------------------------------------------------------

_NON_ALNUM_RE = re.compile(r"[^a-zA-Z0-9]")


def build_s3_key(flight: CompletedFlight) -> str:
    """
    Build the S3 object key for a completed flight.
    Format: flights/{YYYY}/{MM}/{DD}/{icao_hex}_{ident}_{uuid}.json.gz
    """
    dt = flight.last_message.astimezone(timezone.utc)
    yyyy = dt.strftime("%Y")
    mm = dt.strftime("%m")
    dd = dt.strftime("%d")

    icao_hex = flight.aircraft.get("icao_hex", "unknown")
    ident_raw = flight.ident or "unknown"
    ident = _NON_ALNUM_RE.sub("", ident_raw) or "unknown"
    uuid = flight.id  # alias for _id field

    return f"flights/{yyyy}/{mm}/{dd}/{icao_hex}_{ident}_{uuid}.json.gz"


# ---------------------------------------------------------------------------
# Parquet index helper
# ---------------------------------------------------------------------------

def append_to_parquet_index(flight: CompletedFlight, s3_key: str, index_path: str) -> None:
    """
    Append a row to the Parquet flight index at index_path.
    Creates the file if it does not exist.
    """
    os.makedirs(os.path.dirname(index_path), exist_ok=True)

    row = {
        "_id": flight.id,
        "icao_hex": flight.aircraft.get("icao_hex", ""),
        "registration": flight.aircraft.get("registration", "") or "",
        "ident": flight.ident or "",
        "first_message": flight.first_message,
        "last_message": flight.last_message,
        "operator_designator": (
            (flight.operator or {}).get("airline_designator", "") or ""
        ),
        "s3_key": s3_key,
    }

    conn = duckdb.connect()

    if os.path.exists(index_path):
        # Read existing data, append new row, write back
        conn.execute(f"CREATE TABLE idx AS SELECT * FROM read_parquet('{index_path}')")
    else:
        conn.execute(
            "CREATE TABLE idx ("
            "  _id VARCHAR,"
            "  icao_hex VARCHAR,"
            "  registration VARCHAR,"
            "  ident VARCHAR,"
            "  first_message TIMESTAMP WITH TIME ZONE,"
            "  last_message TIMESTAMP WITH TIME ZONE,"
            "  operator_designator VARCHAR,"
            "  s3_key VARCHAR"
            ")"
        )

    conn.execute(
        "INSERT INTO idx VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            row["_id"],
            row["icao_hex"],
            row["registration"],
            row["ident"],
            row["first_message"],
            row["last_message"],
            row["operator_designator"],
            row["s3_key"],
        ],
    )
    conn.execute(f"COPY idx TO '{index_path}' (FORMAT PARQUET)")
    conn.close()


# ---------------------------------------------------------------------------
# SQLite fallback queue
# ---------------------------------------------------------------------------

class _S3FallbackQueue:
    """SQLite-backed fallback for completed flights when S3 is unavailable."""

    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS queue "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT, queued_at REAL)"
        )
        self._conn.commit()
        self._lock = threading.Lock()

    def put(self, payload: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO queue (payload, queued_at) VALUES (?, ?)",
                (payload, time.time()),
            )
            self._conn.commit()

    def drain(self, process_fn) -> None:
        """Drain all queued items oldest-first via process_fn(payload)."""
        while True:
            with self._lock:
                cur = self._conn.execute(
                    "SELECT id, payload FROM queue ORDER BY id ASC LIMIT 1"
                )
                row = cur.fetchone()
                if row is None:
                    break
                row_id, payload = row
            try:
                process_fn(payload)
                with self._lock:
                    self._conn.execute("DELETE FROM queue WHERE id=?", (row_id,))
                    self._conn.commit()
            except Exception:
                break  # S3 went away again; stop draining

    def depth(self) -> int:
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) FROM queue")
            return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Archive Processor
# ---------------------------------------------------------------------------

class ArchiveProcessor:

    def __init__(self, config: dict) -> None:
        self._cfg = config
        self._started_at = datetime.now(timezone.utc).isoformat()
        self._shutdown = threading.Event()

        # Paths
        data_dir = config.get("data_dir", "/app/data")
        os.makedirs(data_dir, exist_ok=True)
        self._index_path = os.path.join(data_dir, "flight_index.parquet")
        self._fallback = _S3FallbackQueue(os.path.join(data_dir, "s3.db"))

        # S3
        self._s3_client: Optional[object] = None
        self._s3_connected = False
        self._s3_lock = threading.Lock()

        # Redis
        rc = config["redis"]
        self._redis = redis_lib.Redis(
            host=rc["host"], port=rc.get("port", 6379),
            decode_responses=True,
        )

        # MQTT
        self._mqtt: Optional[mqtt.Client] = None
        self._mqtt_connected = False

        # RabbitMQ
        self._rmq_connection: Optional[pika.BlockingConnection] = None
        self._rmq_channel = None
        self._rmq_connected = False

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._setup_logging()
        self._connect_mqtt()
        self._connect_s3()

        # Background threads
        threading.Thread(target=self._telemetry_loop, daemon=True, name="telemetry").start()
        threading.Thread(target=self._s3_reconnect_loop, daemon=True, name="s3-reconnect").start()

        self._consume_loop()

    def _setup_logging(self) -> None:
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s")
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(fmt)
        logging.getLogger().addHandler(h)
        logging.getLogger().setLevel(logging.INFO)

    # ------------------------------------------------------------------
    # S3
    # ------------------------------------------------------------------

    def _connect_s3(self) -> None:
        s3_cfg = self._cfg.get("s3", {})
        try:
            session = boto3.Session(
                aws_access_key_id=s3_cfg.get("access_key_id"),
                aws_secret_access_key=s3_cfg.get("secret_access_key"),
                region_name=s3_cfg.get("region", "us-east-1"),
            )
            client = session.client("s3")
            # Quick connectivity check
            client.list_buckets()
            with self._s3_lock:
                self._s3_client = client
                self._s3_connected = True
            logger.info("S3 connected.")
        except Exception as exc:
            logger.warning("S3 unavailable: %s. Will retry in background.", exc)
            with self._s3_lock:
                self._s3_connected = False

    def _s3_reconnect_loop(self) -> None:
        """Periodically attempt to reconnect to S3 if disconnected."""
        while not self._shutdown.is_set():
            time.sleep(10)
            with self._s3_lock:
                already_connected = self._s3_connected
            if not already_connected:
                self._connect_s3()
                with self._s3_lock:
                    reconnected = self._s3_connected
                if reconnected:
                    logger.info("S3 reconnected — draining fallback queue.")
                    threading.Thread(
                        target=self._drain_fallback, daemon=True, name="drain-fallback"
                    ).start()

    def _write_to_s3(self, flight: CompletedFlight, payload_bytes: bytes, s3_key: str) -> None:
        s3_cfg = self._cfg.get("s3", {})
        bucket = s3_cfg.get("bucket", "")
        with self._s3_lock:
            client = self._s3_client
        client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=payload_bytes,
            ContentType="application/json",
            ContentEncoding="gzip",
        )

    # ------------------------------------------------------------------
    # RabbitMQ
    # ------------------------------------------------------------------

    def _rmq_params(self) -> pika.ConnectionParameters:
        rc = self._cfg["rabbitmq"]
        creds = pika.PlainCredentials(rc["username"], rc["password"])
        return pika.ConnectionParameters(
            host=rc["host"], port=rc.get("port", 5672),
            credentials=creds, heartbeat=60,
        )

    def _consume_loop(self) -> None:
        """Main loop: connect to RabbitMQ and consume messages until shutdown."""
        while not self._shutdown.is_set():
            try:
                logger.info("Connecting to RabbitMQ (queue: archive)…")
                self._rmq_connection = pika.BlockingConnection(self._rmq_params())
                self._rmq_channel = self._rmq_connection.channel()
                self._rmq_channel.queue_declare(queue="archive", durable=True)
                self._rmq_channel.basic_qos(prefetch_count=1)
                self._rmq_channel.basic_consume(
                    queue="archive",
                    on_message_callback=self._on_message,
                )
                self._rmq_connected = True
                logger.info("RabbitMQ connected, consuming from archive.")

                # Drain fallback queue now that we're connected to RabbitMQ
                # (S3 drain happens separately in s3_reconnect_loop)
                self._rmq_channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as exc:
                self._rmq_connected = False
                logger.warning("RabbitMQ unavailable: %s. Retrying in 10s…", exc)
                time.sleep(10)
            except Exception as exc:
                self._rmq_connected = False
                logger.error("RabbitMQ error: %s. Retrying in 10s…", exc)
                time.sleep(10)

    def _on_message(self, ch, method, props, body: bytes) -> None:
        try:
            flight = CompletedFlight.model_validate_json(body)
        except Exception as exc:
            logger.warning("Unparseable archive message: %s", exc)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            self._process_flight(flight)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            # Don't ack — let the message be re-queued
            logger.error("Failed to process flight %s: %s", flight.id, exc)
            # But to avoid infinite retry loops, fall back locally and ack
            payload = flight.model_dump_json(by_alias=True)
            self._fallback.put(payload)
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_flight(self, flight: CompletedFlight) -> None:
        """Build GeoJSON, write to S3 (or fallback), update Parquet index."""
        # Build enriched payload dict
        payload_dict = flight.model_dump(by_alias=True, mode="json")

        # Add GeoJSON flight_path
        feature = build_geojson_feature(flight)
        if feature is not None:
            payload_dict["flight_path"] = feature

        payload_json = json.dumps(payload_dict, default=str)
        payload_gz = gzip.compress(payload_json.encode("utf-8"))

        s3_key = build_s3_key(flight)

        with self._s3_lock:
            s3_available = self._s3_connected

        if s3_available:
            self._write_to_s3(flight, payload_gz, s3_key)
            self._post_write_success(flight, s3_key)
        else:
            # Queue the raw JSON payload for later retry
            self._fallback.put(flight.model_dump_json(by_alias=True))
            logger.info(
                "S3 unavailable — queued flight %s to local fallback (depth=%d).",
                flight.id, self._fallback.depth(),
            )

    def _drain_fallback(self) -> None:
        """Drain the SQLite fallback queue into S3."""
        def process(payload: str) -> None:
            flight = CompletedFlight.model_validate_json(payload)
            payload_dict = flight.model_dump(by_alias=True, mode="json")
            feature = build_geojson_feature(flight)
            if feature is not None:
                payload_dict["flight_path"] = feature
            payload_json = json.dumps(payload_dict, default=str)
            payload_gz = gzip.compress(payload_json.encode("utf-8"))
            s3_key = build_s3_key(flight)
            self._write_to_s3(flight, payload_gz, s3_key)
            self._post_write_success(flight, s3_key)

        self._fallback.drain(process)
        logger.info("Fallback drain complete. Remaining depth: %d", self._fallback.depth())

    def _post_write_success(self, flight: CompletedFlight, s3_key: str) -> None:
        """After a successful S3 write: update Parquet index and Redis counters."""
        # Parquet index
        try:
            append_to_parquet_index(flight, s3_key, self._index_path)
        except Exception as exc:
            logger.warning("Parquet index update failed for %s: %s", flight.id, exc)

        # Redis counters
        try:
            self._redis.incr(metrics_flights_archived_key("hour"))
            self._redis.incr(metrics_flights_archived_key("today"))
        except Exception as exc:
            logger.warning("Redis counter update failed: %s", exc)

        logger.info("Archived flight %s -> s3://%s", flight.id, s3_key)

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    def _connect_mqtt(self) -> None:
        mc = self._cfg.get("mqtt")
        if not mc:
            return
        self._mqtt = build_mqtt_client(mc, will_topic="SkyFollower/archive/status")
        self._mqtt.on_connect = self._on_mqtt_connect
        self._mqtt.on_disconnect = self._on_mqtt_disconnect
        try:
            self._mqtt.connect_async(mc["host"], port=mc.get("port", 1883), keepalive=60)
            self._mqtt.loop_start()
        except Exception as exc:
            logger.warning("MQTT connect failed: %s", exc)

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties) -> None:
        self._mqtt_connected = True
        client.publish("SkyFollower/archive/status", "ONLINE", retain=True)
        self._publish_ha_autodiscovery()
        # Publish initial stats immediately so HA gets started_at without delay
        self._publish_telemetry()
        logger.info("MQTT connected.")

    def _on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self._mqtt_connected = False

    # ------------------------------------------------------------------
    # HA autodiscovery
    # ------------------------------------------------------------------

    def _publish_ha_autodiscovery(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return
        device = {
            "ids": "SkyFollower_archive",
            "name": "SkyFollower Archive",
            "manufacturer": "P5Software, LLC",
        }
        availability = {
            "availability_topic": "SkyFollower/archive/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }
        stats_topic = "SkyFollower/archive/statistics"
        sensors = [
            ("flights_archived_hour", "Flights Archived (Hour)", "mdi:airplane-landing", "total_increasing", None, "{{ value_json.flights_archived_hour }}"),
            ("flights_archived_today", "Flights Archived (Today)", "mdi:airplane-landing", "total_increasing", None, "{{ value_json.flights_archived_today }}"),
            ("s3_connected", "S3 Connected", "mdi:cloud-check", None, None, "{{ value_json.s3_connected }}"),
            ("local_queue_depth", "Local Queue Depth", "mdi:tray-full", "measurement", None, "{{ value_json.local_queue_depth }}"),
            ("started_at", "Archive Started At", "mdi:clock", None, None, "{{ value_json.started_at }}"),
        ]
        for name, desc, icon, state_class, unit, tmpl in sensors:
            payload: dict = {
                **availability,
                "state_topic": stats_topic,
                "value_template": tmpl,
                "name": desc,
                "unique_id": f"SkyFollower_archive_{name}",
                "object_id": f"SkyFollower_archive_{name}",
                "device": device,
                "icon": icon,
            }
            if state_class:
                payload["state_class"] = state_class
            if unit:
                payload["unit_of_measurement"] = unit
            self._mqtt.publish(
                f"homeassistant/sensor/SkyFollower_archive_{name}/config",
                json.dumps(payload),
                retain=True,
            )

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

        with self._s3_lock:
            s3_connected = self._s3_connected

        payload = {
            "flights_archived_hour": self._redis_counter(
                metrics_flights_archived_key("hour")
            ),
            "flights_archived_today": self._redis_counter(
                metrics_flights_archived_key("today")
            ),
            "s3_connected": s3_connected,
            "local_queue_depth": self._fallback.depth(),
            "started_at": self._started_at,
        }

        self._mqtt.publish(
            "SkyFollower/archive/statistics",
            json.dumps(payload),
            retain=True,
        )

    def _redis_counter(self, key: str) -> int:
        try:
            v = self._redis.get(key)
            return int(v) if v else 0
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        logger.info("Shutdown requested.")
        self._shutdown.set()
        if self._rmq_channel:
            try:
                self._rmq_channel.stop_consuming()
            except Exception:
                pass
        if self._mqtt:
            self._mqtt.publish("SkyFollower/archive/status", "OFFLINE", retain=True)
            self._mqtt.loop_stop()
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
    processor = ArchiveProcessor(config)

    def _handle_signal(sig, frame):
        processor.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    processor.start()


if __name__ == "__main__":
    main()
