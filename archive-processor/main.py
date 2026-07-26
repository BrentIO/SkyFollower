#!/usr/bin/env python3
"""
SkyFollower Archive Processor

Consumes completed flight records from the RabbitMQ 'archive' queue,
builds a 3D GeoJSON LineString with altitude interpolation, writes
gzip-compressed JSON to AWS S3 alongside a per-flight Parquet index row
(queryable via AWS Athena/Glue), and falls back to SQLite when S3 is
unavailable.
"""

from __future__ import annotations

import gzip
import io
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
import paho.mqtt.client as mqtt
import pika
import pyarrow as pa
import pyarrow.parquet as pq
import redis as redis_lib
from botocore.exceptions import BotoCoreError, ClientError

# Add /app to sys.path so shared/ is importable whether running from
# /app/archive-processor or /app.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.models import CompletedFlight
from shared.mqtt import build_mqtt_client
from shared.redis_keys import (
    archive_last_segment_key,
    config_flight_ttl_seconds_key,
    metrics_flights_archived_key,
    metrics_flights_skipped_key,
)

logger = logging.getLogger("archive-processor")

# How long the "last archived segment" pointer for an aircraft is kept in
# Redis before it expires on its own (see _try_stitch / _update_stitch_pointer).
_STITCH_POINTER_TTL_SECONDS = 86400


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
# Split-flight stitching
# ---------------------------------------------------------------------------

def _normalize_timestamps(items: list[dict]) -> list[dict]:
    """
    Parse a previously-archived S3 object's position/velocity timestamp
    strings back into datetime objects, matching the shape a live
    CompletedFlight's positions/velocities already use — so a merged list
    can be sorted uniformly regardless of which segment an item came from.
    """
    out = []
    for item in items:
        item = dict(item)
        ts = item.get("timestamp")
        if isinstance(ts, str):
            item["timestamp"] = datetime.fromisoformat(ts)
        out.append(item)
    return out


def _merge_segments(new_flight: CompletedFlight, prev: dict) -> CompletedFlight:
    """
    Merge a previously-archived flight segment (raw dict, as read back from
    S3) with the newly-completed segment that continues it, producing a
    single CompletedFlight under the *original* segment's _id.
    """
    prev_positions = _normalize_timestamps(prev.get("positions") or [])
    prev_velocities = _normalize_timestamps(prev.get("velocities") or [])

    merged_positions = sorted(
        prev_positions + new_flight.positions, key=lambda p: p["timestamp"]
    )
    merged_velocities = sorted(
        prev_velocities + new_flight.velocities, key=lambda v: v["timestamp"]
    )
    # Union, not concatenation: a rule may have matched independently on
    # both segments (each processor evaluates rules with no knowledge of
    # the other's matches), and the merged record should show it once.
    merged_rules = list(dict.fromkeys(
        (prev.get("matched_rules") or []) + new_flight.matched_rules
    ))

    return new_flight.model_copy(update={
        "id": prev["_id"],
        "first_message": datetime.fromisoformat(prev["first_message"]),
        "total_messages": (prev.get("total_messages") or 0) + new_flight.total_messages,
        "matched_rules": merged_rules,
        "positions": merged_positions,
        "velocities": merged_velocities,
    })


# ---------------------------------------------------------------------------
# S3 key builder
# ---------------------------------------------------------------------------

_NON_ALNUM_RE = re.compile(r"[^a-zA-Z0-9]")


def build_s3_key(flight: CompletedFlight) -> str:
    """
    Build the S3 object key for a completed flight.
    Format: flights/{YYYY}/{MM}/{DD}/{icao_hex}_{ident}_{uuid}.json.gz

    Dated by first_message, not last_message: split-flight stitching
    (_merge_segments) always preserves the *original* segment's
    first_message across every stitch, while last_message keeps advancing
    to whichever segment most recently continued the flight. This key is
    only ever computed once per flight (a stitch overwrites the object in
    place under its original key, never recomputing it) — first_message
    is what keeps that frozen key's date consistent with the value the
    Parquet index (build_index_s3_key, same rationale) recomputes on every
    stitch, even when a stitch happens to straddle a UTC day boundary.
    """
    dt = flight.first_message.astimezone(timezone.utc)
    yyyy = dt.strftime("%Y")
    mm = dt.strftime("%m")
    dd = dt.strftime("%d")

    icao_hex = flight.aircraft.get("icao_hex", "unknown")
    ident_raw = flight.ident or "unknown"
    ident = _NON_ALNUM_RE.sub("", ident_raw) or "unknown"
    uuid = flight.id  # alias for _id field

    return f"flights/{yyyy}/{mm}/{dd}/{icao_hex}_{ident}_{uuid}.json.gz"


# ---------------------------------------------------------------------------
# Parquet index helpers
# ---------------------------------------------------------------------------

_PARQUET_INDEX_SCHEMA = pa.schema([
    pa.field("icao_hex", pa.string()),
    pa.field("registration", pa.string()),
    pa.field("type_designator", pa.string()),
    pa.field("military", pa.bool_(), nullable=False),
    pa.field("operator_designator", pa.string()),
    pa.field("ident", pa.string()),
    pa.field("first_message", pa.timestamp("us", tz="UTC")),
    pa.field("last_message", pa.timestamp("us", tz="UTC")),
    pa.field("s3_key", pa.string()),
])


def build_index_s3_key(flight: CompletedFlight) -> str:
    """
    Build the S3 object key for a completed flight's Parquet index row.
    Format: index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet

    Hive-style partition segments (year=/month=/day=) so Athena partition
    projection can use its default location-template behavior with no
    explicit storage.location.template table property required. Dated by
    first_message, matching build_s3_key() — unlike the flight object's
    key (computed once, then frozen across any later stitch), this index
    row IS rebuilt on every stitch, so it must derive its date from
    something stitching never changes. last_message advances with every
    stitched segment; first_message is always the original segment's,
    invariant across the whole chain (see _merge_segments). Using
    last_message here would silently orphan a stale index row under the
    original day's partition — and create a second, live one elsewhere —
    the moment a stitch happened to straddle a UTC day boundary.
    """
    dt = flight.first_message.astimezone(timezone.utc)
    yyyy = dt.strftime("%Y")
    mm = dt.strftime("%m")
    dd = dt.strftime("%d")
    return f"index/year={yyyy}/month={mm}/day={dd}/{flight.id}.parquet"


def build_parquet_index_row(flight: CompletedFlight, s3_key: str) -> bytes:
    """
    Build the single-row Parquet file (in-memory bytes) for a completed
    flight's index entry. s3_key is the flight object's own key (from
    build_s3_key), copied into the row so a search hit can be resolved to
    its full flight record. Column set/order matches
    specs/data-dictionary.yaml's archive_parquet_index record exactly.
    """
    row = {
        "icao_hex": flight.aircraft.get("icao_hex", "") or "",
        "registration": flight.aircraft.get("registration", "") or "",
        "type_designator": flight.aircraft.get("type_designator", "") or "",
        # The merged aircraft record only ever has military present-and-true
        # or absent (to_completed_flight() strips an explicit False for
        # legacy compatibility) — normalize absent to False here so the
        # column is a clean non-nullable boolean rather than tri-state.
        "military": bool(flight.aircraft.get("military") or False),
        "operator_designator": (flight.operator or {}).get("airline_designator", "") or "",
        "ident": flight.ident or "",
        "first_message": flight.first_message,
        "last_message": flight.last_message,
        "s3_key": s3_key,
    }
    table = pa.Table.from_pylist([row], schema=_PARQUET_INDEX_SCHEMA)
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


# ---------------------------------------------------------------------------
# RabbitMQ queue-depth high-water mark
# ---------------------------------------------------------------------------

class _DepthHWM:
    """Tracks the highest depth recorded since the last read, resetting on
    each read — same reset-on-publish contract as processor/main.py's
    _TimeTracker.hwm_ms_and_reset(). Starts (and resets to) -1 rather than 0
    so "no valid sample landed this window" (e.g. the consumer channel isn't
    up yet, or the sampler hasn't ticked since the last publish) stays
    distinguishable from an observed depth of zero — record()'s max keeps a
    -1 error/no-sample reading from ever overwriting a real one."""

    def __init__(self) -> None:
        self._hwm = -1
        self._lock = threading.Lock()

    def record(self, value: int) -> None:
        with self._lock:
            if value > self._hwm:
                self._hwm = value

    def value_and_reset(self) -> int:
        with self._lock:
            v = self._hwm
            self._hwm = -1
            return v


# ---------------------------------------------------------------------------
# SQLite fallback queue
# ---------------------------------------------------------------------------

class _S3FallbackQueue:
    """
    SQLite-backed fallback queue for anything that needs to survive an S3
    outage and be retried later. table_name lets two independent queues
    (e.g. full flights vs. index-only retries) share one SQLite file
    without colliding — always an internal literal, never user input.
    """

    def __init__(self, path: str, table_name: str = "queue") -> None:
        self._table = table_name
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT, queued_at REAL)"
        )
        self._conn.commit()
        self._lock = threading.Lock()

    def put(self, payload: str) -> None:
        with self._lock:
            self._conn.execute(
                f"INSERT INTO {self._table} (payload, queued_at) VALUES (?, ?)",
                (payload, time.time()),
            )
            self._conn.commit()

    def drain(self, process_fn) -> None:
        """Drain all queued items oldest-first via process_fn(payload)."""
        while True:
            with self._lock:
                cur = self._conn.execute(
                    f"SELECT id, payload FROM {self._table} ORDER BY id ASC LIMIT 1"
                )
                row = cur.fetchone()
                if row is None:
                    break
                row_id, payload = row
            try:
                process_fn(payload)
                with self._lock:
                    self._conn.execute(f"DELETE FROM {self._table} WHERE id=?", (row_id,))
                    self._conn.commit()
            except Exception:
                break  # S3 went away again; stop draining

    def depth(self) -> int:
        with self._lock:
            cur = self._conn.execute(f"SELECT COUNT(*) FROM {self._table}")
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
        s3_db_path = os.path.join(data_dir, "s3.db")
        self._fallback = _S3FallbackQueue(s3_db_path)
        # Separate table, same file: retries for flights whose object write
        # already succeeded but whose Parquet index row failed to write.
        self._index_fallback = _S3FallbackQueue(s3_db_path, table_name="index_queue")

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

        # flight_ttl_seconds: shared Redis config (config:flight_ttl_seconds),
        # read once at startup and cached. Not hot-reloaded; restart to pick
        # up a changed value.
        self._flight_ttl_seconds: int = 300

        # MQTT
        self._mqtt: Optional[mqtt.Client] = None
        self._mqtt_connected = False

        # RabbitMQ
        self._rmq_connection: Optional[pika.BlockingConnection] = None
        self._rmq_channel = None
        self._rmq_connected = False
        self._rmq_queue_depth_hwm = _DepthHWM()

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._setup_logging()
        self._connect_mqtt()
        self._connect_s3()
        self._load_flight_ttl_seconds()

        # Background threads
        threading.Thread(target=self._telemetry_loop, daemon=True, name="telemetry").start()
        threading.Thread(target=self._s3_reconnect_loop, daemon=True, name="s3-reconnect").start()
        threading.Thread(
            target=self._rmq_queue_depth_sampler_loop, daemon=True, name="rmq-depth-sampler"
        ).start()

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
                    logger.info("S3 reconnected — draining fallback queues.")
                    threading.Thread(
                        target=self._drain_all_fallbacks, daemon=True, name="drain-fallback"
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

    def _write_index_to_s3(self, payload_bytes: bytes, s3_key: str) -> None:
        s3_cfg = self._cfg.get("s3", {})
        bucket = s3_cfg.get("bucket", "")
        with self._s3_lock:
            client = self._s3_client
        client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=payload_bytes,
            ContentType="application/octet-stream",
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

    def _rmq_queue_depth(self) -> int:
        """Best-effort depth of the 'archive' queue via passive declare on
        the existing consumer channel. Returns -1 on any error (no channel
        yet, or the declare itself fails)."""
        if not self._rmq_channel:
            return -1
        try:
            result = self._rmq_channel.queue_declare(
                queue="archive", durable=True, passive=True
            )
            return result.method.message_count
        except Exception:
            return -1

    def _rmq_queue_depth_sampler_loop(self) -> None:
        """Samples the archive queue's depth at most once every 10 seconds,
        independent of telemetry_interval_seconds, feeding the HWM tracker
        that _publish_telemetry() reads and resets each tick."""
        while not self._shutdown.is_set():
            time.sleep(10)
            self._rmq_queue_depth_hwm.record(self._rmq_queue_depth())

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
        """Write to S3 (or fallback) if S3 is currently reachable.

        MLAT-only flights are dropped here, before either the S3
        write or the local fallback queue — deliberately, not deferred, since
        the whole point is avoiding the S3 storage cost of flights the user
        never asked to keep. force_archive (set by a matching rule) overrides
        the drop for MLAT-only flights the user does care about.
        """
        if set(flight.receiver_sources) == {"MLAT"} and not flight.force_archive:
            try:
                self._redis.incr(metrics_flights_skipped_key("hour"))
                self._redis.incr(metrics_flights_skipped_key("today"))
            except Exception as exc:
                logger.warning("Redis counter update failed: %s", exc)
            logger.info("Skipped MLAT-only flight %s (no force_archive match).", flight.id)
            return

        with self._s3_lock:
            s3_available = self._s3_connected

        if s3_available:
            self._archive_flight_to_s3(flight)
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
            self._archive_flight_to_s3(flight)

        self._fallback.drain(process)
        logger.info("Fallback drain complete. Remaining depth: %d", self._fallback.depth())

    def _drain_all_fallbacks(self) -> None:
        """
        Drain both fallback queues. Both get the same two triggers (S3
        reconnect and every telemetry tick) even though only the index
        queue strictly needs the periodic one — the flight queue only
        ever fills while S3 is known to be down, so the reconnect loop's
        edge-triggered detection is sufficient for it in theory. But a
        periodic sweep is a strictly stronger guarantee for near-zero
        extra cost (an empty-queue check when there's nothing to drain),
        so both queues get both triggers rather than leaving the flight
        queue with the weaker one.
        """
        self._drain_fallback()
        self._drain_index_fallback()

    def _drain_index_fallback(self) -> None:
        """
        Retry Parquet index writes for flights whose object write already
        succeeded but whose index row failed. Only rebuilds/rewrites the
        index row — never re-archives the flight object itself.
        """
        def process(payload: str) -> None:
            data = json.loads(payload)
            flight = CompletedFlight.model_validate_json(data["flight_json"])
            s3_key = data["s3_key"]
            index_key = build_index_s3_key(flight)
            index_bytes = build_parquet_index_row(flight, s3_key)
            self._write_index_to_s3(index_bytes, index_key)

        self._index_fallback.drain(process)
        logger.info(
            "Index-fallback drain complete. Remaining depth: %d", self._index_fallback.depth()
        )

    def _archive_flight_to_s3(self, flight: CompletedFlight) -> None:
        """
        Check whether this flight continues a recently-archived segment for
        the same aircraft (a processor-count resize can force an early
        archive mid-flight); if so, merge into that segment instead of
        writing a second S3 object. Otherwise build GeoJSON and write
        normally. Assumes S3 is reachable — raises on failure so the caller
        can decide fallback handling.
        """
        s3_key = build_s3_key(flight)
        stitched = self._try_stitch(flight)
        if stitched is not None:
            flight, s3_key = stitched

        payload_dict = flight.model_dump(by_alias=True, mode="json")
        feature = build_geojson_feature(flight)
        if feature is not None:
            payload_dict["flight_path"] = feature
        payload_json = json.dumps(payload_dict, default=str)
        payload_gz = gzip.compress(payload_json.encode("utf-8"))

        self._write_to_s3(flight, payload_gz, s3_key)
        self._post_write_success(flight, s3_key)
        self._update_stitch_pointer(flight, s3_key)

    # ------------------------------------------------------------------
    # Split-flight stitching
    # ------------------------------------------------------------------

    def _try_stitch(self, flight: CompletedFlight) -> Optional[tuple[CompletedFlight, str]]:
        """
        If this flight's start is within flight_ttl_seconds of the last
        archived segment for the same aircraft, fetch that segment and
        merge. Returns (merged_flight, original_s3_key), or None if this is
        a genuinely new flight (no prior pointer, gap too large, or the
        prior segment couldn't be fetched).
        """
        icao_hex = flight.aircraft.get("icao_hex", "")
        if not icao_hex:
            return None

        try:
            raw = self._redis.get(archive_last_segment_key(icao_hex))
        except Exception as exc:
            logger.warning("Stitch pointer lookup failed for %s: %s", icao_hex, exc)
            return None
        if not raw:
            return None

        try:
            pointer = json.loads(raw)
            prev_last_message = float(pointer["last_message"])
            prev_s3_key = pointer["s3_key"]
        except (ValueError, KeyError, TypeError):
            return None

        ttl = self._flight_ttl_seconds
        gap = flight.first_message.timestamp() - prev_last_message
        if gap > ttl:
            return None

        prev = self._fetch_previous_segment(prev_s3_key)
        if prev is None:
            return None

        return _merge_segments(flight, prev), prev_s3_key

    def _fetch_previous_segment(self, s3_key: str) -> Optional[dict]:
        """Fetch and parse a previously-archived S3 object. None on failure —
        a fetch failure just means this is treated as a new flight instead."""
        s3_cfg = self._cfg.get("s3", {})
        bucket = s3_cfg.get("bucket", "")
        with self._s3_lock:
            client = self._s3_client
        try:
            obj = client.get_object(Bucket=bucket, Key=s3_key)
            return json.loads(gzip.decompress(obj["Body"].read()))
        except Exception as exc:
            logger.warning(
                "Failed to fetch previous segment %s for stitching: %s", s3_key, exc
            )
            return None

    def _update_stitch_pointer(self, flight: CompletedFlight, s3_key: str) -> None:
        icao_hex = flight.aircraft.get("icao_hex", "")
        if not icao_hex:
            return
        pointer = {
            "uuid": flight.id,
            "first_message": flight.first_message.timestamp(),
            "last_message": flight.last_message.timestamp(),
            "s3_key": s3_key,
        }
        try:
            self._redis.set(
                archive_last_segment_key(icao_hex),
                json.dumps(pointer),
                ex=_STITCH_POINTER_TTL_SECONDS,
            )
        except Exception as exc:
            logger.warning("Failed to update stitch pointer for %s: %s", icao_hex, exc)

    def _post_write_success(self, flight: CompletedFlight, s3_key: str) -> None:
        """After a successful S3 write: write the Parquet index row and update Redis counters."""
        # Parquet index row — best-effort: never blocks archiving (the
        # flight object above already succeeded), and a failure here queues
        # a retry rather than re-archiving the whole flight.
        try:
            index_key = build_index_s3_key(flight)
            index_bytes = build_parquet_index_row(flight, s3_key)
            self._write_index_to_s3(index_bytes, index_key)
        except Exception as exc:
            logger.warning(
                "Parquet index write failed for %s: %s — queued for retry", flight.id, exc
            )
            try:
                self._index_fallback.put(json.dumps({
                    "flight_json": flight.model_dump_json(by_alias=True),
                    "s3_key": s3_key,
                }))
            except Exception as queue_exc:
                logger.warning("Failed to queue index retry for %s: %s", flight.id, queue_exc)

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
        base = "SkyFollower/archive/statistic"
        sensors = [
            ("flights_archived_hour", "Flights Archived (Hour)", "mdi:airplane-landing", "total_increasing", None),
            ("flights_archived_today", "Flights Archived (Today)", "mdi:airplane-landing", "total_increasing", None),
            ("flights_skipped_hour", "Flights Skipped MLAT-Only (Hour)", "mdi:airplane-off", "total_increasing", None),
            ("flights_skipped_today", "Flights Skipped MLAT-Only (Today)", "mdi:airplane-off", "total_increasing", None),
            ("s3_connected", "S3 Connected", "mdi:cloud-check", None, None),
            ("local_queue_depth", "Local Queue Depth", "mdi:tray-full", "measurement", None),
            ("local_index_queue_depth", "Local Index Queue Depth", "mdi:tray-full", "measurement", None),
            ("rabbitmq_archive_queue_depth_hwm", "RabbitMQ Archive Queue Depth HWM", "mdi:tray-full", "measurement", None),
            ("started_at", "Archive Started At", "mdi:clock", None, None),
        ]
        for name, desc, icon, state_class, unit in sensors:
            payload: dict = {
                **availability,
                "state_topic": f"{base}/{name}",
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
            # Independent of MQTT/_publish_telemetry below: a periodic
            # sweep of both fallback queues, not just a reaction to
            # _s3_reconnect_loop's edge-triggered "was down, now up"
            # detection — see _drain_all_fallbacks for why both queues
            # get this even though only the index queue strictly needs it.
            with self._s3_lock:
                s3_connected = self._s3_connected
            if s3_connected:
                self._drain_all_fallbacks()
            self._publish_telemetry()

    def _publish_telemetry(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return

        with self._s3_lock:
            s3_connected = self._s3_connected

        base = "SkyFollower/archive/statistic"

        self._mqtt.publish(
            f"{base}/flights_archived_hour",
            str(self._redis_counter(metrics_flights_archived_key("hour"))),
            retain=True,
        )
        self._mqtt.publish(
            f"{base}/flights_archived_today",
            str(self._redis_counter(metrics_flights_archived_key("today"))),
            retain=True,
        )
        self._mqtt.publish(
            f"{base}/flights_skipped_hour",
            str(self._redis_counter(metrics_flights_skipped_key("hour"))),
            retain=True,
        )
        self._mqtt.publish(
            f"{base}/flights_skipped_today",
            str(self._redis_counter(metrics_flights_skipped_key("today"))),
            retain=True,
        )
        self._mqtt.publish(f"{base}/s3_connected", str(s3_connected), retain=True)
        self._mqtt.publish(f"{base}/local_queue_depth", str(self._fallback.depth()), retain=True)
        self._mqtt.publish(
            f"{base}/local_index_queue_depth", str(self._index_fallback.depth()), retain=True
        )
        self._mqtt.publish(
            f"{base}/rabbitmq_archive_queue_depth_hwm",
            str(self._rmq_queue_depth_hwm.value_and_reset()),
            retain=True,
        )
        self._mqtt.publish(f"{base}/started_at", self._started_at, retain=True)

    def _redis_counter(self, key: str) -> int:
        try:
            v = self._redis.get(key)
            return int(v) if v else 0
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    def _load_flight_ttl_seconds(self) -> None:
        """Read flight_ttl_seconds from Redis once at startup. Not
        hot-reloaded — restart the container to pick up a changed value.
        Leaves the default in place if Redis is unreachable at startup."""
        try:
            raw = self._redis.get(config_flight_ttl_seconds_key())
            if raw is not None:
                self._flight_ttl_seconds = int(raw)
        except Exception as exc:
            logger.debug("flight_ttl_seconds load error: %s", exc)

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
