#!/usr/bin/env python3
"""
SkyFollower Archive Processor

Consumes completed flight records from the RabbitMQ 'skyfollower-archive'
queue, writes gzip-compressed JSON to AWS S3 alongside a per-flight Parquet
index row (queryable via AWS Athena/Glue), and falls back to SQLite when S3
is unavailable.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import logging.handlers
import os
import pathlib
import queue
import signal
import sys
import threading
import time
import zlib
from datetime import datetime, timezone
from typing import Optional

import boto3
import paho.mqtt.client as mqtt
import pika
import pyarrow as pa
import pyarrow.parquet as pq
import redis as redis_lib
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

# Add /app to sys.path so shared/ is importable whether running from
# /app/archive-processor or /app.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.config import DATA_DIR, ConfigError, load_config
from shared.index_cache import INDEX_CACHE_DIR, write_local_index
from shared.metrics import next_period_boundary
from shared.redis_client import build_redis_client
from shared.fallback_queue import FallbackQueue
from shared.ha_discovery import build_ha_device
from shared.models import CompletedFlight
from shared.mqtt import build_mqtt_client
from shared.rabbitmq_topology import ARCHIVE_QUEUE_NAME
from shared.redis_keys import (
    archive_last_segment_key,
    config_flight_ttl_seconds_key,
    metrics_flights_archived_key,
    metrics_flights_skipped_key,
)
from shared.timing import (
    DEFAULT_FLIGHT_TTL_SECONDS,
    HEALTHCHECK_INTERVAL_SECONDS,
    MQTT_PUBLISH_INTERVAL_SECONDS,
    RECONNECT_BACKOFF_SECONDS,
    STITCH_POINTER_TTL_SECONDS,
)

logger = logging.getLogger("archive-processor")

# tmpfs-mounted in docker-compose.archive.yaml -- these writes must never
# hit the host's storage, only /app/data (the S3 fallback queue) is
# durable/persistent. Every timing value the archive processor uses is a
# named constant from shared/timing.py, imported above (the
# "last archived segment" stitch pointer TTL is STITCH_POINTER_TTL_SECONDS).
_HEALTHCHECK_HEARTBEAT_PATH = "/app/health/heartbeat"

# Each completed flight is written to S3 independently with no shared
# mutable state between flights, so -- unlike message-processor's
# consistent-hash-exchange affinity concerns -- there's no fair-dispatch or
# ordering reason to keep this at 1. A prefetch_count of 1 forces a full
# ack round trip before RabbitMQ delivers the next message, capping
# throughput at the connection's round-trip latency rather than actual
# processing capacity. 100 matches message-processor's
# _RMQ_PREFETCH_COUNT precedent: enough to remove the round-trip stall as
# the throughput ceiling, without buffering an excessive number of
# messages client-side that would need reprocessing if the connection
# drops mid-batch. It also bounds total in-flight work across the worker
# pool below (RabbitMQ delivers at most this many unacked messages, so the
# per-worker hand-off queues can never grow past it in aggregate).
_RMQ_PREFETCH_COUNT = 100

# A single pika BlockingConnection runs on_message_callback one message at
# a time on one thread, and each flight does two or more sequential
# synchronous S3 round trips (~150-250 ms each from a self-hosted host) --
# so serial processing caps throughput at ~3-5 flights/sec regardless of
# prefetch_count. The pika thread instead hands each delivery to a pool of
# worker threads that do the S3/Redis work concurrently and marshal the
# ack back via connection.add_callback_threadsafe.
#
# Partitioned by icao_hex (same aircraft -> same worker, FIFO): split-flight
# stitching reads and writes a per-aircraft Redis pointer around the S3
# write, so two segments for one aircraft must never be processed
# concurrently or the stitch can split/merge out of order. Routing by
# icao_hex keeps every segment of an aircraft strictly ordered on one
# worker without any per-key locking.
_ARCHIVE_WORKER_COUNT = 12

# boto3's default connection pool is 10; size it to cover every worker
# doing a concurrent PutObject/GetObject plus headroom for the
# reconnect-loop and index-fallback-drain threads.
_S3_MAX_POOL_CONNECTIONS = _ARCHIVE_WORKER_COUNT + 4


# ---------------------------------------------------------------------------
# Split-flight stitching
# ---------------------------------------------------------------------------

def _normalize_timestamps(items: list[dict]) -> list[dict]:
    """
    Parse a position/velocity list's timestamp values back into datetime
    objects wherever they're still strings, so a merged list can be sorted
    uniformly regardless of which segment (or serialization round-trip) an
    item came from.

    Needed on *both* sides of a merge, not just the previously-archived
    segment fetched from S3: CompletedFlight.positions/.velocities are
    untyped `list[dict]` fields (see shared/models.py), so pydantic never
    re-parses their inner "timestamp" strings back into datetime objects on
    model_validate_json() -- meaning even a live flight fresh off RabbitMQ
    (or one that round-tripped through the local SQLite fallback queue) has
    string timestamps by the time it reaches here, same as the S3-fetched
    segment. Skipping this on the live segment previously crashed every
    real stitch attempt with positions/velocities present (comparing a
    normalized datetime against a raw str during the sort below).
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
    new_positions = _normalize_timestamps(new_flight.positions)
    new_velocities = _normalize_timestamps(new_flight.velocities)

    merged_positions = sorted(
        prev_positions + new_positions, key=lambda p: p["timestamp"]
    )
    merged_velocities = sorted(
        prev_velocities + new_velocities, key=lambda v: v["timestamp"]
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

def build_s3_key(flight: CompletedFlight) -> str:
    """
    Build the S3 object key for a completed flight.
    Format: flights/{YYYY}/{MM}/{DD}/{uuid}.json.gz

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

    uuid = flight.id  # alias for _id field

    return f"flights/{yyyy}/{mm}/{dd}/{uuid}.json.gz"


def _drop_default_value_fields(payload_dict: dict) -> None:
    """
    Drop force_archive/matched_rules from an S3 upload payload dict when
    they carry no information -- force_archive False (the overwhelming
    default) and matched_rules [] (no rule matched) are both the common
    case, and persisting them on every flight is pure bloat. Only affects
    the S3 upload payload; the in-memory CompletedFlight model and every
    other consumer (RabbitMQ fallback queues, the Parquet index) are
    unaffected.
    """
    if payload_dict.get("force_archive") is False:
        del payload_dict["force_archive"]
    if payload_dict.get("matched_rules") == []:
        del payload_dict["matched_rules"]


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
# Archive Processor
# ---------------------------------------------------------------------------

class ArchiveProcessor:

    def __init__(self, config: dict) -> None:
        self._cfg = config
        self._started_at = datetime.now(timezone.utc).isoformat()
        self._version = os.environ.get("VERSION", "dev")
        self._shutdown = threading.Event()

        # Paths
        os.makedirs(DATA_DIR, exist_ok=True)
        s3_db_path = os.path.join(DATA_DIR, "s3.db")
        self._fallback = FallbackQueue(s3_db_path)
        # Separate table, same file: retries for flights whose object write
        # already succeeded but whose Parquet index row failed to write.
        self._index_fallback = FallbackQueue(s3_db_path, table_name="index_queue")

        # S3
        self._s3_client: Optional[object] = None
        self._s3_connected = False
        self._s3_lock = threading.Lock()

        # Redis
        rc = config["redis"]
        self._redis = build_redis_client(rc)
        _incr_lua_path = (
            pathlib.Path(__file__).parent.parent / "shared" / "lua" / "incr_period_counter.lua"
        )
        self._incr_period_counter_sha = self._redis.script_load(_incr_lua_path.read_text())

        # flight_ttl_seconds: shared Redis config (config:flight_ttl_seconds),
        # read once at startup and cached. Not hot-reloaded; restart to pick
        # up a changed value.
        self._flight_ttl_seconds: int = DEFAULT_FLIGHT_TTL_SECONDS

        # MQTT
        self._mqtt: Optional[mqtt.Client] = None
        self._mqtt_connected = False

        # RabbitMQ
        self._rmq_connection: Optional[pika.BlockingConnection] = None
        self._rmq_channel = None
        self._rmq_connected = False

        # Worker pool for the live consume path. One unbounded hand-off
        # queue per worker; _on_message routes each delivery to
        # worker_queues[crc32(icao_hex) % N] so an aircraft's segments are
        # always processed FIFO on a single worker (see _ARCHIVE_WORKER_COUNT).
        # Aggregate depth is bounded by _RMQ_PREFETCH_COUNT. The fallback
        # drain path does NOT use this pool -- it stays strictly serial and
        # oldest-first (see _finish_s3_connect / _process_fallback_flight).
        self._worker_queues: list[queue.Queue] = [
            queue.Queue() for _ in range(_ARCHIVE_WORKER_COUNT)
        ]
        self._worker_threads: list[threading.Thread] = []

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._setup_logging()
        logger.info(f"Starting SkyFollower Archive {self._version}")
        self._connect_mqtt()
        if self._connect_s3():
            self._finish_s3_connect()
        self._load_flight_ttl_seconds()

        # Background threads
        threading.Thread(target=self._telemetry_loop, daemon=True, name="telemetry").start()
        threading.Thread(target=self._s3_reconnect_loop, daemon=True, name="s3-reconnect").start()
        threading.Thread(target=self._healthcheck_loop, daemon=True, name="healthcheck").start()

        # Live-path worker pool (see _ARCHIVE_WORKER_COUNT / _worker_loop).
        for i in range(_ARCHIVE_WORKER_COUNT):
            t = threading.Thread(
                target=self._worker_loop, args=(i,), daemon=True, name=f"archive-worker-{i}"
            )
            t.start()
            self._worker_threads.append(t)

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

    def _connect_s3(self) -> bool:
        """Establishes (or re-establishes) the S3 client. Returns whether
        the connectivity check succeeded. Deliberately does NOT set
        _s3_connected itself -- callers decide when it's actually safe to
        route live flights directly to S3, via _finish_s3_connect."""
        s3_cfg = self._cfg.get("s3", {})
        try:
            # No credential arguments: boto3 reads AWS_ACCESS_KEY_ID,
            # AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION from its own
            # default credential chain, which an instance role can also
            # satisfy.
            client = boto3.Session().client(
                "s3",
                config=BotoConfig(max_pool_connections=_S3_MAX_POOL_CONNECTIONS),
            )
            # Quick connectivity check, scoped to just this bucket
            client.head_bucket(Bucket=s3_cfg.get("bucket", ""))
            with self._s3_lock:
                self._s3_client = client
            logger.info("S3 client connected.")
            return True
        except Exception as exc:
            logger.warning("S3 unavailable: %s. Will retry in background.", exc)
            with self._s3_lock:
                self._s3_connected = False
            return False

    def _finish_s3_connect(self) -> None:
        """Called after a successful _connect_s3(): synchronously drains
        the flight-fallback queue to empty *before* allowing _process_flight
        to route any live flight directly to S3.

        Without this gate, a continuation segment for an aircraft whose
        prior segment is still sitting in the fallback queue could be
        live-processed (and miss its _try_stitch() pointer lookup, since
        the prior segment hasn't been written/pointer-updated yet) before
        the background drain gets around to that prior segment -- splitting
        one flight into two archived records.

        Any flight arriving on the RabbitMQ consumer thread while this
        drain is running still sees s3_available=False (this method hasn't
        returned yet), so it queues to the *same* fallback queue rather
        than going live -- fast, non-blocking (a local SQLite insert), no
        RabbitMQ backpressure. Since FallbackQueue.drain() is strictly
        oldest-first, a continuation segment can never be drained (and
        therefore never reach _try_stitch()) before the segment it
        continues. No per-icao_hex locking or queue scanning needed -- the
        single queue's own ordering does the work.

        If the drain stops early (S3 went away again mid-drain), _s3_connected
        is left False and the normal 10s reconnect-loop retry cadence picks
        the whole sequence -- reconnect, then this drain again -- back up
        later, continuing from wherever the queue was left.

        The index-fallback queue doesn't participate in the stitch race (it
        only retries a Parquet index row for a flight object already
        successfully written) so it keeps draining in the background as
        before, not gated on this.
        """
        if not self._fallback.drain(self._process_fallback_flight):
            return
        with self._s3_lock:
            self._s3_connected = True
        logger.info("S3 connected — flight fallback queue fully drained.")
        self._drain_index_fallback()

    def _process_fallback_flight(self, payload: str) -> None:
        flight = CompletedFlight.model_validate_json(payload)
        self._archive_flight_to_s3(flight)

    def _s3_reconnect_loop(self) -> None:
        """Periodically attempt to reconnect to S3 if disconnected."""
        while not self._shutdown.is_set():
            time.sleep(RECONNECT_BACKOFF_SECONDS)
            with self._s3_lock:
                already_connected = self._s3_connected
            if not already_connected and self._connect_s3():
                self._finish_s3_connect()

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

    def _write_local_index_cache(self, index_key: str, index_bytes: bytes) -> None:
        """
        Best-effort local copy of a Parquet index row already durably
        written to S3 above, on the volume shared with archive-compaction
        (see docker-compose.archive.yaml). Lets archive-compaction read the
        row back from local disk instead of downloading it again days
        later. A failure here costs archive-compaction one S3 GetObject
        call for this specific row when it eventually compacts — it never
        affects archiving itself and never triggers a retry.
        """
        try:
            write_local_index(index_key, index_bytes, base_dir=INDEX_CACHE_DIR)
        except Exception as exc:
            logger.warning("Local index cache write failed for %s: %s", index_key, exc)

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
                logger.info(
                    "Connecting to RabbitMQ (queue: %s)…", ARCHIVE_QUEUE_NAME
                )
                self._rmq_connection = pika.BlockingConnection(self._rmq_params())
                self._rmq_channel = self._rmq_connection.channel()
                self._rmq_channel.queue_declare(
                    queue=ARCHIVE_QUEUE_NAME, durable=True
                )
                self._rmq_channel.basic_qos(prefetch_count=_RMQ_PREFETCH_COUNT)
                self._rmq_channel.basic_consume(
                    queue=ARCHIVE_QUEUE_NAME,
                    on_message_callback=self._on_message,
                )
                self._rmq_connected = True
                logger.info(
                    "RabbitMQ connected, consuming from %s.", ARCHIVE_QUEUE_NAME
                )

                # Drain fallback queue now that we're connected to RabbitMQ
                # (S3 drain happens separately in s3_reconnect_loop)
                self._rmq_channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as exc:
                self._rmq_connected = False
                logger.warning(
                    "RabbitMQ unavailable: %s. Retrying in %ss…",
                    exc, RECONNECT_BACKOFF_SECONDS,
                )
                time.sleep(RECONNECT_BACKOFF_SECONDS)
            except Exception as exc:
                self._rmq_connected = False
                logger.error(
                    "RabbitMQ error: %s. Retrying in %ss…",
                    exc, RECONNECT_BACKOFF_SECONDS,
                )
                time.sleep(RECONNECT_BACKOFF_SECONDS)

    def _worker_index_for(self, flight: CompletedFlight) -> int:
        """Which worker owns this aircraft. Deterministic (crc32, not the
        hash-randomised built-in hash()) so the mapping is stable for the
        life of the process. Flights with no icao_hex can't be stitched
        anyway (_try_stitch returns early), so they all landing on worker 0
        is harmless."""
        icao_hex = (flight.aircraft.get("icao_hex", "") or "").encode("utf-8")
        return zlib.crc32(icao_hex) % _ARCHIVE_WORKER_COUNT

    def _on_message(self, ch, method, props, body: bytes) -> None:
        """Runs on the pika connection thread. Parse here (an unparseable
        message is acked and dropped immediately, as before), then hand the
        flight to its owning worker and return -- the worker does the S3/Redis
        work and schedules the ack back on this thread."""
        try:
            flight = CompletedFlight.model_validate_json(body)
        except Exception as exc:
            logger.warning("Unparseable archive message: %s", exc)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        self._worker_queues[self._worker_index_for(flight)].put(
            (flight, method.delivery_tag)
        )

    def _worker_loop(self, idx: int) -> None:
        """One live-path worker. Pulls (flight, delivery_tag) off its own
        hand-off queue and processes serially via _handle_delivery, so
        every segment of a given aircraft (all routed here by
        _worker_index_for) stays strictly ordered without locking."""
        q = self._worker_queues[idx]
        while not self._shutdown.is_set():
            try:
                item = q.get(timeout=1.0)
            except queue.Empty:
                continue
            if item is None:  # shutdown sentinel
                return
            flight, delivery_tag = item
            self._handle_delivery(flight, delivery_tag)

    def _handle_delivery(self, flight: CompletedFlight, delivery_tag: int) -> None:
        """Process one flight and ack it. Same per-message contract as the
        old serial _on_message: on success ack; on a processing error fall
        the flight to the local queue and ack; if even the fallback put
        fails, don't ack so RabbitMQ redelivers."""
        try:
            self._process_flight(flight)
            self._ack(delivery_tag)
        except Exception as exc:
            logger.error("Failed to process flight %s: %s", flight.id, exc)
            try:
                self._fallback.put(
                    flight.model_dump_json(by_alias=True, exclude_none=True)
                )
            except Exception as queue_exc:
                logger.error(
                    "Failed to queue flight %s to local fallback: %s — "
                    "leaving unacked for redelivery", flight.id, queue_exc,
                )
                return
            self._ack(delivery_tag)

    def _ack(self, delivery_tag: int) -> None:
        """Marshal a basic_ack back onto the pika connection thread --
        self._rmq_channel must only ever be touched there. If the channel
        has been replaced by a reconnect since delivery, the ack fails
        harmlessly and RabbitMQ redelivers the message (S3 keys are derived
        from the flight's stable UUID, so a re-archive overwrites rather
        than duplicates)."""
        conn = self._rmq_connection
        if conn is None:
            return

        def _ack_on_rmq_thread() -> None:
            try:
                self._rmq_channel.basic_ack(delivery_tag=delivery_tag)
            except Exception as exc:
                logger.warning(
                    "Ack for delivery_tag %s failed (likely a reconnect): %s",
                    delivery_tag, exc,
                )

        try:
            conn.add_callback_threadsafe(_ack_on_rmq_thread)
        except Exception as exc:
            logger.warning(
                "Could not schedule ack for delivery_tag %s: %s", delivery_tag, exc
            )

    def _incr_period_counters(self, key_fn, periods: tuple[str, ...]) -> None:
        """Atomically increments one or more hour/today period counters via
        shared/lua/incr_period_counter.lua, so each genuinely resets at the
        real UTC boundary (shared.metrics.next_period_boundary()) instead of
        accumulating forever. No "lifetime" period here -- out of scope for
        this component's existing counters (see archive-processor/README.md)."""
        now = datetime.now(timezone.utc)
        for period in periods:
            self._redis.evalsha(
                self._incr_period_counter_sha, 0, key_fn(period), 1, next_period_boundary(period, now),
            )

    def _process_flight(self, flight: CompletedFlight) -> None:
        """Write to S3 (or fallback) if S3 is currently reachable.

        External-only flights are dropped here, before either the S3
        write or the local fallback queue — deliberately, not deferred, since
        the whole point is avoiding the S3 storage cost of flights the user
        never asked to keep. force_archive (set by a matching rule) overrides
        the drop for external-only flights the user does care about.
        """
        if set(flight.receiver_sources) == {"EXTERNAL"} and not flight.force_archive:
            try:
                self._incr_period_counters(metrics_flights_skipped_key, ("hour", "today"))
            except Exception as exc:
                logger.warning("Redis counter update failed: %s", exc)
            logger.debug("Skipped external-only flight %s (no force_archive match).", flight.id)
            return

        with self._s3_lock:
            s3_available = self._s3_connected

        if s3_available:
            self._archive_flight_to_s3(flight)
        else:
            # Queue the raw JSON payload for later retry
            self._fallback.put(flight.model_dump_json(by_alias=True, exclude_none=True))
            logger.info(
                "S3 unavailable — queued flight %s to local fallback (depth=%d).",
                flight.id, self._fallback.depth(),
            )

    def _drain_fallback(self) -> None:
        """Drain the SQLite fallback queue into S3 in the background --
        the periodic telemetry-tick safety sweep's path (_drain_all_fallbacks,
        called whenever s3_connected is already True). The reconnect-
        triggered drain runs synchronously instead, gating _s3_connected
        itself -- see _finish_s3_connect."""
        had_backlog = self._fallback.depth() > 0

        def _log_done() -> None:
            depth = self._fallback.depth()
            log = logger.info if had_backlog else logger.debug
            log("Fallback drain complete. Remaining depth: %d", depth)

        self._fallback.drain_in_background(self._process_fallback_flight, on_done=_log_done)

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
            self._write_local_index_cache(index_key, index_bytes)

        had_backlog = self._index_fallback.depth() > 0

        def _log_done() -> None:
            depth = self._index_fallback.depth()
            log = logger.info if had_backlog else logger.debug
            log("Index-fallback drain complete. Remaining depth: %d", depth)

        self._index_fallback.drain_in_background(process, on_done=_log_done)

    def _archive_flight_to_s3(self, flight: CompletedFlight) -> None:
        """
        Check whether this flight continues a recently-archived segment for
        the same aircraft (a processor-count resize can force an early
        archive mid-flight); if so, merge into that segment instead of
        writing a second S3 object. Otherwise write normally. Assumes S3 is
        reachable — raises on failure so the caller can decide fallback
        handling.
        """
        s3_key = build_s3_key(flight)
        stitched = self._try_stitch(flight)
        if stitched is not None:
            flight, s3_key = stitched

        payload_dict = flight.model_dump(by_alias=True, mode="json", exclude_none=True)
        _drop_default_value_fields(payload_dict)
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
        a genuinely new flight (no prior pointer, gap too large or negative,
        or the prior segment couldn't be fetched).
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
        # A negative gap means the pointer is for a segment that actually
        # started *after* this one -- this flight arrived out of order
        # (e.g. it failed and got parked in the local retry queue while a
        # later continuation raced ahead and archived first).
        # _merge_segments always takes first_message from `prev` and
        # leaves last_message from `flight`, which assumes `prev` is
        # chronologically earlier -- merging backwards here would silently
        # produce a record with last_message before first_message rather
        # than just missing a legitimate stitch, so this is rejected the
        # same way a too-large gap already is.
        if gap > ttl or gap < 0:
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
                ex=STITCH_POINTER_TTL_SECONDS,
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
                    "flight_json": flight.model_dump_json(by_alias=True, exclude_none=True),
                    "s3_key": s3_key,
                }))
            except Exception as queue_exc:
                logger.warning("Failed to queue index retry for %s: %s", flight.id, queue_exc)
        else:
            self._write_local_index_cache(index_key, index_bytes)

        # Redis counters
        try:
            self._incr_period_counters(metrics_flights_archived_key, ("hour", "today"))
        except Exception as exc:
            logger.warning("Redis counter update failed: %s", exc)

        logger.debug("Archived flight %s -> s3://%s", flight.id, s3_key)

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
    # Docker healthcheck (heartbeat file)
    # ------------------------------------------------------------------

    def _healthcheck_loop(self) -> None:
        """Touch a heartbeat file while genuinely connected to RabbitMQ, for
        Docker's HEALTHCHECK to check the mtime of. S3 deliberately isn't
        part of the condition: an S3 outage is absorbed by the fallback queue
        by design, so it isn't an unhealthy container. Runs at
        HEALTHCHECK_INTERVAL_SECONDS, tuned against HEALTHCHECK_MAX_AGE_SECONDS
        (see shared/timing.py) independent of the MQTT publish cadence."""
        heartbeat_path = pathlib.Path(_HEALTHCHECK_HEARTBEAT_PATH)
        heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        while not self._shutdown.is_set():
            if self._rmq_connected:
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
        device = build_ha_device(
            identifier="SkyFollower_archive",
            name="SkyFollower Archive",
            model="Archive",
        )
        availability = {
            "availability_topic": "SkyFollower/archive/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }
        base = "SkyFollower/archive/statistic"
        sensors = [
            ("flights_archived_hour", "Flights Archived (Hour)", "mdi:airplane-landing", "total_increasing", None, None),
            ("flights_archived_today", "Flights Archived (Today)", "mdi:airplane-landing", "total_increasing", None, None),
            ("flights_skipped_hour", "Flights Skipped External-Only (Hour)", "mdi:airplane-off", "total_increasing", None, None),
            ("flights_skipped_today", "Flights Skipped External-Only (Today)", "mdi:airplane-off", "total_increasing", None, None),
            ("s3_connected", "S3 Connected", "mdi:cloud-check", None, None, None),
            ("local_queue_depth", "Local Queue Depth", "mdi:tray-full", "measurement", None, None),
            ("local_index_queue_depth", "Local Index Queue Depth", "mdi:tray-full", "measurement", None, None),
            ("dead_letter_queue_depth", "Dead Letter Queue Depth", "mdi:skull-crossbones", "measurement", None, None),
            ("dead_letter_index_queue_depth", "Dead Letter Index Queue Depth", "mdi:skull-crossbones", "measurement", None, None),
            ("started_at", "Start Time", "mdi:clock", None, None, "timestamp"),
        ]
        for name, desc, icon, state_class, unit, device_class in sensors:
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
            if device_class:
                payload["device_class"] = device_class
            self._mqtt.publish(
                f"homeassistant/sensor/SkyFollower_archive_{name}/config",
                json.dumps(payload),
                retain=True,
            )

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------

    def _telemetry_loop(self) -> None:
        while not self._shutdown.is_set():
            time.sleep(MQTT_PUBLISH_INTERVAL_SECONDS)
            # Independent of MQTT/_publish_telemetry below: a periodic
            # sweep of both fallback queues, not just a reaction to
            # _s3_reconnect_loop's edge-triggered "was down, now up"
            # detection — see _drain_all_fallbacks for why both queues
            # get this even though only the index queue strictly needs it.
            # Each queue's _drain_fallback()/_drain_index_fallback() spawns
            # its own background thread (or skips if one's already
            # running for that specific queue), so this call returns
            # immediately and never delays the telemetry publish below it.
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
            f"{base}/dead_letter_queue_depth", str(self._fallback.dead_letter_depth()), retain=True
        )
        self._mqtt.publish(
            f"{base}/dead_letter_index_queue_depth",
            str(self._index_fallback.dead_letter_depth()),
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
        # Wake every idle worker so it sees the shutdown flag now rather
        # than after its 1s queue poll.
        for q in self._worker_queues:
            try:
                q.put_nowait(None)
            except Exception:
                pass
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

def main() -> None:
    try:
        config = load_config("rabbitmq", "redis", "mqtt", "s3")
    except ConfigError as exc:
        # Logging isn't configured until ArchiveProcessor is constructed,
        # which is exactly what failed here.
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    processor = ArchiveProcessor(config)

    def _handle_signal(sig, frame):
        processor.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    processor.start()


if __name__ == "__main__":
    main()
