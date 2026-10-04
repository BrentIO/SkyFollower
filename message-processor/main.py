#!/usr/bin/env python3
"""
SkyFollower Message Processor

Consumes raw ADS-B/UAT messages from its own RabbitMQ queue, bound to the
consistent-hash exchange the receivers publish to, maintains per-aircraft
flight state in a file-backed SQLite database (survives a process restart),
enriches with Redis lookups, runs the rules engine, publishes MQTT
notifications, and hands completed flights to the archive queue.

One container = one message processor instance.  MESSAGE_PROCESSOR_ID is set
via the environment variable of the same name, and can be any string unique
across the whole deployment.
"""

from __future__ import annotations

import hashlib
import json
import logging
import logging.handlers
import os
import pathlib
import re
import signal
import socket
import sqlite3
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import NamedTuple, Optional

import paho.mqtt.client as mqtt
import pika
import pyModeS978
import redis as redis_lib
from pyModeS import PipeDecoder

from message_processor.route_resolver import resolve_origin_destination
from message_processor.rules_engine import RulesEngine
from shared.config import DATA_DIR, ConfigError, load_config
from shared.redis_client import build_redis_client
from shared.fallback_queue import DEFAULT_DEAD_LETTER_MAX_BYTES, FallbackQueue
from shared.ha_discovery import build_ha_device
from shared.logging_setup import configure_logging
from shared.models import (
    AircraftRecord,
    CompletedFlight,
    InboundMessage,
    OperatorRecord,
    Position,
    RawFrame,
    Velocity,
    generate_flight_id,
)
from shared.mqtt import build_mqtt_client
from shared.mqtt_register import publish_register
from shared.rabbitmq_topology import (
    ARCHIVE_QUEUE_NAME,
    RAW_FRAMES_QUEUE_NAME,
    bind_adsb_queue,
    declare_adsb_topology,
    declare_raw_frames_queue,
    message_processor_queue_name,
)
from shared.metrics import next_period_boundary
from shared.redis_keys import (
    config_areas_version_key,
    config_flight_ttl_seconds_key,
    config_rules_version_key,
    message_processor_heartbeat_key,
    metrics_operator_misses_key,
    metrics_registration_misses_key,
    metrics_total_messages_processed_key,
    normalize_flight_ident,
    operator_key,
    rule_trigger_day_key,
    rule_trigger_lifetime_key,
)
from shared.timing import (
    CONFIG_POLL_INTERVAL_SECONDS,
    DEFAULT_FLIGHT_TTL_SECONDS,
    MAP_UDP_POSITION_MIN_INTERVAL_SECONDS,
    HEALTHCHECK_INTERVAL_SECONDS,
    HEARTBEAT_INTERVAL_SECONDS,
    HEARTBEAT_TTL_SECONDS,
    IDENT_CONFIRM_COUNT,
    IDENT_CONFIRM_WINDOW_SECONDS,
    MAP_HEARTBEAT_INTERVAL_SECONDS,
    MAP_METADATA_RESEND_INTERVAL_SECONDS,
    MAX_MESSAGE_LAG_SECONDS,
    MQTT_PUBLISH_INTERVAL_SECONDS,
    PARITY_ERROR_CONFIRM_WINDOW_SECONDS,
    RATE_WINDOW_SECONDS,
    RECONNECT_BACKOFF_SECONDS,
    RULE_TRIGGER_DAY_TTL_SECONDS,
)

logger = logging.getLogger("message_processor")

# tmpfs-mounted (docker-compose.message-processor.yaml) -- only /app/data
# (the SQLite active store) is durable.
_HEALTHCHECK_HEARTBEAT_PATH = "/app/health/heartbeat"

# One consumer per queue, so this buys throughput (avoids an ack round-trip
# stall per message), not fair dispatch. 100 balances that against how many
# messages would need reprocessing if the connection drops mid-batch.
_RMQ_PREFETCH_COUNT = 100

# ---------------------------------------------------------------------------
# US registration regex (skip operator lookup for tail numbers)
# ---------------------------------------------------------------------------
_US_REG_RE = re.compile(
    r"^[LT]?N[CXR]?[1-9]((\d{0,4})|(\d{0,3}[A-HJ-NP-Z])|(\d{0,2}[A-HJ-NP-Z]{2}))$"
)


# Receiver-decode-only -- registry/Mictronics enrichment must never seed
# this. Collapses both decode paths' 7 raw categories to light/medium/heavy;
# Super is unreachable from live data and rotorcraft/high-performance are a
# different axis (emitter type), not a weight class to remap. Keyed by both
# pyModeS978's EmitterCategory names and pyModeS's TC=4 wake_vortex strings.
_WAKE_TURBULENCE_MAP: dict[str, str] = {
    "LIGHT": "light",
    "MEDIUM": "medium",
    "MEDIUM_LARGE": "medium",
    "MEDIUM_LARGE_HIGH_VORTEX": "medium",
    "HEAVY": "heavy",
    "Light": "light",
    "Medium 1": "medium",
    "Medium 2": "medium",
    "High vortex aircraft": "medium",
    "Heavy": "heavy",
}


# Raw ADS-B emitter category as the "<set><subcategory>" code from the
# identification message (1090 DO-260B Table A-2-8 / UAT DO-282B): set is
# A-D, subcategory 1-7 (0 means absent). Forwarded to the map service as a
# last-resort icon-shape hint for aircraft with no type enrichment;
# deliberately not part of AircraftRecord since it's receiver-decoded, not
# registry data.
_EMITTER_CATEGORY_SETS = "ABCD"


def _emitter_category_code(set_index: int, subcategory: int) -> Optional[str]:
    """Assemble the "<set><subcategory>" emitter-category string (e.g. "A5",
    "B2"), or None when it carries no information (subcategory 0) or falls
    outside sets A-D."""
    if not 0 <= set_index < len(_EMITTER_CATEGORY_SETS):
        return None
    if not 1 <= subcategory <= 7:
        return None
    return f"{_EMITTER_CATEGORY_SETS[set_index]}{subcategory}"


def _ident_matches_registration(ident: str, aircraft: dict) -> bool:
    """True when the broadcast ident is just the aircraft's own tail number
    (registration) rather than a route-bearing flight identifier/callsign —
    dash-insensitive, matching SkyFollower-legacy's setIdent()/_getOperator()
    precedent. Shared by operator enrichment and route-leg resolution, since
    both need the same "is this really a flight number" judgment call."""
    registration = aircraft.get("registration", "")
    return bool(registration) and registration.replace("-", "") == ident.replace("-", "")


# Reserved/emergency squawk codes that corrupted DF5/21 replies
# disproportionately decode into -- require confirmation before being
# trusted when sourced from a message that couldn't be CRC-verified.
_RESERVED_SQUAWKS = frozenset({"7500", "7600", "7700", "7777"})

# Plausibility bounds applied at the single decode site (_decode_1090/
# _decode_978) so every downstream consumer inherits the filter. Altitude
# floor is widened below sea level for pressure-altitude quirks; the ceiling
# is far under the field's ~101,350ft max, since anything near that is
# garbage.
_MIN_LATITUDE = -90
_MAX_LATITUDE = 90
_MIN_LONGITUDE = -180
_MAX_LONGITUDE = 180
_MIN_ALTITUDE_FT = -1500
_MAX_ALTITUDE_FT = 65000


def _short_hash(full: Optional[str]) -> str:
    """Last 8 chars of a config version hash, for a compact HA display.
    Applied only at the MQTT-publish boundary -- never where a version is
    stored or compared. "unknown" if none loaded yet."""
    return full[-8:] if full else "unknown"


# Repeat-sighting count for confirming a reserved squawk sourced from an
# unverifiable message. Ident has its own independent constants
# (IDENT_CONFIRM_COUNT/IDENT_CONFIRM_WINDOW_SECONDS in shared/timing.py) --
# do not reuse this one for ident.
_PARITY_ERROR_CONFIRM_COUNT = 5


def _confirm_after_repeated_sightings(
    pending: Optional[dict], value: str, received_at: float,
    window_seconds: float = PARITY_ERROR_CONFIRM_WINDOW_SECONDS,
    required_count: int = _PARITY_ERROR_CONFIRM_COUNT,
) -> tuple[dict, bool]:
    """Track repeated sightings of `value` in a trailing time window, keyed
    on message timestamps (not wall-clock) so replay is judged like live
    traffic. Returns updated pending state and whether the threshold was
    just reached. Non-consecutive sightings still count -- corruption tends
    to garble each message independently, not identically."""
    if pending is not None and pending.get("value") == value:
        sightings = list(pending.get("sightings", []))
    else:
        sightings = []

    sightings = [t for t in sightings if received_at - t <= window_seconds]
    sightings.append(received_at)

    return {"value": value, "sightings": sightings}, len(sightings) >= required_count


def _flight_metadata_snapshot(flight: Flight) -> str:
    """Hash of the flight fields the map UDP `metadata` message carries,
    compared against Flight.map_metadata_hash to decide whether a resend is
    needed. Hashed rather than field-by-field compared so a new field added
    to Flight later is covered automatically. matched_rules is included so
    a rule match alone triggers a prompt resend."""
    payload = {
        "ident": flight.ident,
        "aircraft": flight.aircraft,
        "operator": flight.operator,
        "registrant": flight.registrant,
        "squawk": flight.squawk,
        "origin": flight.origin,
        "destination": flight.destination,
        "matched_rules": flight.matched_rules,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


class _MapUdpPublisher:
    """Fire-and-forget UDP publisher for the map service's live
    position/metadata/heartbeat feed. Disabled (no socket) when `host` is
    blank, matching the optional-endpoint convention elsewhere in
    shared/config.py.

    send() never affects the processing pipeline: sendto() on a datagram
    socket doesn't block on the peer, so the only failure mode is a local
    OSError, which is swallowed and logged, never propagated."""

    def __init__(
        self, host: str, port: int,
        min_position_interval_seconds: float = MAP_UDP_POSITION_MIN_INTERVAL_SECONDS,
    ) -> None:
        if host and not port:
            logger.warning(
                "MAP_UDP_HOST is set (%s) but MAP_UDP_PORT is not set (or is 0) -- "
                "map UDP feed is misconfigured.", host,
            )
        elif port and not host:
            logger.warning(
                "MAP_UDP_PORT is set (%s) but MAP_UDP_HOST is not set -- "
                "map UDP feed remains disabled.", port,
            )
        self._addr: Optional[tuple[str, int]] = (host, port) if host else None
        self._sock: Optional[socket.socket] = (
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM) if self._addr else None
        )
        # True after a send() failure until the next success, so a WARNING
        # logs once per outage, not once per message.
        self._logged_failure = False
        # Most recent send() time, any message type; consulted by
        # _map_heartbeat_loop's skip-if-recently-sent check.
        self._last_sent_at: Optional[float] = None

        # Per-icao_hex last-sent-position timestamp (message received_at,
        # not send time) -- see should_send_position(). Only `position`
        # sends are throttled; `metadata` is already change-gated.
        self._min_position_interval = min_position_interval_seconds
        self._last_position_sent: dict[str, float] = {}

        if self.enabled:
            logger.info("Map UDP feed enabled -> %s:%s", host, port)
        else:
            logger.info("Map UDP feed disabled (MAP_UDP_HOST not set)")

    @property
    def enabled(self) -> bool:
        return self._sock is not None

    @property
    def last_sent_at(self) -> Optional[float]:
        """time.time() of the most recent send() call attempt (not
        confirmed delivery), or None if never called."""
        return self._last_sent_at

    def should_send_position(self, icao_hex: str, timestamp: float) -> bool:
        """True if enough time has elapsed since icao_hex's last sent
        position (records `timestamp` as a side effect either way).
        Compared on the message's own `received_at`, not wall-clock time,
        so throttling stays stable under replay/backlog."""
        last_sent = self._last_position_sent.get(icao_hex)
        if last_sent is not None and timestamp - last_sent < self._min_position_interval:
            return False
        self._last_position_sent[icao_hex] = timestamp
        return True

    def send(self, payload: dict) -> None:
        if self._sock is None:
            return
        self._last_sent_at = time.time()
        try:
            body = json.dumps(payload, default=str).encode("utf-8")
            self._sock.sendto(body, self._addr)
            self._logged_failure = False
        except Exception as exc:
            # Caught broadly so nothing about this best-effort feed can
            # propagate into the hot path. First failure of an outage logs
            # at WARNING; repeats drop to DEBUG to avoid flooding.
            if not self._logged_failure:
                logger.warning("Map UDP send failed: %s", exc)
                self._logged_failure = True
            else:
                logger.debug("Map UDP send failed: %s", exc)

    def close(self) -> None:
        if self._sock is not None:
            self._sock.close()


# ---------------------------------------------------------------------------
# SQLite schema (active flight store)
# ---------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE IF NOT EXISTS flights (
    icao_hex      TEXT PRIMARY KEY,
    flight_id     TEXT,
    first_message REAL NOT NULL,
    last_message  REAL,
    total_messages INTEGER,
    aircraft      TEXT,
    ident         TEXT,
    operator      TEXT,
    registrant    TEXT,
    squawk        TEXT,
    origin        TEXT,
    destination   TEXT,
    matched_rules TEXT,
    receiver_sources TEXT,
    force_archive INTEGER,
    route_resolution_attempted INTEGER,
    route_candidate_airports TEXT,
    pending_squawk TEXT,
    pending_ident  TEXT,
    map_metadata_hash TEXT
);
CREATE TABLE IF NOT EXISTS positions (
    icao_hex  TEXT,
    timestamp REAL,
    latitude  REAL,
    longitude REAL,
    altitude  INTEGER
);
CREATE TABLE IF NOT EXISTS velocities (
    icao_hex      TEXT,
    timestamp     REAL,
    velocity      REAL,
    heading       REAL,
    vertical_speed INTEGER
);
CREATE TABLE IF NOT EXISTS raw_frames (
    icao_hex  TEXT NOT NULL,
    timestamp REAL NOT NULL,
    raw       TEXT NOT NULL,
    source    TEXT NOT NULL,
    decoded   INTEGER NOT NULL
);
"""
# raw_frames has no unique index (unlike positions/velocities below) --
# legitimate dual 978+1090 frames land microseconds apart, and dropping one
# to a timestamp collision would defeat its forensic purpose.
# positions/velocities' unique index is created in _migrate_schema()
# instead, since an existing db may hold duplicate rows that must be
# cleaned up first -- CREATE UNIQUE INDEX fails outright otherwise.


def _migrate_schema(db: sqlite3.Connection) -> None:
    """Upgrade an active_flights.db predating a given column via ALTER
    TABLE (CREATE TABLE IF NOT EXISTS only handles a brand-new file). Safe
    to call unconditionally -- checks column presence first. No backfill of
    the old `source` column: at most a handful of in-progress flights lose
    their single-source value on the restart that upgrades the schema, then
    accumulate receiver_sources fresh."""
    existing = {row[1] for row in db.execute("PRAGMA table_info(flights)").fetchall()}
    if "receiver_sources" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN receiver_sources TEXT")
    if "force_archive" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN force_archive INTEGER")
    if "route_resolution_attempted" not in existing:
        # NULL/0 (falsy) is the correct starting value, same as a
        # brand-new flight -- no backfill needed.
        db.execute("ALTER TABLE flights ADD COLUMN route_resolution_attempted INTEGER")
    if "route_candidate_airports" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN route_candidate_airports TEXT")
    if "pending_squawk" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN pending_squawk TEXT")
    if "pending_ident" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN pending_ident TEXT")
    if "registrant" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN registrant TEXT")
    if "map_metadata_hash" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN map_metadata_hash TEXT")

    # Dedupe rows left by RabbitMQ's at-least-once redelivery before adding
    # the unique index below -- CREATE UNIQUE INDEX fails on a table that
    # already violates it.
    db.execute(
        "DELETE FROM positions WHERE rowid NOT IN "
        "(SELECT MIN(rowid) FROM positions GROUP BY icao_hex, timestamp)"
    )
    db.execute(
        "DELETE FROM velocities WHERE rowid NOT IN "
        "(SELECT MIN(rowid) FROM velocities GROUP BY icao_hex, timestamp)"
    )
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS positions_icao_hex_timestamp "
        "ON positions (icao_hex, timestamp)"
    )
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS velocities_icao_hex_timestamp "
        "ON velocities (icao_hex, timestamp)"
    )
    db.commit()

# ---------------------------------------------------------------------------
# Message rate tracker (30-second rolling window)
# ---------------------------------------------------------------------------

class _RateTracker:
    def __init__(self, window: int = RATE_WINDOW_SECONDS) -> None:
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
# Period-counter accumulator (in-memory, flushed to Redis on the telemetry
# cadence only -- see MessageProcessor._flush_period_counters())
# ---------------------------------------------------------------------------

class _CounterAccumulator:
    """Thread-safe in-memory delta accumulator for a Redis-backed period
    counter. record() is safe to call from the hot path; flush_and_reset()
    is called only from the telemetry thread, returning (and zeroing) the
    delta since the last flush for the caller to push into Redis."""

    def __init__(self) -> None:
        self._pending = 0
        self._lock = threading.Lock()

    def record(self, n: int = 1) -> None:
        with self._lock:
            self._pending += n

    def flush_and_reset(self) -> int:
        with self._lock:
            v = self._pending
            self._pending = 0
            return v


class _KeyedCounterAccumulator:
    """Same idea as _CounterAccumulator, but keyed (by rule identifier)
    instead of a single scalar."""

    def __init__(self) -> None:
        self._pending: dict[str, int] = {}
        self._lock = threading.Lock()

    def record(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._pending[key] = self._pending.get(key, 0) + n

    def flush_and_reset(self) -> dict[str, int]:
        with self._lock:
            v = self._pending
            self._pending = {}
            return v


# ---------------------------------------------------------------------------
# Processing time tracker (rolling average)
# ---------------------------------------------------------------------------

class _TimeTracker:
    def __init__(self) -> None:
        self._total_ms = 0.0
        self._count = 0
        self._hwm_ms = 0.0
        self._lock = threading.Lock()

    def record(self, ms: float) -> None:
        with self._lock:
            self._total_ms += ms
            self._count += 1

    def record_hwm(self, ms: float) -> None:
        with self._lock:
            if ms > self._hwm_ms:
                self._hwm_ms = ms

    def avg_ms(self) -> float:
        with self._lock:
            if self._count == 0:
                return 0.0
            return self._total_ms / self._count

    def hwm_ms_and_reset(self) -> float:
        """Returns the tracked high-water mark at full float precision; any
        display-side rounding is left to the consumer."""
        with self._lock:
            v = self._hwm_ms
            self._hwm_ms = 0.0
            return v

    def reset(self) -> None:
        with self._lock:
            self._total_ms = 0.0
            self._count = 0


# ---------------------------------------------------------------------------
# HA autodiscovery sensor definitions
# ---------------------------------------------------------------------------

class _Sensor(NamedTuple):
    """One HA autodiscovery sensor entity for a statistic/{field} topic.
    `extra` carries any additional discovery payload keys (e.g.
    suggested_display_precision) a specific entity needs."""
    field: str
    name: str
    icon: str
    state_class: Optional[str]
    unit: Optional[str] = None
    extra: Optional[dict] = None


# ---------------------------------------------------------------------------
# Flight — wraps SQLite state
# ---------------------------------------------------------------------------

class Flight:
    """
    In-memory view of one aircraft's active flight.  Reads from / writes to
    the shared SQLite connection.
    """

    __slots__ = (
        "icao_hex", "flight_id", "first_message", "last_message", "total_messages",
        "aircraft", "ident", "operator", "registrant", "squawk", "origin", "destination",
        "matched_rules", "receiver_sources", "force_archive", "route_resolution_attempted",
        "route_candidate_airports", "pending_squawk", "pending_ident",
        "map_metadata_hash", "positions", "velocities", "raw_frames", "_db",
    )

    def __init__(self, db: sqlite3.Connection) -> None:
        self._db = db
        self.icao_hex: str = ""
        self.flight_id: str = ""
        self.first_message: float = 0.0
        self.last_message: float = 0.0
        self.total_messages: int = 0
        self.aircraft: dict = {}
        self.ident: str = ""
        self.operator: dict = {}
        self.registrant: dict = {}
        self.squawk: str = ""
        self.origin: Optional[dict] = None
        self.destination: Optional[dict] = None
        self.matched_rules: list[str] = []
        self.receiver_sources: list[str] = []
        self.force_archive: bool = False
        # One-shot guard so route resolution (_maybe_resolve_route) isn't
        # re-queried against Redis on every subsequent message.
        self.route_resolution_attempted: bool = False
        # Raw JSON of airport records from route_airports.lua, cached on
        # first fetch so re-evaluation (heading not yet stable) skips the
        # Redis round trip. None until fetched.
        self.route_candidate_airports: Optional[str] = None
        # Confirmation-in-progress candidate for a squawk/ident sourced from
        # an unverifiable message; None once confirmed or not pending. See
        # _confirm_after_repeated_sightings.
        self.pending_squawk: Optional[dict] = None
        self.pending_ident: Optional[dict] = None
        # Snapshot hash of the map UDP `metadata` fields as of the last
        # send -- None until the first send. See _flight_metadata_snapshot.
        self.map_metadata_hash: Optional[str] = None
        self.positions: list[Position] = []
        self.velocities: list[Velocity] = []
        # Populated only by add_raw_frame() or _load_raw_frames() -- unlike
        # positions/velocities, load() never reloads this on the hot path.
        self.raw_frames: list[RawFrame] = []

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load(self, icao_hex: str, limit: bool = True) -> bool:
        """Load flight from SQLite. Returns True if found."""
        self.icao_hex = icao_hex.upper()
        cur = self._db.cursor()
        cur.execute(
            "SELECT icao_hex, flight_id, first_message, last_message, total_messages, "
            "aircraft, ident, operator, registrant, squawk, origin, destination, "
            "matched_rules, receiver_sources, force_archive, route_resolution_attempted, "
            "route_candidate_airports, pending_squawk, pending_ident, map_metadata_hash "
            "FROM flights WHERE icao_hex=?",
            (self.icao_hex,),
        )
        row = cur.fetchone()
        if row is None:
            return False

        self.flight_id = row["flight_id"] or ""
        self.first_message = row["first_message"]
        self.last_message = row["last_message"]
        self.total_messages = row["total_messages"]
        self.aircraft = json.loads(row["aircraft"] or "{}")
        self.ident = row["ident"] or ""
        self.operator = json.loads(row["operator"] or "{}")
        self.registrant = json.loads(row["registrant"] or "{}")
        self.squawk = row["squawk"] or ""
        self.origin = json.loads(row["origin"]) if row["origin"] else None
        self.destination = json.loads(row["destination"]) if row["destination"] else None
        self.matched_rules = json.loads(row["matched_rules"] or "[]")
        self.receiver_sources = json.loads(row["receiver_sources"] or "[]")
        self.force_archive = bool(row["force_archive"])
        self.route_resolution_attempted = bool(row["route_resolution_attempted"])
        self.route_candidate_airports = row["route_candidate_airports"]
        self.pending_squawk = json.loads(row["pending_squawk"]) if row["pending_squawk"] else None
        self.pending_ident = json.loads(row["pending_ident"]) if row["pending_ident"] else None
        self.map_metadata_hash = row["map_metadata_hash"]

        self._load_positions(limit=limit)
        self._load_velocities(limit=limit)
        return True

    def _load_positions(self, limit: bool) -> None:
        sql = ("SELECT timestamp, latitude, longitude, altitude "
               "FROM positions WHERE icao_hex=? ORDER BY timestamp")
        if limit:
            sql += " DESC LIMIT 1"
        cur = self._db.cursor()
        cur.execute(sql, (self.icao_hex,))
        self.positions = [
            Position(timestamp=r["timestamp"], latitude=r["latitude"],
                     longitude=r["longitude"], altitude=r["altitude"])
            for r in cur.fetchall()
        ]

    def _load_velocities(self, limit: bool) -> None:
        sql = ("SELECT timestamp, velocity, heading, vertical_speed "
               "FROM velocities WHERE icao_hex=? ORDER BY timestamp")
        if limit:
            sql += " DESC LIMIT 1"
        cur = self._db.cursor()
        cur.execute(sql, (self.icao_hex,))
        self.velocities = [
            Velocity(timestamp=r["timestamp"], velocity=r["velocity"],
                     heading=r["heading"], vertical_speed=r["vertical_speed"])
            for r in cur.fetchall()
        ]

    def save(self) -> None:
        cur = self._db.cursor()
        cur.execute(
            "REPLACE INTO flights (icao_hex, flight_id, first_message, last_message, "
            "total_messages, aircraft, ident, operator, registrant, squawk, origin, "
            "destination, matched_rules, receiver_sources, force_archive, "
            "route_resolution_attempted, route_candidate_airports, "
            "pending_squawk, pending_ident, map_metadata_hash) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                self.icao_hex, self.flight_id, self.first_message, self.last_message,
                self.total_messages, json.dumps(self.aircraft), self.ident,
                json.dumps(self.operator), json.dumps(self.registrant), self.squawk,
                json.dumps(self.origin) if self.origin else None,
                json.dumps(self.destination) if self.destination else None,
                json.dumps(self.matched_rules), json.dumps(self.receiver_sources),
                int(self.force_archive), int(self.route_resolution_attempted),
                self.route_candidate_airports,
                json.dumps(self.pending_squawk) if self.pending_squawk else None,
                json.dumps(self.pending_ident) if self.pending_ident else None,
                self.map_metadata_hash,
            ),
        )
        self._db.commit()

    def delete(self) -> None:
        cur = self._db.cursor()
        cur.execute("DELETE FROM flights   WHERE icao_hex=?", (self.icao_hex,))
        cur.execute("DELETE FROM positions WHERE icao_hex=?", (self.icao_hex,))
        cur.execute("DELETE FROM velocities WHERE icao_hex=?", (self.icao_hex,))
        cur.execute("DELETE FROM raw_frames WHERE icao_hex=?", (self.icao_hex,))

    def add_position(self, pos: Position) -> None:
        cur = self._db.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO positions (icao_hex, timestamp, latitude, longitude, altitude) "
            "VALUES (?,?,?,?,?)",
            (self.icao_hex, pos.timestamp, pos.latitude, pos.longitude, pos.altitude),
        )
        # rowcount is 0 when the unique index silently ignored a redelivery
        # duplicate -- skip the append too, or this list drifts from what's
        # persisted.
        if cur.rowcount:
            self.positions.append(pos)

    def add_velocity(self, vel: Velocity) -> None:
        cur = self._db.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO velocities (icao_hex, timestamp, velocity, heading, vertical_speed) "
            "VALUES (?,?,?,?,?)",
            (self.icao_hex, vel.timestamp, vel.velocity, vel.heading, vel.vertical_speed),
        )
        if cur.rowcount:
            self.velocities.append(vel)

    def add_raw_frame(self, frame: RawFrame) -> None:
        """Plain INSERT, not "OR IGNORE" like add_position()/add_velocity()
        -- raw_frames has no unique index to collide with."""
        cur = self._db.cursor()
        cur.execute(
            "INSERT INTO raw_frames (icao_hex, timestamp, raw, source, decoded) "
            "VALUES (?,?,?,?,?)",
            (self.icao_hex, frame.timestamp, frame.raw, frame.source, int(frame.decoded)),
        )
        self.raw_frames.append(frame)

    def _load_raw_frames(self) -> None:
        """Always loads full history, unlike _load_positions()/
        _load_velocities() -- no per-message need for a "just the latest"
        variant. Called only when the caller opts in, so
        CAPTURE_RAW_FRAMES-off deployments never issue this query."""
        cur = self._db.cursor()
        cur.execute(
            "SELECT timestamp, raw, source, decoded FROM raw_frames "
            "WHERE icao_hex=? ORDER BY timestamp",
            (self.icao_hex,),
        )
        self.raw_frames = [
            RawFrame(timestamp=r["timestamp"], raw=r["raw"], source=r["source"],
                     decoded=bool(r["decoded"]))
            for r in cur.fetchall()
        ]

    # ------------------------------------------------------------------
    # Serialisation to CompletedFlight (for archive queue)
    # ------------------------------------------------------------------

    def to_completed_flight(self, load_all: bool = False, load_raw_frames: bool = False) -> CompletedFlight:
        """Reload all positions/velocities then build a CompletedFlight record.
        `load_raw_frames` is independent of `load_all` since load() never touches
        raw_frames; callers pass True only when CAPTURE_RAW_FRAMES is enabled."""
        if load_all:
            self._load_positions(limit=False)
            self._load_velocities(limit=False)
        if load_raw_frames:
            self._load_raw_frames()

        # Drop None-valued keys here -- aircraft is a plain dict, so a
        # top-level exclude_none on the CompletedFlight dump can't reach it.
        aircraft = {k: v for k, v in self.aircraft.items() if v is not None}
        if "icao_hex" not in aircraft:
            aircraft["icao_hex"] = self.icao_hex

        # Strip military=False (legacy behaviour)
        if aircraft.get("military") is False:
            aircraft.pop("military")

        # Operator without source key, None-valued keys dropped (same
        # plain-dict reason as aircraft above)
        operator: Optional[dict] = None
        if self.operator:
            operator = {
                k: v for k, v in self.operator.items()
                if k != "source" and v is not None
            }

        registrant: Optional[dict] = None
        if self.registrant:
            registrant = {k: v for k, v in self.registrant.items() if v is not None}

        # Origin/destination: full airport object, None-valued keys dropped
        # (same plain-dict reason as aircraft/operator/registrant above)
        origin: Optional[dict] = None
        if self.origin:
            origin = {k: v for k, v in self.origin.items() if v is not None}

        destination: Optional[dict] = None
        if self.destination:
            destination = {k: v for k, v in self.destination.items() if v is not None}

        return CompletedFlight(**{
            "_id": self.flight_id or generate_flight_id(),
            "first_message": datetime.fromtimestamp(self.first_message, tz=timezone.utc),
            "last_message": datetime.fromtimestamp(self.last_message, tz=timezone.utc),
            "total_messages": self.total_messages,
            "receiver_sources": self.receiver_sources,
            "force_archive": self.force_archive,
            "aircraft": aircraft,
            "ident": self.ident or None,
            "operator": operator,
            "registrant": registrant,
            "squawk": self.squawk or None,
            "origin": origin,
            "destination": destination,
            "matched_rules": self.matched_rules,
            "positions": [p.to_dict() for p in self.positions],
            "velocities": [v.to_dict() for v in self.velocities],
            "raw_frames": [r.to_dict() for r in self.raw_frames],
        })


# ---------------------------------------------------------------------------
# Message Processor
# ---------------------------------------------------------------------------

class MessageProcessor:

    def __init__(self, config: dict, message_processor_id: str) -> None:
        self._cfg = config
        self._id = message_processor_id
        self._queue_name = message_processor_queue_name(message_processor_id)
        self._started_at = datetime.now(timezone.utc).isoformat()
        self._shutdown = threading.Event()
        # Set by the SIGUSR1 handler; polled from _eviction_loop(), which
        # runs the one-time decommission sequence (_decommission()).
        self._force_evict = threading.Event()

        # File-backed (WAL) so an existing active_flights.db survives a
        # crash or restart identically, recovering whatever flights were
        # active when the previous process ended.
        os.makedirs(DATA_DIR, exist_ok=True)
        self._db = sqlite3.connect(
            os.path.join(DATA_DIR, "active_flights.db"), check_same_thread=False
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(_SCHEMA)
        _migrate_schema(self._db)

        # Drives eviction instead of wall-clock time, so a backlog replayed
        # after a restart isn't archived just because real time passed.
        # Floored at the most recent recovered message, or wall-clock if
        # the store was empty.
        row = self._db.execute("SELECT MAX(last_message) FROM flights").fetchone()
        self._message_clock: float = row[0] if row and row[0] is not None else time.time()

        # Archive fallback. An unroutable `archive` queue (archive-processor
        # not deployed here) is non-poison and retries forever, rather than
        # dead-lettering; disk growth is instead bounded by a ring-buffer
        # cap reusing the dead-letter directory's 100MB ceiling.
        self._fallback = FallbackQueue(
            os.path.join(DATA_DIR, "completed_flights.db"),
            non_poison_exceptions=(pika.exceptions.UnroutableError,),
            retryable_max_bytes=DEFAULT_DEAD_LETTER_MAX_BYTES,
        )

        # Metrics
        self._rate = _RateTracker()
        self._processing_time = _TimeTracker()
        self._rules_time = _TimeTracker()
        self._message_latency = _TimeTracker()
        self._db_lock = threading.Lock()

        # Single persistent, stateful 1090 decoder (per-ICAO CPR pairing +
        # self-relative reference) for the process's life -- not
        # thread-safe, but only ever called from _consume_loop()'s thread.
        # Left at pyModeS's own defaults throughout.
        #
        # Surface/taxi CPR has no self-bootstrap the way airborne CPR does:
        # without `surface_ref` set, surface pairs are silently dropped, so
        # the receiver's own lat/lon is wired in here instead.
        lat_cfg = self._cfg.get("latitude")
        lon_cfg = self._cfg.get("longitude")
        surface_ref = (lat_cfg, lon_cfg) if lat_cfg is not None and lon_cfg is not None else None
        self._pipe_decoder = PipeDecoder(surface_ref=surface_ref)

        # Pure in-memory accumulation on the hot path, flushed to Redis
        # only from the telemetry thread. See _flush_period_counters().
        self._total_messages_processed = _CounterAccumulator()
        self._registration_misses = _CounterAccumulator()
        self._operator_misses = _CounterAccumulator()
        # Per-rule trigger counts (rule identifier -> delta), same
        # in-memory-then-flush pattern. See _flush_rule_trigger_counts().
        self._rule_trigger_counts = _KeyedCounterAccumulator()

        # Redis
        rc = config["redis"]
        self._redis = build_redis_client(rc)
        _lua_path = pathlib.Path(__file__).parent.parent / "shared" / "lua" / "merge_aircraft.lua"
        self._merge_sha = self._redis.script_load(_lua_path.read_text())
        _route_lua_path = pathlib.Path(__file__).parent.parent / "shared" / "lua" / "route_airports.lua"
        self._route_sha = self._redis.script_load(_route_lua_path.read_text())
        _incr_lua_path = (
            pathlib.Path(__file__).parent.parent / "shared" / "lua" / "incr_period_counter.lua"
        )
        self._incr_period_counter_sha = self._redis.script_load(_incr_lua_path.read_text())

        # Rules engine
        self._rules_engine = RulesEngine(self._redis)

        # Read once at startup and cached -- read on every message in
        # _update_flight's gap check, so it must never be a synchronous
        # Redis GET. Not hot-reloaded; restart to pick up a changed value.
        self._flight_ttl_seconds: int = DEFAULT_FLIGHT_TTL_SECONDS

        # Read once at startup; restart to pick up a change. Controls
        # whether raw frames are persisted (_update_flight) and whether the
        # raw-frames queue is declared/published to. Never affects the
        # permanent archive path, which excludes raw_frames unconditionally
        # (_archive()).
        self._capture_raw_frames: bool = bool(config.get("capture_raw_frames"))

        # MQTT
        self._mqtt: Optional[mqtt.Client] = None
        self._mqtt_connected = False

        # Map UDP publisher -- disabled (no socket) when MAP_UDP_HOST is
        # unset.
        mu = config.get("map_udp") or {}
        self._map_udp = _MapUdpPublisher(
            mu.get("host", ""), mu.get("port", 0),
        )

        # RabbitMQ
        self._rmq_connection: Optional[pika.BlockingConnection] = None
        self._rmq_channel = None
        self._rmq_connected = False

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._setup_logging()
        self._claim_message_processor_id()
        self._reset_lifetime_counters()
        self._connect_mqtt()
        self._rules_engine.reload_if_changed()
        self._load_flight_ttl_seconds()

        # Background threads
        threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat").start()
        threading.Thread(target=self._healthcheck_loop, daemon=True, name="healthcheck").start()
        threading.Thread(target=self._eviction_loop, daemon=True, name="eviction").start()
        threading.Thread(target=self._telemetry_loop, daemon=True, name="telemetry").start()
        threading.Thread(target=self._config_poll_loop, daemon=True, name="config-poll").start()
        threading.Thread(target=self._map_heartbeat_loop, daemon=True, name="map-heartbeat").start()
        threading.Thread(target=self._map_metadata_resend_loop, daemon=True, name="map-metadata-resend").start()

        self._consume_loop()

    def _setup_logging(self) -> None:
        configure_logging(self._cfg.get("log_level"))
        # pika re-emits ERROR-level lines plus a traceback on every
        # reconnect; _consume_loop's own WARNING already carries the cause.
        logging.getLogger("pika").setLevel(logging.CRITICAL)

    def _claim_message_processor_id(self) -> None:
        """Prevent two message processors with the same ID running simultaneously."""
        key = message_processor_heartbeat_key(self._id)
        claimed = self._redis.set(key, "1", nx=True, ex=HEARTBEAT_TTL_SECONDS)
        if not claimed:
            logger.critical(
                "MESSAGE_PROCESSOR_ID %s is already running on another instance. Exiting.", self._id
            )
            sys.exit(1)
        logger.info("Message processor %s claimed.", self._id)

    def _reset_lifetime_counters(self) -> None:
        """"lifetime" means "since this process started," not "forever" --
        Redis persists across restarts, so these keys need an explicit
        DELETE at boot, before the telemetry thread's first flush."""
        try:
            self._redis.delete(
                metrics_registration_misses_key(self._id, "lifetime"),
                metrics_operator_misses_key(self._id, "lifetime"),
                metrics_total_messages_processed_key(self._id, "lifetime"),
            )
        except Exception as exc:
            logger.warning("Lifetime counter reset failed: %s", exc)

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
                logger.info("Connecting to RabbitMQ (queue: %s)…", self._queue_name)
                self._rmq_connection = pika.BlockingConnection(self._rmq_params())
                self._rmq_channel = self._rmq_connection.channel()
                declare_adsb_topology(self._rmq_channel)
                bind_adsb_queue(self._rmq_channel, self._id)
                if self._capture_raw_frames:
                    # Only declared when the feature is on -- no stray
                    # queue otherwise.
                    declare_raw_frames_queue(self._rmq_channel)
                # Publisher confirms make basic_publish() synchronous and
                # raise UnroutableError instead of silently dropping a
                # message with no archive queue -- see _archive().
                self._rmq_channel.confirm_delivery()
                self._rmq_channel.basic_qos(prefetch_count=_RMQ_PREFETCH_COUNT)
                self._rmq_channel.basic_consume(
                    queue=self._queue_name,
                    on_message_callback=self._on_message,
                )
                self._rmq_connected = True
                logger.info("RabbitMQ connected, consuming from %s.", self._queue_name)

                # _drain_fallback() spawns its own background thread (or
                # skips if one is already running) -- see drain_in_background.
                self._drain_fallback()

                self._rmq_channel.start_consuming()

                # start_consuming() returned without raising: either
                # shutdown, or _force_rmq_reconnect_if_stale() broke it
                # because a publish failure latched _rmq_connected False
                # while the connection stayed up. Rebuild so the flag gets
                # re-validated, or every completed flight routes to the
                # SQLite fallback indefinitely.
                self._rmq_connected = False
                self._close_rmq_connection()
                if not self._shutdown.is_set():
                    logger.warning(
                        "RabbitMQ publish path reported a failure; "
                        "reconnecting in %ss…", RECONNECT_BACKOFF_SECONDS,
                    )
                    time.sleep(RECONNECT_BACKOFF_SECONDS)

            except pika.exceptions.AMQPConnectionError as exc:
                self._rmq_connected = False
                self._close_rmq_connection()
                logger.warning(
                    "RabbitMQ unavailable: %r. Retrying in %ss…",
                    exc, RECONNECT_BACKOFF_SECONDS,
                )
                time.sleep(RECONNECT_BACKOFF_SECONDS)
            except Exception as exc:
                self._rmq_connected = False
                self._close_rmq_connection()
                logger.error(
                    "RabbitMQ error: %r. Retrying in %ss…",
                    exc, RECONNECT_BACKOFF_SECONDS,
                )
                time.sleep(RECONNECT_BACKOFF_SECONDS)

    def _close_rmq_connection(self) -> None:
        """Drop the current connection/channel so _consume_loop's next
        iteration builds a fresh one. Safe to call after start_consuming()
        has returned (its own thread is no longer inside pika)."""
        conn = self._rmq_connection
        self._rmq_connection = None
        self._rmq_channel = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _force_rmq_reconnect_if_stale(self) -> None:
        """Publish failures latch _rmq_connected False even when the
        connection stays up (e.g. broker blocked publishers on a disk-free
        alarm) and keeps delivering inbound messages -- nothing else clears
        the flag, so completed flights would pile into the SQLite fallback
        forever. Breaks start_consuming() so _consume_loop rebuilds and
        re-validates it. No-op if there's no live connection."""
        if self._rmq_connected:
            return
        conn = self._rmq_connection
        channel = self._rmq_channel
        if conn is None or channel is None:
            return
        logger.warning(
            "RabbitMQ publish path latched disconnected while consuming; "
            "forcing a reconnect."
        )
        try:
            conn.add_callback_threadsafe(channel.stop_consuming)
        except Exception:
            pass

    def _on_message(self, ch, method, props, body: bytes) -> None:
        try:
            msg = InboundMessage.model_validate_json(body)
        except Exception as exc:
            logger.debug("Unparseable message: %s", exc)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        t_start = time.monotonic()
        self._rate.record()
        self._total_messages_processed.record()
        self._process(msg)
        elapsed_ms = (time.monotonic() - t_start) * 1000
        self._processing_time.record(elapsed_ms)
        self._processing_time.record_hwm(elapsed_ms)

        # Receipt-through-processed, including RabbitMQ wait time -- must
        # use wall-clock time.time(), not time.monotonic(), since
        # msg.received_at is stamped on a different host. Sensitive to
        # NTP drift between hosts; an accepted tradeoff.
        latency_ms = (time.time() - msg.received_at) * 1000
        self._message_latency.record_hwm(latency_ms)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    # ------------------------------------------------------------------
    # ADS-B decoding + flight state update
    # ------------------------------------------------------------------

    def _process(self, msg: InboundMessage) -> None:
        data = self._decode_message(msg)
        if data is None:
            if not self._capture_raw_frames:
                return
            # CAPTURE_RAW_FRAMES on: route the decode failure into
            # _update_flight anyway with a minimal stand-in `data`, so it's
            # still recorded as a raw frame against this icao_hex.
            data = {"icao_hex": msg.icao_hex}
        with self._db_lock:
            self._update_flight(data, msg)

    def _decode_message(self, msg: InboundMessage) -> Optional[dict]:
        """Route to the source-specific decoder. EXTERNAL-tagged frames are
        still raw Mode-S hex, same as 1090 — this dispatch was never keyed
        on source before, so that's not a behavior change."""
        if msg.source == "978":
            return self._decode_978(msg)
        return self._decode_1090(msg)

    def _decode_1090(self, msg: InboundMessage) -> Optional[dict]:
        """Decode a raw Mode-S frame via the shared, per-process
        PipeDecoder rather than a per-message pms.decode(reference=...)
        call. Pure field-presence extraction -- message types with no
        fields of interest just produce nothing.

        PipeDecoder resolves airborne CPR itself via even/odd pairing and
        a per-ICAO self-relative reference, fixing wrong-CPR-zone and
        isolated-CRC-lucky teleports; a side effect is that a brand-new
        ICAO's first position is held back until a pair/bootstrap cluster
        resolves. Surface CPR has no such self-bootstrap, so it relies on
        the `surface_ref` passed at construction (__init__) instead.
        """
        raw = msg.raw
        if len(raw) < 14:
            return None

        # Reads PipeDecoder's private `_stats` dict directly instead of its
        # `stats` property (which copies on every access) -- acceptable
        # since pyModeS is pinned to an exact version (requirements.txt).
        rejected_before = self._pipe_decoder._stats["position_rejected"]
        try:
            result = self._pipe_decoder.decode(raw, timestamp=msg.received_at)
        except Exception:
            return None
        # True only when this message tripped PipeDecoder's
        # motion-consistency check (implied an impossible groundspeed) --
        # distinct from merely being held pending a pair. Surfaced to
        # _update_flight for logging with a flight_id.
        position_rejected = self._pipe_decoder._stats["position_rejected"] > rejected_before

        # Rejects only a genuine DF17/18 CRC failure (crc_valid is False).
        # DF5/20/21 report crc_valid=None -- pyModeS can't verify them
        # without an ICAO hint we don't supply, and a hint wouldn't verify
        # anything anyway. Those messages instead get the `verified` flag
        # below, and callers apply extra scrutiny to squawk/ident, the
        # fields known to fabricate plausible garbage from corrupted bits.
        if result.get("crc_valid") is False:
            return None

        # True only for a genuinely CRC-verified DF17/18 message; DF5/20/21
        # always reports None. Attached alongside squawk/ident, the fields
        # known to leak fabricated values from corrupted bits.
        verified = result.get("crc_valid") is True

        data: dict = {"icao_hex": msg.icao_hex}

        if result.get("squawk") is not None:
            data["squawk"] = result["squawk"]
            data["verified"] = verified

        if result.get("callsign") is not None:
            # pyModeS 3.x uses "#" (not "_") as its invalid-character
            # sentinel for undecodable callsign slots — reject rather than
            # store a partially-garbage ident.
            ident = result["callsign"].strip()
            if ident and "#" not in ident:
                data["ident"] = ident
                data["verified"] = verified

        canonical_wtc = _WAKE_TURBULENCE_MAP.get(result.get("wake_vortex"))
        if canonical_wtc:
            data["wake_turbulence_category"] = canonical_wtc

        # Raw emitter category (see _emitter_category_code): identification
        # messages are TC 1-4 -> set D/C/B/A, so set_index = 4 - typecode.
        typecode = result.get("typecode")
        category = result.get("category")
        if isinstance(typecode, int) and 1 <= typecode <= 4 and isinstance(category, int):
            emitter_category = _emitter_category_code(4 - typecode, category)
            if emitter_category:
                data["emitter_category"] = emitter_category

        # Position/altitude are trusted only from a CRC-verified message --
        # DF5/20/21 can fabricate them too, but unlike squawk/ident there's
        # no repeat-sighting path for a lat/lon pair, so unverified messages
        # never populate these fields at all.
        if verified:
            if result.get("latitude") is not None:
                lat, lon = result["latitude"], result["longitude"]
                if _MIN_LATITUDE <= lat <= _MAX_LATITUDE and _MIN_LONGITUDE <= lon <= _MAX_LONGITUDE:
                    # Same precision cap as Position._cap_coordinate_precision,
                    # applied here so every downstream consumer (archive, map,
                    # rules) sees consistent values from one place.
                    data["latitude"] = round(lat, 5)
                    data["longitude"] = round(lon, 5)

            altitude = result.get("altitude")
            if altitude is not None and _MIN_ALTITUDE_FT <= altitude <= _MAX_ALTITUDE_FT:
                data["altitude"] = altitude

        # subtype 1/2 (GPS): groundspeed + track; subtype 3/4 (airspeed):
        # airspeed + heading. `or` would mishandle a genuine 0 kt/0 deg
        # reading, so check for None explicitly, not truthiness.
        velocity = result.get("groundspeed")
        if velocity is None:
            velocity = result.get("airspeed")
        if velocity is not None:
            data["velocity"] = velocity

        heading = result.get("track")
        if heading is None:
            heading = result.get("heading")
        if heading is not None:
            # Same precision cap as Velocity._cap_heading_precision.
            data["heading"] = round(heading, 1)

        if result.get("vertical_rate") is not None:
            data["vertical_speed"] = result["vertical_rate"]

        if result.get("version") is not None:
            data["adsb_version"] = result["version"]

        if position_rejected:
            # Transient marker, not a real field -- popped and logged by
            # _update_flight, then discarded.
            data["_position_rejected"] = True

        return data if len(data) > 1 else None

    def _decode_978(self, msg: InboundMessage) -> Optional[dict]:
        """
        Decode a raw UAT frame via pyModeS978. Same pure field-presence
        extraction style as _decode_1090. msg.raw already carries the
        dump978-fa -/+ direction prefix pyModeS978.decode() expects — no
        stripping needed.
        """
        try:
            result = pyModeS978.decode(msg.raw)
        except pyModeS978.DecodeError:
            return None
        if result is None:
            # Uplink frame (FIS-B weather/NOTAM) — no traffic data, not an error.
            return None

        data: dict = {"icao_hex": msg.icao_hex}

        if result.get("squawk") is not None:
            data["squawk"] = result["squawk"]

        if result.get("callsign") is not None:
            # UAT's base40 callsign alphabet has no invalid-character
            # sentinel (unlike pyModeS 3.x's "#") — every index decodes to
            # a defined character, so just strip and check non-empty.
            ident = result["callsign"].strip()
            if ident:
                data["ident"] = ident

        category = result.get("category")
        if category is not None:
            canonical_wtc = _WAKE_TURBULENCE_MAP.get(category.name)
            if canonical_wtc:
                data["wake_turbulence_category"] = canonical_wtc

            # Raw emitter category (see _emitter_category_code): the UAT
            # category byte is 8 values per set -> set A/B/C/D.
            emitter_category = _emitter_category_code(int(category) // 8, int(category) % 8)
            if emitter_category:
                data["emitter_category"] = emitter_category

        if result.get("latitude") is not None:
            lat, lon = result["latitude"], result["longitude"]
            if _MIN_LATITUDE <= lat <= _MAX_LATITUDE and _MIN_LONGITUDE <= lon <= _MAX_LONGITUDE:
                # Same precision cap as Position._cap_coordinate_precision,
                # applied here so every downstream consumer (archive, map,
                # rules) sees consistent values from one place.
                data["latitude"] = round(lat, 5)
                data["longitude"] = round(lon, 5)

        altitude = result.get("altitude")
        if altitude is not None and _MIN_ALTITUDE_FT <= altitude <= _MAX_ALTITUDE_FT:
            data["altitude"] = altitude

        if result.get("groundspeed") is not None:
            data["velocity"] = result["groundspeed"]

        heading = result.get("track")
        if heading is None:
            heading = result.get("heading")
        if heading is not None:
            # Same precision cap as Velocity._cap_heading_precision.
            data["heading"] = round(heading, 1)

        if result.get("vertical_rate") is not None:
            data["vertical_speed"] = result["vertical_rate"]

        if result.get("version") is not None:
            data["adsb_version"] = result["version"]

        return data if len(data) > 1 else None

    def _update_flight(self, data: dict, msg: InboundMessage) -> None:
        self._message_clock = max(self._message_clock, msg.received_at)

        flight = Flight(self._db)
        exists = flight.load(data["icao_hex"])

        if exists:
            ttl = self._flight_ttl_seconds
            if msg.received_at - flight.last_message > ttl:
                # This gap means the loaded flight already ended (replay
                # or live) -- archive it and start fresh rather than
                # extending a flight that's actually over.
                completed = flight.to_completed_flight(
                    load_all=True, load_raw_frames=self._capture_raw_frames,
                )
                flight.delete()
                self._db.commit()
                self._archive(completed)
                self._maybe_publish_raw_frames(completed)
                flight = Flight(self._db)
                exists = False

        if not exists:
            flight.icao_hex = data["icao_hex"].upper()
            flight.flight_id = generate_flight_id()
            flight.first_message = msg.received_at
            self._enrich_aircraft(flight)

        if self._capture_raw_frames:
            # Unconditional for every message while the flag is on, decoded
            # or not. len(data) > 1 means something was extracted; exactly
            # 1 means decode failure or a message with nothing to parse --
            # deliberately not distinguished.
            flight.add_raw_frame(RawFrame(
                timestamp=msg.received_at,
                source=msg.source,
                raw=msg.raw,
                decoded=len(data) > 1,
            ))

        if msg.source not in flight.receiver_sources:
            flight.receiver_sources.append(msg.source)

        # A message can still arrive out of order (cross-receiver skew) --
        # only ever advance last_message forward. Letting an older message
        # roll it back would corrupt the next message's gap/TTL check,
        # potentially force-archiving a flight that never actually ended.
        out_of_order = exists and msg.received_at < flight.last_message
        if not out_of_order:
            flight.last_message = msg.received_at
        flight.total_messages += 1

        if data.pop("_position_rejected", False):
            # PipeDecoder's own motion-consistency check rejected this
            # position as implausible relative to recent history -- an
            # actual rejection, not just an observation, so `data` never
            # carried a lat/lon for this message.
            logger.warning(
                "PipeDecoder rejected an implausible 1090 CPR position for "
                "%s (flight_id=%s, ident=%s): raw=%s -- see #1841",
                data["icao_hex"], flight.flight_id, flight.ident or "unknown",
                msg.raw,
            )

        if "latitude" in data and "longitude" in data:
            flight.add_position(Position(
                timestamp=msg.received_at,
                latitude=data["latitude"],
                longitude=data["longitude"],
                altitude=data.get("altitude"),
            ))

        if "velocity" in data:
            flight.add_velocity(Velocity(
                timestamp=msg.received_at,
                velocity=data.get("velocity"),
                heading=data.get("heading"),
                vertical_speed=data.get("vertical_speed"),
            ))

        # Throttled per aircraft; no-op when MAP_UDP_HOST is unset. Skipped
        # for an out-of-order message -- unlike the lag check, this catches
        # a message that's "fresh enough" but older than what's already
        # shown, which would visibly snap the aircraft backward on the map.
        if not out_of_order:
            self._publish_map_position(flight, data, msg.received_at)

        if "squawk" in data and not flight.squawk:
            squawk = str(data["squawk"])
            if data.get("verified", True) or squawk not in _RESERVED_SQUAWKS:
                # Verified source, or an ordinary value where a lone
                # corrupted reading has no real consequence -- trust on
                # first sighting.
                flight.squawk = squawk
                flight.pending_squawk = None
            else:
                # Reserved/emergency code from an unverifiable message --
                # corrupted bits disproportionately land on these values,
                # so require multiple sightings before committing.
                flight.pending_squawk, confirmed = _confirm_after_repeated_sightings(
                    flight.pending_squawk, squawk, msg.received_at,
                )
                if confirmed:
                    flight.squawk = squawk
                    flight.pending_squawk = None

        if "wake_turbulence_category" in data:
            # Live decode is the sole writer (see _WAKE_TURBULENCE_MAP), so
            # each new reading just replaces the last -- no first-wins guard.
            flight.aircraft["wake_turbulence_category"] = data["wake_turbulence_category"]

        if "ident" in data and not flight.ident:
            ident = data["ident"]
            if ident and ident != "00000000":
                if data.get("verified", True):
                    # DF17/18 TC1-4 -- genuinely CRC-verified.
                    flight.ident = ident
                    flight.pending_ident = None
                    self._enrich_operator(flight)
                else:
                    # DF20/21 Comm-B -- unlike squawk, there's no "safe"
                    # subset to exempt, so every unverified ident needs
                    # confirmation, using its own more lenient count/window
                    # (see shared/timing.py).
                    flight.pending_ident, confirmed = _confirm_after_repeated_sightings(
                        flight.pending_ident, ident, msg.received_at,
                        window_seconds=IDENT_CONFIRM_WINDOW_SECONDS,
                        required_count=IDENT_CONFIRM_COUNT,
                    )
                    if confirmed:
                        flight.ident = ident
                        flight.pending_ident = None
                        self._enrich_operator(flight)

        if "adsb_version" in data:
            flight.aircraft.setdefault("adsb_version", data["adsb_version"])

        if "emitter_category" in data:
            # Receiver-decoded, like adsb_version -- stable, so first
            # sighting wins. Kept in the aircraft dict so it reaches the
            # map UDP metadata payload's `aircraft` sub-object.
            flight.aircraft.setdefault("emitter_category", data["emitter_category"])

        self._maybe_resolve_route(flight)

        # Map UDP `metadata` message -- only when ident/aircraft/operator/
        # registrant/squawk/origin/destination/matched_rules have changed
        # since the last send; no-op when MAP_UDP_HOST is unset.
        self._maybe_publish_map_metadata(flight, msg.received_at)

        # Nanoseconds, not milliseconds -- evaluate() is in-process, no-I/O,
        # almost always sub-millisecond, so ms resolution would lose signal.
        t_rules = time.monotonic()
        matched = self._rules_engine.evaluate(flight)
        rules_ns = (time.monotonic() - t_rules) * 1e9
        self._rules_time.record_hwm(rules_ns)

        for rule in matched:
            flight.matched_rules.append(rule["identifier"])
            # evaluate() skips a rule already in matched_rules, so this
            # runs once per (flight, rule).
            self._rule_trigger_counts.record(rule["identifier"])
            if rule.get("force_archive"):
                flight.force_archive = True
            self._publish_rule_notification(flight, rule, msg.received_at)

        flight.save()

    # ------------------------------------------------------------------
    # Enrichment
    # ------------------------------------------------------------------

    def _enrich_aircraft(self, flight: Flight) -> None:
        try:
            raw = self._redis.evalsha(self._merge_sha, 0, flight.icao_hex)
            if raw:
                aircraft = json.loads(raw)
                # Registry/Mictronics data must never seed wake_turbulence_category —
                # it's receiver-decode-only (see _WAKE_TURBULENCE_MAP above).
                aircraft.pop("wake_turbulence_category", None)
                # registrant describes the aircraft's legal owner, not the
                # airframe -- same category as flight.operator, so it's its
                # own sibling field rather than nested here.
                flight.registrant = aircraft.pop("registrant", None) or {}
                flight.aircraft = aircraft
            else:
                self._registration_misses.record()
                if not flight.aircraft:
                    flight.aircraft = {"icao_hex": flight.icao_hex}
        except Exception as exc:
            logger.debug("Redis enrichment (aircraft) error: %s", exc)
            if not flight.aircraft:
                flight.aircraft = {"icao_hex": flight.icao_hex}

    def _enrich_operator(self, flight: Flight) -> None:
        ident = flight.ident

        # Skip US tail numbers
        if _US_REG_RE.match(ident):
            return

        # Skip if ident matches registration
        if _ident_matches_registration(ident, flight.aircraft):
            return

        # Extract ICAO airline prefix (letters before first digit)
        prefix = re.split(r"[^a-zA-Z]", ident)[0]
        if len(prefix) < 2:
            return

        try:
            # operator:{designator} is a RedisJSON document (same write path
            # as every other enrichment key) -- a plain GET raises WRONGTYPE
            # against it, silently swallowed by the bare except below.
            raw = self._redis.json().get(operator_key(prefix))
            if raw:
                flight.operator = raw
            else:
                self._operator_misses.record()
        except Exception as exc:
            logger.debug("Redis enrichment (operator) error: %s", exc)

    # ------------------------------------------------------------------
    # Route leg resolution (origin/destination)
    # ------------------------------------------------------------------

    def _route_ready(self, flight: Flight) -> bool:
        """True once ident, position, altitude, and heading have all been
        seen at least once for this flight, in any order -- checked against
        full SQLite history, not just what Flight.load(limit=True) loaded
        into memory for this message."""
        if not flight.ident or _ident_matches_registration(flight.ident, flight.aircraft):
            return False

        # altitude is the one field sometimes absent from a stored position
        # (e.g. surface typecodes) -- requiring it also guarantees a
        # lat/lon has been received.
        cur = self._db.cursor()
        cur.execute(
            "SELECT 1 FROM positions WHERE icao_hex=? AND altitude IS NOT NULL LIMIT 1",
            (flight.icao_hex,),
        )
        if cur.fetchone() is None:
            return False

        cur.execute(
            "SELECT 1 FROM velocities WHERE icao_hex=? AND heading IS NOT NULL LIMIT 1",
            (flight.icao_hex,),
        )
        return cur.fetchone() is not None

    def _maybe_resolve_route(self, flight: Flight) -> None:
        """Runs route:{ident} leg resolution at most once per flight, as
        soon as ident/position/altitude/heading are all available -- not at
        archive time, so later rule/notification logic in the same flight
        can see origin/destination too. See route_resolver and
        message-processor/README.md's "Route Leg Resolution" section.

        route_resolution_attempted is set only once the result is final
        (route_resolver.resolve_origin_destination's is_final), except a
        route whose heading hasn't stabilized yet is re-evaluated on later
        messages -- route_candidate_airports caches the fetched airports so
        that re-evaluation skips the Redis round trip. All-or-nothing: both
        fields stay None on any unresolved/failed case."""
        if flight.route_resolution_attempted or not self._route_ready(flight):
            return

        if flight.route_candidate_airports is not None:
            airports = json.loads(flight.route_candidate_airports)
        else:
            try:
                raw = self._redis.evalsha(
                    self._route_sha, 0, normalize_flight_ident(flight.ident)
                )
                airports = json.loads(raw) if raw else []
            except Exception as exc:
                logger.debug("Redis route resolution error: %s", exc)
                flight.route_resolution_attempted = True
                return
            flight.route_candidate_airports = json.dumps(airports)

        if len(airports) < 2:
            flight.route_resolution_attempted = True
            logger.debug(
                "Route resolution: no usable route for ident %s (icao_hex=%s); "
                "route_airports.lua returned: %s",
                flight.ident, flight.icao_hex, airports,
            )
            return

        # flight.positions/velocities may hold only the most recent row
        # (Flight.load's default limit=True) -- the low-altitude heuristic
        # needs the earliest position, so reload full history.
        flight._load_positions(limit=False)
        flight._load_velocities(limit=False)
        positions = [p.to_dict() for p in flight.positions]
        velocities = [v.to_dict() for v in flight.velocities]

        origin, destination, is_final, reason = resolve_origin_destination(
            airports, positions, velocities
        )
        if not is_final:
            return  # heading not yet stable enough to trust -- try again later

        flight.route_resolution_attempted = True
        if origin and destination:
            flight.origin = origin
            flight.destination = destination
        else:
            logger.debug(
                "Route resolution rejected for ident %s (icao_hex=%s): %s. "
                "route_airports.lua returned: %s",
                flight.ident, flight.icao_hex, reason, airports,
            )

    # ------------------------------------------------------------------
    # Stale flight eviction
    # ------------------------------------------------------------------

    def _eviction_loop(self) -> None:
        while not self._shutdown.is_set():
            time.sleep(10)
            # SIGUSR1 (main()) only sets this event -- the decommission
            # work runs here, not in the signal handler itself.
            if self._force_evict.is_set():
                self._decommission()
                return
            self._evict_stale()

    def _evict_stale(self) -> None:
        ttl = self._flight_ttl_seconds
        with self._db_lock:
            # message_clock, not wall-clock time, gates eviction -- after a
            # restart it only advances as the backlog drains, so recovered
            # flights aren't archived just because real time passed.
            cutoff = self._message_clock - ttl
            cur = self._db.cursor()
            cur.execute("SELECT icao_hex FROM flights WHERE last_message < ?", (cutoff,))
            stale = [row[0] for row in cur.fetchall()]

        for icao_hex in stale:
            self._evict_flight(icao_hex)

    def _force_evict_all(self) -> None:
        """Unconditional counterpart to _evict_stale(): forces every active
        flight through the same eviction path regardless of TTL. Used only
        by the SIGUSR1 decommission sequence (_decommission())."""
        with self._db_lock:
            cur = self._db.cursor()
            cur.execute("SELECT icao_hex FROM flights")
            active = [row[0] for row in cur.fetchall()]

        logger.warning(
            "Decommission: force-evicting %d active flight(s) regardless of TTL…",
            len(active),
        )
        for icao_hex in active:
            self._evict_flight(icao_hex)

    def _evict_flight(self, icao_hex: str) -> None:
        """Shared per-flight eviction body, reused by _evict_stale()'s
        TTL-gated sweep and _force_evict_all()'s decommission sweep so the
        two paths can't drift apart."""
        with self._db_lock:
            flight = Flight(self._db)
            if not flight.load(icao_hex, limit=False):
                return
            completed = flight.to_completed_flight(
                load_all=False, load_raw_frames=self._capture_raw_frames,
            )
            flight.delete()
            self._db.commit()

        self._archive(completed)
        self._maybe_publish_raw_frames(completed)

    def _archive(self, flight: CompletedFlight) -> None:
        """Queue a completed flight for the archive processor. May be
        called from the connection's own thread or the eviction thread --
        self._rmq_channel/_rmq_connection must only be touched by the
        former, so every call routes through add_callback_threadsafe
        uniformly. The scheduled callback decides success/failure and
        falls back to self._fallback.put() itself.

        exclude={"raw_frames"} is unconditional, regardless of
        self._capture_raw_frames: this is the only path that reaches the
        permanent archive/S3, so it always drops the field here. Raw
        frames only ever travel via _maybe_publish_raw_frames()'s separate
        short-lived queue."""
        payload = flight.model_dump_json(by_alias=True, exclude_none=True, exclude={"raw_frames"})

        def _publish_on_rmq_thread() -> None:
            try:
                self._rmq_channel.basic_publish(
                    exchange="",
                    routing_key=ARCHIVE_QUEUE_NAME,
                    body=payload.encode(),
                    properties=pika.BasicProperties(delivery_mode=2),
                    mandatory=True,
                )
            except pika.exceptions.UnroutableError:
                # The archive queue doesn't exist (e.g. archive-processor
                # not installed) -- the connection itself is healthy, so
                # don't latch _rmq_connected False for this.
                self._fallback.put(payload)
            except Exception:
                self._rmq_connected = False
                self._fallback.put(payload)

        if self._rmq_connected and self._rmq_connection and self._rmq_channel:
            try:
                self._rmq_connection.add_callback_threadsafe(_publish_on_rmq_thread)
                return
            except Exception:
                self._rmq_connected = False

        self._fallback.put(payload)

    def _maybe_publish_raw_frames(self, flight: CompletedFlight) -> None:
        """Companion to _archive(), called alongside every call site. A
        no-op unless CAPTURE_RAW_FRAMES is on and this flight actually
        captured frames."""
        if self._capture_raw_frames and flight.raw_frames:
            self._publish_raw_frames(flight)

    def _publish_raw_frames(self, flight: CompletedFlight) -> None:
        """Best-effort publish (raw_frames intact) to the short-lived
        forensic raw-frames queue. Unlike _archive(), a failure here never
        falls back to SQLite or touches self._rmq_connected -- losing a
        debug record is not worth retrying over, so this just logs and
        moves on."""
        payload = flight.model_dump_json(by_alias=True, exclude_none=True)

        def _publish_on_rmq_thread() -> None:
            try:
                self._rmq_channel.basic_publish(
                    exchange="",
                    routing_key=RAW_FRAMES_QUEUE_NAME,
                    body=payload.encode(),
                )
            except Exception as exc:
                logger.debug(
                    "Raw-frames publish failed (best-effort, not retried): %s", exc,
                )

        if not (self._rmq_connected and self._rmq_connection and self._rmq_channel):
            logger.debug("Raw-frames publish skipped: RabbitMQ not connected.")
            return
        try:
            self._rmq_connection.add_callback_threadsafe(_publish_on_rmq_thread)
        except Exception as exc:
            logger.debug("Raw-frames publish scheduling failed: %s", exc)

    def _drain_fallback(self) -> None:
        """FallbackQueue.drain() calls process_fn(payload) synchronously
        and decides retry/dead-letter based on whether it raises, so this
        can't fire-and-forget like _archive() does. It schedules the real
        publish via add_callback_threadsafe and blocks on a threading.Event
        the callback sets once it's attempted the publish, re-raising
        whatever it recorded -- preserving drain()'s synchronous contract
        while the socket write happens on the connection's own thread."""
        def publish(payload: str) -> None:
            connection = self._rmq_connection
            if not connection:
                raise RuntimeError("RabbitMQ connection unavailable")

            done = threading.Event()
            outcome: dict = {}

            def _publish_on_rmq_thread() -> None:
                try:
                    self._rmq_channel.basic_publish(
                        exchange="",
                        routing_key=ARCHIVE_QUEUE_NAME,
                        body=payload.encode(),
                        properties=pika.BasicProperties(delivery_mode=2),
                        mandatory=True,
                    )
                except Exception as exc:
                    outcome["error"] = exc
                finally:
                    done.set()

            try:
                connection.add_callback_threadsafe(_publish_on_rmq_thread)
            except Exception:
                self._rmq_connected = False
                raise

            done.wait()
            if "error" in outcome:
                # An unroutable archive queue is a healthy-connection
                # condition, not a connection failure -- see the matching
                # exception classification in _archive()'s publish closure.
                if not isinstance(outcome["error"], pika.exceptions.UnroutableError):
                    self._rmq_connected = False
                raise outcome["error"]

        self._fallback.drain_in_background(publish)

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    def _connect_mqtt(self) -> None:
        mc = self._cfg.get("mqtt")
        if not mc:
            return
        lwtopic = f"SkyFollower/message-processor/{self._id}/status"
        self._mqtt = build_mqtt_client(mc, will_topic=lwtopic)
        self._mqtt.on_connect = self._on_mqtt_connect
        self._mqtt.on_disconnect = self._on_mqtt_disconnect
        try:
            self._mqtt.connect_async(mc["host"], port=mc.get("port", 1883), keepalive=60)
            self._mqtt.loop_start()
        except Exception as exc:
            logger.warning("MQTT connect failed: %s", exc)

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties) -> None:
        self._mqtt_connected = True
        client.publish(f"SkyFollower/message-processor/{self._id}/status", "ONLINE", retain=True)
        self._publish_ha_autodiscovery()
        logger.info("MQTT connected.")

    def _on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self._mqtt_connected = False

    def _build_flight_notification_payload(self, flight: Flight) -> dict:
        """CompletedFlight-shape payload shared by the MQTT rule
        notification and the map UDP `metadata` message: positions/
        velocities/raw_frames/_id popped, empty optional fields omitted.
        Callers add their own key on top (MQTT's `rule`, map UDP's `type`).

        raw_frames is popped explicitly rather than relied upon to stay
        empty, since with CAPTURE_RAW_FRAMES on, `flight` may already carry
        this message's just-added frame in memory."""
        notification = flight.to_completed_flight().model_dump(
            by_alias=True, mode="json", exclude_none=True
        )
        notification.pop("positions", None)
        notification.pop("velocities", None)
        notification.pop("raw_frames", None)
        notification.pop("_id", None)
        if not notification.get("operator"):
            notification.pop("operator", None)
        if not notification.get("registrant"):
            notification.pop("registrant", None)
        if not notification.get("origin"):
            notification.pop("origin", None)
        if not notification.get("destination"):
            notification.pop("destination", None)
        if not notification.get("force_archive"):
            notification.pop("force_archive", None)
        return notification

    def _publish_rule_notification(self, flight: Flight, rule: dict, received_at: float) -> None:
        lag = time.time() - received_at
        if lag > MAX_MESSAGE_LAG_SECONDS:
            logger.debug(
                "Suppressing MQTT rule notification for %s (rule=%s): "
                "message is %.1fs old (backlog replay)",
                flight.icao_hex, rule["identifier"], lag,
            )
            return
        if not (self._mqtt and self._mqtt_connected):
            return
        notification = self._build_flight_notification_payload(flight)
        notification["rule"] = {
            "name": rule.get("name", ""),
            "description": rule.get("description", ""),
            "identifier": rule["identifier"],
        }
        self._mqtt.publish(
            f"SkyFollower/rule/{rule['identifier']}",
            json.dumps(notification, default=str),
        )

    # ------------------------------------------------------------------
    # Map UDP publisher -- fire-and-forget position/metadata/heartbeat
    # feed toward the map service.
    # ------------------------------------------------------------------

    def _publish_map_position(self, flight: Flight, data: dict, received_at: float) -> None:
        """Throttled to at most one per MAP_UDP_POSITION_MIN_INTERVAL_SECONDS
        per flight (see should_send_position). Fields absent from `data`
        are omitted, not sent as null. Carries `processor_id` so the map
        service's liveness roster updates from ordinary traffic too, not
        just the dedicated heartbeat.

        Throttle is keyed on `received_at`, not wall-clock time, so replay
        stays stable. `metadata` sends are never throttled here -- they're
        already change-gated on their own terms."""
        if not self._map_udp.enabled:
            return
        lag = time.time() - received_at
        if lag > MAX_MESSAGE_LAG_SECONDS:
            logger.debug(
                "Suppressing map UDP position for %s: message is %.1fs old (backlog replay)",
                flight.icao_hex, lag,
            )
            return
        if not self._map_udp.should_send_position(flight.icao_hex, received_at):
            return
        payload = {
            "type": "position",
            "icao_hex": flight.icao_hex,
            "ts": received_at,
            "processor_id": self._id,
        }
        for key, short_key in (
            ("latitude", "lat"),
            ("longitude", "lon"),
            ("altitude", "alt"),
            ("velocity", "velocity"),
            ("heading", "hdg"),
            ("vertical_speed", "vs"),
        ):
            if key in data:
                payload[short_key] = data[key]
        self._map_udp.send(payload)

    def _maybe_publish_map_metadata(self, flight: Flight, received_at: float) -> None:
        """Sent the first time a flight's metadata fields are known, and
        again only when one changes -- never on every message. See
        _flight_metadata_snapshot for the change-detection approach."""
        if not self._map_udp.enabled:
            return
        lag = time.time() - received_at
        if lag > MAX_MESSAGE_LAG_SECONDS:
            logger.debug(
                "Suppressing map UDP metadata for %s: message is %.1fs old (backlog replay)",
                flight.icao_hex, lag,
            )
            return
        snapshot = _flight_metadata_snapshot(flight)
        if snapshot == flight.map_metadata_hash:
            return
        payload = self._build_flight_notification_payload(flight)
        payload["type"] = "metadata"
        payload["processor_id"] = self._id
        self._map_udp.send(payload)
        flight.map_metadata_hash = snapshot

    def _map_heartbeat_loop(self) -> None:
        """Fixed MAP_HEARTBEAT_INTERVAL_SECONDS liveness beacon toward the
        map service, independent of aircraft traffic and unrelated to
        _heartbeat_loop's Redis NX guard below. No-op when the map UDP
        feed is disabled.

        Skipped if a position/metadata datagram already went out (any
        aircraft, one shared publisher) within the interval -- a busy
        processor's own traffic already proves liveness. Not gated by
        MAX_MESSAGE_LAG_SECONDS, unlike position/metadata, since this is
        about the processor being up now, not a message's recency."""
        while not self._shutdown.is_set():
            time.sleep(MAP_HEARTBEAT_INTERVAL_SECONDS)
            if not self._map_udp.enabled:
                continue
            last_sent_at = self._map_udp.last_sent_at
            if last_sent_at is not None and time.time() - last_sent_at < MAP_HEARTBEAT_INTERVAL_SECONDS:
                continue
            self._map_udp.send({
                "type": "heartbeat",
                "processor_id": self._id,
                "ts": time.time(),
            })

    def _resend_all_map_metadata(self) -> None:
        """Unconditional counterpart to _maybe_publish_map_metadata: resends
        every active flight's metadata regardless of whether anything
        changed. Deliberately does not touch flight.map_metadata_hash,
        which stays owned by the change-gated path -- a real change still
        goes out immediately, not just on this periodic tick."""
        if not self._map_udp.enabled:
            return
        with self._db_lock:
            cur = self._db.cursor()
            cur.execute("SELECT icao_hex FROM flights")
            active = [row[0] for row in cur.fetchall()]

        for icao_hex in active:
            with self._db_lock:
                flight = Flight(self._db)
                if not flight.load(icao_hex):
                    continue
                payload = self._build_flight_notification_payload(flight)
            payload["type"] = "metadata"
            payload["processor_id"] = self._id
            self._map_udp.send(payload)

    def _map_metadata_resend_loop(self) -> None:
        """Dedicated thread driving _resend_all_map_metadata() every
        MAP_METADATA_RESEND_INTERVAL_SECONDS. Kept separate from
        _telemetry_loop so this map-facing cadence never has to be
        reasoned about jointly with an unrelated MQTT-publish cadence.
        _resend_all_map_metadata() applies its own enabled-guard, so
        there's no gating here."""
        while not self._shutdown.is_set():
            time.sleep(MAP_METADATA_RESEND_INTERVAL_SECONDS)
            self._resend_all_map_metadata()

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------

    def _telemetry_loop(self) -> None:
        while not self._shutdown.is_set():
            time.sleep(MQTT_PUBLISH_INTERVAL_SECONDS)
            # Independent of _consume_loop's reconnect-triggered drain: a
            # publish failure can leave messages queued without ever
            # raising AMQPConnectionError. Cheap no-op when empty;
            # _drain_fallback() spawns the actual drain in the background,
            # so this never delays the telemetry publish below.
            if self._rmq_connected:
                self._drain_fallback()
            else:
                # Can also be latched False by a publish failure on a
                # still-open connection -- break it so _consume_loop
                # rebuilds and re-validates.
                self._force_rmq_reconnect_if_stale()
            self._flush_period_counters()
            self._flush_rule_trigger_counts()
            self._publish_telemetry()

    def _flush_period_counters(self) -> None:
        """Pushes each in-memory counter's delta into Redis --
        incr_period_counter.lua for hour/today (resets at the UTC boundary),
        plain INCRBY for lifetime. Called only from the telemetry thread.
        Not self-published via MQTT/HA -- core-health reads these keys and
        publishes them on this component's behalf."""
        now = datetime.now(timezone.utc)
        for accumulator, key_fn, periods in (
            (self._total_messages_processed, metrics_total_messages_processed_key,
             ("hour", "today", "lifetime")),
            (self._registration_misses, metrics_registration_misses_key,
             ("hour", "today", "lifetime")),
            (self._operator_misses, metrics_operator_misses_key, ("today", "lifetime")),
        ):
            delta = accumulator.flush_and_reset()
            if not delta:
                continue
            try:
                for period in periods:
                    if period == "lifetime":
                        self._redis.incrby(key_fn(self._id, period), delta)
                    else:
                        self._redis.evalsha(
                            self._incr_period_counter_sha, 0,
                            key_fn(self._id, period), delta, next_period_boundary(period, now),
                        )
            except Exception as exc:
                logger.debug("Period counter flush failed for %s: %s", key_fn(self._id, "lifetime"), exc)

    def _flush_rule_trigger_counts(self) -> None:
        """Push each rule's trigger delta into Redis: INCRBY on the
        lifetime key, plus INCRBY + EXPIRE on today's UTC day key, one
        pipelined round trip. Fails soft -- a failed flush just loses that
        cycle's counts, acceptable for a display-only metric."""
        deltas = self._rule_trigger_counts.flush_and_reset()
        if not deltas:
            return
        today = datetime.now(timezone.utc).date().isoformat()
        try:
            pipe = self._redis.pipeline()
            for identifier, delta in deltas.items():
                pipe.incrby(rule_trigger_lifetime_key(identifier), delta)
                day_key = rule_trigger_day_key(identifier, today)
                pipe.incrby(day_key, delta)
                pipe.expire(day_key, RULE_TRIGGER_DAY_TTL_SECONDS)
            pipe.execute()
        except Exception as exc:
            logger.debug("Rule trigger counter flush failed: %s", exc)

    def _publish_telemetry(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return

        pid = self._id

        # No self._db_lock: WAL mode gives this standalone read snapshot
        # isolation without blocking (or being blocked by) per-message
        # writes under that lock.
        cur = self._db.cursor()
        cur.execute("SELECT COUNT(*) FROM flights")
        active = cur.fetchone()[0]

        processing_hwm = self._processing_time.hwm_ms_and_reset()
        self._processing_time.reset()
        rules_hwm_ns = self._rules_time.hwm_ms_and_reset()
        message_latency_hwm = self._message_latency.hwm_ms_and_reset()

        base = f"SkyFollower/message-processor/{pid}/statistic"

        self._mqtt.publish(f"{base}/started_at", self._started_at, retain=True)
        self._mqtt.publish(f"{base}/messages_per_second", str(round(self._rate.rate(), 2)), retain=True)
        self._mqtt.publish(f"{base}/processing_time_hwm_ms", str(processing_hwm), retain=True)
        self._mqtt.publish(f"{base}/message_latency_hwm_ms", str(message_latency_hwm), retain=True)
        self._mqtt.publish(f"{base}/rules_engine_hwm_ns", str(rules_hwm_ns), retain=True)

        self._mqtt.publish(f"{base}/local_archive_queue_depth", str(self._fallback.depth()), retain=True)
        self._mqtt.publish(
            f"{base}/dead_letter_queue_depth", str(self._fallback.dead_letter_depth()), retain=True
        )
        self._mqtt.publish(f"{base}/active_flights", str(active), retain=True)
        self._mqtt.publish(
            f"{base}/rabbitmq_connected", str(self._rmq_connected), retain=True
        )

        # Last-8-chars short-hash, for a compact HA read and direct
        # comparison against core-health's canonical sensor. Truncation
        # happens only at this publish boundary; the engine compares full
        # hashes internally.
        self._mqtt.publish(
            f"{base}/rules_version", _short_hash(self._rules_engine.rules_version), retain=True
        )
        self._mqtt.publish(
            f"{base}/areas_version", _short_hash(self._rules_engine.areas_version), retain=True
        )

        # registration_misses/operator_misses/total_messages_processed are
        # write-only here -- core-health reads those Redis keys and
        # publishes them on this component's behalf instead.

        # Refresh heartbeat
        try:
            self._redis.expire(
                message_processor_heartbeat_key(self._id), HEARTBEAT_TTL_SECONDS
            )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Config polling
    # ------------------------------------------------------------------

    def _config_poll_loop(self) -> None:
        while not self._shutdown.is_set():
            time.sleep(CONFIG_POLL_INTERVAL_SECONDS)
            try:
                self._rules_engine.reload_if_changed()
            except Exception as exc:
                logger.debug("Config poll error: %s", exc)

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
    # Heartbeat (keeps Redis NX key alive)
    # ------------------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        while not self._shutdown.is_set():
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)
            try:
                self._redis.expire(
                    message_processor_heartbeat_key(self._id), HEARTBEAT_TTL_SECONDS
                )
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Docker healthcheck (heartbeat file, distinct from the Redis
    # NX-key heartbeat above)
    # ------------------------------------------------------------------

    def _healthcheck_loop(self) -> None:
        """Touch a heartbeat file while genuinely connected to RabbitMQ, for
        Docker's HEALTHCHECK to check the mtime of. Runs at
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
        pid = self._id
        device = build_ha_device(
            identifier=f"SkyFollower_message_processor_{pid}",
            name=f"SkyFollower Message Processor {pid}",
            model="Message Processor",
        )
        publish_register(self._mqtt, device)
        availability = {
            "availability_topic": f"SkyFollower/message-processor/{pid}/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }
        base = f"SkyFollower/message-processor/{pid}/statistic"
        sensors = [
            _Sensor("started_at", "Start Time", "mdi:clock-start", None,
                    extra={"device_class": "timestamp"}),
            _Sensor("messages_per_second", "Message Rate", "mdi:broadcast", "measurement", "msg/s"),
            # suggested_display_precision only rounds the HA display; the
            # retained state stays full precision. 1 decimal here since
            # this metric reads in low single-digit ms.
            _Sensor("processing_time_hwm_ms", "Processing Time HWM", "mdi:clock", "measurement", "ms",
                    extra={"suggested_display_precision": 1}),
            # Same precision rationale as processing_time_hwm_ms -- this
            # metric is a superset of it, so it never reads narrower.
            _Sensor("message_latency_hwm_ms", "Message Latency HWM", "mdi:clock-alert", "measurement", "ms",
                    extra={"suggested_display_precision": 1}),
            # Nanosecond values are large integers; a fractional ns carries
            # no real signal, so 0 decimal places here, unlike above.
            _Sensor("rules_engine_hwm_ns", "Rules Engine HWM", "mdi:clock", "measurement", "ns",
                    extra={"suggested_display_precision": 0}),
            _Sensor("local_archive_queue_depth", "Local Archive Queue Depth", "mdi:tray-full", "measurement"),
            _Sensor("dead_letter_queue_depth", "Dead Letter Queue Depth", "mdi:skull-crossbones", "measurement"),
            _Sensor("active_flights", "Active Flights", "mdi:airplane", "measurement"),
            # Opaque short-hash identifiers, not measurements -- no
            # state_class/unit. Compare against core-health's canonical
            # sensor to see whether this processor is up to date.
            _Sensor("rules_version", "Rules Version", "mdi:file-document-check", None),
            _Sensor("areas_version", "Areas Version", "mdi:map-check", None),
            _Sensor("rabbitmq_connected", "RabbitMQ Connected", "mdi:rabbit", None),
            # registration_misses/operator_misses/total_messages_processed
            # have no entry here -- core-health publishes their HA discovery
            # config on this component's behalf instead.
        ]
        for sensor in sensors:
            payload = {
                **availability,
                "state_topic": f"{base}/{sensor.field}",
                "name": sensor.name,
                "has_entity_name": True,
                "unique_id": f"SkyFollower_message_processor_{pid}_{sensor.field}",
                "object_id": f"SkyFollower_message_processor_{pid}_{sensor.field}",
                "device": device,
                "icon": sensor.icon,
            }
            if sensor.state_class:
                payload["state_class"] = sensor.state_class
            if sensor.unit:
                payload["unit_of_measurement"] = sensor.unit
            if sensor.extra:
                payload.update(sensor.extra)
            self._mqtt.publish(
                f"homeassistant/sensor/SkyFollower_message_processor_{pid}_{sensor.field}/config",
                json.dumps(payload),
                retain=True,
            )

    # ------------------------------------------------------------------
    # Decommissioning (SIGUSR1)
    # ------------------------------------------------------------------

    def _decommission(self) -> None:
        """Runs on the eviction thread once _force_evict is set.
        Force-evicts every active flight, waits indefinitely for the
        retryable archive queue to drain (never the dead-letter queue),
        warns if anything was dead-lettered, then hands off to shutdown()."""
        logger.warning(
            "SIGUSR1 received: decommissioning -- force-evicting all active "
            "flights and waiting for the archive queue to drain…"
        )
        self._force_evict_all()

        while True:
            depth = self._fallback.depth()
            if depth == 0:
                break
            logger.info(
                "Decommission: %d flight(s) still queued for archive "
                "(retryable); waiting indefinitely for this to reach 0…",
                depth,
            )
            time.sleep(10)

        dead_letters = self._fallback.dead_letter_depth()
        if dead_letters:
            logger.critical(
                "Decommission: %d flight(s) are DEAD-LETTERED and will "
                "never be retried -- they require manual attention before "
                "the volume is destroyed.",
                dead_letters,
            )

        logger.warning("Decommission: archive queue drained -- shutting down.")
        self.shutdown()

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        # No eager flush: the active store is durable, so a stop and a
        # crash recover identically on next startup. (SIGUSR1's
        # decommission path force-archives everything itself first.)
        logger.info("Shutdown requested…")
        self._shutdown.set()
        if self._rmq_channel:
            # SIGTERM/SIGINT run on the main thread, same thread blocked
            # inside start_consuming(), so a direct call is safe. SIGUSR1's
            # decommission path calls this from the eviction thread
            # instead, so it must go through add_callback_threadsafe like
            # every other cross-thread touch of self._rmq_channel.
            if threading.current_thread() is threading.main_thread():
                try:
                    self._rmq_channel.stop_consuming()
                except Exception:
                    pass
            elif self._rmq_connection:
                try:
                    self._rmq_connection.add_callback_threadsafe(self._rmq_channel.stop_consuming)
                except Exception:
                    pass
        if self._mqtt:
            self._mqtt.publish(
                f"SkyFollower/message-processor/{self._id}/status", "OFFLINE", retain=True
            )
            self._mqtt.loop_stop()
        self._map_udp.close()
        self._db.close()
        logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        config = load_config(
            "rabbitmq", "redis", "mqtt", "message_processor", "map_udp"
        )
    except ConfigError as exc:
        configure_logging()
        logger.critical("%s", exc)
        sys.exit(1)

    processor = MessageProcessor(config, config["message_processor_id"])

    def _handle_signal(sig, frame):
        processor.shutdown()
        sys.exit(0)

    def _handle_decommission(sig, frame):
        # Deliberately minimal: just flag it. The actual force-evict/
        # drain/shutdown sequence runs on the eviction thread instead,
        # since it can block indefinitely waiting on the fallback queue.
        processor._force_evict.set()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGUSR1, _handle_decommission)

    processor.start()


if __name__ == "__main__":
    main()
