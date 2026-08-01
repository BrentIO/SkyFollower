#!/usr/bin/env python3
"""
SkyFollower Message Processor

Consumes raw ADS-B/UAT messages from a RabbitMQ queue, maintains per-aircraft
flight state in a file-backed SQLite database (survives a process restart),
enriches with Redis lookups, runs the rules engine, publishes MQTT
notifications, and hands completed flights to the archive queue.

One container = one message processor instance.  MESSAGE_PROCESSOR_ID is set
via the environment variable of the same name.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import pathlib
import re
import signal
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
import pyModeS978
import redis as redis_lib

from message_processor.route_resolver import resolve_origin_destination
from message_processor.rules_engine import RulesEngine
from shared.fallback_queue import FallbackQueue
from shared.logging_setup import configure_logging
from shared.models import (
    AircraftRecord,
    CompletedFlight,
    InboundMessage,
    OperatorRecord,
    Position,
    Velocity,
    generate_flight_id,
)
from shared.mqtt import build_mqtt_client
from shared.redis_keys import (
    config_areas_version_key,
    config_flight_ttl_seconds_key,
    config_rules_version_key,
    message_processor_heartbeat_key,
    metrics_aircraft_type_misses_key,
    metrics_registration_misses_key,
    operator_key,
)

logger = logging.getLogger("message_processor")

# ---------------------------------------------------------------------------
# US registration regex (skip operator lookup for tail numbers)
# ---------------------------------------------------------------------------
_US_REG_RE = re.compile(
    r"^[LT]?N[CXR]?[1-9]((\d{0,4})|(\d{0,3}[A-HJ-NP-Z])|(\d{0,2}[A-HJ-NP-Z]{2}))$"
)


# wake_turbulence_category is receiver-decode-only, sourced exclusively from
# live 1090/978 emitter-category data — registry/Mictronics enrichment must
# never seed it. Both decode paths' 7 possible source values
# collapse to just light/medium/heavy: Super is structurally unreachable from
# live data (the ADS-B category subfield has no code point for it), and
# rotorcraft/high-performance are a different axis (emitter type, not wake
# weight class) rather than a value to remap. Keyed by both pyModeS978's
# EmitterCategory member names and pyModeS's own TC=4 wake_vortex strings,
# which describe the same 7 DO-260B categories.
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


def _ident_matches_registration(ident: str, aircraft: dict) -> bool:
    """True when the broadcast ident is just the aircraft's own tail number
    (registration) rather than a route-bearing flight identifier/callsign —
    dash-insensitive, matching SkyFollower-legacy's setIdent()/_getOperator()
    precedent. Shared by operator enrichment and route-leg resolution, since
    both need the same "is this really a flight number" judgment call."""
    registration = aircraft.get("registration", "")
    return bool(registration) and registration.replace("-", "") == ident.replace("-", "")


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
    squawk        TEXT,
    origin        TEXT,
    destination   TEXT,
    matched_rules TEXT,
    receiver_sources TEXT,
    force_archive INTEGER,
    route_resolution_attempted INTEGER,
    route_candidate_airports TEXT
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
CREATE INDEX IF NOT EXISTS positions_icao_hex  ON positions  (icao_hex);
CREATE INDEX IF NOT EXISTS velocities_icao_hex ON velocities (icao_hex);
"""


def _migrate_schema(db: sqlite3.Connection) -> None:
    """Upgrade an active_flights.db created before receiver_sources/
    force_archive existed. CREATE TABLE IF NOT EXISTS only handles a
    brand-new file; a file created under the old schema still has the old
    `source` column (left in place, unused) and is missing these two, so
    ALTER TABLE fills the gap. Safe to call unconditionally on every
    startup — checks column presence first. No backfill of the old `source`
    column's values: active_flights.db only ever holds currently-in-progress
    flights, not historical ones, so at most a handful of mid-flight rows
    lose their old single-source value on the exact restart that upgrades
    the schema — they simply start accumulating receiver_sources fresh from
    that point on.
    """
    existing = {row[1] for row in db.execute("PRAGMA table_info(flights)").fetchall()}
    if "receiver_sources" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN receiver_sources TEXT")
    if "force_archive" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN force_archive INTEGER")
    if "route_resolution_attempted" not in existing:
        # A flight recovered from a store predating this column has never
        # had route resolution attempted -- NULL/0 (falsy) is the correct
        # starting value, same as a brand-new flight, so no backfill needed
        # beyond adding the column.
        db.execute("ALTER TABLE flights ADD COLUMN route_resolution_attempted INTEGER")
    if "route_candidate_airports" not in existing:
        db.execute("ALTER TABLE flights ADD COLUMN route_candidate_airports TEXT")
    db.commit()

# ---------------------------------------------------------------------------
# Message rate tracker (30-second rolling window)
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
# Processing time tracker (rolling average)
# ---------------------------------------------------------------------------

class _TimeTracker:
    def __init__(self) -> None:
        self._total_ms = 0.0
        self._count = 0
        self._hwm_ms = 0
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

    def hwm_ms_and_reset(self) -> int:
        with self._lock:
            v = self._hwm_ms
            self._hwm_ms = 0
            return v

    def reset(self) -> None:
        with self._lock:
            self._total_ms = 0.0
            self._count = 0


# ---------------------------------------------------------------------------
# RabbitMQ queue-depth high-water mark
# ---------------------------------------------------------------------------

class _DepthHWM:
    """Tracks the highest depth recorded since the last read, resetting on
    each read — same reset-on-publish contract as _TimeTracker.hwm_ms_and_reset().
    Starts (and resets to) -1 rather than 0 so "no valid sample landed this
    window" (e.g. the consumer channel isn't up yet, or the sampler hasn't
    ticked since the last publish) stays distinguishable from an observed
    depth of zero — record()'s max keeps a -1 error/no-sample reading from
    ever overwriting a real one."""

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
# Flight — wraps SQLite state
# ---------------------------------------------------------------------------

class Flight:
    """
    In-memory view of one aircraft's active flight.  Reads from / writes to
    the shared SQLite connection.
    """

    __slots__ = (
        "icao_hex", "flight_id", "first_message", "last_message", "total_messages",
        "aircraft", "ident", "operator", "squawk", "origin", "destination",
        "matched_rules", "receiver_sources", "force_archive", "route_resolution_attempted",
        "route_candidate_airports", "positions", "velocities", "_db",
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
        self.squawk: str = ""
        self.origin: Optional[str] = None
        self.destination: Optional[str] = None
        self.matched_rules: list[str] = []
        self.receiver_sources: list[str] = []
        self.force_archive: bool = False
        # One-shot guard: route:{ident} resolution runs at most once per
        # flight, the moment ident/altitude/heading are all available (see
        # _maybe_resolve_route) -- without this, a valid ident with no
        # known route (or an ambiguous leg) would otherwise be re-queried
        # against Redis on every subsequent message for the rest of the
        # flight.
        self.route_resolution_attempted: bool = False
        # Raw JSON string of the airport records fetched from route_airports.lua,
        # cached the first (and only) time it's fetched -- so a flight whose
        # heading hasn't stabilized enough yet to trust (see
        # route_resolver.heading_is_stable) can be re-evaluated on later
        # messages without a repeat Redis round trip. None until fetched.
        self.route_candidate_airports: Optional[str] = None
        self.positions: list[Position] = []
        self.velocities: list[Velocity] = []

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def load(self, icao_hex: str, limit: bool = True) -> bool:
        """Load flight from SQLite. Returns True if found."""
        self.icao_hex = icao_hex.upper()
        cur = self._db.cursor()
        cur.execute(
            "SELECT icao_hex, flight_id, first_message, last_message, total_messages, "
            "aircraft, ident, operator, squawk, origin, destination, "
            "matched_rules, receiver_sources, force_archive, route_resolution_attempted, "
            "route_candidate_airports "
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
        self.squawk = row["squawk"] or ""
        self.origin = row["origin"]
        self.destination = row["destination"]
        self.matched_rules = json.loads(row["matched_rules"] or "[]")
        self.receiver_sources = json.loads(row["receiver_sources"] or "[]")
        self.force_archive = bool(row["force_archive"])
        self.route_resolution_attempted = bool(row["route_resolution_attempted"])
        self.route_candidate_airports = row["route_candidate_airports"]

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
            "total_messages, aircraft, ident, operator, squawk, origin, "
            "destination, matched_rules, receiver_sources, force_archive, "
            "route_resolution_attempted, route_candidate_airports) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                self.icao_hex, self.flight_id, self.first_message, self.last_message,
                self.total_messages, json.dumps(self.aircraft), self.ident,
                json.dumps(self.operator), self.squawk,
                self.origin, self.destination,
                json.dumps(self.matched_rules), json.dumps(self.receiver_sources),
                int(self.force_archive), int(self.route_resolution_attempted),
                self.route_candidate_airports,
            ),
        )
        self._db.commit()

    def delete(self) -> None:
        cur = self._db.cursor()
        cur.execute("DELETE FROM flights   WHERE icao_hex=?", (self.icao_hex,))
        cur.execute("DELETE FROM positions WHERE icao_hex=?", (self.icao_hex,))
        cur.execute("DELETE FROM velocities WHERE icao_hex=?", (self.icao_hex,))

    def add_position(self, pos: Position) -> None:
        cur = self._db.cursor()
        cur.execute(
            "INSERT INTO positions (icao_hex, timestamp, latitude, longitude, altitude) "
            "VALUES (?,?,?,?,?)",
            (self.icao_hex, pos.timestamp, pos.latitude, pos.longitude, pos.altitude),
        )
        self.positions.append(pos)

    def add_velocity(self, vel: Velocity) -> None:
        cur = self._db.cursor()
        cur.execute(
            "INSERT INTO velocities (icao_hex, timestamp, velocity, heading, vertical_speed) "
            "VALUES (?,?,?,?,?)",
            (self.icao_hex, vel.timestamp, vel.velocity, vel.heading, vel.vertical_speed),
        )
        self.velocities.append(vel)

    # ------------------------------------------------------------------
    # Serialisation to CompletedFlight (for archive queue)
    # ------------------------------------------------------------------

    def to_completed_flight(self, load_all: bool = False) -> CompletedFlight:
        """Reload all positions/velocities then build a CompletedFlight record."""
        if load_all:
            self._load_positions(limit=False)
            self._load_velocities(limit=False)

        # Build aircraft dict — ensure icao_hex is present
        aircraft = dict(self.aircraft)
        if "icao_hex" not in aircraft:
            aircraft["icao_hex"] = self.icao_hex

        # Strip military=False (legacy behaviour)
        if aircraft.get("military") is False:
            aircraft.pop("military")

        # Operator without source key
        operator: Optional[dict] = None
        if self.operator:
            operator = {k: v for k, v in self.operator.items() if k != "source"}

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
            "squawk": self.squawk or None,
            "origin": self.origin,
            "destination": self.destination,
            "matched_rules": self.matched_rules,
            "positions": [p.to_dict() for p in self.positions],
            "velocities": [v.to_dict() for v in self.velocities],
        })


# ---------------------------------------------------------------------------
# Message Processor
# ---------------------------------------------------------------------------

class MessageProcessor:

    def __init__(self, config: dict, message_processor_id: int) -> None:
        self._cfg = config
        self._id = message_processor_id
        self._queue_name = f"adsb-{message_processor_id}"
        self._started_at = datetime.now(timezone.utc).isoformat()
        self._shutdown = threading.Event()

        # SQLite active store — file-backed (WAL) so it survives an
        # ungraceful process death. Reopening an existing file on restart
        # recovers whatever flights were active when the previous process
        # ended, whether that was a crash or a deliberate stop — there's no
        # distinction, see shutdown().
        data_dir = config.get("data_dir", "/app/data")
        os.makedirs(data_dir, exist_ok=True)
        self._db = sqlite3.connect(
            os.path.join(data_dir, "active_flights.db"), check_same_thread=False
        )
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(_SCHEMA)
        _migrate_schema(self._db)

        # message_clock drives eviction instead of wall-clock time, so a
        # backlog of messages replayed after a restart doesn't get archived
        # just because real time passed while the process was down. Floor
        # it at the most recent message any recovered flight actually saw;
        # if the store was empty, there's nothing to protect and wall-clock
        # time is fine to start from.
        row = self._db.execute("SELECT MAX(last_message) FROM flights").fetchone()
        self._message_clock: float = row[0] if row and row[0] is not None else time.time()

        # Archive fallback
        self._fallback = FallbackQueue(os.path.join(data_dir, "completed_flights.db"))

        # Metrics
        self._rate = _RateTracker()
        self._processing_time = _TimeTracker()
        self._rules_time = _TimeTracker()
        self._rmq_queue_depth_hwm = _DepthHWM()
        self._db_lock = threading.Lock()

        # Redis
        rc = config["redis"]
        self._redis = redis_lib.Redis(
            host=rc["host"], port=rc.get("port", 6379),
            decode_responses=True,
        )
        _lua_path = pathlib.Path(__file__).parent.parent / "shared" / "lua" / "merge_aircraft.lua"
        self._merge_sha = self._redis.script_load(_lua_path.read_text())
        _route_lua_path = pathlib.Path(__file__).parent.parent / "shared" / "lua" / "route_airports.lua"
        self._route_sha = self._redis.script_load(_route_lua_path.read_text())

        # Rules engine
        self._rules_engine = RulesEngine(self._redis)

        # flight_ttl_seconds: shared Redis config (config:flight_ttl_seconds),
        # read once at startup and cached — read on every message in
        # _update_flight's gap check, so it must never be a synchronous
        # Redis GET on the hot path. Not hot-reloaded; restart to pick up
        # a changed value.
        self._flight_ttl_seconds: int = 300

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
        self._claim_message_processor_id()
        self._connect_mqtt()
        self._rules_engine.reload_if_changed()
        self._load_flight_ttl_seconds()

        # Background threads
        threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat").start()
        threading.Thread(target=self._eviction_loop, daemon=True, name="eviction").start()
        threading.Thread(target=self._telemetry_loop, daemon=True, name="telemetry").start()
        threading.Thread(target=self._config_poll_loop, daemon=True, name="config-poll").start()
        threading.Thread(
            target=self._rmq_queue_depth_sampler_loop, daemon=True, name="rmq-depth-sampler"
        ).start()

        self._consume_loop()

    def _setup_logging(self) -> None:
        configure_logging(self._cfg.get("log_level"))

    def _claim_message_processor_id(self) -> None:
        """Prevent two message processors with the same ID running simultaneously."""
        interval = self._cfg.get("telemetry_interval_seconds", 30)
        key = message_processor_heartbeat_key(self._id)
        claimed = self._redis.set(key, "1", nx=True, ex=int(interval * 2))
        if not claimed:
            logger.critical(
                "MESSAGE_PROCESSOR_ID %d is already running on another instance. Exiting.", self._id
            )
            sys.exit(1)
        logger.info("Message processor %d claimed.", self._id)

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
                self._rmq_channel.queue_declare(queue=self._queue_name, durable=True)
                self._rmq_channel.basic_qos(prefetch_count=1)
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
            msg = InboundMessage.model_validate_json(body)
        except Exception as exc:
            logger.debug("Unparseable message: %s", exc)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        t_start = time.monotonic()
        self._rate.record()
        self._process(msg)
        elapsed_ms = (time.monotonic() - t_start) * 1000
        self._processing_time.record(elapsed_ms)
        self._processing_time.record_hwm(elapsed_ms)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # ------------------------------------------------------------------
    # ADS-B decoding + flight state update
    # ------------------------------------------------------------------

    def _process(self, msg: InboundMessage) -> None:
        data = self._decode_message(msg)
        if data is None:
            return
        with self._db_lock:
            self._update_flight(data, msg)

    def _decode_message(self, msg: InboundMessage) -> Optional[dict]:
        """Route to the source-specific decoder. MLAT frames are still raw
        Mode-S hex, same as 1090 — this dispatch was never keyed on source
        before, so that's not a behavior change."""
        if msg.source == "978":
            return self._decode_978(msg)
        return self._decode_1090(msg)

    def _decode_1090(self, msg: InboundMessage) -> Optional[dict]:
        """
        Decode a raw Mode-S hex frame via pyModeS 3.x's unified decode().
        Pure field-presence extraction — no DF/typecode dispatch. Message
        types that don't populate any of the fields below (e.g. ACAS RA
        broadcasts) simply produce nothing and get dropped, with no need to
        enumerate which typecodes to skip.
        """
        raw = msg.raw
        if len(raw) < 14:
            return None

        lat_cfg = self._cfg.get("latitude")
        lon_cfg = self._cfg.get("longitude")
        reference = (lat_cfg, lon_cfg) if lat_cfg is not None and lon_cfg is not None else None

        try:
            result = pms.decode(raw, reference=reference)
        except Exception:
            return None

        # A real corruption check only for DF17/18 (crc_valid there is a
        # genuine crc==0 result). For DF0/4/5/11/16/20/21, pyModeS hardcodes
        # crc_valid=True unconditionally — their CRC field encodes the ICAO
        # itself, so there's no single-message corruption signal available
        # for those types at all (confirmed by reading pyModeS's own
        # message.py). This check is a no-op for that DF set, not a gap we
        # can close by passing an icao hint — a hint only overrides what
        # `icao` is reported as, it doesn't verify anything.
        if result.get("crc_valid") is False:
            return None

        data: dict = {"icao_hex": msg.icao_hex}

        if result.get("squawk") is not None:
            data["squawk"] = result["squawk"]

        if result.get("callsign") is not None:
            # pyModeS 3.x uses "#" (not "_") as its invalid-character
            # sentinel for undecodable callsign slots — reject rather than
            # store a partially-garbage ident.
            ident = result["callsign"].strip()
            if ident and "#" not in ident:
                data["ident"] = ident

        canonical_wtc = _WAKE_TURBULENCE_MAP.get(result.get("wake_vortex"))
        if canonical_wtc:
            data["wake_turbulence_category"] = canonical_wtc

        if result.get("latitude") is not None:
            data["latitude"] = result["latitude"]
            data["longitude"] = result["longitude"]

        if result.get("altitude") is not None:
            data["altitude"] = result["altitude"]

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
            data["heading"] = heading

        if result.get("vertical_rate") is not None:
            data["vertical_speed"] = result["vertical_rate"]

        if result.get("version") is not None:
            data["adsb_version"] = result["version"]

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

        if result.get("latitude") is not None:
            data["latitude"] = result["latitude"]
            data["longitude"] = result["longitude"]

        if result.get("altitude") is not None:
            data["altitude"] = result["altitude"]

        if result.get("groundspeed") is not None:
            data["velocity"] = result["groundspeed"]

        heading = result.get("track")
        if heading is None:
            heading = result.get("heading")
        if heading is not None:
            data["heading"] = heading

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
                # The loaded flight is already complete — a gap this size
                # replayed through the backlog (or happened live) means it
                # ended before this message. Archive it and start fresh
                # rather than extending a flight that's actually over.
                completed = flight.to_completed_flight(load_all=True)
                flight.delete()
                self._db.commit()
                self._archive(completed)
                flight = Flight(self._db)
                exists = False

        if not exists:
            flight.icao_hex = data["icao_hex"].upper()
            flight.flight_id = generate_flight_id()
            flight.first_message = msg.received_at
            self._enrich_aircraft(flight)

        if msg.source not in flight.receiver_sources:
            flight.receiver_sources.append(msg.source)

        flight.last_message = msg.received_at
        flight.total_messages += 1

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

        if "squawk" in data and not flight.squawk:
            flight.squawk = str(data["squawk"])

        if "wake_turbulence_category" in data:
            # Live decode is the sole writer (registry enrichment never seeds this
            # field — see _WAKE_TURBULENCE_MAP), so each new reading just replaces
            # the last one; no first-wins protection needed.
            flight.aircraft["wake_turbulence_category"] = data["wake_turbulence_category"]

        if "ident" in data and not flight.ident:
            ident = data["ident"]
            if ident and ident != "00000000":
                flight.ident = ident
                self._enrich_operator(flight)

        if "adsb_version" in data:
            flight.aircraft.setdefault("adsb_version", data["adsb_version"])

        self._maybe_resolve_route(flight)

        # Rules evaluation
        t_rules = time.monotonic()
        matched = self._rules_engine.evaluate(flight)
        rules_ms = (time.monotonic() - t_rules) * 1000
        self._rules_time.record_hwm(rules_ms)

        for rule in matched:
            flight.matched_rules.append(rule["identifier"])
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
                flight.aircraft = aircraft
            else:
                self._redis.incr(metrics_registration_misses_key(self._id, "hour"))
                self._redis.incr(metrics_registration_misses_key(self._id, "today"))
                self._redis.incr(metrics_registration_misses_key(self._id, "lifetime"))
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

        # Skip military aircraft
        if flight.aircraft.get("military"):
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
                self._redis.incr(metrics_registration_misses_key(self._id, "hour"))
                self._redis.incr(metrics_registration_misses_key(self._id, "today"))
                self._redis.incr(metrics_registration_misses_key(self._id, "lifetime"))
        except Exception as exc:
            logger.debug("Redis enrichment (operator) error: %s", exc)

    # ------------------------------------------------------------------
    # Route leg resolution (origin/destination)
    # ------------------------------------------------------------------

    def _route_ready(self, flight: Flight) -> bool:
        """True once every field the resolution heuristics need has been
        seen at least once for this flight: a route-bearing ident (not
        empty/"00000000" -- already enforced before flight.ident is ever
        set -- and not just the aircraft's own tail number), a position, an
        altitude, and a heading. Order of arrival across messages doesn't
        matter; each condition is checked against the flight's full history
        via SQLite, not just whatever _update_flight's single-message
        Flight.load(limit=True) happened to load in memory."""
        if not flight.ident or _ident_matches_registration(flight.ident, flight.aircraft):
            return False

        # A stored position always has latitude/longitude (only ever
        # inserted together, see _update_flight) -- altitude is the one
        # that's sometimes absent (e.g. a surface-position typecode), so
        # requiring it here also guarantees "a lat/long has been received".
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
        """Runs route:{ident} leg resolution at most once per flight, the
        moment ident/position/altitude/heading are all available — not at
        archive time, so a rules-engine condition or MQTT notification
        later in the same flight can see origin/destination too. See
        message-processor/README.md's "Route Leg Resolution" section and
        message_processor.route_resolver for the resolution/sanity-check
        logic itself.

        route_resolution_attempted is only set once the result is final
        (see route_resolver.resolve_origin_destination's is_final) — a
        settled resolution, or a settled "no route"/"rejected" outcome — so
        a valid ident with no known route is never re-queried against
        Redis on every subsequent message for the rest of the flight. The
        one exception is a multi-leg route whose heading data hasn't
        stabilized enough yet to trust (e.g. an aircraft circling in a
        holding pattern): that's deliberately re-evaluated on later
        messages once more heading samples arrive, but the fetched airport
        records are cached on route_candidate_airports the first time so
        those re-evaluations never repeat the Redis round trip.
        All-or-nothing: leaves both fields None on missing route data, an
        unresolvable leg, or a failed sanity check, rather than writing a
        partial or best-guess pair."""
        if flight.route_resolution_attempted or not self._route_ready(flight):
            return

        if flight.route_candidate_airports is not None:
            airports = json.loads(flight.route_candidate_airports)
        else:
            try:
                raw = self._redis.evalsha(self._route_sha, 0, flight.ident)
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

        # Reload full history -- flight.positions/velocities may hold only
        # the most recently loaded row (Flight.load's default limit=True),
        # not the whole flight, and the low-altitude heuristic specifically
        # needs the earliest position.
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
            self._evict_stale()

    def _evict_stale(self) -> None:
        ttl = self._flight_ttl_seconds
        with self._db_lock:
            # message_clock, not wall-clock time, gates eviction — after a
            # restart it only advances as far as the RabbitMQ backlog has
            # actually been drained, so recovered flights aren't archived
            # just because real time passed while the process was down.
            cutoff = self._message_clock - ttl
            cur = self._db.cursor()
            cur.execute("SELECT icao_hex FROM flights WHERE last_message < ?", (cutoff,))
            stale = [row[0] for row in cur.fetchall()]

        for icao_hex in stale:
            with self._db_lock:
                flight = Flight(self._db)
                if not flight.load(icao_hex, limit=False):
                    continue
                completed = flight.to_completed_flight(load_all=False)
                flight.delete()
                self._db.commit()

            self._archive(completed)

    def _archive(self, flight: CompletedFlight) -> None:
        payload = flight.model_dump_json(by_alias=True)
        if self._rmq_connected and self._rmq_channel:
            try:
                self._rmq_channel.basic_publish(
                    exchange="",
                    routing_key="archive",
                    body=payload.encode(),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                return
            except Exception:
                self._rmq_connected = False
        self._fallback.put(payload)

    def _drain_fallback(self) -> None:
        def publish(payload: str) -> None:
            try:
                self._rmq_channel.basic_publish(
                    exchange="",
                    routing_key="archive",
                    body=payload.encode(),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
            except Exception:
                self._rmq_connected = False
                raise
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

    def _publish_rule_notification(self, flight: Flight, rule: dict, received_at: float) -> None:
        max_lag = self._cfg.get("rule_notification_max_lag_seconds", 30)
        lag = time.time() - received_at
        if lag > max_lag:
            logger.debug(
                "Suppressing MQTT rule notification for %s (rule=%s): "
                "message is %.1fs old (backlog replay)",
                flight.icao_hex, rule["identifier"], lag,
            )
            return
        if not (self._mqtt and self._mqtt_connected):
            return
        notification = flight.to_completed_flight().model_dump(by_alias=True, mode="json")
        notification.pop("positions", None)
        notification.pop("velocities", None)
        notification.pop("_id", None)
        if not notification.get("operator"):
            notification.pop("operator", None)
        if not notification.get("origin"):
            notification.pop("origin", None)
        if not notification.get("destination"):
            notification.pop("destination", None)
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
    # Telemetry
    # ------------------------------------------------------------------

    def _telemetry_loop(self) -> None:
        interval = self._cfg.get("telemetry_interval_seconds", 30)
        while not self._shutdown.is_set():
            time.sleep(interval)
            # Independent of _consume_loop's reconnect-triggered drain: a
            # publish failure can pin _rmq_connected False (or leave
            # messages queued) without the underlying connection ever
            # raising AMQPConnectionError, in which case _consume_loop
            # never re-enters its reconnect branch and _drain_fallback
            # never runs again on its own. This periodic sweep is a cheap
            # no-op when the queue is empty and doesn't depend on that
            # edge-triggered detection ever firing. _drain_fallback()
            # itself spawns the actual drain in the background (or skips
            # if one's already running from the reconnect path), so this
            # call returns immediately and never delays the telemetry
            # publish below it.
            if self._rmq_connected:
                self._drain_fallback()
            self._publish_telemetry()

    def _publish_telemetry(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return

        pid = self._id

        with self._db_lock:
            cur = self._db.cursor()
            cur.execute("SELECT COUNT(*) FROM flights")
            active = cur.fetchone()[0]

        processing_hwm = self._processing_time.hwm_ms_and_reset()
        self._processing_time.reset()
        rules_hwm = self._rules_time.hwm_ms_and_reset()

        base = f"SkyFollower/message-processor/{pid}/statistic"

        self._mqtt.publish(f"{base}/started_at", self._started_at, retain=True)
        self._mqtt.publish(f"{base}/messages_per_second", str(round(self._rate.rate(), 2)), retain=True)
        self._mqtt.publish(f"{base}/processing_time_hwm_ms", str(processing_hwm), retain=True)
        self._mqtt.publish(f"{base}/rules_engine_hwm_ms", str(rules_hwm), retain=True)
        self._mqtt.publish(
            f"{base}/rabbitmq_input_queue_depth_hwm",
            str(self._rmq_queue_depth_hwm.value_and_reset()),
            retain=True,
        )
        self._mqtt.publish(f"{base}/local_archive_queue_depth", str(self._fallback.depth()), retain=True)
        self._mqtt.publish(
            f"{base}/dead_letter_queue_depth", str(self._fallback.dead_letter_depth()), retain=True
        )
        self._mqtt.publish(f"{base}/active_flights", str(active), retain=True)
        self._mqtt.publish(
            f"{base}/registration_misses_hour",
            str(self._redis_counter(metrics_registration_misses_key(pid, "hour"))),
            retain=True,
        )
        self._mqtt.publish(
            f"{base}/registration_misses_today",
            str(self._redis_counter(metrics_registration_misses_key(pid, "today"))),
            retain=True,
        )
        self._mqtt.publish(
            f"{base}/aircraft_type_misses_hour",
            str(self._redis_counter(metrics_aircraft_type_misses_key(pid, "hour"))),
            retain=True,
        )
        self._mqtt.publish(
            f"{base}/aircraft_type_misses_today",
            str(self._redis_counter(metrics_aircraft_type_misses_key(pid, "today"))),
            retain=True,
        )

        # Refresh heartbeat
        interval = self._cfg.get("telemetry_interval_seconds", 30)
        try:
            self._redis.expire(message_processor_heartbeat_key(self._id), int(interval * 2))
        except Exception:
            pass

    def _redis_counter(self, key: str) -> int:
        try:
            v = self._redis.get(key)
            return int(v) if v else 0
        except Exception:
            return 0

    def _rmq_queue_depth(self) -> int:
        """Best-effort depth of this processor's input queue via passive
        declare on the existing consumer channel. Returns -1 on any error
        (no channel yet, or the declare itself fails)."""
        if not self._rmq_channel:
            return -1
        try:
            result = self._rmq_channel.queue_declare(
                queue=self._queue_name, durable=True, passive=True
            )
            return result.method.message_count
        except Exception:
            return -1

    def _rmq_queue_depth_sampler_loop(self) -> None:
        """Samples this processor's input queue depth at most once every 10
        seconds, independent of telemetry_interval_seconds, feeding the HWM
        tracker that _publish_telemetry() reads and resets each tick."""
        while not self._shutdown.is_set():
            time.sleep(10)
            self._rmq_queue_depth_hwm.record(self._rmq_queue_depth())

    # ------------------------------------------------------------------
    # Config polling
    # ------------------------------------------------------------------

    def _config_poll_loop(self) -> None:
        while not self._shutdown.is_set():
            time.sleep(30)
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
        interval = self._cfg.get("telemetry_interval_seconds", 30)
        while not self._shutdown.is_set():
            time.sleep(interval)
            try:
                self._redis.expire(message_processor_heartbeat_key(self._id), int(interval * 2))
            except Exception:
                pass

    # ------------------------------------------------------------------
    # HA autodiscovery
    # ------------------------------------------------------------------

    def _publish_ha_autodiscovery(self) -> None:
        if not (self._mqtt and self._mqtt_connected):
            return
        pid = self._id
        device = {
            "ids": f"SkyFollower_message_processor_{pid}",
            "name": f"SkyFollower Message Processor {pid}",
            "manufacturer": "P5Software, LLC",
        }
        availability = {
            "availability_topic": f"SkyFollower/message-processor/{pid}/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }
        base = f"SkyFollower/message-processor/{pid}/statistic"
        sensors = [
            ("messages_per_second", "Message Rate", "mdi:broadcast", "measurement", "msg/s"),
            ("processing_time_hwm_ms", "Processing Time HWM", "mdi:clock", "measurement", "ms"),
            ("rules_engine_hwm_ms", "Rules Engine HWM", "mdi:clock", "measurement", "ms"),
            ("rabbitmq_input_queue_depth_hwm", "RabbitMQ Queue Depth HWM", "mdi:tray-full", "measurement", None),
            ("local_archive_queue_depth", "Local Archive Queue Depth", "mdi:tray-full", "measurement", None),
            ("dead_letter_queue_depth", "Dead Letter Queue Depth", "mdi:skull-crossbones", "measurement", None),
            ("registration_misses_hour", "Registration Misses (Hour)", "mdi:broadcast", "total_increasing", None),
            ("registration_misses_today", "Registration Misses (Today)", "mdi:broadcast", "total_increasing", None),
            ("aircraft_type_misses_hour", "Aircraft Type Misses (Hour)", "mdi:broadcast", "total_increasing", None),
            ("aircraft_type_misses_today", "Aircraft Type Misses (Today)", "mdi:broadcast", "total_increasing", None),
            ("active_flights", "Active Flights", "mdi:airplane", "measurement", None),
        ]
        for field, desc, icon, state_class, unit in sensors:
            payload = {
                **availability,
                "state_topic": f"{base}/{field}",
                "name": desc,
                "unique_id": f"SkyFollower_message_processor_{pid}_{field}",
                "object_id": f"SkyFollower_message_processor_{pid}_{field}",
                "device": device,
                "icon": icon,
                "state_class": state_class,
            }
            if unit:
                payload["unit_of_measurement"] = unit
            self._mqtt.publish(
                f"homeassistant/sensor/SkyFollower_message_processor_{pid}_{field}/config",
                json.dumps(payload),
                retain=True,
            )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        # No eager flush: the active store is durable, so a deliberate
        # stop and a crash are recovered identically on the next startup —
        # nothing needs to be force-archived here.
        logger.info("Shutdown requested…")
        self._shutdown.set()
        if self._rmq_channel:
            try:
                self._rmq_channel.stop_consuming()
            except Exception:
                pass
        if self._mqtt:
            self._mqtt.publish(
                f"SkyFollower/message-processor/{self._id}/status", "OFFLINE", retain=True
            )
            self._mqtt.loop_stop()
        self._db.close()
        logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    path = os.environ.get("SETTINGS_PATH", "/app/settings.json")
    with open(path) as f:
        return json.load(f)


def main() -> None:
    message_processor_id_str = os.environ.get("MESSAGE_PROCESSOR_ID")
    if message_processor_id_str is None:
        print("MESSAGE_PROCESSOR_ID environment variable is required.", file=sys.stderr)
        sys.exit(1)
    try:
        message_processor_id = int(message_processor_id_str)
    except ValueError:
        print(f"MESSAGE_PROCESSOR_ID must be an integer, got: {message_processor_id_str!r}", file=sys.stderr)
        sys.exit(1)

    config = _load_config()
    processor = MessageProcessor(config, message_processor_id)

    def _handle_signal(sig, frame):
        processor.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    processor.start()


if __name__ == "__main__":
    main()
