#!/usr/bin/env python3
"""
SkyFollower Map Load Generator

Fires synthetic UDP datagrams directly at a `map` service instance, in the
exact wire format `message-processor` actually sends (verified against
`map/main.py`'s `_handle_packet()`/`_extract_timestamp()` and
`message-processor/main.py`'s `_publish_map_position`/
`_maybe_publish_map_metadata`/`_map_heartbeat_loop`), so update frequency and
simultaneous-aircraft count can be pushed well past normal live traffic to
find where `map`'s backend ingestion and WebSocket relay actually start to
strain.

This is a backend + WebSocket-relay load generator only -- it never opens a
browser, so it says nothing about actual frontend rendering cost. Use it to
exercise `map`'s UDP ingestion rate, Redis write rate, and WS broadcast
behavior; a headless-browser rendering benchmark is a separate concern.

Usage:
    python main.py --host 192.168.1.20 --port 5566 \
        --aircraft-count 200 --position-rate 2 --duration 300

    python main.py --host 192.168.1.20 --port 5566 \
        --aircraft-count 50 --mode stress
"""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from shared.timing import (
    MAP_HEARTBEAT_INTERVAL_SECONDS,
    MAP_METADATA_RESEND_INTERVAL_SECONDS,
)

# Nautical miles per degree of latitude -- used to convert the configurable
# orbit radius (nautical miles, matching velocity's own knots unit) into
# degrees for the lat/lon math below. Treated as a constant (not
# geodesically exact, which varies slightly with latitude) since this tool
# only needs "genuinely moving, roughly the right scale", not survey-grade
# positioning.
_NM_PER_DEGREE_LATITUDE = 60.0

# Synthetic receiver_sources pool cycled by aircraft index, so a load run
# exercises all three real source tags rather than always sending one.
_SYNTHETIC_RECEIVER_SOURCES = ("1090", "978", "EXTERNAL")

# Synthetic operator/airport pools cycled by aircraft index -- obviously
# fake designators/codes, just enough shape to populate map's info panel
# fields during a load run instead of leaving them blank.
_SYNTHETIC_OPERATORS = (
    {"airline_designator": "LG1", "name": "Load Generator One", "callsign": "LOADGEN"},
    {"airline_designator": "LG2", "name": "Load Generator Two", "callsign": "LOADRUN"},
    {"airline_designator": "LG3", "name": "Load Generator Three", "callsign": "LOADTEST"},
)
_SYNTHETIC_AIRPORT_CODES = tuple(f"ZZ{n:02d}" for n in range(10))


# ---------------------------------------------------------------------------
# Synthetic identity (deterministic, obviously fake)
# ---------------------------------------------------------------------------

def synthetic_icao_hex(aircraft_index: int, prefix: str = "FF") -> str:
    """A distinct, deterministic, obviously-synthetic 6-character hex ICAO
    address for one simulated aircraft: a fixed prefix (default "FF", an
    unallocated ICAO address block) plus a zero-padded hex counter filling
    the remaining digits. Re-running the tool with the same
    --aircraft-count always reproduces the same set of addresses."""
    width = 6 - len(prefix)
    if width <= 0:
        raise ValueError("prefix must be shorter than 6 characters")
    return f"{prefix}{aircraft_index:0{width}X}"[:6].upper()


def synthetic_ident(aircraft_index: int) -> str:
    """A distinct, deterministic, obviously-synthetic flight ident."""
    return f"LOAD{aircraft_index:04d}"


def synthetic_squawk(aircraft_index: int) -> str:
    """A deterministic 4-digit octal squawk code (each digit 0-7)."""
    digits = "".join(str((aircraft_index >> (3 * i)) & 0x7) for i in range(4))[::-1]
    return digits


# ---------------------------------------------------------------------------
# Synthetic motion
# ---------------------------------------------------------------------------

def aircraft_position(
    aircraft_index: int,
    elapsed_seconds: float,
    *,
    aircraft_count: int,
    center_lat: float,
    center_lon: float,
    radius_nm: float,
    orbit_period_seconds: float,
    altitude_ft: float,
) -> dict:
    """Deterministic lat/lon/alt/velocity/hdg/vs for one simulated aircraft
    at a given elapsed time, orbiting a circle of `radius_nm` around
    (center_lat, center_lon) once every `orbit_period_seconds`.

    Every aircraft is phase-offset evenly around the circle
    (2*pi*aircraft_index/aircraft_count) so `aircraft_count` of them spread
    out instead of overlapping; the same (aircraft_index, elapsed_seconds,
    ...) inputs always produce the same output -- no hidden state, no
    randomness -- so a frontend/backend load test is fully reproducible.

    Altitude and vertical_speed oscillate gently (a sine/cosine of the same
    orbit angle) purely so those fields are genuinely non-static too,
    without needing a second independent clock.
    """
    phase = (2.0 * math.pi * aircraft_index / aircraft_count) if aircraft_count else 0.0
    angular_speed = 2.0 * math.pi / orbit_period_seconds
    angle = phase + angular_speed * elapsed_seconds

    lat = center_lat + (radius_nm / _NM_PER_DEGREE_LATITUDE) * math.sin(angle)
    lon_scale = math.cos(math.radians(center_lat))
    if abs(lon_scale) < 1e-9:
        lon_scale = 1e-9
    lon = center_lon + (radius_nm / _NM_PER_DEGREE_LATITUDE) * math.cos(angle) / lon_scale

    heading = math.degrees(angle + math.pi / 2.0) % 360.0
    # Ground speed implied by the orbit's own angular rate: knots = nm/hour.
    velocity = radius_nm * angular_speed * 3600.0
    vertical_speed = 500.0 * math.cos(angle)
    altitude = altitude_ft + 250.0 * math.sin(angle)

    return {
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "alt": round(altitude, 1),
        "velocity": round(velocity, 1),
        "hdg": round(heading, 1),
        "vs": round(vertical_speed, 1),
    }


def tick_elapsed_seconds(tick: int, position_rate: float) -> float:
    """The simulated elapsed time represented by tick `tick` when sending
    `position_rate` position datagrams per second per aircraft. Kept as its
    own pure function so `--mode stress` can advance simulated motion at
    this same rate while not actually sleeping between sends (see
    `run()`)."""
    return tick / position_rate if position_rate > 0 else float(tick)


# ---------------------------------------------------------------------------
# Packet construction (pure -- no socket I/O)
# ---------------------------------------------------------------------------

def build_position_packet(
    icao_hex: str, processor_id: str, ts: float, position: dict
) -> dict:
    """A `position` datagram exactly matching map/main.py's expected shape:
    top-level icao_hex, ts, processor_id, plus whichever of
    lat/lon/alt/velocity/hdg/vs are present in `position` (all individually
    optional on the real wire protocol; this generator always supplies all
    six since synthetic motion computes them all anyway)."""
    return {
        "type": "position",
        "icao_hex": icao_hex,
        "ts": ts,
        "processor_id": processor_id,
        **position,
    }


def build_heartbeat_packet(processor_id: str, ts: float) -> dict:
    """A `heartbeat` datagram -- liveness-only, no flight state."""
    return {"type": "heartbeat", "processor_id": processor_id, "ts": ts}


def build_metadata_packet(
    icao_hex: str,
    ident: str,
    processor_id: str,
    last_message_iso: str,
    *,
    operator: dict | None = None,
    origin: dict | None = None,
    destination: dict | None = None,
    squawk: str | None = None,
    matched_rules: list[str] | None = None,
    receiver_sources: list[str] | None = None,
) -> dict:
    """A `metadata` datagram shaped like message-processor's CompletedFlight
    notification payload (`_build_flight_notification_payload()`) plus
    `"type": "metadata"` -- icao_hex nested under `aircraft`, never
    top-level (see map/main.py's `_handle_packet`), and `last_message` as
    the out-of-order guard's clock for this packet type (metadata carries
    no `ts` -- see map/main.py's `_extract_timestamp`). Optional fields are
    omitted entirely rather than sent as null/empty, matching
    `_build_flight_notification_payload`'s own falsy-drop convention for
    operator/origin/destination."""
    packet: dict = {
        "type": "metadata",
        "aircraft": {"icao_hex": icao_hex},
        "ident": ident,
        "processor_id": processor_id,
        "last_message": last_message_iso,
    }
    if operator:
        packet["operator"] = operator
    if origin:
        packet["origin"] = origin
    if destination:
        packet["destination"] = destination
    if squawk:
        packet["squawk"] = squawk
    if matched_rules:
        packet["matched_rules"] = matched_rules
    if receiver_sources:
        packet["receiver_sources"] = receiver_sources
    return packet


def synthetic_metadata_fields(aircraft_index: int, matched_rule: str | None) -> dict:
    """Deterministic, obviously-fake operator/origin/destination/squawk/
    receiver_sources for one simulated aircraft, cycled by index so a
    multi-aircraft run exercises varied panel content instead of every
    aircraft looking identical. `matched_rule` (from --matched-rule) is
    stamped on every aircraft's matched_rules list unchanged when given, so
    the map's rule-match indicator can be exercised too."""
    operator = _SYNTHETIC_OPERATORS[aircraft_index % len(_SYNTHETIC_OPERATORS)]
    codes = _SYNTHETIC_AIRPORT_CODES
    origin_code = codes[aircraft_index % len(codes)]
    destination_code = codes[(aircraft_index + 1) % len(codes)]
    receiver_source = _SYNTHETIC_RECEIVER_SOURCES[aircraft_index % len(_SYNTHETIC_RECEIVER_SOURCES)]
    return {
        "operator": dict(operator),
        "origin": {"icao_code": origin_code, "name": f"Load Test Airport {origin_code}"},
        "destination": {"icao_code": destination_code, "name": f"Load Test Airport {destination_code}"},
        "squawk": synthetic_squawk(aircraft_index),
        "matched_rules": [matched_rule] if matched_rule else [],
        "receiver_sources": [receiver_source],
    }


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Send loop
# ---------------------------------------------------------------------------

def run(
    sock: socket.socket,
    addr: tuple[str, int],
    *,
    aircraft_count: int,
    position_rate: float,
    metadata_interval: float,
    processor_id: str,
    duration: float,
    mode: str,
    center_lat: float,
    center_lon: float,
    radius_nm: float,
    orbit_period_seconds: float,
    altitude_ft: float,
    matched_rule: str | None,
    stop_event: threading.Event,
    on_progress=None,
) -> dict:
    """Sends position/metadata/heartbeat datagrams until `stop_event` is
    set or `duration` seconds elapse (0/None runs indefinitely, matching
    `traffic-replayer`'s own no-explicit-timeout default when it isn't
    given one). Returns a dict of counters for the final summary.

    Motion always advances by simulated time (`tick_elapsed_seconds`), one
    tick per position round; in `steady` mode the loop also sleeps to hold
    `position_rate` sends/sec/aircraft to wall-clock time, while `stress`
    mode skips the sleep entirely and sends as fast as the socket accepts
    -- simulated motion still advances one tick per round either way, so
    aircraft keep moving realistically even when massively outrunning
    real time.
    """
    icao_hexes = [synthetic_icao_hex(i) for i in range(aircraft_count)]
    idents = [synthetic_ident(i) for i in range(aircraft_count)]
    last_metadata_sent = [None] * aircraft_count  # wall-clock time.monotonic(), or None

    counts = {"position": 0, "metadata": 0, "heartbeat": 0}
    start = time.monotonic()
    last_heartbeat = 0.0
    tick = 0
    last_progress = start

    def _send(payload: dict) -> None:
        sock.sendto(json.dumps(payload, separators=(",", ":")).encode("utf-8"), addr)

    while not stop_event.is_set():
        now = time.monotonic()
        if duration and now - start >= duration:
            break

        elapsed = tick_elapsed_seconds(tick, position_rate)

        if now - last_heartbeat >= MAP_HEARTBEAT_INTERVAL_SECONDS:
            _send(build_heartbeat_packet(processor_id, time.time()))
            counts["heartbeat"] += 1
            last_heartbeat = now

        for i in range(aircraft_count):
            position = aircraft_position(
                i,
                elapsed,
                aircraft_count=aircraft_count,
                center_lat=center_lat,
                center_lon=center_lon,
                radius_nm=radius_nm,
                orbit_period_seconds=orbit_period_seconds,
                altitude_ft=altitude_ft,
            )
            _send(build_position_packet(icao_hexes[i], processor_id, time.time(), position))
            counts["position"] += 1

            due = last_metadata_sent[i] is None or (now - last_metadata_sent[i]) >= metadata_interval
            if due:
                extras = synthetic_metadata_fields(i, matched_rule)
                _send(
                    build_metadata_packet(
                        icao_hexes[i], idents[i], processor_id, _iso_now(), **extras
                    )
                )
                counts["metadata"] += 1
                last_metadata_sent[i] = now

        tick += 1

        if on_progress and now - last_progress >= 5:
            on_progress(counts, now - start)
            last_progress = now

        if mode == "steady" and position_rate > 0:
            target = start + tick_elapsed_seconds(tick, position_rate)
            sleep_for = target - time.monotonic()
            if sleep_for > 0:
                stop_event.wait(sleep_for)

    return counts


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="SkyFollower Map Load Generator")
    parser.add_argument("--host", required=True, help="Target map instance's UDP listen host")
    parser.add_argument("--port", required=True, type=int, help="Target map instance's UDP listen port (MAP_LISTEN_PORT)")
    parser.add_argument(
        "--aircraft-count", type=int, default=10,
        help="Number of simulated aircraft, each a distinct synthetic icao_hex/ident (default: 10)",
    )
    parser.add_argument(
        "--position-rate", type=float, default=2.0,
        help="Position datagrams per second, PER AIRCRAFT. Deliberately allowed to exceed the real "
        "MAP_UDP_MIN_POSITION_INTERVAL_SECONDS throttle (default 1/s in message-processor) -- "
        "finding where that breaks is the point (default: 2.0)",
    )
    parser.add_argument(
        "--metadata-interval", type=float, default=MAP_METADATA_RESEND_INTERVAL_SECONDS / 2,
        help="Seconds between metadata resends per aircraft "
        f"(default: {MAP_METADATA_RESEND_INTERVAL_SECONDS / 2}, half of message-processor's own "
        f"{MAP_METADATA_RESEND_INTERVAL_SECONDS}s unconditional resend interval)",
    )
    parser.add_argument(
        "--processor-id", default="load-gen-1",
        help="Fake processor_id stamped on every packet (default: load-gen-1)",
    )
    parser.add_argument(
        "--duration", type=float, default=0,
        help="Run length in seconds. 0 or omitted runs indefinitely until Ctrl+C/SIGTERM (default: 0)",
    )
    parser.add_argument(
        "--mode", choices=["steady", "stress"], default="steady",
        help="steady: hold --position-rate to wall-clock time (default); "
        "stress: send as fast as the socket accepts, no rate limiting -- simulated motion still "
        "advances one tick per round so aircraft keep moving realistically",
    )
    parser.add_argument("--center-lat", type=float, default=39.8283, help="Orbit center latitude (default: 39.8283)")
    parser.add_argument("--center-lon", type=float, default=-98.5795, help="Orbit center longitude (default: -98.5795)")
    parser.add_argument("--radius-nm", type=float, default=15.0, help="Orbit radius in nautical miles (default: 15.0)")
    parser.add_argument(
        "--orbit-period-seconds", type=float, default=300.0,
        help="Simulated time for one full orbit revolution (default: 300.0)",
    )
    parser.add_argument("--altitude-ft", type=float, default=35000.0, help="Base cruise altitude in feet (default: 35000.0)")
    parser.add_argument(
        "--matched-rule", default=None,
        help="If given, every simulated aircraft's metadata reports this identifier in "
        "matched_rules, exercising the map's rule-match indicator (default: none)",
    )
    args = parser.parse_args()

    if args.aircraft_count < 1:
        print("--aircraft-count must be at least 1", file=sys.stderr)
        sys.exit(1)
    if args.position_rate <= 0:
        print("--position-rate must be greater than 0", file=sys.stderr)
        sys.exit(1)

    addr = (args.host, args.port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    stop_event = threading.Event()

    def _shutdown(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    def _progress(counts: dict, elapsed: float) -> None:
        total = counts["position"] + counts["metadata"] + counts["heartbeat"]
        rate = total / elapsed if elapsed > 0 else 0
        print(
            f"  {counts['position']} position, {counts['metadata']} metadata, "
            f"{counts['heartbeat']} heartbeat  ({rate:.0f} pkt/s)",
            flush=True,
        )

    print(
        f"Sending to {args.host}:{args.port} ({args.mode} mode): "
        f"{args.aircraft_count} aircraft @ {args.position_rate}/s each, "
        f"processor_id={args.processor_id!r} ...",
        flush=True,
    )

    start = time.monotonic()
    try:
        counts = run(
            sock,
            addr,
            aircraft_count=args.aircraft_count,
            position_rate=args.position_rate,
            metadata_interval=args.metadata_interval,
            processor_id=args.processor_id,
            duration=args.duration,
            mode=args.mode,
            center_lat=args.center_lat,
            center_lon=args.center_lon,
            radius_nm=args.radius_nm,
            orbit_period_seconds=args.orbit_period_seconds,
            altitude_ft=args.altitude_ft,
            matched_rule=args.matched_rule,
            stop_event=stop_event,
            on_progress=_progress,
        )
    except KeyboardInterrupt:
        stop_event.set()
        counts = {"position": 0, "metadata": 0, "heartbeat": 0}
    finally:
        sock.close()

    elapsed = time.monotonic() - start
    total = counts["position"] + counts["metadata"] + counts["heartbeat"]
    rate = total / elapsed if elapsed > 0 else 0
    print(
        f"\nDone: {counts['position']} position, {counts['metadata']} metadata, "
        f"{counts['heartbeat']} heartbeat datagrams in {elapsed:.1f}s ({rate:.0f} pkt/s average)"
    )


if __name__ == "__main__":
    main()
