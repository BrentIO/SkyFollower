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

Beyond raw throughput, the simulated fleet is a believable scenario: real
parallel traffic lanes at FAA-hemispheric-correct altitudes, varied
aircraft types (airliners, GA singles, business jets, helicopters, a
balloon), a VFR/IFR mix, migrating emergency squawks, and a staggered
startup ramp-up -- so a load run also exercises `map`'s altitude-layering,
icon-shape-by-type resolution, and emergency-squawk handling.

This is a backend + WebSocket-relay load generator only -- it never opens a
browser, so it says nothing about actual frontend rendering cost. Use it to
exercise `map`'s UDP ingestion rate, Redis write rate, and WS broadcast
behavior; a headless-browser rendering benchmark is a separate concern.

Usage:
    python main.py --host 192.168.1.20 --port 30500 \
        --aircraft-count 200 --position-rate 2 --duration 300

    python main.py --host 192.168.1.20 --port 30500 \
        --aircraft-count 50 --mode stress
"""

from __future__ import annotations

import argparse
import itertools
import json
import math
import os
import random
import signal
import socket
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

from shared.timing import (
    MAP_HEARTBEAT_INTERVAL_SECONDS,
    MAP_METADATA_RESEND_INTERVAL_SECONDS,
)

# Nautical miles per degree of latitude -- used to convert nautical-mile
# distances (lane spacing, area radius, balloon drift) into degrees for the
# lat/lon math below. Treated as a constant (not geodesically exact, which
# varies slightly with latitude) since this tool only needs "genuinely
# moving, roughly the right scale", not survey-grade positioning.
_NM_PER_DEGREE_LATITUDE = 60.0

# In-lane aircraft spacing, nautical miles -- a string of aircraft
# following the same route in sequence, per the request.
_LANE_SPACING_NM = 7.0

# ---------------------------------------------------------------------------
# FAA hemispheric cruising altitude/direction rule (14 CFR 91.159/91.179):
# magnetic course 000-179 ("eastbound") flies odd thousands (VFR +500 on
# top); course 180-359 ("westbound") flies even thousands (VFR +500). At/
# above FL180 (Class A -- IFR only), the same odd/even split continues in
# flight levels instead of MSL thousands, with no VFR variant.
# ---------------------------------------------------------------------------
_IFR_EAST_BELOW_FL180 = (3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000)
_IFR_WEST_BELOW_FL180 = (4000, 6000, 8000, 10000, 12000, 14000, 16000)
_VFR_EAST_BELOW_FL180 = tuple(alt + 500 for alt in _IFR_EAST_BELOW_FL180)
_VFR_WEST_BELOW_FL180 = tuple(alt + 500 for alt in _IFR_WEST_BELOW_FL180)
_IFR_EAST_AT_ABOVE_FL180 = (19000, 21000, 23000, 25000, 27000, 29000)  # FL190-FL290
_IFR_WEST_AT_ABOVE_FL180 = (20000, 22000, 24000, 26000, 28000)  # FL200-FL280

_ALL_EAST_ALTITUDES = _IFR_EAST_BELOW_FL180 + _VFR_EAST_BELOW_FL180 + _IFR_EAST_AT_ABOVE_FL180
_ALL_WEST_ALTITUDES = _IFR_WEST_BELOW_FL180 + _VFR_WEST_BELOW_FL180 + _IFR_WEST_AT_ABOVE_FL180


def is_valid_hemispheric_altitude(altitude_ft: float, eastbound: bool) -> bool:
    """True if `altitude_ft` is a legal FAA hemispheric cruising altitude
    (14 CFR 91.159/91.179) for the given direction -- eastbound (magnetic
    course 000-179) or westbound (180-359). Every lane altitude this tool
    ever assigns is drawn from these exact tables, so this doubles as the
    invariant the test suite checks against."""
    pool = _ALL_EAST_ALTITUDES if eastbound else _ALL_WEST_ALTITUDES
    return altitude_ft in pool


def altitude_flight_rule(altitude_ft: float) -> str:
    """'IFR' for a round-thousand altitude (or any flight level at/above
    FL180, Class A airspace being IFR-only) and 'VFR' for a below-FL180
    +500 altitude. A pure derivation from the value itself -- every
    altitude in the hemispheric tables above is unambiguously one or the
    other, so no separate "rule" needs to be threaded through alongside
    it."""
    if altitude_ft >= 18000:
        return "IFR"
    return "VFR" if altitude_ft % 1000 == 500 else "IFR"


# ---------------------------------------------------------------------------
# Lanes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Lane:
    """One straight traffic lane: a fixed line (a latitude, for an E-W
    lane; a longitude, for a N-S lane) at a fixed hemispheric-correct
    altitude, spanning `length_nm` between two geographic endpoints.
    `start_coord`/`end_coord` are the longitude (E-W) or latitude (N-S) of
    the lane's distance-0 and distance-`length_nm` ends respectively, in
    the direction of travel -- so "distance travelled" is always a single
    increasing number regardless of which way the lane physically points."""

    orientation: str  # "EW" or "NS"
    eastbound: bool  # True = east/north (odd hemisphere); False = west/south (even)
    altitude_ft: float
    length_nm: float
    fixed_coord: float  # latitude (EW) or longitude (NS)
    start_coord: float  # longitude (EW) or latitude (NS) at distance 0
    end_coord: float  # longitude (EW) or latitude (NS) at distance length_nm

    @property
    def flight_rule(self) -> str:
        return altitude_flight_rule(self.altitude_ft)

    @property
    def heading(self) -> float:
        if self.orientation == "EW":
            return 90.0 if self.eastbound else 270.0
        return 0.0 if self.eastbound else 180.0


def lane_position(lane: Lane, distance_nm: float) -> tuple[float, float]:
    """(lat, lon) at `distance_nm` travelled from the lane's entry point
    (0) toward its exit point (`lane.length_nm`), clamped to that range."""
    frac = 0.0 if lane.length_nm <= 0 else max(0.0, min(1.0, distance_nm / lane.length_nm))
    coord = lane.start_coord + frac * (lane.end_coord - lane.start_coord)
    if lane.orientation == "EW":
        return lane.fixed_coord, coord
    return coord, lane.fixed_coord


def build_lanes(center_lat: float, center_lon: float, area_radius_nm: float) -> list[Lane]:
    """The full catalog of hemispheric-correct lanes available in a run:
    every (altitude, direction) combination from the tables above, split
    across E-W and N-S orientations (alternating as the combined,
    altitude-sorted list is walked) and spread evenly across the area so
    lanes are geographically distinct rather than stacked on the same
    line. Pure and deterministic -- no randomness -- so hemispheric
    correctness is independently verifiable."""
    entries = [(alt, True) for alt in _ALL_EAST_ALTITUDES] + [(alt, False) for alt in _ALL_WEST_ALTITUDES]
    entries.sort(key=lambda e: e[0])

    lat_deg_radius = area_radius_nm / _NM_PER_DEGREE_LATITUDE
    lon_scale = math.cos(math.radians(center_lat))
    if abs(lon_scale) < 1e-9:
        lon_scale = 1e-9
    lon_deg_radius = area_radius_nm / (_NM_PER_DEGREE_LATITUDE * lon_scale)
    length_nm = 2.0 * area_radius_nm

    ew_entries = [e for i, e in enumerate(entries) if i % 2 == 0]
    ns_entries = [e for i, e in enumerate(entries) if i % 2 == 1]

    lanes: list[Lane] = []
    for group, orientation in ((ew_entries, "EW"), (ns_entries, "NS")):
        n = len(group)
        for idx, (altitude_ft, eastbound) in enumerate(group):
            offset_frac = 0.0 if n <= 1 else (idx / (n - 1)) * 2.0 - 1.0  # spread -1..1
            if orientation == "EW":
                fixed_coord = center_lat + offset_frac * lat_deg_radius
                west_lon = center_lon - lon_deg_radius
                east_lon = center_lon + lon_deg_radius
                start_coord, end_coord = (west_lon, east_lon) if eastbound else (east_lon, west_lon)
            else:
                fixed_coord = center_lon + offset_frac * lon_deg_radius
                south_lat = center_lat - lat_deg_radius
                north_lat = center_lat + lat_deg_radius
                start_coord, end_coord = (south_lat, north_lat) if eastbound else (north_lat, south_lat)
            lanes.append(Lane(orientation, eastbound, float(altitude_ft), length_nm, fixed_coord, start_coord, end_coord))
    return lanes


def initial_start_distance_nm(lane: Lane, sequence_index: int) -> float:
    """An initial-fill aircraft's starting distance along its lane: a
    randomized per-run phase, plus `sequence_index * 7nm` so aircraft
    sharing a lane at startup form a spaced-out string rather than
    stacking at the same point. Wrapped into the lane's own length so a
    high sequence index still lands somewhere on the line."""
    phase = random.uniform(0.0, _LANE_SPACING_NM)
    span = max(lane.length_nm, _LANE_SPACING_NM)
    return (phase + sequence_index * _LANE_SPACING_NM) % span


def respawn_start_distance_nm(lane: Lane) -> float:
    """A replacement aircraft's starting distance: anywhere along the
    lane, freshly randomized -- matches "start/finish point... randomized
    per run" for the aircraft that fills a vacancy after a lane exit."""
    return random.uniform(0.0, lane.length_nm)


# ---------------------------------------------------------------------------
# Speed / vertical-rate model
# ---------------------------------------------------------------------------

_SPEED_LOW_ALT_FT, _SPEED_LOW_KT = 100.0, 60.0
_SPEED_HIGH_ALT_FT, _SPEED_HIGH_KT = 40000.0, 500.0
_VERTICAL_SPEED_CLAMP_FPM = 800.0
_CLIMB_RATE_FPM = 500.0  # comfortably within the +/-800ft/min clamp


def groundspeed_kt_for_altitude(altitude_ft: float) -> float:
    """Groundspeed linearly interpolated between 100ft->60kt and
    40,000ft->500kt (clamped at both ends), so low-altitude traffic comes
    out around real light-aircraft/helicopter speeds and high-altitude
    lanes come out around real airliner cruise speeds, with no separate
    per-type speed table. Not used for the balloon (outside this
    formula's domain -- see `balloon_position`)."""
    t = (altitude_ft - _SPEED_LOW_ALT_FT) / (_SPEED_HIGH_ALT_FT - _SPEED_LOW_ALT_FT)
    t = max(0.0, min(1.0, t))
    return _SPEED_LOW_KT + t * (_SPEED_HIGH_KT - _SPEED_LOW_KT)


def clamp_vertical_speed_fpm(vs_fpm: float) -> float:
    return max(-_VERTICAL_SPEED_CLAMP_FPM, min(_VERTICAL_SPEED_CLAMP_FPM, vs_fpm))


def climb_altitude_and_vs(
    elapsed_since_spawn_seconds: float, alt_lo: float, alt_hi: float, rate_fpm: float = _CLIMB_RATE_FPM
) -> tuple[float, float]:
    """Altitude and vertical speed for a climbing/descending aircraft that
    triangle-waves between `alt_lo` and `alt_hi` at a steady `rate_fpm`
    (climbing, then descending, repeating) -- a pure function of elapsed
    time since spawn, so it's fully deterministic and testable despite the
    rest of the fleet having no seed."""
    span = alt_hi - alt_lo
    if span <= 0:
        return alt_lo, 0.0
    leg_minutes = span / rate_fpm
    period_minutes = 2.0 * leg_minutes
    elapsed_minutes = elapsed_since_spawn_seconds / 60.0
    t = elapsed_minutes % period_minutes
    if t <= leg_minutes:
        return alt_lo + rate_fpm * t, clamp_vertical_speed_fpm(rate_fpm)
    return alt_hi - rate_fpm * (t - leg_minutes), clamp_vertical_speed_fpm(-rate_fpm)


def climb_altitude_pair(lane: Lane) -> tuple[float, float]:
    """Two distinct, hemispherically-valid altitudes for a climbing
    aircraft assigned to `lane`: its lane's own altitude, and the next
    altitude up in that direction's table (wrapping to the lowest if the
    lane already holds the highest), so a climbing aircraft's endpoints
    are always both legal cruising altitudes for its direction even
    though it passes through non-table values in between."""
    pool = _ALL_EAST_ALTITUDES if lane.eastbound else _ALL_WEST_ALTITUDES
    lo = lane.altitude_ft
    idx = pool.index(lo)
    hi = pool[(idx + 1) % len(pool)]
    return (lo, hi) if lo <= hi else (hi, lo)


# ---------------------------------------------------------------------------
# Balloon (outside the lane grid and the altitude-interpolation speed model)
# ---------------------------------------------------------------------------

_BALLOON_ALTITUDE_FT = 60000.0
_BALLOON_ALTITUDE_WANDER_FT = 400.0
_BALLOON_ALTITUDE_WANDER_PERIOD_S = 311.0  # deliberately non-round, avoids visual lockstep
_BALLOON_HEADING_WANDER_DEG = 20.0
_BALLOON_HEADING_WANDER_PERIOD_S = 173.0
_BALLOON_BASE_HEADINGS_DEG = (45.0, 135.0, 225.0, 315.0)  # diagonal -- off the E-W/N-S lane grid
_BALLOON_DRIFT_KT_RANGE = (15.0, 25.0)


def balloon_position(
    elapsed_since_spawn_seconds: float, *, spawn_lat: float, spawn_lon: float, base_heading_deg: float, drift_kt: float
) -> dict:
    """A gentle, organic high-altitude balloon track: a steady diagonal
    drift at `drift_kt` (not part of the lane grid, not derived from
    `groundspeed_kt_for_altitude`), with a slowly meandering heading and a
    slow altitude wander around 60,000ft -- closer to how a real balloon
    rides the jet stream than a perfectly straight, perfectly level
    line."""
    heading = (base_heading_deg + _BALLOON_HEADING_WANDER_DEG * math.sin(
        elapsed_since_spawn_seconds / _BALLOON_HEADING_WANDER_PERIOD_S
    )) % 360.0
    distance_nm = drift_kt * elapsed_since_spawn_seconds / 3600.0
    heading_rad = math.radians(heading)
    lat = spawn_lat + (distance_nm / _NM_PER_DEGREE_LATITUDE) * math.cos(heading_rad)
    lon_scale = math.cos(math.radians(spawn_lat))
    if abs(lon_scale) < 1e-9:
        lon_scale = 1e-9
    lon = spawn_lon + (distance_nm / (_NM_PER_DEGREE_LATITUDE * lon_scale)) * math.sin(heading_rad)
    altitude = _BALLOON_ALTITUDE_FT + _BALLOON_ALTITUDE_WANDER_FT * math.sin(
        elapsed_since_spawn_seconds / _BALLOON_ALTITUDE_WANDER_PERIOD_S
    )
    # d(altitude)/dt (ft/min), the exact derivative of the sine wander above.
    vs = clamp_vertical_speed_fpm(
        _BALLOON_ALTITUDE_WANDER_FT
        * (60.0 / _BALLOON_ALTITUDE_WANDER_PERIOD_S)
        * math.cos(elapsed_since_spawn_seconds / _BALLOON_ALTITUDE_WANDER_PERIOD_S)
    )
    return {
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "alt": round(altitude, 1),
        "velocity": round(drift_kt, 1),
        "hdg": round(heading, 1),
        "vs": round(vs, 1),
    }


def tick_elapsed_seconds(tick: int, position_rate: float) -> float:
    """The simulated elapsed time represented by tick `tick` when sending
    `position_rate` position datagrams per second per aircraft. Kept as its
    own pure function so `--mode stress` can advance simulated motion at
    this same rate while not actually sleeping between sends (see
    `run()`)."""
    return tick / position_rate if position_rate > 0 else float(tick)


# ---------------------------------------------------------------------------
# Synthetic identity
# ---------------------------------------------------------------------------

# Shared default so synthetic_registration can strip exactly the prefix
# synthetic_icao_hex actually used, rather than a second hardcoded "FF".
_ICAO_HEX_PREFIX = "FF"


def synthetic_icao_hex(aircraft_number: int, prefix: str = _ICAO_HEX_PREFIX) -> str:
    """A distinct, obviously-synthetic 6-character hex ICAO address: a
    fixed prefix (default "FF", an unallocated ICAO address block) plus a
    zero-padded hex counter filling the remaining digits. `aircraft_number`
    is a run-lifetime-unique, ever-incrementing counter (not a fleet-slot
    index), so a replacement aircraft spawned after a lane exit always
    gets a hex that has never appeared before in this run."""
    width = 6 - len(prefix)
    if width <= 0:
        raise ValueError("prefix must be shorter than 6 characters")
    return f"{prefix}{aircraft_number:0{width}X}"[:6].upper()


def synthetic_ident(aircraft_number: int) -> str:
    """A distinct, obviously-synthetic flight ident."""
    return f"LOAD{aircraft_number:04d}"


def synthetic_registration(icao_hex: str, prefix: str = _ICAO_HEX_PREFIX) -> str:
    """A synthetic tail number derived from `icao_hex`, stable per occupant
    with no new counter needed: `icao_hex`'s own synthetic prefix (default
    "FF") replaced with "N" (e.g. "FF0001" -> "N0001"). None of this tool's
    synthetic occupants carry a real airline-style flight number distinct
    from their own tail number, so this is registration's only source."""
    return "N" + icao_hex.removeprefix(prefix)


# Real-world codes this tool must never hand out as an ordinary "synthetic
# discrete" IFR squawk: the VFR code and the four emergency codes -- those
# are assigned deliberately elsewhere (VFR assignment, emergency migration).
_RESERVED_SQUAWKS = frozenset({"1200", "7500", "7600", "7700", "7777"})


def synthetic_squawk(aircraft_number: int) -> str:
    """A 4-digit octal squawk code (each digit 0-7) derived from
    `aircraft_number`, nudged off any of the reserved VFR/emergency codes
    it might otherwise land on exactly."""
    digits = "".join(str((aircraft_number >> (3 * i)) & 0x7) for i in range(4))[::-1]
    if digits in _RESERVED_SQUAWKS:
        digits = digits[:-1] + str((int(digits[-1]) + 1) % 8)
    return digits


_EMERGENCY_SQUAWK_CODES = ("7500", "7600", "7700", "7777")
_EMERGENCY_SQUAWK_COUNT = 2

_RECEIVER_SOURCE_POOL = ("1090", "978", "EXTERNAL")


def random_receiver_sources() -> list[str]:
    """A randomly chosen, randomly sized (1-3) subset of the three real
    receiver source tags -- matches a real aircraft heard by more than one
    receiver type."""
    k = random.randint(1, len(_RECEIVER_SOURCE_POOL))
    return random.sample(_RECEIVER_SOURCE_POOL, k)


# ---------------------------------------------------------------------------
# Fleet variety: real ICAO type designators recognized by
# map/frontend/src/lib/aircraftIconResolver.ts -- see that file's
# AIRCRAFT_SHAPES-generating SVG set (src/assets/aircraft-shapes/*.svg) and
# TYPE_ALIASES/DESCRIPTION_SHAPES tables. Every designator below either has
# a direct dedicated shape (A320, B738, B772, E170, DH8D, C172, SR22, PA46,
# P28A, C25B, GLF6, LJ35, FA7X, R44, EC35, F16, F15, BALL) or resolves to
# one through TYPE_ALIASES (S76 -> AS65) or DESCRIPTION_SHAPES (tiltrotor
# description code "T" -> V22SLOW, used for the V-22 Osprey livery
# aircraft, whose bare type designator "V22" has no direct/aliased entry).
# ---------------------------------------------------------------------------

_AIRLINER_TYPES = ("A320", "B738", "B772", "E170", "DH8D")
_GA_SINGLE_TYPES = ("C172", "SR22", "PA46", "P28A")
_BUSINESS_JET_TYPES = ("C25B", "GLF6", "LJ35", "FA7X")
# Interleaved (airliner, GA, bizjet, airliner, GA, bizjet, ...) so even a
# small fleet's first few "regular" picks span all three categories rather
# than exhausting one before touching the next.
_REGULAR_TYPE_POOL = tuple(
    t
    for triple in itertools.zip_longest(_AIRLINER_TYPES, _GA_SINGLE_TYPES, _BUSINESS_JET_TYPES)
    for t in triple
    if t is not None
)
_HELICOPTER_TYPES = ("R44", "EC35", "S76")  # piston GA / light-twin EMS / medium-twin offshore
_MILITARY_TYPES = ("F16", "F15")
_LIVERY_TYPE_DESIGNATOR = "V22"
_LIVERY_DESCRIPTION_CODE = "T"  # ICAO Doc 8643 tiltrotor -> aircraftIconResolver's V22SLOW shape
_LIVERY_NAME = "Overclocked Osprey"
_BALLOON_TYPE_DESIGNATOR = "BALL"
_EMITTER_CATEGORY_POOL = ("A1", "A2", "A3", "A4", "A5", "A6", "A7", "B1", "B2", "B4", "B6", "B7")
_WAKE_TURBULENCE_CATEGORIES = ("light", "medium", "heavy")

_MILITARY_SEAT_COUNT = 1
_LIVERY_SEAT_COUNT = 1
_NO_METADATA_SEAT_COUNT = 2
_HELICOPTER_SEAT_COUNT = 3

# manufacturer_model + ICAO Doc 8643 description_code for every type
# designator this tool hands out (the three interleaved pools above, the
# helicopter/military pools, and the livery aircraft's own V22) -- so every
# role that gets a type_designator also populates the Detail panel's
# Manufacturer/Model row and the aircraft list's Desc column, not just the
# livery role (which already carries its own description_code separately,
# matching this table's V22 entry).
_TYPE_INFO: dict[str, tuple[str, str]] = {
    "A320": ("AIRBUS A320", "L2J"),
    "B738": ("BOEING 737-800", "L2J"),
    "B772": ("BOEING 777-200", "L2J"),
    "E170": ("EMBRAER 170", "L2J"),
    "DH8D": ("DE HAVILLAND DHC-8-400", "L2T"),
    "C172": ("CESSNA 172 SKYHAWK", "L1P"),
    "SR22": ("CIRRUS SR22", "L1P"),
    "PA46": ("PIPER PA-46 MALIBU", "L1P"),
    "P28A": ("PIPER PA-28 CHEROKEE", "L1P"),
    "C25B": ("CESSNA CITATION CJ3", "L2J"),
    "GLF6": ("GULFSTREAM G650", "L2J"),
    "LJ35": ("LEARJET 35", "L2J"),
    "FA7X": ("DASSAULT FALCON 7X", "L3J"),
    "R44": ("ROBINSON R44", "H1P"),
    "EC35": ("AIRBUS HELICOPTERS EC135", "H2T"),
    "S76": ("SIKORSKY S-76", "H2T"),
    "F16": ("GENERAL DYNAMICS F-16 FIGHTING FALCON", "L1J"),
    "F15": ("MCDONNELL DOUGLAS F-15 EAGLE", "L2J"),
    _LIVERY_TYPE_DESIGNATOR: ("BELL BOEING V-22 OSPREY", _LIVERY_DESCRIPTION_CODE),
}

# Synthetic operator/airport/registrant pools -- obviously fake designators/
# codes/names, just enough shape to populate map's info panel fields during
# a load run.
_SYNTHETIC_OPERATORS = (
    {"airline_designator": "LG1", "name": "Load Generator One", "callsign": "LOADGEN", "country": "Load Country"},
    {"airline_designator": "LG2", "name": "Load Generator Two", "callsign": "LOADRUN", "country": "Load Country"},
    {"airline_designator": "LG3", "name": "Load Generator Three", "callsign": "LOADTEST", "country": "Load Country"},
)
_SYNTHETIC_AIRPORT_CODES = tuple(f"ZZ{n:02d}" for n in range(10))
_SYNTHETIC_REGISTRANT = {"names": ["Load Generator Holdings LLC"]}


def assign_seat_roles(lane_seat_count: int, climbing_count: int) -> dict[int, str]:
    """Randomly assigns each lane seat index a role -- "military",
    "livery", "no_metadata", "helicopter", "climbing", or "regular" --
    with fixed small counts for the special roles (graceful for a small
    `lane_seat_count`: each role gets `min(its count, seats remaining)`,
    so a tiny fleet just comes up short on variety rather than raising).
    Roles are mutually exclusive and, once assigned, persist for the seat
    for the life of the run -- only the *occupant* (icao_hex/ident/squawk/
    etc.) changes across a lane-exit replacement."""
    indices = list(range(lane_seat_count))
    random.shuffle(indices)
    roles: dict[int, str] = {}

    def _take(n: int) -> list[int]:
        nonlocal indices
        chosen, indices = indices[:n], indices[n:]
        return chosen

    for i in _take(min(_MILITARY_SEAT_COUNT, len(indices))):
        roles[i] = "military"
    for i in _take(min(_LIVERY_SEAT_COUNT, len(indices))):
        roles[i] = "livery"
    for i in _take(min(_NO_METADATA_SEAT_COUNT, len(indices))):
        roles[i] = "no_metadata"
    for i in _take(min(_HELICOPTER_SEAT_COUNT, len(indices))):
        roles[i] = "helicopter"
    for i in _take(min(max(climbing_count, 0), len(indices))):
        roles[i] = "climbing"
    for i in indices:
        roles[i] = "regular"
    return roles


@dataclass(frozen=True)
class Seat:
    """A persistent fleet slot: a lane assignment and a role that outlive
    any one occupant. When a seat's current occupant exits its lane, a
    fresh occupant takes the same seat (same lane, same role) -- keeping
    the fleet's overall composition (how many helicopters, how many
    no-metadata aircraft, etc.) stable across a long run even as
    individual tail numbers churn."""

    seat_index: int
    lane: Lane
    role: str
    type_designator: str | None  # fixed per role; None for "military" (randomized per occupant) and "no_metadata"
    climb_altitudes: tuple[float, float] | None = None  # only set for role == "climbing"


def build_seats(lanes: list[Lane], lane_seat_count: int, climbing_count: int) -> list[Seat]:
    role_by_index = assign_seat_roles(lane_seat_count, climbing_count)
    heli_i = 0
    regular_i = 0
    seats: list[Seat] = []
    for i in range(lane_seat_count):
        lane = lanes[i % len(lanes)]
        role = role_by_index[i]
        type_designator: str | None = None
        climb_altitudes: tuple[float, float] | None = None
        if role == "livery":
            type_designator = _LIVERY_TYPE_DESIGNATOR
        elif role == "helicopter":
            type_designator = _HELICOPTER_TYPES[heli_i % len(_HELICOPTER_TYPES)]
            heli_i += 1
        elif role == "climbing":
            type_designator = _REGULAR_TYPE_POOL[regular_i % len(_REGULAR_TYPE_POOL)]
            regular_i += 1
            climb_altitudes = climb_altitude_pair(lane)
        elif role == "regular":
            type_designator = _REGULAR_TYPE_POOL[regular_i % len(_REGULAR_TYPE_POOL)]
            regular_i += 1
        # "military" and "no_metadata" leave type_designator as None here --
        # resolved per-occupant in spawn_occupant().
        seats.append(Seat(i, lane, role, type_designator, climb_altitudes))
    return seats


@dataclass
class Occupant:
    """One simulated aircraft currently holding a seat (or the standalone
    balloon). Mutable only in `metadata_last_sent_monotonic`, which tracks
    this tool's own resend cadence."""

    icao_hex: str
    ident: str
    squawk: str
    operator: dict | None
    origin: dict | None
    destination: dict | None
    registrant: dict | None
    receiver_sources: list[str]
    aircraft_extra: dict
    spawn_elapsed_seconds: float
    start_distance_nm: float
    metadata_last_sent_monotonic: float | None = None


def spawn_occupant(
    seat: Seat,
    aircraft_number: int,
    *,
    spawn_elapsed_seconds: float,
    start_distance_nm: float,
    fixed_origin: dict,
    fixed_destination: dict,
) -> Occupant:
    """A freshly-randomized occupant for `seat`, matching its role (type
    variety, no-metadata/military/livery special-casing) and its lane's
    VFR/IFR flight rule (squawk 1200 + no operator/origin/destination for
    VFR; a synthetic discrete squawk + operator + the one fixed origin/
    destination pair for IFR)."""
    icao_hex = synthetic_icao_hex(aircraft_number)
    ident = synthetic_ident(aircraft_number)

    if seat.lane.flight_rule == "VFR":
        squawk = "1200"
        operator = None
        origin = None
        destination = None
        registrant = None
    else:
        squawk = synthetic_squawk(aircraft_number)
        operator = dict(random.choice(_SYNTHETIC_OPERATORS))
        origin = dict(fixed_origin)
        destination = dict(fixed_destination)
        registrant = dict(_SYNTHETIC_REGISTRANT)

    aircraft_extra: dict = {}
    if seat.role == "no_metadata":
        # Simulates an enrichment-database miss: only the raw ADS-B-
        # broadcast emitter category, nothing looked up.
        aircraft_extra["emitter_category"] = random.choice(_EMITTER_CATEGORY_POOL)
    else:
        type_designator = _MILITARY_TYPES[random.randrange(len(_MILITARY_TYPES))] if seat.role == "military" else seat.type_designator
        aircraft_extra["type_designator"] = type_designator
        aircraft_extra["category"] = "Land"
        aircraft_extra["wake_turbulence_category"] = random.choice(_WAKE_TURBULENCE_CATEGORIES)
        aircraft_extra["registration"] = synthetic_registration(icao_hex)
        type_info = _TYPE_INFO.get(type_designator)
        if type_info:
            aircraft_extra["manufacturer_model"], aircraft_extra["description_code"] = type_info
        if seat.role == "military":
            aircraft_extra["military"] = True
        if seat.role == "livery":
            aircraft_extra["special_livery"] = _LIVERY_NAME

    return Occupant(
        icao_hex=icao_hex,
        ident=ident,
        squawk=squawk,
        operator=operator,
        origin=origin,
        destination=destination,
        registrant=registrant,
        receiver_sources=random_receiver_sources(),
        aircraft_extra=aircraft_extra,
        spawn_elapsed_seconds=spawn_elapsed_seconds,
        start_distance_nm=start_distance_nm,
    )


# ---------------------------------------------------------------------------
# Packet construction (pure -- no socket I/O)
# ---------------------------------------------------------------------------

def build_position_packet(icao_hex: str, processor_id: str, ts: float, position: dict) -> dict:
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
    registrant: dict | None = None,
    squawk: str | None = None,
    matched_rules: list[str] | None = None,
    receiver_sources: list[str] | None = None,
    aircraft_extra: dict | None = None,
) -> dict:
    """A `metadata` datagram shaped like message-processor's CompletedFlight
    notification payload (`_build_flight_notification_payload()`) plus
    `"type": "metadata"` -- icao_hex nested under `aircraft` (along with
    whatever `aircraft_extra` fields this occupant carries -- type_
    designator/category/wake_turbulence_category/registration/
    manufacturer_model/description_code/military/special_livery/
    emitter_category), never top-level (see map/main.py's `_handle_packet`),
    `registrant` as its own sibling field (matching production's wire
    shape -- see message-processor's Flight.registrant), and `last_message`
    as the out-of-order guard's clock for this packet type (metadata
    carries no `ts` -- see map/main.py's `_extract_timestamp`). Optional
    fields are omitted entirely rather than sent as null/empty, matching
    `_build_flight_notification_payload`'s own falsy-drop convention for
    operator/origin/destination."""
    packet: dict = {
        "type": "metadata",
        "aircraft": {"icao_hex": icao_hex, **(aircraft_extra or {})},
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
    if registrant:
        packet["registrant"] = registrant
    if squawk:
        packet["squawk"] = squawk
    if matched_rules:
        packet["matched_rules"] = matched_rules
    if receiver_sources:
        packet["receiver_sources"] = receiver_sources
    return packet


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Fleet simulation
# ---------------------------------------------------------------------------

class FleetSimulator:
    """Owns every seat/occupant/lane/balloon/emergency-squawk-migration
    piece of run-lifetime state, and turns simulated elapsed time into the
    set of currently-active aircraft positions -- independent of real
    sockets or wall-clock time, so it's directly unit-testable. `run()`
    is a thin wrapper that feeds it real elapsed time and sends whatever
    it returns."""

    def __init__(
        self,
        *,
        aircraft_count: int,
        center_lat: float,
        center_lon: float,
        area_radius_nm: float,
        climbing_count: int,
        processor_id: str,
        matched_rule: str | None,
        ramp_up_seconds: float,
        duration: float,
    ) -> None:
        self.processor_id = processor_id
        self.secondary_processor_id = f"{processor_id}-secondary"
        self.matched_rule = matched_rule

        # The balloon always occupies one of --aircraft-count's total; every
        # other aircraft is a lane seat.
        self.lane_seat_count = max(0, aircraft_count - 1)
        self.lanes = build_lanes(center_lat, center_lon, area_radius_nm)
        self.seats = build_seats(self.lanes, self.lane_seat_count, climbing_count)

        self.fixed_origin = {
            "icao_code": _SYNTHETIC_AIRPORT_CODES[0],
            "iata_code": _SYNTHETIC_AIRPORT_CODES[0][-3:],
            "name": f"Load Test Airport {_SYNTHETIC_AIRPORT_CODES[0]}",
            "city": "Load City",
            "region": "Load Region",
            "country": "Load Country",
        }
        self.fixed_destination = {
            "icao_code": _SYNTHETIC_AIRPORT_CODES[1],
            "iata_code": _SYNTHETIC_AIRPORT_CODES[1][-3:],
            "name": f"Load Test Airport {_SYNTHETIC_AIRPORT_CODES[1]}",
            "city": "Load Destination City",
            "region": "Load Destination Region",
            "country": "Load Destination Country",
        }

        # Ramp-up is disabled outright (not scaled down) when a nonzero
        # --duration is shorter than it -- a short deliberate run shouldn't
        # spend its whole duration still ramping up.
        effective_ramp_up = ramp_up_seconds
        if duration and ramp_up_seconds and duration < ramp_up_seconds:
            effective_ramp_up = 0.0
        self.ramp_up_seconds = max(0.0, effective_ramp_up)

        per_aircraft_stagger = (self.ramp_up_seconds / aircraft_count) if (self.ramp_up_seconds and aircraft_count) else 0.0
        self.start_offsets: dict[int, float] = {i: i * per_aircraft_stagger for i in range(self.lane_seat_count)}
        # Independent of the lane-seat stagger sequence entirely (was
        # `lane_seat_count * per_aircraft_stagger` -- exactly one slot after
        # the very last lane seat, which approaches the *entire* ramp-up
        # window as --aircraft-count grows). The balloon is the one aircraft
        # here meant to exercise the BALL icon shape and its accent-cutout
        # detail, so it should be visible promptly like everything else
        # rather than guaranteed to appear last.
        self.balloon_start_offset = 0.0

        self._next_aircraft_number = 0
        self.occupants: dict[int, Occupant] = {}
        for seat in self.seats:
            self.occupants[seat.seat_index] = self._new_occupant(seat, spawn_elapsed_seconds=self.start_offsets[seat.seat_index], initial=True)

        self.emergency_seats: dict[int, str] = {}
        emergency_candidates = list(range(self.lane_seat_count))
        random.shuffle(emergency_candidates)
        for seat_index in emergency_candidates[: min(_EMERGENCY_SQUAWK_COUNT, len(emergency_candidates))]:
            self.emergency_seats[seat_index] = random.choice(_EMERGENCY_SQUAWK_CODES)

        balloon_number = self._take_aircraft_number()
        self.balloon_spawn_lat = center_lat + random.uniform(-0.05, 0.05)
        self.balloon_spawn_lon = center_lon + random.uniform(-0.05, 0.05)
        self.balloon_base_heading = random.choice(_BALLOON_BASE_HEADINGS_DEG)
        self.balloon_drift_kt = random.uniform(*_BALLOON_DRIFT_KT_RANGE)
        self.balloon_occupant = Occupant(
            icao_hex=synthetic_icao_hex(balloon_number),
            ident=synthetic_ident(balloon_number),
            squawk="1200",
            operator=None,
            origin=None,
            destination=None,
            registrant=None,
            receiver_sources=random_receiver_sources(),
            aircraft_extra={
                "type_designator": _BALLOON_TYPE_DESIGNATOR,
                "wake_turbulence_category": random.choice(_WAKE_TURBULENCE_CATEGORIES),
            },
            spawn_elapsed_seconds=self.balloon_start_offset,
            start_distance_nm=0.0,
        )

    def _take_aircraft_number(self) -> int:
        n = self._next_aircraft_number
        self._next_aircraft_number += 1
        return n

    def _new_occupant(self, seat: Seat, *, spawn_elapsed_seconds: float, initial: bool) -> Occupant:
        aircraft_number = self._take_aircraft_number()
        if initial:
            sequence_index = seat.seat_index // len(self.lanes)
            start_distance = initial_start_distance_nm(seat.lane, sequence_index)
        else:
            start_distance = respawn_start_distance_nm(seat.lane)
        return spawn_occupant(
            seat,
            aircraft_number,
            spawn_elapsed_seconds=spawn_elapsed_seconds,
            start_distance_nm=start_distance,
            fixed_origin=self.fixed_origin,
            fixed_destination=self.fixed_destination,
        )

    def _retire_and_replace(self, seat: Seat, elapsed_seconds: float) -> None:
        """The exiting occupant simply stops being tracked -- no synthetic
        "eviction" packet is ever sent, matching the real wire protocol.
        Its seat's replacement spawns immediately with a fresh, never-
        reused icao_hex."""
        was_emergency = seat.seat_index in self.emergency_seats
        if was_emergency:
            del self.emergency_seats[seat.seat_index]
        self.occupants[seat.seat_index] = self._new_occupant(seat, spawn_elapsed_seconds=elapsed_seconds, initial=False)
        if was_emergency:
            self._migrate_emergency_code(exclude_seat_index=seat.seat_index)

    def _migrate_emergency_code(self, exclude_seat_index: int) -> None:
        candidates = [i for i in range(self.lane_seat_count) if i != exclude_seat_index and i not in self.emergency_seats]
        if not candidates:
            return
        new_seat_index = random.choice(candidates)
        self.emergency_seats[new_seat_index] = random.choice(_EMERGENCY_SQUAWK_CODES)

    def occupant_squawk(self, seat_index: int | None, occupant: Occupant) -> str:
        """The squawk actually transmitted right now: an emergency code
        overrides the occupant's normal VFR/IFR squawk while its seat holds
        one."""
        if seat_index is not None and seat_index in self.emergency_seats:
            return self.emergency_seats[seat_index]
        return occupant.squawk

    def tick(self, elapsed_seconds: float) -> list[tuple[int | None, Occupant, dict]]:
        """Every currently-active (lane seat, balloon) occupant's position
        fields at this elapsed time, as `(seat_index, occupant, fields)`
        (`seat_index` is `None` for the balloon). A seat not yet past its
        ramp-up start offset, or whose occupant has just reached its
        lane's finish point (replaced as a side effect here), contributes
        nothing this tick."""
        results: list[tuple[int | None, Occupant, dict]] = []

        for seat in self.seats:
            if elapsed_seconds < self.start_offsets[seat.seat_index]:
                continue
            occupant = self.occupants[seat.seat_index]
            since_spawn = max(0.0, elapsed_seconds - occupant.spawn_elapsed_seconds)
            # Lane-progress speed is pinned to the seat's own lane altitude
            # (not the momentarily-climbing displayed altitude below) so a
            # climber's spacing/exit timing stays predictable; the
            # *displayed* velocity field still reflects its instantaneous
            # altitude, per the speed model.
            progress_nm = since_spawn * (groundspeed_kt_for_altitude(seat.lane.altitude_ft) / 3600.0)
            distance_nm = occupant.start_distance_nm + progress_nm
            if distance_nm >= seat.lane.length_nm:
                self._retire_and_replace(seat, elapsed_seconds)
                continue

            lat, lon = lane_position(seat.lane, distance_nm)
            if seat.role == "climbing" and seat.climb_altitudes is not None:
                alt, vs = climb_altitude_and_vs(since_spawn, seat.climb_altitudes[0], seat.climb_altitudes[1])
            else:
                alt, vs = seat.lane.altitude_ft, 0.0

            fields = {
                "lat": round(lat, 6),
                "lon": round(lon, 6),
                "alt": round(alt, 1),
                "velocity": round(groundspeed_kt_for_altitude(alt), 1),
                "hdg": round(seat.lane.heading, 1),
                "vs": round(clamp_vertical_speed_fpm(vs), 1),
            }
            results.append((seat.seat_index, occupant, fields))

        if elapsed_seconds >= self.balloon_start_offset:
            balloon_elapsed = elapsed_seconds - self.balloon_start_offset
            fields = balloon_position(
                balloon_elapsed,
                spawn_lat=self.balloon_spawn_lat,
                spawn_lon=self.balloon_spawn_lon,
                base_heading_deg=self.balloon_base_heading,
                drift_kt=self.balloon_drift_kt,
            )
            results.append((None, self.balloon_occupant, fields))

        return results


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
    area_radius_nm: float,
    climbing_count: int,
    ramp_up_seconds: float,
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
    sim = FleetSimulator(
        aircraft_count=aircraft_count,
        center_lat=center_lat,
        center_lon=center_lon,
        area_radius_nm=area_radius_nm,
        climbing_count=climbing_count,
        processor_id=processor_id,
        matched_rule=matched_rule,
        ramp_up_seconds=ramp_up_seconds,
        duration=duration,
    )

    counts = {"position": 0, "metadata": 0, "heartbeat": 0}
    start = time.monotonic()
    last_heartbeat = 0.0
    tick = 0
    last_progress = start

    def _send(payload: dict) -> None:
        sock.sendto(json.dumps(payload, separators=(",", ":")).encode("utf-8"), addr)

    # The second, one-shot processor: exactly one heartbeat, at run start,
    # then never again -- its roster entry ages from green to amber to red
    # purely from real wall-clock silence over a long-enough run.
    _send(build_heartbeat_packet(sim.secondary_processor_id, time.time()))
    counts["heartbeat"] += 1

    while not stop_event.is_set():
        now = time.monotonic()
        if duration and now - start >= duration:
            break

        elapsed = tick_elapsed_seconds(tick, position_rate)

        if now - last_heartbeat >= MAP_HEARTBEAT_INTERVAL_SECONDS:
            _send(build_heartbeat_packet(processor_id, time.time()))
            counts["heartbeat"] += 1
            last_heartbeat = now

        for seat_index, occupant, fields in sim.tick(elapsed):
            _send(build_position_packet(occupant.icao_hex, processor_id, time.time(), fields))
            counts["position"] += 1

            due = occupant.metadata_last_sent_monotonic is None or (now - occupant.metadata_last_sent_monotonic) >= metadata_interval
            if due:
                _send(
                    build_metadata_packet(
                        occupant.icao_hex,
                        occupant.ident,
                        processor_id,
                        _iso_now(),
                        operator=occupant.operator,
                        origin=occupant.origin,
                        destination=occupant.destination,
                        registrant=occupant.registrant,
                        squawk=sim.occupant_squawk(seat_index, occupant),
                        matched_rules=[matched_rule] if matched_rule else None,
                        receiver_sources=occupant.receiver_sources,
                        aircraft_extra=occupant.aircraft_extra,
                    )
                )
                counts["metadata"] += 1
                occupant.metadata_last_sent_monotonic = now

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
    parser.add_argument(
        "--port", type=int, default=30500,
        help="Target map instance's UDP listen port (MAP_LISTEN_PORT) (default: 30500)",
    )
    parser.add_argument(
        "--aircraft-count", type=int, default=10,
        help="Number of simulated aircraft, each a distinct synthetic icao_hex/ident, including the "
        "one balloon (default: 10)",
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
        help="Fake processor_id stamped on every packet (default: load-gen-1). A second, one-shot "
        "processor_id (this value + '-secondary') sends exactly one heartbeat at run start.",
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
    parser.add_argument("--center-lat", type=float, default=39.8283, help="Lane grid center latitude (default: 39.8283)")
    parser.add_argument("--center-lon", type=float, default=-98.5795, help="Lane grid center longitude (default: -98.5795)")
    parser.add_argument(
        "--area-radius-nm", type=float, default=15.0,
        help="Lane grid size: half the length of every lane, nautical miles (default: 15.0)",
    )
    parser.add_argument(
        "--climbing-count", type=int, default=2,
        help="Number of aircraft that transition between two altitudes over their run instead of "
        "holding level (default: 2)",
    )
    parser.add_argument(
        "--ramp-up-seconds", type=float, default=60.0,
        help="Seconds over which the fleet joins staggered instead of all appearing at tick 0. "
        "0 disables ramp-up explicitly. Disabled outright (not scaled down) if --duration is "
        "nonzero and shorter than this (default: 60.0)",
    )
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
    if args.area_radius_nm <= 0:
        print("--area-radius-nm must be greater than 0", file=sys.stderr)
        sys.exit(1)
    if args.climbing_count < 0:
        print("--climbing-count must be zero or greater", file=sys.stderr)
        sys.exit(1)
    if args.ramp_up_seconds < 0:
        print("--ramp-up-seconds must be zero or greater", file=sys.stderr)
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
            area_radius_nm=args.area_radius_nm,
            climbing_count=args.climbing_count,
            ramp_up_seconds=args.ramp_up_seconds,
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
