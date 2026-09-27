from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator

from shared.config import RECEIVER_SOURCE_TAGS


def generate_flight_id() -> str:
    """Return a new UUID-v7 string for use as a flight _id."""
    from uuid_extensions import uuid7
    return str(uuid7())


class InboundMessage(BaseModel):
    """Raw ADS-B/UAT message published by the Receiver to the adsb exchange."""

    raw: str
    icao_hex: str
    received_at: float  # Unix timestamp (seconds)
    source: Literal[RECEIVER_SOURCE_TAGS]

    @field_validator("icao_hex")
    @classmethod
    def normalise_icao_hex(cls, v: str) -> str:
        v = v.strip().upper()
        if len(v) != 6 or not all(c in "0123456789ABCDEF" for c in v):
            raise ValueError(f"icao_hex must be a 6-character hex string, got: {v!r}")
        return v


class Position(BaseModel):
    """Single aircraft position report."""

    timestamp: float        # Unix timestamp
    latitude: float
    longitude: float
    altitude: Optional[int] = None  # feet MSL; None when not present in message

    @field_validator("latitude", "longitude")
    @classmethod
    def _cap_coordinate_precision(cls, v: float) -> float:
        # 5 decimal places is ~1.1 m, far tighter than ADS-B accuracy
        # itself; caps the 13+ significant digits CPR decoding can emit.
        return round(v, 5)

    def to_dict(self) -> dict:
        """Return legacy-compatible dict with UTC datetime timestamp.
        Keys whose value is None are omitted rather than serialised as
        explicit null, since those nulls add up across every position
        row on every flight."""
        d = {
            "timestamp": datetime.fromtimestamp(self.timestamp, tz=timezone.utc),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "altitude": self.altitude,
        }
        return {k: v for k, v in d.items() if v is not None}


class RawFrame(BaseModel):
    """Single captured Mode-S/UAT hex frame, decoded or not, recorded only
    when message-processor's CAPTURE_RAW_FRAMES is enabled. Forensic, not
    part of the permanent archive: message-processor's _archive() strips
    CompletedFlight.raw_frames before publishing to the S3-bound
    `skyfollower-archive` queue."""

    timestamp: float                              # Unix timestamp (msg.received_at)
    source: Literal[RECEIVER_SOURCE_TAGS]
    raw: str                                       # the raw hex frame, verbatim
    decoded: bool                                  # True if this message produced usable `data`

    def to_dict(self) -> dict:
        """Same convention as Position.to_dict()/Velocity.to_dict(), but
        every field here is always present (no None-dropping needed)."""
        return {
            "timestamp": datetime.fromtimestamp(self.timestamp, tz=timezone.utc),
            "source": self.source,
            "raw": self.raw,
            "decoded": self.decoded,
        }


class Velocity(BaseModel):
    """Single aircraft velocity report."""

    timestamp: float        # Unix timestamp
    velocity: Optional[float] = None       # knots
    heading: Optional[float] = None        # degrees 0-359
    vertical_speed: Optional[int] = None   # ft/min; negative = descending

    @field_validator("heading")
    @classmethod
    def _cap_heading_precision(cls, v: Optional[float]) -> Optional[float]:
        # 1 decimal place on a 0-359° heading is finer than any consumer
        # needs, capping what pyModeS can emit.
        return v if v is None else round(v, 1)

    def to_dict(self) -> dict:
        """Keys whose value is None are omitted rather than serialised
        as explicit null (e.g. a velocity report with no heading)."""
        d = {
            "timestamp": datetime.fromtimestamp(self.timestamp, tz=timezone.utc),
            "velocity": self.velocity,
            "heading": self.heading,
            "vertical_speed": self.vertical_speed,
        }
        return {k: v for k, v in d.items() if v is not None}


# ── Enrichment models (shape matches AROI API responses) ───────────────────


class PowerplantInfo(BaseModel):
    count: Optional[int] = None
    type: Optional[str] = None


class AircraftRecord(BaseModel):
    """Aircraft registration and type enrichment. Written across three
    Redis keys (aircraft:mictronics/registry/livery:{icao_hex}) and
    deep-merged at read time by shared/lua/merge_aircraft.lua, later
    sources winning on overlap. Field names match the AROI
    /registration/icao_hex/{hex} response."""

    icao_hex: str = Field(title="ICAO Hex")
    registration: Optional[str] = None
    type_designator: Optional[str] = None   # ICAO type code, e.g. "B763"
    type: Optional[str] = None              # aircraft category, e.g. "Airplane"/"Rotorcraft"/"Glider"
    category: Optional[str] = None          # landing-gear category, e.g. "Land"/"Sea"/"Amphibian"
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    manufacturer_model: Optional[str] = None  # combined manufacturer + model, e.g. "BOEING 757-200"; synthesized by merge_aircraft.lua if absent
    description_code: Optional[str] = None  # ICAO Doc 8643 code, e.g. "L2J": char 1 = category, digit = engine count, char 3 = engine type
    seats: Optional[int] = None
    powerplant: Optional[PowerplantInfo] = None
    military: Optional[bool] = None
    serial_number: Optional[str] = None
    manufactured_date: Optional[str] = None
    special_livery: Optional[str] = None    # cleaned, TTS-ready livery name -- see airportwebcams-special-liveries/README.md
    country: Optional[str] = None           # resolved country-of-registration name; see country_code for the raw ISO 3166-1 alpha-2 code
    country_code: Optional[str] = None      # ISO 3166-1 alpha-2 code -- registry runner's value wins, else resolved from the ICAO hex-range allocation table
    data_sources: Optional[list[str]] = None  # data runners that contributed a field, in mictronics -> registry -> livery order


class OperatorRecord(BaseModel):
    """Airline operator enrichment. Stored in Redis at
    operator:{designator}. Shape matches the AROI /operator/{designator}
    response."""

    airline_designator: str
    name: Optional[str] = None
    callsign: Optional[str] = None
    country: Optional[str] = None
    iata: Optional[str] = Field(default=None, title="IATA")
    source: Optional[str] = None


class AirportRecord(BaseModel):
    """Airport metadata. Stored in Redis at airport:{icao_code}."""

    icao_code: str = Field(title="ICAO Code")
    iata_code: Optional[str] = None         # IATA 3-character code; absent if blank
    name: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None            # resolved subdivision name; see region_code for the raw ISO 3166-2 code
    region_code: Optional[str] = None       # ISO 3166-2 subdivision code, e.g. "AU-QLD"
    country: Optional[str] = None           # resolved country name; see country_code for the raw ISO 3166-1 alpha-2 code
    country_code: Optional[str] = None      # ISO 3166-1 alpha-2 country code, e.g. "AU"
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    phonic: Optional[str] = None            # voice-friendly spoken name for TTS announcements


# ── Completed flight record ─────────────────────────────────────────────────


class CompletedFlight(BaseModel):
    """Completed flight record published to the RabbitMQ archive queue by
    the message processor. Matches the legacy MongoDB document shape,
    with `_id` now UUID-v7 and `receiver_sources`/`force_archive` added.

    origin/destination carry the full resolved airport object here; the
    archive processor reduces each to its bare ICAO code string before
    writing the S3 object.

    Serialise with .model_dump(by_alias=True, mode="json") to produce the
    {"_id": ...} key downstream consumers expect.
    """

    model_config = {"populate_by_name": True}

    id: str = Field(alias="_id")
    first_message: datetime
    last_message: datetime
    total_messages: int
    receiver_sources: list[Literal[RECEIVER_SOURCE_TAGS]] = []  # every distinct ADS-B receive source seen
    force_archive: bool = False              # True if a matching rule (force_archive) overrides the external-only archive skip
    aircraft: dict                           # AircraftRecord fields; must include icao_hex
    ident: Optional[str] = None
    operator: Optional[dict] = None          # OperatorRecord fields; source key stripped
    registrant: Optional[dict] = None        # aircraft's legal owner (an entity like operator, not a property of the airframe)
    squawk: Optional[str] = None
    origin: Optional[dict] = None            # full AirportRecord fields; reduced to a bare ICAO code string only when persisted to S3
    destination: Optional[dict] = None       # full AirportRecord fields; see origin
    matched_rules: list[str] = []
    positions: list[dict] = []               # Position.to_dict() output
    velocities: list[dict] = []              # Velocity.to_dict() output
    raw_frames: list[dict] = []              # RawFrame.to_dict() output; message-processor's _archive() always excludes this before publishing to the permanent archive queue
