from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator


def generate_flight_id() -> str:
    """Return a new UUID-v7 string for use as a flight _id."""
    from uuid_extensions import uuid7
    return str(uuid7())


class InboundMessage(BaseModel):
    """Raw ADS-B/UAT message published by the Receiver to a RabbitMQ adsb-{id} queue."""

    raw: str
    icao_hex: str
    received_at: float  # Unix timestamp (seconds)
    source: Literal["1090", "978", "mlat"]

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

    def to_dict(self) -> dict:
        """Return legacy-compatible dict with UTC datetime timestamp."""
        return {
            "timestamp": datetime.fromtimestamp(self.timestamp, tz=timezone.utc),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "altitude": self.altitude,
        }


class Velocity(BaseModel):
    """Single aircraft velocity report."""

    timestamp: float        # Unix timestamp
    velocity: Optional[float] = None       # knots
    heading: Optional[float] = None        # degrees 0-359
    vertical_speed: Optional[int] = None   # ft/min; negative = descending

    def to_dict(self) -> dict:
        """Return legacy-compatible dict with UTC datetime timestamp."""
        return {
            "timestamp": datetime.fromtimestamp(self.timestamp, tz=timezone.utc),
            "velocity": self.velocity,
            "heading": self.heading,
            "vertical_speed": self.vertical_speed,
        }


# ── Enrichment models (shape matches AROI API responses) ───────────────────


class PowerplantInfo(BaseModel):
    count: Optional[int] = None
    type: Optional[str] = None


class AircraftRecord(BaseModel):
    """
    Aircraft registration and type enrichment.
    Stored in Redis at icao_hex:{icao_hex}. Shape matches the AROI
    /registration/icao_hex/{hex} response.
    """

    icao_hex: str
    registration: Optional[str] = None
    type_designator: Optional[str] = None   # ICAO type code, e.g. "B763"
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    wake_turbulence_category: Optional[str] = None
    powerplant: Optional[PowerplantInfo] = None
    military: Optional[bool] = None
    operator: Optional[str] = None          # operator/owner name from registry
    airline_code: Optional[str] = None      # ICAO airline code
    serial_number: Optional[str] = None
    year_built: Optional[str] = None
    is_private_operator: Optional[bool] = None
    source: Optional[str] = None            # data runner that wrote this record


class OperatorRecord(BaseModel):
    """
    Airline operator enrichment.
    Stored in Redis at operator:{designator}. Shape matches the AROI
    /operator/{designator} response.
    """

    airline_designator: str
    name: Optional[str] = None
    callsign: Optional[str] = None
    country: Optional[str] = None
    iata: Optional[str] = None
    source: Optional[str] = None


class AirportRecord(BaseModel):
    """Airport metadata. Stored in Redis at airport:{icao_code}."""

    icao_code: str
    name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_feet: Optional[int] = None
    country: Optional[str] = None
    municipality: Optional[str] = None
    type: Optional[str] = None


class FlightEnrichment(BaseModel):
    """
    Origin/destination enrichment.
    Stored in Redis at flight:{ident} with a short TTL. Shape matches the AROI
    /flight/{ident} response.
    """

    ident: str
    flight_number: Optional[str] = None
    airline_designator: Optional[str] = None
    origin: Optional[dict] = None       # {"icao_code": "KATL", ...}
    destination: Optional[dict] = None  # {"icao_code": "KLAX", ...}


# ── Completed flight record ─────────────────────────────────────────────────


class CompletedFlight(BaseModel):
    """
    Completed flight record published to the RabbitMQ archive queue by the
    message processor. Shape matches the legacy MongoDB document written by
    Flight.persist() in SkyFollower-legacy, with two additive fields:
    _id is now UUID-v7 (was UUID-v4) and source is new.

    Serialise with .model_dump(by_alias=True, mode="json") for RabbitMQ
    transport and S3 storage to produce the {"_id": ...} key expected by
    downstream consumers.
    """

    model_config = {"populate_by_name": True}

    id: str = Field(alias="_id")
    first_message: datetime
    last_message: datetime
    total_messages: int
    source: Literal["1090", "978", "mlat"]  # additive; not in legacy format
    aircraft: dict                           # AircraftRecord fields; must include icao_hex
    ident: Optional[str] = None
    operator: Optional[dict] = None          # OperatorRecord fields; source key stripped
    squawk: Optional[str] = None
    origin: Optional[str] = None             # ICAO code string, e.g. "KATL"
    destination: Optional[str] = None        # ICAO code string
    matched_rules: list[str] = []
    positions: list[dict] = []               # Position.to_dict() output
    velocities: list[dict] = []              # Velocity.to_dict() output
