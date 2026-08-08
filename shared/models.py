from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional
from pydantic import BaseModel, Field, field_validator


def generate_flight_id() -> str:
    """Return a new UUID-v7 string for use as a flight _id."""
    from uuid_extensions import uuid7
    return str(uuid7())


class InboundMessage(BaseModel):
    """Raw ADS-B/UAT message published by the Receiver to the adsb exchange."""

    raw: str
    icao_hex: str
    received_at: float  # Unix timestamp (seconds)
    source: Literal["1090", "978", "MLAT"]

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


class RegistrantInfo(BaseModel):
    """Owner/registrant information from the national civil aircraft registry."""

    names: Optional[list[str]] = None       # primary name first, additional DBA names follow
    street: Optional[list[str]] = None      # street address lines
    city: Optional[str] = None
    administrative_area: Optional[str] = None  # first-level country subdivision (US state, Canadian province, etc.)
    postal_code: Optional[str] = None
    country: Optional[str] = None           # ISO 3166-1 alpha-2 country code
    type: Optional[str] = None              # registrant category, e.g. "Individual"/"Corporation"/"Government"


class AircraftRecord(BaseModel):
    """
    Aircraft registration and type enrichment.
    Written across three Redis keys — aircraft:mictronics:{icao_hex}
    (Mictronics), aircraft:registry:{icao_hex} (country registry runners),
    and aircraft:livery:{icao_hex} (the airportwebcams-special-liveries runner) —
    and deep-merged at read time by shared/lua/merge_aircraft.lua, with
    later sources in that list winning on any field overlap. This shape is
    the merged result. Field names match the AROI /registration/icao_hex/{hex}
    response.
    """

    icao_hex: str = Field(title="ICAO Hex")
    registration: Optional[str] = None
    type_designator: Optional[str] = None   # ICAO type code, e.g. "B763"
    type: Optional[str] = None              # aircraft category, e.g. "Airplane"/"Rotorcraft"/"Glider"
    category: Optional[str] = None          # landing-gear category, e.g. "Land"/"Sea"/"Amphibian"
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    manufacturer_model: Optional[str] = None  # combined manufacturer + model string, e.g. "BOEING 757-200"; synthesized by merge_aircraft.lua from manufacturer/model if absent
    seats: Optional[int] = None
    powerplant: Optional[PowerplantInfo] = None
    military: Optional[bool] = None
    operator: Optional[str] = None          # operator/owner name from registry
    airline_code: Optional[str] = None      # ICAO airline code
    serial_number: Optional[str] = None
    manufactured_date: Optional[str] = None
    is_private_operator: Optional[bool] = None
    registrant: Optional[RegistrantInfo] = None
    special_livery: Optional[str] = None    # cleaned, TTS-ready livery name if wearing one — see airportwebcams-special-liveries/README.md; absent when not
    data_sources: Optional[list[str]] = None  # every data runner that contributed a field, mictronics -> registry -> livery order


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
    iata: Optional[str] = Field(default=None, title="IATA")
    source: Optional[str] = None


class AirportRecord(BaseModel):
    """Airport metadata. Stored in Redis at airport:{icao_code}."""

    icao_code: str = Field(title="ICAO Code")
    name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    altitude_feet: Optional[int] = None
    country: Optional[str] = None
    municipality: Optional[str] = None
    type: Optional[str] = None


# ── Completed flight record ─────────────────────────────────────────────────


class CompletedFlight(BaseModel):
    """
    Completed flight record published to the RabbitMQ archive queue by the
    message processor. Shape matches the legacy MongoDB document written by
    Flight.persist() in SkyFollower-legacy, with additive fields:
    _id is now UUID-v7 (was UUID-v4), and receiver_sources/force_archive are
    new.

    receiver_sources and matched_rules both default to an empty list rather
    than being required, since neither exists in legacy flight records —
    the legacy-to-S3 migration plan deliberately leaves those files
    untouched rather than backfilling a synthetic value.

    Serialise with .model_dump(by_alias=True, mode="json") for RabbitMQ
    transport and S3 storage to produce the {"_id": ...} key expected by
    downstream consumers.
    """

    model_config = {"populate_by_name": True}

    id: str = Field(alias="_id")
    first_message: datetime
    last_message: datetime
    total_messages: int
    receiver_sources: list[Literal["1090", "978", "MLAT"]] = []  # every distinct ADS-B receive source seen
    force_archive: bool = False              # True if a matching rule (force_archive) overrides the MLAT-only archive skip
    aircraft: dict                           # AircraftRecord fields; must include icao_hex
    ident: Optional[str] = None
    operator: Optional[dict] = None          # OperatorRecord fields; source key stripped
    squawk: Optional[str] = None
    origin: Optional[str] = None             # ICAO code string, e.g. "KATL"
    destination: Optional[str] = None        # ICAO code string
    matched_rules: list[str] = []
    positions: list[dict] = []               # Position.to_dict() output
    velocities: list[dict] = []              # Velocity.to_dict() output
