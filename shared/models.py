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
        # CPR-decoded lat/lon commonly carries 13+ significant digits; 5
        # decimal places is ~1.1 m, far tighter than ADS-B accuracy itself.
        # Capping here — the one construction site, right after pyModeS
        # decode — means every downstream JSON representation inherits it.
        return round(v, 5)

    def to_dict(self) -> dict:
        """Return legacy-compatible dict with UTC datetime timestamp.

        Keys whose value is None are omitted rather than serialised as
        explicit null — multiplied across every position row on every
        flight, those nulls are a real contributor to payload size."""
        d = {
            "timestamp": datetime.fromtimestamp(self.timestamp, tz=timezone.utc),
            "latitude": self.latitude,
            "longitude": self.longitude,
            "altitude": self.altitude,
        }
        return {k: v for k, v in d.items() if v is not None}


class Velocity(BaseModel):
    """Single aircraft velocity report."""

    timestamp: float        # Unix timestamp
    velocity: Optional[float] = None       # knots
    heading: Optional[float] = None        # degrees 0-359
    vertical_speed: Optional[int] = None   # ft/min; negative = descending

    @field_validator("heading")
    @classmethod
    def _cap_heading_precision(cls, v: Optional[float]) -> Optional[float]:
        # 1 decimal place on a 0-359° heading is already finer than any
        # consumer needs; pyModeS emits far more. velocity (knots) and
        # vertical_speed (int ft/min) have no fractional precision to cap.
        return v if v is None else round(v, 1)

    def to_dict(self) -> dict:
        """Return legacy-compatible dict with UTC datetime timestamp.

        Keys whose value is None are omitted rather than serialised as
        explicit null (e.g. a velocity report that carried no heading)."""
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
    serial_number: Optional[str] = None
    manufactured_date: Optional[str] = None
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
    receiver_sources: list[Literal[RECEIVER_SOURCE_TAGS]] = []  # every distinct ADS-B receive source seen
    force_archive: bool = False              # True if a matching rule (force_archive) overrides the external-only archive skip
    aircraft: dict                           # AircraftRecord fields; must include icao_hex
    ident: Optional[str] = None
    operator: Optional[dict] = None          # OperatorRecord fields; source key stripped
    registrant: Optional[dict] = None        # names/street/city/administrative_area/postal_code/country/type -- the aircraft's legal owner, an entity like operator, not a property of the airframe
    squawk: Optional[str] = None
    origin: Optional[str] = None             # ICAO code string, e.g. "KATL"
    destination: Optional[str] = None        # ICAO code string
    matched_rules: list[str] = []
    positions: list[dict] = []               # Position.to_dict() output
    velocities: list[dict] = []              # Velocity.to_dict() output
