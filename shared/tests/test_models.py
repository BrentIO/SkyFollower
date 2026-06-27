from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from shared.models import (
    AircraftRecord,
    CompletedFlight,
    FlightEnrichment,
    InboundMessage,
    OperatorRecord,
    Position,
    Velocity,
    generate_flight_id,
)


class TestInboundMessage:
    def test_valid_message(self):
        msg = InboundMessage(raw="8D4B1900EA4E56", icao_hex="4b1900", received_at=1.0, source="1090")
        assert msg.icao_hex == "4B1900"

    def test_icao_hex_uppercased(self):
        msg = InboundMessage(raw="x", icao_hex="abcdef", received_at=0.0, source="978")
        assert msg.icao_hex == "ABCDEF"

    def test_icao_hex_too_short(self):
        with pytest.raises(ValidationError, match="icao_hex"):
            InboundMessage(raw="x", icao_hex="ABC", received_at=0.0, source="1090")

    def test_icao_hex_too_long(self):
        with pytest.raises(ValidationError, match="icao_hex"):
            InboundMessage(raw="x", icao_hex="ABCDEFG", received_at=0.0, source="1090")

    def test_icao_hex_invalid_chars(self):
        with pytest.raises(ValidationError, match="icao_hex"):
            InboundMessage(raw="x", icao_hex="ZZZZZZ", received_at=0.0, source="1090")

    def test_invalid_source(self):
        with pytest.raises(ValidationError):
            InboundMessage(raw="x", icao_hex="ABCDEF", received_at=0.0, source="978MHz")

    def test_mlat_source(self):
        msg = InboundMessage(raw="x", icao_hex="ABCDEF", received_at=0.0, source="MLAT")
        assert msg.source == "MLAT"


class TestPosition:
    def test_to_dict_has_datetime(self):
        pos = Position(timestamp=0.0, latitude=38.9, longitude=-77.0, altitude=10000)
        d = pos.to_dict()
        assert isinstance(d["timestamp"], datetime)
        assert d["latitude"] == 38.9
        assert d["altitude"] == 10000

    def test_altitude_optional(self):
        pos = Position(timestamp=0.0, latitude=0.0, longitude=0.0)
        assert pos.altitude is None
        assert pos.to_dict()["altitude"] is None

    def test_timestamp_conversion(self):
        pos = Position(timestamp=1717100000.0, latitude=0.0, longitude=0.0)
        d = pos.to_dict()
        assert d["timestamp"] == datetime.fromtimestamp(1717100000.0, tz=timezone.utc)


class TestVelocity:
    def test_to_dict_has_datetime(self):
        vel = Velocity(timestamp=1.0, velocity=450.0, heading=270.0, vertical_speed=500)
        d = vel.to_dict()
        assert isinstance(d["timestamp"], datetime)
        assert d["velocity"] == 450.0
        assert d["heading"] == 270.0
        assert d["vertical_speed"] == 500

    def test_all_optional(self):
        vel = Velocity(timestamp=0.0)
        d = vel.to_dict()
        assert d["velocity"] is None
        assert d["heading"] is None
        assert d["vertical_speed"] is None

    def test_negative_vertical_speed(self):
        vel = Velocity(timestamp=0.0, vertical_speed=-1500)
        assert vel.vertical_speed == -1500


class TestGenerateFlightId:
    def test_returns_string(self):
        fid = generate_flight_id()
        assert isinstance(fid, str)

    def test_returns_unique_values(self):
        ids = {generate_flight_id() for _ in range(100)}
        assert len(ids) == 100

    def test_looks_like_uuid(self):
        fid = generate_flight_id()
        # UUID format: 8-4-4-4-12 hex chars separated by hyphens
        parts = fid.split("-")
        assert len(parts) == 5


class TestAircraftRecord:
    def test_minimal(self):
        rec = AircraftRecord(icao_hex="A8AE7F")
        assert rec.icao_hex == "A8AE7F"
        assert rec.registration is None

    def test_full(self):
        from shared.models import PowerplantInfo
        rec = AircraftRecord(
            icao_hex="A8AE7F",
            registration="N659DL",
            type_designator="B763",
            military=False,
            powerplant=PowerplantInfo(count=2, type="jet"),
        )
        assert rec.registration == "N659DL"
        assert rec.powerplant.count == 2


class TestOperatorRecord:
    def test_minimal(self):
        rec = OperatorRecord(airline_designator="DAL")
        assert rec.airline_designator == "DAL"
        assert rec.name is None

    def test_full(self):
        rec = OperatorRecord(
            airline_designator="DAL",
            name="Delta Air Lines",
            callsign="DELTA",
            country="US",
        )
        assert rec.name == "Delta Air Lines"


class TestCompletedFlight:
    def _make(self, **kwargs) -> CompletedFlight:
        defaults = dict(
            id="01900000-0000-7000-8000-000000000001",
            first_message=datetime(2026, 5, 30, 10, 0, 0, tzinfo=timezone.utc),
            last_message=datetime(2026, 5, 30, 10, 15, 0, tzinfo=timezone.utc),
            total_messages=100,
            source="1090",
            aircraft={"icao_hex": "A8AE7F"},
        )
        defaults.update(kwargs)
        return CompletedFlight(**{"_id": defaults.pop("id"), **defaults})

    def test_alias_id_serialises_as_underscore_id(self):
        flight = self._make()
        d = flight.model_dump(by_alias=True)
        assert "_id" in d
        assert "id" not in d

    def test_source_field_present(self):
        flight = self._make(source="978")
        assert flight.source == "978"

    def test_positions_and_velocities_default_empty(self):
        flight = self._make()
        assert flight.positions == []
        assert flight.velocities == []

    def test_matched_rules_default_empty(self):
        flight = self._make()
        assert flight.matched_rules == []

    def test_optional_fields_default_none(self):
        flight = self._make()
        assert flight.ident is None
        assert flight.origin is None
        assert flight.destination is None

    def test_json_roundtrip(self):
        flight = self._make(ident="DAL659", origin="KATL", destination="KLAX")
        json_str = flight.model_dump_json(by_alias=True)
        restored = CompletedFlight.model_validate_json(json_str)
        assert restored.ident == "DAL659"
        assert restored.origin == "KATL"
