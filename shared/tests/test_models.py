from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from shared.models import (
    AircraftRecord,
    CompletedFlight,
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

    def test_external_source(self):
        msg = InboundMessage(raw="x", icao_hex="ABCDEF", received_at=0.0, source="EXTERNAL")
        assert msg.source == "EXTERNAL"


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
        # None-valued keys are omitted from to_dict(), not serialised as null
        assert "altitude" not in pos.to_dict()

    def test_timestamp_conversion(self):
        pos = Position(timestamp=1717100000.0, latitude=0.0, longitude=0.0)
        d = pos.to_dict()
        assert d["timestamp"] == datetime.fromtimestamp(1717100000.0, tz=timezone.utc)

    def test_coordinate_precision_capped_on_construction(self):
        pos = Position(
            timestamp=0.0,
            latitude=38.913672183345678,
            longitude=-77.036543219876543,
            altitude=10000,
        )
        assert pos.latitude == 38.91367
        assert pos.longitude == -77.03654

    def test_fully_none_optional_absent_from_exclude_none_json(self):
        import json

        pos = Position(timestamp=0.0, latitude=1.0, longitude=2.0)
        payload = json.loads(pos.model_dump_json(exclude_none=True))
        assert "altitude" not in payload


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
        # None-valued keys are omitted entirely
        assert "velocity" not in d
        assert "heading" not in d
        assert "vertical_speed" not in d
        assert d == {"timestamp": datetime.fromtimestamp(0.0, tz=timezone.utc)}

    def test_negative_vertical_speed(self):
        vel = Velocity(timestamp=0.0, vertical_speed=-1500)
        assert vel.vertical_speed == -1500

    def test_heading_precision_capped_on_construction(self):
        vel = Velocity(timestamp=0.0, heading=273.1363583683326)
        assert vel.heading == 273.1

    def test_heading_none_stays_none(self):
        vel = Velocity(timestamp=0.0, velocity=100.0)
        assert vel.heading is None

    def test_fully_none_optional_absent_from_exclude_none_json(self):
        import json

        vel = Velocity(timestamp=0.0, velocity=100.0)
        payload = json.loads(vel.model_dump_json(exclude_none=True))
        assert "heading" not in payload
        assert "vertical_speed" not in payload


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

    def test_special_livery_defaults_to_none(self):
        rec = AircraftRecord(icao_hex="A8AE7F")
        assert rec.special_livery is None

    def test_special_livery_field(self):
        rec = AircraftRecord(
            icao_hex="AA7C64",
            registration="N775JB",
            special_livery="America250",
        )
        assert rec.special_livery == "America250"

    def test_data_sources_defaults_to_none(self):
        rec = AircraftRecord(icao_hex="A8AE7F")
        assert rec.data_sources is None

    def test_data_sources_field(self):
        rec = AircraftRecord(
            icao_hex="A8AE7F",
            data_sources=["mictronics", "us-faa-registry"],
        )
        assert rec.data_sources == ["mictronics", "us-faa-registry"]

    def test_type_category_seats_manufacturer_model_default_to_none(self):
        rec = AircraftRecord(icao_hex="A8AE7F")
        assert rec.type is None
        assert rec.category is None
        assert rec.seats is None
        assert rec.manufacturer_model is None

    def test_type_category_seats_manufacturer_model_fields(self):
        rec = AircraftRecord(
            icao_hex="A8AE7F",
            type="Airplane",
            category="Land",
            seats=189,
            manufacturer_model="BOEING 767-332ER",
        )
        assert rec.type == "Airplane"
        assert rec.category == "Land"
        assert rec.seats == 189
        assert rec.manufacturer_model == "BOEING 767-332ER"


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
            receiver_sources=["1090"],
            aircraft={"icao_hex": "A8AE7F"},
        )
        defaults.update(kwargs)
        return CompletedFlight(**{"_id": defaults.pop("id"), **defaults})

    def test_alias_id_serialises_as_underscore_id(self):
        flight = self._make()
        d = flight.model_dump(by_alias=True)
        assert "_id" in d
        assert "id" not in d

    def test_receiver_sources_field_present(self):
        flight = self._make(receiver_sources=["978"])
        assert flight.receiver_sources == ["978"]

    def test_receiver_sources_multiple_values(self):
        flight = self._make(receiver_sources=["EXTERNAL", "1090"])
        assert flight.receiver_sources == ["EXTERNAL", "1090"]

    def test_receiver_sources_defaults_to_empty_list(self):
        """Must default, not be required — legacy migrated flights have no
        receive-source history at all and must still deserialize."""
        flight = self._make(receiver_sources=[])
        assert flight.receiver_sources == []

    def test_receiver_sources_absent_from_input_still_defaults(self):
        flight = CompletedFlight(
            _id="01900000-0000-7000-8000-000000000002",
            first_message=datetime(2026, 5, 30, 10, 0, 0, tzinfo=timezone.utc),
            last_message=datetime(2026, 5, 30, 10, 15, 0, tzinfo=timezone.utc),
            total_messages=1,
            aircraft={"icao_hex": "A8AE7F"},
        )
        assert flight.receiver_sources == []

    def test_force_archive_defaults_to_false(self):
        flight = self._make()
        assert flight.force_archive is False

    def test_force_archive_true(self):
        flight = self._make(force_archive=True)
        assert flight.force_archive is True

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

    def test_registrant_field(self):
        """registrant is a sibling of operator on CompletedFlight -- an
        entity (the aircraft's legal owner), not a property of the airframe,
        so it does not live on AircraftRecord."""
        flight = self._make(registrant={
            "names": ["Delta Air Lines Inc"],
            "street": ["1030 Delta Blvd"],
            "city": "Atlanta",
            "administrative_area": "GA",
            "postal_code": "30354",
            "country": "US",
            "type": "Corporation",
        })
        assert flight.registrant["names"] == ["Delta Air Lines Inc"]
        assert flight.registrant["city"] == "Atlanta"
        assert flight.registrant["type"] == "Corporation"

    def test_registrant_defaults_to_none(self):
        flight = self._make()
        assert flight.registrant is None

    def test_json_roundtrip(self):
        flight = self._make(ident="DAL659", origin="KATL", destination="KLAX")
        json_str = flight.model_dump_json(by_alias=True)
        restored = CompletedFlight.model_validate_json(json_str)
        assert restored.ident == "DAL659"
        assert restored.origin == "KATL"

    def test_fully_none_optionals_absent_from_exclude_none_json(self):
        import json

        flight = self._make()
        payload = json.loads(flight.model_dump_json(by_alias=True, exclude_none=True))
        for key in ("ident", "operator", "registrant", "squawk", "origin", "destination"):
            assert key not in payload

    def test_positions_and_velocities_entries_omit_none_keys(self):
        import json

        # positions/velocities are list[dict] built from Position/Velocity
        # .to_dict(), which already drops None-valued keys.
        flight = self._make(
            positions=[Position(timestamp=0.0, latitude=1.0, longitude=2.0).to_dict()],
            velocities=[Velocity(timestamp=0.0, velocity=100.0).to_dict()],
        )
        payload = json.loads(flight.model_dump_json(by_alias=True, exclude_none=True))
        assert "altitude" not in payload["positions"][0]
        assert "heading" not in payload["velocities"][0]
        assert "vertical_speed" not in payload["velocities"][0]
