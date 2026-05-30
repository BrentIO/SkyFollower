import pytest

from shared.redis_keys import (
    airport_key,
    config_areas_key,
    config_areas_version_key,
    config_rules_key,
    config_rules_version_key,
    flight_key,
    icao_hex_key,
    metrics_aircraft_type_misses_key,
    metrics_flights_archived_key,
    metrics_registration_misses_key,
    operator_key,
    processor_heartbeat_key,
    registration_key,
)


class TestEnrichmentKeys:
    def test_icao_hex_key(self):
        assert icao_hex_key("a8ae7f") == "icao_hex:A8AE7F"
        assert icao_hex_key("A8AE7F") == "icao_hex:A8AE7F"

    def test_registration_key(self):
        assert registration_key("n659dl") == "registration:N659DL"
        assert registration_key("N659DL") == "registration:N659DL"

    def test_operator_key(self):
        assert operator_key("dal") == "operator:DAL"
        assert operator_key("DAL") == "operator:DAL"

    def test_flight_key(self):
        assert flight_key("dal659") == "flight:DAL659"
        assert flight_key("DAL659") == "flight:DAL659"

    def test_airport_key(self):
        assert airport_key("katl") == "airport:KATL"
        assert airport_key("KATL") == "airport:KATL"


class TestConfigKeys:
    def test_config_rules_key(self):
        assert config_rules_key() == "config:rules"

    def test_config_rules_version_key(self):
        assert config_rules_version_key() == "config:rules:version"

    def test_config_areas_key(self):
        assert config_areas_key() == "config:areas"

    def test_config_areas_version_key(self):
        assert config_areas_version_key() == "config:areas:version"


class TestProcessorKeys:
    def test_heartbeat_key(self):
        assert processor_heartbeat_key(0) == "processor:0:heartbeat"
        assert processor_heartbeat_key(3) == "processor:3:heartbeat"


class TestMetricKeys:
    def test_registration_misses_valid_periods(self):
        assert metrics_registration_misses_key(0, "hour") == "metrics:processor:0:registration_misses:hour"
        assert metrics_registration_misses_key(0, "today") == "metrics:processor:0:registration_misses:today"
        assert metrics_registration_misses_key(0, "lifetime") == "metrics:processor:0:registration_misses:lifetime"
        assert metrics_registration_misses_key(1, "hour") == "metrics:processor:1:registration_misses:hour"

    def test_registration_misses_invalid_period(self):
        with pytest.raises(ValueError, match="period"):
            metrics_registration_misses_key(0, "week")

    def test_aircraft_type_misses_valid_periods(self):
        assert metrics_aircraft_type_misses_key(0, "hour") == "metrics:processor:0:aircraft_type_misses:hour"
        assert metrics_aircraft_type_misses_key(2, "lifetime") == "metrics:processor:2:aircraft_type_misses:lifetime"

    def test_aircraft_type_misses_invalid_period(self):
        with pytest.raises(ValueError, match="period"):
            metrics_aircraft_type_misses_key(0, "yesterday")

    def test_flights_archived_valid_periods(self):
        assert metrics_flights_archived_key("hour") == "metrics:archive:flights_archived:hour"
        assert metrics_flights_archived_key("today") == "metrics:archive:flights_archived:today"

    def test_flights_archived_lifetime_invalid(self):
        with pytest.raises(ValueError, match="period"):
            metrics_flights_archived_key("lifetime")

    def test_flights_archived_invalid_period(self):
        with pytest.raises(ValueError, match="period"):
            metrics_flights_archived_key("week")
