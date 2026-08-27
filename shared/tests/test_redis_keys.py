import pytest

from shared.redis_keys import (
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    AIRCRAFT_REGISTRY_SEARCH_INDEX,
    airport_key,
    aircraft_mictronics_key,
    aircraft_registry_key,
    aircraft_type_key,
    archive_last_segment_key,
    archive_search_index_key,
    config_areas_key,
    config_areas_version_key,
    config_flight_ttl_seconds_key,
    config_rules_key,
    config_rules_version_key,
    message_processor_heartbeat_key,
    metrics_flights_archived_key,
    metrics_operator_misses_key,
    metrics_registration_misses_key,
    metrics_total_messages_processed_key,
    operator_key,
    receiver_heartbeat_key,
    receiver_message_count_key,
    receiver_registration_key,
    receiver_registry_index_key,
)


class TestEnrichmentKeys:
    def test_aircraft_mictronics_key(self):
        assert aircraft_mictronics_key("a8ae7f") == "aircraft:mictronics:A8AE7F"
        assert aircraft_mictronics_key("A8AE7F") == "aircraft:mictronics:A8AE7F"

    def test_aircraft_registry_key(self):
        assert aircraft_registry_key("a8ae7f") == "aircraft:registry:A8AE7F"
        assert aircraft_registry_key("A8AE7F") == "aircraft:registry:A8AE7F"

    def test_aircraft_mictronics_search_index_name(self):
        assert AIRCRAFT_MICTRONICS_SEARCH_INDEX == "idx:aircraft:mictronics"

    def test_aircraft_registry_search_index_name(self):
        assert AIRCRAFT_REGISTRY_SEARCH_INDEX == "idx:aircraft:registry"

    def test_operator_key(self):
        assert operator_key("dal") == "operator:DAL"
        assert operator_key("DAL") == "operator:DAL"

    def test_aircraft_type_key(self):
        assert aircraft_type_key("b763") == "aircraft:type:B763"
        assert aircraft_type_key("B763") == "aircraft:type:B763"

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

    def test_config_flight_ttl_seconds_key(self):
        assert config_flight_ttl_seconds_key() == "config:flight_ttl_seconds"


class TestArchiveKeys:
    def test_archive_last_segment_key(self):
        assert archive_last_segment_key("a8ae7f") == "archive:last_segment:A8AE7F"

    def test_archive_search_index_key(self):
        assert archive_search_index_key() == "archive_search:index"


class TestProcessorKeys:
    def test_heartbeat_key(self):
        assert message_processor_heartbeat_key(0) == "skyfollower-message-processor-0"
        assert message_processor_heartbeat_key(3) == "skyfollower-message-processor-3"


class TestReceiverKeys:
    def test_heartbeat_key(self):
        assert receiver_heartbeat_key("ATTIC") == "skyfollower-receiver-ATTIC"

    def test_registry_index_key(self):
        assert receiver_registry_index_key() == "receiver:index"

    def test_registration_key(self):
        assert receiver_registration_key("ATTIC") == "receiver:registration:ATTIC"

    def test_message_count_key_valid_periods(self):
        assert (
            receiver_message_count_key("ATTIC", "localhost_30002", "hour")
            == "metrics:receiver:ATTIC:localhost_30002:messages:hour"
        )
        assert (
            receiver_message_count_key("ATTIC", "localhost_30002", "today")
            == "metrics:receiver:ATTIC:localhost_30002:messages:today"
        )
        assert (
            receiver_message_count_key("ATTIC", "localhost_30002", "lifetime")
            == "metrics:receiver:ATTIC:localhost_30002:messages:lifetime"
        )

    def test_message_count_key_invalid_period(self):
        with pytest.raises(ValueError, match="period"):
            receiver_message_count_key("ATTIC", "localhost_30002", "week")


class TestMetricKeys:
    def test_registration_misses_valid_periods(self):
        assert metrics_registration_misses_key(0, "hour") == "metrics:message_processor:0:registration_misses:hour"
        assert metrics_registration_misses_key(0, "today") == "metrics:message_processor:0:registration_misses:today"
        assert metrics_registration_misses_key(0, "lifetime") == "metrics:message_processor:0:registration_misses:lifetime"
        assert metrics_registration_misses_key(1, "hour") == "metrics:message_processor:1:registration_misses:hour"

    def test_registration_misses_invalid_period(self):
        with pytest.raises(ValueError, match="period"):
            metrics_registration_misses_key(0, "week")

    def test_operator_misses_valid_periods(self):
        assert metrics_operator_misses_key(0, "today") == "metrics:message_processor:0:operator_misses:today"
        assert metrics_operator_misses_key(2, "lifetime") == "metrics:message_processor:2:operator_misses:lifetime"

    def test_operator_misses_invalid_period(self):
        with pytest.raises(ValueError, match="period"):
            metrics_operator_misses_key(0, "hour")
        with pytest.raises(ValueError, match="period"):
            metrics_operator_misses_key(0, "yesterday")

    def test_total_messages_processed_valid_periods(self):
        assert (
            metrics_total_messages_processed_key(0, "hour")
            == "metrics:message_processor:0:total_messages_processed:hour"
        )
        assert (
            metrics_total_messages_processed_key(0, "today")
            == "metrics:message_processor:0:total_messages_processed:today"
        )
        assert (
            metrics_total_messages_processed_key(0, "lifetime")
            == "metrics:message_processor:0:total_messages_processed:lifetime"
        )

    def test_total_messages_processed_invalid_period(self):
        with pytest.raises(ValueError, match="period"):
            metrics_total_messages_processed_key(0, "yesterday")

    def test_flights_archived_valid_periods(self):
        assert metrics_flights_archived_key("hour") == "metrics:archive:flights_archived:hour"
        assert metrics_flights_archived_key("today") == "metrics:archive:flights_archived:today"

    def test_flights_archived_lifetime_invalid(self):
        with pytest.raises(ValueError, match="period"):
            metrics_flights_archived_key("lifetime")

    def test_flights_archived_invalid_period(self):
        with pytest.raises(ValueError, match="period"):
            metrics_flights_archived_key("week")
