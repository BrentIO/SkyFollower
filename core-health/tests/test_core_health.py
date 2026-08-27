"""
Tests for core-health/main.py components that don't require live
infrastructure (a real RabbitMQ Management API, Redis server, or MQTT
broker).
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock

import pytest
import redis as redis_lib

# core-health/ contains a hyphen, so it can't be imported as a normal
# package -- same workaround archive-compaction/tests uses.
_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL_DIR = os.path.dirname(_HERE)  # core-health/
_REPO_ROOT = os.path.abspath(os.path.join(_TOOL_DIR, ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "core_health_main",
        os.path.join(_TOOL_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["core_health_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

CoreHealth = _mod.CoreHealth
_queue_target = _mod._queue_target
_sanitize_id = _mod._sanitize_id
_mp_counter_key = _mod._mp_counter_key
_metrics_operator_misses_key = _mod._metrics_operator_misses_key
_metrics_total_messages_processed_key = _mod._metrics_total_messages_processed_key
_receiver_index_key = _mod._receiver_index_key
_receiver_registration_key = _mod._receiver_registration_key
_receiver_message_total_key = _mod._receiver_message_total_key
MQTT_ROOT = _mod.MQTT_ROOT
CORE_DEVICE_IDENTIFIER = _mod.CORE_DEVICE_IDENTIFIER
RABBITMQ_POLL_INTERVAL_SECONDS = _mod.RABBITMQ_POLL_INTERVAL_SECONDS


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _minimal_config() -> dict:
    return {
        "rabbitmq_management": {
            "host": "rmq.example.com", "port": 15672,
            "username": "mon", "password": "p",
        },
        "redis": {"host": "redis.example.com", "port": 6379, "password": "p"},
        "redis_monitoring": {
            "host": "redis.example.com", "port": 6379,
            "username": "mon", "password": "p",
        },
        "mqtt": {"host": "", "port": 1883, "username": "", "password": ""},
        "log_level": "info",
    }


def _wired_app() -> CoreHealth:
    """A CoreHealth instance with its MQTT/Redis/HTTP clients replaced by
    mocks and MQTT marked connected, without going through start()/real
    network connections."""
    app = CoreHealth(_minimal_config())
    app._mqtt = MagicMock()
    app._mqtt_connected = True
    app._redis = MagicMock()
    app._redis_monitoring = MagicMock()
    app._session = MagicMock()
    return app


def _discovery_payloads(mqtt_mock: MagicMock) -> dict:
    """{topic: parsed JSON payload} for every homeassistant/... publish call."""
    out = {}
    for call in mqtt_mock.publish.call_args_list:
        args, kwargs = call
        topic = args[0]
        if topic.startswith("homeassistant/"):
            out[topic] = json.loads(args[1])
    return out


def _state_publishes(mqtt_mock: MagicMock) -> dict:
    """{topic: value} for every non-discovery publish call (last write wins,
    matching how a retained MQTT topic behaves)."""
    out = {}
    for call in mqtt_mock.publish.call_args_list:
        args, kwargs = call
        topic = args[0]
        if not topic.startswith("homeassistant/"):
            out[topic] = args[1]
    return out


# ---------------------------------------------------------------------------
# Queue -> device/topic classification
# ---------------------------------------------------------------------------

class TestQueueTarget:
    def test_message_processor_queue_merges_onto_its_own_device(self):
        target = _queue_target("skyfollower-message-processor-mp-1")
        assert target.device["ids"] == "SkyFollower_message_processor_mp-1"
        assert target.state_base == f"{MQTT_ROOT}/message-processor/mp-1/statistic"
        assert target.unique_prefix == "SkyFollower_message_processor_mp-1_queue"

    def test_archive_queue_merges_onto_archive_device(self):
        target = _queue_target("archive")
        assert target.device["ids"] == "SkyFollower_archive"
        assert target.state_base == f"{MQTT_ROOT}/archive/statistic"

    def test_unroutable_queue_merges_onto_core_device(self):
        target = _queue_target("adsb-unroutable")
        assert target.device["ids"] == CORE_DEVICE_IDENTIFIER
        assert target.state_base == f"{MQTT_ROOT}/queue/adsb-unroutable/statistic"

    def test_unique_prefixes_never_collide_with_owning_component(self):
        """The whole reason for the "_queue_" infix: message-processor's
        own entities (e.g. ..._processing_time_hwm_ms) must never share a
        unique_id with anything core-health publishes on the same device."""
        target = _queue_target("skyfollower-message-processor-mp-1")
        assert target.unique_prefix.endswith("_queue")


class TestSanitizeId:
    def test_replaces_disallowed_characters(self):
        assert _sanitize_id("192.168.10.5:30002") == "192-168-10-5-30002"

    def test_leaves_allowed_characters_untouched(self):
        assert _sanitize_id("mp-1_ok") == "mp-1_ok"


# ---------------------------------------------------------------------------
# Redis counter reads: missing key -> 0, connectivity failure -> None
# ---------------------------------------------------------------------------

class TestRedisCounterOrNone:
    def test_missing_key_is_zero(self):
        app = _wired_app()
        client = MagicMock()
        client.get.return_value = None
        assert app._redis_counter_or_none(client, "some:key") == 0

    def test_present_key_is_parsed_as_int(self):
        app = _wired_app()
        client = MagicMock()
        client.get.return_value = "42"
        assert app._redis_counter_or_none(client, "some:key") == 42

    def test_connectivity_failure_returns_none_not_zero(self):
        app = _wired_app()
        client = MagicMock()
        client.get.side_effect = redis_lib.exceptions.ConnectionError("down")
        assert app._redis_counter_or_none(client, "some:key") is None


class TestMpCounterKey:
    def test_registration_uses_shared_redis_keys_builder(self):
        from shared.redis_keys import metrics_registration_misses_key
        assert _mp_counter_key("mp-1", "registration", "hour") == (
            metrics_registration_misses_key("mp-1", "hour")
        )

    def test_operator_key_shape(self):
        assert _mp_counter_key("mp-1", "operator", "today") == (
            "metrics:message_processor:mp-1:operator_misses:today"
        )

    def test_total_messages_key_shape(self):
        assert _mp_counter_key("mp-1", "total_messages", "lifetime") == (
            "metrics:message_processor:mp-1:total_messages_processed:lifetime"
        )

    def test_unknown_kind_raises(self):
        with pytest.raises(ValueError):
            _mp_counter_key("mp-1", "bogus", "hour")


# ---------------------------------------------------------------------------
# Queue stats publishing
# ---------------------------------------------------------------------------

class TestPublishQueueStats:
    def _queue(self, **overrides) -> dict:
        base = {
            "name": "skyfollower-message-processor-mp-1",
            "consumers": 1,
            "consumer_utilisation": 0.5,
            "messages_ready": 3,
            "messages_unacknowledged": 1,
            "state": "running",
            "memory": 12345,
            "message_bytes": 678,
            "message_stats": {
                "publish_details": {"rate": 1.5},
                "deliver_details": {"rate": 1.4},
                "ack_details": {"rate": 1.4},
                "redeliver_details": {"rate": 0.0},
            },
        }
        base.update(overrides)
        return base

    def test_consumer_utilisation_converted_to_percent(self):
        app = _wired_app()
        app._publish_queue_stats(self._queue())
        published = _state_publishes(app._mqtt)
        assert published[
            f"{MQTT_ROOT}/message-processor/mp-1/statistic/consumer_utilisation_percent"
        ] == "50.0"

    def test_consumer_capacity_used_when_utilisation_absent(self):
        """consumer_utilisation was renamed consumer_capacity in newer
        RabbitMQ releases -- must not silently go blank across a broker
        upgrade."""
        app = _wired_app()
        queue = self._queue(consumer_capacity=0.25)
        del queue["consumer_utilisation"]
        app._publish_queue_stats(queue)
        published = _state_publishes(app._mqtt)
        assert published[
            f"{MQTT_ROOT}/message-processor/mp-1/statistic/consumer_utilisation_percent"
        ] == "25.0"

    def test_missing_message_stats_defaults_rates_to_zero(self):
        app = _wired_app()
        queue = self._queue()
        del queue["message_stats"]
        app._publish_queue_stats(queue)
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/message-processor/mp-1/statistic/publish_rate"] == "0.0"

    def test_publishes_discovery_once_per_queue(self):
        app = _wired_app()
        app._publish_queue_stats(self._queue())
        first_call_count = app._mqtt.publish.call_count
        app._publish_queue_stats(self._queue())
        second_call_count = app._mqtt.publish.call_count
        # Second call publishes state again, but not another round of
        # discovery configs.
        assert second_call_count - first_call_count < first_call_count

    def test_discovery_payload_uses_core_health_availability_not_processors(self):
        app = _wired_app()
        app._publish_queue_stats(self._queue())
        discovery = _discovery_payloads(app._mqtt)
        consumers_payload = discovery[
            "homeassistant/sensor/SkyFollower_message_processor_mp-1_queue_consumers/config"
        ]
        assert consumers_payload["availability_topic"] == f"{MQTT_ROOT}/status"
        assert consumers_payload["device"]["ids"] == "SkyFollower_message_processor_mp-1"
        assert consumers_payload["expire_after"] == RABBITMQ_POLL_INTERVAL_SECONDS * 3


# ---------------------------------------------------------------------------
# Broker-wide overview
# ---------------------------------------------------------------------------

class TestPublishBrokerOverview:
    def test_alarm_true_if_any_node_has_it(self):
        app = _wired_app()
        overview = {"object_totals": {"connections": 7}}
        nodes = [{"mem_alarm": False, "disk_free_alarm": False},
                 {"mem_alarm": True, "disk_free_alarm": False}]
        app._publish_broker_overview(overview, nodes)
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/rabbitmq/statistic/rabbitmq_memory_alarm"] == "True"
        assert published[f"{MQTT_ROOT}/rabbitmq/statistic/rabbitmq_disk_free_alarm"] == "False"
        assert published[f"{MQTT_ROOT}/rabbitmq/statistic/rabbitmq_connections_total"] == "7"

    def test_tolerates_none_overview_and_nodes(self):
        app = _wired_app()
        app._publish_broker_overview(None, None)
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/rabbitmq/statistic/rabbitmq_memory_alarm"] == "False"
        assert f"{MQTT_ROOT}/rabbitmq/statistic/rabbitmq_connections_total" not in published


# ---------------------------------------------------------------------------
# Message-processor counter mimicry
# ---------------------------------------------------------------------------

class TestPublishMessageProcessorCounters:
    def test_missing_keys_publish_zero(self):
        """Before #1044 lands, none of these Redis keys exist yet --
        every field must still publish 0, not be skipped."""
        app = _wired_app()
        app._redis.get.return_value = None
        app._publish_message_processor_counters("mp-1")
        published = _state_publishes(app._mqtt)
        assert published["SkyFollower/message-processor/mp-1/statistic/registration_misses_hour"] == "0"
        assert published["SkyFollower/message-processor/mp-1/statistic/operator_misses_today"] == "0"
        assert published["SkyFollower/message-processor/mp-1/statistic/total_messages_processed_lifetime"] == "0"

    def test_redis_outage_skips_publish_entirely(self):
        app = _wired_app()
        app._redis.get.side_effect = redis_lib.exceptions.ConnectionError("down")
        app._publish_message_processor_counters("mp-1")
        published = _state_publishes(app._mqtt)
        assert "SkyFollower/message-processor/mp-1/statistic/registration_misses_hour" not in published

    def test_discovery_uses_processors_own_topic_and_device(self):
        app = _wired_app()
        app._redis.get.return_value = "3"
        app._publish_message_processor_counters("mp-1")
        discovery = _discovery_payloads(app._mqtt)
        payload = discovery[
            "homeassistant/sensor/SkyFollower_message_processor_mp-1_registration_misses_hour/config"
        ]
        assert payload["state_topic"] == (
            "SkyFollower/message-processor/mp-1/statistic/registration_misses_hour"
        )
        assert payload["device"]["ids"] == "SkyFollower_message_processor_mp-1"
        # Mimicry entities use core-health's OWN availability, per design --
        # never the owning processor's own status topic.
        assert payload["availability_topic"] == f"{MQTT_ROOT}/status"


# ---------------------------------------------------------------------------
# Receiver counter mimicry / index self-healing
# ---------------------------------------------------------------------------

class TestPollReceivers:
    def test_publishes_counters_for_each_registered_source(self):
        app = _wired_app()
        app._redis.smembers.return_value = {"attic"}
        registration = {"sources": [{"host": "192.168.10.5", "port": 30002, "source": "1090"}]}
        app._redis.get.side_effect = lambda key: (
            json.dumps(registration) if key == _receiver_registration_key("attic") else "5"
        )

        app._poll_receivers()

        published = _state_publishes(app._mqtt)
        assert published[
            "SkyFollower/receiver/attic/statistic/messages_192-168-10-5_30002_total_hour"
        ] == "5"

    def test_expired_registration_self_heals_the_index(self):
        app = _wired_app()
        app._redis.smembers.return_value = {"stale-receiver"}
        app._redis.get.return_value = None  # registration expired/missing

        app._poll_receivers()

        app._redis.srem.assert_called_once_with(_receiver_index_key(), "stale-receiver")

    def test_malformed_registration_json_is_skipped_not_raised(self):
        app = _wired_app()
        app._redis.smembers.return_value = {"broken"}
        app._redis.get.return_value = "not json"

        app._poll_receivers()  # must not raise

        app._redis.srem.assert_not_called()

    def test_index_read_failure_is_a_no_op(self):
        app = _wired_app()
        app._redis.smembers.side_effect = redis_lib.exceptions.ConnectionError("down")

        app._poll_receivers()  # must not raise

        app._mqtt.publish.assert_not_called()


# ---------------------------------------------------------------------------
# Redis INFO/MEMORY STATS -> published fields
# ---------------------------------------------------------------------------

class TestPublishRedisStats:
    def test_hit_ratio_computed_from_hits_and_misses(self):
        app = _wired_app()
        info = {"keyspace_hits": 90, "keyspace_misses": 10}
        app._publish_redis_stats(info, {})
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/redis/statistic/redis_keyspace_hit_ratio_percent"] == "90.0"

    def test_hit_ratio_skipped_when_no_data_yet(self):
        app = _wired_app()
        app._publish_redis_stats({}, {})
        published = _state_publishes(app._mqtt)
        assert f"{MQTT_ROOT}/redis/statistic/redis_keyspace_hit_ratio_percent" not in published

    def test_used_memory_peak_percent_parsed_from_percent_string(self):
        app = _wired_app()
        info = {"used_memory_peak_perc": "73.21%"}
        app._publish_redis_stats(info, {})
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/redis/statistic/redis_used_memory_peak_percent"] == "73.21"

    def test_unparseable_peak_percent_is_skipped_not_zero(self):
        app = _wired_app()
        info = {"used_memory_peak_perc": "n/a"}
        app._publish_redis_stats(info, {})
        published = _state_publishes(app._mqtt)
        assert f"{MQTT_ROOT}/redis/statistic/redis_used_memory_peak_percent" not in published

    def test_auth_errors_sum_noauth_and_wrongpass(self):
        app = _wired_app()
        info = {
            "errorstat_NOAUTH": {"count": 2},
            "errorstat_WRONGPASS": {"count": 1},
            "errorstat_ERR": {"count": 99},
        }
        app._publish_redis_stats(info, {})
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/redis/statistic/redis_auth_error_count"] == "3"

    def test_keys_count_sourced_from_memory_stats(self):
        app = _wired_app()
        app._publish_redis_stats({}, {"keys.count": 12345})
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/redis/statistic/redis_keys_count"] == "12345"


# ---------------------------------------------------------------------------
# MQTT connect/disconnect lifecycle
# ---------------------------------------------------------------------------

class TestMqttLifecycle:
    def test_on_connect_publishes_online_and_static_discovery(self):
        app = CoreHealth(_minimal_config())
        # In real usage this is the same object as self._mqtt -- paho
        # invokes on_connect on the client that fired it, which is always
        # the one _connect_mqtt() assigned to self._mqtt.
        app._mqtt = MagicMock()
        app._on_mqtt_connect(app._mqtt, None, None, 0, None)

        assert app._mqtt_connected is True
        app._mqtt.publish.assert_any_call(f"{MQTT_ROOT}/status", "ONLINE", retain=True)
        assert app._core_discovery_published is True

    def test_on_connect_clears_dynamic_discovery_dedup(self):
        app = _wired_app()
        app._known_queues.add("archive")
        app._known_mp_counters.add(("mp-1", "registration_misses_hour"))
        app._known_receiver_fields.add(("attic", "messages_x_y_total_hour"))

        app._on_mqtt_connect(app._mqtt, None, None, 0, None)

        assert app._known_queues == set()
        assert app._known_mp_counters == set()
        assert app._known_receiver_fields == set()

    def test_on_disconnect_marks_not_connected(self):
        app = _wired_app()
        app._on_mqtt_disconnect(app._mqtt, None, None, 0, None)
        assert app._mqtt_connected is False

    def test_publish_stat_no_ops_when_not_connected(self):
        app = _wired_app()
        app._mqtt_connected = False
        app._publish_stat("some/topic", "value")
        app._mqtt.publish.assert_not_called()

    def test_publish_stat_no_ops_on_none_value(self):
        app = _wired_app()
        app._publish_stat("some/topic", None)
        app._mqtt.publish.assert_not_called()

    def test_publish_stat_publishes_falsy_non_none_values(self):
        """0/False are legitimate values, not "no data" -- only None is the
        skip sentinel."""
        app = _wired_app()
        app._publish_stat("some/topic", 0)
        app._mqtt.publish.assert_called_once_with("some/topic", "0", retain=True)


# ---------------------------------------------------------------------------
# RabbitMQ poll orchestration
# ---------------------------------------------------------------------------

class TestPollRabbitmqOnce:
    def test_http_failure_marks_disconnected_and_publishes_flag(self):
        app = _wired_app()
        app._session.get.side_effect = Exception("connection refused")

        app._poll_rabbitmq_once()

        assert app._rmq_connected is False
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/statistic/rabbitmq_connected"] == "False"

    def test_successful_poll_publishes_queue_and_broker_stats(self):
        app = _wired_app()

        def _get(url, auth, timeout):
            response = MagicMock()
            if url.endswith("/api/overview"):
                response.json.return_value = {"object_totals": {"connections": 4}}
            elif url.endswith("/api/nodes"):
                response.json.return_value = [{"mem_alarm": False, "disk_free_alarm": False}]
            elif url.endswith("/api/queues/%2F"):
                response.json.return_value = [
                    {"name": "archive", "consumers": 1, "messages_ready": 0,
                     "messages_unacknowledged": 0, "state": "running"},
                    {"name": "not-skyfollowers-queue", "consumers": 0},
                ]
            response.raise_for_status = MagicMock()
            return response

        app._session.get.side_effect = _get
        app._redis.smembers.return_value = set()

        app._poll_rabbitmq_once()

        assert app._rmq_connected is True
        published = _state_publishes(app._mqtt)
        assert f"{MQTT_ROOT}/archive/statistic/consumers" in published
        # A queue that doesn't match SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN
        # must never be published under any topic.
        assert not any("not-skyfollowers-queue" in topic for topic in published)
        assert published[f"{MQTT_ROOT}/rabbitmq/statistic/rabbitmq_connections_total"] == "4"

    def test_message_processor_queues_trigger_counter_publish(self):
        app = _wired_app()

        def _get(url, auth, timeout):
            response = MagicMock()
            if url.endswith("/api/overview"):
                response.json.return_value = {}
            elif url.endswith("/api/nodes"):
                response.json.return_value = []
            elif url.endswith("/api/queues/%2F"):
                response.json.return_value = [
                    {"name": "skyfollower-message-processor-mp-1", "consumers": 1,
                     "messages_ready": 0, "messages_unacknowledged": 0, "state": "running"},
                ]
            response.raise_for_status = MagicMock()
            return response

        app._session.get.side_effect = _get
        app._redis.get.return_value = None
        app._redis.smembers.return_value = set()

        app._poll_rabbitmq_once()

        published = _state_publishes(app._mqtt)
        assert "SkyFollower/message-processor/mp-1/statistic/registration_misses_hour" in published


# ---------------------------------------------------------------------------
# Redis poll orchestration
# ---------------------------------------------------------------------------

class TestPollRedisOnce:
    def test_failure_marks_disconnected(self):
        app = _wired_app()
        app._redis_monitoring.info.side_effect = redis_lib.exceptions.ConnectionError("down")

        app._poll_redis_once()

        assert app._redis_connected is False
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/statistic/redis_connected"] == "False"

    def test_success_publishes_redis_stats(self):
        app = _wired_app()
        app._redis_monitoring.info.return_value = {"connected_clients": 3}
        app._redis_monitoring.memory_stats.return_value = {"keys.count": 100}

        app._poll_redis_once()

        assert app._redis_connected is True
        published = _state_publishes(app._mqtt)
        assert published[f"{MQTT_ROOT}/redis/statistic/redis_connected_clients"] == "3"
        assert published[f"{MQTT_ROOT}/redis/statistic/redis_keys_count"] == "100"


# ---------------------------------------------------------------------------
# Provisional Redis key helpers
# ---------------------------------------------------------------------------

class TestProvisionalRedisKeys:
    def test_operator_misses_key_shape(self):
        assert _metrics_operator_misses_key("mp-1", "today") == (
            "metrics:message_processor:mp-1:operator_misses:today"
        )

    def test_total_messages_processed_key_shape(self):
        assert _metrics_total_messages_processed_key("mp-1", "hour") == (
            "metrics:message_processor:mp-1:total_messages_processed:hour"
        )

    def test_receiver_index_key(self):
        assert _receiver_index_key() == "receiver:index"

    def test_receiver_registration_key(self):
        assert _receiver_registration_key("attic") == "receiver:attic:registration"

    def test_receiver_message_total_key_shape(self):
        assert _receiver_message_total_key("attic", "192.168.10.5", 30002, "hour") == (
            "metrics:receiver:attic:messages_192.168.10.5_30002_total:hour"
        )


# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------

class TestShutdown:
    def test_publishes_offline_and_stops_loop(self):
        app = _wired_app()
        app.shutdown()
        app._mqtt.publish.assert_any_call(f"{MQTT_ROOT}/status", "OFFLINE", retain=True)
        app._mqtt.loop_stop.assert_called_once()
        assert app._shutdown.is_set()

    def test_shutdown_without_mqtt_configured_does_not_raise(self):
        app = CoreHealth(_minimal_config())
        app.shutdown()  # self._mqtt is None -- must not raise
        assert app._shutdown.is_set()
