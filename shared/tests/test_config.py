"""
Tests for shared/config.py -- the environment-variable configuration loader
every component uses in place of a bind-mounted settings file.
"""

from __future__ import annotations

import pytest

from shared.config import (
    DATA_DIR,
    ConfigError,
    ConfigLoader,
    athena_config,
    legacy_migration_s3_config,
    load_config,
    message_processor_config,
    mongo_config,
    mqtt_config,
    parse_receiver_sources,
    rabbitmq_config,
    rabbitmq_management_config,
    receiver_config,
    redis_config,
    s3_config,
)

_MQTT = {
    "MQTT_HOST": "broker.example.com",
    "MQTT_USERNAME": "user",
    "MQTT_PASSWORD": "secret",
}
_RABBITMQ = {
    "RABBITMQ_HOST": "rmq.example.com",
    "RABBITMQ_USERNAME": "skyfollower",
    "RABBITMQ_PASSWORD": "secret",
}
_REDIS = {"REDIS_HOST": "redis.example.com", "REDIS_PASSWORD": "secret"}
_RABBITMQ_MANAGEMENT = {
    "RABBITMQ_HOST": "rmq.example.com",
    "RABBITMQ_MONITORING_USERNAME": "skyfollower-monitoring",
    "RABBITMQ_MONITORING_PASSWORD": "secret",
}
_S3 = {
    "S3_BUCKET": "flights",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "AKIA",
    "AWS_SECRET_ACCESS_KEY": "shh",
}
_RECEIVER = {
    "RECEIVER_NAME": "Attic 1090",
    "RECEIVER_SOURCES": "192.168.10.5:30002:1090",
}
_MESSAGE_PROCESSOR = {
    "MESSAGE_PROCESSOR_ID": "turing-node-3-1",
    "LATITUDE": "40.7",
    "LONGITUDE": "-73.9",
}
_MONGO = {"MONGO_URI": "mongodb://legacy.example.com/skyfollower"}
_LEGACY_MIGRATION_S3 = {
    "SOURCE_S3_BUCKET": "com.skyfollower.datastore",
    "DEST_S3_BUCKET": "skyfollower-archive",
    "AWS_DEFAULT_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "AKIA",
    "AWS_SECRET_ACCESS_KEY": "shh",
}


def _env(*groups: dict, **overrides: str) -> dict:
    env: dict = {}
    for group in groups:
        env.update(group)
    env.update(overrides)
    return env


# ---------------------------------------------------------------------------
# Required variables
# ---------------------------------------------------------------------------


class TestRequiredVariables:
    def test_every_missing_variable_is_reported_in_one_error(self):
        with pytest.raises(ConfigError) as excinfo:
            load_config("rabbitmq", "mqtt", "receiver", environ={})

        # mqtt contributes nothing here -- it's optional everywhere, so an
        # unset MQTT_HOST/USERNAME/PASSWORD is not a problem to report.
        assert set(excinfo.value.problems) == {
            "RABBITMQ_HOST is required but is not set",
            "RABBITMQ_USERNAME is required but is not set",
            "RABBITMQ_PASSWORD is required but is not set",
            "RECEIVER_NAME is required but is not set",
            "RECEIVER_SOURCES is required but is not set",
        }

    def test_error_message_lists_every_problem(self):
        with pytest.raises(ConfigError) as excinfo:
            load_config("redis", "rabbitmq", environ={})

        message = str(excinfo.value)
        for name in ("REDIS_HOST", "RABBITMQ_HOST", "RABBITMQ_USERNAME"):
            assert name in message

    def test_blank_value_counts_as_missing(self):
        with pytest.raises(ConfigError) as excinfo:
            load_config(
                "redis", environ={"REDIS_HOST": "   ", "REDIS_PASSWORD": "secret"}
            )

        assert excinfo.value.problems == ["REDIS_HOST is required but is not set"]

    def test_redis_password_is_required(self):
        with pytest.raises(ConfigError) as excinfo:
            load_config("redis", environ={"REDIS_HOST": "redis.example.com"})

        assert excinfo.value.problems == [
            "REDIS_PASSWORD is required but is not set"
        ]

    def test_aws_credentials_are_required_but_not_returned(self):
        cfg = load_config("s3", environ=_env(_S3))
        assert cfg["s3"] == {"bucket": "flights"}

    def test_missing_aws_credentials_named_individually(self):
        with pytest.raises(ConfigError) as excinfo:
            load_config("s3", environ={"S3_BUCKET": "flights"})

        assert set(excinfo.value.problems) == {
            "AWS_DEFAULT_REGION is required but is not set",
            "AWS_ACCESS_KEY_ID is required but is not set",
            "AWS_SECRET_ACCESS_KEY is required but is not set",
        }

    def test_unknown_block_is_a_programming_error(self):
        with pytest.raises(ValueError, match="Unknown config block"):
            load_config("nonsense", environ={})


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


class TestDefaults:
    def test_every_documented_default_is_applied_when_unset(self):
        cfg = load_config(
            "mqtt", "rabbitmq", "redis", "athena",
            "message_processor",
            environ=_env(_MQTT, _RABBITMQ, _REDIS, _MESSAGE_PROCESSOR),
        )

        assert cfg["log_level"] == "info"
        assert cfg["mqtt"]["port"] == 1883
        assert cfg["rabbitmq"]["port"] == 5672
        assert cfg["redis"]["port"] == 6379
        assert cfg["redis"]["password"] == "secret"
        assert cfg["athena"] == {
            "workgroup": "skyfollower",
            "database": "skyfollower",
            "table": "archive_flights",
        }

    def test_set_values_override_defaults(self):
        cfg = load_config(
            "mqtt", "redis",
            environ=_env(
                _MQTT, _REDIS,
                LOG_LEVEL="debug",
                MQTT_PORT="8883",
                REDIS_PORT="6380",
            ),
        )

        assert cfg["log_level"] == "debug"
        assert cfg["mqtt"]["port"] == 8883
        assert cfg["redis"]["port"] == 6380


# ---------------------------------------------------------------------------
# Numeric coercion
# ---------------------------------------------------------------------------


class TestNumericCoercion:
    def test_non_numeric_port_fails_at_load_not_at_connect(self):
        with pytest.raises(ConfigError) as excinfo:
            load_config("redis", environ=_env(_REDIS, REDIS_PORT="six-thousand"))

        assert excinfo.value.problems == [
            "REDIS_PORT must be a whole number (got 'six-thousand')"
        ]

    def test_non_numeric_latitude_is_reported(self):
        with pytest.raises(ConfigError) as excinfo:
            load_config(
                "message_processor",
                environ=_env(_MESSAGE_PROCESSOR, LATITUDE="north"),
            )

        assert excinfo.value.problems == ["LATITUDE must be a number (got 'north')"]

    def test_numeric_problems_accumulate_with_missing_ones(self):
        with pytest.raises(ConfigError) as excinfo:
            load_config("redis", "mqtt", environ={"REDIS_PORT": "abc"})

        assert "REDIS_HOST is required but is not set" in excinfo.value.problems
        assert "REDIS_PORT must be a whole number (got 'abc')" in excinfo.value.problems

    def test_float_latitude_is_parsed(self):
        cfg = load_config("message_processor", environ=_env(_MESSAGE_PROCESSOR))
        assert cfg["latitude"] == pytest.approx(40.7)
        assert cfg["longitude"] == pytest.approx(-73.9)


# ---------------------------------------------------------------------------
# MESSAGE_PROCESSOR_ID
# ---------------------------------------------------------------------------


class TestMessageProcessorId:
    def test_id_stays_a_string(self):
        cfg = load_config(
            "message_processor",
            environ=_env(_MESSAGE_PROCESSOR, MESSAGE_PROCESSOR_ID="7"),
        )
        assert cfg["message_processor_id"] == "7"

    def test_non_numeric_id_is_accepted(self):
        cfg = load_config("message_processor", environ=_env(_MESSAGE_PROCESSOR))
        assert cfg["message_processor_id"] == "turing-node-3-1"

    def test_missing_id_is_reported(self):
        env = _env(_MESSAGE_PROCESSOR)
        del env["MESSAGE_PROCESSOR_ID"]
        with pytest.raises(ConfigError) as excinfo:
            load_config("message_processor", environ=env)

        assert "MESSAGE_PROCESSOR_ID is required but is not set" in excinfo.value.problems


# ---------------------------------------------------------------------------
# RECEIVER_SOURCES
# ---------------------------------------------------------------------------


class TestReceiverSources:
    def test_single_triple(self):
        assert parse_receiver_sources("out.adsb.lol:1366:EXTERNAL") == [
            {"host": "out.adsb.lol", "port": 1366, "source": "EXTERNAL"}
        ]

    def test_multiple_triples(self):
        parsed = parse_receiver_sources(
            "192.168.10.5:30002:1090,192.168.10.5:30978:978"
        )
        assert parsed == [
            {"host": "192.168.10.5", "port": 30002, "source": "1090"},
            {"host": "192.168.10.5", "port": 30978, "source": "978"},
        ]

    def test_surrounding_whitespace_is_tolerated(self):
        parsed = parse_receiver_sources(" 192.168.10.5 : 30002 : 1090 , host2:30002:978 ")
        assert [s["host"] for s in parsed] == ["192.168.10.5", "host2"]

    def test_lowercase_external_is_canonicalised(self):
        assert parse_receiver_sources("h:1:external")[0]["source"] == "EXTERNAL"

    def test_empty_value_is_rejected(self):
        with pytest.raises(ValueError, match="at least one host:port:source triple"):
            parse_receiver_sources("")

    def test_comma_only_value_is_rejected(self):
        with pytest.raises(ValueError, match="at least one host:port:source triple"):
            parse_receiver_sources(" , ")

    def test_too_few_fields_names_the_triple(self):
        with pytest.raises(ValueError, match="'192.168.10.5:30002'"):
            parse_receiver_sources("192.168.10.5:30002")

    def test_too_many_fields_names_the_triple(self):
        with pytest.raises(ValueError, match="'a:1:1090:extra'"):
            parse_receiver_sources("a:1:1090:extra")

    def test_empty_host_names_the_triple(self):
        with pytest.raises(ValueError, match="empty host"):
            parse_receiver_sources(":30002:1090")

    def test_non_numeric_port_names_the_triple(self):
        with pytest.raises(ValueError, match="invalid port 'thirty'"):
            parse_receiver_sources("host:thirty:1090")

    def test_out_of_range_port_names_the_triple(self):
        with pytest.raises(ValueError, match="invalid port '70000'"):
            parse_receiver_sources("host:70000:1090")

    def test_unknown_source_tag_names_the_triple_and_lists_valid_tags(self):
        with pytest.raises(ValueError) as excinfo:
            parse_receiver_sources("host:30002:1091")

        message = str(excinfo.value)
        assert "'host:30002:1091'" in message
        assert "invalid source '1091'" in message
        assert "1090, 978, EXTERNAL" in message

    def test_only_the_offending_triple_of_several_is_named(self):
        with pytest.raises(ValueError) as excinfo:
            parse_receiver_sources("good:30002:1090,bad:30978:AAA")

        assert "'bad:30978:AAA'" in str(excinfo.value)
        assert "good:30002:1090" not in str(excinfo.value)

    def test_malformed_sources_surface_as_a_config_problem(self):
        with pytest.raises(ConfigError) as excinfo:
            load_config(
                "receiver",
                environ=_env(_RECEIVER, RECEIVER_SOURCES="host:30002:1091"),
            )

        assert any("invalid source" in p for p in excinfo.value.problems)

    def test_sources_parse_into_the_receiver_block(self):
        cfg = load_config("receiver", environ=_env(_RECEIVER))
        assert cfg["name"] == "Attic 1090"
        assert cfg["sources"] == [
            {"host": "192.168.10.5", "port": 30002, "source": "1090"}
        ]


# ---------------------------------------------------------------------------
# Receiver's own optional Redis block -- unlike every other
# Redis-consuming component, REDIS_HOST is optional here.
# ---------------------------------------------------------------------------


class TestReceiverOptionalRedis:
    def test_redis_host_unset_does_not_raise_and_yields_blank_host(self):
        cfg = load_config("receiver", environ=_env(_RECEIVER))
        assert cfg["redis"] == {"host": "", "port": 6379, "password": ""}

    def test_redis_host_set_is_read_through(self):
        cfg = load_config(
            "receiver",
            environ=_env(
                _RECEIVER,
                REDIS_HOST="redis.example.com",
                REDIS_PORT="6380",
                REDIS_PASSWORD="secret",
            ),
        )
        assert cfg["redis"] == {
            "host": "redis.example.com",
            "port": 6380,
            "password": "secret",
        }

    def test_redis_port_defaults_when_unset(self):
        cfg = load_config(
            "receiver", environ=_env(_RECEIVER, REDIS_HOST="redis.example.com")
        )
        assert cfg["redis"]["port"] == 6379

    def test_unset_redis_host_is_never_reported_as_a_problem(self):
        """REDIS_HOST is optional for the receiver -- omitting it must not
        raise, unlike every other component's "redis" block."""
        # Does not raise ConfigError.
        load_config("receiver", environ=_env(_RECEIVER))


# ---------------------------------------------------------------------------
# Shape
# ---------------------------------------------------------------------------


class TestShape:
    def test_receiver_shape(self):
        cfg = load_config(
            "rabbitmq", "mqtt", "receiver",
            environ=_env(_RABBITMQ, _MQTT, _RECEIVER),
        )
        assert cfg["rabbitmq"] == {
            "host": "rmq.example.com",
            "port": 5672,
            "username": "skyfollower",
            "password": "secret",
        }
        assert cfg["mqtt"] == {
            "host": "broker.example.com",
            "port": 1883,
            "username": "user",
            "password": "secret",
        }
        assert "sources" in cfg and "name" in cfg

    def test_archive_processor_shape(self):
        cfg = load_config(
            "rabbitmq", "redis", "mqtt", "s3",
            environ=_env(_RABBITMQ, _REDIS, _MQTT, _S3),
        )
        assert set(cfg) == {
            "log_level", "rabbitmq", "redis", "mqtt", "s3",
        }

    def test_runner_shape(self):
        cfg = load_config("redis", "mqtt", environ=_env(_REDIS, _MQTT))
        assert set(cfg) == {"log_level", "redis", "mqtt"}

    def test_data_dir_is_a_constant_not_a_config_field(self):
        cfg = load_config("redis", environ=_env(_REDIS))
        assert "data_dir" not in cfg
        assert DATA_DIR == "/app/data"

    def test_core_health_shape(self):
        cfg = load_config(
            "rabbitmq_management", "redis", "mqtt",
            environ=_env(_RABBITMQ_MANAGEMENT, _REDIS, _MQTT),
        )
        assert set(cfg) == {
            "log_level", "rabbitmq_management", "redis", "mqtt",
        }
        assert cfg["rabbitmq_management"] == {
            "host": "rmq.example.com",
            "port": 15672,
            "username": "skyfollower-monitoring",
            "password": "secret",
        }
        # Same default-user credential every other component uses --
        # core-health authenticates with this for Redis INFO/MEMORY
        # introspection too, no separate scoped user.
        assert cfg["redis"] == {
            "host": "redis.example.com",
            "port": 6379,
            "password": "secret",
        }

    def test_legacy_migration_shape(self):
        cfg = load_config(
            "rabbitmq", "mongo", "legacy_migration_s3",
            environ=_env(_RABBITMQ, _MONGO, _LEGACY_MIGRATION_S3),
        )
        assert set(cfg) == {
            "log_level", "rabbitmq", "mongo", "legacy_migration_s3",
        }
        assert cfg["mongo"] == {
            "uri": "mongodb://legacy.example.com/skyfollower",
            "database": "skyfollower",
            "collection": "flights",
        }
        assert cfg["legacy_migration_s3"] == {
            "source_bucket": "com.skyfollower.datastore",
            "dest_bucket": "skyfollower-archive",
        }


# ---------------------------------------------------------------------------
# Standalone block helpers
# ---------------------------------------------------------------------------


class TestBlockHelpers:
    def test_helpers_validate_on_their_own(self, monkeypatch):
        monkeypatch.delenv("RABBITMQ_HOST", raising=False)
        monkeypatch.delenv("RABBITMQ_USERNAME", raising=False)
        monkeypatch.delenv("RABBITMQ_PASSWORD", raising=False)
        with pytest.raises(ConfigError):
            rabbitmq_config()

    def test_mqtt_config_never_raises_on_its_own(self, monkeypatch):
        """Unlike every other block, mqtt has nothing required -- calling it
        with no MQTT env vars set at all must not raise."""
        monkeypatch.delenv("MQTT_HOST", raising=False)
        monkeypatch.delenv("MQTT_USERNAME", raising=False)
        monkeypatch.delenv("MQTT_PASSWORD", raising=False)
        assert mqtt_config() == {
            "host": "",
            "port": 1883,
            "username": "",
            "password": "",
        }

    def test_helpers_share_a_loader_when_given_one(self):
        loader = ConfigLoader({})
        mqtt_config(loader)
        redis_config(loader)
        rabbitmq_config(loader)
        rabbitmq_management_config(loader)
        s3_config(loader)
        athena_config(loader)
        receiver_config(loader)
        message_processor_config(loader)

        with pytest.raises(ConfigError) as excinfo:
            loader.raise_for_problems()

        # One accumulated report rather than a separate failure per block.
        assert len(excinfo.value.problems) > 8

    def test_rabbitmq_management_config_requires_monitoring_credentials(self, monkeypatch):
        monkeypatch.delenv("RABBITMQ_MONITORING_USERNAME", raising=False)
        monkeypatch.delenv("RABBITMQ_MONITORING_PASSWORD", raising=False)
        for name, value in {"RABBITMQ_HOST": "rmq.example.com"}.items():
            monkeypatch.setenv(name, value)
        with pytest.raises(ConfigError):
            rabbitmq_management_config()

    def test_rabbitmq_management_port_defaults_to_15672(self):
        assert rabbitmq_management_config(
            ConfigLoader(_RABBITMQ_MANAGEMENT)
        )["port"] == 15672

    def test_helpers_read_the_process_environment_by_default(self, monkeypatch):
        for name, value in _REDIS.items():
            monkeypatch.setenv(name, value)
        monkeypatch.setenv("REDIS_PORT", "6380")
        assert redis_config() == {
            "host": "redis.example.com",
            "port": 6380,
            "password": "secret",
        }

    def test_mongo_config_defaults_database_and_collection(self):
        assert mongo_config(ConfigLoader(_MONGO)) == {
            "uri": "mongodb://legacy.example.com/skyfollower",
            "database": "skyfollower",
            "collection": "flights",
        }

    def test_mongo_config_requires_uri(self, monkeypatch):
        monkeypatch.delenv("MONGO_URI", raising=False)
        with pytest.raises(ConfigError):
            mongo_config()

    def test_legacy_migration_s3_config_requires_both_buckets(self, monkeypatch):
        monkeypatch.delenv("SOURCE_S3_BUCKET", raising=False)
        monkeypatch.delenv("DEST_S3_BUCKET", raising=False)
        with pytest.raises(ConfigError):
            legacy_migration_s3_config()

    def test_legacy_migration_s3_config_reads_both_buckets(self):
        assert legacy_migration_s3_config(ConfigLoader(_LEGACY_MIGRATION_S3)) == {
            "source_bucket": "com.skyfollower.datastore",
            "dest_bucket": "skyfollower-archive",
        }
