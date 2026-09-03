"""
Anti-drift guard for specs/asyncapi-amqp.yaml -- the AsyncAPI document
covering SkyFollower's RabbitMQ/AMQP topology (the sibling document to
specs/asyncapi.yaml's MQTT topics).

shared/rabbitmq_topology.py is the single source of truth for every
exchange/queue name, argument, and binding value used at runtime. Nothing
enforces that the AsyncAPI document describing that topology stays in sync
with it -- these assertions are that enforcement, so a future rename of
any of those constants can't land without this file (and therefore
specs/asyncapi-amqp.yaml) being touched too.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from shared.rabbitmq_topology import (
    ADSB_BINDING_WEIGHT,
    ADSB_EXCHANGE,
    ADSB_EXCHANGE_ARGUMENTS,
    ADSB_EXCHANGE_TYPE,
    ADSB_UNROUTABLE_EXCHANGE,
    ADSB_UNROUTABLE_QUEUE,
    ARCHIVE_QUEUE_NAME,
    message_processor_queue_name,
)

_SPEC_PATH = (
    Path(__file__).resolve().parents[2] / "specs" / "asyncapi-amqp.yaml"
)
_SPEC = yaml.safe_load(_SPEC_PATH.read_text())


def _channel(name: str) -> dict:
    return _SPEC["channels"][name]


def _amqp_binding(channel_name: str) -> dict:
    return _channel(channel_name)["bindings"]["amqp"]


class TestDocumentShape:
    def test_spec_parses_and_declares_amqp_protocol(self):
        assert _SPEC["asyncapi"].startswith("3.")
        server = _SPEC["servers"]["rabbitmq"]
        assert server["protocol"] == "amqp"

    def test_version_is_the_checked_in_placeholder(self):
        # Mirrors test_spec_version_placeholders.py's convention; asserted
        # again here so this file's own suite fails loudly if the
        # placeholder is ever overwritten with a real version on main.
        assert _SPEC["info"]["version"] == "9999.99.99"


class TestAdsbExchange:
    def test_channel_address_matches_constant(self):
        assert _channel("adsbExchange")["address"] == ADSB_EXCHANGE

    def test_binding_declares_the_exchange_by_name(self):
        binding = _amqp_binding("adsbExchange")
        assert binding["is"] == "routingKey"
        assert binding["exchange"]["name"] == ADSB_EXCHANGE

    def test_exchange_type_extension_matches_the_real_plugin_type(self):
        """exchange.type is a strict AsyncAPI enum (topic/direct/fanout/
        default/headers) that x-consistent-hash cannot appear in --
        represented instead via the x-rabbitmqExchangeType specification
        extension. This guards that extension value against drifting from
        the real ADSB_EXCHANGE_TYPE constant."""
        binding = _amqp_binding("adsbExchange")
        assert "type" not in binding["exchange"], (
            "exchange.type must stay unset -- x-consistent-hash is not a "
            "member of the AMQP binding's exchange.type enum, so setting "
            "it to any of the standard values here would misdocument the "
            "real broker behaviour."
        )
        assert binding["x-rabbitmqExchangeType"] == ADSB_EXCHANGE_TYPE

    def test_alternate_exchange_argument_matches(self):
        binding = _amqp_binding("adsbExchange")
        assert (
            binding["x-rabbitmqExchangeArguments"]["alternate-exchange"]
            == ADSB_EXCHANGE_ARGUMENTS["alternate-exchange"]
        )

    def test_binding_weight_is_documented_in_the_message_processor_queue(self):
        # ADSB_BINDING_WEIGHT ("1") is the routing key every processor
        # binds with -- documented in messageProcessorQueue's description
        # rather than as a separate binding field (AMQP channel bindings
        # have no routing-key-of-a-queue-binding field to hold it).
        description = _channel("messageProcessorQueue")["description"]
        assert f'routing key of "{ADSB_BINDING_WEIGHT}"' in description


class TestAdsbUnroutable:
    def test_channel_address_matches_both_constants(self):
        # The exchange and the queue share one literal name by design --
        # asserted here first so the single address check below actually
        # covers both constants, not just one of two that happen to be
        # spelled the same today.
        assert ADSB_UNROUTABLE_EXCHANGE == ADSB_UNROUTABLE_QUEUE
        assert _channel("adsbUnroutable")["address"] == ADSB_UNROUTABLE_QUEUE

    def test_binding_declares_the_queue_by_name(self):
        binding = _amqp_binding("adsbUnroutable")
        assert binding["is"] == "queue"
        assert binding["queue"]["name"] == ADSB_UNROUTABLE_QUEUE

    def test_feeder_exchange_extension_matches(self):
        binding = _amqp_binding("adsbUnroutable")
        assert binding["x-rabbitmqFeederExchange"]["name"] == ADSB_UNROUTABLE_EXCHANGE
        assert binding["x-rabbitmqFeederExchange"]["type"] == "fanout"


class TestArchiveQueue:
    def test_channel_address_matches_constant(self):
        assert _channel("archiveQueue")["address"] == ARCHIVE_QUEUE_NAME

    def test_binding_declares_the_queue_by_name(self):
        binding = _amqp_binding("archiveQueue")
        assert binding["is"] == "queue"
        assert binding["queue"]["name"] == ARCHIVE_QUEUE_NAME


class TestMessageProcessorQueue:
    def test_channel_address_matches_the_naming_helper(self):
        expected = message_processor_queue_name("{message_processor_id}")
        assert _channel("messageProcessorQueue")["address"] == expected

    def test_binding_declares_the_templated_queue_name(self):
        binding = _amqp_binding("messageProcessorQueue")
        assert binding["is"] == "queue"
        expected = message_processor_queue_name("{message_processor_id}")
        assert binding["queue"]["name"] == expected


class TestEveryChannelUsesTheCurrentBindingVersion:
    def test_all_amqp_bindings_pin_0_3_0(self):
        """0.3.0 is the AMQP binding version actually bundled in the
        AsyncAPI 3.1.0 JSON Schema (verified against
        asyncapi/spec-json-schemas' 3.1.0 schema during implementation) --
        pinning it explicitly, and checking it here, means a future
        AsyncAPI/bindings upgrade that changes the enum is caught by a
        failing test rather than a silently-invalid document."""
        for name, channel in _SPEC["channels"].items():
            assert channel["bindings"]["amqp"]["bindingVersion"] == "0.3.0", name
        for name, operation in _SPEC["operations"].items():
            assert operation["bindings"]["amqp"]["bindingVersion"] == "0.3.0", name
