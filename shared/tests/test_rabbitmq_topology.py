from unittest.mock import MagicMock, call

from shared.rabbitmq_topology import (
    ADSB_BINDING_WEIGHT,
    ADSB_EXCHANGE,
    ADSB_EXCHANGE_ARGUMENTS,
    ADSB_UNROUTABLE_EXCHANGE,
    ADSB_UNROUTABLE_QUEUE,
    adsb_queue_name,
    bind_adsb_queue,
    declare_adsb_topology,
)


class TestAdsbQueueName:
    def test_numeric_id(self):
        assert adsb_queue_name("0") == "adsb-0"

    def test_arbitrary_string_id(self):
        assert adsb_queue_name("turing-node-3-1") == "adsb-turing-node-3-1"


class TestDeclareAdsbTopology:
    def test_declares_hash_exchange_pointing_at_the_alternate_exchange(self):
        channel = MagicMock()
        declare_adsb_topology(channel)

        channel.exchange_declare.assert_any_call(
            exchange=ADSB_EXCHANGE,
            exchange_type="x-consistent-hash",
            durable=True,
            arguments={"alternate-exchange": ADSB_UNROUTABLE_EXCHANGE},
        )
        assert ADSB_EXCHANGE_ARGUMENTS == {
            "alternate-exchange": ADSB_UNROUTABLE_EXCHANGE
        }

    def test_unroutable_queue_is_bound_to_the_alternate_exchange(self):
        channel = MagicMock()
        declare_adsb_topology(channel)

        channel.exchange_declare.assert_any_call(
            exchange=ADSB_UNROUTABLE_EXCHANGE, exchange_type="fanout", durable=True
        )
        channel.queue_declare.assert_called_once_with(
            queue=ADSB_UNROUTABLE_QUEUE, durable=True
        )
        channel.queue_bind.assert_called_once_with(
            queue=ADSB_UNROUTABLE_QUEUE, exchange=ADSB_UNROUTABLE_EXCHANGE
        )

    def test_alternate_exchange_exists_before_the_hash_exchange_names_it(self):
        channel = MagicMock()
        declare_adsb_topology(channel)

        assert channel.exchange_declare.call_args_list[0] == call(
            exchange=ADSB_UNROUTABLE_EXCHANGE, exchange_type="fanout", durable=True
        )

    def test_is_idempotent_across_repeated_connects(self):
        """Both components redeclare on every connect; the second pass must
        issue the identical calls so RabbitMQ treats it as a no-op rather
        than a conflicting redeclaration."""
        first, second = MagicMock(), MagicMock()
        declare_adsb_topology(first)
        declare_adsb_topology(second)

        assert first.mock_calls == second.mock_calls


class TestBindAdsbQueue:
    def test_declares_and_binds_with_weight_one(self):
        channel = MagicMock()

        queue_name = bind_adsb_queue(channel, "turing-node-3-1")

        assert queue_name == "adsb-turing-node-3-1"
        channel.queue_declare.assert_called_once_with(
            queue="adsb-turing-node-3-1", durable=True
        )
        channel.queue_bind.assert_called_once_with(
            queue="adsb-turing-node-3-1",
            exchange=ADSB_EXCHANGE,
            routing_key=ADSB_BINDING_WEIGHT,
        )

    def test_binding_weight_is_one(self):
        assert ADSB_BINDING_WEIGHT == "1"
