from unittest.mock import MagicMock, call

from shared.rabbitmq_topology import (
    ADSB_BINDING_WEIGHT,
    ADSB_EXCHANGE,
    ADSB_EXCHANGE_ARGUMENTS,
    ADSB_UNROUTABLE_EXCHANGE,
    ADSB_UNROUTABLE_QUEUE,
    ARCHIVE_QUEUE_NAME,
    SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN,
    bind_adsb_queue,
    declare_adsb_topology,
    is_skyfollower_queue,
    message_processor_id_from_queue_name,
    message_processor_queue_name,
)


class TestMessageProcessorQueueName:
    def test_numeric_id(self):
        assert message_processor_queue_name("0") == "skyfollower-message-processor-0"

    def test_arbitrary_string_id(self):
        assert (
            message_processor_queue_name("7")
            == "skyfollower-message-processor-7"
        )


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

        queue_name = bind_adsb_queue(channel, "7")

        assert queue_name == "skyfollower-message-processor-7"
        channel.queue_declare.assert_called_once_with(
            queue="skyfollower-message-processor-7", durable=True
        )
        channel.queue_bind.assert_called_once_with(
            queue="skyfollower-message-processor-7",
            exchange=ADSB_EXCHANGE,
            routing_key=ADSB_BINDING_WEIGHT,
        )

    def test_binding_weight_is_one(self):
        assert ADSB_BINDING_WEIGHT == "1"


class TestIsSkyfollowerQueue:
    def test_matches_pattern_used_by_install_sh(self):
        """Guards against the two copies (this constant and
        scripts/install.sh's provision_rabbitmq_users literal) drifting
        apart -- bash can't import this constant directly, so this is the
        one automated check that the value hasn't silently changed here
        without a human also updating the shell script."""
        assert SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN == (
            r"^(adsb.*|skyfollower-message-processor-.*|archive|amq\.default)$"
        )

    def test_adsb_prefixed_queue_matches(self):
        assert is_skyfollower_queue("adsb-unroutable") is True

    def test_message_processor_queue_matches(self):
        assert is_skyfollower_queue("skyfollower-message-processor-mp-1") is True

    def test_archive_queue_matches(self):
        assert is_skyfollower_queue(ARCHIVE_QUEUE_NAME) is True

    def test_unrelated_queue_does_not_match(self):
        assert is_skyfollower_queue("some-other-teams-queue") is False

    def test_amq_default_never_actually_occurs_as_a_queue_name(self):
        """amq.default is an exchange name pulled in from the same
        permission regex this mirrors, not a queue that can ever really
        exist -- included for parity with install.sh, not because a real
        queue is expected to match it."""
        assert is_skyfollower_queue("amq.default") is True


class TestMessageProcessorIdFromQueueName:
    def test_round_trips_with_message_processor_queue_name(self):
        assert message_processor_id_from_queue_name(
            message_processor_queue_name("mp-1")
        ) == "mp-1"

    def test_id_containing_hyphens_round_trips(self):
        """MESSAGE_PROCESSOR_ID is any unique string, not a contiguous
        ordinal -- it may itself contain hyphens, so this must be a prefix
        strip, not a split on "-"."""
        assert message_processor_id_from_queue_name(
            message_processor_queue_name("host-a-mp-2")
        ) == "host-a-mp-2"

    def test_non_processor_queue_returns_none(self):
        assert message_processor_id_from_queue_name("archive") is None
        assert message_processor_id_from_queue_name("adsb-unroutable") is None
