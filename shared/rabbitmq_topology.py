"""
Centralised RabbitMQ exchange/queue names and declaration helpers for SkyFollower.

The receiver and the message processor both declare this topology on every
connect, and both must agree on it exactly: RabbitMQ answers a redeclaration
whose type or arguments differ from what already exists with a channel-level
error. Declaring from one module turns that into a guarantee rather than a
convention.

Also holds the one regex pattern that defines "what SkyFollower owns" on a
RabbitMQ broker that may be shared with unrelated processes -- referenced by
core-health (to filter the Management API's queue list down to SkyFollower's
own queues) and mirrored, not imported (bash can't import Python), by
scripts/install.sh's provision_rabbitmq_users(), which is the actual
authoritative source these permissions are granted from. The two copies
must change together.
"""

import re

# Every inbound ADS-B/UAT message is published here with the aircraft's ICAO
# hex as the routing key. The x-consistent-hash type maps that key onto one
# of the bound queues, which is what keeps an aircraft with a single message
# processor -- and therefore with the SQLite flight state that processor
# holds -- without the publisher knowing how many processors exist.
ADSB_EXCHANGE = "skyfollower-adsb"
ADSB_EXCHANGE_TYPE = "x-consistent-hash"

# Catches whatever the hash exchange cannot route, which is every message
# published while no processor queue is bound. The receiver publishes
# without publisher confirms and without the mandatory flag, so absent an
# alternate exchange the broker discards those messages without telling
# anyone; here they land in a durable queue whose depth is observable.
ADSB_UNROUTABLE_EXCHANGE = "skyfollower-adsb-unroutable"
ADSB_UNROUTABLE_QUEUE = "skyfollower-adsb-unroutable"

ADSB_EXCHANGE_ARGUMENTS = {"alternate-exchange": ADSB_UNROUTABLE_EXCHANGE}

# Binding weight carried as the routing key on every processor binding. The
# plugin's own documentation recommends an equal weight of 1 for all
# bindings; weights above that are for deliberately uneven consumers and
# measurably do not improve the evenness of the distribution.
ADSB_BINDING_WEIGHT = "1"


# Mirrors scripts/install.sh's provision_rabbitmq_users() permission regex
# exactly (configure/write pattern, `amq.default` included) -- see the
# module docstring. No queue is ever actually named "amq.default" (that
# alternation branch exists only because this is shared with an
# exchange-matching permission, not a queue-only one), so it's a harmless
# no-op branch here, not a bug.
SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN = r"^(skyfollower-adsb.*|skyfollower-message-processor-.*|skyfollower-archive|amq\.default)$"
_SKYFOLLOWER_RABBITMQ_RESOURCE_RE = re.compile(SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN)

# The literal queue name completed flights are published to via the default
# exchange (see archive-processor/main.py). Not fleet/ID-derived like a
# message processor's queue, so it's just a constant rather than a builder
# function.
ARCHIVE_QUEUE_NAME = "skyfollower-archive"

_MESSAGE_PROCESSOR_QUEUE_PREFIX = "skyfollower-message-processor-"


def is_skyfollower_queue(queue_name: str) -> bool:
    """True if `queue_name` is one of SkyFollower's own RabbitMQ resources.

    Used by core-health to filter the Management API's full queue list down
    to SkyFollower's own, on a broker that may be shared with unrelated
    processes.
    """
    return bool(_SKYFOLLOWER_RABBITMQ_RESOURCE_RE.match(queue_name))


def message_processor_queue_name(message_processor_id: str) -> str:
    """
    Input queue owned by a single message processor. Same fleet-wide flat ID
    used for the compose service/container name and the Redis heartbeat key --
    no separate queue-naming scheme layered on top.
    skyfollower-message-processor-{MESSAGE_PROCESSOR_ID}
    """
    return f"{_MESSAGE_PROCESSOR_QUEUE_PREFIX}{message_processor_id}"


def message_processor_id_from_queue_name(queue_name: str) -> "str | None":
    """
    Inverse of message_processor_queue_name(): recovers {id} from a queue
    name as returned by RabbitMQ's Management API. Returns None for a queue
    name that isn't a message processor's own input queue -- used by
    core-health to decide which SkyFollower_message_processor_{id} device a
    polled queue's entities belong on, and which metrics:message_processor:
    {id}:* Redis keys to read for it.

    A straight prefix strip, not a split on "-": MESSAGE_PROCESSOR_ID is any
    unique string (not a contiguous integer ordinal, see #1031) and may
    itself contain hyphens.
    """
    if queue_name.startswith(_MESSAGE_PROCESSOR_QUEUE_PREFIX):
        return queue_name[len(_MESSAGE_PROCESSOR_QUEUE_PREFIX):]
    return None


def declare_adsb_topology(channel) -> None:
    """Declare the hash exchange together with its unroutable-message path.

    The alternate exchange is declared first so the hash exchange never
    briefly exists pointing at something absent.
    """
    channel.exchange_declare(
        exchange=ADSB_UNROUTABLE_EXCHANGE,
        exchange_type="fanout",
        durable=True,
    )
    channel.queue_declare(queue=ADSB_UNROUTABLE_QUEUE, durable=True)
    channel.queue_bind(
        queue=ADSB_UNROUTABLE_QUEUE, exchange=ADSB_UNROUTABLE_EXCHANGE
    )
    channel.exchange_declare(
        exchange=ADSB_EXCHANGE,
        exchange_type=ADSB_EXCHANGE_TYPE,
        durable=True,
        arguments=ADSB_EXCHANGE_ARGUMENTS,
    )


def bind_adsb_queue(channel, message_processor_id: str) -> str:
    """Declare and bind one message processor's input queue; returns its name.

    Binding order assigns the positional slot the exchange hashes onto, and
    that slot is fixed for as long as the binding exists. Rebinding an
    existing binding is a no-op, so a restarting processor keeps its slot
    and moves no aircraft.
    """
    queue_name = message_processor_queue_name(message_processor_id)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        queue=queue_name,
        exchange=ADSB_EXCHANGE,
        routing_key=ADSB_BINDING_WEIGHT,
    )
    return queue_name
