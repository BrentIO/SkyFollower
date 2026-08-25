"""
Centralised RabbitMQ exchange/queue names and declaration helpers for SkyFollower.

The receiver and the message processor both declare this topology on every
connect, and both must agree on it exactly: RabbitMQ answers a redeclaration
whose type or arguments differ from what already exists with a channel-level
error. Declaring from one module turns that into a guarantee rather than a
convention.
"""

# Every inbound ADS-B/UAT message is published here with the aircraft's ICAO
# hex as the routing key. The x-consistent-hash type maps that key onto one
# of the bound queues, which is what keeps an aircraft with a single message
# processor -- and therefore with the SQLite flight state that processor
# holds -- without the publisher knowing how many processors exist.
ADSB_EXCHANGE = "adsb"
ADSB_EXCHANGE_TYPE = "x-consistent-hash"

# Catches whatever the hash exchange cannot route, which is every message
# published while no processor queue is bound. The receiver publishes
# without publisher confirms and without the mandatory flag, so absent an
# alternate exchange the broker discards those messages without telling
# anyone; here they land in a durable queue whose depth is observable.
ADSB_UNROUTABLE_EXCHANGE = "adsb-unroutable"
ADSB_UNROUTABLE_QUEUE = "adsb-unroutable"

ADSB_EXCHANGE_ARGUMENTS = {"alternate-exchange": ADSB_UNROUTABLE_EXCHANGE}

# Binding weight carried as the routing key on every processor binding. The
# plugin's own documentation recommends an equal weight of 1 for all
# bindings; weights above that are for deliberately uneven consumers and
# measurably do not improve the evenness of the distribution.
ADSB_BINDING_WEIGHT = "1"


def message_processor_queue_name(message_processor_id: str) -> str:
    """
    Input queue owned by a single message processor. Same fleet-wide flat ID
    used for the compose service/container name and the Redis heartbeat key --
    no separate queue-naming scheme layered on top.
    skyfollower-message-processor-{MESSAGE_PROCESSOR_ID}
    """
    return f"skyfollower-message-processor-{message_processor_id}"


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
