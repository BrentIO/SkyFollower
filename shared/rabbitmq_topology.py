"""
Centralised RabbitMQ exchange/queue names and declaration helpers for
SkyFollower.

The receiver and message processor both declare this topology on every
connect, and both must agree on it exactly: RabbitMQ raises a channel-level
error on a redeclaration whose type or arguments differ from what already
exists. Declaring from one module turns that into a guarantee.

Also holds the regex defining "what SkyFollower owns" on a RabbitMQ broker
that may be shared with unrelated processes, used by core-health. It is
mirrored (not imported) by scripts/install.sh's provision_rabbitmq_users(),
the actual authoritative source these permissions are granted from -- the
two copies must change together.
"""

import re

from shared.fallback_queue import DEFAULT_DEAD_LETTER_MAX_BYTES
from shared.timing import RAW_FRAMES_QUEUE_TTL_SECONDS

# Every inbound ADS-B/UAT message is published here with the aircraft's
# ICAO hex as the routing key. x-consistent-hash maps that key onto one
# bound queue, keeping an aircraft on a single message processor without
# the publisher knowing how many processors exist.
ADSB_EXCHANGE = "skyfollower-adsb"
ADSB_EXCHANGE_TYPE = "x-consistent-hash"

# Catches whatever the hash exchange cannot route (every message published
# while no processor queue is bound). The receiver publishes without
# confirms or the mandatory flag, so absent an alternate exchange the
# broker would silently discard these instead.
ADSB_UNROUTABLE_EXCHANGE = "skyfollower-adsb-unroutable"
ADSB_UNROUTABLE_QUEUE = "skyfollower-adsb-unroutable"

ADSB_EXCHANGE_ARGUMENTS = {"alternate-exchange": ADSB_UNROUTABLE_EXCHANGE}

# Binding weight carried as the routing key on every processor binding.
# The plugin's docs recommend equal weight 1 for all bindings; higher
# weights are for deliberately uneven consumers.
ADSB_BINDING_WEIGHT = "1"


# Mirrors scripts/install.sh's provision_rabbitmq_users() permission regex
# exactly -- see the module docstring. `amq.default` is a harmless no-op
# branch here (shared with an exchange-matching permission), not a bug.
SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN = r"^(skyfollower-adsb.*|skyfollower-message-processor-.*|skyfollower-archive|skyfollower-archive-raw-frames|amq\.default)$"
_SKYFOLLOWER_RABBITMQ_RESOURCE_RE = re.compile(SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN)

# Queue completed flights are published to via the default exchange. Not
# fleet/ID-derived like a message processor's queue, so it's a constant.
ARCHIVE_QUEUE_NAME = "skyfollower-archive"

# Short-lived forensic queue for CAPTURE_RAW_FRAMES (see
# message-processor/README.md). No dedicated consumer service -- meant for
# manual inspection/drain while investigating a decode anomaly, so
# message-processor is both its sole declarer and publisher.
RAW_FRAMES_QUEUE_NAME = "skyfollower-archive-raw-frames"

# 8-hour TTL and a 100MB size cap (reusing FallbackQueue's
# DEFAULT_DEAD_LETTER_MAX_BYTES), whichever limit is hit first evicting the
# oldest message. Declared only when CAPTURE_RAW_FRAMES is on.
RAW_FRAMES_QUEUE_ARGUMENTS = {
    "x-message-ttl": RAW_FRAMES_QUEUE_TTL_SECONDS * 1000,
    "x-max-length-bytes": DEFAULT_DEAD_LETTER_MAX_BYTES,
}

_MESSAGE_PROCESSOR_QUEUE_PREFIX = "skyfollower-message-processor-"


def is_skyfollower_queue(queue_name: str) -> bool:
    """True if `queue_name` is one of SkyFollower's own RabbitMQ resources.

    Used by core-health to filter the Management API's full queue list down
    to SkyFollower's own, on a broker that may be shared with unrelated
    processes.
    """
    return bool(_SKYFOLLOWER_RABBITMQ_RESOURCE_RE.match(queue_name))


def message_processor_queue_name(message_processor_id: str) -> str:
    """Input queue owned by a single message processor. Same fleet-wide
    flat ID used for the compose service/container name and the Redis
    heartbeat key."""
    return f"{_MESSAGE_PROCESSOR_QUEUE_PREFIX}{message_processor_id}"


def message_processor_id_from_queue_name(queue_name: str) -> "str | None":
    """Inverse of message_processor_queue_name(). Returns None for a
    queue name that isn't a message processor's own input queue.

    A straight prefix strip, not a split on "-": MESSAGE_PROCESSOR_ID is
    any unique string, not a contiguous integer ordinal, and may itself
    contain hyphens.
    """
    if queue_name.startswith(_MESSAGE_PROCESSOR_QUEUE_PREFIX):
        return queue_name[len(_MESSAGE_PROCESSOR_QUEUE_PREFIX):]
    return None


def declare_adsb_topology(channel) -> None:
    """Declare the hash exchange together with its unroutable-message
    path. The alternate exchange is declared first so the hash exchange
    never briefly exists pointing at something absent."""
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
    """Declare and bind one message processor's input queue; returns its
    name. Binding order assigns the positional slot the exchange hashes
    onto; rebinding an existing binding is a no-op, so a restarting
    processor keeps its slot and moves no aircraft."""
    queue_name = message_processor_queue_name(message_processor_id)
    channel.queue_declare(queue=queue_name, durable=True)
    channel.queue_bind(
        queue=queue_name,
        exchange=ADSB_EXCHANGE,
        routing_key=ADSB_BINDING_WEIGHT,
    )
    return queue_name


def declare_raw_frames_queue(channel) -> None:
    """Declare the short-lived forensic raw-frames queue (see
    RAW_FRAMES_QUEUE_NAME above). Called only when CAPTURE_RAW_FRAMES is
    enabled, unlike declare_adsb_topology()/bind_adsb_queue(), which every
    message processor declares unconditionally."""
    channel.queue_declare(
        queue=RAW_FRAMES_QUEUE_NAME, durable=True, arguments=RAW_FRAMES_QUEUE_ARGUMENTS,
    )
