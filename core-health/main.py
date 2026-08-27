#!/usr/bin/env python3
"""
SkyFollower Core Health

Standalone, always-on component that polls RabbitMQ's Management HTTP API
and Redis's INFO/MEMORY STATS on its own connections -- independent of any
processor's consuming thread -- and publishes curated MQTT/Home Assistant
telemetry for both. Replaces per-component RabbitMQ queue-depth self-polling
(message-processor's/archive-processor's own rmq_queue_depth samplers,
removal tracked separately) with one centralized poller, and surfaces
richer per-queue and broker-wide RabbitMQ data plus Redis health signals
than existed before.

Also publishes, on behalf of message-processor and the receiver, a handful
of their own Redis-backed application counters (registration/operator
misses, total messages processed, per-connection message totals) using
those components' own exact topic paths, unique_id/object_id, and device
blocks -- nothing on the wire distinguishes core-health publishing these
from the owning component publishing them itself. See
_publish_message_processor_counters()/_poll_receivers() below, and
README.md's "Provisional Redis keys" section for the exact (not yet
finalized elsewhere) key names assumed here: the components that actually
*write* these counters are separate, not-yet-implemented changes at the
time this was built, so this reads defensively -- a missing key means the
count is genuinely zero, never an error.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import NamedTuple, Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

# Add /app to sys.path so shared/ is importable whether running from
# /app/core-health or /app.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.config import ConfigError, load_config
from shared.ha_discovery import build_ha_device
from shared.logging_setup import configure_logging
from shared.mqtt import build_mqtt_client
from shared.rabbitmq_topology import (
    ARCHIVE_QUEUE_NAME,
    is_skyfollower_queue,
    message_processor_id_from_queue_name,
)
from shared.redis_client import build_redis_client
from shared.redis_keys import metrics_registration_misses_key

logger = logging.getLogger("core-health")

MQTT_ROOT = "SkyFollower/core-health"
CORE_DEVICE_IDENTIFIER = "SkyFollower_Core"

# Settled in the issue's design discussion, not operator-tunable (unlike
# TELEMETRY_INTERVAL_SECONDS elsewhere, which is about *publish* cadence):
# RabbitMQ's Management API aggregates stats on its own ~5s internal
# interval broker-side, so 10s stays fresh without re-reading unchanged
# cached data (also the old self-polling's own cadence). Redis's signals
# (memory, persistence status, error counts) don't move on a sub-minute
# timescale in ways that matter here, and a keyspace SCAN/--bigkeys is
# explicitly out of scope for the recurring loop -- INFO/MEMORY STATS alone
# cover every field below.
RABBITMQ_POLL_INTERVAL_SECONDS = 10
REDIS_POLL_INTERVAL_SECONDS = 60

_HEALTHCHECK_HEARTBEAT_PATH = "/app/health/heartbeat"
_HEALTHCHECK_INTERVAL_SECONDS = 15
_HTTP_TIMEOUT_SECONDS = 10


def _sanitize_id(value: str) -> str:
    """Replace any character outside [a-zA-Z0-9_-] with '-' -- Home
    Assistant discovery requires object_id/unique_id to match
    ^[a-zA-Z0-9_-]+$. Same rule receiver/main.py's own _sanitize_mqtt_id
    applies to host/port topic segments."""
    return re.sub(r"[^a-zA-Z0-9_-]", "-", value)


# ---------------------------------------------------------------------------
# Provisional Redis key names -- see module docstring and README.md.
#
# metrics_registration_misses_key() already exists in shared/redis_keys.py
# and is imported above; the two helpers below mirror its exact shape for
# the two new message-processor counters described by #1044's design (not
# yet landed at the time core-health was built), and the two receiver
# helpers mirror shared/redis_keys.py's existing archive_search_index_key()/
# archive_search_key() precedent (an index SET of live names, plus a
# per-name JSON record) for #1046's receiver registration mechanism. None of
# these four are added to shared/redis_keys.py itself here, deliberately --
# #1044/#1046 are separate, not-yet-implemented issues that may land with
# a slightly different exact key, and this avoids a duplicate/conflicting
# definition landing in the same shared module from two different PRs.
# Reconcile these with shared/redis_keys.py once #1044/#1046 land.
# ---------------------------------------------------------------------------

def _metrics_operator_misses_key(message_processor_id: str, period: str) -> str:
    return f"metrics:message_processor:{message_processor_id}:operator_misses:{period}"


def _metrics_total_messages_processed_key(message_processor_id: str, period: str) -> str:
    return f"metrics:message_processor:{message_processor_id}:total_messages_processed:{period}"


def _receiver_index_key() -> str:
    """SET of every receiver name currently claimed (SADD'd by a receiver
    at claim time, per #1046). core-health does one cheap SMEMBERS per
    RabbitMQ poll cycle against this instead of a keyspace SCAN."""
    return "receiver:index"


def _receiver_registration_key(name: str) -> str:
    """Per-receiver registration entry: JSON {"sources": [{"host","port",
    "source"}, ...]}. Refreshed alongside the receiver's own Redis
    heartbeat and TTL'd the same way (per #1046) -- a missing/expired entry
    means the receiver is no longer live, not just claimed once in the
    distant past, and core-health self-heals the index SET when it finds
    one (see _poll_receivers)."""
    return f"receiver:{name}:registration"


def _receiver_message_total_key(name: str, host: str, port: int, period: str) -> str:
    return f"metrics:receiver:{name}:messages_{host}_{port}_total:{period}"


# ---------------------------------------------------------------------------
# HA device / entity tables
# ---------------------------------------------------------------------------

def _core_device() -> dict:
    return build_ha_device(
        identifier=CORE_DEVICE_IDENTIFIER,
        name="SkyFollower Core",
        model="Core Health",
    )


class _QueueTarget(NamedTuple):
    """Where one RabbitMQ queue's entities land: which device they merge
    onto, the MQTT topic root core-health publishes their state under, the
    unique_id/object_id prefix, and a label prefix distinguishing them from
    the owning device's other entities (e.g. "Queue " so message-processor's
    "Queue Consumers" doesn't read as a bare, ambiguous "Consumers" next to
    its own "Active Flights")."""
    device: dict
    state_base: str
    unique_prefix: str
    label_prefix: str


def _queue_target(queue_name: str) -> _QueueTarget:
    pid = message_processor_id_from_queue_name(queue_name)
    if pid is not None:
        return _QueueTarget(
            device=build_ha_device(
                identifier=f"SkyFollower_message_processor_{pid}",
                name=f"SkyFollower Message Processor {pid}",
                model=f"Message Processor {pid}",
            ),
            state_base=f"{MQTT_ROOT}/message-processor/{pid}/statistic",
            unique_prefix=f"SkyFollower_message_processor_{pid}_queue",
            label_prefix="Queue ",
        )
    if queue_name == ARCHIVE_QUEUE_NAME:
        return _QueueTarget(
            device=build_ha_device(
                identifier="SkyFollower_archive", name="SkyFollower Archive", model="Archive"
            ),
            state_base=f"{MQTT_ROOT}/archive/statistic",
            unique_prefix="SkyFollower_archive_queue",
            label_prefix="Queue ",
        )
    # adsb-unroutable, or any other SkyFollower-owned queue with no natural
    # device of its own -- merges onto the shared "SkyFollower Core" device.
    sanitized = _sanitize_id(queue_name)
    return _QueueTarget(
        device=_core_device(),
        state_base=f"{MQTT_ROOT}/queue/{sanitized}/statistic",
        unique_prefix=f"SkyFollower_core_health_queue_{sanitized}",
        label_prefix=f"{queue_name} Queue ",
    )


# (field, name suffix, icon, state_class, unit)
_QUEUE_SENSORS = [
    ("consumers", "Consumers", "mdi:account-multiple", "measurement", None),
    ("consumer_utilisation_percent", "Consumer Utilisation", "mdi:gauge", "measurement", "%"),
    ("messages_ready", "Messages Ready", "mdi:tray-full", "measurement", None),
    ("messages_unacknowledged", "Messages Unacknowledged", "mdi:tray-alert", "measurement", None),
    ("publish_rate", "Publish Rate", "mdi:upload", "measurement", "msg/s"),
    ("deliver_rate", "Deliver Rate", "mdi:download", "measurement", "msg/s"),
    ("ack_rate", "Ack Rate", "mdi:check-circle-outline", "measurement", "msg/s"),
    ("redeliver_rate", "Redeliver Rate", "mdi:refresh", "measurement", "msg/s"),
    ("state", "State", "mdi:information-outline", None, None),
    ("memory_bytes", "Memory", "mdi:memory", "measurement", "B"),
    ("message_bytes", "Message Bytes", "mdi:file-multiple-outline", "measurement", "B"),
]

# (field, name, icon, state_class, unit)
_CORE_GENERAL_SENSORS = [
    ("started_at", "Core Health Started At", "mdi:clock-start", None, None),
    ("version", "Core Health Version", "mdi:tag", None, None),
    ("rabbitmq_connected", "RabbitMQ Management API Connected", "mdi:rabbit", None, None),
    ("redis_connected", "Redis Monitoring Connected", "mdi:database-check", None, None),
]

_CORE_RABBITMQ_SENSORS = [
    ("rabbitmq_connections_total", "RabbitMQ Connections", "mdi:lan-connect", "measurement", None),
    ("rabbitmq_memory_alarm", "RabbitMQ Memory Alarm", "mdi:alert", None, None),
    ("rabbitmq_disk_free_alarm", "RabbitMQ Disk Free Alarm", "mdi:alert-octagon", None, None),
]

_CORE_REDIS_SENSORS = [
    ("redis_used_memory_bytes", "Redis Memory Used", "mdi:memory", "measurement", "B"),
    ("redis_used_memory_peak_percent", "Redis Memory Used Peak", "mdi:memory", "measurement", "%"),
    ("redis_maxmemory_bytes", "Redis Max Memory", "mdi:memory", "measurement", "B"),
    ("redis_maxmemory_policy", "Redis Max Memory Policy", "mdi:cog-outline", None, None),
    ("redis_connected_clients", "Redis Connected Clients", "mdi:account-multiple", "measurement", None),
    ("redis_ops_per_second", "Redis Ops Per Second", "mdi:speedometer", "measurement", "ops/s"),
    ("redis_keyspace_hits", "Redis Keyspace Hits", "mdi:target", "total_increasing", None),
    ("redis_keyspace_misses", "Redis Keyspace Misses", "mdi:target", "total_increasing", None),
    ("redis_keyspace_hit_ratio_percent", "Redis Keyspace Hit Ratio", "mdi:percent", "measurement", "%"),
    ("redis_keys_count", "Redis Keyspace Size", "mdi:key-variant", "measurement", None),
    ("redis_rdb_last_bgsave_status", "Redis RDB Last Bgsave Status", "mdi:content-save", None, None),
    ("redis_aof_last_bgrewrite_status", "Redis AOF Last Bgrewrite Status", "mdi:content-save-cog", None, None),
    ("redis_aof_last_write_status", "Redis AOF Last Write Status", "mdi:content-save-alert", None, None),
    ("redis_role", "Redis Role", "mdi:server", None, None),
    ("redis_connected_slaves", "Redis Connected Replicas", "mdi:server-network", "measurement", None),
    ("redis_total_error_replies", "Redis Total Error Replies", "mdi:alert-circle-outline", "total_increasing", None),
    ("redis_auth_error_count", "Redis Auth Error Count", "mdi:shield-alert-outline", "total_increasing", None),
    ("redis_rejected_connections", "Redis Rejected Connections", "mdi:connection", "total_increasing", None),
    ("redis_evicted_keys", "Redis Evicted Keys", "mdi:delete-alert-outline", "total_increasing", None),
]

# (field, period, kind, label, icon) -- mimicked message-processor counters.
# kind selects which Redis key builder applies (see _mp_counter_key below).
_MP_COUNTER_FIELDS = [
    ("registration_misses_hour", "hour", "registration", "Registration Misses (Hour)", "mdi:broadcast"),
    ("registration_misses_today", "today", "registration", "Registration Misses (Today)", "mdi:broadcast"),
    ("registration_misses_lifetime", "lifetime", "registration", "Registration Misses (Lifetime)", "mdi:broadcast"),
    ("operator_misses_today", "today", "operator", "Operator Misses (Today)", "mdi:account-alert"),
    ("operator_misses_lifetime", "lifetime", "operator", "Operator Misses (Lifetime)", "mdi:account-alert"),
    ("total_messages_processed_hour", "hour", "total_messages", "Total Messages Processed (Hour)", "mdi:counter"),
    ("total_messages_processed_today", "today", "total_messages", "Total Messages Processed (Today)", "mdi:counter"),
    ("total_messages_processed_lifetime", "lifetime", "total_messages", "Total Messages Processed (Lifetime)", "mdi:counter"),
]


def _mp_counter_key(pid: str, kind: str, period: str) -> str:
    if kind == "registration":
        return metrics_registration_misses_key(pid, period)
    if kind == "operator":
        return _metrics_operator_misses_key(pid, period)
    if kind == "total_messages":
        return _metrics_total_messages_processed_key(pid, period)
    raise ValueError(f"unknown counter kind: {kind!r}")


# ---------------------------------------------------------------------------
# Core Health
# ---------------------------------------------------------------------------

class CoreHealth:

    def __init__(self, config: dict) -> None:
        self._cfg = config
        self._started_at = datetime.now(timezone.utc).isoformat()
        self._version = os.environ.get("VERSION", "dev")
        self._shutdown = threading.Event()

        self._session = requests.Session()
        rmc = config["rabbitmq_management"]
        self._rmq_auth = (rmc["username"], rmc["password"])
        self._rmq_base_url = f"http://{rmc['host']}:{rmc['port']}"

        # Two separate Redis clients, deliberately: self._redis is the same
        # default-user credential every other component already uses, for
        # plain key reads (message-processor's/the receiver's application
        # counters). self._redis_monitoring authenticates as the new
        # ACL-scoped, INFO/MEMORY-only user, kept genuinely least-privilege
        # rather than folding INFO/MEMORY access into the default user.
        self._redis = build_redis_client(config["redis"])
        self._redis_monitoring = build_redis_client(config["redis_monitoring"])

        self._mqtt: Optional[mqtt.Client] = None
        self._mqtt_connected = False

        self._rmq_connected = False
        self._redis_connected = False

        # Dynamic-discovery dedup, so a queue/counter/receiver's HA
        # discovery config is published once per MQTT connection lifetime
        # rather than on every poll tick. Cleared on every fresh MQTT
        # connect (see _on_mqtt_connect) so a broker restart still gets a
        # full republish, matching every other component's on-connect
        # discovery behavior.
        self._known_queues: set[str] = set()
        self._known_mp_counters: set[tuple[str, str]] = set()
        self._known_receiver_fields: set[tuple[str, str]] = set()
        self._core_discovery_published = False

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self) -> None:
        configure_logging(self._cfg.get("log_level"))
        logger.info("Starting SkyFollower Core Health %s", self._version)
        self._connect_mqtt()

        threading.Thread(target=self._rabbitmq_poll_loop, daemon=True, name="rabbitmq-poll").start()
        threading.Thread(target=self._redis_poll_loop, daemon=True, name="redis-poll").start()
        threading.Thread(target=self._healthcheck_loop, daemon=True, name="healthcheck").start()

        self._shutdown.wait()

    def _connect_mqtt(self) -> None:
        mc = self._cfg.get("mqtt")
        if not mc or not mc.get("host"):
            logger.warning("No MQTT configured; core-health will poll but publish nothing.")
            return
        self._mqtt = build_mqtt_client(mc, will_topic=f"{MQTT_ROOT}/status")
        self._mqtt.on_connect = self._on_mqtt_connect
        self._mqtt.on_disconnect = self._on_mqtt_disconnect
        try:
            self._mqtt.connect_async(mc["host"], port=mc.get("port", 1883), keepalive=60)
            self._mqtt.loop_start()
        except Exception as exc:
            logger.warning("MQTT connect failed: %s", exc)

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties) -> None:
        self._mqtt_connected = True
        client.publish(f"{MQTT_ROOT}/status", "ONLINE", retain=True)
        self._known_queues.clear()
        self._known_mp_counters.clear()
        self._known_receiver_fields.clear()
        self._core_discovery_published = False
        self._publish_core_discovery()
        logger.info("MQTT connected.")

    def _on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self._mqtt_connected = False

    # ------------------------------------------------------------------
    # Publish helpers
    # ------------------------------------------------------------------

    def _publish_stat(self, topic: str, value) -> None:
        """No-op on None (a sentinel meaning "no fresh reading this tick",
        distinct from a legitimate falsy value like 0 or "False") and
        whenever MQTT isn't currently connected. Leaving a retained topic
        alone rather than overwriting it with a placeholder is what lets
        expire_after/availability be the thing that ages a stale entity
        out, instead of every skip actively lying about the last-known
        value."""
        if value is None or not (self._mqtt and self._mqtt_connected):
            return
        self._mqtt.publish(topic, str(value), retain=True)

    def _redis_counter_or_none(self, client: redis_lib.Redis, key: str) -> Optional[int]:
        """Mirrors message-processor's own _redis_counter() precedent
        (missing/falsy value -> 0) but additionally distinguishes a genuine
        Redis connectivity failure (returns None, so the caller skips
        publishing this tick rather than fabricating a value) from a period
        key that simply doesn't exist yet (0 -- the count is genuinely
        zero, not unknown)."""
        try:
            value = client.get(key)
        except redis_lib.exceptions.RedisError as exc:
            logger.debug("Redis counter read failed for %s: %s", key, exc)
            return None
        return int(value) if value else 0

    # ------------------------------------------------------------------
    # Static "SkyFollower Core" device discovery (broker-wide/Redis/general)
    # ------------------------------------------------------------------

    def _publish_core_discovery(self) -> None:
        if self._core_discovery_published or not (self._mqtt and self._mqtt_connected):
            return
        device = _core_device()
        availability = {
            "availability_topic": f"{MQTT_ROOT}/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }
        groups = (
            (_CORE_GENERAL_SENSORS, f"{MQTT_ROOT}/statistic"),
            (_CORE_RABBITMQ_SENSORS, f"{MQTT_ROOT}/rabbitmq/statistic"),
            (_CORE_REDIS_SENSORS, f"{MQTT_ROOT}/redis/statistic"),
        )
        for sensors, base in groups:
            for field, name, icon, state_class, unit in sensors:
                payload: dict = {
                    **availability,
                    "state_topic": f"{base}/{field}",
                    "name": name,
                    "unique_id": f"SkyFollower_core_health_{field}",
                    "object_id": f"SkyFollower_core_health_{field}",
                    "device": device,
                    "icon": icon,
                }
                if state_class:
                    payload["state_class"] = state_class
                if unit:
                    payload["unit_of_measurement"] = unit
                if field == "started_at":
                    payload["device_class"] = "timestamp"
                self._mqtt.publish(
                    f"homeassistant/sensor/SkyFollower_core_health_{field}/config",
                    json.dumps(payload),
                    retain=True,
                )
        self._core_discovery_published = True

    # ------------------------------------------------------------------
    # RabbitMQ polling
    # ------------------------------------------------------------------

    def _rmq_get(self, path: str):
        response = self._session.get(
            f"{self._rmq_base_url}{path}", auth=self._rmq_auth, timeout=_HTTP_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        return response.json()

    def _rabbitmq_poll_loop(self) -> None:
        while not self._shutdown.is_set():
            self._poll_rabbitmq_once()
            self._shutdown.wait(RABBITMQ_POLL_INTERVAL_SECONDS)

    def _poll_rabbitmq_once(self) -> None:
        try:
            overview = self._rmq_get("/api/overview")
            # /api/overview doesn't itself carry mem_alarm/disk_free_alarm
            # (those are per-node fields) despite being the endpoint named
            # in the original design for "broker-wide memory/disk alarm
            # state" -- polling /api/nodes too is what actually answers
            # that data point; still one cheap GET, same cadence.
            nodes = self._rmq_get("/api/nodes")
            queues = self._rmq_get("/api/queues/%2F")
            self._rmq_connected = True
        except Exception as exc:
            if self._rmq_connected:
                logger.warning("RabbitMQ Management API poll failed: %s", exc)
            self._rmq_connected = False
            overview = nodes = queues = None

        self._publish_core_discovery()
        self._publish_stat(f"{MQTT_ROOT}/statistic/started_at", self._started_at)
        self._publish_stat(f"{MQTT_ROOT}/statistic/version", self._version)
        self._publish_stat(f"{MQTT_ROOT}/statistic/rabbitmq_connected", self._rmq_connected)

        if not self._rmq_connected:
            return

        skyfollower_queues = [q for q in (queues or []) if is_skyfollower_queue(q.get("name", ""))]
        for queue in skyfollower_queues:
            self._publish_queue_stats(queue)

        self._publish_broker_overview(overview, nodes)

        pids = sorted({
            pid for q in skyfollower_queues
            if (pid := message_processor_id_from_queue_name(q.get("name", ""))) is not None
        })
        for pid in pids:
            self._publish_message_processor_counters(pid)

        self._poll_receivers()

    def _publish_broker_overview(self, overview: Optional[dict], nodes: Optional[list]) -> None:
        connections = ((overview or {}).get("object_totals") or {}).get("connections")
        self._publish_stat(f"{MQTT_ROOT}/rabbitmq/statistic/rabbitmq_connections_total", connections)

        mem_alarm = any(bool(n.get("mem_alarm")) for n in (nodes or []))
        disk_alarm = any(bool(n.get("disk_free_alarm")) for n in (nodes or []))
        self._publish_stat(f"{MQTT_ROOT}/rabbitmq/statistic/rabbitmq_memory_alarm", mem_alarm)
        self._publish_stat(f"{MQTT_ROOT}/rabbitmq/statistic/rabbitmq_disk_free_alarm", disk_alarm)

    def _publish_queue_stats(self, queue: dict) -> None:
        name = queue.get("name", "")
        target = _queue_target(name)
        base = target.state_base

        message_stats = queue.get("message_stats") or {}

        def _rate(stat: str) -> float:
            details = message_stats.get(f"{stat}_details") or {}
            return round(details.get("rate") or 0.0, 2)

        # consumer_utilisation was renamed consumer_capacity in newer
        # RabbitMQ releases (the management UI's "Utilisation" column kept
        # the same meaning); read either so this doesn't silently go blank
        # across a broker upgrade.
        consumer_utilisation = queue.get("consumer_utilisation", queue.get("consumer_capacity"))
        utilisation_percent = (
            round(consumer_utilisation * 100, 1)
            if isinstance(consumer_utilisation, (int, float))
            else None
        )

        values = {
            "consumers": queue.get("consumers", 0),
            "consumer_utilisation_percent": utilisation_percent,
            "messages_ready": queue.get("messages_ready", 0),
            "messages_unacknowledged": queue.get("messages_unacknowledged", 0),
            "publish_rate": _rate("publish"),
            "deliver_rate": _rate("deliver"),
            "ack_rate": _rate("ack"),
            "redeliver_rate": _rate("redeliver"),
            "state": queue.get("state", "unknown"),
            "memory_bytes": queue.get("memory", 0),
            "message_bytes": queue.get("message_bytes", 0),
        }
        for field, value in values.items():
            self._publish_stat(f"{base}/{field}", value)

        self._ensure_queue_discovery(name, target)

    def _ensure_queue_discovery(self, queue_name: str, target: _QueueTarget) -> None:
        if queue_name in self._known_queues or not (self._mqtt and self._mqtt_connected):
            return
        availability = {
            "availability_topic": f"{MQTT_ROOT}/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
        }
        for field, name_suffix, icon, state_class, unit in _QUEUE_SENSORS:
            payload: dict = {
                **availability,
                "state_topic": f"{target.state_base}/{field}",
                "name": f"{target.label_prefix}{name_suffix}",
                "unique_id": f"{target.unique_prefix}_{field}",
                "object_id": f"{target.unique_prefix}_{field}",
                "device": target.device,
                "icon": icon,
                # Mirrors message-processor's own
                # rabbitmq_input_queue_depth_hwm precedent: a poll failure
                # leaves the retained value in place without lying about
                # freshness, and this is what actually ages the entity out
                # to unavailable if the outage is sustained. 3x the poll
                # interval tolerates one skipped tick without flapping.
                "expire_after": RABBITMQ_POLL_INTERVAL_SECONDS * 3,
            }
            if state_class:
                payload["state_class"] = state_class
            if unit:
                payload["unit_of_measurement"] = unit
            self._mqtt.publish(
                f"homeassistant/sensor/{target.unique_prefix}_{field}/config",
                json.dumps(payload),
                retain=True,
            )
        self._known_queues.add(queue_name)

    # ------------------------------------------------------------------
    # Message-processor counter mimicry
    # ------------------------------------------------------------------

    def _publish_message_processor_counters(self, pid: str) -> None:
        device = build_ha_device(
            identifier=f"SkyFollower_message_processor_{pid}",
            name=f"SkyFollower Message Processor {pid}",
            model=f"Message Processor {pid}",
        )
        base = f"SkyFollower/message-processor/{pid}/statistic"
        for field, period, kind, label, icon in _MP_COUNTER_FIELDS:
            value = self._redis_counter_or_none(self._redis, _mp_counter_key(pid, kind, period))
            if value is None:
                continue
            self._publish_stat(f"{base}/{field}", value)
            self._ensure_mp_counter_discovery(pid, field, label, icon, device, base)

    def _ensure_mp_counter_discovery(
        self, pid: str, field: str, label: str, icon: str, device: dict, base: str
    ) -> None:
        dedup = (pid, field)
        if dedup in self._known_mp_counters or not (self._mqtt and self._mqtt_connected):
            return
        payload = {
            "availability_topic": f"{MQTT_ROOT}/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
            "state_topic": f"{base}/{field}",
            "name": label,
            "unique_id": f"SkyFollower_message_processor_{pid}_{field}",
            "object_id": f"SkyFollower_message_processor_{pid}_{field}",
            "device": device,
            "icon": icon,
            "state_class": "total_increasing",
        }
        self._mqtt.publish(
            f"homeassistant/sensor/SkyFollower_message_processor_{pid}_{field}/config",
            json.dumps(payload),
            retain=True,
        )
        self._known_mp_counters.add(dedup)

    # ------------------------------------------------------------------
    # Receiver counter mimicry
    # ------------------------------------------------------------------

    def _poll_receivers(self) -> None:
        try:
            names = self._redis.smembers(_receiver_index_key())
        except redis_lib.exceptions.RedisError as exc:
            logger.debug("Receiver index read failed: %s", exc)
            return
        for name in names:
            self._publish_receiver(name)

    def _publish_receiver(self, name: str) -> None:
        try:
            raw = self._redis.get(_receiver_registration_key(name))
        except redis_lib.exceptions.RedisError as exc:
            logger.debug("Receiver registration read failed for %s: %s", name, exc)
            return

        if not raw:
            # Expired/missing registration -- the receiver is gone, or its
            # heartbeat lapsed. Self-heals the index the same way
            # archive_search_index_key's own SMEMBERS callers do for a
            # stale archive_search:{uuid} entry.
            try:
                self._redis.srem(_receiver_index_key(), name)
            except redis_lib.exceptions.RedisError:
                pass
            return

        try:
            registration = json.loads(raw)
            sources = registration.get("sources") or []
        except (TypeError, ValueError) as exc:
            logger.debug("Receiver registration for %s is not valid JSON: %s", name, exc)
            return

        device = build_ha_device(
            identifier=f"SkyFollower_receiver_{name}",
            name=f"SkyFollower Receiver {name}",
            model=name,
        )
        base = f"SkyFollower/receiver/{name}/statistic"
        for src in sources:
            host, port = src.get("host"), src.get("port")
            if host is None or port is None:
                continue
            self._publish_receiver_connection_counters(name, host, port, device, base)

    def _publish_receiver_connection_counters(
        self, name: str, host, port, device: dict, base: str
    ) -> None:
        host_s, port_s = _sanitize_id(str(host)), _sanitize_id(str(port))
        for period, label_suffix in (("hour", "Hour"), ("today", "Today"), ("lifetime", "Lifetime")):
            field = f"messages_{host_s}_{port_s}_total_{period}"
            value = self._redis_counter_or_none(
                self._redis, _receiver_message_total_key(name, host, port, period)
            )
            if value is None:
                continue
            self._publish_stat(f"{base}/{field}", value)
            self._ensure_receiver_discovery(name, host, port, field, label_suffix, device, base)

    def _ensure_receiver_discovery(
        self, name: str, host, port, field: str, label_suffix: str, device: dict, base: str
    ) -> None:
        dedup = (name, field)
        if dedup in self._known_receiver_fields or not (self._mqtt and self._mqtt_connected):
            return
        payload = {
            "availability_topic": f"{MQTT_ROOT}/status",
            "payload_available": "ONLINE",
            "payload_not_available": "OFFLINE",
            "state_topic": f"{base}/{field}",
            "name": f"{host}:{port} Messages Total ({label_suffix})",
            "has_entity_name": True,
            "unique_id": f"SkyFollower_receiver_{name}_{field}",
            "object_id": f"SkyFollower_receiver_{name}_{field}",
            "device": device,
            "icon": "mdi:counter",
            "state_class": "total_increasing",
        }
        self._mqtt.publish(
            f"homeassistant/sensor/SkyFollower_receiver_{name}_{field}/config",
            json.dumps(payload),
            retain=True,
        )
        self._known_receiver_fields.add(dedup)

    # ------------------------------------------------------------------
    # Redis polling
    # ------------------------------------------------------------------

    def _redis_poll_loop(self) -> None:
        while not self._shutdown.is_set():
            self._poll_redis_once()
            self._shutdown.wait(REDIS_POLL_INTERVAL_SECONDS)

    def _poll_redis_once(self) -> None:
        try:
            info = self._redis_monitoring.info(section="everything")
            memory_stats = self._redis_monitoring.memory_stats()
            self._redis_connected = True
        except redis_lib.exceptions.RedisError as exc:
            if self._redis_connected:
                logger.warning("Redis INFO/MEMORY STATS poll failed: %s", exc)
            self._redis_connected = False
            info = memory_stats = None

        self._publish_core_discovery()
        self._publish_stat(f"{MQTT_ROOT}/statistic/redis_connected", self._redis_connected)

        if not self._redis_connected:
            return

        self._publish_redis_stats(info or {}, memory_stats or {})

    def _publish_redis_stats(self, info: dict, memory_stats: dict) -> None:
        base = f"{MQTT_ROOT}/redis/statistic"

        peak_percent = self._parse_percent(info.get("used_memory_peak_perc"))

        hits = info.get("keyspace_hits") or 0
        misses = info.get("keyspace_misses") or 0
        hit_ratio = round(hits / (hits + misses) * 100, 2) if (hits + misses) > 0 else None

        def _error_count(code: str) -> int:
            # INFO ERRORSTATS lines look like "errorstat_NOAUTH:count=N" --
            # redis-py parses each into {"errorstat_NOAUTH": {"count": N}},
            # the same shape it uses for COMMANDSTATS' "calls" field, just
            # keyed "count" here instead.
            entry = info.get(f"errorstat_{code}")
            return int(entry.get("count", 0)) if isinstance(entry, dict) else 0

        auth_errors = _error_count("NOAUTH") + _error_count("WRONGPASS")

        values = {
            "redis_used_memory_bytes": info.get("used_memory"),
            "redis_used_memory_peak_percent": peak_percent,
            "redis_maxmemory_bytes": info.get("maxmemory"),
            "redis_maxmemory_policy": info.get("maxmemory_policy"),
            "redis_connected_clients": info.get("connected_clients"),
            "redis_ops_per_second": info.get("instantaneous_ops_per_sec"),
            "redis_keyspace_hits": hits,
            "redis_keyspace_misses": misses,
            "redis_keyspace_hit_ratio_percent": hit_ratio,
            "redis_keys_count": memory_stats.get("keys.count"),
            "redis_rdb_last_bgsave_status": info.get("rdb_last_bgsave_status"),
            "redis_aof_last_bgrewrite_status": info.get("aof_last_bgrewrite_status"),
            "redis_aof_last_write_status": info.get("aof_last_write_status"),
            "redis_role": info.get("role"),
            "redis_connected_slaves": info.get("connected_slaves"),
            "redis_total_error_replies": info.get("total_error_replies"),
            "redis_auth_error_count": auth_errors,
            "redis_rejected_connections": info.get("rejected_connections"),
            "redis_evicted_keys": info.get("evicted_keys"),
        }
        for field, value in values.items():
            self._publish_stat(f"{base}/{field}", value)

    @staticmethod
    def _parse_percent(raw) -> Optional[float]:
        """used_memory_peak_perc comes back from INFO as a string like
        "50.00%"; redis-py doesn't parse it further. Returns None (skip
        publish) rather than 0 for anything unparseable, so a format change
        upstream shows up as a stale/missing entity, not a silently wrong
        zero."""
        if isinstance(raw, (int, float)):
            return float(raw)
        if isinstance(raw, str) and raw.endswith("%"):
            try:
                return float(raw[:-1])
            except ValueError:
                return None
        return None

    # ------------------------------------------------------------------
    # Docker healthcheck (heartbeat file)
    # ------------------------------------------------------------------

    def _healthcheck_loop(self) -> None:
        """Touch a heartbeat file while genuinely able to reach at least one
        of RabbitMQ's Management API or Redis on its own connections,
        matching every other long-running component's Docker HEALTHCHECK
        precedent (shared/healthcheck.py). Deliberately "or", not "and": a
        single backend being unreachable already surfaces as that backend's
        own entities going unavailable in Home Assistant (via
        expire_after/availability), and shouldn't also flip the whole
        container unhealthy while the other backend is still being polled
        and published just fine."""
        heartbeat_path = pathlib.Path(_HEALTHCHECK_HEARTBEAT_PATH)
        heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
        while not self._shutdown.is_set():
            if self._rmq_connected or self._redis_connected:
                try:
                    heartbeat_path.touch()
                except OSError:
                    pass
            time.sleep(_HEALTHCHECK_INTERVAL_SECONDS)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def shutdown(self) -> None:
        logger.info("Shutdown requested…")
        self._shutdown.set()
        if self._mqtt:
            self._mqtt.publish(f"{MQTT_ROOT}/status", "OFFLINE", retain=True)
            self._mqtt.loop_stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        cfg = load_config("rabbitmq_management", "redis", "redis_monitoring", "mqtt")
    except ConfigError as exc:
        configure_logging()
        logger.critical("%s", exc)
        sys.exit(1)

    app = CoreHealth(cfg)

    def _handle_signal(sig, frame):
        app.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    app.start()


if __name__ == "__main__":
    main()
