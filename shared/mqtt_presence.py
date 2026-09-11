"""
Minimal MQTT presence for the two long-running services that publish no
telemetry of their own -- management-ui and map.

The receiver, message processor, and archive processor each run a periodic
telemetry loop. management-ui and map have nothing equivalent to report:
they just need to (a) appear in Home Assistant with their running image
version and (b) self-register so core-health can drive an "update
available" entity for them. So this helper connects, publishes the
retained discovery + started_at + version topics once per connect, and
then simply stays connected -- no periodic publish, no stats.

It is built on three primitives every other component's MQTT code uses:
shared/mqtt.py's build_mqtt_client() (connection + optional auth +
last-will), shared/ha_discovery.py's build_ha_device() (the discovery
`device` block, whose sw_version carries the running version), and
shared/mqtt_register.py's publish_register() (the self-registration
message core-health reads to learn this component exists and which GHCR
image to check for updates).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional

from shared.ha_discovery import build_ha_device
from shared.mqtt import build_mqtt_client
from shared.mqtt_register import publish_register

logger = logging.getLogger("mqtt-presence")

ONLINE = "ONLINE"
OFFLINE = "OFFLINE"


class MqttPresence:
    """Connect-and-stay-connected MQTT presence for a single-instance
    service. Construct it with the component's `mqtt` config block (from
    shared/config.py's load_config("mqtt", ...)); when the block has no
    host it is inert -- start()/stop() do nothing -- matching the
    optional-MQTT convention every component follows.
    """

    def __init__(
        self,
        mqtt_config: Optional[dict],
        *,
        component: str,
        device_identifier: str,
        device_name: str,
        device_model: str,
        configuration_url: Optional[str] = None,
    ) -> None:
        self._cfg = mqtt_config or {}
        self._component = component
        self._device_identifier = device_identifier
        self._device_name = device_name
        self._device_model = device_model
        self._configuration_url = configuration_url

        self._status_topic = f"SkyFollower/{component}/status"
        self._stat_base = f"SkyFollower/{component}/statistic"
        self._started_at = datetime.now(timezone.utc).isoformat()
        # Same source as the receiver's own version statistic and as
        # build_ha_device()'s sw_version: the VERSION build-arg env var,
        # "dev" for a non-release/local build.
        self._version = os.environ.get("VERSION", "dev")

        self._client = None
        self._connected = False

    @property
    def enabled(self) -> bool:
        return bool(self._cfg.get("host"))

    def start(self) -> None:
        """Build the client (with an OFFLINE last-will on the status topic)
        and start its network loop. No-op when MQTT is not configured."""
        self._client = build_mqtt_client(self._cfg, will_topic=self._status_topic)
        if self._client is None:
            return
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        try:
            self._client.connect_async(
                self._cfg["host"], port=self._cfg.get("port", 1883), keepalive=60
            )
            self._client.loop_start()
        except Exception as exc:  # noqa: BLE001 -- never block startup on the broker
            logger.warning("MQTT connect failed: %s", exc)

    def stop(self) -> None:
        """Publish a retained OFFLINE and stop the network loop -- a clean
        shutdown, so the last-will never has to fire. No-op when MQTT is
        not configured."""
        if self._client is None:
            return
        try:
            self._client.publish(self._status_topic, OFFLINE, retain=True)
            self._client.loop_stop()
        except Exception as exc:  # noqa: BLE001
            logger.debug("MQTT stop error: %s", exc)

    # ------------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        self._connected = True
        client.publish(self._status_topic, ONLINE, retain=True)
        self._publish_discovery()
        self._publish_state()
        logger.info("MQTT connected.")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self._connected = False

    def _publish_state(self) -> None:
        if not (self._client and self._connected):
            return
        self._client.publish(f"{self._stat_base}/started_at", self._started_at, retain=True)
        self._client.publish(f"{self._stat_base}/version", self._version, retain=True)

    def _publish_discovery(self) -> None:
        if not (self._client and self._connected):
            return
        device_kwargs = {}
        if self._configuration_url:
            device_kwargs["configuration_url"] = self._configuration_url
        device = build_ha_device(
            identifier=self._device_identifier,
            name=self._device_name,
            model=self._device_model,
            **device_kwargs,
        )
        publish_register(self._client, device)
        availability = {
            "availability_topic": self._status_topic,
            "payload_available": ONLINE,
            "payload_not_available": OFFLINE,
        }
        # Only Start Time gets an entity. The running version is already
        # carried in every discovery payload's device block via sw_version
        # (and published as a plain retained statistic topic for core-health
        # to read), so a standalone version sensor would just duplicate it --
        # the same choice the receiver and archive processor make.
        payload = {
            **availability,
            "state_topic": f"{self._stat_base}/started_at",
            "name": "Start Time",
            "unique_id": f"{self._device_identifier}_started_at",
            "object_id": f"{self._device_identifier}_started_at",
            "device": device,
            "icon": "mdi:clock-start",
            "device_class": "timestamp",
        }
        self._client.publish(
            f"homeassistant/sensor/{self._device_identifier}_started_at/config",
            json.dumps(payload),
            retain=True,
        )
