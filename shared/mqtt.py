"""
Shared MQTT client construction for SkyFollower.

Every component that publishes to MQTT (receiver, processor,
archive-processor, and all data-runners) builds its mqtt.Client through
build_mqtt_client() so optional username/password authentication is applied
consistently, instead of being reimplemented -- or missed -- at each of the
~40 independent call sites.
"""

from __future__ import annotations

from typing import Optional

import paho.mqtt.client as mqtt


def build_mqtt_client(
    mqtt_config: Optional[dict],
    will_topic: Optional[str] = None,
    will_payload: str = "OFFLINE",
    will_retain: bool = True,
) -> Optional[mqtt.Client]:
    """
    Build an mqtt.Client from a component's `mqtt` settings.json block.

    Returns None if mqtt_config is falsy -- the mqtt block is optional
    everywhere, and every caller already skips MQTT entirely in that case,
    so this preserves today's anonymous/disabled-MQTT behavior unchanged.

    Applies username_pw_set() when `username`/`password` are present in the
    config. The caller still owns connect()/connect_async() and
    loop_start()/loop_stop(), since long-lived services (receiver,
    processor, archive-processor) and one-shot data-runners use different
    connection lifecycles.
    """
    if not mqtt_config:
        return None

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    username = mqtt_config.get("username")
    if username:
        client.username_pw_set(username, mqtt_config.get("password"))

    if will_topic:
        client.will_set(will_topic, will_payload, retain=will_retain)

    return client
