"""
Component self-registration.

Every MQTT-enabled component publishes one retained message announcing
itself -- its Home Assistant discovery `device` block plus the bare GHCR
repository name of the image it was built from. core-health subscribes to
these to drive a per-component Home Assistant `update` entity, without
ever having to infer a component's image name from its own identity.

The image name is baked in at build time via `COMPONENT_IMAGE` (every
Dockerfile declares `ARG IMAGE=unknown`); this module only reads that
value, never constructs or guesses one.
"""

from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

# Retained topic root a component's self-registration lands under:
# SkyFollower/register/{device['ids']} -- a SkyFollower-native topic, not
# part of the homeassistant/ discovery namespace.
REGISTER_TOPIC_ROOT = "SkyFollower/register"


def publish_register(client, device: dict) -> None:
    """Publish one retained self-registration message for `device` (the
    discovery `device` block from build_ha_device()) to
    ``SkyFollower/register/{device['ids']}``.

    Payload is ``{"image": ..., "device": ...}``, where ``image`` is read
    from the COMPONENT_IMAGE build-time environment variable.

    A no-op, logged at debug, when COMPONENT_IMAGE is unset or still the
    Dockerfile default of "unknown" (a local/manual build) -- registering
    with no real image name would only cause core-health to poll the
    registry for a repository that doesn't exist. Also a no-op if
    `device` carries no ``ids``.
    """
    image = os.environ.get("COMPONENT_IMAGE")
    identifier = device.get("ids")
    if not image or image == "unknown":
        logger.debug(
            "COMPONENT_IMAGE not set (build with no --build-arg IMAGE=...); "
            "skipping self-registration for %s", identifier,
        )
        return
    if not identifier:
        logger.debug("Discovery device has no 'ids'; skipping self-registration")
        return
    payload = {"image": image, "device": device}
    client.publish(f"{REGISTER_TOPIC_ROOT}/{identifier}", json.dumps(payload), retain=True)
