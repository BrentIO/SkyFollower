"""
Component self-registration.

Every MQTT-enabled component publishes one retained message announcing
itself -- its Home Assistant discovery `device` block (identifiers, name,
model, sw_version) plus the bare GHCR repository name of the image it was
built from. core-health subscribes to these and uses them to drive a
per-component Home Assistant `update` entity (see
core-health/main.py's _ingest_register()), without ever having to infer a
component's image name from its own identity string.

The image name is baked in at build time the same way VERSION/GIT_COMMIT
already are (see shared/ha_discovery.py's build_ha_device()): every
Dockerfile declares `ARG IMAGE=unknown` / `ENV COMPONENT_IMAGE=$IMAGE`,
and build-container-images.yaml passes `IMAGE=skyfollower-${{ matrix.name }}`
-- the exact same bare name its own discover-images job already computed
as the single source of truth for what each component's published image
is called. This module only ever reads that baked-in value; it never
constructs or guesses an image name itself.
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
    same discovery `device` block the caller already built via
    build_ha_device(), immediately before or after publishing its own HA
    discovery) to ``SkyFollower/register/{device['ids']}``.

    Payload is a single JSON object: ``image`` (the bare
    ``skyfollower-<component>`` GHCR repository name, read from the
    COMPONENT_IMAGE environment variable baked in at build time) and
    ``device`` (the block itself, verbatim -- it already carries
    ``sw_version``, ``name``, and ``ids``, everything core-health needs in
    one message).

    A no-op, logged at debug level, when COMPONENT_IMAGE is unset or still
    the Dockerfile default of "unknown" (a local/manual build with no
    ``--build-arg IMAGE=...``) -- a registration with no real image name
    would only cause core-health to poll the registry for a repository
    that doesn't exist. Also a no-op if `device` carries no ``ids`` (it
    always does in practice; this is just defensive).
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
