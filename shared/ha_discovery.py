"""
Shared Home Assistant MQTT discovery device builder.

Every component that publishes HA discovery built its own inline `device`
dict independently at each call site -- the same duplication
`shared/mqtt.py`'s build_mqtt_client() was extracted to avoid for
connection setup. This does the equivalent for the discovery `device`
block, so `manufacturer`, `sw_version`, and any future common field are
set consistently everywhere instead of needing an identical edit at every
call site.
"""

from __future__ import annotations

import os

MANUFACTURER = "P5Software, LLC"
CONFIGURATION_URL = "https://github.com/BrentIO/SkyFollower"


def build_ha_device(
    identifier: str, name: str, model: str, configuration_url: str = CONFIGURATION_URL
) -> dict:
    """Build a Home Assistant MQTT discovery `device` block.

    sw_version is read from the VERSION and GIT_COMMIT environment variables
    on every call (not cached), so a discovery publish always reflects
    whatever image is currently running -- both baked in by
    build-container-images.yaml via Docker build-args. VERSION falls back to
    "dev" for non-release builds; GIT_COMMIT is appended in parentheses (e.g.
    "9999.99.99 (abcdef01)") only when it's set and isn't the Dockerfiles'
    "unknown" default, so a manual/local docker build with no --build-arg
    COMMIT=... still shows a bare version, unchanged from before this field
    existed.

    configuration_url defaults to the repo root; callers with their own
    docs page (e.g. a data runner) can override it to link there instead.
    """
    version = os.environ.get("VERSION", "dev")
    commit = os.environ.get("GIT_COMMIT", "unknown")
    sw_version = f"{version} ({commit})" if commit != "unknown" else version
    return {
        "ids": identifier,
        "name": name,
        "manufacturer": MANUFACTURER,
        "model": model,
        "sw_version": sw_version,
        "configuration_url": configuration_url,
    }


def build_ha_update_entity(
    device: dict,
    name: str,
    state_topic: str,
    availability: dict | None = None,
) -> dict:
    """Build a Home Assistant MQTT `update` discovery config.

    Surfaces "an update is available" for a component: its running image
    version versus the latest tag published to the container registry.
    This is an indicator only -- the returned config carries no
    `command_topic`, no `payload_install`, and no `INSTALL` supported
    feature, so Home Assistant shows the newer version but offers no
    in-place upgrade button. Applying an update stays a manual
    `docker compose pull`.

    `device` is the same discovery `device` block the component already
    publishes for its other entities (from `build_ha_device()`); passing
    it here nests the update entity under that same device. Its `ids`
    value is reused as the unique_id / object_id stem, so the update
    entity is named consistently with the component it belongs to.

    `state_topic` receives a JSON payload with `installed_version` and
    `latest_version` keys. Home Assistant's MQTT `update` integration
    reads those keys from the JSON natively, so no value template is
    configured here.

    `availability`, when given, is merged in verbatim -- the same
    `availability_topic` / `payload_available` / `payload_not_available`
    dict the caller uses for its other entities, so the update entity
    goes unavailable with the rest when the component drops off the
    broker.
    """
    stem = f"{device['ids']}_update"
    config: dict = {
        "name": name,
        "unique_id": stem,
        "object_id": stem,
        "state_topic": state_topic,
        "entity_category": "diagnostic",
        "device": device,
    }
    if availability:
        config.update(availability)
    return config
