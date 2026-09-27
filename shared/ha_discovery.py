"""
Shared Home Assistant MQTT discovery device builder.

Builds the discovery `device` block so `manufacturer`, `sw_version`, and
any future common field are set consistently across every component
instead of needing an identical edit at every call site.
"""

from __future__ import annotations

import os

MANUFACTURER = "P5Software, LLC"
CONFIGURATION_URL = "https://github.com/BrentIO/SkyFollower"


def build_ha_device(
    identifier: str, name: str, model: str, configuration_url: str = CONFIGURATION_URL
) -> dict:
    """Build a Home Assistant MQTT discovery `device` block.

    sw_version is read from VERSION/GIT_COMMIT on every call (not cached),
    so a discovery publish always reflects the running image. GIT_COMMIT
    is appended in parentheses only when set and not the Dockerfiles'
    "unknown" default, so a manual/local build still shows a bare version.

    configuration_url defaults to the repo root; callers with their own
    docs page can override it to link there instead.
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

    Indicator only -- no `command_topic`/`payload_install`/`INSTALL`
    feature, so Home Assistant shows the newer version but offers no
    in-place upgrade button; applying one stays a manual
    `docker compose pull`.

    `device` nests this entity under the component's existing discovery
    device, reusing its `ids` as the unique_id/object_id stem.
    `state_topic` receives JSON with `installed_version` and
    `latest_version` keys, which HA's MQTT `update` integration reads
    natively.
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
