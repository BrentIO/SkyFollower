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


def build_ha_device(identifier: str, name: str, model: str) -> dict:
    """Build a Home Assistant MQTT discovery `device` block.

    sw_version is read from the VERSION environment variable on every call
    (not cached), so a discovery publish always reflects whatever image is
    currently running -- baked in by build-container-images.yaml via a
    Docker build-arg, falling back to "dev" for non-release builds.
    """
    return {
        "ids": identifier,
        "name": name,
        "manufacturer": MANUFACTURER,
        "model": model,
        "sw_version": os.environ.get("VERSION", "dev"),
        "configuration_url": CONFIGURATION_URL,
    }
