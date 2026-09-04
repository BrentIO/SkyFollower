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
