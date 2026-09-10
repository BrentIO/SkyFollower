"""
Tests for the map service's minimal MQTT presence
(shared/mqtt_presence.py): Home Assistant discovery + version + start time,
published once per broker connect. Optional -- nothing happens without
MQTT_HOST.

The lifespan is driven directly (its async context manager entered/exited
by hand) with Redis and the state stores mocked, so this stays a fast,
broker-free, Redis-free unit test -- the heavier live-Redis integration
path is test_api.py's job.
"""

from __future__ import annotations

import asyncio
import json
import socket
from unittest.mock import MagicMock, patch

import map.main as map_main


def _free_udp_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _drive_lifespan(monkeypatch, *, mqtt_host=None, version=None, on_connect_client=None):
    monkeypatch.setenv("MAP_REDIS_HOST", "map-redis.invalid")
    monkeypatch.setenv("MAP_LISTEN_HOST", "127.0.0.1")
    monkeypatch.setenv("MAP_LISTEN_PORT", str(_free_udp_port()))
    if mqtt_host is not None:
        monkeypatch.setenv("MQTT_HOST", mqtt_host)
    else:
        monkeypatch.delenv("MQTT_HOST", raising=False)
    if version is not None:
        monkeypatch.setenv("VERSION", version)

    captured = {}

    async def go():
        cm = map_main.lifespan(map_main.app)
        await cm.__aenter__()
        presence = map_main._mqtt_presence
        captured["enabled"] = presence.enabled
        captured["client"] = presence._client
        if on_connect_client is not None and presence._client is not None:
            presence._on_connect(on_connect_client, None, None, 0, None)
            captured["on_connect_pubs"] = list(on_connect_client.publish.call_args_list)
        await cm.__aexit__(None, None, None)
        captured["stopped_pubs"] = (
            list(on_connect_client.publish.call_args_list) if on_connect_client else []
        )
        return presence

    with patch.object(map_main, "configure_logging"), patch.object(
        map_main, "build_redis_client", return_value=MagicMock()
    ), patch.object(map_main, "FlightStateStore", return_value=MagicMock()), patch.object(
        map_main, "RangeOutlineStore", return_value=MagicMock()
    ):
        presence = asyncio.run(go())
    return presence, captured


def test_no_client_created_when_mqtt_host_unset(monkeypatch):
    presence, captured = _drive_lifespan(monkeypatch, mqtt_host=None)
    assert presence.enabled is False
    assert captured["client"] is None


def test_presence_built_for_map_component_when_configured(monkeypatch):
    fake_client = MagicMock()
    with patch("shared.mqtt_presence.build_mqtt_client", return_value=fake_client):
        presence, captured = _drive_lifespan(monkeypatch, mqtt_host="broker.example")
    assert presence.enabled is True
    assert presence._component == "map"
    assert presence._device_identifier == "SkyFollower_map"
    assert presence._status_topic == "SkyFollower/map/status"


def test_discovery_started_at_version_published_retained_on_connect(monkeypatch):
    fake_client = MagicMock()
    with patch(
        "shared.mqtt_presence.build_mqtt_client", return_value=fake_client
    ) as bmc:
        _presence, captured = _drive_lifespan(
            monkeypatch,
            mqtt_host="broker.example",
            version="2026.09.09",
            on_connect_client=fake_client,
        )

    assert bmc.call_args.kwargs["will_topic"] == "SkyFollower/map/status"

    retained = {
        c.args[0]: c.args[1]
        for c in captured["on_connect_pubs"]
        if c.kwargs.get("retain") is True
    }
    assert retained["SkyFollower/map/status"] == "ONLINE"
    assert retained["SkyFollower/map/statistic/version"] == "2026.09.09"
    assert "SkyFollower/map/statistic/started_at" in retained

    discovery = json.loads(retained["homeassistant/sensor/SkyFollower_map_started_at/config"])
    assert discovery["device_class"] == "timestamp"
    assert discovery["state_topic"] == "SkyFollower/map/statistic/started_at"
    assert discovery["device"]["sw_version"] == "2026.09.09"


def test_offline_published_on_clean_shutdown(monkeypatch):
    fake_client = MagicMock()
    with patch("shared.mqtt_presence.build_mqtt_client", return_value=fake_client):
        _presence, captured = _drive_lifespan(
            monkeypatch,
            mqtt_host="broker.example",
            on_connect_client=fake_client,
        )
    offline = [
        c
        for c in captured["stopped_pubs"]
        if c.args[0] == "SkyFollower/map/status" and c.args[1] == "OFFLINE"
    ]
    assert offline and offline[0].kwargs["retain"] is True
    fake_client.loop_stop.assert_called_once()
