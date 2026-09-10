"""
Tests for shared/mqtt_presence.py's MqttPresence -- the connect-and-stay
minimal MQTT presence used by management-ui and map.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from shared.mqtt_presence import MqttPresence


def _presence(mqtt_config, **overrides):
    kwargs = dict(
        component="widget",
        device_identifier="SkyFollower_widget",
        device_name="SkyFollower Widget",
        device_model="Widget",
    )
    kwargs.update(overrides)
    return MqttPresence(mqtt_config, **kwargs)


class TestDisabled:
    def test_not_enabled_without_host(self):
        assert _presence(None).enabled is False
        assert _presence({}).enabled is False
        assert _presence({"host": ""}).enabled is False

    def test_start_is_noop_without_host(self):
        p = _presence({"host": "", "port": 1883})
        with patch("shared.mqtt_presence.build_mqtt_client", return_value=None) as bmc:
            p.start()
        bmc.assert_called_once()
        assert p._client is None
        # stop() must also be safe when nothing was started
        p.stop()


class TestConnect:
    def _connected_presence(self, **overrides):
        p = _presence({"host": "broker", "port": 1883}, **overrides)
        fake_client = MagicMock()
        with patch("shared.mqtt_presence.build_mqtt_client", return_value=fake_client) as bmc:
            p.start()
        # LWT is registered by build_mqtt_client -- assert we asked for it.
        assert bmc.call_args.kwargs["will_topic"] == "SkyFollower/widget/status"
        fake_client.connect_async.assert_called_once()
        fake_client.loop_start.assert_called_once()
        # Simulate the broker's on_connect callback firing.
        p._on_connect(fake_client, None, None, 0, None)
        return p, fake_client

    def _publishes(self, fake_client):
        return {c.args[0]: (c.args[1], c.kwargs) for c in fake_client.publish.call_args_list}

    def test_publishes_status_online_retained_on_connect(self):
        _p, client = self._connected_presence()
        pubs = self._publishes(client)
        assert pubs["SkyFollower/widget/status"][0] == "ONLINE"
        assert pubs["SkyFollower/widget/status"][1]["retain"] is True

    def test_publishes_started_at_and_version_retained(self):
        with patch.dict("os.environ", {"VERSION": "2026.09.09"}):
            _p, client = self._connected_presence()
        pubs = self._publishes(client)
        assert pubs["SkyFollower/widget/statistic/version"][0] == "2026.09.09"
        assert pubs["SkyFollower/widget/statistic/version"][1]["retain"] is True
        started = pubs["SkyFollower/widget/statistic/started_at"]
        assert started[0].endswith("+00:00")
        assert started[1]["retain"] is True

    def test_version_falls_back_to_dev(self):
        import os

        env = {k: v for k, v in os.environ.items() if k != "VERSION"}
        with patch.dict("os.environ", env, clear=True):
            _p, client = self._connected_presence()
        pubs = self._publishes(client)
        assert pubs["SkyFollower/widget/statistic/version"][0] == "dev"

    def test_publishes_ha_discovery_for_started_at_retained(self):
        with patch.dict("os.environ", {"VERSION": "2026.09.09"}):
            _p, client = self._connected_presence()
        pubs = self._publishes(client)
        topic = "homeassistant/sensor/SkyFollower_widget_started_at/config"
        payload_str, kwargs = pubs[topic]
        assert kwargs["retain"] is True
        payload = json.loads(payload_str)
        assert payload["device_class"] == "timestamp"
        assert payload["state_topic"] == "SkyFollower/widget/statistic/started_at"
        assert payload["unique_id"] == "SkyFollower_widget_started_at"
        assert payload["object_id"] == "SkyFollower_widget_started_at"
        assert payload["availability_topic"] == "SkyFollower/widget/status"
        assert payload["payload_not_available"] == "OFFLINE"
        # Running version rides in the device block, not a standalone sensor.
        assert payload["device"]["sw_version"] == "2026.09.09"

    def test_no_standalone_version_discovery_entity(self):
        _p, client = self._connected_presence()
        topics = {c.args[0] for c in client.publish.call_args_list}
        assert "homeassistant/sensor/SkyFollower_widget_version/config" not in topics

    def test_configuration_url_passed_through_to_device(self):
        _p, client = self._connected_presence(
            configuration_url="https://example.invalid/widget.html"
        )
        pubs = self._publishes(client)
        payload = json.loads(pubs["homeassistant/sensor/SkyFollower_widget_started_at/config"][0])
        assert payload["device"]["configuration_url"] == "https://example.invalid/widget.html"

    def test_stop_publishes_offline_retained_and_stops_loop(self):
        p, client = self._connected_presence()
        client.publish.reset_mock()
        p.stop()
        offline = [c for c in client.publish.call_args_list if c.args[0] == "SkyFollower/widget/status"]
        assert offline and offline[0].args[1] == "OFFLINE"
        assert offline[0].kwargs["retain"] is True
        client.loop_stop.assert_called_once()

    def test_connect_failure_does_not_raise(self):
        p = _presence({"host": "broker", "port": 1883})
        fake_client = MagicMock()
        fake_client.connect_async.side_effect = OSError("no route to host")
        with patch("shared.mqtt_presence.build_mqtt_client", return_value=fake_client):
            p.start()  # must not raise
