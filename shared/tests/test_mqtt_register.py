import json
import os
from unittest.mock import MagicMock, patch

from shared.mqtt_register import REGISTER_TOPIC_ROOT, publish_register


def _device(**overrides):
    device = {
        "ids": "SkyFollower_map",
        "name": "SkyFollower Map",
        "manufacturer": "P5Software, LLC",
        "model": "Map",
        "sw_version": "2026.09.10 (abcdef01)",
    }
    device.update(overrides)
    return device


class TestRegisterTopicRoot:
    def test_is_a_skyfollower_native_topic_not_homeassistant_discovery(self):
        assert REGISTER_TOPIC_ROOT == "SkyFollower/register"


class TestPublishRegister:
    def test_publishes_retained_message_under_register_root_plus_device_id(self):
        client = MagicMock()
        with patch.dict(os.environ, {"COMPONENT_IMAGE": "skyfollower-map"}, clear=True):
            publish_register(client, _device())
        client.publish.assert_called_once_with(
            "SkyFollower/register/SkyFollower_map",
            json.dumps({"image": "skyfollower-map", "device": _device()}),
            retain=True,
        )

    def test_payload_carries_image_and_the_device_block_verbatim(self):
        client = MagicMock()
        device = _device()
        with patch.dict(os.environ, {"COMPONENT_IMAGE": "skyfollower-map"}, clear=True):
            publish_register(client, device)
        topic, raw, kwargs = client.publish.call_args[0][0], client.publish.call_args[0][1], client.publish.call_args[1]
        payload = json.loads(raw)
        assert payload["image"] == "skyfollower-map"
        assert payload["device"] == device
        assert kwargs["retain"] is True

    def test_topic_uses_the_devices_own_identifier(self):
        client = MagicMock()
        with patch.dict(os.environ, {"COMPONENT_IMAGE": "skyfollower-runner-mictronics"}, clear=True):
            publish_register(client, _device(ids="SkyFollower_runner_mictronics"))
        topic = client.publish.call_args[0][0]
        assert topic == "SkyFollower/register/SkyFollower_runner_mictronics"

    def test_no_op_when_component_image_unset(self):
        client = MagicMock()
        with patch.dict(os.environ, {}, clear=True):
            publish_register(client, _device())
        client.publish.assert_not_called()

    def test_no_op_when_component_image_is_the_dockerfile_default(self):
        # ARG IMAGE=unknown -- a local/manual build with no
        # --build-arg IMAGE=... never registers a bogus image name.
        client = MagicMock()
        with patch.dict(os.environ, {"COMPONENT_IMAGE": "unknown"}, clear=True):
            publish_register(client, _device())
        client.publish.assert_not_called()

    def test_no_op_when_component_image_is_empty_string(self):
        client = MagicMock()
        with patch.dict(os.environ, {"COMPONENT_IMAGE": ""}, clear=True):
            publish_register(client, _device())
        client.publish.assert_not_called()

    def test_no_op_when_device_has_no_ids(self):
        client = MagicMock()
        device = _device()
        del device["ids"]
        with patch.dict(os.environ, {"COMPONENT_IMAGE": "skyfollower-map"}, clear=True):
            publish_register(client, device)
        client.publish.assert_not_called()

    def test_never_raises_on_a_malformed_device_block(self):
        client = MagicMock()
        with patch.dict(os.environ, {"COMPONENT_IMAGE": "skyfollower-map"}, clear=True):
            publish_register(client, {})  # no 'ids' at all -- must not raise
        client.publish.assert_not_called()
