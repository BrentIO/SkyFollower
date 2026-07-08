from unittest.mock import patch

from shared.mqtt import build_mqtt_client


class TestBuildMqttClient:
    def test_returns_none_when_config_absent(self):
        assert build_mqtt_client(None) is None
        assert build_mqtt_client({}) is None

    def test_returns_client_when_config_present(self):
        client = build_mqtt_client({"host": "localhost"})
        assert client is not None

    def test_no_auth_applied_when_credentials_absent(self):
        with patch("shared.mqtt.mqtt.Client") as mock_cls:
            mock_client = mock_cls.return_value
            build_mqtt_client({"host": "localhost"})
            mock_client.username_pw_set.assert_not_called()

    def test_auth_applied_when_username_present(self):
        with patch("shared.mqtt.mqtt.Client") as mock_cls:
            mock_client = mock_cls.return_value
            build_mqtt_client({"host": "localhost", "username": "u", "password": "p"})
            mock_client.username_pw_set.assert_called_once_with("u", "p")

    def test_auth_applied_with_no_password(self):
        """A username with no password still calls username_pw_set (paho
        treats password=None as no-password auth, which is valid for some
        brokers)."""
        with patch("shared.mqtt.mqtt.Client") as mock_cls:
            mock_client = mock_cls.return_value
            build_mqtt_client({"host": "localhost", "username": "u"})
            mock_client.username_pw_set.assert_called_once_with("u", None)

    def test_no_will_set_when_will_topic_absent(self):
        with patch("shared.mqtt.mqtt.Client") as mock_cls:
            mock_client = mock_cls.return_value
            build_mqtt_client({"host": "localhost"})
            mock_client.will_set.assert_not_called()

    def test_will_set_applied_when_will_topic_present(self):
        with patch("shared.mqtt.mqtt.Client") as mock_cls:
            mock_client = mock_cls.return_value
            build_mqtt_client({"host": "localhost"}, will_topic="SkyFollower/x/status")
            mock_client.will_set.assert_called_once_with(
                "SkyFollower/x/status", "OFFLINE", retain=True
            )

    def test_will_set_custom_payload_and_retain(self):
        with patch("shared.mqtt.mqtt.Client") as mock_cls:
            mock_client = mock_cls.return_value
            build_mqtt_client(
                {"host": "localhost"},
                will_topic="SkyFollower/x/status",
                will_payload="DOWN",
                will_retain=False,
            )
            mock_client.will_set.assert_called_once_with(
                "SkyFollower/x/status", "DOWN", retain=False
            )
