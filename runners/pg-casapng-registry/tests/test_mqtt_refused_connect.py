from __future__ import annotations

import importlib.util
import inspect
import logging
import os
import sys
from unittest.mock import MagicMock, patch

from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.packettypes import PacketTypes

_RUNNER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

_NAME = f"{os.path.basename(_RUNNER_DIR).replace('-', '_')}_refused_connect_main"
_spec = importlib.util.spec_from_file_location(_NAME, os.path.join(_RUNNER_DIR, "main.py"))
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_NAME] = _mod
_spec.loader.exec_module(_mod)

_CFG = {"mqtt": {"host": "localhost", "port": 1883}}


def _publish():
    kwargs = {}
    for name in inspect.signature(_mod.publish_completion_stats).parameters:
        if name == "cfg":
            kwargs[name] = _CFG
        elif name == "status":
            kwargs[name] = "success"
        else:
            kwargs[name] = 1
    _mod.publish_completion_stats(**kwargs)


def _client_answering_connect_with(reason_code):
    client = MagicMock()
    client.connect.side_effect = lambda *a, **k: client.on_connect(
        client, None, None, reason_code, None
    )
    return client


def test_refused_connection_skips_publish_and_does_not_report_success(caplog):
    refused = ReasonCode(PacketTypes.CONNACK, "Not authorized")
    client = _client_answering_connect_with(refused)
    with patch.object(_mod, "build_mqtt_client", return_value=client), \
            caplog.at_level(logging.INFO):
        _publish()
    client.publish.assert_not_called()
    assert "MQTT connection refused" in caplog.text
    assert "MQTT stats published" not in caplog.text


def test_accepted_connection_publishes(caplog):
    accepted = ReasonCode(PacketTypes.CONNACK, "Success")
    client = _client_answering_connect_with(accepted)
    with patch.object(_mod, "build_mqtt_client", return_value=client), \
            patch("time.sleep"), caplog.at_level(logging.INFO):
        _publish()
    assert client.publish.called
    assert "MQTT stats published" in caplog.text
