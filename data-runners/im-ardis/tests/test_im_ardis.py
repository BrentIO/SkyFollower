"""Tests for the Isle of Man ARDIS data runner."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup  # noqa: used in TestHeaderText

# ---------------------------------------------------------------------------
# Module import
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "im_ardis_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["im_ardis_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_build_record = _mod._build_record
_header_text = _mod._header_text
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    registration="M-ABCD",
    manufacturer="Cessna",
    model="172S",
    serial="172S12345",
    mode_s="43E717",
    owners="Island Air Ltd, 1 Strand Street",
    status="Active",
) -> dict:
    return {
        "Registration Mark": registration,
        "Aircraft Manufacturer": manufacturer,
        "Aircraft Type": model,
        "Serial Number": serial,
        "Mode S Number": mode_s,
        "Registered Owners": owners,
        "Aircraft Status": status,
    }


def _make_redis() -> MagicMock:
    return MagicMock()


# ---------------------------------------------------------------------------
# Tests: _header_text
# ---------------------------------------------------------------------------

class TestHeaderText:
    def _cell(self, html: str):
        return BeautifulSoup(html, "lxml").find("th")

    def test_plain_header_unchanged(self):
        cell = self._cell("<th>Aircraft Manufacturer</th>")
        assert _header_text(cell) == "Aircraft Manufacturer"

    def test_strips_sort_link_text(self):
        # ARDIS wraps sort links in <a>; get_text() would concatenate both
        cell = self._cell(
            '<th><a href="#">Sort column by Registration Mark</a>Registration Mark</th>'
        )
        assert _header_text(cell) == "Registration Mark"

    def test_falls_back_to_get_text_if_no_direct_text(self):
        cell = self._cell('<th><span>Mode S Number</span></th>')
        assert _header_text(cell) == "Mode S Number"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row)
        assert record is not None
        assert record["icao_hex"] == "43E717"
        assert record["registration"] == "M-ABCD"
        assert record["source"] == "im-ardis"
        assert record["aircraft"]["manufacturer"] == "Cessna"
        assert record["aircraft"]["model"] == "172S"
        assert record["aircraft"]["serial_number"] == "172S12345"
        assert record["registrant"]["names"] == ["Island Air Ltd"]
        assert record["registrant"]["street"] == ["1 Strand Street"]

    def test_deregistered_returns_none(self):
        row = _make_row(status="Deregistered")
        assert _build_record(row) is None

    def test_missing_mode_s_returns_none(self):
        row = _make_row(mode_s="")
        assert _build_record(row) is None

    def test_missing_registration_returns_none(self):
        row = _make_row(registration="")
        assert _build_record(row) is None

    def test_mode_s_uppercased(self):
        row = _make_row(mode_s="43e717")
        record = _build_record(row)
        assert record["icao_hex"] == "43E717"

    def test_owners_no_comma_stored_as_name_only(self):
        row = _make_row(owners="Island Air Ltd")
        record = _build_record(row)
        assert record["registrant"]["names"] == ["Island Air Ltd"]
        assert "street" not in record["registrant"]

    def test_owners_with_comma_splits_name_and_street(self):
        row = _make_row(owners="Mr John Smith, 10 High Street Douglas")
        record = _build_record(row)
        assert record["registrant"]["names"] == ["Mr John Smith"]
        assert record["registrant"]["street"] == ["10 High Street Douglas"]

    def test_empty_owners_omits_registrant(self):
        row = _make_row(owners="")
        record = _build_record(row)
        assert "registrant" not in record

    def test_empty_manufacturer_omits_field(self):
        row = _make_row(manufacturer="")
        record = _build_record(row)
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_model_omits_field(self):
        row = _make_row(model="")
        record = _build_record(row)
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omits_field(self):
        row = _make_row(serial="")
        record = _build_record(row)
        assert "serial_number" not in record.get("aircraft", {})

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(manufacturer="", model="", serial="")
        record = _build_record(row)
        assert "aircraft" not in record

    def test_source_field(self):
        row = _make_row()
        record = _build_record(row)
        assert record["source"] == "im-ardis"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_active_record_written(self):
        rows = [_make_row()]
        r = _make_redis()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_deregistered_record_skipped(self):
        rows = [_make_row(status="Deregistered")]
        r = _make_redis()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_missing_mode_s_skipped(self):
        rows = [_make_row(mode_s="")]
        r = _make_redis()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_list_returns_zero(self):
        r = _make_redis()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_writes_to_detail_key(self):
        rows = [_make_row(mode_s="43E717")]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        assert set_call[0][0] == "aircraft:registry:43E717"

    def test_ttl_applied(self):
        rows = [_make_row(mode_s="43E717")]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:43E717", REDIS_TTL)

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "im-ardis"

    def test_mixed_active_and_deregistered(self):
        rows = [
            _make_row(registration="M-ABCD", mode_s="43E717", status="Active"),
            _make_row(registration="M-ZZZZ", mode_s="43E718", status="Deregistered"),
            _make_row(registration="M-EFGH", mode_s="43E719", status="Active"),
        ]
        r = _make_redis()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 2


# ---------------------------------------------------------------------------
# Tests: publish_completion_stats
# ---------------------------------------------------------------------------

class TestPublishCompletionStats:
    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_no_mqtt_config_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_mqtt_connect_timeout_does_not_raise(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        with patch("im_ardis_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("im_ardis_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("im_ardis_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 67, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "67"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("im_ardis_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/im-ardis"
