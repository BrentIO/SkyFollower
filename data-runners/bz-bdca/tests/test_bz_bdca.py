"""Tests for the Belize BDCA data runner."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

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
        "bz_bdca_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bz_bdca_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_clean_owner = _mod._clean_owner
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    registration="V3-ABC",
    make_model="Cessna, 172",
    serial="28-12345",
    owner="Belize Air Ltd",
    address="Belize City",
) -> dict:
    return {
        "Registration Number": registration,
        "Manufacturer, Model": make_model,
        "Serial Number": serial,
        "Owner": owner,
        "Address": address,
    }


def _make_redis_with_search(icao_hex="0A0123", registration="V3-ABC"):
    r = MagicMock()
    doc = MagicMock()
    doc.id = f"aircraft:mictronics:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    return r


def _make_redis_no_match():
    r = MagicMock()
    results = MagicMock()
    results.docs = []
    r.ft.return_value.search.return_value = results
    return r


# ---------------------------------------------------------------------------
# Tests: _clean_owner
# ---------------------------------------------------------------------------

class TestCleanOwner:
    def test_plain_name_unchanged(self):
        assert _clean_owner("Belize Air Ltd") == "Belize Air Ltd"

    def test_strips_charterer_suffix(self):
        result = _clean_owner("Belize Air Ltd (Charterer by Demise)")
        assert result == "Belize Air Ltd"

    def test_strips_charterer_case_insensitive(self):
        result = _clean_owner("Owner Name (charterer by demise)")
        assert result == "Owner Name"

    def test_strips_leading_trailing_whitespace(self):
        assert _clean_owner("  Belize Air Ltd  ") == "Belize Air Ltd"

    def test_empty_string(self):
        assert _clean_owner("") == ""

    def test_only_charterer_suffix_returns_empty(self):
        result = _clean_owner("(Charterer by Demise)")
        assert result == ""


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "0A0123", "V3-ABC")
        assert record["icao_hex"] == "0A0123"
        assert record["registration"] == "V3-ABC"
        assert record["source"] == "bz-bdca"
        assert record["aircraft"]["model"] == "Cessna, 172"
        assert record["aircraft"]["serial_number"] == "28-12345"
        assert record["registrant"]["names"] == ["Belize Air Ltd"]
        assert record["registrant"]["city"] == "Belize City"
        assert "street" not in record["registrant"]

    def test_null_serial_em_dash_omitted(self):
        row = _make_row(serial="–")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert "serial_number" not in record.get("aircraft", {})

    def test_null_serial_hyphen_omitted(self):
        row = _make_row(serial="-")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert "serial_number" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert "serial_number" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(make_model="")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert "model" not in record.get("aircraft", {})

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(make_model="", serial="")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert "aircraft" not in record

    def test_owner_charterer_stripped(self):
        row = _make_row(owner="Private Owner (Charterer by Demise)")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert record["registrant"]["names"] == ["Private Owner"]

    def test_empty_owner_empty_address_omits_registrant(self):
        row = _make_row(owner="", address="")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert "registrant" not in record

    def test_address_with_comma_splits_street_and_city(self):
        row = _make_row(address="Blue Creek, O/Walk")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert record["registrant"]["street"] == ["Blue Creek"]
        assert record["registrant"]["city"] == "O/Walk"

    def test_address_without_comma_stored_as_city_only(self):
        row = _make_row(address="Belize City")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert record["registrant"]["city"] == "Belize City"
        assert "street" not in record["registrant"]

    def test_address_comma_no_street_stored_as_city_only(self):
        row = _make_row(address=", Belize City")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert record["registrant"]["city"] == "Belize City"
        assert "street" not in record["registrant"]

    def test_empty_address_omits_city(self):
        row = _make_row(address="")
        record = _build_record(row, "0A0123", "V3-ABC")
        assert "city" not in record.get("registrant", {})


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("V3ABC") == "V3ABC"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("V3-ABC")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="0A0123", registration="V3-ABC")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_registration_skipped(self):
        rows = [_make_row(registration="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="0A0123", registration="V3-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        key_used = set_call[0][0]
        assert key_used == "aircraft:registry:0A0123"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="0A0123", registration="V3-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        written = set_call[0][2]
        assert written.get("source") == "bz-bdca"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="0A0123", registration="V3-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:0A0123", REDIS_TTL)


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
        with patch("bz_bdca_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("bz_bdca_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("bz_bdca_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 67, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "67"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("bz_bdca_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/bz-bdca"
